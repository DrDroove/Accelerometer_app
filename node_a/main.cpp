#include <iostream>
#include <memory>
#include <string>
#include <mutex>
#include <cmath>
#include <chrono>
#include <thread>
#include <fstream>
#include <filesystem>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <sstream>

#include <grpcpp/grpcpp.h>
#include "accelerometer.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientBidiReactor;

class SensorApplication;

class NodeAClientReactor : public ClientBidiReactor<accel::AccelPacket, accel::AccelModule> {
private:
    ClientContext context_;
    accel::AccelModule incoming_module_; 
    std::queue<accel::AccelPacket> write_queue_;
    bool write_in_progress_ = false;
    std::mutex write_mtx_;
    std::ofstream log_file_;

    accel::AccelPacket active_packet_;

    SensorApplication& app_;

    void WriteNext() {
        if (write_queue_.empty()) {
            write_in_progress_ = false;
            return;
        }
        write_in_progress_ = true;
        
        active_packet_ = write_queue_.front();
        write_queue_.pop();
        StartWrite(&active_packet_);
    }

public:
    NodeAClientReactor(accel::AccelerometerService::Stub* stub, SensorApplication& app) : app_(app) {
        std::filesystem::create_directories("accel");
        log_file_.open("accel/module.log", std::ios::app);
        
        stub->async()->StreamAccelData(&context_, this);
        StartRead(&incoming_module_);
        StartCall();
    }

    void OnReadDone(bool ok) override {
        if (!ok) return;

        if (log_file_.is_open()) {
            log_file_ << incoming_module_.timestamp() << " " << incoming_module_.module() << "\n";
            log_file_.flush();
        }
        StartRead(&incoming_module_);
    }

    void OnWriteDone(bool ok) override {
        if (!ok) return;
        std::lock_guard<std::mutex> lock(write_mtx_);
        WriteNext();
    }

    void OnDone(const grpc::Status& status) override;

    void SendData(int64_t timestamp, float x, float y, float z) {
        std::lock_guard<std::mutex> lock(write_mtx_);
        accel::AccelPacket packet;
        packet.set_timestamp(timestamp);
        packet.set_x(x);
        packet.set_y(y);
        packet.set_z(z);

        write_queue_.push(packet);
        if (!write_in_progress_) {
            WriteNext();
        }
    }
};


class SensorApplication {
private:
    std::string target_address_;
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<accel::AccelerometerService::Stub> stub_;
    
    std::mutex reactor_mtx_;
    NodeAClientReactor* active_reactor_{nullptr};
    
    std::mutex cv_mtx_;
    std::condition_variable cv_;
    bool reactor_done_ = false;

    std::thread sensor_thread_;
    std::atomic<bool> running_{true};

    int frequency_hz_ = 50;
    int sleep_interval_ms_ = 20;

    friend class NodeAClientReactor;

    void LoadConfiguration(const std::string& config_path) {
        std::ifstream file(config_path);
        if (!file.is_open()) {
            std::cout << "[Node A] Config file not found at " << config_path 
                      << ". Using default frequency: " << frequency_hz_ << " Hz\n";
            return;
        }

        std::string line;
        while (std::getline(file, line)) {
            if (line.empty() || line[0] == '#') continue;
            std::size_t delimiter_pos = line.find('=');
            if (delimiter_pos == std::string::npos) continue;

            std::string key = line.substr(0, delimiter_pos);
            std::string value_str = line.substr(delimiter_pos + 1);

            if (key == "sensor_frequency_hz") {
                try {
                    int hz = std::stoi(value_str);
                    if (hz > 0) {
                        frequency_hz_ = hz;
                        sleep_interval_ms_ = 1000 / frequency_hz_;
                    } else{
                        std::cerr << "[Warning] Frequency must be positive. Using default.\n";
                    }
                } catch (...) {
                    std::cerr << "[Warning] Invalid frequency value in config. Using default.\n";
                }
            }
        }
        std::cout << "[Node A] Configuration loaded. Frequency: " << frequency_hz_ 
                  << " Hz (Interval: " << sleep_interval_ms_ << " ms)\n";
    }

    void SensorEmulatorLoop() {
        float time_counter = 0.0f;
        while (running_) {
            auto now = std::chrono::system_clock::now();
            int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()).count();

            float x = std::sin(time_counter) * 2.0f;
            float y = std::cos(time_counter) * 9.8f;
            float z = std::sin(time_counter * 0.5f) * 0.5f;

            {
                std::lock_guard<std::mutex> lock(reactor_mtx_);
                if (active_reactor_ != nullptr) {
                    active_reactor_->SendData(timestamp, x, y, z);
                }
            }

            time_counter += 0.1f;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_interval_ms_));
        }
    }

public:
    SensorApplication(const std::string& address, const std::string& config_path) 
        : target_address_(address), running_(true) {
        
        LoadConfiguration(config_path);
        channel_ = grpc::CreateChannel(target_address_, grpc::InsecureChannelCredentials());
        stub_ = accel::AccelerometerService::NewStub(channel_);
        sensor_thread_ = std::thread(&SensorApplication::SensorEmulatorLoop, this);
    }

    ~SensorApplication() {
        running_ = false;
        if (sensor_thread_.joinable()) {
            sensor_thread_.join();
        }
    }

    
    void NotifyReactorDone(NodeAClientReactor* reporting_reactor) {
        {
            std::lock_guard<std::mutex> lock(reactor_mtx_);
            if (active_reactor_ == reporting_reactor) {
                active_reactor_ = nullptr;
            }
        }

        std::lock_guard<std::mutex> lock(cv_mtx_);
        reactor_done_ = true;
        cv_.notify_one();
    }

    void Run() {
        std::cout << "[Node A] Application context initialized. Starting reconnect loop...\n";
        
        while (running_) {
            std::cout << "[Node A] Connecting to server at " << target_address_ << "...\n";
            
            {
                std::lock_guard<std::mutex> lock(cv_mtx_);
                reactor_done_ = false;
            }

            NodeAClientReactor* reactor = new NodeAClientReactor(stub_.get(), *this);
            
            {
                std::lock_guard<std::mutex> lock(reactor_mtx_);
                active_reactor_ = reactor;
            }
            
            {
                std::unique_lock<std::mutex> lock(cv_mtx_);
                cv_.wait(lock, [this] { return reactor_done_; });
            }

            if (!running_) break;

            std::cout << "[Node A] Connection lost. Reconnecting in 3 seconds...\n";
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
};

void NodeAClientReactor::OnDone(const grpc::Status& status) {
    if (log_file_.is_open()) {
        log_file_.close();
    }
    
    app_.NotifyReactorDone(this);

    delete this;
}

int main(int argc, char* argv[]) {
    std::string server_adress = "127.0.0.1:50051";
    if(argc > 1){
        server_adress = argv[1];
    }
    SensorApplication app(server_adress, "config.txt");
    app.Run();
    return 0;
}