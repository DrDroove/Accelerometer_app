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
    
    std::mutex mtx_;
    std::condition_variable cv_;
    bool done_ = false;
    std::ofstream log_file_;

    void WriteNext() {
        if (write_queue_.empty()) {
            write_in_progress_ = false;
            return;
        }
        write_in_progress_ = true;
        static accel::AccelPacket active_packet;
        active_packet = write_queue_.front();
        write_queue_.pop();
        StartWrite(&active_packet);
    }

public:
    NodeAClientReactor(accel::AccelerometerService::Stub* stub) {
        std::filesystem::create_directories("accel");
        log_file_.open("accel/module.log", std::ios::app);
        
        stub->async()->StreamAccelData(&context_, this);
        StartRead(&incoming_module_);
        StartCall();
    }

    void OnReadDone(bool ok) override {
        if (!ok) {
            OnDone(grpc::Status::OK);
            return;
        }

        std::cout << "[Node A] Recv back from server: Timestamp=" << incoming_module_.timestamp() 
                  << " Module=" << incoming_module_.module() << "\n";

        if (log_file_.is_open()) {
            log_file_ << incoming_module_.timestamp() << " " << incoming_module_.module() << "\n";
            log_file_.flush();
        }
        StartRead(&incoming_module_);
    }

    void OnWriteDone(bool ok) override {
        if (!ok) {
            OnDone(grpc::Status::CANCELLED);
            return;
        }
        std::lock_guard<std::mutex> lock(write_mtx_);
        WriteNext();
    }

    void OnDone(const grpc::Status& status) override {
        std::lock_guard<std::mutex> lock(mtx_);
        done_ = true;
        cv_.notify_one();
        if (log_file_.is_open()) log_file_.close();
    }

    void SendData(int64_t timestamp, float x, float y, float z) {
        accel::AccelPacket packet;
        packet.set_timestamp(timestamp);
        packet.set_x(x);
        packet.set_y(y);
        packet.set_z(z);

        std::lock_guard<std::mutex> lock(write_mtx_);
        write_queue_.push(packet);
        if (!write_in_progress_) {
            WriteNext();
        }
    }

    void Wait() {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this] { return done_; });
    }
};


class SensorApplication {
private:
    std::string target_address_;
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<accel::AccelerometerService::Stub> stub_;
    
    
    std::atomic<NodeAClientReactor*> active_reactor_{nullptr};
    
    std::thread sensor_thread_;
    std::atomic<bool> running_{true};

    
    void SensorEmulatorLoop() {
        float time_counter = 0.0f;
        while (running_) {
            auto now = std::chrono::system_clock::now();
            int64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()).count();

            float x = std::sin(time_counter) * 2.0f;
            float y = std::cos(time_counter) * 9.8f;
            float z = std::sin(time_counter * 0.5f) * 0.5f;

            NodeAClientReactor* reactor = active_reactor_.load();
            if (reactor != nullptr) {
                reactor->SendData(timestamp, x, y, z);
            }

            time_counter += 0.1f;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

public:
    SensorApplication(const std::string& address) 
        : target_address_(address), running_(true) {
        
        
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

    
    void Run() {
        std::cout << "[Node A] Application context initialized. Starting reconnect loop...\n";
        
        while (running_) {
            std::cout << "[Node A] Connecting to server at " << target_address_ << "...\n";
            
            auto reactor = std::make_unique<NodeAClientReactor>(stub_.get());
            
            active_reactor_.store(reactor.get());
            
            reactor->Wait();

            active_reactor_.store(nullptr);

            std::cout << "[Node A] Connection lost. Reconnecting in 3 seconds...\n";
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
};


int main() {
    SensorApplication app("127.0.0.1:50051");
    app.Run();
    return 0;
}