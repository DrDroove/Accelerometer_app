#include <iostream>
#include <memory>
#include <string>
#include <mutex>
#include <cmath>
#include <condition_variable>
#include <thread>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "accelerometer.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientBidiReactor;


class NodeBClientReactor : public ClientBidiReactor<accel::AccelModule, accel::AccelPacket> {
private:
    ClientContext context_;
    accel::AccelPacket incoming_packet_; 
    accel::AccelModule outgoing_module_; 
    
    std::mutex mtx_;
    std::condition_variable cv_;
    bool done_ = false;

public:
    NodeBClientReactor(accel::AccelerometerService::Stub* stub) {
        
        stub->async()->ProcessAccelData(&context_, this);
        
        
        StartRead(&incoming_packet_);
        
        
        StartCall();
        std::cout << "[Node B] Stream opened. Waiting for coordinates...\n";
    }

    
    void OnReadDone(bool ok) override {
        if (!ok) {
            std::cout << "[Node B] Stream closed by server.\n";
            OnDone(grpc::Status::OK);
            return;
        }

        
        float module = std::hypot(incoming_packet_.x(), incoming_packet_.y(), incoming_packet_.z());
        
        outgoing_module_.set_timestamp(incoming_packet_.timestamp());
        outgoing_module_.set_module(module);

        
        StartWrite(&outgoing_module_);
    }

    
    void OnWriteDone(bool ok) override {
        if (!ok) {
            std::cout << "[Node B] Write failed.\n";
            OnDone(grpc::Status::CANCELLED);
            return;
        }

        StartRead(&incoming_packet_);
    }

    void OnDone(const grpc::Status& status) override {
        std::lock_guard<std::mutex> lock(mtx_);
        done_ = true;
        cv_.notify_one();
        if (!status.ok()) {
            std::cout << "[Node B] Stream error: " << status.error_message() << "\n";
        }
    }

    void Wait() {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this] { return done_; });
    }
};

class ComputeApplication {
private:
    std::string target_address_;
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<accel::AccelerometerService::Stub> stub_;
    std::atomic<bool> running_{true};

public:
    ComputeApplication(const std::string& address) 
        : target_address_(address), running_(true) {
        
        channel_ = grpc::CreateChannel(target_address_, grpc::InsecureChannelCredentials());
        stub_ = accel::AccelerometerService::NewStub(channel_);
    }

    ~ComputeApplication() {
        running_ = false;
    }

    void Run() {
        std::cout << "[Node B] Application context initialized. Ready to process data.\n";
        
        while (running_) {
            std::cout << "[Node B] Connecting to server at " << target_address_ << "...\n";
            
            auto reactor = std::make_unique<NodeBClientReactor>(stub_.get());
            
            
            reactor->Wait();

           
            std::cout << "[Node B] Connection lost. Reconnecting in 3 seconds...\n";
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
};

int main() {
    ComputeApplication app("127.0.0.1:50051");
    app.Run();
    return 0;
}