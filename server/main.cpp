#include <iostream>
#include <memory>
#include <string>
#include <mutex>
#include <cmath>
#include <queue>
#include <atomic>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "accelerometer.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::CallbackServerContext;
using grpc::ServerBidiReactor;


class NodeAReactor;
class NodeBReactor;
class AccelerometerServiceImpl;


class NodeAReactor : public ServerBidiReactor<accel::AccelPacket, accel::AccelModule> {
private:
    AccelerometerServiceImpl* service_;
    accel::AccelPacket request_;
    std::queue<accel::AccelModule> write_queue_;
    bool write_in_progress_ = false;
    std::mutex write_mtx_;

    void WriteNext() {
        if (write_queue_.empty()) { write_in_progress_ = false; return; }
        write_in_progress_ = true;
        static accel::AccelModule active_response; 
        active_response = write_queue_.front();
        write_queue_.pop();
        StartWrite(&active_response);
    }
public:
    NodeAReactor(AccelerometerServiceImpl* service);
    void OnReadDone(bool ok) override;
    void OnWriteDone(bool ok) override;
    void OnDone() override;
    void PushModuleToClient(const accel::AccelModule& module) {
        std::lock_guard<std::mutex> lock(write_mtx_);
        write_queue_.push(module);
        if (!write_in_progress_) WriteNext();
    }
};

class NodeBReactor : public ServerBidiReactor<accel::AccelModule, accel::AccelPacket> {
private:
    AccelerometerServiceImpl* service_;
    accel::AccelModule request_;
    std::queue<accel::AccelPacket> write_queue_;
    bool write_in_progress_ = false;
    std::mutex write_mtx_;

    void WriteNext() {
        if (write_queue_.empty()) { write_in_progress_ = false; return; }
        write_in_progress_ = true;
        static accel::AccelPacket active_response;
        active_response = write_queue_.front();
        write_queue_.pop();
        StartWrite(&active_response);
    }
public:
    NodeBReactor(AccelerometerServiceImpl* service);
    void OnReadDone(bool ok) override;
    void OnWriteDone(bool ok) override;
    void OnDone() override;
    void PushPacketToClient(const accel::AccelPacket& packet) {
        std::lock_guard<std::mutex> lock(write_mtx_);
        write_queue_.push(packet);
        if (!write_in_progress_) WriteNext();
    }
};


class AccelerometerServiceImpl final : public accel::AccelerometerService::CallbackService {
public:
    std::mutex mtx_;
    NodeAReactor* node_a_reactor_ = nullptr;
    NodeBReactor* node_b_reactor_ = nullptr;
private:
    bool has_previous_ = false;
    float last_x_ = 0.0f, last_y_ = 0.0f, last_z_ = 0.0f;
public:
    bool IsDuplicate(const accel::AccelPacket& packet) {
        if (!has_previous_) return false;
        return (std::abs(packet.x() - last_x_) < 0.001f) &&
               (std::abs(packet.y() - last_y_) < 0.001f) &&
               (std::abs(packet.z() - last_z_) < 0.001f);
    }
    void UpdateLastPacket(const accel::AccelPacket& packet) {
        last_x_ = packet.x(); last_y_ = packet.y(); last_z_ = packet.z(); has_previous_ = true;
    }
    ServerBidiReactor<accel::AccelPacket, accel::AccelModule>* StreamAccelData(CallbackServerContext*) override {
        return new NodeAReactor(this);
    }
    ServerBidiReactor<accel::AccelModule, accel::AccelPacket>* ProcessAccelData(CallbackServerContext*) override {
        return new NodeBReactor(this);
    }
};


NodeAReactor::NodeAReactor(AccelerometerServiceImpl* service) : service_(service) {
    std::lock_guard<std::mutex> lock(service_->mtx_);
    service_->node_a_reactor_ = this;
    std::cout << "[Server] Node A connected.\n";
    StartRead(&request_);
}
void NodeAReactor::OnReadDone(bool ok) {
    if (!ok) { OnDone(); return; }
    std::lock_guard<std::mutex> lock(service_->mtx_);
    if (!service_->IsDuplicate(request_)) {
        service_->UpdateLastPacket(request_);
        if (service_->node_b_reactor_) {
            service_->node_b_reactor_->PushPacketToClient(request_);
        }
    } else {
        std::cout << "[Server] Duplicate dropped (Precision 0.001).\n";
    }
    StartRead(&request_);
}
void NodeAReactor::OnWriteDone(bool ok) {
    if (!ok) { OnDone(); return; }
    std::lock_guard<std::mutex> lock(write_mtx_);
    WriteNext();
}
void NodeAReactor::OnDone() {
    std::lock_guard<std::mutex> lock(service_->mtx_);
    if (service_->node_a_reactor_ == this) service_->node_a_reactor_ = nullptr;
    std::cout << "[Server] Node A disconnected.\n";
    delete this;
}
NodeBReactor::NodeBReactor(AccelerometerServiceImpl* service) : service_(service) {
    std::lock_guard<std::mutex> lock(service_->mtx_);
    service_->node_b_reactor_ = this;
    std::cout << "[Server] Node B connected.\n";
    StartRead(&request_);
}
void NodeBReactor::OnReadDone(bool ok) {
    if (!ok) { OnDone(); return; }
    std::lock_guard<std::mutex> lock(service_->mtx_);
    if (service_->node_a_reactor_) {
        service_->node_a_reactor_->PushModuleToClient(request_);
    }
    StartRead(&request_);
}
void NodeBReactor::OnWriteDone(bool ok) {
    if (!ok) { OnDone(); return; }
    std::lock_guard<std::mutex> lock(write_mtx_);
    WriteNext();
}
void NodeBReactor::OnDone() {
    std::lock_guard<std::mutex> lock(service_->mtx_);
    if (service_->node_b_reactor_ == this) service_->node_b_reactor_ = nullptr;
    std::cout << "[Server] Node B disconnected.\n";
    delete this;
}



class ServerApplication {
private:
    std::string address_;
    AccelerometerServiceImpl service_;
    std::unique_ptr<Server> server_;

public:
    ServerApplication(const std::string& address) : address_(address) {}

    
    ~ServerApplication() {
        Shutdown();
    }

    void Start() {
        ServerBuilder builder;
        
        builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
        
        
        builder.RegisterService(&service_);

        
        server_ = builder.BuildAndStart();
        std::cout << "[Server] Application context started. Listening on " << address_ << std::endl;
    }

    void Wait() {
        if (server_) {
            server_->Wait(); 
        }
    }

    void Shutdown() {
        if (server_) {
            std::cout << "[Server] Shutting down gRPC engine gracefully...\n";
            server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
            server_.reset();
        }
    }
};


int main(int argc, char* argv[]) {
    std::string port = "50051";

    if(argc > 1){
        port = argv[1];
    }

    std::string server_adress = "0.0.0.0:" + port;
    ServerApplication app(server_adress);
    
    app.Start();
    app.Wait();
    
    return 0;
}