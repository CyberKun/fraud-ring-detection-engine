#include "service/server.hpp"
#include <csignal>
#include <iostream>

namespace {
volatile std::sig_atomic_t stopping=0;
void stop(int){stopping=1;}
}
int main(int argc,char** argv) {
    using namespace fraud::service;
    try {
        fs::path dir="fraud-data";
        std::string listen="127.0.0.1:50051";
        std::string metrics_address="127.0.0.1";
        int port=9091,tick_ms=100,snapshot_ms=60000;
        Options opts;
        for(int i=1;i<argc;i+=2) {
            if(i+1==argc) throw std::invalid_argument("missing option value");
            std::string key=argv[i],value=argv[i+1];
            if(key=="--data") dir=value;
            else if(key=="--listen") listen=value;
            else if(key=="--metrics-port") port=std::stoi(value);
            else if(key=="--metrics-address") metrics_address=value;
            else if(key=="--tick-ms") tick_ms=std::stoi(value);
            else if(key=="--snapshot-ms") snapshot_ms=std::stoi(value);
            else if(key=="--engine" && (value=="lct" || value=="hdt")) opts.lct=value=="lct";
            else throw std::invalid_argument("unknown option or value: "+key);
        }
        if(port<0 || port>65535 || tick_ms<1 || snapshot_ms<1) throw std::invalid_argument("invalid port or interval");
        std::signal(SIGTERM,stop); std::signal(SIGINT,stop);
        Node node(dir,opts); Api api(node); MetricsHttp metrics(node,port,metrics_address);
        grpc::ServerBuilder builder;
        int bound=0;
        builder.AddListeningPort(listen,grpc::InsecureServerCredentials(),&bound); builder.RegisterService(&api);
        auto server=builder.BuildAndStart();
        if(!server || !bound) throw std::runtime_error("gRPC listen failed");
        std::cout<<"grpc_port="<<bound<<" metrics_port="<<metrics.port()<<" recovered_lsn="<<node.position().lsn
                 <<" truncated_bytes="<<node.truncated()<<std::endl;
        auto last=Clock::now();
        while(!stopping) {
            std::this_thread::sleep_for(std::chrono::milliseconds(tick_ms));
            node.tick(wall_ms());
            if(Clock::now()-last>=std::chrono::milliseconds(snapshot_ms)) {node.snapshot();last=Clock::now();}
        }
        server->Shutdown(std::chrono::system_clock::now()+std::chrono::seconds(1));
        node.snapshot();
    } catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
