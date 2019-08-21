#include <iostream>
#include <string.h>

#include "rpc_process.h"
#include "tcp_server.h"
#include "utils.h"

INITIALIZE_EASYLOGGINGPP;

const char *exeName(const char *name) {
    int pos = 0;
    if (name == nullptr || (pos = strlen(name)) < 1) {
        return nullptr;
    }

    for (; pos > 0; pos--) {
        if (name[pos - 1] == '/') {
            break;
        }
    }

    return name + pos;
}

void help(const char *name) {
    std::cout << "usage: " << name << " host file_dir [clear]" << std::endl;
    std::cout << "   eg: " << name << " tcp://0.0.0.0 ./data" << std::endl;
    std::cout << "       " << name << " tcp://0.0.0.0 ./data clear" << std::endl;
    exit(-1);
}

void initLog(const char * name) {
    el::Configurations conf;
    conf.setToDefault();
    char log_path[256] = "logs/";
    strcat(log_path, name);
    strcat(log_path, ".log");
    conf.set(el::Level::Global, el::ConfigurationType::Filename, log_path);

    el::Loggers::reconfigureAllLoggers(conf);
}

void pause() {
    system("stty raw -echo");
    std::cout << "press any key to exit ...";
    getchar();
    system("stty -raw echo");
    std::cout << std::endl;
}

int main(int argc, char * argv[]) {
    START_EASYLOGGINGPP(argc, argv);
    const char * name = exeName(argv[0]);
    initLog(name);

    if (argc != 3 && argc != 4) {
        LOG(ERROR) << "param should be 3 or 4";
        help(name);
        return -1;
    }

    const char * host = argv[1];
    const char * dir  = argv[2];
    bool clear  = false;
    if (argc == 4) {
        if (strcmp("clear", argv[3]) != 0) {
            LOG(ERROR) << "param [4] should be \"clear\" if you want to clear local data";
            help(name);
            return -1;
        } else {
            clear = true;
        }
    }
    KV_LOG(INFO) << "[" << name << "] local store demo ...";
    KV_LOG(INFO) << "  >> bind  host : " << host;
    KV_LOG(INFO) << "  >> data  dir  : " << dir;
    KV_LOG(INFO) << "  >> clear dir  : " << clear;


    auto rpc_process= std::make_shared<RpcProcess>(dir);
    rpc_process->Run(dir, clear);

    char url[256];
    strcpy(url, host);
    strcat(url, ":9527");

    int ret = TcpServer::Run((const char *)url, rpc_process->GetPtr());

    if (ret != -1) {
        pause();
        TcpServer::StopAll();
    }

    return ret;
}
