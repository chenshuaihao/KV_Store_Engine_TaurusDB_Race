
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include <iostream>

#include "easylogging++.h"
#include "simple_case.h"

using namespace std;

INITIALIZE_EASYLOGGINGPP

static const int kTimes = 1000;

const char *exeName(const char *name)
{
    int pos = 0;
    if (name == nullptr || (pos = strlen(name)) < 1)
    {
        return nullptr;
    }

    for (; pos > 0; pos--)
    {
        if (name[pos - 1] == '/')
        {
            break;
        }
    }

    return name + pos;
}

void help(const char *name)
{
    cout << "usage: " << name << " kv_num(million) threads so_path store_host" << endl;
    cout << "   eg: " << name << " 10 2 ./libkv_service.so tcp://127.0.0.1" << endl;
    exit(-1);
}

void initLog(const char *name)
{
    el::Configurations conf;
    conf.setToDefault();
    char log_path[NAME_MAX] = "logs/";
    strcat(log_path, name);
    strcat(log_path, ".log");
    conf.set(el::Level::Global, el::ConfigurationType::Filename, log_path);

    el::Loggers::reconfigureAllLoggers(conf);
}

int main(int argc, char *argv[])
{
    START_EASYLOGGINGPP(argc, argv);
    const char * name = exeName(argv[0]);
    initLog(name);

    if (argc != 5)
    {
        help(name);
        return -1;
    }

    int kv_num = atoi(argv[1]);
    int thread_num = atoi(argv[2]);
    const char * path = argv[3];
    const char * host = argv[4];

    if (kv_num < 0 || kv_num > 10000 || thread_num < 1 || thread_num > 16 ||
            path == nullptr || strlen(path) < 4 || host == nullptr || strlen(host) < 1)
    {
        help(name);
        return -1;
    }

    LOG(INFO) << "Begin test, it is just a demo!!!";
    LOG(INFO) << "  >> KV number  : " << kv_num << " K";
    LOG(INFO) << "  >> threads    : " << thread_num << " thread";
    LOG(INFO) << "  >> KVService  : " << path;
    LOG(INFO) << "  >> Store host : " << host;

    SimpleCase tester;
    tester.Init(path);

    int error = 0;
    double time = tester.Run(thread_num, host, kv_num * kTimes, error);
    LOG(INFO) << "time: " << time << " seconds, error number: " << error;

    tester.Uninit();
    return time * 100;
}
