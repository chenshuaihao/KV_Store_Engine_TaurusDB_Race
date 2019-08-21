#pragma once

#include "kv_string.h"
#include "kv_intf.h"

class SimpleCase {
public:
    SimpleCase() {};

    virtual ~SimpleCase() {};

    bool Init(const KVString & path);

    void Uninit();

    double Run(int thread_num, const char * url, int times, int &err);

protected:
    void * loadSo(const char * path, const char * entry);

    int write(std::shared_ptr<KVIntf> stor, int prefix, int times);

    int read(std::shared_ptr<KVIntf> stor, int prefix, int times);

    int runJobWrite(int no, int prefix, const char * dir, int times);
    int runJobRead(int no, int prefix, const char * dir, int times);

private:
    void *so_ = nullptr;

    std::shared_ptr<KVIntf> (* entry_)() = nullptr;

    const char * entry_name_ = "GetKVIntf";
};
