#include "simple_case.h"

#include <dlfcn.h>
#include <chrono>
#include <thread>
#include <vector>
#include <future>

#include <easylogging++.h>

using namespace std::chrono;

bool SimpleCase::Init(const KVString &path) {
    auto entry = loadSo(path.Buf(), entry_name_);
    if (entry == nullptr) {
        return false;
    }

    entry_ = reinterpret_cast<std::shared_ptr<KVIntf> (*)()>(entry);

    return entry_ != nullptr;
}

void SimpleCase::Uninit() {
    if (so_ != NULL) {
        dlclose(so_);
        so_ = NULL;
    }
}

void * SimpleCase::loadSo(const char * path, const char * entry) {
    Uninit();

    void *dl = dlopen(path, RTLD_NOW | RTLD_DEEPBIND | RTLD_LOCAL);
    if (!dl) {
      LOG(ERROR) << "load " << path << " failed: " << dlerror();
      return nullptr;
    }

    so_ = dl;
    auto f_entry = dlsym(dl, entry);
    if (NULL == f_entry) {
      LOG(ERROR) << "find symbol " << entry << " failed: " << dlerror();
      return nullptr;
    }

    return f_entry;
}

int SimpleCase::runJobWrite(int no, int prefix, const char * dir, int times) {
    //LOG(ERROR) << "Entry function runJobWrite";
    if (entry_ == nullptr) {
        //LOG(ERROR) << "Entry function in user so is nullptr, call Init() first";
        return 1;
    }

    //LOG(INFO) << "before entry_";
    auto stor = entry_();
    //LOG(INFO) << "after entry_";
    if (stor == nullptr) {
        //LOG(ERROR) << "Get kv_store shared_ptr failed, check \"GetKVIntf\" in user SO first";
        return 1;
    }

    //LOG(INFO) << "stor->Init(dir, no);";
    stor->Init(dir, no);
    //LOG(INFO) << "swrite(stor, prefix, times);";
    int write_err = write(stor, prefix, times);
    LOG(INFO) << "stor->Close();";
    stor->Close();

    LOG(INFO) << "stor->Init(dir, no)";
    stor->Init(dir, no);
    LOG(INFO) << "read";
    int read_err = read(stor, prefix, times);
    stor->Close();
    return write_err + read_err;
}

int SimpleCase::runJobRead(int no, int prefix, const char * dir, int times) {
    //LOG(ERROR) << "Entry function runJobRead";
     if (entry_ == nullptr) {
        //LOG(ERROR) << "Entry function in user so is nullptr, call Init() first";
        return 1;
    }
    
    auto stor = entry_();
    if (stor == nullptr) {
        //LOG(ERROR) << "Get kv_store shared_ptr failed, check \"GetKVIntf\" in user SO first";
        return 1;
    }

    stor->Init(dir, no);    
    int read_err = read(stor, prefix, times);
    stor->Close();
    return read_err;
}

double SimpleCase::Run(int thread_num, const char * dir, int times, int &err) {
    if (entry_ == nullptr) {
        LOG(ERROR) << "Entry function in user so is nullptr, call Init() first";
        return -1.0;
    }

    std::vector<std::thread> thds;
    std::vector<std::future<int>> rets;
    std::vector<int> prefixs;
    thds.resize(thread_num);
    rets.resize(thread_num);
    prefixs.resize(thread_num);

    int base = std::time(nullptr);//123;
    for (int i = 0; i < thread_num; i ++) {
        prefixs[i] = i + base;
    }

    auto begin = system_clock::now();
    // for write & read
    for (int i = 0; i < thread_num; i ++) {
        std::packaged_task<int ()> task(std::bind(&SimpleCase::runJobWrite, this, i, prefixs[i], dir, times / thread_num));
        rets[i] = task.get_future();
        thds[i] = std::thread(std::move(task));
    }
    for (int i = 0; i < thread_num; i ++) {
        thds[i].join();
    }

    for (int i = 0; i < thread_num; i ++) {
        err += rets[i].get();
    }

    // for read again
    int prefix;
    for (int i = 0; i < thread_num; i ++) {
        std::packaged_task<int ()> task(std::bind(&SimpleCase::runJobRead, this, i,
                    prefixs[i], dir, times / thread_num));//prefixs[(i + thread_num / 2) % thread_num ]
        rets[i] = task.get_future();
        thds[i] = std::thread(std::move(task));
    }
    for (int i = 0; i < thread_num; i ++) {
        thds[i].join();
    }

    for (int i = 0; i < thread_num; i ++) {
        err += rets[i].get();
    }

    return duration_cast<duration<double>>(system_clock::now() - begin).count();
}


static auto buildKey = [](int64_t idx)->KVString {
    return KVString((const char *)&idx, sizeof(idx));
};

static auto buildVal = [](int idx)->KVString {
    char buf[4096] = {'p'};
    const char * prefix = "hello";
    snprintf(buf, sizeof(buf),"%s_%d", prefix, idx);
    return KVString(buf, 4096);
};

int SimpleCase::write(std::shared_ptr<KVIntf> stor, int prefix, int times) {
    if (stor == nullptr) {
        return 1;
    }
    /////////////////////////////////////////////////////////////////////////////
    int64_t base = prefix;
    base <<= 32;
    for (int i = 0; i < times; i ++) {
        auto key = buildKey(i + base);
        auto val = buildVal(i);
        //if(i % 1000 == 0)
        //    LOG(INFO) << "cur thread id: " << stor->GetThreadId() << " Set key " << i << key.Buf() << " val:" << val.Size() << "-" << val.Buf();
        stor->Set(key, val);
    }

    int err = 0;
    KVString val;
    for (int i = 0; i < times; i ++) {
        auto key = buildKey(i + base);
        stor->Get(key, val);
        if (!(val == buildVal(i)) ) {
            err ++;
            LOG(ERROR) << "get key " << i << "-" << key.Buf() << " error, val: " << val.Size() << "-" << val.Buf();
            //break;
        }
    }
    /////////////////////////////////////////////////////////////////////////////
    return err;
}

int SimpleCase::read(std::shared_ptr<KVIntf> stor, int prefix, int times) {
    int err = 0;
    KVString val;
    int64_t base = prefix;
    base <<= 32;
    printf("inverted order read .....\n");
    for (int i = times-1; i >= 0; i --) {
        auto key = buildKey(i + base);
        stor->Get(key, val);
        if (!(val == buildVal(i)) ) {
            err ++;
            LOG(ERROR) << "get key " << i << " error, val: " << val.Size() << "-" << val.Buf();
            //break;
        }
    }

    return err;
}

