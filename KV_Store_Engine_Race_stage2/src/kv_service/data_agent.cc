#include "data_agent.h"

#include "../include/utils.h"
#include "tcp_client.h"
#include "../third_party/memcopy4k.h"

std::shared_ptr<char> DataAgent::valueCache_[16];

DataAgent::DataAgent(int id)
    : id_(id), valueFilePosition_((size_t)4000000*4096),
    isThreadsCreate(false),valueCachelocal_(new char[READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024], std::default_delete<char[]>()) //error
{
    valueCache_[id] = valueCachelocal_;
    fd_ = new TcpClient();
    // 初始化读取数据连接池
    for (int i = 0; i < READ_THREAD_NUM + 1; ++i) {
        readConnectPool_[i] = new TcpClient();
    }
   // 每个缓冲区可装载的 value 个数
    this->valuePerBuffer = PARTITION_SIZE * 1024 / 4;
    for(int i = 0; i < READ_THREAD_NUM; ++i) {
        partitionInMemSet_[i] = -1;
        loadFinish[i] = false;
        readFinish[i] = true;
        end_[i] = false;
        invertedOrderCnt_[i] = 0;
    }
}

DataAgent::~DataAgent() {
    KV_LOG(INFO) << "~DataAgent, id:" << id_;
    Release();
    for (int i = 0; i < READ_THREAD_NUM + 1; ++i) {
        if(readConnectPool_[i])
            delete readConnectPool_[i];
    }
    if(fd_)
        delete fd_;
}

//连接对应的服务器
int DataAgent::Init(const char * host, const char * port) {
    fd_->Connect(host, port);

    KV_LOG(INFO) << "connect to store node success. fd: " << fd_;
//    this->CreateReadConnectPool(host, port);
    return 0; //
}

void DataAgent::CreateReadConnectPool(const char * host, const char * port){
    for (int i = 0; i < READ_THREAD_NUM + 1; ++i) {
        this->readConnectPool_[i]->Connect(host, port);
    }
}

void DataAgent::CloseReadConnectPool() {
    KV_LOG(INFO) << "CloseReadConnectPool, id:" << id_;
    for (int i = 0; i < READ_THREAD_NUM + 1; ++i) {
        this->readConnectPool_[i]->Close();
    }
}
void DataAgent::Release() {
    KV_LOG(INFO) << "Release, id:" << id_;
    if(isThreadsCreate) {
        isThreadsCreate = false;
        for(int i = 0; i < READ_THREAD_NUM; ++i) {
            loadCondition_[i].notify_one();
        }       
        for(auto &t : read_threads_) {
            t.detach();
            // if(t.joinable())
            //     t.join();
        }                    
    }
    this->CloseSetPort();
    // 关闭读取连接池
    //this->CloseReadConnectPool();
}

void DataAgent::Clear() {
}

void DataAgent::SetValueFilePosition(size_t pos) {
    this->valueFilePosition_ = (size_t)pos * 4096;
}

int DataAgent::GetValueFilePosition() {
    return this->valueFilePosition_;
}

int DataAgent::CloseSetPort() {
    //printf("CloseSetPort, id:%d\n", id_);
    this->fd_->Close();
    return 0;
}

uint64_t DataAgent::Append(uint32_t type, KVString & key, KVString & val) {
    /*
        计算节点：
        set()
            直接调用set
    */
    //printf("DataAgent::Append, id: %d\n", id_);
    int key_size = 8; //key.Size();
    int val_size = 4096; //val.Size();
    // 构造 send 包，按  HEADER | key_size | val_size | key | val 的格式放到 send_buf 里面 ，
    // type 表示 传输方式， 包括
    int send_len = key_size + val_size + sizeof(int) * 2;
    char * send_buf = new char [PACKET_HEADER_SIZE + send_len];
    auto & send_pkt = *(Packet *)send_buf;
    memcpy(send_pkt.buf, (char *)&key_size, sizeof(int));
    memcpy(send_pkt.buf + sizeof(int), (char *)&val_size, sizeof(int));
    memcpy(send_pkt.buf + sizeof(int) * 2, key.Buf(), key_size);
    memcpy(send_pkt.buf + sizeof(int) * 2 + key_size, val.Buf(), val_size);
    send_pkt.len    = send_len;
    send_pkt.sn     = 0;
    send_pkt.type   = type; // 这里使用的 type 是 const int KV_OP_DATA_APPEND = 3;
    send_pkt.crc    = send_pkt.Sum(); // 一共发送多少个字节

    // 结束构造 send 包
    // send 出去 ， 获取 ret 信息 // append 成功后返回是什么
    //KV_LOG(INFO) << "send key  " << *((uint64_t*)key.Buf());
    char * ret_buf = this->fd_->Send(send_buf, send_len + PACKET_HEADER_SIZE);
    delete [] send_buf;
    // 删除 send_buf

    if (ret_buf == nullptr) {
        KV_LOG(ERROR) << "Append return null";
        return -1;
    }
    delete [] ret_buf;
    return 0;
}

void DataAgent::CreateReadLoadThread(int state, int fileId) {
    //创建DIO线程，预读，以及在后台加载文件到内存
    //printf("CreateReadLoadThread\n");
    KV_LOG(INFO) << "CreateReadLoadThread, id:" << id_;
    isThreadsCreate = true;
    if(state == 1) {
        for(int i = 0; i < READ_THREAD_NUM; ++i) { 
            //partitionInMemSet_[i] = i;
            read_threads_[i] = std::thread(&DataAgent::LoadDataToMem, this, i, fileId);
        }
    } else {
        for(int i = 0; i < READ_THREAD_NUM; ++i) { 
            read_threads_[i] = std::thread(&DataAgent::LoadDataToMemRand, this, i, fileId);
        }
    }    
}
//生产者，partition表示第几分区，0,1,2
void DataAgent::LoadDataToMem(int partition, int fileId) {
    //printf("loadDataToMem--------------------\n");
    //KV_LOG(INFO) << "loadDataToMem...................";
    char* cache = valueCache_[fileId].get() + partition * PARTITION_SIZE * 1024 * 1024;
    size_t len = PARTITION_SIZE * 1024 * 1024; //分区大小
    size_t offset = PARTITION_SIZE * 1024 * 1024 * partition; //对应的文件偏移位置
    int curPartition = partition;
    bool loop = true;
    int cnt = partition;
    partitionInMemSet_[partition] = partition;
    KVString val; 
    KVString key; 
    while(loop && isThreadsCreate) {
        //判断看看本区域是否可写，是的话，加锁加载硬盘数据                 
        std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况        
        while(!readFinish[partition])
        {
            loadCondition_[partition].wait(lock);
            if(isThreadsCreate == false)
                return;
        }
        //printf("load partition %d\n", partition);
        partitionInMemSet_[partition] = curPartition;//当前加载的文件分区，这一句放哪里好呢？
        loadFinish[partition] = false;
        for(int i = 0; i * 4096 < len; ++i) {      
            if(unlikely(offset >= valueFilePosition_)) {
                //IO线程读取文件偏移超出实际长度，结束线程
                //printf("------------ load thread:%d, IO线程读取文件偏移超出实际长度，结束线程!!!-----------\n", partition);
                loop = false;
                printf("set partition %d offset %ld to true\n", partition, offset);
		        end_[partition] = true;
                break;
            }
            //printf("loading partition... fileId, offset %d %d\n", fileId, offset);
            //写死了为顺序读
//            int val_num = this->Get(KV_OP_DATA_GET, offset/4096, fileId, key, val, 1);
            int rc = this->Get(KV_OP_DATA_GET, offset/4096, readConnectPool_[partition + 1], key, val, 1);
            if(rc == -1)
                return;
            memcpy_4k(cache, val.Buf());
            //printf("loading partition... value num %d offset %d \n", val_num, offset);
            cache += 4096;
            offset += 4096;  
        }
        cnt += READ_THREAD_NUM;
        cache = valueCache_[fileId].get() + partition * PARTITION_SIZE * 1024 * 1024; //本区数据已经全部加载，重新覆盖缓存区

        offset += len * (READ_THREAD_NUM - 1);  //文件偏移到下一个分区，+2

        curPartition += READ_THREAD_NUM;//下一个待读文件分区

        loadFinish[partition] = true;
        readFinish[partition] = false;
        //printf("load partition %d finish\n", partition);
//    }
        readCondition_[partition].notify_one();//唤醒等待的测试线程
    }        
}

void DataAgent::LoadDataToMemRand(int partition, int fileId) {
    //printf("entering DataAgent::LoadDataMenRand\n");
    char* cache = valueCache_[fileId].get() + partition * PARTITION_SIZE * 1024 * 1024;
    long long int len = PARTITION_SIZE * 1024 * 1024; //分区大小
    long long int offset = (valueFilePosition_ / (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024))\
     * (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024) + PARTITION_SIZE * 1024 * 1024 * partition; //对应的文件偏移位置
    int curPartition = (valueFilePosition_ / (READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024)) * READ_THREAD_NUM + partition;
    bool loop = true;
    int cnt = partition;

    //如果当前线程初始偏移超出文件长度的话，执行校正，往前偏移n个分区单位，n为线程数，例如：向前偏移3*64MB
    if(offset >= valueFilePosition_) {
        offset -= READ_THREAD_NUM * PARTITION_SIZE * 1024 * 1024;
        curPartition -= READ_THREAD_NUM;
    }
    partitionInMemSet_[partition] = partition;

    KVString key;
    KVString val;
    while(loop && isThreadsCreate) {
    {
        //判断看看本区域是否可写，是的话，加锁加载硬盘数据                 
        std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况        
        while(!readFinish[partition])
        {
            loadCondition_[partition].wait(lock);
            if(isThreadsCreate == false)
                return;
        }
        //printf("load partition %d\n", partition);
        partitionInMemSet_[partition] = curPartition;//当前加载的文件分区，这一句放哪里好呢？        
        loadFinish[partition] = false;
        size_t ret = 0;
        for(int i = 0; i * 4096 < len; ++i) {      
            if(unlikely(offset < 0)) {
                //IO线程读取文件偏移超出实际长度，结束线程
                //printf("------------ load thread:%d, IO线程读取文件偏移超出实际长度，结束线程!!!-----------\n", partition);
                //printf("set partition %d offset %ld to true\n", partition, offset);
                loop = false;
                end_[partition] = true;
                break;
            }
            //写死了逆序读
            int rc = this->Get(KV_OP_DATA_GET, offset/4096, readConnectPool_[partition + 1], key, val, 2);
            if(rc == -1) 
                return;
            memcpy_4k(cache, val.Buf());
            cache += 4096;
            offset += 4096;  
        }

        cnt += READ_THREAD_NUM;

        cache = valueCache_[fileId].get() + partition * PARTITION_SIZE * 1024 * 1024; //本区数据已经全部加载，重新覆盖缓存区

        offset -= len * (READ_THREAD_NUM + 1);  //文件偏移到下一个分区，+2
        curPartition -= READ_THREAD_NUM;//下一个待读文件分区
        loadFinish[partition] = true;
        readFinish[partition] = false;
        invertedOrderCnt_[partition] = 0;
        //printf("load partition %d finish\n", partition);
    }
        readCondition_[partition].notify_one();//唤醒等待的测试线程
    }        
}

//判断分区在不在内存
bool DataAgent::isPartitionInMem(int partitionPosition) {
    for(int i = 0; i < READ_THREAD_NUM; ++i) {
        if(partitionPosition == partitionInMemSet_[i])
            return true;
    }
    return false;
}

int DataAgent::GetValueFromCache(uint64_t keyPosition, int fileId, KVString & key, KVString &val, int state) {
    //其实跟ValueFile::GetValue逻辑一样的，哈哈直接copy改一改吧
    //valueCache_[],就是我们的计算节点的缓存，应该要改成全局，因为是根据文件ID：fileId来找
    static int mem_readcnt = 0;
    static int disk_readcnt = 0;
    if(state == 1) {
        if(unlikely(!isThreadsCreate)) {
            CreateReadLoadThread(state, fileId);
        }
        //计算key所在的文件分区位置
        int partitionPosition = keyPosition / this->valuePerBuffer;

        //计算在第几个缓存分区
        int partition = partitionPosition % READ_THREAD_NUM;

        //判断key所在的分区是否被加载到内存中
        if(isPartitionInMem(partitionPosition) && end_[partition] == false) {
            //KV_LOG(INFO) << "state1 cache read id:" << id_ << ",partition:" << partition << ",times:" << ++mem_readcnt;
            std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况
            while(!loadFinish[partition])//loadFinish[partition] == false
            {
                //printf("GetValueFromCache read from cache waiting\n");
                readCondition_[partition].wait(lock);
            }
            readFinish[partition] = false;
            if(val.Size() == 4096) {
                KVString tmp(std::move(val));
                const char * str = valueCache_[fileId].get() + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096;
                memcpy_4k(const_cast<char*> (tmp.Buf()), str);
                val = std::move(tmp);          
            } else {
                //printf("********KVString tmp KVString tmp KVString tmp KVString tmp KVString tmp********\n");
                KVString tmp(valueCache_[fileId].get() + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096, 4096);
                val = std::move(tmp);  
            }

            if((keyPosition + 1) % this->valuePerBuffer == 0 || ((keyPosition + 1) * 4096) >= valueFilePosition_) {
                //已经get读完本分区数据了
                partitionInMemSet_[partition] = -1;
                readFinish[partition] = true;
                loadFinish[partition] == false;
                loadCondition_[partition].notify_one();//唤醒等待的分区IO线程            
            }
            return 4096;
        } else {
            //KV_LOG(INFO) << "state1 disk read id:" << id_ << ",partition:" << partition << ",times:" << ++disk_readcnt;
            KVString tmp;
            int ret = this->Get(KV_OP_DATA_GET, keyPosition, readConnectPool_[0], key, tmp, state);
            val = std::move(tmp);
            return ret;         
        }
    } else if(state == 2) {
        if(unlikely(!isThreadsCreate)) {
            CreateReadLoadThread(state, fileId);
        }
        int lastpartion = (valueFilePosition_ / 4096 - 1) / this->valuePerBuffer; // (1025-1)/1024, (1024-1)/1024
        int last_partion_num = (valueFilePosition_ / 4096) % this->valuePerBuffer;
        //计算key所在的文件分区位置
        int partitionPosition = keyPosition / this->valuePerBuffer;
        //计算在第几个缓存分区
        int partition = partitionPosition % READ_THREAD_NUM;
        if(isPartitionInMem(partitionPosition) && end_[partition] == false) {
            //KV_LOG(INFO) << "state2 cache read id:" << id_ << ",partition:" << partition << ",times:" << ++mem_readcnt;
            std::unique_lock<std::mutex> lock(mutex_[partition]);//unique_lock支持解锁又上锁情况
            while(!loadFinish[partition])//loadFinish[partition] == false
            {
                readCondition_[partition].wait(lock);
            }
            readFinish[partition] = false;
            KVString tmp(valueCache_[fileId].get() + partition * PARTITION_SIZE * 1024 * 1024 + (keyPosition % this->valuePerBuffer) * 4096, 4096);
            val = std::move(tmp);
            invertedOrderCnt_[partition] ++;
            if( ( invertedOrderCnt_[partition] % (this->valuePerBuffer * 1) == 0 && partitionPosition != lastpartion )
                || ( invertedOrderCnt_[partition] == last_partion_num && partitionPosition == lastpartion ) ) {
                //已经get读完本分区数据了
                partitionInMemSet_[partition] = -1;
                readFinish[partition] = true;
                loadFinish[partition] == false;
                loadCondition_[partition].notify_one();//唤醒等待的分区IO线程
            }
            return 4096;
        } else {
            //KV_LOG(INFO) << "state2 disk read id:" << id_ << ",partition:" << partition << ",times:" << ++disk_readcnt;
            KVString tmp;
            int ret = this->Get(KV_OP_DATA_GET, keyPosition, readConnectPool_[0], key, tmp, state);
            val = std::move(tmp);  
            return ret;     
        }
   } else {
        //KV_LOG(INFO) << "state0 disk read id:" << id_ << ",times:" << ++disk_readcnt;
        KVString tmp;
        int ret = this->Get(KV_OP_DATA_GET, keyPosition, this->fd_, key, tmp, state);
        val = std::move(tmp);
        return ret;   
   }
}

int DataAgent::Get(uint32_t type, uint64_t pos, TcpClient * tcpClient, KVString &key, KVString &val, int state) {
    //printf("DataAgent::Get\n");
    int pos_size = sizeof(uint64_t);
    int state_size = sizeof(int);
    // 构建 send_buf 按 PACKET_HEADER_SIZE | pos | state 来组织发送
    // thinking : 为最大限度的打满网络 io 带宽， 应该在知道 state 之后， 开启后台线程接收 key , value
    int send_len = pos_size + state_size;
    char * send_buf = new char[PACKET_HEADER_SIZE + send_len];
    auto & send_pkt = *(Packet *)send_buf;
    memcpy(send_pkt.buf, (char *)&pos, pos_size);
    memcpy(send_pkt.buf + pos_size, (char *)&state, state_size);
    send_pkt.len    = send_len;
    send_pkt.sn     = 0;
    send_pkt.type   = type; // 设置 type ，此处 Get 使用的 type 为 const int KV_OP_DATA_GET = 4;
    send_pkt.crc = send_pkt.Sum();
    // 把 pos 跟 state 发送出去，等待回应
    //char * ret_buf = tcpClient->Send(send_buf, send_len + PACKET_HEADER_SIZE);
    int rc = tcpClient->SendOnly(send_buf, send_len + PACKET_HEADER_SIZE);
    delete [] send_buf;
    if(rc == -1)
        return -1;

    char * ret_buf = new char[4128];
    rc = tcpClient->Recv(ret_buf, 4128);
    if(rc == -1) {
        KV_LOG(ERROR) << "Get return null";
        delete [] ret_buf;
        return -1;
    }


    // 解析取回来的包， key_size | val_size | key | value
    auto & recv_pkt = *(Packet *)ret_buf;
    int key_size = 8; //*(int *)recv_pkt.buf;
    int val_size = 4096; //*(int *)(recv_pkt.buf + sizeof(int));
    char * v = new char [val_size];
    //memcpy(k, recv_pkt.buf + sizeof(int) * 2, key_size); // key size 没用上， store 端没有传过来
    memcpy(v, recv_pkt.buf + sizeof(int) * 2 + key_size, val_size);    
    delete [] ret_buf;

    //key.Reset(k, key_size); // 传 key 没用
    val.Reset(v, val_size);
    KV_LOG(INFO) << "p:" << pos << " v:" << *((uint64_t*)val.Buf());
    //存储节点，未启动新数据加载，bug fix
    return val_size;
}
    /*
        计算节点：
        get()
            函数 KeyFile::GetKeyPosition(KVString & key, int & state)迁移到计算节点运行。
            当计算节点发生get()调用时，
            调用GetKeyPosition，首先判断当前的状态state，
            如果state == 默认的随机读
            {
                1、查找索引，找到pos
                //好像跟之前逻辑差不多
                2、发送req包给存储节点，state==0，存储节点执行普通的随机读，然后返回val

                req包的结构信息：pos，state
            }
            如果state == 顺序读
            {
                //这里可能要加个计数器或者pos分区末尾判断，当读完分区数据后，网络IO线程需要发req包给
                存储节点的网络IO线程启动新一轮分区数据传输

                1、通过计数器获得pos
                2、如果pos所在分区不在缓冲区
                        发送req包给存储节点，启动分区顺序加载以及多线程网络传输；
                        计算节点：启动后台网络IO线程，打满带宽接收reply包，value
                   否则，
                        直接根据pos

                    reply包的结构信息：value、value所在的pos或者分区号，以便识别出写入哪个缓存分区
            }
            如果state == 倒序读
            {
                //这里可能要加个计数器，统计当前分区读了多少个数据，当读完分区数据后，网络IO线程需要发req包给
                存储节点的网络IO线程启动新一轮分区数据传输

                1、确定当前key的当前倒序分区编号（或者不更新,保持原来的分区），通过什么方式或数据结构快速定位key的分区编号呢（思考）？
                   （通过计数器得到倒序分区编号）
                2、根据当前倒序分区编号，查找对应的分区的索引，得到pos
                3、如果pos所在分区不在缓冲区
                        发送req包给存储节点，启动分区倒序加载以及多线程网络传输；
                        计算节点：启动后台网络IO线程，打满带宽接收reply包，value
                   否则，
                        根据pos，去缓存分区那里找到value
                
                reply包的结构信息：value、value所在的pos或者分区号，以便识别出写入哪个缓存分区
                
            }
    */

int DataAgent::GetKeys(uint32_t type, uint64_t pos, char* key_buf, int state, int  cur_key_num) { // cur_key_num 应初始化为 0 ,cur_key_num 为 0 时，表示缓冲区里没有 key
    KV_LOG(INFO) << "DataAgent::GetKeys,id:" << id_;
    int begin_key_num_size = sizeof(int);
    int req_key_nums_size = sizeof(int);
    // 发送大小, 每次发送的格式为： HEADER | begin_key_num | req_key_nums
    int send_len = begin_key_num_size + req_key_nums_size;
    char * send_buf = new char[PACKET_HEADER_SIZE + send_len];
    auto & send_pkt = *(Packet *)send_buf;
    // 第一次，获取 store端的 key_num， 将 发送值全设 -1
    int begin_key_num = -1; //
    int req_key_nums = -1; //
    memcpy(send_pkt.buf, (char *)&begin_key_num, begin_key_num_size);
    memcpy(send_pkt.buf + begin_key_num_size, (char *)&req_key_nums, req_key_nums_size);
    send_pkt.len    = send_len;
    send_pkt.sn     = 0;
    send_pkt.type   = type;
    send_pkt.crc = send_pkt.Sum();
    
    // 第一次发送 SendKeys  , 第一次发送的格式为： HEADER | -1 | -1
    char * ret_buf = this->fd_->Send(send_buf, PACKET_HEADER_SIZE + send_len);
    memset(send_buf, 0, PACKET_HEADER_SIZE + send_len);
    if (ret_buf == nullptr) {
        KV_LOG(ERROR) << "Get return null";
        return -1;
    }

    // 第一次接收到的消息 : PACKET_HEADER | key_num
    auto & recv_pkt = *(Packet *)ret_buf;
    int key_num = *(int *)recv_pkt.buf;
    KV_LOG(INFO) << "id:" << id_ << " 第一次接收到消息: PACKET_HEADER | key_num, key_num:" << key_num;
    delete[] ret_buf;

    // 计算
    int key_num_per_req = 512; //每次请求的key数量
    req_key_nums = key_num - cur_key_num; // 需要获取的 key 总数
    int req_times = req_key_nums / key_num_per_req; //计算传输次数
    req_times = req_key_nums % key_num_per_req == 0 ? req_times : req_times + 1;
    begin_key_num = cur_key_num ; // 获取的key的下标从 0 开始
    int req_num = key_num_per_req; // 每次请求的 key 个数
    int recv_nums = 0;  //收到的个数，用于计算 要请求的个数
    char* ret_buf_test = new char[PACKET_HEADER_SIZE + req_num * 8];
    for (int i = 0; i < req_times; ++i){
        if (i == (req_times - 1)) {
            req_num = req_key_nums - recv_nums; // 最后一次不一定满 100000个
        }
        try
        {
            send_pkt = *(Packet *)send_buf;
            memcpy(send_pkt.buf, (char *)&begin_key_num, begin_key_num_size);
            memcpy(send_pkt.buf + begin_key_num_size, (char *)&req_num, req_key_nums_size);
            send_pkt.len    = send_len;
            send_pkt.sn     = 0;
            send_pkt.type   = type;
            send_pkt.crc = send_pkt.Sum();
            // 接收， 格式为 key | key | ...... | key
            int rc = this->fd_->SendOnly(send_buf, send_len + PACKET_HEADER_SIZE);            
            if(rc == -1) {
                delete [] send_buf;
                delete [] ret_buf_test;
                return -1;
            }                

            rc = this->fd_->Recv(ret_buf_test, PACKET_HEADER_SIZE + req_num * 8);
            if(rc == -1) {
                KV_LOG(ERROR) << "Get return null";
                delete [] send_buf;
                delete [] ret_buf_test;
                return -1;
            }
            memcpy(key_buf + begin_key_num * 8, ret_buf_test + PACKET_HEADER_SIZE, req_num * 8);
            begin_key_num += req_num;
            recv_nums += req_num;          
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << " for loop GetKeys\n";
            return -1;
        }
    }
    if(ret_buf_test)
        delete [] ret_buf_test;
    if(send_buf)
        delete []send_buf;
    cur_key_num = key_num;
    return key_num;
}
