/*

*/
#include <unistd.h>
#include <fcntl.h>  

#include <sstream>
#include <string>
#include <thread> 

#include "KvStoreEngine.h"
#include "ValueFile.h"
#include "KeyFile.h"
#include "params.h"
#include "../include/utils.h"

char fixbuf[8192];

int fixlen;

uint32_t fixType;


/*
// KeyFile* keyFiles[FILE_NUM];
// ValueFile* valueFiles[FILE_NUM];
// BufferFile* bufferFiles[FILE_NUM];
*/
KvStoreEngine::KvStoreEngine(/* args */) {
    threadId_ = -1;
    stagetype_ = ORDER_READ;
    keyFile_ = nullptr;
    valueFile_ = nullptr;
    count_ = 0;
    fixlen = 0;
    memset(fixbuf, 0, 8192);
}

KvStoreEngine::~KvStoreEngine() {
    //delete []valueCache_;
}


bool KvStoreEngine::Init(const char * dir, int id) {
    
    string path(dir);
    std::ostringstream ss;
    ss << path << "/value-" << id;
    string filePath = ss.str();

    //判断线程标识id对应的文件是否已经存在
    if(access(filePath.data(), 0) != -1) {
        //如果文件已经存在，说明现在是随机读取阶段
        std::string s(dir);
        dir_ = s;
        threadId_ = id;
        stagetype_ = RANDOM_READ;

        keyFile_ = new KeyFile(dir, id, true);
        valueFile_ = new ValueFile(dir, id, true);
        //读取keyFile文件，构件索引

        //valueFile_->CreateReadLoadThread();
    }
    //第一次init
    else {
        //如果文件不存在，说明该线程第一次调用Init，创建keyFiles、valueFiles、bufferFiles（缓冲区丢失恢复用）
        std::string s(dir);
        dir_ = s;
        threadId_ = id;
        stagetype_ = ORDER_READ;
        keyFile_ = new KeyFile(dir, id, false);
        valueFile_ = new ValueFile(dir, id, false);
        //bufferFile = new bufferFile(dir, id);
    }
}

void KvStoreEngine::Close() {
    delete valueFile_;
    delete keyFile_;
}

int64_t KvStoreEngine::Set(KVString &key, KVString & val) {
    
    valueFile_->SetValue(val);
    size_t pos = keyFile_->SetKey(key);       
    return pos; 
}

int64_t KvStoreEngine::Get(KVString &key, KVString & val) {
    int state = 0;
    int64_t keyPosition = keyFile_->GetKeyPosition(key, state);
    if(keyPosition != -1)
        valueFile_->GetValue(keyPosition, val, state);
    else
        return -1;
    /*
    // if(stage == random) {
    //     //随机读阶段
    //     //1.根据key查找索引，看看文件偏移位置offset，看是否在内存中命中
    //     //2.如果命中读内存位置，拷贝到val，淘汰更新
    //     //3.否，pread对应位置
    // }
    // else {
    //     //顺序读阶段
    //     //这里顺序读，好像也不用找索引也行，搞个计数器？
    //     //1.根据key查找索引，看看位置offset，按道理应该已经提前加载到内存了
    //     //2.读内存位置，拷贝到val
    //     //3.提前加载到内存
    // }
     */
}

//新接口，针对计算存储分离架构，传入pos找val
int64_t KvStoreEngine::Get(int64_t keyPosition, KVString & val, int state) {
    if(keyPosition != -1)
        return valueFile_->GetValue(keyPosition, val, state);
    else
        return -1;
}

size_t KvStoreEngine::Process(char * buf, int len) {
    //printf("KvStoreEngine::Process\n");
    /*
        req数据包分类处理：
        set：set key 和 value
        get：get value
        get：get key
     */
    /*
        存储节点：
            创建16个线程，每个线程对应一个服务器实例，监听一个端口，端口从9527开始
        同时创建16个数据库实例，将数据库实例指针传入服务器。
            当服务器收到数据时，判断数据包类型：
            如果是set操作
                则直接执行 KvStoreEngine::Set(KVString &key, KVString & val);
            如果是get操作
                进一步进行判断数据包中的 state，state是计算节点根据key推测出的随机读、顺序读、热点随机读状态
                    如果state == 随机读
                    {
                        执行普通的pread
                    }                    
                    如果state == 顺序读
                    {
                        1、启动18个分区存储IO线程，打满存储IO带宽，顺序加载value到内存缓冲区，注意：存储节点2GB内存
                        缓存分区大小：1MB、2MB、5MB、10MB四种之一 （目前只完成第一部分？ 第二点 多线程发送value尚未看到，，）
                        2、启动18个分区网络IO线程，打满网络IO带宽，将value发送出去
                    }
                    如果state == 热点随机读
                    {
                        1、启动18个分区存储IO线程，打满存储IO带宽，倒序加载value到内存缓冲区，注意：存储节点2GB内存
                        缓存分区大小：1MB、2MB、5MB、10MB四种之一
                        2、启动18个分区网络IO线程，打满网络IO带宽，将value发送出去
                    }
    */
    auto & req = *(Packet *)buf;

//    KV_LOG(INFO) << "packet len:" << len;

    size_t ret_len = 0;
    switch(req.type) { //判断 请求类型
        case KV_OP_META_APPEND:
            //ProcessAppend(meta_, packet.buf, packet.cb);
            break;

        case KV_OP_META_GET: // 此标记用于传输 KeyFile
            //get key
            ret_len = ProcessGetKey(buf);
            break;

        case KV_OP_DATA_APPEND: // 写入 key val 键值对
            //set key value
            if(len ==4128)
                ret_len = ProcessAppend(buf);
            else
            {

                memcpy(fixbuf + fixlen, buf, len); //tcp packet bug fix
                fixlen += len;
//                memcpy(fixbuf, buf, len); //tcp packet bug fix
//                fixlen = len;
            }            
            break;

        case KV_OP_DATA_GET: // 获取 key val 键值对
            //get value
            ret_len = ProcessGet(buf);
            break;

        case KV_OP_CLEAR:
            //TODO: clear local data
            break;

        default:
            LOG(ERROR) << "unknown req type: " << req.type;
            memcpy(fixbuf + fixlen, buf, len);
            fixlen += len;
            if(fixlen == 4128) {
                memcpy(buf, fixbuf, 4128);
                ret_len = ProcessAppend(buf);
                memset(fixbuf, 0, 8192);
                fixlen = 0;
            }
            break;
        }
    return ret_len;
}

size_t KvStoreEngine::ProcessUnknown(char * buf , int len){
    auto & req = *(Packet *)buf;
    LOG(INFO) << "processing unknown req type : " << req.type;
    memcpy(fixbuf + fixlen, buf, len);
    fixlen += len;
    size_t ret_len = 0;
    if (fixlen == 4128) {
        memcpy(buf, fixbuf, 4128);
        ret_len = ProcessAppend(buf);
        memset(fixbuf, 0, 8192);
        fixlen = 0;
    }
    return ret_len;
}

//set
size_t KvStoreEngine::ProcessAppend(char * buf) {

    //printf("KvStoreEngine::ProcessAppend\n");
    auto & req = *(Packet *) buf;
    int key_size = 8; //*(int *)req.buf;
    int val_size = 4096; //*(int *)(req.buf + sizeof(int));
    if (key_size + val_size + sizeof(int) * 2 > req.len) {
        KV_LOG(ERROR) << "kv size error: " << key_size << "+" << val_size << "-" << req.len;
        return 0;
    }
    KVString key;
    KVString val;
    char * key_buf = new char [key_size];
    char * val_buf = new char [val_size];
    memcpy(key_buf, req.buf + sizeof(int) * 2, key_size);
    memcpy(val_buf, req.buf + sizeof(int) * 2 + key_size, val_size);
    key.Reset(key_buf, key_size);
    val.Reset(val_buf, val_size);
    //auto pos = target.Append(key, val);
    int64_t pos = Set(key, val); // 本地操作，将 key 和 val 存到硬盘中

    KV_LOG(INFO) << "p" << pos << " k:" << *((uint64_t*)key.Buf()) << " v:" << *((uint64_t*)val.Buf());


    int    ret_len = PACKET_HEADER_SIZE + sizeof(pos);

    memset(buf, 0, 8192 * sizeof(char));
    auto & reply = *(Packet *)buf;
    memcpy(reply.buf, (char *)&pos, sizeof(pos));
    reply.len   = sizeof(pos);
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();
    //printf("fin ProcessAppend, ret_let:%d\n", ret_len);
    return ret_len;
}

//get value base pos
size_t KvStoreEngine::ProcessGet(char * buf) {
    //printf("KvStoreEngine::ProcessGet\n");
    //static int i = 0;
    auto & req = *(Packet *)buf;
    if (req.len < sizeof(int64_t)) {
        KV_LOG(ERROR) << "index size error: " << req.len;
        return 0;
    }

    // Get 的时候， 获取 pos 位置值 与 state 读取状态
    auto pos = *(uint64_t *)req.buf;
    auto state = *(int *)(req.buf + sizeof(uint64_t));
    //printf("ProcessGet state: %d\n", state);

    KVString key;
    KVString val;
    int ret = Get(pos, val, state);
    keyFile_->GetKey(pos, key);

    KV_LOG(INFO) << "p" << pos << " k:" << *((uint64_t*)key.Buf()) << " v:" << *((uint64_t*)val.Buf());


    // 打包取回来的 key 与 val ，按 PACKET_HEADER_SIZE | key_size | val_size | key | val
    int key_size = 8; //key.Size();
    int val_size = 4096; //val.Size();
    int    ret_len = PACKET_HEADER_SIZE + sizeof(int) * 2  + key_size + val_size;

    memset(buf, 0, 8192 * sizeof(char));
    auto & reply = *(Packet *)buf;
    memcpy(reply.buf, (char *)&key_size, sizeof(int));
    memcpy(reply.buf + sizeof(int), (char *)&val_size, sizeof(int));
    memcpy(reply.buf + sizeof(int) * 2 + key_size, val.Buf(), val_size);
    reply.len   = sizeof(int) * 2 + key_size + val_size;
    reply.sn    = req.sn;
    reply.type  = req.type;
    reply.crc   = reply.Sum();
    return ret_len;
}

//发送key给计算节点，可以考虑循环发送 , 受传输大小限制, 需要循环传
size_t KvStoreEngine::ProcessGetKey(char * buf) {
    //printf("KvStoreEngine::ProcessGetKey \n");
    auto & req = *(Packet *)buf;
    if (req.len < sizeof(int64_t) ) {
        KV_LOG(ERROR) << "index size error: " << req.len;
        return 0;
    }

    auto begin_key_num = *(int *)req.buf;
    auto req_key_nums = *(int *)(req.buf + sizeof(int));

    // 第一次接收的消息为 ： HEADER | -1 | -1
    if (unlikely(begin_key_num == -1 && req_key_nums == -1)) {
        int key_num = keyFile_->GetKeyNum();
        //printf("id:%d, keyFile_->GetKeyNum(), key_num: %d \n", threadId_, key_num);
        //KV_LOG(INFO) << "id:" << threadId_ << ",key_num:" << key_num;

        int ret_len = PACKET_HEADER_SIZE + sizeof(int);

        memset(buf, 0, 8192 * sizeof(char));
        auto & send_pkt = *(Packet *)buf;
        memcpy(send_pkt.buf, (char *)&key_num, sizeof(int));
//        memcpy(send_pkt.buf + begin_key_num_size, (char *)&req_num, req_key_nums_size);
        send_pkt.len    = sizeof(int);
        send_pkt.sn     = 0;
        send_pkt.type   = req.type;
        send_pkt.crc = send_pkt.Sum();

        return ret_len;
    }
    // 收到的消息不是 -1 | -1
    char *src = keyFile_->GetKeyCacheBuffer(); // 获取 keyFile 缓存

    int ret_len = PACKET_HEADER_SIZE + req_key_nums * 8;

    memset(buf, 0, 8192 * sizeof(char));
    auto  & reply = *(Packet *)buf;
    memcpy(reply.buf, src + begin_key_num * 8, req_key_nums * 8);
    reply.len = req_key_nums * 8;
    reply.sn = req.sn;
    reply.type = req.type;
    reply.crc = reply.Sum();
    //printf("id:%d, KvStoreEngine::ProcessGetKey, sending key_num: %d\n", threadId_, req_key_nums); 

    return ret_len;
}
