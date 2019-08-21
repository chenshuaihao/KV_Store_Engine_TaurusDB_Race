
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <set>
#include <map>
#include <unordered_map>
#include <thread>
#include <iostream>
#include <sstream>
#include <string>

#include "kv_string.h"
#include "KeyFile.h"



KeyFile::KeyFile(const char * dir, int id, bool exist)
    : keyFileFd_(-1), keyFilePosition_(0), keyCacheBuffer_(nullptr)
{
    std::ostringstream fd;
    fd << dir << "/key-" << id;
    this->keyFileFd_ = open(fd.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
    ftruncate(this->keyFileFd_, 8*4000000);     
    this->keyCacheBuffer_ = static_cast<char*>(mmap(nullptr, 8*4000000, PROT_READ | PROT_WRITE, MAP_SHARED, this->keyFileFd_, 0));

    if(!exist) {
        //keymeta文件创建
        std::ostringstream keymeta;
        keymeta << dir << "/keymetafd-" << id;
        this->keyMetaFd_ = open(keymeta.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777);
        ftruncate(this->keyMetaFd_, 4096);     
        this->keyMetaCacheBuffer_ = static_cast<char*>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, this->keyMetaFd_, 0));
        *((size_t*)(this->keyMetaCacheBuffer_)) = 0;
    } else {
        //keyMeta数据恢复
        std::ostringstream keymeta;
        keymeta << dir << "/keymetafd-" << id;
        this->keyMetaFd_ = open(keymeta.str().data(), O_CREAT | O_RDWR | O_NOATIME, 0777); 
        this->keyMetaCacheBuffer_ = static_cast<char*>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, this->keyMetaFd_, 0));
        // keyFilePosition_ : 当前在keyFile中写入了多少个 key
        keyFilePosition_ = *((size_t*)(this->keyMetaCacheBuffer_));        
    }
    // init
    this->state_ = 0;
    this->preOrder_ = 0;
    this->curOrder_ = 0;
    this->increaseTimes_ = 0;
    this->decreaseTimes_ = 0;
    this->totalTimes_ = 0;
    this->thresold_ = 1;

}

KeyFile::~KeyFile() {
    munmap(keyMetaCacheBuffer_, 4096);
    munmap(keyCacheBuffer_, 8*4000000);
    close(this->keyMetaFd_);
    close(this->keyFileFd_);
}

void KeyFile::SetKey(KVString & key) {
    //写入key
    memcpy(keyCacheBuffer_ + (keyFilePosition_ << 3), key.Buf(), 8); //*(keyCacheBuffer_ + keyFilePosition_ << 3) = *((size_t *) key.Buf()); 
    keyFilePosition_++;
    *((size_t*)(this->keyMetaCacheBuffer_)) = keyFilePosition_;

    /*
    //key相关
    //当前缓冲区为 keyCacheBuffer, 缓冲区待写入位置为 keyCacheBufferPosition，获取本线程对应的缓冲区和写入位置
    keyCacheBuffer = keyFiles[id].keyCacheBuffer;
    keyCacheBufferPosition = keyFiles[id].keyCacheBufferPosition++;
    //假如用DIO的话，冠军队伍好像key也使用了DIO

    //写key进?KB缓冲区，位置递增
    memcpy(keyCacheBuffer + (keyCacheBufferPosition << 3), key, 8);

    //key缓冲区写满就落盘,假如用DIO的话，冠军队伍好像key也使用了DIO
    if (keyCacheBufferPosition == BUFFER_KEY_NUM) {
        fd = keyFiles[id].fd;
        filePosition = keyFiles[id].filePosition;

        //1.测试线程亲自落盘，阻塞
        pwrite(fd, keyCacheBuffer, KEY_BLOCK_SIZE, filePosition);
        //2.或者送入任务队列，DIO线程落盘
        Task task(fd, keyCacheBuffer, KEY_BLOCK_SIZE, filePosition);
        dioTaskQueue.push(task);//队列任务太多时，也会阻塞等待，否则测试线程生产太快，DIO线程赶不上，内存一定会爆掉

        keyFiles[id].filePosition += KEY_SIZE;

        //从keyCacheBufferPool中取一个空的keyCacheBuffer缓冲区，作为下一次写入用
        keyFiles[id].keyCacheBuffer = keyCacheBufferPool.getFreekeyCacheBuffer();
        keyFiles[id].keyCacheBufferPosition = 0;        
    }
    */
}

// 构建 map<key, order>
void KeyFile::ConstructKeyOrderMap(int begin) {
    for (int i = begin; i < this->keyFilePosition_; ++i) {
//        int64_t order = (int64_t)i;
        int64_t key = *(int64_t*)(this->keyCacheBuffer_ + i * 8);
        this->keyOrderMap_.insert(pair<int64_t, int64_t>(key, i));
    }
}

int64_t KeyFile::FindKey(int64_t key) {
    if (this->keyOrderMap_.empty()) {
        this->ConstructKeyOrderMap(0);
    } else if (this->keyOrderMap_.size() < this->keyFilePosition_) {
        this->ConstructKeyOrderMap(this->keyOrderMap_.size());
    }
    unordered_map<int64_t, int64_t>::const_iterator isiter = this->keyOrderMap_.find(key);
    if (isiter != this->keyOrderMap_.end()) {
        return isiter->second;
    }
    return -1;
}

int64_t KeyFile::GetKeyPosition(KVString & key, int & state){
    /*
     * 涉及变量： keyFilePosition_ , preOrder, curOrder, increaseTimes, decreaseTimes totalTimes, thresold,  state{随机读:0, 顺序读:1, 倒序读:2 预判阶段: 0+ keyOrderMap=0}(缺省：随机读：0)
     * preOrder 与 curOrder用于比较读取顺序，判断递增还是递减，
     * increaseTimes : 递增次数
     * decreaseTimes : 递减次数
     * totalTimes : 总次数, 超过阈值尚未确定 递增还是递减，则直接判断为随机读
     * thresold : 次数阈值，超过一定次数则判断为顺序读/倒序读
     * state : 读取状态，分为 随机读: 0, 顺序读: 1, 倒序读: 2,
     * keyOrderMap : 存储 <key, order> 键值对，负责对key进行排序，被判断为随机读时，再初始化此map
     * keyFilePosition_ : key总个数
     * */
//    std::cout << "GetKeyPosition.state_ : -----" << this->state_ << "  ---------" << std::endl;
    // printf("getKeyPositon .state :  -------- %d -------\n", this->state_);
    if (this->keyOrderMap_.empty() && this->state_ == 0) {
        // 预判阶段
        for(size_t i = 0; i < this->keyFilePosition_; ++i) {
            if(memcmp(this->keyCacheBuffer_ + i * 8, key.Buf(), 8) == 0) {
                if (this->totalTimes_ == 0) {
                    this->preOrder_ = i;
                    this->curOrder_ = i;
                    this->totalTimes_ ++;
                } else {
                    this->curOrder_ = i;
                    int diff = this->curOrder_ - this->preOrder_;
                    if (diff == 1) {
                        this->increaseTimes_ ++;
                        this->totalTimes_ ++;
                    } else if (diff == -1) {
                        this->decreaseTimes_ ++;
                        this->totalTimes_ ++;
                    } else {
                        // 前后索引差距大于1 ，肯定是随机读, 结束判断, 开始构建 Map
                        this->state_ = 0;
                        // 构建 Map
                        this->ConstructKeyOrderMap(0);
                    }
                }
                break;
            }
        }
        if (this->increaseTimes_ > this->thresold_){
            this->state_ = 1; // 判断状态为 顺序读
        } else if (this->decreaseTimes_ > this->thresold_) {
            this->state_ = 2; //  判断当前状态为 倒序读
        } else if (this->totalTimes_ > this->thresold_ + 1){
            // 判断次数超出阈值，判断当前状态为随机读
            this->state_ = 0;
            // 判断为随机读后， 构建 this->keyOrderMap_
            this->ConstructKeyOrderMap(0);
        }

        // if (!this->keyOrderMap_.empty()){
        //     printf("预判状态 ： keyOrderMap 已构建， state : %d\n", this->state_);
        // } else {
        //     printf("预判状态 ： keyOrderMap 还没构建， state : %d\n", this->state_);
        // }

        this->preOrder_ = this->curOrder_;
        state = state_;
        return this->curOrder_;
    } else if (this->state_ == 1) {
        // 顺序读
        // 根据 preOrder取key
        this->curOrder_ = this->preOrder_ + 1;
        // 校验，如果取出来的key 与 传入的key 不匹配，预判错误！把状态重设为 随机读， 并构建 map
        /*if (memcmp(this->keyCacheBuffer_ + this->curOrder_ * 8, key.Buf(), 8) != 0) { //校验错误
            this->state_ = 0;
            this->curOrder_ = this->FindKey(*(int64_t*)(key.Buf()));
        } */

        // if (!this->keyOrderMap_.empty()){
        //     printf("顺序读：keyOrderMap 已构建， state : %d\n", this->state_);
        // } else {
        //     printf("顺序读：keyOrderMap 还没构建， state : %d\n", this->state_);
        // }
        state = state_;
        this->preOrder_ = this->curOrder_;
        return this->curOrder_; //返回
    } else if (this->state_ == 2) {
        // 倒序读
        // 根据 preOrder 取key
        this->curOrder_ = this->preOrder_ - 1;
        // 校验，如果取出来的key 与 传入的key 不匹配，预判错误！把状态重设为 随机读， 并构建 map
        /*if (memcmp(this->keyCacheBuffer_ + this->curOrder_ * 8, key.Buf(), 8) != 0) {
            // 校验错误， 从 keyOrderMap_中寻找
            this->state_ = 0;
            this->curOrder_ = this->FindKey(*(int64_t*)(key.Buf()));
        }*/

        // if (!this->keyOrderMap_.empty()){
        //     printf("倒序读： keyOrderMap 已构建， state : %d\n", this->state_);
        // } else {
        //     printf("倒序读： keyOrderMap 还没构建， state : %d\n", this->state_);
        // }

        this->preOrder_ = this->curOrder_;
        state = state_;
        return this->curOrder_; //返回
    } else {
        // 随机读
        this->curOrder_ = this->FindKey(*(int64_t*)(key.Buf()));
    
        // if (!this->keyOrderMap_.empty()){
        //     printf("随机读：keyOrderMap 已构建， state : %d\n", this->state_);
        // } else {
        //     printf("随机读：keyOrderMap 还没构建， state : %d\n", this->state_);
        // }

        this->preOrder_ = this->curOrder_;
        return this->curOrder_; //返回
    }
}