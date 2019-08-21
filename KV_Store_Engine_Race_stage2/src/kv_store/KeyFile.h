/*
    KeyFile.h KeyFile头文件
*/

#ifndef Key_FILE_H_
#define Key_FILE_H_

#include <iostream>
#include <sstream>
#include <string>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include <map>
#include <unordered_map>

#include "params.h"

using namespace std;

class KeyFile
{
private:
    //key文件相关
    //KeyFile文件描述符fd，现在用mmap映射
    int keyFileFd_;
    //文件写入偏移位置,用个数表示,需要恢复
    size_t keyFilePosition_;
    //keyFile的mmap写入缓存，8B*400 0000 / 1024 / 1024
    char* keyCacheBuffer_;

    //key元数据相关
    //key元数据文件描述符fd
    int keyMetaFd_;
    //keyMetaFile的缓存，4KB
    char* keyMetaCacheBuffer_;

    // 新增 keyOrderMap, 记录 <key, i>
    std::unordered_map<int64_t , int64_t> keyOrderMap_;
    int state_; // 随机读：0， 顺序读：1， 倒序读：2
    int preOrder_, curOrder_, increaseTimes_, decreaseTimes_, totalTimes_;
    int thresold_;

    void ConstructKeyOrderMap(int begin);
    int64_t FindKey(int64_t key);
    /*
     * 涉及变量： keyFilePosition_ , preOrder, curOrder, increaseTimes, decreaseTimes, thresold,  state{随机读:0, 顺序读:1, 倒序读:2 预判阶段: 0+ keyOrderMap=0}(缺省：随机读：0)
     * preOrder 与 curOrder用于比较读取顺序，判断递增还是递减，
     * increaseTimes : 递增次数
     * decreaseTimes : 递减次数
     * thresold : 次数阈值，超过一定次数则判断为顺序读/倒序读
     * state : 读取状态，分为 随机读: 0, 顺序读: 1, 倒序读: 2,
     * keyOrderMap : 存储 <key, order> 键值对，负责对key进行排序，被为随机读时，再初始化此map
     * keyFilePosition_ : key总个数
     * */



public:
    KeyFile(const char * dir, int id, bool exist);
    ~KeyFile();

    size_t SetKey(KVString & key);
    int64_t GetKeyPosition(KVString & key, int & state);
    int64_t GetKey(int64_t pos, KVString & key);
    size_t GetKeyNum() const {
        return keyFilePosition_;
    }
    char* GetKeyCacheBuffer() const {
        return keyCacheBuffer_;
    }
};


#endif
