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
#include <sparsepp/spp.h>
#include <mutex> 
#include <condition_variable>
#include <memory>
#include "data_agent.h"
#include <utils.h>
//#include "params.h"

using namespace std;

class KeyFile
{
private:
    //文件写入偏移位置,用个数表示,需要恢复
    size_t keyFilePosition_;
    char* keyCacheBuffer_;

    // 保存id
    int id_;
    // 全局 keyOrderMap 数组
    static std::shared_ptr<spp::sparse_hash_map<int64_t , int32_t>> keyOrderMaparray_[16];
    // 统计已经构建好的map个数
    static int mapCnt_;
    // 互斥量，保护map
    static std::mutex mutex_[16];
    //同步的条件变量
    static std::condition_variable Condition_;

    // 新增 keyOrderMap, 记录 <key, i>
    std::shared_ptr<spp::sparse_hash_map<int64_t , int32_t>> keyOrderMap_;
    int state_; // 随机读：0， 顺序读：1， 倒序读：2
//    int preOrder_, curOrder_, increaseTimes_, decreaseTimes_, totalTimes_;
//    int thresold_;

    // 连接代理
    DataAgent* agent_;

    int ConstructKeyOrderMap(int begin);
    // 返回pos, fileId为key对应的文件id
    int64_t FindKey(int64_t key, int &fileId);
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
    void JudgeStage(int pos);



public:
    KeyFile(DataAgent* agent, int id, bool exist);
    ~KeyFile();

    void SetKey(KVString & key);
    int64_t GetKeyPosition(KVString & key, int &fileId);
    int GetState();

    size_t GetKeyFilePosition();
    void SetKeyFilePosition(int pos);
};

#endif
