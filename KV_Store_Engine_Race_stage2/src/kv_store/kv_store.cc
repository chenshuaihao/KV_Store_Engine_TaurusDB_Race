#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <iostream>
#include <sstream>
#include <string.h>
#include <easylogging++.h>

#include "KvStoreEngine.h"
#include "tcp_server.h"

INITIALIZE_EASYLOGGINGPP;

#define KV_LOG(level) LOG(level) << "[" << __FUNCTION__ << ":" << __LINE__ << "] "

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

void mypause() {
    //system("stty raw -echo");
    std::cout << "press any key to exit ...\n";
    while (1)
    {
        sleep(1);
    }
    getchar();
    system("stty -raw echo");
    std::cout << std::endl;
}

void remove_files(const char *dir, bool clear)
{
    struct stat st;
    if (stat(dir, &st) == -1 || !S_ISDIR(st.st_mode))
    {
        LOG(INFO) << "make dir: " << dir;
        mkdir(dir, 0755);
        return;
    }

    DIR *dp;
    struct dirent *item;
    dp = opendir(dir);
    if (!dp)
    {
        perror("Open data directory failed!");
        return;
    }

    char path[NAME_MAX];
    int pos = strlen(dir);
    strncpy(path, dir, pos);
    if (dir[pos - 1] != '/')
    {
        pos++;
        strcat(path, "/");
    }

    if(clear) {
        while ((item = readdir(dp)) != NULL)
        {
            if ((0 == strcmp(".", item->d_name)) || (0 == strcmp("..", item->d_name)))
            {
                continue;
            }
            if ((item->d_type & DT_DIR) == DT_DIR)
            {
                continue;
            }
            strncpy(path + pos, item->d_name, NAME_MAX - pos);
            unlink(path);
        }
    }

    closedir(dp);
}
void Run(short port, std::shared_ptr<KvStoreEngine> kvStoreEngine) {

    TcpServer s(port, kvStoreEngine);
    s.Run();
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
    KV_LOG(INFO) << "  >> bind host : " << host;
    KV_LOG(INFO) << "  >> datax dir : " << dir;
    KV_LOG(INFO) << "  >> clear dir : " << clear;

    ////////////////////////////////////////////////////
    //TODO: your code

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
                        缓存分区大小：1MB、2MB、5MB、10MB四种之一
                        2、启动18个分区网络IO线程，打满网络IO带宽，将value发送出去
                    }
                    如果state == 热点随机读
                    {
                        1、启动18个分区存储IO线程，打满存储IO带宽，倒序加载value到内存缓冲区，注意：存储节点2GB内存
                        缓存分区大小：1MB、2MB、5MB、10MB四种之一
                        2、启动18个分区网络IO线程，打满网络IO带宽，将value发送出去
                    }
    */

    /*
        计算节点：
        set()
            直接调用set
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

    remove_files(dir, clear);
    int port = 9527; //端口从9527开始, 创建16个实例执行 Run , 每个线程都有自己的 kvStoreEngineIns 和 tcpServer
    for(int i = 0; i < 16; ++i, ++port) {
        //创建数据库实例
        auto kvStoreEngineIns = std::make_shared<KvStoreEngine>();
        kvStoreEngineIns->Init(dir, i);
        std::thread(Run, port, kvStoreEngineIns->GetPtr()).detach();
    }


    int ret = 0;
    std::cout << "No user code exists!!!" << std::endl;

    if (ret != -1) {
        mypause();
    }

    return ret;

}
