# KV_Store_Engine_TaurusDB_Race

A C++ KV Store Engine (华为云TaurusDB性能挑战赛，司机队，初赛第11名，复赛第26名)

[![license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://opensource.org/licenses/MIT)

## 项目说明
简介：华为云TaurusDB性能挑战赛，基于C++编写的简易KV数据库存储引擎。  
比赛结果：初赛279.911s，复赛使用nanomsg：5270.72s，后期替换成asio库后速度飞跃，但是线上一直有bug没来得及debug，只跑出了写入成绩260-350s。  

团队成员（排名不分先后）：  
[YohannLiang](https://github.com/YohannLiang2016)  
[chenshuaihao](https://github.com/chenshuaihao)  
[Alan-Paul](https://github.com/Alan-Paul)  

## Envoirment
* CPU：Intel(R) Xeon(R) Gold 6151 CPU @ 3.00GHz  (16核)
* OS： Linux version 3.10.0-957.21.3.el7.x86_64

## build - 在代码根目录执行
* 目录：  
KV_Store_Engine_TaurusDB_Race_stage1 初赛代码，计算/存储不分离  
KV_Store_Engine_TaurusDB_Race_stage1 复赛代码，计算/存储分离的设计，缓存设计。
* 进入对应的目录，再编译构建，具体可参见目录下的readme

### Tech

## 初赛
* 随机写入阶段：  
文件划分：key文件、value文件、metadata文件；  
KV写入方式：KV均采用mmap缓冲区，保证kill -9后KV数据不丢失，所有key全部存mmap，value缓冲区凑够16个value（参数可调）就刷盘；  
key文件：16个测试线程各自对应一个key文件，set时候直接向key文件对应的mmap内存追加key数据；  
value文件：采用DIO方式顺序写入，16个测试线程各自对应一个value文件，先写入mmap缓冲区，凑够16个value调用pwrite向文件追加数据； 
metadata文件：保存元数据信息，用于数据恢复，具体保存了kill时候的文件偏移以及缓冲区已有value个数的信息

* 顺序读取阶段：  
线程模型：每个测试线程开启18个存储IO线程（生产者线程）在后台运行，18个存储IO线程各自对应4MB内存分区，分区加载value文件到内存缓冲区，这样可以尽可能地打满SSD的IO带宽；  
当测试线程get完当前分区数据后，唤醒对应的存储IO线程加载新的分区数据，并开始get下一个分区数据，典型的生产者消费者模型；  
索引：采用STL的map实现，但是由于顺序读取特征，直接用计数器代替了。  

* 随机读取阶段：  
华为这次初赛中随机读取阶段并不随机，采用倒序读的方式，所以思路和顺序读取一样，只不过是存储IO线程变成倒序加载文件分区而已；  

* 待补充完善

## 复赛
* 计算节点：  
思路：1.将存储节点的索引构建、查找等过程分离到了计算节点执行，计算节点只需要传递偏移pos给store节点即可；  
      2.创建多个网络IO线程分区向store发请求，加载数据到缓存；  
网络模型：为了把网络IO带宽打满，每个测试线程都开了n个网络IO线程，向store端get请求数据，加载到计算节点的缓存中；  
get读取阶段：16个线程首先向store请求各自文件的key，并行构建好16个索引（每个文件一个），然后遍历16个hash索引，直到查找key对应的文件及其偏移pos；  
索引部分：16个线程并行构建索引，为了降低内存消耗，采用sparse_hash_map，内存占用低，性能也较高；  
网络部分：开始使用nanomsg实现通信，可能我们使用方式不对，网络性能很低，带宽只用了一点，最后两天才急忙替换成asio库，速度瞬间提升，但是线上测评始终有点问题，结果来不及Debug了；  

* 存储节点：  
思路：存储节点的进程开了16个storeEngine实例，每个实例都有一个Server，每个实例各自负责维护自己的文件；  
set：set后来改成异步落盘，采用双缓冲区设计（可以改参数为多缓冲区），一个后台的存储IO线程调用pwrite刷盘；  
网络部分：使用asio库，参考asio例程实现服务器；  
其他：其他地方与原来区别不大；  

* 待补充完善

## Others

Enjoy!


