/*
    params.h 参数设置头文件
*/

#ifndef PARAMS_H_
#define PARAMS_H_

//using namespace std;

//测试线程数量
#define TEST_THREADS_NUM 16

//文件个数
#define FILE_NUM 16
//KEY的字节大小
#define KEY_SIZE 8
//VALUE的字节大小
#define VALUE_SIZE 4096
//value缓冲区存储的value个数，16KB=4个value
#define BUFFER_VALUE_NUM 8
//key缓冲区存储的value个数，4KB=512个key
#define BUFFER_KEY_NUM 512
//value一次落盘的大小
#define VALUE_BLOCK_SIZE 8*4096
// 写缓冲区个数，暂时为双缓冲区
#define WRITE_BUFFER_NUM 2

//每个文件分区个数，每个区64MB
#define PARTITION_NUM_PER_FILE 245
//value文件大小
#define SIZE_PER_VALUE_FILE 64*1024*1024*245
//key文件大小
#define SIZE_PER_KEY_FILE 8*4000000
//key mmap缓冲区大小 30MB
#define KEY_BLOCK_SIZE 8*4000000

//read线程数量, 开多可以打满ssd的IO
#define READ_THREAD_NUM 10
// read 线程使用的缓冲区大小，单位为M
#define PARTITION_SIZE 4
// 每次读多少个value
#define VALUE_NUM_PER_READ 4
#endif


////////////////////////////////////////////////////////
/*
    同步set版本，来n个value，阻塞式pwrite一次，
*/
// /*
//     params.h 参数设置头文件
// */

// #ifndef PARAMS_H_
// #define PARAMS_H_

// //using namespace std;

// //测试线程数量
// #define TEST_THREADS_NUM 16

// //文件个数
// #define FILE_NUM 16
// //KEY的字节大小
// #define KEY_SIZE 8
// //VALUE的字节大小
// #define VALUE_SIZE 4096
// //value缓冲区存储的value个数，16KB=4个value
// #define BUFFER_VALUE_NUM 1
// //key缓冲区存储的value个数，4KB=512个key
// #define BUFFER_KEY_NUM 512
// //value一次落盘的大小
// #define VALUE_BLOCK_SIZE 1*4096

// //每个文件分区个数，每个区64MB
// #define PARTITION_NUM_PER_FILE 245
// //value文件大小
// #define SIZE_PER_VALUE_FILE 64*1024*1024*245
// //key文件大小
// #define SIZE_PER_KEY_FILE 8*4000000
// //key mmap缓冲区大小 30MB
// #define KEY_BLOCK_SIZE 8*4000000

// //read线程数量
// #define READ_THREAD_NUM 16
// // read 线程使用的缓冲区大小，单位为M
// #define PARTITION_SIZE 4
// #endif