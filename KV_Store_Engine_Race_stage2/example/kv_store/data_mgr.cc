#include "data_mgr.h"

#include <sys/stat.h>

int DataMgr::Init(const char * dir, const char * ext, int file_size) {
    Release();

    max_file_size_ = file_size;

    ext_ = ext;
    dir_ = FileIo::Dir(dir);
    if (ScanDir(dir_.Buf()) < 1) {
        NewFile();
    }

    return 0;
}

void DataMgr::Release( ) {
    files_info_.clear();
}

void DataMgr::Clear() {
    Release();
    if (dir_.Size() > 0) {
        FileIo::Remove(dir_.Buf());
    }

    cur_file_no_    = -1;
    cur_file_size_  = 0;
}

uint64_t DataMgr::Append(KVString & key, KVString & val) {
    uint64_t pos = cur_file_no_;
    pos <<= 32;
    pos += cur_file_size_;

    int size = files_info_[cur_file_no_]->Append(key, val);
    if (size > 0) {
        cur_file_size_ += size;
    }
    files_info_[cur_file_no_]->Flush();
    if (cur_file_size_ > max_file_size_) {
        NewFile();
    }

    return pos;
}

void DataMgr::NewFile() {
    cur_file_no_ ++;

    files_info_[cur_file_no_] = std::unique_ptr<FileIo>(
        new FileIo(No2FileName(cur_file_no_, ext_.Buf()).Buf()));
    cur_file_size_ = 0;
}

int DataMgr::Get(uint64_t pos, KVString & key, KVString &val) {
    int no     = (pos >> 32) & 0xffffffff;
    int offset = (pos & 0xffffffff);
    if (files_info_.count(no) > 0) {
        return files_info_[no]->ReadKV(offset, key, val);
    } else {
        return -1;
    }
}

int DataMgr::ScanDir(const char * dir) {
    std::vector<KVString> files;
    int num = FileIo::ScanDir(dir, true, files);
    if (num < 1) {
        return 0;
    }

    auto filename2num = [&](KVString & name)->int {
        const char * data_ext = ext_.Buf();
        if (name.Suffix(data_ext)) {
            const char * buf = name.Buf();
            const int buf_size = name.Size();
            int begin = 0;
            for (int i = buf_size - 1; i > 0; i --) {
                if (buf[i] == '/') {
                    begin = i + 1;
                    break;
                }
            }
            return std::atoi((name.SubStr(begin, name.Size() - strlen(data_ext) - 1).Buf()));
        }
        return -1;
    };

    files_info_.clear();

    int file_num = 0;
    for (auto file : files) {
        num = filename2num(file);
        if (num < 0) {
            continue;
        }
        files_info_[num] = std::unique_ptr<FileIo>(new FileIo(file.Buf()));
        if (num > cur_file_no_) {
            cur_file_no_ = num;
        }
        file_num ++;
    }
    if (file_num > 0) {
        cur_file_size_ = files_info_[cur_file_no_]->Size();
    }

    if (cur_file_size_ >= max_file_size_) {
        NewFile();
        file_num ++;
    }
    return file_num;
}

KVString DataMgr::No2FileName(int no, const char * ext) {
    char name[256];
    strcpy(name, dir_.Buf());
    snprintf(name + strlen(name), sizeof(name), "%010d%s", no, ext);

    return KVString(name);
}
