#include "file_io.h"

#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>

#include "easylogging++.h"

FileIo::FileIo(const char * name) {
    const auto mode = "a+";
    fh_ = fopen(name, mode);
    if (fh_ == NULL) {
        LOG(ERROR) << "open file failed: " << errno << "-" <<  strerror(errno);
        exit(-1);
    }
    fseek(fh_, 0, SEEK_END);
}

FileIo::~FileIo() {
    if (fh_ != NULL) {
        fclose(fh_);
    }
}

int FileIo::Append(const char * buf, int size) {
    fseek(fh_, 0, SEEK_END);

    if (fwrite(reinterpret_cast<const char *>(&size), sizeof(size), 1, fh_) != 1) {
        LOG(ERROR) << "write size failed: " << errno << "-" <<  strerror(errno);
        return 0;
    }
    if (fwrite(buf, size, 1, fh_) != 1) {
        LOG(ERROR) << "write buf failed: " << errno << "-" <<  strerror(errno);
        return sizeof(size);
    }

    return size + sizeof(size);
}

int FileIo::Append(KVString & info) {
    return Append(info.Buf(), info.Size());
}

int FileIo::Append(KVString &key, KVString & val) {
    fseek(fh_, 0, SEEK_END);

    int key_size = key.Size();
    int val_size = val.Size();
    if (fwrite(reinterpret_cast<const char *>(&key_size), sizeof(key_size), 1, fh_) != 1) {
        LOG(ERROR) << "write key size failed: " << errno << "-" <<  strerror(errno);
        return 0;
    }
    if (fwrite(reinterpret_cast<const char *>(&val_size), sizeof(val_size), 1, fh_) != 1) {
        LOG(ERROR) << "write val size failed: " << errno << "-" <<  strerror(errno);
        return sizeof(val_size);
    }
    if (fwrite(key.Buf(), key_size, 1, fh_) != 1) {
        LOG(ERROR) << "write key buf failed: " << errno << "-" <<  strerror(errno);
        return sizeof(int) * 2;
    }
    if (fwrite(val.Buf(), val_size, 1, fh_) != 1) {
        LOG(ERROR) << "write key buf failed: " << errno << "-" <<  strerror(errno);
        return sizeof(int) * 2 + key_size;
    }

    return key_size + val_size + sizeof(key_size) + sizeof(val_size);
}

int FileIo::Read(int offset, int size, KVString &out) {
    fseek(fh_, offset, SEEK_SET);

    char * buf = new char[size];
    auto ret = fread(buf, size, sizeof(char), fh_);
    out.Reset(buf, ret * size);

    return ret * size;
}

int FileIo::ReadKV(int offset, KVString &key, KVString &val) {
    fseek(fh_, offset, SEEK_SET);

    char buf[sizeof(int) * 2];
    int ret = fread(buf, sizeof(buf), 1, fh_);
    if (ret != 1) {
        return 0;
    }

    int &key_size = *(int*)buf;
    int &val_size = *(int*)(buf + sizeof(int));

    char * key_buf = new char [key_size];
    char * val_buf = new char [val_size];
    auto ret_key = fread(key_buf, key_size, 1, fh_);
    auto ret_val = fread(val_buf, val_size, 1, fh_);
    if (ret_key != 1 || ret_val != 1) {
        return 0;
    }
    key.Reset(key_buf, key_size * ret_key);
    val.Reset(val_buf, val_size * ret_val);

    return key_size + val_size + sizeof(key_size) + sizeof(val_size);
}

int FileIo::Size() {
    fseek(fh_, 0, SEEK_END);

    return ftell(fh_);
}

bool FileIo::Remove(const char * name) {
    if (name == nullptr) {
        return true;
    }

    struct stat st;
    if (stat(name, &st) == -1) {
        return false;
    }
    if (!S_ISDIR(st.st_mode)) {
        return unlink(name) == 0;
    }

    std::vector<KVString> files;
    ScanDir(name, false, files);
    for (auto f : files) {
        unlink(f.Buf());
    }
    return true;
}

int FileIo::ScanDir(const char * dir, bool create_if_no_exist, std::vector<KVString> &files) {
    struct stat st;
    if (stat(dir, &st) == -1 || !S_ISDIR(st.st_mode)) {
        if (create_if_no_exist) {
            mkdir(dir, 0755);
        }
        return 0;

    }
    DIR * dp;
    struct dirent * item;
    dp = opendir(dir);
    if( !dp) {
        perror("Open data directory failed!");
        return 0;
    }

    auto base_dir = Dir(dir);
    char path[NAME_MAX];
    int pos = base_dir.Size() - 1;
    strncpy(path, base_dir.Buf(), NAME_MAX - 1);

    while ((item = readdir(dp)) != NULL) {
        if( (0 == strcmp(".", item->d_name)) || (0 == strcmp("..", item->d_name))) {
            continue;
        }
        if( (item->d_type & DT_DIR) == DT_DIR) {
            continue;
        }
        strncpy(path + pos, item->d_name, NAME_MAX - pos);
        files.emplace_back(path);
    }
    closedir(dp);
    return files.size();
}
