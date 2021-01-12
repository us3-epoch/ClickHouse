#include "DiskUS3.h"

#include "IO/US3/US3Client.h"
#include "IO/ReadBufferFromUS3.h"
#include "IO/ReadBufferFromFile.h"
#include "IO/ReadHelpers.h"
#include "IO/SeekAvoidingReadBuffer.h"
#include "IO/WriteBufferFromUS3.h"
#include "IO/WriteBufferFromFile.h"
#include "IO/WriteHelpers.h"
#include "IO/US3/op/CopyObjectRequest.h"
#include "IO/US3/op/DeleteObjectRequest.h"

#include "Common/checkStackSize.h"
#include "Common/createHardLink.h"
#include "Common/quoteString.h"
#include "Common/thread_local_rng.h"

#include <Poco/File.h>

#include <boost/algorithm/string.hpp>

namespace DB { 

#define LOG &Poco::Logger::get("DiskUS3")

namespace {

    String get_random_name()
    {
        std::uniform_int_distribution<int> distribution('a', 'z');
        String res(32, ' '); /// The number of bits of entropy should be not less than 128.
        for (auto & c : res) {
            c = distribution(thread_local_rng);
        }
        return res;
    }
}

namespace ErrorCodes {
    extern const int US3_ERROR; 
    extern const int UNKNOWN_FORMAT;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int INCORRECT_DISK_INDEX;
    extern const int FILE_ALREADY_EXISTS;
}

class Metadata {
public:
    static constexpr UInt32 VERSION_ABSOLUTE_PATHS = 1;
    static constexpr UInt32 VERSION_RELATIVE_PATHS = 2;
    //static constexpr UInt32 VERSION_READ_ONLY_FLAG = 3;

    Metadata(const String& root_path, const String& disk, const String& md_file_path, bool create = false)
        :us3_root_path_(root_path), disk_path_(disk), metadata_file_path_(md_file_path)
    {
        if (create) {
            return;
        }

        ReadBufferFromFile out(disk_path_ + metadata_file_path_, 1024);

        UInt32 version;
        readIntText(version, out);
        if (version < VERSION_ABSOLUTE_PATHS || version > VERSION_RELATIVE_PATHS) {
            throw Exception(
                "US3 Error metadatafile version, path is " + disk_path_ + metadata_file_path_ + " version is " + std::to_string(version),
                ErrorCodes::UNKNOWN_FORMAT);
        }
        assertChar('\n', out);

        UInt32 sz = 0;
        readIntText(sz, out);
        assertChar('\t', out);
        readIntText(total_, out);
        assertChar('\n', out);
        us3_objects_.reserve(sz);

        for (UInt32 i = 0; i < sz; ++i) {
            size_t s = 0;
            String path;
            readIntText(s, out);
            assertChar('\t', out);
            readEscapedString(path, out);
            assertChar('\n', out);

            if (version == VERSION_ABSOLUTE_PATHS) {
                // c++20 string::starts_with()
                if (!boost::algorithm::starts_with(path, us3_root_path_)) {
                    throw Exception(
                        "Path in metadata does not correspond US3 root path. Path: " + path + ", root path: " + us3_root_path_
                            + ", disk path: " + disk_path_,
                        ErrorCodes::UNKNOWN_FORMAT);
                }
                path = path.substr(us3_root_path_.size());
            }

            us3_objects_.emplace_back(path, s);
        }

        readIntText(nlink_, out);
        assertChar('\n', out);
    }

    void add_object(const String& path, size_t s) {
        total_ += s;
        us3_objects_.emplace_back(path, s);
    }

    void save(bool sync = false) { 
        // metadata file format
        // 2
        // 1       30
        // 30      cqmmczhgslphdphysovtonqsacixuffy
        // 0

        WriteBufferFromFile wb(disk_path_ + metadata_file_path_, 1024);

        writeIntText(VERSION_RELATIVE_PATHS, wb);
        writeChar('\n', wb);

        writeIntText(us3_objects_.size(), wb);
        writeChar('\t', wb);
        writeIntText(total_, wb);
        writeChar('\n', wb);

        for (const auto& [path, size] : us3_objects_) {
            writeIntText(size, wb);
            writeChar('\t', wb);
            writeEscapedString(path, wb);
            writeChar('\n', wb);
        }
        writeIntText(nlink_, wb);
        writeChar('\n', wb);

        wb.finalize();
        if (sync) {
            wb.sync();
        }
    }

    const String& root() const { return us3_root_path_; }
public:
    // stored in DiskUS3, so we use **reference**
    const String& us3_root_path_; // us3 object store root path;
    const String& disk_path_; // local fs path;

    String metadata_file_path_; // local metadata file name;
    std::vector<std::pair<String, size_t>> us3_objects_; // path in US3 and size
    size_t total_{0};
    UInt32 nlink_{0}; // hardlink
};

class WriteIndirectBufferFromUS3 final : public WriteBufferFromFileBase {
public:
    WriteIndirectBufferFromUS3(std::shared_ptr<US3::US3Client> client, const String& bucket, Metadata md, String us3_obj_name,
        bool is_mput, size_t bs)
        :WriteBufferFromFileBase(bs, nullptr, 0), impl_(std::move(client), md.root() + us3_obj_name, is_mput, bs),
        md_(std::move(md)), us3_path_(std::move(us3_obj_name))
    {}
    ~WriteIndirectBufferFromUS3() override {
        try {
            finalize();
        }
        catch (...) {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void sync() override {
        if (finalized_) {
            md_.save();
        }
    }
    std::string getFileName() const override { return md_.metadata_file_path_;}
    void finalize() override 
    {
        if (finalized_) {
            return;
        }
        next();
        impl_.finalize();

        md_.add_object(us3_path_, count());
        md_.save();
        finalized_ = true;
    }
private:
    void nextImpl() override {
        /// Transfer current working buffer to WriteBufferFromUS3.
        impl_.swap(*this);

        /// Write actual data to US3.
        impl_.next();

        /// Return back working buffer.
        impl_.swap(*this);
    }

    WriteBufferFromUS3 impl_;
    Metadata md_;
    bool finalized_{false};
    String us3_path_; // us3 object name;
};

class ReadIndirectBufferFromUS3 final : public ReadBufferFromFileBase {
public:
    ReadIndirectBufferFromUS3(Metadata md, std::shared_ptr<US3::US3Client> client, const String& bucket, size_t bs)
        :md_(std::move(md)), client_(std::move(client)), bucket_(bucket), buffer_size_(bs)
    {}

    std::string getFileName() const override { return md_.metadata_file_path_;}

    off_t seek(off_t off, int whence) override
    {
        switch (whence) {
            // from current position
            case SEEK_CUR:
                if (working_buffer.size() && off < available()) {
                    pos += off;
                    return getPosition();
                }
                else {
                    // FIXME:
                    absolute_pos_ = getPosition() + off;
                }
                break;
            // from 0
            case SEEK_SET:
                // offset is in current bound: [lower, upper)
                if (working_buffer.size() && off < absolute_pos_ && off >= (absolute_pos_ - working_buffer.size())) {
                    pos = working_buffer.end() - (absolute_pos_ - off);
                    assert(getPosition() == off);
                    return getPosition();
                }
                else {
                    absolute_pos_ = off;
                }
                break;
            default:
                throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
        }

        curr_ = init();
        pos = working_buffer.end();
        return absolute_pos_;
    }

    off_t getPosition() override { return absolute_pos_ - available();}
private:
    std::unique_ptr<ReadBufferFromUS3> init() {
        auto off = absolute_pos_;
        auto sz = md_.us3_objects_.size();
        for (size_t i = 0; i < sz; ++i) {
            curr_index_ = i;
            const auto& [path, size] = md_.us3_objects_[curr_index_]; // path is relative
            if (off < size) {
                auto p = std::make_unique<ReadBufferFromUS3>(client_, bucket_, md_.root() + path, buffer_size_);
                // seek current position
                p->seek(off, SEEK_SET);
                return p;
            }
            off -= size;
        }
        // offset has exceed total objects's size
        return nullptr;
    }

    bool nextImpl() override
    {
        if (!curr_) {
            curr_ = init();
        }

        if (curr_ && curr_->next()) {
            working_buffer = curr_->buffer();
            absolute_pos_ += working_buffer.size();
            return true;
        }
        // current buffer has no data, change to next object if there is available object;
        if (curr_index_ + 1 >= md_.us3_objects_.size()) {
            return false;
        }

        ++curr_index_;
        const auto& [path, size] = md_.us3_objects_[curr_index_];
        curr_ = std::make_unique<ReadBufferFromUS3>(client_, bucket_, md_.root() + path, buffer_size_);
        curr_->next();
        working_buffer = curr_->buffer();
        absolute_pos_ += working_buffer.size();
        return true;
    }
private:
    Metadata md_;
    std::shared_ptr<US3::US3Client> client_;
    const String& bucket_; // won't change
    size_t buffer_size_;
    size_t absolute_pos_{0}; // since one local metadata file could contain more than one us3 object, this is cumulative pos
    std::unique_ptr<ReadBufferFromUS3> curr_;
    size_t curr_index_{0};
};

class DiskUS3DirectoryIterator final : public IDiskDirectoryIterator {
public:
    DiskUS3DirectoryIterator(const String& fullpath, const String& dir)
        :iter_(fullpath), dir_path_(dir)
    {

    }
    
    void next() override { ++iter_; }

    bool isValid() const override { return iter_ != Poco::DirectoryIterator(); }

    String path() const override {
        return iter_->isDirectory() ? dir_path_ + iter_.name() + '/' : dir_path_ + iter_.name();
    }
    String name() const override { return iter_.name(); }
private:
    Poco::DirectoryIterator iter_;
    String dir_path_;
};

class US3AsyncExecutor : public Executor {
public:
    explicit US3AsyncExecutor() = default;

    std::future<void> execute(std::function<void()> task) override
    {
        auto promise = std::make_shared<std::promise<void>>();

        GlobalThreadPool::instance().scheduleOrThrowOnError([promise, task]() {
            try {
                task();
                promise->set_value();
            }
            catch (...) {
                tryLogCurrentException(&Poco::Logger::get("DiskUS3"), "Failed to run async task");
                try {
                    promise->set_exception(std::current_exception());
                }
                catch (...) {

                }
            }
        });
        return promise->get_future();
    }
};


class DiskUS3Reservation final : public IReservation {
public:
    DiskUS3Reservation(std::shared_ptr<DiskUS3> disk, UInt64 size)
        :disk_(std::move(disk)), size_(size), metric_increment_(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {

    }
    UInt64 getSize() const override { return size_; }
    DiskPtr getDisk(size_t i) const override 
    {
        if (i != 0) {
            throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
        }
        return disk_;
    }
    Disks getDisks() const override { return {disk_}; }
    void update(UInt64 new_size) override {
        std::lock_guard<std::mutex> locker(disk_->reserve_mutex_);
        disk_->reserved_bytes_ -= size_;
        size_ = new_size;
        disk_->reserved_bytes_ += size_;
    }
    ~DiskUS3Reservation() override{
        try {
            std::lock_guard locker{disk_->reserve_mutex_};
            if (disk_->reserved_bytes_ < size_) {
                disk_->reserved_bytes_ = 0;
                LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservations size for disk '{}'.", disk_->getName());
            }
            else {
                disk_->reserved_bytes_ -= size_;
            }
            if (disk_->reservation_count_ == 0) {
                LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservation count for disk '{}'.", disk_->getName());
            }
            else {
                --disk_->reservation_count_;
            }
        }
        catch (...) {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    } 
private:
    std::shared_ptr<DiskUS3> disk_;
    UInt64 size_;
    CurrentMetrics::Increment metric_increment_;
};

DiskUS3::DiskUS3(String name, std::shared_ptr<US3::US3Client> client, String bucket, String us3_root_path, 
        String metadata_path, size_t min_bytes_for_seek, size_t max_put_size)
    :IDisk(std::make_unique<US3AsyncExecutor>()),
    name_(std::move(name)),
    client_(std::move(client)),
    bucket_(std::move(bucket)),
    us3_root_path_(std::move(us3_root_path)),
    metadata_path_(std::move(metadata_path)), min_bytes_for_seek_(min_bytes_for_seek), max_put_size_(max_put_size)
{
    LOG_WARNING(&Poco::Logger::get("DiskUS3"), "Init DiskUS3 local_md_path={} us3_path={}", metadata_path_, us3_root_path_);
}

ReservationPtr DiskUS3::reserve(UInt64 bytes)
{
    if (try_reserve(bytes)) {
        return std::make_unique<DiskUS3Reservation>(std::static_pointer_cast<DiskUS3>(shared_from_this()), bytes);
    }
    return nullptr;
}

bool DiskUS3::exists(const String& path) const {
    return Poco::File(metadata_path_ + path).exists();
}

bool DiskUS3::isFile(const String& path) const {
    return Poco::File(metadata_path_ + path).isFile();
}

bool DiskUS3::isDirectory(const String& path) const {
    return Poco::File(metadata_path_ + path).isDirectory();
}

size_t DiskUS3::getFileSize(const String& path) const {
    Metadata md(us3_root_path_, metadata_path_, path);
    return md.total_;
}

void DiskUS3::createDirectory(const String& path) {
    Poco::File(metadata_path_ + path).createDirectory();
}

void DiskUS3::createDirectories(const String& path) {
    Poco::File(metadata_path_ + path).createDirectories();
}

void DiskUS3::clearDirectory(const String& path) {
    for (auto iter = iterateDirectory(path); iter->isValid(); iter->next()) {
        if (isFile(iter->path())) {
            remove(iter->path());
        }
    }
}

DiskDirectoryIteratorPtr DiskUS3::iterateDirectory(const String& path) {
    return std::make_unique<DiskUS3DirectoryIterator>(metadata_path_ + path, path);
}

void DiskUS3::moveFile(const String& from_path, const String& to_path) {
    if (exists(to_path)) {
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);
    }
    Poco::File(metadata_path_ + from_path).renameTo(metadata_path_ + to_path);
}

void DiskUS3::replaceFile(const String& from_path, const String& to_path) {
    // FIXME:
    Poco::File(metadata_path_ + from_path).renameTo(metadata_path_ + to_path);
}

void DiskUS3::copyFile(const String& from_path, const String& to_path) {
    if (exists(to_path)) {
        remove(to_path);
    }
    Metadata from(us3_root_path_, metadata_path_, from_path);
    Metadata to(us3_root_path_, metadata_path_, to_path, true);

    for (const auto& [path, size] : from.us3_objects_) {
        auto new_path = get_random_name();
        // copy data
        US3::CopyObjectRequest request{from.root() + path, from.root() + new_path};
        // TODO: thread pool
        auto outcome{std::move(client_->copy_object(request))};
        if (!outcome) {
            throw Exception(outcome.error().message(), ErrorCodes::US3_ERROR);
        }

        to.add_object(new_path, size);

    }
    to.save();
}

void DiskUS3::listFiles(const String& path, std::vector<String>& file_names) {
    for (auto iter = iterateDirectory(path); iter->isValid(); iter->next()) {
        file_names.push_back(iter->name());
    }
}
std::unique_ptr<ReadBufferFromFileBase>
DiskUS3::readFile(const String & path, size_t buf_size, size_t estimated_size, size_t aio_threshold, size_t mmap_threshold) const
{
    Metadata md(us3_root_path_, metadata_path_, path);
    LOG_WARNING(&Poco::Logger::get("UDiskS3"), "Read from file by path: {}. Existing S3 objects: {}",
        backQuote(metadata_path_ + path), md.us3_objects_.size());

    auto r = std::make_unique<ReadIndirectBufferFromUS3>(std::move(md), client_, bucket_, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(r), min_bytes_for_seek_); // FIXME: r vs move(r)
}
std::unique_ptr<WriteBufferFromFileBase>
DiskUS3::writeFile(const String & path, size_t buf_size, WriteMode mode, size_t estimated_size, size_t aio_threshold)
{
    using namespace US3;
    bool exist = exists(path);
    /// Path to store new S3 object.
    auto us3_path = get_random_name();
    bool is_multipart = estimated_size >= max_put_size_;
    if (!exist || mode == WriteMode::Rewrite) {
       /// If metadata file exists - remove and create new.
       if (exist)
          remove(path);

       Metadata metadata(us3_root_path_, metadata_path_, path, true);
       /// Save empty metadata to disk to have ability to get file size while buffer is not finalized.
       metadata.save();

       LOG_DEBUG(&Poco::Logger::get("DiskUS3"), "Write to file by path: {} New US3 path: {}", backQuote(metadata_path_ + path), us3_root_path_ + us3_path);
       return std::make_unique<WriteIndirectBufferFromUS3>(client_, bucket_, std::move(metadata), us3_path, is_multipart, buf_size);
    }
    else {
       Metadata metadata(us3_root_path_, metadata_path_, path);

       LOG_DEBUG(&Poco::Logger::get("DiskUS3"), "Append to file by path: {}. New US3 path: {}. Existing S3 objects: {}.",
          backQuote(metadata_path_ + path), us3_root_path_ + us3_path, metadata.us3_objects_.size());

       return std::make_unique<WriteIndirectBufferFromUS3>(client_, bucket_, std::move(metadata), us3_path, is_multipart, buf_size);
    }
}

void DiskUS3::remove(const String & path) {
    std::vector<String> to_delete;
    remove_meta(path, to_delete);
    remove_us3(to_delete);
}

void DiskUS3::removeRecursive(const String& path) {
    std::vector<String> to_delete;
    remove_meta_recursive(path, to_delete);
    remove_us3(to_delete);
}

void DiskUS3::createHardLink(const String& src, const String& dst) {
    Metadata md(us3_root_path_, metadata_path_, src);
    ++md.nlink_;
    md.save();

    DB::createHardLink(metadata_path_ + src, metadata_path_ + dst);
}

void DiskUS3::setLastModified(const String& path, const Poco::Timestamp& ts) {
    Poco::File(metadata_path_ + path).setLastModified(ts);
}

Poco::Timestamp DiskUS3::getLastModified(const String& path) {
    return Poco::File(metadata_path_ + path).getLastModified();
}

void DiskUS3::createFile(const String& path) {
    Metadata md(us3_root_path_, metadata_path_, path, true);
    md.save();
}

void DiskUS3::setReadOnly(const String& path) {
    Poco::File(metadata_path_ + path).setReadOnly(true);
}

void DiskUS3::shutdown() {

}

void DiskUS3::remove_meta(const String& path, std::vector<String>& to_delete)
{
    Poco::File file(metadata_path_ + path);
    if (file.isFile()) {
        Metadata md(us3_root_path_, metadata_path_, path);
        // FIXME: thread safety ???
        if (md.nlink_ == 0) {
            // remove data from us3
            for (const auto & [p, size] : md.us3_objects_) {
                to_delete.push_back(md.root() + p);
            }
        }
        else {                
            --md.nlink_;
            md.save();
        }
        file.remove();
    }
    else {
        // FIXME: delete dir???
        file.remove();
    }
}

void DiskUS3::remove_us3(std::vector<String>& to_delete)
{
    for (const auto& name : to_delete) {
        US3::DeleteObjectRequest request{name};
        auto outcome{std::move(client_->delete_object(request))};
        if (!outcome) {
            // TODO: op idempotent
            throw Exception(outcome.error().message(), ErrorCodes::US3_ERROR);
        }
    }
}

void DiskUS3::remove_meta_recursive(const String& path, std::vector<String>& to_delete)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    Poco::File f(metadata_path_ + path);
    if (f.isFile()) {
        remove_meta(path, to_delete);
    }
    else {
        for (auto iter{iterateDirectory(path)}; iter->isValid(); iter->next()) {
            remove_meta_recursive(iter->path(), to_delete);
        }
        f.remove();
    }
}

bool DiskUS3::try_reserve(UInt64 bytes)
{
    std::lock_guard locker{reserve_mutex_};
    if (bytes == 0) {
        LOG_DEBUG(&Poco::Logger::get("DiskUS3"), "Reserving 0 bytes on s3 disk {}", backQuote(name_));
        ++reservation_count_;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes_);
    if (unreserved_space >= bytes) {
        LOG_DEBUG(&Poco::Logger::get("DiskUS3"), "Reserving {} on disk {}, having unreserved {}.",
            ReadableSize(bytes), backQuote(name_), ReadableSize(unreserved_space));
        ++reservation_count_;
        reserved_bytes_ += bytes;
        return true;
    }
    return false;
}

}
