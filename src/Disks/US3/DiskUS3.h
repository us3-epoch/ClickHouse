#pragma once

#include "Disks/DiskFactory.h"
#include "Disks/Executor.h"


#include <Poco/DirectoryIterator.h>

namespace DB {

namespace US3 {
class US3Client;
}

class DiskUS3 : public IDisk {
public:
    friend class DiskUS3Reservation;

    DiskUS3(
        String name,
        std::shared_ptr<US3::US3Client> client,
        String bucket,
        String us3_root_path,
        String metadata_path, size_t min_bytes_for_seek, size_t max_put_size);

    const String & getName() const override { return name_; }

    const String & getPath() const override { return metadata_path_; }

    ReservationPtr reserve(UInt64 bytes) override;

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override { moveFile(from_path, to_path); }

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void copyFile(const String & from_path, const String & to_path) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        size_t estimated_size,
        size_t aio_threshold) override;

    void remove(const String & path) override;

    void removeRecursive(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    void createFile(const String & path) override;

    void setReadOnly(const String & path) override;

    const String getType() const override { return "us3"; }

    void shutdown() override;

private:
    bool try_reserve(UInt64 bytes);
    void remove_meta(const String& path, std::vector<String>& to_delete);
    void remove_us3(std::vector<String>& to_delete);
    void remove_meta_recursive(const String& path, std::vector<String>& to_del);

private:
    const String name_;
    std::shared_ptr<US3::US3Client> client_;
    const String bucket_;
    const String us3_root_path_; // us3 root prefix
    const String metadata_path_; // metadata directory in local disk
    size_t min_bytes_for_seek_{0};
    size_t max_put_size_{0};

    UInt64 reserved_bytes_ = 0;
    UInt64 reservation_count_ = 0;
    std::mutex reserve_mutex_;
};
}

