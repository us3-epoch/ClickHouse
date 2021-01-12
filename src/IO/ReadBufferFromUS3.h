#pragma once

#include "SeekableReadBuffer.h"

#include "Common/config.h"
#include "IO/HTTPCommon.h"
#include "IO/US3/op/GetObjectReply.h"

namespace DB {

namespace US3 {
class US3Client;
}


class ReadBufferFromUS3 : public SeekableReadBuffer {
public:
    explicit ReadBufferFromUS3(std::shared_ptr<US3::US3Client> client, String bucket, String key, size_t buffer_size);
    ~ReadBufferFromUS3() override = default;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
private:
    bool nextImpl() override;
    std::unique_ptr<ReadBuffer> init();
private:
    std::shared_ptr<US3::US3Client> client_;
    String bucket_;
    String key_;
    size_t buffer_size_;
    std::unique_ptr<ReadBuffer> impl_;
    bool init_{false};
    off_t offset_{0};
    US3::GetObjectReply reply_;

    Poco::Logger* log_{&Poco::Logger::get("ReadBufferFromUS3")};
};

}