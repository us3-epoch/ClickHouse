#pragma once

#include "Common/config.h"

#include "IO/BufferWithOwnMemory.h"
#include "IO/WriteBuffer.h"
#include "IO/HTTPCommon.h"

#include <vector>
#include "common/types.h"

namespace DB {

class WriteBufferFromOwnString;

namespace US3 {
class US3Client;
}

// Perform single file put or multipart upload
class WriteBufferFromUS3 : public BufferWithOwnMemory<WriteBuffer> {
public:
    explicit WriteBufferFromUS3(std::shared_ptr<US3::US3Client> client, String key, bool is_mput, size_t buffer_size);
    ~WriteBufferFromUS3() override;

    void finalize() override;

private:
    void nextImpl() override;
    void write_part(const String& s);
    void init_mput();
    void finalize_impl();
    void finish_upload();
    void put();

private:
    std::shared_ptr<US3::US3Client> client_;

    String bucket_;
    String key_;
    bool is_multipart_{false};
    String upload_id_;
    std::vector<String> part_etags_;
    size_t last_part_size_{0};
    std::unique_ptr<WriteBufferFromOwnString> tmp_buf_;

    bool finalized_{false};

    Poco::Logger* log_{&Poco::Logger::get("WriteBufferFromUS3")};
};
}