#pragma once

#include "common/types.h"

namespace DB::US3 {

class InitMultipartUploadReply {
public:
    InitMultipartUploadReply() = default;
    explicit InitMultipartUploadReply(const std::string& id)
        :upload_id_(id)
    {}

    const std::string& upload_id() const { return upload_id_; }
private:
    String upload_id_;
    size_t block_size_{0};
    String key_;
};

}