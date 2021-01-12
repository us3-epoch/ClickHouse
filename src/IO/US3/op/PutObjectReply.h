#pragma once

#include "common/types.h"

namespace DB::US3 {

class PutObjectReply {
public:
    PutObjectReply() = default;
    PutObjectReply(std::string etag)
        :etag_(std::move(etag))
    {}
    String etag() { return etag_; }
public:
    std::string etag_;
};
}