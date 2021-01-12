#pragma once

#include "IO/US3/op/US3Response.h"

namespace DB::US3 {

class GetObjectReply {
public:
    GetObjectReply(US3Response&& response)
        : response_(std::move(response))
    {}
    GetObjectReply() = default;
    GetObjectReply(const US3Response& response) = delete;
    GetObjectReply(GetObjectReply&& o)
        :response_(std::move(o.response_))
    {}
    GetObjectReply& operator=(GetObjectReply&& r) {
        response_ = std::move(r.response_);
        return *this;
    }
    GetObjectReply& operator=(const GetObjectReply& r) = delete;
    std::istream& get_body() { return response_.response_stream(); }
public:
    US3Response response_;
};
}