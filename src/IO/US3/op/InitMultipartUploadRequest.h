#pragma once

#include "IO/US3/op/Request.h"

namespace DB::US3 {

class InitMultipartUploadRequest : public Request {
public:
    InitMultipartUploadRequest(const std::string& key)
        :Request(Poco::Net::HTTPRequest::HTTP_POST), key_(key)
    {}

    std::string key() const override { return key_ + "?uploads"; }
public:
    String key_;
};

}