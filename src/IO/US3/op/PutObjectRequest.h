#pragma once

#include "IO/US3/op/Request.h"

namespace DB::US3 {

class PutObjectRequest : public Request {
public:
    PutObjectRequest(const String& key, std::shared_ptr<std::stringstream>& body)
        :Request(Poco::Net::HTTPRequest::HTTP_PUT), key_(key)
    {
        body_ = std::move(body);
    }

    String key() const override { return key_; }

public:
    std::string key_;
};

}