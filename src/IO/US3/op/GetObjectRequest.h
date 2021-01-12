#pragma once

#include "IO/US3/op/Request.h"

namespace DB::US3 {

class GetObjectRequest : public Request {
public:
    GetObjectRequest(const std::string& key)
        :Request(Poco::Net::HTTPRequest::HTTP_GET), key_(key)
    {}
    void set_range(const std::string& range)
    {
        header_.emplace(std::unordered_map<std::string, std::string>({{"Range", range}}));
    }
    std::string key() const override { return key_; }
private:
    std::string key_;
};

}