#pragma once

#include "IO/US3/op/Request.h"
#include <Poco/URI.h>

namespace DB::US3 {

class FinishMultipartUploadRequest : public Request {
public:
    FinishMultipartUploadRequest(const std::string& key, const std::string& id)
        :Request(Poco::Net::HTTPRequest::HTTP_POST), key_(key)
    {
        uri_.setPath(key);
        uri_.addQueryParameter("uploadId", id);
    }
    void set_etags(const std::vector<std::string>& etags) {
        body_ = std::make_shared<std::stringstream>();
        auto& ss = *body_;
        auto sz = etags.size();
        for (size_t i = 0; i < sz - 1; ++i) {
            ss << etags[i] << ",";
        }
        ss << etags[sz - 1];
    }
    std::string key() const override {return uri_.toString(); }

private:
    std::string key_;
    std::string new_key_;
    Poco::URI uri_;
};
}