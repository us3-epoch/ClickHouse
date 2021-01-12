#pragma once

#include "IO/US3/op/Request.h"
#include <Poco/URI.h>

namespace DB::US3 {
class UploadPartRequest : public Request {
public:
    UploadPartRequest(std::string key, std::string upload_id, int n)
        :Request(Poco::Net::HTTPRequest::HTTP_PUT), key_(std::move(key))
    {
        uri_.setPath(key_);
        uri_.addQueryParameter("uploadId", upload_id);
        uri_.addQueryParameter("partNumber", std::to_string(n));
    }

    String key() const override { return uri_.toString(); }
private:
    Poco::URI uri_;
    std::string key_;
};

}