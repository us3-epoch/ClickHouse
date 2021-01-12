#pragma once

#include "IO/US3/op/Request.h"
#include <Poco/URI.h>

namespace DB::US3 {

class AbortMultipartUploadRequest : public Request {
public:
    explicit AbortMultipartUploadRequest(const std::string& key, const std::string& upload_id)
        :Request(Poco::Net::HTTPRequest::HTTP_DELETE), key_(key)
    {
        uri_.setPath(key);
        uri_.addQueryParameter("uploadId", upload_id);
    }
    std::string key() const override { return uri_.toString(); }
private:
    std::string key_;
    Poco::URI uri_;
};
}