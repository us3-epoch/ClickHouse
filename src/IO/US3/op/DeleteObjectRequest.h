#pragma once

#include "IO/US3/op/Request.h"

namespace DB::US3 {

class DeleteObjectRequest : public Request {
public:
    DeleteObjectRequest(String key)
        :Request(Poco::Net::HTTPRequest::HTTP_DELETE),
        key_(key)
    {}
    String key() const  override {return key_;}
private:
    String key_;
};

}