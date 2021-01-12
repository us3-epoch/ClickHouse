#pragma once

#include "IO/US3/op/Request.h"

namespace DB::US3 {

class CopyObjectRequest : public Request {
public:
    CopyObjectRequest(String src, String dst)
        :Request(Poco::Net::HTTPRequest::HTTP_PUT),
        src_(std::move(src)), dst_(std::move(dst))
    {
        std::unordered_map<String, String> t{
            {"X-Ufile-Copy-Source", src}
        };
        header_.emplace(std::move(t));
    }

    String key() const override { return dst_; }

private:
    String src_;
    String dst_;
};
}
