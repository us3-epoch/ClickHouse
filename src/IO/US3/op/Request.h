#pragma once

#include "common/types.h"

#include <Poco/Net/HTTPRequest.h>

#include <optional>
#include <unordered_map>
#include <memory>

namespace DB::US3 {

class Request {
public:
    Request(String method)
        :method_(std::move(method))
    {}
    virtual ~Request() = default;

    const std::optional<std::unordered_map<String, String>>& header() const { return header_; }
    const std::shared_ptr<std::stringstream>& body() const { return body_; }
    const String& method() const { return method_; }

    virtual String key() const = 0;

protected:
    std::optional<std::unordered_map<String, String>> header_{std::nullopt};
    std::shared_ptr<std::stringstream> body_{nullptr};
private:
    String method_;
};

}