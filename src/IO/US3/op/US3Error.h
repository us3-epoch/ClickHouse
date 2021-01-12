#pragma once

#include "common/types.h"

#include <sstream>

namespace DB::US3 {

using ErrorCode = int;

class US3Error {
public:
    US3Error() = default;

    std::string message() {
        std::stringstream ss;
        ss << "XSessionID:" << x_session_id_ << " Status:" << status_ << " Reason:" << reason_
            << " RetCode:" << error_code_ << " ErrMsg:" << error_message_;
        return ss.str();
    }
    void set(std::string session_id, int status, String reason, int ret_code, String msg)
    {
        x_session_id_ = std::move(session_id);
        status_ = status;
        reason_ = std::move(reason);
        error_code_ = ret_code;
        error_message_ = std::move(msg);
    }

    void clear() {
        x_session_id_.clear();
        status_ = 0;
        reason_.clear();
        error_code_ = 0;
        error_message_.clear();
    }
private:
    std::string x_session_id_;
    int status_{0};
    std::string reason_;
    ErrorCode error_code_;
    std::string error_message_;
};

}