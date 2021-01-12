#pragma once

#include "IO/US3/op/US3Error.h"

#include <Poco/Net/HTTPMessage.h>

#include <type_traits>

namespace DB::US3 {


template<typename R>
class US3Outcome {
public:
    US3Outcome() = default;
    ~US3Outcome() = default;

    US3Outcome(const US3Error& e)
        :error_(e), succ_(false)
    {}
    US3Outcome(US3Error&& e)
        :error_(std::move(e)), succ_(false)
    {}
    US3Outcome(US3Outcome&& o)
        :result_(std::forward<R>(o.result_)), error_(std::move(o.error_)), succ_(o.succ_)
    {}

    template<typename ...Args, typename = std::enable_if_t<std::is_constructible_v<R, Args&&...>>>
    US3Outcome(Args&&... args)
        :result_(std::forward<Args>(args)...), succ_(true)
    {}

    US3Outcome(const US3Outcome& other) = delete;
    US3Outcome& operator=(const US3Outcome& other) = delete;

    const US3Error& error() const { return error_; }
    US3Error& error() { return error_; }

    bool is_succ() const { return succ_; }
    void set_succ(bool succ) { succ_ = succ; }

    operator bool() const { return succ_; }

    R& result() { return result_; }
    const R& result() const { return result_; }

    R&& result_with_ownership() { return std::move(result_); }

    void clear() {
        succ_ = false;
        error_.clear();
    }

private:
    R result_;
    US3Error error_;
    bool succ_{false};
};

}