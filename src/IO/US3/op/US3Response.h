#pragma once

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPResponse.h>


namespace DB::US3 {

class US3Response {
public:
    explicit US3Response(const std::string& host)
        :resp_(std::make_shared<Poco::Net::HTTPResponse>()), session_(std::make_shared<Poco::Net::HTTPClientSession>(host))
    {}
    US3Response() = default;
    ~US3Response() = default;
    US3Response(const US3Response& o) = delete;
    US3Response& operator=(const US3Response& o) = delete;

    US3Response(US3Response&& o)
        :resp_(std::move(o.resp_)),
        session_(std::move(o.session_)), response_stream_(o.response_stream_)
    {}
    US3Response& operator=(US3Response&& o) {
        resp_ = std::move(o.resp_);
        session_ = std::move(o.session_);
        response_stream_ = o.response_stream_;
        return *this;
    }

    const Poco::Net::HTTPResponse& response() const { return *resp_; }
    Poco::Net::HTTPResponse& response() { return *resp_; }

    const Poco::Net::HTTPClientSession& session() const { return *session_; }
    Poco::Net::HTTPClientSession& session() { return *session_; }

    void set_response_stream(std::istream& istr) {response_stream_ = &istr;}
    std::istream& response_stream() { return *response_stream_; }
private:
    std::shared_ptr<Poco::Net::HTTPResponse> resp_;
    std::shared_ptr<Poco::Net::HTTPClientSession> session_;
    std::istream* response_stream_;
};
}