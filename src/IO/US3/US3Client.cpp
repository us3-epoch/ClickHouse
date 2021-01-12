#include "US3Client.h"

#include "IO/US3/op/Request.h"
#include "IO/US3/op/AbortMultipartUploadRequest.h"
#include "IO/US3/op/FinishMultipartUploadRequest.h"
#include "IO/US3/op/GetObjectRequest.h"
#include "IO/US3/op/InitMultipartUploadRequest.h"
#include "IO/US3/op/PutObjectRequest.h"
#include "IO/US3/op/UploadPartRequest.h"
#include "IO/US3/op/CopyObjectRequest.h"
#include "IO/US3/op/DeleteObjectRequest.h"
#include "IO/HTTPCommon.h"

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/HMACEngine.h>
#include <Poco/SHA1Engine.h>
#include <Poco/DigestStream.h>
#include <Poco/Base64Encoder.h>
#include <Poco/StreamCopier.h>
#include <Poco/JSON/Parser.h>

#include <aws/core/utils/base64/Base64.h>

namespace DB {

namespace US3 {

#define LOG &Poco::Logger::get("US3Client")

US3InitMultipartUploadOutcome US3Client::init_multipart_upload(const InitMultipartUploadRequest& request) {
    auto outcome{std::move(make_request(request))};
    if (!outcome.is_succ()) {
        return US3InitMultipartUploadOutcome(outcome.error());
    }

    auto& body = outcome.result().response_stream();
    std::ostringstream ostr;
    Poco::StreamCopier::copyStream(body, ostr);
    Poco::JSON::Parser parser;
    auto res = parser.parse(ostr.str());
    auto obj = res.extract<Poco::JSON::Object::Ptr>();
    auto upload_id = obj->getValue<std::string>("UploadId");
    return US3InitMultipartUploadOutcome(upload_id);
}

US3PutObjectOutcome US3Client::put_object(const PutObjectRequest& request)
{
    auto outcome{std::move(make_request(request))};
    if (!outcome.is_succ()) {
        return US3PutObjectOutcome(outcome.error());
    }

    return US3PutObjectOutcome(outcome.result().response().get("ETag"));
}

US3GetObjectOutcome US3Client::get_object(const GetObjectRequest& request)
{
    auto outcome{std::move(make_request(request))};
    if (!outcome.is_succ()) {
        return US3GetObjectOutcome(outcome.error());
    }

    return US3GetObjectOutcome(outcome.result_with_ownership());
}

US3AbortMultipartOutcome US3Client::abort_multipart_upload(const AbortMultipartUploadRequest& request)
{
    auto outcome{std::move(make_request(request))};
    if (!outcome.is_succ()) {
        return US3AbortMultipartOutcome(outcome.error());
    }
    return US3AbortMultipartOutcome(AbortMultipartUploadReply());
}

US3FinishMultipartOutcome US3Client::finish_multipart_upload(const FinishMultipartUploadRequest& request)
{
    auto outcome{std::move(make_request(request))};
    if (!outcome.is_succ()) {
        return US3FinishMultipartOutcome(outcome.error());
    }

    return US3FinishMultipartOutcome(FinishMultipartUploadReply());
}

US3UploadPartOutcome US3Client::upload_part(const UploadPartRequest& request)
{
    auto outcome{std::move(make_request(request))};
    if (!outcome) {
        return US3UploadPartOutcome(outcome.error());
    }
    return US3UploadPartOutcome(UploadPartReply{});
}

US3CopyObjectOutcome US3Client::copy_object(const CopyObjectRequest& request)
{
    auto outcome{std::move(make_request(request))};
    if (!outcome) {
        return US3CopyObjectOutcome(outcome.error());
    }
    return US3CopyObjectOutcome(CopyObjectReply{});
}

US3DeleteObjectOutcome US3Client::delete_object(const DeleteObjectRequest& request)
{
    auto outcome{std::move(make_request(request))};
    if (!outcome) {
        return US3DeleteObjectOutcome(outcome.error());
    }

    return US3DeleteObjectOutcome(DeleteObjectReply{});
}

US3RespOutcome US3Client::make_request(const Request& req) {
    US3RespOutcome outcome(get_host());
    auto& session = outcome.result().session();

    session.setKeepAlive(true);

    // make request
    Poco::Net::HTTPRequest request(req.method(), req.key(), Poco::Net::HTTPMessage::HTTP_1_1);
    request.set("User-Agent", "US3CH/1.0");
    request.setContentType("");
    if (config_.check_md5_) {
        request.set("Content-MD5", "");
    }

    const auto& header = req.header();
    const auto& data = req.body();
    if (header) {
        const auto& h = header.value();
        for (const auto& [k, v] : h) {
            request.set(k, v);
        }
    }
    std::istringstream dig(req.method() + "\n" + request.get("Content-MD5", "") + "\n" + request.getContentType() + "\n" + request.get("Date", "") + "\n"
        + "/" + bucket() + req.key());

    request.set("Authorization", us3_auth(dig));

    for (auto i = 0; i < 5; ++i) {
        outcome.clear();
        if (data) {
            auto sz = data->tellp();
            auto sz2 = data->str().size();
#if defined(POCO_HAVE_INT64)
            request.setContentLength64(sz2);
#else
            request.setContentLength(sz2);
#endif
        }
        auto& ostr = session.sendRequest(request);
        if (data) {
            // in -> out
            std::istringstream istr(data->str()); // copy input data
            Poco::StreamCopier::copyStream(istr, ostr);
        }

        auto& response = outcome.result().response();
        std::istream& recv_body = session.receiveResponse(response);

        auto status = response.getStatus();
        if (status < Poco::Net::HTTPResponse::HTTP_OK || status > Poco::Net::HTTPResponse::HTTP_IM_USED) {
            auto reason = response.getReason();
            // decode error body
            auto session_id = response.get("X-SessionId");

            int ret_code = 0;
            std::string err_msg;
            if (response.getContentLength() > 0) {
                std::ostringstream recv_ostr;
                Poco::StreamCopier::copyStream(recv_body, recv_ostr);
                Poco::JSON::Parser parser;
                auto res = parser.parse(recv_ostr.str());
                auto obj = res.extract<Poco::JSON::Object::Ptr>();
                err_msg = obj->getValue<std::string>("ErrMsg");
                ret_code = obj->getValue<int>("RetCode");
                LOG_ERROR(&Poco::Logger::get("UDiskS3"), "HTTP Error, path={} status={}, reason={} retry={} sessionid={} ret={}, msg={}",
                    req.key(), status, reason, i, session_id, ret_code, err_msg);
            }
            else {
                LOG_ERROR(&Poco::Logger::get("UDiskS3"), "HTTP Error, path={} status={}, reason={} retry={} sessionid={} no response body",
                    req.key(), status, reason, i, session_id);
            }
            outcome.error().set(session_id, static_cast<int>(status), reason, ret_code, err_msg);
            continue;
        }

        outcome.set_succ(true);
        if (response.getContentLength() > 0) {
            outcome.result().set_response_stream(recv_body);
        }
        break;
    }
    return outcome;
}

std::string US3Client::get_host() {
    return config_.bucket_ + "." + config_.endpoint_;
}


std::shared_ptr<std::ostringstream> US3Client::hmac_auth(std::istringstream& istr)
{
    Poco::HMACEngine<Poco::SHA1Engine> hmac(config_.secret_key_);
    //Poco::DigestOutputStream dos(hmac);

    //// copy data
    //Poco::StreamCopier::copyStream(istr, dos);
    //dos.close();
    hmac.update(istr.str());
    //auto di = Poco::DigestEngine::digestToHex(hmac.digest());
    const auto& di = hmac.digest();

    auto ostr{std::make_shared<std::ostringstream>()};
    //std::istringstream distr(di);
    Poco::Base64Encoder encoder(*ostr);
    //Poco::StreamCopier::copyStream(distr, encoder);
    for (const auto& i : di) {
        encoder << i;
    }
    //encoder << di;
    encoder.close();

    return ostr;
}

std::string US3Client::us3_auth(std::istringstream& istr)
{
    auto o{std::move(hmac_auth(istr))};
    return "UCloud " + config_.access_key_ + ":" + o->str();
}

}
}