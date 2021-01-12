#pragma once

#include "IO/US3/op/AbortMultipartUploadReply.h"
#include "IO/US3/op/FinishMultipartUploadReply.h"
#include "IO/US3/op/GetObjectReply.h"
#include "IO/US3/op/InitMultipartUploadReply.h"
#include "IO/US3/op/PutObjectReply.h"
#include "IO/US3/op/UploadPartReply.h"
#include "IO/US3/op/CopyObjectReply.h"
#include "IO/US3/op/DeleteObjectReply.h"
#include "IO/US3/op/US3Outcome.h"


namespace DB {

namespace US3 {

using US3RespOutcome = US3Outcome<US3Response>;
using US3InitMultipartUploadOutcome = US3Outcome<InitMultipartUploadReply>;
using US3PutObjectOutcome = US3Outcome<PutObjectReply>;
using US3GetObjectOutcome = US3Outcome<GetObjectReply>;
using US3UploadPartOutcome = US3Outcome<UploadPartReply>;
using US3AbortMultipartOutcome = US3Outcome<AbortMultipartUploadReply>;
using US3FinishMultipartOutcome = US3Outcome<FinishMultipartUploadReply>;
using US3CopyObjectOutcome = US3Outcome<CopyObjectReply>;
using US3DeleteObjectOutcome = US3Outcome<DeleteObjectReply>;

class Request;
class PutObjectRequest;
class InitMultipartUploadRequest;
class GetObjectRequest;
class UploadPartRequest;
class FinishMultipartUploadRequest;
class AbortMultipartUploadRequest;
class CopyObjectRequest;
class DeleteObjectRequest;

class ClientConfiguration {
public:
    ClientConfiguration() = default;
    ~ClientConfiguration() = default;
    ClientConfiguration& operator=(const ClientConfiguration& o) = default;
public:
    String bucket_;
    String access_key_;
    String secret_key_;
    String endpoint_;
    String prefix_; 

    bool check_md5_{false};
};

class US3Client {
public:
    explicit US3Client(const ClientConfiguration& config)
        :config_(config)
    {}
    ~US3Client() = default;

    US3GetObjectOutcome get_object(const GetObjectRequest& request);
    US3PutObjectOutcome put_object(const PutObjectRequest& request);
    US3InitMultipartUploadOutcome init_multipart_upload(const InitMultipartUploadRequest& request);
    US3AbortMultipartOutcome abort_multipart_upload(const AbortMultipartUploadRequest& request);
    US3FinishMultipartOutcome finish_multipart_upload(const FinishMultipartUploadRequest& request);
    US3UploadPartOutcome upload_part(const UploadPartRequest& request);
    US3CopyObjectOutcome copy_object(const CopyObjectRequest& request);
    US3DeleteObjectOutcome delete_object(const DeleteObjectRequest& request);

    String bucket() { return config_.bucket_; }
    String prefix() { return config_.prefix_; }

private:
    US3RespOutcome make_request(const Request& request);
    String get_host();
    std::shared_ptr<std::ostringstream> hmac_auth(std::istringstream& istr);
    std::string us3_auth(std::istringstream& istr);

private:
    ClientConfiguration config_;
};


}
}