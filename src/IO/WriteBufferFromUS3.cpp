#include "WriteBufferFromUS3.h"

#include "IO/US3/US3Client.h"
#include "IO/WriteBufferFromString.h"

#include "IO/US3/op/AbortMultipartUploadRequest.h"
#include "IO/US3/op/FinishMultipartUploadRequest.h"
#include "IO/US3/op/InitMultipartUploadRequest.h"
#include "IO/US3/op/PutObjectRequest.h"
#include "IO/US3/op/UploadPartRequest.h"
#include "common/logger_useful.h"

#include <utility>

namespace ProfileEvents {
    extern const Event US3WriteBytes;
}

namespace DB {

namespace ErrorCodes {
    extern const int US3_ERROR;
}

namespace {
    constexpr size_t US3_PARTSIZE = 4 * 1024 * 1024;
}

WriteBufferFromUS3::WriteBufferFromUS3(std::shared_ptr<US3::US3Client> client, String key, bool is_mput, size_t buffer_size)
    :BufferWithOwnMemory<WriteBuffer>(buffer_size, nullptr, 0),
    client_(std::move(client)), key_(std::move(key)), is_multipart_(is_mput),
    tmp_buf_{std::make_unique<WriteBufferFromOwnString>()}
{
    LOG_WARNING(log_, "Init key={}", key_);
    if (is_multipart_) {
        init_mput();
    }
}

WriteBufferFromUS3::~WriteBufferFromUS3()
{
    try {
        finalize_impl();
    }
    catch (...) {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void WriteBufferFromUS3::nextImpl()
{
    /// no data to write
    if (!offset()) {
        return;
    }

    // move data from working buffer to own temp buffer
    tmp_buf_->write(working_buffer.begin(), offset());
    ProfileEvents::increment(ProfileEvents::US3WriteBytes, offset());

    if (is_multipart_) {
        last_part_size_ += offset();
        while (last_part_size_ >= US3_PARTSIZE) {
            auto& str{tmp_buf_->str()};
            write_part(str.substr(0, US3_PARTSIZE));

            auto next_part{str.substr(US3_PARTSIZE)}; 
            last_part_size_ = next_part.size();

            tmp_buf_->restart();
            tmp_buf_->write(next_part.data(), next_part.size());
        }
    }
}

void WriteBufferFromUS3::write_part(const String& s)
{
    US3::UploadPartRequest request{key_, upload_id_, part_etags_.size() + 1};

    auto outcome = client_->upload_part(request);
    if (outcome) {
        auto etag = outcome.result().etag();
        LOG_DEBUG(log_, "upload part bucket:{} key:{} etag:{}", bucket_, key_, etag);
        part_etags_.emplace_back(etag);
    }
    else {
        throw Exception(outcome.error().message(), ErrorCodes::US3_ERROR);
    }
}

void WriteBufferFromUS3::finalize()
{
    finalize_impl();
}

void WriteBufferFromUS3::finalize_impl()
{
    if (finalized_)  {
        return;
    }
    next();

    if (is_multipart_) {
        write_part(tmp_buf_->str());
        finish_upload();
    }
    else {
        put();
    }

    finalized_ = true;
}

void WriteBufferFromUS3::init_mput() {
    US3::InitMultipartUploadRequest request(key_);
    auto outcome{std::move(client_->init_multipart_upload(request))};
    if (outcome) {
        upload_id_ = outcome.result().upload_id();
        LOG_DEBUG(log_, "Multipart upload initiated. Upload id: {}", upload_id_);
    }
    else {
        throw Exception(outcome.error().message(), ErrorCodes::US3_ERROR);
    }
}

void WriteBufferFromUS3::finish_upload() {
        // abort or finish
    if (part_etags_.empty()) {
        LOG_DEBUG(log_, "Multipart upload has no data. Aborting it. Bucket: {}, Key: {}, Upload_id: {}", bucket_, key_, upload_id_);

        US3::AbortMultipartUploadRequest request(key_, upload_id_);
        auto outcome{std::move(client_->abort_multipart_upload(request))};
        if (outcome.is_succ()) {
            LOG_DEBUG(log_, "Aborting multipart upload completed. Upload_id: {}", upload_id_);
        }
        else {
            throw Exception(outcome.error().message(), ErrorCodes::US3_ERROR);
        }
    }
    else {
        US3::FinishMultipartUploadRequest request(key_, upload_id_);
        auto outcome{std::move(client_->finish_multipart_upload(request))};
        if (outcome.is_succ()) {
            LOG_DEBUG(log_, "Multipart upload completed. Upload_id: {}", upload_id_);
        }
        else {
            throw Exception(outcome.error().message(), ErrorCodes::US3_ERROR);
        }
    }
}

void WriteBufferFromUS3::put() {
    auto body{std::make_shared<std::stringstream>(tmp_buf_->str())};
    LOG_DEBUG(log_, "Start put {} {}", body->str(), body->tellp());
    US3::PutObjectRequest request(key_, body); // FIXME: brace-enclosed initializer list
    tmp_buf_ = std::make_unique<WriteBufferFromOwnString>();
    auto outcome{std::move(client_->put_object(request))};
    if (outcome) {
        LOG_WARNING(log_, "Single part upload has completed. Bucket: {}, Key: {}", bucket_, key_);
    }
    else {
        throw Exception(outcome.error().message(), ErrorCodes::US3_ERROR);
    }
}

}