#include "ReadBufferFromUS3.h"

#include "IO/ReadBufferFromIStream.h"

#include "IO/US3/US3Client.h"
#include "IO/US3/op/GetObjectRequest.h"

#include "Common/Stopwatch.h"
#include "common/logger_useful.h"

#include <Poco/StreamCopier.h>

#include <utility>


namespace ProfileEvents {
    extern const Event US3ReadMicroseconds;
    extern const Event US3ReadBytes;
}

namespace DB {
namespace ErrorCodes {
    extern const int US3_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

ReadBufferFromUS3::ReadBufferFromUS3(std::shared_ptr<US3::US3Client> client, String bucket, String key, size_t buffer_size)
    :SeekableReadBuffer(nullptr, 0), client_(std::move(client)), bucket_(std::move(bucket)), key_(std::move(key)), buffer_size_(buffer_size)
{
    LOG_WARNING(log_, "Init key={} buf_size={}", key_, buffer_size_);
}

// put data to buffer
bool ReadBufferFromUS3::nextImpl()
{
    if (!init_) {
        impl_ = init();
        init_ = true;
    }

    Stopwatch watch;
    auto ret = impl_->next();

    ProfileEvents::increment(ProfileEvents::US3ReadMicroseconds, watch.elapsedMicroseconds());
    if (!ret) {
        return false;
    }

    internal_buffer = impl_->buffer();
    ProfileEvents::increment(ProfileEvents::US3ReadBytes, internal_buffer.size());
    working_buffer = internal_buffer;
    return true;
}

off_t ReadBufferFromUS3::seek(off_t offset, int whence) {
    if (init_) {
        throw Exception("Seek is allowed only before first read attempt from the buffer.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    }
    if (whence != SEEK_SET) {
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    }

    if (offset < 0) {
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }

    offset_ = offset;
    return offset;
}

off_t ReadBufferFromUS3::getPosition()
{
    return offset_ + count();
}

std::unique_ptr<ReadBuffer> ReadBufferFromUS3::init() {
    LOG_DEBUG(log_, "Read US3 object. Bucket: {}, Key: {}, Offset: {}", bucket_, key_, std::to_string(offset_));

    US3::GetObjectRequest request(key_);
    if (offset_ != 0) {
        request.set_range("bytes=" + std::to_string(offset_) + "-");
    }
    auto outcome{std::move(client_->get_object(request))};
    if (outcome) {
        // move assignment operator
        reply_ = outcome.result_with_ownership();
        LOG_WARNING(log_, "Get Succ={}", reply_.response_.response().getContentLength());
        return std::make_unique<ReadBufferFromIStream>(reply_.get_body(), buffer_size_);
    }
    else {
        throw Exception(outcome.error().message(), ErrorCodes::US3_ERROR);
    }
}
}
