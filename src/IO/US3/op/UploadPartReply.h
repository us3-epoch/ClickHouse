#pragma once

#include "common/types.h"
namespace DB::US3 {

class UploadPartReply {
public:
    UploadPartReply() = default;
    String etag() {return ""; } 
};
}