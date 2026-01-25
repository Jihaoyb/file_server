#pragma once

#include <string>

namespace nebulafs::core {

/// @brief Canonical error codes used across modules and mapped to HTTP responses.
enum class ErrorCode {
    kOk = 0,
    kInvalidArgument,
    kNotFound,
    kAlreadyExists,
    kIoError,
    kDbError,
    kUnauthorized,
    kForbidden,
    kInternal,
};

/// @brief Error payload describing a failure with a code and human-readable message.
struct Error {
    ErrorCode code{ErrorCode::kOk};
    std::string message;
};

}  // namespace nebulafs::core
