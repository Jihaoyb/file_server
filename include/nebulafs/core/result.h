#pragma once

#include <optional>

#include "nebulafs/core/error.h"

namespace nebulafs::core {

/// @brief Minimal Result type used to avoid exceptions across module boundaries.
template <typename T>
class Result {
public:
    Result(const T& value) : value_(value) {}
    Result(T&& value) : value_(std::move(value)) {}
    Result(const Error& error) : value_(std::nullopt), error_(error) {}

    bool ok() const { return value_.has_value(); }
    const T& value() const { return value_.value(); }
    T& value() { return value_.value(); }
    const Error& error() const { return error_; }

private:
    std::optional<T> value_;
    Error error_{ErrorCode::kOk, ""};
};

template <>
class Result<void> {
public:
    Result() : ok_(true) {}
    Result(const Error& error) : ok_(false), error_(error) {}

    bool ok() const { return ok_; }
    void value() const {}
    const Error& error() const { return error_; }

private:
    bool ok_{false};
    Error error_{ErrorCode::kOk, ""};
};

/// @brief Convenience helper for a successful empty result.
inline Result<void> Ok() { return Result<void>(); }

}  // namespace nebulafs::core
