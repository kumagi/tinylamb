/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_TIMESTAMP_TRUNC_KERNEL_HPP
#define TINYLAMB_EXECUTOR_DETAIL_TIMESTAMP_TRUNC_KERNEL_HPP

#include <cstdint>
#include <optional>
#include <string_view>

#include "common/status_or.hpp"
#include "type/value.hpp"

namespace tinylamb {
class FunctionCallExpression;
}

namespace tinylamb::relational_detail {

enum class UtcTimestampTruncUnit : uint8_t { kSecond, kMinute, kHour, kDay };

// Only constant units and a UTC session/target zone are compiled. Other
// signatures retain ordinary evaluation, including its NULL/error rules.
std::optional<UtcTimestampTruncUnit> CompileUtcTimestampTrunc(
    const FunctionCallExpression& call);

// Recognizes canonical civil text with an optional +00 suffix. The packed
// YYYYMMDDhhmmss key preserves equality and ordering of the canonical UTC
// output strings for years 0001..9999. A rejected input requires ordinary
// evaluation; rejection itself is not a SQL error.
std::optional<int64_t> TryUtcTimestampTruncKey(
    std::string_view text, UtcTimestampTruncUnit unit) noexcept;

StatusOr<Value> FormatUtcTimestampTruncKey(int64_t key);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_TIMESTAMP_TRUNC_KERNEL_HPP
