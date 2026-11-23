/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_EXPLAIN_FORMAT_HPP
#define TINYLAMB_EXECUTOR_DETAIL_EXPLAIN_FORMAT_HPP

#include <iosfwd>
#include <string>
#include <string_view>

#include "query/statement.hpp"

namespace tinylamb {
class TransactionContext;
}

namespace tinylamb::relational_detail {

std::string IndentLines(std::string_view text, int spaces);
// Formats a non-negative byte count as B, KiB, MiB, or GiB (one decimal
// place for scaled units; zero is "0B"). Convert or clamp signed estimates
// before calling this unsigned API.
std::string FormatBytes(size_t bytes);

void WriteEstimatedPhysicalPlan(TransactionContext& context,
                                const SelectStatement& statement,
                                std::ostream& output, int indent);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_EXPLAIN_FORMAT_HPP
