/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "common/status_or.hpp"

namespace tinylamb::relational_detail {

bool BuildGroupDictionary(
    size_t row_count, size_t max_unique,
    const std::function<Status(std::vector<uint64_t>&)>& fill_hashes,
    const std::function<bool(size_t, size_t)>& eq_rows,
    std::vector<uint32_t>* out_subgid, uint32_t* out_count,
    const std::vector<uint8_t>* row_active,
    std::vector<uint32_t>* out_rep_rows);

}  // namespace tinylamb::relational_detail
