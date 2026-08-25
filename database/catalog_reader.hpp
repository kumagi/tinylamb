/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_DATABASE_CATALOG_READER_HPP
#define TINYLAMB_DATABASE_CATALOG_READER_HPP

#include <string_view>
#include <cstdint>

#include "common/status_or.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/function.hpp"

namespace tinylamb {

class TransactionContext;

// The catalog capability required by a transaction/query. Keeping this
// separate from Database prevents executor state from acquiring DDL, crash,
// filesystem, and lifecycle operations through TransactionContext.
class CatalogReader {
 public:
  virtual ~CatalogReader() = default;

  [[nodiscard]] virtual uint64_t CatalogEpoch() const noexcept = 0;

  virtual StatusOr<Table> GetTable(TransactionContext& context,
                                   std::string_view name) = 0;
  virtual StatusOr<TableStatistics> GetStatistics(
      TransactionContext& context, std::string_view name) = 0;
  virtual StatusOr<Function> GetOrAddFunction(TransactionContext& context,
                                              std::string_view name,
                                              int argument_count) = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_CATALOG_READER_HPP
