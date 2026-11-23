/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_DATABASE_CATALOG_READER_HPP
#define TINYLAMB_DATABASE_CATALOG_READER_HPP

#include <cstdint>
#include <string_view>

#include "common/status_or.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/function.hpp"

namespace tinylamb {

class TransactionContext;

// Catalog operations available through TransactionContext: table and
// statistics lookup, function registration, and cache invalidation. Table
// DDL, recovery, filesystem access, and database lifecycle live on Database.
// This interface owns no resources.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
class CatalogReader {
 public:
  virtual ~CatalogReader() = default;

  [[nodiscard]] virtual uint64_t CatalogEpoch() const noexcept = 0;

  // Invalidates caches stamped at the current schema epoch (T485). Only
  // readers that own a schema epoch implement this; the default is a no-op.
  virtual void BumpCatalogEpoch() noexcept {}

  virtual StatusOr<bool> TableExists(TransactionContext& context, std::string_view name) {
    const auto table = GetTable(context, name);
    if (table.HasValue()) { return true;
}
    if (table.GetStatus() == Status::kNotExists) { return false;
}
    return table.GetStatus();
  }
  virtual StatusOr<Table> GetTable(TransactionContext& context,
                                   std::string_view name) = 0;
  virtual StatusOr<TableStatistics> GetStatistics(TransactionContext& context,
                                                  std::string_view name) = 0;
  virtual StatusOr<Function> GetOrAddFunction(TransactionContext& context,
                                              std::string_view name,
                                              int argument_count) = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_CATALOG_READER_HPP
