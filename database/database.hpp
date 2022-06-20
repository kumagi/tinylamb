#ifndef TINYLAMB_DATABASE_HPP
#define TINYLAMB_DATABASE_HPP

#include <functional>

#include "database/page_storage.hpp"
#include "relation_storage.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class IndexSchema;

class Database {
 public:
  explicit Database(std::string_view dbname) : relations_(dbname) {}
  TransactionContext BeginContext();

  StatusOr<Table> CreateTable(TransactionContext& ctx, const Schema& schema);
  Status CreateIndex(TransactionContext& ctx, std::string_view schema_name,
                     const IndexSchema& idx);

  StatusOr<Table> GetTable(TransactionContext& ctx,
                           std::string_view schema_name);

  [[maybe_unused]] void DebugDump(TransactionContext& ctx, std::ostream& o);

  StatusOr<TableStatistics> GetStatistics(TransactionContext& ctx,
                                          std::string_view schema_name);
  Status UpdateStatistics(TransactionContext& txn, std::string_view schema_name,
                          const TableStatistics& ts);
  Status RefreshStatistics(TransactionContext& txn,
                           std::string_view schema_name);

  PageStorage& Storage() { return *relations_.GetPageStorage(); }

 private:
  RelationStorage relations_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_HPP
