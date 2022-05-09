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
  Transaction Begin();
  TransactionContext BeginContext();

  Status CreateTable(Transaction& txn, const Schema& schema);
  Status CreateIndex(Transaction& txn, std::string_view schema_name,
                     const IndexSchema& idx);

  StatusOr<Table> GetTable(Transaction& txn, std::string_view schema_name);

  [[maybe_unused]] void DebugDump(Transaction& txn, std::ostream& o);

  StatusOr<TableStatistics> GetStatistics(Transaction& txn,
                                          std::string_view schema_name);
  Status UpdateStatistics(Transaction& txn, std::string_view schema_name,
                          const TableStatistics& ts);
  Status RefreshStatistics(Transaction& txn, std::string_view schema_name);

  PageStorage& Storage() { return *relations_.GetPageStorage(); }

 private:
  RelationStorage relations_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_DATABASE_HPP
