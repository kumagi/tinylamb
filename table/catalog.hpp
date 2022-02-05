#ifndef TINYLAMB_CATALOG_HPP
#define TINYLAMB_CATALOG_HPP

#include <cstdint>
#include <ostream>
#include <string_view>

namespace tinylamb {

class PageManager;
class Transaction;
class Schema;

class Catalog {
  static constexpr uint64_t kCatalogPageId = 1;

 public:
  explicit Catalog(PageManager* pm);

  void Initialize();

  // Return false if there is the same name table is already exists.
  bool CreateTable(Transaction& txn, Schema& schema);

  Schema GetSchema(Transaction& txn, std::string_view table_name);

  [[nodiscard]] size_t Schemas() const;

  [[maybe_unused]] void DebugDump(std::ostream& o);

 private:
  PageManager* pm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CATALOG_HPP
