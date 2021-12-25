#include "type/catalog.hpp"

#include <cstdint>
#include <string>

#include "log_message.hpp"
// #include "page/catalog_page.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "transaction/transaction.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

void Catalog::Initialize() {}

bool Catalog::CreateTable(Transaction& txn, Schema& schema) { return false; }

Schema Catalog::GetSchema(Transaction& txn, std::string_view table_name) {
  throw std::runtime_error("Not implemented: " + std::string(table_name));
}

[[nodiscard]] size_t Catalog::Schemas() const {
  return 0;
}

[[maybe_unused]] void Catalog::DebugDump(std::ostream& o) {
}

}  // namespace tinylamb
