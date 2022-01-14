//
// Created by kumagi on 2022/01/15.
//

#include "page/leaf_page.hpp"

namespace tinylamb {

bool tinylamb::LeafPage::Insert(page_id_t page_id, Transaction& txn,
                                std::string_view key, std::string_view value) {}

bool LeafPage::Update(page_id_t page_id, Transaction& txn, std::string_view key,
                      std::string_view value) {}
bool LeafPage::Delete(page_id_t page_id, Transaction& txn,
                      std::string_view key) {}

bool LeafPage::Read(page_id_t page_id, Transaction& txn, std::string_view key,
                    std::string_view* result) {}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::LeafPage>::operator()(
    const ::tinylamb::LeafPage& r) const {
  return 0;
}
