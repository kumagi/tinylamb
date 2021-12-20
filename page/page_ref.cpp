//
// Created by kumagi on 2021/11/14.
//
#include "page/page_ref.hpp"
#include "page/page_pool.hpp"

namespace tinylamb {

PageRef::~PageRef() { pool_->Unpin(page_->PageId()); }

}  // namespace tinylamb