#ifndef TINYLAMB_OPERATION_HPP
#define TINYLAMB_OPERATION_HPP

#include <memory>

namespace tinylamb {

enum OpType {
  kUpdateRow,
  kInsertRow,
  kDeleteRow,
  kCheckpoint
};



}  // namespace tinylamb

#endif // TINYLAMB_OPERATION_HPP
