#ifndef TINYLAMB_LOG_RECORD_HPP
#define TINYLAMB_LOG_RECORD_HPP

namespace tinylamb {

enum LogType {
  kBegin,
  kEnd,
  kInsertRow,
  kUpdateRow,
  kDeleteRow,
  kCheckpointBegin,
  kCheckpointEnd
};

struct LogRecord {
  size_t length;
  uint64_t lsn;
  uint64_t prev_lsn;
  uint64_t txn_id;
  LogType type;
};

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_HPP
