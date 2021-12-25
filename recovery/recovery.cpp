#include "recovery.hpp"

#include <fcntl.h>
#include <sys/mman.h>

#include <filesystem>
#include <transaction/transaction.hpp>

#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "recovery/log_record.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

Recovery::Recovery(std::string_view log_path, std::string_view db_path,
                   PagePool *pp, TransactionManager *tm)
    : log_name_(log_path), log_data_(nullptr), pool_(pp), tm_(tm) {}

void Recovery::StartFrom(size_t offset) {
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);
  int fd = open(log_name_.c_str(), O_RDONLY, 0666);
  if (fd == -1) {
    std::string str_err = strerror(errno);
    throw std::runtime_error("could not open the file: " + str_err);
  }
  log_data_ = static_cast<char *>(
      mmap(nullptr, filesize, PROT_READ, MAP_PRIVATE, fd, 0));
  std::string_view entire_log(log_data_, filesize);
  entire_log.remove_prefix(offset);

  std::unordered_map<uint64_t, size_t> log_offsets;
  std::unordered_map<uint64_t, uint64_t> txn_lsns;

  // Redo phase starts here.
  LogRecord log;
  size_t log_offset = 0;
  while (!entire_log.empty()) {
    bool success = LogRecord::ParseLogRecord(entire_log, &log);
    if (!success) {
      LOG(ERROR) << "Failed to parse log at offset: " << log_offset;
      break;
    }
    txn_lsns[log.txn_id] = log_offset;
    entire_log.remove_prefix(log.Size());
    LogRedo(log_offset, log);
    log_offset += log.Size();
  }
  LOG(TRACE) << "redo finished: " << log_offset;

  // Undo phase starts here.
  for (auto &txn_id : tm_->active_transactions_) {
    uint64_t prev_lsn = txn_lsns[txn_id];
    while (prev_lsn != 0) {
      size_t prev_offset = prev_lsn;
      std::string_view undo_log_data(log_data_ + prev_offset, filesize);
      if (!LogRecord::ParseLogRecord(undo_log_data, &log)) {
        LOG(ERROR) << "Failed to parse log at offset on undo: " << prev_offset;
        break;
      }
      LogUndo(prev_lsn, log);
      prev_lsn = log.prev_lsn;
    }
  }
}

void Recovery::LogRedo(uint64_t lsn, const LogRecord &log) {
  switch (log.type) {
    case LogType::kUnknown:
      assert(!"unknown log type must not be parsed");
    case LogType::kBegin:
      tm_->active_transactions_.insert(log.txn_id);
      break;
    case LogType::kInsertRow: {
      PageRef target = pool_->GetPage(log.pos.page_id);
      if (target->PageLSN() < lsn) {
        target->InsertImpl(log.pos, log.redo_data);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kUpdateRow: {
      PageRef target = pool_->GetPage(log.pos.page_id);
      if (target->PageLSN() < lsn) {
        target->UpdateImpl(log.pos, log.redo_data);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kDeleteRow: {
      PageRef target = pool_->GetPage(log.pos.page_id);
      if (target->PageLSN() < lsn) {
        target->DeleteImpl(log.pos);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kCommit:
      tm_->active_transactions_.erase(log.txn_id);
      break;
    case LogType::kBeginCheckpoint:
      break;
    case LogType::kEndCheckpoint:
      break;
    case LogType::kSystemAllocPage: {
      PageRef target = pool_->GetPage(log.allocated_page_id);
      if (target->PageLSN() < lsn) {
        target->PageInit(log.allocated_page_id, log.allocated_page_type);
      }
      break;
    }
    case LogType::kSystemDestroyPage:
      break;
  }
}

void Recovery::LogUndo(uint64_t lsn, const LogRecord &log) {
  Transaction txn(log.txn_id, tm_, tm_->lock_manager_, tm_->page_manager_,
                  tm_->logger_);
  LOG(TRACE) << "Undo: " << log;
  switch (log.type) {
    case LogType::kUnknown:
      LOG(FATAL) << "Unknown type log";
      throw std::runtime_error("broken log");
    case LogType::kBegin:
      break;
    case LogType::kInsertRow: {
      PageRef target = pool_->GetPage(log.pos.page_id);
      txn.CompensateInsertLog(log.pos, lsn);
      target->DeleteImpl(log.pos);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kUpdateRow: {
      PageRef target = pool_->GetPage(log.pos.page_id);
      txn.CompensateUpdateLog(log.pos, lsn, log.undo_data);
      target->UpdateImpl(log.pos, log.undo_data);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kDeleteRow: {
      PageRef target = pool_->GetPage(log.pos.page_id);
      target->InsertImpl(log.pos, log.undo_data);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kCommit:
      tm_->active_transactions_.erase(log.txn_id);
      break;
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      LOG(ERROR) << "Cannot undo a checkpoint log record";
      break;
    case LogType::kSystemAllocPage:
      break;
    case LogType::kSystemDestroyPage: {
      PageRef target = pool_->GetPage(log.destroy_page_id);
      target->PageInit(log.destroy_page_id, log.allocated_page_type);
      break;
    }
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
      LOG(ERROR) << "Cannot undo a compensate log record";
      break;
  }
}

}  // namespace tinylamb