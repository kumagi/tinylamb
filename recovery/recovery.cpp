#include "recovery.hpp"

#include <fcntl.h>
#include <sys/mman.h>

#include <filesystem>
#include <unordered_set>

#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "recovery/log_record.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

namespace {

bool is_page_manipulation(LogType type) {
  switch (type) {
    case LogType::kUnknown:
      throw std::runtime_error("Invalid format log");

    case LogType::kInsertRow:
    case LogType::kUpdateRow:
    case LogType::kDeleteRow:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
    case LogType::kSystemAllocPage:
    case LogType::kSystemDestroyPage:
      return true;

    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      return false;
  }
  return false;
}

}  // namespace

Recovery::Recovery(std::string_view log_path, std::string_view db_path,
                   PagePool *pp)
    : log_name_(log_path), log_data_(nullptr), pool_(pp) {
  RefreshMap();
}

void Recovery::RefreshMap() {
  LOG(INFO) << "reading: " << log_name_;
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);
  int fd = open(log_name_.c_str(), O_RDONLY, 0666);
  if (fd == -1) {
    std::string str_err = strerror(errno);
    throw std::runtime_error("could not open the file: " + str_err);
  }
  log_data_ = static_cast<char *>(
      mmap(nullptr, filesize, PROT_READ, MAP_PRIVATE, fd, 0));
}

void Recovery::StartFrom(size_t offset, TransactionManager *tm) {
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);
  RefreshMap();

  std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, LogRecord>>>
      page_logs;
  std::unordered_set<uint64_t> finished_txn;
  // Analysis phase starts here.
  {
    LogRecord log;
    while (offset < filesize) {
      bool success = LogRecord::ParseLogRecord(&log_data_[offset], &log);
      if (!success) {
        LOG(ERROR) << "Failed to parse log at offset: " << offset;
        break;
      }
      if (is_page_manipulation(log.type)) {
        if (log.type == LogType::kSystemAllocPage) {
          page_logs[log.allocated_page_id].emplace_back(offset, log);
        } else if (log.type == LogType::kSystemDestroyPage) {
          page_logs[log.destroy_page_id].emplace_back(offset, log);
        } else {
          page_logs[log.pos.page_id].emplace_back(offset, log);
        }
      } else if (log.type == LogType::kCommit) {
        finished_txn.insert(log.txn_id);
        LOG(INFO) << "txn: " << log.txn_id << " is finished";
      }
      offset += log.Size();
      LOG(INFO) << "analyzed " << offset << ": " << log;
    }
  }

  std::unordered_map<uint64_t, size_t> log_offsets;
  std::unordered_map<uint64_t, uint64_t> txn_lsn;

  // Redo & Undo phase starts here.
  for (const auto &page_log : page_logs) {
    const uint64_t &page_id = page_log.first;
    // Page redo phase.
    for (const auto &lsn_log : page_log.second) {
      const uint64_t &lsn = lsn_log.first;
      const LogRecord &log = lsn_log.second;
      LOG(INFO) << "redo: " << log;
      LogRedo(lsn, log, tm);
    }

    // Page undo phase.
    for (auto iter = page_log.second.rbegin(); iter != page_log.second.rend();
         iter++) {
      const uint64_t &lsn = iter->first;
      const LogRecord &log = iter->second;
      if (finished_txn.find(log.txn_id) == finished_txn.end()) {
        LOG(INFO) << "undo: " << log;
        LogUndo(lsn, log, tm);
      }
    }
  }
}

bool Recovery::ReadLog(uint64_t lsn, LogRecord *dst) {
  RefreshMap();
  return LogRecord::ParseLogRecord(&log_data_[lsn], dst);
}

void Recovery::LogRedo(uint64_t lsn, const LogRecord &log,
                       TransactionManager *tm) {
  switch (log.type) {
    case LogType::kUnknown:
      assert(!"unknown log type must not be parsed");
    case LogType::kBegin:
      tm->active_transactions_.insert(log.txn_id);
      break;
    case LogType::kInsertRow: {
      PageRef target = pool_->GetPage(log.pos.page_id, nullptr);
      if (target->PageLSN() < lsn) {
        target->InsertImpl(log.pos, log.redo_data);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kUpdateRow: {
      PageRef target = pool_->GetPage(log.pos.page_id, nullptr);
      if (target->PageLSN() < lsn) {
        target->UpdateImpl(log.pos, log.redo_data);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kDeleteRow: {
      PageRef target = pool_->GetPage(log.pos.page_id, nullptr);
      if (target->PageLSN() < lsn) {
        target->DeleteImpl(log.pos);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kCommit:
      tm->active_transactions_.erase(log.txn_id);
      break;
    case LogType::kBeginCheckpoint:
      break;
    case LogType::kEndCheckpoint:
      break;
    case LogType::kSystemAllocPage: {
      bool cache_hit;
      PageRef target = pool_->GetPage(log.allocated_page_id, &cache_hit);
      if (target->PageLSN() < lsn || (!cache_hit && !target->IsValid())) {
        target->PageInit(log.allocated_page_id, log.allocated_page_type);
      }
      break;
    }
    case LogType::kSystemDestroyPage:
      break;
  }
}

void Recovery::LogUndo(uint64_t lsn, const LogRecord &log,
                       TransactionManager *tm) {
  Transaction txn(log.txn_id, tm, tm->lock_manager_, tm->page_manager_,
                  tm->logger_);
  switch (log.type) {
    case LogType::kUnknown:
      LOG(FATAL) << "Unknown type log";
      throw std::runtime_error("broken log");
    case LogType::kBegin:
      break;
    case LogType::kInsertRow: {
      PageRef target = pool_->GetPage(log.pos.page_id, nullptr);
      txn.CompensateInsertLog(log.pos, lsn);
      target->DeleteImpl(log.pos);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kUpdateRow: {
      PageRef target = pool_->GetPage(log.pos.page_id, nullptr);
      txn.CompensateUpdateLog(log.pos, lsn, log.undo_data);
      target->UpdateImpl(log.pos, log.undo_data);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kDeleteRow: {
      PageRef target = pool_->GetPage(log.pos.page_id, nullptr);
      target->InsertImpl(log.pos, log.undo_data);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kCommit:
      tm->active_transactions_.erase(log.txn_id);
      break;
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      LOG(ERROR) << "Cannot undo a checkpoint log record";
      break;
    case LogType::kSystemAllocPage:
      break;
    case LogType::kSystemDestroyPage: {
      PageRef target = pool_->GetPage(log.destroy_page_id, nullptr);
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