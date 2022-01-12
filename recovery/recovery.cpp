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

bool IsPageManipulation(LogType type) {
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

void LogRedo(PageRef &target, lsn_t lsn, const LogRecord &log,
             TransactionManager *tm) {
  switch (log.type) {
    case LogType::kUnknown:
      assert(!"unknown log type must not be parsed");
    case LogType::kBegin:
      tm->Begin(log.txn_id, TransactionStatus::kRunning, 0);
      break;
    case LogType::kInsertRow: {
      if (target->PageLSN() < lsn) {
        target->InsertImpl(log.pos, log.redo_data);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kUpdateRow: {
      if (target->PageLSN() < lsn) {
        target->UpdateImpl(log.pos, log.redo_data);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kDeleteRow: {
      if (target->PageLSN() < lsn) {
        target->DeleteImpl(log.pos);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kCommit:
      tm->active_transactions_.erase(log.txn_id);
      break;
    case LogType::kSystemAllocPage: {
      if (target->PageLSN() < lsn || !target->IsValid()) {
        target->PageInit(log.pos.page_id, log.allocated_page_type);
      }
      break;
    }
    case LogType::kSystemDestroyPage: {
      throw std::runtime_error("not implemented yet");
    }
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
    case LogType::kCompensateInsertRow: {
      if (target->PageLSN() < lsn) {
        target->DeleteImpl(log.pos);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kCompensateUpdateRow: {
      if (target->PageLSN() < lsn) {
        target->UpdateImpl(log.pos, log.redo_data);
        target->SetPageLSN(lsn);
      }
      break;
    }
    case LogType::kCompensateDeleteRow: {
      if (target->PageLSN() < lsn) {
        target->InsertImpl(log.pos, log.redo_data);
        target->SetPageLSN(lsn);
      }
      break;
    }
  }
}

void LogUndo(PageRef &target, lsn_t lsn, const LogRecord &log,
             TransactionManager *tm) {
  switch (log.type) {
    case LogType::kUnknown:
      LOG(FATAL) << "Unknown type log";
      throw std::runtime_error("broken log");
    case LogType::kBegin:
      break;
    case LogType::kInsertRow: {
      tm->CompensateInsertLog(log.txn_id, log.pos);
      target->DeleteImpl(log.pos);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kUpdateRow: {
      tm->CompensateUpdateLog(log.txn_id, log.pos, log.undo_data);
      target->UpdateImpl(log.pos, log.undo_data);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kDeleteRow: {
      tm->CompensateDeleteLog(log.txn_id, log.pos, log.undo_data);
      target->InsertImpl(log.pos, log.undo_data);
      target->SetPageLSN(lsn);
      break;
    }
    case LogType::kCommit:
      break;
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      LOG(ERROR) << "Cannot undo a checkpoint log record";
      break;
    case LogType::kSystemAllocPage:
      break;
    case LogType::kSystemDestroyPage: {
      target->PageInit(log.pos.page_id, log.allocated_page_type);
      break;
    }
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
      LOG(ERROR) << "Cannot undo a compensate log record";
      break;
  }
}

// Precondition: the page is locked by this thread.
void PageReplay(
    PageRef &&target, const std::vector<std::pair<page_id_t, LogRecord>> &logs,
    const std::unordered_map<txn_id_t, std::pair<TransactionStatus, lsn_t>>
        &active_transaction_table,
    TransactionManager *tm) {
  for (const auto &it : logs) {
    LOG(ERROR) << it.first << ": " << it.second;
  }
  // Redo & Undo specific page.

  // Redo phase.
  for (const auto &lsn_log : logs) {
    const lsn_t &lsn = lsn_log.first;
    const LogRecord &log = lsn_log.second;
    LOG(INFO) << "redo: " << log;
    LogRedo(target, lsn, log, tm);
  }

  // Undo phase.
  for (auto iter = logs.rbegin(); iter != logs.rend(); iter++) {
    const lsn_t &lsn = iter->first;
    const LogRecord &undo_log = iter->second;
    const auto it = active_transaction_table.find(undo_log.txn_id);
    if (it != active_transaction_table.end() &&
        it->second.first != TransactionStatus::kCommitted) {
      LogUndo(target, lsn, undo_log, tm);
      LOG(INFO) << "undo: " << undo_log;
    }
  }

  // Release page latch.
  target.PageUnlock();
}

}  // namespace

Recovery::Recovery(std::string_view log_path, PagePool *pp)
    : log_name_(log_path), log_data_(nullptr), pool_(pp) {
  RefreshMap();
}

void Recovery::RefreshMap() {
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);
  int fd = open(log_name_.c_str(), O_RDONLY, 0666);
  if (fd == -1) {
    std::string str_err = strerror(errno);
    throw std::runtime_error("could not open the file: " + str_err);
  }
  log_data_ = static_cast<char *>(
      mmap(nullptr, filesize, PROT_READ, MAP_PRIVATE, fd, 0));
}

void Recovery::RecoverFrom(lsn_t checkpoint_lsn, TransactionManager *tm) {
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);
  RefreshMap();

  // Analysis phase starts here.
  std::unordered_map<page_id_t, lsn_t> dirty_page_table;
  std::unordered_map<txn_id_t, std::pair<TransactionStatus, lsn_t>>
      active_transaction_table;
  lsn_t redo_start_point = filesize;
  {
    size_t offset = checkpoint_lsn;
    LogRecord log;
    {
      while (offset < filesize) {
        bool success = LogRecord::ParseLogRecord(&log_data_[offset], &log);
        if (!success) {
          LOG(ERROR) << "Failed to parse log at offset: " << offset;
          break;
        }
        LOG(TRACE) << "analyzing: " << offset << ": " << log;
        switch (log.type) {
          case LogType::kUnknown:
            throw std::runtime_error("invalid log");
          case LogType::kBegin:
            active_transaction_table.emplace(
                log.txn_id, std::make_pair(TransactionStatus::kRunning, 0));
            break;
          case LogType::kSystemAllocPage: {
            const auto it = dirty_page_table.find(log.txn_id);
            if (it == dirty_page_table.end()) {
              dirty_page_table.emplace(log.pos.page_id, offset);
            } else {
              dirty_page_table[log.pos.page_id] = std::min(offset, it->second);
            }
            break;
          }
          case LogType::kSystemDestroyPage: {
            const auto it = dirty_page_table.find(log.txn_id);
            if (it == dirty_page_table.end()) {
              dirty_page_table.emplace(log.pos.page_id, offset);
            } else {
              dirty_page_table[log.pos.page_id] = std::min(offset, it->second);
            }
            break;
          }
          case LogType::kInsertRow:
          case LogType::kUpdateRow:
          case LogType::kDeleteRow:
          case LogType::kCompensateInsertRow:
          case LogType::kCompensateUpdateRow:
          case LogType::kCompensateDeleteRow: {
            const auto it = dirty_page_table.find(log.txn_id);
            if (it == dirty_page_table.end()) {
              dirty_page_table.emplace(log.pos.page_id, offset);
            } else {
              dirty_page_table[log.pos.page_id] = std::min(offset, it->second);
            }
            break;
          }
          case LogType::kCommit:
            active_transaction_table[log.txn_id] =
                std::make_pair(TransactionStatus::kCommitted, offset);
            break;
          case LogType::kEndCheckpoint: {
            for (const auto &dpt : log.dirty_page_table) {
              const auto it = dirty_page_table.find(log.txn_id);
              if (it == dirty_page_table.end()) {
                dirty_page_table.emplace(dpt.first, dpt.second);
              } else {
                dirty_page_table[it->first] = std::min(dpt.second, it->second);
              }
            }
            for (const auto &at : log.active_transaction_table) {
              active_transaction_table.emplace(
                  at.txn_id, std::make_pair(at.status, at.last_lsn));
            }
            break;
          }
          default:
            break;
        }
        offset += log.Size();
      }
    }

    // Take minimum LSN from the dirty page table.
    for (const auto &d : dirty_page_table) {
      redo_start_point = std::min(redo_start_point, d.second);
    }
  }

  // Collect logs to redo.
  std::unordered_map<page_id_t, std::vector<std::pair<lsn_t, LogRecord>>>
      page_logs;
  page_logs.reserve(dirty_page_table.size());

  std::unordered_map<page_id_t, PageRef> pages;
  {
    // Hold all dirty page's lock.
    for (const auto &it : dirty_page_table) {
      pages.emplace(it.first, pool_->GetPage(it.first, nullptr));
      pool_->PageLock(it.first);
      LOG(TRACE) << "page: " << it.first << " is dirty!";
    }
  }

  // Now, other user transactions can start concurrently.
  {
    lsn_t offset = redo_start_point;
    LogRecord log;
    while (offset < filesize) {
      bool success = LogRecord::ParseLogRecord(&log_data_[offset], &log);
      if (!success) {
        LOG(ERROR) << "Failed to parse log at offset: " << offset;
        break;
      }
      if (IsPageManipulation(log.type)) {
        page_logs[log.pos.page_id].emplace_back(offset, log);
      } else if (log.type == LogType::kCommit) {
        auto it = active_transaction_table.find(log.txn_id);
        if (it != active_transaction_table.end()) {
          it->second.first = TransactionStatus::kCommitted;
        }
      }
      offset += log.Size();
    }

    // Redo & Undo phase starts here.
    for (const auto &page_log : page_logs) {
      const uint64_t &page_id = page_log.first;
      if (pages.find(page_id) == pages.end()) {
      }
      PageReplay(std::move(pages.find(page_id)->second), page_log.second,
                 active_transaction_table, tm);
    }
  }
}

bool Recovery::ReadLog(lsn_t lsn, LogRecord *dst) {
  RefreshMap();
  return LogRecord::ParseLogRecord(&log_data_[lsn], dst);
}

void Recovery::LogUndoWithPage(lsn_t lsn, const LogRecord &log,
                               TransactionManager *tm) {
  bool cache_hit;
  if (IsPageManipulation(log.type)) {
    PageRef target = pool_->GetPage(log.pos.page_id, &cache_hit);
    LogUndo(target, lsn, log, tm);
  }
}

}  // namespace tinylamb