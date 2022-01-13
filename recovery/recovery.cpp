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

void LogRedo(PageRef &target, lsn_t lsn, const LogRecord &log) {
  switch (log.type) {
    case LogType::kUnknown:
      assert(!"unknown log type must not be parsed");
    case LogType::kBegin:
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
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
      break;
    case LogType::kSystemAllocPage:
      LOG(ERROR) << "Redoing alloc is not implemented yet";
      break;
    case LogType::kSystemDestroyPage: {
      target->PageInit(log.pos.page_id, log.allocated_page_type);
      break;
    }
  }
}

// Precondition: the page is locked by this thread.
void PageReplay(PageRef &&target,
                const std::vector<std::pair<lsn_t, LogRecord>> &logs,
                const std::unordered_set<txn_id_t> &committed_txn,
                TransactionManager *tm) {
  // Redo & Undo a specific page.

  // Redo phase.
  for (const auto &lsn_log : logs) {
    const lsn_t &lsn = lsn_log.first;
    const LogRecord &log = lsn_log.second;
    if (target->PageLSN() < lsn) {
      assert(log.pos.page_id == target->PageId());
      LOG(INFO) << "redo: " << log;
      LogRedo(target, lsn, log);
    }
  }

  // Undo phase.
  for (auto iter = logs.rbegin(); iter != logs.rend(); iter++) {
    const lsn_t &lsn = iter->first;
    const LogRecord &undo_log = iter->second;
    const auto it = committed_txn.find(undo_log.txn_id);
    assert(undo_log.pos.page_id == target->PageId());
    if (it == committed_txn.end()) {
      LOG(INFO) << "undo: " << undo_log;
      LogUndo(target, lsn, undo_log, tm);
    }
  }

  // Release the page latch.
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

void Recovery::SinglePageRecovery(PageRef &&page, TransactionManager *tm) {
  RefreshMap();
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);

  // Collects all logs to redo & undo for each page.
  std::vector<std::pair<lsn_t, LogRecord>> page_logs;
  std::unordered_set<txn_id_t> committed_txn;
  {
    lsn_t offset = 0;
    LogRecord log;
    while (offset < filesize) {
      bool success = LogRecord::ParseLogRecord(&log_data_[offset], &log);
      if (!success) {
        LOG(ERROR) << "Failed to parse log at offset: " << offset;
        break;
      }
      if (IsPageManipulation(log.type) && log.pos.page_id == page->PageId()) {
        page_logs.emplace_back(offset, log);
      } else if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      }
      offset += log.Size();
    }
  }

  // Redo & Undo phase starts here.
  // Note that this loop could be parallelized for each page.
  PageReplay(std::move(page), page_logs, committed_txn, tm);
}

void Recovery::RecoverFrom(lsn_t checkpoint_lsn, TransactionManager *tm) {
  RefreshMap();
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);

  // Analysis phase starts here.
  std::unordered_map<page_id_t, lsn_t> dirty_page_table;

  auto UpdateOldestLSN = [&](page_id_t pid, lsn_t maybe_oldest) {
    const auto it = dirty_page_table.find(pid);
    if (it == dirty_page_table.end()) {
      dirty_page_table.emplace(pid, maybe_oldest);
    } else {
      dirty_page_table[it->first] = std::min(maybe_oldest, it->second);
    }
  };

  std::unordered_set<txn_id_t> committed_txn;
  lsn_t redo_start_point = filesize;
  {
    size_t offset = checkpoint_lsn;
    LogRecord log;
    while (offset < filesize) {
      bool success = LogRecord::ParseLogRecord(&log_data_[offset], &log);
      if (!success) {
        throw std::runtime_error("Invalid log: " + std::to_string(offset));
      }

      LOG(TRACE) << "analyzing: " << offset << ": " << log;
      switch (log.type) {
        case LogType::kUnknown:
          throw std::runtime_error("Invalid log: " + std::to_string(offset));
        case LogType::kBegin:
          break;
        case LogType::kSystemAllocPage:
        case LogType::kSystemDestroyPage:
        case LogType::kInsertRow:
        case LogType::kUpdateRow:
        case LogType::kDeleteRow:
        case LogType::kCompensateInsertRow:
        case LogType::kCompensateUpdateRow:
        case LogType::kCompensateDeleteRow: {
          // Collect the oldest LSN to dirty_page_table.
          UpdateOldestLSN(log.pos.page_id, offset);
          break;
        }
        case LogType::kCommit:
          committed_txn.insert(log.txn_id);
          break;
        case LogType::kEndCheckpoint: {
          // Collect the oldest LSN to dirty_page_table.
          for (const auto &dpt : log.dirty_page_table) {
            UpdateOldestLSN(dpt.first, dpt.second);
          }
          for (const auto &at : log.active_transaction_table) {
            if (at.status == TransactionStatus::kCommitted) {
              committed_txn.insert(at.txn_id);
            }
          }
          break;
        }
        default:
          break;
      }
      offset += log.Size();
    }

    // Take minimum LSN from the dirty page table.
    for (const auto &d : dirty_page_table) {
      redo_start_point = std::min(redo_start_point, d.second);
    }
  }

  // Collect all page references.
  std::unordered_map<page_id_t, PageRef> pages;
  pages.reserve(dirty_page_table.size());

  // Take all dirty page's lock.
  for (const auto &it : dirty_page_table) {
    PageRef &&page = pool_->GetPage(it.first, nullptr);
    pool_->PageLock(it.first);
    if (!page->IsValid()) {
      page->page_lsn = 0;
      page->page_id = it.first;
      LOG(INFO) << "Page " << it.first
                << " is broken, trying Single Page Recovery";
      SinglePageRecovery(std::move(page), tm);
    } else {
      pages.emplace(it.first, std::move(page));
    }
  }
  // Now other user transactions can start concurrently.

  // Collects all logs to redo & undo for each page.
  std::unordered_map<page_id_t, std::vector<std::pair<lsn_t, LogRecord>>>
      page_logs;
  page_logs.reserve(pages.size());
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
        const auto it = pages.find(log.pos.page_id);
        if (it != pages.end()) {
          page_logs[log.pos.page_id].emplace_back(offset, log);
        }
      } else if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      }
      offset += log.Size();
    }
  }

  // Redo & Undo phase starts here.
  // Note that this loop could be parallelized for each page.
  for (const auto &page_log : page_logs) {
    const uint64_t &page_id = page_log.first;
    PageReplay(std::move(pages.find(page_id)->second), page_log.second,
               committed_txn, tm);
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