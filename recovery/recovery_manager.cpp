#include "recovery_manager.hpp"

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

    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
      return false;

    default:
      return true;
  }
}

void LogRedo(PageRef& target, lsn_t lsn, const LogRecord& log) {
  if (!IsPageManipulation(log.type) || lsn <= target->PageLSN()) return;

  switch (log.type) {
    case LogType::kUnknown:
      assert(!"unknown log type must not be parsed");
    case LogType::kInsertRow:
    case LogType::kCompensateDeleteRow:
      target->InsertImpl(log.redo_data);
      break;
    case LogType::kUpdateRow:
    case LogType::kCompensateUpdateRow:
      target->UpdateImpl(log.slot, log.redo_data);
      break;
    case LogType::kDeleteRow:
    case LogType::kCompensateInsertRow:
      target->DeleteImpl(log.slot);
      break;
    case LogType::kUpdateLeaf:
      target->UpdateImpl(log.key, log.redo_data);
      break;
    case LogType::kDeleteLeaf:
    case LogType::kCompensateInsertLeaf:
      target->DeleteImpl(log.key);
      break;
    case LogType::kDeleteInternal:
    case LogType::kCompensateInsertInternal:
      target->DeleteInternalImpl(log.key);
      break;
    case LogType::kCompensateUpdateLeaf:
      target->UpdateImpl(log.key, log.redo_data);
      break;
    case LogType::kInsertLeaf:
    case LogType::kCompensateDeleteLeaf:
      target->InsertImpl(log.key, log.redo_data);
      break;
    case LogType::kInsertInternal:
    case LogType::kCompensateDeleteInternal:
      target->InsertInternalImpl(log.key, log.redo_page);
      break;
    case LogType::kUpdateInternal:
    case LogType::kCompensateUpdateInternal:
      target->UpdateInternalImpl(log.key, log.redo_page);
      break;
    case LogType::kLowestValue:
      target->SetLowestValueInternalImpl(log.redo_page);
      break;
    case LogType::kSystemAllocPage:
      if (!target->IsValid()) {
        target->PageInit(log.pid, log.allocated_page_type);
      }
      break;
    case LogType::kSystemDestroyPage:
      throw std::runtime_error("not implemented yet");
    default:
      assert(!"must not reach here");
  }
  target->SetPageLSN(lsn);
}

void LogUndo(PageRef& target, lsn_t lsn, const LogRecord& log,
             TransactionManager* tm) {
  switch (log.type) {
    case LogType::kUnknown:
      LOG(FATAL) << "Unknown type log";
      throw std::runtime_error("broken log");
    case LogType::kInsertRow:
      tm->CompensateInsertLog(log.txn_id, log.pid, log.slot);
      target->DeleteImpl(log.slot);
      break;
    case LogType::kUpdateRow:
      tm->CompensateUpdateLog(log.txn_id, log.pid, log.slot, log.undo_data);
      target->UpdateImpl(log.slot, log.undo_data);
      break;
    case LogType::kDeleteRow:
      tm->CompensateDeleteLog(log.txn_id, log.pid, log.slot, log.undo_data);
      target->InsertImpl(log.undo_data);
      break;
    case LogType::kSystemAllocPage:
      LOG(ERROR) << "Redoing alloc is not implemented yet";
      break;
    case LogType::kSystemDestroyPage:
      target->PageInit(log.pid, log.allocated_page_type);
      break;
    case LogType::kInsertLeaf:
      tm->CompensateInsertLog(log.txn_id, log.pid, log.key);
      target->DeleteImpl(log.key);
      break;
    case LogType::kInsertInternal:
      tm->CompensateInsertInternalLog(log.txn_id, log.pid, log.key);
      target->DeleteInternalImpl(log.key);
      break;
    case LogType::kUpdateLeaf:
      tm->CompensateUpdateLog(log.txn_id, log.pid, log.key, log.undo_data);
      target->UpdateImpl(log.key, log.undo_data);
      break;
    case LogType::kUpdateInternal:
      tm->CompensateUpdateInternalLog(log.txn_id, log.pid, log.key,
                                      log.undo_page);
      target->UpdateInternalImpl(log.key, log.undo_page);
      break;
    case LogType::kDeleteLeaf:
      tm->CompensateDeleteLog(log.txn_id, log.pid, log.key, log.undo_data);
      target->InsertImpl(log.key, log.undo_data);
      break;
    case LogType::kDeleteInternal:
      tm->CompensateDeleteInternalLog(log.txn_id, log.pid, log.key,
                                      log.undo_page);
      target->InsertInternalImpl(log.key, log.undo_page);
      break;
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
    case LogType::kEndCheckpoint:
    case LogType::kCompensateInsertRow:
    case LogType::kCompensateUpdateRow:
    case LogType::kCompensateDeleteRow:
    case LogType::kCompensateInsertLeaf:
    case LogType::kCompensateInsertInternal:
    case LogType::kCompensateUpdateLeaf:
    case LogType::kCompensateUpdateInternal:
    case LogType::kCompensateDeleteLeaf:
    case LogType::kCompensateDeleteInternal:
    case LogType::kLowestValue:  // Just ignore it!.
      // Compensating log cannot undo.
      break;
  }
  target->SetPageLSN(lsn);
}

// Precondition: the page is locked by this thread.
void PageReplay(PageRef&& target,
                const std::vector<std::pair<lsn_t, LogRecord>>& logs,
                const std::unordered_set<txn_id_t>& committed_txn,
                TransactionManager* tm) {
  // Redo & Undo a specific page.

  // Redo phase.
  for (const auto& lsn_log : logs) {
    const lsn_t& lsn = lsn_log.first;
    const LogRecord& log = lsn_log.second;
    assert(log.pid == target->PageID());
    if (target->PageLSN() < lsn) {
      LOG(INFO) << "redo: " << log;
      LogRedo(target, lsn, log);
    }
  }

  // Undo phase.
  for (auto iter = logs.rbegin(); iter != logs.rend(); iter++) {
    const lsn_t& lsn = iter->first;
    const LogRecord& undo_log = iter->second;
    const auto it = committed_txn.find(undo_log.txn_id);
    assert(undo_log.pid == target->PageID());
    if (it == committed_txn.end()) {
      LOG(INFO) << "undo: " << undo_log;
      LogUndo(target, lsn, undo_log, tm);
    }
  }

  // Release the page latch.
  LOG(INFO) << "SPR " << target->PageID() << " finished";
  target.PageUnlock();
}

}  // namespace

RecoveryManager::RecoveryManager(std::string_view log_path, PagePool* pp)
    : log_name_(log_path),
      log_file_(log_name_, std::ios::in | std::ios::binary),
      pool_(pp) {}

void RecoveryManager::SinglePageRecovery(PageRef&& page,
                                         TransactionManager* tm) {
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);

  // Collects all logs to redo & undo for each page.
  std::vector<std::pair<lsn_t, LogRecord>> page_logs;
  std::unordered_set<txn_id_t> committed_txn;
  {
    lsn_t offset = 0;
    LogRecord log;
    while (offset < filesize) {
      bool success = ReadLog(offset, &log);
      if (!success) {
        LOG(ERROR) << "Failed to parse log at offset: " << offset;
        break;
      }
      if (IsPageManipulation(log.type) && log.pid == page->PageID()) {
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

void RecoveryManager::RecoverFrom(lsn_t checkpoint_lsn,
                                  TransactionManager* tm) {
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
      bool success = ReadLog(offset, &log);
      if (!success) {
        throw std::runtime_error("Invalid log: " + std::to_string(offset));
      }

      LOG(TRACE) << "analyzing: " << offset << ": " << log
                 << "  to: " << offset + log.Size();
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
        case LogType::kInsertLeaf:
        case LogType::kUpdateLeaf:
        case LogType::kDeleteLeaf:
        case LogType::kInsertInternal:
        case LogType::kUpdateInternal:
        case LogType::kDeleteInternal:
        case LogType::kCompensateInsertRow:
        case LogType::kCompensateUpdateRow:
        case LogType::kCompensateDeleteRow: {
          // Collect the oldest LSN to dirty_page_table.
          UpdateOldestLSN(log.pid, offset);
          break;
        }
        case LogType::kCommit:
          committed_txn.insert(log.txn_id);
          break;
        case LogType::kEndCheckpoint: {
          // Collect the oldest LSN to dirty_page_table.
          for (const auto& dpt : log.dirty_page_table) {
            UpdateOldestLSN(dpt.first, dpt.second);
          }
          for (const auto& at : log.active_transaction_table) {
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
    for (const auto& d : dirty_page_table) {
      redo_start_point = std::min(redo_start_point, d.second);
    }
  }

  // Collect all page references.
  std::unordered_map<page_id_t, PageRef> pages;
  pages.reserve(dirty_page_table.size());

  // Take all dirty page's lock.
  for (const auto& it : dirty_page_table) {
    PageRef&& page = pool_->GetPage(it.first, nullptr);
    if (!page->IsValid()) {
      page->page_lsn = 0;
      page->page_id = it.first;
      LOG(INFO) << "Page " << it.first << " is broken, start SPR.";
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
      bool success = ReadLog(offset, &log);
      if (!success) {
        LOG(ERROR) << "Failed to parse log at offset: " << offset;
        break;
      }
      if (IsPageManipulation(log.type)) {
        const auto it = pages.find(log.pid);
        if (it != pages.end()) {
          page_logs[log.pid].emplace_back(offset, log);
        }
      } else if (log.type == LogType::kCommit) {
        committed_txn.insert(log.txn_id);
      }
      offset += log.Size();
    }
  }

  // Redo & Undo phase starts here.
  // Note that this loop could be parallelized for each page.
  for (const auto& page_log : page_logs) {
    const uint64_t& page_id = page_log.first;
    PageReplay(std::move(pages.find(page_id)->second), page_log.second,
               committed_txn, tm);
  }
}

bool RecoveryManager::ReadLog(lsn_t lsn, LogRecord* dst) const {
  dst->Clear();
  std::istream in(log_file_.rdbuf());
  in.seekg(lsn);
  return LogRecord::ParseLogRecord(in, dst);
}

void RecoveryManager::LogUndoWithPage(lsn_t lsn, const LogRecord& log,
                                      TransactionManager* tm) {
  bool cache_hit;
  if (IsPageManipulation(log.type)) {
    PageRef target = pool_->GetPage(log.pid, &cache_hit);
    LogUndo(target, lsn, log, tm);
  }
}

}  // namespace tinylamb