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
    : log_name_(log_path), log_data_(nullptr), pool_(pp) {}

void Recovery::StartFrom(size_t offset, TransactionManager *tm) {
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

  std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, LogRecord>>>
      page_logs;
  std::unordered_set<uint64_t> finished_txn;
  // Analysis phase starts here.
  {
    LogRecord log;
    while (!entire_log.empty()) {
      bool success = LogRecord::ParseLogRecord(entire_log, &log);
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
      entire_log.remove_prefix(log.Size());
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

void Recovery::SinglePageRecover(uint64_t page_id, Page *target,
                                 TransactionManager *tm) {
  std::uintmax_t filesize = std::filesystem::file_size(log_name_);
  size_t offset = 0;
  int fd = open(log_name_.c_str(), O_RDONLY, 0666);
  if (fd == -1) {
    std::string str_err = strerror(errno);
    throw std::runtime_error("could not open the file: " + str_err);
  }
  log_data_ = static_cast<char *>(
      mmap(nullptr, filesize, PROT_READ, MAP_PRIVATE, fd, 0));
  std::string_view entire_log(log_data_, filesize);
  entire_log.remove_prefix(offset);

  // Collect the logs for the page.
  std::vector<size_t> page_logs;
  LogRecord log;
  size_t log_offset = 0;
  while (!entire_log.empty()) {
    bool success = LogRecord::ParseLogRecord(entire_log, &log);
    if (!success) {
      LOG(ERROR) << "Failed to parse log at offset: " << log_offset;
      break;
    }
    if (log.type == LogType::kBegin || log.type == LogType::kCommit) {
      page_logs.push_back(log_offset);
      continue;
    }
    if ((log.type == LogType::kInsertRow || log.type == LogType::kUpdateRow ||
         log.type == LogType::kDeleteRow ||
         log.type == LogType::kCompensateInsertRow ||
         log.type == LogType::kCompensateUpdateRow ||
         log.type == LogType::kCompensateDeleteRow) &&
        log.pos.page_id == page_id) {
      page_logs.push_back(log_offset);
    }
    entire_log.remove_prefix(log.Size());
    log_offset += log.Size();
  }

  std::unordered_map<uint64_t, uint64_t> last_lsn;
  // Redo all logs for the page.
  for (const auto &lsn : page_logs) {
    bool success = LogRecord::ParseLogRecord(&log_data_[lsn], &log);
    assert(success);
    if (log.type == LogType::kCommit) {
      last_lsn.erase(log.txn_id);
    } else {
      last_lsn[log.txn_id] = lsn;
      LogRedo(log_offset, log, tm);
    }

    log_offset += log.Size();
  }
  LOG(TRACE) << "redo finished: " << page_logs.size();

  // Undo phase starts here.
  for (auto &txn : last_lsn) {
    uint64_t txn_id = txn.first;
    uint64_t prev_lsn = txn.second;
    while (prev_lsn != 0) {
      size_t prev_offset = prev_lsn;
      std::string_view undo_log_data(log_data_ + prev_offset, filesize);
      if (!LogRecord::ParseLogRecord(undo_log_data, &log)) {
        LOG(ERROR) << "Failed to parse log at offset on undo: " << prev_offset;
        break;
      }
      LogUndo(prev_lsn, log, tm);
      prev_lsn = log.prev_lsn;
    }
  }
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