#include "recovery.hpp"

#include <fcntl.h>
#include <sys/mman.h>

#include <filesystem>

#include "page/catalog_page.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_pool.hpp"
#include "page/row_page.hpp"
#include "recovery/log_record.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

Recovery::Recovery(std::string_view log_path, std::string_view db_path,
                   PageManager *pm, TransactionManager *tm)
    : log_name_(log_path),
      log_data_(nullptr),
      pool_(&pm->pool_),
      tm_(tm) {}

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

  // Redo phase starts here.
  LogRecord log;
  size_t log_offset = 0;
  while (!entire_log.empty()) {
    bool success = LogRecord::ParseLogRecord(entire_log, &log);
    if (!success) {
      LOG(ERROR) << "Failed to parse log at offset: " << log_offset;
      break;
    }
    log_offsets.emplace(log.lsn, log_offset);
    entire_log.remove_prefix(log.Size());
    log_offset += log.Size();
    LOG(TRACE) << log << " rest: " << entire_log.size();
    LogRedo(log);
  }

  // Undo phase starts here.
  for (auto& txn_id : tm_->active_transactions_) {
    uint64_t prev_lsn = txn_id
    size_t offset =
  }
}

void Recovery::LogRedo(const LogRecord &log) {
  switch (log.type) {
    case LogType::kUnknown:
      assert(!"unknown log type must not be parsed");
    case LogType::kBegin:
      tm_->active_transactions_.insert(log.txn_id);
      break;
    case LogType::kInsertRow: {
      Page *target = pool_->GetPage(log.pos.page_id);
      target->InsertImpl(log.pos, log.redo_data);
      target->last_lsn = log.lsn;
      pool_->Unpin(target->PageId());
      break;
    }
    case LogType::kUpdateRow: {
      Page *target = pool_->GetPage(log.pos.page_id);
      target->UpdateImpl(log.pos, log.redo_data);
      target->last_lsn = log.lsn;
      pool_->Unpin(target->PageId());
      break;
    }
    case LogType::kDeleteRow: {
      Page *target = pool_->GetPage(log.pos.page_id);
      target->DeleteImpl(log.pos);
      target->last_lsn = log.lsn;
      pool_->Unpin(target->PageId());
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
      Page *target = pool_->GetPage(log.allocated_page_id);
      target->PageInit(log.allocated_page_id, log.allocated_page_type);
      pool_->Unpin(target->PageId());
      break;
    }
    case LogType::kSystemDestroyPage:
      break;
  }
}

}  // namespace tinylamb