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
    : log_name_(log_path), db_name_(db_path), pool_(&pm->pool_), tm_(tm) {}

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
  LogRecord log;
  while (!entire_log.empty()) {
    bool success = ParseLogRecord(entire_log, &log);
    if (!success) break;
    LOG(TRACE) << log;
    entire_log.remove_prefix(log.Size());
    LogRedo(log);
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
      target->InsertImpl(log.redo_data);
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
    case LogType::kDeleteRow:
      break;
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

bool Recovery::ParseLogRecord(std::string_view src, tinylamb::LogRecord *dst) {
  size_t offset = 0;
  if (src.length() < sizeof(dst->length) + sizeof(dst->type) +
                         sizeof(dst->lsn) + sizeof(dst->prev_lsn) +
                         sizeof(dst->txn_id)) {
    LOG(DEBUG) << "too small data " << src.length();
    return false;
  }
  memcpy(&dst->length, src.data(), sizeof(dst->length));
  src.remove_prefix(sizeof(dst->length));
  memcpy(&dst->type, src.data(), sizeof(dst->type));
  src.remove_prefix(sizeof(dst->type));
  memcpy(&dst->lsn, src.data(), sizeof(dst->lsn));
  src.remove_prefix(sizeof(dst->lsn));
  memcpy(&dst->prev_lsn, src.data(), sizeof(dst->prev_lsn));
  src.remove_prefix(sizeof(dst->prev_lsn));
  memcpy(&dst->txn_id, src.data(), sizeof(dst->txn_id));
  src.remove_prefix(sizeof(dst->txn_id));
  switch (dst->type) {
    case LogType::kUnknown:
      return false;
    case LogType::kBegin:
    case LogType::kCommit:
    case LogType::kBeginCheckpoint:
      return true;
    case LogType::kInsertRow: {
      src.remove_prefix(dst->pos.Parse(src.data()));
      uint32_t redo_size;
      memcpy(&redo_size, src.data(), sizeof(redo_size));
      dst->redo_data = std::string_view(src.data(), redo_size);
      src.remove_prefix(redo_size);
      return true;
    }
    case LogType::kUpdateRow: {
      src.remove_prefix(dst->pos.Parse(src.data()));
      uint32_t redo_size;
      memcpy(&redo_size, src.data(), sizeof(redo_size));
      dst->redo_data = std::string_view(src.data(), redo_size);
      src.remove_prefix(redo_size);

      uint32_t undo_size;
      memcpy(&undo_size, src.data(), sizeof(undo_size));
      dst->undo_data = std::string_view(src.data(), undo_size);
      src.remove_prefix(undo_size);
      return true;
    }
    case LogType::kDeleteRow: {
      uint32_t undo_size;
      memcpy(&undo_size, src.data(), sizeof(undo_size));
      dst->undo_data = std::string_view(src.data(), undo_size);
      src.remove_prefix(undo_size);
      return true;
    }

    case LogType::kEndCheckpoint: {
      uint64_t dpt_size;
      memcpy(&dpt_size, src.data(), sizeof(dpt_size));
      src.remove_prefix(sizeof(dpt_size));
      for (size_t i = 0; i < dpt_size; ++i) {
        uint64_t dirty_page;
        memcpy(&dirty_page, src.data(), sizeof(dirty_page));
        src.remove_prefix(sizeof(dirty_page));
        dst->dirty_page_table.insert(dirty_page);
      }

      uint64_t tt_size;
      memcpy(&tt_size, src.data(), sizeof(tt_size));
      src.remove_prefix(sizeof(tt_size));
      for (size_t i = 0; i < tt_size; ++i) {
        uint64_t txn_id;
        memcpy(&txn_id, src.data(), sizeof(txn_id));
        src.remove_prefix(sizeof(txn_id));
        dst->transaction_table.insert(txn_id);
      }
      return true;
    }
    case LogType::kSystemAllocPage:
      memcpy(&dst->allocated_page_id, src.data(),
             sizeof(dst->allocated_page_id));
      src.remove_prefix(sizeof(dst->allocated_page_id));
      memcpy(&dst->allocated_page_type, src.data(),
             sizeof(dst->allocated_page_type));
      src.remove_prefix(sizeof(dst->allocated_page_type));
      return true;
    case LogType::kSystemDestroyPage:
      memcpy(&dst->destroy_page_id, src.data(), sizeof(dst->destroy_page_id));
      src.remove_prefix(sizeof(dst->destroy_page_id));
      return true;
  }
  return false;
}

}  // namespace tinylamb