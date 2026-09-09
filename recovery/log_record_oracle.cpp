/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "recovery/log_record_oracle.hpp"

#include <exception>
#include <sstream>
#include <utility>

#include "common/decoder.hpp"

namespace tinylamb {
namespace {

std::string Payload(std::mt19937& rng, size_t max_payload) {
  // Tiny alphabet with structural edge cases: empty, 1 byte, and
  // multi-byte payloads. Embedded NULs are avoided: several WAL payloads
  // are handled as C-compatible ranges downstream.
  static constexpr std::string_view kAlphabet = "ab";
  const auto length =
      static_cast<size_t>(rng() % static_cast<uint32_t>(max_payload + 1));
  std::string out;
  out.reserve(length);
  for (size_t i = 0; i < length; ++i) {
    out.push_back(kAlphabet[rng() % 2]);
  }
  return out;
}

uint64_t Small(std::mt19937& rng, uint64_t bound = 8) {
  return rng() % (bound + 1);
}

PageType RandomPageType(std::mt19937& rng) {
  switch (rng() % 4) {
    case 0:
      return PageType::kRowPage;
    case 1:
      return PageType::kLeafPage;
    case 2:
      return PageType::kBranchPage;
    default:
      return PageType::kPaxPage;
  }
}

std::string Describe(const LogRecord& record) {
  std::ostringstream out;
  out << record;
  return out.str();
}

LogRecord Decode(const std::string& bytes) {
  std::istringstream stream(bytes, std::istringstream::binary);
  Decoder decoder(stream);
  LogRecord record;
  decoder >> record;
  if (stream.fail()) {
    throw std::runtime_error("decoder stream failed");
  }
  return record;
}

}  // namespace

GeneratedLogRecord GenerateLogRecord(std::mt19937& rng,
                                     const LogRecordGenConfig& config) {
  const auto prev = static_cast<lsn_t>(Small(rng));
  const auto txn = static_cast<txn_id_t>(Small(rng));
  const auto pid = static_cast<page_id_t>(Small(rng, 9) + 1);
  const auto slot = static_cast<slot_t>(Small(rng));
  const std::string key = Payload(rng, 16);
  const std::string redo = Payload(rng, config.max_payload);
  const std::string undo = Payload(rng, config.max_payload);
  const auto redo_page = static_cast<page_id_t>(Small(rng, 9) + 1);
  const auto undo_page = static_cast<page_id_t>(Small(rng, 9) + 1);
  GeneratedLogRecord generated;
  // One case per factory family so every LogType on the wire is reachable.
  switch (rng() % 30) {
    case 0:
      generated.record = LogRecord(prev, txn, LogType::kBegin);
      break;
    case 1:
      generated.record = LogRecord(prev, txn, LogType::kCommit);
      break;
    case 2:
      generated.record =
          LogRecord::InsertingLogRecord(prev, txn, pid, slot, redo);
      break;
    case 3:
      generated.record =
          LogRecord::InsertingLeafLogRecord(prev, txn, pid, key, redo);
      break;
    case 4:
      generated.record =
          LogRecord::InsertingBranchLogRecord(prev, txn, pid, key, redo_page);
      break;
    case 5:
      generated.record =
          LogRecord::UpdatingLogRecord(prev, txn, pid, slot, redo, undo);
      break;
    case 6:
      generated.record =
          LogRecord::UpdatingLeafLogRecord(prev, txn, pid, key, redo, undo);
      break;
    case 7:
      generated.record = LogRecord::UpdatingBranchLogRecord(
          prev, txn, pid, key, redo_page, undo_page);
      break;
    case 8:
      generated.record =
          LogRecord::DeletingLogRecord(prev, txn, pid, slot, undo);
      break;
    case 9:
      generated.record =
          LogRecord::DeletingLeafLogRecord(prev, txn, pid, key, undo);
      break;
    case 10:
      generated.record =
          LogRecord::DeletingBranchLogRecord(prev, txn, pid, key, undo_page);
      break;
    case 11:
      generated.record = LogRecord::CompensatingInsertLogRecord(txn, pid, slot);
      break;
    case 12:
      generated.record = LogRecord::CompensatingInsertLogRecord(txn, pid, key);
      break;
    case 13:
      generated.record =
          LogRecord::CompensatingInsertBranchLogRecord(txn, pid, key);
      break;
    case 14:
      generated.record =
          LogRecord::CompensatingUpdateLogRecord(txn, pid, slot, redo);
      break;
    case 15:
      generated.record =
          LogRecord::CompensatingUpdateLeafLogRecord(txn, pid, key, redo);
      break;
    case 16:
      generated.record = LogRecord::CompensatingUpdateBranchLogRecord(
          txn, pid, key, redo_page);
      break;
    case 17:
      generated.record =
          LogRecord::CompensatingDeleteLogRecord(txn, pid, slot, redo);
      break;
    case 18:
      generated.record =
          LogRecord::CompensatingDeleteLeafLogRecord(txn, pid, key, redo);
      break;
    case 19:
      generated.record = LogRecord::CompensatingDeleteBranchLogRecord(
          txn, pid, key, redo_page);
      break;
    case 20:
      generated.record = LogRecord::SetLowFenceLogRecord(
          prev, txn, pid, IndexKey(redo), IndexKey(undo));
      break;
    case 21:
      generated.record = LogRecord::SetHighFenceLogRecord(
          prev, txn, pid, IndexKey(redo), IndexKey(undo));
      break;
    case 22:
      generated.record = LogRecord::CompensateSetLowFenceLogRecord(
          prev, txn, pid, IndexKey(redo));
      break;
    case 23:
      generated.record = LogRecord::CompensateSetHighFenceLogRecord(
          prev, txn, pid, IndexKey(redo));
      break;
    case 24:
      generated.record = LogRecord::SetFosterLogRecord(
          prev, txn, pid, FosterPair(redo, redo_page),
          FosterPair(undo, undo_page));
      break;
    case 25:
      generated.record = LogRecord::CompensateSetFosterLogRecord(
          prev, txn, pid, FosterPair(redo, redo_page));
      break;
    case 26:
      generated.record =
          LogRecord::SetLowestLogRecord(prev, txn, pid, redo_page, undo_page);
      break;
    case 27:
      generated.record =
          LogRecord::AllocatePageLogRecord(prev, txn, pid, RandomPageType(rng));
      break;
    case 28:
      generated.record = LogRecord::DestroyPageLogRecord(
          prev, txn, pid, RandomPageType(rng), undo);
      break;
    default: {
      std::vector<std::pair<page_id_t, lsn_t>> dpt;
      const auto dpt_size = static_cast<size_t>(rng() % 3);
      dpt.reserve(dpt_size);
      for (size_t i = 0; i < dpt_size; ++i) {
        dpt.emplace_back(static_cast<page_id_t>(Small(rng, 9) + 1), Small(rng));
      }
      std::vector<CheckpointManager::ActiveTransactionEntry> att;
      const auto att_size = static_cast<size_t>(rng() % 3);
      att.reserve(att_size);
      for (size_t i = 0; i < att_size; ++i) {
        att.emplace_back(Small(rng), TransactionStatus::kRunning,
                         static_cast<lsn_t>(Small(rng)));
      }
      generated.record = rng() % 2 == 0
                             ? LogRecord::BeginCheckpointLogRecord()
                             : LogRecord::EndCheckpointLogRecord(dpt, att);
      break;
    }
  }
  return generated;
}

std::string CheckLogRecordEquivalence(const GeneratedLogRecord& generated) {
  const std::string where = Describe(generated.record);
  std::string serialized;
  try {
    serialized = generated.record.Serialize();
  } catch (const std::exception& e) {
    return "serialize threw for " + where + ": " + e.what();
  }
  // Property 3: Size() matches the on-disk byte count.
  if (generated.record.Size() != serialized.size()) {
    return "Size() mismatch for " + where +
           ": Size()=" + std::to_string(generated.record.Size()) +
           " serialized=" + std::to_string(serialized.size());
  }
  // Property 1: semantic roundtrip.
  LogRecord decoded;
  try {
    decoded = Decode(serialized);
  } catch (const std::exception& e) {
    return "decode of serialized bytes threw for " + where + ": " + e.what();
  }
  if (!(decoded == generated.record)) {
    return "roundtrip mismatch for " + where + " got " + Describe(decoded);
  }
  // Property 2: byte stability.
  std::string reserialized;
  try {
    reserialized = decoded.Serialize();
  } catch (const std::exception& e) {
    return "reserialize threw for " + where + ": " + e.what();
  }
  if (reserialized != serialized) {
    return "byte instability for " + where;
  }
  // Property 4: every strict prefix decodes cleanly-or-throws, never crashes.
  // Payloads are bounded by the generator, so the full prefix walk is cheap.
  for (size_t length = 0; length < serialized.size(); ++length) {
    try {
      static_cast<void>(Decode(serialized.substr(0, length)));
    } catch (const std::exception&) {
      continue;
    }
  }
  return "";
}

GeneratedLogRecord ShrinkLogRecord(const GeneratedLogRecord& generated) {
  if (CheckLogRecordEquivalence(generated).empty()) {
    return generated;
  }
  GeneratedLogRecord current = generated;
  // Shrink string payloads by halving while the mismatch is preserved.
  auto shrink_field = [&](std::string LogRecord::* field) {
    while ((current.record.*field).size() > 1) {
      GeneratedLogRecord candidate = current;
      (candidate.record.*field).resize((current.record.*field).size() / 2);
      if (!CheckLogRecordEquivalence(candidate).empty()) {
        current = std::move(candidate);
      } else {
        break;
      }
    }
    if (!(current.record.*field).empty()) {
      GeneratedLogRecord candidate = current;
      (candidate.record.*field).clear();
      if (!CheckLogRecordEquivalence(candidate).empty()) {
        current = std::move(candidate);
      }
    }
  };
  shrink_field(&LogRecord::key);
  shrink_field(&LogRecord::redo_data);
  shrink_field(&LogRecord::undo_data);
  // Zero numeric fields while the mismatch is preserved.
  auto zero_numeric = [&](auto LogRecord::* field) {
    if ((current.record.*field) != 0) {
      GeneratedLogRecord candidate = current;
      (candidate.record.*field) = 0;
      if (!CheckLogRecordEquivalence(candidate).empty()) {
        current = std::move(candidate);
      }
    }
  };
  zero_numeric(&LogRecord::prev_lsn);
  zero_numeric(&LogRecord::txn_id);
  return current;
}

}  // namespace tinylamb
