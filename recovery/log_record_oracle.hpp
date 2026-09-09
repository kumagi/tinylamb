/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_LOG_RECORD_ORACLE_HPP
#define TINYLAMB_LOG_RECORD_ORACLE_HPP

#include <random>
#include <string>

#include "recovery/log_record.hpp"

namespace tinylamb {

// Seeded random LogRecord generator + serdes-equivalence oracle for the
// ARIES WAL (recovery layer PBT entry point).
//
// The byte-driven `log_record_fuzzer` feeds arbitrary bytes to the decoder;
// this oracle complements it from the other side by generating *valid*
// records of every factory family (row/leaf/branch insert/update/delete,
// fence/foster/lowest, alloc/destroy, checkpoints, transaction markers) and
// verifying the properties the WAL format promises (docs/wal_format.md):
//
//   1. semantic roundtrip: decode(Serialize(record)) == record;
//   2. byte stability: re-serializing the decoded record is byte-identical;
//   3. Size() consistency: Size() equals the serialized byte count;
//   4. truncation robustness: decoding any strict prefix of the serialized
//      bytes either fails cleanly (exception / failed stream) or yields a
//      record -- it must never crash or hang.
//
// Any non-empty return is a logic bug. ShrinkLogRecord reduces a failing
// record while the mismatch is preserved. The generator is deterministic in
// the RNG stream: the same seed always yields the same record, so a failure
// replays from the seed alone. This header lives in the recovery layer, so
// it only depends on common/page/recovery; the byte-level fuzzer driver
// stays in the *_fuzzer targets above the layer DAG.

struct LogRecordGenConfig {
  // Upper bound on generated key/payload string lengths.
  size_t max_payload = 64;
};

struct GeneratedLogRecord {
  LogRecord record;
};

// Deterministic in the RNG stream: the same seed always yields the same
// record. Only uses the RNG (no map iteration, no I/O).
GeneratedLogRecord GenerateLogRecord(std::mt19937& rng,
                                     const LogRecordGenConfig& config = {});

// Full oracle for one generated record. Returns "" when all four properties
// hold, otherwise a diagnostic.
std::string CheckLogRecordEquivalence(const GeneratedLogRecord& generated);

// Bounded delta-shrinker: shrinks payload strings and zeroes numeric fields
// while the mismatch (as reported by CheckLogRecordEquivalence) is
// preserved. Always terminates; returns the smallest mismatch-preserving
// record found (or the input when nothing shrinks).
GeneratedLogRecord ShrinkLogRecord(const GeneratedLogRecord& generated);

}  // namespace tinylamb

#endif  // TINYLAMB_LOG_RECORD_ORACLE_HPP
