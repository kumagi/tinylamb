/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#ifndef TINYLAMB_QUERY_SQL_ORACLE_CHECKS_HPP
#define TINYLAMB_QUERY_SQL_ORACLE_CHECKS_HPP

#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "query/sql_oracle_fuzzer.hpp"
#include "type/row.hpp"

namespace tinylamb {

// Sentinel for nullable mirror fields; INT64_MIN cannot collide with the
// small generated value domain (-3..3).
constexpr int64_t kNull = std::numeric_limits<int64_t>::min();

// INT64 sentinel meaning "no default argument supplied" for LAG/LEAD.
constexpr int64_t kNoDefault = (INT64_MIN / 2);
constexpr int64_t kAnyValue = (INT64_MAX / 2);

// Lexicographically sorted string pool shared between fuzzer and checks.
inline constexpr std::array<const char*, 4> kStrPool = {"", "a", "bb", "zz"};

// Reconstructed or generated table row for ground-truth model evaluation.
struct MirrorRow {
  int64_t u{0};
  int64_t a{0};
  int64_t b{0};
  int64_t flag{0};  // kNull = NULL, 0 = FALSE, 1 = TRUE
  int64_t s{0};     // kNull = NULL, else index into kStrPool
};

// ---------------------------------------------------------------------------
// Engine execution helpers shared across oracle checks and fuzzer driver.
// ---------------------------------------------------------------------------

std::optional<std::vector<Row>> RunRowsMode(Database& db,
                                            TransactionContext& ctx,
                                            const std::string& sql,
                                            bool force_relational,
                                            std::string* error);

std::optional<std::vector<Row>> RunRows(Database& db, TransactionContext& ctx,
                                        const std::string& sql,
                                        std::string* error = nullptr);

bool RunUpdate(Database& db, TransactionContext& ctx, const std::string& sql,
               std::string* error = nullptr);

std::optional<std::string> RunScalar(Database& db, TransactionContext& ctx,
                                     const std::string& sql,
                                     std::string* error = nullptr);

std::vector<std::string> SerializeSorted(const std::vector<Row>& rows);

std::string DumpRows(const std::vector<std::string>& rows);

// ---------------------------------------------------------------------------
// Verification check functions.
// ---------------------------------------------------------------------------

bool CheckTlp(Database& db, TransactionContext& ctx,
              const std::vector<std::string>& tlp, std::string* report,
              bool verbose);

bool CheckTlpAgg(Database& db, TransactionContext& ctx,
                 const std::vector<std::string>& tlp_agg, std::string* report,
                 bool verbose);

bool CheckUnionAll(Database& db, TransactionContext& ctx,
                   const std::vector<std::string>& unionall,
                   std::string* report, bool verbose);

bool CheckSubq(Database& db, TransactionContext& ctx,
               const std::vector<std::string>& subq, std::string* report,
               bool verbose);

// Returns whether at least one recorded NoREC pair executed (true when the
// arm recorded nothing, so it cannot count as skipped).
bool CheckNoRec(Database& db, TransactionContext& ctx, const OracleTrace& t,
                const char* phase, std::string* report, bool verbose);

void AnalyzeCreatedTables(Database& db, TransactionContext& ctx,
                          const std::vector<std::string>& setup);

bool CheckPqs(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose);

bool CheckIdx(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose);

bool CheckXMode(Database& db, TransactionContext& ctx, const OracleTrace& t,
                std::string* report, bool verbose);

bool CheckWindow(Database& db, TransactionContext& ctx, const OracleTrace& t,
                 std::string* report, bool verbose);

bool CheckSkip(Database& db, TransactionContext& ctx, const OracleTrace& t,
               std::string* report, bool verbose);

bool CheckTopTies(Database& db, TransactionContext& ctx, const OracleTrace& t,
                  std::string* report, bool verbose);

bool CheckTroc(Database& db, const OracleTrace& t, std::string* report,
               bool verbose);

bool CheckDqe(Database& db, TransactionContext& ctx, const OracleTrace& t,
              std::string* report, bool verbose);

bool CheckExpected(Database& db, TransactionContext& ctx,
                   const std::vector<std::string>& queries,
                   const std::vector<std::string>& expected, bool ordered,
                   const char* tag, std::string* report, bool verbose);

bool CheckPair(Database& db, TransactionContext& ctx,
               const std::vector<std::string>& cte, const char* tag,
               std::string* report, bool verbose);

bool CheckRecursive(Database& db, TransactionContext& ctx, const OracleTrace& t,
                    std::string* report, bool verbose);

}  // namespace tinylamb

#endif  // TINYLAMB_QUERY_SQL_ORACLE_CHECKS_HPP
