/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "query/sql_ooc_fuzzer.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "common/digest.hpp"
#include "common/memory_budget.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "page/page_manager.hpp"
#include "query/fuzz_scoped_db.hpp"
#include "query/sql_engine.hpp"
#include "query/sql_ooc_query_gen.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

std::vector<std::string> SplitTokens(std::string_view line) {
  std::vector<std::string> out;
  size_t p = 0;
  while (p < line.size()) {
    while (p < line.size() && line[p] == ' ') {
      ++p;
    }
    const size_t start = p;
    while (p < line.size() && line[p] != ' ') {
      ++p;
    }
    if (p > start) {
      out.emplace_back(line.substr(start, p - start));
    }
  }
  return out;
}

bool ParseI64(std::string_view s, int64_t* out) {
  if (s.empty()) {
    return false;
  }
  char* end = nullptr;
  const std::string tmp(s);
  errno = 0;
  const long long v = std::strtoll(tmp.c_str(), &end, 10);
  if (errno != 0 || end == nullptr || end == tmp.c_str() || *end != '\0') {
    return false;
  }
  *out = static_cast<int64_t>(v);
  return true;
}

uint64_t EnvU64(const char* name, uint64_t fallback) {
  const char* v = std::getenv(name);
  if (v == nullptr || v[0] == '\0') {
    return fallback;
  }
  char* end = nullptr;
  errno = 0;
  const auto parsed = std::strtoull(v, &end, 10);
  if (end == v || *end != '\0' || v[0] == '-' || errno == ERANGE) {
    return fallback;
  }
  return parsed;
}

std::string I(int64_t x) { return std::to_string(x); }
std::string Nul() { return "NULL"; }

std::string Shorten(std::string_view s, size_t n = 80) {
  if (s.size() <= n) {
    return std::string(s);
  }
  return std::string(s.substr(0, n)) + "...(" + std::to_string(s.size()) + "B)";
}

// Tag column domain: index 0 is NULL.
// NOLINTNEXTLINE(modernize-avoid-c-arrays)  // runtime-sized buffer, std::array not applicable
constexpr std::string_view kTags[] = {"", "a", "bb", "ccc", "dddd"};
constexpr int kTagCount = 5;

int64_t PosMod(int64_t a, int64_t m) {
  const int64_t r = a % m;
  return r < 0 ? r + m : r;
}

// ---------------------------------------------------------------------------
// Mirror
// ---------------------------------------------------------------------------

struct MRow {
  int64_t g{0};
  std::optional<int64_t> v;
  int tag{0};  // index into kTags; 0 = NULL
  std::string pad;

  bool operator==(const MRow&) const = default;
};

struct MTable {
  std::string name;
  int64_t groups{1};
  int64_t pad_min{0};
  int64_t pad_span{1};
  // Data-shape knobs (ooca-v2.md G4 skew): v is drawn from [0, v_mod), every
  // vnull_mod-th row has a NULL v, and pad_mode 1 drops the key prefix so
  // rows of equal length share one identical (wide) pad value.
  int64_t v_mod{1000};
  int64_t vnull_mod{11};
  int64_t pad_mode{0};
  std::map<int64_t, MRow> rows;
  int64_t next_k{1};
  bool unique_k{false};
  std::set<std::string> index_kinds;
  bool compacted{false};
  size_t pad_bytes{0};
  size_t pad_hi{0};  // longest pad ever stored (upper bound for budgets)

  bool operator==(const MTable&) const = default;

  void Put(int64_t k, MRow r) {
    pad_hi = std::max(pad_hi, r.pad.size());
    auto it = rows.find(k);
    if (it != rows.end()) {
      pad_bytes -= it->second.pad.size();
      pad_bytes += r.pad.size();
      it->second = std::move(r);
    } else {
      pad_bytes += r.pad.size();
      rows.emplace(k, std::move(r));
    }
    next_k = std::max(next_k, k + 1);
  }
  void Erase(int64_t k) {
    auto it = rows.find(k);
    if (it != rows.end()) {
      pad_bytes -= it->second.pad.size();
      rows.erase(it);
    }
  }
};

MTable* FindTable(std::vector<MTable>& v, std::string_view name) {
  for (MTable& t : v) {
    if (t.name == name) {
      return &t;
    }
  }
  return nullptr;
}

const MTable* FindTable(const std::vector<MTable>& v, std::string_view name) {
  for (const MTable& t : v) {
    if (t.name == name) {
      return &t;
    }
  }
  return nullptr;
}

// Generated row for key x (bulk / VALUES inserts).  SQL twin: InsertExprs.
MRow GenRow(const MTable& t, int64_t x, int64_t salt) {
  MRow r;
  r.g = PosMod((x * 7) + salt, t.groups);
  if (PosMod(x + salt, t.vnull_mod) == 0) {
    r.v = std::nullopt;
  } else {
    r.v = PosMod((x * 13) + salt, t.v_mod);
  }
  r.tag = static_cast<int>(PosMod((x * 3) + salt, kTagCount));
  const int64_t len = t.pad_min + PosMod((x * 31) + salt, t.pad_span);
  if (t.pad_mode == 1) {
    r.pad = std::string(static_cast<size_t>(len),
                        static_cast<char>('a' + PosMod(salt, 26)));
  } else {
    r.pad = std::to_string(x) + ":" +
            std::string(static_cast<size_t>(len),
                        static_cast<char>('a' + PosMod(x + salt, 26)));
  }
  return r;
}

std::string TagSqlCase(const std::string& expr) {
  return "CASE MOD(" + expr +
         ", 5) WHEN 0 THEN NULL WHEN 1 THEN 'a' WHEN 2 THEN 'bb' WHEN 3 THEN "
         "'ccc' ELSE 'dddd' END";
}

std::string InsertSelectList(const MTable& t, int64_t salt) {
  const std::string s = I(salt);
  const std::string len =
      I(t.pad_min) + " + MOD(x * 31 + " + s + ", " + I(t.pad_span) + ")";
  const std::string pad =
      t.pad_mode == 1
          ? "REPEAT(CHR(" + I(97 + PosMod(salt, 26)) + "), " + len + ")"
          : "CONCAT(CAST(x AS STRING), ':', REPEAT(CHR(97 + MOD(x + " + s +
                ", 26)), " + len + "))";
  return "x, MOD(x * 7 + " + s + ", " + I(t.groups) + "), CASE WHEN MOD(x + " +
         s + ", " + I(t.vnull_mod) + ") = 0 THEN NULL ELSE MOD(x * 13 + " + s +
         ", " + I(t.v_mod) + ") END, " + TagSqlCase("x * 3 + " + s) + ", " +
         pad;
}

std::string SqlLiteral(std::string_view s) {
  std::string out = "'";
  for (char c : s) {
    if (c == '\'') {
      out += "\\'";
    } else {
      out.push_back(c);
    }
  }
  out += "'";
  return out;
}

// ---------------------------------------------------------------------------
// WHERE / SET specs shared by UPDATE and DELETE.
//   where: kind a b c d      set: kind p q  (twice for UPDATE)
// ---------------------------------------------------------------------------

struct WhereSpec {
  std::string kind{"all"};
  int64_t a{0}, b{0}, c{1}, d{0};

  [[nodiscard]] bool Eval(int64_t k, const MRow& r) const {
    if (kind == "range") {
      return k >= a && k <= b;
    }
    if (kind == "rmod") {
      return k >= a && k <= b && c > 0 && PosMod(k, c) == d;
    }
    if (kind == "grp") {
      return r.g == a;
    }
    if (kind == "vgt") {
      return r.v.has_value() && *r.v > a;
    }
    if (kind == "vnull") {
      return !r.v.has_value();
    }
    if (kind == "padgt") {
      return std::cmp_greater(r.pad.size(), a);
    }
    if (kind == "tag") {
      return r.tag != 0 && r.tag == a;
    }
    return true;  // all
  }
  [[nodiscard]] std::string Sql() const {
    if (kind == "range") {
      return " WHERE k BETWEEN " + I(a) + " AND " + I(b);
    }
    if (kind == "rmod") {
      return " WHERE k BETWEEN " + I(a) + " AND " + I(b) + " AND MOD(k, " +
             I(c) + ") = " + I(d);
    }
    if (kind == "grp") {
      return " WHERE g = " + I(a);
    }
    if (kind == "vgt") {
      return " WHERE v > " + I(a);
    }
    if (kind == "vnull") {
      return " WHERE v IS NULL";
    }
    if (kind == "padgt") {
      return " WHERE LENGTH(pad) > " + I(a);
    }
    if (kind == "tag") {
      return " WHERE t = " +
             SqlLiteral(kTags[std::clamp<int64_t>(a, 1, kTagCount - 1)]);
    }
    return "";
  }
  [[nodiscard]] bool Valid() const {
    static const std::set<std::string> kKinds = {
        "range", "rmod", "grp", "vgt", "vnull", "padgt", "tag", "all"};
    return kKinds.contains(kind) && (kind != "rmod" || c > 0) &&
           (kind != "tag" || (a >= 1 && a < kTagCount));
  }
  [[nodiscard]] std::string Text() const {
    return kind + " " + I(a) + " " + I(b) + " " + I(c) + " " + I(d);
  }
};

struct SetSpec {
  std::string kind{"none"};
  int64_t p{0}, q{0};

  void Apply(int64_t k, MRow* r, const MTable& t) const {
    if (kind == "vadd") {
      if (r->v.has_value()) {
        *r->v += p;
      }
    } else if (kind == "vset") {
      r->v = p;
    } else if (kind == "vnull") {
      r->v = std::nullopt;
    } else if (kind == "pad") {
      const int64_t len = p + PosMod(k, 7);
      r->pad = std::to_string(k) + "#" +
               std::string(static_cast<size_t>(len),
                           static_cast<char>('a' + PosMod(k + q, 26)));
    } else if (kind == "padapp") {
      r->pad += "Z";
    } else if (kind == "gmod") {
      r->g = PosMod(k + p, t.groups);
    } else if (kind == "tag") {
      r->tag = static_cast<int>(p);
    } else if (kind == "tagnull") {
      r->tag = 0;
    }
  }
  [[nodiscard]] std::string Sql(const MTable& t) const {
    if (kind == "vadd") {
      return "v = v + " + I(p);
    }
    if (kind == "vset") {
      return "v = " + I(p);
    }
    if (kind == "vnull") {
      return "v = NULL";
    }
    if (kind == "pad") {
      return "pad = CONCAT(CAST(k AS STRING), '#', REPEAT(CHR(97 + MOD(k + " +
             I(q) + ", 26)), " + I(p) + " + MOD(k, 7)))";
    }
    if (kind == "padapp") {
      return "pad = CONCAT(pad, 'Z')";
    }
    if (kind == "gmod") {
      return "g = MOD(k + " + I(p) + ", " + I(t.groups) + ")";
    }
    if (kind == "tag") {
      return "t = " + SqlLiteral(kTags[std::clamp<int64_t>(p, 1, 4)]);
    }
    if (kind == "tagnull") {
      return "t = NULL";
    }
    return "";
  }
  // Target column ("" for none): one UPDATE may assign a column only once.
  [[nodiscard]] std::string Column() const {
    if (kind == "vadd" || kind == "vset" || kind == "vnull") {
      return "v";
    }
    if (kind == "pad" || kind == "padapp") {
      return "pad";
    }
    if (kind == "gmod") {
      return "g";
    }
    if (kind == "tag" || kind == "tagnull") {
      return "t";
    }
    return "";
  }
  [[nodiscard]] bool Valid() const {
    static const std::set<std::string> kKinds = {"vadd", "vset",    "vnull",
                                                 "pad",  "padapp",  "gmod",
                                                 "tag",  "tagnull", "none"};
    return kKinds.contains(kind) && (kind != "tag" || (p >= 1 && p <= 4)) &&
           (kind != "pad" || (p >= 0 && p <= 200000));
  }
  [[nodiscard]] std::string Text() const {
    return kind + " " + I(p) + " " + I(q);
  }
};

bool ParseWhere(const std::vector<std::string>& tok, size_t at, WhereSpec* w) {
  if (tok.size() < at + 5) {
    return false;
  }
  w->kind = tok[at];
  return ParseI64(tok[at + 1], &w->a) && ParseI64(tok[at + 2], &w->b) &&
         ParseI64(tok[at + 3], &w->c) && ParseI64(tok[at + 4], &w->d) &&
         w->Valid();
}

bool ParseSet(const std::vector<std::string>& tok, size_t at, SetSpec* s) {
  if (tok.size() < at + 3) {
    return false;
  }
  s->kind = tok[at];
  return ParseI64(tok[at + 1], &s->p) && ParseI64(tok[at + 2], &s->q) &&
         s->Valid();
}

// ---------------------------------------------------------------------------
// Value rendering
// ---------------------------------------------------------------------------

std::string RenderValue(const Value& v) {
  switch (v.type) {
    case ValueType::kNull:
      return Nul();
    case ValueType::kInt64:
      return I(v.AsInt64());
    case ValueType::kVarChar:
      return std::string(v.AsStringView());
    case ValueType::kDouble: {
      std::ostringstream o;
      o.precision(17);
      o << v.AsDouble();
      return o.str();
    }
    default:
      return v.AsString();
  }
}

std::string RenderRow(const Row& row) {
  std::string out;
  for (size_t i = 0; i < row.Count(); ++i) {
    if (i > 0) {
      out += '|';
    }
    out += RenderValue(row[i]);
  }
  return out;
}

std::string Join(const std::vector<std::string>& cells) {
  std::string out;
  for (size_t i = 0; i < cells.size(); ++i) {
    if (i > 0) {
      out += '|';
    }
    out += cells[i];
  }
  return out;
}

std::string OptI(const std::optional<int64_t>& v) {
  return v.has_value() ? I(*v) : Nul();
}
std::string TagS(int tag) { return tag == 0 ? Nul() : std::string(kTags[tag]); }

std::string DiffReport(const std::vector<std::string>& expected,
                       const std::vector<std::string>& actual) {
  std::string out = "  expected " + std::to_string(expected.size()) +
                    " rows, engine returned " + std::to_string(actual.size()) +
                    "\n";
  const size_t n = std::max(expected.size(), actual.size());
  int shown = 0;
  for (size_t i = 0; i < n && shown < 6; ++i) {
    const std::string e = i < expected.size() ? expected[i] : "<none>";
    const std::string a = i < actual.size() ? actual[i] : "<none>";
    if (e != a) {
      out += "  row " + std::to_string(i) + ": expected [" + Shorten(e, 120) +
             "] got [" + Shorten(a, 120) + "]\n";
      ++shown;
    }
  }
  return out;
}

// Coarse feature tag of a query, so findings bucket by the operators they
// involve rather than by their literal text.
std::string SqlFeatures(std::string_view sql) {
  static const std::vector<std::pair<std::string_view, std::string_view>>
      kMarks = {{"RECURSIVE", "recursive"},
                {" FULL JOIN ", "fulljoin"},
                {" LEFT JOIN ", "leftjoin"},
                {" RIGHT JOIN ", "rightjoin"},
                {" CROSS JOIN ", "crossjoin"},
                {" JOIN ", "join"},
                {" NOT IN (", "notin"},
                {" IN (SELECT", "insubquery"},
                {"NOT EXISTS", "notexists"},
                {" EXISTS", "exists"},
                {" OVER (", "window"},
                {"QUALIFY", "qualify"},
                {"UNION", "union"},
                {"INTERSECT", "intersect"},
                {"EXCEPT", "except"},
                {"GROUP BY", "groupby"},
                {"DISTINCT", "distinct"},
                {"STRING_AGG", "stringagg"},
                {"ARRAY_AGG", "arrayagg"},
                {" LIMIT ", "limit"},
                {"ORDER BY", "orderby"}};
  std::string out;
  for (const auto& [mark, name] : kMarks) {
    if (sql.find(mark) != std::string_view::npos) {
      if (name == "join" && out.find("join") != std::string::npos) {
        continue;  // already tagged with its join type
      }
      out += (out.empty() ? "" : "+") + std::string(name);
    }
  }
  return out.empty() ? "scan" : out;
}

// TINYLAMB_OOC_FUZZ_IGNORE: ';'-separated substrings of failure signatures
// (known findings a campaign should step over).
bool IgnoredSignature(std::string_view report);

// Restores every process-wide knob the session touches, so one session (or
// a failed one) never leaks its memory profile into the next.
class KnobGuard {
 public:
  KnobGuard() {
    ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
    storage_ = b.StoragePoolLimit();
    columnar_ = b.ColumnarPoolLimit();
    op_ = b.OpMemoryOverrideBytes();
    spill_ = b.AlwaysSpillEnabled();
    temp_ = b.TempLimitBytes();
  }
  KnobGuard(const KnobGuard&) = delete;
  KnobGuard& operator=(const KnobGuard&) = delete;
  KnobGuard(KnobGuard&&) = delete;
  KnobGuard& operator=(KnobGuard&&) = delete;
  ~KnobGuard() {
    ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
    b.SetStoragePoolLimitForTest(storage_);
    b.SetColumnarPoolLimitForTest(columnar_);
    b.SetTestOpMemoryBytes(op_);
    b.SetTestAlwaysSpill(spill_);
    b.SetTestTempLimitBytes(temp_);
    b.SetTestMemoryFailCountdown(0);
    b.SetTestSpillIoFailCountdown(0);
  }

 private:
  size_t storage_{0};
  size_t columnar_{0};
  size_t op_{0};
  bool spill_{false};
  size_t temp_{0};
};

void ApplyPoolKnob(size_t bytes) {
  ProcessMemoryBudget::Global().SetStoragePoolLimitForTest(bytes);
}

// ---------------------------------------------------------------------------
// Session interpreter
// ---------------------------------------------------------------------------

struct SqlOutcome {
  bool ok{false};
  Status status{Status::kSuccess};
  std::string why;
  std::vector<std::string> rows;
};

bool TolerableUnderPressure(StatusCode code) {
  return code == StatusCode::kMemoryLimitExceeded ||
         code == StatusCode::kNoSpace || code == StatusCode::kTooBigData;
}

bool TolerableUnderInjection(StatusCode code) {
  return TolerableUnderPressure(code) || code == StatusCode::kIOError;
}

class OocSession {
 public:
  OocSession(const OocProfile& profile, bool verbose, OocStats* stats)
      : profile_(profile), verbose_(verbose), stats_(stats) {}

  OocSession(const OocSession&) = delete;
  OocSession& operator=(const OocSession&) = delete;
  OocSession(OocSession&&) = delete;
  OocSession& operator=(OocSession&&) = delete;

  ~OocSession() {
    StopEvictThread();
    snap_.reset();
    ctx_.reset();
    if (sdb_ != nullptr) {
      if (*sdb_) {
        sdb_.reset();
      } else {
        sdb_->Cleanup();
        sdb_.reset();
      }
    }
  }

  // Applies the profile and opens the database.  "" on success.
  std::string Open() {
    ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
    ApplyPoolKnob(profile_.pool_bytes);
    b.SetColumnarPoolLimitForTest(profile_.columnar_bytes);
    b.SetTestOpMemoryBytes(profile_.op_memory_bytes);
    b.SetTestAlwaysSpill(profile_.always_spill);
    b.SetTestTempLimitBytes(profile_.temp_limit_bytes);
    force_relational_ = profile_.force_relational;
    sdb_ = std::make_unique<ScopedDb>("sql_ooc_fuzz");
    if (!*sdb_) {
      return "skip";  // disk quota guard: not a finding
    }
    db_ = sdb_->get();
    ctx_.emplace(db_->BeginContext());
    ResetFdBaseline();
    if (EnvU64("TINYLAMB_OOC_FUZZ_EVICT_THREAD", 0) != 0) {
      StartEvictThread();
    }
    return "";
  }

  const std::vector<MTable>& Tables() const { return work_; }
  const std::vector<MTable>& Committed() const { return committed_; }
  bool SnapOpen() const { return snap_.has_value(); }
  const std::vector<MTable>& SnapTables() const { return snap_tables_; }
  [[nodiscard]] size_t TotalPadBytes() const {
    size_t total = 0;
    for (const MTable& t : work_) {
      total += t.pad_bytes;
    }
    return total;
  }

  // Executes one op; "" = held.
  std::string Apply(const std::string& op) {
    if (verbose_) {
      std::cerr << "[ooc][op] " << Shorten(op, 200) << "\n";
    }
    const std::vector<std::string> tok = SplitTokens(op);
    if (tok.empty()) {
      return "";
    }
    const std::string& cmd = tok[0];
    std::string r;
    if (cmd == "inject") {
      r = ArmInjection(tok);
    } else if (cmd == "knob") {
      r = ApplyKnob(tok);
    } else if (cmd == "sweep") {
      r = OpSweep(op);
    } else {
      r = RunOp(op);
    }
    if (lenient_ && r.starts_with("malformed")) {
      // A minimizer removed the op this one depended on: skip it.
      return "";
    }
    if (!r.empty() && ReadOnlyOp(op) && Ignored(r)) {
      // A known finding on a statement that changes nothing: count it and
      // keep going so one shallow bug does not hide the deeper ones.
      Bump(&OocStats::ignored_findings);
      if (verbose_) {
        std::cerr << "[ooc] ignored known finding: " << Shorten(r, 200) << "\n";
      }
      r.clear();
    }
    if (r.empty()) {
      r = LeakCheck(op);
    }
    if (r.empty()) {
      SamplePressure();
    }
    return r;
  }

  void SetLenient(bool lenient) { lenient_ = lenient; }

  static bool Ignored(std::string_view report) {
    return IgnoredSignature(report);
  }

  static bool ReadOnlyOp(std::string_view op) {
    if (op.starts_with("sweep ")) {
      const size_t sep = op.find(" :: ");
      if (sep == std::string_view::npos) {
        return false;
      }
      op.remove_prefix(sep + 4);
    }
    return op.starts_with("q ") || op.starts_with("check ") ||
           op.starts_with("snap check ");
  }
  void NoteShape(const std::string& shape) {
    if (stats_ != nullptr) {
      ++stats_->shapes[shape];
    }
  }

 private:
  void Bump(int OocStats::* field, int by = 1) {
    if (stats_ != nullptr) {
      (stats_->*field) += by;
    }
  }

  // Runs one op with the pending injection (if any) applied to it.
  std::string RunOp(const std::string& op) {
    const std::vector<std::string> tok = SplitTokens(op);
    if (tok.empty()) {
      return "";
    }
    std::string r;
    if (tok[0] == "q") {
      // Arms the injection itself, around the pressure variant only.
      r = OpQuery(op);
    } else {
      ArmPending();
      r = Dispatch(tok[0], tok, op);
      DisarmAndDetect();
    }
    if (read_failed_) {
      read_failed_ = false;
      // Conservative mode: treat a failed read like a failed write and roll
      // the transaction back.  Without it the session keeps using the
      // transaction, which is how "a failed query leaves the transaction
      // unable to write or commit" findings surface; with it a campaign
      // steps over that family and reaches what lies behind.
      if (r.empty() &&
          EnvU64("TINYLAMB_OOC_FUZZ_ABORT_AFTER_FAILED_READ", 0) != 0) {
        r = OpAbort("post-failed-read abort");
      }
    }
    return r;
  }

  // Remembers that a statement of the writer transaction failed (tolerated).
  void NoteFailedRead(const TransactionContext& ctx) {
    if (ctx_.has_value() && &ctx == &*ctx_) {
      read_failed_ = true;
    }
  }

  void ArmPending() {
    armed_mem_ = pending_mem_;
    armed_io_ = pending_io_;
    pending_mem_ = 0;
    pending_io_ = 0;
    last_fired_ = false;
    injecting_ = armed_mem_ > 0 || armed_io_ > 0;
    if (injecting_) {
      ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
      b.SetTestMemoryFailCountdown(armed_mem_);
      b.SetTestSpillIoFailCountdown(armed_io_);
      Bump(&OocStats::injections);
    }
  }

  // Disarms the injection and records whether it fired.  The budget exposes
  // no getter for the countdown, but a countdown that is still pending
  // reaches zero within `armed` further consume calls, while one that
  // already fired never reports true again.
  void DisarmAndDetect() {
    if (!injecting_) {
      return;
    }
    ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
    bool mem_pending = false;
    for (uint64_t i = 0; i < armed_mem_ && !mem_pending; ++i) {
      mem_pending = b.ConsumeInjectedMemoryFailure();
    }
    bool io_pending = false;
    for (uint64_t i = 0; i < armed_io_ && !io_pending; ++i) {
      io_pending = b.ConsumeInjectedSpillIoFailure();
    }
    last_fired_ =
        (armed_mem_ > 0 && !mem_pending) || (armed_io_ > 0 && !io_pending);
    b.SetTestMemoryFailCountdown(0);
    b.SetTestSpillIoFailCountdown(0);
    armed_mem_ = 0;
    armed_io_ = 0;
    injecting_ = false;
  }

  std::string Dispatch(const std::string& cmd,
                       const std::vector<std::string>& tok,
                       const std::string& op) {
    if (cmd == "create") {
      return OpCreate(tok);
    }
    if (cmd == "drop") {
      return OpDrop(tok);
    }
    if (cmd == "index") {
      return OpIndex(tok);
    }
    if (cmd == "ins_bulk" || cmd == "ins_values") {
      return OpInsertGenerated(tok, cmd == "ins_values");
    }
    if (cmd == "ins_copy") {
      return OpInsertCopy(tok);
    }
    if (cmd == "ins_dup") {
      return OpInsertDup(tok);
    }
    if (cmd == "upd") {
      return OpUpdate(tok);
    }
    if (cmd == "del") {
      return OpDelete(tok);
    }
    if (cmd == "commit") {
      return OpCommit();
    }
    if (cmd == "abort") {
      return OpAbort("abort");
    }
    if (cmd == "crash") {
      return OpCrash();
    }
    if (cmd == "reopen") {
      return OpReopen();
    }
    if (cmd == "checkpoint") {
      return OpCheckpoint();
    }
    if (cmd == "evict") {
      return OpEvict(tok);
    }
    if (cmd == "compact") {
      return OpCompact(tok);
    }
    if (cmd == "analyze") {
      return OpAnalyze(tok);
    }
    if (cmd == "snap") {
      return OpSnap(tok);
    }
    if (cmd == "check") {
      return OpCheck(tok, /*snapshot=*/false);
    }
    if (cmd == "selftest") {
      // selftest <table> <rows>: reports a (fake) finding once the mirror of
      // <table> holds at least <rows> rows.  Unit-test hook for the trace
      // minimizer; the generator never emits it.
      int64_t rows = 0;
      const MTable* t = tok.size() == 3 ? FindTable(work_, tok[1]) : nullptr;
      if (t == nullptr || !ParseI64(tok[2], &rows)) {
        return "malformed selftest op\n";
      }
      return std::cmp_greater_equal(t->rows.size(), rows)
                 ? "selftest threshold reached: " + t->name + " holds " +
                       I(static_cast<int64_t>(t->rows.size())) + " rows\n"
                 : "";
    }
    return "malformed op: " + op + "\n";
  }

  // ---- knobs / injection -------------------------------------------------

  std::string ArmInjection(const std::vector<std::string>& tok) {
    int64_t n = 0;
    if (tok.size() != 3 || !ParseI64(tok[2], &n) || n <= 0) {
      return "malformed inject op\n";
    }
    if (tok[1] == "mem") {
      pending_mem_ = static_cast<uint64_t>(n);
    } else if (tok[1] == "io") {
      pending_io_ = static_cast<uint64_t>(n);
    } else {
      return "malformed inject op\n";
    }
    return "";
  }

  std::string ApplyKnob(const std::vector<std::string>& tok) {
    int64_t n = 0;
    if (tok.size() != 3 || !ParseI64(tok[2], &n) || n < 0) {
      return "malformed knob op\n";
    }
    ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
    const auto bytes = static_cast<size_t>(n);
    if (tok[1] == "pool") {
      ApplyPoolKnob(bytes);
    } else if (tok[1] == "columnar") {
      b.SetColumnarPoolLimitForTest(bytes);
    } else if (tok[1] == "opmem") {
      b.SetTestOpMemoryBytes(bytes);
    } else if (tok[1] == "spill") {
      b.SetTestAlwaysSpill(n != 0);
    } else if (tok[1] == "temp") {
      b.SetTestTempLimitBytes(bytes);
    } else if (tok[1] == "route") {
      force_relational_ = n != 0;
    } else {
      return "malformed knob op\n";
    }
    return "";
  }

  [[nodiscard]] static bool TempCapped() {
    return ProcessMemoryBudget::Global().TempLimitBytes() > 0;
  }

  // ---- SQL execution -----------------------------------------------------

  // `take` >= 0 stops reading after that many rows and drops the cursor
  // mid-stream (the early-close path of every spilling operator).
  SqlOutcome RunSql(TransactionContext& ctx, const std::string& sql,
                    int64_t take = -1) {
    SqlOutcome out;
    Bump(&OocStats::statements);
    SqlEngine engine(*db_);
    engine.SetForceRelationalFallback(force_relational_);
    StatusOr<QueryResult> result = engine.Execute(ctx, sql);
    if (!result.HasValue()) {
      out.status = result.GetStatus();
      out.why = "prepare: " + ToString(out.status) + " " + engine.LastError();
      return out;
    }
    QueryResult qr = result.MoveValue();
    Row row;
    while ((take < 0 || std::cmp_less(out.rows.size(), take)) &&
           qr.Next(&row)) {
      out.rows.push_back(RenderRow(row));
    }
    out.status = qr.GetStatus();
    if (out.status != Status::kSuccess) {
      out.why = "stream: " + ToString(out.status);
      out.rows.clear();
      return out;
    }
    out.ok = true;
    return out;
  }

  // Classifies a failed statement.  Returns "" when the failure is an
  // allowed consequence of the active pressure (the caller then rolls the
  // transaction back), otherwise a finding.
  std::string ClassifyFailure(const std::string& what, const std::string& sql,
                              const SqlOutcome& o, bool big_rows) {
    const StatusCode code = o.status.GetCode();
    bool tolerated = false;
    // NOLINTNEXTLINE(bugprone-branch-clone) distinct intent in each branch
    if (injecting_ && o.why.find("injected") != std::string::npos) {
      // The engine reports the injected fault itself (whatever the code).
      tolerated = true;
    } else if (injecting_ || TempCapped()) {
      tolerated = TolerableUnderInjection(code);
    } else if (code == StatusCode::kMemoryLimitExceeded) {
      tolerated = true;
    } else if (big_rows && (code == StatusCode::kNoSpace ||
                            code == StatusCode::kTooBigData)) {
      tolerated = true;
    }
    if (tolerated) {
      Bump(&OocStats::tolerated_failures);
      if (verbose_) {
        std::cerr << "[ooc] tolerated failure (" << o.why << ")\n";
      }
      return "";
    }
    std::string report = what + " failed unexpectedly: " + Shorten(sql, 300) +
                         "\n  " + o.why + "\n";
    if (IgnoredSignature(report)) {
      // Known finding: the statement failed, so rolling back (what the
      // caller does for a tolerated failure) keeps the mirror exact.
      Bump(&OocStats::ignored_findings);
      return "";
    }
    return report;
  }

  // Rolls the writer back after a tolerated statement failure and verifies
  // the rollback restored the committed image.
  std::string RollbackAfterFailure() {
    // The rollback and its verification run without the injection, so a
    // late-firing countdown cannot excuse a wrong post-rollback image.
    DisarmAndDetect();
    return OpAbort("post-failure abort");
  }

  // Runs a DML statement, checks the affected-row count, then applies the
  // mirror mutation.  `mutate` is only called on success.
  template <typename Fn>
  std::string RunDml(const std::string& what, const std::string& sql,
                     int64_t expected_rows, bool big_rows, Fn&& mutate) {
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    SqlOutcome o = RunSql(*ctx_, sql);
    if (!o.ok) {
      std::string finding = ClassifyFailure(what, sql, o, big_rows);
      if (!finding.empty()) {
        return finding;
      }
      return RollbackAfterFailure();
    }
    int64_t affected = -1;
    if (!o.rows.empty()) {
      const std::vector<std::string> cells = SplitCells(o.rows[0]);
      if (cells.size() >= 2) {
        ParseI64(cells[1], &affected);
      }
    }
    if (affected != expected_rows) {
      return what + " affected " + I(affected) + " rows, mirror expects " +
             I(expected_rows) + ": " + Shorten(sql, 300) + "\n";
    }
    mutate();
    return "";
  }

  static std::vector<std::string> SplitCells(const std::string& row) {
    std::vector<std::string> out;
    size_t p = 0;
    while (true) {
      const size_t bar = row.find('|', p);
      out.push_back(row.substr(
          p, bar == std::string::npos ? std::string::npos : bar - p));
      if (bar == std::string::npos) {
        break;
      }
      p = bar + 1;
    }
    return out;
  }

  // ---- DDL ---------------------------------------------------------------

  std::string OpCreate(const std::vector<std::string>& tok) {
    int64_t groups = 0;
    int64_t pmin = 0;
    int64_t pspan = 0;
    // create <name> <groups> <pad_min> <pad_span> [<v_mod> <vnull_mod>
    // <pad_mode>]
    int64_t v_mod = 1000;
    int64_t vnull_mod = 11;
    int64_t pad_mode = 0;
    if ((tok.size() != 5 && tok.size() != 8) || !ParseI64(tok[2], &groups) ||
        !ParseI64(tok[3], &pmin) || !ParseI64(tok[4], &pspan) || groups < 1 ||
        pmin < 0 || pspan < 1 || FindTable(work_, tok[1]) != nullptr) {
      return "malformed create op\n";
    }
    if (tok.size() == 8 &&
        (!ParseI64(tok[5], &v_mod) || !ParseI64(tok[6], &vnull_mod) ||
         !ParseI64(tok[7], &pad_mode) || v_mod < 1 || vnull_mod < 1 ||
         pad_mode < 0 || pad_mode > 1)) {
      return "malformed create op\n";
    }
    const std::string sql =
        "CREATE TABLE " + tok[1] +
        " (k INT64, g INT64, v INT64, t VARCHAR(8), pad VARCHAR(65535));";
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    SqlOutcome o = RunSql(*ctx_, sql);
    if (!o.ok) {
      std::string finding = ClassifyFailure("CREATE TABLE", sql, o, false);
      return finding.empty() ? RollbackAfterFailure() : finding;
    }
    MTable t;
    t.name = tok[1];
    t.groups = groups;
    t.pad_min = pmin;
    t.pad_span = pspan;
    t.v_mod = v_mod;
    t.vnull_mod = vnull_mod;
    t.pad_mode = pad_mode;
    work_.push_back(std::move(t));
    return "";
  }

  std::string OpDrop(const std::vector<std::string>& tok) {
    if (tok.size() != 2 || FindTable(work_, tok[1]) == nullptr) {
      return "malformed drop op\n";
    }
    const std::string sql = "DROP TABLE " + tok[1] + ";";
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    SqlOutcome o = RunSql(*ctx_, sql);
    if (!o.ok) {
      std::string finding = ClassifyFailure("DROP TABLE", sql, o, false);
      return finding.empty() ? RollbackAfterFailure() : finding;
    }
    std::erase_if(work_, [&](const MTable& t) { return t.name == tok[1]; });
    return "";
  }

  std::string OpIndex(const std::vector<std::string>& tok) {
    MTable* t = tok.size() == 4 ? FindTable(work_, tok[1]) : nullptr;
    if (t == nullptr) {
      return "malformed index op\n";
    }
    const std::string& kind = tok[3];
    std::string spec;
    if (kind == "uk") {
      spec = "CREATE UNIQUE INDEX " + tok[2] + " ON " + t->name + " (k)";
    } else if (kind == "g") {
      spec = "CREATE INDEX " + tok[2] + " ON " + t->name + " (g)";
    } else if (kind == "tk") {
      spec = "CREATE INDEX " + tok[2] + " ON " + t->name + " (t, k)";
    } else if (kind == "pad") {
      spec = "CREATE INDEX " + tok[2] + " ON " + t->name + " (pad)";
    } else if (kind == "gv") {
      spec = "CREATE INDEX " + tok[2] + " ON " + t->name + " (g, v)";
    } else {
      return "malformed index op\n";
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    SqlOutcome o = RunSql(*ctx_, spec + ";");
    if (!o.ok) {
      // Wide pad keys may exceed the index entry limit.
      std::string finding =
          ClassifyFailure("CREATE INDEX", spec, o, kind == "pad");
      return finding.empty() ? RollbackAfterFailure() : finding;
    }
    Bump(&OocStats::indexes);
    t->index_kinds.insert(kind);
    if (kind == "uk") {
      t->unique_k = true;
    }
    return "";
  }

  // ---- DML ---------------------------------------------------------------

  std::string OpInsertGenerated(const std::vector<std::string>& tok,
                                bool values) {
    int64_t lo = 0;
    int64_t hi = 0;
    int64_t salt = 0;
    MTable* t = tok.size() == 5 ? FindTable(work_, tok[1]) : nullptr;
    if (t == nullptr || !ParseI64(tok[2], &lo) || !ParseI64(tok[3], &hi) ||
        !ParseI64(tok[4], &salt) || lo < 1 || hi < lo || salt < 0 ||
        (values && hi - lo >= 64)) {
      return "malformed insert op\n";
    }
    for (int64_t x = lo; x <= hi; ++x) {
      if (t->rows.contains(x)) {
        return "malformed insert op (key " + I(x) + " present)\n";
      }
    }
    std::string sql;
    if (values) {
      sql = "INSERT INTO " + t->name + " VALUES ";
      for (int64_t x = lo; x <= hi; ++x) {
        const MRow r = GenRow(*t, x, salt);
        sql += (x == lo ? "(" : ", (") + I(x) + ", " + I(r.g) + ", " +
               (r.v.has_value() ? I(*r.v) : "NULL") + ", " +
               (r.tag == 0 ? "NULL" : SqlLiteral(kTags[r.tag])) + ", " +
               SqlLiteral(r.pad) + ")";
      }
      sql += ";";
    } else {
      sql = "INSERT INTO " + t->name + " SELECT " + InsertSelectList(*t, salt) +
            " FROM UNNEST(GENERATE_ARRAY(" + I(lo) + ", " + I(hi) + ")) AS x;";
    }
    const bool big = t->pad_min + t->pad_span > 24000;
    const std::string name = t->name;
    return RunDml(values ? "INSERT VALUES" : "INSERT SELECT", sql, hi - lo + 1,
                  big, [&, name]() {
                    MTable* tt = FindTable(work_, name);
                    for (int64_t x = lo; x <= hi; ++x) {
                      tt->Put(x, GenRow(*tt, x, salt));
                    }
                    if (stats_ != nullptr) {
                      stats_->rows_inserted += static_cast<int>(hi - lo + 1);
                    }
                  });
  }

  std::string OpInsertCopy(const std::vector<std::string>& tok) {
    int64_t lo = 0;
    int64_t hi = 0;
    int64_t off = 0;
    MTable* dst = tok.size() == 6 ? FindTable(work_, tok[1]) : nullptr;
    MTable* src = tok.size() == 6 ? FindTable(work_, tok[2]) : nullptr;
    if (dst == nullptr || src == nullptr || !ParseI64(tok[3], &lo) ||
        !ParseI64(tok[4], &hi) || !ParseI64(tok[5], &off) || hi < lo ||
        off < 0) {
      return "malformed ins_copy op\n";
    }
    std::vector<std::pair<int64_t, MRow>> added;
    for (auto it = src->rows.lower_bound(lo);
         it != src->rows.end() && it->first <= hi; ++it) {
      if (dst->rows.contains(it->first + off)) {
        return "malformed ins_copy op (target key present)\n";
      }
      added.emplace_back(it->first + off, it->second);
    }
    size_t max_pad = 0;
    for (const auto& [k, r] : added) {
      max_pad = std::max(max_pad, r.pad.size());
    }
    const std::string sql = "INSERT INTO " + dst->name + " SELECT k + " +
                            I(off) + ", g, v, t, pad FROM " + src->name +
                            " WHERE k BETWEEN " + I(lo) + " AND " + I(hi) + ";";
    const std::string name = dst->name;
    return RunDml("INSERT COPY", sql, static_cast<int64_t>(added.size()),
                  max_pad > 24000, [&, name]() {
                    MTable* tt = FindTable(work_, name);
                    for (auto& [k, r] : added) {
                      tt->Put(k, r);
                    }
                    if (stats_ != nullptr) {
                      stats_->rows_inserted += static_cast<int>(added.size());
                    }
                  });
  }

  // Deliberate unique-key violation: the statement must fail.
  std::string OpInsertDup(const std::vector<std::string>& tok) {
    int64_t k = 0;
    MTable* t = tok.size() == 3 ? FindTable(work_, tok[1]) : nullptr;
    if (t == nullptr || !ParseI64(tok[2], &k) || !t->unique_k ||
        !t->rows.contains(k)) {
      return "malformed ins_dup op\n";
    }
    const std::string sql =
        "INSERT INTO " + t->name + " VALUES (" + I(k) + ", 0, 0, 'a', 'dup');";
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    SqlOutcome o = RunSql(*ctx_, sql);
    if (o.ok) {
      return "unique-violating INSERT accepted: " + sql + "\n";
    }
    if (o.status.GetCode() != StatusCode::kDuplicates &&
        !(injecting_ && TolerableUnderInjection(o.status.GetCode()))) {
      return "unique-violating INSERT failed with the wrong status: " + sql +
             "\n  " + o.why + "\n";
    }
    // A rejected statement must leave no trace in the open transaction.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    return QuickCheck(*t, *ctx_, "post-duplicate-reject");
  }

  std::string OpUpdate(const std::vector<std::string>& tok) {
    WhereSpec w;
    SetSpec s1;
    SetSpec s2;
    MTable* t = tok.size() == 13 ? FindTable(work_, tok[1]) : nullptr;
    if (t == nullptr || !ParseWhere(tok, 2, &w) || !ParseSet(tok, 7, &s1) ||
        !ParseSet(tok, 10, &s2) || s1.kind == "none" ||
        s1.Column() == s2.Column()) {
      return "malformed upd op\n";
    }
    int64_t count = 0;
    size_t max_pad = 0;
    for (const auto& [k, r] : t->rows) {
      if (w.Eval(k, r)) {
        ++count;
        MRow nr = r;
        s1.Apply(k, &nr, *t);
        s2.Apply(k, &nr, *t);
        max_pad = std::max(max_pad, nr.pad.size());
      }
    }
    std::string sql = "UPDATE " + t->name + " SET " + s1.Sql(*t);
    if (s2.kind != "none") {
      sql += ", " + s2.Sql(*t);
    }
    sql += w.Sql() + ";";
    const std::string name = t->name;
    return RunDml("UPDATE", sql, count, max_pad > 24000, [&, name]() {
      MTable* tt = FindTable(work_, name);
      std::vector<std::pair<int64_t, MRow>> changed;
      for (const auto& [k, r] : tt->rows) {
        if (w.Eval(k, r)) {
          MRow nr = r;
          s1.Apply(k, &nr, *tt);
          s2.Apply(k, &nr, *tt);
          changed.emplace_back(k, std::move(nr));
        }
      }
      for (auto& [k, r] : changed) {
        tt->Put(k, std::move(r));
      }
    });
  }

  std::string OpDelete(const std::vector<std::string>& tok) {
    WhereSpec w;
    MTable* t = tok.size() == 7 ? FindTable(work_, tok[1]) : nullptr;
    if (t == nullptr || !ParseWhere(tok, 2, &w)) {
      return "malformed del op\n";
    }
    int64_t count = 0;
    for (const auto& [k, r] : t->rows) {
      count += w.Eval(k, r) ? 1 : 0;
    }
    const std::string sql = "DELETE FROM " + t->name + w.Sql() + ";";
    const std::string name = t->name;
    return RunDml("DELETE", sql, count, false, [&, name]() {
      MTable* tt = FindTable(work_, name);
      std::vector<int64_t> gone;
      for (const auto& [k, r] : tt->rows) {
        if (w.Eval(k, r)) {
          gone.push_back(k);
        }
      }
      for (int64_t k : gone) {
        tt->Erase(k);
      }
    });
  }

  std::string OpAnalyze(const std::vector<std::string>& tok) {
    if (tok.size() != 2 || FindTable(work_, tok[1]) == nullptr) {
      return "malformed analyze op\n";
    }
    const std::string sql = "ANALYZE " + tok[1] + ";";
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    SqlOutcome o = RunSql(*ctx_, sql);
    if (!o.ok) {
      std::string finding = ClassifyFailure("ANALYZE", sql, o, false);
      return finding.empty() ? RollbackAfterFailure() : finding;
    }
    return "";
  }

  // ---- transaction / storage boundaries ----------------------------------

  std::string OpCommit() {
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    const Status st = ctx_->PreCommit();
    if (st != Status::kSuccess) {
      if (injecting_ && TolerableUnderInjection(st.GetCode())) {
        return ResolveAmbiguousCommit(st);
      }
      return "PreCommit failed: " + ToString(st) + "\n";
    }
    Bump(&OocStats::commits);
    committed_ = work_;
    ctx_.emplace(db_->BeginContext());
    return "";
  }

  // True when every table of `tables` answers its count/sum probe exactly.
  bool ImageMatches(const std::vector<MTable>& tables) {
    // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer here
    for (const MTable& t : tables) {
      CheckSpec spec = CountSpec(t);
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
      SqlOutcome o = RunSql(*ctx_, spec.sql);
      if (!o.ok || o.rows != spec.expected) {
        return false;
      }
    }
    return true;
  }

  // A commit that failed under an injected fault must still be atomic: the
  // database holds either the whole transaction or none of it.
  std::string ResolveAmbiguousCommit(const Status& st) {
    DisarmAndDetect();
    Bump(&OocStats::tolerated_failures);
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    std::ignore = ctx_->txn_.Abort();
    ctx_.emplace(db_->BeginContext());
    // The ambiguous-commit probes must run with relaxed knobs: under the
    // session's tight budgets a count/sum probe legitimately fails with
    // kMemoryLimitExceeded, which ImageMatches would misread as "image does
    // not match" and report a bogus torn transaction.
    const Knobs session = CurrentKnobs();
    SetKnobs(Knobs{});
    // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is non-copyable by design
    struct RestoreKnobs {
      OocSession* self;
      Knobs knobs;
      ~RestoreKnobs() { self->SetKnobs(knobs); }
    } restore(this, session);
    if (ImageMatches(committed_)) {
      work_ = committed_;
      return "";
    }
    if (ImageMatches(work_)) {
      committed_ = work_;
      Bump(&OocStats::commits);
      return "";
    }
    return "commit failed with " + ToString(st) +
           " under an injected fault and left a torn image: the tables match "
           "neither the pre-transaction nor the post-transaction state\n";
  }

  std::string OpAbort(const std::string& label) {
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    if (ctx_->txn_.Abort() != Status::kSuccess) {
      return label + ": Abort failed\n";
    }
    Bump(&OocStats::aborts);
    ctx_.emplace(db_->BeginContext());
    work_ = committed_;
    return QuickCheckAll("post-" + label);
  }

  void CloseSnapshot() {
    if (snap_.has_value()) {
      std::ignore = snap_->txn_.Abort();
      snap_.reset();
      snap_tables_.clear();
    }
  }

  std::string OpCrash() {
    // The in-flight writer and snapshot reader die with the process: keep
    // them alive across the reopen (their destructors are no-ops once the
    // old TransactionManager is gone), so no abort record is written.
    PauseEvictThread pause(this);
    evict_pm_ = nullptr;
    if (!sdb_->CrashAndReopen() || !*sdb_) {
      return "crash recovery failed to reopen the database\n  status: " +
             ToString(sdb_->last_reopen_status()) + "\n";
    }
    snap_.reset();
    snap_tables_.clear();
    ctx_.reset();
    Bump(&OocStats::crashes);
    db_ = sdb_->get();
    ctx_.emplace(db_->BeginContext());
    ResetFdBaseline();
    RebindEvictThreadLocked();
    work_ = committed_;
    return QuickCheckAll("post-crash");
  }

  std::string OpReopen() {
    CloseSnapshot();
    // An open transaction is rolled back before the clean shutdown.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    if (ctx_->txn_.Abort() != Status::kSuccess) {
      return "reopen: Abort failed\n";
    }
    PauseEvictThread pause(this);
    evict_pm_ = nullptr;
    ctx_.reset();
    if (!sdb_->CloseAndReopen() || !*sdb_) {
      return "clean reopen failed\n  status: " +
             ToString(sdb_->last_reopen_status()) + "\n";
    }
    Bump(&OocStats::reopens);
    db_ = sdb_->get();
    ctx_.emplace(db_->BeginContext());
    ResetFdBaseline();
    RebindEvictThreadLocked();
    work_ = committed_;
    return QuickCheckAll("post-reopen");
  }

  std::string OpCheckpoint() {
    StatusOr<lsn_t> st = db_->WriteCheckpoint();
    if (!st.HasValue()) {
      if (injecting_ && TolerableUnderInjection(st.GetStatus().GetCode())) {
        return "";
      }
      return "checkpoint failed: " + ToString(st.GetStatus()) + "\n";
    }
    Bump(&OocStats::checkpoints);
    return "";
  }

  std::string OpEvict(const std::vector<std::string>& tok) {
    int64_t target = 0;
    if (tok.size() != 2 || !ParseI64(tok[1], &target) || target < 0) {
      return "malformed evict op\n";
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    PageManager* pm = ctx_->txn_.GetPageManager();
    if (pm == nullptr) {
      return "";
    }
    const Status st = pm->EvictColdPagesForTest(static_cast<size_t>(target));
    if (st != Status::kSuccess &&
        !(injecting_ && TolerableUnderInjection(st.GetCode()))) {
      return "EvictColdPagesForTest(" + I(target) +
             ") failed: " + ToString(st) + "\n";
    }
    Bump(&OocStats::evictions);
    return "";
  }

  // TLCS compaction runs in its own committed transaction.
  std::string OpCompact(const std::vector<std::string>& tok) {
    MTable* t = tok.size() == 2 ? FindTable(work_, tok[1]) : nullptr;
    if (t == nullptr || snap_.has_value()) {
      return "malformed compact op\n";
    }
    if (std::string r = OpCommit(); !r.empty()) {
      return r;
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    const Status st = db_->CompactTableToTlcs(*ctx_, t->name);
    if (st != Status::kSuccess) {
      if ((injecting_ || TempCapped()) &&
          TolerableUnderInjection(st.GetCode())) {
        Bump(&OocStats::tolerated_failures);
        DisarmAndDetect();
        return OpAbort("post-compact-failure abort");
      }
      return "CompactTableToTlcs(" + t->name + ") failed: " + ToString(st) +
             "\n";
    }
    Bump(&OocStats::compactions);
    t->compacted = true;
    if (std::string r = OpCommit(); !r.empty()) {
      return "after compaction: " + r;
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    return QuickCheck(*FindTable(work_, tok[1]), *ctx_, "post-compact");
  }

  std::string OpSnap(const std::vector<std::string>& tok) {
    if (tok.size() == 3 && tok[1] == "open") {
      if (snap_.has_value()) {
        return "malformed snap op (already open)\n";
      }
      snap_.emplace(tok[2] == "ro" ? db_->BeginReadOnlyContext()
                                   : db_->BeginContext());
      snap_tables_ = committed_;
      // Pin the snapshot with a first read of every table.
      for (const MTable& t : snap_tables_) {
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
        if (std::string r = QuickCheck(t, *snap_, "snapshot-open");
            !r.empty()) {
          return r;
        }
      }
      return "";
    }
    if (tok.size() == 3 && tok[1] == "check") {
      if (!snap_.has_value()) {
        return "malformed snap op (not open)\n";
      }
      const MTable* t = FindTable(snap_tables_, tok[2]);
      if (t == nullptr) {
        return "";
      }
      Bump(&OocStats::snapshot_checks);
      std::vector<std::string> args = {"check", "full", tok[2]};
      return OpCheck(args, /*snapshot=*/true);
    }
    if (tok.size() == 2 && tok[1] == "close") {
      if (!snap_.has_value()) {
        return "malformed snap op (not open)\n";
      }
      const Status st = snap_->PreCommit();
      snap_.reset();
      snap_tables_.clear();
      if (st != Status::kSuccess) {
        return "snapshot reader commit failed: " + ToString(st) + "\n";
      }
      return "";
    }
    return "malformed snap op\n";
  }

  // ---- checks --------------------------------------------------------------

  struct CheckSpec {
    std::string sql;
    std::vector<std::string> expected;
    bool ordered{true};
  };

  static std::string SumOrNull(int64_t count, int64_t sum) {
    return count == 0 ? Nul() : I(sum);
  }

  static CheckSpec CountSpec(const MTable& t) {
    int64_t n = 0;
    int64_t sk = 0;
    int64_t sv = 0;
    int64_t cv = 0;
    int64_t sl = 0;
    for (const auto& [k, r] : t.rows) {
      ++n;
      sk += k;
      if (r.v.has_value()) {
        sv += *r.v;
        ++cv;
      }
      sl += static_cast<int64_t>(r.pad.size());
    }
    return {.sql =
                "SELECT COUNT(*), SUM(k), SUM(v), COUNT(v), SUM(LENGTH(pad)) "
                "FROM " +
                t.name,
            .expected = {Join({I(n), SumOrNull(n, sk), SumOrNull(cv, sv), I(cv),
                               SumOrNull(n, sl)})}};
  }

  std::string QuickCheck(const MTable& t, TransactionContext& ctx,
                         const std::string& label) {
    return RunCheck(CountSpec(t), ctx, label + " count(" + t.name + ")");
  }

  std::string QuickCheckAll(const std::string& label) {
    for (const MTable& t : work_) {
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
      if (std::string r = QuickCheck(t, *ctx_, label); !r.empty()) {
        return r;
      }
    }
    return "";
  }

  std::string RunCheck(CheckSpec spec, TransactionContext& ctx,
                       const std::string& label) {
    Bump(&OocStats::checks);
    SqlOutcome o = RunSql(ctx, spec.sql);
    if (!o.ok) {
      const StatusCode code = o.status.GetCode();
      // ADR-0004 rule 9: a row wider than a quarter of the operator budget
      // fails with kTooBigData by design. The sweep arm already tolerates
      // that once a stored pad can reach the quarter; checks must agree.
      const size_t opmem =
          ProcessMemoryBudget::Global().OpMemoryOverrideBytes();
      const bool row_over_quarter = code == StatusCode::kTooBigData &&
                                    opmem > 0 && MaxPadLen() * 4 >= opmem;
      if (((injecting_ || TempCapped()) && TolerableUnderInjection(code)) ||
          (injecting_ && o.why.find("injected") != std::string::npos) ||
          code == StatusCode::kMemoryLimitExceeded || row_over_quarter) {
        Bump(&OocStats::tolerated_failures);
        NoteFailedRead(ctx);
        return "";
      }
      return label + " query failed: " + Shorten(spec.sql, 300) + "\n  " +
             o.why + "\n";
    }
    if (!spec.ordered) {
      std::ranges::sort(o.rows);
      std::ranges::sort(spec.expected);
    }
    if (o.rows != spec.expected) {
      return label + " mismatch: " + Shorten(spec.sql, 300) + "\n" +
             DiffReport(spec.expected, o.rows);
    }
    return "";
  }

  std::string OpCheck(const std::vector<std::string>& tok, bool snapshot) {
    if (tok.size() < 3) {
      return "malformed check op\n";
    }
    std::vector<MTable>& tables = snapshot ? snap_tables_ : work_;
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access) intentional in this code
    TransactionContext& ctx = snapshot ? *snap_ : *ctx_;
    const std::string& kind = tok[1];
    const MTable* t = FindTable(tables, tok[2]);
    if (t == nullptr) {
      return "malformed check op (no table " + tok[2] + ")\n";
    }
    std::vector<int64_t> args;
    for (size_t i = 3; i < tok.size(); ++i) {
      int64_t v = 0;
      if (!ParseI64(tok[i], &v)) {
        if (kind == "joink" || kind == "joing" || kind == "leftk" ||
            kind == "setop" || kind == "joinrows" || kind == "fullk") {
          continue;  // table-name argument
        }
        return "malformed check op args\n";
      }
      args.push_back(v);
    }
    std::optional<CheckSpec> spec;
    if (kind == "count") {
      spec = CountSpec(*t);
    } else if (kind == "full" || kind == "scan") {
      CheckSpec s;
      s.sql = "SELECT k, g, v, t, pad FROM " + t->name +
              (kind == "full" ? " ORDER BY k" : "");
      s.ordered = kind == "full";
      for (const auto& [k, r] : t->rows) {
        s.expected.push_back(
            Join({I(k), I(r.g), OptI(r.v), TagS(r.tag), r.pad}));
      }
      spec = std::move(s);
    } else if (kind == "range" && args.size() == 4) {
      spec = RangeSpec(*t, args[0], args[1], args[2], args[3]);
    } else if (kind == "agg") {
      spec = AggSpec(*t);
    } else if (kind == "aggk") {
      int64_t n = 0;
      int64_t sl = 0;
      for (const auto& [k, r] : t->rows) {
        ++n;
        sl += static_cast<int64_t>(r.pad.size());
      }
      spec = CheckSpec{
          .sql =
              "SELECT COUNT(*), SUM(m), SUM(c) FROM (SELECT k, "
              "MAX(LENGTH(pad)) "
              "AS m, COUNT(*) AS c FROM " +
              t->name + " GROUP BY k)",
          .expected = {Join({I(n), SumOrNull(n, sl), SumOrNull(n, n)})}};
    } else if (kind == "strag") {
      spec = StringAggSpec(*t);
    } else if (kind == "distinct") {
      spec = DistinctSpec(*t);
    } else if (kind == "window") {
      spec = WindowSpec(*t);
    } else if (kind == "sortpad" && args.size() == 1) {
      spec = SortPadSpec(*t, args[0]);
    } else if (kind == "point" && args.size() == 1) {
      CheckSpec s;
      s.sql =
          "SELECT k, g, v, t, pad FROM " + t->name + " WHERE k = " + I(args[0]);
      if (auto it = t->rows.find(args[0]); it != t->rows.end()) {
        const MRow& r = it->second;
        s.expected.push_back(
            Join({I(args[0]), I(r.g), OptI(r.v), TagS(r.tag), r.pad}));
      }
      spec = std::move(s);
    } else if (kind == "grp" && args.size() == 1) {
      CheckSpec s;
      s.sql = "SELECT k, v FROM " + t->name + " WHERE g = " + I(args[0]) +
              " ORDER BY k";
      for (const auto& [k, r] : t->rows) {
        if (r.g == args[0]) {
          s.expected.push_back(Join({I(k), OptI(r.v)}));
        }
      }
      spec = std::move(s);
    } else if (kind == "tag" && args.size() == 2 && args[0] >= 1 &&
               args[0] < kTagCount) {
      CheckSpec s;
      s.sql = "SELECT k, LENGTH(pad) FROM " + t->name +
              " WHERE t = " + SqlLiteral(kTags[args[0]]) + " AND k > " +
              I(args[1]) + " ORDER BY k LIMIT 30";
      for (const auto& [k, r] : t->rows) {
        if (r.tag == args[0] && k > args[1] && s.expected.size() < 30) {
          s.expected.push_back(
              Join({I(k), I(static_cast<int64_t>(r.pad.size()))}));
        }
      }
      spec = std::move(s);
    } else if (kind == "topn" && args.size() == 2 && args[0] >= 0 &&
               args[1] >= 0) {
      spec = TopNSpec(*t, args[0], args[1]);
    } else if (kind == "winrows") {
      spec = WindowRowsSpec(*t);
    } else if (kind == "distrows") {
      spec = DistinctRowsSpec(*t);
    } else if ((kind == "joink" || kind == "joing" || kind == "leftk" ||
                kind == "setop" || kind == "joinrows" || kind == "fullk") &&
               tok.size() >= 4) {
      const MTable* u = FindTable(tables, tok[3]);
      if (u == nullptr) {
        return "malformed check op (no table " + tok[3] + ")\n";
      }
      if (kind == "joink") {
        spec = JoinKSpec(*t, *u);
      } else if (kind == "joing") {
        spec = JoinGSpec(*t, *u);
      } else if (kind == "leftk") {
        spec = LeftKSpec(*t, *u);
      } else if (kind == "joinrows") {
        spec = JoinRowsSpec(*t, *u);
      } else if (kind == "fullk") {
        spec = FullKSpec(*t, *u);
      } else {
        const int64_t mode = args.empty() ? 0 : args[0];
        spec = SetOpSpec(*t, *u, mode);
      }
    }
    if (!spec.has_value()) {
      return "malformed check op: " + kind + "\n";
    }
    return RunCheck(std::move(*spec), ctx,
                    std::string(snapshot ? "snapshot " : "") + "check " + kind +
                        "(" + t->name + ")");
  }

  // The specs below return raw rows from a plain (derived-table-free)
  // statement: unlike the aggregated digests above, these stay on the
  // pipeline sinks (TopN, Window, Distinct, HashJoin) under OOC knobs.

  static CheckSpec TopNSpec(const MTable& t, int64_t limit, int64_t offset) {
    std::vector<std::pair<int64_t, const MRow*>> order;
    order.reserve(t.rows.size());
    for (const auto& [k, r] : t.rows) {
      order.emplace_back(k, &r);
    }
    // v DESC puts NULLs last; k breaks ties.
    std::ranges::sort(order, [](const auto& a, const auto& b) {
      const bool an = !a.second->v.has_value();
      const bool bn = !b.second->v.has_value();
      if (an != bn) {
        return bn;
      }
      if (!an && *a.second->v != *b.second->v) {
        return *a.second->v > *b.second->v;
      }
      return a.first < b.first;
    });
    CheckSpec s;
    s.sql = "SELECT k, v FROM " + t.name + " ORDER BY v DESC, k LIMIT " +
            I(limit) + " OFFSET " + I(offset);
    for (auto i = static_cast<size_t>(offset);
         i < order.size() && s.expected.size() < static_cast<size_t>(limit);
         ++i) {
      s.expected.push_back(Join({I(order[i].first), OptI(order[i].second->v)}));
    }
    return s;
  }

  static CheckSpec WindowRowsSpec(const MTable& t) {
    std::map<int64_t, std::vector<std::pair<int64_t, const MRow*>>> groups;
    for (const auto& [k, r] : t.rows) {
      groups[r.g].emplace_back(k, &r);  // ascending k
    }
    CheckSpec s;
    s.ordered = false;
    s.sql =
        "SELECT k, ROW_NUMBER() OVER (PARTITION BY g ORDER BY k DESC), "
        "SUM(v) OVER (PARTITION BY g ORDER BY k ROWS BETWEEN 3 PRECEDING "
        "AND 1 FOLLOWING), COUNT(*) OVER (PARTITION BY g) FROM " +
        t.name;
    for (const auto& [gk, members] : groups) {
      const auto m = static_cast<int64_t>(members.size());
      for (int64_t i = 0; i < m; ++i) {
        int64_t w = 0;
        bool any = false;
        for (int64_t j = std::max<int64_t>(0, i - 3);
             j <= std::min<int64_t>(m - 1, i + 1); ++j) {
          const MRow* r = members[static_cast<size_t>(j)].second;
          if (r->v.has_value()) {
            w += *r->v;
            any = true;
          }
        }
        s.expected.push_back(Join({I(members[static_cast<size_t>(i)].first),
                                   I(m - i), any ? I(w) : Nul(), I(m)}));
      }
    }
    return s;
  }

  static CheckSpec DistinctRowsSpec(const MTable& t) {
    std::set<std::pair<int64_t, int>> seen;
    for (const auto& [k, r] : t.rows) {
      seen.emplace(r.g, r.tag);
    }
    CheckSpec s;
    s.ordered = false;
    s.sql = "SELECT DISTINCT g, t FROM " + t.name;
    for (const auto& [g, tag] : seen) {
      s.expected.push_back(Join({I(g), TagS(tag)}));
    }
    return s;
  }

  static CheckSpec JoinRowsSpec(const MTable& a, const MTable& b) {
    CheckSpec s;
    s.ordered = false;
    s.sql = "SELECT a.k, a.g, b.v, LENGTH(b.pad) FROM " + a.name + " a JOIN " +
            b.name + " b ON a.k = b.k";
    for (const auto& [k, ra] : a.rows) {
      if (auto it = b.rows.find(k); it != b.rows.end()) {
        s.expected.push_back(
            Join({I(k), I(ra.g), OptI(it->second.v),
                  I(static_cast<int64_t>(it->second.pad.size()))}));
      }
    }
    return s;
  }

  static CheckSpec FullKSpec(const MTable& a, const MTable& b) {
    CheckSpec s;
    s.ordered = false;
    s.sql = "SELECT a.k, b.k, a.t, b.v FROM " + a.name + " a FULL JOIN " +
            b.name + " b ON a.k = b.k";
    for (const auto& [k, ra] : a.rows) {
      auto it = b.rows.find(k);
      if (it != b.rows.end()) {
        s.expected.push_back(
            Join({I(k), I(k), TagS(ra.tag), OptI(it->second.v)}));
      } else {
        s.expected.push_back(Join({I(k), Nul(), TagS(ra.tag), Nul()}));
      }
    }
    for (const auto& [k, rb] : b.rows) {
      if (!a.rows.contains(k)) {
        s.expected.push_back(Join({Nul(), I(k), Nul(), OptI(rb.v)}));
      }
    }
    return s;
  }

  static CheckSpec RangeSpec(const MTable& t, int64_t lo, int64_t hi,
                             int64_t limit, int64_t offset) {
    CheckSpec s;
    s.sql = "SELECT k, v, LENGTH(pad), pad FROM " + t.name +
            " WHERE k BETWEEN " + I(lo) + " AND " + I(hi) +
            " ORDER BY k DESC LIMIT " + I(limit) + " OFFSET " + I(offset);
    std::vector<std::string> all;
    for (auto it = t.rows.upper_bound(hi); it != t.rows.begin();) {
      --it;
      if (it->first < lo) {
        break;
      }
      all.push_back(Join({I(it->first), OptI(it->second.v),
                          I(static_cast<int64_t>(it->second.pad.size())),
                          it->second.pad}));
    }
    for (auto i = static_cast<size_t>(offset);
         i < all.size() && s.expected.size() < static_cast<size_t>(limit);
         ++i) {
      s.expected.push_back(all[i]);
    }
    return s;
  }

  static CheckSpec AggSpec(const MTable& t) {
    struct G {
      int64_t n{0}, cv{0}, sv{0}, mink{0}, maxk{0}, sl{0};
      int maxtag{0};
    };
    std::map<int64_t, G> groups;
    for (const auto& [k, r] : t.rows) {
      G& g = groups[r.g];
      if (g.n == 0) {
        g.mink = k;
        g.maxk = k;
      }
      ++g.n;
      g.mink = std::min(g.mink, k);
      g.maxk = std::max(g.maxk, k);
      if (r.v.has_value()) {
        ++g.cv;
        g.sv += *r.v;
      }
      g.sl += static_cast<int64_t>(r.pad.size());
      if (r.tag != 0 && (g.maxtag == 0 || kTags[r.tag] > kTags[g.maxtag])) {
        g.maxtag = r.tag;
      }
    }
    CheckSpec s;
    s.sql =
        "SELECT g, COUNT(*), COUNT(v), SUM(v), MIN(k), MAX(k), "
        "SUM(LENGTH(pad)), MAX(t) FROM " +
        t.name + " GROUP BY g ORDER BY g";
    for (const auto& [gk, g] : groups) {
      s.expected.push_back(
          Join({I(gk), I(g.n), I(g.cv), SumOrNull(g.cv, g.sv), I(g.mink),
                I(g.maxk), I(g.sl), TagS(g.maxtag)}));
    }
    return s;
  }

  static CheckSpec StringAggSpec(const MTable& t) {
    std::map<int64_t, std::vector<std::pair<int64_t, int>>> groups;
    for (const auto& [k, r] : t.rows) {
      groups[r.g].emplace_back(k, r.tag);
    }
    CheckSpec s;
    s.sql =
        "SELECT g, LENGTH(STRING_AGG(t, ',' ORDER BY k DESC)), "
        "SUBSTR(STRING_AGG(t, ',' ORDER BY k DESC), 1, 24), COUNT(t) "
        "FROM " +
        t.name + " GROUP BY g ORDER BY g";
    for (auto& [gk, members] : groups) {
      std::ranges::sort(members, [](const auto& a, const auto& b) {
        return a.first > b.first;
      });
      std::string agg;
      int64_t cnt = 0;
      for (const auto& [k, tag] : members) {
        if (tag == 0) {
          continue;
        }
        if (cnt > 0) {
          agg += ",";
        }
        agg += kTags[tag];
        ++cnt;
      }
      s.expected.push_back(
          Join({I(gk), cnt == 0 ? Nul() : I(static_cast<int64_t>(agg.size())),
                cnt == 0 ? Nul() : agg.substr(0, 24), I(cnt)}));
    }
    return s;
  }

  static CheckSpec DistinctSpec(const MTable& t) {
    std::set<int64_t> g;
    std::set<int> tag;
    std::set<size_t> len;
    std::set<int64_t> v;
    for (const auto& [k, r] : t.rows) {
      g.insert(r.g);
      if (r.tag != 0) {
        tag.insert(r.tag);
      }
      len.insert(r.pad.size());
      if (r.v.has_value()) {
        v.insert(*r.v);
      }
    }
    return {.sql =
                "SELECT COUNT(DISTINCT g), COUNT(DISTINCT t), COUNT(DISTINCT "
                "LENGTH(pad)), COUNT(DISTINCT v) FROM " +
                t.name,
            .expected = {Join({I(static_cast<int64_t>(g.size())),
                               I(static_cast<int64_t>(tag.size())),
                               I(static_cast<int64_t>(len.size())),
                               I(static_cast<int64_t>(v.size()))})}};
  }

  static CheckSpec WindowSpec(const MTable& t) {
    std::map<int64_t, std::vector<std::pair<int64_t, const MRow*>>> groups;
    for (const auto& [k, r] : t.rows) {
      groups[r.g].emplace_back(k, &r);  // ascending k
    }
    int64_t n = 0;
    int64_t s_krn = 0;
    int64_t s_rk = 0;
    int64_t s_w = 0;
    int64_t c_w = 0;
    for (const auto& [gk, members] : groups) {
      const auto m = static_cast<int64_t>(members.size());
      // Rows per tag, so RANK is O(1) per row even for one huge partition.
      // NOLINTNEXTLINE(modernize-avoid-c-arrays)  // runtime-sized buffer, std::array not applicable
      int64_t per_tag[kTagCount] = {0, 0, 0, 0, 0};
      for (const auto& member : members) {
        ++per_tag[member.second->tag];
      }
      for (int64_t i = 0; i < m; ++i) {
        ++n;
        // ROW_NUMBER over k DESC.
        s_krn += members[static_cast<size_t>(i)].first * (m - i);
        // RANK over t ASC, NULLs first.
        const int tag_i = members[static_cast<size_t>(i)].second->tag;
        int64_t less = 0;
        if (tag_i != 0) {
          less = per_tag[0];
          for (int tag_o = 1; tag_o < kTagCount; ++tag_o) {
            less += kTags[tag_o] < kTags[tag_i] ? per_tag[tag_o] : 0;
          }
        }
        s_rk += less + 1;
        // SUM(v) over ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING (k ASC).
        int64_t w = 0;
        bool any = false;
        for (int64_t j = std::max<int64_t>(0, i - 3);
             j <= std::min<int64_t>(m - 1, i + 1); ++j) {
          const MRow* r = members[static_cast<size_t>(j)].second;
          if (r->v.has_value()) {
            w += *r->v;
            any = true;
          }
        }
        if (any) {
          s_w += w;
          ++c_w;
        }
      }
    }
    return {
        .sql =
            "SELECT COUNT(*), SUM(k * rn), SUM(rk), SUM(w), COUNT(w) FROM "
            "(SELECT k, ROW_NUMBER() OVER (PARTITION BY g ORDER BY k DESC) AS "
            "rn, RANK() OVER (PARTITION BY g ORDER BY t) AS rk, SUM(v) OVER "
            "(PARTITION BY g ORDER BY k ROWS BETWEEN 3 PRECEDING AND 1 "
            "FOLLOWING) AS w FROM " +
            t.name + ")",
        .expected = {Join({I(n), SumOrNull(n, s_krn), SumOrNull(n, s_rk),
                           SumOrNull(c_w, s_w), I(c_w)})}};
  }

  static CheckSpec SortPadSpec(const MTable& t, int64_t limit) {
    std::vector<std::pair<const std::string*, int64_t>> order;
    order.reserve(t.rows.size());
    for (const auto& [k, r] : t.rows) {
      order.emplace_back(&r.pad, k);
    }
    std::ranges::sort(order, [](const auto& a, const auto& b) {
      if (*a.first != *b.first) {
        return *a.first > *b.first;
      }
      return a.second < b.second;
    });
    CheckSpec s;
    s.sql = "SELECT k, LENGTH(pad), SUBSTR(pad, 1, 16) FROM " + t.name +
            " ORDER BY pad DESC, k LIMIT " + I(limit);
    for (size_t i = 0; i < order.size() && std::cmp_less(i, limit); ++i) {
      s.expected.push_back(Join(
          {I(order[i].second), I(static_cast<int64_t>(order[i].first->size())),
           order[i].first->substr(0, 16)}));
    }
    return s;
  }

  static CheckSpec JoinKSpec(const MTable& a, const MTable& b) {
    int64_t n = 0;
    int64_t sk = 0;
    int64_t sv = 0;
    int64_t cv = 0;
    int64_t sl = 0;
    for (const auto& [k, ra] : a.rows) {
      auto it = b.rows.find(k);
      if (it == b.rows.end()) {
        continue;
      }
      ++n;
      sk += k;
      if (it->second.v.has_value()) {
        sv += *it->second.v;
        ++cv;
      }
      sl += static_cast<int64_t>(ra.pad.size() + it->second.pad.size());
    }
    return {.sql =
                "SELECT COUNT(*), SUM(a.k), SUM(b.v), SUM(LENGTH(a.pad) + "
                "LENGTH(b.pad)) FROM " +
                a.name + " a JOIN " + b.name + " b ON a.k = b.k",
            .expected = {Join({I(n), SumOrNull(n, sk), SumOrNull(cv, sv),
                               SumOrNull(n, sl)})}};
  }

  static CheckSpec JoinGSpec(const MTable& a, const MTable& b) {
    std::map<int64_t, std::pair<int64_t, int64_t>> ga;
    std::map<int64_t, std::pair<int64_t, int64_t>> gb;
    for (const auto& [k, r] : a.rows) {
      ga[r.g].first += 1;
      ga[r.g].second += k;
    }
    for (const auto& [k, r] : b.rows) {
      gb[r.g].first += 1;
      gb[r.g].second += k;
    }
    int64_t n = 0;
    int64_t sa = 0;
    int64_t sb = 0;
    for (const auto& [g, ca] : ga) {
      auto it = gb.find(g);
      if (it == gb.end()) {
        continue;
      }
      n += ca.first * it->second.first;
      sa += ca.second * it->second.first;
      sb += ca.first * it->second.second;
    }
    return {.sql = "SELECT COUNT(*), SUM(a.k), SUM(b.k) FROM " + a.name +
                   " a JOIN " + b.name + " b ON a.g = b.g",
            .expected = {Join({I(n), SumOrNull(n, sa), SumOrNull(n, sb)})}};
  }

  static CheckSpec LeftKSpec(const MTable& a, const MTable& b) {
    int64_t n = 0;
    int64_t matched = 0;
    int64_t sk = 0;
    for (const auto& [k, r] : a.rows) {
      ++n;
      sk += k;
      auto it = b.rows.find(k);
      if (it != b.rows.end() && it->second.v.has_value() &&
          *it->second.v > 500) {
        ++matched;
      }
    }
    return {.sql = "SELECT COUNT(*), COUNT(b.k), SUM(a.k) FROM " + a.name +
                   " a LEFT JOIN " + b.name + " b ON a.k = b.k AND b.v > 500",
            .expected = {Join({I(n), I(matched), SumOrNull(n, sk)})}};
  }

  static CheckSpec SetOpSpec(const MTable& a, const MTable& b, int64_t mode) {
    if (mode == 1) {
      int64_t n = 0;
      for (const auto& [k, r] : a.rows) {
        n += b.rows.contains(k) ? 1 : 0;
      }
      return {.sql = "SELECT COUNT(*) FROM (SELECT k FROM " + a.name +
                     " INTERSECT DISTINCT SELECT k FROM " + b.name + ")",
              .expected = {I(n)}};
    }
    std::set<std::pair<int64_t, int>> pa;
    std::set<std::pair<int64_t, int>> pb;
    for (const auto& [k, r] : a.rows) {
      pa.emplace(r.g, r.tag);
    }
    for (const auto& [k, r] : b.rows) {
      pb.emplace(r.g, r.tag);
    }
    if (mode == 2) {
      std::set<std::pair<int64_t, int>> u = pa;
      u.insert(pb.begin(), pb.end());
      return {.sql = "SELECT COUNT(*) FROM (SELECT g, t FROM " + a.name +
                     " UNION DISTINCT SELECT g, t FROM " + b.name + ")",
              .expected = {I(static_cast<int64_t>(u.size()))}};
    }
    int64_t n = 0;
    for (const auto& p : pa) {
      n += pb.contains(p) ? 0 : 1;
    }
    return {.sql = "SELECT COUNT(*) FROM (SELECT g, t FROM " + a.name +
                   " EXCEPT DISTINCT SELECT g, t FROM " + b.name + ")",
            .expected = {I(n)}};
  }

  // ---- budget-invariance oracle (ooca-v2.md G2) --------------------------

  // The per-statement memory knobs a query can be executed under.
  struct Knobs {
    size_t opmem{0};
    bool spill{false};
    size_t temp{0};
    bool route{false};

    bool operator==(const Knobs&) const = default;
    [[nodiscard]] std::string Text() const {
      return "opmem=" + std::to_string(opmem) +
             " spill=" + (spill ? "1" : "0") + " temp=" + std::to_string(temp) +
             " route=" + (route ? "1" : "0");
    }
  };

  [[nodiscard]] Knobs CurrentKnobs() const {
    const ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
    return {.opmem = b.OpMemoryOverrideBytes(),
            .spill = b.AlwaysSpillEnabled(),
            .temp = b.TempLimitBytes(),
            .route = force_relational_};
  }

  void SetKnobs(const Knobs& k) {
    ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
    b.SetTestOpMemoryBytes(k.opmem);
    b.SetTestAlwaysSpill(k.spill);
    b.SetTestTempLimitBytes(k.temp);
    force_relational_ = k.route;
  }

  [[nodiscard]] size_t MaxPadLen() const {
    size_t longest = 0;
    for (const MTable& t : work_) {
      longest = std::max(longest, t.pad_hi);
    }
    return longest;
  }

  // True when `sql` names a fuzz table (t<digits>) the mirror does not have.
  [[nodiscard]] bool MentionsMissingTable(std::string_view sql,
                                          bool snapshot) const {
    const std::vector<MTable>& tables = snapshot ? snap_tables_ : work_;
    for (size_t i = 0; i + 1 < sql.size(); ++i) {
      const bool boundary =
          i == 0 ||
          (std::isalnum(static_cast<unsigned char>(sql[i - 1])) == 0 &&
           sql[i - 1] != '_' && sql[i - 1] != '.');
      if (!boundary || sql[i] != 't' ||
          std::isdigit(static_cast<unsigned char>(sql[i + 1])) == 0) {
        continue;
      }
      size_t end = i + 1;
      while (end < sql.size() &&
             std::isdigit(static_cast<unsigned char>(sql[end])) != 0) {
        ++end;
      }
      if (end < sql.size() &&
          (std::isalnum(static_cast<unsigned char>(sql[end])) != 0 ||
           sql[end] == '_')) {
        continue;
      }
      if (FindTable(tables, sql.substr(i, end - i)) == nullptr) {
        return true;
      }
    }
    return false;
  }

  static bool KeyValue(const std::string& tok, std::string_view key,
                       int64_t* out) {
    return tok.size() > key.size() + 1 && tok.starts_with(key) &&
           tok[key.size()] == '=' &&
           ParseI64(std::string_view(tok).substr(key.size() + 1), out);
  }

  // The compiled-plan and template caches key on the SQL text and know
  // nothing about the memory knobs; a trailing comment gives every execution
  // its own fingerprint so each variant is really planned under its knobs.
  std::string Tagged(const std::string& sql, int64_t variant) const {
    return sql + " /*ooc:" + std::to_string(query_serial_) + ":" +
           std::to_string(variant) + "*/";
  }

  static std::string CompareVariant(const std::vector<std::string>& ref,
                                    std::vector<std::string> got, bool ordered,
                                    int64_t take) {
    if (take < 0) {
      std::vector<std::string> expected = ref;
      if (!ordered) {
        std::ranges::sort(expected);
        std::ranges::sort(got);
      }
      return got == expected ? "" : DiffReport(expected, got);
    }
    // Early-closed cursor: exactly min(take, |result|) rows, each of them a
    // row of the full result (the prefix when the query is ordered).
    const size_t want = std::min(static_cast<size_t>(take), ref.size());
    if (got.size() != want) {
      return "  cursor closed after " + I(take) + " rows produced " +
             std::to_string(got.size()) + " rows; the full result has " +
             std::to_string(ref.size()) + "\n";
    }
    if (ordered) {
      const std::vector<std::string> prefix(
          ref.begin(), ref.begin() + static_cast<std::ptrdiff_t>(want));
      return got == prefix ? "" : DiffReport(prefix, got);
    }
    std::map<std::string, int64_t> remaining;
    for (const std::string& row : ref) {
      ++remaining[row];
    }
    for (const std::string& row : got) {
      if (--remaining[row] < 0) {
        return "  row [" + Shorten(row, 120) +
               "] of the partial read is not in the full result (or occurs "
               "too often)\n";
      }
    }
    return "";
  }

  void DropPending() {
    pending_mem_ = 0;
    pending_io_ = 0;
    last_fired_ = false;
  }

  // q ord=<0|1> heavy=<0|1> take=<n|-1> nv=<n> vs=<seed> ex=<0|1> snap=<0|1>
  //   :: <sql>
  // Runs <sql> with every memory knob relaxed (the reference), then under
  // the session's knobs and nv-1 random pressure settings; all results must
  // agree.  take >= 0 closes the first pressure cursor after that many rows.
  std::string OpQuery(const std::string& op) {
    const size_t sep = op.find(" :: ");
    if (sep == std::string::npos) {
      DropPending();
      return "malformed q op\n";
    }
    const std::string sql = op.substr(sep + 4);
    int64_t ord = 0;
    int64_t heavy = 0;
    int64_t take = -1;
    int64_t nv = 1;
    int64_t vs = 0;
    int64_t ex = 0;
    int64_t snap = 0;
    const std::vector<std::string> tok =
        SplitTokens(std::string_view(op).substr(0, sep));
    for (size_t i = 1; i < tok.size(); ++i) {
      if (!(KeyValue(tok[i], "ord", &ord) ||
            KeyValue(tok[i], "heavy", &heavy) ||
            KeyValue(tok[i], "take", &take) || KeyValue(tok[i], "nv", &nv) ||
            KeyValue(tok[i], "vs", &vs) || KeyValue(tok[i], "ex", &ex) ||
            KeyValue(tok[i], "snap", &snap))) {
        DropPending();
        return "malformed q op\n";
      }
    }
    if (sql.empty() || nv < 1 || nv > 8 || take < -1 ||
        (snap != 0 && !snap_.has_value())) {
      DropPending();
      return "malformed q op\n";
    }
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
    TransactionContext& ctx = snap != 0 ? *snap_ : *ctx_;
    const Knobs session = CurrentKnobs();
    // Whatever happens below, hand the session its knobs back.
    struct Restore {
      OocSession* self;
      Knobs knobs;
      Restore(OocSession* s, const Knobs& k) : self(s), knobs(k) {}
      Restore(const Restore&) = delete;
      Restore& operator=(const Restore&) = delete;
      Restore(Restore&&) = delete;
      Restore& operator=(Restore&&) = delete;
      ~Restore() { self->SetKnobs(knobs); }
    } restore(this, session);
    Bump(&OocStats::diff_queries);
    ++query_serial_;

    const Knobs relaxed{};
    SetKnobs(relaxed);
    SqlOutcome ref = RunSql(ctx, Tagged(sql, 0));
    if (!ref.ok) {
      DropPending();
      const StatusCode code = ref.status.GetCode();
      if (code == StatusCode::kMemoryLimitExceeded ||
          (heavy != 0 && code == StatusCode::kTooBigData)) {
        Bump(&OocStats::diff_tolerated);
        NoteFailedRead(ctx);
        return "";
      }
      if (lenient_ && MentionsMissingTable(sql, snap != 0)) {
        // A minimizer removed the table this query reads.
        Bump(&OocStats::diff_skipped);
        return "";
      }
      return "reference execution (every memory knob relaxed) failed [" +
             SqlFeatures(sql) + "]: " + Shorten(sql, 600) + "\n  " + ref.why +
             "\n";
    }

    std::mt19937_64 vr((static_cast<uint64_t>(vs) * 0x9E3779B97F4A7C15ULL) + 1);
    for (int64_t i = 0; i < nv; ++i) {
      Knobs k = session;
      if (i > 0 || k == relaxed) {
        // NOLINTNEXTLINE(modernize-avoid-c-arrays)  // runtime-sized buffer, std::array not applicable
        static constexpr size_t kBudgets[] = {
            size_t{4} << 10,   size_t{16} << 10, size_t{64} << 10,
            size_t{256} << 10, size_t{1} << 20,  0};
        k.opmem = kBudgets[vr() % 6];
        k.spill = (vr() % 3) != 0;
        k.route = (vr() % 5) == 0;
        if (k.opmem == 0 && !k.spill) {
          k.spill = true;
        }
      }
      const int64_t this_take = i == 0 ? take : -1;
      SetKnobs(k);
      if (i == 0) {
        ArmPending();
      }
      const bool injected = injecting_;
      SqlOutcome got = RunSql(ctx, Tagged(sql, i + 1), this_take);
      if (i == 0) {
        DisarmAndDetect();
      }
      Bump(&OocStats::diff_variants);
      if (this_take >= 0) {
        Bump(&OocStats::partial_cursors);
      }
      if (!got.ok) {
        const StatusCode code = got.status.GetCode();
        const bool tolerated =
            code == StatusCode::kMemoryLimitExceeded ||
            (injected && got.why.find("injected") != std::string::npos) ||
            ((injected || k.temp > 0) && TolerableUnderInjection(code)) ||
            (code == StatusCode::kTooBigData &&
             (heavy != 0 || (k.opmem > 0 && MaxPadLen() * 4 >= k.opmem)));
        if (tolerated) {
          Bump(&OocStats::diff_tolerated);
          NoteFailedRead(ctx);
          if (verbose_) {
            std::cerr << "[ooc] variant {" << k.Text() << "} tolerated ("
                      << got.why << ")\n";
          }
          continue;
        }
        // The status line comes second so the signature buckets on it.
        return "query fails under memory pressure but runs without it [" +
               SqlFeatures(sql) + "]: " + Shorten(sql, 600) + "\n  " + got.why +
               "\n  knobs {" + k.Text() + "}\n";
      }
      if (std::string diff = CompareVariant(ref.rows, std::move(got.rows),
                                            ord != 0, this_take);
          !diff.empty()) {
        return "budget-invariance mismatch [" + SqlFeatures(sql) +
               "] (same query, different memory knobs): " + Shorten(sql, 600) +
               "\n  reference {" + relaxed.Text() + "} vs {" + k.Text() + "}" +
               (this_take >= 0 ? " reading only the first rows" : "") + "\n" +
               diff;
      }
    }
    if (ex != 0) {
      SetKnobs(session);
      return ExplainCheck(ctx, sql, ref.rows.size());
    }
    return "";
  }

  // EXPLAIN ANALYZE executes the statement: its row count must match, and
  // its pipeline line tells which operators the fuzzer really reached.
  std::string ExplainCheck(TransactionContext& ctx, const std::string& sql,
                           size_t expected_rows) {
    SqlOutcome o = RunSql(ctx, "EXPLAIN ANALYZE " + Tagged(sql, 99));
    if (!o.ok) {
      const StatusCode code = o.status.GetCode();
      if (code == StatusCode::kMemoryLimitExceeded ||
          code == StatusCode::kTooBigData ||
          (TempCapped() && TolerableUnderInjection(code)) || lenient_) {
        Bump(&OocStats::diff_tolerated);
        NoteFailedRead(ctx);
        return "";
      }
      return "EXPLAIN ANALYZE fails for a query that runs: " +
             Shorten(sql, 600) + "\n  " + o.why + "\n";
    }
    Bump(&OocStats::explain_checks);
    bool saw_pipeline = false;
    static constexpr std::string_view kActual = "Actual Rows: ";
    static constexpr std::string_view kPipeline = "Pipeline: ";
    for (const std::string& line : o.rows) {
      if (line.starts_with(kActual)) {
        int64_t n = -1;
        if (ParseI64(std::string_view(line).substr(kActual.size()), &n) &&
            std::cmp_not_equal(n, expected_rows)) {
          return "EXPLAIN ANALYZE reports `" + line +
                 "` but the query returns " + std::to_string(expected_rows) +
                 " rows: " + Shorten(sql, 600) + "\n";
        }
      } else if (line.starts_with(kPipeline)) {
        saw_pipeline = true;
        const std::string_view body =
            std::string_view(line).substr(kPipeline.size());
        RecordPipeline(body);
        if (stats_ != nullptr) {
          const bool pure =
              body.find("legacy_source_ms=0 ") != std::string_view::npos ||
              body.ends_with("legacy_source_ms=0");
          ++stats_->routes[SqlFeatures(sql) +
                           (pure ? " => pipeline" : " => legacy")];
        }
      }
    }
    if (!saw_pipeline && stats_ != nullptr) {
      ++stats_->routes[SqlFeatures(sql) + " => other"];
    }
    return "";
  }

  // "Stage 0 (HashBuild): FullScan: t1(...) -> Projection -> HashBuild |
  //  Stage 1 (Output): ... -> Output dop=1 granted=..B spill_bytes=..B
  //  legacy_source_ms=0"
  void RecordPipeline(std::string_view line) {
    if (stats_ == nullptr) {
      return;
    }
    if (line.find("legacy_source_ms=0 ") != std::string_view::npos ||
        line.ends_with("legacy_source_ms=0")) {
      ++stats_->pure_pipeline_queries;
    }
    if (const size_t at = line.find("spill_bytes=");
        at != std::string_view::npos && at + 12 < line.size() &&
        line[at + 12] != '0') {
      ++stats_->spilled_queries;
    }
    size_t p = 0;
    while (p < line.size()) {
      size_t bar = line.find(" | ", p);
      if (bar == std::string_view::npos) {
        bar = line.size();
      }
      std::string_view stage = line.substr(p, bar - p);
      p = bar + 3;
      const size_t head = stage.find("): ");
      if (head == std::string_view::npos) {
        continue;
      }
      stage.remove_prefix(head + 3);
      size_t q = 0;
      while (q < stage.size()) {
        size_t arrow = stage.find(" -> ", q);
        if (arrow == std::string_view::npos) {
          arrow = stage.size();
        }
        const std::string_view item = stage.substr(q, arrow - q);
        q = arrow + 4;
        size_t end = 0;
        while (end < item.size() &&
               (std::isalnum(static_cast<unsigned char>(item[end])) != 0)) {
          ++end;
        }
        if (end > 0) {
          ++stats_->operators[std::string(item.substr(0, end))];
        }
      }
    }
  }

  // ---- failure-injection sweep (ooca-v2.md G6) -----------------------------

  // sweep <mem|io> <cap> :: <op>
  // Runs <op> with the 1st, 2nd, 3rd ... reservation (or spill I/O call)
  // failing, until the op makes fewer calls than the countdown.  Every
  // attempt must either fail cleanly (the transaction rolls back to the
  // committed image, nothing leaks) or succeed with the exact result.
  std::string OpSweep(const std::string& op) {
    const size_t sep = op.find(" :: ");
    if (sep == std::string::npos) {
      return "malformed sweep op\n";
    }
    const std::vector<std::string> head =
        SplitTokens(std::string_view(op).substr(0, sep));
    const std::string inner = op.substr(sep + 4);
    const std::vector<std::string> itok = SplitTokens(inner);
    int64_t cap = 0;
    static const std::set<std::string> kMutating = {
        "ins_bulk", "ins_values", "ins_copy", "upd", "del", "index"};
    static const std::set<std::string> kReadOnly = {"check", "q", "checkpoint",
                                                    "evict", "analyze"};
    if (head.size() != 3 || (head[1] != "mem" && head[1] != "io") ||
        !ParseI64(head[2], &cap) || cap < 1 || cap > 1000000 || itok.empty() ||
        (!kMutating.contains(itok[0]) && !kReadOnly.contains(itok[0]))) {
      return "malformed sweep op\n";
    }
    const bool mutating = kMutating.contains(itok[0]);
    if (mutating && !(work_ == committed_)) {
      // Each failed attempt rolls back to the committed image, so the sweep
      // must start from one.
      if (std::string r = OpCommit(); !r.empty()) {
        return r;
      }
    }
    Bump(&OocStats::sweeps);
    const std::string label =
        "sweep " + head[1] + " `" + Shorten(inner, 120) + "` ";
    int points = 0;
    // Visit every point up to 256 (and to cap when smaller); only beyond that
    // relax to geometric spacing. The old `n/4` step from 33 onward skipped
    // ~97% of failure points in 33..400, so off-by-one budget bugs at small
    // injection counts went untested.
    for (int64_t n = 1; n <= cap;
         n = n < 256 ? n + 1 : n + std::max<int64_t>(1, n / 4)) {
      (head[1] == "mem" ? pending_mem_ : pending_io_) =
          static_cast<uint64_t>(n);
      std::string r = RunOp(inner);
      const bool fired = last_fired_;
      ++points;
      if (!r.empty()) {
        if (r.starts_with("malformed")) {
          return r;
        }
        return label + "with failure #" + I(n) + ": " + r;  // NOLINT(performance-inefficient-string-concatenation)
      // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed concatenations
      }
      if (std::string leak = LeakCheck(inner); !leak.empty()) {
        return label + "with failure #" + I(n) + ": " + leak;  // NOLINT(performance-inefficient-string-concatenation)
      // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed concatenations
      }
      if (!fired) {
        break;  // the op makes fewer than n calls: every point was visited
      }
      Bump(&OocStats::sweep_fired);
      if (mutating && !(work_ == committed_)) {
        // The statement survived the fault (spill fallback).  Undo it so the
        // next attempt starts from the same image.
        if (std::string a = OpAbort("sweep rollback"); !a.empty()) {
          return label + "after surviving failure #" + I(n) + ": " + a;  // NOLINT(performance-inefficient-string-concatenation)
        // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed concatenations
        }
      }
    }
    Bump(&OocStats::sweep_points, points);
    return "";
  }

  // ---- accounting ----------------------------------------------------------

  // Open descriptors of unlinked files: every SpillStore / SpillFile keeps
  // one while it lives (Linux only; 0 elsewhere).
  static int DeletedFdCount() {
    int count = 0;
    std::error_code ec;
    for (std::filesystem::directory_iterator it("/proc/self/fd", ec), end;
         !ec && it != end; it.increment(ec)) {
      std::error_code link_ec;
      const std::filesystem::path target =
          std::filesystem::read_symlink(it->path(), link_ec);
      if (!link_ec && target.native().ends_with(" (deleted)")) {
        ++count;
      }
    }
    return count;
  }

  // After a statement finished (no live QueryResult), the query pool must be
  // back to zero and no statement-scoped spill file may remain open: a
  // leftover is an operator or spill-store leak (ooca-v2.md G6), typically
  // on an error or early-close path.
  std::string LeakCheck(const std::string& op) {
    if (EnvU64("TINYLAMB_OOC_FUZZ_NO_LEAK_CHECK", 0) != 0) {
      return "";
    }
    const size_t q = ProcessMemoryBudget::Global().QueryUsed();
    if (q != 0) {
      return "query memory pool still holds " + std::to_string(q) +
             " bytes after op `" + Shorten(op, 160) + "` completed\n";
    }
    const int fds = DeletedFdCount();
    if (stats_ != nullptr) {
      stats_->max_deleted_fds = std::max(stats_->max_deleted_fds, fds);
    }
    // The database itself owns a few unlinked files (private spill of
    // uncommitted pages, replaced TLCS images); statement-scoped ones must
    // not pile up on top of them.
    constexpr int kFdSlack = 12;
    if (fds > fd_baseline_ + kFdSlack) {
      return "unlinked temp files pile up: " + std::to_string(fds) +
             " descriptors of deleted files are open after op `" +
             Shorten(op, 160) + "` (" + std::to_string(fd_baseline_) +
             " when the database opened): a spill store outlives its "
             "statement\n";
    }
    return "";
  }

  void ResetFdBaseline() { fd_baseline_ = DeletedFdCount(); }

  void SamplePressure() {
    if (stats_ == nullptr || db_ == nullptr || !ctx_.has_value()) {
      return;
    }
    PageManager* pm = ctx_->txn_.GetPageManager();
    if (pm != nullptr) {
      stats_->evicted_pages =
          std::max<uint64_t>(stats_->evicted_pages, pm->EvictedPageCount());
      stats_->uncommitted_spill_evictions = std::max<uint64_t>(
          stats_->uncommitted_spill_evictions, pm->UncommittedSpillEvictions());
    }
    const OocTelemetrySnapshot tel =
        ProcessMemoryBudget::Global().TelemetrySnapshot();
    stats_->spill_bytes = std::max(stats_->spill_bytes, tel.spill_bytes);
    stats_->spill_ops = std::max(stats_->spill_ops, tel.spill_ops);
    stats_->max_partition_depth =
        std::max(stats_->max_partition_depth, tel.max_partition_depth);
    stats_->skew_fallbacks =
        std::max(stats_->skew_fallbacks, tel.skew_fallbacks);
    stats_->max_db_bytes = std::max(stats_->max_db_bytes, TotalPadBytes());
  }

  // ---- optional background evictor (nondeterministic stress) -------------

  struct PauseEvictThread {
    explicit PauseEvictThread(OocSession* s) : lock(s->evict_mu_) {}
    std::scoped_lock<std::mutex> lock;
  };

  void StartEvictThread() {
    {
      std::scoped_lock lk(evict_mu_);
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)  // test/fuzzer assumes success
      evict_pm_ = ctx_->txn_.GetPageManager();
    }
    evict_stop_.store(false);
    evict_thread_ = std::thread([this]() {
      // NOLINTNEXTLINE(cert-msc32-c,cert-msc51-cpp) deterministic fuzz replay
      std::mt19937 rng(12345);
      while (!evict_stop_.load(std::memory_order_acquire)) {
        {
          std::scoped_lock lk(evict_mu_);
          if (evict_pm_ != nullptr) {
            std::ignore = evict_pm_->EvictColdPagesForTest(
                rng() % 2 == 0 ? 0 : (rng() % 65536));
          }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
      }
    });
  }

  // Called with evict_mu_ held (PauseEvictThread) after a reopen.
  void RebindEvictThreadLocked() {
    evict_pm_ = (evict_thread_.joinable() && ctx_.has_value())
                    ? ctx_->txn_.GetPageManager()
                    : nullptr;
  }

  void StopEvictThread() {
    if (evict_thread_.joinable()) {
      evict_stop_.store(true, std::memory_order_release);
      evict_thread_.join();
    }
  }

  OocProfile profile_;
  bool verbose_{false};
  OocStats* stats_{nullptr};
  std::unique_ptr<ScopedDb> sdb_;
  Database* db_{nullptr};
  std::optional<TransactionContext> ctx_;
  std::optional<TransactionContext> snap_;
  std::vector<MTable> work_;
  std::vector<MTable> committed_;
  std::vector<MTable> snap_tables_;
  bool force_relational_{false};
  bool lenient_{false};
  uint64_t pending_mem_{0};
  uint64_t pending_io_{0};
  uint64_t armed_mem_{0};
  uint64_t armed_io_{0};
  bool injecting_{false};
  bool last_fired_{false};
  bool read_failed_{false};
  uint64_t query_serial_{0};
  int fd_baseline_{0};
  std::mutex evict_mu_;
  PageManager* evict_pm_{nullptr};
  std::atomic<bool> evict_stop_{false};
  std::thread evict_thread_;
};

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

using Gen = OocRng;

// Session flavours: the same op vocabulary with different weights and data
// shapes, so a campaign concentrates pressure on one risk at a time instead
// of diluting every session into the same average mix.
struct Flavor {
  const char* name;
  // Relative op weights.
  int insert, values, copy, dup, update, del, check, query, commit, abort,
      crash, reopen, checkpoint, evict, compact, knob, inject, sweep, snap,
      index, analyze, ddl;
  int skew_pct;     // tables with a handful of distinct keys (G4)
  int wide_pct;     // tables whose rows are several KiB
  int narrow_pct;   // tables of tiny rows, many of them (deep spill recursion)
  bool tiny_pool;   // storage pool of one or two pages (P6: huge transactions)
  int rows_factor;  // multiplier on the row cap of narrow tables
};

// NOLINTNEXTLINE(modernize-avoid-c-arrays)  // runtime-sized buffer, std::array not applicable
constexpr Flavor kFlavors[] = {
    // name     ins val cpy dup  upd  del  chk  qry com abo cra reo ckp evi cmp
    //          knb inj swp snp idx ana ddl skew wide narrow tiny rows
    {.name = "mixed",
     .insert = 110,
     .values = 40,
     .copy = 30,
     .dup = 10,
     .update = 120,
     .del = 60,
     .check = 150,
     .query = 120,
     .commit = 70,
     .abort = 25,
     .crash = 25,
     .reopen = 15,
     .checkpoint = 30,
     .evict = 55,
     .compact = 20,
     .knob = 40,
     .inject = 30,
     .sweep = 10,
     .snap = 35,
     .index = 20,
     .analyze = 10,
     .ddl = 20,
     .skew_pct = 15,
     .wide_pct = 12,
     .narrow_pct = 20,
     .tiny_pool = false,
     .rows_factor = 2},
    {.name = "query",
     .insert = 40,
     .values = 10,
     .copy = 15,
     .dup = 5,
     .update = 40,
     .del = 20,
     .check = 80,
     .query = 520,
     .commit = 40,
     .abort = 10,
     .crash = 10,
     .reopen = 8,
     .checkpoint = 15,
     .evict = 40,
     .compact = 15,
     .knob = 70,
     .inject = 25,
     .sweep = 10,
     .snap = 20,
     .index = 25,
     .analyze = 10,
     .ddl = 10,
     .skew_pct = 20,
     .wide_pct = 8,
     .narrow_pct = 45,
     .tiny_pool = false,
     .rows_factor = 6},
    {.name = "bigtxn",
     .insert = 160,
     .values = 30,
     .copy = 50,
     .dup = 5,
     .update = 260,
     .del = 110,
     .check = 110,
     .query = 60,
     .commit = 12,
     .abort = 40,
     .crash = 35,
     .reopen = 10,
     .checkpoint = 25,
     .evict = 90,
     .compact = 5,
     .knob = 20,
     .inject = 20,
     .sweep = 5,
     .snap = 60,
     .index = 10,
     .analyze = 5,
     .ddl = 10,
     .skew_pct = 10,
     .wide_pct = 25,
     .narrow_pct = 10,
     .tiny_pool = true,
     .rows_factor = 2},
    {.name = "skew",
     .insert = 60,
     .values = 15,
     .copy = 20,
     .dup = 5,
     .update = 60,
     .del = 25,
     .check = 100,
     .query = 480,
     .commit = 40,
     .abort = 10,
     .crash = 10,
     .reopen = 8,
     .checkpoint = 15,
     .evict = 40,
     .compact = 15,
     .knob = 60,
     .inject = 20,
     .sweep = 10,
     .snap = 15,
     .index = 25,
     .analyze = 10,
     .ddl = 10,
     .skew_pct = 90,
     .wide_pct = 5,
     .narrow_pct = 40,
     .tiny_pool = false,
     .rows_factor = 4},
    {.name = "inject",
     .insert = 70,
     .values = 20,
     .copy = 20,
     .dup = 5,
     .update = 70,
     .del = 30,
     .check = 60,
     .query = 120,
     .commit = 50,
     .abort = 20,
     .crash = 20,
     .reopen = 10,
     .checkpoint = 20,
     .evict = 40,
     .compact = 15,
     .knob = 30,
     .inject = 150,
     .sweep = 220,
     .snap = 20,
     .index = 15,
     .analyze = 5,
     .ddl = 10,
     .skew_pct = 20,
     .wide_pct = 10,
     .narrow_pct = 20,
     .tiny_pool = false,
     .rows_factor = 2},
    {.name = "wide",
     .insert = 120,
     .values = 40,
     .copy = 40,
     .dup = 5,
     .update = 140,
     .del = 50,
     .check = 140,
     .query = 160,
     .commit = 60,
     .abort = 20,
     .crash = 25,
     .reopen = 15,
     .checkpoint = 30,
     .evict = 60,
     .compact = 25,
     .knob = 40,
     .inject = 30,
     .sweep = 10,
     .snap = 30,
     .index = 20,
     .analyze = 10,
     .ddl = 20,
     .skew_pct = 10,
     .wide_pct = 85,
     .narrow_pct = 0,
     .tiny_pool = false,
     .rows_factor = 1},
};
constexpr int kFlavorCount = static_cast<int>(std::size(kFlavors));

OocProfile GenProfile(Gen& g, const Flavor& f) {
  OocProfile p;
  const std::vector<size_t> pools = {size_t{16} << 10,  size_t{32} << 10,
                                     size_t{64} << 10,  size_t{128} << 10,
                                     size_t{256} << 10, size_t{1} << 20};
  p.pool_bytes = g.Chance(92) ? g.OneOf(pools) : 0;
  if (f.tiny_pool) {
    p.pool_bytes = size_t{16} << static_cast<size_t>(g.Pick(10, 12));
  }
  const std::vector<size_t> columnar = {0, size_t{64} << 10, size_t{256} << 10,
                                        size_t{1} << 20};
  p.columnar_bytes = g.OneOf(columnar);
  const std::vector<size_t> ops = {0, size_t{16} << 10, size_t{64} << 10,
                                   size_t{256} << 10, size_t{1} << 20};
  p.op_memory_bytes = g.OneOf(ops);
  p.always_spill = g.Chance(45);
  p.temp_limit_bytes =
      g.Chance(6) ? static_cast<size_t>(g.Pick(64, 2048)) << 10 : 0;
  p.force_relational = g.Chance(15);
  return p;
}

class Driver {
 public:
  Driver(Gen& g, OocSession& s, OocTrace& t, const Flavor& f, int scale,
         const OocRunOptions& options)
      : g_(g),
        s_(s),
        t_(t),
        f_(f),
        scale_(std::max(1, scale)),
        options_(options) {}

  // Applies (and records) one op.
  std::string Do(const std::string& op) {
    t_.ops.push_back(op);
    if (options_.on_op) {
      options_.on_op(op);
    }
    return s_.Apply(op);
  }

  [[nodiscard]] static bool Narrow(const MTable& t) {
    return t.pad_min + t.pad_span <= 48;
  }
  [[nodiscard]] int64_t MaxRows(const MTable& t) const {
    return 1500LL * scale_ * (Narrow(t) ? f_.rows_factor : 1);
  }

  const MTable* PickTable() {
    const std::vector<MTable>& v = s_.Tables();
    if (v.empty()) {
      return nullptr;
    }
    return &v[static_cast<size_t>(
        g_.Pick(0, static_cast<int64_t>(v.size()) - 1))];
  }

  std::string NewTableOp() {
    const std::string name = "t" + std::to_string(table_counter_++);
    int64_t groups = g_.Chance(15) ? g_.Pick(1, 2) : g_.Pick(3, 60);
    int64_t pmin = g_.Pick(0, 300);
    int64_t pspan = g_.Pick(1, 2500);
    int64_t v_mod = 1000;
    int64_t vnull_mod = 11;
    int64_t pad_mode = 0;
    const int64_t roll = g_.Pick(0, 99);
    if (roll < f_.wide_pct) {
      // Wide rows: a few per 32 KiB page, sometimes more than a page each.
      pmin = g_.Chance(15) ? g_.Pick(20000, 40000) : g_.Pick(4000, 9000);
      pspan = g_.Pick(1, 6000);
    } else if (roll < f_.wide_pct + f_.narrow_pct) {
      pmin = g_.Pick(0, 8);
      pspan = g_.Pick(1, 24);
    }
    if (g_.Chance(f_.skew_pct)) {
      // Skew: nearly every row shares its group, value and (wide) pad.
      groups = g_.Pick(1, 2);
      v_mod = g_.OneOf(std::vector<int64_t>{1, 2, 3, 7});
      vnull_mod = g_.OneOf(std::vector<int64_t>{1, 2, 11, 1000000});
      pad_mode = g_.Chance(50) ? 1 : 0;
      if (pad_mode == 1) {
        pspan = g_.Pick(1, 3);
      }
    } else if (g_.Chance(10)) {
      vnull_mod = g_.OneOf(std::vector<int64_t>{2, 3, 1000000});
    }
    std::string op =
        "create " + name + " " + I(groups) + " " + I(pmin) + " " + I(pspan);
    if (v_mod != 1000 || vnull_mod != 11 || pad_mode != 0) {
      op += " " + I(v_mod) + " " + I(vnull_mod) + " " + I(pad_mode);
    }
    return op;
  }

  std::string BulkInsertOp(const MTable& t, bool values) {
    int64_t room = MaxRows(t) - static_cast<int64_t>(t.rows.size());
    // Keep the whole database within a few MiB per scale unit: enough to be
    // dozens of times the shrunken pool, small enough for 60 s test units.
    const auto max_bytes = static_cast<int64_t>(size_t{6} << 20) * scale_;
    const int64_t avg = t.pad_min + (t.pad_span / 2) + 16;
    const int64_t byte_room =
        (max_bytes - static_cast<int64_t>(s_.TotalPadBytes())) / avg;
    room = std::min(room, byte_room);
    if (room <= 0) {
      return "";
    }
    const int64_t chunk = Narrow(t) ? 6000 : 500;
    const int64_t want =
        values ? g_.Pick(1, 12) : g_.Pick(1, std::min<int64_t>(room, chunk));
    const int64_t cnt = std::min(want, room);
    int64_t lo = t.next_k;
    // Sometimes refill a deleted hole below next_k.
    if (g_.Chance(25) && t.next_k > cnt + 1) {
      const int64_t cand = g_.Pick(1, t.next_k - cnt);
      bool free = true;
      for (int64_t x = cand; x < cand + cnt && free; ++x) {
        free = !t.rows.contains(x);
      }
      if (free) {
        lo = cand;
      }
    }
    return std::string(values ? "ins_values " : "ins_bulk ") + t.name + " " +
           I(lo) + " " + I(lo + cnt - 1) + " " + I(g_.Pick(0, 999));
  }

  std::string CopyOp(const MTable& dst, const MTable& src) {
    if (src.rows.empty()) {
      return "";
    }
    const int64_t lo_key = src.rows.begin()->first;
    const int64_t hi_key = src.rows.rbegin()->first;
    const int64_t lo = g_.Pick(lo_key, hi_key);
    const int64_t hi = std::min(hi_key, lo + g_.Pick(0, 300));
    int64_t moved = 0;
    for (auto it = src.rows.lower_bound(lo);
         it != src.rows.end() && it->first <= hi; ++it) {
      ++moved;
    }
    if (static_cast<int64_t>(dst.rows.size()) + moved > MaxRows(dst)) {
      return "";
    }
    const int64_t off = std::max<int64_t>(dst.next_k, hi + 1) - lo;
    return "ins_copy " + dst.name + " " + src.name + " " + I(lo) + " " + I(hi) +
           " " + I(off);
  }

  int64_t SomeKey(const MTable& t) {
    if (t.rows.empty() || g_.Chance(10)) {
      return g_.Pick(1, t.next_k + 5);
    }
    auto it = t.rows.lower_bound(g_.Pick(1, t.next_k));
    if (it == t.rows.end()) {
      it = t.rows.begin();
    }
    return it->first;
  }

  WhereSpec RandomWhere(const MTable& t, bool allow_all) {
    WhereSpec w;
    const int64_t roll = g_.Pick(0, 99);
    if (roll < 40) {
      w.kind = "range";
      w.a = SomeKey(t);
      w.b = w.a + g_.Pick(0, g_.Chance(20) ? 2000 : 150);
    } else if (roll < 55) {
      w.kind = "rmod";
      w.a = SomeKey(t);
      w.b = w.a + g_.Pick(0, 1500);
      w.c = g_.Pick(2, 9);
      w.d = g_.Pick(0, w.c - 1);
    } else if (roll < 67) {
      w.kind = "grp";
      w.a = g_.Pick(0, t.groups - 1);
    } else if (roll < 75) {
      w.kind = "vgt";
      w.a = g_.Pick(0, 1100);
    } else if (roll < 80) {
      w.kind = "vnull";
    } else if (roll < 87) {
      w.kind = "padgt";
      w.a = g_.Pick(0, 3000);
    } else if (roll < 95 || !allow_all) {
      w.kind = "tag";
      w.a = g_.Pick(1, kTagCount - 1);
    } else {
      w.kind = "all";
    }
    return w;
  }

  SetSpec RandomSet(bool allow_none) {
    SetSpec s;
    const int64_t roll = g_.Pick(0, allow_none ? 119 : 99);
    if (roll < 20) {
      s.kind = "vadd";
      s.p = g_.Pick(-50, 50);
    } else if (roll < 30) {
      s.kind = "vset";
      s.p = g_.Pick(0, 1200);
    } else if (roll < 34) {
      s.kind = "vnull";
    } else if (roll < 60) {
      s.kind = "pad";
      s.p = g_.Chance(8)
                ? g_.Pick(9000, 16000)
                : (g_.Chance(2) ? g_.Pick(30000, 40000) : g_.Pick(0, 3000));
      s.q = g_.Pick(0, 99);
    } else if (roll < 70) {
      s.kind = "padapp";
    } else if (roll < 82) {
      s.kind = "gmod";
      s.p = g_.Pick(0, 100);
    } else if (roll < 94) {
      s.kind = "tag";
      s.p = g_.Pick(1, 4);
    } else if (roll < 100) {
      s.kind = "tagnull";
    } else {
      s.kind = "none";
    }
    return s;
  }

  std::string UpdateOp(const MTable& t) {
    WhereSpec w = RandomWhere(t, true);
    const SetSpec s1 = RandomSet(false);
    SetSpec s2 = RandomSet(true);
    if (s2.Column() == s1.Column()) {
      // GoogleSQL rejects assigning one column twice in a single UPDATE.
      s2 = SetSpec{};
    }
    const bool wide = (s1.kind == "pad" && s1.p >= 9000) ||
                      (s2.kind == "pad" && s2.p >= 9000);
    const bool grows = s1.kind == "pad" || s2.kind == "pad" ||
                       s1.kind == "padapp" || s2.kind == "padapp";
    if (wide || (grows && !f_.tiny_pool)) {
      // Bound the bytes a single statement can add.
      w.kind = "range";
      w.a = SomeKey(t);
      w.b = w.a + g_.Pick(0, wide ? 40 : 400);
    } else if (f_.tiny_pool && g_.Chance(35)) {
      // Huge-transaction flavour: touch every page of the table at once.
      w = WhereSpec{};
    }
    return "upd " + t.name + " " + w.Text() + " " + s1.Text() + " " + s2.Text();
  }

  std::string DeleteOp(const MTable& t) {
    // Whole-table deletes are rare; they make the remaining session dull.
    const WhereSpec w = RandomWhere(t, g_.Chance(20));
    return "del " + t.name + " " + w.Text();
  }

  std::string CheckOp(const MTable& t) {
    const std::vector<MTable>& v = s_.Tables();
    const MTable& u =
        v[static_cast<size_t>(g_.Pick(0, static_cast<int64_t>(v.size()) - 1))];
    const int64_t roll = g_.Pick(0, 129);
    if (roll >= 100) {
      // Raw-row checks that stay on the pipeline sinks.
      if (roll < 108) {
        return "check topn " + t.name + " " + I(g_.Pick(1, 300)) + " " +
               I(g_.Chance(50)
                     ? 0
                     : g_.Pick(0, static_cast<int64_t>(t.rows.size())));
      }
      if (roll < 116) {
        return "check winrows " + t.name;
      }
      if (roll < 120) {
        return "check distrows " + t.name;
      }
      if (roll < 126) {
        return "check joinrows " + t.name + " " + u.name;
      }
      return "check fullk " + t.name + " " + u.name;
    }
    if (roll < 8) {
      return "check count " + t.name;
    }
    if (roll < 16) {
      return "check full " + t.name;
    }
    if (roll < 20) {
      return "check scan " + t.name;
    }
    if (roll < 30) {
      const int64_t lo = SomeKey(t);
      return "check range " + t.name + " " + I(lo) + " " +
             I(lo + g_.Pick(0, 3000)) + " " + I(g_.Pick(1, 200)) + " " +
             I(g_.Pick(0, 50));
    }
    if (roll < 38) {
      return "check agg " + t.name;
    }
    if (roll < 42) {
      return "check aggk " + t.name;
    }
    if (roll < 46) {
      return "check strag " + t.name;
    }
    if (roll < 50) {
      return "check distinct " + t.name;
    }
    if (roll < 58) {
      return "check window " + t.name;
    }
    if (roll < 64) {
      return "check sortpad " + t.name + " " + I(g_.Pick(1, 300));
    }
    if (roll < 70) {
      return "check point " + t.name + " " + I(SomeKey(t));
    }
    if (roll < 75) {
      return "check grp " + t.name + " " + I(g_.Pick(0, t.groups - 1));
    }
    if (roll < 79) {
      return "check tag " + t.name + " " + I(g_.Pick(1, kTagCount - 1)) + " " +
             I(SomeKey(t));
    }
    if (roll < 86) {
      return "check joink " + t.name + " " + u.name;
    }
    if (roll < 92) {
      // Many-to-many on g; keep the join output bounded.
      std::map<int64_t, int64_t> ca;
      std::map<int64_t, int64_t> cb;
      for (const auto& [k, r] : t.rows) {
        ++ca[r.g];
      }
      for (const auto& [k, r] : u.rows) {
        ++cb[r.g];
      }
      int64_t out = 0;
      for (const auto& [gk, c] : ca) {
        if (auto it = cb.find(gk); it != cb.end()) {
          out += c * it->second;
        }
      }
      if (out <= 400000LL * scale_) {
        return "check joing " + t.name + " " + u.name;
      }
      return "check joink " + t.name + " " + u.name;
    }
    if (roll < 96) {
      return "check leftk " + t.name + " " + u.name;
    }
    return "check setop " + t.name + " " + u.name + " " + I(g_.Pick(0, 2));
  }

  // A random query for the budget-invariance oracle.
  std::string QueryOp(bool snapshot) {
    std::vector<OocGenTable> tables;
    for (const MTable& t : snapshot ? s_.SnapTables() : s_.Tables()) {
      OocGenTable gt;
      gt.name = t.name;
      gt.rows = static_cast<int64_t>(t.rows.size());
      gt.key_lo = t.rows.empty() ? 1 : t.rows.begin()->first;
      gt.key_hi = t.rows.empty() ? 1 : t.rows.rbegin()->first;
      gt.groups = t.groups;
      gt.v_mod = t.v_mod;
      gt.pad_max = t.pad_hi;
      tables.push_back(std::move(gt));
    }
    if (tables.empty()) {
      return "";
    }
    const OocGenQuery q = GenerateOocQuery(g_, tables, 60000LL * scale_);
    s_.NoteShape(q.shape);
    const int64_t take = g_.Chance(18) ? g_.Pick(0, 40) : -1;
    const int64_t nv = 1 + (g_.Chance(45) ? 1 : 0) + (g_.Chance(15) ? 1 : 0);
    return "q ord=" + I(q.ordered ? 1 : 0) + " heavy=" + I(q.heavy ? 1 : 0) +
           " take=" + I(take) + " nv=" + I(nv) +
           " vs=" + I(g_.Pick(0, 1 << 30)) + " ex=" + I(g_.Chance(20) ? 1 : 0) +
           " snap=" + I(snapshot ? 1 : 0) + " :: " + q.sql;
  }

  // An op to put under an exhaustive failure sweep.
  std::string SweepOp(const MTable& t) {
    std::string inner;
    const int64_t roll = g_.Pick(0, 99);
    if (roll < 22) {
      inner = UpdateOp(t);
    } else if (roll < 38) {
      inner = BulkInsertOp(t, false);
    } else if (roll < 46) {
      inner = DeleteOp(t);
    } else if (roll < 54) {
      const MTable* src = PickTable();
      inner = CopyOp(t, *src);
    } else if (roll < 70) {
      inner = CheckOp(t);
    } else if (roll < 92) {
      inner = QueryOp(false);
    } else if (roll < 96) {
      inner = "checkpoint";
    } else {
      inner = "evict 0";
    }
    if (inner.empty()) {
      return "";
    }
    const bool mem = g_.Chance(65);
    return std::string("sweep ") + (mem ? "mem " : "io ") +
           I(mem ? g_.Pick(8, 400) : g_.Pick(4, 80)) + " :: " + inner;
  }

  std::string IndexOp(const MTable& t) {
    std::vector<std::string> kinds;
    for (const char* k : {"uk", "g", "tk", "pad", "gv"}) {
      if (!t.index_kinds.contains(k)) {
        kinds.emplace_back(k);
      }
    }
    if (kinds.empty()) {
      return "";
    }
    return "index " + t.name + " ix" + std::to_string(index_counter_++) + " " +
           g_.OneOf(kinds);
  }

  std::string KnobOp() {
    const int64_t roll = g_.Pick(0, 99);
    if (roll < 40) {
      const std::vector<int64_t> pools = {16 << 10,  32 << 10,  64 << 10,
                                          128 << 10, 512 << 10, 4 << 20};
      return "knob pool " + I(g_.OneOf(pools));
    }
    if (roll < 60) {
      const std::vector<int64_t> ops = {0,        4 << 10,   16 << 10,
                                        64 << 10, 256 << 10, 1 << 20};
      return "knob opmem " + I(g_.OneOf(ops));
    }
    if (roll < 78) {
      return "knob spill " + I(g_.Pick(0, 1));
    }
    if (roll < 86) {
      return "knob columnar " + I(g_.Chance(50) ? 0 : (64 << 10));
    }
    if (roll < 93) {
      return "knob route " + I(g_.Pick(0, 1));
    }
    return "knob temp " + I(g_.Chance(60) ? 0 : (g_.Pick(64, 4096) << 10));
  }

  int table_counter_{0};
  int index_counter_{0};

 private:
  Gen& g_;
  OocSession& s_;
  OocTrace& t_;
  const Flavor& f_;
  int scale_;
  const OocRunOptions& options_;
};

// TINYLAMB_OOC_FUZZ_SKIP_OPS: ','-separated op kinds the generator must not
// emit (e.g. "compact"), so a campaign can leave an area with a known
// shallow crash alone and spend its sessions elsewhere.
bool SkippedByEnv(std::string_view op) {
  const char* env = std::getenv("TINYLAMB_OOC_FUZZ_SKIP_OPS");
  if (env == nullptr || env[0] == '\0') {
    return false;
  }
  if (op.starts_with("sweep ")) {
    if (const size_t sep = op.find(" :: "); sep != std::string_view::npos) {
      if (SkippedByEnv(op.substr(sep + 4))) {
        return true;
      }
    }
  }
  const std::string_view kind = op.substr(0, op.find(' '));
  std::string_view list(env);
  while (!list.empty()) {
    const size_t comma = list.find(',');
    if (list.substr(0, comma) == kind) {
      return true;
    }
    if (comma == std::string_view::npos) {
      break;
    }
    list.remove_prefix(comma + 1);
  }
  return false;
}

std::string GenerateSession(Gen& g, OocSession& s, OocTrace& trace,
                            const Flavor& f, int scale, double budget_seconds,
                            const OocRunOptions& options) {
  Driver d(g, s, trace, f, scale, options);
  const auto start = std::chrono::steady_clock::now();
  auto elapsed = [&]() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                         start)
        .count();
  };

  // Setup: two or three tables, optional pre-load indexes, initial load.
  const int64_t initial_tables = f.query >= 400 ? 3 : 2;
  for (int64_t i = 0; i < initial_tables; ++i) {
    if (std::string r = d.Do(d.NewTableOp()); !r.empty()) {
      return r;
    }
  }
  for (const MTable& t : std::vector<MTable>(s.Tables())) {
    if (g.Chance(35)) {
      if (std::string op = d.IndexOp(t); !op.empty()) {
        if (std::string r = d.Do(op); !r.empty()) {
          return r;
        }
      }
    }
  }
  for (size_t ti = 0; ti < s.Tables().size(); ++ti) {
    int64_t target = g.Pick(150, 900) * scale;
    if (Driver::Narrow(s.Tables()[ti])) {
      target *= g.Pick(2, 4 * f.rows_factor);  // NOLINT(bugprone-implicit-widening-of-multiplication-result)
    // NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result) operands bounded
    }
    while (ti < s.Tables().size() &&
           std::cmp_less(s.Tables()[ti].rows.size(), target) &&
           elapsed() < budget_seconds / 3) {
      std::string op = d.BulkInsertOp(s.Tables()[ti], false);
      if (op.empty()) {
        break;
      }
      if (std::string r = d.Do(op); !r.empty()) {
        return r;
      }
      if (g.Chance(f.tiny_pool ? 8 : 30)) {
        if (std::string r = d.Do("commit"); !r.empty()) {
          return r;
        }
      }
    }
  }
  if (!f.tiny_pool || g.Chance(50)) {
    if (std::string r = d.Do("commit"); !r.empty()) {
      return r;
    }
  }

  enum class Kind {
    kInsert,
    kValues,
    kCopy,
    kDup,
    kUpdate,
    kDelete,
    kCheck,
    kQuery,
    kCommit,
    kAbort,
    kCrash,
    kReopen,
    kCheckpoint,
    kEvict,
    kCompact,
    kKnob,
    kInject,
    kSweep,
    kSnap,
    kIndex,
    kAnalyze,
    kDdl
  };
  const std::vector<std::pair<int, Kind>> weights = {
      {f.insert, Kind::kInsert},
      {f.values, Kind::kValues},
      {f.copy, Kind::kCopy},
      {f.dup, Kind::kDup},
      {f.update, Kind::kUpdate},
      {f.del, Kind::kDelete},
      {f.check, Kind::kCheck},
      {f.query, Kind::kQuery},
      {f.commit, Kind::kCommit},
      {f.abort, Kind::kAbort},
      {f.crash, Kind::kCrash},
      {f.reopen, Kind::kReopen},
      {f.checkpoint, Kind::kCheckpoint},
      {f.evict, Kind::kEvict},
      {f.compact, Kind::kCompact},
      {f.knob, Kind::kKnob},
      {f.inject, Kind::kInject},
      {f.sweep, Kind::kSweep},
      {f.snap, Kind::kSnap},
      {f.index, Kind::kIndex},
      {f.analyze, Kind::kAnalyze},
      {f.ddl, Kind::kDdl}};
  int64_t total_weight = 0;
  for (const auto& [w, kind] : weights) {
    total_weight += w;
  }

  const int64_t steps = f.query >= 400 ? g.Pick(60, 160) : g.Pick(40, 90);
  for (int64_t step = 0; step < steps; ++step) {
    if (elapsed() > budget_seconds) {
      if (options.verbose) {
        std::cerr << "[ooc] time budget reached after " << step << " steps\n";
      }
      break;
    }
    const MTable* t = d.PickTable();
    std::string op;
    if (t == nullptr) {
      op = d.NewTableOp();
    } else {
      int64_t roll = g.Pick(0, total_weight - 1);
      Kind kind = Kind::kCheck;
      for (const auto& [w, k] : weights) {
        if (roll < w) {
          kind = k;
          break;
        }
        roll -= w;
      }
      switch (kind) {
        case Kind::kInsert:
          op = d.BulkInsertOp(*t, false);
          break;
        case Kind::kValues:
          op = d.BulkInsertOp(*t, true);
          break;
        case Kind::kCopy: {
          const MTable* src = d.PickTable();
          op = d.CopyOp(*t, *src);
          break;
        }
        case Kind::kDup:
          if (t->unique_k && !t->rows.empty()) {
            int64_t k = d.SomeKey(*t);
            if (!t->rows.contains(k)) {
              k = t->rows.begin()->first;
            }
            op = "ins_dup " + t->name + " " + I(k);
          }
          break;
        case Kind::kUpdate:
          op = d.UpdateOp(*t);
          break;
        case Kind::kDelete:
          op = d.DeleteOp(*t);
          break;
        case Kind::kCheck:
          op = d.CheckOp(*t);
          break;
        case Kind::kQuery:
          op = d.QueryOp(s.SnapOpen() && g.Chance(20));
          break;
        case Kind::kCommit:
          op = "commit";
          break;
        case Kind::kAbort:
          op = "abort";
          break;
        case Kind::kCrash:
          op = "crash";
          break;
        case Kind::kReopen:
          op = "reopen";
          break;
        case Kind::kCheckpoint:
          op = "checkpoint";
          break;
        case Kind::kEvict:
          op = "evict " + I(g.Chance(50) ? 0 : g.Pick(0, 256 << 10));
          break;
        case Kind::kCompact:
          if (!s.SnapOpen() && !t->rows.empty()) {
            op = "compact " + t->name;
          }
          break;
        case Kind::kKnob:
          op = d.KnobOp();
          break;
        case Kind::kInject: {
          // One fault armed for the very next statement.
          const bool mem = g.Chance(60);
          const std::string arm = std::string("inject ") +
                                  (mem ? "mem " : "io ") +
                                  I(g.Pick(1, mem ? 400 : 40));
          std::string next;
          const int64_t which = g.Pick(0, 9);
          if (which < 2) {
            next = d.UpdateOp(*t);
          } else if (which < 4) {
            next = d.BulkInsertOp(*t, false);
          } else if (which < 6) {
            next = d.CheckOp(*t);
          } else if (which < 8) {
            next = d.QueryOp(false);
          } else if (which < 9) {
            next = "commit";
          } else {
            next = "checkpoint";
          }
          if (!next.empty()) {
            if (std::string r = d.Do(arm); !r.empty()) {
              return r;
            }
            op = next;
          }
          break;
        }
        case Kind::kSweep:
          op = d.SweepOp(*t);
          break;
        case Kind::kSnap:
          if (!s.SnapOpen()) {
            op = std::string("snap open ") + (g.Chance(50) ? "ro" : "rw");
          } else if (g.Chance(70)) {
            const std::vector<MTable>& st = s.SnapTables();
            if (!st.empty()) {
              op = "snap check " + g.OneOf(st).name;
            }
          } else {
            op = "snap close";
          }
          break;
        case Kind::kIndex:
          if (!s.SnapOpen()) {
            op = d.IndexOp(*t);
          }
          break;
        case Kind::kAnalyze:
          op = "analyze " + t->name;
          break;
        case Kind::kDdl:
          if (!s.SnapOpen() && s.Tables().size() >= 3 && g.Chance(50)) {
            op = "drop " + t->name;
          } else if (s.Tables().size() < 4) {
            op = d.NewTableOp();
          }
          break;
      }
    }
    if (op.empty() || SkippedByEnv(op)) {
      continue;
    }
    if (std::string r = d.Do(op); !r.empty()) {
      return r;
    }
    if (op.starts_with("drop ")) {
      // An uncommitted DROP TABLE is already visible to other transactions
      // (a snapshot reader opened now fails with "table not found"; see the
      // findings report).  Commit at once so sessions get past it.
      if (std::string r = d.Do("commit"); !r.empty()) {
        return r;
      }
    }
  }

  // Epilogue: everything committed must survive a crash and a clean reopen.
  if (s.SnapOpen()) {
    if (std::string r = d.Do("snap close"); !r.empty()) {
      return r;
    }
  }
  if (std::string r = d.Do("commit"); !r.empty()) {
    return r;
  }
  for (const char* boundary : {"crash", "reopen"}) {
    if (std::string r = d.Do(boundary); !r.empty()) {
      return r;
    }
    for (const MTable& t : std::vector<MTable>(s.Tables())) {
      if (std::string r = d.Do("check full " + t.name); !r.empty()) {
        return r;
      }
    }
  }
  return "";
}

std::string ProfileText(const OocProfile& p) {
  return "pool=" + std::to_string(p.pool_bytes) +
         " columnar=" + std::to_string(p.columnar_bytes) +
         " opmem=" + std::to_string(p.op_memory_bytes) +
         " spill=" + std::to_string(p.always_spill ? 1 : 0) +
         " temp=" + std::to_string(p.temp_limit_bytes) +
         " route=" + std::to_string(p.force_relational ? 1 : 0);
}

bool ParseProfile(std::string_view text, OocProfile* p) {
  for (const std::string& kv : SplitTokens(text)) {
    const size_t eq = kv.find('=');
    if (eq == std::string::npos) {
      return false;
    }
    const std::string key = kv.substr(0, eq);
    int64_t v = 0;
    if (!ParseI64(kv.substr(eq + 1), &v) || v < 0) {
      return false;
    }
    if (key == "pool") {
      p->pool_bytes = static_cast<size_t>(v);
    } else if (key == "columnar") {
      p->columnar_bytes = static_cast<size_t>(v);
    } else if (key == "opmem") {
      p->op_memory_bytes = static_cast<size_t>(v);
    } else if (key == "spill") {
      p->always_spill = v != 0;
    } else if (key == "temp") {
      p->temp_limit_bytes = static_cast<size_t>(v);
    } else if (key == "route") {
      p->force_relational = v != 0;
    } else {
      return false;
    }
  }
  return true;
}

bool IgnoredSignature(std::string_view report) {
  const char* env = std::getenv("TINYLAMB_OOC_FUZZ_IGNORE");
  if (env == nullptr || env[0] == '\0') {
    return false;
  }
  const std::string signature = OocFailureSignature(report);
  std::string_view list(env);
  while (!list.empty()) {
    const size_t semi = list.find(';');
    const std::string_view item = list.substr(0, semi);
    // Require a meaningful token length; a 1-character ignore entry would
    // suppress nearly every finding by substring coincidence.
    if (item.size() >= 8 && signature.find(item) != std::string::npos) {
      return true;
    }
    if (semi == std::string_view::npos) {
      break;
    }
    list.remove_prefix(semi + 1);
  }
  return false;
}

// Tracked bytes per pool; a session must hand back everything it reserved
// once its database is gone (accounting drift silently shrinks or inflates
// the budget of every later query in a long-running server).
struct PoolUsage {
  size_t storage{0};
  size_t columnar{0};
  size_t query{0};
  size_t tx{0};
  size_t emergency{0};

  static PoolUsage Now() {
    const ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
    return {.storage = b.StorageUsed(),
            .columnar = b.ColumnarUsed(),
            .query = b.QueryUsed(),
            .tx = b.TxUsed(),
            .emergency = b.EmergencyUsed()};
  }
  bool operator==(const PoolUsage&) const = default;
  [[nodiscard]] std::string Text() const {
    return "storage=" + std::to_string(storage) +
           " columnar=" + std::to_string(columnar) +
           " query=" + std::to_string(query) + " tx=" + std::to_string(tx) +
           " emergency=" + std::to_string(emergency);
  }
};

std::string DriftReport(const PoolUsage& before) {
  if (EnvU64("TINYLAMB_OOC_FUZZ_NO_LEAK_CHECK", 0) != 0) {
    return "";
  }
  const PoolUsage after = PoolUsage::Now();
  if (after == before) {
    return "";
  }
  std::string report =
      "memory pool accounting drifted across the session: before {" +
      before.Text() + "} after {" + after.Text() + "}\n";
  if (IgnoredSignature(report)) {
    return "";
  }
  return report;
}

std::string Digitless(std::string_view text) {
  std::string out;
  bool in_digits = false;
  for (const char c : text) {
    if (std::isdigit(static_cast<unsigned char>(c)) != 0) {
      if (!in_digits) {
        out.push_back('#');
      }
      in_digits = true;
    } else {
      out.push_back(c);
      in_digits = false;
    }
  }
  return out;
}

}  // namespace

void OocStats::Merge(const OocStats& o) {
  statements += o.statements;
  checks += o.checks;
  rows_inserted += o.rows_inserted;
  commits += o.commits;
  aborts += o.aborts;
  crashes += o.crashes;
  reopens += o.reopens;
  checkpoints += o.checkpoints;
  evictions += o.evictions;
  compactions += o.compactions;
  injections += o.injections;
  tolerated_failures += o.tolerated_failures;
  snapshot_checks += o.snapshot_checks;
  indexes += o.indexes;
  diff_queries += o.diff_queries;
  diff_variants += o.diff_variants;
  diff_tolerated += o.diff_tolerated;
  diff_skipped += o.diff_skipped;
  partial_cursors += o.partial_cursors;
  explain_checks += o.explain_checks;
  pure_pipeline_queries += o.pure_pipeline_queries;
  spilled_queries += o.spilled_queries;
  sweeps += o.sweeps;
  sweep_points += o.sweep_points;
  sweep_fired += o.sweep_fired;
  evicted_pages += o.evicted_pages;
  uncommitted_spill_evictions += o.uncommitted_spill_evictions;
  // Telemetry counters are process-cumulative: keep the high-water mark.
  spill_bytes = std::max(spill_bytes, o.spill_bytes);
  spill_ops = std::max(spill_ops, o.spill_ops);
  max_partition_depth = std::max(max_partition_depth, o.max_partition_depth);
  skew_fallbacks = std::max(skew_fallbacks, o.skew_fallbacks);
  max_db_bytes = std::max(max_db_bytes, o.max_db_bytes);
  max_deleted_fds = std::max(max_deleted_fds, o.max_deleted_fds);
  for (const auto& [name, n] : o.shapes) {
    shapes[name] += n;
  }
  for (const auto& [name, n] : o.operators) {
    operators[name] += n;
  }
  for (const auto& [name, n] : o.routes) {
    routes[name] += n;
  }
  ignored_findings += o.ignored_findings;
}

std::string OocStats::Summary() const {
  std::ostringstream o;
  o << "statements=" << statements << " checks=" << checks
    << " rows_inserted=" << rows_inserted << " commits=" << commits
    << " aborts=" << aborts << " crashes=" << crashes << " reopens=" << reopens
    << " checkpoints=" << checkpoints << " evictions=" << evictions
    << " compactions=" << compactions << " injections=" << injections
    << " tolerated_failures=" << tolerated_failures
    << " snapshot_checks=" << snapshot_checks << " indexes=" << indexes
    << " diff_queries=" << diff_queries << " diff_variants=" << diff_variants
    << " diff_tolerated=" << diff_tolerated << " diff_skipped=" << diff_skipped
    << " partial_cursors=" << partial_cursors
    << " explain_checks=" << explain_checks
    << " pure_pipeline_queries=" << pure_pipeline_queries
    << " spilled_queries=" << spilled_queries << " sweeps=" << sweeps
    << " sweep_points=" << sweep_points << " sweep_fired=" << sweep_fired
    << " evicted_pages=" << evicted_pages
    << " uncommitted_spill_evictions=" << uncommitted_spill_evictions
    << " spill_bytes=" << spill_bytes << " spill_ops=" << spill_ops
    << " max_partition_depth=" << max_partition_depth
    << " skew_fallbacks=" << skew_fallbacks << " max_db_bytes=" << max_db_bytes
    << " max_deleted_fds=" << max_deleted_fds
    << " ignored_findings=" << ignored_findings;
  return o.str();
}

std::vector<std::string> OocFlavorNames() {
  std::vector<std::string> names;
  for (const Flavor& f : kFlavors) {
    names.emplace_back(f.name);
  }
  return names;
}

std::string RunOocSession(const uint8_t* data, size_t size, uint64_t seed,
                          const OocRunOptions& options, OocStats* stats,
                          OocTrace* trace) {
  KnobGuard guard;
  Gen g(data, size, seed);
  OocTrace local;
  OocTrace& t = trace != nullptr ? *trace : local;
  t = OocTrace{};
  const int drawn = static_cast<int>(g.Pick(0, kFlavorCount - 1));
  const Flavor& flavor =
      kFlavors[options.flavor >= 0 && options.flavor < kFlavorCount
                   ? options.flavor
                   : drawn];
  t.profile = GenProfile(g, flavor);
  if (options.on_profile) {
    options.on_profile(t.profile);
  }
  if (options.verbose) {
    std::cerr << "[ooc] flavor " << flavor.name << " profile "
              << ProfileText(t.profile) << "\n";
  }
  const PoolUsage before = PoolUsage::Now();
  std::string report;
  {
    OocSession session(t.profile, options.verbose, stats);
    if (std::string r = session.Open(); !r.empty()) {
      return r == "skip" ? "" : r;
    }
    const int scale = options.scale > 0
                          ? options.scale
                          : static_cast<int>(std::max<uint64_t>(
                                1, EnvU64("TINYLAMB_OOC_FUZZ_SCALE", 1)));
    const double budget =
        options.budget_seconds > 0
            ? options.budget_seconds
            : static_cast<double>(EnvU64("TINYLAMB_OOC_FUZZ_SECONDS", 20));
    report = GenerateSession(g, session, t, flavor, scale, budget, options);
  }
  if (report.empty()) {
    report = DriftReport(before);
  }
  return report;
}

std::string RunOocIteration(const uint8_t* data, size_t size, uint64_t seed,
                            bool verbose, OocStats* stats, OocTrace* trace,
                            double budget_seconds) {
  OocRunOptions options;
  options.verbose = verbose;
  options.budget_seconds = budget_seconds;
  return RunOocSession(data, size, seed, options, stats, trace);
}

std::string ReplayOocTrace(const OocTrace& trace, const OocRunOptions& options,
                           OocStats* stats) {
  KnobGuard guard;
  if (options.on_profile) {
    options.on_profile(trace.profile);
  }
  const PoolUsage before = PoolUsage::Now();
  {
    OocSession session(trace.profile, options.verbose, stats);
    session.SetLenient(options.lenient);
    if (std::string r = session.Open(); !r.empty()) {
      return r == "skip" ? "" : r;
    }
    for (size_t i = 0; i < trace.ops.size(); ++i) {
      if (options.on_op) {
        options.on_op(trace.ops[i]);
      }
      if (std::string r = session.Apply(trace.ops[i]); !r.empty()) {
        return "op " + std::to_string(i) + " `" + Shorten(trace.ops[i], 160) +
               "`: " + r;
      }
    }
  }
  return DriftReport(before);
}

std::string ReplayOocTrace(const OocTrace& trace, bool verbose,
                           OocStats* stats) {
  OocRunOptions options;
  options.verbose = verbose;
  return ReplayOocTrace(trace, options, stats);
}

std::string OocFailureSignature(std::string_view report) {
  std::string prefix;
  // Replay prefix: "op 12 `<op text>`: ".
  if (report.starts_with("op ")) {
    if (const size_t at = report.find("`: "); at != std::string_view::npos) {
      report.remove_prefix(at + 3);
    }
  }
  // Sweep label: "sweep mem `<op>` with failure #3: <inner report>".
  if (report.starts_with("sweep ")) {
    const bool mem = report.starts_with("sweep mem");
    if (const size_t at = report.find("failure #");
        at != std::string_view::npos) {
      if (const size_t colon = report.find(": ", at);
          colon != std::string_view::npos) {
        prefix = mem ? "sweep mem / " : "sweep io / ";
        report.remove_prefix(colon + 2);
      }
    }
  }
  const size_t nl = report.find('\n');
  std::string_view head = report.substr(0, nl);
  std::string_view rest =
      nl == std::string_view::npos ? std::string_view{} : report.substr(nl + 1);
  // Drop the statement text and op text that follow the message.
  size_t cut = head.find(": ");
  const size_t tick = head.find(" `");
  cut = std::min(tick, cut);
  head = head.substr(0, cut);
  std::string out = prefix + Digitless(head);
  while (!rest.empty() && rest.front() == ' ') {
    rest.remove_prefix(1);
  }
  if (rest.starts_with("prepare: ") || rest.starts_with("stream: ")) {
    std::string_view line = rest.substr(0, rest.find('\n'));
    const size_t first = line.find(": ");
    const size_t second = line.find(": ", first + 2);
    out += " | " + Digitless(line.substr(0, second));
  } else if (rest.starts_with("status: ")) {
    out += " | " +
           Digitless(rest.substr(0, std::min<size_t>(rest.find('\n'), 120)));
  }
  return out;
}

OocTrace MinimizeOocTrace(const OocTrace& trace, std::string_view report,
                          double budget_seconds, std::string* final_report) {
  const std::string want = OocFailureSignature(report);
  const auto deadline =
      std::chrono::steady_clock::now() +
      std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          std::chrono::duration<double>(budget_seconds));
  auto in_time = [&]() { return std::chrono::steady_clock::now() < deadline; };
  OocRunOptions lenient;
  lenient.lenient = true;
  auto fails = [&](const OocTrace& candidate, std::string* why) {
    const std::string r = ReplayOocTrace(candidate, lenient, nullptr);
    if (r.empty() || OocFailureSignature(r) != want) {
      return false;
    }
    *why = r;
    return true;
  };
  OocTrace best = trace;
  std::string best_report;
  if (!fails(best, &best_report)) {
    // Not reproducible by replay (nondeterministic): nothing to shrink.
    if (final_report != nullptr) {
      final_report->clear();
    }
    return trace;
  }
  // Everything after the failing op is dead weight.
  auto truncate = [&]() {
    if (!best_report.starts_with("op ")) {
      return;
    }
    int64_t at = 0;
    const size_t end = best_report.find(' ', 3);
    if (end != std::string::npos &&
        ParseI64(std::string_view(best_report).substr(3, end - 3), &at) &&
        at >= 0 && static_cast<size_t>(at) + 1 < best.ops.size()) {
      best.ops.resize(static_cast<size_t>(at) + 1);
    }
  };
  truncate();
  size_t chunk = std::max<size_t>(1, best.ops.size() / 2);
  while (in_time()) {
    bool removed = false;
    for (size_t start = 0; start < best.ops.size() && in_time();) {
      OocTrace candidate = best;
      const size_t stop = std::min(candidate.ops.size(), start + chunk);
      candidate.ops.erase(
          candidate.ops.begin() + static_cast<std::ptrdiff_t>(start),
          candidate.ops.begin() + static_cast<std::ptrdiff_t>(stop));
      std::string why;
      if (!candidate.ops.empty() && fails(candidate, &why)) {
        best = std::move(candidate);
        best_report = std::move(why);
        truncate();
        removed = true;
      } else {
        start += chunk;
      }
    }
    if (chunk > 1) {
      chunk /= 2;
    } else if (!removed) {
      break;
    }
  }
  // Relax every profile knob the failure does not need.
  const std::vector<std::function<void(OocProfile&)>> relaxations = {
      [](OocProfile& p) { p.temp_limit_bytes = 0; },
      [](OocProfile& p) { p.force_relational = false; },
      [](OocProfile& p) { p.columnar_bytes = 0; },
      [](OocProfile& p) { p.always_spill = false; },
      [](OocProfile& p) { p.op_memory_bytes = 0; },
      [](OocProfile& p) { p.pool_bytes = 0; },
  };
  for (const auto& relax : relaxations) {
    if (!in_time()) {
      break;
    }
    OocTrace candidate = best;
    relax(candidate.profile);
    std::string why;
    if (!(candidate.profile == best.profile) && fails(candidate, &why)) {
      best = std::move(candidate);
      best_report = std::move(why);
    }
  }
  if (final_report != nullptr) {
    *final_report = best_report;
  }
  return best;
}

// Digest over the seed, profile and op lines. The seed alone cannot regenerate
// the original libFuzzer `data` bytes, so an integrity digest is what actually
// lets replay/minimize detect a hand-edited or merge-corrupted trace.
std::string OocTraceDigest(uint64_t seed, std::string_view profile_text,  // NOLINT(misc-use-internal-linkage)
                           const std::vector<std::string>& ops) {
  std::string canonical = std::to_string(seed) + "\n";
  canonical += profile_text;
  canonical += "\n";
  for (const std::string& op : ops) {
    canonical += op;
    canonical += "\n";
  }
  return digest::ToHex(digest::Sha256Digest(canonical));
}

std::string SerializeOocHeader(uint64_t seed, const OocProfile& profile) {
  return "-- sql_ooc_fuzzer regression\n-- seed: " + std::to_string(seed) +
         "\n-- profile: " + ProfileText(profile) + "\n";
}

std::string SerializeOocTest(uint64_t seed, const OocTrace& trace,
                             const std::string& failure_summary) {
  std::string out = "-- sql_ooc_fuzzer regression\n";
  out += "-- seed: " + std::to_string(seed) + "\n";
  const std::string profile_text = ProfileText(trace.profile);
  out += "-- trace-digest: " + OocTraceDigest(seed, profile_text, trace.ops) +
         "\n";
  out += "-- profile: " + profile_text + "\n";
  out += "-- failure: " + failure_summary + "\n";
  for (const std::string& op : trace.ops) {
    out += op + "\n";
  }
  return out;
}

bool ParseOocTest(std::string_view text, uint64_t* seed, OocTrace* trace,
                  std::string* failure_summary) {
  *trace = OocTrace{};
  failure_summary->clear();
  bool saw_profile = false;
  std::string expected_digest;
  std::string profile_text;
  size_t p = 0;
  while (p < text.size()) {
    size_t nl = text.find('\n', p);
    if (nl == std::string_view::npos) {
      nl = text.size();
    }
    std::string_view line = text.substr(p, nl - p);
    p = nl + 1;
    while (!line.empty() && (line.back() == '\r' || line.back() == ' ')) {
      line.remove_suffix(1);
    }
    if (line.empty()) {
      continue;
    }
    if (line.starts_with("-- seed: ")) {
      // Strict parse like the other fuzzers: reject trailing junk / overflow
      // instead of silently mapping a corrupt regression to 0/ULLONG_MAX.
      const std::string seed_text(line.substr(9));
      try {
        size_t used = 0;
        *seed = std::stoull(seed_text, &used);
        if (used != seed_text.size()) {
          return false;
        }
      } catch (const std::exception&) {
        return false;
      }
    } else if (line.starts_with("-- trace-digest: ")) {
      expected_digest = std::string(line.substr(17));
    } else if (line.starts_with("-- profile: ")) {
      profile_text = std::string(line.substr(12));
      if (!ParseProfile(line.substr(12), &trace->profile)) {
        return false;
      }
      saw_profile = true;
    } else if (line.starts_with("-- failure: ")) {
      *failure_summary = std::string(line.substr(12));
    } else if (line.starts_with("--")) {
      continue;
    } else {
      trace->ops.emplace_back(line);
    }
  }
  if (!saw_profile || trace->ops.empty()) {
    return false;
  }
  if (!expected_digest.empty() &&
      expected_digest != OocTraceDigest(*seed, profile_text, trace->ops)) {
    // A committed trace whose ops/profile were edited after generation no
    // longer matches its recorded seed/digest: refuse to call it a replay.
    return false;
  }
  return true;
}

void OocFuzzTry(const uint8_t* data, size_t size, bool verbose) {
  uint64_t seed = 0x00c0ffee5eedULL;
  for (size_t i = 0; i < size; ++i) {
    seed = (seed * 257) + data[i] + 1;
  }
  OocTrace trace;
  std::string report =
      RunOocIteration(data, size, seed, verbose, nullptr, &trace);
  if (report.empty()) {
    return;
  }
  // Shrink before persisting: a 100-op session is rarely the story.
  std::string verify;
  std::string minimized_report;
  OocTrace minimized = MinimizeOocTrace(
      trace, report,
      static_cast<double>(EnvU64("TINYLAMB_OOC_FUZZ_MINIMIZE_SECONDS", 120)),
      &minimized_report);
  if (minimized_report.empty()) {
    verify = "note: replay of the serialized trace held (nondeterministic)";
  } else {
    verify = "replay reproduces (" + std::to_string(trace.ops.size()) +
             " ops minimized to " + std::to_string(minimized.ops.size()) +
             "): " + minimized_report;
    trace = std::move(minimized);
    report = minimized_report;
  }
  const std::string text = SerializeOocTest(
      seed, trace,
      FormatFuzzFailureSummary("auto-generated by sql_ooc_fuzzer", report));
  const std::string path =
      "sql_ooc_fuzz-repro-" + std::to_string(seed) + ".test";
  EmitFuzzFinding("sql_ooc_fuzz", seed, path, text, report, verify);
}

}  // namespace tinylamb
