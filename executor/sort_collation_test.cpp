/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
//
// Collation-aware ordering across every sort-shaped operator. The contract
// under test is the shared one in type/collation.hpp: NULL placement,
// direction and the resolved collation, so SortExecutor's memcomparable
// encoding, its external merge runs, TopNExecutor's heap, PdqSort,
// IncrementalSort and MergeAppend cannot drift apart.
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exc_shim.hpp"
#include "common/status_or.hpp"
#include "executor/constant_executor.hpp"
#include "executor/executor_base.hpp"
#include "executor/incremental_sort.hpp"
#include "executor/merge_append.hpp"
#include "executor/pdqsort.hpp"
#include "executor/query_memory.hpp"
#include "executor/sort.hpp"
#include "executor/topn.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace {

const Schema& TextSchema() {
  static const Schema schema(
      "synthetic",
      {Column("key", ValueType::kInt64), Column("name", ValueType::kVarChar)});
  return schema;
}

const Schema& NameSchema() {
  static const Schema schema("synthetic",
                             {Column("name", ValueType::kVarChar)});
  return schema;
}

Expression Name() { return ColumnValueExp("name"); }
Expression Key() { return ColumnValueExp("key"); }

// The COLLATE() expression the SQL frontend builds for `ORDER BY x COLLATE
// 'und:ci'`: the collator is a plain string literal (GoogleSQL rejects
// anything else at parse time).
Expression Collate(Expression inner, std::string spec) {
  return std::make_shared<FunctionCallExpression>(
      "collate",
      std::vector<Expression>{std::move(inner),
                              ConstantValueExp(Value(std::move(spec)))});
}

Expression CiName() { return Collate(Name(), "und:ci"); }

SortExecutor::Key CiSortKey(Expression expression, bool ascending = true,
                            std::optional<bool> nulls_first = std::nullopt) {
  return SortExecutor::Key{.expression = std::move(expression),
                           .ascending = ascending,
                           .nulls_first = nulls_first,
                           .collation = Collation::kCaseInsensitive};
}

TopNExecutor::Key CiTopNKey(Expression expression, bool ascending = true,
                            std::optional<bool> nulls_first = std::nullopt) {
  return TopNExecutor::Key{.expression = std::move(expression),
                           .ascending = ascending,
                           .nulls_first = nulls_first,
                           .collation = Collation::kCaseInsensitive};
}

// make_shared forwards through a template parameter pack, where a braced
// key list is a non-deduced argument, so every key vector is named here.
std::vector<SortExecutor::Key> CiSortKeys(
    Expression expression, bool ascending = true,
    std::optional<bool> nulls_first = std::nullopt) {
  return std::vector<SortExecutor::Key>{
      CiSortKey(std::move(expression), ascending, nulls_first)};
}

std::vector<SortExecutor::Key> SortKeys(
    Expression expression, bool ascending = true,
    std::optional<bool> nulls_first = std::nullopt) {
  return std::vector<SortExecutor::Key>{
      SortExecutor::Key{.expression = std::move(expression),
                        .ascending = ascending,
                        .nulls_first = nulls_first,
                        .collation = Collation::kNone}};
}

std::vector<TopNExecutor::Key> CiTopNKeys(
    Expression expression, bool ascending = true,
    std::optional<bool> nulls_first = std::nullopt) {
  return std::vector<TopNExecutor::Key>{
      CiTopNKey(std::move(expression), ascending, nulls_first)};
}

std::vector<TopNExecutor::Key> TopNKeys(
    Expression expression, bool ascending = true,
    std::optional<bool> nulls_first = std::nullopt) {
  return std::vector<TopNExecutor::Key>{
      TopNExecutor::Key{.expression = std::move(expression),
                        .ascending = ascending,
                        .nulls_first = nulls_first,
                        .collation = Collation::kNone}};
}

std::vector<SortExecutor::Key> TwoKeys() {
  return std::vector<SortExecutor::Key>{
      SortExecutor::Key{.expression = Key(),
                        .ascending = true,
                        .nulls_first = std::nullopt,
                        .collation = Collation::kNone},
      CiSortKey(CiName())};
}

std::vector<std::string> DrainNames(const Executor& source) {
  std::vector<std::string> out;
  Row row;
  RowPosition position;
  while (source->Next(&row, &position)) {
    out.emplace_back(row[0].value.varchar_value);
  }
  return out;
}

std::vector<int64_t> DrainKeys(const Executor& source) {
  std::vector<int64_t> out;
  Row row;
  RowPosition position;
  while (source->Next(&row, &position)) {
    out.push_back(row[0].value.int_value);
  }
  return out;
}

Value CiValue(std::string text) {
  return Value(std::move(text))
      .WithCollation(static_cast<uint8_t>(Collation::kCaseInsensitive));
}

std::vector<Row> SampleRows() {
  return {Row({Value(1), Value("Banana")}), Row({Value(2), Value("apple")}),
          Row({Value(3), Value("Cherry")}), Row({Value(4), Value("APPLE")}),
          Row({Value(5), Value("cherry")}), Row({Value(6), Value("date")}),
          Row({Value(7), Value("Egg")})};
}

// A collated ASC order over SampleRows: apple == APPLE (peers under the
// fold, input order kept) < Banana < Cherry == cherry (peers) < date < Egg.
const std::vector<int64_t>& CiExpectedOrder() {
  static const std::vector<int64_t> order{2, 4, 1, 3, 5, 6, 7};
  return order;
}

// DESC reverses the group order; the engine's sorts are stable, so the peers
// inside a group keep their input order rather than mirroring the ASC one.
const std::vector<int64_t>& CiExpectedDescendingOrder() {
  static const std::vector<int64_t> order{7, 6, 3, 5, 1, 2, 4};
  return order;
}

std::vector<std::string> SampleNames() {
  return {"Banana", "apple", "Cherry", "APPLE", "cherry", "date", "Egg"};
}

// RAII: temporarily replaces the process-wide query working-memory budget so
// Materialize() is forced down the spill path. Restored even on failure, so a
// spilled test cannot corrupt the later ones.
class ScopedQueryMemory {
 public:
  explicit ScopedQueryMemory(size_t limit)
      : original_(QueryMemoryBudget::Global().Limit()) {
    QueryMemoryBudget::Global().ResetForTest(limit);
  }
  ~ScopedQueryMemory() { QueryMemoryBudget::Global().ResetForTest(original_); }
  ScopedQueryMemory(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory(ScopedQueryMemory&&) = delete;
  ScopedQueryMemory& operator=(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory& operator=(ScopedQueryMemory&&) = delete;

 private:
  size_t original_;
};

// Stamps a distinct RowPosition per row so a spilled run's position handling
// can be checked alongside the ordering.
class PositionedRows final : public ExecutorBase {
 public:
  explicit PositionedRows(std::vector<Row> rows) : rows_(std::move(rows)) {}
  size_t NextBatch(DataChunk* dst, size_t max_rows) override {
    dst->Reset();
    while (offset_ < rows_.size() && dst->Size() < max_rows) {
      dst->Append(rows_[offset_],
                  RowPosition(11, static_cast<slot_t>(offset_)));
      ++offset_;
    }
    return dst->Size();
  }
  void Dump(std::ostream& out, int /*indent*/) const override {
    out << "PositionedRows";
  }

 private:
  std::vector<Row> rows_{};
  size_t offset_{0};
};

// A key expression that fails for one specific value, so the sort's error
// plumbing can be exercised on the collated path (a fold must never swallow a
// key-evaluation failure).
class ExplodingKey final : public ExpressionBase {
 public:
  [[nodiscard]] TypeTag Type() const override {
    return TypeTag::kFunctionCallExp;
  }
  [[nodiscard]] StatusOr<Value> TryEvaluate(
      const Row& row, const Schema& schema) const override {
    const int offset = schema.Offset(ColumnName("name"));
    if (offset < 0) {
      return StatusError(StatusCode::kInvalidArgument, "no name column");
    }
    const std::string_view text =
        row.values_[static_cast<size_t>(offset)].value.varchar_value;
    if (text == "boom") {
      return StatusError(StatusCode::kInvalidArgument,
                         "collation key evaluation failed");
    }
    return Value(std::string(text));
  }
  [[nodiscard]] Value Evaluate(const Row& row,
                               const Schema& schema) const override {
    return ExcShimUnwrap(TryEvaluate(row, schema), "ExplodingKey");
  }
  [[nodiscard]] std::string ToString() const override {
    return "exploding_key";
  }
  void Dump(std::ostream& out) const override { out << "exploding_key"; }
};

std::vector<SortExecutor::Key> ExplodingSortKeys() {
  return std::vector<SortExecutor::Key>{
      SortExecutor::Key{.expression = std::make_shared<ExplodingKey>(),
                        .ascending = true,
                        .nulls_first = std::nullopt,
                        .collation = Collation::kCaseInsensitive}};
}

std::vector<TopNExecutor::Key> ExplodingTopNKeys() {
  return std::vector<TopNExecutor::Key>{
      TopNExecutor::Key{.expression = std::make_shared<ExplodingKey>(),
                        .ascending = true,
                        .nulls_first = std::nullopt,
                        .collation = Collation::kCaseInsensitive}};
}

}  // namespace

// ======================== SortExecutor (memcomparable encoding) ==============

TEST(SortCollationTest, BinaryOrderIsUnchanged) {
  const std::vector<Row> rows{Row({Value("B")}), Row({Value("a")}),
                              Row({Value("C")}), Row({Value("A")})};
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<ConstantExecutor>(rows), NameSchema(), SortKeys(Name()));
  // Every uppercase ASCII letter precedes 'a' (0x61): the engine default is
  // still byte order.
  EXPECT_EQ(DrainNames(sort), (std::vector<std::string>{"A", "B", "C", "a"}));
}

TEST(SortCollationTest, CollatedAndBinarySortDisagreeOnCaseOnlyKeys) {
  const std::vector<Row> rows = SampleRows();
  auto binary = std::make_shared<SortExecutor>(
      std::make_shared<ConstantExecutor>(rows), TextSchema(), SortKeys(Name()));
  auto collated =
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     TextSchema(), CiSortKeys(CiName()));
  const std::vector<int64_t> binary_order = DrainKeys(binary);
  const std::vector<int64_t> collated_order = DrainKeys(collated);
  // Byte-wise "APPLE" leads (uppercase first); folded, "apple" leads.
  ASSERT_EQ(binary_order.front(), 4U);
  ASSERT_EQ(collated_order.front(), 2U);
  EXPECT_NE(binary_order, collated_order);
  EXPECT_EQ(collated_order, CiExpectedOrder());
}

TEST(SortCollationTest, CollatedSortDescendingReversesTheGroupOrder) {
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<ConstantExecutor>(SampleRows()), TextSchema(),
      CiSortKeys(CiName(), false));
  EXPECT_EQ(DrainKeys(sort), CiExpectedDescendingOrder());
}

TEST(SortCollationTest, CollatedSortIsStableWithinFoldedPeers) {
  const std::vector<Row> rows{Row({Value("b")}), Row({Value("B")}),
                              Row({Value("a")}), Row({Value("A")})};
  auto sort =
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), CiSortKeys(CiName()));
  // Peers keep their input order, so a LIMIT over this order can never pick
  // an arbitrary member of a folded group.
  EXPECT_EQ(DrainNames(sort), (std::vector<std::string>{"a", "A", "b", "B"}));
}

TEST(SortCollationTest, CollatedKeyHonoursExplicitNullsFirstAndLast) {
  const std::vector<Row> rows{Row({Value("b")}), Row({Value()}),
                              Row({Value("a")})};
  for (const bool nulls_first : {true, false}) {
    auto sort = std::make_shared<SortExecutor>(
        std::make_shared<ConstantExecutor>(rows), NameSchema(),
        CiSortKeys(CiName(), true, nulls_first));
    const std::vector<std::string> names = DrainNames(sort);
    ASSERT_EQ(names.size(), 3U);
    if (nulls_first) {
      EXPECT_TRUE(names[0].empty());
      EXPECT_EQ(names[1], "a");
      EXPECT_EQ(names[2], "b");
    } else {
      EXPECT_EQ(names[0], "a");
      EXPECT_EQ(names[1], "b");
      EXPECT_TRUE(names[2].empty());
    }
  }
  // The engine default still derives NULL placement from the direction.
  auto defaulted =
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), CiSortKeys(CiName(), true));
  EXPECT_TRUE(DrainNames(defaulted)[0].empty());
}

TEST(SortCollationTest, CollatedSortOfEmptyAndSingleRowInput) {
  {
    auto sort = std::make_shared<SortExecutor>(
        std::make_shared<ConstantExecutor>(std::vector<Row>{}), NameSchema(),
        CiSortKeys(CiName()));
    EXPECT_TRUE(DrainNames(sort).empty());
  }
  {
    auto sort = std::make_shared<SortExecutor>(
        std::make_shared<ConstantExecutor>(std::vector<Row>{Row({Value("Z")})}),
        NameSchema(), CiSortKeys(CiName()));
    EXPECT_EQ(DrainNames(sort), (std::vector<std::string>{"Z"}));
  }
}

TEST(SortCollationTest, CollatedSortKeepsDuplicatesAndBoundaryBytesIntact) {
  const std::vector<Row> rows{Row({Value("")}),     Row({Value("")}),
                              Row({Value("\x7F")}), Row({Value("\xC3\x84")}),
                              Row({Value("zz")}),   Row({Value("\x01")})};
  auto sort =
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), CiSortKeys(CiName()));
  // Empty strings, control bytes, DEL and non-ASCII all compare byte-wise
  // under the ASCII fold; nothing is dropped or merged.
  EXPECT_EQ(DrainNames(sort), (std::vector<std::string>{"", "", "\x01", "zz",
                                                        "\x7F", "\xC3\x84"}));
}

TEST(SortCollationTest, CollatedSortLeavesIntervalShapedTextByteOrdered) {
  const std::vector<Row> rows{Row({Value("1 year")}), Row({Value("12 MONTHS")}),
                              Row({Value("2 DAYs")})};
  const std::vector<std::string> collated = DrainNames(
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), CiSortKeys(CiName())));
  const std::vector<std::string> binary = DrainNames(
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), SortKeys(Name())));
  // No locale or interval collation exists, so the collated order equals the
  // binary order: '1' < '2' and '1 year' < '12 MONTHS' byte-wise.
  const std::vector<std::string> expected{"1 year", "12 MONTHS", "2 DAYs"};
  EXPECT_EQ(collated, expected);
  EXPECT_EQ(binary, expected);
}

TEST(SortCollationTest, CollatedSortPreservesNonAsciiUtf8Order) {
  const std::vector<Row> rows{Row({Value("\xD0\x93")}),  // Г
                              Row({Value("Z")}),         // Z
                              Row({Value("\xC3\x84")}),  // Ä
                              Row({Value("a")})};
  const std::vector<std::string> collated = DrainNames(
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), CiSortKeys(CiName())));
  const std::vector<std::string> binary = DrainNames(
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), SortKeys(Name())));
  // The fold reorders the ASCII pair ('Z' becomes 'z', so it now follows
  // 'a'), but it cannot reach the multi-byte rows: those keep their UTF-8
  // byte order and stay above every ASCII string, exactly as the binary
  // collation orders them.
  EXPECT_EQ(collated,
            (std::vector<std::string>{"a", "Z", "\xC3\x84", "\xD0\x93"}));
  EXPECT_EQ(binary,
            (std::vector<std::string>{"Z", "a", "\xC3\x84", "\xD0\x93"}));
}

TEST(SortCollationTest, CollatedSortFoldsEveryKeyOfACompositeOrder) {
  const std::vector<Row> rows{
      Row({Value(1), Value("b")}), Row({Value(1), Value("A")}),
      Row({Value(0), Value("C")}), Row({Value(0), Value("a")})};
  auto sort = std::make_shared<SortExecutor>(
      std::make_shared<ConstantExecutor>(rows), TextSchema(), TwoKeys());
  EXPECT_EQ(DrainKeys(sort), (std::vector<int64_t>{0, 0, 1, 1}));
  // The integer key keeps binary order (collation does not apply to it) and
  // the folded text breaks the tie: {C, a} sorts as a < C and {b, A} as A < b.
  auto names = std::make_shared<SortExecutor>(
      std::make_shared<ConstantExecutor>(rows), TextSchema(), TwoKeys());
  std::vector<std::string> texts;
  Row row;
  RowPosition position;
  while (names->Next(&row, &position)) {
    texts.emplace_back(row[1].value.varchar_value);
  }
  EXPECT_EQ(texts, (std::vector<std::string>{"a", "C", "A", "b"}));
}

TEST(SortCollationTest, CollatedSortPropagatesKeyEvaluationErrors) {
  const std::vector<Row> rows{Row({Value("a")}), Row({Value("boom")}),
                              Row({Value("b")})};
  auto sort =
      std::make_shared<SortExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), ExplodingSortKeys());
  EXPECT_FALSE(sort->Next(nullptr, nullptr));
  EXPECT_NE(sort->GetStatus(), Status::kSuccess);
}

// ======================== external merge sort ===============================

TEST(SortCollationTest, ExternalMergeSortKeepsTheCollatedOrder) {
  // 4000 wide rows under a 16 KiB budget: Materialize() spills sorted runs and
  // merges them, so the collated encoding must survive the round trip through
  // disk (Row serialization drops the collation tag; the key expression
  // re-attaches it on the way back in).
  std::vector<Row> rows;
  rows.reserve(4000);
  for (int64_t i = 0; i < 4000; ++i) {
    std::string payload =
        SampleNames()[static_cast<size_t>(i) % SampleNames().size()] + "-" +
        std::to_string(i);
    payload.resize(48, 'x');
    rows.emplace_back(std::vector<Value>{Value(i), Value(std::move(payload))});
  }
  ScopedQueryMemory memory(16384);
  auto sort =
      std::make_shared<SortExecutor>(std::make_shared<PositionedRows>(rows),
                                     TextSchema(), CiSortKeys(CiName()));
  std::vector<std::string> names;
  Row row;
  RowPosition position;
  size_t count = 0;
  while (sort->Next(&row, &position)) {
    names.emplace_back(row[1].value.varchar_value);
    // Every row keeps its position across the spill/merge.
    EXPECT_EQ(row[0].value.int_value, static_cast<int64_t>(position.slot));
    EXPECT_EQ(position.page_id, static_cast<page_id_t>(11));
    ++count;
  }
  EXPECT_EQ(count, 4000U);
  for (size_t i = 1; i < names.size(); ++i) {
    EXPECT_LE(CompareWithCollation(CiValue(names[i - 1]), CiValue(names[i])), 0)
        << "out of order at " << i << ": " << names[i - 1] << " then "
        << names[i];
  }
}

TEST(SortCollationTest, ExternalMergeSortDescendingCollatedOrder) {
  std::vector<Row> rows;
  for (int64_t i = 0; i < 2000; ++i) {
    std::string name =
        SampleNames()[static_cast<size_t>(i) % SampleNames().size()] + "-" +
        std::to_string(i);
    rows.emplace_back(std::vector<Value>{Value(i), Value(std::move(name))});
  }
  ScopedQueryMemory memory(16384);
  auto sort =
      std::make_shared<SortExecutor>(std::make_shared<PositionedRows>(rows),
                                     TextSchema(), CiSortKeys(CiName(), false));
  std::vector<std::string> names;
  Row row;
  RowPosition position;
  while (sort->Next(&row, &position)) {
    names.emplace_back(row[1].value.varchar_value);
  }
  ASSERT_EQ(names.size(), 2000U);
  for (size_t i = 1; i < names.size(); ++i) {
    EXPECT_GE(CompareWithCollation(CiValue(names[i - 1]), CiValue(names[i])), 0)
        << "out of order at " << i;
  }
}

// ======================== TopN ==============================================

TEST(SortCollationTest, TopNKeepsTheCollatedTopRows) {
  const std::vector<Row> rows{Row({Value("Banana")}), Row({Value("apple")}),
                              Row({Value("Cherry")}), Row({Value("APPLE")}),
                              Row({Value("date")}),   Row({Value("Egg")})};
  auto topn =
      std::make_shared<TopNExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), CiTopNKeys(CiName()), 3, 0);
  EXPECT_EQ(DrainNames(topn),
            (std::vector<std::string>{"apple", "APPLE", "Banana"}));
}

TEST(SortCollationTest, TopNOffsetAndNullPlacementFollowTheContract) {
  const std::vector<Row> rows{Row({Value("b")}), Row({Value()}),
                              Row({Value("A")}), Row({Value("a")})};
  {
    auto topn = std::make_shared<TopNExecutor>(
        std::make_shared<ConstantExecutor>(rows), NameSchema(),
        CiTopNKeys(CiName(), true, false), 2, 1);
    // NULLS LAST: the collated order is A == a (peers, input order) < b <
    // NULL, so OFFSET 1 LIMIT 2 keeps "a" and "b".
    EXPECT_EQ(DrainNames(topn), (std::vector<std::string>{"a", "b"}));
  }
  {
    auto topn = std::make_shared<TopNExecutor>(
        std::make_shared<ConstantExecutor>(rows), NameSchema(),
        CiTopNKeys(CiName(), true, true), 1, 0);
    EXPECT_EQ(DrainNames(topn), (std::vector<std::string>{""}));
  }
  {
    auto topn = std::make_shared<TopNExecutor>(
        std::make_shared<ConstantExecutor>(rows), NameSchema(),
        CiTopNKeys(CiName(), false, true), 1, 0);
    // DESC with NULLS FIRST keeps the NULL row.
    EXPECT_EQ(DrainNames(topn), (std::vector<std::string>{""}));
  }
  {
    // The engine default still derives NULL placement from the direction.
    auto topn = std::make_shared<TopNExecutor>(
        std::make_shared<ConstantExecutor>(rows), NameSchema(),
        CiTopNKeys(CiName(), false), 1, 0);
    EXPECT_EQ(DrainNames(topn), (std::vector<std::string>{"b"}));
  }
}

TEST(SortCollationTest, TopNWithTiesGroupsFoldedPeersAtTheBoundary) {
  const std::vector<Row> rows{Row({Value("b")}), Row({Value("A")}),
                              Row({Value("a")}), Row({Value("B")})};
  auto topn = std::make_shared<TopNExecutor>(
      std::make_shared<ConstantExecutor>(rows), NameSchema(),
      CiTopNKeys(CiName()), 1, 0, /*with_ties=*/true);
  // "A"/"a" are peers under the fold, so FETCH FIRST WITH TIES returns both
  // instead of stopping at the first one.
  EXPECT_EQ(DrainNames(topn), (std::vector<std::string>{"A", "a"}));
}

TEST(SortCollationTest, TopNWithTiesKeepsBinaryPeersSeparate) {
  const std::vector<Row> rows{Row({Value("b")}), Row({Value("A")}),
                              Row({Value("a")}), Row({Value("B")})};
  auto topn = std::make_shared<TopNExecutor>(
      std::make_shared<ConstantExecutor>(rows), NameSchema(), TopNKeys(Name()),
      1, 0, /*with_ties=*/true);
  // Binary order puts "A" first and "B" is not its peer, so only "A" comes
  // back: the tie rule did not silently inherit the fold.
  EXPECT_EQ(DrainNames(topn), (std::vector<std::string>{"A"}));
}

TEST(SortCollationTest, TopNEmptyInputZeroLimitAndOffsetPastTheEnd) {
  {
    auto topn = std::make_shared<TopNExecutor>(
        std::make_shared<ConstantExecutor>(std::vector<Row>{}), NameSchema(),
        CiTopNKeys(CiName()), 5, 0);
    EXPECT_TRUE(DrainNames(topn).empty());
  }
  {
    auto topn = std::make_shared<TopNExecutor>(
        std::make_shared<ConstantExecutor>(SampleRows()), NameSchema(),
        CiTopNKeys(CiName()), 0, 0);
    EXPECT_TRUE(DrainNames(topn).empty());
  }
  {
    auto topn = std::make_shared<TopNExecutor>(
        std::make_shared<ConstantExecutor>(SampleRows()), NameSchema(),
        CiTopNKeys(CiName()), 2, 99);
    EXPECT_TRUE(DrainNames(topn).empty());
  }
}

TEST(SortCollationTest, CollatedKeyOverANonTextColumnKeepsExactOrder) {
  // The fold only applies to VARCHAR, so a collated integer key keeps exact
  // numeric order: 1..7 ascending, and the exact reverse descending.
  auto topn = std::make_shared<TopNExecutor>(
      std::make_shared<ConstantExecutor>(SampleRows()), TextSchema(),
      CiTopNKeys(Collate(Key(), "und:ci")), 3, 0);
  EXPECT_EQ(DrainKeys(topn), (std::vector<int64_t>{1, 2, 3}));
  auto descending = std::make_shared<TopNExecutor>(
      std::make_shared<ConstantExecutor>(SampleRows()), TextSchema(),
      CiTopNKeys(Collate(Key(), "und:ci"), false), 3, 0);
  EXPECT_EQ(DrainKeys(descending), (std::vector<int64_t>{7, 6, 5}));
}

TEST(SortCollationTest, TopNAndSortAgreeOnTheCollatedNameLimit) {
  for (size_t limit = 1; limit <= SampleNames().size(); ++limit) {
    auto sort = std::make_shared<SortExecutor>(
        std::make_shared<ConstantExecutor>(SampleRows()), TextSchema(),
        CiSortKeys(Collate(Name(), "und:ci")));
    const std::vector<int64_t> sorted = DrainKeys(sort);
    auto topn = std::make_shared<TopNExecutor>(
        std::make_shared<ConstantExecutor>(SampleRows()), TextSchema(),
        CiTopNKeys(Collate(Name(), "und:ci")), limit, 0);
    EXPECT_EQ(DrainKeys(topn),
              std::vector<int64_t>(
                  sorted.begin(),
                  sorted.begin() + static_cast<std::ptrdiff_t>(limit)));
  }
}

TEST(SortCollationTest, TopNPropagatesKeyEvaluationErrors) {
  const std::vector<Row> rows{Row({Value("a")}), Row({Value("boom")})};
  auto topn =
      std::make_shared<TopNExecutor>(std::make_shared<ConstantExecutor>(rows),
                                     NameSchema(), ExplodingTopNKeys(), 2, 0);
  EXPECT_FALSE(topn->Next(nullptr, nullptr));
  EXPECT_NE(topn->GetStatus(), Status::kSuccess);
}

// ======================== the other sort shapes =============================

TEST(SortCollationTest, PdqSortFoldsACollatedKey) {
  std::vector<Row> rows{Row({Value("Banana")}), Row({Value("apple")}),
                        Row({Value("Cherry")}), Row({Value("APPLE")})};
  PdqSort::Sort(rows, NameSchema(), CiSortKeys(CiName()));
  std::vector<std::string> names;
  names.reserve(rows.size());
  for (const Row& row : rows) {
    names.emplace_back(row[0].value.varchar_value);
  }
  EXPECT_EQ(names,
            (std::vector<std::string>{"apple", "APPLE", "Banana", "Cherry"}));
}

TEST(SortCollationTest, IncrementalSortTreatsFoldedPeersAsOneRun) {
  // The input is already ordered on the *collated* name prefix, so every row
  // is one run and the suffix key decides the order inside it. A binary
  // prefix would have split "APPLE" from "apple" into two runs and left the
  // suffix unsorted (3, 1, 2, 4).
  const std::vector<Row> rows{
      Row({Value(3), Value("APPLE")}), Row({Value(1), Value("apple")}),
      Row({Value(2), Value("banana")}), Row({Value(4), Value("Banana")})};
  auto sort = std::make_shared<IncrementalSortExecutor>(
      std::make_shared<ConstantExecutor>(rows), TextSchema(),
      CiSortKeys(CiName()), CiSortKeys(Key()));
  EXPECT_EQ(DrainKeys(sort), (std::vector<int64_t>{1, 3, 2, 4}));
  auto binary_prefix = std::make_shared<IncrementalSortExecutor>(
      std::make_shared<ConstantExecutor>(rows), TextSchema(), SortKeys(Name()),
      CiSortKeys(Key()));
  EXPECT_EQ(DrainKeys(binary_prefix), (std::vector<int64_t>{3, 1, 2, 4}));
}

TEST(SortCollationTest, MergeAppendFoldsACollatedKey) {
  // The output schema must name the same (unnamed) relation as the children
  // for MergeAppend's column binding, as executor/merge_append_test.cpp does.
  const Schema schema("", {Column("name", ValueType::kVarChar)});
  const std::vector<Executor> children{
      std::make_shared<ConstantExecutor>(
          std::vector<Row>{Row({Value("Banana")}), Row({Value("date")})}),
      std::make_shared<ConstantExecutor>(
          std::vector<Row>{Row({Value("apple")}), Row({Value("Cherry")})})};
  auto merge = std::make_shared<MergeAppendExecutor>(
      children, std::vector<Schema>{schema, schema}, schema,
      CiSortKeys(Collate(Name(), "und:ci")));
  EXPECT_EQ(DrainNames(merge),
            (std::vector<std::string>{"apple", "Banana", "Cherry", "date"}));
}

}  // namespace tinylamb
