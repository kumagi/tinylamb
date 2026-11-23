/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Direct unit tests for the plan-cache expression plumbing in
// query/plan_cache.hpp: ParameterSlot evaluation metadata and rebinding,
// the bindable-literal classifier, and the CloneWithPreparedValues tree
// walker across every expression node kind.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "query/plan_cache.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {

std::shared_ptr<const PreparedValues> Vals(std::vector<Value> values) {
  return std::make_shared<const PreparedValues>(std::move(values));
}

}  // namespace

TEST(VolatileFingerprintTest, SaturationIsBoundedAndCannotReviveStalePlans) {
  PreparedPlanSessionCache session;
  ScopedPreparedPlanSession scope(&session);
  for (size_t i = 0; i < kVolatileFingerprintCapacity; ++i) {
    EXPECT_TRUE(MarkFingerprintVolatile("fold-" + std::to_string(i)));
  }
  EXPECT_FALSE(MarkFingerprintVolatile("overflow"));
  EXPECT_TRUE(session.volatile_fingerprints.empty());
  for (size_t i = 0; i < kVolatileFingerprintCapacity * 2; ++i) {
    const std::string key = "fold-" + std::to_string(i);
    EXPECT_FALSE(MarkFingerprintVolatile(key));
    DemoteVolatileFingerprint(key, true);
    EXPECT_TRUE(IsVolatileSpecializedPlan(key));
  }
  EXPECT_TRUE(IsVolatileSpecializedPlan("globally-cached-stale-plan"));
  EXPECT_TRUE(session.volatile_fingerprints.empty());
  PreparedPlanSessionCache other;
  ScopedPreparedPlanSession other_scope(&other);
  EXPECT_FALSE(IsVolatileSpecializedPlan("fold-0"));
  EXPECT_TRUE(MarkFingerprintVolatile("temporary"));
  DemoteVolatileFingerprint("temporary", true);
  EXPECT_FALSE(IsVolatileSpecializedPlan("temporary"));
}

TEST(VolatileFingerprintTest, FailedCompileKeepsOnlyAppliedFoldsVolatile) {
  PreparedPlanSessionCache session;
  ScopedPreparedPlanSession scope(&session);
  const std::string key = "failed-statement";
  for (const bool apply_fold : {false, true}) {
    bool folded = false;
    const auto compile = [&] {
      ScopedFoldFingerprint guard(key, folded);
      EXPECT_TRUE(IsVolatileSpecializedPlan(key));
      folded = apply_fold;
      throw std::runtime_error("compile failed");
    };
    EXPECT_THROW(compile(), std::runtime_error);
    EXPECT_EQ(IsVolatileSpecializedPlan(key), apply_fold);
  }
}

TEST(PlanCacheParameterSlotTest, EvaluateReturnsSlotValue) {
  auto values = Vals({Value(int64_t{7}), Value(std::string("s"))});
  const ParameterSlot slot(1, ValueType::kVarChar, values);

  Row row({});
  Schema schema("t", {Column("a", ValueType::kInt64)});
  EXPECT_EQ(slot.Type(), TypeTag::kConstantValue);
  EXPECT_EQ(slot.Index(), 1U);
  EXPECT_EQ(slot.Evaluate(row, schema), Value(std::string("s")));
  EXPECT_EQ(slot.Evaluate(&row, schema, nullptr, schema),
            Value(std::string("s")));
  EXPECT_EQ(slot.ResultType(schema).GetType(), TypeTag::kVarChar);
  EXPECT_EQ(slot.ResultType(schema, schema).GetType(), TypeTag::kVarChar);
  EXPECT_TRUE(slot.TouchedColumns().empty());
  EXPECT_EQ(slot.ToString(), "<?1>");
  std::ostringstream dump;
  slot.Dump(dump);
  EXPECT_EQ(dump.str(), "<?1>");
}

TEST(PlanCacheParameterSlotTest, ResultTypeCoversAllValueTypes) {
  auto values = Vals({});
  const std::vector<std::pair<ValueType, TypeTag>> cases = {
      {ValueType::kInt64, TypeTag::kBigInt},
      {ValueType::kDouble, TypeTag::kDouble},
      {ValueType::kVarChar, TypeTag::kVarChar},
      {ValueType::kDate, TypeTag::kDate},
      {ValueType::kArray, TypeTag::kArray},
      {ValueType::kNull, TypeTag::kBigInt},
  };
  for (const auto& [value_type, tag] : cases) {
    const ParameterSlot slot(0, value_type, values);
    SCOPED_TRACE(static_cast<int>(value_type));
    EXPECT_EQ(slot.ResultType(Schema("t", {})).GetType(), tag);
    EXPECT_EQ(slot.ResultType(Schema("t", {}), Schema("t", {})).GetType(), tag);
  }
}

TEST(PlanCacheParameterSlotTest, BindUpdatesSniffedValue) {
  ParameterSlot slot(0, Value(int64_t{1}));
  Row row({});
  EXPECT_EQ(slot.Evaluate(row, Schema("t", {})), Value(int64_t{1}));
  EXPECT_EQ(slot.AsConstantValue().GetValue(), Value(int64_t{1}));
  slot.Bind(Value(int64_t{9}));
  EXPECT_EQ(slot.Evaluate(row, Schema("t", {})), Value(int64_t{9}));
  EXPECT_EQ(slot.AsConstantValue().GetValue(), Value(int64_t{9}));
  EXPECT_EQ(slot.ToString(), "9");
}

TEST(SelectReuseThresholdTest, RetiresOnlyExtremeRegressions) {
  EXPECT_FALSE(SelectReuseExceededThreshold(1'000'000, 0));
  EXPECT_FALSE(SelectReuseExceededThreshold(10'000'000, 1'000'000));
  EXPECT_FALSE(SelectReuseExceededThreshold(40'000'000, 5'000'000));
  EXPECT_TRUE(SelectReuseExceededThreshold(80'000'000, 5'000'000));
  EXPECT_FALSE(
      SelectReuseExceededThreshold(6'000'000'000ULL, 5'000'000'000ULL));
}

TEST(PlanCacheParameterSlotTest, RebindPointsAtNewValues) {
  auto first = Vals({Value(int64_t{1})});
  auto second = Vals({Value(int64_t{2})});
  const ParameterSlot slot(0, ValueType::kInt64, first);
  Expression rebound = slot.Rebind(second);
  ASSERT_EQ(rebound->Type(), TypeTag::kConstantValue);
  Row row({});
  EXPECT_EQ(rebound->Evaluate(row, Schema("t", {})), Value(int64_t{2}));
}

TEST(PlanCacheParameterSlotTest, IsBindableLiteralClassifiesValueTypes) {
  EXPECT_TRUE(IsBindableLiteral(Value(int64_t{1})));
  EXPECT_TRUE(IsBindableLiteral(Value(1.5)));
  EXPECT_TRUE(IsBindableLiteral(Value(std::string("s"))));
  EXPECT_TRUE(IsBindableLiteral(Value::Date("2026-01-02")));
  EXPECT_FALSE(IsBindableLiteral(Value()));
  EXPECT_FALSE(IsBindableLiteral(Value::Array({Value(int64_t{1})}, "INT64")));
}

TEST(PlanCacheParameterSlotTest, StructSlotsFollowFieldLayout) {
  for (bool has_flags : {false, true}) {
    SCOPED_TRACE(has_flags);
    const size_t width = has_flags ? 3 : 2;
    const std::vector<Value> parameters{Value(11), Value(22), Value(33)};
    std::vector<Expression> args;
    for (size_t field = 0; field < parameters.size(); ++field) {
      args.push_back(ConstantValueExp(Value("field" + std::to_string(field))));
      args.push_back(ConstantValueExp(parameters[field]));
      if (has_flags) {
        args.push_back(ConstantValueExp(Value(0)));
      }
    }
    const auto expression = FunctionCallExp("__struct_json__", args);
    size_t cursor = 0;
    bool ok = true;
    const auto numbered =
        SlotizeLiterals(expression, &cursor, &ok, &parameters);
    ASSERT_TRUE(ok);
    ASSERT_EQ(cursor, parameters.size());
    const auto& numbered_args = numbered->AsFunctionCallExpression().Args();
    for (size_t field = 0; field < parameters.size(); ++field) {
      const size_t index = field * width;
      EXPECT_EQ(numbered_args[index], args[index]);
      const auto* slot =
          dynamic_cast<const ParameterSlot*>(numbered_args[index + 1].get());
      ASSERT_NE(slot, nullptr);
      EXPECT_EQ(slot->Index(), field);
      if (has_flags) {
        EXPECT_EQ(numbered_args[index + 2], args[index + 2]);
      }
    }
  }
}

TEST(PlanCacheParameterSlotTest, ContainsParameterSlotWalksEveryNodeKind) {
  auto values = Vals({Value(int64_t{1})});
  Schema schema("t", {Column("a", ValueType::kInt64)});

  Expression plain_slot =
      std::make_shared<ParameterSlot>(0, ValueType::kInt64, values);
  EXPECT_TRUE(ContainsParameterSlot(plain_slot));
  EXPECT_FALSE(ContainsParameterSlot(Expression{}));
  EXPECT_FALSE(ContainsParameterSlot(ConstantValueExp(Value(int64_t{1}))));
  EXPECT_FALSE(ContainsParameterSlot(ColumnValueExp("a")));

  const auto slot = [] {
    return std::make_shared<ParameterSlot>(0, ValueType::kInt64,
                                           Vals({Value(int64_t{1})}));
  };

  EXPECT_TRUE(ContainsParameterSlot(
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kAdd, slot())));
  EXPECT_TRUE(
      ContainsParameterSlot(UnaryExpressionExp(slot(), UnaryOperation::kNot)));
  EXPECT_TRUE(ContainsParameterSlot(
      UnaryExpressionExp(slot(), UnaryOperation::kIsNull)));
  EXPECT_TRUE(ContainsParameterSlot(
      AggregateExpressionExp(AggregationType::kSum, slot(), false)));
  EXPECT_TRUE(ContainsParameterSlot(CaseExpressionExp(
      {{slot(), ConstantValueExp(Value(int64_t{1}))}}, ColumnValueExp("a"))));
  EXPECT_TRUE(ContainsParameterSlot(
      InExpressionExp(slot(), {ConstantValueExp(Value(int64_t{1}))})));
  EXPECT_TRUE(ContainsParameterSlot(
      FunctionCallExp("concat", {ColumnValueExp("a"), slot()})));
  // The walkers descend into cast nodes: SlotizeLiterals still refuses to
  // parameterize inside a CAST today, but a slot that reaches one (hand-built
  // IR, a future slotizer) must be found and rebound, not silently kept.
  EXPECT_TRUE(ContainsParameterSlot(CastExpressionExp(slot(), "INT64", false)));
  // Nested two levels deep.
  EXPECT_TRUE(ContainsParameterSlot(
      BinaryExpressionExp(UnaryExpressionExp(slot(), UnaryOperation::kNot),
                          BinaryOperation::kAdd, ColumnValueExp("a"))));
}

TEST(PlanCacheParameterSlotTest, CloneWithPreparedValuesRewritesSlotsInPlace) {
  auto values = Vals({Value(int64_t{42})});
  Schema schema("t", {Column("a", ValueType::kInt64)});
  Row row({Value(int64_t{1})});

  const auto slot = [&values] {
    return std::make_shared<ParameterSlot>(0, ValueType::kInt64, values);
  };

  Expression binary =
      BinaryExpressionExp(ColumnValueExp("a"), BinaryOperation::kAdd, slot());
  Expression cloned = CloneWithPreparedValues(binary, values);
  EXPECT_EQ(cloned->Evaluate(row, schema), Value(int64_t{43}));

  Expression unary = UnaryExpressionExp(slot(), UnaryOperation::kNot);
  EXPECT_EQ(CloneWithPreparedValues(unary, values)->Evaluate(row, schema),
            Value(int64_t{0}));

  Expression aggregate =
      AggregateExpressionExp(AggregationType::kSum, slot(), false);
  // Slots are rebound (not removed), so a successful descent is observed as
  // the node being rebuilt rather than returned untouched.
  Expression cloned_aggregate = CloneWithPreparedValues(aggregate, values);
  EXPECT_NE(cloned_aggregate.get(), aggregate.get());
  EXPECT_TRUE(ContainsParameterSlot(cloned_aggregate));

  Expression searched = CaseExpressionExp(
      {{slot(), ConstantValueExp(Value(int64_t{9}))}}, slot());
  Expression cloned_case = CloneWithPreparedValues(searched, values);
  EXPECT_EQ(cloned_case->Evaluate(row, schema), Value(int64_t{9}));

  Expression in_list =
      InExpressionExp(slot(), {slot(), ConstantValueExp(Value(int64_t{5}))});
  Expression cloned_in_list = CloneWithPreparedValues(in_list, values);
  EXPECT_NE(cloned_in_list.get(), in_list.get());
  EXPECT_TRUE(ContainsParameterSlot(cloned_in_list));

  // A slot under a CAST is rebound too: CAST(42 AS INT64) evaluates to 42.
  Expression cast = CastExpressionExp(slot(), "INT64", false);
  EXPECT_EQ(CloneWithPreparedValues(cast, values)->Evaluate(row, schema),
            Value(int64_t{42}));

  // Trees without slots are returned untouched.
  Expression plain = ConstantValueExp(Value(int64_t{3}));
  EXPECT_EQ(CloneWithPreparedValues(plain, values), plain);
}

// P295: ContainsParameterSlot descends into the WHERE filter, HAVING
// condition, inner ORDER BY / GROUP BY, and secondary/trailing arguments of
// an aggregate, but CloneWithPreparedValues rebuilt aggregates as
// (type, child, distinct) only.  A slot sitting in one of those attributes
// was reported as present and then silently deleted together with the
// attribute; every attribute must survive the rebuild and rebind.
TEST(PlanCacheParameterSlotTest,
     CloneWithPreparedValuesKeepsAggregateAttributes) {
  auto values = Vals({Value(int64_t{0})});
  const auto slot = [&values] {
    return std::make_shared<ParameterSlot>(0, ValueType::kInt64, values);
  };

  auto aggregate = std::make_shared<AggregateExpression>(
      AggregationType::kSum, ColumnValueExp("a"), false);
  aggregate->SetIgnoreNulls(true);
  aggregate->SetWhereFilter(BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kGreaterThan, slot()));
  aggregate->SetHaving(AggregateHavingModifier::kMax, slot());
  WindowOrderTerm term;
  term.expression = slot();
  aggregate->SetInnerOrderBy({term});
  aggregate->SetInnerGroupBy({ColumnValueExp("a")});
  ASSERT_TRUE(ContainsParameterSlot(Expression{aggregate}));

  Expression cloned = CloneWithPreparedValues(Expression{aggregate}, values);
  ASSERT_TRUE(cloned->Type() == TypeTag::kAggregateExp);
  const auto& out = cloned->AsAggregateExpression();
  EXPECT_TRUE(out.IgnoreNulls());
  ASSERT_TRUE(out.HasWhereFilter());
  ASSERT_TRUE(out.WhereFilter()->Type() == TypeTag::kBinaryExp);
  const Expression& bound_filter_rhs =
      out.WhereFilter()->AsBinaryExpression().Right();
  const auto* rhs_slot =
      dynamic_cast<const ParameterSlot*>(bound_filter_rhs.get());
  ASSERT_TRUE(rhs_slot != nullptr);
  EXPECT_EQ(rhs_slot->Evaluate(Row(), Schema()), Value(int64_t{0}));
  EXPECT_TRUE(out.Having() == AggregateHavingModifier::kMax);
  EXPECT_TRUE(out.HavingCondition() != nullptr);
  EXPECT_EQ(out.InnerOrderBy().size(), 1U);
  EXPECT_TRUE(out.InnerOrderBy()[0].expression != nullptr);
  EXPECT_EQ(out.InnerGroupBy().size(), 1U);
}

// Lean proof: lean/TinylambOpt/ResearchProofs/PlanCacheToggleInvalidation.lean
// (sound)
TEST(PlanCacheToggleInvalidationTest, BothCachesCheckAllMemorySettings) {
  PreparedPlanSessionCache session;
  ScopedPreparedPlanSession scope(&session);
  const auto settings = PlanMemorySettings::Current();
  auto expect_miss = [&](PlanMemorySettings old) {
    auto plan = std::make_shared<CompiledPlan>();
    plan->memory_settings = old;
    plan->opt_bits = CurrentPlanOptToggleBits();
    const std::string sql = "memory-settings-test";
    StoreThreadCompiledPlan(sql, plan);
    PreparedPlanCache::Instance().Store(sql, plan);
    EXPECT_EQ(FindThreadCompiledPlan(sql, nullptr, 0), nullptr);
    EXPECT_EQ(PreparedPlanCache::Instance().Find(sql, nullptr, 0), nullptr);
  };
  expect_miss({.process_limit=settings.process_limit ^ 1U, .operator_limit=settings.operator_limit,
               .always_spill=settings.always_spill});
  expect_miss({.process_limit=settings.process_limit, .operator_limit=settings.operator_limit ^ 1U,
               .always_spill=settings.always_spill});
  expect_miss({.process_limit=settings.process_limit, .operator_limit=settings.operator_limit,
               .always_spill=!settings.always_spill});
}

TEST(PlanCacheToggleInvalidationTest, OptBitsIsolateCachedPlansPerToggle) {
  struct ScopedUnset {
    const char* name;
    std::string prev;
    bool had_prev{false};
    explicit ScopedUnset(const char* n) : name(n) {
      if (const char* cur = std::getenv(name)) {
        prev = cur;
        had_prev = true;
      }
      ::unsetenv(name);
    }
    ~ScopedUnset() {
      if (had_prev) {
        ::setenv(name, prev.c_str(), 1);
      } else {
        ::unsetenv(name);
      }
    }
  };

  ScopedUnset guard("TINYLAMB_DISABLE_ROW_IN_MASK");
  PreparedPlanSessionCache session;
  ScopedPreparedPlanSession session_scope(&session);

  const auto* dummy_db = reinterpret_cast<const Database*>(0x123456);
  const uint64_t epoch = 42;
  const std::string fp = "SELECT * FROM t WHERE (a, b) IN ((1, 2))";

  const uint64_t bits_on = CurrentPlanOptToggleBits();
  auto plan_on = std::make_shared<CompiledPlan>();
  plan_on->database = dummy_db;
  plan_on->epoch = epoch;
  plan_on->opt_bits = bits_on;
  StoreThreadCompiledPlan(fp, plan_on);
  PreparedPlanCache::Instance().Store(fp, plan_on);

  EXPECT_EQ(FindThreadCompiledPlan(fp, dummy_db, epoch, bits_on), plan_on);
  EXPECT_EQ(PreparedPlanCache::Instance().Find(fp, dummy_db, epoch, bits_on),
            plan_on);

  ::setenv("TINYLAMB_DISABLE_ROW_IN_MASK", "1", 1);
  const uint64_t bits_off = CurrentPlanOptToggleBits();
  EXPECT_NE(bits_off, bits_on);
  EXPECT_NE(PlanCacheKeyWithOptBits(fp, bits_off),
            PlanCacheKeyWithOptBits(fp, bits_on));

  // Changing the toggle must NOT hit the stale ON-mode plan.
  EXPECT_EQ(FindThreadCompiledPlan(fp, dummy_db, epoch), nullptr);
  EXPECT_EQ(PreparedPlanCache::Instance().Find(fp, dummy_db, epoch), nullptr);

  auto plan_off = std::make_shared<CompiledPlan>();
  plan_off->database = dummy_db;
  plan_off->epoch = epoch;
  plan_off->opt_bits = bits_off;
  StoreThreadCompiledPlan(fp, plan_off);
  PreparedPlanCache::Instance().Store(fp, plan_off);

  EXPECT_EQ(FindThreadCompiledPlan(fp, dummy_db, epoch), plan_off);
  EXPECT_EQ(FindThreadCompiledPlan(fp, dummy_db, epoch, bits_on), plan_on);
  EXPECT_EQ(PreparedPlanCache::Instance().Find(fp, dummy_db, epoch), plan_off);
  EXPECT_EQ(PreparedPlanCache::Instance().Find(fp, dummy_db, epoch, bits_on),
            plan_on);

  ForgetThreadCompiledPlan(fp, bits_on);
  EXPECT_EQ(FindThreadCompiledPlan(fp, dummy_db, epoch, bits_on), nullptr);
  EXPECT_EQ(FindThreadCompiledPlan(fp, dummy_db, epoch, bits_off), plan_off);
  ForgetThreadCompiledPlan(fp, bits_off);
}

}  // namespace tinylamb
