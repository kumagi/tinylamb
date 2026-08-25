/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/expression_eval.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "query/evaluation_context_impl.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

struct FuzzStream {
  const uint8_t* data;
  size_t size;
  size_t offset{0};

  uint8_t NextU8() {
    if (offset < size) {
      return data[offset++];
    }
    return 0;
  }

  int64_t NextI64() {
    uint64_t val = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
      val = (val << 8) | NextU8();
    }
    return static_cast<int64_t>(val);
  }

  double NextDouble() {
    int64_t raw = NextI64();
    double d = 0.0;
    std::memcpy(&d, &raw, sizeof(d));
    return d;
  }

  bool NextBool() {
    return (NextU8() % 2) != 0;
  }

  bool Exhausted() const {
    return offset >= size;
  }
};

Value GenerateValue(FuzzStream& stream) {
  const uint8_t type_pick = stream.NextU8() % 5;
  switch (type_pick) {
    case 0:
      return Value();  // Null
    case 1: {
      const uint8_t special = stream.NextU8() % 6;
      if (special == 0) return Value(int64_t{0});
      if (special == 1) return Value(int64_t{1});
      if (special == 2) return Value(int64_t{-1});
      if (special == 3) return Value(std::numeric_limits<int64_t>::max());
      if (special == 4) return Value(std::numeric_limits<int64_t>::min());
      return Value(stream.NextI64());
    }
    case 2: {
      const uint8_t special = stream.NextU8() % 6;
      if (special == 0) return Value(0.0);
      if (special == 1) return Value(1.0);
      if (special == 2) return Value(-1.0);
      if (special == 3) return Value(std::numeric_limits<double>::infinity());
      if (special == 4) return Value(std::numeric_limits<double>::quiet_NaN());
      return Value(stream.NextDouble());
    }
    case 3:
      return Value(stream.NextBool());
    case 4: {
      const uint8_t len = stream.NextU8() % 8;
      std::string s;
      for (size_t i = 0; i < len; ++i) {
        s.push_back(static_cast<char>('a' + (stream.NextU8() % 26)));
      }
      return Value(std::move(s));
    }
  }
  return Value();
}

Expression GenerateExpression(FuzzStream& stream, int depth) {
  if (depth <= 0 || stream.Exhausted()) {
    if (stream.NextBool()) {
      return ConstantValueExp(GenerateValue(stream));
    }
    const uint8_t col = stream.NextU8() % 3;
    if (col == 0) return ColumnValueExp("col_i");
    if (col == 1) return ColumnValueExp("col_d");
    return ColumnValueExp("col_s");
  }

  const uint8_t kind = stream.NextU8() % 6;
  switch (kind) {
    case 0:
    case 1:
    case 2: {
      // Binary expression
      static const BinaryOperation ops[] = {
          BinaryOperation::kAdd,        BinaryOperation::kSubtract,
          BinaryOperation::kMultiply,   BinaryOperation::kDivide,
          BinaryOperation::kModulo,     BinaryOperation::kEquals,
          BinaryOperation::kNotEquals,  BinaryOperation::kLessThan,
          BinaryOperation::kLessThanEquals, BinaryOperation::kGreaterThan,
          BinaryOperation::kGreaterThanEquals, BinaryOperation::kAnd,
          BinaryOperation::kOr,         BinaryOperation::kXor,
          BinaryOperation::kLike,       BinaryOperation::kNotLike,
      };
      BinaryOperation op = ops[stream.NextU8() % (sizeof(ops) / sizeof(ops[0]))];
      Expression left = GenerateExpression(stream, depth - 1);
      Expression right = GenerateExpression(stream, depth - 1);
      return BinaryExpressionExp(std::move(left), op, std::move(right));
    }
    case 3: {
      // Unary expression
      static const UnaryOperation un_ops[] = {
          UnaryOperation::kMinus,      UnaryOperation::kNot,
          UnaryOperation::kIsNull,     UnaryOperation::kIsNotNull,
          UnaryOperation::kIsTrue,     UnaryOperation::kIsNotTrue,
          UnaryOperation::kIsFalse,    UnaryOperation::kIsNotFalse,
      };
      UnaryOperation op = un_ops[stream.NextU8() % (sizeof(un_ops) / sizeof(un_ops[0]))];
      Expression child = GenerateExpression(stream, depth - 1);
      return UnaryExpressionExp(std::move(child), op);
    }
    case 4: {
      // Case expression
      const size_t when_count = 1 + (stream.NextU8() % 3);
      std::vector<std::pair<Expression, Expression>> whens;
      for (size_t i = 0; i < when_count; ++i) {
        whens.emplace_back(GenerateExpression(stream, depth - 1),
                           GenerateExpression(stream, depth - 1));
      }
      Expression else_clause =
          stream.NextBool() ? GenerateExpression(stream, depth - 1) : nullptr;
      return CaseExpressionExp(std::move(whens), std::move(else_clause));
    }
    case 5: {
      // In expression
      const size_t list_count = 1 + (stream.NextU8() % 3);
      Expression target = GenerateExpression(stream, depth - 1);
      std::vector<Expression> list;
      for (size_t i = 0; i < list_count; ++i) {
        list.push_back(GenerateExpression(stream, depth - 1));
      }
      return InExpressionExp(std::move(target), std::move(list));
    }
  }
  return ConstantValueExp(GenerateValue(stream));
}

bool ValuesMatch(const Value& a, const Value& b) {
  if (a.IsNull() && b.IsNull()) return true;
  if (a.IsNull() != b.IsNull()) return false;
  if (a.type != b.type) return false;
  if (a.type == ValueType::kDouble) {
    if (std::isnan(a.value.double_value) && std::isnan(b.value.double_value)) {
      return true;
    }
    if (std::isinf(a.value.double_value) && std::isinf(b.value.double_value)) {
      return (a.value.double_value > 0) == (b.value.double_value > 0);
    }
    return std::abs(a.value.double_value - b.value.double_value) < 1e-9;
  }
  return a == b;
}

}  // namespace
}  // namespace tinylamb

extern "C" [[maybe_unused]] int LLVMFuzzerTestOneInput(const uint8_t* data,
                                                       size_t size) {
  if (size < 4 || size > 4096) {
    return 0;
  }

  using namespace tinylamb;
  FuzzStream stream{data, size};

  Schema schema("fuzz_schema", {
      Column("col_i", ValueType::kInt64),
      Column("col_d", ValueType::kDouble),
      Column("col_s", ValueType::kVarChar),
  });

  Row row({
      GenerateValue(stream),
      GenerateValue(stream),
      GenerateValue(stream),
  });

  Expression exp = GenerateExpression(stream, 4);
  if (!exp) return 0;

  // (1) AST Ground Truth evaluation
  bool ast_threw = false;
  Value ast_result;
  try {
    ast_result = exp->Evaluate(row, schema);
  } catch (...) {
    ast_threw = true;
  }

  // (2) Bytecode VM differential check
  std::optional<BytecodeProgram> program =
      BytecodeCompiler::Compile(exp, schema);
  if (program.has_value()) {
    bool bc_threw = false;
    Value bc_result;
    try {
      DataChunk chunk(schema);
      chunk.Append(row);
      ColumnVector out = program->EvaluateBatch(chunk);
      assert(out.Size() == 1);
      bc_result = out.ValueAt(0);
    } catch (...) {
      bc_threw = true;
    }
    assert(ast_threw == bc_threw);
    if (!ast_threw) {
      assert(ValuesMatch(ast_result, bc_result));
    }
  }

  // (3) relational_detail::Evaluate fallback differential check
  bool detail_threw = false;
  Value detail_result;
  try {
    TransactionContext context(Transaction{}, nullptr);
    const relational_detail::Scope scope{&row, &schema, nullptr};
    const relational_detail::CteMap ctes;
    detail_result = relational_detail::Evaluate(exp, scope, nullptr, context, ctes);
  } catch (...) {
    detail_threw = true;
  }
  assert(ast_threw == detail_threw);
  if (!ast_threw) {
    assert(ValuesMatch(ast_result, detail_result));
  }

  return 0;
}
