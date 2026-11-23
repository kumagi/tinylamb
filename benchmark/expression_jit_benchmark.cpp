/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <random>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/jit.hpp"
#include "expression/unary_expression.hpp"
#include "type/schema.hpp"

// Separate calls with identical buffers and observation for AOT and JIT.
// In particular, neither implementation updates a volatile sink per row.
extern "C" [[gnu::noinline]] void NativeFilter(const int64_t* __restrict input,
                                               uint8_t* __restrict output,
                                               size_t size, int64_t constant) {
  for (size_t i = 0; i < size; ++i) {
    output[i] = static_cast<uint8_t>(input[i] > constant);
  }
}
extern "C" [[gnu::noinline]] void NativeProject(const int64_t* __restrict input,
                                                int64_t* __restrict output,
                                                size_t size, int64_t mul,
                                                int64_t add) {
  for (size_t i = 0; i < size; ++i) {
    output[i] = (input[i] * mul) + add;
  }
}
extern "C" [[gnu::noinline]] int64_t NativeSum(const int64_t* input,
                                               size_t size) {
  int64_t sum = 0;
  for (size_t i = 0; i < size; ++i) {
    sum += input[i];
  }
  return sum;
}

// Pipeline diagnostic: identical filter -> projection -> SUM semantics,
// either three whole-array passes or one fused loop. Inputs below are bounded
// so the unchecked kernels cannot overflow, including intermediate sums.
extern "C" [[gnu::noinline]] int64_t NativeMaskedSum(
    const int64_t* __restrict input, const uint8_t* __restrict mask,
    size_t size) {
  int64_t total = 0;
  for (size_t i = 0; i < size; ++i) {
    total += mask[i] != 0 ? input[i] : 0;
  }
  return total;
}
extern "C" [[gnu::noinline]] int64_t NativeFusedFilterProjectSum(
    const int64_t* input, size_t size, int64_t threshold, int64_t multiplier,
    int64_t addend) {
  int64_t total = 0;
  for (size_t i = 0; i < size; ++i) {
    total += input[i] > threshold ? (input[i] * multiplier) + addend : 0;
  }
  return total;
}

namespace {

template <class Fn>
double Measure(Fn fn, size_t repetitions, uint64_t& checksum) {
  std::vector<double> samples;
  samples.reserve(8);
  for (size_t sample = 0; sample < 8; ++sample) {
    const auto begin = std::chrono::steady_clock::now();
    uint64_t observed = 0;
    for (size_t repeat = 0; repeat < repetitions; ++repeat) {
      // Prevent AOT from hoisting repeated reads of the unchanged input.
      asm volatile("" ::: "memory");
      observed += static_cast<uint64_t>(fn());
    }
    const double ms = std::chrono::duration<double, std::milli>(
                          std::chrono::steady_clock::now() - begin)
                          .count();
    checksum += observed;
    if (sample != 0) {
      samples.push_back(ms / static_cast<double>(repetitions));
    }
  }
  std::sort(samples.begin(), samples.end());
  return samples[samples.size() / 2];
}

struct SumResult {
  int64_t total{0};
  uint64_t selected{0};
  bool operator==(const SumResult&) const = default;

  [[nodiscard]] uint64_t Checksum() const {
    return static_cast<uint64_t>(total) ^ selected;
  }
};

[[gnu::noinline]] SumResult ReduceSelected(const int64_t* __restrict values,
                                           const uint8_t* __restrict mask,
                                           size_t count) {
  SumResult result;
  for (size_t i = 0; i < count; ++i) {
    result.total += mask[i] != 0 ? values[i] : 0;
    result.selected += mask[i];
  }
  return result;
}

[[gnu::noinline]] size_t CompactSelected(const int64_t* __restrict values,
                                         const uint8_t* __restrict mask,
                                         int64_t* __restrict compact,
                                         size_t count) {
  size_t kept = 0;
  for (size_t i = 0; i < count; ++i) {
    if (mask[i] != 0) {
      compact[kept++] = values[i];
    }
  }
  return kept;
}

int BenchmarkFusion() {
  using tinylamb::BinaryOperation;
  using tinylamb::JitInt64Kernels;
  constexpr auto kOp = BinaryOperation::kGreaterThan;
  auto filter = JitInt64Kernels::CompileFilter(kOp);
  auto project = JitInt64Kernels::CompileProjection();
  auto sum = JitInt64Kernels::CompileSum();
  auto masked = JitInt64Kernels::CompileFilterProjectSum(kOp);
  auto sparse = JitInt64Kernels::CompileFilterProjectSum(kOp, true);
  if (!filter || !project || !sum || !masked || !sparse) {
    std::cerr << "llvm_jit=unavailable\n";
    return 1;
  }
  std::cerr << "compile_ms masked=" << masked->CompileMilliseconds()
            << " conditional=" << sparse->CompileMilliseconds() << '\n';
  std::cout
      << "pattern,rows,actual_selectivity,split_dense_ms,split_compact_ms,"
         "split_tiled_ms,fused_masked_ms,fused_conditional_ms\n";
  uint64_t checksum = 0;
  for (size_t size : {size_t{1024}, size_t{1048576}, size_t{10000000}}) {
    std::vector<int64_t> values(size), keys(size), projected(size),
        compact(size);
    std::vector<uint8_t> mask(size);
    uint64_t random = 0x3617a99bac3fefULL;
    for (auto& v : values) {
      random ^= random << 13;
      random ^= random >> 7;
      random ^= random << 17;
      v = static_cast<int64_t>(random % 1000001) - 500000;
    }
    constexpr int64_t kMul = 3;
    constexpr int64_t kAdd = 7;
    for (bool clustered : {false, true}) {
      for (int64_t percent : {0, 1, 10, 50, 100}) {
        const int64_t threshold = 99 - percent;
        size_t prefix = (size * static_cast<size_t>(percent)) / 100;
        for (size_t i = 0; i < size; ++i) {
          random ^= random << 13;
          random ^= random >> 7;
          random ^= random << 17;
          keys[i] = clustered ? (i < prefix ? 99 : 0)
                              : static_cast<int64_t>(random % 100);
        }
        const auto split_dense = [&] {
          filter->Filter(keys.data(), mask.data(), size, threshold);
          project->Project(values.data(), projected.data(), size, kMul, kAdd);
          return ReduceSelected(projected.data(), mask.data(), size);
        };
        const auto split_compact = [&] {
          filter->Filter(keys.data(), mask.data(), size, threshold);
          const size_t kept =
              CompactSelected(values.data(), mask.data(), compact.data(), size);
          project->Project(compact.data(), projected.data(), kept, kMul, kAdd);
          return SumResult{.total = sum->Sum(projected.data(), kept),
                           .selected = kept};
        };
        const auto split_tiled = [&] {
          SumResult result;
          for (size_t offset = 0; offset < size; offset += 1024) {
            const size_t count = std::min<size_t>(1024, size - offset);
            filter->Filter(keys.data() + offset, mask.data(), count, threshold);
            project->Project(values.data() + offset, projected.data(), count,
                             kMul, kAdd);
            const auto tile =
                ReduceSelected(projected.data(), mask.data(), count);
            result.total += tile.total;
            result.selected += tile.selected;
          }
          return result;
        };
        const auto fused = [&](const JitInt64Kernels& kernel) {
          SumResult result;
          result.total = kernel.FilterProjectSum(keys.data(), values.data(),
                                                 size, threshold, kMul, kAdd, 0,
                                                 &result.selected);
          return result;
        };
        SumResult expected;
        for (size_t i = 0; i < size; ++i) {
          if (keys[i] > threshold) {
            expected.total += (values[i] * kMul) + kAdd;
            ++expected.selected;
          }
        }
        if (split_dense() != expected || split_compact() != expected ||
            split_tiled() != expected || fused(*masked) != expected ||
            fused(*sparse) != expected) {
          std::cerr << "fused SUM mismatch\n";
          return 3;
        }
        const size_t repeats = std::max<size_t>(3, 4000000 / size);
        const double dense_ms = Measure(
            [&] { return split_dense().Checksum(); }, repeats, checksum);
        const double compact_ms = Measure(
            [&] { return split_compact().Checksum(); }, repeats, checksum);
        const double tiled_ms = Measure(
            [&] { return split_tiled().Checksum(); }, repeats, checksum);
        const double masked_ms = Measure(
            [&] { return fused(*masked).Checksum(); }, repeats, checksum);
        const double sparse_ms = Measure(
            [&] { return fused(*sparse).Checksum(); }, repeats, checksum);
        std::cout << (clustered ? "clustered" : "shuffled") << ',' << size
                  << ','
                  << static_cast<double>(expected.selected) /
                         static_cast<double>(size)
                  << ',' << dense_ms << ',' << compact_ms << ',' << tiled_ms
                  << ',' << masked_ms << ',' << sparse_ms << '\n';
      }
    }
  }
  std::cerr << "checksum=" << checksum << '\n';
  return 0;
}

int BenchmarkHashProbe() {
  using tinylamb::BinaryOperation;
  using tinylamb::FilteredHashProbe;
  using tinylamb::JitInt64Kernels;
  using tinylamb::JoinHashIndex;
  constexpr auto kOp = BinaryOperation::kGreaterThan;
  const auto direct = JitInt64Kernels::CompileFilterHashProbe(kOp);
  const auto filter = JitInt64Kernels::CompileFilter(kOp);
  if (!direct || !filter) {
    std::cerr << "llvm_jit=unavailable\n";
    return 1;
  }
  std::cerr << "compile_ms direct=" << direct->CompileMilliseconds()
            << " compact=" << filter->CompileMilliseconds() << '\n';
  constexpr size_t kRows = 1 << 20;
  constexpr size_t kBatch = 1024;
  std::mt19937_64 random(0x99ab);  // NOLINT(cert-msc32-c,cert-msc51-cpp):
                                   // reproducible benchmark input
  std::vector<int64_t> keys(kRows);
  std::vector<int64_t> predicates(kRows);
  uint64_t checksum = 0;
  std::cout
      << "build_keys,slot_mib,pattern,selectivity,matches,scalar_ms,"
         "direct_ms,compact_ms,adaptive_ms,direct_batches,compact_batches\n";
  for (size_t build_size :
       {size_t{256}, size_t{32768}, size_t{1} << 20, size_t{1} << 22}) {
    JoinHashIndex index;
    index.Init(JoinHashIndex::KeyMode::kInt64, build_size);
    for (size_t i = 0; i < build_size; ++i) {
      const int64_t key =
          static_cast<int64_t>(i) - static_cast<int64_t>(build_size / 2);
      index.Insert(JoinHashIndex::HashInt64(key), key, {}, i);
    }
    for (int64_t& key : keys) {
      key = static_cast<int64_t>(random() % (build_size * 2)) -
            static_cast<int64_t>(build_size);
    }
    for (bool clustered : {false, true}) {
      for (size_t pct : {1U, 5U, 10U, 25U, 50U, 100U}) {
        for (size_t i = 0; i < kRows; ++i) {
          const size_t bucket =
              clustered ? (i % 4096) * 100 / 4096 : random() % 100;
          predicates[i] = static_cast<int64_t>(bucket < pct);
        }
        size_t expected_matches = 0;
        const auto scalar = [&] {
          uint64_t result = 0;
          size_t matches = 0;
          for (size_t i = 0; i < kRows; ++i) {
            if (predicates[i] == 0) {
              continue;
            }
            const size_t head =
                index.Find(JoinHashIndex::HashInt64(keys[i]), keys[i], {});
            if (head != JoinHashIndex::kNil) {
              result = (result * 33) + head + i;
              ++matches;
            }
          }
          expected_matches = matches;
          return result;
        };
        const uint64_t expected = scalar();
        FilteredHashProbe cursor;
        size_t direct_batches = 0;
        const auto probe = [&](std::optional<FilteredHashProbe::Mode> fixed) {
          using Mode = FilteredHashProbe::Mode;
          direct_batches = 0;
          uint64_t result = 0;
          for (size_t base = 0; base < kRows; base += kBatch) {
            const Mode mode =
                fixed ? *fixed
                      : FilteredHashProbe::ChooseMode(
                            index, predicates.data() + base, kBatch, kOp, 0);
            direct_batches += mode == Mode::kDirect;
            cursor.Start(mode, mode == Mode::kDirect ? *direct : *filter, index,
                         predicates.data() + base, keys.data() + base, kBatch,
                         0);
            for (;;) {
              size_t row = 0;
              size_t accepted = 0;
              const size_t head = cursor.Next(&row, &accepted);
              if (head == JoinHashIndex::kNil) {
                break;
              }
              result = (result * 33) + head + base + row;
            }
          }
          return result;
        };
        const auto fused = [&] {
          return probe(FilteredHashProbe::Mode::kDirect);
        };
        const auto compact = [&] {
          return probe(FilteredHashProbe::Mode::kCompact);
        };
        const auto adaptive = [&] { return probe(std::nullopt); };
        if (expected != fused() || expected != compact() ||
            expected != adaptive()) {
          std::cerr << "hash probe result mismatch\n";
          return 1;
        }
        const double scalar_ms = Measure(scalar, 1, checksum);
        const double direct_ms = Measure(fused, 1, checksum);
        const double compact_ms = Measure(compact, 1, checksum);
        const double adaptive_ms = Measure(adaptive, 1, checksum);
        const size_t slots = index.ViewInt64Slots().mask + 1;
        std::cout << build_size << ','
                  << static_cast<double>(slots * 16) / (1 << 20) << ','
                  << (clustered ? "clustered" : "shuffled") << ',' << pct << ','
                  << expected_matches << ',' << scalar_ms << ',' << direct_ms
                  << ',' << compact_ms << ',' << adaptive_ms << ','
                  << direct_batches << ','
                  << ((kRows / kBatch) - direct_batches) << '\n';
      }
    }
  }
  std::cerr << "checksum=" << checksum << '\n';
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string_view(argv[1]) == "--fusion") {
    return BenchmarkFusion();
  }
  if (argc == 2 && std::string_view(argv[1]) == "--hash-probe") {
    return BenchmarkHashProbe();
  }
  if (argc > 1) {
    std::cerr << "usage: " << argv[0] << " [--fusion|--hash-probe]\n";
    return 2;
  }
  using tinylamb::JitInt64Kernels;
  auto filter =
      JitInt64Kernels::CompileFilter(tinylamb::BinaryOperation::kGreaterThan);
  auto project = JitInt64Kernels::CompileProjection();
  auto sum = JitInt64Kernels::CompileSum();
  auto fused_sum = JitInt64Kernels::CompileFilterProjectSum(
      tinylamb::BinaryOperation::kGreaterThan);
  if (!filter || !project || !sum || !fused_sum) {
    std::cerr << "llvm_jit=unavailable\n";
    return 1;
  }
  std::cout << "kernel,rows,aot_ms,jit_ms,compile_ms\n";
  uint64_t checksum = 0;
  for (const size_t size : {size_t{63}, size_t{1023}, size_t{16384},
                            size_t{1048576}, size_t{10000000}}) {
    std::vector<int64_t> input(size), expected(size), actual(size);
    std::vector<uint8_t> mask(size), expected_mask(size);
    uint64_t random = 0x123456789abcdefULL;
    for (auto& value : input) {
      random ^= random << 13;
      random ^= random >> 7;
      random ^= random << 17;
      value = static_cast<int64_t>(random % 1000001) - 500000;
    }
    NativeFilter(input.data(), expected_mask.data(), size, 12345);
    filter->Filter(input.data(), mask.data(), size, 12345);
    NativeProject(input.data(), expected.data(), size, 3, 7);
    project->Project(input.data(), actual.data(), size, 3, 7);
    if (expected_mask != mask || expected != actual ||
        NativeSum(input.data(), size) != sum->Sum(input.data(), size)) {
      std::cerr << "kernel result mismatch\n";
      return 2;
    }
    const size_t repetitions = std::max<size_t>(4, 20000000 / size);
    const double native_filter = Measure(
        [&] {
          NativeFilter(input.data(), mask.data(), size, 12345);
          return mask.back();
        },
        repetitions, checksum);
    const double jit_filter = Measure(
        [&] {
          filter->Filter(input.data(), mask.data(), size, 12345);
          return mask.back();
        },
        repetitions, checksum);
    const double native_project = Measure(
        [&] {
          NativeProject(input.data(), actual.data(), size, 3, 7);
          return actual.back();
        },
        repetitions, checksum);
    const double jit_project = Measure(
        [&] {
          project->Project(input.data(), actual.data(), size, 3, 7);
          return actual.back();
        },
        repetitions, checksum);
    const double native_sum = Measure(
        [&] { return NativeSum(input.data(), size); }, repetitions, checksum);
    const double jit_sum = Measure([&] { return sum->Sum(input.data(), size); },
                                   repetitions, checksum);
    const int64_t pipeline_expected =
        NativeFusedFilterProjectSum(input.data(), size, 12345, 3, 7);
    if (NativeMaskedSum(actual.data(), mask.data(), size) !=
        pipeline_expected) {
      std::cerr << "pipeline result mismatch\n";
      return 3;
    }
    const double native_split = Measure(
        [&] {
          NativeFilter(input.data(), mask.data(), size, 12345);
          NativeProject(input.data(), actual.data(), size, 3, 7);
          return NativeMaskedSum(actual.data(), mask.data(), size);
        },
        repetitions, checksum);
    const double jit_split = Measure(
        [&] {
          filter->Filter(input.data(), mask.data(), size, 12345);
          project->Project(input.data(), actual.data(), size, 3, 7);
          return NativeMaskedSum(actual.data(), mask.data(), size);
        },
        repetitions, checksum);
    const double native_fused = Measure(
        [&] {
          return NativeFusedFilterProjectSum(input.data(), size, 12345, 3, 7);
        },
        repetitions, checksum);
    uint64_t selected = 0;
    if (fused_sum->FilterProjectSum(input.data(), input.data(), size, 12345, 3,
                                    7, 0, &selected) != pipeline_expected) {
      std::cerr << "fused pipeline result mismatch\n";
      return 3;
    }
    const double jit_fused = Measure(
        [&] {
          return fused_sum->FilterProjectSum(input.data(), input.data(), size,
                                             12345, 3, 7, 0, &selected);
        },
        repetitions, checksum);
    std::cout << "pipeline_split," << size << ',' << native_split << ','
              << jit_split << ','
              << filter->CompileMilliseconds() + project->CompileMilliseconds()
              << '\n'
              << "pipeline_fused," << size << ',' << native_fused << ','
              << jit_fused << ',' << fused_sum->CompileMilliseconds() << '\n';
    std::cout << "filter," << size << ',' << native_filter << ',' << jit_filter
              << ',' << filter->CompileMilliseconds() << '\n'
              << "project," << size << ',' << native_project << ','
              << jit_project << ',' << project->CompileMilliseconds() << '\n'
              << "sum," << size << ',' << native_sum << ',' << jit_sum << ','
              << sum->CompileMilliseconds() << '\n';
  }

  // ==========================================================================
  // Proof-Backed Branchless Bytecode JIT Benchmark (Compound 3VL + CASE + NULL)
  // ==========================================================================
  {
    using namespace tinylamb;
    const Schema schema("bench", {Column("qty", ValueType::kInt64),
                                  Column("price", ValueType::kInt64),
                                  Column("discount", ValueType::kInt64),
                                  Column("brand", ValueType::kInt64)});

    // 1) Compound 3VL WHERE filter (TPC-H Q19-like with NULLs + IN + COALESCE):
    //    (qty >= 10 AND qty <= 30 AND brand IN (12, 23, 34))
    //    OR (discount IS NULL AND price > 500)
    const Expression compound_filter = BinaryExpressionExp(
        BinaryExpressionExp(
            BinaryExpressionExp(
                BinaryExpressionExp(ColumnValueExp("qty"),
                                    BinaryOperation::kGreaterThanEquals,
                                    ConstantValueExp(Value(int64_t{10}))),
                BinaryOperation::kAnd,
                BinaryExpressionExp(ColumnValueExp("qty"),
                                    BinaryOperation::kLessThanEquals,
                                    ConstantValueExp(Value(int64_t{30})))),
            BinaryOperation::kAnd,
            InExpressionExp(ColumnValueExp("brand"),
                            {ConstantValueExp(Value(int64_t{12})),
                             ConstantValueExp(Value(int64_t{23})),
                             ConstantValueExp(Value(int64_t{34}))})),
        BinaryOperation::kOr,
        BinaryExpressionExp(
            UnaryExpressionExp(ColumnValueExp("discount"),
                               UnaryOperation::kIsNull),
            BinaryOperation::kAnd,
            BinaryExpressionExp(ColumnValueExp("price"),
                                BinaryOperation::kGreaterThan,
                                ConstantValueExp(Value(int64_t{500})))));

    // 2) Multi-branch Searched CASE + COALESCE + ABS + Multiplication:
    //    CASE
    //      WHEN qty > 20 THEN price * (100 - COALESCE(discount, 0))
    //      WHEN qty > 0  THEN price * 100 - ABS(COALESCE(discount, 0))
    //      ELSE 0
    //    END
    const Expression coalesce_disc = FunctionCallExp(
        "coalesce",
        {ColumnValueExp("discount"), ConstantValueExp(Value(int64_t{0}))});
    const Expression case_pricing = CaseExpressionExp(
        {{BinaryExpressionExp(ColumnValueExp("qty"),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{20}))),
          BinaryExpressionExp(
              ColumnValueExp("price"), BinaryOperation::kMultiply,
              BinaryExpressionExp(ConstantValueExp(Value(int64_t{100})),
                                  BinaryOperation::kSubtract, coalesce_disc))},
         {BinaryExpressionExp(ColumnValueExp("qty"),
                              BinaryOperation::kGreaterThan,
                              ConstantValueExp(Value(int64_t{0}))),
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp("price"),
                                  BinaryOperation::kMultiply,
                                  ConstantValueExp(Value(int64_t{100}))),
              BinaryOperation::kSubtract,
              FunctionCallExp("abs", {coalesce_disc}))}},
        ConstantValueExp(Value(int64_t{0})));

    const auto bc_filter = BytecodeCompiler::Compile(
        compound_filter, schema, BytecodeCompiler::Context::kFilter);
    const auto bc_case = BytecodeCompiler::Compile(
        case_pricing, schema, BytecodeCompiler::Context::kValue);
    const auto jit_filter_prog =
        bc_filter ? JitBranchlessProgram::Compile(*bc_filter, schema)
                  : std::nullopt;
    const auto jit_case_prog =
        bc_case ? JitBranchlessProgram::Compile(*bc_case, schema)
                : std::nullopt;
    if (!jit_filter_prog || !jit_case_prog) {
      std::cerr << "branchless_jit=unavailable\n";
      return 4;
    }

    constexpr size_t kBatch = 1024;
    constexpr size_t kNumBatches = 1024;  // 1,048,576 rows total (1024 chunks)
    DataChunk chunk(schema, kBatch);
    std::vector<int64_t> c_qty(kBatch), c_price(kBatch), c_disc(kBatch),
        c_brand(kBatch);
    std::vector<uint8_t> n_disc(kBatch);
    uint64_t rng = 0x9e3779b97f4a7c15ULL;
    for (size_t i = 0; i < kBatch; ++i) {
      rng ^= rng << 13;
      rng ^= rng >> 7;
      rng ^= rng << 17;
      c_qty[i] = static_cast<int64_t>(rng % 50) - 5;
      c_price[i] = static_cast<int64_t>((rng >> 8) % 1000) + 100;
      c_disc[i] = static_cast<int64_t>((rng >> 20) % 30) - 5;
      c_brand[i] = static_cast<int64_t>((rng >> 32) % 40);
      n_disc[i] = ((i % 8) == 0) ? 1 : 0;
      chunk.Append(Row({Value(c_qty[i]), Value(c_price[i]),
                        (n_disc[i] != 0U) ? Value() : Value(c_disc[i]),
                        Value(c_brand[i])}));
    }

    auto make_inputs = [&](const JitBranchlessProgram& prog, bool with_bounds) {
      std::vector<JitColumnInput> out;
      out.reserve(prog.ReferencedColumns().size());
      for (uint16_t col : prog.ReferencedColumns()) {
        if (col == 0) {
          out.push_back({.values = c_qty.data(),
                         .has_nulls = false,
                         .has_bounds = with_bounds,
                         .min_value = -5,
                         .max_value = 45});
        } else if (col == 1) {
          out.push_back({.values = c_price.data(),
                         .has_nulls = false,
                         .has_bounds = with_bounds,
                         .min_value = 100,
                         .max_value = 1100});
        } else if (col == 2) {
          out.push_back({.values = c_disc.data(),
                         .nulls = n_disc.data(),
                         .has_nulls = true,
                         .has_bounds = with_bounds,
                         .min_value = -5,
                         .max_value = 25});
        } else {
          out.push_back({.values = c_brand.data(),
                         .has_nulls = false,
                         .has_bounds = with_bounds,
                         .min_value = 0,
                         .max_value = 40});
        }
      }
      return out;
    };

    const auto filter_inputs = make_inputs(*jit_filter_prog, true);
    const auto case_inputs_checked = make_inputs(*jit_case_prog, false);
    const auto case_inputs_zonemap = make_inputs(*jit_case_prog, true);

    std::vector<uint8_t> keep_out(kBatch, 0);
    std::vector<int64_t> val_out(kBatch, 0);
    std::vector<uint8_t> nul_out(kBatch, 0);
    bool any_null = false;
    bool used_unchecked = false;

    const ColumnVector ref_filter = bc_filter->EvaluateBatch(chunk);
    const ColumnVector ref_case = bc_case->EvaluateBatch(chunk);
    if (ref_filter.IntegerData().size() != kBatch ||
        ref_case.IntegerData().size() != kBatch) {
      std::cerr << "bytecode benchmark expected a full integer batch\n";
      return 5;
    }
    if (!jit_filter_prog->Filter(filter_inputs, kBatch, keep_out.data()) ||
        !jit_case_prog->Evaluate(case_inputs_zonemap, kBatch, val_out.data(),
                                 nul_out.data(), &any_null, &used_unchecked) ||
        !used_unchecked) {
      std::cerr << "branchless jit verification failed\n";
      return 5;
    }
    for (size_t i = 0; i < kBatch; ++i) {
      const bool exp_keep =
          !ref_filter.IsNull(i) && ref_filter.ValueAt(i).Truthy();
      if ((keep_out[i] != 0) != exp_keep) {
        std::cerr << "compound filter mismatch at " << i << "\n";
        return 6;
      }
      const Value got = (nul_out[i] != 0U) ? Value() : Value(val_out[i]);
      if (got != ref_case.ValueAt(i)) {
        std::cerr << "case pricing mismatch at " << i << "\n";
        return 7;
      }
    }

    const double vm_filter_ms = Measure(
        [&] {
          uint64_t acc = 0;
          for (size_t b = 0; b < kNumBatches; ++b) {
            ColumnVector col = bc_filter->EvaluateBatch(chunk);
            acc += static_cast<uint64_t>(col.IntegerData().back());
          }
          return acc;
        },
        1, checksum);
    const double jit_compound_filter_ms = Measure(
        [&] {
          uint64_t acc = 0;
          for (size_t b = 0; b < kNumBatches; ++b) {
            (void)jit_filter_prog->Filter(filter_inputs, kBatch,
                                          keep_out.data());
            acc += keep_out.back();
          }
          return acc;
        },
        4, checksum);

    const double vm_case_ms = Measure(
        [&] {
          uint64_t acc = 0;
          for (size_t b = 0; b < kNumBatches; ++b) {
            ColumnVector col = bc_case->EvaluateBatch(chunk);
            acc += static_cast<uint64_t>(col.IntegerData().back());
          }
          return acc;
        },
        1, checksum);
    const double jit_case_checked_ms = Measure(
        [&] {
          uint64_t acc = 0;
          for (size_t b = 0; b < kNumBatches; ++b) {
            (void)jit_case_prog->Evaluate(case_inputs_checked, kBatch,
                                          val_out.data(), nul_out.data(),
                                          &any_null);
            acc += static_cast<uint64_t>(val_out.back());
          }
          return acc;
        },
        4, checksum);
    const double jit_case_zonemap_ms = Measure(
        [&] {
          uint64_t acc = 0;
          for (size_t b = 0; b < kNumBatches; ++b) {
            (void)jit_case_prog->Evaluate(case_inputs_zonemap, kBatch,
                                          val_out.data(), nul_out.data(),
                                          &any_null);
            acc += static_cast<uint64_t>(val_out.back());
          }
          return acc;
        },
        4, checksum);

    const size_t total_rows = kBatch * kNumBatches;
    std::cout << "\n# Proof-Backed Branchless Bytecode JIT (rows=" << total_rows
              << ", nullable=true)\n"
              << "compound_3vl_filter_vm_vs_jit," << total_rows << ','
              << vm_filter_ms << ',' << jit_compound_filter_ms << ','
              << jit_filter_prog->CompileMilliseconds() << '\n'
              << "case_coalesce_pricing_vm_vs_checked_jit," << total_rows << ','
              << vm_case_ms << ',' << jit_case_checked_ms << ','
              << jit_case_prog->CompileMilliseconds() << '\n'
              << "case_coalesce_pricing_vm_vs_zonemap_simd_jit," << total_rows
              << ',' << vm_case_ms << ',' << jit_case_zonemap_ms << ','
              << jit_case_prog->CompileMilliseconds() << '\n';
  }
  std::cerr << "checksum=" << checksum << '\n';
}
