/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_JIT_HPP
#define TINYLAMB_EXPRESSION_JIT_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include "common/constants.hpp"

namespace tinylamb {

class JitInt64Kernels {
 public:
  struct Impl;
  using FilterFn = void (*)(const int64_t*, uint8_t*, uint64_t, int64_t);
  using ProjectionFn = void (*)(const int64_t*, int64_t*, uint64_t, int64_t,
                                int64_t);
  using SumFn = int64_t (*)(const int64_t*, uint64_t);

  static std::optional<JitInt64Kernels> CompileFilter(BinaryOperation op);
  static std::optional<JitInt64Kernels> CompileProjection();
  static std::optional<JitInt64Kernels> CompileSum();

  JitInt64Kernels(JitInt64Kernels&&) noexcept;
  JitInt64Kernels& operator=(JitInt64Kernels&&) noexcept;
  ~JitInt64Kernels();

  void Filter(const int64_t* input, uint8_t* output, size_t count,
              int64_t constant) const;
  void Project(const int64_t* input, int64_t* output, size_t count,
               int64_t multiplier, int64_t addend) const;
  [[nodiscard]] int64_t Sum(const int64_t* input, size_t count) const;
  [[nodiscard]] double CompileMilliseconds() const;

 private:
  explicit JitInt64Kernels(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace tinylamb
#endif
