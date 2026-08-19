/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <chrono>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <vector>

#include "expression/jit.hpp"

int main() {
  using Clock = std::chrono::steady_clock;
  auto kernel = tinylamb::JitInt64Kernels::CompileFilter(
      tinylamb::BinaryOperation::kGreaterThan);
  if (!kernel) {
    std::cout << "llvm_jit=unavailable\n";
    return 0;
  }
  std::cout << "compile_ms=" << kernel->CompileMilliseconds() << "\n";
  size_t break_even = 0;
  volatile uint64_t checksum = 0;
  for (size_t rows : {64U, 256U, 1024U, 4096U, 16384U, 65536U,
                      262144U, 1048576U}) {
    std::vector<int64_t> input(rows);
    std::iota(input.begin(), input.end(), int64_t{0});
    std::vector<uint8_t> output(rows);
    constexpr size_t repetitions = 20;
    const auto scalar_begin = Clock::now();
    for (size_t repeat = 0; repeat < repetitions; ++repeat) {
      for (size_t index = 0; index < rows; ++index) {
        output[index] = input[index] > 12345;
        checksum = checksum + output[index];
      }
    }
    const double scalar_ms =
        std::chrono::duration<double, std::milli>(Clock::now() - scalar_begin)
            .count();
    const auto jit_begin = Clock::now();
    for (size_t repeat = 0; repeat < repetitions; ++repeat) {
      kernel->Filter(input.data(), output.data(), rows, 12345);
      checksum = checksum + output.back();
    }
    const double jit_ms =
        std::chrono::duration<double, std::milli>(Clock::now() - jit_begin)
            .count();
    if (break_even == 0 && kernel->CompileMilliseconds() + jit_ms < scalar_ms) {
      break_even = rows * repetitions;
    }
    std::cout << "rows=" << rows << " repetitions=" << repetitions
              << " scalar_ms=" << scalar_ms << " jit_ms=" << jit_ms << "\n";
  }
  std::cout << "break_even_evaluations=" << break_even
            << " checksum=" << checksum << "\n";
}
