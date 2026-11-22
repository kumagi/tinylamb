/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/jit.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ratio>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_hash.hpp"
#include "common/log_message.hpp"
#include "common/regex_guard.hpp"
#include "expression/bytecode.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

#ifdef TINYLAMB_HAS_LLVM
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Value.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcpp"
#include <llvm/Config/llvm-config.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/ObjectTransformLayer.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/FileSystem.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#pragma GCC diagnostic pop
namespace {
// getOrInsertDeclaration replaced getDeclaration in LLVM 20.
llvm::Function* IntrinsicDecl(llvm::Module* module, llvm::Intrinsic::ID id,
                              llvm::ArrayRef<llvm::Type*> tys) {
#if LLVM_VERSION_MAJOR >= 20
  return llvm::Intrinsic::getOrInsertDeclaration(module, id, tys);
#else
  return llvm::Intrinsic::getDeclaration(module, id, tys);
#endif
}
}  // namespace
#endif

// LLVM's PHINode uses hung-off-operands: memory preceding the User object is
// valid by construction, but clang's analyzer cannot model the allocation and
// reports a bogus out-of-bounds read inside LLVM headers. Suppress the check
// for this LLVM-integration TU only.
#ifdef __clang__
#pragma clang diagnostic push
// Older analyzers expose the check under a name clang 21 no longer accepts
// as a compiler warning group; __has_warning keeps the pragma portable.
#if __has_warning("-Wanalyzer-security.ArrayBound")
#pragma clang diagnostic ignored "-Wanalyzer-security.ArrayBound"
#endif
#endif

namespace tinylamb {

void UnpackNullBitmapToBytes(std::span<const uint64_t> bitmap, size_t count,
                             uint8_t* out_bytes) noexcept {
  if (count == 0) {
    return;
  }
  const size_t full_words = count / 64;
  for (size_t w = 0; w < full_words; ++w) {
    const uint64_t word = w < bitmap.size() ? bitmap[w] : 0ULL;
    uint8_t* dst = out_bytes + (w * 64);
    if (word == 0ULL) {
      std::memset(dst, 0, 64);
      continue;
    }
    for (size_t b = 0; b < 64; ++b) {
      dst[b] = static_cast<uint8_t>((word >> b) & 1ULL);
    }
  }
  const size_t rem_base = full_words * 64;
  if (rem_base < count) {
    const uint64_t word =
        full_words < bitmap.size() ? bitmap[full_words] : 0ULL;
    for (size_t i = rem_base; i < count; ++i) {
      out_bytes[i] = static_cast<uint8_t>((word >> (i - rem_base)) & 1ULL);
    }
  }
}

// Lean proof: lean/TinylambOpt/BranchlessJitCodegen.lean
//   (zonemap_prefix_sum_masked_sound; the masked sum degenerates to the
//   plain sum on NULL-free batches, so this envelope also covers the
//   unchecked-sum and fused filter-project-sum callers)
bool JitInt64Kernels::CanUseUncheckedSum(int64_t running_total,
                                         int64_t min_value, int64_t max_value,
                                         size_t count) noexcept {
  if (min_value > max_value) {
    return false;
  }
  const auto n = static_cast<__int128>(count);
  const __int128 min_neg = min_value < 0 ? static_cast<__int128>(min_value) : 0;
  const __int128 max_pos = max_value > 0 ? static_cast<__int128>(max_value) : 0;
  const __int128 lo_env = static_cast<__int128>(running_total) + (n * min_neg);
  const __int128 hi_env = static_cast<__int128>(running_total) + (n * max_pos);
  return lo_env >= std::numeric_limits<int64_t>::min() &&
         hi_env <= std::numeric_limits<int64_t>::max();
}

// Lean proof: lean/TinylambOpt/FusedAggregateJit.lean
// (affine_endpoints_bound, guarded_fused_sum)
bool JitInt64Kernels::CanUseFilterProjectSum(int64_t min_value,
                                             int64_t max_value,
                                             int64_t multiplier, int64_t addend,
                                             int64_t initial,
                                             size_t count) noexcept {
  if (min_value > max_value) {
    return false;
  }
  const auto first = static_cast<__int128>(min_value) * multiplier;
  const auto last = static_cast<__int128>(max_value) * multiplier;
  const auto low = std::min(first, last);
  const auto high = std::max(first, last);
  constexpr auto kMin = std::numeric_limits<int64_t>::min();
  constexpr auto kMax = std::numeric_limits<int64_t>::max();
  return low >= kMin && high <= kMax && low + addend >= kMin &&
         high + addend <= kMax &&
         CanUseUncheckedSum(initial, static_cast<int64_t>(low + addend),
                            static_cast<int64_t>(high + addend), count);
}

struct JitInt64Kernels::Impl {
#ifdef TINYLAMB_HAS_LLVM
  std::unique_ptr<llvm::orc::LLJIT> jit;
#endif
  FilterFn filter{nullptr};
  ProjectionFn projection{nullptr};
  SumFn sum{nullptr};
  MaskedSumFn masked_sum{nullptr};
  FilterProjectSumFn filter_project_sum{nullptr};
  FilterHashProbeFn filter_hash_probe{nullptr};
  SumCheckedFn sum_checked{nullptr};
  ProjectionCheckedFn projection_checked{nullptr};
  double compile_ms{0};
};

#ifdef TINYLAMB_HAS_LLVM
namespace {
// The process-wide kernel caches are intentionally never destroyed. Tearing
// an LLJIT down during static destruction races LLVM's own statics: on
// Darwin, ORC deregisters compact-unwind sections through a function-local
// static UnwindInfoManager that may already be gone at exit (SIGSEGV in
// UnwindInfoManager::deregisterSectionsImpl). The OS reclaims the JIT memory
// at exit anyway.
using KernelImplPtr = std::shared_ptr<JitInt64Kernels::Impl>;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
std::mutex& jit_cache_mutex = *new std::mutex;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
std::unordered_map<BinaryOperation, KernelImplPtr>& filter_kernel_cache =
    *new std::unordered_map<BinaryOperation, KernelImplPtr>;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
auto& filter_project_sum_cache =
    *new std::array<std::unordered_map<BinaryOperation, KernelImplPtr>, 2>;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
auto& filter_hash_probe_cache =
    *new std::unordered_map<BinaryOperation, KernelImplPtr>;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
KernelImplPtr& projection_kernel_cache = *new KernelImplPtr;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
KernelImplPtr& sum_kernel_cache = *new KernelImplPtr;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
KernelImplPtr& masked_sum_kernel_cache = *new KernelImplPtr;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
KernelImplPtr& projection_checked_kernel_cache = *new KernelImplPtr;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
KernelImplPtr& sum_checked_kernel_cache = *new KernelImplPtr;

void InitializeLlvm() {
  static std::once_flag initialized;
  std::call_once(initialized, [] {
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
  });
}

std::unique_ptr<llvm::orc::LLJIT> OptimizeAndCompileModule(
    std::unique_ptr<llvm::Module> module,
    std::unique_ptr<llvm::LLVMContext> context, std::string_view dump_tag) {
  auto target = llvm::orc::JITTargetMachineBuilder::detectHost();
  if (!target) {
    llvm::consumeError(target.takeError());
    return nullptr;
  }
#if LLVM_VERSION_MAJOR >= 18
  target->setCodeGenOptLevel(llvm::CodeGenOptLevel::Aggressive);
#else
  target->setCodeGenOptLevel(llvm::CodeGenOpt::Aggressive);
#endif
  auto machine = target->createTargetMachine();
  if (!machine) {
    llvm::consumeError(machine.takeError());
    return nullptr;
  }
  module->setDataLayout((*machine)->createDataLayout());
  module->setTargetTriple(target->getTargetTriple());
  for (auto& function : *module) {
    if (!function.isDeclaration()) {
      function.addFnAttr("target-cpu", target->getCPU());
      function.addFnAttr("target-features", target->getFeatures().getString());
    }
  }
  if (llvm::verifyModule(*module, &llvm::errs())) {
    return nullptr;
  }
  // ORC compiles IR to machine code; it does not implicitly run the LLVM
  // middle-end. Run the same target-aware O3 loop/vector pipeline as AOT.
  if (std::getenv("TINYLAMB_JIT_DISABLE_OPT") == nullptr) {
    llvm::LoopAnalysisManager loops;
    llvm::FunctionAnalysisManager functions;
    llvm::CGSCCAnalysisManager cgscc;
    llvm::ModuleAnalysisManager modules;
    llvm::PassBuilder passes(machine->get());
    passes.registerModuleAnalyses(modules);
    passes.registerCGSCCAnalyses(cgscc);
    passes.registerFunctionAnalyses(functions);
    passes.registerLoopAnalyses(loops);
    passes.crossRegisterProxies(loops, functions, cgscc, modules);
    auto pipeline =
        passes.buildPerModuleDefaultPipeline(llvm::OptimizationLevel::O3);
    pipeline.run(*module, modules);
  }
  auto jit = llvm::orc::LLJITBuilder()
                 .setJITTargetMachineBuilder(std::move(*target))
                 .create();
  if (!jit) {
    llvm::consumeError(jit.takeError());
    return nullptr;
  }
  if (const char* directory = std::getenv("TINYLAMB_JIT_DUMP_DIR")) {
    static std::atomic<uint64_t> sequence{0};
    std::error_code error;
    std::filesystem::create_directories(directory, error);
    if (!error) {
      const auto prefix = (std::filesystem::path(directory) /
                           (std::string(dump_tag) + "-" +
                            std::to_string(sequence.fetch_add(1))))
                              .string();
      llvm::raw_fd_ostream ir(prefix + ".ll", error);
      if (!error) {
        module->print(ir, nullptr);
      }
      (*jit)->getObjTransformLayer().setTransform(
          [prefix](std::unique_ptr<llvm::MemoryBuffer> object)
              -> llvm::Expected<std::unique_ptr<llvm::MemoryBuffer>> {
            std::ofstream file(prefix + ".o", std::ios::binary);
            file.write(object->getBufferStart(),
                       static_cast<std::streamsize>(object->getBufferSize()));
            return object;
          });
    }
  }
  if (llvm::Error error = (*jit)->addIRModule(
          llvm::orc::ThreadSafeModule(std::move(module), std::move(context)))) {
    llvm::consumeError(std::move(error));
    return nullptr;
  }
  return std::move(*jit);
}

std::shared_ptr<JitInt64Kernels::Impl> CreateImpl(
    std::unique_ptr<llvm::Module> module,
    std::unique_ptr<llvm::LLVMContext> context, std::string_view symbol) {
  auto impl = std::make_shared<JitInt64Kernels::Impl>();
  impl->jit =
      OptimizeAndCompileModule(std::move(module), std::move(context), symbol);
  if (!impl->jit) {
    return nullptr;
  }
  auto address = impl->jit->lookup(symbol);
  if (!address) {
    llvm::consumeError(address.takeError());
    return nullptr;
  }
  if (symbol == "tinylamb_filter") {
    impl->filter = address->toPtr<JitInt64Kernels::FilterFn>();
  } else if (symbol == "tinylamb_project") {
    impl->projection = address->toPtr<JitInt64Kernels::ProjectionFn>();
  } else if (symbol == "tinylamb_sum") {
    impl->sum = address->toPtr<JitInt64Kernels::SumFn>();
  } else if (symbol == "tinylamb_masked_sum") {
    impl->masked_sum = address->toPtr<JitInt64Kernels::MaskedSumFn>();
  } else if (symbol == "tinylamb_filter_project_sum") {
    impl->filter_project_sum =
        address->toPtr<JitInt64Kernels::FilterProjectSumFn>();
  } else if (symbol == "tinylamb_filter_hash_probe") {
    impl->filter_hash_probe =
        address->toPtr<JitInt64Kernels::FilterHashProbeFn>();
  } else if (symbol == "tinylamb_project_checked") {
    impl->projection_checked =
        address->toPtr<JitInt64Kernels::ProjectionCheckedFn>();
  } else {
    assert(symbol == "tinylamb_sum_checked");
    impl->sum_checked = address->toPtr<JitInt64Kernels::SumCheckedFn>();
  }
  return impl;
}

llvm::Value* Comparison(llvm::IRBuilder<>& builder, BinaryOperation operation,
                        llvm::Value* left, llvm::Value* right) {
  switch (operation) {
    case BinaryOperation::kEquals:
      return builder.CreateICmpEQ(left, right);
    case BinaryOperation::kNotEquals:
      return builder.CreateICmpNE(left, right);
    case BinaryOperation::kLessThan:
      return builder.CreateICmpSLT(left, right);
    case BinaryOperation::kLessThanEquals:
      return builder.CreateICmpSLE(left, right);
    case BinaryOperation::kGreaterThan:
      return builder.CreateICmpSGT(left, right);
    case BinaryOperation::kGreaterThanEquals:
      return builder.CreateICmpSGE(left, right);
    default:
      // CompileFilter already gates on IsComparison; if this switch and that
      // predicate ever drift, fail loudly instead of feeding nullptr to LLVM.
      std::abort();
  }
}

}  // namespace
#endif

JitInt64Kernels::JitInt64Kernels(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
JitInt64Kernels::JitInt64Kernels(JitInt64Kernels&&) noexcept = default;
JitInt64Kernels& JitInt64Kernels::operator=(JitInt64Kernels&&) noexcept =
    default;
JitInt64Kernels::~JitInt64Kernels() = default;

std::optional<JitInt64Kernels> JitInt64Kernels::CompileFilter(
    BinaryOperation operation) {
#ifndef TINYLAMB_HAS_LLVM
  (void)operation;
  return std::nullopt;
#else
  if (!IsComparison(operation)) {
    return std::nullopt;
  }
  {
    std::scoped_lock lock(jit_cache_mutex);
    const auto cached = filter_kernel_cache.find(operation);
    if (cached != filter_kernel_cache.end()) {
      return JitInt64Kernels(cached->second);
    }
  }
  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("tinylamb_filter", *context);
  llvm::IRBuilder<> builder(*context);
  llvm::Type* i64 = builder.getInt64Ty();
  llvm::Type* i8 = builder.getInt8Ty();
  auto* type = llvm::FunctionType::get(
      builder.getVoidTy(),
      {llvm::PointerType::getUnqual(*context),
       llvm::PointerType::getUnqual(*context), i64, i64},
      false);
  auto* function = llvm::Function::Create(type, llvm::Function::ExternalLinkage,
                                          "tinylamb_filter", *module);
  function->addParamAttr(0, llvm::Attribute::NoAlias);
  function->addParamAttr(1, llvm::Attribute::NoAlias);
  auto* argument = function->arg_begin();
  llvm::Value* input = argument++;
  llvm::Value* output = argument++;
  llvm::Value* count = argument++;
  llvm::Value* constant = argument++;
  auto* entry = llvm::BasicBlock::Create(*context, "entry", function);
  auto* loop = llvm::BasicBlock::Create(*context, "loop", function);
  auto* body = llvm::BasicBlock::Create(*context, "body", function);
  auto* exit = llvm::BasicBlock::Create(*context, "exit", function);
  builder.SetInsertPoint(entry);
  builder.CreateBr(loop);
  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i64, 2, "index");
  index->addIncoming(builder.getInt64(0), entry);
  builder.CreateCondBr(builder.CreateICmpULT(index, count), body, exit);
  builder.SetInsertPoint(body);
  auto* input_ptr = builder.CreateGEP(i64, input, index);
  auto* value = builder.CreateLoad(i64, input_ptr);
  auto* compared = Comparison(builder, operation, value, constant);
  auto* output_ptr = builder.CreateGEP(i8, output, index);
  builder.CreateStore(builder.CreateZExt(compared, i8), output_ptr);
  auto* next = builder.CreateAdd(index, builder.getInt64(1));
  builder.CreateBr(loop);
  index->addIncoming(next, body);
  builder.SetInsertPoint(exit);
  builder.CreateRetVoid();
  auto impl =
      CreateImpl(std::move(module), std::move(context), "tinylamb_filter");
  if (!impl) {
    return std::nullopt;
  }
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  {
    std::scoped_lock lock(jit_cache_mutex);
    auto [it, inserted] = filter_kernel_cache.try_emplace(operation, impl);
    return JitInt64Kernels(it->second);
  }
#endif
}

std::optional<JitInt64Kernels> JitInt64Kernels::CompileProjection() {
#ifndef TINYLAMB_HAS_LLVM
  return std::nullopt;
#else
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (projection_kernel_cache) {
      return JitInt64Kernels(projection_kernel_cache);
    }
  }
  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("tinylamb_project", *context);
  llvm::IRBuilder<> builder(*context);
  llvm::Type* i64 = builder.getInt64Ty();
  auto* type = llvm::FunctionType::get(
      builder.getVoidTy(),
      {llvm::PointerType::getUnqual(*context),
       llvm::PointerType::getUnqual(*context), i64, i64, i64},
      false);
  auto* function = llvm::Function::Create(type, llvm::Function::ExternalLinkage,
                                          "tinylamb_project", *module);
  function->addParamAttr(0, llvm::Attribute::NoAlias);
  function->addParamAttr(1, llvm::Attribute::NoAlias);
  auto* argument = function->arg_begin();
  llvm::Value* input = argument++;
  llvm::Value* output = argument++;
  llvm::Value* count = argument++;
  llvm::Value* multiplier = argument++;
  llvm::Value* addend = argument++;
  auto* entry = llvm::BasicBlock::Create(*context, "entry", function);
  auto* loop = llvm::BasicBlock::Create(*context, "loop", function);
  auto* body = llvm::BasicBlock::Create(*context, "body", function);
  auto* exit = llvm::BasicBlock::Create(*context, "exit", function);
  builder.SetInsertPoint(entry);
  builder.CreateBr(loop);
  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i64, 2);
  index->addIncoming(builder.getInt64(0), entry);
  builder.CreateCondBr(builder.CreateICmpULT(index, count), body, exit);
  builder.SetInsertPoint(body);
  auto* value = builder.CreateLoad(i64, builder.CreateGEP(i64, input, index));
  auto* projected =
      builder.CreateAdd(builder.CreateMul(value, multiplier), addend);
  builder.CreateStore(projected, builder.CreateGEP(i64, output, index));
  auto* next = builder.CreateAdd(index, builder.getInt64(1));
  builder.CreateBr(loop);
  index->addIncoming(next, body);
  builder.SetInsertPoint(exit);
  builder.CreateRetVoid();
  auto impl =
      CreateImpl(std::move(module), std::move(context), "tinylamb_project");
  if (!impl) {
    return std::nullopt;
  }
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (!projection_kernel_cache) {
      projection_kernel_cache = std::move(impl);
    }
    return JitInt64Kernels(projection_kernel_cache);
  }
#endif
}

std::optional<JitInt64Kernels> JitInt64Kernels::CompileSum() {
#ifndef TINYLAMB_HAS_LLVM
  return std::nullopt;
#else
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (sum_kernel_cache) {
      return JitInt64Kernels(sum_kernel_cache);
    }
  }
  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("tinylamb_sum", *context);
  llvm::IRBuilder<> builder(*context);
  llvm::Type* i64 = builder.getInt64Ty();
  auto* type = llvm::FunctionType::get(
      i64, {llvm::PointerType::getUnqual(*context), i64}, false);
  auto* function = llvm::Function::Create(type, llvm::Function::ExternalLinkage,
                                          "tinylamb_sum", *module);
  function->addParamAttr(0, llvm::Attribute::NoAlias);
  auto* argument = function->arg_begin();
  llvm::Value* input = argument++;
  llvm::Value* count = argument++;
  auto* entry = llvm::BasicBlock::Create(*context, "entry", function);
  auto* loop = llvm::BasicBlock::Create(*context, "loop", function);
  auto* body = llvm::BasicBlock::Create(*context, "body", function);
  auto* exit = llvm::BasicBlock::Create(*context, "exit", function);
  builder.SetInsertPoint(entry);
  builder.CreateBr(loop);
  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i64, 2);
  auto* total = builder.CreatePHI(i64, 2);
  index->addIncoming(builder.getInt64(0), entry);
  total->addIncoming(builder.getInt64(0), entry);
  builder.CreateCondBr(builder.CreateICmpULT(index, count), body, exit);
  builder.SetInsertPoint(body);
  auto* value = builder.CreateLoad(i64, builder.CreateGEP(i64, input, index));
  auto* next_total = builder.CreateAdd(total, value);
  auto* next = builder.CreateAdd(index, builder.getInt64(1));
  builder.CreateBr(loop);
  index->addIncoming(next, body);
  total->addIncoming(next_total, body);
  builder.SetInsertPoint(exit);
  builder.CreateRet(total);
  auto impl = CreateImpl(std::move(module), std::move(context), "tinylamb_sum");
  if (!impl) {
    return std::nullopt;
  }
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (!sum_kernel_cache) {
      sum_kernel_cache = std::move(impl);
    }
    return JitInt64Kernels(sum_kernel_cache);
  }
#endif
}

// Lean proof: lean/TinylambOpt/BranchlessJitCodegen.lean
//   (zonemap_prefix_sum_masked_sound)
std::optional<JitInt64Kernels> JitInt64Kernels::CompileMaskedSum() {
#ifndef TINYLAMB_HAS_LLVM
  return std::nullopt;
#else
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (masked_sum_kernel_cache) {
      return JitInt64Kernels(masked_sum_kernel_cache);
    }
  }
  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("tinylamb_masked_sum", *context);
  llvm::IRBuilder<> builder(*context);
  llvm::Type* i64 = builder.getInt64Ty();
  llvm::Type* i8 = builder.getInt8Ty();
  auto* ptr_ty = llvm::PointerType::getUnqual(*context);
  auto* type = llvm::FunctionType::get(i64, {ptr_ty, ptr_ty, i64}, false);
  auto* function = llvm::Function::Create(type, llvm::Function::ExternalLinkage,
                                          "tinylamb_masked_sum", *module);
  function->addParamAttr(0, llvm::Attribute::NoAlias);
  function->addParamAttr(1, llvm::Attribute::NoAlias);
  auto* argument = function->arg_begin();
  llvm::Value* input = argument++;
  llvm::Value* nulls = argument++;
  llvm::Value* count = argument++;
  auto* entry = llvm::BasicBlock::Create(*context, "entry", function);
  auto* loop = llvm::BasicBlock::Create(*context, "loop", function);
  auto* body = llvm::BasicBlock::Create(*context, "body", function);
  auto* exit = llvm::BasicBlock::Create(*context, "exit", function);
  builder.SetInsertPoint(entry);
  builder.CreateBr(loop);
  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i64, 2);
  auto* total = builder.CreatePHI(i64, 2);
  index->addIncoming(builder.getInt64(0), entry);
  total->addIncoming(builder.getInt64(0), entry);
  builder.CreateCondBr(builder.CreateICmpULT(index, count), body, exit);
  builder.SetInsertPoint(body);
  auto* value = builder.CreateLoad(i64, builder.CreateGEP(i64, input, index));
  auto* null_byte = builder.CreateLoad(i8, builder.CreateGEP(i8, nulls, index));
  auto* is_null = builder.CreateICmpNE(null_byte, builder.getInt8(0));
  auto* masked = builder.CreateSelect(is_null, builder.getInt64(0), value);
  auto* next_total = builder.CreateAdd(total, masked);
  auto* next = builder.CreateAdd(index, builder.getInt64(1));
  builder.CreateBr(loop);
  index->addIncoming(next, body);
  total->addIncoming(next_total, body);
  builder.SetInsertPoint(exit);
  builder.CreateRet(total);
  auto impl =
      CreateImpl(std::move(module), std::move(context), "tinylamb_masked_sum");
  if (!impl) {
    return std::nullopt;
  }
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (!masked_sum_kernel_cache) {
      masked_sum_kernel_cache = std::move(impl);
    }
    return JitInt64Kernels(masked_sum_kernel_cache);
  }
#endif
}

// Lean proof: lean/TinylambOpt/FusedAggregateJit.lean
// (fused_matches_materialized, guarded_fused_sum)
std::optional<JitInt64Kernels> JitInt64Kernels::CompileFilterProjectSum(
    BinaryOperation op, bool skip_rejected_loads) {
#ifndef TINYLAMB_HAS_LLVM
  (void)op;
  (void)skip_rejected_loads;
  return std::nullopt;
#else
  if (!IsComparison(op)) {
    return std::nullopt;
  }
  auto& cache = filter_project_sum_cache[skip_rejected_loads ? 1 : 0];
  const std::scoped_lock lock(jit_cache_mutex);
  if (const auto hit = cache.find(op); hit != cache.end()) {
    return JitInt64Kernels(hit->second);
  }
  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>(
      skip_rejected_loads ? "fused_sum_conditional_load" : "fused_sum_masked",
      *context);
  llvm::IRBuilder<> builder(*context);
  auto* i64 = builder.getInt64Ty();
  auto* ptr = builder.getPtrTy();
  auto* type = llvm::FunctionType::get(
      i64, {ptr, ptr, i64, i64, i64, i64, i64, ptr}, false);
  constexpr const char* kSymbol = "tinylamb_filter_project_sum";
  auto* function = llvm::Function::Create(type, llvm::Function::ExternalLinkage,
                                          kSymbol, *module);
  function->addParamAttr(7, llvm::Attribute::NoAlias);
  auto* keys = function->getArg(0);
  auto* payload = function->getArg(1);
  auto* count = function->getArg(2);
  auto* constant = function->getArg(3);
  auto* multiplier = function->getArg(4);
  auto* addend = function->getArg(5);
  auto* initial = function->getArg(6);
  auto* output_count = function->getArg(7);

  auto block = [&](const char* name) {
    return llvm::BasicBlock::Create(*context, name, function);
  };
  auto* entry = block("entry");
  auto* loop = block("loop");
  auto* body = block("body");
  auto* exit = block("exit");
  auto* zero = builder.getInt64(0);
  builder.SetInsertPoint(entry);
  builder.CreateBr(loop);
  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i64, 2, "row");
  auto* sum = builder.CreatePHI(i64, 2, "sum");
  auto* kept = builder.CreatePHI(i64, 2, "kept");
  index->addIncoming(zero, entry);
  sum->addIncoming(initial, entry);
  kept->addIncoming(zero, entry);
  builder.CreateCondBr(builder.CreateICmpULT(index, count), body, exit);
  builder.SetInsertPoint(body);
  auto* key = builder.CreateLoad(i64, builder.CreateGEP(i64, keys, index));
  auto* pass = Comparison(builder, op, key, constant);
  const auto project = [&] {
    auto* value =
        builder.CreateLoad(i64, builder.CreateGEP(i64, payload, index));
    return builder.CreateAdd(builder.CreateMul(value, multiplier), addend);
  };
  llvm::Value* contribution = nullptr;
  if (skip_rejected_loads) {
    auto* accepted = block("accepted");
    auto* latch = block("latch");
    builder.CreateCondBr(pass, accepted, latch);
    builder.SetInsertPoint(accepted);
    auto* value = project();
    builder.CreateBr(latch);
    builder.SetInsertPoint(latch);
    auto* selected = builder.CreatePHI(i64, 2, "contribution");
    selected->addIncoming(zero, body);
    selected->addIncoming(value, accepted);
    contribution = selected;
  } else {
    contribution = builder.CreateSelect(pass, project(), zero);
  }
  auto* latch = builder.GetInsertBlock();
  index->addIncoming(builder.CreateAdd(index, builder.getInt64(1)), latch);
  sum->addIncoming(builder.CreateAdd(sum, contribution), latch);
  kept->addIncoming(builder.CreateAdd(kept, builder.CreateZExt(pass, i64)),
                    latch);
  builder.CreateBr(loop);
  builder.SetInsertPoint(exit);
  builder.CreateStore(kept, output_count);
  builder.CreateRet(sum);
  auto impl = CreateImpl(std::move(module), std::move(context), kSymbol);
  if (!impl) {
    return std::nullopt;
  }
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  return JitInt64Kernels(cache.emplace(op, std::move(impl)).first->second);
#endif
}

// Lean model: TinylambOpt.FilterHashProbe. The native and generated loops use
// the same hash constants and stop linear probing on the same empty slot.
std::optional<JitInt64Kernels> JitInt64Kernels::CompileFilterHashProbe(
    BinaryOperation op) {
#ifndef TINYLAMB_HAS_LLVM
  (void)op;
  return std::nullopt;
#else
  if (sizeof(size_t) != sizeof(uint64_t) ||
      std::endian::native != std::endian::little || !IsComparison(op)) {
    return std::nullopt;
  }
  const std::scoped_lock lock(jit_cache_mutex);
  if (const auto hit = filter_hash_probe_cache.find(op);
      hit != filter_hash_probe_cache.end()) {
    return JitInt64Kernels(hit->second);
  }
  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("filter_hash_probe", *context);
  llvm::IRBuilder<> builder(*context);
  auto* i64 = builder.getInt64Ty();
  auto* ptr = builder.getPtrTy();
  auto* type = llvm::FunctionType::get(
      i64, {ptr, ptr, i64, i64, ptr, ptr, i64, ptr, ptr, ptr}, false);
  constexpr const char* kSymbol = "tinylamb_filter_hash_probe";
  auto* function = llvm::Function::Create(type, llvm::Function::ExternalLinkage,
                                          kSymbol, *module);
  auto* filter = function->getArg(0);
  auto* keys = function->getArg(1);
  auto* count = function->getArg(2);
  auto* constant = function->getArg(3);
  auto* heads = function->getArg(4);
  auto* slot_keys = function->getArg(5);
  auto* mask = function->getArg(6);
  auto* cursor = function->getArg(7);
  auto* output_row = function->getArg(8);
  auto* accepted = function->getArg(9);

  const auto block = [&](const char* name) {
    return llvm::BasicBlock::Create(*context, name, function);
  };
  auto* entry = block("entry");
  auto* loop = block("mask_loop");
  auto* refill = block("refill");
  auto* test_width = block("test_width");
  auto* compare_vector = block("compare_vector");
  auto* compare_tail = block("compare_tail");
  auto* hash_key = block("hash");
  auto* slot_loop = block("slot_loop");
  auto* test_key = block("test_key");
  auto* collision = block("collision");
  auto* done = block("done");
  auto* one = builder.getInt64(1);
  auto* width = builder.getInt64(8);
  auto* empty = builder.getInt64(UINT64_MAX);
  auto* zero = builder.getInt64(0);
  const auto load = [&](llvm::Value* base, llvm::Value* offset) {
    return builder.CreateLoad(i64, builder.CreateGEP(i64, base, offset));
  };

  builder.SetInsertPoint(entry);
  auto* packed = builder.CreateLoad(i64, cursor);
  auto* begin_block = builder.CreateLShr(packed, 8);
  auto* begin_mask = builder.CreateAnd(packed, builder.getInt64(255));
  builder.CreateBr(loop);
  builder.SetInsertPoint(loop);
  auto* block_end = builder.CreatePHI(i64, 4, "block_end");
  auto* pending = builder.CreatePHI(i64, 4, "pending");
  auto* kept = builder.CreatePHI(i64, 4, "kept");
  block_end->addIncoming(begin_block, entry);
  pending->addIncoming(begin_mask, entry);
  kept->addIncoming(zero, entry);
  builder.CreateCondBr(builder.CreateICmpNE(pending, zero), hash_key, refill);

  builder.SetInsertPoint(refill);
  builder.CreateCondBr(builder.CreateICmpULT(block_end, count), test_width,
                       done);
  builder.SetInsertPoint(test_width);
  builder.CreateCondBr(
      builder.CreateICmpUGE(builder.CreateSub(count, block_end), width),
      compare_vector, compare_tail);
  builder.SetInsertPoint(compare_vector);
  auto* vector_type = llvm::FixedVectorType::get(i64, 8);
  auto* vector = builder.CreateAlignedLoad(
      vector_type, builder.CreateGEP(i64, filter, block_end), llvm::Align(8));
  auto* passes =
      Comparison(builder, op, vector, builder.CreateVectorSplat(8, constant));
  auto* bits = builder.CreateBitCast(passes, builder.getInt8Ty());
  block_end->addIncoming(builder.CreateAdd(block_end, width), compare_vector);
  pending->addIncoming(builder.CreateZExt(bits, i64), compare_vector);
  kept->addIncoming(kept, compare_vector);
  builder.CreateBr(loop);

  builder.SetInsertPoint(compare_tail);
  auto* pass = Comparison(builder, op, load(filter, block_end), constant);
  block_end->addIncoming(builder.CreateAdd(block_end, one), compare_tail);
  pending->addIncoming(builder.CreateSelect(pass, builder.getInt64(128), zero),
                       compare_tail);
  kept->addIncoming(kept, compare_tail);
  builder.CreateBr(loop);

  builder.SetInsertPoint(hash_key);
  auto* cttz = IntrinsicDecl(module.get(), llvm::Intrinsic::cttz, {i64});
  auto* lane = builder.CreateCall(cttz, {pending, builder.getTrue()});
  auto* row = builder.CreateSub(block_end, builder.CreateSub(width, lane));
  auto* remaining = builder.CreateAnd(pending, builder.CreateSub(pending, one));
  auto* after_kept = builder.CreateAdd(kept, one);
  auto* key = load(keys, row);
  llvm::Value* hash =
      builder.CreateMul(key, builder.getInt64(kJoinHashMultiplier1));
  hash = builder.CreateXor(hash, builder.CreateLShr(hash, kJoinHashShift1));
  hash = builder.CreateMul(hash, builder.getInt64(kJoinHashMultiplier2));
  hash = builder.CreateXor(hash, builder.CreateLShr(hash, kJoinHashShift2));
  auto* home = builder.CreateAnd(hash, mask);
  builder.CreateBr(slot_loop);

  builder.SetInsertPoint(slot_loop);
  auto* slot = builder.CreatePHI(i64, 2, "slot");
  slot->addIncoming(home, hash_key);
  auto* head = load(heads, slot);
  block_end->addIncoming(block_end, slot_loop);
  pending->addIncoming(remaining, slot_loop);
  kept->addIncoming(after_kept, slot_loop);
  builder.CreateCondBr(builder.CreateICmpEQ(head, empty), loop, test_key);
  builder.SetInsertPoint(test_key);
  builder.CreateCondBr(builder.CreateICmpEQ(load(slot_keys, slot), key), done,
                       collision);
  builder.SetInsertPoint(collision);
  slot->addIncoming(builder.CreateAnd(builder.CreateAdd(slot, one), mask),
                    collision);
  builder.CreateBr(slot_loop);

  builder.SetInsertPoint(done);
  auto* result = builder.CreatePHI(i64, 2);
  auto* result_row = builder.CreatePHI(i64, 2);
  auto* result_mask = builder.CreatePHI(i64, 2);
  auto* selected = builder.CreatePHI(i64, 2);
  result->addIncoming(empty, refill);
  result->addIncoming(head, test_key);
  result_row->addIncoming(count, refill);
  result_row->addIncoming(row, test_key);
  result_mask->addIncoming(zero, refill);
  result_mask->addIncoming(remaining, test_key);
  selected->addIncoming(kept, refill);
  selected->addIncoming(after_kept, test_key);
  builder.CreateStore(
      builder.CreateOr(builder.CreateShl(block_end, 8), result_mask), cursor);
  builder.CreateStore(result_row, output_row);
  builder.CreateStore(selected, accepted);
  builder.CreateRet(result);
  auto impl = CreateImpl(std::move(module), std::move(context), kSymbol);
  if (!impl) {
    return std::nullopt;
  }
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  return JitInt64Kernels(
      filter_hash_probe_cache.emplace(op, std::move(impl)).first->second);
#endif
}

std::optional<JitInt64Kernels> JitInt64Kernels::CompileProjectionChecked() {
#ifndef TINYLAMB_HAS_LLVM
  return std::nullopt;
#else
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (projection_checked_kernel_cache) {
      return JitInt64Kernels(projection_checked_kernel_cache);
    }
  }
  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb_project_checked", *context);
  llvm::IRBuilder<> builder(*context);
  llvm::Type* i64 = builder.getInt64Ty();
  llvm::Type* i8 = builder.getInt8Ty();
  llvm::Type* i1 = builder.getInt1Ty();
  auto* i8_ptr = llvm::PointerType::getUnqual(*context);
  auto* type = llvm::FunctionType::get(
      builder.getVoidTy(),
      {llvm::PointerType::getUnqual(*context),
       llvm::PointerType::getUnqual(*context), i64, i64, i64, i8_ptr, i8_ptr},
      false);
  auto* function = llvm::Function::Create(type, llvm::Function::ExternalLinkage,
                                          "tinylamb_project_checked", *module);
  function->addParamAttr(0, llvm::Attribute::NoAlias);
  function->addParamAttr(1, llvm::Attribute::NoAlias);
  function->addParamAttr(5, llvm::Attribute::NoAlias);
  function->addParamAttr(6, llvm::Attribute::NoAlias);
  auto* argument = function->arg_begin();
  llvm::Value* input = argument++;
  llvm::Value* output = argument++;
  llvm::Value* count = argument++;
  llvm::Value* multiplier = argument++;
  llvm::Value* addend = argument++;
  llvm::Value* mul_overflow_out = argument++;
  llvm::Value* add_overflow_out = argument++;
  llvm::Function* smul =
      IntrinsicDecl(module.get(), llvm::Intrinsic::smul_with_overflow, {i64});
  llvm::Function* sadd =
      IntrinsicDecl(module.get(), llvm::Intrinsic::sadd_with_overflow, {i64});
  auto* entry = llvm::BasicBlock::Create(*context, "entry", function);
  auto* loop = llvm::BasicBlock::Create(*context, "loop", function);
  auto* body = llvm::BasicBlock::Create(*context, "body", function);
  auto* exit = llvm::BasicBlock::Create(*context, "exit", function);
  builder.SetInsertPoint(entry);
  builder.CreateBr(loop);
  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i64, 2);
  auto* mul_overflow = builder.CreatePHI(i1, 2);
  auto* add_overflow = builder.CreatePHI(i1, 2);
  index->addIncoming(builder.getInt64(0), entry);
  mul_overflow->addIncoming(builder.getFalse(), entry);
  add_overflow->addIncoming(builder.getFalse(), entry);
  builder.CreateCondBr(builder.CreateICmpULT(index, count), body, exit);
  builder.SetInsertPoint(body);
  auto* value = builder.CreateLoad(i64, builder.CreateGEP(i64, input, index));
  auto* mul_pair = builder.CreateCall(smul, {value, multiplier});
  auto* product = builder.CreateExtractValue(mul_pair, 0);
  auto* mul_of = builder.CreateExtractValue(mul_pair, 1);
  auto* add_pair = builder.CreateCall(sadd, {product, addend});
  auto* projected = builder.CreateExtractValue(add_pair, 0);
  auto* add_of = builder.CreateExtractValue(add_pair, 1);
  builder.CreateStore(projected, builder.CreateGEP(i64, output, index));
  auto* next = builder.CreateAdd(index, builder.getInt64(1));
  auto* next_mul_overflow = builder.CreateOr(mul_overflow, mul_of);
  auto* next_add_overflow = builder.CreateOr(add_overflow, add_of);
  builder.CreateBr(loop);
  index->addIncoming(next, body);
  mul_overflow->addIncoming(next_mul_overflow, body);
  add_overflow->addIncoming(next_add_overflow, body);
  builder.SetInsertPoint(exit);
  builder.CreateStore(builder.CreateZExt(mul_overflow, i8), mul_overflow_out);
  builder.CreateStore(builder.CreateZExt(add_overflow, i8), add_overflow_out);
  builder.CreateRetVoid();
  auto impl = CreateImpl(std::move(module), std::move(context),
                         "tinylamb_project_checked");
  if (!impl) {
    return std::nullopt;
  }
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (!projection_checked_kernel_cache) {
      projection_checked_kernel_cache = std::move(impl);
    }
    return JitInt64Kernels(projection_checked_kernel_cache);
  }
#endif
}

std::optional<JitInt64Kernels> JitInt64Kernels::CompileSumChecked() {
#ifndef TINYLAMB_HAS_LLVM
  return std::nullopt;
#else
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (sum_checked_kernel_cache) {
      return JitInt64Kernels(sum_checked_kernel_cache);
    }
  }
  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb_sum_checked", *context);
  llvm::IRBuilder<> builder(*context);
  llvm::Type* i64 = builder.getInt64Ty();
  llvm::Type* i8 = builder.getInt8Ty();
  llvm::Type* i1 = builder.getInt1Ty();
  auto* type =
      llvm::FunctionType::get(i64,
                              {llvm::PointerType::getUnqual(*context), i64,
                               llvm::PointerType::getUnqual(*context)},
                              false);
  auto* function = llvm::Function::Create(type, llvm::Function::ExternalLinkage,
                                          "tinylamb_sum_checked", *module);
  function->addParamAttr(0, llvm::Attribute::NoAlias);
  function->addParamAttr(2, llvm::Attribute::NoAlias);
  auto* argument = function->arg_begin();
  llvm::Value* input = argument++;
  llvm::Value* count = argument++;
  llvm::Value* overflow_out = argument++;
  llvm::Function* sadd =
      IntrinsicDecl(module.get(), llvm::Intrinsic::sadd_with_overflow, {i64});
  auto* entry = llvm::BasicBlock::Create(*context, "entry", function);
  auto* loop = llvm::BasicBlock::Create(*context, "loop", function);
  auto* body = llvm::BasicBlock::Create(*context, "body", function);
  auto* exit = llvm::BasicBlock::Create(*context, "exit", function);
  builder.SetInsertPoint(entry);
  builder.CreateBr(loop);
  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i64, 2);
  auto* total = builder.CreatePHI(i64, 2);
  auto* overflow = builder.CreatePHI(i1, 2);
  index->addIncoming(builder.getInt64(0), entry);
  total->addIncoming(builder.getInt64(0), entry);
  overflow->addIncoming(builder.getFalse(), entry);
  builder.CreateCondBr(builder.CreateICmpULT(index, count), body, exit);
  builder.SetInsertPoint(body);
  auto* value = builder.CreateLoad(i64, builder.CreateGEP(i64, input, index));
  auto* pair = builder.CreateCall(sadd, {total, value});
  auto* next_total = builder.CreateExtractValue(pair, 0);
  auto* step_overflow = builder.CreateExtractValue(pair, 1);
  auto* next = builder.CreateAdd(index, builder.getInt64(1));
  auto* next_overflow = builder.CreateOr(overflow, step_overflow);
  builder.CreateBr(loop);
  index->addIncoming(next, body);
  total->addIncoming(next_total, body);
  overflow->addIncoming(next_overflow, body);
  builder.SetInsertPoint(exit);
  builder.CreateStore(builder.CreateZExt(overflow, i8), overflow_out);
  builder.CreateRet(total);
  auto impl =
      CreateImpl(std::move(module), std::move(context), "tinylamb_sum_checked");
  if (!impl) {
    return std::nullopt;
  }
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (!sum_checked_kernel_cache) {
      sum_checked_kernel_cache = std::move(impl);
    }
    return JitInt64Kernels(sum_checked_kernel_cache);
  }
#endif
}

// Clang's function-type sanitizer probes eight bytes BEFORE an indirect-call
// target for its type header. ORC machine code has no Clang UBSan header and
// can start at the first byte of an executable mapping: that probe itself
// faults before entering the kernel. Disable only this header-based check at
// the foreign-code boundaries; address/other undefined-behavior checks and
// the checked-kernel overflow contract remain enabled.
#ifdef __clang__
#define TINYLAMB_JIT_CALL_BOUNDARY __attribute__((no_sanitize("function")))
#else
#define TINYLAMB_JIT_CALL_BOUNDARY
#endif

// Lean proof: lean/TinylambOpt/PhysicalJitFusion.lean
// (anchored_first_match, first_mask_lane, vector_window_load_in_bounds,
//  overlapping_tail_safe, shifted_literal_mask_exact, literal_mask_tail_safe)
std::optional<JitLiteralSearch> JitLiteralSearch::Compile(
    std::string_view needle) {
#ifndef TINYLAMB_HAS_LLVM
  (void)needle;
  return std::nullopt;
#else
  constexpr size_t kMaxNeedle = 16;
  constexpr size_t kMaxCachedNeedles = 32;
  // The lane mask uses lane 0 as its least significant bit.
  if (std::endian::native != std::endian::little || needle.size() < 2 ||
      needle.size() > kMaxNeedle ||
      std::getenv("TINYLAMB_DISABLE_LITERAL_SEARCH_JIT") != nullptr) {
    return std::nullopt;
  }
  bool full_literal_mask = false;
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
  full_literal_mask =
      needle.size() <= 8 && __builtin_cpu_supports("avx512bw") &&
      std::getenv("TINYLAMB_DISABLE_FULL_LITERAL_MASK_JIT") == nullptr;
#endif
  const size_t lanes = full_literal_mask ? 64 : 32;
  struct Entry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    FindFn find;
  };
  // ORC must outlive the compiled handles, including at static destruction.
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto* const cache = new std::unordered_map<std::string, Entry>;
  const std::scoped_lock lock(jit_cache_mutex);
  std::string key(needle);
  const auto cached = cache->find(key);
  if (cached != cache->end()) {
    return JitLiteralSearch(cached->second.find, needle.size());
  }
  if (cache->size() == kMaxCachedNeedles) {
    return std::nullopt;
  }

  InitializeLlvm();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.literal_search", *context);
  llvm::IRBuilder<> builder(*context);
  constexpr std::string_view kSymbol = "tinylamb_find_literal";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(builder.getInt64Ty(),
                              {builder.getPtrTy(), builder.getInt64Ty()},
                              false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  auto* bytes = fn->getArg(0);
  auto* size = fn->getArg(1);
  auto block = [&](std::string_view name) {
    return llvm::BasicBlock::Create(*context, name, fn);
  };
  auto* entry = block("entry");
  auto* loop = block("loop");
  auto* dispatch = block("dispatch");
  auto* vector = block("vector");
  auto* advance = block("advance");
  auto* candidate = block("candidate");
  auto* verify = block("verify");
  auto* next = block("next");
  auto* hit = block("hit");
  auto* miss = block("miss");

  builder.SetInsertPoint(entry);
  const size_t last = needle.size() - 1;
  const size_t starts = full_literal_mask ? lanes - last : lanes;
  auto* mask_type = builder.getIntNTy(static_cast<unsigned>(lanes));
  auto* limit = builder.CreateSub(size, builder.getInt64(last));
  builder.CreateBr(loop);

  builder.SetInsertPoint(loop);
  auto* offset = builder.CreatePHI(builder.getInt64Ty(), 3, "offset");
  offset->addIncoming(builder.getInt64(0), entry);
  builder.CreateCondBr(builder.CreateICmpULT(offset, limit), dispatch, miss);

  builder.SetInsertPoint(dispatch);
  builder.CreateCondBr(builder.CreateICmpUGE(limit, builder.getInt64(starts)),
                       vector, verify);

  auto load_at = [&](llvm::Type* type, llvm::Value* start, size_t delta) {
    auto* index = builder.CreateAdd(start, builder.getInt64(delta));
    auto* ptr = builder.CreateInBoundsGEP(builder.getInt8Ty(), bytes, index);
    return builder.CreateAlignedLoad(type, ptr, llvm::Align(1));
  };
  auto byte = [&](size_t i) {
    return builder.getInt8(static_cast<uint8_t>(needle[i]));
  };

  builder.SetInsertPoint(vector);
  // For the tail, load the last complete SIMD window. Mask out the starts
  // already visited by the loop. Its last lane still fits the original input.
  auto* last_vector = builder.CreateSub(limit, builder.getInt64(starts));
  auto* base = builder.CreateSelect(builder.CreateICmpULT(offset, last_vector),
                                    offset, last_vector);
  const auto splat = [&](size_t i) {
    return llvm::ConstantVector::getSplat(
        llvm::ElementCount::getFixed(static_cast<unsigned>(lanes)), byte(i));
  };
  auto* vector_type = splat(0)->getType();
  llvm::Value* mask = nullptr;
  if (full_literal_mask) {
    auto* window = load_at(vector_type, base, 0);
    std::array<llvm::Value*, 256> equals{};
    mask = llvm::ConstantInt::getAllOnesValue(mask_type);
    for (size_t i = 0; i < needle.size(); ++i) {
      auto*& same = equals[static_cast<uint8_t>(needle[i])];
      if (same == nullptr) {
        same = builder.CreateBitCast(builder.CreateICmpEQ(window, splat(i)),
                                     mask_type);
      }
      mask = builder.CreateAnd(mask, builder.CreateLShr(same, i));
    }
  } else {
    auto* first_matches =
        builder.CreateICmpEQ(load_at(vector_type, base, 0), splat(0));
    auto* last_matches =
        builder.CreateICmpEQ(load_at(vector_type, base, last), splat(last));
    mask = builder.CreateBitCast(builder.CreateAnd(first_matches, last_matches),
                                 mask_type);
  }
  mask = builder.CreateAnd(
      mask, builder.CreateShl(llvm::ConstantInt::getAllOnesValue(mask_type),
                              builder.CreateZExtOrTrunc(
                                  builder.CreateSub(offset, base), mask_type)));
  builder.CreateCondBr(
      builder.CreateICmpEQ(mask, llvm::ConstantInt::get(mask_type, 0)), advance,
      candidate);

  builder.SetInsertPoint(advance);
  offset->addIncoming(builder.CreateAdd(base, builder.getInt64(starts)),
                      advance);
  builder.CreateBr(loop);

  builder.SetInsertPoint(candidate);
  auto* lane = builder.CreateCall(
      IntrinsicDecl(module.get(), llvm::Intrinsic::cttz, {mask_type}),
      {mask, builder.getTrue()});
  auto* start = builder.CreateAdd(
      base, builder.CreateZExtOrTrunc(lane, builder.getInt64Ty()));
  if (full_literal_mask) {
    builder.CreateRet(start);
  } else {
    builder.CreateBr(verify);
  }

  builder.SetInsertPoint(verify);
  auto* position = builder.CreatePHI(builder.getInt64Ty(), 2, "position");
  position->addIncoming(offset, dispatch);
  if (!full_literal_mask) {
    position->addIncoming(start, candidate);
  }
  llvm::Value* equal = builder.getTrue();
  for (size_t i = 0; i < needle.size(); ++i) {
    equal = builder.CreateAnd(
        equal, builder.CreateICmpEQ(load_at(builder.getInt8Ty(), position, i),
                                    byte(i)));
  }
  builder.CreateCondBr(equal, hit, next);

  builder.SetInsertPoint(next);
  offset->addIncoming(builder.CreateAdd(position, builder.getInt64(1)), next);
  builder.CreateBr(loop);

  builder.SetInsertPoint(hit);
  builder.CreateRet(position);
  builder.SetInsertPoint(miss);
  builder.CreateRet(size);

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(context),
                                      "literal-search");
  if (jit == nullptr) {
    return std::nullopt;
  }
  auto address = jit->lookup(kSymbol);
  if (!address) {
    llvm::consumeError(address.takeError());
    return std::nullopt;
  }
  const auto find = address->toPtr<FindFn>();
  cache->emplace(std::move(key), Entry{.jit = std::move(jit), .find = find});
  return JitLiteralSearch(find, needle.size());
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
size_t JitLiteralSearch::Find(std::span<const uint8_t> bytes) const noexcept {
  if (bytes.size() < length_) {
    return bytes.size();
  }
  return static_cast<size_t>(find_(bytes.data(), bytes.size()));
}

// Lean: DelimitedCaptureJit (prefix_expansion, first_delimiter_capture,
// packed_slice, PhysicalJitFusion.overlapping_tail_safe).
std::optional<JitDelimitedCapture> JitDelimitedCapture::Compile(
    const DelimitedRegexCapture& capture) {
#ifndef TINYLAMB_HAS_LLVM
  (void)capture;
  return std::nullopt;
#else
  constexpr size_t kLanes = 32;
  if (std::endian::native != std::endian::little || capture.prefixes.empty() ||
      capture.prefixes.size() > 8 ||
      std::ranges::any_of(
          capture.prefixes,
          [](const auto& prefix) { return prefix.size() > 32; }) ||
      std::getenv("TINYLAMB_DISABLE_DELIMITED_CAPTURE_JIT") != nullptr) {
    return std::nullopt;
  }
  struct Entry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    CaptureFn capture;
  };
  static auto* const cache = new std::unordered_map<std::string, Entry>;
  std::string key(1, static_cast<char>(capture.delimiter));
  for (const auto& prefix : capture.prefixes) {
    key.push_back(static_cast<char>(prefix.size()));
    key += prefix;
  }
  const std::scoped_lock lock(jit_cache_mutex);
  if (const auto found = cache->find(key); found != cache->end()) {
    return JitDelimitedCapture(found->second.capture);
  }
  if (cache->size() >= 32) {
    return std::nullopt;
  }

  InitializeLlvm();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.delimited_capture", *context);
  llvm::IRBuilder<> b(*context);
  constexpr std::string_view kSymbol = "tinylamb_delimited_capture";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(b.getInt64Ty(), {b.getPtrTy(), b.getInt64Ty()},
                              false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  auto* bytes = fn->getArg(0);
  auto* size = fn->getArg(1);
  const auto block = [&](std::string_view name) {
    return llvm::BasicBlock::Create(*context, name, fn);
  };
  const auto load = [&](llvm::Type* type, llvm::Value* index) {
    return b.CreateAlignedLoad(
        type, b.CreateInBoundsGEP(b.getInt8Ty(), bytes, index), llvm::Align(1));
  };
  auto* next = block("entry");
  auto* delimiter = b.getInt8(capture.delimiter);
  auto* delimiter_vector = llvm::ConstantVector::getSplat(
      llvm::ElementCount::getFixed(kLanes), delimiter);
  auto* cttz =
      IntrinsicDecl(module.get(), llvm::Intrinsic::cttz, {b.getInt32Ty()});
  for (const auto& prefix : capture.prefixes) {
    auto* guard = next;
    auto* compare = block("prefix");
    auto* loop = block("delimiter");
    auto* dispatch = block("delimiter.dispatch");
    auto* vector = block("delimiter.vector");
    auto* vector_step = block("delimiter.vector.step");
    auto* vector_hit = block("delimiter.vector.hit");
    auto* scalar = block("delimiter.scalar");
    auto* scalar_step = block("delimiter.scalar.step");
    auto* found = block("delimiter.found");
    auto* result = block("capture");
    next = block("next.prefix");
    auto* start = b.getInt64(prefix.size());
    b.SetInsertPoint(guard);
    b.CreateCondBr(b.CreateICmpUGE(size, b.getInt64(prefix.size() + 2)),
                   compare, next);
    b.SetInsertPoint(compare);
    llvm::Value* matches = b.getTrue();
    for (size_t i = 0; i < prefix.size();) {
      const size_t width =
          std::bit_floor(std::min<size_t>(8, prefix.size() - i));
      uint64_t value = 0;
      for (size_t j = 0; j < width; ++j) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(prefix[i + j]))
                 << (8 * j);
      }
      auto* type = b.getIntNTy(static_cast<unsigned>(8 * width));
      matches = b.CreateAnd(
          matches, b.CreateICmpEQ(load(type, b.getInt64(i)),
                                  llvm::ConstantInt::get(type, value)));
      i += width;
    }
    b.CreateCondBr(matches, loop, next);

    b.SetInsertPoint(loop);
    auto* offset = b.CreatePHI(b.getInt64Ty(), 3);
    offset->addIncoming(start, compare);
    b.CreateCondBr(b.CreateICmpULT(offset, size), dispatch, next);
    b.SetInsertPoint(dispatch);
    b.CreateCondBr(b.CreateICmpUGE(size, b.getInt64(kLanes)), vector, scalar);

    b.SetInsertPoint(vector);
    auto* last = b.CreateSub(size, b.getInt64(kLanes));
    auto* base = b.CreateSelect(b.CreateICmpULT(offset, last), offset, last);
    auto* bits =
        b.CreateBitCast(b.CreateICmpEQ(load(delimiter_vector->getType(), base),
                                       delimiter_vector),
                        b.getInt32Ty());
    auto* mask = b.CreateAnd(
        bits,
        b.CreateShl(b.getInt32(UINT32_MAX),
                    b.CreateTrunc(b.CreateSub(offset, base), b.getInt32Ty())));
    b.CreateCondBr(b.CreateICmpNE(mask, b.getInt32(0)), vector_hit,
                   vector_step);
    b.SetInsertPoint(vector_step);
    offset->addIncoming(b.CreateAdd(base, b.getInt64(kLanes)), vector_step);
    b.CreateBr(loop);
    b.SetInsertPoint(vector_hit);
    auto* lane = b.CreateCall(cttz, {mask, b.getTrue()});
    auto* position = b.CreateAdd(base, b.CreateZExt(lane, b.getInt64Ty()));
    b.CreateBr(found);

    b.SetInsertPoint(scalar);
    b.CreateCondBr(b.CreateICmpEQ(load(b.getInt8Ty(), offset), delimiter),
                   found, scalar_step);
    b.SetInsertPoint(scalar_step);
    offset->addIncoming(b.CreateAdd(offset, b.getInt64(1)), scalar_step);
    b.CreateBr(loop);

    b.SetInsertPoint(found);
    auto* end = b.CreatePHI(b.getInt64Ty(), 2);
    end->addIncoming(position, vector_hit);
    end->addIncoming(offset, scalar);
    b.CreateCondBr(b.CreateICmpUGT(end, start), result, next);
    b.SetInsertPoint(result);
    b.CreateRet(b.CreateOr(b.getInt64(uint64_t{prefix.size()} << 32),
                           b.CreateSub(end, start)));
  }
  b.SetInsertPoint(next);
  b.CreateRet(b.getInt64(0));
  auto jit = OptimizeAndCompileModule(std::move(module), std::move(context),
                                      "delimited-capture");
  if (jit == nullptr) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  const auto compiled = symbol->toPtr<CaptureFn>();
  cache->emplace(std::move(key),
                 Entry{.jit = std::move(jit), .capture = compiled});
  return JitDelimitedCapture(compiled);
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
std::string_view JitDelimitedCapture::Replace(
    std::string_view input) const noexcept {
  if (input.size() > UINT32_MAX) {
    return input;  // TLCS value offsets are 32-bit.
  }
  const uint64_t capture = capture_(input.data(), input.size());
  return capture == 0 ? input : input.substr(capture >> 32, uint32_t(capture));
}

#ifdef TINYLAMB_HAS_LLVM
namespace {
constexpr unsigned kGroupHashLanes = 8;

llvm::Constant* GroupHashConstant(llvm::Type* type, uint64_t n) {
  auto* c = llvm::ConstantInt::get(type->getScalarType(), n);
  return type->isVectorTy()
             ? llvm::ConstantVector::getSplat(
                   llvm::ElementCount::getFixed(kGroupHashLanes), c)
             : c;
}

llvm::Value* EmitGroupMix64(llvm::IRBuilder<>& b, llvm::Value* x) {
  auto* type = x->getType();
  const auto constant = [&](uint64_t n) { return GroupHashConstant(type, n); };
  x = b.CreateAdd(x, constant(0x9e3779b97f4a7c15ULL));
  x = b.CreateMul(b.CreateXor(x, b.CreateLShr(x, constant(30))),
                  constant(0xbf58476d1ce4e5b9ULL));
  x = b.CreateMul(b.CreateXor(x, b.CreateLShr(x, constant(27))),
                  constant(0x94d049bb133111ebULL));
  return b.CreateXor(x, b.CreateLShr(x, constant(31)));
}

template <typename Body>
void EmitGroupHashLoop(llvm::IRBuilder<>& b, llvm::Function* fn,
                       llvm::Value* size, llvm::Value* seed, Body emit) {
  constexpr unsigned lanes = kGroupHashLanes;
  auto* i64 = b.getInt64Ty();
  const auto block = [&](std::string_view name) {
    return llvm::BasicBlock::Create(b.getContext(), name, fn);
  };
  auto* entry = block("entry");
  auto* vector_loop = block("vector.loop");
  auto* vector_body = block("vector.body");
  auto* tail_loop = block("tail.loop");
  auto* tail_body = block("tail.body");
  auto* exit = block("exit");
  auto* vector = llvm::FixedVectorType::get(i64, lanes);
  b.SetInsertPoint(entry);
  auto* full = b.CreateAnd(size, b.getInt64(~uint64_t{lanes - 1}));
  auto* vector_seed = b.CreateVectorSplat(lanes, seed);
  b.CreateBr(vector_loop);
  b.SetInsertPoint(vector_loop);
  auto* i = b.CreatePHI(i64, 2);
  i->addIncoming(b.getInt64(0), entry);
  b.CreateCondBr(b.CreateICmpULT(i, full), vector_body, tail_loop);
  b.SetInsertPoint(vector_body);
  emit(vector, i, vector_seed);
  i->addIncoming(b.CreateAdd(i, b.getInt64(lanes)), vector_body);
  b.CreateBr(vector_loop);
  b.SetInsertPoint(tail_loop);
  auto* tail = b.CreatePHI(i64, 2);
  tail->addIncoming(full, vector_loop);
  b.CreateCondBr(b.CreateICmpULT(tail, size), tail_body, exit);
  b.SetInsertPoint(tail_body);
  emit(i64, tail, seed);
  tail->addIncoming(b.CreateAdd(tail, b.getInt64(1)), tail_body);
  b.CreateBr(tail_loop);
  b.SetInsertPoint(exit);
  b.CreateRetVoid();
}
}  // namespace
#endif

// Lean: SimdGroupHash (int_hash_lane, full_vector_fits).
std::optional<JitGroupHash> JitGroupHash::Compile() {
#ifndef TINYLAMB_HAS_LLVM
  return std::nullopt;
#else
  if (std::getenv("TINYLAMB_DISABLE_GROUP_HASH_JIT") != nullptr) {
    return std::nullopt;
  }
  const std::scoped_lock lock(jit_cache_mutex);
  static auto* const owner = new std::unique_ptr<llvm::orc::LLJIT>;
  static HashFn compiled = nullptr;
  if (compiled != nullptr) {
    return JitGroupHash(compiled);
  }
  InitializeLlvm();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>("tinylamb.group_hash", *context);
  llvm::IRBuilder<> b(*context);
  constexpr std::string_view kSymbol = "tinylamb_group_hash";
  auto* i64 = b.getInt64Ty();
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(b.getVoidTy(),
                              {b.getPtrTy(), b.getPtrTy(), i64, i64}, false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  fn->addParamAttr(0, llvm::Attribute::NoAlias);
  fn->addParamAttr(1, llvm::Attribute::NoAlias);
  auto* input = fn->getArg(0);
  auto* output = fn->getArg(1);
  auto* size = fn->getArg(2);
  auto* seed = fn->getArg(3);
  EmitGroupHashLoop(
      b, fn, size, seed,
      [&](llvm::Type* type, llvm::Value* index, llvm::Value* group_seed) {
        auto* key = b.CreateAlignedLoad(
            type, b.CreateInBoundsGEP(i64, input, index), llvm::Align(8));
        auto* hash =
            EmitGroupMix64(b, b.CreateXor(group_seed, EmitGroupMix64(b, key)));
        b.CreateAlignedStore(hash, b.CreateInBoundsGEP(i64, output, index),
                             llvm::Align(8));
      });
  auto jit = OptimizeAndCompileModule(std::move(module), std::move(context),
                                      "group-hash");
  if (jit == nullptr) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  compiled = symbol->toPtr<HashFn>();
  *owner = std::move(jit);
  return JitGroupHash(compiled);
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitGroupHash::Hash(const int64_t* keys, uint64_t* hashes, size_t size,
                        uint64_t seed) const noexcept {
  hash_(keys, hashes, size, seed);
}

// Lean: SimdGroupHash (decimal_pair_little_endian, civil_component,
// fixed_component_load_fits).
std::optional<JitDatePartHash> JitDatePartHash::Compile(unsigned offset) {
#ifndef TINYLAMB_HAS_LLVM
  (void)offset;
  return std::nullopt;
#else
  if (std::endian::native != std::endian::little ||
      (offset != 11 && offset != 14) ||
      std::getenv("TINYLAMB_DISABLE_DATE_PART_HASH_JIT") != nullptr) {
    return std::nullopt;
  }
  struct Entry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    HashFn hash{nullptr};
  };
  static auto* const cache = new std::array<Entry, 2>;
  const std::scoped_lock lock(jit_cache_mutex);
  auto& slot = (*cache)[offset == 11 ? 0 : 1];
  if (slot.hash != nullptr) {
    return JitDatePartHash(slot.hash);
  }
  InitializeLlvm();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.date_part_hash", *context);
  llvm::IRBuilder<> b(*context);
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_date_part_hash";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(b.getVoidTy(),
                              {ptr, ptr, ptr, ptr, ptr, i64, i64}, false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  for (unsigned i = 0; i < 5; ++i) {
    fn->addParamAttr(i, llvm::Attribute::NoAlias);
  }
  auto* keys = fn->getArg(0);
  auto* bytes = fn->getArg(1);
  auto* starts = fn->getArg(2);
  auto* parts = fn->getArg(3);
  auto* hashes = fn->getArg(4);
  auto* i32_vector = llvm::FixedVectorType::get(i32, kGroupHashLanes);
  EmitGroupHashLoop(
      b, fn, fn->getArg(5), fn->getArg(6),
      [&](llvm::Type* type, llvm::Value* index, llvm::Value* seed) {
        const bool vector = type->isVectorTy();
        llvm::Type* index_type =
            vector ? static_cast<llvm::Type*>(i32_vector) : i32;
        auto* start = b.CreateAlignedLoad(
            index_type, b.CreateInBoundsGEP(i32, starts, index),
            llvm::Align(4));
        auto* location = b.CreateAdd(b.CreateZExt(start, type),
                                     GroupHashConstant(type, offset));
        auto* address = b.CreateInBoundsGEP(b.getInt8Ty(), bytes, location);
        llvm::Value* packed =
            vector ? static_cast<llvm::Value*>(b.CreateMaskedGather(
                         index_type, address, llvm::Align(1)))
                   : b.CreateAlignedLoad(i32, address, llvm::Align(1));
        const auto digit = [&](unsigned shift) {
          return b.CreateSub(
              b.CreateAnd(
                  b.CreateLShr(packed, GroupHashConstant(index_type, shift)),
                  GroupHashConstant(index_type, 255)),
              GroupHashConstant(index_type, '0'));
        };
        auto* part = b.CreateZExt(
            b.CreateAdd(
                b.CreateMul(digit(0), GroupHashConstant(index_type, 10)),
                digit(8)),
            type);
        auto* key = b.CreateAlignedLoad(
            type, b.CreateInBoundsGEP(i64, keys, index), llvm::Align(8));
        auto* first =
            EmitGroupMix64(b, b.CreateXor(seed, EmitGroupMix64(b, key)));
        auto* second = EmitGroupMix64(
            b,
            b.CreateXor(first, b.CreateAdd(EmitGroupMix64(b, part),
                                           GroupHashConstant(
                                               type, 0x9e3779b97f4a7c15ULL))));
        b.CreateAlignedStore(part, b.CreateInBoundsGEP(i64, parts, index),
                             llvm::Align(8));
        b.CreateAlignedStore(second, b.CreateInBoundsGEP(i64, hashes, index),
                             llvm::Align(8));
      });
  auto jit = OptimizeAndCompileModule(std::move(module), std::move(context),
                                      "date-part-hash");
  if (jit == nullptr) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  slot.hash = symbol->toPtr<HashFn>();
  slot.jit = std::move(jit);
  return JitDatePartHash(slot.hash);
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitDatePartHash::Hash(const int64_t* keys, const char* bytes,
                           const uint32_t* starts, int64_t* date_parts,
                           uint64_t* hashes, size_t size,
                           uint64_t seed) const noexcept {
  hash_(keys, bytes, starts, date_parts, hashes, size, seed);
}

namespace {
constexpr std::array<uint64_t, 4> kFingerprintMultipliers{
    0x9e3779b97f4a7c15ULL, 0xe7037ed1a0b428dbULL, 0x8ebc6af09c88c6e3ULL,
    0x589965cc75374cc3ULL};
}  // namespace

uint64_t JitStringFingerprint::Scalar(std::string_view value,
                                      uint64_t seed) noexcept {
  const size_t length = value.size();
  const size_t extent = length >= 4 ? length - 4 : 0;
  const auto word = [&](size_t offset) {
    uint32_t result = 0;
    for (size_t i = 0; i < std::min<size_t>(4, length); ++i) {
      result |= static_cast<uint32_t>(static_cast<uint8_t>(value[offset + i]))
                << (8 * i);
    }
    return result;
  };
  uint64_t x = seed ^ (length * kFingerprintMultipliers[0]) ^
               (word(0) * kFingerprintMultipliers[1]) ^
               (word(extent / 2) * kFingerprintMultipliers[2]) ^
               (word(extent) * kFingerprintMultipliers[3]);
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

// Lean: SimdGroupHash (sample_window_fits, short_sample_mask,
// fingerprint_congr).
std::optional<JitStringFingerprint> JitStringFingerprint::Compile() {
#ifndef TINYLAMB_HAS_LLVM
  return std::nullopt;
#else
  if (std::endian::native != std::endian::little ||
      std::getenv("TINYLAMB_DISABLE_STRING_FINGERPRINT_JIT") != nullptr) {
    return std::nullopt;
  }
  const std::scoped_lock lock(jit_cache_mutex);
  static auto* const owner = new std::unique_ptr<llvm::orc::LLJIT>;
  static HashFn compiled = nullptr;
  if (compiled != nullptr) {
    return JitStringFingerprint(compiled);
  }
  InitializeLlvm();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.string_fingerprint", *context);
  llvm::IRBuilder<> b(*context);
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_string_fingerprint";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(b.getVoidTy(),
                              {ptr, i64, ptr, ptr, ptr, i64, i64}, false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  fn->addParamAttr(4, llvm::Attribute::NoAlias);
  auto* data = fn->getArg(0);
  auto* byte_count = fn->getArg(1);
  auto* starts = fn->getArg(2);
  auto* ends = fn->getArg(3);
  auto* output = fn->getArg(4);
  auto* i32_vector = llvm::FixedVectorType::get(i32, kGroupHashLanes);
  EmitGroupHashLoop(
      b, fn, fn->getArg(5), fn->getArg(6),
      [&](llvm::Type* type, llvm::Value* index, llvm::Value* seed) {
        const bool vector = type->isVectorTy();
        llvm::Type* words = vector ? static_cast<llvm::Type*>(i32_vector) : i32;
        const auto c = [&](uint64_t value) {
          return GroupHashConstant(type, value);
        };
        const auto c32 = [&](uint32_t value) {
          return GroupHashConstant(words, value);
        };
        const auto offset = [&](llvm::Value* base) {
          return b.CreateZExt(
              b.CreateAlignedLoad(words, b.CreateInBoundsGEP(i32, base, index),
                                  llvm::Align(4)),
              type);
        };
        auto* start = offset(starts);
        auto* end = offset(ends);
        auto* size = vector ? b.CreateVectorSplat(kGroupHashLanes, byte_count)
                            : byte_count;
        auto* valid = b.CreateAnd(b.CreateICmpULE(start, end),
                                  b.CreateICmpULE(end, size));
        start = b.CreateSelect(valid, start, c(0));
        end = b.CreateSelect(valid, end, c(0));
        auto* length = b.CreateSub(end, start);
        auto* long_value = b.CreateICmpUGE(length, c(4));
        auto* extent =
            b.CreateSelect(long_value, b.CreateSub(length, c(4)), c(0));
        auto* last_safe = b.CreateSub(size, c(4));
        auto* first_position =
            b.CreateSelect(b.CreateICmpULE(start, last_safe), start, last_safe);
        const auto load = [&](llvm::Value* position) -> llvm::Value* {
          auto* address = b.CreateInBoundsGEP(b.getInt8Ty(), data, position);
          if (vector) {
            return b.CreateMaskedGather(words, address, llvm::Align(1));
          }
          return b.CreateAlignedLoad(i32, address, llvm::Align(1));
        };
        auto* short_shift = b.CreateMul(
            b.CreateAnd(b.CreateSub(start, first_position), c(3)), c(8));
        auto* mask_shift =
            b.CreateMul(b.CreateAnd(b.CreateSub(c(4), length), c(3)), c(8));
        mask_shift = b.CreateSelect(long_value, c(0), mask_shift);
        auto* first = b.CreateAnd(
            b.CreateLShr(load(first_position),
                         b.CreateTrunc(short_shift, words)),
            b.CreateLShr(c32(UINT32_MAX), b.CreateTrunc(mask_shift, words)));
        first = b.CreateSelect(b.CreateICmpEQ(length, c(0)), c32(0), first);
        auto* middle = b.CreateSelect(
            long_value,
            load(b.CreateSelect(long_value,
                                b.CreateAdd(start, b.CreateLShr(extent, c(1))),
                                first_position)),
            first);
        auto* last = b.CreateSelect(
            long_value,
            load(b.CreateSelect(long_value, b.CreateSub(end, c(4)),
                                first_position)),
            first);
        auto* x = b.CreateXor(
            seed, b.CreateMul(length, c(kFingerprintMultipliers[0])));
        const std::array samples{first, middle, last};
        for (size_t j = 0; j < samples.size(); ++j) {
          x = b.CreateXor(x, b.CreateMul(b.CreateZExt(samples[j], type),
                                         c(kFingerprintMultipliers[j + 1])));
        }
        b.CreateAlignedStore(EmitGroupMix64(b, x),
                             b.CreateInBoundsGEP(i64, output, index),
                             llvm::Align(8));
      });
  auto jit = OptimizeAndCompileModule(std::move(module), std::move(context),
                                      "string-fingerprint");
  if (jit == nullptr) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  compiled = symbol->toPtr<HashFn>();
  *owner = std::move(jit);
  return JitStringFingerprint(compiled);
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitStringFingerprint::Hash(const char* bytes, size_t byte_count,
                                const uint32_t* starts, const uint32_t* ends,
                                uint64_t* hashes, size_t size,
                                uint64_t seed) const noexcept {
  if (byte_count >= 4) {
    hash_(bytes, byte_count, starts, ends, hashes, size, seed);
    return;
  }
  for (size_t i = 0; i < size; ++i) {
    const size_t begin = starts[i], end = ends[i];
    hashes[i] = Scalar(begin < end && end <= byte_count
                           ? std::string_view(bytes + begin, end - begin)
                           : std::string_view{},
                       seed);
  }
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitInt64Kernels::Filter(const int64_t* input, uint8_t* output,
                             size_t count, int64_t constant) const {
  if (impl_ == nullptr || impl_->filter == nullptr) {
    CHECK_MSG(false, "not a filter kernel");
  }
  impl_->filter(input, output, static_cast<uint64_t>(count), constant);
}
TINYLAMB_JIT_CALL_BOUNDARY
void JitInt64Kernels::Project(const int64_t* input, int64_t* output,
                              size_t count, int64_t multiplier,
                              int64_t addend) const {
  if (impl_ == nullptr || impl_->projection == nullptr) {
    CHECK_MSG(false, "not a projection kernel");
  }
  impl_->projection(input, output, static_cast<uint64_t>(count), multiplier,
                    addend);
}
TINYLAMB_JIT_CALL_BOUNDARY
int64_t JitInt64Kernels::Sum(const int64_t* input, size_t count) const {
  if (impl_ == nullptr || impl_->sum == nullptr) {
    CHECK_MSG(false, "not a sum kernel");
  }
  return impl_->sum(input, static_cast<uint64_t>(count));
}
TINYLAMB_JIT_CALL_BOUNDARY
int64_t JitInt64Kernels::MaskedSum(const int64_t* input, const uint8_t* nulls,
                                   size_t count) const {
  if (impl_ == nullptr || impl_->masked_sum == nullptr) {
    CHECK_MSG(false, "not a masked sum kernel");
  }
  return impl_->masked_sum(input, nulls, static_cast<uint64_t>(count));
}
TINYLAMB_JIT_CALL_BOUNDARY
int64_t JitInt64Kernels::FilterProjectSum(const int64_t* filter,
                                          const int64_t* input, size_t count,
                                          int64_t constant, int64_t multiplier,
                                          int64_t addend, int64_t initial,
                                          uint64_t* selected) const {
  CHECK_MSG(impl_ && impl_->filter_project_sum, "not a fused sum kernel");
  return impl_->filter_project_sum(filter, input, count, constant, multiplier,
                                   addend, initial, selected);
}
TINYLAMB_JIT_CALL_BOUNDARY
size_t JitInt64Kernels::FilterHashProbe(const int64_t* filter,
                                        const int64_t* keys, size_t count,
                                        int64_t constant, const size_t* heads,
                                        const int64_t* slot_keys,
                                        size_t slot_mask, uint64_t* cursor,
                                        size_t* row, size_t* accepted) const {
  CHECK_MSG(impl_ && impl_->filter_hash_probe, "not a fused hash probe kernel");
  return impl_->filter_hash_probe(filter, keys, count, constant, heads,
                                  slot_keys, slot_mask, cursor, row, accepted);
}
TINYLAMB_JIT_CALL_BOUNDARY
void JitInt64Kernels::ProjectChecked(const int64_t* input, int64_t* output,
                                     size_t count, int64_t multiplier,
                                     int64_t addend, bool* multiply_overflowed,
                                     bool* add_overflowed) const {
  if (impl_ == nullptr || impl_->projection_checked == nullptr) {
    CHECK_MSG(false, "not a checked projection kernel");
  }
  uint8_t mul_of = 0;
  uint8_t add_of = 0;
  impl_->projection_checked(input, output, static_cast<uint64_t>(count),
                            multiplier, addend, &mul_of, &add_of);
  if (multiply_overflowed != nullptr) {
    *multiply_overflowed = mul_of != 0;
  }
  if (add_overflowed != nullptr) {
    *add_overflowed = add_of != 0;
  }
}
TINYLAMB_JIT_CALL_BOUNDARY
int64_t JitInt64Kernels::SumChecked(const int64_t* input, size_t count,
                                    bool* overflowed) const {
  if (impl_ == nullptr || impl_->sum_checked == nullptr) {
    CHECK_MSG(false, "not a checked sum kernel");
  }
  uint8_t of = 0;
  const int64_t total =
      impl_->sum_checked(input, static_cast<uint64_t>(count), &of);
  if (overflowed != nullptr) {
    *overflowed = of != 0;
  }
  return total;
}

double JitInt64Kernels::CompileMilliseconds() const {
  return impl_ ? impl_->compile_ms : 0.0;
}

void JitInt64Kernels::WarmUpAll() {
#ifdef TINYLAMB_HAS_LLVM
  static std::once_flag warmed;
  std::call_once(warmed, [] {
    (void)CompileFilter(BinaryOperation::kEquals);
    (void)CompileFilter(BinaryOperation::kNotEquals);
    (void)CompileFilter(BinaryOperation::kLessThan);
    (void)CompileFilter(BinaryOperation::kLessThanEquals);
    (void)CompileFilter(BinaryOperation::kGreaterThan);
    (void)CompileFilter(BinaryOperation::kGreaterThanEquals);
    (void)CompileSum();
    (void)CompileMaskedSum();
    (void)CompileProjection();
    // These are the kernels production actually uses (projection/aggregation);
    // omitting them left the first large query paying the JIT cost.
    (void)CompileSumChecked();
    (void)CompileProjectionChecked();
  });
#endif
}

// ============================================================================
// Proof-backed General Branchless Bytecode JIT (`JitBranchlessProgram`)
// ============================================================================

namespace {

constexpr size_t kMaxJitReferencedColumns = 32;
constexpr size_t kZeroNullSliceRows = 4096;
alignas(64) constexpr std::array<uint8_t, kZeroNullSliceRows> kZeroNullSlice{};

struct ValidatedProgram {
  std::vector<BytecodeInstruction> instructions;
  std::vector<Value> constants;
  std::vector<uint16_t> referenced_columns;
  std::vector<uint16_t> inst_dense_col;
  std::vector<size_t> depths;
  bool has_fallible_ops{false};
  std::string cache_key;
};

// Static Bytecode DAG & Stack-Depth Verifier.
// Enforces `ValidStackDepths` and `stepInst_forward`, required by the
// `if_conversion_sound_and_complete` theorem in
// `lean/TinylambOpt/BranchlessJitCodegen.lean`.
std::optional<ValidatedProgram> ValidateForBranchlessJit(
    const BytecodeProgram& program, const Schema& schema) {
  if (program.ResultType() != ValueType::kInt64 ||
      program.Instructions().empty()) {
    return std::nullopt;
  }
  ValidatedProgram out;
  out.instructions = program.Instructions();
  out.constants = program.Constants();
  const size_t n = out.instructions.size();
  out.inst_dense_col.assign(n, 0);

  std::unordered_map<uint16_t, uint16_t> col_to_dense;
  for (size_t pc = 0; pc < n; ++pc) {
    const BytecodeInstruction& inst = out.instructions[pc];
    switch (inst.opcode) {
      case BytecodeOp::kLoadColumn: {
        if (inst.operand >= schema.ColumnCount()) {
          return std::nullopt;
        }
        const Column& col = schema.GetColumn(inst.operand);
        if (col.Type() != ValueType::kInt64 || col.IsUnsigned()) {
          return std::nullopt;
        }
        auto [it, inserted] = col_to_dense.try_emplace(
            inst.operand, static_cast<uint16_t>(out.referenced_columns.size()));
        if (inserted) {
          if (out.referenced_columns.size() >= kMaxJitReferencedColumns) {
            return std::nullopt;
          }
          out.referenced_columns.push_back(inst.operand);
        }
        out.inst_dense_col[pc] = it->second;
        break;
      }
      case BytecodeOp::kLoadConstant: {
        if (inst.operand >= out.constants.size()) {
          return std::nullopt;
        }
        const Value& c = out.constants[inst.operand];
        if (!c.IsNull() && (c.type != ValueType::kInt64 || c.IsUnsigned())) {
          return std::nullopt;
        }
        break;
      }
      case BytecodeOp::kBinaryInt64:
        switch (inst.binary) {
          case BinaryOperation::kAdd:
          case BinaryOperation::kSubtract:
          case BinaryOperation::kMultiply:
          case BinaryOperation::kModulo:
            out.has_fallible_ops = true;
            break;
          case BinaryOperation::kEquals:
          case BinaryOperation::kNotEquals:
          case BinaryOperation::kLessThan:
          case BinaryOperation::kLessThanEquals:
          case BinaryOperation::kGreaterThan:
          case BinaryOperation::kGreaterThanEquals:
          case BinaryOperation::kIsDistinctFrom:
          case BinaryOperation::kIsNotDistinctFrom:
          case BinaryOperation::kAnd:
          case BinaryOperation::kOr:
          case BinaryOperation::kXor:
            break;
          default:
            return std::nullopt;
        }
        break;
      case BytecodeOp::kUnaryInt64:
        switch (inst.unary) {
          case UnaryOperation::kMinus:
            out.has_fallible_ops = true;
            break;
          case UnaryOperation::kIsNull:
          case UnaryOperation::kIsNotNull:
          case UnaryOperation::kIsTrue:
          case UnaryOperation::kIsNotTrue:
          case UnaryOperation::kIsFalse:
          case UnaryOperation::kIsNotFalse:
          case UnaryOperation::kNot:
          case UnaryOperation::kBitwiseNot:
            break;
        }
        break;
      case BytecodeOp::kAbs:
        out.has_fallible_ops = true;
        break;
      case BytecodeOp::kPop:
      case BytecodeOp::kDup:
        break;
      case BytecodeOp::kJump:
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue:
      case BytecodeOp::kJumpIfNotNull: {
        if (inst.jump_target < 1) {
          return std::nullopt;
        }
        const size_t target = pc + static_cast<size_t>(inst.jump_target);
        if (target > n) {
          return std::nullopt;
        }
        break;
      }
      default:
        return std::nullopt;
    }
  }

  // Forward abstract stack-depth check (`ValidStackDepths` in Lean).
  constexpr size_t kUnreached = std::numeric_limits<size_t>::max();
  out.depths.assign(n + 1, kUnreached);
  out.depths[0] = 0;
  auto propagate = [&](size_t target, size_t depth) -> bool {
    if (target > n) {
      return false;
    }
    if (out.depths[target] == kUnreached) {
      out.depths[target] = depth;
      return true;
    }
    return out.depths[target] == depth;
  };

  for (size_t pc = 0; pc < n; ++pc) {
    const size_t d = out.depths[pc];
    if (d == kUnreached) {
      // Unreachable dead code slot after an unconditional jump: assign a
      // dummy depth of 0 if needed, though BytecodeCompiler never emits
      // unreachable roots; reject if it cannot be typed consistently.
      continue;
    }
    const BytecodeInstruction& inst = out.instructions[pc];
    switch (inst.opcode) {
      case BytecodeOp::kLoadColumn:
      case BytecodeOp::kLoadConstant:
        if (!propagate(pc + 1, d + 1)) {
          return std::nullopt;
        }
        break;
      case BytecodeOp::kBinaryInt64:
        if (d < 2 || !propagate(pc + 1, d - 1)) {
          return std::nullopt;
        }
        break;
      case BytecodeOp::kUnaryInt64:
      case BytecodeOp::kAbs:
        if (d < 1 || !propagate(pc + 1, d)) {
          return std::nullopt;
        }
        break;
      case BytecodeOp::kPop:
        if (d <= inst.operand || !propagate(pc + 1, d - 1)) {
          return std::nullopt;
        }
        break;
      case BytecodeOp::kDup:
        if (d <= inst.operand || !propagate(pc + 1, d + 1)) {
          return std::nullopt;
        }
        break;
      case BytecodeOp::kJump: {
        const size_t target = pc + static_cast<size_t>(inst.jump_target);
        if (!propagate(target, d)) {
          return std::nullopt;
        }
        break;
      }
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue:
      case BytecodeOp::kJumpIfNotNull: {
        if (d < 1) {
          return std::nullopt;
        }
        const size_t target = pc + static_cast<size_t>(inst.jump_target);
        if (!propagate(target, d) || !propagate(pc + 1, d)) {
          return std::nullopt;
        }
        break;
      }
      default:
        return std::nullopt;
    }
  }
  if (out.depths[n] != 1) {
    return std::nullopt;
  }
  for (size_t pc = 0; pc <= n; ++pc) {
    if (out.depths[pc] == kUnreached) {
      out.depths[pc] = 0;
    }
  }

  // Build deterministic cache key over dense-column-normalized bytecode +
  // literal constants so identical shapes across tables/queries share one
  // compiled LLJIT module.
  std::string key;
  key.reserve((n * 16) + 32);
  key.append("cols=");
  key.append(std::to_string(out.referenced_columns.size()));
  key.push_back(';');
  for (size_t pc = 0; pc < n; ++pc) {
    const BytecodeInstruction& inst = out.instructions[pc];
    key.append(std::to_string(static_cast<uint32_t>(inst.opcode)));
    key.push_back(':');
    if (inst.opcode == BytecodeOp::kLoadColumn) {
      key.append(std::to_string(out.inst_dense_col[pc]));
    } else if (inst.opcode == BytecodeOp::kLoadConstant) {
      const Value& c = out.constants[inst.operand];
      if (c.IsNull()) {
        key.append("N");
      } else {
        key.push_back('I');
        key.append(std::to_string(c.value.int_value));
      }
    } else {
      key.append(std::to_string(inst.operand));
      key.push_back(',');
      key.append(std::to_string(static_cast<uint32_t>(inst.binary)));
      key.push_back(',');
      key.append(std::to_string(static_cast<uint32_t>(inst.unary)));
      key.push_back(',');
      key.append(std::to_string(inst.jump_target));
    }
    key.push_back(';');
  }
  out.cache_key = std::move(key);
  return out;
}

// Interval abstract domain for ZoneMap-guided overflow-check elimination.
// Lean proofs: lean/TinylambOpt/BranchlessJitCodegen.lean
//   (`zonemap_interval_mul_4corners_sound`,
//    `zonemap_interval_add_sub_abs_neg_sound`)
struct AbstractSlot {
  bool may_be_nonnull{false};
  bool may_be_null{true};
  int64_t lo{0};
  int64_t hi{0};
};

struct AbstractEdge {
  size_t target{0};
  bool may_be_active{false};
  std::vector<AbstractSlot> stk;
};

AbstractSlot MergeAbstractSlot(const AbstractSlot& a, const AbstractSlot& b) {
  if (!a.may_be_nonnull && !b.may_be_nonnull) {
    return {.may_be_nonnull = false, .may_be_null = true, .lo = 0, .hi = 0};
  }
  if (!a.may_be_nonnull) {
    return {.may_be_nonnull = true,
            .may_be_null = a.may_be_null || b.may_be_null,
            .lo = b.lo,
            .hi = b.hi};
  }
  if (!b.may_be_nonnull) {
    return {.may_be_nonnull = true,
            .may_be_null = a.may_be_null || b.may_be_null,
            .lo = a.lo,
            .hi = a.hi};
  }
  return {.may_be_nonnull = true,
          .may_be_null = a.may_be_null || b.may_be_null,
          .lo = std::min(a.lo, b.lo),
          .hi = std::max(a.hi, b.hi)};
}

bool FitsI64(__int128 v) noexcept {
  return v >= static_cast<__int128>(std::numeric_limits<int64_t>::min()) &&
         v <= static_cast<__int128>(std::numeric_limits<int64_t>::max());
}

}  // namespace

struct JitBranchlessProgram::Impl {
  using EvalFn = void (*)(const int64_t* const* col_values,
                          const uint8_t* const* col_nulls, uint64_t count,
                          int64_t* out_values, uint8_t* out_nulls,
                          uint8_t* out_any_null, uint8_t* out_any_err);
  using FilterFn = void (*)(const int64_t* const* col_values,
                            const uint8_t* const* col_nulls, uint64_t count,
                            uint8_t* out_keep, uint8_t* out_any_err);

#ifdef TINYLAMB_HAS_LLVM
  std::unique_ptr<llvm::orc::LLJIT> jit;
#endif
  ValidatedProgram spec;
  EvalFn eval_nonnull_checked{nullptr};
  EvalFn eval_nullable_checked{nullptr};
  EvalFn eval_nonnull_unchecked{nullptr};
  EvalFn eval_nullable_unchecked{nullptr};
  FilterFn filter_nonnull_checked{nullptr};
  FilterFn filter_nullable_checked{nullptr};
  FilterFn filter_nonnull_unchecked{nullptr};
  FilterFn filter_nullable_unchecked{nullptr};
  double compile_ms{0.0};
};

#ifdef TINYLAMB_HAS_LLVM
namespace {

using BranchlessImplPtr = std::shared_ptr<JitBranchlessProgram::Impl>;
// Live programs retain their own shared ownership after cache eviction.
constexpr size_t kMaxBranchlessCacheEntries = 64;
// NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
std::unordered_map<std::string, BranchlessImplPtr>& branchless_program_cache =
    *new std::unordered_map<std::string, BranchlessImplPtr>;

struct SsaLane {
  llvm::Value* bits{nullptr};     // i64
  llvm::Value* is_null{nullptr};  // i1
};

struct SsaCutEdge {
  size_t target{0};
  llvm::Value* cond{nullptr};  // i1
  std::vector<SsaLane> stk;    // bottom-to-top (back() is stack top)
};

// Emits an `internal noinline` inner loop with direct `NoAlias` output pointer
// arguments (so LLVM BasicAA + LoopVectorize sees zero memory aliasing) plus
// an external C-callable wrapper accepting pointer arrays.
void EmitBranchlessKernelPair(llvm::Module* module,
                              const ValidatedProgram& spec, bool nullable_cols,
                              bool checked_arith, bool filter_only,
                              std::string_view outer_name) {
  llvm::LLVMContext& ctx = module->getContext();
  llvm::IRBuilder<> builder(ctx);
  llvm::Type* void_ty = builder.getVoidTy();
  llvm::Type* ptr_ty = llvm::PointerType::getUnqual(ctx);
  llvm::Type* i64 = builder.getInt64Ty();
  llvm::Type* i8 = builder.getInt8Ty();
  llvm::Type* i1 = builder.getInt1Ty();

  const size_t k = spec.referenced_columns.size();

  // Inner function parameter layout:
  //   [0 .. k-1]           : const int64_t* col_val_u
  //   [k .. 2k-1] (if nul) : const uint8_t* col_null_u
  //   count                : i64
  //   if filter_only:
  //     out_keep (NoAlias), out_any_err (NoAlias)
  //   else:
  //     out_values (NoAlias), out_nulls (NoAlias),
  //     out_any_null (NoAlias), out_any_err (NoAlias)
  std::vector<llvm::Type*> inner_params;
  inner_params.reserve((nullable_cols ? 2 * k : k) + 5);
  for (size_t u = 0; u < k; ++u) {
    inner_params.push_back(ptr_ty);
  }
  if (nullable_cols) {
    for (size_t u = 0; u < k; ++u) {
      inner_params.push_back(ptr_ty);
    }
  }
  inner_params.push_back(i64);  // count
  const size_t first_out_param = inner_params.size();
  if (filter_only) {
    inner_params.push_back(ptr_ty);  // out_keep
    inner_params.push_back(ptr_ty);  // out_any_err
  } else {
    inner_params.push_back(ptr_ty);  // out_values
    inner_params.push_back(ptr_ty);  // out_nulls
    inner_params.push_back(ptr_ty);  // out_any_null
    inner_params.push_back(ptr_ty);  // out_any_err
  }

  auto* inner_fn_ty = llvm::FunctionType::get(void_ty, inner_params, false);
  auto* inner_fn =
      llvm::Function::Create(inner_fn_ty, llvm::Function::InternalLinkage,
                             std::string(outer_name) + "_inner", *module);
  inner_fn->addFnAttr(llvm::Attribute::NoInline);
  for (size_t p = first_out_param; p < inner_params.size(); ++p) {
    inner_fn->addParamAttr(static_cast<unsigned>(p), llvm::Attribute::NoAlias);
  }

  auto* arg_it = inner_fn->arg_begin();
  std::vector<llvm::Value*> col_val_ptrs(k);
  std::vector<llvm::Value*> col_null_ptrs(k, nullptr);
  for (size_t u = 0; u < k; ++u) {
    col_val_ptrs[u] = arg_it++;
  }
  if (nullable_cols) {
    for (size_t u = 0; u < k; ++u) {
      col_null_ptrs[u] = arg_it++;
    }
  }
  llvm::Value* count = arg_it++;
  llvm::Value* out_keep = nullptr;
  llvm::Value* out_values = nullptr;
  llvm::Value* out_nulls = nullptr;
  llvm::Value* out_any_null = nullptr;
  llvm::Value* out_any_err = nullptr;
  if (filter_only) {
    out_keep = arg_it++;
    out_any_err = arg_it++;
  } else {
    out_values = arg_it++;
    out_nulls = arg_it++;
    out_any_null = arg_it++;
    out_any_err = arg_it++;
  }

  llvm::Function* sadd =
      checked_arith
          ? IntrinsicDecl(module, llvm::Intrinsic::sadd_with_overflow, {i64})
          : nullptr;
  llvm::Function* ssub =
      checked_arith
          ? IntrinsicDecl(module, llvm::Intrinsic::ssub_with_overflow, {i64})
          : nullptr;
  llvm::Function* smul =
      checked_arith
          ? IntrinsicDecl(module, llvm::Intrinsic::smul_with_overflow, {i64})
          : nullptr;

  auto* entry = llvm::BasicBlock::Create(ctx, "entry", inner_fn);
  auto* loop = llvm::BasicBlock::Create(ctx, "loop", inner_fn);
  auto* body = llvm::BasicBlock::Create(ctx, "body", inner_fn);
  auto* exit = llvm::BasicBlock::Create(ctx, "exit", inner_fn);

  builder.SetInsertPoint(entry);
  builder.CreateBr(loop);

  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i64, 2, "index");
  auto* any_err_phi = builder.CreatePHI(i1, 2, "any_err");
  llvm::PHINode* any_null_phi =
      filter_only ? nullptr : builder.CreatePHI(i1, 2, "any_null");
  index->addIncoming(builder.getInt64(0), entry);
  any_err_phi->addIncoming(builder.getFalse(), entry);
  if (any_null_phi != nullptr) {
    any_null_phi->addIncoming(builder.getFalse(), entry);
  }
  builder.CreateCondBr(builder.CreateICmpULT(index, count), body, exit);

  // ========================================================================
  // SINGLE STRAIGHT-LINE BASIC BLOCK (`body`): ZERO internal branches!
  // Directly implements `runPredLoop` + `mergeIncoming` + `stepInst` proven
  // in `lean/TinylambOpt/BranchlessJitCodegen.lean`.
  // ========================================================================
  builder.SetInsertPoint(body);

  // Pre-load each referenced column once per row inside `body`.
  std::vector<SsaLane> loaded_cols(k);
  for (size_t u = 0; u < k; ++u) {
    llvm::Value* v =
        builder.CreateLoad(i64, builder.CreateGEP(i64, col_val_ptrs[u], index));
    llvm::Value* is_n = builder.getFalse();
    if (nullable_cols) {
      llvm::Value* nb = builder.CreateLoad(
          i8, builder.CreateGEP(i8, col_null_ptrs[u], index));
      is_n = builder.CreateICmpNE(nb, builder.getInt8(0));
    }
    loaded_cols[u] = {.bits = v, .is_null = is_n};
  }

  const size_t num_insts = spec.instructions.size();
  auto select_stack = [&](llvm::Value* cond, const std::vector<SsaLane>& on_t,
                          const std::vector<SsaLane>& on_f) {
    assert(on_t.size() == on_f.size());
    std::vector<SsaLane> merged(on_t.size());
    for (size_t s = 0; s < on_t.size(); ++s) {
      merged[s] = {
          .bits = builder.CreateSelect(cond, on_t[s].bits, on_f[s].bits),
          .is_null =
              builder.CreateSelect(cond, on_t[s].is_null, on_f[s].is_null)};
    }
    return merged;
  };

  auto merge_incoming =
      [&](size_t pc, size_t default_depth, std::vector<SsaCutEdge>* edges,
          llvm::Value** out_active, std::vector<SsaLane>* out_stk) {
        llvm::Value* acc_act = builder.getFalse();
        std::vector<SsaLane> acc_stk(
            default_depth,
            SsaLane{.bits = builder.getInt64(0), .is_null = builder.getTrue()});
        bool first_hit = true;
        std::vector<SsaCutEdge> rem;
        rem.reserve(edges->size());
        // Iterate right-to-left to match `mergeIncoming` list fold order,
        // though incoming active conditions are pairwise disjoint
        // (`UniqueActiveEdge`).
        for (size_t idx = edges->size(); idx-- > 0;) {
          SsaCutEdge& e = (*edges)[idx];
          if (e.target == pc) {
            if (first_hit) {
              acc_act = e.cond;
              acc_stk = std::move(e.stk);
              first_hit = false;
            } else {
              acc_act = builder.CreateOr(e.cond, acc_act);
              acc_stk = select_stack(e.cond, e.stk, acc_stk);
            }
          } else {
            rem.push_back(std::move(e));
          }
        }
        std::reverse(rem.begin(), rem.end());
        *edges = std::move(rem);
        *out_active = acc_act;
        *out_stk = std::move(acc_stk);
      };

  std::vector<SsaCutEdge> cut_edges;
  cut_edges.reserve(num_insts);
  cut_edges.push_back(
      SsaCutEdge{.target = 0, .cond = builder.getTrue(), .stk = {}});
  llvm::Value* row_err = builder.getFalse();

  const llvm::Value* const i64_zero = builder.getInt64(0);
  const llvm::Value* const i64_one = builder.getInt64(1);
  const llvm::Value* const i64_neg_one = builder.getInt64(-1);
  const llvm::Value* const i64_min =
      builder.getInt64(std::numeric_limits<int64_t>::min());

  for (size_t pc = 0; pc < num_insts; ++pc) {
    llvm::Value* active = nullptr;
    std::vector<SsaLane> stk;
    merge_incoming(pc, spec.depths[pc], &cut_edges, &active, &stk);

    const BytecodeInstruction& inst = spec.instructions[pc];
    llvm::Value* local_err = builder.getFalse();

    switch (inst.opcode) {
      case BytecodeOp::kLoadColumn: {
        stk.push_back(loaded_cols[spec.inst_dense_col[pc]]);
        cut_edges.push_back(
            {.target = pc + 1, .cond = active, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kLoadConstant: {
        const Value& c = spec.constants[inst.operand];
        if (c.IsNull()) {
          stk.push_back(
              {.bits = builder.getInt64(0), .is_null = builder.getTrue()});
        } else {
          stk.push_back({.bits = builder.getInt64(c.value.int_value),
                         .is_null = builder.getFalse()});
        }
        cut_edges.push_back(
            {.target = pc + 1, .cond = active, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kPop: {
        stk.erase(stk.end() - 1 - static_cast<std::ptrdiff_t>(inst.operand));
        cut_edges.push_back(
            {.target = pc + 1, .cond = active, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kDup: {
        stk.push_back(stk[stk.size() - 1 - inst.operand]);
        cut_edges.push_back(
            {.target = pc + 1, .cond = active, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kJump: {
        const size_t target = pc + static_cast<size_t>(inst.jump_target);
        cut_edges.push_back(
            {.target = target, .cond = active, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue: {
        const SsaLane& top = stk.back();
        llvm::Value* truthy =
            builder.CreateICmpNE(top.bits, const_cast<llvm::Value*>(i64_zero));
        llvm::Value* not_null = builder.CreateNot(top.is_null);
        llvm::Value* decides =
            inst.opcode == BytecodeOp::kJumpIfFalse
                ? builder.CreateAnd(not_null, builder.CreateNot(truthy))
                : builder.CreateAnd(not_null, truthy);
        std::vector<SsaLane> taken_stk = stk;
        taken_stk.back() = {.bits = inst.opcode == BytecodeOp::kJumpIfTrue
                                        ? const_cast<llvm::Value*>(i64_one)
                                        : const_cast<llvm::Value*>(i64_zero),
                            .is_null = builder.getFalse()};
        const size_t target = pc + static_cast<size_t>(inst.jump_target);
        cut_edges.push_back({.target = target,
                             .cond = builder.CreateAnd(active, decides),
                             .stk = std::move(taken_stk)});
        cut_edges.push_back(
            {.target = pc + 1,
             .cond = builder.CreateAnd(active, builder.CreateNot(decides)),
             .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kJumpIfNotNull: {
        const SsaLane& top = stk.back();
        llvm::Value* decides = builder.CreateNot(top.is_null);
        std::vector<SsaLane> taken_stk = stk;
        const size_t target = pc + static_cast<size_t>(inst.jump_target);
        cut_edges.push_back({.target = target,
                             .cond = builder.CreateAnd(active, decides),
                             .stk = std::move(taken_stk)});
        cut_edges.push_back(
            {.target = pc + 1,
             .cond = builder.CreateAnd(active, builder.CreateNot(decides)),
             .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kAbs: {
        // Lean proof: `branchlessAbsBV_exact`
        SsaLane child = stk.back();
        llvm::Value* mask = builder.CreateAShr(child.bits, 63);
        llvm::Value* abs_val =
            builder.CreateXor(builder.CreateAdd(child.bits, mask), mask);
        if (checked_arith) {
          llvm::Value* is_min = builder.CreateICmpEQ(
              child.bits, const_cast<llvm::Value*>(i64_min));
          local_err =
              builder.CreateAnd(builder.CreateNot(child.is_null), is_min);
        }
        stk.back() = {.bits = abs_val, .is_null = child.is_null};
        row_err =
            builder.CreateOr(row_err, builder.CreateAnd(active, local_err));
        cut_edges.push_back(
            {.target = pc + 1, .cond = active, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kUnaryInt64: {
        SsaLane child = stk.back();
        SsaLane out_lane{};
        llvm::Value* truthy = builder.CreateICmpNE(
            child.bits, const_cast<llvm::Value*>(i64_zero));
        switch (inst.unary) {
          case UnaryOperation::kIsNull:
            out_lane = {.bits = builder.CreateZExt(child.is_null, i64),
                        .is_null = builder.getFalse()};
            break;
          case UnaryOperation::kIsNotNull:
            out_lane = {.bits = builder.CreateZExt(
                            builder.CreateNot(child.is_null), i64),
                        .is_null = builder.getFalse()};
            break;
          case UnaryOperation::kIsTrue:
            out_lane = {
                .bits = builder.CreateZExt(
                    builder.CreateAnd(builder.CreateNot(child.is_null), truthy),
                    i64),
                .is_null = builder.getFalse()};
            break;
          case UnaryOperation::kIsNotTrue:
            out_lane = {
                .bits = builder.CreateZExt(
                    builder.CreateOr(child.is_null, builder.CreateNot(truthy)),
                    i64),
                .is_null = builder.getFalse()};
            break;
          case UnaryOperation::kIsFalse:
            out_lane = {.bits = builder.CreateZExt(
                            builder.CreateAnd(builder.CreateNot(child.is_null),
                                              builder.CreateNot(truthy)),
                            i64),
                        .is_null = builder.getFalse()};
            break;
          case UnaryOperation::kIsNotFalse:
            out_lane = {.bits = builder.CreateZExt(
                            builder.CreateOr(child.is_null, truthy), i64),
                        .is_null = builder.getFalse()};
            break;
          case UnaryOperation::kNot:
            out_lane = {
                .bits = builder.CreateZExt(builder.CreateNot(truthy), i64),
                .is_null = child.is_null};
            break;
          case UnaryOperation::kBitwiseNot:
            out_lane = {.bits = builder.CreateNot(child.bits),
                        .is_null = child.is_null};
            break;
          case UnaryOperation::kMinus:
            if (checked_arith) {
              auto* pair = builder.CreateCall(
                  ssub, {const_cast<llvm::Value*>(i64_zero), child.bits});
              out_lane = {.bits = builder.CreateExtractValue(pair, 0),
                          .is_null = child.is_null};
              local_err =
                  builder.CreateAnd(builder.CreateNot(child.is_null),
                                    builder.CreateExtractValue(pair, 1));
            } else {
              out_lane = {.bits = builder.CreateNSWNeg(child.bits),
                          .is_null = child.is_null};
            }
            break;
        }
        stk.back() = out_lane;
        row_err =
            builder.CreateOr(row_err, builder.CreateAnd(active, local_err));
        cut_edges.push_back(
            {.target = pc + 1, .cond = active, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kBinaryInt64: {
        SsaLane right = stk.back();
        stk.pop_back();
        SsaLane left = stk.back();
        stk.pop_back();
        SsaLane res{};
        llvm::Value* either_null =
            builder.CreateOr(left.is_null, right.is_null);
        llvm::Value* neither_null = builder.CreateNot(either_null);

        switch (inst.binary) {
          // Lean proof: `jit_3vl_lane_sound`
          case BinaryOperation::kAnd: {
            llvm::Value* lt = builder.CreateAnd(
                builder.CreateNot(left.is_null),
                builder.CreateICmpNE(left.bits,
                                     const_cast<llvm::Value*>(i64_zero)));
            llvm::Value* rt = builder.CreateAnd(
                builder.CreateNot(right.is_null),
                builder.CreateICmpNE(right.bits,
                                     const_cast<llvm::Value*>(i64_zero)));
            llvm::Value* lf = builder.CreateAnd(
                builder.CreateNot(left.is_null),
                builder.CreateICmpEQ(left.bits,
                                     const_cast<llvm::Value*>(i64_zero)));
            llvm::Value* rf = builder.CreateAnd(
                builder.CreateNot(right.is_null),
                builder.CreateICmpEQ(right.bits,
                                     const_cast<llvm::Value*>(i64_zero)));
            llvm::Value* res_false = builder.CreateOr(lf, rf);
            res = {.bits = builder.CreateZExt(builder.CreateAnd(lt, rt), i64),
                   .is_null = builder.CreateAnd(builder.CreateNot(res_false),
                                                either_null)};
            break;
          }
          case BinaryOperation::kOr: {
            llvm::Value* lt = builder.CreateAnd(
                builder.CreateNot(left.is_null),
                builder.CreateICmpNE(left.bits,
                                     const_cast<llvm::Value*>(i64_zero)));
            llvm::Value* rt = builder.CreateAnd(
                builder.CreateNot(right.is_null),
                builder.CreateICmpNE(right.bits,
                                     const_cast<llvm::Value*>(i64_zero)));
            llvm::Value* res_true = builder.CreateOr(lt, rt);
            res = {.bits = builder.CreateZExt(res_true, i64),
                   .is_null = builder.CreateAnd(builder.CreateNot(res_true),
                                                either_null)};
            break;
          }
          case BinaryOperation::kXor: {
            llvm::Value* lt = builder.CreateICmpNE(
                left.bits, const_cast<llvm::Value*>(i64_zero));
            llvm::Value* rt = builder.CreateICmpNE(
                right.bits, const_cast<llvm::Value*>(i64_zero));
            res = {.bits = builder.CreateZExt(builder.CreateXor(lt, rt), i64),
                   .is_null = either_null};
            break;
          }
          case BinaryOperation::kIsDistinctFrom:
          case BinaryOperation::kIsNotDistinctFrom: {
            llvm::Value* both_null =
                builder.CreateAnd(left.is_null, right.is_null);
            llvm::Value* eq_vals = builder.CreateICmpEQ(left.bits, right.bits);
            llvm::Value* not_distinct = builder.CreateOr(
                both_null, builder.CreateAnd(neither_null, eq_vals));
            llvm::Value* flag =
                inst.binary == BinaryOperation::kIsNotDistinctFrom
                    ? not_distinct
                    : builder.CreateNot(not_distinct);
            res = {.bits = builder.CreateZExt(flag, i64),
                   .is_null = builder.getFalse()};
            break;
          }
          case BinaryOperation::kEquals:
          case BinaryOperation::kNotEquals:
          case BinaryOperation::kLessThan:
          case BinaryOperation::kLessThanEquals:
          case BinaryOperation::kGreaterThan:
          case BinaryOperation::kGreaterThanEquals: {
            llvm::Value* cmp =
                Comparison(builder, inst.binary, left.bits, right.bits);
            res = {.bits = builder.CreateZExt(cmp, i64),
                   .is_null = either_null};
            break;
          }
          case BinaryOperation::kAdd:
          case BinaryOperation::kSubtract:
          case BinaryOperation::kMultiply: {
            // Lean proof: `jitCheckedAdd_null_garbage_immune`,
            //             `zonemap_interval_mul_4corners_sound`,
            //             `zonemap_interval_add_sub_abs_neg_sound`
            if (checked_arith) {
              llvm::Function* fn = inst.binary == BinaryOperation::kAdd ? sadd
                                   : inst.binary == BinaryOperation::kSubtract
                                       ? ssub
                                       : smul;
              auto* pair = builder.CreateCall(fn, {left.bits, right.bits});
              res = {.bits = builder.CreateExtractValue(pair, 0),
                     .is_null = either_null};
              local_err = builder.CreateAnd(
                  neither_null, builder.CreateExtractValue(pair, 1));
            } else {
              llvm::Value* v =
                  inst.binary == BinaryOperation::kAdd
                      ? builder.CreateNSWAdd(left.bits, right.bits)
                  : inst.binary == BinaryOperation::kSubtract
                      ? builder.CreateNSWSub(left.bits, right.bits)
                      : builder.CreateNSWMul(left.bits, right.bits);
              res = {.bits = v, .is_null = either_null};
            }
            break;
          }
          case BinaryOperation::kModulo: {
            // Lean proof: `safeDivisorBV_never_traps_and_exact`
            llvm::Value* r_is_zero = builder.CreateICmpEQ(
                right.bits, const_cast<llvm::Value*>(i64_zero));
            llvm::Value* min_div_neg1 = builder.CreateAnd(
                builder.CreateICmpEQ(left.bits,
                                     const_cast<llvm::Value*>(i64_min)),
                builder.CreateICmpEQ(right.bits,
                                     const_cast<llvm::Value*>(i64_neg_one)));
            llvm::Value* bad_div = builder.CreateOr(r_is_zero, min_div_neg1);
            llvm::Value* must_sanitize = builder.CreateOr(either_null, bad_div);
            llvm::Value* safe_divisor = builder.CreateSelect(
                must_sanitize, const_cast<llvm::Value*>(i64_one), right.bits);
            llvm::Value* rem_val = builder.CreateSRem(left.bits, safe_divisor);
            if (checked_arith) {
              local_err = builder.CreateAnd(neither_null, bad_div);
            }
            res = {.bits = rem_val, .is_null = either_null};
            break;
          }
          default:
            assert(false);
            break;
        }

        stk.push_back(res);
        row_err =
            builder.CreateOr(row_err, builder.CreateAnd(active, local_err));
        cut_edges.push_back(
            {.target = pc + 1, .cond = active, .stk = std::move(stk)});
        break;
      }
      default:
        assert(false);
        break;
    }
  }

  llvm::Value* end_active = nullptr;
  std::vector<SsaLane> end_stk;
  merge_incoming(num_insts, 1, &cut_edges, &end_active, &end_stk);
  assert(end_stk.size() == 1);
  const SsaLane& final_lane = end_stk[0];

  if (filter_only) {
    llvm::Value* keep = builder.CreateAnd(
        builder.CreateNot(final_lane.is_null),
        builder.CreateICmpNE(final_lane.bits,
                             const_cast<llvm::Value*>(i64_zero)));
    builder.CreateStore(builder.CreateZExt(keep, i8),
                        builder.CreateGEP(i8, out_keep, index));
  } else {
    builder.CreateStore(final_lane.bits,
                        builder.CreateGEP(i64, out_values, index));
    builder.CreateStore(builder.CreateZExt(final_lane.is_null, i8),
                        builder.CreateGEP(i8, out_nulls, index));
  }

  llvm::Value* next_index = builder.CreateAdd(index, builder.getInt64(1));
  llvm::Value* next_any_err = builder.CreateOr(any_err_phi, row_err);
  llvm::Value* next_any_null =
      any_null_phi != nullptr
          ? builder.CreateOr(any_null_phi, final_lane.is_null)
          : nullptr;
  builder.CreateBr(loop);
  index->addIncoming(next_index, body);
  any_err_phi->addIncoming(next_any_err, body);
  if (any_null_phi != nullptr) {
    any_null_phi->addIncoming(next_any_null, body);
  }

  builder.SetInsertPoint(exit);
  builder.CreateStore(builder.CreateZExt(any_err_phi, i8), out_any_err);
  if (any_null_phi != nullptr) {
    builder.CreateStore(builder.CreateZExt(any_null_phi, i8), out_any_null);
  }
  builder.CreateRetVoid();

  // Emit thin C-ABI outer trampoline unpacking pointer arrays into direct args.
  std::vector<llvm::Type*> outer_params =
      filter_only
          ? std::vector<llvm::Type*>{ptr_ty, ptr_ty, i64, ptr_ty, ptr_ty}
          : std::vector<llvm::Type*>{ptr_ty, ptr_ty, i64,   ptr_ty,
                                     ptr_ty, ptr_ty, ptr_ty};
  auto* outer_fn_ty = llvm::FunctionType::get(void_ty, outer_params, false);
  auto* outer_fn = llvm::Function::Create(
      outer_fn_ty, llvm::Function::ExternalLinkage, outer_name, *module);
  auto* oarg = outer_fn->arg_begin();
  llvm::Value* arr_vals = oarg++;
  llvm::Value* arr_nulls = oarg++;
  llvm::Value* o_count = oarg++;

  auto* outer_entry = llvm::BasicBlock::Create(ctx, "entry", outer_fn);
  builder.SetInsertPoint(outer_entry);
  std::vector<llvm::Value*> call_args;
  call_args.reserve(inner_params.size());
  for (size_t u = 0; u < k; ++u) {
    call_args.push_back(builder.CreateLoad(
        ptr_ty, builder.CreateGEP(ptr_ty, arr_vals, builder.getInt64(u))));
  }
  if (nullable_cols) {
    for (size_t u = 0; u < k; ++u) {
      call_args.push_back(builder.CreateLoad(
          ptr_ty, builder.CreateGEP(ptr_ty, arr_nulls, builder.getInt64(u))));
    }
  }
  call_args.push_back(o_count);
  while (oarg != outer_fn->arg_end()) {
    call_args.push_back(oarg++);
  }
  builder.CreateCall(inner_fn, call_args);
  builder.CreateRetVoid();
}

}  // namespace
#endif

JitBranchlessProgram::JitBranchlessProgram(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
JitBranchlessProgram::JitBranchlessProgram(JitBranchlessProgram&&) noexcept =
    default;
JitBranchlessProgram& JitBranchlessProgram::operator=(
    JitBranchlessProgram&&) noexcept = default;
JitBranchlessProgram::~JitBranchlessProgram() = default;

std::optional<JitBranchlessProgram> JitBranchlessProgram::Compile(
    const BytecodeProgram& program, const Schema& schema) {
#ifndef TINYLAMB_HAS_LLVM
  (void)program;
  (void)schema;
  return std::nullopt;
#else
  std::optional<ValidatedProgram> validated =
      ValidateForBranchlessJit(program, schema);
  if (!validated) {
    return std::nullopt;
  }
  {
    std::scoped_lock lock(jit_cache_mutex);
    const auto it = branchless_program_cache.find(validated->cache_key);
    if (it != branchless_program_cache.end()) {
      return JitBranchlessProgram(it->second);
    }
  }

  InitializeLlvm();
  const auto begin = std::chrono::steady_clock::now();
  auto context = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb_branchless_bytecode", *context);

  EmitBranchlessKernelPair(module.get(), *validated, /*nullable_cols=*/false,
                           /*checked_arith=*/true, /*filter_only=*/false,
                           "tinylamb_bc_eval_nonnull_checked");
  EmitBranchlessKernelPair(module.get(), *validated, /*nullable_cols=*/true,
                           /*checked_arith=*/true, /*filter_only=*/false,
                           "tinylamb_bc_eval_nullable_checked");
  EmitBranchlessKernelPair(module.get(), *validated, /*nullable_cols=*/false,
                           /*checked_arith=*/true, /*filter_only=*/true,
                           "tinylamb_bc_filter_nonnull_checked");
  EmitBranchlessKernelPair(module.get(), *validated, /*nullable_cols=*/true,
                           /*checked_arith=*/true, /*filter_only=*/true,
                           "tinylamb_bc_filter_nullable_checked");

  if (validated->has_fallible_ops) {
    EmitBranchlessKernelPair(module.get(), *validated, /*nullable_cols=*/false,
                             /*checked_arith=*/false, /*filter_only=*/false,
                             "tinylamb_bc_eval_nonnull_unchecked");
    EmitBranchlessKernelPair(module.get(), *validated, /*nullable_cols=*/true,
                             /*checked_arith=*/false, /*filter_only=*/false,
                             "tinylamb_bc_eval_nullable_unchecked");
    EmitBranchlessKernelPair(module.get(), *validated, /*nullable_cols=*/false,
                             /*checked_arith=*/false, /*filter_only=*/true,
                             "tinylamb_bc_filter_nonnull_unchecked");
    EmitBranchlessKernelPair(module.get(), *validated, /*nullable_cols=*/true,
                             /*checked_arith=*/false, /*filter_only=*/true,
                             "tinylamb_bc_filter_nullable_unchecked");
  }

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(context),
                                      "tinylamb_branchless_bc");
  if (!jit) {
    return std::nullopt;
  }

  auto impl = std::make_shared<Impl>();
  impl->jit = std::move(jit);
  auto lookup_eval = [&](const char* name) -> Impl::EvalFn {
    auto sym = impl->jit->lookup(name);
    if (!sym) {
      llvm::consumeError(sym.takeError());
      return nullptr;
    }
    return sym->toPtr<Impl::EvalFn>();
  };
  auto lookup_filter = [&](const char* name) -> Impl::FilterFn {
    auto sym = impl->jit->lookup(name);
    if (!sym) {
      llvm::consumeError(sym.takeError());
      return nullptr;
    }
    return sym->toPtr<Impl::FilterFn>();
  };

  impl->eval_nonnull_checked = lookup_eval("tinylamb_bc_eval_nonnull_checked");
  impl->eval_nullable_checked =
      lookup_eval("tinylamb_bc_eval_nullable_checked");
  impl->filter_nonnull_checked =
      lookup_filter("tinylamb_bc_filter_nonnull_checked");
  impl->filter_nullable_checked =
      lookup_filter("tinylamb_bc_filter_nullable_checked");
  if (impl->eval_nonnull_checked == nullptr ||
      impl->eval_nullable_checked == nullptr ||
      impl->filter_nonnull_checked == nullptr ||
      impl->filter_nullable_checked == nullptr) {
    return std::nullopt;
  }

  if (validated->has_fallible_ops) {
    impl->eval_nonnull_unchecked =
        lookup_eval("tinylamb_bc_eval_nonnull_unchecked");
    impl->eval_nullable_unchecked =
        lookup_eval("tinylamb_bc_eval_nullable_unchecked");
    impl->filter_nonnull_unchecked =
        lookup_filter("tinylamb_bc_filter_nonnull_unchecked");
    impl->filter_nullable_unchecked =
        lookup_filter("tinylamb_bc_filter_nullable_unchecked");
    if (impl->eval_nonnull_unchecked == nullptr ||
        impl->eval_nullable_unchecked == nullptr ||
        impl->filter_nonnull_unchecked == nullptr ||
        impl->filter_nullable_unchecked == nullptr) {
      return std::nullopt;
    }
  } else {
    // No fallible ops: checked and unchecked IR are already identical!
    impl->eval_nonnull_unchecked = impl->eval_nonnull_checked;
    impl->eval_nullable_unchecked = impl->eval_nullable_checked;
    impl->filter_nonnull_unchecked = impl->filter_nonnull_checked;
    impl->filter_nullable_unchecked = impl->filter_nullable_checked;
  }

  impl->spec = std::move(*validated);
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin)
                         .count();
  BranchlessImplPtr evicted;
  BranchlessImplPtr selected;
  {
    std::scoped_lock lock(jit_cache_mutex);
    const auto existing = branchless_program_cache.find(impl->spec.cache_key);
    if (existing != branchless_program_cache.end()) {
      selected = existing->second;
    } else {
      if (branchless_program_cache.size() >= kMaxBranchlessCacheEntries) {
        auto victim = branchless_program_cache.begin();
        evicted = std::move(victim->second);
        branchless_program_cache.erase(victim);
      }
      branchless_program_cache.emplace(impl->spec.cache_key, impl);
      selected = impl;
    }
  }
  // LLJIT destruction can be expensive; do it outside the shared cache lock.
  return JitBranchlessProgram(std::move(selected));
#endif
}

const std::vector<uint16_t>& JitBranchlessProgram::ReferencedColumns()
    const noexcept {
  static const std::vector<uint16_t> kEmpty;
  return impl_ ? impl_->spec.referenced_columns : kEmpty;
}

double JitBranchlessProgram::CompileMilliseconds() const noexcept {
  return impl_ ? impl_->compile_ms : 0.0;
}

bool JitBranchlessProgram::HasUncheckedFastPath() const noexcept {
  return impl_ != nullptr && impl_->eval_nonnull_unchecked != nullptr;
}

// Lean proofs: lean/TinylambOpt/BranchlessJitCodegen.lean
//   (`zonemap_interval_mul_4corners_sound`,
//    `zonemap_interval_add_sub_abs_neg_sound`)
bool JitBranchlessProgram::ProveOverflowFree(
    std::span<const JitColumnInput> columns) const noexcept {
  if (!impl_) {
    return false;
  }
  const ValidatedProgram& spec = impl_->spec;
  if (columns.size() != spec.referenced_columns.size()) {
    return false;
  }
  if (!spec.has_fallible_ops) {
    return true;
  }

  const size_t n = spec.instructions.size();
  std::vector<AbstractEdge> edges;
  edges.push_back({.target = 0, .may_be_active = true, .stk = {}});

  for (size_t pc = 0; pc < n; ++pc) {
    bool active = false;
    std::vector<AbstractSlot> stk(spec.depths[pc]);
    bool first = true;
    std::vector<AbstractEdge> rem;
    rem.reserve(edges.size());
    for (auto& e : edges) {
      if (e.target == pc) {
        if (!e.may_be_active) {
          continue;
        }
        active = true;
        if (first) {
          stk = std::move(e.stk);
          first = false;
        } else {
          for (size_t s = 0; s < stk.size(); ++s) {
            stk[s] = MergeAbstractSlot(stk[s], e.stk[s]);
          }
        }
      } else {
        rem.push_back(std::move(e));
      }
    }
    edges = std::move(rem);
    if (!active) {
      continue;
    }

    const BytecodeInstruction& inst = spec.instructions[pc];
    switch (inst.opcode) {
      case BytecodeOp::kLoadColumn: {
        const JitColumnInput& col = columns[spec.inst_dense_col[pc]];
        AbstractSlot slot{};
        if (col.has_bounds && col.min_value <= col.max_value) {
          slot = {.may_be_nonnull = true,
                  .may_be_null = col.has_nulls,
                  .lo = col.min_value,
                  .hi = col.max_value};
        } else if (!col.has_bounds && col.has_nulls) {
          // Unbounded or all-null: must conservatively assume full INT64 range
          // unless caller provided bounds.
          slot = {.may_be_nonnull = true,
                  .may_be_null = true,
                  .lo = std::numeric_limits<int64_t>::min(),
                  .hi = std::numeric_limits<int64_t>::max()};
        } else {
          slot = {.may_be_nonnull = true,
                  .may_be_null = col.has_nulls,
                  .lo = std::numeric_limits<int64_t>::min(),
                  .hi = std::numeric_limits<int64_t>::max()};
        }
        stk.push_back(slot);
        edges.push_back(
            {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kLoadConstant: {
        const Value& c = spec.constants[inst.operand];
        if (c.IsNull()) {
          stk.push_back(
              {.may_be_nonnull = false, .may_be_null = true, .lo = 0, .hi = 0});
        } else {
          stk.push_back({.may_be_nonnull = true,
                         .may_be_null = false,
                         .lo = c.value.int_value,
                         .hi = c.value.int_value});
        }
        edges.push_back(
            {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kPop:
        stk.erase(stk.end() - 1 - static_cast<std::ptrdiff_t>(inst.operand));
        edges.push_back(
            {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        break;
      case BytecodeOp::kDup:
        stk.push_back(stk[stk.size() - 1 - inst.operand]);
        edges.push_back(
            {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        break;
      case BytecodeOp::kJump:
        edges.push_back({.target = pc + static_cast<size_t>(inst.jump_target),
                         .may_be_active = true,
                         .stk = std::move(stk)});
        break;
      case BytecodeOp::kJumpIfFalse:
      case BytecodeOp::kJumpIfTrue: {
        std::vector<AbstractSlot> taken = stk;
        const int64_t norm = inst.opcode == BytecodeOp::kJumpIfTrue ? 1 : 0;
        taken.back() = {.may_be_nonnull = true,
                        .may_be_null = false,
                        .lo = norm,
                        .hi = norm};
        edges.push_back({.target = pc + static_cast<size_t>(inst.jump_target),
                         .may_be_active = true,
                         .stk = std::move(taken)});
        edges.push_back(
            {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kJumpIfNotNull: {
        std::vector<AbstractSlot> taken = stk;
        taken.back().may_be_null = false;
        if (taken.back().may_be_nonnull) {
          edges.push_back({.target = pc + static_cast<size_t>(inst.jump_target),
                           .may_be_active = true,
                           .stk = std::move(taken)});
        }
        if (stk.back().may_be_null) {
          stk.back() = {
              .may_be_nonnull = false, .may_be_null = true, .lo = 0, .hi = 0};
          edges.push_back(
              {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        }
        break;
      }
      case BytecodeOp::kAbs: {
        const AbstractSlot child = stk.back();
        if (child.may_be_nonnull) {
          if (child.lo == std::numeric_limits<int64_t>::min()) {
            return false;
          }
          const int64_t a = child.lo < 0 ? -child.lo : child.lo;
          const int64_t b = child.hi < 0 ? -child.hi : child.hi;
          const int64_t new_hi = std::max(a, b);
          const int64_t new_lo =
              (child.lo <= 0 && 0 <= child.hi) ? 0 : std::min(a, b);
          stk.back() = {.may_be_nonnull = true,
                        .may_be_null = child.may_be_null,
                        .lo = new_lo,
                        .hi = new_hi};
        }
        edges.push_back(
            {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kUnaryInt64: {
        const AbstractSlot child = stk.back();
        if (inst.unary == UnaryOperation::kMinus) {
          if (child.may_be_nonnull) {
            if (child.lo == std::numeric_limits<int64_t>::min()) {
              return false;
            }
            stk.back() = {.may_be_nonnull = true,
                          .may_be_null = child.may_be_null,
                          .lo = -child.hi,
                          .hi = -child.lo};
          }
        } else if (inst.unary == UnaryOperation::kBitwiseNot) {
          if (child.may_be_nonnull) {
            stk.back() = {.may_be_nonnull = true,
                          .may_be_null = child.may_be_null,
                          .lo = ~child.hi,
                          .hi = ~child.lo};
          }
        } else if (inst.unary == UnaryOperation::kNot) {
          if (child.may_be_nonnull) {
            stk.back() = {.may_be_nonnull = true,
                          .may_be_null = child.may_be_null,
                          .lo = 0,
                          .hi = 1};
          }
        } else {
          // IS [NOT] NULL/TRUE/FALSE: always non-null boolean [0, 1].
          stk.back() = {
              .may_be_nonnull = true, .may_be_null = false, .lo = 0, .hi = 1};
        }
        edges.push_back(
            {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        break;
      }
      case BytecodeOp::kBinaryInt64: {
        const AbstractSlot right = stk.back();
        stk.pop_back();
        const AbstractSlot left = stk.back();
        stk.pop_back();
        const bool both_nonnull = left.may_be_nonnull && right.may_be_nonnull;
        const bool res_may_null = left.may_be_null || right.may_be_null;
        AbstractSlot res{};
        switch (inst.binary) {
          case BinaryOperation::kAdd:
            if (both_nonnull) {
              const __int128 lo = static_cast<__int128>(left.lo) + right.lo;
              const __int128 hi = static_cast<__int128>(left.hi) + right.hi;
              if (!FitsI64(lo) || !FitsI64(hi)) {
                return false;
              }
              res = {.may_be_nonnull = true,
                     .may_be_null = res_may_null,
                     .lo = static_cast<int64_t>(lo),
                     .hi = static_cast<int64_t>(hi)};
            } else {
              res = {.may_be_nonnull = false, .may_be_null = true};
            }
            break;
          case BinaryOperation::kSubtract:
            if (both_nonnull) {
              const __int128 lo = static_cast<__int128>(left.lo) - right.hi;
              const __int128 hi = static_cast<__int128>(left.hi) - right.lo;
              if (!FitsI64(lo) || !FitsI64(hi)) {
                return false;
              }
              res = {.may_be_nonnull = true,
                     .may_be_null = res_may_null,
                     .lo = static_cast<int64_t>(lo),
                     .hi = static_cast<int64_t>(hi)};
            } else {
              res = {.may_be_nonnull = false, .may_be_null = true};
            }
            break;
          case BinaryOperation::kMultiply:
            if (both_nonnull) {
              const __int128 c0 = static_cast<__int128>(left.lo) * right.lo;
              const __int128 c1 = static_cast<__int128>(left.lo) * right.hi;
              const __int128 c2 = static_cast<__int128>(left.hi) * right.lo;
              const __int128 c3 = static_cast<__int128>(left.hi) * right.hi;
              const __int128 mn = std::min({c0, c1, c2, c3});
              const __int128 mx = std::max({c0, c1, c2, c3});
              if (!FitsI64(mn) || !FitsI64(mx)) {
                return false;
              }
              res = {.may_be_nonnull = true,
                     .may_be_null = res_may_null,
                     .lo = static_cast<int64_t>(mn),
                     .hi = static_cast<int64_t>(mx)};
            } else {
              res = {.may_be_nonnull = false, .may_be_null = true};
            }
            break;
          case BinaryOperation::kModulo:
            if (both_nonnull) {
              // Divisor must strictly exclude 0, and exclude -1 if left can be
              // INT64_MIN.
              if (right.lo <= 0 && 0 <= right.hi) {
                return false;
              }
              if (left.lo == std::numeric_limits<int64_t>::min() &&
                  right.lo <= -1 && -1 <= right.hi) {
                return false;
              }
              const int64_t max_abs_div =
                  std::max(right.lo == std::numeric_limits<int64_t>::min()
                               ? std::numeric_limits<int64_t>::max()
                               : std::abs(right.lo),
                           right.hi == std::numeric_limits<int64_t>::min()
                               ? std::numeric_limits<int64_t>::max()
                               : std::abs(right.hi));
              const int64_t bound = std::max<int64_t>(0, max_abs_div - 1);
              res = {.may_be_nonnull = true,
                     .may_be_null = res_may_null,
                     .lo = left.lo < 0 ? -bound : 0,
                     .hi = left.hi > 0 ? bound : 0};
            } else {
              res = {.may_be_nonnull = false, .may_be_null = true};
            }
            break;
          case BinaryOperation::kIsDistinctFrom:
          case BinaryOperation::kIsNotDistinctFrom:
            res = {
                .may_be_nonnull = true, .may_be_null = false, .lo = 0, .hi = 1};
            break;
          default:
            res = {.may_be_nonnull = true,
                   .may_be_null = res_may_null,
                   .lo = 0,
                   .hi = 1};
            break;
        }
        stk.push_back(res);
        edges.push_back(
            {.target = pc + 1, .may_be_active = true, .stk = std::move(stk)});
        break;
      }
      default:
        return false;
    }
  }
  return true;
}

TINYLAMB_JIT_CALL_BOUNDARY
bool JitBranchlessProgram::Evaluate(std::span<const JitColumnInput> columns,
                                    size_t count, int64_t* out_values,
                                    uint8_t* out_nulls, bool* out_has_any_null,
                                    bool* out_used_unchecked) const {
  if (!impl_ || columns.size() != impl_->spec.referenced_columns.size()) {
    return false;
  }
  if (count == 0) {
    if (out_has_any_null != nullptr) {
      *out_has_any_null = false;
    }
    if (out_used_unchecked != nullptr) {
      *out_used_unchecked = true;
    }
    return true;
  }

  const size_t k = columns.size();
  bool any_input_nulls = false;
  for (const JitColumnInput& col : columns) {
    if (col.has_nulls) {
      any_input_nulls = true;
      break;
    }
  }
  const bool unchecked = ProveOverflowFree(columns);
  if (out_used_unchecked != nullptr) {
    *out_used_unchecked = unchecked;
  }

  std::array<const int64_t*, kMaxJitReferencedColumns> val_ptrs{};
  std::array<const uint8_t*, kMaxJitReferencedColumns> null_ptrs{};

  if (!any_input_nulls) {
    for (size_t u = 0; u < k; ++u) {
      val_ptrs[u] = columns[u].values;
    }
    uint8_t any_null = 0;
    uint8_t any_err = 0;
    const Impl::EvalFn fn =
        unchecked ? impl_->eval_nonnull_unchecked : impl_->eval_nonnull_checked;
    fn(val_ptrs.data(), nullptr, static_cast<uint64_t>(count), out_values,
       out_nulls, &any_null, &any_err);
    if (out_has_any_null != nullptr) {
      *out_has_any_null = any_null != 0;
    }
    return any_err == 0;
  }

  const Impl::EvalFn fn =
      unchecked ? impl_->eval_nullable_unchecked : impl_->eval_nullable_checked;
  bool accumulated_null = false;
  size_t offset = 0;
  while (offset < count) {
    const size_t slice = std::min(count - offset, kZeroNullSliceRows);
    for (size_t u = 0; u < k; ++u) {
      val_ptrs[u] = columns[u].values + offset;
      null_ptrs[u] = columns[u].has_nulls ? (columns[u].nulls + offset)
                                          : kZeroNullSlice.data();
    }
    uint8_t slice_null = 0;
    uint8_t slice_err = 0;
    fn(val_ptrs.data(), null_ptrs.data(), static_cast<uint64_t>(slice),
       out_values + offset, out_nulls + offset, &slice_null, &slice_err);
    if (slice_err != 0) {
      return false;
    }
    accumulated_null = accumulated_null || (slice_null != 0);
    offset += slice;
  }
  if (out_has_any_null != nullptr) {
    *out_has_any_null = accumulated_null;
  }
  return true;
}

TINYLAMB_JIT_CALL_BOUNDARY
bool JitBranchlessProgram::Filter(std::span<const JitColumnInput> columns,
                                  size_t count, uint8_t* out_keep,
                                  bool* out_used_unchecked) const {
  if (!impl_ || columns.size() != impl_->spec.referenced_columns.size()) {
    return false;
  }
  if (count == 0) {
    if (out_used_unchecked != nullptr) {
      *out_used_unchecked = true;
    }
    return true;
  }

  const size_t k = columns.size();
  bool any_input_nulls = false;
  for (const JitColumnInput& col : columns) {
    if (col.has_nulls) {
      any_input_nulls = true;
      break;
    }
  }
  const bool unchecked = ProveOverflowFree(columns);
  if (out_used_unchecked != nullptr) {
    *out_used_unchecked = unchecked;
  }

  std::array<const int64_t*, kMaxJitReferencedColumns> val_ptrs{};
  std::array<const uint8_t*, kMaxJitReferencedColumns> null_ptrs{};

  if (!any_input_nulls) {
    for (size_t u = 0; u < k; ++u) {
      val_ptrs[u] = columns[u].values;
    }
    uint8_t any_err = 0;
    const Impl::FilterFn fn = unchecked ? impl_->filter_nonnull_unchecked
                                        : impl_->filter_nonnull_checked;
    fn(val_ptrs.data(), nullptr, static_cast<uint64_t>(count), out_keep,
       &any_err);
    return any_err == 0;
  }

  const Impl::FilterFn fn = unchecked ? impl_->filter_nullable_unchecked
                                      : impl_->filter_nullable_checked;
  size_t offset = 0;
  while (offset < count) {
    const size_t slice = std::min(count - offset, kZeroNullSliceRows);
    for (size_t u = 0; u < k; ++u) {
      val_ptrs[u] = columns[u].values + offset;
      null_ptrs[u] = columns[u].has_nulls ? (columns[u].nulls + offset)
                                          : kZeroNullSlice.data();
    }
    uint8_t slice_err = 0;
    fn(val_ptrs.data(), null_ptrs.data(), static_cast<uint64_t>(slice),
       out_keep + offset, &slice_err);
    if (slice_err != 0) {
      return false;
    }
    offset += slice;
  }
  return true;
}

#ifdef TINYLAMB_HAS_LLVM
namespace {

llvm::Value* EmitSplitMix64Packed(llvm::IRBuilder<>& b, llvm::Value* k) {
  llvm::Value* h = b.CreateMul(b.CreateXor(k, b.CreateLShr(k, b.getInt64(30))),
                               b.getInt64(0xbf58476d1ce4e5b9ULL));
  h = b.CreateMul(b.CreateXor(h, b.CreateLShr(h, b.getInt64(27))),
                  b.getInt64(0x94d049bb133111ebULL));
  return b.CreateXor(h, b.CreateLShr(h, b.getInt64(31)));
}

void EmitPrefetchRead(llvm::IRBuilder<>& b, llvm::Function* pf_fn,
                      llvm::Value* ptr) {
  b.CreateCall(pf_fn, {ptr, b.getInt32(0), b.getInt32(3), b.getInt32(1)});
}

void EmitPrefetchWrite(llvm::IRBuilder<>& b, llvm::Function* pf_fn,
                       llvm::Value* ptr) {
  b.CreateCall(pf_fn, {ptr, b.getInt32(1), b.getInt32(3), b.getInt32(1)});
}

llvm::MDNode* UnlikelyBranchWeights(llvm::LLVMContext& ctx) {
  auto* i32 = llvm::Type::getInt32Ty(ctx);
  llvm::Metadata* md[] = {
      llvm::MDString::get(ctx, "branch_weights"),
      llvm::ConstantAsMetadata::get(llvm::ConstantInt::get(i32, 1)),
      llvm::ConstantAsMetadata::get(llvm::ConstantInt::get(i32, 100000)),
  };
  return llvm::MDNode::get(ctx, md);
}

llvm::Value* LoadAsDouble(llvm::IRBuilder<>& b, llvm::Value* ptr,
                          llvm::Value* idx, bool as_int64) {
  auto* i64 = b.getInt64Ty();
  auto* raw = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, ptr, idx));
  return as_int64 ? b.CreateSIToFP(raw, b.getDoubleTy())
                  : b.CreateBitCast(raw, b.getDoubleTy());
}

}  // namespace
#endif  // TINYLAMB_HAS_LLVM

// ----------------------------------------------------------------------------
// JitDenseSplitRadixAgg
// ----------------------------------------------------------------------------

std::optional<JitDenseSplitRadixAgg::ExtractKernel>
JitDenseSplitRadixAgg::CompileExtract(ExtractSpec spec) {
#ifndef TINYLAMB_HAS_LLVM
  (void)spec;
  return std::nullopt;
#else
  if (spec.part_shift == 0 || spec.part_shift > 21 ||
      static_cast<uint8_t>(spec.op) >
          static_cast<uint8_t>(ExprOp::kCoalesceMulIntZero)) {
    return std::nullopt;
  }
  uint8_t used_mask = 0x03U;
  if (spec.op == ExprOp::kColDouble || spec.op == ExprOp::kColInt64) {
    used_mask = 0x01U;
  } else if (spec.op == ExprOp::kSubSubAddDivConst) {
    used_mask = 0x0FU;
  }
  const uint8_t masked_val_nulls = spec.val_null_mask & used_mask;
  const uint32_t key =
      (static_cast<uint32_t>(spec.op) & 0x0FU) |
      (static_cast<uint32_t>(spec.use_sel ? 1U : 0U) << 4U) |
      (static_cast<uint32_t>(spec.jk_has_null ? 1U : 0U) << 5U) |
      (static_cast<uint32_t>(spec.pk0_has_null ? 1U : 0U) << 6U) |
      (static_cast<uint32_t>(masked_val_nulls) << 8U) |
      (static_cast<uint32_t>(spec.part_shift) << 12U);

  struct CacheEntry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    ExtractFn fn{nullptr};
  };
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto& cache = *new std::unordered_map<uint32_t, CacheEntry>;
  const std::scoped_lock lock(jit_cache_mutex);
  if (auto it = cache.find(key); it != cache.end()) {
    return ExtractKernel(it->second.fn);
  }

  InitializeLlvm();
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.dense_split_extract", *ctx);
  llvm::IRBuilder<> b(*ctx);
  auto* i8 = b.getInt8Ty();
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* f64 = b.getDoubleTy();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_dense_split_extract";
  std::array<llvm::Type*, 25> arg_tys{
      i32, ptr, ptr, ptr, i64, i64, ptr, ptr, ptr, i64, i64, ptr, i32,
      ptr, ptr, ptr, ptr, ptr, ptr, ptr, ptr, f64, ptr, ptr, ptr};
  auto* fn =
      llvm::Function::Create(llvm::FunctionType::get(i32, arg_tys, false),
                             llvm::Function::ExternalLinkage, kSymbol, *module);
  for (unsigned ai = 1; ai <= 20; ++ai) {
    if (arg_tys[ai] == ptr) {
      fn->addParamAttr(ai, llvm::Attribute::ReadOnly);
    }
  }
  fn->addParamAttr(22, llvm::Attribute::NoAlias);
  fn->addParamAttr(23, llvm::Attribute::NoAlias);
  fn->addParamAttr(24, llvm::Attribute::NoAlias);

  auto* count = fn->getArg(0);
  auto* sel = fn->getArg(1);
  auto* jk_vals = fn->getArg(2);
  auto* jk_nulls = fn->getArg(3);
  auto* jk_min = fn->getArg(4);
  auto* jk_span = fn->getArg(5);
  auto* key_to_rsubgid = fn->getArg(6);
  auto* pk0_vals = fn->getArg(7);
  auto* pk0_nulls = fn->getArg(8);
  auto* pk0_min = fn->getArg(9);
  auto* pk0_span = fn->getArg(10);
  auto* pk0_subgid = fn->getArg(11);
  auto* pk0_card = fn->getArg(12);
  std::array<llvm::Value*, 4> v_ptrs{fn->getArg(13), fn->getArg(15),
                                     fn->getArg(17), fn->getArg(19)};
  std::array<llvm::Value*, 4> n_ptrs{fn->getArg(14), fn->getArg(16),
                                     fn->getArg(18), fn->getArg(20)};
  auto* div_const = fn->getArg(21);
  auto* out_tagged = fn->getArg(22);
  auto* out_vals = fn->getArg(23);
  auto* part_hist = fn->getArg(24);

  const auto blk = [&](std::string_view name) {
    return llvm::BasicBlock::Create(*ctx, name, fn);
  };
  auto* entry = blk("entry");
  auto* loop_hdr = blk("loop.hdr");
  auto* loop_latch = blk("loop.latch");
  auto* exit_blk = blk("exit");

  b.SetInsertPoint(entry);
  b.CreateCondBr(b.CreateICmpEQ(count, b.getInt32(0)), exit_blk, loop_hdr);

  b.SetInsertPoint(loop_hdr);
  auto* idx_phi = b.CreatePHI(i32, 2, "idx");
  auto* m_phi = b.CreatePHI(i32, 2, "m");
  idx_phi->addIncoming(b.getInt32(0), entry);
  m_phi->addIncoming(b.getInt32(0), entry);

  llvm::Value* r64 = b.CreateZExt(idx_phi, i64);
  if (spec.use_sel) {
    auto* r32 = b.CreateLoad(i32, b.CreateInBoundsGEP(i32, sel, r64));
    r64 = b.CreateZExt(r32, i64);
  }

  std::vector<llvm::BasicBlock*> skip_blocks;
  if (spec.jk_has_null) {
    auto* next_b = blk("jk.lookup");
    auto* jkn = b.CreateLoad(i8, b.CreateInBoundsGEP(i8, jk_nulls, r64));
    auto* cur = b.GetInsertBlock();
    b.CreateCondBr(b.CreateICmpEQ(jkn, b.getInt8(0)), next_b, loop_latch);
    skip_blocks.push_back(cur);
    b.SetInsertPoint(next_b);
  }

  auto* jk = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, jk_vals, r64));
  auto* jk_off = b.CreateSub(jk, jk_min);
  {
    auto* next_b = blk("jk.table");
    auto* cur = b.GetInsertBlock();
    b.CreateCondBr(b.CreateICmpULT(jk_off, jk_span), next_b, loop_latch);
    skip_blocks.push_back(cur);
    b.SetInsertPoint(next_b);
  }
  auto* r_subgid =
      b.CreateLoad(i32, b.CreateInBoundsGEP(i32, key_to_rsubgid, jk_off));
  {
    auto* next_b = blk(spec.pk0_has_null ? "pk0.null" : "pk0.lookup");
    auto* cur = b.GetInsertBlock();
    b.CreateCondBr(b.CreateICmpNE(r_subgid, b.getInt32(UINT32_MAX)), next_b,
                   loop_latch);
    skip_blocks.push_back(cur);
    b.SetInsertPoint(next_b);
  }

  if (spec.pk0_has_null) {
    auto* next_b = blk("pk0.lookup");
    auto* pkn = b.CreateLoad(i8, b.CreateInBoundsGEP(i8, pk0_nulls, r64));
    auto* cur = b.GetInsertBlock();
    b.CreateCondBr(b.CreateICmpEQ(pkn, b.getInt8(0)), next_b, loop_latch);
    skip_blocks.push_back(cur);
    b.SetInsertPoint(next_b);
  }

  auto* pk = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, pk0_vals, r64));
  auto* pk_off = b.CreateSub(pk, pk0_min);
  {
    auto* next_b = blk("pk0.table");
    auto* cur = b.GetInsertBlock();
    b.CreateCondBr(b.CreateICmpULT(pk_off, pk0_span), next_b, loop_latch);
    skip_blocks.push_back(cur);
    b.SetInsertPoint(next_b);
  }
  auto* p_subgid =
      b.CreateLoad(i32, b.CreateInBoundsGEP(i32, pk0_subgid, pk_off));
  {
    auto* next_b = blk("emit");
    auto* cur = b.GetInsertBlock();
    b.CreateCondBr(b.CreateICmpNE(p_subgid, b.getInt32(UINT32_MAX)), next_b,
                   loop_latch);
    skip_blocks.push_back(cur);
    b.SetInsertPoint(next_b);
  }

  auto* gid = b.CreateAdd(b.CreateMul(r_subgid, pk0_card), p_subgid);
  llvm::Value* any_null = b.getFalse();
  for (uint8_t c = 0; c < 4; ++c) {
    if ((masked_val_nulls & (1U << c)) != 0U) {
      auto* nc = b.CreateLoad(i8, b.CreateInBoundsGEP(i8, n_ptrs[c], r64));
      any_null = b.CreateOr(any_null, b.CreateICmpNE(nc, b.getInt8(0)));
    }
  }

  llvm::Value* raw_val = llvm::ConstantFP::get(f64, 0.0);
  bool is_coalesce = false;
  switch (spec.op) {
    case ExprOp::kColDouble:
      raw_val = LoadAsDouble(b, v_ptrs[0], r64, false);
      break;
    case ExprOp::kColInt64:
      raw_val = LoadAsDouble(b, v_ptrs[0], r64, true);
      break;
    case ExprOp::kSubDouble:
      raw_val = b.CreateFSub(LoadAsDouble(b, v_ptrs[0], r64, false),
                             LoadAsDouble(b, v_ptrs[1], r64, false));
      break;
    case ExprOp::kAddDouble:
      raw_val = b.CreateFAdd(LoadAsDouble(b, v_ptrs[0], r64, false),
                             LoadAsDouble(b, v_ptrs[1], r64, false));
      break;
    case ExprOp::kSubDivConst:
      raw_val =
          b.CreateFDiv(b.CreateFSub(LoadAsDouble(b, v_ptrs[0], r64, false),
                                    LoadAsDouble(b, v_ptrs[1], r64, false)),
                       div_const);
      break;
    case ExprOp::kSubSubAddDivConst: {
      auto* d01 = b.CreateFSub(LoadAsDouble(b, v_ptrs[0], r64, false),
                               LoadAsDouble(b, v_ptrs[1], r64, false));
      auto* d2 = b.CreateFSub(d01, LoadAsDouble(b, v_ptrs[2], r64, false));
      auto* d3 = b.CreateFAdd(d2, LoadAsDouble(b, v_ptrs[3], r64, false));
      raw_val = b.CreateFDiv(d3, div_const);
      break;
    }
    case ExprOp::kMulDoubleInt:
      raw_val = b.CreateFMul(LoadAsDouble(b, v_ptrs[0], r64, false),
                             LoadAsDouble(b, v_ptrs[1], r64, true));
      break;
    case ExprOp::kCoalesceMulZero:
      is_coalesce = true;
      raw_val = b.CreateFMul(LoadAsDouble(b, v_ptrs[0], r64, false),
                             LoadAsDouble(b, v_ptrs[1], r64, true));
      break;
    case ExprOp::kMulDoubleDouble:
      raw_val = b.CreateFMul(LoadAsDouble(b, v_ptrs[0], r64, false),
                             LoadAsDouble(b, v_ptrs[1], r64, false));
      break;
    case ExprOp::kCoalesceMulDoubleZero:
      is_coalesce = true;
      raw_val = b.CreateFMul(LoadAsDouble(b, v_ptrs[0], r64, false),
                             LoadAsDouble(b, v_ptrs[1], r64, false));
      break;
    case ExprOp::kMulIntInt:
      raw_val = b.CreateFMul(LoadAsDouble(b, v_ptrs[0], r64, true),
                             LoadAsDouble(b, v_ptrs[1], r64, true));
      break;
    case ExprOp::kCoalesceMulIntZero:
      is_coalesce = true;
      raw_val = b.CreateFMul(LoadAsDouble(b, v_ptrs[0], r64, true),
                             LoadAsDouble(b, v_ptrs[1], r64, true));
      break;
  }

  auto* emit_val =
      b.CreateSelect(any_null, llvm::ConstantFP::get(f64, 0.0), raw_val);
  llvm::Value* tagged = gid;
  if (!is_coalesce && masked_val_nulls != 0U) {
    auto* sign_bit = b.CreateShl(b.CreateZExt(any_null, i32), b.getInt32(31));
    tagged = b.CreateOr(gid, sign_bit);
  }
  auto* m64 = b.CreateZExt(m_phi, i64);
  b.CreateStore(tagged, b.CreateInBoundsGEP(i32, out_tagged, m64));
  b.CreateStore(emit_val, b.CreateInBoundsGEP(f64, out_vals, m64));
  auto* p64 = b.CreateZExt(b.CreateLShr(gid, b.getInt32(spec.part_shift)), i64);
  auto* hist_p = b.CreateInBoundsGEP(i32, part_hist, p64);
  b.CreateStore(b.CreateAdd(b.CreateLoad(i32, hist_p), b.getInt32(1)), hist_p);
  auto* m_inc = b.CreateAdd(m_phi, b.getInt32(1));
  auto* emit_blk = b.GetInsertBlock();
  b.CreateBr(loop_latch);

  b.SetInsertPoint(loop_latch);
  auto* m_next =
      b.CreatePHI(i32, static_cast<unsigned>(skip_blocks.size() + 1));
  for (llvm::BasicBlock* sb : skip_blocks) {
    m_next->addIncoming(m_phi, sb);
  }
  m_next->addIncoming(m_inc, emit_blk);
  auto* idx_next = b.CreateAdd(idx_phi, b.getInt32(1));
  idx_phi->addIncoming(idx_next, loop_latch);
  m_phi->addIncoming(m_next, loop_latch);
  b.CreateCondBr(b.CreateICmpULT(idx_next, count), loop_hdr, exit_blk);

  b.SetInsertPoint(exit_blk);
  auto* ret_phi = b.CreatePHI(i32, 2);
  ret_phi->addIncoming(b.getInt32(0), entry);
  ret_phi->addIncoming(m_next, loop_latch);
  b.CreateRet(ret_phi);

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(ctx),
                                      "dense-split-extract");
  if (!jit) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  auto compiled = symbol->toPtr<ExtractFn>();
  cache.emplace(key, CacheEntry{.jit = std::move(jit), .fn = compiled});
  return ExtractKernel(compiled);
#endif
}

std::optional<JitDenseSplitRadixAgg::ScatterKernel>
JitDenseSplitRadixAgg::CompileScatter(uint8_t part_shift) {
#ifndef TINYLAMB_HAS_LLVM
  (void)part_shift;
  return std::nullopt;
#else
  if (part_shift == 0 || part_shift > 24) {
    return std::nullopt;
  }
  struct CacheEntry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    ScatterFn fn{nullptr};
  };
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto& cache = *new std::unordered_map<uint8_t, CacheEntry>;
  const std::scoped_lock lock(jit_cache_mutex);
  if (auto it = cache.find(part_shift); it != cache.end()) {
    return ScatterKernel(it->second.fn);
  }

  InitializeLlvm();
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.dense_split_scatter", *ctx);
  llvm::IRBuilder<> b(*ctx);
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* f64 = b.getDoubleTy();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_dense_split_scatter";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(b.getVoidTy(), {i32, ptr, ptr, ptr, ptr, ptr},
                              false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  fn->addParamAttr(1, llvm::Attribute::ReadOnly);
  fn->addParamAttr(2, llvm::Attribute::ReadOnly);
  fn->addParamAttr(3, llvm::Attribute::ReadOnly);
  fn->addParamAttr(4, llvm::Attribute::ReadOnly);
  fn->addParamAttr(5, llvm::Attribute::NoAlias);

  auto* count = fn->getArg(0);
  auto* in_tgs = fn->getArg(1);
  auto* in_vals = fn->getArg(2);
  auto* dst_gids = fn->getArg(3);
  auto* dst_vals = fn->getArg(4);
  auto* dst_pos = fn->getArg(5);

  auto* entry = llvm::BasicBlock::Create(*ctx, "entry", fn);
  auto* body = llvm::BasicBlock::Create(*ctx, "body", fn);
  auto* exit_blk = llvm::BasicBlock::Create(*ctx, "exit", fn);

  b.SetInsertPoint(entry);
  b.CreateCondBr(b.CreateICmpEQ(count, b.getInt32(0)), exit_blk, body);

  b.SetInsertPoint(body);
  auto* i_phi = b.CreatePHI(i32, 2);
  i_phi->addIncoming(b.getInt32(0), entry);
  auto* i64_v = b.CreateZExt(i_phi, i64);
  auto* tg = b.CreateLoad(i32, b.CreateInBoundsGEP(i32, in_tgs, i64_v));
  auto* val = b.CreateLoad(f64, b.CreateInBoundsGEP(f64, in_vals, i64_v));
  auto* gid = b.CreateAnd(tg, b.getInt32(0x7FFFFFFFU));
  auto* p64 = b.CreateZExt(b.CreateLShr(gid, b.getInt32(part_shift)), i64);
  const uint32_t low_mask = (uint32_t{1} << part_shift) - 1U;
  auto* local_tg = b.CreateOr(b.CreateAnd(gid, b.getInt32(low_mask)),
                              b.CreateAnd(tg, b.getInt32(0x80000000U)));
  auto* pos_ptr = b.CreateInBoundsGEP(i32, dst_pos, p64);
  auto* pos = b.CreateLoad(i32, pos_ptr);
  b.CreateStore(b.CreateAdd(pos, b.getInt32(1)), pos_ptr);
  auto* pos64 = b.CreateZExt(pos, i64);
  auto* gp = b.CreateLoad(ptr, b.CreateInBoundsGEP(ptr, dst_gids, p64));
  auto* vp = b.CreateLoad(ptr, b.CreateInBoundsGEP(ptr, dst_vals, p64));
  b.CreateStore(local_tg, b.CreateInBoundsGEP(i32, gp, pos64));
  b.CreateStore(val, b.CreateInBoundsGEP(f64, vp, pos64));
  auto* i_next = b.CreateAdd(i_phi, b.getInt32(1));
  i_phi->addIncoming(i_next, body);
  b.CreateCondBr(b.CreateICmpULT(i_next, count), body, exit_blk);

  b.SetInsertPoint(exit_blk);
  b.CreateRetVoid();

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(ctx),
                                      "dense-split-scatter");
  if (!jit) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  auto compiled = symbol->toPtr<ScatterFn>();
  cache.emplace(part_shift, CacheEntry{.jit = std::move(jit), .fn = compiled});
  return ScatterKernel(compiled);
#endif
}

std::optional<JitDenseSplitRadixAgg::AccumKernel>
JitDenseSplitRadixAgg::CompileAccum(bool is_avg, bool check_inf) {
#ifndef TINYLAMB_HAS_LLVM
  (void)is_avg;
  (void)check_inf;
  return std::nullopt;
#else
  const uint8_t key =
      static_cast<uint8_t>((is_avg ? 1U : 0U) | (check_inf ? 2U : 0U));
  struct CacheEntry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    AccumFn fn{nullptr};
  };
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto& cache = *new std::array<CacheEntry, 4>;
  const std::scoped_lock lock(jit_cache_mutex);
  if (cache[key].fn != nullptr) {
    return AccumKernel(cache[key].fn);
  }

  InitializeLlvm();
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.dense_split_accum", *ctx);
  llvm::IRBuilder<> b(*ctx);
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* f64 = b.getDoubleTy();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_dense_split_accum";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(b.getVoidTy(),
                              {i32, ptr, ptr, ptr, ptr, ptr, ptr, ptr}, false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  fn->addParamAttr(1, llvm::Attribute::ReadOnly);
  fn->addParamAttr(2, llvm::Attribute::ReadOnly);
  fn->addParamAttr(3, llvm::Attribute::NoAlias);
  fn->addParamAttr(4, llvm::Attribute::NoAlias);
  fn->addParamAttr(5, llvm::Attribute::NoAlias);
  fn->addParamAttr(6, llvm::Attribute::NoAlias);
  fn->addParamAttr(7, llvm::Attribute::NoAlias);

  auto* count = fn->getArg(0);
  auto* in_tgs = fn->getArg(1);
  auto* in_vals = fn->getArg(2);
  auto* sums = fn->getArg(3);
  auto* counts = fn->getArg(4);
  auto* matched_bits = fn->getArg(5);
  auto* nonnull_bits = fn->getArg(6);
  auto* inf_flag = fn->getArg(7);

  llvm::Function* pf_fn =
      IntrinsicDecl(module.get(), llvm::Intrinsic::prefetch, {ptr});
  llvm::Function* fabs_fn =
      check_inf ? IntrinsicDecl(module.get(), llvm::Intrinsic::fabs, {f64})
                : nullptr;
  llvm::Constant* pos_inf =
      check_inf ? llvm::ConstantFP::getInfinity(f64) : nullptr;
  llvm::MDNode* unlikely_md = check_inf ? UnlikelyBranchWeights(*ctx) : nullptr;

  auto* entry = llvm::BasicBlock::Create(*ctx, "entry", fn);
  auto* pf_cond = llvm::BasicBlock::Create(*ctx, "pf.cond", fn);
  auto* tail_cond = llvm::BasicBlock::Create(*ctx, "tail.cond", fn);
  auto* exit_blk = llvm::BasicBlock::Create(*ctx, "exit", fn);

  b.SetInsertPoint(entry);
  auto* has_pf = b.CreateICmpUGT(count, b.getInt32(8));
  auto* pf_end =
      b.CreateSelect(has_pf, b.CreateSub(count, b.getInt32(8)), b.getInt32(0));
  b.CreateBr(pf_cond);

  const auto build_stage =
      [&](llvm::BasicBlock* cond_blk, llvm::BasicBlock* prev_blk,
          llvm::BasicBlock* next_stage, llvm::Value* start_i,
          llvm::Value* end_i, bool do_pf,
          std::string_view tag) -> llvm::Value* {
    const auto mk = [&](std::string_view s) {
      return llvm::BasicBlock::Create(*ctx, std::string(tag) + std::string(s),
                                      fn);
    };
    auto* body = mk(".body");
    auto* nonnull_blk = mk(".nonnull");
    auto* latch = mk(".latch");

    b.SetInsertPoint(cond_blk);
    auto* i_phi = b.CreatePHI(i32, 2);
    i_phi->addIncoming(start_i, prev_blk);
    b.CreateCondBr(b.CreateICmpULT(i_phi, end_i), body, next_stage);

    b.SetInsertPoint(body);
    auto* i64_v = b.CreateZExt(i_phi, i64);
    if (do_pf) {
      auto* pf_idx = b.CreateAdd(i64_v, b.getInt64(8));
      auto* pf_tg = b.CreateLoad(i32, b.CreateInBoundsGEP(i32, in_tgs, pf_idx));
      auto* pf_lg =
          b.CreateZExt(b.CreateAnd(pf_tg, b.getInt32(0x7FFFFFFFU)), i64);
      EmitPrefetchWrite(b, pf_fn, b.CreateInBoundsGEP(f64, sums, pf_lg));
    }
    auto* tg = b.CreateLoad(i32, b.CreateInBoundsGEP(i32, in_tgs, i64_v));
    auto* lg32 = b.CreateAnd(tg, b.getInt32(0x7FFFFFFFU));
    auto* lg64 = b.CreateZExt(lg32, i64);
    auto* widx = b.CreateLShr(lg64, b.getInt64(6));
    auto* bit = b.CreateShl(b.getInt64(1), b.CreateAnd(lg64, b.getInt64(63)));
    auto* mb_ptr = b.CreateInBoundsGEP(i64, matched_bits, widx);
    b.CreateStore(b.CreateOr(b.CreateLoad(i64, mb_ptr), bit), mb_ptr);
    b.CreateCondBr(b.CreateICmpSGE(tg, b.getInt32(0)), nonnull_blk, latch);

    b.SetInsertPoint(nonnull_blk);
    auto* nb_ptr = b.CreateInBoundsGEP(i64, nonnull_bits, widx);
    b.CreateStore(b.CreateOr(b.CreateLoad(i64, nb_ptr), bit), nb_ptr);
    auto* s_ptr = b.CreateInBoundsGEP(f64, sums, lg64);
    auto* prev = b.CreateLoad(f64, s_ptr);
    auto* dv = b.CreateLoad(f64, b.CreateInBoundsGEP(f64, in_vals, i64_v));
    auto* next_s = b.CreateFAdd(prev, dv);
    if (check_inf) {
      auto* inf_next =
          b.CreateFCmpOEQ(b.CreateCall(fabs_fn, {next_s}), pos_inf);
      auto* fin_prev = b.CreateFCmpUNE(b.CreateCall(fabs_fn, {prev}), pos_inf);
      auto* fin_dv = b.CreateFCmpUNE(b.CreateCall(fabs_fn, {dv}), pos_inf);
      auto* ovf = b.CreateAnd(inf_next, b.CreateAnd(fin_prev, fin_dv));
      auto* set_inf = mk(".set_inf");
      auto* after_inf = mk(".after_inf");
      auto* br = b.CreateCondBr(ovf, set_inf, after_inf);
      br->setMetadata(llvm::LLVMContext::MD_prof, unlikely_md);
      b.SetInsertPoint(set_inf);
      b.CreateStore(b.getInt8(1), inf_flag);
      b.CreateBr(after_inf);
      b.SetInsertPoint(after_inf);
    }
    b.CreateStore(next_s, s_ptr);
    if (is_avg) {
      auto* c_ptr = b.CreateInBoundsGEP(i64, counts, lg64);
      b.CreateStore(b.CreateAdd(b.CreateLoad(i64, c_ptr), b.getInt64(1)),
                    c_ptr);
    }
    b.CreateBr(latch);

    b.SetInsertPoint(latch);
    auto* i_next = b.CreateAdd(i_phi, b.getInt32(1));
    i_phi->addIncoming(i_next, latch);
    b.CreateBr(cond_blk);
    return end_i;
  };

  llvm::Value* tail_start =
      build_stage(pf_cond, entry, tail_cond, b.getInt32(0), pf_end, true, "pf");
  (void)build_stage(tail_cond, pf_cond, exit_blk, tail_start, count, false,
                    "tl");

  b.SetInsertPoint(exit_blk);
  b.CreateRetVoid();

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(ctx),
                                      "dense-split-accum");
  if (!jit) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  auto compiled = symbol->toPtr<AccumFn>();
  cache[key] = CacheEntry{.jit = std::move(jit), .fn = compiled};
  return AccumKernel(compiled);
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
uint32_t JitDenseSplitRadixAgg::ExtractKernel::Run(
    uint32_t count, const uint32_t* sel, const int64_t* jk_vals,
    const uint8_t* jk_nulls, int64_t jk_min, uint64_t jk_span,
    const uint32_t* key_to_rsubgid, const int64_t* pk0_vals,
    const uint8_t* pk0_nulls, int64_t pk0_min, uint64_t pk0_span,
    const uint32_t* pk0_subgid, uint32_t pk0_card, const int64_t* v0,
    const uint8_t* n0, const int64_t* v1, const uint8_t* n1, const int64_t* v2,
    const uint8_t* n2, const int64_t* v3, const uint8_t* n3, double div_const,
    uint32_t* out_tagged_gids, double* out_vals,
    uint32_t* part_hist) const noexcept {
  return fn_(count, sel, jk_vals, jk_nulls, jk_min, jk_span, key_to_rsubgid,
             pk0_vals, pk0_nulls, pk0_min, pk0_span, pk0_subgid, pk0_card, v0,
             n0, v1, n1, v2, n2, v3, n3, div_const, out_tagged_gids, out_vals,
             part_hist);
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitDenseSplitRadixAgg::ScatterKernel::Run(
    uint32_t count, const uint32_t* in_tagged_gids, const double* in_vals,
    uint32_t* const* dst_gids, double* const* dst_vals,
    uint32_t* dst_pos) const noexcept {
  fn_(count, in_tagged_gids, in_vals, dst_gids, dst_vals, dst_pos);
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitDenseSplitRadixAgg::AccumKernel::Run(
    uint32_t count, const uint32_t* local_tagged_gids, const double* vals,
    double* sums, uint64_t* counts, uint64_t* matched_bits,
    uint64_t* nonnull_bits, uint8_t* inf_flag) const noexcept {
  fn_(count, local_tagged_gids, vals, sums, counts, matched_bits, nonnull_bits,
      inf_flag);
}

// ----------------------------------------------------------------------------
// JitPacked2x32HashProbe
// ----------------------------------------------------------------------------

std::optional<JitPacked2x32HashProbe> JitPacked2x32HashProbe::Compile(
    bool has_null0, bool has_null1, bool has_bloom, bool has_bias) {
#ifndef TINYLAMB_HAS_LLVM
  (void)has_null0;
  (void)has_null1;
  (void)has_bloom;
  (void)has_bias;
  return std::nullopt;
#else
  const uint8_t key =
      static_cast<uint8_t>((has_null0 ? 1U : 0U) | (has_null1 ? 2U : 0U) |
                           (has_bloom ? 4U : 0U) | (has_bias ? 8U : 0U));
  struct CacheEntry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    ProbeFn fn{nullptr};
  };
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto& cache = *new std::array<CacheEntry, 16>;
  const std::scoped_lock lock(jit_cache_mutex);
  if (cache[key].fn != nullptr) {
    return JitPacked2x32HashProbe(cache[key].fn);
  }

  InitializeLlvm();
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.packed2x32_hash_probe", *ctx);
  llvm::IRBuilder<> b(*ctx);
  auto* i8 = b.getInt8Ty();
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_packed2x32_hash_probe";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(
          b.getVoidTy(),
          {i32, ptr, ptr, ptr, ptr, i64, i64, ptr, i64, ptr, ptr, i64, ptr},
          false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  for (unsigned ai : {1U, 2U, 3U, 4U, 7U, 9U, 10U}) {
    fn->addParamAttr(ai, llvm::Attribute::ReadOnly);
  }
  fn->addParamAttr(12, llvm::Attribute::NoAlias);

  auto* count = fn->getArg(0);
  auto* k0_ptr = fn->getArg(1);
  auto* n0_ptr = fn->getArg(2);
  auto* k1_ptr = fn->getArg(3);
  auto* n1_ptr = fn->getArg(4);
  auto* bias0 = fn->getArg(5);
  auto* bias1 = fn->getArg(6);
  auto* bloom_words = fn->getArg(7);
  auto* bloom_mask = fn->getArg(8);
  auto* ht_keys = fn->getArg(9);
  auto* ht_heads = fn->getArg(10);
  auto* ht_mask = fn->getArg(11);
  auto* out_heads = fn->getArg(12);

  llvm::Function* pf_fn =
      IntrinsicDecl(module.get(), llvm::Intrinsic::prefetch, {ptr});

  auto* entry = llvm::BasicBlock::Create(*ctx, "entry", fn);
  auto* pf_cond = llvm::BasicBlock::Create(*ctx, "pf.cond", fn);
  auto* tl_cond = llvm::BasicBlock::Create(*ctx, "tl.cond", fn);
  auto* exit_blk = llvm::BasicBlock::Create(*ctx, "exit", fn);

  b.SetInsertPoint(entry);
  auto* pf_end =
      b.CreateSelect(b.CreateICmpUGT(count, b.getInt32(8)),
                     b.CreateSub(count, b.getInt32(8)), b.getInt32(0));
  b.CreateBr(pf_cond);

  const auto pack_and_hash = [&](llvm::Value* v0, llvm::Value* v1,
                                 llvm::Value** out_packed,
                                 llvm::Value** out_fits) {
    if (has_bias) {
      v0 = b.CreateAdd(v0, bias0);
      v1 = b.CreateAdd(v1, bias1);
    }
    auto* t0 = b.CreateTrunc(v0, i32);
    auto* t1 = b.CreateTrunc(v1, i32);
    *out_fits = b.CreateAnd(b.CreateICmpEQ(b.CreateSExt(t0, i64), v0),
                            b.CreateICmpEQ(b.CreateSExt(t1, i64), v1));
    *out_packed = b.CreateOr(b.CreateShl(b.CreateZExt(t0, i64), b.getInt64(32)),
                             b.CreateZExt(t1, i64));
    return EmitSplitMix64Packed(b, *out_packed);
  };

  const auto build_stage = [&](llvm::BasicBlock* cond_blk,
                               llvm::BasicBlock* prev_blk,
                               llvm::BasicBlock* next_stage,
                               llvm::Value* start_i, llvm::Value* end_i,
                               bool do_pf, std::string_view tag) {
    const auto mk = [&](std::string_view s) {
      return llvm::BasicBlock::Create(*ctx, std::string(tag) + std::string(s),
                                      fn);
    };
    auto* body = mk(".body");
    auto* check_range = mk(".range");
    auto* probe_hdr = mk(".probe.hdr");
    auto* probe_key = mk(".probe.key");
    auto* write_miss = mk(".miss");
    auto* latch = mk(".latch");

    b.SetInsertPoint(cond_blk);
    auto* i_phi = b.CreatePHI(i32, 2);
    i_phi->addIncoming(start_i, prev_blk);
    b.CreateCondBr(b.CreateICmpULT(i_phi, end_i), body, next_stage);

    b.SetInsertPoint(body);
    auto* i64_v = b.CreateZExt(i_phi, i64);
    if (do_pf) {
      auto* pi = b.CreateAdd(i64_v, b.getInt64(8));
      auto* pv0 = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, k0_ptr, pi));
      auto* pv1 = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, k1_ptr, pi));
      llvm::Value* pf_packed = nullptr;
      llvm::Value* pf_fits = nullptr;
      auto* ph = pack_and_hash(pv0, pv1, &pf_packed, &pf_fits);
      (void)pf_packed;
      (void)pf_fits;
      if (has_bloom) {
        auto* bw = b.CreateAnd(b.CreateLShr(ph, b.getInt64(6)), bloom_mask);
        EmitPrefetchRead(b, pf_fn, b.CreateInBoundsGEP(i64, bloom_words, bw));
      } else {
        auto* ps = b.CreateAnd(ph, ht_mask);
        EmitPrefetchRead(b, pf_fn, b.CreateInBoundsGEP(i32, ht_heads, ps));
        EmitPrefetchRead(b, pf_fn, b.CreateInBoundsGEP(i64, ht_keys, ps));
      }
    }

    if (has_null0 || has_null1) {
      llvm::Value* any_null = b.getFalse();
      if (has_null0) {
        auto* nv0 = b.CreateLoad(i8, b.CreateInBoundsGEP(i8, n0_ptr, i64_v));
        any_null = b.CreateOr(any_null, b.CreateICmpNE(nv0, b.getInt8(0)));
      }
      if (has_null1) {
        auto* nv1 = b.CreateLoad(i8, b.CreateInBoundsGEP(i8, n1_ptr, i64_v));
        any_null = b.CreateOr(any_null, b.CreateICmpNE(nv1, b.getInt8(0)));
      }
      b.CreateCondBr(any_null, write_miss, check_range);
    } else {
      b.CreateBr(check_range);
    }

    b.SetInsertPoint(check_range);
    auto* v0 = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, k0_ptr, i64_v));
    auto* v1 = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, k1_ptr, i64_v));
    llvm::Value* packed = nullptr;
    llvm::Value* fits = nullptr;
    auto* h = pack_and_hash(v0, v1, &packed, &fits);
    auto* init_slot = b.CreateAnd(h, ht_mask);
    if (has_bloom) {
      auto* bloom_chk = mk(".bloom");
      b.CreateCondBr(fits, bloom_chk, write_miss);
      b.SetInsertPoint(bloom_chk);
      auto* w0_idx = b.CreateAnd(b.CreateLShr(h, b.getInt64(6)), bloom_mask);
      auto* w1_idx = b.CreateAnd(b.CreateLShr(h, b.getInt64(30)), bloom_mask);
      auto* w0 =
          b.CreateLoad(i64, b.CreateInBoundsGEP(i64, bloom_words, w0_idx));
      auto* w1 =
          b.CreateLoad(i64, b.CreateInBoundsGEP(i64, bloom_words, w1_idx));
      auto* b0 = b.CreateAnd(b.CreateLShr(w0, b.CreateAnd(h, b.getInt64(63))),
                             b.getInt64(1));
      auto* b1 = b.CreateAnd(
          b.CreateLShr(
              w1, b.CreateAnd(b.CreateLShr(h, b.getInt64(24)), b.getInt64(63))),
          b.getInt64(1));
      auto* both = b.CreateICmpNE(b.CreateAnd(b0, b1), b.getInt64(0));
      b.CreateCondBr(both, probe_hdr, write_miss);
    } else {
      b.CreateCondBr(fits, probe_hdr, write_miss);
    }

    llvm::BasicBlock* probe_pred = b.GetInsertBlock();
    b.SetInsertPoint(probe_hdr);
    auto* slot_phi = b.CreatePHI(i64, 2);
    slot_phi->addIncoming(init_slot, probe_pred);
    auto* head =
        b.CreateLoad(i32, b.CreateInBoundsGEP(i32, ht_heads, slot_phi));
    b.CreateCondBr(b.CreateICmpEQ(head, b.getInt32(UINT32_MAX)), write_miss,
                   probe_key);

    b.SetInsertPoint(probe_key);
    auto* hk = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, ht_keys, slot_phi));
    auto* hit_blk = mk(".hit");
    auto* next_slot_blk = mk(".next_slot");
    b.CreateCondBr(b.CreateICmpEQ(hk, packed), hit_blk, next_slot_blk);

    b.SetInsertPoint(hit_blk);
    b.CreateStore(head, b.CreateInBoundsGEP(i32, out_heads, i64_v));
    b.CreateBr(latch);

    b.SetInsertPoint(next_slot_blk);
    auto* next_slot =
        b.CreateAnd(b.CreateAdd(slot_phi, b.getInt64(1)), ht_mask);
    slot_phi->addIncoming(next_slot, next_slot_blk);
    b.CreateBr(probe_hdr);

    b.SetInsertPoint(write_miss);
    b.CreateStore(b.getInt32(UINT32_MAX),
                  b.CreateInBoundsGEP(i32, out_heads, i64_v));
    b.CreateBr(latch);

    b.SetInsertPoint(latch);
    auto* i_next = b.CreateAdd(i_phi, b.getInt32(1));
    i_phi->addIncoming(i_next, latch);
    b.CreateBr(cond_blk);
  };

  build_stage(pf_cond, entry, tl_cond, b.getInt32(0), pf_end, true, "pf");
  build_stage(tl_cond, pf_cond, exit_blk, pf_end, count, false, "tl");

  b.SetInsertPoint(exit_blk);
  b.CreateRetVoid();

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(ctx),
                                      "packed2x32-hash-probe");
  if (!jit) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  auto compiled = symbol->toPtr<ProbeFn>();
  cache[key] = CacheEntry{.jit = std::move(jit), .fn = compiled};
  return JitPacked2x32HashProbe(compiled);
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitPacked2x32HashProbe::Probe(uint32_t count, const int64_t* k0,
                                   const uint8_t* n0, const int64_t* k1,
                                   const uint8_t* n1, int64_t bias0,
                                   int64_t bias1, const uint64_t* bloom_words,
                                   uint64_t bloom_word_mask,
                                   const uint64_t* ht_keys,
                                   const uint32_t* ht_heads, uint64_t ht_mask,
                                   uint32_t* out_heads) const noexcept {
  fn_(count, k0, n0, k1, n1, bias0, bias1, bloom_words, bloom_word_mask,
      ht_keys, ht_heads, ht_mask, out_heads);
}

// ----------------------------------------------------------------------------
// JitPacked2x32RadixDedup
// ----------------------------------------------------------------------------

std::optional<JitPacked2x32RadixDedup::PackKernel>
JitPacked2x32RadixDedup::CompilePack(bool has_null0, bool has_null1,
                                     bool use_sel, uint8_t part_shift) {
#ifndef TINYLAMB_HAS_LLVM
  (void)has_null0;
  (void)has_null1;
  (void)use_sel;
  (void)part_shift;
  return std::nullopt;
#else
  if (part_shift < 40 || part_shift >= 64) {
    return std::nullopt;
  }
  const uint32_t key = (has_null0 ? 1U : 0U) | (has_null1 ? 2U : 0U) |
                       (use_sel ? 4U : 0U) |
                       (static_cast<uint32_t>(part_shift) << 8U);
  struct CacheEntry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    PackFn fn{nullptr};
  };
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto& cache = *new std::unordered_map<uint32_t, CacheEntry>;
  const std::scoped_lock lock(jit_cache_mutex);
  if (auto it = cache.find(key); it != cache.end()) {
    return PackKernel(it->second.fn);
  }

  InitializeLlvm();
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.packed2x32_dedup_pack", *ctx);
  llvm::IRBuilder<> b(*ctx);
  auto* i8 = b.getInt8Ty();
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_packed2x32_dedup_pack";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(
          b.getVoidTy(),
          {i32, ptr, ptr, ptr, ptr, ptr, ptr, ptr, ptr, ptr, ptr}, false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  for (unsigned ai : {1U, 2U, 3U, 4U, 5U}) {
    fn->addParamAttr(ai, llvm::Attribute::ReadOnly);
  }
  for (unsigned ai : {6U, 7U, 8U, 9U, 10U}) {
    fn->addParamAttr(ai, llvm::Attribute::NoAlias);
  }

  auto* count = fn->getArg(0);
  auto* sel = fn->getArg(1);
  auto* k0_ptr = fn->getArg(2);
  auto* n0_ptr = fn->getArg(3);
  auto* k1_ptr = fn->getArg(4);
  auto* n1_ptr = fn->getArg(5);
  auto* out_packed = fn->getArg(6);
  auto* out_hist = fn->getArg(7);
  auto* out_fb = fn->getArg(8);
  auto* out_packed_cnt = fn->getArg(9);
  auto* out_fb_cnt = fn->getArg(10);

  auto* entry = llvm::BasicBlock::Create(*ctx, "entry", fn);
  auto* loop_hdr = llvm::BasicBlock::Create(*ctx, "loop.hdr", fn);
  auto* range_chk = llvm::BasicBlock::Create(*ctx, "range.chk", fn);
  auto* emit_pack = llvm::BasicBlock::Create(*ctx, "emit.pack", fn);
  auto* emit_fb = llvm::BasicBlock::Create(*ctx, "emit.fb", fn);
  auto* loop_latch = llvm::BasicBlock::Create(*ctx, "loop.latch", fn);
  auto* exit_blk = llvm::BasicBlock::Create(*ctx, "exit", fn);

  b.SetInsertPoint(entry);
  b.CreateCondBr(b.CreateICmpEQ(count, b.getInt32(0)), exit_blk, loop_hdr);

  b.SetInsertPoint(loop_hdr);
  auto* idx_phi = b.CreatePHI(i32, 2);
  auto* m_phi = b.CreatePHI(i32, 2);
  auto* fb_phi = b.CreatePHI(i32, 2);
  idx_phi->addIncoming(b.getInt32(0), entry);
  m_phi->addIncoming(b.getInt32(0), entry);
  fb_phi->addIncoming(b.getInt32(0), entry);

  llvm::Value* r32 = idx_phi;
  if (use_sel) {
    r32 = b.CreateLoad(
        i32, b.CreateInBoundsGEP(i32, sel, b.CreateZExt(idx_phi, i64)));
  }
  auto* r64 = b.CreateZExt(r32, i64);
  if (has_null0 || has_null1) {
    llvm::Value* any_null = b.getFalse();
    if (has_null0) {
      auto* nv0 = b.CreateLoad(i8, b.CreateInBoundsGEP(i8, n0_ptr, r64));
      any_null = b.CreateOr(any_null, b.CreateICmpNE(nv0, b.getInt8(0)));
    }
    if (has_null1) {
      auto* nv1 = b.CreateLoad(i8, b.CreateInBoundsGEP(i8, n1_ptr, r64));
      any_null = b.CreateOr(any_null, b.CreateICmpNE(nv1, b.getInt8(0)));
    }
    b.CreateCondBr(any_null, emit_fb, range_chk);
  } else {
    b.CreateBr(range_chk);
  }

  b.SetInsertPoint(range_chk);
  auto* v0 = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, k0_ptr, r64));
  auto* v1 = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, k1_ptr, r64));
  auto* t0 = b.CreateTrunc(v0, i32);
  auto* t1 = b.CreateTrunc(v1, i32);
  auto* fits = b.CreateAnd(b.CreateICmpEQ(b.CreateSExt(t0, i64), v0),
                           b.CreateICmpEQ(b.CreateSExt(t1, i64), v1));
  b.CreateCondBr(fits, emit_pack, emit_fb);

  b.SetInsertPoint(emit_pack);
  auto* packed = b.CreateOr(b.CreateShl(b.CreateZExt(t0, i64), b.getInt64(32)),
                            b.CreateZExt(t1, i64));
  auto* h = EmitSplitMix64Packed(b, packed);
  auto* part = b.CreateLShr(h, b.getInt64(part_shift));
  b.CreateStore(packed,
                b.CreateInBoundsGEP(i64, out_packed, b.CreateZExt(m_phi, i64)));
  auto* hist_ptr = b.CreateInBoundsGEP(i32, out_hist, part);
  b.CreateStore(b.CreateAdd(b.CreateLoad(i32, hist_ptr), b.getInt32(1)),
                hist_ptr);
  auto* m_inc = b.CreateAdd(m_phi, b.getInt32(1));
  b.CreateBr(loop_latch);

  b.SetInsertPoint(emit_fb);
  b.CreateStore(r32,
                b.CreateInBoundsGEP(i32, out_fb, b.CreateZExt(fb_phi, i64)));
  auto* fb_inc = b.CreateAdd(fb_phi, b.getInt32(1));
  b.CreateBr(loop_latch);

  b.SetInsertPoint(loop_latch);
  auto* m_next = b.CreatePHI(i32, 2);
  m_next->addIncoming(m_inc, emit_pack);
  m_next->addIncoming(m_phi, emit_fb);
  auto* fb_next = b.CreatePHI(i32, 2);
  fb_next->addIncoming(fb_phi, emit_pack);
  fb_next->addIncoming(fb_inc, emit_fb);
  auto* idx_next = b.CreateAdd(idx_phi, b.getInt32(1));
  idx_phi->addIncoming(idx_next, loop_latch);
  m_phi->addIncoming(m_next, loop_latch);
  fb_phi->addIncoming(fb_next, loop_latch);
  b.CreateCondBr(b.CreateICmpULT(idx_next, count), loop_hdr, exit_blk);

  b.SetInsertPoint(exit_blk);
  auto* m_final = b.CreatePHI(i32, 2);
  m_final->addIncoming(b.getInt32(0), entry);
  m_final->addIncoming(m_next, loop_latch);
  auto* fb_final = b.CreatePHI(i32, 2);
  fb_final->addIncoming(b.getInt32(0), entry);
  fb_final->addIncoming(fb_next, loop_latch);
  b.CreateStore(m_final, out_packed_cnt);
  b.CreateStore(fb_final, out_fb_cnt);
  b.CreateRetVoid();

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(ctx),
                                      "packed2x32-dedup-pack");
  if (!jit) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  auto compiled = symbol->toPtr<PackFn>();
  cache.emplace(key, CacheEntry{.jit = std::move(jit), .fn = compiled});
  return PackKernel(compiled);
#endif
}

std::optional<JitPacked2x32RadixDedup::ScatterKernel>
JitPacked2x32RadixDedup::CompileScatter(uint8_t part_shift) {
#ifndef TINYLAMB_HAS_LLVM
  (void)part_shift;
  return std::nullopt;
#else
  if (part_shift < 40 || part_shift >= 64) {
    return std::nullopt;
  }
  struct CacheEntry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    ScatterFn fn{nullptr};
  };
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto& cache = *new std::unordered_map<uint8_t, CacheEntry>;
  const std::scoped_lock lock(jit_cache_mutex);
  if (auto it = cache.find(part_shift); it != cache.end()) {
    return ScatterKernel(it->second.fn);
  }

  InitializeLlvm();
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.packed2x32_dedup_scatter", *ctx);
  llvm::IRBuilder<> b(*ctx);
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_packed2x32_dedup_scatter";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(b.getVoidTy(), {i32, ptr, ptr, ptr}, false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  fn->addParamAttr(1, llvm::Attribute::ReadOnly);
  fn->addParamAttr(2, llvm::Attribute::ReadOnly);
  fn->addParamAttr(3, llvm::Attribute::NoAlias);

  auto* count = fn->getArg(0);
  auto* in_packed = fn->getArg(1);
  auto* dst_ptrs = fn->getArg(2);
  auto* dst_pos = fn->getArg(3);

  auto* entry = llvm::BasicBlock::Create(*ctx, "entry", fn);
  auto* body = llvm::BasicBlock::Create(*ctx, "body", fn);
  auto* exit_blk = llvm::BasicBlock::Create(*ctx, "exit", fn);

  b.SetInsertPoint(entry);
  b.CreateCondBr(b.CreateICmpEQ(count, b.getInt32(0)), exit_blk, body);

  b.SetInsertPoint(body);
  auto* i_phi = b.CreatePHI(i32, 2);
  i_phi->addIncoming(b.getInt32(0), entry);
  auto* i64_v = b.CreateZExt(i_phi, i64);
  auto* packed = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, in_packed, i64_v));
  auto* p =
      b.CreateLShr(EmitSplitMix64Packed(b, packed), b.getInt64(part_shift));
  auto* pos_ptr = b.CreateInBoundsGEP(i32, dst_pos, p);
  auto* pos = b.CreateLoad(i32, pos_ptr);
  b.CreateStore(b.CreateAdd(pos, b.getInt32(1)), pos_ptr);
  auto* dst_base = b.CreateLoad(ptr, b.CreateInBoundsGEP(ptr, dst_ptrs, p));
  b.CreateStore(packed,
                b.CreateInBoundsGEP(i64, dst_base, b.CreateZExt(pos, i64)));
  auto* i_next = b.CreateAdd(i_phi, b.getInt32(1));
  i_phi->addIncoming(i_next, body);
  b.CreateCondBr(b.CreateICmpULT(i_next, count), body, exit_blk);

  b.SetInsertPoint(exit_blk);
  b.CreateRetVoid();

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(ctx),
                                      "packed2x32-dedup-scatter");
  if (!jit) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  auto compiled = symbol->toPtr<ScatterFn>();
  cache.emplace(part_shift, CacheEntry{.jit = std::move(jit), .fn = compiled});
  return ScatterKernel(compiled);
#endif
}

std::optional<JitPacked2x32RadixDedup::DedupKernel>
JitPacked2x32RadixDedup::CompileOpenAddrDedup() {
#ifndef TINYLAMB_HAS_LLVM
  return std::nullopt;
#else
  const std::scoped_lock lock(jit_cache_mutex);
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto* const owner = new std::unique_ptr<llvm::orc::LLJIT>;
  static DedupFn compiled = nullptr;
  if (compiled != nullptr) {
    return DedupKernel(compiled);
  }

  InitializeLlvm();
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto module = std::make_unique<llvm::Module>(
      "tinylamb.packed2x32_open_addr_dedup", *ctx);
  llvm::IRBuilder<> b(*ctx);
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_packed2x32_open_addr_dedup";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(
          i32, {i32, ptr, ptr, ptr, i32, i64, i32, ptr, ptr, ptr}, false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  fn->addParamAttr(1, llvm::Attribute::ReadOnly);
  for (unsigned ai : {2U, 3U, 7U, 8U, 9U}) {
    fn->addParamAttr(ai, llvm::Attribute::NoAlias);
  }

  auto* count = fn->getArg(0);
  auto* in_keys = fn->getArg(1);
  auto* ht_keys = fn->getArg(2);
  auto* ht_gens = fn->getArg(3);
  auto* cur_gen = fn->getArg(4);
  auto* ht_mask = fn->getArg(5);
  auto* max_load = fn->getArg(6);
  auto* n_occ_ptr = fn->getArg(7);
  auto* out_k0 = fn->getArg(8);
  auto* out_k1 = fn->getArg(9);

  llvm::Function* pf_fn =
      IntrinsicDecl(module.get(), llvm::Intrinsic::prefetch, {ptr});

  auto* entry = llvm::BasicBlock::Create(*ctx, "entry", fn);
  auto* pf_cond = llvm::BasicBlock::Create(*ctx, "pf.cond", fn);
  auto* tl_cond = llvm::BasicBlock::Create(*ctx, "tl.cond", fn);
  auto* full_exit = llvm::BasicBlock::Create(*ctx, "full.exit", fn);
  auto* done_exit = llvm::BasicBlock::Create(*ctx, "done.exit", fn);

  b.SetInsertPoint(entry);
  auto* init_occ = b.CreateLoad(i32, n_occ_ptr);
  auto* pf_end =
      b.CreateSelect(b.CreateICmpUGT(count, b.getInt32(8)),
                     b.CreateSub(count, b.getInt32(8)), b.getInt32(0));
  b.CreateBr(pf_cond);

  std::vector<std::pair<llvm::Value*, llvm::BasicBlock*>> full_preds;
  std::vector<std::pair<llvm::Value*, llvm::BasicBlock*>> full_occ_preds;

  const auto build_stage = [&](llvm::BasicBlock* cond_blk,
                               llvm::BasicBlock* prev_blk, llvm::Value* start_i,
                               llvm::Value* start_occ,
                               llvm::BasicBlock* next_stage, llvm::Value* end_i,
                               bool do_pf, std::string_view tag) {
    const auto mk = [&](std::string_view s) {
      return llvm::BasicBlock::Create(*ctx, std::string(tag) + std::string(s),
                                      fn);
    };
    auto* body = mk(".body");
    auto* probe_hdr = mk(".probe.hdr");
    auto* probe_key = mk(".probe.key");
    auto* ins_check = mk(".ins.chk");
    auto* ins_do = mk(".ins.do");
    auto* next_slot = mk(".next.slot");
    auto* latch = mk(".latch");

    b.SetInsertPoint(cond_blk);
    auto* i_phi = b.CreatePHI(i32, 2);
    auto* occ_phi = b.CreatePHI(i32, 2);
    i_phi->addIncoming(start_i, prev_blk);
    occ_phi->addIncoming(start_occ, prev_blk);
    b.CreateCondBr(b.CreateICmpULT(i_phi, end_i), body, next_stage);

    b.SetInsertPoint(body);
    auto* i64_v = b.CreateZExt(i_phi, i64);
    if (do_pf) {
      auto* pf_k = b.CreateLoad(
          i64,
          b.CreateInBoundsGEP(i64, in_keys, b.CreateAdd(i64_v, b.getInt64(8))));
      auto* pf_slot = b.CreateAnd(EmitSplitMix64Packed(b, pf_k), ht_mask);
      EmitPrefetchRead(b, pf_fn, b.CreateInBoundsGEP(i32, ht_gens, pf_slot));
      EmitPrefetchRead(b, pf_fn, b.CreateInBoundsGEP(i64, ht_keys, pf_slot));
    }
    auto* k = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, in_keys, i64_v));
    auto* init_slot = b.CreateAnd(EmitSplitMix64Packed(b, k), ht_mask);
    b.CreateBr(probe_hdr);

    b.SetInsertPoint(probe_hdr);
    auto* slot_phi = b.CreatePHI(i64, 2);
    slot_phi->addIncoming(init_slot, body);
    auto* g = b.CreateLoad(i32, b.CreateInBoundsGEP(i32, ht_gens, slot_phi));
    b.CreateCondBr(b.CreateICmpNE(g, cur_gen), ins_check, probe_key);

    b.SetInsertPoint(ins_check);
    b.CreateCondBr(b.CreateICmpUGE(occ_phi, max_load), full_exit, ins_do);
    full_preds.emplace_back(i_phi, ins_check);
    full_occ_preds.emplace_back(occ_phi, ins_check);

    b.SetInsertPoint(ins_do);
    b.CreateStore(k, b.CreateInBoundsGEP(i64, ht_keys, slot_phi));
    b.CreateStore(cur_gen, b.CreateInBoundsGEP(i32, ht_gens, slot_phi));
    auto* occ64 = b.CreateZExt(occ_phi, i64);
    auto* k0_val =
        b.CreateSExt(b.CreateTrunc(b.CreateLShr(k, b.getInt64(32)), i32), i64);
    auto* k1_val = b.CreateSExt(b.CreateTrunc(k, i32), i64);
    b.CreateStore(k0_val, b.CreateInBoundsGEP(i64, out_k0, occ64));
    b.CreateStore(k1_val, b.CreateInBoundsGEP(i64, out_k1, occ64));
    auto* occ_inc = b.CreateAdd(occ_phi, b.getInt32(1));
    b.CreateBr(latch);

    b.SetInsertPoint(probe_key);
    auto* hk = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, ht_keys, slot_phi));
    b.CreateCondBr(b.CreateICmpEQ(hk, k), latch, next_slot);

    b.SetInsertPoint(next_slot);
    slot_phi->addIncoming(
        b.CreateAnd(b.CreateAdd(slot_phi, b.getInt64(1)), ht_mask), next_slot);
    b.CreateBr(probe_hdr);

    b.SetInsertPoint(latch);
    auto* occ_next = b.CreatePHI(i32, 2);
    occ_next->addIncoming(occ_inc, ins_do);
    occ_next->addIncoming(occ_phi, probe_key);
    auto* i_next = b.CreateAdd(i_phi, b.getInt32(1));
    i_phi->addIncoming(i_next, latch);
    occ_phi->addIncoming(occ_next, latch);
    b.CreateBr(cond_blk);

    return std::pair<llvm::Value*, llvm::Value*>{i_phi, occ_phi};
  };

  auto [pf_end_i, pf_end_occ] = build_stage(
      pf_cond, entry, b.getInt32(0), init_occ, tl_cond, pf_end, true, "pf");
  auto [tl_end_i, tl_end_occ] = build_stage(
      tl_cond, pf_cond, pf_end_i, pf_end_occ, done_exit, count, false, "tl");
  (void)tl_end_i;

  b.SetInsertPoint(full_exit);
  auto* ret_full = b.CreatePHI(i32, static_cast<unsigned>(full_preds.size()));
  auto* occ_full =
      b.CreatePHI(i32, static_cast<unsigned>(full_occ_preds.size()));
  for (const auto& [v, bb] : full_preds) {
    ret_full->addIncoming(v, bb);
  }
  for (const auto& [v, bb] : full_occ_preds) {
    occ_full->addIncoming(v, bb);
  }
  b.CreateStore(occ_full, n_occ_ptr);
  b.CreateRet(ret_full);

  b.SetInsertPoint(done_exit);
  b.CreateStore(tl_end_occ, n_occ_ptr);
  b.CreateRet(count);

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(ctx),
                                      "packed2x32-open-addr-dedup");
  if (!jit) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  compiled = symbol->toPtr<DedupFn>();
  *owner = std::move(jit);
  return DedupKernel(compiled);
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitPacked2x32RadixDedup::PackKernel::Run(
    uint32_t count, const uint32_t* sel, const int64_t* k0, const uint8_t* n0,
    const int64_t* k1, const uint8_t* n1, uint64_t* out_packed,
    uint32_t* out_hist, uint32_t* out_fb_rows, uint32_t* out_packed_count,
    uint32_t* out_fb_count) const noexcept {
  fn_(count, sel, k0, n0, k1, n1, out_packed, out_hist, out_fb_rows,
      out_packed_count, out_fb_count);
}

TINYLAMB_JIT_CALL_BOUNDARY
void JitPacked2x32RadixDedup::ScatterKernel::Run(
    uint32_t count, const uint64_t* in_packed, uint64_t* const* dst_ptrs,
    uint32_t* dst_pos) const noexcept {
  fn_(count, in_packed, dst_ptrs, dst_pos);
}

TINYLAMB_JIT_CALL_BOUNDARY
uint32_t JitPacked2x32RadixDedup::DedupKernel::Run(
    uint32_t count, const uint64_t* in_keys, uint64_t* ht_keys,
    uint32_t* ht_gens, uint32_t cur_gen, uint64_t ht_mask, uint32_t max_load,
    uint32_t* n_occ, int64_t* out_k0, int64_t* out_k1) const noexcept {
  return fn_(count, in_keys, ht_keys, ht_gens, cur_gen, ht_mask, max_load,
             n_occ, out_k0, out_k1);
}

// ----------------------------------------------------------------------------
// JitRadixKeyDoubleAgg
// ----------------------------------------------------------------------------

std::optional<JitRadixKeyDoubleAgg> JitRadixKeyDoubleAgg::Compile(
    bool check_inf) {
#ifndef TINYLAMB_HAS_LLVM
  (void)check_inf;
  return std::nullopt;
#else
  struct CacheEntry {
    std::unique_ptr<llvm::orc::LLJIT> jit;
    AggFn fn{nullptr};
  };
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cert-err58-cpp)
  static auto& cache = *new std::array<CacheEntry, 2>;
  const std::scoped_lock lock(jit_cache_mutex);
  const size_t key = check_inf ? 1U : 0U;
  if (cache[key].fn != nullptr) {
    return JitRadixKeyDoubleAgg(cache[key].fn);
  }

  InitializeLlvm();
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto module =
      std::make_unique<llvm::Module>("tinylamb.radix_key_double_agg", *ctx);
  llvm::IRBuilder<> b(*ctx);
  auto* i8 = b.getInt8Ty();
  auto* i32 = b.getInt32Ty();
  auto* i64 = b.getInt64Ty();
  auto* f64 = b.getDoubleTy();
  auto* ptr = b.getPtrTy();
  constexpr std::string_view kSymbol = "tinylamb_radix_key_double_agg";
  auto* fn = llvm::Function::Create(
      llvm::FunctionType::get(i32,
                              {i32, ptr, ptr, ptr, ptr, ptr, ptr, ptr, ptr, i32,
                               i64, i32, ptr, ptr},
                              false),
      llvm::Function::ExternalLinkage, kSymbol, *module);
  for (unsigned ai : {1U, 2U, 3U}) {
    fn->addParamAttr(ai, llvm::Attribute::ReadOnly);
  }
  for (unsigned ai : {4U, 5U, 6U, 7U, 8U, 12U, 13U}) {
    fn->addParamAttr(ai, llvm::Attribute::NoAlias);
  }

  auto* count = fn->getArg(0);
  auto* in_keys = fn->getArg(1);
  auto* in_vals = fn->getArg(2);
  auto* in_valids = fn->getArg(3);
  auto* ht_keys = fn->getArg(4);
  auto* ht_sums = fn->getArg(5);
  auto* ht_counts = fn->getArg(6);
  auto* ht_gens = fn->getArg(7);
  auto* ht_inf = fn->getArg(8);
  auto* cur_gen = fn->getArg(9);
  auto* ht_mask = fn->getArg(10);
  auto* max_load = fn->getArg(11);
  auto* occupied = fn->getArg(12);
  auto* n_occ_ptr = fn->getArg(13);

  llvm::Function* pf_fn =
      IntrinsicDecl(module.get(), llvm::Intrinsic::prefetch, {ptr});
  llvm::Function* fabs_fn =
      check_inf ? IntrinsicDecl(module.get(), llvm::Intrinsic::fabs, {f64})
                : nullptr;
  llvm::Constant* pos_inf =
      check_inf ? llvm::ConstantFP::getInfinity(f64) : nullptr;
  llvm::MDNode* unlikely_md = check_inf ? UnlikelyBranchWeights(*ctx) : nullptr;

  auto* entry = llvm::BasicBlock::Create(*ctx, "entry", fn);
  auto* pf_cond = llvm::BasicBlock::Create(*ctx, "pf.cond", fn);
  auto* tl_cond = llvm::BasicBlock::Create(*ctx, "tl.cond", fn);
  auto* full_exit = llvm::BasicBlock::Create(*ctx, "full.exit", fn);
  auto* done_exit = llvm::BasicBlock::Create(*ctx, "done.exit", fn);

  b.SetInsertPoint(entry);
  auto* init_occ = b.CreateLoad(i32, n_occ_ptr);
  auto* pf_end =
      b.CreateSelect(b.CreateICmpUGT(count, b.getInt32(8)),
                     b.CreateSub(count, b.getInt32(8)), b.getInt32(0));
  b.CreateBr(pf_cond);

  std::vector<std::pair<llvm::Value*, llvm::BasicBlock*>> full_preds;
  std::vector<std::pair<llvm::Value*, llvm::BasicBlock*>> full_occ_preds;

  const auto build_stage = [&](llvm::BasicBlock* cond_blk,
                               llvm::BasicBlock* prev_blk, llvm::Value* start_i,
                               llvm::Value* start_occ,
                               llvm::BasicBlock* next_stage, llvm::Value* end_i,
                               bool do_pf, std::string_view tag) {
    const auto mk = [&](std::string_view s) {
      return llvm::BasicBlock::Create(*ctx, std::string(tag) + std::string(s),
                                      fn);
    };
    auto* body = mk(".body");
    auto* probe_hdr = mk(".probe.hdr");
    auto* probe_key = mk(".probe.key");
    auto* ins_chk = mk(".ins.chk");
    auto* ins_do = mk(".ins.do");
    auto* next_slot = mk(".next.slot");
    auto* check_valid = mk(".chk.valid");
    auto* do_add = mk(".do.add");
    auto* latch = mk(".latch");

    b.SetInsertPoint(cond_blk);
    auto* i_phi = b.CreatePHI(i32, 2);
    auto* occ_phi = b.CreatePHI(i32, 2);
    i_phi->addIncoming(start_i, prev_blk);
    occ_phi->addIncoming(start_occ, prev_blk);
    b.CreateCondBr(b.CreateICmpULT(i_phi, end_i), body, next_stage);

    b.SetInsertPoint(body);
    auto* i64_v = b.CreateZExt(i_phi, i64);
    if (do_pf) {
      auto* pf_k = b.CreateLoad(
          i64,
          b.CreateInBoundsGEP(i64, in_keys, b.CreateAdd(i64_v, b.getInt64(8))));
      auto* pf_slot = b.CreateAnd(EmitSplitMix64Packed(b, pf_k), ht_mask);
      EmitPrefetchWrite(b, pf_fn, b.CreateInBoundsGEP(i32, ht_gens, pf_slot));
      EmitPrefetchWrite(b, pf_fn, b.CreateInBoundsGEP(i64, ht_keys, pf_slot));
      EmitPrefetchWrite(b, pf_fn, b.CreateInBoundsGEP(f64, ht_sums, pf_slot));
    }
    auto* k = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, in_keys, i64_v));
    auto* init_slot = b.CreateAnd(EmitSplitMix64Packed(b, k), ht_mask);
    b.CreateBr(probe_hdr);

    b.SetInsertPoint(probe_hdr);
    auto* slot_phi = b.CreatePHI(i64, 2);
    slot_phi->addIncoming(init_slot, body);
    auto* g = b.CreateLoad(i32, b.CreateInBoundsGEP(i32, ht_gens, slot_phi));
    b.CreateCondBr(b.CreateICmpNE(g, cur_gen), ins_chk, probe_key);

    b.SetInsertPoint(ins_chk);
    b.CreateCondBr(b.CreateICmpUGE(occ_phi, max_load), full_exit, ins_do);
    full_preds.emplace_back(i_phi, ins_chk);
    full_occ_preds.emplace_back(occ_phi, ins_chk);

    b.SetInsertPoint(ins_do);
    b.CreateStore(k, b.CreateInBoundsGEP(i64, ht_keys, slot_phi));
    b.CreateStore(llvm::ConstantFP::get(f64, 0.0),
                  b.CreateInBoundsGEP(f64, ht_sums, slot_phi));
    b.CreateStore(b.getInt64(0), b.CreateInBoundsGEP(i64, ht_counts, slot_phi));
    b.CreateStore(cur_gen, b.CreateInBoundsGEP(i32, ht_gens, slot_phi));
    if (check_inf) {
      b.CreateStore(b.getInt8(0), b.CreateInBoundsGEP(i8, ht_inf, slot_phi));
    }
    b.CreateStore(
        b.CreateTrunc(slot_phi, i32),
        b.CreateInBoundsGEP(i32, occupied, b.CreateZExt(occ_phi, i64)));
    auto* occ_inc = b.CreateAdd(occ_phi, b.getInt32(1));
    b.CreateBr(check_valid);

    b.SetInsertPoint(probe_key);
    auto* hk = b.CreateLoad(i64, b.CreateInBoundsGEP(i64, ht_keys, slot_phi));
    b.CreateCondBr(b.CreateICmpEQ(hk, k), check_valid, next_slot);

    b.SetInsertPoint(next_slot);
    slot_phi->addIncoming(
        b.CreateAnd(b.CreateAdd(slot_phi, b.getInt64(1)), ht_mask), next_slot);
    b.CreateBr(probe_hdr);

    b.SetInsertPoint(check_valid);
    auto* occ_cur = b.CreatePHI(i32, 2);
    occ_cur->addIncoming(occ_inc, ins_do);
    occ_cur->addIncoming(occ_phi, probe_key);
    auto* vld = b.CreateLoad(i8, b.CreateInBoundsGEP(i8, in_valids, i64_v));
    b.CreateCondBr(b.CreateICmpNE(vld, b.getInt8(0)), do_add, latch);

    b.SetInsertPoint(do_add);
    auto* sum_ptr = b.CreateInBoundsGEP(f64, ht_sums, slot_phi);
    auto* cnt_ptr = b.CreateInBoundsGEP(i64, ht_counts, slot_phi);
    auto* prev = b.CreateLoad(f64, sum_ptr);
    auto* dv = b.CreateLoad(f64, b.CreateInBoundsGEP(f64, in_vals, i64_v));
    auto* next_s = b.CreateFAdd(prev, dv);
    if (check_inf) {
      auto* inf_next =
          b.CreateFCmpOEQ(b.CreateCall(fabs_fn, {next_s}), pos_inf);
      auto* fin_prev = b.CreateFCmpUNE(b.CreateCall(fabs_fn, {prev}), pos_inf);
      auto* fin_dv = b.CreateFCmpUNE(b.CreateCall(fabs_fn, {dv}), pos_inf);
      auto* ovf = b.CreateAnd(inf_next, b.CreateAnd(fin_prev, fin_dv));
      auto* set_inf = mk(".set_inf");
      auto* after_inf = mk(".after_inf");
      auto* br = b.CreateCondBr(ovf, set_inf, after_inf);
      br->setMetadata(llvm::LLVMContext::MD_prof, unlikely_md);
      b.SetInsertPoint(set_inf);
      b.CreateStore(b.getInt8(1), b.CreateInBoundsGEP(i8, ht_inf, slot_phi));
      b.CreateBr(after_inf);
      b.SetInsertPoint(after_inf);
    }
    b.CreateStore(next_s, sum_ptr);
    b.CreateStore(b.CreateAdd(b.CreateLoad(i64, cnt_ptr), b.getInt64(1)),
                  cnt_ptr);
    auto* add_end = b.GetInsertBlock();
    b.CreateBr(latch);

    b.SetInsertPoint(latch);
    (void)add_end;
    auto* i_next = b.CreateAdd(i_phi, b.getInt32(1));
    i_phi->addIncoming(i_next, latch);
    occ_phi->addIncoming(occ_cur, latch);
    b.CreateBr(cond_blk);

    return std::pair<llvm::Value*, llvm::Value*>{i_phi, occ_phi};
  };

  auto [pf_end_i, pf_end_occ] = build_stage(
      pf_cond, entry, b.getInt32(0), init_occ, tl_cond, pf_end, true, "pf");
  auto [tl_end_i, tl_end_occ] = build_stage(
      tl_cond, pf_cond, pf_end_i, pf_end_occ, done_exit, count, false, "tl");
  (void)tl_end_i;

  b.SetInsertPoint(full_exit);
  auto* ret_full = b.CreatePHI(i32, static_cast<unsigned>(full_preds.size()));
  auto* occ_full =
      b.CreatePHI(i32, static_cast<unsigned>(full_occ_preds.size()));
  for (const auto& [v, bb] : full_preds) {
    ret_full->addIncoming(v, bb);
  }
  for (const auto& [v, bb] : full_occ_preds) {
    occ_full->addIncoming(v, bb);
  }
  b.CreateStore(occ_full, n_occ_ptr);
  b.CreateRet(ret_full);

  b.SetInsertPoint(done_exit);
  b.CreateStore(tl_end_occ, n_occ_ptr);
  b.CreateRet(count);

  auto jit = OptimizeAndCompileModule(std::move(module), std::move(ctx),
                                      "radix-key-double-agg");
  if (!jit) {
    return std::nullopt;
  }
  auto symbol = jit->lookup(kSymbol);
  if (!symbol) {
    llvm::consumeError(symbol.takeError());
    return std::nullopt;
  }
  auto compiled = symbol->toPtr<AggFn>();
  cache[key] = CacheEntry{.jit = std::move(jit), .fn = compiled};
  return JitRadixKeyDoubleAgg(compiled);
#endif
}

TINYLAMB_JIT_CALL_BOUNDARY
uint32_t JitRadixKeyDoubleAgg::Accumulate(
    uint32_t count, const uint64_t* in_keys, const double* in_vals,
    const uint8_t* in_valids, uint64_t* ht_keys, double* ht_sums,
    uint64_t* ht_counts, uint32_t* ht_gens, uint8_t* ht_inf, uint32_t cur_gen,
    uint64_t ht_mask, uint32_t max_load, uint32_t* occupied,
    uint32_t* n_occ) const noexcept {
  return fn_(count, in_keys, in_vals, in_valids, ht_keys, ht_sums, ht_counts,
             ht_gens, ht_inf, cur_gen, ht_mask, max_load, occupied, n_occ);
}

#undef TINYLAMB_JIT_CALL_BOUNDARY

}  // namespace tinylamb

#ifdef __clang__
#pragma clang diagnostic pop
#endif
