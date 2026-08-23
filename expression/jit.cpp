/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/jit.hpp"
#include <llvm/IR/Value.h>
#include <llvm/IR/DerivedTypes.h>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <ratio>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include "common/constants.hpp"
#include <utility>

#ifdef TINYLAMB_HAS_LLVM
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/TargetSelect.h>
#endif

namespace tinylamb {

struct JitInt64Kernels::Impl {
#ifdef TINYLAMB_HAS_LLVM
  std::unique_ptr<llvm::orc::LLJIT> jit;
#endif
  FilterFn filter{nullptr};
  ProjectionFn projection{nullptr};
  SumFn sum{nullptr};
  double compile_ms{0};
};

#ifdef TINYLAMB_HAS_LLVM
namespace {
std::mutex jit_cache_mutex;
std::unordered_map<BinaryOperation, std::shared_ptr<JitInt64Kernels::Impl>>
    filter_kernel_cache;
std::shared_ptr<JitInt64Kernels::Impl> projection_kernel_cache;
std::shared_ptr<JitInt64Kernels::Impl> sum_kernel_cache;

void InitializeLlvm() {
  static std::once_flag initialized;
  std::call_once(initialized, [] {
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
  });
}

std::shared_ptr<JitInt64Kernels::Impl> CreateImpl(
    std::unique_ptr<llvm::Module> module,
    std::unique_ptr<llvm::LLVMContext> context, std::string_view symbol) {
  auto impl = std::make_shared<JitInt64Kernels::Impl>();
  auto jit = llvm::orc::LLJITBuilder().create();
  if (!jit) {
    llvm::consumeError(jit.takeError());
    return nullptr;
  }
  impl->jit = std::move(*jit);
  if (llvm::Error error = impl->jit->addIRModule(
          llvm::orc::ThreadSafeModule(std::move(module), std::move(context)))) {
    llvm::consumeError(std::move(error));
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
  } else {
    assert(symbol == "tinylamb_sum");
    impl->sum = address->toPtr<JitInt64Kernels::SumFn>();
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
      // Callers guard with IsComparison(); reaching here is a logic error.
      assert(false);
      return nullptr;
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
  if (!IsComparison(operation)) { return std::nullopt;
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
  auto* function = llvm::Function::Create(
      type, llvm::Function::ExternalLinkage, "tinylamb_filter", *module);
  auto *argument = function->arg_begin();
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
  auto impl = CreateImpl(std::move(module), std::move(context),
                         "tinylamb_filter");
  if (!impl) { return std::nullopt;
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
       llvm::PointerType::getUnqual(*context), i64, i64, i64}, false);
  auto* function = llvm::Function::Create(
      type, llvm::Function::ExternalLinkage, "tinylamb_project", *module);
  auto *argument = function->arg_begin();
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
  auto* projected = builder.CreateAdd(builder.CreateMul(value, multiplier), addend);
  builder.CreateStore(projected, builder.CreateGEP(i64, output, index));
  auto* next = builder.CreateAdd(index, builder.getInt64(1));
  builder.CreateBr(loop);
  index->addIncoming(next, body);
  builder.SetInsertPoint(exit);
  builder.CreateRetVoid();
  auto impl = CreateImpl(std::move(module), std::move(context),
                         "tinylamb_project");
  if (!impl) { return std::nullopt;
}
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin).count();
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
  auto* function = llvm::Function::Create(
      type, llvm::Function::ExternalLinkage, "tinylamb_sum", *module);
  auto *argument = function->arg_begin();
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
  if (!impl) { return std::nullopt;
}
  impl->compile_ms = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - begin).count();
  {
    std::scoped_lock lock(jit_cache_mutex);
    if (!sum_kernel_cache) {
      sum_kernel_cache = std::move(impl);
    }
    return JitInt64Kernels(sum_kernel_cache);
  }
#endif
}

void JitInt64Kernels::Filter(const int64_t* input, uint8_t* output,
                             size_t count, int64_t constant) const {
  if (impl_ == nullptr || impl_->filter == nullptr) {
    throw std::logic_error("not a filter kernel");
  }
  impl_->filter(input, output, static_cast<uint64_t>(count), constant);
}
void JitInt64Kernels::Project(const int64_t* input, int64_t* output,
                              size_t count, int64_t multiplier,
                              int64_t addend) const {
  if (impl_ == nullptr || impl_->projection == nullptr) {
    throw std::logic_error("not a projection kernel");
  }
  impl_->projection(input, output, static_cast<uint64_t>(count), multiplier,
                    addend);
}
int64_t JitInt64Kernels::Sum(const int64_t* input, size_t count) const {
  if (impl_ == nullptr || impl_->sum == nullptr) {
    throw std::logic_error("not a sum kernel");
  }
  return impl_->sum(input, static_cast<uint64_t>(count));
}
double JitInt64Kernels::CompileMilliseconds() const {
  return impl_ ? impl_->compile_ms : 0.0;
}

}  // namespace tinylamb
