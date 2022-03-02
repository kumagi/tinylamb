//
// Created by kumagi on 2022/02/23.
//

#ifndef TINYLAMB_HASH_JOIN_HPP
#define TINYLAMB_HASH_JOIN_HPP

#include <memory>
#include <unordered_map>

#include "executor/executor_base.hpp"
#include "expression/expression_base.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class HashJoin : public ExecutorBase {
 public:
  HashJoin(std::unique_ptr<ExecutorBase>&& left, std::vector<size_t> left_cols,
           std::unique_ptr<ExecutorBase>&& right,
           std::vector<size_t> right_cols);

  ~HashJoin() override = default;
  bool Next(Row* dst) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  void BucketConstruct();

 private:
  std::unique_ptr<ExecutorBase> left_;
  std::vector<size_t> left_cols_;
  std::unique_ptr<ExecutorBase> right_;
  std::vector<size_t> right_cols_;

  Row hold_left_;
  std::string left_key_;
  bool bucket_constructed_{false};
  std::unordered_multimap<std::string, Row> right_buckets_;
  std::unordered_multimap<std::string, Row>::const_iterator
      right_buckets_iterator_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_HASH_JOIN_HPP
