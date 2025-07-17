/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_FUNCTION_HPP
#define TINYLAMB_FUNCTION_HPP

#include <string>
#include <vector>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "type/type.hpp"

namespace tinylamb {

class Function {
 public:
  Function() = default;
  Function(std::string name, int argument_count)
      : name_(std::move(name)), argument_count_(argument_count) {}
  Function(std::string name, std::vector<Type> args, Type return_type)
      : name_(std::move(name)),
        args_(std::move(args)),
        return_type_(std::move(return_type)),
        argument_count_(args.size()) {}

  friend Encoder& operator<<(Encoder& e, const Function& f) {
    e << f.name_ << f.args_ << f.return_type_
      << static_cast<int64_t>(f.argument_count_);
    return e;
  }
  friend Decoder& operator>>(Decoder& d, Function& f) {
    int64_t argument_count;
    d >> f.name_ >> f.args_ >> f.return_type_ >> argument_count;
    f.argument_count_ = argument_count;
    return d;
  }

 private:
  std::string name_;
  std::vector<Type> args_;
  Type return_type_;
  int argument_count_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FUNCTION_HPP
