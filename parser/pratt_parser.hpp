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

#ifndef TINYLAMB_PRATT_PARSER_HPP
#define TINYLAMB_PRATT_PARSER_HPP

#include <memory>
#include <vector>

#include "parser/ast.hpp"
#include "parser/token.hpp"

namespace tinylamb {

class PrattParser {
 public:
  explicit PrattParser(std::vector<Token>::const_iterator begin,
                       std::vector<Token>::const_iterator end);
  Expression ParseExpression(int precedence = 0);
  [[nodiscard]] size_t GetPos() const {
    return std::distance(begin_pos_, current_pos_);
  }
  void SetPos(size_t p) { current_pos_ = begin_pos_ + p; }

 private:
  Expression ParsePrimary();
  Expression ParseUnary();
  int GetPrecedence();

  Token Peek();
  Token Advance();
  void Expect(TokenType type);

  std::vector<Token>::const_iterator begin_pos_;
  std::vector<Token>::const_iterator current_pos_;
  std::vector<Token>::const_iterator end_pos_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PRATT_PARSER_HPP
