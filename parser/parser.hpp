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

#ifndef TINYLAMB_PARSER_HPP
#define TINYLAMB_PARSER_HPP

#include <memory>
#include <vector>

#include "parser/ast.hpp"
#include "parser/token.hpp"

namespace tinylamb {

class Parser {
 public:
  explicit Parser(const std::vector<Token>& tokens);
  std::unique_ptr<Statement> Parse();

 private:
  std::unique_ptr<Statement> ParseCreateTable();
  std::unique_ptr<Statement> ParseDropTable();
  std::unique_ptr<Statement> ParseSelect();
  std::unique_ptr<Statement> ParseInsert();
  std::unique_ptr<Statement> ParseUpdate();
  std::unique_ptr<Statement> ParseDelete();

  Expression ParseExpression();

  Token Peek();
  Token Advance();
  void Expect(TokenType type);

  std::vector<Token> tokens_;
  size_t pos_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_PARSER_HPP
