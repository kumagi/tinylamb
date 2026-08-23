/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_ast.hpp"

#include <charconv>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include "common/status_or.hpp"
#include "common/constants.hpp"

namespace tinylamb {
namespace {

bool ParseLocation(std::string& label, size_t* start, size_t* end) {
  if (label.empty() || label.back() != ']') { return false;
}
  const size_t open = label.rfind(" [");
  if (open == std::string::npos) { return false;
}
  const size_t dash = label.find('-', open + 2);
  if (dash == std::string::npos) { return false;
}
  const char* first = label.data() + open + 2;
  const char* middle = label.data() + dash;
  const char* last = label.data() + label.size() - 1;
  size_t parsed_start = 0;
  size_t parsed_end = 0;
  if (std::from_chars(first, middle, parsed_start).ec != std::errc() ||
      std::from_chars(middle + 1, last, parsed_end).ec != std::errc()) {
    return false;
  }
  *start = parsed_start;
  *end = parsed_end;
  label.resize(open);
  return true;
}

std::unique_ptr<GoogleSqlAstNode> ParseNode(std::string label) {
  auto node = std::make_unique<GoogleSqlAstNode>();
  ParseLocation(label, &node->start, &node->end);
  const size_t open = label.find('(');
  if (open != std::string::npos && label.back() == ')') {
    node->kind = label.substr(0, open);
    node->detail = label.substr(open + 1, label.size() - open - 2);
  } else {
    node->kind = std::move(label);
  }
  return node;
}

}  // namespace

const GoogleSqlAstNode* GoogleSqlAstNode::Child(std::string_view child_kind,
                                                size_t occurrence) const {
  for (const auto& child : children) {
    if (child->kind == child_kind && occurrence-- == 0) { return child.get();
}
  }
  return nullptr;
}

std::vector<const GoogleSqlAstNode*> GoogleSqlAstNode::Children(
    std::string_view child_kind) const {
  std::vector<const GoogleSqlAstNode*> result;
  for (const auto& child : children) {
    if (child->kind == child_kind) { result.push_back(child.get());
}
  }
  return result;
}

StatusOr<std::unique_ptr<GoogleSqlAstNode>> GoogleSqlAstParser::Parse(
    std::string_view dump) {
  std::unique_ptr<GoogleSqlAstNode> root;
  std::vector<GoogleSqlAstNode*> parents;
  size_t cursor = 0;
  while (cursor < dump.size()) {
    size_t line_end = dump.find('\n', cursor);
    if (line_end == std::string_view::npos) { line_end = dump.size();
}
    std::string_view line = dump.substr(cursor, line_end - cursor);
    if (!line.empty() && line.back() == '\r') { line.remove_suffix(1);
}
    cursor = line_end + 1;
    if (line.empty()) { continue;
}

    size_t spaces = 0;
    while (spaces < line.size() && line[spaces] == ' ') { ++spaces;
}
    if (spaces % 2 != 0 || spaces == line.size()) { return Status::kUnknown;
}
    const size_t depth = spaces / 2;
    auto node = ParseNode(std::string(line.substr(spaces)));
    GoogleSqlAstNode* raw = node.get();
    if (depth == 0) {
      if (root) { return Status::kUnknown;
}
      root = std::move(node);
    } else {
      if (depth > parents.size()) { return Status::kUnknown;
}
      parents[depth - 1]->children.push_back(std::move(node));
    }
    if (parents.size() <= depth) { parents.resize(depth + 1);
}
    parents[depth] = raw;
    parents.resize(depth + 1);
  }
  if (!root) { return Status::kUnknown;
}
  return root;
}

}  // namespace tinylamb
