#!/usr/bin/env python3
#
# Copyright 2026 KUMAZAKI Hiroki
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""include-lint: enforce the declaration-layer DAG of tinylamb.

Scans every tracked C++ source for quoted #include directives and rejects
edges that point to a higher layer.  Layers (left = low, right = high;
a layer may only include equal or lower layers):

    common -> type -> storage(page + recovery/wal + transaction) -> index
           -> table -> database -> expression
           -> relational(executor/detail) -> plan -> executor
           -> sql(query/, parser/) -> server

benchmark/, main.cpp and test targets (*_test*, *_fuzzer*, *_benchmark.*) sit
above everything and may include freely.  legacy/ is an explicit archive and
is not checked.  page/recovery/transaction share one rank because CMake
declares them as the single tightly-coupled Layer 3 (see CMakeLists.txt);
splitting them apart is future work.  Known, accepted violations live in
DEFAULT_ALLOWLIST -- shrinking that list is how layering regressions get
paid off.

Usage:
    python3 scripts/check_layering.py                # enforce (exit 0/1)
    python3 scripts/check_layering.py --allowlist f  # extend allowlist
"""

import argparse
import fnmatch
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (layer name, path prefixes relative to repo root).  Longest prefix wins,
# so "executor/detail/" must precede "executor/".
LAYERS = (
    ("common", ("common/",)),
    ("type", ("type/",)),
    ("storage", ("page/", "recovery/", "transaction/")),
    ("index", ("index/",)),
    ("table", ("table/",)),
    ("database", ("database/",)),
    ("expression", ("expression/",)),
    ("relational", ("executor/detail/",)),
    ("plan", ("plan/",)),
    ("executor", ("executor/",)),
    ("sql", ("query/", "parser/")),
    ("server", ("server/",)),
)
RANK = {name: rank for rank, (name, _) in enumerate(LAYERS)}

TOP_PREFIXES = ("benchmark/",)  # plus main.cpp and test targets, below
TEST_PATTERN = re.compile(r"_test\.|_fuzzer|_benchmark\.")
IGNORED_PREFIXES = ("legacy/", "scripts/", "docs/", ".git/", "build")
INCLUDE_RE = re.compile(r'^[ \t]*#[ \t]*include[ \t]*"([^"]+)"', re.MULTILINE)

# Edges the target architecture bans outright (expression→database の逆転など).  They are
# violations even where the rank order would tolerate them; today they survive
# via the allowlist until the dependency gets inverted.
FORBIDDEN_PATTERNS = (
    "expression/* -> database/*",
)

# Accepted violations (fnmatch patterns over "<file> -> <include>").
DEFAULT_ALLOWLIST = (
    # --- 過去レビューで既知だった違反 (V1 / V3' / V4) -------------------
    # V1: expression -> database。A1 EvaluationContext 導入で解消予定。
    "expression/expression.hpp -> database/transaction_context.hpp",
    "expression/function_call_expression.cpp -> database/*",
    # V4: plan -> executor。A5 relational ファクトリ移設でほぼ解消
    # (plan/plan.hpp は不透明前方宣言、各 *_plan.cpp の EmitExecutor 実装は
    #  executor/relational_factory.cpp へ移設済み)。残存はハイブリッド
    # ハッシュのコスト推定ヘルパ (PreferHybridHashJoin / kHashJoinRowBytes
    # Estimate) が executor/hash_join_mode.hpp に在籍しているための 2 辺のみ。
    "plan/product_plan.cpp -> executor/hash_join_mode.hpp",
    "plan/implementation_rules.cpp -> executor/hash_join_mode.hpp",
    # V3': statement IR が executor 層より上位にある問題。
    #      A2-1 シム撤去前は parser/ast.hpp 経由、撤去後は直参照になるため
    #      移行期間中は両方を許容する。
    "executor/detail/* -> parser/ast.hpp",
    "executor/detail/* -> query/statement.hpp",
    "executor/relational.cpp -> query/statement.hpp",
    # --- 構造上既知だが未対処のエッジ (潰したらこのリストから削る) -------
    # common から type/value_type.hpp (依存ゼロの列挙型ヘッダ) を使う。
    "common/* -> type/value_type.hpp",
    # type/column.hpp が page/row_position.hpp (定数のみ) を使う。
    "type/column.hpp -> page/row_position.hpp",
    # V2 後遺症: index_scan_iterator だけ table ターゲット所属の循環回避。
    "index/index_scan_iterator.cpp -> table/*",
    "index/index_scan_iterator.hpp -> table/*",
    # ゾーンマップ/統計が述語式を直接評価する。
    "table/* -> expression/*",
    # bytecode と PAX が executor/data_chunk.hpp を要求 (S6/A1 で分離候補)。
    "expression/bytecode.hpp -> executor/data_chunk.hpp",
    "expression/bytecode.cpp -> executor/data_chunk.hpp",
    "page/pax_block.hpp -> executor/data_chunk.hpp",
    "page/pax_block.cpp -> executor/data_chunk.hpp",
    "page/pax_page.hpp -> executor/data_chunk.hpp",
    "page/pax_page.cpp -> executor/data_chunk.hpp",

    # relational(executor/detail) から executor 本体への上向き。A5 で解消予定。
    "executor/detail/* -> executor/*",
    # optimizer は query_data を使う (CMake 上 tinylamb_executor 所属)。
    "plan/* -> query/query_data.hpp",
    # サブクエリ脱相関 (tpch Phase2-4) のため optimizer が QueryExpression
    # 内の SelectStatement を解析する。sql 局のステートメント IR を読むだけ
    # で、実行は relational/subquery_runtime 側に留まる。
    "plan/optimizer.cpp -> query/statement.hpp",
)


def layer_of(rel_path):
    if TEST_PATTERN.search(rel_path) or rel_path == "main.cpp":
        return "top"
    if rel_path.startswith(TOP_PREFIXES):
        return "top"
    best, best_len = None, -1
    for name, prefixes in LAYERS:
        for prefix in prefixes:
            if rel_path.startswith(prefix) and len(prefix) > best_len:
                best, best_len = name, len(prefix)
    return best  # None => outside the DAG (e.g. cli_main_test.cpp)


def source_files():
    for path in sorted(ROOT.rglob("*")):
        rel = path.relative_to(ROOT).as_posix()
        if not path.is_file() or path.suffix not in (".hpp", ".cpp"):
            continue
        if rel.startswith(IGNORED_PREFIXES) or "/.git/" in rel:
            continue
        yield rel


def allowed(edge, extra):
    return any(fnmatch.fnmatchcase(edge, pattern) for pattern in tuple(DEFAULT_ALLOWLIST) + tuple(extra))


def is_violation(src, src_layer, include, dst_layer):
    edge = f"{src} -> {include}"
    if any(fnmatch.fnmatchcase(edge, p) for p in FORBIDDEN_PATTERNS):
        return True
    return RANK[dst_layer] > RANK[src_layer]


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=str(ROOT), help="repo root")
    parser.add_argument("--allowlist", action="append", default=[],
                        help="extra allowlist file ('src -> inc' lines)")
    args = parser.parse_args()

    extra = []
    for file_name in args.allowlist:
        text = Path(file_name).read_text(encoding="utf-8")
        extra += [line.strip() for line in text.splitlines()
                  if line.strip() and not line.lstrip().startswith("#")]

    violations = []
    for src in source_files():
        src_layer = layer_of(src)
        if src_layer is None or src_layer == "top":
            continue
        for include in INCLUDE_RE.findall(
                Path(args.root, src).read_text(encoding="utf-8", errors="replace")):
            dst_layer = layer_of(include)
            if dst_layer is None or dst_layer == "top":
                continue
            if not is_violation(src, src_layer, include, dst_layer):
                continue
            violations.append((src, src_layer, include, dst_layer))

    unallowed = [v for v in violations if not allowed(f"{v[0]} -> {v[2]}", extra)]
    for src, src_layer, include, dst_layer in sorted(unallowed):
        print(f"VIOLATION {src} [{src_layer}] -> {include} [{dst_layer}]")

    counts = {}
    for src, src_layer, _, dst_layer in violations:
        counts[f"{src_layer}->{dst_layer}"] = counts.get(f"{src_layer}->{dst_layer}", 0) + 1
    print(f"{len(violations)} upward edge(s): "
          + ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
          + f"; allowlisted={len(violations) - len(unallowed)}")
    return 1 if unallowed else 0


if __name__ == "__main__":
    sys.exit(main())
