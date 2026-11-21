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

Scans every *.hpp/*.cpp under the repository root (outside build*/, scripts/
and docs/) for quoted #include directives and rejects
edges that point to a higher layer.  Layers (left = low, right = high;
a layer may only include equal or lower layers):

    common -> type -> storage(page + recovery/wal + transaction) -> index
           -> table -> database -> expression
           -> relational(executor/detail) -> plan -> executor
           -> sql(query/) -> server

benchmark/, main.cpp and test targets (*_test*, *_fuzzer*, *_benchmark.*) sit
above everything and may include freely -- except the fuzzer harnesses listed
in PRODUCTION_FUZZERS, which CMake compiles into the production `tinylamb_sql`
static library and are therefore ordinary layer members.  page/recovery/
transaction share one rank because CMake declares them as the single
tightly-coupled Layer 3 (see CMakeLists.txt); splitting them apart is future
work.  Known, accepted violations live in DEFAULT_ALLOWLIST -- shrinking that
list is how layering regressions get paid off.

Usage:
    python3 scripts/check_layering.py                # enforce (exit 0/1)
    python3 scripts/check_layering.py --root DIR     # check another checkout
    python3 scripts/check_layering.py --allowlist f  # extend allowlist
"""

import argparse
import fnmatch
from itertools import chain
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
    ("sql", ("query/",)),
    ("server", ("server/",)),
)
RANK = {name: rank for rank, (name, _) in enumerate(LAYERS)}

TOP_PREFIXES = ("benchmark/",)  # plus main.cpp and test targets, below
TEST_PATTERN = re.compile(r"_test[\._]|_fuzzer|_benchmark\.")
# Fuzzer harnesses compiled into a production library (the `tinylamb_sql`
# sources in CMakeLists.txt), not into a test/executable target.  The generic
# TEST_PATTERN exemption must not hide their includes from the DAG.
PRODUCTION_FUZZERS = frozenset({
    "query/expr_oracle_fuzzer.cpp",
    "query/griffin_fuzzer.cpp",
    "query/sql_aggregate_fuzzer.cpp",
    "query/sql_diff_oracle_fuzzer.cpp",
    "query/sql_join_fuzzer.cpp",
    "query/sql_ooc_fuzzer.cpp",
    "query/sql_oracle_fuzzer.cpp",
    "query/sql_plan_cache_fuzzer.cpp",
    "query/sql_session_fuzzer.cpp",
})
IGNORED_PREFIXES = ("scripts/", "docs/", ".git/", "build")
INCLUDE_RE = re.compile(r'^[ \t]*#[ \t]*include[ \t]*"([^"]+)"', re.MULTILINE)

# Edges the target architecture bans outright (expression→database の逆転など).  They are
# violations even where the rank order would tolerate them; today they survive
# via the allowlist until the dependency gets inverted.
FORBIDDEN_PATTERNS = (
    "expression/* -> database/*",
)

# Accepted violations (fnmatch patterns over "<file> -> <include>").
DEFAULT_ALLOWLIST = (
    # --- 過去レビューで既知だった違反 (V3' / V4) -------------------------
    # V1 (expression -> database) は解消済みのためエントリ削除済み。
    # V4: plan -> executor。A5 relational ファクトリ移設でほぼ解消
    # (plan/plan.hpp は不透明前方宣言、各 *_plan.cpp の EmitExecutor 実装は
    #  executor/relational_factory.cpp へ移設済み)。残存はハイブリッド
    # ハッシュのコスト推定ヘルパー (PreferHybridHashJoin / kHashJoinRowBytes
    # Estimate) が executor/hash_join_mode.hpp に在籍しているための 2 辺のみ。
    "plan/product_plan.cpp -> executor/hash_join_mode.hpp",
    "plan/implementation_rules.cpp -> executor/hash_join_mode.hpp",
    # V3': statement IR が executor 層より上位にある問題。
    #      移行期間中の直参照を許容する。
    # Expanded from wildcard 'executor/detail/* -> query/statement.hpp' (29 edges).
    # Exact edges only: a wildcard silently allows future regressions.
    "executor/detail/aggregate_accumulator.cpp -> query/statement.hpp",
    "executor/detail/build_input.cpp -> query/statement.hpp",
    "executor/detail/build_input_internal.hpp -> query/statement.hpp",
    "executor/detail/build_input_joins.cpp -> query/statement.hpp",
    "executor/detail/columnar_tlcs_fast_path.hpp -> query/statement.hpp",
    "executor/detail/explain_format.cpp -> query/statement.hpp",
    "executor/detail/explain_format.hpp -> query/statement.hpp",
    "executor/detail/expression_eval.cpp -> query/statement.hpp",
    "executor/detail/expression_eval.hpp -> query/statement.hpp",
    "executor/detail/fused_grouped_join.cpp -> query/statement.hpp",
    "executor/detail/fused_grouped_join.hpp -> query/statement.hpp",
    "executor/detail/fused_grouped_join_stream.cpp -> query/statement.hpp",
    "executor/detail/parallel_table_scan.cpp -> query/statement.hpp",
    "executor/detail/pipeline/ooc_join_cte_sources.cpp -> query/statement.hpp",
    "executor/detail/planning_heuristics.cpp -> query/statement.hpp",
    "executor/detail/planning_heuristics.hpp -> query/statement.hpp",
    "executor/detail/relational_sort_limit.cpp -> query/statement.hpp",
    "executor/detail/scan_filter.cpp -> query/statement.hpp",
    "executor/detail/scan_filter.hpp -> query/statement.hpp",
    "executor/detail/subquery_runtime.cpp -> query/statement.hpp",
    "executor/detail/subquery_runtime.hpp -> query/statement.hpp",
    "executor/detail/typed_column_eval.cpp -> query/statement.hpp",
    "executor/detail/typed_stream_aggregate.cpp -> query/statement.hpp",
    "executor/detail/typed_stream_aggregate.hpp -> query/statement.hpp",
    "executor/detail/typed_stream_aggregate_internal.hpp -> query/statement.hpp",
    "executor/detail/typed_stream_aggregate_partitioned.hpp -> query/statement.hpp",
    "executor/detail/window_eval.cpp -> query/statement.hpp",
    "executor/detail/window_eval.hpp -> query/statement.hpp",
    "executor/detail/window_eval_internal.hpp -> query/statement.hpp",
    "executor/relational.cpp -> query/statement.hpp",
    # apply / recursive_cte / unnest 物理演算子とその plan ノードは
    # SelectStatement IR を直接扱う (relational と同じ V3' 系)。
    "executor/apply.cpp -> query/statement.hpp",
    "executor/apply.hpp -> query/statement.hpp",
    "executor/recursive_cte.cpp -> query/statement.hpp",
    "executor/recursive_cte.hpp -> query/statement.hpp",
    "executor/unnest.cpp -> query/statement.hpp",
    "plan/apply_plan.cpp -> query/statement.hpp",
    "plan/apply_plan.hpp -> query/statement.hpp",
    "plan/cascades.hpp -> query/statement.hpp",
    "plan/implementation_rules.cpp -> query/statement.hpp",
    "plan/recursive_cte_plan.cpp -> query/statement.hpp",
    "plan/recursive_cte_plan.hpp -> query/statement.hpp",
    # --- 構造上既知だが未対処のエッジ (潰したらこのリストから削る) -------
    # common から type/value_type.hpp (依存ゼロの列挙型ヘッダ) を使う。
    # Expanded from wildcard 'common/* -> type/value_type.hpp' (6 edges).
    # Exact edges only: a wildcard silently allows future regressions.
    "common/decoder.cpp -> type/value_type.hpp",
    "common/decoder.hpp -> type/value_type.hpp",
    "common/encoder.cpp -> type/value_type.hpp",
    "common/encoder.hpp -> type/value_type.hpp",
    "common/runtime_filter.hpp -> type/value_type.hpp",
    "common/serdes.hpp -> type/value_type.hpp",
    # type/column.hpp が page/row_position.hpp (定数のみ) を使う。
    "type/column.hpp -> page/row_position.hpp",
    # V2 後遺症: index_scan_iterator だけ table ターゲット所属の循環回避。
    # Expanded from wildcard 'index/index_scan_iterator.cpp -> table/*' (2 edges).
    # Exact edges only: a wildcard silently allows future regressions.
    "index/index_scan_iterator.cpp -> table/iterator_base.hpp",
    "index/index_scan_iterator.cpp -> table/table.hpp",
    # Expanded from wildcard 'index/index_scan_iterator.hpp -> table/*' (2 edges).
    # Exact edges only: a wildcard silently allows future regressions.
    "index/index_scan_iterator.hpp -> table/full_scan_iterator.hpp",
    "index/index_scan_iterator.hpp -> table/iterator_base.hpp",
    # ゾーンマップ/統計が述語式を直接評価する。
    # Expanded from wildcard 'table/* -> expression/*' (7 edges).
    # Exact edges only: a wildcard silently allows future regressions.
    "table/table_statistics.cpp -> expression/binary_expression.hpp",
    "table/table_statistics.cpp -> expression/column_value.hpp",
    "table/table_statistics.cpp -> expression/constant_value.hpp",
    "table/table_statistics.cpp -> expression/expression.hpp",
    "table/table_statistics.cpp -> expression/in_expression.hpp",
    "table/table_statistics.cpp -> expression/unary_expression.hpp",
    "table/table_statistics.hpp -> expression/expression.hpp",
    # bytecode が executor/data_chunk.hpp を要求 (S6/A1 で分離候補)。
    "expression/bytecode.hpp -> executor/data_chunk.hpp",
    "expression/bytecode.cpp -> executor/data_chunk.hpp",

    # relational(executor/detail) から executor 本体への上向き。A5 で解消予定。
    # Expanded from wildcard 'executor/detail/* -> executor/*' (45 edges).
    # Exact edges only: a wildcard silently allows future regressions.
    "executor/detail/build_input_joins.cpp -> executor/query_memory.hpp",
    "executor/detail/columnar_table.hpp -> executor/query_memory.hpp",
    "executor/detail/columnar_tlcs_fast_path.cpp -> executor/query_memory.hpp",
    "executor/detail/columnar_tlcs_internal.hpp -> executor/query_memory.hpp",
    "executor/detail/explain_format.cpp -> executor/hash_join_mode.hpp",
    "executor/detail/explain_format.cpp -> executor/query_memory.hpp",
    "executor/detail/fused_grouped_join.cpp -> executor/hash_join_mode.hpp",
    "executor/detail/fused_grouped_join.cpp -> executor/query_memory.hpp",
    "executor/detail/fused_grouped_join.cpp -> executor/spill_file.hpp",
    "executor/detail/ooc/chunk_codec.hpp -> executor/data_chunk.hpp",
    "executor/detail/ooc/external_sorter.hpp -> executor/data_chunk.hpp",
    "executor/detail/ooc/recursive_partitioner.hpp -> executor/data_chunk.hpp",
    "executor/detail/ooc/spillable_store.hpp -> executor/data_chunk.hpp",
    "executor/detail/parallel_table_scan.cpp -> executor/query_memory.hpp",
    "executor/detail/pipeline/ooc_join_cte_sources.cpp -> executor/data_chunk.hpp",
    "executor/detail/pipeline/ooc_setop_incsort_sink.cpp -> executor/data_chunk.hpp",
    "executor/detail/pipeline/ooc_sinks.hpp -> executor/grouped_aggregate.hpp",
    "executor/detail/pipeline/ooc_sinks.hpp -> executor/hash_join.hpp",
    "executor/detail/pipeline/ooc_sinks.hpp -> executor/join_kind.hpp",
    "executor/detail/pipeline/ooc_window_sink.cpp -> executor/data_chunk.hpp",
    "executor/detail/pipeline/pipeline_ir.hpp -> executor/data_chunk.hpp",
    "executor/detail/pipeline/pipeline_ir.hpp -> executor/executor_base.hpp",
    "executor/detail/pipeline/pipeline_ops.hpp -> executor/distinct.hpp",
    "executor/detail/pipeline/pipeline_ops.hpp -> executor/projection.hpp",
    "executor/detail/pipeline/pipeline_ops.hpp -> executor/selection.hpp",
    "executor/detail/planning_heuristics.cpp -> executor/hash_join_mode.hpp",
    "executor/detail/planning_heuristics.cpp -> executor/query_memory.hpp",
    "executor/detail/planning_heuristics.cpp -> executor/spill_file.hpp",
    "executor/detail/relation.cpp -> executor/query_memory.hpp",
    "executor/detail/relation.cpp -> executor/spill_file.hpp",
    "executor/detail/relation.hpp -> executor/query_memory.hpp",
    "executor/detail/relation.hpp -> executor/spill_file.hpp",
    "executor/detail/relational_sort_limit.cpp -> executor/query_memory.hpp",
    "executor/detail/relational_sort_limit.cpp -> executor/spill_file.hpp",
    "executor/detail/relational_sort_limit.hpp -> executor/query_memory.hpp",
    "executor/detail/relational_sort_limit.hpp -> executor/spill_file.hpp",
    "executor/detail/subquery_runtime.cpp -> executor/aggregation.hpp",
    "executor/detail/subquery_runtime.hpp -> executor/query_memory.hpp",
    "executor/detail/typed_column_eval.cpp -> executor/aggregation.hpp",
    "executor/detail/typed_stream_aggregate.cpp -> executor/aggregation.hpp",
    "executor/detail/typed_stream_aggregate.cpp -> executor/query_memory.hpp",
    "executor/detail/typed_stream_aggregate_internal.hpp -> executor/aggregation.hpp",
    "executor/detail/typed_stream_aggregate_internal.hpp -> executor/query_memory.hpp",
    "executor/detail/typed_stream_aggregate_partitioned.cpp -> executor/query_memory.hpp",
    "executor/detail/window_eval.cpp -> executor/query_memory.hpp",
    # optimizer は query_data を使う (CMake 上 tinylamb_executor 所属)。
    # Expanded from wildcard 'plan/* -> query/query_data.hpp' (6 edges).
    # Exact edges only: a wildcard silently allows future regressions.
    "plan/cascades.cpp -> query/query_data.hpp",
    "plan/cascades_search.cpp -> query/query_data.hpp",
    "plan/implementation_rules.cpp -> query/query_data.hpp",
    "plan/implementation_rules.hpp -> query/query_data.hpp",
    "plan/optimizer.cpp -> query/query_data.hpp",
    "plan/optimizer.hpp -> query/query_data.hpp",
    # サブクエリ脱相関 (tpch Phase2-4) のため optimizer が QueryExpression
    # 内の SelectStatement を解析する。sql 局のステートメント IR を読むだけ
    # で、実行は relational/subquery_runtime 側に留まる。
    "plan/optimizer.cpp -> query/statement.hpp",
    # GroupByFinish の EXPLAIN が保持しているステートメントの GROUP BY キー数
    # を報告するだけ (statement IR の読み取りのみ)。
    "plan/group_by_plan.cpp -> query/statement.hpp",
    # Union of exact edges across tree states (no wildcard).
)


def layer_of(rel_path):
    if rel_path not in PRODUCTION_FUZZERS and (
            TEST_PATTERN.search(rel_path) or rel_path == "main.cpp"):
        return "top"
    if rel_path.startswith(TOP_PREFIXES):
        return "top"
    best, best_len = None, -1
    for name, prefixes in LAYERS:
        for prefix in prefixes:
            if rel_path.startswith(prefix) and len(prefix) > best_len:
                best, best_len = name, len(prefix)
    return best  # None => outside the DAG (e.g. cli_main_test.cpp)


def source_files(root):
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if not path.is_file() or path.suffix not in (".hpp", ".cpp"):
            continue
        if rel.startswith(IGNORED_PREFIXES) or "/.git/" in rel:
            continue
        yield rel


def header_index(root):
    """Map basename -> list of repo-relative header paths (for bare includes)."""
    index = {}
    for path in sorted(root.rglob("*.hpp")):
        rel = path.relative_to(root).as_posix()
        if rel.startswith(IGNORED_PREFIXES) or "/.git/" in rel:
            continue
        index.setdefault(path.name, []).append(rel)
    return index


def resolve_include(src, include, root, index):
    """Resolve a quoted include to a repo-relative path.

    Prefixed includes ("plan/plan.hpp") are already repo-relative. Bare
    includes ("plan.hpp") are resolved by preferring the including file's own
    directory, then the unique basename match. Returns None when the include
    cannot be resolved unambiguously (caller skips it, as before).
    """
    if "/" in include:
        return include
    src_dir = src.rpartition("/")[0]
    if src_dir:
        sibling = f"{src_dir}/{include}"
        if (root / sibling).is_file():
            return sibling
    candidates = index.get(include, [])
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        layers = {layer_of(c) for c in candidates}
        if len(layers) == 1:
            same = [c for c in candidates if c.startswith(src_dir + "/")]
            return same[0] if len(same) == 1 else candidates[0]
        same = [c for c in candidates if c.startswith(src_dir + "/")]
        if len(same) == 1:
            return same[0]
    return None


def allowed(edge, extra):
    return any(fnmatch.fnmatchcase(edge, pattern) for pattern in chain(DEFAULT_ALLOWLIST, extra))


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
    index = header_index(Path(args.root))
    root = Path(args.root)
    for src in source_files(Path(args.root)):
        src_layer = layer_of(src)
        if src_layer is None or src_layer == "top":
            continue
        for include in INCLUDE_RE.findall(
                Path(args.root, src).read_text(encoding="utf-8", errors="replace")):
            resolved = resolve_include(src, include, root, index)
            if resolved is None:
                continue
            dst_layer = layer_of(resolved)
            if dst_layer is None or dst_layer == "top":
                continue
            if not is_violation(src, src_layer, resolved, dst_layer):
                continue
            violations.append((src, src_layer, resolved, dst_layer))

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
