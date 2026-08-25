# tinylamb アーキテクチャ (レイヤー所有権マップ)

> 現在の改善バックログは [docs/next-actions.md](docs/next-actions.md) を参照。
> 設計判断の記録 (ADR) は [docs/adr/](docs/adr/) (`NNN-<topic>.md`, 001-010)。
> 新しい設計判断は最初から docs/adr/ に書く (旧 docs/review/ + adr-proposal の
> 二段階運用は 2026-08 に終了)。判断を覆すときは旧 ADR の Status を
> `superseded by NNN` に変えるだけで本文は書き換えない。

この文書は「今どこを触っているのか」「新しい変更はどの層に属するのか」の共通言語を
提供する。依存方向の強制は `scripts/check_layering.py` (include-lint) が行う。
根拠: `improvement3.md` §0/§2、`CMakeLists.txt` の `tinylamb_add_layer` 宣言。

## 1. レイヤー図 (宣言 DAG)

下に行くほど上位。**上位の層だけが下位を include してよい** (同一ランク内は自由)。

```
common                common/
  ↓
type                  type/            (Value / Row / Schema / Type)
  ↓
storage               page/ + recovery/(WAL) + transaction/
                      ※ CMake Layer 3 で単一バンド宣言 ("tightly coupled
                        storage foundation")。page→recovery→transaction への
                        分離は将来課題 (V5)
  ↓
index                 index/           (B+Tree, LSM)
  ↓
table                 table/           (Table, FullScanIterator, 統計)
  ↓
database              database/        (カタログ, Database, TransactionContext)
  ↓
expression            expression/      (式 AST, bytecode, JIT)
  ↓
relational            executor/detail/ (物理中間表現, scan filter, subquery,
                                        planning heuristics, explain)
                      ※ pseudo-layer: relational_detail 名前空間。plan と
                        executor の双方から使われる独立部品置き場 (A5 で昇格案)
  ↓
plan                  plan/            (Cascades memo, Plan ノード, rules)
  ↓
executor              executor/*.      (物理演算子)
  ↓
sql                   query/           (GoogleSQL frontend, SqlEngine,
                       parser/ 旧ヘッダ) statement IR 本体は query/statement.hpp
  ↓
server                server/          (postgres wire protocol)

[任意上位] main.cpp / benchmark/ / *_test* / *_fuzzer* / *_benchmark*
[対象外]   legacy/  (アーカイブ。legacy/parser/README.md 参照)
```

ビルドターゲットとの対応: `tinylamb_common` → `tinylamb_type` → `tinylamb_page`
(page+recovery+transaction) → `tinylamb_index` → `tinylamb_table` →
`tinylamb_database` → `tinylamb_expression` → `tinylamb_executor`
(plan + executor/detail + executor + `query/query_data.*`) → `tinylamb_sql` →
`tinylamb_postgres_server`。

## 2. 各層の代表ディレクトリ

| 層 | ディレクトリ | 代表ファイル | 主な責務 |
|---|---|---|---|
| common | `common/` | `status_or.hpp`, `log_message.hpp`, `vm_cache.hpp` | エラー型, ログ, 符号化, 仮想メモリキャッシュ |
| type | `type/` | `value.hpp`, `row.hpp`, `schema.hpp`, `date.hpp` | 値・行・スキーマの表現 |
| storage | `page/` `recovery/` `transaction/` | `page_pool.hpp`, `logger.hpp`(WAL), `transaction_manager.hpp` | バッファ管理, 先書きログ, 分離レベル |
| index | `index/` | `b_plus_tree.hpp`, `lsm_tree.hpp`, `index_schema.hpp` | 順次/LSM インデックス |
| table | `table/` | `table.hpp`, `full_scan_iterator.hpp`, `table_statistics.hpp` | 行データアクセスと統計 |
| database | `database/` | `database.hpp`, `catalog_test.cpp`, `transaction_context.hpp` | カタログ, DDL, トランザクション文脈 |
| expression | `expression/` | `expression.hpp`, `bytecode.hpp`, `jit.hpp` | 式評価 (bytecode 正本, docs/expression_evaluation.md) |
| relational | `executor/detail/` | `relation.hpp`, `scan_filter.hpp`, `subquery_runtime.hpp` | 物理中間表現と実行補助 |
| plan | `plan/` | `optimizer.hpp`, `cascades.hpp`, `plan.hpp` | 論理→物理計画, rule sets |
| executor | `executor/` | `hash_join.hpp`, `sort.hpp`, `data_chunk.hpp` | morsel 実行, パイプライン |
| sql | `query/` | `statement.hpp`, `sql_engine.hpp`, `googlesql_frontend.hpp` | SQL frontend と statement IR |
| server | `server/` | `postgres_server.hpp`, `postgres_protocol.hpp` | wire protocol |

## 3. 「この変更はどの層か?」判定表

| やりたい変更 | 所属層 | 触れてよい下位ヘッダ | 注意 |
|---|---|---|---|
| 新しい物理演算子 (join/agg 変種) | executor (+relational 部品) | expression, type, table, … | 対応する Plan ノードと implementation rule も必要 |
| join 順序・pushdown 等の計画判断 | plan | relational, expression, … | executor ヘッダ直参照は V4 (許可済みだが増やさない) |
| 式の意味論・演算子追加 | expression | type, … | **bytecode 経路と AST 直評価の両方**を更新 (differential test) |
| JIT 生成コードの変更 | expression (`jit.cpp`) | 同上 | bytecode を正本に揃えること |
| ページフォーマット変更 | storage(`page`) | type, common | docs/page_format.md 更新, magic+version 要検討 |
| WAL レコード/リカバリ手続き | storage(`recovery`) | page, type, common | docs/wal_format.md, docs/recovery_invariants.md |
| ロック/分離レベル | storage(`transaction`) | page, type, common | docs/lock_order.md のロック順序を守る |
| インデックス構造 (B+木/LSM) | index | storage, type, common | table⇄index 循環に注意 (V2) |
| スキャン API・統計 | table | index, storage, … | batch-first 原則 (docs/table_access.md 予定地) |
| カタログ/DDL | database | table 以下全部 | TransactionContext キャッシュ失効 (improvements2 §5.8) |
| SQL 構文・statement 形状 | sql (`query/statement.hpp`) | expression, type, … | executor からの参照が V3' の温床。増やさない |
| wire protocol / サーバ挙動 | server | sql 以下全部 | postgres streaming 応答の契約を維持 |
| ベンチ・テスト | [任意上位] | 何でも | 本表の制約を受けない |

迷ったら: **下位に置けるものは下位に置く**。新規ヘッダが既存上位層から
include される瞬間に `check_layering.py` が赤くなる。

## 4. include-lint: scripts/check_layering.py

全ソースの `#include "..."` を抽出し、上記 DAG に対する上向き依存を列挙する。

```console
$ python3 scripts/check_layering.py        # 違反があれば VIOLATION を列挙し exit 1
0 ... ; allowlisted=N                     # 全部許容済みなら exit 0
$ python3 scripts/check_layering.py --allowlist my_edges.txt
```

- `--root DIR`: リポジトリルート指定 (既定: スクリプト位置から自動判定)
- `--allowlist FILE`: 追加許容エッジ (`src -> include` 行, `#` コメント, fnmatch)
- 判定ルール: 上位→下位のみ OK。同一バンド (page/recovery/transaction) 内は自由
- 対象外: `legacy/` (アーカイブ), `build*`, テスト/ファズ/ベンチ
  (`*_test*`, `*_fuzzer*`, `_benchmark.`), `main.cpp`
- **運用**: 既知の負債エッジはスクリプト内 `DEFAULT_ALLOWLIST` にコメント付きで
  許容している。修正が完了したらその行を削る (潰すごとにリストが減る)。
  新規に赤くなったら、allowlist 追加ではなく層の配置を見直すのが原則

## 5. 現在許容している境界違反 (2026-08 時点, 計 77 辺)

improvement3.md の番号付き:

| 区分 | エッジ | 辺数 | 解消計画 |
|---|---|---|---|
| V1 | expression → database (`TransactionContext` 引け込み) | 3 | A1 EvaluationContext 抽象化 |
| V3' | relational/executor → statement IR (query/statement.hpp) | 11 | A2-2 IR 配置決定 (expression 隣接へ移動推奨) |
| V4 | plan → executor (Plan ノードが Executor 直接 new) | 30 | A5 relational ファクトリ経由化 |

無番号の許容エッジ (詳細はスクリプト内コメント参照): common→type/value_type (5),
type→page/row_position (1), index_scan_iterator→table (V2 後遺症, 3),
table→expression (統計の述語評価, 7), bytecode/pax_block→data_chunk (4),
relational(detail)→executor 本体 (9), plan→query/query_data.hpp (4)。
