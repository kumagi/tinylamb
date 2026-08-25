# 次に対処すべき指摘事項(設計バックログ)

2026-08-24 時点。各改善計画書(tpcc-improvements / tpch-sf10-plan / improvement3)の
実施可能項目は全て消化済み。ここには**意図的に先送りした設計判断・
大規模項目**だけを残す。完了履歴は git 履歴と BenchmarkHistory.md を参照。

## 2026-08-24 再監査で消化した項目

- **D-8: AST 直評価の release 廃止** はバックログから除外した。
  `docs/expression_evaluation.md` では AST (`Expression::Evaluate` /
  `EvaluateBinary`) が意味論の正本、bytecode はバッチ実行用 IR であり、
  AST fallback は意図した構成である。したがって AST 廃止は現行方針と矛盾する。
  実際に残っていた detail 経路の `CONCAT(..., NULL, ...)` の差は、
  AST と同じ strict NULL 伝播に統一し、differential test で固定した。
- **D-1: LockManager 完全 MVCC 統合** — production の行ロック依存を外し、
  version chain 上の write intent へ統合した。TPC-C 5.11.0 のisolation testに
  合わせ、競合writerは短時間待機後に先行commitの最新版を読んで継続する
  strict write locking とした。未stage intentは通常readerには不可視である。
- **D-2: Engine facade** — `SqlEngine::Execute` / `QueryResult` を導入し、CLI、
  pgwire、TPC-C/TPC-H benchmark の準備・stream・affected rows 処理を統一した。
- **D-3: TransactionContext 分解** — `CatalogReader` capability を抽出して
  `Database*` を除去し、executor の `thread_local active_runtime` を
  `TransactionContext` 経由の明示 runtime に置換した。
- **D-4: relational IR の Cascades memo 接続** — `kRelational` logical operator
  と `RelationalPlan` implementation rule を追加し、CTE/subquery/outer join 等の
  specialized relational IR も memo/search を経由して物理実装を選ぶ。
- **D-5: PAX ページ型の統合** — `PageType::kPaxPage`、page union、v1 directory、
  null bitmap、固定幅/VARCHAR の `DataChunk` 永続化を実装した。
- **D-7: serdes BE固定 + magic/version ヘッダ** — scalar codec、page、WAL、
  master checkpoint record を big-endian v1 に更新した。ユーザー承認により
  v0 dual reader は設けない破壊的 format bump とした。

## 継続して先送りする項目

| # | 項目 | 保留理由と再開条件 |
|---|---|---|
| D-6 | **分散実行**(`distributed*.md`) | Raft、replication、network transport、snapshot/install、障害試験が未実装の新サブシステムで、小規模リファクタではない。分散化をロードマップに戻したとき、最初のスライスで `LogStorage` interface と単一ノード動作テストを追加 |

意味論・公開API・disk formatを理由に保留していた項目は残っていない。
ユーザー判断により破壊的変更を許容してD-1〜D-5/D-7を消化済みで、継続保留は
分散基盤D-6だけである。

## 測定ゲート

性能系の効果検証は次のコマンドで行い、結果を BenchmarkHistory.md に記録:

- TPC-H: `tinylamb_tpch_benchmark`
- TPC-C: `tinylamb_tpcc_benchmark --clients 10 --seconds 60`
- ベンチ回帰ゲート: `scripts/bench_gate.py`(CI: .github/workflows/benchmark.yml 週次)
