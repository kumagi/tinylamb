# LLVM JIT target profile

2026-08-19に`tinylamb_expression_jit_benchmark`を実行した。対象はbytecode化後も反復回数の多いINT64比較filterで、LLVM ORC compileは約2.36msだった。

| 評価回数 | scalar | JIT kernel |
|---:|---:|---:|
| 1,310,720 | 0.69ms | 0.27ms |
| 5,242,880 | 2.76ms | 1.07ms |
| 20,971,520 | 11.09ms | 4.32ms |

コンパイル費込みの損益分岐は約2,097万評価だった。このため通常の短時間クエリはbytecodeのまま実行し、Selectionは累積2,000万行からJITへ昇格する。JIT対象はINT64 filter、線形projection、SUM aggregate kernelに限定し、複雑式・NULLを含むbatchはbytecodeへフォールバックする。

方針の詳細は [`expression_evaluation.md`](expression_evaluation.md) を参照（AST が意味論の正本、bytecode は batch 実行用 IR、JIT はその compiler）。

## 実装上の注意(2026-08-24 レビュー反映)

- 本稿は測定記録。昇格判定は静的ヒューリスティクス(プロファイラによる動的判定は未実装)。
- 適用対象と閾値: Selection/Projection/Aggregation 各エグゼキュータ単位。累積2000万行に加え「512K行×満幅バッチ」での早期昇格条件を追加(selection.cpp kJitEarlyPromotionRows)。
- filter JIT の形状: 単一列 vs INT64定数の二項比較(bytecode 3命令)のみ。列対列・複合式は対象外。
- SUM kernel は非並列 AggregationExecutor 専用(推定8192行以上は ParallelAggregation へ迂回し JIT 非対応)。
- フォールバック階層: JIT → bytecode → 式ツリー評価。JIT/bytecode以前にゾーンマップ pruning(BatchMayMatch, NULL count==0 判定)が効く。
- break_even 約2097万は測定グリッド最大点(LLVM ORC compile 約2.36ms、測定環境依存)。
