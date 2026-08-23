# LLVM JIT target profile

2026-08-19に`tinylamb_expression_jit_benchmark`を実行した。対象はbytecode化後も反復回数の多いINT64比較filterで、LLVM ORC compileは約2.36msだった。

| 評価回数 | scalar | JIT kernel |
|---:|---:|---:|
| 1,310,720 | 0.69ms | 0.27ms |
| 5,242,880 | 2.76ms | 1.07ms |
| 20,971,520 | 11.09ms | 4.32ms |

コンパイル費込みの損益分岐は約2,097万評価だった。このため通常の短時間クエリはbytecodeのまま実行し、Selectionは累積2,000万行からJITへ昇格する。JIT対象はINT64 filter、線形projection、SUM aggregate kernelに限定し、複雑式・NULLを含むbatchはbytecodeへフォールバックする。

方針の詳細は [`expression_evaluation.md`](expression_evaluation.md) を参照（bytecode が batch IR の正本、JIT はその compiler）。
