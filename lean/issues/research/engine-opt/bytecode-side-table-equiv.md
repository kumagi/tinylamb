# Bytecode 命令列だけでなく側表・型を揃えた集約スロット重複排除

**未証明。** TPC-DS Q90 の再計測で実際に発見・修正した不具合に由来し、`BytecodeProgram` を集約スロット共通部分式除去（CSE）する際、命令列と定数プールだけでなく側表（`Int64Ranges`, `Reciprocals`, `StringSets`）と `ResultType` を含めて同値判定する必要があることを検証する。

## 提出型

[`BytecodeSideTableEquiv.Goal`](../../../TinylambOpt/ResearchGoals/BytecodeSideTableEquiv.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.BytecodeSideTableEquiv.Goal` を提出する。

全フィールド一致（`sameFullProgram`）でスロット重複排除した評価結果が、全集約式を独立評価した場合と任意のプログラム・行データに対して一致することを要求する。

## 境界例と適用範囲

TPC-DS Q90 型の `BETWEEN 8 AND 9` (`amc`) と `BETWEEN 19 AND 20` (`pmc`) はどちらも `[loadIntCol 0, inInt64Range 0]`・定数表 `[]` となり、`ranges` を無視する `badSameInstsConstsOnly` は両者を同じスロットに統合して誤った比率 `1.0` を返す。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (`same_child_program`)
- `executor/relational_regression_test_part4.cpp` (`FusedGroupedJoinDoesNotAliasDistinctSideTablePrograms`)
