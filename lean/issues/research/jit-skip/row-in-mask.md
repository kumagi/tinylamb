# 複合 IN の候補64行を二つのマスクで評価する

**証明済み。** 証明は `lean/TinylambOpt/ResearchProofs/RowInMask.lean` の `theorem sound` にあります。 右表の各候補を1ビットに割り当て、列ごとに「すでに確実に一致する候補」と「まだ一致し得る候補」を絞る。

## 証明対象

[`RowInMask.Goal`](../../../TinylambOpt/ResearchGoals/RowInMask.lean) を含むモジュールを import し、提出済みの `theorem sound` は `ResearchProofs/RowInMask.lean` にあります。

任意の列数・入力・有効候補マスクで、行ごとの三値等価を OR した結果と一致する。同じ候補行に UNKNOWN と FALSE が並ぶとその行は FALSE。候補間では TRUE が UNKNOWN に優先する。

## 境界例と適用範囲

NULL 比較があっても別列の不一致で全候補が落ちる例、空の右表、ゼロ列のタプルを固定。1バッチが対象で、投影に使える三値を比較する。
