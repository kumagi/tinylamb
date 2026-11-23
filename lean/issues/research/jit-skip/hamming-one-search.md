# 1バイト置換を許す検索を2本のシフトレジスタにする

**未証明。** リテラル長64以下に対し、バイト別の一致マスクを準備し、完全一致と置換1回以内の二状態を固定回数のビット演算で更新する。

## 提出型

[`HammingOneSearch.Goal`](../../../TinylambOpt/ResearchGoals/HammingOneSearch.lean) を含むモジュールを import し、別ファイルで `theorem sound : TinylambOpt.ResearchGoals.HammingOneSearch.Goal` を提出する。

全窓で不一致位置を数える参照検索と、重なりも含む開始位置列が一致すること。空リテラルは末尾を含む全境界に一致する。

## 境界例と適用範囲

`aaaba`から `aba` を検索する例では位置0と2が一致。1バイトなら任意の1バイト窓が通る。置換距離を使う将来の検索述語向けで、挿入・削除は扱わない。
