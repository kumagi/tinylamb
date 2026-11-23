# 高選択性整数プレフィルタ後の遅延スライス集約と `CASE` ガード付き二次列デコード

**未証明。** 複数の整数条件 `intColsPass` によって生存行数が極少数へ絞られた後段で文字列非空判定・文字列 `GROUP BY` を行う際、文字列本文を先に全行展開せず、(1) 選択ベクタ `sel` 上だけでオフセット差 `ends[r] - prevEnd(r) > 0` を検査し、(2) `CASE WHEN cond THEN ref ELSE ''` の偽分岐では二次文字列列 `ref` を 1 バイトもスライス／デコードせず生存バイトスライスのみでハッシュ集約できることを検証する。

## 対象敗北クエリとボトルネック

- **ClickBench Q37** (`25.72 ms` vs DuckDB `15.48 ms`)・**Q38** (`7.06 ms` vs `2.52 ms`, `2.81x` 差): `CounterID = 62 AND EventDate IN ... AND DontCountHits = 0 AND IsRefresh = 0` で 1 億行が数万〜数十万行へ絞られた後も、`URL` / `Title` 文字列列のハンドリング・スライス・辞書比較に余分なコストがかかっている。
- **ClickBench Q40** (`35.40 ms` vs DuckDB `24.24 ms`): フィルタ後 `722,688` 行中 `713,118` 行（98.7%）が `SearchEngineID = 0 AND AdvEngineID = 0` を通り、`ELSE ''` 側の参照回避や遅延スライスが重要になる。

## 提出型

[`SelectiveFilterLateSliceGroup.Goal`](../../../TinylambOpt/ResearchGoals/SelectiveFilterLateSliceGroup.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.SelectiveFilterLateSliceGroup.Goal` を提出する。

`ValidRowLengths` の下で `tr.groups = refFilterGroup rows checkNonEmptyStr`、ならびに `strSliceDecodes ≤ intMatched`・`refSliceDecodes ≤ strSliceDecodes`・真分岐皆無時 `refSliceDecodes = 0` を要求する。

## 境界例と適用範囲

- `strNonEmpty` と `strVal != ""` が一致している限り（ゼロ長 NULL/空文字境界）、`strVal` の文字デコード前に空文字行を弾ける。
- `useRef = false`（`CASE` 偽分岐）の行で `refVal` に一切触れなくてもグループキーは完全一致する。

## 実装との照合先

- `table/tlcs_file.cpp` (`DecodeBlockColumnsWithOptions`, `varchar_lengths_only`)
- `executor/detail/typed_stream_aggregate_dict.cpp` / `executor/detail/scan_filter.cpp`
