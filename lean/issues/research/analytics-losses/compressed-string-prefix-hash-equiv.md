# 短固定長文字列の 64-bit Prefix+長ビットパック等価表現（Compressed String Prefix Hash Equivalence）

**未証明。** `page-123` や `year-1990` のような短 ASCII 文字列や固定長・最大 7 バイトの文字列キーでは、先頭 7 バイトをビッグエンディアン 56-bit へ 0 パディングし、最下位 8-bit に文字列長 `s.length` を格納した 64-bit 符号なし整数 `pack7(s)` を使うことで、ヒープ上の文字列比較・長さチェック・バイトコード評価なしに **単一 64-bit 整数として完全同値な等値比較・ハッシュ・辞書順比較が可能**（かつ長さタグを省いた `beWord7` では `[]` と `[0]` が衝突する）であることを検証する。

## 対象敗北クエリとボトルネック

- **BigBench B2** (`44.7 ms` [srv `40.2 ms`] vs DuckDB `10.6 ms`, `4.21x` 差): `dest_url = page_url`（`page-1`..`page-20000`）で毎回可変長文字列比較＋残差評価が走っている。
- **BigBench B1** (`29.0 ms` [srv `15.2 ms`] vs DuckDB `11.5 ms`, `2.51x` 差): `GROUP BY dest_url` の文字列ハッシュ構築とバケット比較。
- **STATS-CEB C4** (`103.5 ms` vs ClickHouse `4.4 ms`, `23.52x` 差): `'sql'`, `'tag-12'` 等の短文字列 `p.tags = t.tag_name`。
- **JOB J4** (`8.8 ms`) / **LDBC L1** (`81.9 ms`) / **LDBC L5** (`3.2 ms`): 低〜中カーディナリティ短文字列キー（`keyword`, `country` 等）。

## 提出型

[`CompressedStringPrefixHashEquiv.Goal`](../../../TinylambOpt/ResearchGoals/CompressedStringPrefixHashEquiv.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.CompressedStringPrefixHashEquiv.Goal` を提出する。

`s1.length ≤ 7 ∧ s2.length ≤ 7` のとき `(pack7 s1 = pack7 s2 ↔ s1 = s2) ∧ (pack7 s1 < pack7 s2 ↔ byteLexLt s1 s2 = true)` が成立すること、および `beWord7 [] = beWord7 [0]` 反例を示す。

## 境界例と適用範囲

- 末尾に `0x00` バイトを含むバイナリ列 `['a', 0]` と `['a']`、あるいは `[]` と `[0]` は上位 56-bit だけでは区別不能なため、**必ず長さ 8-bit（`+ s.length`）を最下位にパック**することが単射性・辞書順保存の必須条件。
- 8 バイト以上の文字列はプレフィックス（8 バイト以内）で高速棄却（early-reject）し、プレフィックス一致時のみ残余バイトを比較する二段階フィルタへ分離する。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (Line 2790 `integer_key` 判定、Line 2822 `pack_fused_2x32` 参照パターン)
- `executor/detail/typed_stream_aggregate.cpp` (グループキーのパック済み整数パス)
