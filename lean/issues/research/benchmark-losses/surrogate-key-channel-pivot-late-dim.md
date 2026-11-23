# 単射代理キー整数空間でのチャネルピボット＋遅延ワイド属性参照

**未証明。** 複数ファクトチャネルを顧客・店舗等の次元テーブルと結合し、顧客 ID や氏名などのワイド文字列列群で `GROUP BY` して年度比較ピボットを取るクエリについて、代理キー `sk` が一意かつ業務キー `bk` が単射（`a.bk = b.bk → a.sk = b.sk`）であれば、巨大な文字列列を結合・グループキーに載せずに整数 `sk` 空間だけで集約・チャネル間 `y1 > 0` セミジョイン枝刈り・成長率比較を完了させ、最終勝者行にのみワイド文字列属性を遅延結合しても結果が完全一致することを検証する。

## 対象敗北クエリとボトルネック

- **TPC-DS Q04** (`288.00 ms` vs DuckDB `234.11 ms`, `+53.90 ms`)・**Q11** (`190.91 ms` vs `145.08 ms`, `+45.83 ms`)・**Q74** (`202.22 ms` vs `124.73 ms`, `+77.48 ms`): `customer` テーブル（150 万件）の氏名・国・メール・ログイン等のワイド文字列列をファクト集約の段階で展開・ハッシュ化しているため、3 チャネル×2 年度の集約・結合コストが肥大化している。
- **TPC-DS Q79** (`51.77 ms` vs DuckDB `40.12 ms`): `c_last_name, c_first_name` 等の結合・ソートが早期に行われている。

## 提出型

[`SurrogateKeyChannelPivotLateDim.Goal`](../../../TinylambOpt/ResearchGoals/SurrogateKeyChannelPivotLateDim.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.SurrogateKeyChannelPivotLateDim.Goal` を提出する。

`UniqueSk d ∧ InjectiveBk d` 下で `optSkPivotLateDim d fSmall fLarge = refPivot d fSmall fLarge` が成り立つことと、同一 `bk` に複数 `sk` が紐づく非単射次元 `dupBkDim` では整数 `sk` 集約が壊れる反例を要求する。

## 境界例と適用範囲

- 業務キーの一意性（`Unique` / `DISTINCT` または `ColumnSynopsis` 上のキー情報）がない次元（SCD Type 2 で同一 `customer_id` が複数 `sk` を持つ履歴テーブルなど）には適用しない（反例で検証済み）。
- `InjectiveBk` が保証されれば、最狭チャネルで `y1 > 0` を満たす `sk` 集合を先に構築して巨大チャネルの非対象 `sk` をハッシュ前から落とせる。

## 実装との照合先

- `plan/optimizer.cpp` (`LateMaterialization` / functional dependency projection pruning)
- `query/select_rewrites.cpp`
