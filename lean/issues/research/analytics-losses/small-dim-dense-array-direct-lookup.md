# 小整数次元キーのハッシュレス密配列直接索引結合（Small-Dimension Dense Array Direct Lookup）

**未証明。** 2 つの小次元表 `D1`, `D2` の結合キーが非 NULL かつ一意（PK）で狭い連続整数域 `[minK, minK + span)` に収まるスター結合 `F ⋈ D1 ⋈ D2` では、各次元のハッシュ表構築・多段プローブを廃止し、`span` 長の密配列 `arr1[k - min1]`, `arr2[k - min2]` へ次元属性（無効・filtered out は番兵）を格納してファクト表 `F` を 1 パス走査＋直接配列引きで集約する方式が、負数・範囲外・NULL 外部キーを含む全ファクトに対し完全同値であり、反対に次元側に重複キーがあると配列上書きにより壊れることを検証する。

## 対象敗北クエリとボトルネック

- **BigBench B3** (`3.7 ms` [srv `3.6 ms`] vs DuckDB `3.2 ms`, `1.14x` 差): `store(100行) ⋈ item(2,000行) ⋈ customer(6,000行) ⋈ store_sales(180,000行)` で 3 段の汎用ハッシュ結合・プローブ（`167,706` 比較）を実行。キー域が密な連番 `[1..N]` であるにもかかわらず直接添字配列に落ちていない。
- **LDBC-SNB L5** (`3.2 ms` vs DuckDB `2.7 ms`, `1.19x` 差): `place(200行) ⋈ person(8,000行) ⋈ person_knows(40,000行)`。
- **JOB J4** (`8.8 ms`) / **STATS-CEB C2** (`3.9 ms`) / **STATS-CEB C5** (`5.3 ms`): 小次元（`keyword 2,000`, `users 5,000`）との連続整数キー結合。

## 提出型

[`SmallDimDenseArrayDirectLookup.Goal`](../../../TinylambOpt/ResearchGoals/SmallDimDenseArrayDirectLookup.lean) を import し、`theorem sound : TinylambOpt.ResearchGoals.SmallDimDenseArrayDirectLookup.Goal` を提出する。

1. 一意＋値域仮定 `UniqueNonNullDimInRange` 下での `optStarDirect = refTwoHashJoins`
2. 重複キーを持つ次元 `dupDim1` で `optStarDirect ≠ refTwoHashJoins` となる具体反例

## 境界例と適用範囲

- 一意性チェック（たとえば TLCS シノプシス `is_identity_1_to_n` やハッシュ無し構築時の一意検証）を欠いたまま直接配列へ書き込むと、`dupDim1` 反例のように 2 件以上の次元行が同一スロットに潰れてカウント・和が過小評価（`1` vs `2`）になる。
- ファクト表の外部キーが `none`（NULL）、負数（`< minK`）、上限超え（`minK + span ≤ k`）のいずれであっても範囲チェックで安全に弾かれる。

## 実装との照合先

- `executor/detail/fused_grouped_join.cpp` (Line 23060 `pk=` / `use_dense_int_rows` — 現状 2 段以上同時直接引きが未発動)
- `executor/detail/build_input.cpp` (Lines 1358–1389 次元プリロード)
