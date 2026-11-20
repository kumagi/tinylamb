import TinylambOpt.AggBase

/-
# Step 4-B: Join 越しの多段集約（Eager / Split / Factorized Aggregation）の新ルール探索と証明

現行の `plan/cascades.cpp` には `eager_aggregation_over_join` が登録されています。本稿は、この実装が引用する事前集約の成立条件と、その拡張を扱います。

Join 越しの多段集約は OLAP / Star Schema で数桁の高速化をもたらす一方、
SQL の **NULL・空集合・重複キー（M:N fan-out）・外部結合の NULL パディング** が絡むため、
素朴に実装すると非常に深刻な Wrong-Result バグを踏みやすい領域でもあります。

このファイルでは Lean 4 を使って **4 つのアグレッシブな多段集約ルール候補** を探索し、
「素朴な実装が踏むバグ（反例定理）」と「100% 安全になる正確な変換式（正当性定理）」の両方を機械証明します。
-/

namespace TinylambOpt

/-!
================================================================================
## 候補ルール 1: 非 Unique（M:N）内部結合での片側事前集約 + `NULL` キー早期除去
================================================================================

### 発見のポイント
「対向テーブル `R` の結合キーが `UNIQUE / PK` でないと `Aggregate` を Join の下に押し込めない」と誤解されがちですが、
それは **2 段目の再集約を省略する 1 段押し込み（Complete Transpose）** の場合だけです。
**Partial Agg → Join → Final Agg の 2 段集約** に分割すれば、有限 Fubini の定理（`aggFold_comm`）により、
**対向側 `R` に一切の Unique 制約が無い M:N 結合であっても、`SUM / COUNT(*) / COUNT(col) / MIN / MAX` すべてで完全成立**します！
さらに `ON L.k = R.k` で必ず落ちる `L.k IS NULL` 行は、事前集約の**前**に捨てられます。
-/

/-- **罠 1-A（2 段目の再集約を省く 1 段押し込みが壊れる反例）**:
    `GROUP BY d.region` のように結合キー `dept_id` より粗い軸で集約する場合、
    `L` を `dept_id` で事前集約しただけで上段の再集約を省くと、同じ `region` が複数行に分かれたままになり壊れる。 -/
theorem complete_transpose_fails_when_coarser_group :
    let l : Rel (Int × Int) := [(10, 100), (20, 200)]
    let r : Rel (Int × Int) := [(10, 1), (20, 1)]
    let onPred := fun (x : Int × Int) (y : Int × Int) => sqlEq (some x.1) (some y.1)
    let orig := groupBy
      (fun lr => match lr.2 with | some d => d.2 | none => -1)
      (fun _ bucket => sqlSum (fun lr => some lr.1.2) bucket)
      (innerJoin onPred l r)
    let preL := groupBy Prod.fst (fun _ b => sqlSum (fun x => some x.2) b) l
    let naiveNoReagg := (innerJoin (fun p y => sqlEq (some p.1) (some y.1)) preL r).map
      (fun pr => (match pr.2 with | some d => d.2 | none => -1, pr.1.2))
    orig.length ≠ naiveNoReagg.length := by
  decide

/-- **罠 1-B（スカラー `COUNT(*)` の 2 段目を単純な `SUM` にすると空結合で `NULL` に化ける有名バグ！）**:
    `GROUP BY` の無いスカラー集約 `SELECT COUNT(*) FROM L JOIN R ...` を
    `SELECT SUM(partial_cnt) FROM (SELECT k, COUNT(*) AS partial_cnt FROM L GROUP BY k) JOIN R ...`
    に書き換えると、Join 結果が 0 行のとき本来の `0` ではなく `NULL`（`none`）が返ってしまう！ -/
theorem scalar_count_reagg_null_bug :
    let l : Rel Int := [1]
    let r : Rel Int := [2]
    let onPred := fun x y => sqlEq (some x) (some y)
    let origCount : SqlVal Int := some (sqlCountStar (innerJoin onPred l r))
    let partialL := groupBy id (fun _ b => sqlCountStar b) l
    let buggyReaggSum : SqlVal Int :=
      sqlSum (fun pr => some pr.1.2) (innerJoin (fun p y => sqlEq (some p.1) (some y)) partialL r)
    origCount ≠ buggyReaggSum := by
  decide

/-- **罠 1-B の修復定理**: `COALESCE(SUM(partial_cnt), 0)` にすれば、空結合でも非空結合でも `countMonoid` と完全一致する！ -/
theorem coalesce_sqlSum_eq_countFold {Row : Type} (f : Row → Int) (r : Rel Row) :
    sqlCoalesce (sqlSum (fun x => some (f x)) r) 0 = aggFold countMonoid f r := by
  suffices h : ∀ xs : Rel Row,
      sqlSum (fun x => some (f x)) xs = none ∧ aggFold countMonoid f xs = 0 ∨
      sqlSum (fun x => some (f x)) xs = some (aggFold countMonoid f xs) by
    rcases h r with ⟨hNone, hZero⟩ | hSome
    · simp [sqlCoalesce, hNone, hZero]
    · simp [sqlCoalesce, hSome]
  intro xs
  induction xs with
  | nil => left; exact ⟨rfl, rfl⟩
  | cons x xs ih =>
    right
    rcases ih with ⟨hNone, hZero⟩ | hSome
    · rw [sqlSum_cons, hNone, aggFold_cons, hZero]
      simp [mergeSum, countMonoid]
    · rw [sqlSum_cons, hSome, aggFold_cons]
      simp [mergeSum, countMonoid]

/-- **`NULL` 結合キー早期刈り込み補題**:
    等値内部結合 `ON lk(x) = rk(y)` の前に、左テーブルから `lk(x) IS NULL` の行を除去しても結果は完全一致する。 -/
theorem innerJoin_filter_left_isSome {L R K : Type} [DecidableEq K]
    (lk : L → SqlVal K) (rk : R → SqlVal K) (l : Rel L) (r : Rel R) :
    innerJoin (fun x y => sqlEq (lk x) (rk y)) l r =
    innerJoin (fun x y => sqlEq (lk x) (rk y)) (l.filter (fun x => (lk x).isSome)) r := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    cases hlk : lk x with
    | none =>
      have hEmpty : r.filter (fun y => (sqlEq none (rk y)).inWhere) = [] := by
        rw [List.filter_eq_nil_iff]
        intro y _; simp [sqlEq, SqlBool.inWhere]
      simp [innerJoin, List.filter, hlk, hEmpty] at ih ⊢
      exact ih
    | some _ =>
      simp [innerJoin, List.filter, hlk] at ih ⊢
      exact ih

/-- 内部ヘルパ: バケット内の全要素 `x` で結合相手リストが共通の `matches_` に定まるとき、`flatMap` を固定リスト化できる。 -/
theorem flatMap_const_matches {L R Out : Type}
    (f : L → Rel R) (matches_ : Rel R) (mkOut : L → R → Out) :
    ∀ (bucket : Rel L), (∀ x ∈ bucket, f x = matches_) →
      bucket.flatMap (fun x => (f x).map (mkOut x)) =
      bucket.flatMap (fun x => matches_.map (mkOut x))
  | [], _ => rfl
  | b :: bs, h => by
    have hb := h b (List.mem_cons_self ..)
    have hbs := flatMap_const_matches f matches_ mkOut bs (fun z hz => h z (List.mem_cons_of_mem b hz))
    simp [List.flatMap_cons, hb, hbs]

/-- **片側事前集約の抽象コア補題 (`general_join_preagg_fold`)**:
    結合条件 `pred : PK → R → Bool` が左行 `x` に事前集約キー `pkFn x` を通じてのみ依存するなら、
    `l` を `pkFn` ごとに事前集約してから結合・再集約した値は元の値と完全一致する！ -/
private theorem general_join_preagg_fold {L R PK M : Type} [DecidableEq PK]
    (m : AggMonoid M) (pkFn : L → PK) (pred : PK → R → Bool)
    (measureL : L → M) (l : Rel L) (r : Rel R) :
    aggFold m (fun xy : L × R => measureL xy.1)
      (l.flatMap (fun x => (r.filter (pred (pkFn x))).map (fun y => (x, y)))) =
    aggFold m (fun py : (PK × M) × R => py.1.2)
      ((groupBy pkFn (fun _ b => aggFold m measureL b) l).flatMap
        (fun p => (r.filter (pred p.1)).map (fun y => (p, y)))) := by
  have hPerm := perm_distinctKey_buckets pkFn l
  rw [aggFold_flatMap, aggFold_perm m _ hPerm, aggFold_flatMap]
  unfold groupBy
  rw [aggFold_flatMap]
  clear hPerm
  induction (distinctKeys (l.map pkFn)) with
  | nil => rfl
  | cons pk pks ih =>
    simp only [List.map_cons, aggFold_cons, ih]
    congr 1
    rw [← aggFold_flatMap]
    have hSame := flatMap_const_matches
      (fun x => r.filter (pred (pkFn x)))
      (r.filter (pred pk))
      Prod.mk
      (l.filter (fun row => pkFn row == pk))
      (by
        intro x hx
        rw [List.mem_filter, beq_iff_eq] at hx
        rw [hx.2])
    rw [hSame, aggFold_flatMap]
    have hInner :
        (fun x : L => aggFold m (fun xy : L × R => measureL xy.1)
          ((r.filter (pred pk)).map (fun y => (x, y)))) =
        (fun x : L => aggFold m (fun _ => measureL x) (r.filter (pred pk))) := by
      funext x; rw [aggFold_map]
    rw [hInner, aggFold_comm m (fun x _ => measureL x), aggFold_map]

/-- **主定理 1-1 (`eager_scalar_agg_over_mn_join_sound`)**:
    `R` 側に **一切の Unique 制約が無い M:N 結合** であっても、任意の可換モノイド集約 `m`
    （`SUM`, `COUNT(*)`, `COUNT(col)`, `MIN`, `MAX`）について：
    `L` から `lk(x) IS NULL` を事前除去し、`lk(x)` ごとに事前集約 `partialL` を計算してから
    `R` と結合・最終マージする 2 段集約は、元クエリと **100% 完全一致** する！ -/
theorem eager_scalar_agg_over_mn_join_sound {L R K M : Type} [DecidableEq K]
    (m : AggMonoid M)
    (lk : L → SqlVal K) (rk : R → SqlVal K)
    (measureL : L → M)
    (l : Rel L) (r : Rel R) :
    let onOrig := fun x y => sqlEq (lk x) (rk y)
    let lClean := l.filter (fun x => (lk x).isSome)
    let partialL := groupBy lk (fun _ bucket => aggFold m measureL bucket) lClean
    let onPart := fun (p : SqlVal K × M) (y : R) => sqlEq p.1 (rk y)
    aggFold m (fun lr => measureL lr.1) (innerJoin onOrig l r) =
    aggFold m (fun pr => pr.1.2) (innerJoin onPart partialL r) := by
  intro onOrig lClean partialL onPart
  rw [innerJoin_filter_left_isSome lk rk l r]
  have hCore := general_join_preagg_fold m lk (fun k y => (sqlEq k (rk y)).inWhere) measureL lClean r
  have hMapL : ∀ xs : Rel L,
      aggFold m (fun lr : L × Option R => measureL lr.1) (innerJoin onOrig xs r) =
      aggFold m (fun xy : L × R => measureL xy.1)
        (xs.flatMap (fun x => (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y)))) := by
    intro xs
    unfold innerJoin
    rw [aggFold_flatMap, aggFold_flatMap]
    congr 1; funext x; rw [aggFold_map, aggFold_map]
  have hMapP :
      aggFold m (fun pr : (SqlVal K × M) × Option R => pr.1.2) (innerJoin onPart partialL r) =
      aggFold m (fun py : (SqlVal K × M) × R => py.1.2)
        (partialL.flatMap (fun p => (r.filter (fun y => (sqlEq p.1 (rk y)).inWhere)).map (fun y => (p, y)))) := by
    unfold innerJoin
    rw [aggFold_flatMap, aggFold_flatMap]
    congr 1; funext p; rw [aggFold_map, aggFold_map]
  rw [hMapL lClean, hMapP]
  exact hCore

/-- 等値内部結合のペア表現 `equiJoinPairs`（多段 Join カスケードでもそのまま連鎖できる形）。 -/
def equiJoinPairs {L R K : Type} [DecidableEq K]
    (lk : L → SqlVal K) (rk : R → SqlVal K) (l : Rel L) (r : Rel R) : Rel (L × R) :=
  l.flatMap fun x => (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y))

theorem equiJoinPairs_filter_left_isSome {L R K : Type} [DecidableEq K]
    (lk : L → SqlVal K) (rk : R → SqlVal K) (l : Rel L) (r : Rel R) :
    equiJoinPairs lk rk l r = equiJoinPairs lk rk (l.filter (fun x => (lk x).isSome)) r := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    cases hlk : lk x with
    | none =>
      have hNil : r.filter (fun y => (sqlEq none (rk y)).inWhere) = [] := by
        rw [List.filter_eq_nil_iff]; intro y _; simp [sqlEq, SqlBool.inWhere]
      simp [equiJoinPairs, List.filter, hlk, hNil] at ih ⊢
      exact ih
    | some _ =>
      simp [equiJoinPairs, List.filter, hlk] at ih ⊢
      exact ih

private theorem filter_equiJoinPairs_eq {A R K : Type} [DecidableEq K]
    (lk : A → SqlVal K) (rk : R → SqlVal K) (q : A → R → Bool)
    (xs : Rel A) (r : Rel R) :
    (equiJoinPairs lk rk xs r).filter (fun xy => q xy.1 xy.2) =
    xs.flatMap (fun x => (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere && q x y)).map (fun y => (x, y))) := by
  unfold equiJoinPairs
  rw [List.filter_flatMap]
  congr 1; funext x
  induction r with
  | nil => rfl
  | cons y ys ih =>
    cases hm : (sqlEq (lk x) (rk y)).inWhere <;>
      cases hq : q x y <;>
      simp [List.filter, hm, hq, ih]

theorem exists_mem_groupBy {Row K V : Type} [DecidableEq K]
    (key : Row → K) (fn : K → Rel Row → V) (r : Rel Row) {x : Row} (hx : x ∈ r) :
    ∃ v, (key x, v) ∈ groupBy key fn r := by
  unfold groupBy
  refine ⟨_, List.mem_map_of_mem ?_⟩
  rw [mem_distinctKeys]
  exact List.mem_map_of_mem hx

/-- **主定理 1-2 (`eager_group_agg_over_mn_join_sound`)**:
    `SELECT gFinal(gL(L), R), AGG(L.v) FROM L JOIN R ON L.k = R.k GROUP BY gFinal(gL(L), R)`
    に対し、`R` に **Unique 制約が無くても**：
    `L` を `L.k IS NOT NULL` で絞って `(gL(L), L.k)` で事前集約 → `R` と結合 → `gFinal` で再集約
    した結果は、元クエリと **Bag 等価 (`BagEq`)** である（幻影グループも欠落グループもゼロ）！ -/
theorem eager_group_agg_over_mn_join_sound {L R K GL G M : Type}
    [DecidableEq K] [DecidableEq GL] [DecidableEq G]
    (m : AggMonoid M)
    (lk : L → SqlVal K) (rk : R → SqlVal K)
    (gL : L → GL) (gFinal : GL → R → G)
    (measureL : L → M)
    (l : Rel L) (r : Rel R) :
    let lClean := l.filter (fun x => (lk x).isSome)
    let partialL := groupBy (fun x => (gL x, lk x)) (fun _ b => aggFold m measureL b) lClean
    BagEq
      (groupBy (fun xy => gFinal (gL xy.1) xy.2)
               (fun _ b => aggFold m (fun xy => measureL xy.1) b)
               (equiJoinPairs lk rk l r))
      (groupBy (fun py => gFinal py.1.1.1 py.2)
               (fun _ b => aggFold m (fun py => py.1.2) b)
               (equiJoinPairs (fun p => p.1.2) rk partialL r)) := by
  intro lClean partialL
  have hCleanEq : equiJoinPairs lk rk l r = equiJoinPairs lk rk lClean r :=
    equiJoinPairs_filter_left_isSome lk rk l r
  rw [hCleanEq]
  apply groupBy_bagEq_of_keys_and_vals
  · -- (1) グループキーの存在同値（0 行グループの混入・消失が起きないこと）
    intro g
    constructor
    · rintro ⟨⟨x, y⟩, hMem, hKey⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter] at hMem
      rcases hMem with ⟨x', hxClean, y', ⟨hy', hMatch⟩, hPairEq⟩
      cases hPairEq
      rcases exists_mem_groupBy (fun x => (gL x, lk x)) (fun _ b => aggFold m measureL b) lClean hxClean
        with ⟨v, hpMem⟩
      refine ⟨(((gL x, lk x), v), y), ?_, hKey⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨_, hpMem, y, ⟨hy', hMatch⟩, rfl⟩
    · rintro ⟨⟨pRow, y⟩, hMem, hKey⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter] at hMem
      rcases hMem with ⟨p', hp', y', ⟨hy', hMatch⟩, hPairEq⟩
      cases hPairEq
      unfold partialL groupBy at hp'
      rw [List.mem_map] at hp'
      rcases hp' with ⟨⟨gl, k⟩, hkMem, hpEq⟩
      rw [mem_distinctKeys, List.mem_map] at hkMem
      rcases hkMem with ⟨x, hxClean, hpkEq⟩
      cases hpEq
      cases hpkEq
      refine ⟨(x, y), ?_, hKey⟩
      simp only [equiJoinPairs, List.mem_flatMap, List.mem_map, List.mem_filter]
      exact ⟨x, hxClean, y, ⟨hy', hMatch⟩, rfl⟩
  · -- (2) 各グループ `g` の集約値一致: `filter_equiJoinPairs_eq` で `general_join_preagg_fold` に一発帰着！
    intro g _
    rw [filter_equiJoinPairs_eq lk rk (fun x y => gFinal (gL x) y == g) lClean r,
        filter_equiJoinPairs_eq (fun p : (GL × SqlVal K) × M => p.1.2) rk
          (fun p y => gFinal p.1.1 y == g) partialL r]
    exact general_join_preagg_fold m
      (fun x => (gL x, lk x))
      (fun pk y => (sqlEq pk.2 (rk y)).inWhere && (gFinal pk.1 y == g))
      measureL lClean r

/-!
================================================================================
## 候補ルール 2: M:N 結合の「両側同時」事前集約 + テーブル横断式 (`SUM(L.x * R.y)` / `SUM(L.x + R.y)`)
================================================================================
-/

/-- `sqlMul` は `mergeSum` に対して完全分配する（`none` = NULL の吸収まで含めて全 8 ケースで成立！）。 -/
theorem sqlMul_mergeSum_left (a b c : SqlVal Int) :
    sqlMul a (mergeSum b c) = mergeSum (sqlMul a b) (sqlMul a c) := by
  cases a <;> cases b <;> cases c <;> simp [sqlMul, mergeSum, Int.mul_add]

theorem sqlMul_mergeSum_right (a b c : SqlVal Int) :
    sqlMul (mergeSum a b) c = mergeSum (sqlMul a c) (sqlMul b c) := by
  cases a <;> cases b <;> cases c <;> simp [sqlMul, mergeSum, Int.add_mul]

private theorem sqlSum_append {Row : Type} (f : Row → SqlVal Int) (r1 r2 : Rel Row) :
    sqlSum f (r1 ++ r2) = mergeSum (sqlSum f r1) (sqlSum f r2) :=
  aggFold_append sumMonoid f r1 r2

private theorem sqlSum_map {A B : Type} (f : B → SqlVal Int) (g : A → B) (xs : Rel A) :
    sqlSum f (xs.map g) = sqlSum (fun x => f (g x)) xs :=
  aggFold_map sumMonoid f g xs

private theorem sqlMul_sqlSum_right {B : Type} (a : SqlVal Int) (fy : B → SqlVal Int) (ys : Rel B) :
    sqlMul a (sqlSum fy ys) = sqlSum (fun y => sqlMul a (fy y)) ys := by
  induction ys with
  | nil => cases a <;> rfl
  | cons y ys ih =>
    simp only [sqlSum_cons, sqlMul_mergeSum_left, ih]

/-- **主定理 2-1 (`sqlSum_cartesian_mul_sound` — 二重テーブル積 `SUM(L.x * R.y)` の独立事前集約)**:
    同じ結合キーバケット内の直積 `xs × ys` 上で `SUM(fx(x) * fy(y))` を計算した結果は、
    **左だけで `SUM(fx)`、右だけで `SUM(fy)` を独立に事前集約してから 1 回掛け算した値 `sqlMul (sqlSum fx xs) (sqlSum fy ys)`**
    と、NULL 行や空リストが混在していても **無条件で 100% 一致** する！ -/
theorem sqlSum_cartesian_mul_sound {A B : Type}
    (fx : A → SqlVal Int) (fy : B → SqlVal Int)
    (xs : Rel A) (ys : Rel B) :
    sqlSum (fun xy : A × B => sqlMul (fx xy.1) (fy xy.2))
      (xs.flatMap (fun x => ys.map (fun y => (x, y)))) =
    sqlMul (sqlSum fx xs) (sqlSum fy ys) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    rw [List.flatMap_cons, sqlSum_append, sqlSum_map, ih, sqlSum_cons,
        sqlMul_mergeSum_right]
    dsimp only
    rw [← sqlMul_sqlSum_right (fx x)]

/-- **罠 2-A（和の集約 `SUM(L.x + R.y)` を `COUNT(*)` で重み付けすると NULL で狂うバグ！）**:
    `SUM(L.x + R.y)` を両側事前集約するとき、うっかり `SUM(L.x) * COUNT(*)_R + SUM(R.y) * COUNT(*)_L` と書くと、
    `R` に `y = NULL` の行が 1 行混ざっているだけで（本来 `10 + NULL = NULL` で無視されるべきなのに）
    `10` が余計に加算されて計算結果が狂う！ -/
theorem sum_add_naive_cnt_star_bug :
    let xs : Rel (SqlVal Int) := [some 10]
    let ys : Rel (SqlVal Int) := [some 1, none]
    let trueSum := sqlSum (fun xy => sqlAdd xy.1 xy.2)
      (xs.flatMap (fun x => ys.map (fun y => (x, y))))
    let buggyFormula := sqlAdd
      (sqlMul (sqlSum id xs) (some (sqlCountStar ys)))
      (sqlMul (sqlSum id ys) (some (sqlCountStar xs)))
    trueSum ≠ buggyFormula := by
  decide

private theorem sqlSum_add_const_left {B : Type} (a : Int) (fy : B → SqlVal Int) (ys : Rel B) :
    sqlSum (fun y => sqlAdd (some a) (fy y)) ys =
    sqlAdd (sqlMul (some a) (some (sqlCountCol fy ys))) (sqlSum fy ys) := by
  induction ys with
  | nil => rfl
  | cons y ys ih =>
    cases hy : fy y with
    | none =>
      simp only [sqlSum_cons, sqlCountCol_cons, hy, Option.isSome, Bool.false_eq_true,
                 ↓reduceIte, Int.zero_add, sqlAdd, mergeSum]
      exact ih
    | some b =>
      simp only [sqlSum_cons, sqlCountCol_cons, hy, Option.isSome, ↓reduceIte]
      rw [ih]
      cases hSumYs : sqlSum fy ys with
      | none =>
        have hZero := sqlCountCol_eq_zero_of_sqlSum_none fy ys hSumYs
        simp [hZero, mergeSum, sqlAdd, sqlMul]
      | some s =>
        simp only [mergeSum, sqlAdd, sqlMul]
        congr 1
        have hDist := Int.mul_add a 1 (sqlCountCol fy ys)
        omega

/-- **主定理 2-2 (`sqlSum_cartesian_add_sound` — 二重テーブル和 `SUM(L.x + R.y)` の正確な事前集約公式)**:
    `COUNT(*)` ではなく **非 NULL 件数 `sqlCountCol`** を掛けて足す式：
      `SUM(L.x) * COUNT(R.y) + SUM(R.y) * COUNT(L.x)`
    は、片側が全行 `NULL` のとき `sqlAdd _ none = none` によって自動的に `NULL` へ落ちるため、
    `CASE WHEN` 分岐すら無しで **あらゆる NULL 混在・空リストに対して `SUM(L.x + R.y)` と完全一致** する！ -/
theorem sqlSum_cartesian_add_sound {A B : Type}
    (fx : A → SqlVal Int) (fy : B → SqlVal Int)
    (xs : Rel A) (ys : Rel B) :
    sqlSum (fun xy : A × B => sqlAdd (fx xy.1) (fy xy.2))
      (xs.flatMap (fun x => ys.map (fun y => (x, y)))) =
    sqlAdd
      (sqlMul (sqlSum fx xs) (some (sqlCountCol fy ys)))
      (sqlMul (sqlSum fy ys) (some (sqlCountCol fx xs))) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    rw [List.flatMap_cons, sqlSum_append, sqlSum_map, ih]
    change mergeSum (sqlSum (fun y => sqlAdd (fx x) (fy y)) ys) _ = _
    cases hx : fx x with
    | none =>
      have hNoneYs : sqlSum (fun y => sqlAdd none (fy y)) ys = none :=
        aggFold_const_zero sumMonoid ys
      simp only [sqlSum_cons, sqlCountCol_cons, hx, hNoneYs, Option.isSome,
                 Bool.false_eq_true, ↓reduceIte, Int.zero_add, mergeSum]
    | some a =>
      rw [sqlSum_add_const_left a fy ys]
      simp only [sqlSum_cons, sqlCountCol_cons, hx, Option.isSome, ↓reduceIte]
      cases hSumY : sqlSum fy ys with
      | none =>
        simp [mergeSum, sqlAdd, sqlMul]
      | some sy =>
        cases hSumX : sqlSum fx xs with
        | none =>
          have hZero := sqlCountCol_eq_zero_of_sqlSum_none fx xs hSumX
          simp [hZero, mergeSum, sqlAdd, sqlMul]
        | some sx =>
          simp only [mergeSum, sqlAdd, sqlMul]
          congr 1
          have h1 := Int.add_mul a sx (sqlCountCol fy ys)
          have h2 := Int.mul_add sy 1 (sqlCountCol fx xs)
          omega

private theorem equiJoinPairs_perm_left {L R K : Type} [DecidableEq K]
    (lk : L → SqlVal K) (rk : R → SqlVal K) {l1 l2 : Rel L} (r : Rel R)
    (h : BagEq l1 l2) :
    BagEq (equiJoinPairs lk rk l1 r) (equiJoinPairs lk rk l2 r) := by
  unfold equiJoinPairs
  induction h with
  | nil => exact List.Perm.nil
  | cons x _ ih => simp only [List.flatMap_cons]; exact ih.append_left _
  | swap x y xs =>
    simp only [List.flatMap_cons]
    rw [← List.append_assoc, ← List.append_assoc]
    exact (List.perm_append_comm).append_right _
  | trans _ _ ih1 ih2 => exact ih1.trans ih2

theorem filter_nodup_keys_eq_some {K V : Type} [DecidableEq K]
    (evalKey : SqlVal K → V) (kv : K) :
    ∀ (ks : List (SqlVal K)), ks.Nodup →
      (ks.map (fun k => (k, evalKey k))).filter (fun p => (sqlEq (some kv) p.1).inWhere) =
      if some kv ∈ ks then [(some kv, evalKey (some kv))] else []
  | [], _ => rfl
  | k :: ks, hNodup => by
    rw [List.nodup_cons] at hNodup
    obtain ⟨hkNotMem, hNodupKs⟩ := hNodup
    have ih := filter_nodup_keys_eq_some evalKey kv ks hNodupKs
    by_cases hEq : k = some kv
    · subst hEq
      have hRest : (ks.map (fun k' => (k', evalKey k'))).filter
          (fun p => (sqlEq (some kv) p.1).inWhere) = [] := by
        rw [ih]; simp [hkNotMem]
      have hHead : (sqlEq (some kv) (some kv)).inWhere = true := by
        simp [sqlEq, SqlBool.inWhere]
      simp only [List.map, List.filter, hHead, hRest, List.mem_cons, true_or, ↓reduceIte]
    · have hFalse : (sqlEq (some kv) k).inWhere = false := by
        cases k with
        | none => rfl
        | some kv' =>
          have hne : kv ≠ kv' := fun h => hEq (h ▸ rfl)
          simp [sqlEq, hne, SqlBool.inWhere]
      have hMemIff : (some kv ∈ k :: ks) ↔ (some kv ∈ ks) := by
        simp [Ne.symm hEq]
      simp only [List.map, List.filter, hFalse, ih, hMemIff]

/-- **主定理 2-3 (`dual_eager_cross_expr_over_mn_join_sound` — M:N Join 両側同時事前集約の全体定理)**:
    重複キーだらけの M:N 内部結合 `equiJoinPairs lk rk l r` の上で
    テーブル横断式 `SUM(lx(L) * ry(R))` および `SUM(lx(L) + ry(R))` を求めるクエリは：
    1. 左 `L`（`lk IS NOT NULL`）を `lk` で単独事前集約 `(sumL, cntL)`
    2. 右 `R`（`rk IS NOT NULL`）も `rk` で単独事前集約 `(sumR, cntR)`
    3. 圧縮された 2 つのサマリ表同士を 1 対 1 等値結合し、
       - 積は `sqlMul sumL sumR`
       - 和は `sqlAdd (sqlMul sumL (some cntR)) (sqlMul sumR (some cntL))`
       を再集約 `sqlSum` した結果と **全データ・全 NULL パターンで 100% 完全一致** する！ -/
theorem dual_eager_cross_expr_over_mn_join_sound {L R K : Type} [DecidableEq K]
    (lk : L → SqlVal K) (rk : R → SqlVal K)
    (lx : L → SqlVal Int) (ry : R → SqlVal Int)
    (l : Rel L) (r : Rel R) :
    let lClean := l.filter (fun x => (lk x).isSome)
    let rClean := r.filter (fun y => (rk y).isSome)
    let partialL := groupBy lk (fun _ b => (sqlSum lx b, sqlCountCol lx b)) lClean
    let partialR := groupBy rk (fun _ b => (sqlSum ry b, sqlCountCol ry b)) rClean
    let origJoined := equiJoinPairs lk rk l r
    let dualJoined := equiJoinPairs Prod.fst Prod.fst partialL partialR
    (sqlSum (fun xy => sqlMul (lx xy.1) (ry xy.2)) origJoined =
      sqlSum (fun pq => sqlMul pq.1.2.1 pq.2.2.1) dualJoined) ∧
    (sqlSum (fun xy => sqlAdd (lx xy.1) (ry xy.2)) origJoined =
      sqlSum (fun pq =>
        sqlAdd (sqlMul pq.1.2.1 (some pq.2.2.2)) (sqlMul pq.2.2.1 (some pq.1.2.2))) dualJoined) := by
  intro lClean rClean partialL partialR origJoined dualJoined
  -- 左の NULL キー除去 + 左テーブルを `lk` バケット順列で展開
  have hOrigClean : origJoined = equiJoinPairs lk rk lClean r :=
    equiJoinPairs_filter_left_isSome lk rk l r
  have hPermL := perm_distinctKey_buckets lk lClean
  have hJoinPermL := hOrigClean ▸ equiJoinPairs_perm_left lk rk r hPermL
  -- 両成分とも「`distinctKeys (lClean.map lk)` の各キー `k` ごとの寄与が一致する」ことに帰着
  have hBucketR : ∀ kv : K,
      r.filter (fun y => (sqlEq (some kv) (rk y)).inWhere) =
      @List.filter R (fun y => @BEq.beq (SqlVal K) instBEqOfDecidableEq (rk y) (some kv)) rClean := by
    intro kv
    unfold rClean
    rw [List.filter_filter]
    apply List.filter_congr
    intro y _
    cases hrk : rk y with
    | none =>
      have hne : (@BEq.beq (SqlVal K) instBEqOfDecidableEq none (some kv)) = false :=
        @beq_eq_false_iff_ne (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mpr (by simp)
      simp [sqlEq, SqlBool.inWhere, hne]
    | some kv' =>
      by_cases h : kv = kv'
      · subst h; simp [sqlEq, SqlBool.inWhere]
      · have h2 : kv' ≠ kv := Ne.symm h
        have hne : (@BEq.beq (SqlVal K) instBEqOfDecidableEq (some kv') (some kv)) = false :=
          @beq_eq_false_iff_ne (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mpr (by simp [h2])
        simp [sqlEq, SqlBool.inWhere, h, hne]
  have hPartR : ∀ kv : K,
      partialR.filter (fun q => (sqlEq (some kv) q.1).inWhere) =
      if some kv ∈ distinctKeys (rClean.map rk) then
        [(some kv, (
          sqlSum ry (@List.filter R (fun y => @BEq.beq (SqlVal K) instBEqOfDecidableEq (rk y) (some kv)) rClean),
          sqlCountCol ry (@List.filter R (fun y => @BEq.beq (SqlVal K) instBEqOfDecidableEq (rk y) (some kv)) rClean)))]
      else [] := by
    intro kv
    unfold partialR groupBy
    exact filter_nodup_keys_eq_some
      (fun k => (
        sqlSum ry (@List.filter R (fun y => @BEq.beq (SqlVal K) instBEqOfDecidableEq (rk y) k) rClean),
        sqlCountCol ry (@List.filter R (fun y => @BEq.beq (SqlVal K) instBEqOfDecidableEq (rk y) k) rClean)))
      kv (distinctKeys (rClean.map rk)) (nodup_distinctKeys _)
  -- 各キー `k` ごとの一致補題
  have hPerKey : ∀ k : SqlVal K,
      let bL := @List.filter L (fun x => @BEq.beq (SqlVal K) instBEqOfDecidableEq (lk x) k) lClean
      let pRow := (k, (sqlSum lx bL, sqlCountCol lx bL))
      let rawPairs := bL.flatMap (fun x => (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y)))
      let sumPairs := (partialR.filter (fun q => (sqlEq k q.1).inWhere)).map (fun q => (pRow, q))
      (sqlSum (fun xy => sqlMul (lx xy.1) (ry xy.2)) rawPairs =
        sqlSum (fun pq => sqlMul pq.1.2.1 pq.2.2.1) sumPairs) ∧
      (sqlSum (fun xy => sqlAdd (lx xy.1) (ry xy.2)) rawPairs =
        sqlSum (fun pq =>
          sqlAdd (sqlMul pq.1.2.1 (some pq.2.2.2)) (sqlMul pq.2.2.1 (some pq.1.2.2))) sumPairs) := by
    intro k
    cases k with
    | none =>
      dsimp only
      have hNilR1 : ∀ x : L, lk x = none → r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere) = [] := by
        intro x hx; rw [hx, List.filter_eq_nil_iff]; intro y _; simp [sqlEq, SqlBool.inWhere]
      have hNilRaw :
          (@List.filter L (fun x => @BEq.beq (SqlVal K) instBEqOfDecidableEq (lk x) none) lClean).flatMap
            (fun x => (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y))) = [] := by
        rw [List.flatMap_eq_nil_iff]
        intro x hx
        rw [List.mem_filter] at hx
        have hEqNone := @beq_iff_eq (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mp hx.2
        simp [hNilR1 x hEqNone]
      have hNilSum : partialR.filter (fun q => (sqlEq none q.1).inWhere) = [] := by
        rw [List.filter_eq_nil_iff]; intro q _; simp [sqlEq, SqlBool.inWhere]
      simp [hNilRaw, hNilSum]
    | some kv =>
      dsimp only
      let bL := @List.filter L (fun x => @BEq.beq (SqlVal K) instBEqOfDecidableEq (lk x) (some kv)) lClean
      let bR := @List.filter R (fun y => @BEq.beq (SqlVal K) instBEqOfDecidableEq (rk y) (some kv)) rClean
      have hRawEq :
          bL.flatMap (fun x => (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y))) =
          bL.flatMap (fun x => bR.map (fun y => (x, y))) := by
        apply flatMap_const_matches (fun x => r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)) bR Prod.mk bL
        intro x hx
        unfold bL at hx
        rw [List.mem_filter] at hx
        have hlkEq := @beq_iff_eq (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mp hx.2
        rw [hlkEq]
        exact hBucketR kv
      change (sqlSum _ (bL.flatMap _) = _) ∧ (sqlSum _ (bL.flatMap _) = _)
      rw [hRawEq, hPartR kv]
      by_cases hMemR : some kv ∈ distinctKeys (rClean.map rk)
      · simp only [hMemR, ↓reduceIte, List.map, sqlSum_cons, sqlSum_nil]
        have hOpZ : ∀ v : SqlVal Int, mergeSum v none = v := sumMonoid.op_zero
        rw [hOpZ, hOpZ]
        exact ⟨sqlSum_cartesian_mul_sound lx ry bL bR,
               sqlSum_cartesian_add_sound lx ry bL bR⟩
      · have hbRNil : bR = [] := by
          unfold bR
          rw [List.filter_eq_nil_iff]
          intro y hy hBeq
          have hEq := @beq_iff_eq (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mp hBeq
          apply hMemR; rw [mem_distinctKeys, List.mem_map]; exact ⟨y, hy, hEq⟩
        have hFlatNil : bL.flatMap (fun x => bR.map (fun y => (x, y))) = [] := by
          simp [hbRNil]
        simp [hMemR, hFlatNil]
  -- 最後に左キー列全体で畳み込んで両式の等号を取り出す
  have hExpandOrig : ∀ f : L × R → SqlVal Int,
      sqlSum f origJoined =
      aggFold sumMonoid (fun k =>
        sqlSum f ((@List.filter L (fun row => @BEq.beq (SqlVal K) instBEqOfDecidableEq (lk row) k) lClean).flatMap
          (fun x => (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y)))))
        (distinctKeys (lClean.map lk)) := by
    intro f
    unfold sqlSum
    rw [aggFold_perm sumMonoid f hJoinPermL]
    unfold equiJoinPairs
    rw [aggFold_flatMap, aggFold_flatMap]
    congr 1; funext k; rw [aggFold_flatMap]
  have hExpandDual : ∀ g : (SqlVal K × (SqlVal Int × Int)) × (SqlVal K × (SqlVal Int × Int)) → SqlVal Int,
      sqlSum g dualJoined =
      aggFold sumMonoid (fun k =>
        let bL := @List.filter L (fun row => @BEq.beq (SqlVal K) instBEqOfDecidableEq (lk row) k) lClean
        let pRow := (k, (sqlSum lx bL, sqlCountCol lx bL))
        sqlSum g ((partialR.filter (fun q => (sqlEq k q.1).inWhere)).map (fun q => (pRow, q))))
        (distinctKeys (lClean.map lk)) := by
    intro g
    unfold sqlSum dualJoined equiJoinPairs partialL groupBy
    rw [aggFold_flatMap, aggFold_map]
    rfl
  constructor
  · rw [hExpandOrig, hExpandDual]
    congr 1; funext k; exact (hPerKey k).1
  · rw [hExpandOrig, hExpandDual]
    congr 1; funext k; exact (hPerKey k).2

/-!
================================================================================
## 候補ルール 3: `LEFT OUTER JOIN` 右側（NULL 供給側）の事前集約と `COUNT(*)` の罠
================================================================================
-/

/-- 右テーブル `S` の事前集約タプル: `(cnt_star, cnt_col, sum_col)`。 -/
structure RightPreAgg where
  cntStar : Int
  cntCol  : Int
  sumCol  : SqlVal Int
  deriving DecidableEq, Repr

/-- **罠 3-A（`D LEFT JOIN PartialAgg(S)` で `COUNT(*)` に `COALESCE(..., 0)` を使うと壊れるバグ！）**:
    売上 `S` が 0 件の部署 `d` は、`LEFT JOIN` の結果として NULL パディングされた行 `(d, NULL)` が **1 行** 出るため、
    本来の `COUNT(*)` は `1` になる。うっかり `COALESCE(cnt_star, 0)` にすると `0` になってしまいバグる！ -/
theorem left_join_count_star_zero_bug :
    let d : Rel Int := [10]
    let s : Rel (Int × SqlVal Int) := []
    let onOrig := fun x (y : Int × SqlVal Int) => sqlEq (some x) (some y.1)
    let origCountStar := sqlCountStar (leftOuterJoin onOrig d s)
    let partialS := groupBy (fun y : Int × SqlVal Int => some y.1)
      (fun _ b => RightPreAgg.mk (sqlCountStar b) (sqlCountCol Prod.snd b) (sqlSum Prod.snd b)) s
    let joined := leftOuterJoin (fun x p => sqlEq (some x) p.1) d partialS
    let buggyCountStar := aggFold countMonoid
      (fun dp => match dp.2 with | none => 0 | some p => p.2.cntStar) joined
    origCountStar ≠ buggyCountStar := by
  decide

private theorem filter_map_const_fst_false {D R' : Type}
    (pred : D → Bool) (d : D) (hp : pred d = false) :
    ∀ zs : Rel R', (zs.map (fun y => (d, some y))).filter (fun dr => pred dr.1) = []
  | [] => rfl
  | _ :: zs => by simp only [List.map, List.filter, hp, filter_map_const_fst_false pred d hp zs]

private theorem filter_map_const_fst_true {D R' : Type}
    (pred : D → Bool) (d : D) (hp : pred d = true) :
    ∀ zs : Rel R', (zs.map (fun y => (d, some y))).filter (fun dr => pred dr.1) =
      zs.map (fun y => (d, some y))
  | [] => rfl
  | _ :: zs => by simp only [List.map, List.filter, hp, filter_map_const_fst_true pred d hp zs]

/-- `leftOuterJoin` の直上の左テーブル列フィルタ `pred(d)` は、左入力へ押し込める（`push_filter_through_left_join_left_side`）。 -/
theorem leftOuterJoin_filter_left {D R' : Type}
    (onP : D → R' → SqlBool) (pred : D → Bool) :
    ∀ (ds : Rel D) (r' : Rel R'),
      (leftOuterJoin onP ds r').filter (fun dr => pred dr.1) =
      leftOuterJoin onP (ds.filter pred) r'
  | [], _ => rfl
  | d :: ds, r' => by
    have ih := leftOuterJoin_filter_left onP pred ds r'
    unfold leftOuterJoin at ih ⊢
    simp only [List.flatMap_cons, List.filter_append]
    cases hp : pred d
    · simp only [List.filter, hp]
      rw [ih]
      cases hM : r'.filter (fun y => (onP d y).inWhere) with
      | nil => simp only [List.isEmpty_nil, ↓reduceIte, List.filter, hp, List.nil_append]
      | cons y0 ys =>
        simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte,
                   filter_map_const_fst_false pred d hp (y0 :: ys), List.nil_append]
    · simp only [List.filter, hp, List.flatMap_cons]
      rw [ih]
      cases hM : r'.filter (fun y => (onP d y).inWhere) with
      | nil => simp only [List.isEmpty_nil, ↓reduceIte, List.filter, hp]
      | cons y0 ys =>
        simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte,
                   filter_map_const_fst_true pred d hp (y0 :: ys)]

/-- **主定理 3-1 (`eager_agg_right_of_left_join_sound`)**:
    `D LEFT JOIN S ON dk(d) = sk(s)` の右側 `S` を `sk(s) IS NOT NULL` で絞って `sk` ごとに事前集約し、
    2 段目で：
    - `COUNT(*)` には **`COALESCE(cntStar, 1)`**（未マッチは NULL 補完 1 行！）
    - `COUNT(S.col)` には **`COALESCE(cntCol, 0)`**
    - `SUM(S.col)` には **`sumCol`**（未マッチなら `none`）
    を使って任意の左テーブル軸 `GROUP BY gD(d)` で再集約すると、元の `(COUNT(*), COUNT(S.col), SUM(S.col))` と
    **Bag 等価 (`BagEq`)** になる！ -/
theorem eager_agg_right_of_left_join_sound {D S K GD : Type}
    [DecidableEq K] [DecidableEq GD]
    (dk : D → SqlVal K) (sk : S → SqlVal K)
    (sval : S → SqlVal Int) (gD : D → GD)
    (dRel : Rel D) (sRel : Rel S) :
    let sClean := sRel.filter (fun s => (sk s).isSome)
    let partialS := groupBy sk
      (fun _ b => RightPreAgg.mk (sqlCountStar b) (sqlCountCol sval b) (sqlSum sval b)) sClean
    let origJoined := leftOuterJoin (fun d s => sqlEq (dk d) (sk s)) dRel sRel
    let optJoined  := leftOuterJoin (fun d p => sqlEq (dk d) p.1) dRel partialS
    BagEq
      (groupBy (fun ds => gD ds.1)
        (fun _ b => (
          sqlCountStar b,
          sqlCountCol (fun ds => ds.2.bind sval) b,
          sqlSum (fun ds => ds.2.bind sval) b))
        origJoined)
      (groupBy (fun dp => gD dp.1)
        (fun _ b => (
          aggFold countMonoid (fun dp => match dp.2 with | none => 1 | some p => p.2.cntStar) b,
          aggFold countMonoid (fun dp => match dp.2 with | none => 0 | some p => p.2.cntCol) b,
          aggFold sumMonoid   (fun dp => match dp.2 with | none => none | some p => p.2.sumCol) b))
        optJoined) := by
  intro sClean partialS origJoined optJoined
  have hPerD : ∀ d : D,
      let mOrig := sRel.filter (fun s => (sqlEq (dk d) (sk s)).inWhere)
      let rowsOrig : Rel (D × Option S) :=
        if mOrig.isEmpty then [(d, none)] else mOrig.map (fun s => (d, some s))
      let mOpt := partialS.filter (fun p => (sqlEq (dk d) p.1).inWhere)
      let rowsOpt : Rel (D × Option (SqlVal K × RightPreAgg)) :=
        if mOpt.isEmpty then [(d, none)] else mOpt.map (fun p => (d, some p))
      (sqlCountStar rowsOrig =
        aggFold countMonoid (fun dp => match dp.2 with | none => 1 | some p => p.2.cntStar) rowsOpt) ∧
      (sqlCountCol (fun ds => ds.2.bind sval) rowsOrig =
        aggFold countMonoid (fun dp => match dp.2 with | none => 0 | some p => p.2.cntCol) rowsOpt) ∧
      (sqlSum (fun ds => ds.2.bind sval) rowsOrig =
        aggFold sumMonoid (fun dp => match dp.2 with | none => none | some p => p.2.sumCol) rowsOpt) := by
    intro d
    cases hdk : dk d with
    | none =>
      have hEmpty1 : sRel.filter (fun s => (sqlEq none (sk s)).inWhere) = [] := by
        rw [List.filter_eq_nil_iff]; intro s _; simp [sqlEq, SqlBool.inWhere]
      have hEmpty2 : partialS.filter (fun p => (sqlEq none p.1).inWhere) = [] := by
        rw [List.filter_eq_nil_iff]; intro p _; simp [sqlEq, SqlBool.inWhere]
      simp [hEmpty1, hEmpty2]
      exact ⟨countMonoid.op_zero 1 |>.symm, rfl, rfl⟩
    | some kv =>
      let bucket := @List.filter S (fun s => @BEq.beq (SqlVal K) instBEqOfDecidableEq (sk s) (some kv)) sClean
      have hBucketEq :
          sRel.filter (fun s => (sqlEq (some kv) (sk s)).inWhere) = bucket := by
        unfold bucket sClean
        rw [List.filter_filter]
        apply List.filter_congr
        intro s _
        cases hsk : sk s with
        | none =>
          have hne : (@BEq.beq (SqlVal K) instBEqOfDecidableEq none (some kv)) = false :=
            @beq_eq_false_iff_ne (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mpr (by simp)
          simp [sqlEq, SqlBool.inWhere, hne]
        | some kv' =>
          by_cases h : kv = kv'
          · subst h
            simp [sqlEq, SqlBool.inWhere]
          · have h2 : kv' ≠ kv := Ne.symm h
            have hne : (@BEq.beq (SqlVal K) instBEqOfDecidableEq (some kv') (some kv)) = false :=
              @beq_eq_false_iff_ne (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mpr (by simp [h2])
            simp [sqlEq, SqlBool.inWhere, h, hne]
      have hPartFilter :
          partialS.filter (fun p => (sqlEq (some kv) p.1).inWhere) =
          if some kv ∈ distinctKeys (sClean.map sk) then
            [(some kv, RightPreAgg.mk
              (sqlCountStar bucket)
              (sqlCountCol sval bucket)
              (sqlSum sval bucket))]
          else [] := by
        unfold partialS groupBy bucket
        exact filter_nodup_keys_eq_some
          (fun k => RightPreAgg.mk
            (sqlCountStar (@List.filter S (fun row => @BEq.beq (SqlVal K) instBEqOfDecidableEq (sk row) k) sClean))
            (sqlCountCol sval (@List.filter S (fun row => @BEq.beq (SqlVal K) instBEqOfDecidableEq (sk row) k) sClean))
            (sqlSum sval (@List.filter S (fun row => @BEq.beq (SqlVal K) instBEqOfDecidableEq (sk row) k) sClean)))
          kv (distinctKeys (sClean.map sk)) (nodup_distinctKeys _)
      dsimp only
      rw [hBucketEq, hPartFilter]
      by_cases hMem : some kv ∈ distinctKeys (sClean.map sk)
      · have hNonEmpty : bucket.isEmpty = false := by
          rw [mem_distinctKeys, List.mem_map] at hMem
          rcases hMem with ⟨s0, hs0, hsk0⟩
          have hs0In : s0 ∈ bucket := by
            unfold bucket
            rw [List.mem_filter]
            exact ⟨hs0, @beq_iff_eq (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mpr hsk0⟩
          cases hL : bucket with
          | nil => rw [hL] at hs0In; cases hs0In
          | cons _ _ => rfl
        simp only [hMem, ↓reduceIte, hNonEmpty, Bool.false_eq_true, List.isEmpty_cons,
                   List.map, aggFold_cons, aggFold_nil]
        refine ⟨?_, ?_, ?_⟩
        · rw [countMonoid.op_zero]; unfold sqlCountStar; rw [aggFold_map]
        · rw [countMonoid.op_zero]; unfold sqlCountCol; rw [aggFold_map]; rfl
        · rw [sumMonoid.op_zero]; unfold sqlSum; rw [aggFold_map]; rfl
      · have hIsNil : bucket = [] := by
          unfold bucket
          rw [List.filter_eq_nil_iff]
          intro s hs; intro hBeq
          have hEq := @beq_iff_eq (SqlVal K) instBEqOfDecidableEq _ _ _ |>.mp hBeq
          apply hMem; rw [mem_distinctKeys, List.mem_map]; exact ⟨s, hs, hEq⟩
        simp only [hMem, ↓reduceIte, hIsNil, List.isEmpty_nil,
                   aggFold_cons, aggFold_nil]
        exact ⟨by simp [countMonoid.op_zero], by simp [countMonoid.op_zero], rfl⟩
  apply groupBy_bagEq_of_keys_and_vals
  · intro g
    have hLeftPreserved : ∀ {R' : Type} (onP : D → R' → SqlBool) (r' : Rel R'),
        (∃ dr ∈ leftOuterJoin onP dRel r', gD dr.1 = g) ↔ (∃ d ∈ dRel, gD d = g) := by
      intro R' onP r'
      constructor
      · rintro ⟨⟨d', optR⟩, hMem, hKey⟩
        simp only [leftOuterJoin, List.mem_flatMap] at hMem
        rcases hMem with ⟨d, hd, hInBranch⟩
        by_cases hEmp : (r'.filter (fun y => (onP d y).inWhere)).isEmpty
        · simp [hEmp] at hInBranch
          rcases hInBranch with ⟨hEqD, _⟩
          exact ⟨d, hd, hEqD ▸ hKey⟩
        · simp [hEmp, List.mem_map] at hInBranch
          rcases hInBranch with ⟨_, _, hEqD, _⟩
          exact ⟨d, hd, hEqD.symm ▸ hKey⟩
      · rintro ⟨d, hd, hKey⟩
        by_cases hEmp : (r'.filter (fun y => (onP d y).inWhere)).isEmpty
        · refine ⟨(d, none), ?_, hKey⟩
          simp only [leftOuterJoin, List.mem_flatMap]
          exact ⟨d, hd, by simp [hEmp]⟩
        · cases hF : r'.filter (fun y => (onP d y).inWhere) with
          | nil => simp [hF] at hEmp
          | cons y0 ys =>
            refine ⟨(d, some y0), ?_, hKey⟩
            simp only [leftOuterJoin, List.mem_flatMap]
            exact ⟨d, hd, by simp [hF]⟩
    unfold origJoined optJoined
    rw [hLeftPreserved, hLeftPreserved]
  · intro g _
    unfold origJoined optJoined
    rw [leftOuterJoin_filter_left _ (fun d => gD d == g),
        leftOuterJoin_filter_left _ (fun d => gD d == g)]
    unfold sqlCountStar sqlCountCol sqlSum leftOuterJoin
    rw [aggFold_flatMap, aggFold_flatMap, aggFold_flatMap,
        aggFold_flatMap, aggFold_flatMap, aggFold_flatMap]
    refine Prod.ext ?_ (Prod.ext ?_ ?_)
    · dsimp only; congr 1; funext d; exact (hPerD d).1
    · dsimp only; congr 1; funext d; exact (hPerD d).2.1
    · dsimp only; congr 1; funext d; exact (hPerD d).2.2

/-!
================================================================================
## 候補ルール 4: 多段スター・スノーフレーク Join カスケード集約 (`Fact ⋈ Dim1 ⋈ Dim2`)
================================================================================
-/

private theorem groupBy_aggFold_perm {Row G M : Type} [DecidableEq G]
    (m : AggMonoid M) (key : Row → G) (f : Row → M)
    {r1 r2 : Rel Row} (h : BagEq r1 r2) :
    BagEq
      (groupBy key (fun _ b => aggFold m f b) r1)
      (groupBy key (fun _ b => aggFold m f b) r2) := by
  apply groupBy_bagEq_of_keys_and_vals
  · intro g
    constructor
    · rintro ⟨x, hx, hk⟩; exact ⟨x, h.subset hx, hk⟩
    · rintro ⟨x, hx, hk⟩; exact ⟨x, h.symm.subset hx, hk⟩
  · intro g _
    exact aggFold_perm m f (h.filter _)

theorem equiJoinPairs_filter_left_pred {L R K : Type} [DecidableEq K]
    (lk : L → SqlVal K) (rk : R → SqlVal K) (pred : L → Bool) :
    ∀ (xs : Rel L) (r : Rel R),
      (equiJoinPairs lk rk xs r).filter (fun xy => pred xy.1) =
      equiJoinPairs lk rk (xs.filter pred) r
  | [], _ => rfl
  | x :: xs, r => by
    have ih := equiJoinPairs_filter_left_pred lk rk pred xs r
    unfold equiJoinPairs at ih ⊢
    simp only [List.flatMap_cons, List.filter_append]
    cases hp : pred x
    · have hDrop :
          ((r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y))).filter
            (fun xy => pred xy.1) = [] := by
        rw [List.filter_eq_nil_iff]
        intro xy hM; rw [List.mem_map] at hM
        rcases hM with ⟨_, _, rfl⟩; simp [hp]
      simp [List.filter, hp, hDrop, ih]
    · have hKeep :
          ((r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y))).filter
            (fun xy => pred xy.1) =
          (r.filter (fun y => (sqlEq (lk x) (rk y)).inWhere)).map (fun y => (x, y)) := by
        rw [List.filter_eq_self]
        intro xy hM; rw [List.mem_map] at hM
        rcases hM with ⟨_, _, rfl⟩; exact hp
      simp [List.filter, hp, hKeep, List.flatMap_cons, ih]

private theorem distinctKeys_filter_comm {K : Type} [DecidableEq K] (pred : K → Bool) :
    ∀ xs : List K, (distinctKeys xs).filter pred = distinctKeys (xs.filter pred)
  | [] => rfl
  | k :: ks => by
    have ih := distinctKeys_filter_comm pred ks
    have hSwap : ∀ (a b : K → Bool) (ys : List K), (ys.filter a).filter b = (ys.filter b).filter a := by
      intro a b ys
      rw [List.filter_filter, List.filter_filter]
      apply List.filter_congr; intro z _; exact Bool.and_comm _ _
    cases hk : pred k
    · simp only [distinctKeys, List.filter, hk]
      rw [hSwap, ih]
      apply List.filter_eq_self.mpr
      intro x hx
      rw [mem_distinctKeys, List.mem_filter] at hx
      simp only [ne_eq, decide_eq_true_eq]
      intro hEq; rw [hEq, hk] at hx; cases hx.2
    · simp only [distinctKeys, List.filter, hk]
      congr 1
      rw [hSwap, ih]

/-- `GROUP BY (k1(x), k2(x))` の直後にある第 1 キーフィルタ `pred(k1)` は、`groupBy` の入力へ押し込める。 -/
private theorem groupBy_filter_fst_key {Row K1 K2 M : Type}
    [DecidableEq K1] [DecidableEq K2]
    (m : AggMonoid M) (k1 : Row → K1) (k2 : Row → K2) (f : Row → M)
    (pred : K1 → Bool) (xs : Rel Row) :
    (groupBy (fun x => (k1 x, k2 x)) (fun _ b => aggFold m f b) xs).filter
      (fun p => pred p.1.1) =
    groupBy (fun x => (k1 x, k2 x)) (fun _ b => aggFold m f b)
      (xs.filter (fun x => pred (k1 x))) := by
  unfold groupBy
  have hMapFilter :
      (xs.filter (fun x => pred (k1 x))).map (fun x => (k1 x, k2 x)) =
      (xs.map (fun x => (k1 x, k2 x))).filter (fun pk => pred pk.1) := by
    induction xs with
    | nil => rfl
    | cons x xs ih =>
      cases hx : pred (k1 x) <;> simp [List.filter, hx, ih]
  rw [hMapFilter, ← distinctKeys_filter_comm (fun pk : K1 × K2 => pred pk.1)]
  clear hMapFilter
  induction (distinctKeys (xs.map (fun x => (k1 x, k2 x)))) with
  | nil => rfl
  | cons pk pks ih =>
    cases hpk : pred pk.1
    · simp [List.filter, hpk, ih]
    · simp only [List.map, List.filter, hpk, ih]
      congr 1; congr 1; congr 1
      rw [List.filter_filter]
      apply List.filter_congr
      intro x _
      by_cases hEq : (k1 x, k2 x) == pk
      · have hEq' := beq_iff_eq.mp hEq
        have hpx : pred (k1 x) = true := by
          have := congrArg (fun q => pred q.1) hEq'
          simpa [hpk] using this
        simp [hEq, hpx]
      · simp [hEq]

/-- **主定理 4 (`star_cascade_two_hop_agg_sound` — 2 ホップ Join を跨ぐ 3 段カスケード集約の完全証明)**:
    `Fact ⋈ Dim1 ⋈ Dim2` 上の `GROUP BY (attr1(d1), attr2(d2))` に対し、
    Join の段ごとに不要になった外部キー（`fk1` → `fk2`）を順次潰しながら集約する 3 段カスケード計画は、
    `Dim1`・`Dim2` の Unique 制約の有無にかかわらず、任意の可換モノイド集約で **Bag 等価 (`BagEq`)** である！ -/
theorem star_cascade_two_hop_agg_sound {F D1 D2 K1 K2 A1 A2 M : Type}
    [DecidableEq K1] [DecidableEq K2] [DecidableEq A1] [DecidableEq A2]
    (m : AggMonoid M)
    (fk1 : F → SqlVal K1) (fk2 : F → SqlVal K2) (measureF : F → M)
    (d1k : D1 → SqlVal K1) (attr1 : D1 → A1)
    (d2k : D2 → SqlVal K2) (attr2 : D2 → A2)
    (fact : Rel F) (dim1 : Rel D1) (dim2 : Rel D2) :
    let join1 := equiJoinPairs fk1 d1k fact dim1
    let join2 := equiJoinPairs (fun fd1 => fk2 fd1.1) d2k join1 dim2
    let origPlan := groupBy
      (fun fd12 => (attr1 fd12.1.2, attr2 fd12.2))
      (fun _ b => aggFold m (fun fd12 => measureF fd12.1.1) b)
      join2
    -- 3段カスケード計画:
    --   Stage 1: Fact を (fk2, fk1) で事前集約（fk1 IS NOT NULL 早期フィルタ）
    let stage1 := groupBy (fun f => (fk2 f, fk1 f)) (fun _ b => aggFold m measureF b)
      (fact.filter (fun f => (fk1 f).isSome))
    --   Stage 2: Dim1 と結合した直後に fk1 を消去して (attr1(d1), fk2) で中段集約（fk2 IS NOT NULL 早期フィルタ）
    let joined1 := equiJoinPairs (fun p => p.1.2) d1k stage1 dim1
    let stage2 := groupBy (fun pd1 => (attr1 pd1.2, pd1.1.1.1))
      (fun _ b => aggFold m (fun pd1 => pd1.1.2) b)
      (joined1.filter (fun pd1 => pd1.1.1.1.isSome))
    --   Stage 3: Dim2 と結合して最終 (attr1, attr2) 集約
    let joined2 := equiJoinPairs (fun p => p.1.2) d2k stage2 dim2
    let cascadePlan := groupBy
      (fun pd2 => (pd2.1.1.1, attr2 pd2.2))
      (fun _ b => aggFold m (fun pd2 => pd2.1.2) b)
      joined2
    BagEq origPlan cascadePlan := by
  intro join1 join2 origPlan stage1 joined1 stage2 joined2 cascadePlan
  -- Step A: 外側の `join1 ⋈ dim2` に主定理 1-2 を適用
  have hOuter := eager_group_agg_over_mn_join_sound m
    (fun fd1 : F × D1 => fk2 fd1.1) d2k
    (fun fd1 : F × D1 => attr1 fd1.2)
    (fun a1 d2 => (a1, attr2 d2))
    (fun fd1 : F × D1 => measureF fd1.1)
    join1 dim2
  -- Step B: 中段グループ `join1.filter (fk2 IS NOT NULL)` と `stage2` の Bag 等価性を主定理 1-2 で導く
  have hMidBagEq :
      BagEq
        (groupBy (fun fd1 : F × D1 => (attr1 fd1.2, fk2 fd1.1))
          (fun _ b => aggFold m (fun fd1 => measureF fd1.1) b)
          (join1.filter (fun fd1 => (fk2 fd1.1).isSome)))
        stage2 := by
    have hPushOrig :
        join1.filter (fun fd1 => (fk2 fd1.1).isSome) =
        equiJoinPairs fk1 d1k (fact.filter (fun f => (fk2 f).isSome)) dim1 :=
      equiJoinPairs_filter_left_pred fk1 d1k (fun f => (fk2 f).isSome) fact dim1
    have hFilterComm :
        (fact.filter (fun f => (fk1 f).isSome)).filter (fun f => (fk2 f).isSome) =
        (fact.filter (fun f => (fk2 f).isSome)).filter (fun f => (fk1 f).isSome) := by
      rw [List.filter_filter, List.filter_filter]
      apply List.filter_congr; intro f _; exact Bool.and_comm _ _
    have hPushStage1 :
        joined1.filter (fun pd1 => pd1.1.1.1.isSome) =
        equiJoinPairs (fun p => p.1.2) d1k
          (groupBy (fun f => (fk2 f, fk1 f)) (fun _ b => aggFold m measureF b)
            ((fact.filter (fun f => (fk2 f).isSome)).filter (fun f => (fk1 f).isSome)))
          dim1 := by
      unfold joined1 stage1
      rw [equiJoinPairs_filter_left_pred (fun p : (SqlVal K2 × SqlVal K1) × M => p.1.2) d1k
            (fun p => p.1.1.isSome),
          groupBy_filter_fst_key, hFilterComm]
    unfold stage2
    rw [hPushOrig, hPushStage1]
    exact eager_group_agg_over_mn_join_sound m
      fk1 d1k fk2 (fun k2 d1 => (attr1 d1, k2)) measureF
      (fact.filter (fun f => (fk2 f).isSome)) dim1
  -- Step C: `hOuter` と `hMidBagEq` を `BagEq.trans` で連結して完了！
  have hJoin2Perm := equiJoinPairs_perm_left (fun p => p.1.2) d2k dim2 hMidBagEq
  have hFinalPerm := groupBy_aggFold_perm m
    (fun pd2 : ((A1 × SqlVal K2) × M) × D2 => (pd2.1.1.1, attr2 pd2.2))
    (fun pd2 => pd2.1.2)
    hJoin2Perm
  exact hOuter.trans hFinalPerm

end TinylambOpt
