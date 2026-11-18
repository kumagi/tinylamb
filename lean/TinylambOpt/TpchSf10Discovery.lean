import TinylambOpt.TpcdsDiscovery
import TinylambOpt.ClickBenchDiscovery

/-
# Step 13: TPC-H SF=10 対 DuckDB 敗北クエリ群に向けた証明付きオプティマイザ拡充探索

過去のベンチマーク記録（`benchmark/comparison/README-tpch.md`, `build/quality/tpch-scaling-20261117/verification.json`,
`build/quality/tpch-sf10-performance-20261117/gate47`）および SF=10（約 8,658 万行）全 22 問プロファイルを精査し、
DuckDB 2.0 に対して劣後またはスピル超過しているクエリ群
（**Q19, Q13, Q18, Q9, Q15, Q10, Q8, Q20, Q11, Q16, Q12, Q17**）
の根本原因を解消する **6 系統のアグレッシブな書き換えルール（TPCH-1〜TPCH-6）** と、
実装時に踏み抜くとサイレント誤答を引き起こす **8 つの SQL 三値論理・NULL・空集合・多重度トラップ反例** を
Lean 4 で `sorry` ゼロで形式化・機械証明します。

## 収録する 6 大 TPC-H SF=10 最適化ファミリー（すべて `sorry` ゼロ）

1. **TPCH-1 (Q19: `480.42x` / `25.89s` vs `53.9ms`)**:
   - Kleene 3値論理の N 項 `OR` 共通連言括り出し (`kleene_common_conjunct_factor_or_sound`)
   - Join 横断 `OR` からの単一テーブル選言包絡線プッシュダウン (`disjunctive_join_envelope_pushdown_sound`)
   - 数値区間の選言包絡線（Bounding Hull）抽出 (`interval_hull_of_disjuncts_sound`)
   - **罠 1**: 片側条件を持たない枝があるときに包絡線を作ると行が消失するバグ (`envelope_missing_branch_pred_drops_rows_bug`)

2. **TPCH-2 (Q13: `> 60s` スピル超過 vs `168.9ms`)**:
   - `LEFT OUTER JOIN` 右側の先行 `COUNT` 事前集約 ＋ 左主キー（`UNIQUE NOT NULL`）による内側 `GROUP BY` 完全消去 (`left_join_right_preagg_and_unique_elide_groupby_sound`)
   - **罠 2a**: `NOT NULL` のない SQL `UNIQUE` 列で複数 `NULL` 行があるとき内側 `GROUP BY` を消すと外側度数分布が壊れるバグ (`nullable_unique_left_elides_groupby_bug`)
   - **罠 2b**: `COUNT(COALESCE(r.col, -1))` のような非 NULL 厳密式を `0` 補完すると未マッチ行カウントが壊れるバグ (`left_join_count_nonstrict_expr_bug`)

3. **TPCH-3 (Q18: `> 60s` タイムアウト vs `196.8ms`)**:
   - 自己準結合 `IN (SELECT k FROM T GROUP BY k HAVING P(SUM))` と外側 `T` 結合集約の 1 パス吸収 (`self_semijoin_having_agg_absorption_sound`)
   - **罠 3**: 外側 `WHERE` のみに追加の `T` フィルタがあるとき単純吸収すると `HAVING` 判定が変わるバグ (`self_semijoin_mismatched_where_bug`)

4. **TPCH-4 (Q15: `3.64x` / `728.5ms` vs `200.2ms`, Q11: `2.14x` / `59.0ms` vs `27.6ms`)**:
   - 共有 CTE 自己最大値フィルタ `val = (SELECT MAX(val) FROM R)` の `IS NOT NULL` 付き 1 パス最大タイ集合還元 (`cte_argmax_subquery_to_nonnull_max_ties_sound`)
   - `HAVING SUM(v) > c * (SELECT SUM(v) FROM same_join)` の Fubini 1 パス Window 総和還元 (`grouped_having_global_sum_fubini_sound`)
   - **罠 4a**: 全行 `NULL` 時に `IS NOT NULL` ガード無しで `WITH TIES` / `RANK() = 1` に変換すると全行誤出力するバグ (`argmax_with_ties_without_is_not_null_bug`)
   - **罠 4b**: `SUM(DISTINCT v)` でグループ和を総和と同一視すると重複値で破綻するバグ (`grouped_having_distinct_sum_fubini_bug`)

5. **TPCH-5 (Q10: `3.58x` / `513.7ms` vs `143.3ms`, Q9: `4.11x` / `1148.3ms` vs `279.3ms`)**:
   - 狭キー準結合検証付き Top-N 集約後の遅延ディメンション属性結合 (`topn_agg_late_unique_dim_attribute_join_sound`)
   - 厳密等値ドメイン上の推移的等価クラス閉包による複合結合キー導出 (`equijoin_transitive_closure_sound`)
   - **罠 5**: FK 非保証時に準結合検証なしで先に `LIMIT K` を取ると未マッチキーが枠を奪うバグ (`late_dim_join_without_key_semijoin_steals_limit_bug`)

6. **TPCH-6 (Q20: `2.28x` / `161.2ms` vs `70.7ms`, Q16: `2.09x` / `608.5ms` vs `290.8ms`)**:
   - 相関スカラー `SUM` 不等号 `avail > c * (SELECT SUM(qty) FROM ...)` の空集合 `NULL` 棄却性を利用した `INNER JOIN` 相関除去 (`correlated_gt_sum_decorrelate_to_inner_join_sound`)
   - **罠 6a (Kim の COUNT バグ)**: `SUM` でなく `COUNT(*)` のときに `INNER JOIN` へ降格すると 0 件一致行が消失するバグ (`correlated_gt_count_demotes_to_inner_join_bug`)
   - `NOT IN` 非 NULL サブクエリの `AntiJoin` 還元と空サブクエリ `NULL` 左辺トラップ (`not_in_nonnull_nonempty_to_antijoin_sound`, `not_in_empty_subquery_null_lhs_bug`)
-/

namespace TinylambOpt

/-!
## 共通補題 (`sqlEq` と `distinctKeys` の基本性質)
-/

private theorem sqlEq_some_inWhere_dec {α : Type} [DecidableEq α] (a : α) (b : SqlVal α) :
    (sqlEq (some a) b).inWhere = decide (b = some a) := by
  cases b with
  | none => rfl
  | some vb =>
    by_cases h : a = vb
    · subst h; simp [sqlEq, SqlBool.inWhere]
    · have hne : vb ≠ a := Ne.symm h
      simp [sqlEq, h, hne, SqlBool.inWhere]

private theorem distinctKeys_const_nonempty_append {A K : Type} [DecidableEq K]
    (k : K) (tail : List K) :
    ∀ (a0 : A) (as : List A),
      distinctKeys ((a0 :: as).map (fun _ => k) ++ tail) =
      k :: (distinctKeys tail).filter (· ≠ k)
  | _, [] => by simp [distinctKeys]
  | _, a1 :: as1 => by
    have ih := distinctKeys_const_nonempty_append k tail a1 as1
    simp only [List.map_cons, List.cons_append, distinctKeys] at ih ⊢
    rw [ih]
    simp

/-!
## 1. TPCH-1 (Q19): Kleene 3値論理の共通因子括り出し・単一テーブル選言包絡線・区間 Hull
-/

/-- 3値論理のリスト選言（空リストは `tFalse`）。 -/
def sqlOrList : List SqlBool → SqlBool
  | []      => .tFalse
  | b :: bs => SqlBool.or b (sqlOrList bs)

@[simp] theorem sqlOrList_nil : sqlOrList [] = .tFalse := rfl
@[simp] theorem sqlOrList_cons (b : SqlBool) (bs : List SqlBool) :
    sqlOrList (b :: bs) = SqlBool.or b (sqlOrList bs) := rfl

/-- 補題: Kleene 3値論理における 2 項の `AND` に対する `OR` の厳密分配律。
    `c = tNull`（`NULL`）を含め $3^3 = 27$ 通りすべてで 3 値真理値が完全一致する！ -/
theorem kleene_and_or_distrib_left (c a b : SqlBool) :
    SqlBool.or (SqlBool.and c a) (SqlBool.and c b) = SqlBool.and c (SqlBool.or a b) := by
  cases c <;> cases a <;> cases b <;> rfl

/-- **定理 TPCH-1a (`kleene_common_conjunct_factor_or_sound`, Q19)**:
    任意の非空選言リスト `ds` のすべての枝に共通連言 `c`（例: Q19 の `p_partkey = l_partkey`,
    `l_shipmode IN ('AIR', 'AIR REG')`, `l_shipinstruct = 'DELIVER IN PERSON'`）が含まれるとき、
    `c` を `OR` の外側へ括り出しても 3 値論理の真理値そのものが完全一致する。 -/
theorem kleene_common_conjunct_factor_or_sound (c : SqlBool) :
    ∀ (d0 : SqlBool) (ds : List SqlBool),
      sqlOrList ((d0 :: ds).map (fun d => SqlBool.and c d)) =
      SqlBool.and c (sqlOrList (d0 :: ds))
  | d0, [] => by
    cases c <;> cases d0 <;> rfl
  | d0, d1 :: ds => by
    have ih := kleene_common_conjunct_factor_or_sound c d1 ds
    simp only [List.map_cons, sqlOrList_cons] at ih ⊢
    rw [ih, kleene_and_or_distrib_left]

theorem sqlOrList_inWhere_iff (bs : List SqlBool) :
    (sqlOrList bs).inWhere = true ↔ ∃ b ∈ bs, b.inWhere = true := by
  induction bs with
  | nil => simp [SqlBool.inWhere]
  | cons b bs ih =>
    simp only [sqlOrList_cons, or_inWhere, Bool.or_eq_true, List.mem_cons, ih]
    constructor
    · rintro (hb | ⟨b', hb', hTrue⟩)
      · exact ⟨b, Or.inl rfl, hb⟩
      · exact ⟨b', Or.inr hb', hTrue⟩
    · rintro ⟨b', rfl | hb', hTrue⟩
      · exact Or.inl hTrue
      · exact Or.inr ⟨b', hb', hTrue⟩

/-- **定理 TPCH-1b (`disjunctive_join_envelope_pushdown_sound`, Q19)**:
    2 テーブル `L ⋈ R` 上の選言フィルタ $\bigvee_i (P_i(x) \land Q_i(y) \land J_i(x, y))$ に対して、
    たとえ枝同士が重複していても・`NULL`（`tNull`）を含んでいても、
    左スキャンへ単一テーブル包絡線 $\bigvee_i P_i(x)$ を、右スキャンへ $\bigvee_i Q_i(y)$ を
    事前プッシュダウンした `InnerJoin` は、元の `InnerJoin` と **行リストとして完全一致（`=`）** する！ -/
theorem disjunctive_join_envelope_pushdown_sound {L R : Type}
    (branches : List ((L → SqlBool) × (R → SqlBool) × (L → R → SqlBool)))
    (onPred : L → R → SqlBool)
    (l : Rel L) (r : Rel R) :
    let fullOr := fun (x : L) (y : R) =>
      sqlOrList (branches.map (fun b => SqlBool.and (b.1 x) (SqlBool.and (b.2.1 y) (b.2.2 x y))))
    let envL := fun (x : L) => sqlOrList (branches.map (fun b => b.1 x))
    let envR := fun (y : R) => sqlOrList (branches.map (fun b => b.2.1 y))
    select (fun p => match p with | (x, some y) => fullOr x y | (_, none) => .tFalse)
      (innerJoin onPred l r) =
    select (fun p => match p with | (x, some y) => fullOr x y | (_, none) => .tFalse)
      (innerJoin onPred (select envL l) (select envR r)) := by
  intro fullOr envL envR
  have hAndTrue : ∀ a b : SqlBool, (SqlBool.and a b).inWhere = true ↔ a.inWhere = true ∧ b.inWhere = true := by
    intro a b; cases a <;> cases b <;> simp [SqlBool.and, SqlBool.inWhere]
  have hImpL : ∀ x y, (fullOr x y).inWhere = true → (envL x).inWhere = true := by
    intro x y hFull
    rw [sqlOrList_inWhere_iff] at hFull ⊢
    rcases hFull with ⟨_, hMem, hTrue⟩
    rw [List.mem_map] at hMem
    rcases hMem with ⟨b, hbMem, rfl⟩
    rw [hAndTrue] at hTrue
    exact ⟨b.1 x, List.mem_map.mpr ⟨b, hbMem, rfl⟩, hTrue.1⟩
  have hImpR : ∀ x y, (fullOr x y).inWhere = true → (envR y).inWhere = true := by
    intro x y hFull
    rw [sqlOrList_inWhere_iff] at hFull ⊢
    rcases hFull with ⟨_, hMem, hTrue⟩
    rw [List.mem_map] at hMem
    rcases hMem with ⟨b, hbMem, rfl⟩
    rw [hAndTrue, hAndTrue] at hTrue
    exact ⟨b.2.1 y, List.mem_map.mpr ⟨b, hbMem, rfl⟩, hTrue.2.1⟩
  unfold select innerJoin
  induction l with
  | nil => rfl
  | cons x xs ih =>
    by_cases hx : (envL x).inWhere = true
    · simp only [List.filter_cons, hx, ↓reduceIte, List.flatMap_cons, List.filter_append]
      rw [ih]
      congr 1
      clear ih xs
      induction r with
      | nil => rfl
      | cons y ys ihy =>
        by_cases hy : (envR y).inWhere = true
        · by_cases hOn : (onPred x y).inWhere = true <;>
            simp [List.filter_cons, hy, hOn, ihy]
        · have hFullFalse : (fullOr x y).inWhere = false := by
            cases hF : (fullOr x y).inWhere
            · rfl
            · exact absurd (hImpR x y hF) hy
          by_cases hOn : (onPred x y).inWhere = true
          · simp [hy, hOn, hFullFalse, ihy]
          · simp [hy, hOn, ihy]
    · have hEmptyX :
        List.filter (fun p : L × Option R =>
          (match p with | (x', some y') => fullOr x' y' | (_, none) => SqlBool.tFalse).inWhere)
          (List.map (fun y => (x, some y)) (List.filter (fun y => (onPred x y).inWhere) r)) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro p hp
        rw [List.mem_map] at hp
        rcases hp with ⟨y, _, rfl⟩
        exact fun hF => hx (hImpL x y hF)
      simp only [List.filter_cons, hx, Bool.false_eq_true, ↓reduceIte, List.flatMap_cons,
                 List.filter_append, hEmptyX, List.nil_append]
      exact ih

/-- **定理 TPCH-1c (`interval_hull_of_disjuncts_sound`, Q19)**:
    各枝が区間 $[lo_i, hi_i]$ を課す選言（例: Q19 の `[5, 15] OR [15, 25] OR [29, 39]`）は、
    必ず外包区間 $[\min_i lo_i,\, \max_i hi_i]$（`[5, 39]`）に含まれ、SARGable 範囲スキャン条件として安全に課せる。 -/
theorem interval_hull_of_disjuncts_sound (x lo1 hi1 lo2 hi2 lo3 hi3 : Int)
    (h : (lo1 ≤ x ∧ x ≤ hi1) ∨ (lo2 ≤ x ∧ x ≤ hi2) ∨ (lo3 ≤ x ∧ x ≤ hi3)) :
    min lo1 (min lo2 lo3) ≤ x ∧ x ≤ max hi1 (max hi2 hi3) := by
  omega

/-- **反例トラップ TPCH-1 (`envelope_missing_branch_pred_drops_rows_bug`)**:
    ある枝が `L` 側の条件を持たない（`P_2(L) = TRUE`）とき、条件を持つ枝の `P_1(L)` だけで
    `L` の包絡線を作ってしまうと、第 2 枝でマッチするはずの正当な行が消失する！ -/
theorem envelope_missing_branch_pred_drops_rows_bug :
    let l : Rel Int := [1, 2]
    let r : Rel Int := [10]
    let p1L : Int → SqlBool := fun x => if x == 1 then .tTrue else .tFalse
    let q2R : Int → SqlBool := fun y => if y == 10 then .tTrue else .tFalse
    let truePred := fun (x : Int) (y : Int) => SqlBool.or (p1L x) (q2R y)
    let exactOut := select (fun p => match p with | (x, some y) => truePred x y | _ => .tFalse)
                      (innerJoin (fun _ _ => .tTrue) l r)
    let buggyOut := select (fun p => match p with | (x, some y) => truePred x y | _ => .tFalse)
                      (innerJoin (fun _ _ => .tTrue) (select p1L l) r)
    exactOut ≠ buggyOut := by
  decide

/-!
## 2. TPCH-2 (Q13): `LEFT OUTER JOIN` 右側の先行集約と `UNIQUE NOT NULL` 左キーによる `GROUP BY` 消去
-/

private theorem filter_onEq_eq_filter_nonnull {K R : Type} [DecidableEq K]
    (ka : K) (keyR : R → SqlVal K) (onR : R → Bool) (r : Rel R) :
    r.filter (fun y => (SqlBool.and (sqlEq (some ka) (keyR y))
                         (if onR y then .tTrue else .tFalse)).inWhere) =
    (r.filter (fun y => onR y && (keyR y).isSome)).filter (fun y => decide (keyR y = some ka)) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro y _
  rw [and_inWhere, sqlEq_some_inWhere_dec]
  cases hky : keyR y <;> cases hon : onR y <;> simp [SqlBool.inWhere]

private theorem sqlCountCol_map_some {L R α : Type}
    (x : L) (colR : R → SqlVal α) (ms : Rel R) :
    sqlCountCol (fun p : L × Option R => match p.2 with | some y => colR y | none => none)
      (ms.map (fun y => (x, some y))) =
    sqlCountCol colR ms := by
  induction ms with
  | nil => rfl
  | cons y ys ih => simp [sqlCountCol_cons, ih]

private theorem find_in_groupBy_unique {Row K V : Type} [DecidableEq K]
    (key : Row → K) (fn : K → Rel Row → V) (r : Rel Row) (k : K) :
    (groupBy key fn r).filter (fun kv => decide (kv.1 = k)) =
    if (r.filter (fun row => decide (key row = k))).isEmpty then []
    else [(k, fn k (r.filter (fun row => decide (key row = k))))] := by
  unfold groupBy
  change List.filter (fun kv => decide (kv.1 = k))
      (List.map (fun k' => (k', fn k' (List.filter (fun row => decide (key row = k')) r)))
        (distinctKeys (List.map key r))) = _
  have hMemIff : k ∈ distinctKeys (r.map key) ↔ (r.filter (fun row => decide (key row = k))).isEmpty = false := by
    rw [mem_distinctKeys, List.mem_map]
    cases hBucket : r.filter (fun row => decide (key row = k)) with
    | nil =>
      simp only [List.isEmpty_nil, Bool.true_eq_false, iff_false, not_exists, not_and]
      intro row hRow hKey
      have hIn : row ∈ r.filter (fun row => decide (key row = k)) :=
        List.mem_filter.mpr ⟨hRow, decide_eq_true hKey⟩
      rw [hBucket] at hIn
      nomatch hIn
    | cons b bs =>
      simp only [List.isEmpty_cons, iff_true]
      have hb : b ∈ r.filter (fun row => decide (key row = k)) := by rw [hBucket]; exact List.Mem.head _
      rw [List.mem_filter] at hb
      exact ⟨b, hb.1, of_decide_eq_true hb.2⟩
  have hNodup := nodup_distinctKeys (r.map key)
  generalize distinctKeys (r.map key) = ks at hNodup hMemIff
  by_cases hEmp : (r.filter (fun row => decide (key row = k))).isEmpty = true
  · simp only [hEmp, ↓reduceIte]
    have hkNot : k ∉ ks := by
      intro hk
      have := hMemIff.mp hk
      rw [hEmp] at this
      cases this
    apply List.filter_eq_nil_iff.mpr
    intro kv hkv
    rw [List.mem_map] at hkv
    rcases hkv with ⟨k', hk', rfl⟩
    exact fun hD => hkNot (of_decide_eq_true hD ▸ hk')
  · simp only [hEmp, Bool.false_eq_true, ↓reduceIte]
    have hEmpF : (r.filter (fun row => decide (key row = k))).isEmpty = false :=
      Bool.eq_false_iff.mpr hEmp
    have hkIn : k ∈ ks := hMemIff.mpr hEmpF
    clear hMemIff hEmp hEmpF
    induction ks with
    | nil => nomatch hkIn
    | cons k1 ks1 ih =>
      rw [List.nodup_cons] at hNodup
      rw [List.mem_cons] at hkIn
      rcases hkIn with rfl | hkTail
      · simp only [List.map_cons, List.filter_cons, decide_true, ↓reduceIte]
        congr 1
        apply List.filter_eq_nil_iff.mpr
        intro kv hkv
        rw [List.mem_map] at hkv
        rcases hkv with ⟨k2, hk2, rfl⟩
        exact fun hD => hNodup.1 (of_decide_eq_true hD ▸ hk2)
      · have hne : decide (k1 = k) = false := decide_eq_false (fun h => hNodup.1 (h ▸ hkTail))
        simp only [List.map_cons, List.filter_cons, hne, Bool.false_eq_true, ↓reduceIte]
        exact ih hNodup.2 hkTail

/-- **定理 TPCH-2 (`left_join_right_preagg_and_unique_elide_groupby_sound`, Q13)**:
    左テーブル `l`（`customer`）の結合キー `keyL` が **`NOT NULL` かつ `UNIQUE`** であり、
    右テーブル `r`（`orders`）との `LEFT OUTER JOIN ... ON keyL = keyR AND onR(r)` の上で
    `GROUP BY keyL` と右列の NULL 厳密な `COUNT(colR)` を行うクエリは：
    1. `r` を先に `WHERE onR(r) AND keyR IS NOT NULL` でフィルタして `keyR` ごとに `COUNT(colR)` へ事前集約し（80B の `o_comment` をスキャン直後に破棄！）、
    2. `l` と `rAgg` を `LEFT OUTER JOIN` して `COALESCE(rAgg.cnt, 0)` を射影する **内側 `GROUP BY` ゼロ** の計画と **完全に同じリスト（`=`）** を返す！ -/
theorem left_join_right_preagg_and_unique_elide_groupby_sound {L R K α : Type} [DecidableEq K]
    (keyL : L → SqlVal K) (keyR : R → SqlVal K)
    (onR : R → Bool) (colR : R → SqlVal α)
    (l : Rel L) (r : Rel R)
    (hNonNullL : ∀ x ∈ l, (keyL x).isSome = true)
    (hUniqueL : (l.map keyL).Nodup) :
    let onPred := fun (x : L) (y : R) =>
      SqlBool.and (sqlEq (keyL x) (keyR y)) (if onR y then .tTrue else .tFalse)
    let joined := leftOuterJoin onPred l r
    let slowInner := groupBy (fun p => keyL p.1)
      (fun _ bucket => sqlCountCol (fun p => match p.2 with | some y => colR y | none => none) bucket)
      joined
    let rFiltered := r.filter (fun y => onR y && (keyR y).isSome)
    let rAgg := groupBy keyR (fun _ b => sqlCountCol colR b) rFiltered
    let fastNoGroupBy := (leftOuterJoin (fun x kv => sqlEq (keyL x) kv.1) l rAgg).map
      (fun p => (keyL p.1, match p.2 with | some kv => kv.2 | none => 0))
    slowInner = fastNoGroupBy := by
  dsimp only
  let onPred : L → R → SqlBool := fun (x : L) (y : R) =>
    SqlBool.and (sqlEq (keyL x) (keyR y)) (if onR y then .tTrue else .tFalse)
  let rFiltered : Rel R := r.filter (fun y => onR y && (keyR y).isSome)
  -- Step 1: `leftOuterJoin` 上の `distinctKeys` は `l.map keyL` そのもの
  have hMapJoined : ∀ (xs : Rel L),
      distinctKeys ((leftOuterJoin onPred xs r).map (fun p => keyL p.1)) = distinctKeys (xs.map keyL) := by
    intro xs
    induction xs with
    | nil => rfl
    | cons x0 xs0 ih =>
      unfold leftOuterJoin at ih ⊢
      simp only [List.flatMap_cons, List.map_append]
      generalize List.filter (fun y => (onPred x0 y).inWhere) r = ms
      cases ms with
      | nil =>
        simp only [List.isEmpty_nil, ↓reduceIte, List.map_cons, List.map_nil, List.singleton_append,
                   distinctKeys]
        rw [ih]
      | cons m msTail =>
        simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, List.map_map]
        have hComp : ((fun p : L × Option R => keyL p.1) ∘ fun y : R => (x0, some y)) = fun _ => keyL x0 := rfl
        rw [hComp, distinctKeys_const_nonempty_append (keyL x0) _ m msTail, ih]
        simp [distinctKeys]
  have hNodupId : ∀ (ks : List (SqlVal K)), ks.Nodup → distinctKeys ks = ks := by
    intro ks hN
    induction ks with
    | nil => rfl
    | cons k ks ih =>
      rw [List.nodup_cons] at hN
      rw [distinctKeys, ih hN.2]
      congr 1
      apply List.filter_eq_self.mpr
      intro k' hk'
      exact decide_eq_true (ne_of_mem_of_not_mem hk' hN.1)
  have hDistinctKeys : distinctKeys ((leftOuterJoin onPred l r).map (fun p => keyL p.1)) = l.map keyL :=
    (hMapJoined l).trans (hNodupId (l.map keyL) hUniqueL)
  -- Step 2: `l` がユニークキーを持つので、バケットは `x` 自身の行群のみ
  have hBucketOfX : ∀ (xs : Rel L), (xs.map keyL).Nodup → ∀ x ∈ xs,
      (leftOuterJoin onPred xs r).filter (fun p => decide (keyL p.1 = keyL x)) =
      (let ms := r.filter (fun y => (onPred x y).inWhere)
       if ms.isEmpty then [(x, none)] else ms.map (fun y => (x, some y))) := by
    intro xs hN x hx
    unfold leftOuterJoin
    induction xs with
    | nil => nomatch hx
    | cons x1 xs1 ih =>
      rw [List.map_cons, List.nodup_cons] at hN
      rw [List.mem_cons] at hx
      simp only [List.flatMap_cons, List.filter_append]
      rcases hx with rfl | hxTail
      · have hHeadSelf :
            List.filter (fun p : L × Option R => decide (keyL p.1 = keyL x))
              (let ms := r.filter (fun y => (onPred x y).inWhere)
               if ms.isEmpty then [(x, none)] else ms.map (fun y => (x, some y))) =
            (let ms := r.filter (fun y => (onPred x y).inWhere)
             if ms.isEmpty then [(x, none)] else ms.map (fun y => (x, some y))) := by
          apply List.filter_eq_self.mpr
          intro p hp
          generalize r.filter (fun y => (onPred x y).inWhere) = ms at hp
          cases ms with
          | nil =>
            simp only [List.isEmpty_nil, ↓reduceIte, List.mem_singleton] at hp
            subst hp; exact decide_eq_true rfl
          | cons m msT =>
            simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, List.mem_map] at hp
            rcases hp with ⟨_, _, rfl⟩; exact decide_eq_true rfl
        have hTailEmpty :
            List.filter (fun p : L × Option R => decide (keyL p.1 = keyL x))
              (xs1.flatMap fun x' =>
                let ms := r.filter (fun y => (onPred x' y).inWhere)
                if ms.isEmpty then [(x', none)] else ms.map (fun y => (x', some y))) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro p hp
          rw [List.mem_flatMap] at hp
          rcases hp with ⟨x2, hx2, hp2⟩
          have hne : keyL x2 ≠ keyL x := fun hEq => hN.1 (hEq ▸ List.mem_map_of_mem hx2)
          generalize r.filter (fun y => (onPred x2 y).inWhere) = ms2 at hp2
          cases ms2 with
          | nil =>
            simp only [List.isEmpty_nil, ↓reduceIte, List.mem_singleton] at hp2
            subst hp2; exact fun hD => hne (of_decide_eq_true hD)
          | cons m2 ms2T =>
            simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, List.mem_map] at hp2
            rcases hp2 with ⟨_, _, rfl⟩; exact fun hD => hne (of_decide_eq_true hD)
        rw [hHeadSelf, hTailEmpty, List.append_nil]
      · have hne : keyL x1 ≠ keyL x := fun hEq => hN.1 (hEq ▸ List.mem_map_of_mem hxTail)
        have hHeadEmpty :
            List.filter (fun p : L × Option R => decide (keyL p.1 = keyL x))
              (let ms := r.filter (fun y => (onPred x1 y).inWhere)
               if ms.isEmpty then [(x1, none)] else ms.map (fun y => (x1, some y))) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro p hp
          generalize r.filter (fun y => (onPred x1 y).inWhere) = ms1 at hp
          cases ms1 with
          | nil =>
            simp only [List.isEmpty_nil, ↓reduceIte, List.mem_singleton] at hp
            subst hp; exact fun hD => hne (of_decide_eq_true hD)
          | cons m1 ms1T =>
            simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, List.mem_map] at hp
            rcases hp with ⟨_, _, rfl⟩; exact fun hD => hne (of_decide_eq_true hD)
        rw [hHeadEmpty, List.nil_append]
        exact ih hN.2 hxTail
  -- Step 3: 両辺を `l.map (...)` へ書き換えて一致させる
  have hSlowRewrite :
      groupBy (fun p : L × Option R => keyL p.1)
        (fun _ bucket => sqlCountCol (fun p => match p.2 with | some y => colR y | none => none) bucket)
        (leftOuterJoin onPred l r) =
      l.map (fun x =>
        (keyL x,
         let ms := r.filter (fun y => (onPred x y).inWhere)
         sqlCountCol (fun p : L × Option R => match p.2 with | some y => colR y | none => none)
           (if ms.isEmpty then [(x, none)] else ms.map (fun y => (x, some y))))) := by
    unfold groupBy
    rw [hDistinctKeys, List.map_map]
    apply List.map_congr_left
    intro x hx
    simp only [Function.comp_apply]
    congr 1
    congr 1
    exact hBucketOfX l hUniqueL x hx
  have hPerX : ∀ x ∈ l,
      List.map (fun p : L × Option (SqlVal K × Int) =>
          (keyL p.1, match p.2 with | some kv => kv.2 | none => 0))
        (let matches_ := (groupBy keyR (fun _ b => sqlCountCol colR b) rFiltered).filter
            (fun kv => (sqlEq (keyL x) kv.1).inWhere)
         if matches_.isEmpty then [(x, none)] else matches_.map (fun y => (x, some y))) =
      [(keyL x,
        let ms := r.filter (fun y => (onPred x y).inWhere)
        sqlCountCol (fun p : L × Option R => match p.2 with | some y => colR y | none => none)
          (if ms.isEmpty then [(x, none)] else ms.map (fun y => (x, some y))))] := by
    intro x hx
    have hxSome := hNonNullL x hx
    cases hkx : keyL x with
    | none => simp [hkx] at hxSome
    | some ka =>
      have hEqRagg :
          (groupBy keyR (fun _ b => sqlCountCol colR b) rFiltered).filter
            (fun kv => (sqlEq (some ka) kv.1).inWhere) =
          (groupBy keyR (fun _ b => sqlCountCol colR b) rFiltered).filter
            (fun kv => decide (kv.1 = some ka)) := by
        apply List.filter_congr
        intro kv _
        exact sqlEq_some_inWhere_dec ka kv.1
      have hFind := find_in_groupBy_unique keyR (fun _ b => sqlCountCol colR b) rFiltered (some ka)
      have hBucketEq : r.filter (fun y => (onPred x y).inWhere) =
          rFiltered.filter (fun y => decide (keyR y = some ka)) := by
        simpa [onPred, hkx, rFiltered] using filter_onEq_eq_filter_nonnull ka keyR onR r
      dsimp only
      rw [hEqRagg, hFind, ← hBucketEq]
      generalize r.filter (fun y => (onPred x y).inWhere) = ms
      cases ms with
      | nil => simp [hkx, sqlCountCol, countMonoid]
      | cons m msT =>
        have hR :
            sqlCountCol (fun p : L × Option R => match p.2 with | some y => colR y | none => none)
              ((m :: msT).map (fun y => (x, some y))) =
            sqlCountCol colR (m :: msT) :=
          sqlCountCol_map_some x colR (m :: msT)
        have hGoalL :
            List.map (fun p : L × Option (SqlVal K × Int) =>
              (keyL p.1, match p.2 with | some kv => kv.2 | none => 0))
              (if (if (m :: msT).isEmpty then [] else [(some ka, sqlCountCol colR (m :: msT))]).isEmpty then [(x, none)]
               else List.map (fun y => (x, some y))
                 (if (m :: msT).isEmpty then [] else [(some ka, sqlCountCol colR (m :: msT))])) =
            [(some ka, sqlCountCol colR (m :: msT))] := by
          simp [hkx]
        rw [hGoalL]
        simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte]
        rw [hR]
  have hFastRewrite : ∀ (xs : Rel L),
      (∀ x ∈ xs,
        List.map (fun p : L × Option (SqlVal K × Int) =>
            (keyL p.1, match p.2 with | some kv => kv.2 | none => 0))
          (let matches_ := (groupBy keyR (fun _ b => sqlCountCol colR b) rFiltered).filter
              (fun kv => (sqlEq (keyL x) kv.1).inWhere)
           if matches_.isEmpty then [(x, none)] else matches_.map (fun y => (x, some y))) =
        [(keyL x,
          let ms := r.filter (fun y => (onPred x y).inWhere)
          sqlCountCol (fun p : L × Option R => match p.2 with | some y => colR y | none => none)
            (if ms.isEmpty then [(x, none)] else ms.map (fun y => (x, some y))))]) →
      (leftOuterJoin (fun x kv => sqlEq (keyL x) kv.1) xs
        (groupBy keyR (fun _ b => sqlCountCol colR b) rFiltered)).map
        (fun p => (keyL p.1, match p.2 with | some kv => kv.2 | none => 0)) =
      xs.map (fun x =>
        (keyL x,
         let ms := r.filter (fun y => (onPred x y).inWhere)
         sqlCountCol (fun p : L × Option R => match p.2 with | some y => colR y | none => none)
           (if ms.isEmpty then [(x, none)] else ms.map (fun y => (x, some y))))) := by
    intro xs hP
    unfold leftOuterJoin
    induction xs with
    | nil => rfl
    | cons x rest ih =>
      simp only [List.flatMap_cons, List.map_append, List.map_cons]
      rw [hP x (List.Mem.head _), List.singleton_append]
      congr 1
      exact ih (fun z hz => hP z (List.Mem.tail _ hz))
  exact hSlowRewrite.trans (hFastRewrite l hPerX).symm

/-- **反例トラップ TPCH-2a (`nullable_unique_left_elides_groupby_bug`)**:
    SQL の `UNIQUE` 制約（`NOT NULL` 無し）では左キーに `NULL` が 2 行入りうる。
    このとき内側 `GROUP BY c_custkey` は 2 つの `NULL` を **1 グループ**（`c_count = 0` が 1 件）に畳むが、
    `GROUP BY` を省略すると `c_count = 0` が **2 件** になり、Q13 外側の度数分布が破壊される！ -/
theorem nullable_unique_left_elides_groupby_bug :
    let l : Rel (SqlVal Int) := [none, none]
    let r : Rel (SqlVal Int × SqlVal Int) := []
    let joined := leftOuterJoin (fun x y => sqlEq x y.1) l r
    let exactInner := groupBy (fun p => p.1)
      (fun _ b => sqlCountCol (fun p => match p.2 with | some y => y.2 | none => none) b) joined
    let buggyElided := joined.map (fun p => (p.1, match p.2 with | some _ => (1 : Int) | none => 0))
    exactInner.length = 1 ∧ buggyElided.length = 2 := by
  decide

/-- **反例トラップ TPCH-2b (`left_join_count_nonstrict_expr_bug`)**:
    `COUNT(COALESCE(o_orderkey, -1))` のように右行が `NULL` 補完されても非 `NULL` を返す式では、
    未マッチ左行のカウントは `1` になるため、`COALESCE(rAgg.cnt, 0)`（`0`）に書き換えると誤答する！ -/
theorem left_join_count_nonstrict_expr_bug :
    let l : Rel (SqlVal Int) := [some 1]
    let r : Rel (SqlVal Int × SqlVal Int) := []
    let nonStrictExpr : Option (SqlVal Int × SqlVal Int) → SqlVal Int :=
      fun opt => some (sqlCoalesce (match opt with | some y => y.2 | none => none) (-1))
    let exactCnt := sqlCountCol (fun p => nonStrictExpr p.2)
                      (leftOuterJoin (fun x y => sqlEq x y.1) l r)
    exactCnt = 1 ∧ exactCnt ≠ 0 := by
  decide

/-!
## 3. TPCH-3 (Q18): 自己準結合 `IN (SELECT k FROM T GROUP BY k HAVING P(SUM))` 集約の 1 パス吸収
-/

private theorem flatMap_const_sqlSum {α T : Type} (valT : T → SqlVal Int) (tBucket : Rel T) :
    ∀ {oRows : List α}, oRows ≠ [] →
      sqlSum (fun p : α × T => valT p.2) (oRows.flatMap (fun o => tBucket.map (fun t => (o, t)))) =
      aggFold sumMonoid (fun _ => sqlSum valT tBucket) oRows := by
  intro oRows _
  unfold sqlSum
  rw [aggFold_flatMap]
  congr 1
  funext o
  rw [aggFold_map]

/-- **定理 TPCH-3 (`self_semijoin_having_agg_absorption_sound`, Q18)**:
    外側リレーション `oRel`（`customer ⋈ orders`、重複行があってもよい！）とファクト表 `tRel`（`lineitem`）を
    `keyO o = keyT t` で結合し、さらに自己準結合 `keyO o IN (SELECT keyT FROM tRel WHERE wT GROUP BY keyT HAVING hPred(SUM(valT)))`
    を課して `(grpO o, keyO o)` で `GROUP BY` し `SUM(valT)` を求めるクエリは：
    1. `tRel` を **1 回だけ** スキャンして `WHERE wT AND keyT IS NOT NULL`・`GROUP BY keyT HAVING hPred(SUM(valT))` へ集約した小さな導出表 `dRel`（Q18 SF=10 で 6,000 万行 → 約 620 行！）を作り、
    2. `oRel ⋈ dRel` の上で `GROUP BY (grpO o, keyO o)` し `SUM(dRel.sum_val)` を取る計画と **完全に同じ結果リスト（`=`）** を返す！ -/
theorem self_semijoin_having_agg_absorption_sound {O T K G : Type} [DecidableEq K] [DecidableEq G]
    (grpO : O → G) (keyO : O → SqlVal K)
    (keyT : T → SqlVal K) (wT : T → Bool) (valT : T → SqlVal Int)
    (hPred : SqlVal Int → Bool)
    (oRel : Rel O) (tRel : Rel T) :
    let tFiltered := tRel.filter (fun t => wT t && (keyT t).isSome)
    let subGroups := (groupBy keyT (fun _ b => sqlSum valT b) (tRel.filter wT)).filter (fun kv => hPred kv.2)
    let inSub := fun (ko : SqlVal K) => subGroups.any (fun kv => (sqlEq ko kv.1).inWhere)
    let rawJoined := oRel.flatMap (fun o =>
      if inSub (keyO o) then
        (tRel.filter (fun t => wT t && (sqlEq (keyO o) (keyT t)).inWhere)).map (fun t => (o, t))
      else [])
    let origOut := groupBy (fun p => (grpO p.1, keyO p.1))
      (fun _ b => sqlSum (fun p => valT p.2) b) rawJoined
    let dRel := (groupBy keyT (fun _ b => sqlSum valT b) tFiltered).filter (fun kv => hPred kv.2)
    let absJoined := oRel.flatMap (fun o =>
      (dRel.filter (fun d => (sqlEq (keyO o) d.1).inWhere)).map (fun d => (o, d)))
    let optOut := groupBy (fun p => (grpO p.1, keyO p.1))
      (fun _ b => sqlSum (fun p => p.2.2) b) absJoined
    origOut = optOut := by
  dsimp only
  let tFiltered : Rel T := tRel.filter (fun t => wT t && (keyT t).isSome)
  let subGroups := (groupBy keyT (fun _ b => sqlSum valT b) (tRel.filter wT)).filter (fun kv => hPred kv.2)
  let inSub : SqlVal K → Bool := fun ko => subGroups.any (fun kv => (sqlEq ko kv.1).inWhere)
  let dRel := (groupBy keyT (fun _ b => sqlSum valT b) tFiltered).filter (fun kv => hPred kv.2)
  -- 補題 A: 非 NULL キー `some k` に対して、`tRel.filter wT` と `tFiltered` のバケットは完全一致
  have hNonNullBucket : ∀ k : K,
      (tRel.filter wT).filter (fun t => decide (keyT t = some k)) =
      tFiltered.filter (fun t => decide (keyT t = some k)) := by
    intro k
    dsimp only [tFiltered]
    rw [List.filter_filter, List.filter_filter]
    apply List.filter_congr
    intro t _
    cases hkt : keyT t <;> simp
  -- 補題 B: 任意の `o` について、`dRel.filter (sqlEq (keyO o) d.1)` の特徴づけ
  have hDrelLookup : ∀ o : O,
      dRel.filter (fun d => (sqlEq (keyO o) d.1).inWhere) =
      match keyO o with
      | none => []
      | some k =>
        let b := tFiltered.filter (fun t => decide (keyT t = some k))
        if !b.isEmpty && hPred (sqlSum valT b) then [(some k, sqlSum valT b)] else [] := by
    intro o
    cases hko : keyO o with
    | none =>
      apply List.filter_eq_nil_iff.mpr
      intro d _; simp [sqlEq, SqlBool.inWhere]
    | some k =>
      have hSub : dRel.filter (fun d => (sqlEq (some k) d.1).inWhere) =
          ((groupBy keyT (fun _ b => sqlSum valT b) tFiltered).filter (fun d => decide (d.1 = some k))).filter
            (fun d => hPred d.2) := by
        dsimp only [dRel]
        rw [List.filter_filter, List.filter_filter]
        apply List.filter_congr
        intro d _
        rw [sqlEq_some_inWhere_dec k d.1]
        exact Bool.and_comm _ _
      rw [hSub, find_in_groupBy_unique]
      dsimp only
      generalize tFiltered.filter (fun t => decide (keyT t = some k)) = bk
      cases bk with
      | nil => simp
      | cons t0 ts =>
        cases hp : hPred (sqlSum valT (t0 :: ts)) <;>
          simp only [List.isEmpty_cons, Bool.not_false, Bool.true_and, List.filter_cons, List.filter_nil,
                     hp, Bool.false_eq_true, ↓reduceIte]
  -- 補題 C: `inSub (some k)` の特徴づけ
  have hInSubIff : ∀ k : K,
      inSub (some k) = (!((tFiltered.filter (fun t => decide (keyT t = some k))).isEmpty) &&
                        hPred (sqlSum valT (tFiltered.filter (fun t => decide (keyT t = some k))))) := by
    intro k
    dsimp only [inSub, subGroups]
    have h1 : ∀ xs : List (SqlVal K × SqlVal Int),
        (xs.filter (fun kv => hPred kv.2)).any (fun kv => (sqlEq (some k) kv.1).inWhere) =
        !((xs.filter (fun kv => decide (kv.1 = some k))).filter (fun kv => hPred kv.2)).isEmpty := by
      intro xs
      induction xs with
      | nil => rfl
      | cons kv rest ih =>
        by_cases hp : hPred kv.2 = true
        · simp only [List.filter_cons, hp, ↓reduceIte, List.any_cons]
          rw [sqlEq_some_inWhere_dec k kv.1, ih]
          by_cases hEq : decide (kv.1 = some k) = true <;> simp [hEq, hp]
        · have hpF : hPred kv.2 = false := Bool.eq_false_iff.mpr hp
          by_cases hEq : decide (kv.1 = some k) = true <;> simp [hpF, hEq, ih]
    rw [h1, find_in_groupBy_unique, hNonNullBucket k]
    generalize tFiltered.filter (fun t => decide (keyT t = some k)) = bk
    cases bk with
    | nil => simp
    | cons t0 ts =>
      cases hp : hPred (sqlSum valT (t0 :: ts))
      · simp only [List.isEmpty_cons, Bool.not_false, Bool.true_and, List.filter_cons, List.filter_nil,
                   hp, Bool.false_eq_true, ↓reduceIte]
        rfl
      · simp only [List.isEmpty_cons, Bool.not_false, Bool.true_and, List.filter_cons, List.filter_nil,
                   hp, Bool.false_eq_true, ↓reduceIte]
  have hInNone : inSub none = false := by
    dsimp only [inSub]
    apply List.any_eq_false.mpr
    intro kv _; simp [sqlEq, SqlBool.inWhere]
  -- 補題 D: `o` ごとの展開キー列と部分バケットの比較
  have hMatchRowsEq : ∀ (k : K),
      tRel.filter (fun t => wT t && (sqlEq (some k) (keyT t)).inWhere) =
      tFiltered.filter (fun t => decide (keyT t = some k)) := by
    intro k
    dsimp only [tFiltered]
    rw [List.filter_filter]
    apply List.filter_congr
    intro t _
    rw [sqlEq_some_inWhere_dec k (keyT t)]
    cases hkt : keyT t <;> cases hw : wT t <;> simp
  have hDistinctEq : ∀ (os : Rel O),
      distinctKeys ((os.flatMap (fun o =>
        if inSub (keyO o) then
          (tRel.filter (fun t => wT t && (sqlEq (keyO o) (keyT t)).inWhere)).map (fun t => (o, t))
        else [])).map (fun p => (grpO p.1, keyO p.1))) =
      distinctKeys ((os.flatMap (fun o =>
        (dRel.filter (fun d => (sqlEq (keyO o) d.1).inWhere)).map (fun d => (o, d)))).map
        (fun p => (grpO p.1, keyO p.1))) := by
    intro os
    induction os with
    | nil => rfl
    | cons o rest ih =>
      simp only [List.flatMap_cons, List.map_append]
      rw [hDrelLookup o]
      cases hko : keyO o with
      | none =>
        simp [hInNone, ih]
      | some k =>
        dsimp only
        rw [hInSubIff k, hMatchRowsEq k]
        generalize tFiltered.filter (fun t => decide (keyT t = some k)) = bk
        cases bk with
        | nil =>
          simp only [List.isEmpty_nil, Bool.not_true, Bool.false_and, Bool.false_eq_true, ↓reduceIte,
                     List.map_nil, List.nil_append]
          exact ih
        | cons t0 ts =>
          by_cases hp : hPred (sqlSum valT (t0 :: ts)) = true
          · simp only [List.isEmpty_cons, Bool.not_false, hp, Bool.and_self, ↓reduceIte,
                       List.map_map]
            have hComp1 : ((fun p : O × T => (grpO p.1, keyO p.1)) ∘ fun t : T => (o, t)) =
                fun _ => (grpO o, some k) := by funext _; simp [hko]
            have hComp2 : ((fun p : O × (SqlVal K × SqlVal Int) => (grpO p.1, keyO p.1)) ∘ fun d => (o, d)) =
                fun _ => (grpO o, some k) := by funext _; simp [hko]
            rw [hComp1, hComp2,
                distinctKeys_const_nonempty_append (grpO o, some k) _ t0 ts,
                distinctKeys_const_nonempty_append (grpO o, some k) _ (some k, sqlSum valT (t0 :: ts)) [],
                ih]
          · have hpF : hPred (sqlSum valT (t0 :: ts)) = false := Bool.eq_false_iff.mpr hp
            simp only [List.isEmpty_cons, Bool.not_false, hpF, Bool.and_false, Bool.false_eq_true,
                       ↓reduceIte, List.map_nil, List.nil_append]
            exact ih
  -- 補題 E: 各グループキー `gk = (g, ko)` に対して `sqlSum` が一致
  change groupBy (fun p => (grpO p.1, keyO p.1)) (fun _ b => sqlSum (fun p => valT p.2) b)
      (oRel.flatMap (fun o =>
        if inSub (keyO o) then
          (tRel.filter (fun t => wT t && (sqlEq (keyO o) (keyT t)).inWhere)).map (fun t => (o, t))
        else [])) =
    groupBy (fun p => (grpO p.1, keyO p.1)) (fun _ b => sqlSum (fun p => p.2.2) b)
      (oRel.flatMap (fun o => (dRel.filter (fun d => (sqlEq (keyO o) d.1).inWhere)).map (fun d => (o, d))))
  rw [groupBy, groupBy, hDistinctEq oRel]
  apply List.map_congr_left
  intro gk hgk
  congr 1
  rcases gk with ⟨g, optK⟩
  cases optK with
  | none =>
    have hNotNone : (g, (none : SqlVal K)) ∉ distinctKeys
        ((oRel.flatMap (fun o =>
          (dRel.filter (fun d => (sqlEq (keyO o) d.1).inWhere)).map (fun d => (o, d)))).map
          (fun p => (grpO p.1, keyO p.1))) := by
      rw [mem_distinctKeys, List.mem_map]
      rintro ⟨⟨o, d⟩, hMem, hEq⟩
      rw [List.mem_flatMap] at hMem
      rcases hMem with ⟨o', _, hMem2⟩
      rw [hDrelLookup o'] at hMem2
      cases hko' : keyO o' with
      | none => simp [hko'] at hMem2
      | some k' =>
        simp only [hko'] at hMem2
        split at hMem2
        · simp only [List.map_cons, List.map_nil, List.mem_singleton] at hMem2
          rcases hMem2 with ⟨rfl, _⟩
          have := congrArg Prod.snd hEq
          simp [hko'] at this
        · nomatch hMem2
    exact absurd hgk hNotNone
  | some k =>
    let bk := tFiltered.filter (fun t => decide (keyT t = some k))
    have hRawFilter : ∀ (os : Rel O),
        (os.flatMap (fun o =>
          if inSub (keyO o) then
            (tRel.filter (fun t => wT t && (sqlEq (keyO o) (keyT t)).inWhere)).map (fun t => (o, t))
          else [])).filter (fun row => decide ((grpO row.1, keyO row.1) = (g, some k))) =
        (os.filter (fun o => decide ((grpO o, keyO o) = (g, some k)))).flatMap (fun o =>
          if !bk.isEmpty && hPred (sqlSum valT bk) then bk.map (fun t => (o, t)) else []) := by
      intro os
      induction os with
      | nil => rfl
      | cons o rest ih =>
        simp only [List.flatMap_cons, List.filter_append, List.filter_cons]
        rw [ih]
        by_cases hMatchO : decide ((grpO o, keyO o) = (g, some k)) = true
        · have hPair := of_decide_eq_true hMatchO
          have hEqG : grpO o = g := congrArg Prod.fst hPair
          have hEqK : keyO o = some k := congrArg Prod.snd hPair
          simp only [hMatchO, ↓reduceIte, List.flatMap_cons]
          congr 1
          rw [hEqK, hInSubIff k, hMatchRowsEq k]
          dsimp only [bk]
          by_cases hBkCond : (!(tFiltered.filter (fun t => decide (keyT t = some k))).isEmpty &&
              hPred (sqlSum valT (tFiltered.filter (fun t => decide (keyT t = some k))))) = true
          · simp only [hBkCond, ↓reduceIte]
            apply List.filter_eq_self.mpr
            intro p hp; rw [List.mem_map] at hp; rcases hp with ⟨_, _, rfl⟩
            exact hMatchO
          · simp [hBkCond]
        · simp only [hMatchO, Bool.false_eq_true, ↓reduceIte]
          have hLeftNil : List.filter (fun row : O × T => decide ((grpO row.1, keyO row.1) = (g, some k)))
              (if inSub (keyO o) = true then
                List.map (fun t => (o, t)) (tRel.filter fun t => wT t && (sqlEq (keyO o) (keyT t)).inWhere)
               else []) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro p hp
            split at hp
            · rw [List.mem_map] at hp; rcases hp with ⟨_, _, rfl⟩
              exact hMatchO
            · nomatch hp
          rw [hLeftNil, List.nil_append]
    have hAbsFilter : ∀ (os : Rel O),
        (os.flatMap (fun o =>
          (dRel.filter (fun d => (sqlEq (keyO o) d.1).inWhere)).map (fun d => (o, d)))).filter
          (fun row => decide ((grpO row.1, keyO row.1) = (g, some k))) =
        (os.filter (fun o => decide ((grpO o, keyO o) = (g, some k)))).flatMap (fun o =>
          if !bk.isEmpty && hPred (sqlSum valT bk) then [(o, (some k, sqlSum valT bk))] else []) := by
      intro os
      induction os with
      | nil => rfl
      | cons o rest ih =>
        simp only [List.flatMap_cons, List.filter_append, List.filter_cons]
        rw [ih, hDrelLookup o]
        by_cases hMatchO : decide ((grpO o, keyO o) = (g, some k)) = true
        · have hPair := of_decide_eq_true hMatchO
          have hEqK : keyO o = some k := congrArg Prod.snd hPair
          simp only [hMatchO, ↓reduceIte, List.flatMap_cons]
          congr 1
          rw [hEqK]
          dsimp only [bk]
          by_cases hBkCond : (!(tFiltered.filter (fun t => decide (keyT t = some k))).isEmpty &&
              hPred (sqlSum valT (tFiltered.filter (fun t => decide (keyT t = some k))))) = true
          · simp only [hBkCond, ↓reduceIte, List.map_cons, List.map_nil, List.filter_cons,
                       hMatchO, List.filter_nil]
          · simp [hBkCond]
        · simp only [hMatchO, Bool.false_eq_true, ↓reduceIte]
          have hLeftNil : List.filter (fun row : O × (SqlVal K × SqlVal Int) =>
              decide ((grpO row.1, keyO row.1) = (g, some k)))
              (List.map (fun d => (o, d))
                (match keyO o with
                 | none => []
                 | some ko =>
                   let b := tFiltered.filter (fun t => decide (keyT t = some ko))
                   if !b.isEmpty && hPred (sqlSum valT b) then [(some ko, sqlSum valT b)] else [])) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro p hp
            rw [List.mem_map] at hp; rcases hp with ⟨_, _, rfl⟩
            exact hMatchO
          rw [hLeftNil, List.nil_append]
    change sqlSum (fun p : O × T => valT p.2)
        (List.filter (fun row : O × T => decide ((grpO row.1, keyO row.1) = (g, some k))) _) =
      sqlSum (fun p : O × (SqlVal K × SqlVal Int) => p.2.2)
        (List.filter (fun row : O × (SqlVal K × SqlVal Int) => decide ((grpO row.1, keyO row.1) = (g, some k))) _)
    rw [hRawFilter oRel, hAbsFilter oRel]
    generalize oRel.filter (fun o => decide ((grpO o, keyO o) = (g, some k))) = oMatch
    by_cases hCond : (!bk.isEmpty && hPred (sqlSum valT bk)) = true
    · simp only [hCond, ↓reduceIte]
      cases oMatch with
      | nil => rfl
      | cons o0 os =>
        rw [flatMap_const_sqlSum valT bk (List.cons_ne_nil o0 os)]
        unfold sqlSum
        rw [aggFold_flatMap]
        congr 1
        funext x
        exact (sumMonoid.op_zero _).symm
    · have hCondF : (!bk.isEmpty && hPred (sqlSum valT bk)) = false :=
        Bool.eq_false_iff.mpr hCond
      have hNil1 : (oMatch.flatMap fun o =>
          if (!bk.isEmpty && hPred (sqlSum valT bk)) = true then bk.map (fun t => (o, t)) else []) = [] := by
        simp [hCondF]
      have hNil2 : (oMatch.flatMap fun o =>
          if (!bk.isEmpty && hPred (sqlSum valT bk)) = true then [(o, (some k, sqlSum valT bk))] else []) = [] := by
        simp [hCondF]
      rw [hNil1, hNil2]
      rfl

/-- **反例トラップ TPCH-3 (`self_semijoin_mismatched_where_bug`)**:
    外側 `WHERE` のみに追加の `T` 条件 `extraT`（例: `l_returnflag = 'R'`）があるとき、
    `IN` サブクエリ側にも `extraT` を混ぜて 1 パス吸収すると `HAVING SUM(l_quantity) > 230` の合否が変わる！ -/
theorem self_semijoin_mismatched_where_bug :
    let tRel : Rel (Int × Bool × Int) := [(100, true, 150), (100, false, 100)]
    let subTotal := sqlSum (fun r => some r.2.2) tRel
    let subPasses := match subTotal with | some s => decide (s > 230) | none => false
    let exactOuterSum : SqlVal Int :=
      if subPasses then sqlSum (fun r => some r.2.2) (tRel.filter (fun r => r.2.1)) else none
    let filteredTotal := sqlSum (fun r => some r.2.2) (tRel.filter (fun r => r.2.1))
    let buggyPasses := match filteredTotal with | some s => decide (s > 230) | none => false
    let buggySum : SqlVal Int := if buggyPasses then filteredTotal else none
    exactOuterSum = some 150 ∧ buggySum = none := by
  decide

/-!
## 4. TPCH-4 (Q15 & Q11): 共有 CTE 最大値タイ還元 (`WITH TIES`) & `HAVING` 全体和 Fubini 還元
-/

private theorem le_sqlMax_of_mem {Row : Type} (val : Row → SqlVal Int) :
    ∀ {r : Rel Row} {x : Row} {v : Int},
      x ∈ r → val x = some v → ∃ m : Int, sqlMax val r = some m ∧ v ≤ m
  | [], _, _, hx, _ => nomatch hx
  | y :: ys, x, v, hx, hv => by
    rw [List.mem_cons] at hx
    rcases hx with rfl | hTail
    · simp only [sqlMax_cons, hv]
      cases hRest : sqlMax val ys with
      | none => exact ⟨v, rfl, Int.le_refl v⟩
      | some m2 => exact ⟨max v m2, rfl, Int.le_max_left v m2⟩
    · obtain ⟨m2, hm2, hle⟩ := le_sqlMax_of_mem val hTail hv
      simp only [sqlMax_cons, hm2]
      cases hy : val y with
      | none => exact ⟨m2, rfl, hle⟩
      | some vy => exact ⟨max vy m2, rfl, Int.le_trans hle (Int.le_max_right vy m2)⟩

private theorem exists_mem_of_sqlMax_some {Row : Type} (val : Row → SqlVal Int) :
    ∀ {r : Rel Row} {m : Int},
      sqlMax val r = some m → ∃ x ∈ r, val x = some m
  | [], _, h => nomatch h
  | y :: ys, m, h => by
    simp only [sqlMax_cons] at h
    cases hy : val y with
    | none =>
      rw [hy] at h
      cases hRest : sqlMax val ys with
      | none => rw [hRest] at h; nomatch h
      | some m2 =>
        rw [hRest] at h
        simp only [mergeMax, Option.some.injEq] at h
        subst h
        obtain ⟨x, hx, hvx⟩ := exists_mem_of_sqlMax_some val hRest
        exact ⟨x, List.Mem.tail _ hx, hvx⟩
    | some vy =>
      rw [hy] at h
      cases hRest : sqlMax val ys with
      | none =>
        rw [hRest] at h
        simp only [mergeMax, Option.some.injEq] at h
        subst h
        exact ⟨y, List.Mem.head _, hy⟩
      | some m2 =>
        rw [hRest] at h
        simp only [mergeMax, Option.some.injEq] at h
        by_cases hge : vy ≤ m2
        · have hmax : max vy m2 = m2 := Int.max_eq_right hge
          rw [hmax] at h; subst h
          obtain ⟨x, hx, hvx⟩ := exists_mem_of_sqlMax_some val hRest
          exact ⟨x, List.Mem.tail _ hx, hvx⟩
        · have hmax : max vy m2 = vy := Int.max_eq_left (Int.le_of_not_le hge)
          rw [hmax] at h; subst h
          exact ⟨y, List.Mem.head _, hy⟩

/-- **定理 TPCH-4a (`cte_argmax_subquery_to_nonnull_max_ties_sound`, Q15)**:
    CTE / テーブル `r`（Q15 の `revenue`）に対して `val row = (SELECT MAX(val) FROM r)` でフィルタする操作は、
    `val row IS NOT NULL` を満たす行のうち「他のどの非 NULL 行よりも小さくない（上界タイ集合 =
    `WHERE val IS NOT NULL ORDER BY val DESC FETCH FIRST 1 ROWS WITH TIES`）」を残す 1 パス操作と
    **行リストとして完全一致（`=`）** する！これにより `revenue` の二重参照が消え 1 パス実行できる。 -/
theorem cte_argmax_subquery_to_nonnull_max_ties_sound {Row : Type}
    (val : Row → SqlVal Int) (r : Rel Row) :
    let maxSub := sqlMax val r
    let orig := select (fun row => sqlEq (val row) maxSub) r
    let nonNullR := r.filter (fun row => (val row).isSome)
    let topTies := nonNullR.filter (fun row =>
      match val row with
      | none => false
      | some v => nonNullR.all (fun row2 => match val row2 with | none => true | some v2 => decide (v2 ≤ v)))
    orig = topTies := by
  dsimp only
  unfold select
  rw [List.filter_filter]
  apply List.filter_congr
  intro row hRow
  dsimp only
  cases hv : val row with
  | none =>
    simp [sqlEq, SqlBool.inWhere]
  | some v =>
    obtain ⟨m, hm, hvm⟩ := le_sqlMax_of_mem val hRow hv
    rw [hm]
    simp only [Option.isSome_some, Bool.and_true]
    by_cases hEq : v = m
    · subst hEq
      have hTrueL : (sqlEq (some v) (some v)).inWhere = true := by simp [sqlEq, SqlBool.inWhere]
      rw [hTrueL]
      symm
      apply List.all_eq_true.mpr
      intro row2 hRow2
      rw [List.mem_filter] at hRow2
      cases hv2 : val row2 with
      | none => rfl
      | some v2 =>
        obtain ⟨m', hm', hv2m'⟩ := le_sqlMax_of_mem val hRow2.1 hv2
        rw [hm] at hm'
        cases hm'
        exact decide_eq_true hv2m'
    · have hFalseL : (sqlEq (some v) (some m)).inWhere = false := by
        simp [sqlEq, hEq, SqlBool.inWhere]
      rw [hFalseL]
      symm
      apply Bool.eq_false_iff.mpr
      intro hAll
      obtain ⟨wRow, hwMem, hwVal⟩ := exists_mem_of_sqlMax_some val hm
      have hwInNN : wRow ∈ r.filter (fun r0 => (val r0).isSome) :=
        List.mem_filter.mpr ⟨hwMem, by simp [hwVal]⟩
      have hCheck := List.all_eq_true.mp hAll wRow hwInNN
      simp only [hwVal, decide_eq_true_eq] at hCheck
      exact hEq (Int.le_antisymm hvm hCheck)

/-- **反例トラップ TPCH-4a (`argmax_with_ties_without_is_not_null_bug`)**:
    `WHERE val IS NOT NULL` を付けずに `ORDER BY val DESC FETCH FIRST 1 ROWS WITH TIES`（あるいは `RANK() = 1`）
    に変換すると、全行 `val = NULL` のテーブルで本来 **0 行** のはずが **全行誤出力** される！ -/
theorem argmax_with_ties_without_is_not_null_bug :
    let r : Rel (Int × SqlVal Int) := [(1, none), (2, none)]
    let maxSub := sqlMax Prod.snd r
    let exactOrig := select (fun row => sqlEq row.2 maxSub) r
    let buggyWithTies := r.filter (fun row => r.all (fun row2 =>
      match row.2, row2.2 with
      | none, none => true
      | some v, some v2 => decide (v2 ≤ v)
      | some _, none => true
      | none, some _ => false))
    exactOrig = [] ∧ buggyWithTies.length = 2 := by
  decide

/-- **定理 TPCH-4b (`grouped_having_global_sum_fubini_sound`, Q11)**:
    任意のテーブル `r`（`partsupp ⋈ supplier ⋈ nation`）と任意のグループキー `key`（`NULL` を含んでよい）について、
    各グループの `SUM(val)` を並べた結果リスト上の `sqlSum`（`SUM(grp_sum) OVER ()`）は、
    元の全テーブル上の `sqlSum val r`（Q11 のスカラーサブクエリ）と **完全に一致（`=`）** する！
    したがって `HAVING SUM(val) > f((SELECT SUM(val) FROM same_join))` は 3 テーブル結合を 2 回走査せず 1 パスで計算できる。 -/
theorem grouped_having_global_sum_fubini_sound {Row K : Type} [DecidableEq K]
    (key : Row → K) (val : Row → SqlVal Int) (r : Rel Row) :
    let groups := groupBy key (fun _ b => sqlSum val b) r
    sqlSum Prod.snd groups = sqlSum val r := by
  dsimp only
  have hPerm : BagEq r ((distinctKeys (r.map key)).flatMap (fun k => r.filter (fun row => key row == k))) :=
    perm_buckets_of_nodup key (distinctKeys (r.map key)) r
      (nodup_distinctKeys _)
      (fun row hMem => (mem_distinctKeys _ _).mpr (List.mem_map_of_mem hMem))
  unfold sqlSum groupBy
  rw [aggFold_perm sumMonoid val hPerm, aggFold_flatMap, aggFold_map]

/-- **反例トラップ TPCH-4b (`grouped_having_distinct_sum_fubini_bug`)**:
    集約が `SUM(v)` ではなく `SUM(DISTINCT v)` の場合、異なるグループに同じ値 `10` が現れると
    グループごとの `SUM(DISTINCT v)` の総和（`20`）は全表の `SUM(DISTINCT v)`（`10`）と一致しない！ -/
theorem grouped_having_distinct_sum_fubini_bug :
    let r : Rel (Int × Int) := [(1, 10), (2, 10)]
    let sqlSumDistinct := fun (xs : List Int) => sqlSum (fun v => some v) (distinctKeys xs)
    let perGroupSums := (groupBy Prod.fst (fun _ b => sqlSumDistinct (b.map Prod.snd)) r).map Prod.snd
    sqlSum id perGroupSums = some 20 ∧ sqlSumDistinct (r.map Prod.snd) = some 10 := by
  decide

/-!
## 5. TPCH-5 (Q10 & Q9): Top-N 集約後の遅延ディメンション属性結合 & 推移的等価クラス閉包
-/

/-- **定理 TPCH-5a (`topn_agg_late_unique_dim_attribute_join_sound`, Q10)**:
    ファクト行集合 `facts`（`orders ⋈ lineitem`）と、`dimKey` が一意なディメンション `dims`（`customer ⋈ nation`、
    `c_name, c_address, c_phone, c_comment` 等の幅広属性を持つ）を結合して
    `(dimKey, wideAttrs)` で `GROUP BY` し、集約値順の Top-`kLimit` を取るクエリは：
    1. 先に `dims` の **整数主キー集合だけ** で準結合フィルタし（幅広文字列列は一切読まない！）、
    2. 単一整数キー `fKey` だけで集約＋`sortByRank`＋`take kLimit` を実行し（Q10 で 20 行に絞る！）、
    3. 確定した **最大 `kLimit` 個の勝者キーだけ** に対して `dims` の幅広属性を引く遅延結合計画と
    **完全一致（`=`）** する！ -/
theorem topn_agg_late_unique_dim_attribute_join_sound {F D K Attr V : Type}
    [DecidableEq K] [DecidableEq Attr]
    (fKey : F → K) (dKey : D → K) (dAttr : D → Attr)
    (aggFn : Rel F → V) (leRank : V → V → Bool) (kLimit : Nat)
    (facts : Rel F) (dims : Rel D)
    (hDimUnique : (dims.map dKey).Nodup) :
    let earlyJoined : Rel (F × Attr) := facts.flatMap (fun f =>
      (dims.filter (fun d => dKey d == fKey f)).map (fun d => (f, dAttr d)))
    let earlyTopK := (sortByRank (fun a b => leRank a.2 b.2)
      (groupBy (fun p => (fKey p.1, p.2)) (fun _ b => aggFn (b.map Prod.fst)) earlyJoined)).take kLimit
    let validFacts := facts.filter (fun f => dims.any (fun d => dKey d == fKey f))
    let narrowTopK := (sortByRank (fun a b => leRank a.2 b.2)
      (groupBy fKey (fun _ b => aggFn b) validFacts)).take kLimit
    let lateTopK := narrowTopK.flatMap (fun kv =>
      (dims.filter (fun d => dKey d == kv.1)).map (fun d => ((kv.1, dAttr d), kv.2)))
    earlyTopK = lateTopK := by
  dsimp only
  -- 一意性補題: `dims` 内で `dKey d == k` を満たす要素は高々 1 個！
  have hLookupUnique : ∀ (ds : Rel D), (ds.map dKey).Nodup → ∀ k : K,
      (ds.any (fun d => dKey d == k) = false ∧ ds.filter (fun d => dKey d == k) = []) ∨
      ∃ d0 : D, dKey d0 = k ∧ ds.any (fun d => dKey d == k) = true ∧
                ds.filter (fun d => dKey d == k) = [d0] := by
    intro ds hN k
    induction ds with
    | nil => exact Or.inl ⟨rfl, rfl⟩
    | cons d dsRest ih =>
      rw [List.map_cons, List.nodup_cons] at hN
      by_cases hdk : dKey d = k
      · right
        refine ⟨d, hdk, by simp [hdk], ?_⟩
        have hTailEmpty : dsRest.filter (fun d' => dKey d' == k) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro d' hd' hEq
          have hEq' : dKey d' = dKey d := (beq_iff_eq.mp hEq).trans hdk.symm
          exact hN.1 (hEq' ▸ List.mem_map_of_mem hd')
        simp [hdk, hTailEmpty]
      · have hne : (dKey d == k) = false := beq_false_of_ne hdk
        rcases ih hN.2 with ⟨hAnyF, hFiltNil⟩ | ⟨d0, hd0, hAnyT, hFiltOne⟩
        · left; simp [List.any_cons, hne, hAnyF, hFiltNil]
        · right; refine ⟨d0, hd0, ?_, ?_⟩ <;> simp [List.any_cons, hne, hAnyT, hFiltOne]
  have hDimU := hLookupUnique dims hDimUnique
  clear hLookupUnique
  have hKeysRel : ∀ (fs : Rel F),
      distinctKeys ((fs.flatMap (fun f =>
        (dims.filter (fun d => dKey d == fKey f)).map (fun d => (f, dAttr d)))).map
        (fun p => (fKey p.1, p.2))) =
      (distinctKeys ((fs.filter (fun f => dims.any (fun d => dKey d == fKey f))).map fKey)).flatMap (fun k =>
        (dims.filter (fun d => dKey d == k)).map (fun d => (k, dAttr d))) := by
    intro fs
    induction fs with
    | nil => rfl
    | cons f rest ih =>
      rcases hDimU (fKey f) with ⟨hAnyF, hFiltNil⟩ | ⟨d0, _, hAnyT, hFiltOne⟩
      · simp only [List.flatMap_cons, List.filter_cons, hAnyF, Bool.false_eq_true, ↓reduceIte,
                   hFiltNil, List.map_nil, List.nil_append]
        exact ih
      · simp only [List.flatMap_cons, List.filter_cons, hAnyT, ↓reduceIte, hFiltOne,
                   List.map_cons, List.map_nil, List.singleton_append, distinctKeys]
        rw [ih]
        congr 1
        clear ih
        generalize distinctKeys (List.map fKey (List.filter (fun f' => dims.any (fun d => dKey d == fKey f')) rest)) = ks
        induction ks with
        | nil => rfl
        | cons k1 ks1 ihk =>
          by_cases hk1 : k1 = fKey f
          · subst hk1
            have hFalseK : decide (fKey f ≠ fKey f) = false := decide_eq_false (fun h => h rfl)
            have hFalseP : decide ((fKey f, dAttr d0) ≠ (fKey f, dAttr d0)) = false :=
              decide_eq_false (fun h => h rfl)
            simp only [List.flatMap_cons, hFiltOne, List.map_cons, List.map_nil, List.singleton_append,
                       List.filter_cons, hFalseP, hFalseK, Bool.false_eq_true, ↓reduceIte]
            exact ihk
          · have hneB : decide (k1 ≠ fKey f) = true := decide_eq_true hk1
            simp only [List.filter_cons, hneB, ↓reduceIte, List.flatMap_cons]
            rcases hDimU k1 with ⟨_, hNil1⟩ | ⟨d1, _, _, hOne1⟩
            · simp only [hNil1, List.map_nil, List.nil_append]
              exact ihk
            · have hnePair : decide ((k1, dAttr d1) ≠ (fKey f, dAttr d0)) = true :=
                decide_eq_true (fun h => hk1 (congrArg Prod.fst h))
              simp only [hOne1, List.map_cons, List.map_nil, List.singleton_append,
                         List.filter_cons, hnePair, ↓reduceIte, ihk]
  have hBucketProj : ∀ (fs : Rel F) (k : K) (d0 : D),
      dims.filter (fun d => dKey d == k) = [d0] →
      ((fs.flatMap (fun f =>
        (dims.filter (fun d => dKey d == fKey f)).map (fun d => (f, dAttr d)))).filter
        (fun p => (fKey p.1, p.2) == (k, dAttr d0))).map Prod.fst =
      (fs.filter (fun f => dims.any (fun d => dKey d == fKey f))).filter (fun f => fKey f == k) := by
    intro fs k d0 hOne
    induction fs with
    | nil => rfl
    | cons f rest ih =>
      by_cases hfk : fKey f = k
      · have hAnyK : dims.any (fun d => dKey d == fKey f) = true := by
          rw [hfk]
          rcases hDimU k with ⟨_, hNil⟩ | ⟨_, _, hAnyT, _⟩
          · rw [hOne] at hNil; nomatch hNil
          · exact hAnyT
        have hOneF : dims.filter (fun d => dKey d == fKey f) = [d0] := by rw [hfk]; exact hOne
        have hPairSelf : ((fKey f, dAttr d0) == (k, dAttr d0)) = true := by simp [hfk]
        have hKeySelf : (fKey f == k) = true := by simp [hfk]
        simp only [List.flatMap_cons, hOneF, List.map_cons, List.map_nil, List.singleton_append,
                   List.filter_cons, hPairSelf, hAnyK, hKeySelf, ↓reduceIte]
        congr 1
      · have hneK : (fKey f == k) = false := beq_false_of_ne hfk
        rcases hDimU (fKey f) with ⟨hAnyF, hNilF⟩ | ⟨df, _, hAnyT, hOneF⟩
        · simp only [List.flatMap_cons, hNilF, List.map_nil, List.nil_append,
                     List.filter_cons, hAnyF, Bool.false_eq_true, ↓reduceIte]
          exact ih
        · have hnePair : ((fKey f, dAttr df) == (k, dAttr d0)) = false :=
            beq_false_of_ne (fun h => hfk (congrArg Prod.fst h))
          simp only [List.flatMap_cons, List.filter_append, hOneF, List.map_cons, List.map_nil,
                     List.filter_cons, hnePair, Bool.false_eq_true, ↓reduceIte, List.filter_nil,
                     List.nil_append, hAnyT, hneK]
          exact ih
  have hEarlyGroupsEq :
      groupBy (fun p : F × Attr => (fKey p.1, p.2)) (fun _ b => aggFn (b.map Prod.fst))
        (facts.flatMap (fun f => (dims.filter (fun d => dKey d == fKey f)).map (fun d => (f, dAttr d)))) =
      (groupBy fKey (fun _ b => aggFn b)
        (facts.filter (fun f => dims.any (fun d => dKey d == fKey f)))).flatMap (fun kv =>
        (dims.filter (fun d => dKey d == kv.1)).map (fun d => ((kv.1, dAttr d), kv.2))) := by
    unfold groupBy
    rw [hKeysRel facts]
    have hAllMatched : ∀ k ∈ distinctKeys
        ((facts.filter (fun f => dims.any (fun d => dKey d == fKey f))).map fKey),
        ∃ d0 : D, dKey d0 = k ∧ dims.filter (fun d => dKey d == k) = [d0] := by
      intro k hk
      rw [mem_distinctKeys, List.mem_map] at hk
      rcases hk with ⟨f, hfMem, rfl⟩
      rw [List.mem_filter] at hfMem
      rcases hDimU (fKey f) with ⟨hAnyF, _⟩ | ⟨d0, hd0, _, hOne⟩
      · rw [hAnyF] at hfMem; cases hfMem.2
      · exact ⟨d0, hd0, hOne⟩
    generalize distinctKeys ((facts.filter (fun f => dims.any (fun d => dKey d == fKey f))).map fKey) = ks at hAllMatched
    induction ks with
    | nil => rfl
    | cons k ks ih =>
      obtain ⟨d0, _, hOne⟩ := hAllMatched k (List.Mem.head _)
      simp only [List.flatMap_cons, hOne, List.map_cons, List.map_nil, List.singleton_append]
      rw [hBucketProj facts k d0 hOne]
      congr 1
      exact ih (fun z hz => hAllMatched z (List.Mem.tail _ hz))
  have hMapSortComm : ∀ {A B : Type} (g : A → B) (leB : B → B → Bool) (xs : List A),
      sortByRank leB (xs.map g) = (sortByRank (fun x y => leB (g x) (g y)) xs).map g := by
    intro A B g leB xs
    have hIns : ∀ (a : A) (ys : List A),
        insertByRank leB (g a) (ys.map g) = (insertByRank (fun x y => leB (g x) (g y)) a ys).map g := by
      intro a ys
      induction ys with
      | nil => rfl
      | cons y ys ihy =>
        simp only [List.map_cons, insertByRank]
        by_cases hle : leB (g a) (g y) = true <;> simp [hle, ihy]
    induction xs with
    | nil => rfl
    | cons x xs ihx =>
      simp only [List.map_cons, sortByRank, ihx, hIns]
  have hGroupsAllMatch : ∀ kv ∈ groupBy fKey (fun _ b => aggFn b)
      (facts.filter (fun f => dims.any (fun d => dKey d == fKey f))),
      ∃ d0 : D, dims.filter (fun d => dKey d == kv.1) = [d0] := by
    intro kv hkv
    unfold groupBy at hkv
    rw [List.mem_map] at hkv
    rcases hkv with ⟨k, hk, rfl⟩
    rw [mem_distinctKeys, List.mem_map] at hk
    rcases hk with ⟨f, hf, rfl⟩
    rw [List.mem_filter] at hf
    rcases hDimU (fKey f) with ⟨hAnyF, _⟩ | ⟨d0, _, _, hOne⟩
    · rw [hAnyF] at hf; cases hf.2
    · exact ⟨d0, hOne⟩
  have hSubFlatMapEqMap : ∀ (ds : Rel D) {xs : List (K × V)},
      (∀ kv ∈ xs, ∃ d0 : D, ds.filter (fun d => dKey d == kv.1) = [d0]) →
      ∀ dDefault : D,
        xs.flatMap (fun kv => (ds.filter (fun d => dKey d == kv.1)).map (fun d => ((kv.1, dAttr d), kv.2))) =
        xs.map (fun kv =>
          let d0 := (ds.filter (fun d => dKey d == kv.1)).headD dDefault
          ((kv.1, dAttr d0), kv.2)) := by
    intro ds xs hAll dDef
    induction xs with
    | nil => rfl
    | cons kv rest ih =>
      obtain ⟨d0, hOne⟩ := hAll kv (List.Mem.head _)
      rw [List.flatMap_cons, List.map_cons, hOne]
      simp only [List.map_cons, List.map_nil, List.singleton_append, List.headD_cons]
      congr 1
      exact ih (fun z hz => hAll z (List.Mem.tail _ hz))
  cases hDims : dims with
  | nil =>
    have hEarlyNil : ∀ (fs : Rel F),
        fs.flatMap (fun f => (([] : Rel D).filter (fun d => dKey d == fKey f)).map (fun d => (f, dAttr d))) = [] := by
      intro fs; induction fs with | nil => rfl | cons _ _ ih => exact ih
    have hValNil : facts.filter (fun f => ([] : Rel D).any (fun d => dKey d == fKey f)) = [] := by
      apply List.filter_eq_nil_iff.mpr; intro _ _; exact Bool.false_ne_true
    rw [hEarlyNil facts, hValNil]
    simp [groupBy, distinctKeys, sortByRank]
  | cons dFirst dsRest =>
    have hMemSort : ∀ {T1 : Type} (le1 : T1 → T1 → Bool) (xs : List T1) (z : T1),
        z ∈ sortByRank le1 xs → z ∈ xs := by
      intro T1 le1 xs
      have hMemIns : ∀ (a : T1) (ys : List T1) (z : T1),
          z ∈ insertByRank le1 a ys → z = a ∨ z ∈ ys := by
        intro a ys z hz
        induction ys with
        | nil => simp [insertByRank] at hz; exact Or.inl hz
        | cons y ys ihy =>
          simp only [insertByRank] at hz
          by_cases hle : le1 a y = true
          · simp only [hle, ↓reduceIte, List.mem_cons] at hz
            rcases hz with rfl | hz2
            · exact Or.inl rfl
            · exact Or.inr (List.mem_cons.mpr hz2)
          · simp only [hle, Bool.false_eq_true, ↓reduceIte, List.mem_cons] at hz
            rcases hz with rfl | hTail
            · exact Or.inr (List.Mem.head _)
            · rcases ihy hTail with rfl | hInYs
              · exact Or.inl rfl
              · exact Or.inr (List.Mem.tail _ hInYs)
      induction xs with
      | nil => intro z hz; nomatch hz
      | cons x xs ihx =>
        intro z hz
        simp only [sortByRank] at hz
        rcases hMemIns x (sortByRank le1 xs) z hz with rfl | hInSort
        · exact List.Mem.head _
        · exact List.Mem.tail _ (ihx z hInSort)
    let mapFn : (K × V) → ((K × Attr) × V) := fun kv =>
      let d0 := ((dFirst :: dsRest).filter (fun d => dKey d == kv.1)).headD dFirst
      ((kv.1, dAttr d0), kv.2)
    have hGroupsAllMatch' : ∀ kv ∈ groupBy fKey (fun _ b => aggFn b)
        (facts.filter (fun f => (dFirst :: dsRest).any (fun d => dKey d == fKey f))),
        ∃ d0 : D, (dFirst :: dsRest).filter (fun d => dKey d == kv.1) = [d0] := by
      simpa [hDims] using hGroupsAllMatch
    have hEarlyGroupsEq' :
        groupBy (fun p : F × Attr => (fKey p.1, p.2)) (fun _ b => aggFn (b.map Prod.fst))
          (facts.flatMap (fun f => ((dFirst :: dsRest).filter (fun d => dKey d == fKey f)).map (fun d => (f, dAttr d)))) =
        (groupBy fKey (fun _ b => aggFn b)
          (facts.filter (fun f => (dFirst :: dsRest).any (fun d => dKey d == fKey f)))).flatMap (fun kv =>
          ((dFirst :: dsRest).filter (fun d => dKey d == kv.1)).map (fun d => ((kv.1, dAttr d), kv.2))) := by
      simpa [hDims] using hEarlyGroupsEq
    have hEarlyMap :
        groupBy (fun p : F × Attr => (fKey p.1, p.2)) (fun _ b => aggFn (b.map Prod.fst))
          (facts.flatMap (fun f => ((dFirst :: dsRest).filter (fun d => dKey d == fKey f)).map (fun d => (f, dAttr d)))) =
        (groupBy fKey (fun _ b => aggFn b)
          (facts.filter (fun f => (dFirst :: dsRest).any (fun d => dKey d == fKey f)))).map mapFn := by
      rw [hEarlyGroupsEq']
      exact hSubFlatMapEqMap (dFirst :: dsRest) hGroupsAllMatch' dFirst
    have hSortedMem : ∀ kv ∈ (sortByRank (fun a b : K × V => leRank a.2 b.2)
        (groupBy fKey (fun _ b => aggFn b)
          (facts.filter (fun f => (dFirst :: dsRest).any (fun d => dKey d == fKey f))))).take kLimit,
        ∃ d0 : D, (dFirst :: dsRest).filter (fun d => dKey d == kv.1) = [d0] := by
      intro kv hkv
      have hInSort := List.mem_of_mem_take hkv
      have hInGrp := hMemSort _ _ kv hInSort
      exact hGroupsAllMatch' kv hInGrp
    rw [hEarlyMap, hMapSortComm mapFn (fun a b => leRank a.2 b.2), ← List.map_take]
    exact (hSubFlatMapEqMap (dFirst :: dsRest) hSortedMem dFirst).symm

/-- **反例トラップ TPCH-5 (`late_dim_join_without_key_semijoin_steals_limit_bug`)**:
    FK が無い環境で `dims` の整数キー存在チェック（準結合）を省いて先に `facts` の Top-1 を取ると、
    `dims` に存在しない孤児キー `999` が `LIMIT 1` の枠を奪い、遅延結合後に **0 行** になってしまう！ -/
theorem late_dim_join_without_key_semijoin_steals_limit_bug :
    let facts : Rel (Int × Int) := [(999, 1000), (1, 50)]
    let dims : Rel (Int × String) := [(1, "Customer#1")]
    let leDesc : SqlVal Int → SqlVal Int → Bool := fun a b =>
      match a, b with
      | some va, some vb => decide (vb ≤ va)
      | _, _ => false
    let validFacts := facts.filter (fun f => dims.any (fun d => d.1 == f.1))
    let exactTop1 := ((sortByRank (fun a b => leDesc a.2 b.2)
      (groupBy Prod.fst (fun _ b => sqlSum (fun x => some x.2) b) validFacts)).take 1).flatMap
      (fun kv => (dims.filter (fun d => d.1 == kv.1)).map (fun d => (d.2, kv.2)))
    let buggyTop1 := ((sortByRank (fun a b => leDesc a.2 b.2)
      (groupBy Prod.fst (fun _ b => sqlSum (fun x => some x.2) b) facts)).take 1).flatMap
      (fun kv => (dims.filter (fun d => d.1 == kv.1)).map (fun d => (d.2, kv.2)))
    exactTop1 = [("Customer#1", some 50)] ∧ buggyTop1 = [] := by
  decide

/-- **定理 TPCH-5b (`equijoin_transitive_closure_sound`, Q9)**:
    厳密等値ドメイン `K` 上で、SQL `WHERE` 文脈における `(a = b) AND (c = b)` は
    推移閉包 `(a = b) AND (c = b) AND (a = c)` と完全に同値である（`a, b, c` のいずれが `NULL` でも成立！）。
    これにより Q9 の `ps_partkey = l_partkey AND p_partkey = l_partkey` から `p_partkey = ps_partkey` が導出され、
    `part`（`%tomato%`）から `partsupp` へ直接結合・Runtime Filter プッシュダウンが可能になる。 -/
theorem equijoin_transitive_closure_sound {K : Type} [DecidableEq K]
    (a b c : SqlVal K) :
    (SqlBool.and (sqlEq a b) (sqlEq c b)).inWhere =
    (SqlBool.and (SqlBool.and (sqlEq a b) (sqlEq c b)) (sqlEq a c)).inWhere := by
  rw [and_inWhere, and_inWhere, and_inWhere]
  cases a with
  | none => rfl
  | some va =>
    cases b with
    | none => rfl
    | some vb =>
      cases c with
      | none => simp [sqlEq, SqlBool.inWhere]
      | some vc =>
        by_cases hab : va = vb
        · subst hab
          by_cases hcb : vc = va
          · subst hcb; simp [sqlEq, SqlBool.inWhere]
          · have hne : va ≠ vc := Ne.symm hcb
            simp [sqlEq, hcb, hne, SqlBool.inWhere]
        · simp [sqlEq, hab, SqlBool.inWhere]

/-!
## 6. TPCH-6 (Q20 & Q16): 相関 `SUM` 不等号の `INNER JOIN` 相関除去 & `NOT IN` 空集合トラップ
-/

/-- SQL の厳密大なり比較 `a > b`（いずれかが `NULL` なら `tNull`）。 -/
def sqlGtInt : SqlVal Int → SqlVal Int → SqlBool
  | some a, some b => if a > b then .tTrue else .tFalse
  | _,      _      => .tNull

/-- **定理 TPCH-6a (`correlated_gt_sum_decorrelate_to_inner_join_sound`, Q20)**:
    `ps_availqty > (SELECT scale * SUM(l_quantity) FROM lineitem WHERE keyL = keyPS AND wL)` のように、
    相関スカラーサブクエリが `SUM`（0 件一致または全 `NULL` で `none` を返す集約）の単調スカラー倍との不等号比較であるとき、
    0 件一致行は `avail > NULL` = `tNull` で必ず棄却される！
    したがって `LEFT OUTER JOIN` ではなく **`INNER JOIN`** へ直接相関除去でき、
    さらに外側の準結合ドメイン `outerDomain`（Q20 の `PERU` サプライヤ × `tan%` 部品）を `lineitem` 事前集約へ押し込める！ -/
theorem correlated_gt_sum_decorrelate_to_inner_join_sound {PS L K : Type} [DecidableEq K]
    (keyPS : PS → SqlVal K) (availPS : PS → SqlVal Int)
    (keyL : L → SqlVal K) (wL : L → Bool) (qtyL : L → SqlVal Int)
    (scale : Int)
    (outerDomain : K → Bool)
    (psRel : Rel PS) (lRel : Rel L) :
    let corrOut := psRel.filter (fun ps =>
      (match keyPS ps with | some k => outerDomain k | none => false) &&
      (let matches_ := lRel.filter (fun l => wL l && (sqlEq (keyPS ps) (keyL l)).inWhere)
       (sqlGtInt (availPS ps) (sqlMul (some scale) (sqlSum qtyL matches_))).inWhere))
    let lDomainFiltered := lRel.filter (fun l =>
      wL l && (match keyL l with | some k => outerDomain k | none => false))
    let lAgg := groupBy keyL (fun _ b => sqlSum qtyL b) lDomainFiltered
    let decorrOut := (psRel.flatMap (fun ps =>
      (lAgg.filter (fun kv =>
        (match keyPS ps with | some k => outerDomain k | none => false) &&
        (sqlEq (keyPS ps) kv.1).inWhere &&
        (sqlGtInt (availPS ps) (sqlMul (some scale) kv.2)).inWhere)).map (fun _ => ps)))
    corrOut = decorrOut := by
  dsimp only
  let lDomainFiltered := lRel.filter (fun l =>
    wL l && (match keyL l with | some k => outerDomain k | none => false))
  have hMain : ∀ (psList : Rel PS),
      psList.filter (fun ps =>
        (match keyPS ps with | some k => outerDomain k | none => false) &&
        (let matches_ := lRel.filter (fun l => wL l && (sqlEq (keyPS ps) (keyL l)).inWhere)
         (sqlGtInt (availPS ps) (sqlMul (some scale) (sqlSum qtyL matches_))).inWhere)) =
      psList.flatMap (fun ps =>
        ((groupBy keyL (fun _ b => sqlSum qtyL b) lDomainFiltered).filter (fun kv =>
          (match keyPS ps with | some k => outerDomain k | none => false) &&
          (sqlEq (keyPS ps) kv.1).inWhere &&
          (sqlGtInt (availPS ps) (sqlMul (some scale) kv.2)).inWhere)).map (fun _ => ps)) := by
    intro psList
    induction psList with
    | nil => rfl
    | cons ps rest ih =>
      simp only [List.filter_cons, List.flatMap_cons]
      cases hkps : keyPS ps with
      | none =>
        simp [ih]
      | some k =>
        by_cases hDom : outerDomain k = true
        · have hBucketEq :
              lRel.filter (fun l => wL l && (sqlEq (some k) (keyL l)).inWhere) =
              lDomainFiltered.filter (fun l => decide (keyL l = some k)) := by
            dsimp only [lDomainFiltered]
            rw [List.filter_filter]
            apply List.filter_congr
            intro l _
            rw [sqlEq_some_inWhere_dec k (keyL l)]
            cases hkl : keyL l with
            | none => simp
            | some kl =>
              by_cases hEq : kl = k
              · subst hEq; simp [hDom]
              · simp [hEq]
          have hLaggEq :
              (groupBy keyL (fun _ b => sqlSum qtyL b) lDomainFiltered).filter (fun kv =>
                (sqlEq (some k) kv.1).inWhere &&
                (sqlGtInt (availPS ps) (sqlMul (some scale) kv.2)).inWhere) =
              (((groupBy keyL (fun _ b => sqlSum qtyL b) lDomainFiltered).filter
                (fun kv => decide (kv.1 = some k))).filter
                (fun kv => (sqlGtInt (availPS ps) (sqlMul (some scale) kv.2)).inWhere)) := by
            rw [List.filter_filter]
            apply List.filter_congr
            intro kv _
            rw [sqlEq_some_inWhere_dec k kv.1]
            exact Bool.and_comm _ _
          have hFind := find_in_groupBy_unique keyL (fun _ b => sqlSum qtyL b) lDomainFiltered (some k)
          simp only [hDom, Bool.true_and]
          rw [hBucketEq, hLaggEq, hFind]
          generalize lDomainFiltered.filter (fun l => decide (keyL l = some k)) = bk
          cases bk with
          | nil =>
            have hFalseGt : (sqlGtInt (availPS ps) (sqlMul (some scale) (sqlSum qtyL []))).inWhere = false := by
              simp [sqlSum_nil, sqlMul, sqlGtInt, SqlBool.inWhere]
            simp only [hFalseGt, Bool.false_eq_true, ↓reduceIte, List.isEmpty_nil, List.filter_nil,
                       List.map_nil, List.nil_append]
            exact ih
          | cons l0 ls =>
            cases hGt : (sqlGtInt (availPS ps) (sqlMul (some scale) (sqlSum qtyL (l0 :: ls)))).inWhere
            · simp only [hGt, Bool.false_eq_true, ↓reduceIte, List.isEmpty_cons, List.filter_cons,
                         List.filter_nil, List.map_nil, List.nil_append]
              exact ih
            · simp only [hGt, ↓reduceIte, List.isEmpty_cons, Bool.false_eq_true, List.filter_cons,
                         List.filter_nil, List.map_cons, List.map_nil, List.singleton_append]
              congr 1
        · have hDomF : outerDomain k = false := Bool.eq_false_iff.mpr hDom
          simp [hDomF, ih]
  exact hMain psRel

/-- **反例トラップ TPCH-6a (`correlated_gt_count_demotes_to_inner_join_bug`, Kim の COUNT バグ)**:
    Q20 型の相関サブクエリが `SUM` ではなく `COUNT(*)`（または `COALESCE(SUM, 0)`）だった場合、
    `lineitem` に 0 件一致の行は `COUNT(*) = 0` となり `avail (10) > 0` が **TRUE** になるため、
    `INNER JOIN` へ相関除去すると 0 件一致の正当な行がすべて消失する！ -/
theorem correlated_gt_count_demotes_to_inner_join_bug :
    let psRel : Rel (SqlVal Int × SqlVal Int) := [(some 1, some 10)]
    let lRel : Rel (SqlVal Int) := []
    let exactCorrCount := psRel.filter (fun ps =>
      let cnt := sqlCountStar (lRel.filter (fun lk => (sqlEq ps.1 lk).inWhere))
      (sqlGtInt ps.2 (some cnt)).inWhere)
    let lAgg := groupBy id (fun _ b => sqlCountStar b) lRel
    let buggyInnerJoin := psRel.flatMap (fun ps =>
      (lAgg.filter (fun kv => (sqlEq ps.1 kv.1).inWhere && (sqlGtInt ps.2 (some kv.2)).inWhere)).map (fun _ => ps))
    exactCorrCount.length = 1 ∧ buggyInnerJoin = [] := by
  decide

/-- SQL の `x NOT IN sub`（3値論理: `sub` の全要素 `y` に対して `x != y` が `tTrue` のときのみ `tTrue`）。 -/
def sqlNotIn {K : Type} [DecidableEq K] (x : SqlVal K) (sub : List (SqlVal K)) : SqlBool :=
  sub.foldr (fun y acc => SqlBool.and (SqlBool.not (sqlEq x y)) acc) .tTrue

private theorem sqlNotIn_some_nonnull_inWhere {K : Type} [DecidableEq K]
    (vx : K) (sub : List (SqlVal K))
    (hNoNullSub : ∀ y ∈ sub, y.isSome = true) :
    (sqlNotIn (some vx) sub).inWhere = !(sub.any (fun y => (sqlEq (some vx) y).inWhere)) := by
  induction sub with
  | nil => rfl
  | cons y ys ih =>
    have hy := hNoNullSub y (List.Mem.head _)
    cases y with
    | none => simp at hy
    | some vy =>
      have ih' := ih (fun z hz => hNoNullSub z (List.Mem.tail _ hz))
      have hStep : sqlNotIn (some vx) (some vy :: ys) =
          SqlBool.and (SqlBool.not (sqlEq (some vx) (some vy))) (sqlNotIn (some vx) ys) := rfl
      rw [hStep, and_inWhere, ih', List.any_cons]
      by_cases hEq : vx = vy
      · subst hEq; simp [sqlEq, SqlBool.not, SqlBool.inWhere]
      · simp [sqlEq, hEq, SqlBool.not, SqlBool.inWhere]

/-- **定理 TPCH-6b (`not_in_nonnull_nonempty_to_antijoin_sound`, Q16)**:
    サブクエリ出力 `sub` が **非空（`sub ≠ []`）かつ `NULL` を 1 つも含まない** とき、
    `WHERE x NOT IN sub` は「`x IS NOT NULL` かつ通常の `AntiJoin`」と完全に一致する。 -/
theorem not_in_nonnull_nonempty_to_antijoin_sound {Row K : Type} [DecidableEq K]
    (lhs : Row → SqlVal K) (r : Rel Row) (sub : List (SqlVal K))
    (hNonEmpty : sub ≠ [])
    (hNoNullSub : ∀ y ∈ sub, y.isSome = true) :
    select (fun row => sqlNotIn (lhs row) sub) r =
    antiJoin (fun row y => sqlEq (lhs row) y) (r.filter (fun row => (lhs row).isSome)) sub := by
  unfold select antiJoin
  rw [List.filter_filter]
  apply List.filter_congr
  intro row _
  dsimp only
  cases hx : lhs row with
  | none =>
    cases sub with
    | nil => exact absurd rfl hNonEmpty
    | cons y0 ys =>
      have hy0 := hNoNullSub y0 (List.Mem.head _)
      cases y0 with
      | none => simp at hy0
      | some vy =>
        have hNotInNone : (sqlNotIn none (some vy :: ys)).inWhere = false := by
          have hStep : sqlNotIn none (some vy :: ys) =
              SqlBool.and (SqlBool.not (sqlEq none (some vy))) (sqlNotIn none ys) := rfl
          rw [hStep, and_inWhere]
          rfl
        rw [hNotInNone]
        simp
  | some vx =>
    rw [sqlNotIn_some_nonnull_inWhere vx sub hNoNullSub]
    simp

/-- **反例トラップ TPCH-6b (`not_in_empty_subquery_null_lhs_bug`, Q16)**:
    サブクエリ `sub` が `NOT NULL` 列であっても、実行時に **0 行（`sub = []`）** だった場合、
    SQL 仕様では `NULL NOT IN ()` は **`TRUE`** になる！
    したがって `sub ≠ []` を確認せずに無条件で左辺へ `ps_suppkey IS NOT NULL` を押し込むと、
    `sub = []` のときに `ps_suppkey = NULL` のみのグループが誤って消失する！ -/
theorem not_in_empty_subquery_null_lhs_bug :
    let r : Rel (String × SqlVal Int) := [("Brand#99", none)]
    let emptySub : List (SqlVal Int) := []
    let exactNotIn := select (fun row => sqlNotIn row.2 emptySub) r
    let buggyUnconditionalNonNull := antiJoin (fun row y => sqlEq row.2 y)
                                      (r.filter (fun row => row.2.isSome)) emptySub
    exactNotIn.length = 1 ∧ buggyUnconditionalNonNull = [] := by
  decide

end TinylambOpt
