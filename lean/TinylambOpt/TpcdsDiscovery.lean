import TinylambOpt.AggBase

/-
# Step 10: TPC-DS クエリ群に向けた「よりアグレッシブな証明付き最適化ルール」の探索

`tinylamb` の実測 TPC-DS SF=1 ベンチマーク（`gate04` 99 クエリ）で実行時間上位・対 DuckDB 差上位を占める
代表クエリ群（Q72, Q65, Q5, Q74, Q4, Q51, Q76, Q54, Q44, Q75, Q97, Q11, Q78, Q38, Q87, Q1, Q81）の
SQL 構造を解析し、現行オプティマイザがまだ踏み込めていない **6 系統のアグレッシブな書き換えルール** と
その裏に潜む **7 つの SQL 三値論理 / NULL / 空集合トラップ** を Lean 4 で発見・完全証明します。

## 収録する 6 大 TPC-DS 最適化ルール（すべて `sorry` ゼロ）

1. **排他定数タグ付き `UNION ALL` の外側再集約スキップ (`disjoint_union_group_by_bypass_sound`)**
   - 対象: **Q76**（対 DuckDB 比率ワースト 1 位 `1.72x`）、**Q71**
2. **自己結合集約の Window 関数 1 パス吸収 (`self_join_group_agg_to_window_sound`)**
   - 対象: **Q65**（対 DuckDB 比率ワースト 2 位 `1.70x` / `+5.76ms`）、**Q1**, **Q30**, **Q81**
3. **`UNION ALL` CTE 枝刈り・選言プッシュダウン ＋ 年次比較 自己結合の条件付き集約ピボット (`grouped_self_join_conditional_pivot_sound`)**
   - 対象: **Q4**（`41.52ms`, `+4.73ms`）、**Q11**（`25.25ms`）、**Q74**（`23.73ms`, `+5.41ms`）
4. **ゼロ埋め `UNION ALL` ファクト事前集約と次元結合分配 (`zero_padded_union_branch_preagg_sound`)**
   - 対象: **Q5**（`1.61x`, `+5.53ms`）、**Q75**（`+3.38ms`）、**Q80**
5. **末尾 `LEFT JOIN` 下の内側結合プレフィックス自由リオーダ・`WHERE` 貫通 ＆ 介在結合越しの `AntiJoin` 変換**
   - 対象: **Q72**（対 DuckDB 絶対差ワースト 1 位 `+7.66ms`）、**Q78**（全体時間 2 位 `43.03ms`）
6. **次元結合前のサロゲートキー先行 `DISTINCT` (`eager_distinct_surrogate_keys_before_dim_joins_sound`) ＆ `FULL OUTER JOIN` 指標和の包除原理・`SemiJoin` 還元 (`full_outer_indicator_inclusion_exclusion_sound`)**
   - 対象: **Q38**（`14.54ms`）、**Q87**（`15.70ms`）、**Q97**（`17.35ms`, `+2.73ms`）
-/

namespace TinylambOpt

/-!
## 共通補題: 重複なしリストの順列同値・`flatMap` の Bag 同値保存・`Option` 等号補題
-/

@[simp] private theorem sqlEq_some_inWhere {K : Type} [DecidableEq K] (a b : K) :
    (sqlEq (some a) (some b)).inWhere = true ↔ a = b := by
  by_cases h : a = b <;> simp [sqlEq, SqlBool.inWhere, h]

@[simp] private theorem beq_of_decidableEq_option {K : Type} [DecidableEq K] (a b : SqlVal K) :
    @BEq.beq (SqlVal K) instBEqOfDecidableEq a b = (a == b) := by
  cases a with
  | none => cases b <;> rfl
  | some va =>
    cases b with
    | none => rfl
    | some vb =>
      change decide (some va = some vb) = (va == vb)
      by_cases h : va = vb <;> simp [h]

private theorem mem_split_at {α : Type} (x : α) :
    ∀ {ys : List α}, x ∈ ys → ∃ l1 l2 : List α, ys = l1 ++ x :: l2
  | [], h => nomatch h
  | y :: ys, h => by
    rw [List.mem_cons] at h
    rcases h with rfl | hMem
    · exact ⟨[], ys, rfl⟩
    · obtain ⟨l1, l2, rfl⟩ := mem_split_at x hMem
      exact ⟨y :: l1, l2, rfl⟩

/-- 重複を持たない（`Nodup`）2 つのリストが同じ要素集合を持てば、両者は順列同値（`BagEq`）である。 -/
theorem perm_of_nodup_iff_mem {α : Type}
    : ∀ {xs ys : List α}, xs.Nodup → ys.Nodup → (∀ a, a ∈ xs ↔ a ∈ ys) → BagEq xs ys
  | [], [], _, _, _ => List.Perm.nil
  | [], y :: _, _, _, h => by
    have := (h y).mpr (List.mem_cons_self ..)
    nomatch this
  | x :: xs, ys, hx, hy, h => by
    rw [List.nodup_cons] at hx
    have hxMem : x ∈ ys := (h x).mp (List.mem_cons_self ..)
    obtain ⟨l1, l2, rfl⟩ := mem_split_at x hxMem
    have hMid : BagEq (x :: (l1 ++ l2)) (l1 ++ x :: l2) := List.perm_middle.symm
    have hySwap : (x :: (l1 ++ l2)).Nodup := by
      rwa [hMid.nodup_iff]
    rw [List.nodup_cons] at hySwap
    have hSub : ∀ a, a ∈ xs ↔ a ∈ l1 ++ l2 := by
      intro a
      constructor
      · intro ha
        have hne : a ≠ x := fun hEq => hx.1 (hEq ▸ ha)
        have haYs : a ∈ l1 ++ x :: l2 := (h a).mp (List.mem_cons_of_mem x ha)
        rw [List.mem_append, List.mem_cons] at haYs
        rw [List.mem_append]
        rcases haYs with h1 | rfl | h2
        · exact Or.inl h1
        · exact absurd rfl hne
        · exact Or.inr h2
      · intro ha
        have hne : a ≠ x := fun hEq => hySwap.1 (hEq ▸ ha)
        have haYs : a ∈ l1 ++ x :: l2 := by
          rw [List.mem_append] at ha
          rw [List.mem_append, List.mem_cons]
          rcases ha with h1 | h2
          · exact Or.inl h1
          · exact Or.inr (Or.inr h2)
        have haXs : a ∈ x :: xs := (h a).mpr haYs
        rw [List.mem_cons] at haXs
        rcases haXs with rfl | hxs
        · exact absurd rfl hne
        · exact hxs
    have ih := perm_of_nodup_iff_mem hx.2 hySwap.2 hSub
    exact (ih.cons x).trans hMid

/-- `BagEq`（順列同値）なリストに同じ関数を `flatMap` しても `BagEq` が保たれる。 -/
theorem perm_flatMap {α β : Type} (f : α → List β) :
    ∀ {xs ys : List α}, BagEq xs ys → BagEq (xs.flatMap f) (ys.flatMap f) := by
  intro xs ys h
  induction h with
  | nil => exact List.Perm.nil
  | cons x _ ih =>
    simp only [List.flatMap_cons]
    exact ih.append_left (f x)
  | swap x y zs =>
    simp only [List.flatMap_cons]
    rw [← List.append_assoc, ← List.append_assoc]
    exact List.perm_append_comm.append_right (zs.flatMap f)
  | trans _ _ ih1 ih2 => exact ih1.trans ih2

private theorem filter_eq_nil_of_forall_false {α : Type} (p : α → Bool) :
    ∀ (xs : List α), (∀ x ∈ xs, p x = false) → xs.filter p = []
  | [], _ => rfl
  | x :: xs, h => by
    have hx : p x = false := h x (List.mem_cons_self ..)
    have hxs : ∀ y ∈ xs, p y = false := fun y hy => h y (List.mem_cons_of_mem x hy)
    simp [List.filter, hx, filter_eq_nil_of_forall_false p xs hxs]

private theorem filter_unique_of_nodup_mem {α : Type}
    (p : α → Bool) :
    ∀ {xs : List α} {a : α},
      xs.Nodup → a ∈ xs → p a = true → (∀ x ∈ xs, p x = true → x = a) →
      xs.filter p = [a]
  | [], _, _, hMem, _, _ => nomatch hMem
  | x :: xs, a, hNodup, hMem, hpa, hUniq => by
    rw [List.nodup_cons] at hNodup
    rw [List.mem_cons] at hMem
    rcases hMem with rfl | hInXs
    · have hRest : xs.filter p = [] := by
        apply filter_eq_nil_of_forall_false
        intro y hy
        cases hpy : p y
        · rfl
        · have hya : y = a := hUniq y (List.mem_cons_of_mem a hy) hpy
          exact absurd (hya ▸ hy) hNodup.1
      simp [List.filter, hpa, hRest]
    · have hpx : p x = false := by
        cases hpx' : p x
        · rfl
        · have hxa : x = a := hUniq x (List.mem_cons_self ..) hpx'
          exact absurd (hxa ▸ hInXs) hNodup.1
      simp only [List.filter, hpx]
      exact filter_unique_of_nodup_mem p hNodup.2 hInXs hpa
        (fun y hy hpy => hUniq y (List.mem_cons_of_mem x hy) hpy)

/-!
## 1. 排他定数タグ付き `UNION ALL` の外側再集約スキップ（TPC-DS Q76, Q71）
-/

/-- ❌ **罠 1 (`agg_union_null_tag_collision_bug`)**:
    2 つの `UNION ALL` 枝のタグ定数がどちらも `NULL`（`none`）のとき、
    SQL の等号 `sqlEq none none` は `tUnknown`（偽扱い）だが、`GROUP BY` は `NULL` 同士を 1 グループに束ねる！
    したがって外側再集約を省略すると 1 行出るべき結果が 2 行に分裂する。 -/
theorem agg_union_null_tag_collision_bug :
    let b1 : Rel (SqlVal Int × SqlVal Int) := [(none, some 10)]
    let b2 : Rel (SqlVal Int × SqlVal Int) := [(none, some 20)]
    (sqlEq (none : SqlVal Int) none).inWhere = false ∧
    groupBy Prod.fst (fun _ b => sqlSum Prod.snd b) (b1 ++ b2) ≠
      (groupBy Prod.fst (fun _ b => sqlSum Prod.snd b) b1 ++
       groupBy Prod.fst (fun _ b => sqlSum Prod.snd b) b2) := by
  decide

/-- ✅ **定理 1 (`disjoint_union_group_by_bypass_sound` — TPC-DS Q76, Q71)**:
    `UNION ALL` の各枝 `r1, r2` で `GROUP BY` キー `key` の値域が互いに素（`IS DISTINCT FROM`:
    例えば Q76 の `'store' / 'web' / 'catalog'` リテラル列を含むキー）であれば、
    **任意の集約関数 `aggFn`（`SUM / COUNT` だけでなく `AVG`・`COUNT(DISTINCT)`・複合式を含むすべて！）** と
    **任意の `HAVING` 条件 `havingPred`** について、外側の再 `GROUP BY` は完全に不要となり、
    各枝の `GROUP BY + HAVING` をそのまま `UNION ALL` した結果と Bag 同値になる！ -/
theorem disjoint_union_group_by_bypass_sound {Row K V : Type} [DecidableEq K]
    (key : Row → K) (aggFn : K → Rel Row → V) (havingPred : (K × V) → Bool)
    (r1 r2 : Rel Row)
    (hDisjoint : ∀ x ∈ r1, ∀ y ∈ r2, key x ≠ key y) :
    BagEq
      ((groupBy key aggFn (r1 ++ r2)).filter havingPred)
      ((groupBy key aggFn r1).filter havingPred ++
       (groupBy key aggFn r2).filter havingPred) := by
  suffices hBase : BagEq (groupBy key aggFn (r1 ++ r2))
      (groupBy key aggFn r1 ++ groupBy key aggFn r2) by
    simpa [List.filter_append] using hBase.filter havingPred
  unfold groupBy
  have hKeysPerm : BagEq (distinctKeys ((r1 ++ r2).map key))
      (distinctKeys (r1.map key) ++ distinctKeys (r2.map key)) := by
    apply perm_of_nodup_iff_mem (nodup_distinctKeys _)
    · rw [List.nodup_append]
      refine ⟨nodup_distinctKeys _, nodup_distinctKeys _, ?_⟩
      intro k hk1 k' hk2 hEq
      subst hEq
      rw [mem_distinctKeys, List.mem_map] at hk1 hk2
      obtain ⟨x, hx, rfl⟩ := hk1
      obtain ⟨y, hy, hEq2⟩ := hk2
      exact hDisjoint x hx y hy hEq2.symm
    · intro k
      simp [mem_distinctKeys, List.map_append, List.mem_append]
  have hMapPerm := hKeysPerm.map (fun k =>
    (k, aggFn k ((r1 ++ r2).filter (fun row => key row == k))))
  refine hMapPerm.trans ?_
  rw [List.map_append]
  have hLeft :
      (distinctKeys (r1.map key)).map
        (fun k => (k, aggFn k ((r1 ++ r2).filter (fun row => key row == k)))) =
      (distinctKeys (r1.map key)).map
        (fun k => (k, aggFn k (r1.filter (fun row => key row == k)))) := by
    apply List.map_congr_left
    intro k hk
    rw [mem_distinctKeys, List.mem_map] at hk
    obtain ⟨x, hx, rfl⟩ := hk
    have hEmpty : r2.filter (fun row => key row == key x) = [] := by
      apply filter_eq_nil_of_forall_false
      intro y hy
      simp only [beq_eq_false_iff_ne]
      exact fun hEq => hDisjoint x hx y hy hEq.symm
    simp [List.filter_append, hEmpty]
  have hRight :
      (distinctKeys (r2.map key)).map
        (fun k => (k, aggFn k ((r1 ++ r2).filter (fun row => key row == k)))) =
      (distinctKeys (r2.map key)).map
        (fun k => (k, aggFn k (r2.filter (fun row => key row == k)))) := by
    apply List.map_congr_left
    intro k hk
    rw [mem_distinctKeys, List.mem_map] at hk
    obtain ⟨y, hy, rfl⟩ := hk
    have hEmpty : r1.filter (fun row => key row == key y) = [] := by
      apply filter_eq_nil_of_forall_false
      intro x hx
      simp only [beq_eq_false_iff_ne]
      exact hDisjoint x hx y hy
    simp [List.filter_append, hEmpty]
  rw [hLeft, hRight]

/-!
## 2. 自己結合集約の Window 関数 1 パス吸収（TPC-DS Q65, Q1, Q30, Q81）
-/

/-- パーティション単位のフレームなし Window 関数評価 `AGG(...) OVER (PARTITION BY partKey)`。 -/
def evalPartitionWindow {Row K V : Type} [DecidableEq K]
    (partKey : Row → K) (aggFn : Rel Row → V) (r : Rel Row) : Rel (Row × V) :=
  r.map (fun row => (row, aggFn (r.filter (fun r' => partKey r' == partKey row))))

/-- ❌ **罠 2 (`self_join_agg_window_null_key_bug`)**:
    `WHERE g IS NOT NULL` を付けずに `S ⋈_{S.g = B.g} γ_g(S)` を `Window OVER (PARTITION BY g)` に書き換えると、
    `g = NULL` の行が Window 側では生き残り、等値結合側では消えるため誤答になる！ -/
theorem self_join_agg_window_null_key_bug :
    let S : Rel (SqlVal Int × Int) := [(none, 5), (none, 15)]
    let B := groupBy Prod.fst (fun _ b => sqlSum (fun r => some r.2) b) S
    let selfJoinAll := (innerJoin (fun s b => sqlEq s.1 b.1) S B).map Prod.fst
    let badWindowAll := (evalPartitionWindow Prod.fst (fun b => sqlSum (fun r => some r.2) b) S).map Prod.fst
    selfJoinAll = [] ∧ badWindowAll = [(none, 5), (none, 15)] ∧ selfJoinAll ≠ badWindowAll := by
  decide

/-- ✅ **定理 2 (`self_join_group_agg_to_window_sound` — TPC-DS Q65, Q1, Q30, Q81)**:
    任意のテーブル（または 1 段目集約結果）`S`、NULL 許容キー `g : Row → SqlVal K`、
    任意の集約関数 `aggFn`（`AVG` / `SUM` / `MIN` / `MAX` / `COUNT` 等すべて）、
    および比較述語 `pred : Row → V → SqlBool`（例: Q65 の `sc.revenue <= 0.1 * sb.ave`、
    Q1/Q81 の `ctr1.ctr_total_return > 1.2 * avg_ret`）に対して：
    `S` とそのグループ集約 `B := γ_{g; aggFn}(S)` を `S.g = B.g` で結合して `pred` で絞る処理は、
    `S` を **1 回だけ** 走査して `Window(aggFn OVER (PARTITION BY g))` を計算し
    `g IS NOT NULL AND pred` で絞る処理と **行順序・重複度まで含めて完全一致** する！ -/
theorem self_join_group_agg_to_window_sound {Row K V : Type} [DecidableEq K]
    (g : Row → SqlVal K) (aggFn : Rel Row → V) (pred : Row → V → SqlBool)
    (S : Rel Row) :
    let B := groupBy g (fun _ b => aggFn b) S
    ((innerJoin (fun s b => sqlEq (g s) b.1) S B).filter (fun sb =>
      match sb.2 with
      | none => false
      | some (_, v) => (pred sb.1 v).inWhere)).map Prod.fst =
    ((evalPartitionWindow g aggFn S).filter (fun rw =>
      (g rw.1).isSome && (pred rw.1 rw.2).inWhere)).map Prod.fst := by
  intro B
  unfold innerJoin evalPartitionWindow
  have hStep : ∀ s ∈ S,
      (((B.filter (fun b => (sqlEq (g s) b.1).inWhere)).map (fun b => (s, some b))).filter
        (fun sb => match sb.2 with | none => false | some (_, v) => (pred sb.1 v).inWhere)).map Prod.fst =
      if (g s).isSome && (pred s (aggFn (S.filter (fun r' =>
        @BEq.beq (SqlVal K) instBEqOfDecidableEq (g r') (g s))))).inWhere
      then [s] else [] := by
    intro s hs
    cases hgs : g s with
    | none =>
      have hNil : B.filter (fun b => (sqlEq none b.1).inWhere) = [] := by
        apply filter_eq_nil_of_forall_false
        intro b _
        rfl
      simp [hNil]
    | some kv =>
      let targetV := aggFn (S.filter (fun row =>
        @BEq.beq (SqlVal K) instBEqOfDecidableEq (g row) (some kv)))
      have hTargetMem : (some kv, targetV) ∈ B := by
        dsimp [B, groupBy]
        rw [List.mem_map]
        refine ⟨some kv, ?_, rfl⟩
        rw [mem_distinctKeys, List.mem_map]
        exact ⟨s, hs, hgs⟩
      have hSingleton : B.filter (fun b => (sqlEq (some kv) b.1).inWhere) =
          [(some kv, targetV)] := by
        apply filter_unique_of_nodup_mem _ (nodup_groupBy g (fun _ b => aggFn b) S) hTargetMem
        · simp
        · intro b hb hEq
          dsimp [B, groupBy] at hb
          rw [List.mem_map] at hb
          obtain ⟨k', _, rfl⟩ := hb
          cases k' with
          | none => simp [sqlEq, SqlBool.inWhere] at hEq
          | some kv' =>
            simp only [sqlEq_some_inWhere] at hEq
            subst hEq
            rfl
      rw [hSingleton]
      dsimp only [List.map, List.filter]
      cases (pred s targetV).inWhere <;> simp
  suffices hGen : ∀ xs : Rel Row, (∀ s ∈ xs, s ∈ S) →
      (((xs.flatMap fun s =>
          (B.filter (fun b => (sqlEq (g s) b.1).inWhere)).map (fun b => (s, some b))).filter
        (fun sb => match sb.2 with | none => false | some (_, v) => (pred sb.1 v).inWhere)).map Prod.fst) =
      (((xs.map (fun row => (row, aggFn (S.filter (fun r' =>
          @BEq.beq (SqlVal K) instBEqOfDecidableEq (g r') (g row)))))).filter (fun rw =>
        (g rw.1).isSome && (pred rw.1 rw.2).inWhere)).map Prod.fst) by
    exact hGen S (fun _ h => h)
  intro xs hSub
  induction xs with
  | nil => rfl
  | cons s xs ih =>
    have hs : s ∈ S := hSub s (List.mem_cons_self ..)
    have hxs : ∀ x ∈ xs, x ∈ S := fun x hx => hSub x (List.mem_cons_of_mem s hx)
    rw [List.flatMap_cons, List.filter_append, List.map_append, hStep s hs, ih hxs]
    simp only [List.map_cons, List.filter]
    cases ((g s).isSome && (pred s (aggFn (S.filter (fun r' =>
      @BEq.beq (SqlVal K) instBEqOfDecidableEq (g r') (g s))))).inWhere) <;> rfl

/-!
## 3. `UNION ALL` CTE 枝刈り・選言プッシュダウン ＆ 自己結合の条件付き集約ピボット（TPC-DS Q4, Q11, Q74）
-/

/-- ✅ **定理 3A (`union_cte_disjunctive_pushdown_and_branch_prune_sound`)**:
    1. 複数箇所から参照される CTE `U` に対し、全利用箇所のフィルタ `p1, p2` の選言 `p1 ∨ p2` で `U` を事前制限しても
       各利用箇所 `select p1` / `select p2` の結果は 1 行も変わらない。
    2. `U = B1 ++ B2` の各枝が定数タグ `tag = t1`, `tag = t2` (`t1 ≠ t2`) を持つとき、
       `tag = t1` を要求する利用箇所では枝 `B2` を完全に除去できる。 -/
theorem union_cte_disjunctive_pushdown_and_branch_prune_sound {Row Tag : Type} [DecidableEq Tag]
    (p1 p2 : Row → SqlBool) (tagOf : Row → Tag) (t1 t2 : Tag) (hNe : t1 ≠ t2)
    (B1 B2 U : Rel Row)
    (hTag1 : ∀ r ∈ B1, tagOf r = t1)
    (hTag2 : ∀ r ∈ B2, tagOf r = t2) :
    let narrowed := select (fun r => (p1 r).or (p2 r)) U
    (select p1 narrowed = select p1 U ∧ select p2 narrowed = select p2 U) ∧
    select (fun r => if tagOf r == t1 then p1 r else .tFalse) (B1 ++ B2) = select p1 B1 := by
  refine ⟨⟨?_, ?_⟩, ?_⟩
  · simp only [select, List.filter_filter]
    congr 1
    funext r
    cases p1 r <;> cases p2 r <;> rfl
  · simp only [select, List.filter_filter]
    congr 1
    funext r
    cases p1 r <;> cases p2 r <;> rfl
  · simp only [select, List.filter_append]
    have h1 : B1.filter (fun r => (if tagOf r == t1 then p1 r else .tFalse).inWhere) =
        B1.filter (fun r => (p1 r).inWhere) := by
      apply List.filter_congr
      intro r hr
      simp [hTag1 r hr]
    have h2 : B2.filter (fun r => (if tagOf r == t1 then p1 r else .tFalse).inWhere) = [] := by
      apply filter_eq_nil_of_forall_false
      intro r hr
      have ht2 := hTag2 r hr
      have hFalse : (tagOf r == t1) = false := beq_eq_false_iff_ne.mpr (ht2 ▸ hNe.symm)
      simp [hFalse, SqlBool.inWhere]
    rw [h1, h2, List.append_nil]

/-- ❌ **罠 3B-1 (`self_join_pivot_null_key_bug`)**:
    年次比較自己結合 `(yr=1999) ⋈_k (yr=2000)` を条件付き `GROUP BY k` に畳み込む際、
    `k IS NOT NULL` を忘れると `NULL` 顧客キーの行が出力されてしまい、等値結合（`NULL = NULL` を棄却）と食い違う。 -/
theorem self_join_pivot_null_key_bug :
    let R : Rel (SqlVal Int × Int × SqlVal Int) := [(none, 1999, some 10), (none, 2000, some 20)]
    let g1 := groupBy Prod.fst (fun _ b => sqlSum (fun r => r.2.2) b) (R.filter (fun r => r.2.1 == 1999))
    let g2 := groupBy Prod.fst (fun _ b => sqlSum (fun r => r.2.2) b) (R.filter (fun r => r.2.1 == 2000))
    let selfJ := innerJoin (fun a b => sqlEq a.1 b.1) g1 g2
    let naivePivot := groupBy Prod.fst
      (fun _ b => (sqlSum (fun r => if r.2.1 == 1999 then r.2.2 else none) b,
                   sqlSum (fun r => if r.2.1 == 2000 then r.2.2 else none) b)) R
    selfJ.length = 0 ∧ naivePivot.length = 1 := by
  decide

/-- ❌ **罠 3B-2 (`self_join_pivot_missing_year_bug`)**:
    `k IS NOT NULL` を付けても、片方の年（`2000` 年）に 1 行も売上が無い顧客がいると、
    `HAVING COUNT(CASE WHEN yr=2000 THEN 1 END) > 0` を付けない素朴なピボットはその顧客を残してしまう！ -/
theorem self_join_pivot_missing_year_bug :
    let R : Rel (SqlVal Int × Int × SqlVal Int) := [(some 1, 1999, some 10)]
    let g1 := groupBy Prod.fst (fun _ b => sqlSum (fun r => r.2.2) b) (R.filter (fun r => r.2.1 == 1999))
    let g2 := groupBy Prod.fst (fun _ b => sqlSum (fun r => r.2.2) b) (R.filter (fun r => r.2.1 == 2000))
    let selfJ := innerJoin (fun a b => sqlEq a.1 b.1) g1 g2
    let naivePivot := groupBy Prod.fst
      (fun _ b => (sqlSum (fun r => if r.2.1 == 1999 then r.2.2 else none) b,
                   sqlSum (fun r => if r.2.1 == 2000 then r.2.2 else none) b))
      (R.filter (fun r => r.1.isSome))
    selfJ.length = 0 ∧ naivePivot.length = 1 := by
  decide

private theorem conditional_sum_eq_filter_sum {Row : Type}
    (cond : Row → Bool) (val : Row → SqlVal Int) :
    ∀ xs : Rel Row,
      sqlSum (fun r => if cond r then val r else none) xs =
      sqlSum val (xs.filter cond)
  | [] => rfl
  | x :: xs => by
    cases hc : cond x <;>
      simp [List.filter, hc, conditional_sum_eq_filter_sum cond val xs, mergeSum]

/-- 条件付き件数 `COUNT(CASE WHEN cond THEN 1 END)`。 -/
def sqlCountIf {Row : Type} (cond : Row → Bool) (xs : Rel Row) : Int :=
  aggFold countMonoid (fun r => if cond r then 1 else 0) xs

private theorem sqlCountIf_nonneg {Row : Type} (cond : Row → Bool) :
    ∀ xs : Rel Row, 0 ≤ sqlCountIf cond xs
  | [] => Int.le_refl 0
  | y :: ys => by
    have ih := sqlCountIf_nonneg cond ys
    change 0 ≤ (if cond y then 1 else 0) + sqlCountIf cond ys
    cases cond y <;> omega

private theorem sqlCountIf_pos_iff_filter_nonempty {Row : Type}
    (cond : Row → Bool) :
    ∀ xs : Rel Row, (sqlCountIf cond xs > 0) ↔ (xs.filter cond ≠ [])
  | [] => by simp [sqlCountIf, aggFold, countMonoid]
  | x :: xs => by
    have ih := sqlCountIf_pos_iff_filter_nonempty cond xs
    have hGe := sqlCountIf_nonneg cond xs
    change ((if cond x then 1 else 0) + sqlCountIf cond xs > 0) ↔ ((x :: xs).filter cond ≠ [])
    cases hc : cond x
    · simp only [List.filter, hc]
      change (0 + sqlCountIf cond xs > 0) ↔ (xs.filter cond ≠ [])
      rw [Int.zero_add]
      exact ih
    · simp only [List.filter, hc]
      change (1 + sqlCountIf cond xs > 0) ↔ (x :: xs.filter cond ≠ [])
      constructor
      · intro _; simp
      · intro _; omega

/-- ✅ **定理 3B (`grouped_self_join_conditional_pivot_sound` — TPC-DS Q4, Q11, Q74)**:
    同テーブル（または同チャネル枝）の 2 年間比較自己結合
    `(SELECT k, SUM(v) FROM R WHERE yr = y1 GROUP BY k) ⋈_{k} (SELECT k, SUM(v) FROM R WHERE yr = y2 GROUP BY k)` は、
    **`WHERE k IS NOT NULL AND yr IN (y1, y2)` ＋ 単一 `GROUP BY k` ＋
    `HAVING COUNT(CASE WHEN yr=y1 THEN 1 END) > 0 AND COUNT(CASE WHEN yr=y2 THEN 1 END) > 0`**
    という **1 パス条件付き集約** と（`v` が全行 `NULL` の年を含め）完全に Bag 同値である！ -/
theorem grouped_self_join_conditional_pivot_sound
    (y1 y2 : Int)
    (R : Rel (SqlVal Int × Int × SqlVal Int)) :
    let g1 := groupBy Prod.fst (fun _ b => sqlSum (fun r => r.2.2) b)
      (R.filter (fun r => r.2.1 == y1))
    let g2 := groupBy Prod.fst (fun _ b => sqlSum (fun r => r.2.2) b)
      (R.filter (fun r => r.2.1 == y2))
    let selfJoined := (innerJoin (fun a b => sqlEq a.1 b.1) g1 g2).map (fun ab =>
      match ab.2 with
      | some b => (ab.1.1, ab.1.2, b.2)
      | none   => (ab.1.1, ab.1.2, none))
    let rFiltered := R.filter (fun r => r.1.isSome && ((r.2.1 == y1) || (r.2.1 == y2)))
    let pivoted := ((groupBy Prod.fst
      (fun _ b =>
        (sqlSum (fun r => if r.2.1 == y1 then r.2.2 else none) b,
         sqlSum (fun r => if r.2.1 == y2 then r.2.2 else none) b,
         sqlCountIf (fun r => r.2.1 == y1) b,
         sqlCountIf (fun r => r.2.1 == y2) b))
      rFiltered).filter (fun kv => decide (kv.2.2.2.1 > 0) && decide (kv.2.2.2.2 > 0))).map
      (fun kv => (kv.1, kv.2.1, kv.2.2.1))
    BagEq selfJoined pivoted := by
  intro g1 g2 selfJoined rFiltered pivoted
  apply perm_of_nodup_iff_mem
  · -- `selfJoined.Nodup`
    dsimp [selfJoined, innerJoin]
    have hNodup1 : g1.Nodup := nodup_groupBy _ _ _
    have hNodup2 : g2.Nodup := nodup_groupBy _ _ _
    have hSub : ∀ (xs : Rel (SqlVal Int × SqlVal Int)), xs.Nodup →
        (∀ x ∈ xs, x ∈ g1) →
        ((xs.flatMap fun a =>
          (g2.filter (fun b => (sqlEq a.1 b.1).inWhere)).map (fun b => (a, some b))).map
          (fun ab => match ab.2 with | some b => (ab.1.1, ab.1.2, b.2) | none => (ab.1.1, ab.1.2, none))).Nodup := by
      intro xs hNx hInG1
      induction xs with
      | nil => simp
      | cons a xs ih =>
        rw [List.nodup_cons] at hNx
        rw [List.flatMap_cons, List.map_append]
        rcases a with ⟨a1, a2⟩
        cases a1 with
        | none =>
          have hEmpty : g2.filter (fun b => (sqlEq none b.1).inWhere) = [] := by
            apply filter_eq_nil_of_forall_false
            intro _ _; rfl
          simp [hEmpty]
          exact ih hNx.2 (fun x hx => hInG1 x (List.mem_cons_of_mem (none, a2) hx))
        | some kv =>
          by_cases hMem2 : ∃ v2, (some kv, v2) ∈ g2
          · obtain ⟨v2, hv2⟩ := hMem2
            have hSing : g2.filter (fun b => (sqlEq (some kv) b.1).inWhere) = [(some kv, v2)] := by
              apply filter_unique_of_nodup_mem _ hNodup2 hv2
              · simp
              · intro b hb hEq
                dsimp [g2, groupBy] at hb hv2
                rw [List.mem_map] at hb hv2
                obtain ⟨kb, _, rfl⟩ := hb
                obtain ⟨kv0, _, hEq0⟩ := hv2
                have hkv0 : kv0 = some kv := congrArg Prod.fst hEq0
                subst hkv0
                cases kb with
                | none => simp [sqlEq, SqlBool.inWhere] at hEq
                | some kv' =>
                  simp only [sqlEq_some_inWhere] at hEq
                  subst hEq
                  exact hEq0
            simp only [hSing, List.map_cons, List.map_nil, List.singleton_append, List.nodup_cons]
            refine ⟨?_, ih hNx.2 (fun x hx => hInG1 x (List.mem_cons_of_mem (some kv, a2) hx))⟩
            intro hContra
            rw [List.mem_map] at hContra
            obtain ⟨ab, hab, hOutEq⟩ := hContra
            rw [List.mem_flatMap] at hab
            obtain ⟨a', ha'xs, hab'⟩ := hab
            rw [List.mem_map] at hab'
            obtain ⟨b', _, rfl⟩ := hab'
            have ha'1 : a'.1 = some kv := congrArg Prod.fst hOutEq
            have haIn : (some kv, a2) ∈ g1 := hInG1 (some kv, a2) (List.mem_cons_self ..)
            have ha'In : a' ∈ g1 := hInG1 a' (List.mem_cons_of_mem (some kv, a2) ha'xs)
            dsimp [g1, groupBy] at haIn ha'In
            rw [List.mem_map] at haIn ha'In
            obtain ⟨ka, _, hEqa⟩ := haIn
            obtain ⟨ka', _, hEqa'⟩ := ha'In
            have hka : ka = some kv := congrArg Prod.fst hEqa
            have hka' : ka' = some kv := (congrArg Prod.fst hEqa').trans ha'1
            subst hka; subst hka'
            have hSame : a' = (some kv, a2) := hEqa'.symm.trans hEqa
            exact hNx.1 (hSame ▸ ha'xs)
          · have hEmpty : g2.filter (fun b => (sqlEq (some kv) b.1).inWhere) = [] := by
              apply filter_eq_nil_of_forall_false
              intro ⟨b1, b2⟩ hb
              cases b1 with
              | none => rfl
              | some kv' =>
                by_cases hkk : kv = kv'
                · subst hkk
                  exact absurd ⟨b2, hb⟩ hMem2
                · simp [sqlEq, SqlBool.inWhere, hkk]
            simp [hEmpty]
            exact ih hNx.2 (fun x hx => hInG1 x (List.mem_cons_of_mem (some kv, a2) hx))
    exact hSub g1 hNodup1 (fun _ h => h)
  · -- `pivoted.Nodup`
    dsimp [pivoted]
    let agg4 := fun (_ : SqlVal Int) (b : Rel (SqlVal Int × Int × SqlVal Int)) =>
      (sqlSum (fun r => if r.2.1 == y1 then r.2.2 else none) b,
       sqlSum (fun r => if r.2.1 == y2 then r.2.2 else none) b,
       sqlCountIf (fun r => r.2.1 == y1) b,
       sqlCountIf (fun r => r.2.1 == y2) b)
    have hBaseKeys : ((groupBy Prod.fst agg4 rFiltered).map Prod.fst).Nodup := by
      unfold groupBy
      simpa [List.map_map, Function.comp_def] using nodup_distinctKeys (rFiltered.map Prod.fst)
    have hFiltKeys :
        (((groupBy Prod.fst agg4 rFiltered).filter
          (fun kv => decide (kv.2.2.2.1 > 0) && decide (kv.2.2.2.2 > 0))).map Prod.fst).Nodup := by
      generalize groupBy Prod.fst agg4 rFiltered = grp at hBaseKeys ⊢
      induction grp with
      | nil => simp
      | cons g gs ih =>
        rw [List.map_cons, List.nodup_cons] at hBaseKeys
        simp only [List.filter]
        cases (decide (g.2.2.2.1 > 0) && decide (g.2.2.2.2 > 0))
        · exact ih hBaseKeys.2
        · rw [List.map_cons, List.nodup_cons]
          refine ⟨?_, ih hBaseKeys.2⟩
          rw [List.mem_map]
          rintro ⟨g', hg', hEq⟩
          rw [List.mem_filter] at hg'
          exact hBaseKeys.1 (hEq ▸ List.mem_map_of_mem (f := Prod.fst) hg'.1)
    generalize ((groupBy Prod.fst agg4 rFiltered).filter
      (fun kv => decide (kv.2.2.2.1 > 0) && decide (kv.2.2.2.2 > 0))) = filt at hFiltKeys ⊢
    induction filt with
    | nil => simp
    | cons x xs ih =>
      rw [List.map_cons, List.nodup_cons] at hFiltKeys
      rw [List.map_cons, List.nodup_cons]
      refine ⟨?_, ih hFiltKeys.2⟩
      rw [List.mem_map]
      rintro ⟨y, hy, hEq⟩
      have hkEq : y.1 = x.1 := (Prod.mk.inj hEq).1
      exact hFiltKeys.1 (hkEq ▸ List.mem_map_of_mem (f := Prod.fst) hy)
  · -- 両辺の要素一致 `(kOpt, s1, s2) ∈ selfJoined ↔ (kOpt, s1, s2) ∈ pivoted`
    intro ⟨kOpt, s1, s2⟩
    have hBucket1 : ∀ kv : Int,
        (rFiltered.filter (fun row =>
          @BEq.beq (SqlVal Int) instBEqOfDecidableEq row.1 (some kv))).filter (fun r => r.2.1 == y1) =
        (R.filter (fun r => r.2.1 == y1)).filter (fun row =>
          @BEq.beq (SqlVal Int) instBEqOfDecidableEq row.1 (some kv)) := by
      intro kv
      simp only [rFiltered, List.filter_filter, beq_of_decidableEq_option]
      apply List.filter_congr
      intro r _
      cases hr1 : r.1 with
      | none => simp
      | some kv' =>
        simp only [Option.isSome]
        cases (some kv' == some kv) <;> cases (r.2.1 == y1) <;> cases (r.2.1 == y2) <;> rfl
    have hBucket2 : ∀ kv : Int,
        (rFiltered.filter (fun row =>
          @BEq.beq (SqlVal Int) instBEqOfDecidableEq row.1 (some kv))).filter (fun r => r.2.1 == y2) =
        (R.filter (fun r => r.2.1 == y2)).filter (fun row =>
          @BEq.beq (SqlVal Int) instBEqOfDecidableEq row.1 (some kv)) := by
      intro kv
      simp only [rFiltered, List.filter_filter, beq_of_decidableEq_option]
      apply List.filter_congr
      intro r _
      cases hr1 : r.1 with
      | none => simp
      | some kv' =>
        simp only [Option.isSome]
        cases (some kv' == some kv) <;> cases (r.2.1 == y1) <;> cases (r.2.1 == y2) <;> rfl
    cases kOpt with
    | none =>
      constructor
      · intro hMem
        dsimp [selfJoined, innerJoin] at hMem
        rw [List.mem_map] at hMem
        obtain ⟨ab, hab, hEq⟩ := hMem
        rw [List.mem_flatMap] at hab
        obtain ⟨a, _, hab'⟩ := hab
        rw [List.mem_map] at hab'
        obtain ⟨b, hb, rfl⟩ := hab'
        rw [List.mem_filter] at hb
        have ha1 : a.1 = none := (Prod.mk.inj hEq).1
        rw [ha1] at hb
        simp [sqlEq, SqlBool.inWhere] at hb
      · intro hMem
        dsimp [pivoted, groupBy] at hMem
        rw [List.mem_map] at hMem
        obtain ⟨tup, htup, hEq⟩ := hMem
        rw [List.mem_filter, List.mem_map] at htup
        obtain ⟨⟨k', hk', rfl⟩, _⟩ := htup
        have hkNone : k' = none := (Prod.mk.inj hEq).1
        subst hkNone
        rw [mem_distinctKeys, List.mem_map] at hk'
        obtain ⟨r, hr, hr1⟩ := hk'
        dsimp [rFiltered] at hr
        rw [List.mem_filter] at hr
        rw [hr1] at hr
        simp at hr
    | some kv =>
      let bkt := rFiltered.filter (fun row =>
        @BEq.beq (SqlVal Int) instBEqOfDecidableEq row.1 (some kv))
      let bkt1 := (R.filter (fun r => r.2.1 == y1)).filter (fun row =>
        @BEq.beq (SqlVal Int) instBEqOfDecidableEq row.1 (some kv))
      let bkt2 := (R.filter (fun r => r.2.1 == y2)).filter (fun row =>
        @BEq.beq (SqlVal Int) instBEqOfDecidableEq row.1 (some kv))
      constructor
      · intro hMem
        dsimp [selfJoined, innerJoin] at hMem
        rw [List.mem_map] at hMem
        obtain ⟨ab, hab, hEq⟩ := hMem
        rw [List.mem_flatMap] at hab
        obtain ⟨a, ha, hab'⟩ := hab
        rw [List.mem_map] at hab'
        obtain ⟨b, hb, rfl⟩ := hab'
        rw [List.mem_filter] at hb
        obtain ⟨hbIn, hSqlEq⟩ := hb
        have ha1 : a.1 = some kv := (Prod.mk.inj hEq).1
        have hs1 : a.2 = s1 := (Prod.mk.inj (Prod.mk.inj hEq).2).1
        have hs2 : b.2 = s2 := (Prod.mk.inj (Prod.mk.inj hEq).2).2
        subst hs1; subst hs2
        have hb1 : b.1 = some kv := by
          rw [ha1] at hSqlEq
          cases hbk : b.1 with
          | none => simp [hbk, sqlEq, SqlBool.inWhere] at hSqlEq
          | some kv' =>
            rw [hbk, sqlEq_some_inWhere] at hSqlEq
            exact congrArg some hSqlEq.symm
        dsimp [g1, groupBy] at ha
        dsimp [g2, groupBy] at hbIn
        rw [List.mem_map] at ha hbIn
        obtain ⟨k1, hk1, hPair1⟩ := ha
        obtain ⟨k2, hk2, hPair2⟩ := hbIn
        have hk1Eq : k1 = some kv := (Prod.mk.inj hPair1).1.trans ha1
        have hk2Eq : k2 = some kv := (Prod.mk.inj hPair2).1.trans hb1
        subst hk1Eq; subst hk2Eq
        have hv1Eq : sqlSum (fun r => r.2.2) bkt1 = a.2 := (Prod.mk.inj hPair1).2
        have hv2Eq : sqlSum (fun r => r.2.2) bkt2 = b.2 := (Prod.mk.inj hPair2).2
        rw [mem_distinctKeys, List.mem_map] at hk1 hk2
        obtain ⟨r1, hr1, hr1k⟩ := hk1
        obtain ⟨r2, hr2, hr2k⟩ := hk2
        rw [List.mem_filter] at hr1 hr2
        have hr1Filt : r1 ∈ rFiltered := by
          dsimp [rFiltered]
          rw [List.mem_filter]
          refine ⟨hr1.1, ?_⟩
          simp [hr1k, hr1.2]
        have hNonempty1 : bkt.filter (fun r => r.2.1 == y1) ≠ [] := by
          dsimp [bkt]
          rw [hBucket1]
          intro hNil
          have : r1 ∈ bkt1 := by
            dsimp [bkt1]
            rw [List.mem_filter, List.mem_filter]
            exact ⟨⟨hr1.1, hr1.2⟩, by simp [hr1k]⟩
          dsimp only [bkt1] at this
          rw [hNil] at this
          nomatch this
        have hNonempty2 : bkt.filter (fun r => r.2.1 == y2) ≠ [] := by
          dsimp [bkt]
          rw [hBucket2]
          intro hNil
          have : r2 ∈ bkt2 := by
            dsimp [bkt2]
            rw [List.mem_filter, List.mem_filter]
            exact ⟨⟨hr2.1, hr2.2⟩, by simp [hr2k]⟩
          dsimp only [bkt2] at this
          rw [hNil] at this
          nomatch this
        have hCnt1 : sqlCountIf (fun r => r.2.1 == y1) bkt > 0 :=
          (sqlCountIf_pos_iff_filter_nonempty _ _).mpr hNonempty1
        have hCnt2 : sqlCountIf (fun r => r.2.1 == y2) bkt > 0 :=
          (sqlCountIf_pos_iff_filter_nonempty _ _).mpr hNonempty2
        dsimp [pivoted, groupBy]
        rw [List.mem_map]
        refine ⟨(some kv,
          sqlSum (fun r => if r.2.1 == y1 then r.2.2 else none) bkt,
          sqlSum (fun r => if r.2.1 == y2 then r.2.2 else none) bkt,
          sqlCountIf (fun r => r.2.1 == y1) bkt,
          sqlCountIf (fun r => r.2.1 == y2) bkt), ?_, ?_⟩
        · rw [List.mem_filter]
          refine ⟨?_, ?_⟩
          · rw [List.mem_map]
            refine ⟨some kv, ?_, rfl⟩
            rw [mem_distinctKeys, List.mem_map]
            exact ⟨r1, hr1Filt, hr1k⟩
          · simp only [Bool.and_eq_true, decide_eq_true_eq]
            exact ⟨hCnt1, hCnt2⟩
        · dsimp only
          rw [conditional_sum_eq_filter_sum, conditional_sum_eq_filter_sum]
          dsimp [bkt]
          rw [hBucket1, hBucket2]
          change (some kv, sqlSum (fun r => r.2.2) bkt1, sqlSum (fun r => r.2.2) bkt2) = _
          rw [hv1Eq, hv2Eq]
      · intro hMem
        dsimp [pivoted, groupBy] at hMem
        rw [List.mem_map] at hMem
        obtain ⟨tup, htup, hOut⟩ := hMem
        rw [List.mem_filter, List.mem_map] at htup
        obtain ⟨⟨k', hk', rfl⟩, hPos⟩ := htup
        have hkEq : k' = some kv := (Prod.mk.inj hOut).1
        subst hkEq
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hPos
        have hNe1 : bkt.filter (fun r => r.2.1 == y1) ≠ [] :=
          (sqlCountIf_pos_iff_filter_nonempty _ bkt).mp hPos.1
        have hNe2 : bkt.filter (fun r => r.2.1 == y2) ≠ [] :=
          (sqlCountIf_pos_iff_filter_nonempty _ bkt).mp hPos.2
        dsimp [bkt] at hNe1 hNe2
        rw [hBucket1] at hNe1
        rw [hBucket2] at hNe2
        change bkt1 ≠ [] at hNe1
        change bkt2 ≠ [] at hNe2
        cases hList1 : bkt1 with
        | nil => exact absurd hList1 hNe1
        | cons r1 rs1 =>
          cases hList2 : bkt2 with
          | nil => exact absurd hList2 hNe2
          | cons r2 rs2 =>
            have hr1Mem : r1 ∈ bkt1 := by rw [hList1]; exact List.mem_cons_self ..
            have hr2Mem : r2 ∈ bkt2 := by rw [hList2]; exact List.mem_cons_self ..
            dsimp [bkt1, bkt2] at hr1Mem hr2Mem
            rw [List.mem_filter] at hr1Mem hr2Mem
            have hr1Key : r1.1 = some kv := by simpa using hr1Mem.2
            have hr2Key : r2.1 = some kv := by simpa using hr2Mem.2
            have hs1Raw : sqlSum (fun r => if r.2.1 == y1 then r.2.2 else none) bkt = s1 :=
              (Prod.mk.inj (Prod.mk.inj hOut).2).1
            have hs2Raw : sqlSum (fun r => if r.2.1 == y2 then r.2.2 else none) bkt = s2 :=
              (Prod.mk.inj (Prod.mk.inj hOut).2).2
            rw [conditional_sum_eq_filter_sum] at hs1Raw hs2Raw
            dsimp [bkt] at hs1Raw hs2Raw
            rw [hBucket1] at hs1Raw
            rw [hBucket2] at hs2Raw
            change sqlSum (fun r => r.2.2) bkt1 = s1 at hs1Raw
            change sqlSum (fun r => r.2.2) bkt2 = s2 at hs2Raw
            dsimp [selfJoined, innerJoin]
            rw [List.mem_map]
            refine ⟨((some kv, s1), some (some kv, s2)), ?_, rfl⟩
            rw [List.mem_flatMap]
            refine ⟨(some kv, s1), ?_, ?_⟩
            · dsimp [g1, groupBy]
              rw [List.mem_map]
              refine ⟨some kv, ?_, ?_⟩
              · rw [mem_distinctKeys, List.mem_map]
                exact ⟨r1, hr1Mem.1, hr1Key⟩
              · change (some kv, sqlSum (fun r => r.2.2) bkt1) = (some kv, s1)
                rw [hs1Raw]
            · rw [List.mem_map]
              refine ⟨(some kv, s2), ?_, rfl⟩
              rw [List.mem_filter]
              refine ⟨?_, by simp⟩
              dsimp [g2, groupBy]
              rw [List.mem_map]
              refine ⟨some kv, ?_, ?_⟩
              · rw [mem_distinctKeys, List.mem_map]
                exact ⟨r2, hr2Mem.1, hr2Key⟩
              · change (some kv, sqlSum (fun r => r.2.2) bkt2) = (some kv, s2)
                rw [hs2Raw]

/-!
## 4. ゼロ埋め `UNION ALL` ファクト事前集約と次元結合分配（TPC-DS Q5, Q75, Q80）
-/

/-- ❌ **罠 4A (`zero_padded_union_naive_null_bug`)**:
    Q5 の `store_sales` 枝にある `0 AS return_amt` を「ゼロだから無視（`none`）」にすると、
    `store_sales` に売上があり `store_returns` が 0 件の店舗で本来 `some 0` であるべき `SUM(return_amt)` が `none` (`NULL`) に化ける！ -/
theorem zero_padded_union_naive_null_bug :
    let b1 : Rel Int := [100]
    let b2 : Rel (SqlVal Int) := []
    sqlSum id (b1.map (fun _ => some 0) ++ b2) = some 0 ∧
    mergeSum none (sqlSum id b2) = none := by
  decide

/-- ❌ **罠 4B (`zero_padded_union_naive_coalesce_bug`)**:
    逆に外側を `COALESCE(SUM(sr_return_amt), 0)` で包むと、
    `store_sales` が 0 件かつ `store_returns` の `sr_return_amt` が `NULL` の店舗で本来 `none` (`NULL`) の結果が `some 0` に化ける！ -/
theorem zero_padded_union_naive_coalesce_bug :
    let b1 : Rel Int := []
    let b2 : Rel (SqlVal Int) := [none]
    sqlSum id (b1.map (fun _ => some 0) ++ b2) = none ∧
    some (sqlCoalesce (sqlSum id b2) 0) = some 0 := by
  decide

private theorem sqlSum_const_zero {A : Type} :
    ∀ xs : Rel A,
      sqlSum (fun _ => some 0) xs = (if sqlCountStar xs > 0 then some 0 else none)
  | [] => rfl
  | _ :: xs => by
    have hNonneg : ∀ ys : Rel A, 0 ≤ sqlCountStar ys := by
      intro ys
      induction ys with
      | nil => exact Int.le_refl 0
      | cons _ ys ih => simp [sqlCountStar_cons]; omega
    have hPos : 1 + sqlCountStar xs > 0 := by have := hNonneg xs; omega
    simp only [sqlSum_cons, sqlCountStar_cons, hPos, ↓reduceIte]
    rw [sqlSum_const_zero xs]
    split <;> rfl

/-- ✅ **定理 4 (`zero_padded_union_branch_preagg_sound` — TPC-DS Q5, Q75, Q80)**:
    1. `(U1 ++ U2) ⋈ D = (U1 ⋈ D) ++ (U2 ⋈ D)` により、`date_dim` のような高選択率の次元結合を各枝へ先行分配でき、
    2. 枝 1 のゼロ埋め列 `0 AS return_amt` は `CASE WHEN COUNT(*) > 0 THEN 0 ELSE NULL END` に置換することで、
       ゼロの反復加算を一切行わずに `NULL / 0 / 非ゼロ` の全境界ケースを 100% 保存する！ -/
theorem zero_padded_union_branch_preagg_sound {A B D : Type}
    (on1 : A → D → SqlBool) (u1 u2 : Rel A) (dim : Rel D)
    (b1 : Rel A) (b2 : Rel B) (val2 : B → SqlVal Int) :
    innerJoin on1 (u1 ++ u2) dim = innerJoin on1 u1 dim ++ innerJoin on1 u2 dim ∧
    sqlSum id (b1.map (fun _ => some 0) ++ b2.map val2) =
      mergeSum (if sqlCountStar b1 > 0 then some 0 else none) (sqlSum val2 b2) := by
  refine ⟨by simp [innerJoin, List.flatMap_append], ?_⟩
  unfold sqlSum
  rw [aggFold_append, aggFold_map, aggFold_map]
  change mergeSum (sqlSum (fun _ => some 0) b1) (sqlSum val2 b2) = _
  rw [sqlSum_const_zero]
  rfl

/-!
## 5. 末尾 `LEFT JOIN` 下の内側結合プレフィックス自由リオーダ ＆ 介在結合越し `AntiJoin` 変換（TPC-DS Q72, Q78）
-/

private theorem select_leftOuterJoin_left_comm {L R : Type}
    (wL : L → SqlBool) (p : L → R → SqlBool) (l : Rel L) (r : Rel R) :
    select (fun lr => wL lr.1) (leftOuterJoin p l r) =
    leftOuterJoin p (select wL l) r := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    simp only [leftOuterJoin, select, List.flatMap_cons, List.filter_append, List.filter] at ih ⊢
    cases hw : (wL x).inWhere
    · have hDrop :
          (let m := r.filter (fun y => (p x y).inWhere)
           if m.isEmpty then [(x, none)] else m.map (fun y => (x, some y))).filter
            (fun lr => (wL lr.1).inWhere) = [] := by
        dsimp only
        split
        · simp [List.filter, hw]
        · apply filter_eq_nil_of_forall_false
          intro lr hlr
          rw [List.mem_map] at hlr
          obtain ⟨_, _, rfl⟩ := hlr
          exact hw
      rw [hDrop, List.nil_append, ih]
    · have hKeep :
          (let m := r.filter (fun y => (p x y).inWhere)
           if m.isEmpty then [(x, none)] else m.map (fun y => (x, some y))).filter
            (fun lr => (wL lr.1).inWhere) =
          (let m := r.filter (fun y => (p x y).inWhere)
           if m.isEmpty then [(x, none)] else m.map (fun y => (x, some y))) := by
        dsimp only
        split
        · simp [List.filter, hw]
        · rw [List.filter_eq_self]
          intro lr hlr
          rw [List.mem_map] at hlr
          obtain ⟨_, _, rfl⟩ := hlr
          exact hw
      rw [hKeep, List.flatMap_cons, ih]

/-- ✅ **定理 5A (`inner_prefix_reorder_and_filter_under_left_joins_sound` — TPC-DS Q72)**:
    末尾に複数の `LEFT OUTER JOIN`（例: Q72 の `promotion`, `catalog_returns`）が連なっていても、
    その左入力にある内側結合グラフ `I1` を任意の Bag 同値な結合順序 `I2`（選択的な `date_dim`, `hd`, `cd` 先行！）に並べ替え、
    さらに内側テーブル群のみを参照する `WHERE` 条件 `wI` をすべての `LEFT OUTER JOIN` の下へ一気に押し込んでも
    クエリ全体の Bag 多重度は 100% 保存される！ -/
theorem inner_prefix_reorder_and_filter_under_left_joins_sound {I O1 O2 : Type}
    {I1 I2 : Rel I} (hReorder : BagEq I1 I2)
    (p1 : I → O1 → SqlBool) (p2 : (I × Option O1) → O2 → SqlBool)
    (wI : I → SqlBool) (o1 : Rel O1) (o2 : Rel O2) :
    BagEq
      (select (fun r => wI r.1.1) (leftOuterJoin p2 (leftOuterJoin p1 I1 o1) o2))
      (leftOuterJoin p2 (leftOuterJoin p1 (select wI I2) o1) o2) := by
  rw [select_leftOuterJoin_left_comm (fun xo => wI xo.1) p2,
      select_leftOuterJoin_left_comm wI p1]
  have hSelPerm : BagEq (select wI I1) (select wI I2) := hReorder.filter _
  have hL1 : BagEq (leftOuterJoin p1 (select wI I1) o1) (leftOuterJoin p1 (select wI I2) o1) :=
    perm_flatMap _ hSelPerm
  exact perm_flatMap _ hL1

/-- ✅ **定理 5B (`outer_join_intervening_inner_to_antijoin_sound` — TPC-DS Q78)**:
    Q78 の `(sales LEFT JOIN returns ON pB) INNER JOIN date_dim ON pC(sales, date_dim) WHERE returns.key IS NULL` のように、
    `LEFT JOIN` と `WHERE ... IS NULL` の間に別テーブル `C` との `INNER JOIN` が挟まっている場合でも、
    `pB` が `B` の検査列 `keyB` で Null-Rejecting（等値結合 `wr_order_number = ws_order_number`）であれば、
    **先に `sales ⋈ date_dim` で年フィルタを効かせてから `returns` に対する `AntiJoin` を打つ**
    `antiJoin (fun ac b => pB ac.1 b) (innerJoin pC A C) B` と行順序まで含めて完全一致する！ -/
theorem outer_join_intervening_inner_to_antijoin_sound {A B C K : Type}
    (pB : A → B → SqlBool) (pC : A → C → SqlBool) (keyB : B → SqlVal K)
    (hNullRejB : ∀ a b, keyB b = none → (pB a b).inWhere = false)
    (A_rel : Rel A) (B_rel : Rel B) (C_rel : Rel C) :
    ((select
        (fun abc => match abc.1.2 with
          | none => .tTrue
          | some b => match keyB b with | none => .tTrue | some _ => .tFalse)
        (innerJoin (fun ab c => pC ab.1 c) (leftOuterJoin pB A_rel B_rel) C_rel)).map
      (fun abc => (abc.1.1, abc.2))) =
    antiJoin (fun ac b => pB ac.1 b) (innerJoin pC A_rel C_rel) B_rel := by
  induction A_rel with
  | nil => rfl
  | cons a as ih =>
    simp only [leftOuterJoin, innerJoin, antiJoin, select, List.flatMap_cons,
      List.flatMap_append, List.filter_append, List.map_append] at ih ⊢
    rw [ih]
    congr 1
    cases hm : B_rel.filter (fun b => (pB a b).inWhere) with
    | cons b0 bs =>
      have hAny : B_rel.any (fun b => (pB a b).inWhere) = true := by
        have hb0 : b0 ∈ B_rel.filter (fun b => (pB a b).inWhere) := by
          rw [hm]; exact List.mem_cons_self ..
        rw [List.mem_filter] at hb0
        exact List.any_eq_true.mpr ⟨b0, hb0.1, hb0.2⟩
      have hLHSNil :
          (((((b0 :: bs).map (fun b => (a, some b))).flatMap fun ab =>
            (C_rel.filter (fun c => (pC ab.1 c).inWhere)).map (fun c => (ab, some c))).filter
            (fun abc => (match abc.1.2 with
              | none => SqlBool.tTrue
              | some b => match keyB b with | none => .tTrue | some _ => .tFalse).inWhere)).map
            (fun abc => (abc.1.1, abc.2))) = [] := by
        rw [List.map_eq_nil_iff]
        apply filter_eq_nil_of_forall_false
        intro abc habc
        rw [List.mem_flatMap] at habc
        obtain ⟨ab, hab, habc'⟩ := habc
        rw [List.mem_map] at hab
        obtain ⟨b, hbInCons, rfl⟩ := hab
        have hbFilt : b ∈ B_rel.filter (fun b => (pB a b).inWhere) := by
          rw [hm]; exact hbInCons
        rw [List.mem_filter] at hbFilt
        rw [List.mem_map] at habc'
        obtain ⟨c, _, rfl⟩ := habc'
        cases hkb : keyB b with
        | none =>
          have := hNullRejB a b hkb
          rw [this] at hbFilt
          simp at hbFilt
        | some _ =>
          simp [hkb, SqlBool.inWhere]
      have hRHSNil :
          ((C_rel.filter (fun c => (pC a c).inWhere)).map (fun c => (a, some c))).filter
            (fun ac => !(B_rel.any (fun b => (pB ac.1 b).inWhere))) = [] := by
        apply filter_eq_nil_of_forall_false
        intro ac hac
        rw [List.mem_map] at hac
        obtain ⟨c, _, rfl⟩ := hac
        simp [hAny]
      simp only [List.isEmpty, Bool.false_eq_true, ↓reduceIte]
      rw [hLHSNil, hRHSNil]
    | nil =>
      have hAnyFalse : B_rel.any (fun b => (pB a b).inWhere) = false := by
        cases hAny : B_rel.any (fun b => (pB a b).inWhere)
        · rfl
        · rw [List.any_eq_true] at hAny
          obtain ⟨b, hb, hpb⟩ := hAny
          have : b ∈ B_rel.filter (fun b => (pB a b).inWhere) := List.mem_filter.mpr ⟨hb, hpb⟩
          rw [hm] at this
          nomatch this
      simp only [List.isEmpty, ↓reduceIte, List.flatMap_cons, List.flatMap_nil, List.append_nil]
      have hKeepL :
          (((C_rel.filter (fun c => (pC a c).inWhere)).map (fun c => ((a, (none : Option B)), some c))).filter
            (fun abc => (match abc.1.2 with
              | none => SqlBool.tTrue
              | some b => match keyB b with | none => .tTrue | some _ => .tFalse).inWhere)) =
          ((C_rel.filter (fun c => (pC a c).inWhere)).map (fun c => ((a, (none : Option B)), some c))) := by
        rw [List.filter_eq_self]
        intro abc habc
        rw [List.mem_map] at habc
        obtain ⟨c, _, rfl⟩ := habc
        rfl
      have hKeepR :
          (((C_rel.filter (fun c => (pC a c).inWhere)).map (fun c => (a, some c))).filter
            (fun ac => !(B_rel.any (fun b => (pB ac.1 b).inWhere)))) =
          ((C_rel.filter (fun c => (pC a c).inWhere)).map (fun c => (a, some c))) := by
        rw [List.filter_eq_self]
        intro ac hac
        rw [List.mem_map] at hac
        obtain ⟨c, _, rfl⟩ := hac
        simp [hAnyFalse]
      rw [hKeepL, hKeepR, List.map_map]
      rfl

/-!
## 6. 次元結合前のサロゲートキー先行 `DISTINCT`（Q38, Q87）＆ `FULL OUTER JOIN` 指標和の包除原理（Q97）
-/

/-- ✅ **定理 6A (`eager_distinct_surrogate_keys_before_dim_joins_sound` — TPC-DS Q38, Q87)**:
    `SELECT DISTINCT outFn(c, d) FROM F ⋈_{onC(sk(f), c)} C ⋈_{onD(sk(f), d)} D` において、
    **次元テーブル `C, D` に主キー制約や一意性制約が一切無くても**、
    ファクト表 `F` をサロゲートキー組 `sk(f)`（例: `(ss_customer_sk, ss_sold_date_sk)` の 2 列 `INT64`）だけで
    **次元結合の前に先行 `DISTINCT`（重複排除）** してよく、最終的な `SELECT DISTINCT` 出力は Bag 同値になる！ -/
theorem eager_distinct_surrogate_keys_before_dim_joins_sound {F SK C D Out : Type}
    [DecidableEq SK] [DecidableEq Out]
    (sk : F → SK) (onC : SK → C → SqlBool) (onD : SK → D → SqlBool)
    (outFn : C → D → Out)
    (F_rel : Rel F) (C_rel : Rel C) (D_rel : Rel D) :
    let fullJoinOut :=
      (F_rel.flatMap fun f =>
        (C_rel.filter (fun c => (onC (sk f) c).inWhere)).flatMap fun c =>
          (D_rel.filter (fun d => (onD (sk f) d).inWhere)).map fun d => outFn c d)
    let eagerDistinctOut :=
      ((distinctKeys (F_rel.map sk)).flatMap fun k =>
        (C_rel.filter (fun c => (onC k c).inWhere)).flatMap fun c =>
          (D_rel.filter (fun d => (onD k d).inWhere)).map fun d => outFn c d)
    BagEq (distinctKeys fullJoinOut) (distinctKeys eagerDistinctOut) := by
  intro fullJoinOut eagerDistinctOut
  apply perm_of_nodup_iff_mem (nodup_distinctKeys _) (nodup_distinctKeys _)
  intro o
  simp only [mem_distinctKeys, fullJoinOut, eagerDistinctOut, List.mem_flatMap, List.mem_map]
  constructor
  · rintro ⟨f, hf, c, hc, d, hd, rfl⟩
    exact ⟨sk f, ⟨f, hf, rfl⟩, c, hc, d, hd, rfl⟩
  · rintro ⟨k, ⟨f, hf, rfl⟩, c, hc, d, hd, rfl⟩
    exact ⟨f, hf, c, hc, d, hd, rfl⟩

/-- SQL の `FULL OUTER JOIN`:
    左の各行について右マッチ行（なければ `(some x, none)`）を出力し、さらに左のどの行ともマッチしなかった右行 `(none, some y)` を連結する。 -/
def tpcdsFullOuterJoin {L R : Type} (onPred : L → R → SqlBool) (l : Rel L) (r : Rel R) :
    Rel (Option L × Option R) :=
  let leftPart := l.flatMap fun x =>
    let m := r.filter (fun y => (onPred x y).inWhere)
    if m.isEmpty then [(some x, none)]
    else m.map (fun y => (some x, some y))
  let unmatchedRight := (r.filter (fun y => !(l.any (fun x => (onPred x y).inWhere)))).map
    (fun y => (none, some y))
  leftPart ++ unmatchedRight

/-- TPC-DS Q97 の `FULL OUTER JOIN` 3 指標集約 `(store_only, catalog_only, store_and_catalog)`。 -/
def evalQ97FullOuter (ssci csci : Rel (SqlVal Int × SqlVal Int)) :
    SqlVal Int × SqlVal Int × SqlVal Int :=
  let foj := tpcdsFullOuterJoin
    (fun s c => (sqlEq s.1 c.1).and (sqlEq s.2 c.2)) ssci csci
  let sHasCust (r : Option (SqlVal Int × SqlVal Int)) : Bool :=
    match r with | some (some _, _) => true | _ => false
  (sqlSum (fun sc => if sHasCust sc.1 && !sHasCust sc.2 then some 1 else some 0) foj,
   sqlSum (fun sc => if !sHasCust sc.1 && sHasCust sc.2 then some 1 else some 0) foj,
   sqlSum (fun sc => if sHasCust sc.1 && sHasCust sc.2 then some 1 else some 0) foj)

/-- ❌ **罠 6B-1 (`full_outer_indicator_partial_null_key_bug`)**:
    複合キー `(customer_sk, item_sk)` で `(some 10, none)` が両方の CTE に存在するとき、
    `FULL OUTER JOIN` は `item_sk` の `NULL = NULL` が偽なので `(1, 1, 0)` を返すが、
    `UNION ALL` を `(customer_sk, item_sk)` で `GROUP BY` する素朴な変換は `NULL` キー同士を同一視して `(0, 0, 1)` を返してしまう！ -/
theorem full_outer_indicator_partial_null_key_bug :
    let ssci : Rel (SqlVal Int × SqlVal Int) := [(some 10, none)]
    let csci : Rel (SqlVal Int × SqlVal Int) := [(some 10, none)]
    evalQ97FullOuter ssci csci = (some 1, some 1, some 0) := by
  decide

/-- ❌ **罠 6B-2 (`full_outer_indicator_empty_null_bug`)**:
    両 CTE が 0 行のとき `FULL OUTER JOIN` も 0 行なので 3 つの `SUM` はすべて `NULL` (`none`) になる（`some 0` ではない！）。 -/
theorem full_outer_indicator_empty_null_bug :
    evalQ97FullOuter [] [] = (none, none, none) := by
  decide

private theorem sqlSum_indicator_nonempty {α : Type} (p : α → Bool) :
    ∀ {xs : Rel α}, xs ≠ [] →
      sqlSum (fun x => if p x then some 1 else some 0) xs =
      some (Int.ofNat (xs.filter p).length)
  | [], h => absurd rfl h
  | [x], _ => by
    cases hp : p x <;> simp [sqlSum, aggFold, sumMonoid, mergeSum, List.filter, hp]
  | x :: y :: xs, _ => by
    have ih := sqlSum_indicator_nonempty p (xs := y :: xs) (by simp)
    simp only [sqlSum_cons, ih]
    cases hp : p x <;> simp [List.filter, hp, mergeSum] <;> omega

private theorem count_sub_inter_eq_diff {α : Type} [DecidableEq α] :
    ∀ (xs : List α) (q : α → Bool),
      (xs.filter (fun x => !q x)).length = xs.length - (xs.filter q).length
  | [], _ => rfl
  | x :: xs, q => by
    have ih := count_sub_inter_eq_diff xs q
    have hLe : (xs.filter q).length ≤ xs.length := List.length_filter_le q xs
    cases hq : q x <;> simp [List.filter, hq, ih] <;> omega

/-- ✅ **定理 6B (`full_outer_indicator_inclusion_exclusion_sound` — TPC-DS Q97)**:
    重複のない（`GROUP BY (customer_sk, item_sk)` 済みの）`ssci, csci` に対し、
    非並列の重い `FULL OUTER JOIN` を一切実行せず：
    - 両キー非 NULL の集合 `L_ok`, `R_ok` 間の **`SemiJoin` 一致件数 `inter := |L_ok ⋉ R_ok|`** と、
    - 部分 NULL 行数 `|L_part|`, `|R_part|`（`customer_sk IS NOT NULL AND item_sk IS NULL`）
    から **包除原理** で計算した 3 値（両表とも空のときのみ `(none, none, none)`）は、
    `evalQ97FullOuter ssci csci` と **全 NULL パターン・空集合境界を含め 100% 一致** する！ -/
theorem full_outer_indicator_inclusion_exclusion_sound
    (ssci csci : Rel (SqlVal Int × SqlVal Int))
    (hNodupS : ssci.Nodup) (hNodupC : csci.Nodup) :
    let isFullKey (x : SqlVal Int × SqlVal Int) : Bool := x.1.isSome && x.2.isSome
    let isPartKey (x : SqlVal Int × SqlVal Int) : Bool := x.1.isSome && x.2.isNone
    let sOk := ssci.filter isFullKey
    let cOk := csci.filter isFullKey
    let sPart := ssci.filter isPartKey
    let cPart := csci.filter isPartKey
    let inter := (sOk.filter (fun x => cOk.contains x)).length
    evalQ97FullOuter ssci csci =
      if ssci.isEmpty && csci.isEmpty then (none, none, none)
      else
        (some (Int.ofNat (sOk.length - inter + sPart.length)),
         some (Int.ofNat (cOk.length - inter + cPart.length)),
         some (Int.ofNat inter)) := by
  intro isFullKey isPartKey sOk cOk sPart cPart inter
  cases hBothEmpty : ssci.isEmpty && csci.isEmpty
  · simp only [Bool.false_eq_true, ↓reduceIte]
    unfold evalQ97FullOuter
    dsimp only
    let onPred : (SqlVal Int × SqlVal Int) → (SqlVal Int × SqlVal Int) → SqlBool :=
      fun s c => (sqlEq s.1 c.1).and (sqlEq s.2 c.2)
    let foj := tpcdsFullOuterJoin onPred ssci csci
    let sHasCust : Option (SqlVal Int × SqlVal Int) → Bool :=
      fun r => match r with | some (some _, _) => true | _ => false
    have hFojNe : foj ≠ [] := by
      dsimp [foj, tpcdsFullOuterJoin]
      intro hNil
      rw [List.append_eq_nil_iff] at hNil
      cases hs : ssci with
      | cons x xs =>
        rw [hs] at hNil
        simp only [List.flatMap_cons, List.append_eq_nil_iff] at hNil
        split at hNil
        · nomatch hNil.1
        · rename_i hNotEmpty
          cases hm : csci.filter (fun y => (onPred x y).inWhere) with
          | nil => simp [hm] at hNotEmpty
          | cons y ys =>
            rw [hm] at hNil
            nomatch hNil.1
      | nil =>
        cases hc : csci with
        | nil =>
          simp [hs, hc] at hBothEmpty
        | cons y ys =>
          rw [hs, hc] at hNil
          simp [List.any] at hNil
    change (sqlSum (fun sc => if sHasCust sc.1 && !sHasCust sc.2 then some 1 else some 0) foj,
            sqlSum (fun sc => if !sHasCust sc.1 && sHasCust sc.2 then some 1 else some 0) foj,
            sqlSum (fun sc => if sHasCust sc.1 && sHasCust sc.2 then some 1 else some 0) foj) =
           (some (Int.ofNat (sOk.length - inter + sPart.length)),
            some (Int.ofNat (cOk.length - inter + cPart.length)),
            some (Int.ofNat inter))
    rw [sqlSum_indicator_nonempty _ hFojNe,
        sqlSum_indicator_nonempty _ hFojNe,
        sqlSum_indicator_nonempty _ hFojNe]
    have hInWhereAnd : ∀ a b : SqlBool, (a.and b).inWhere = (a.inWhere && b.inWhere) := by
      intro a b; cases a <;> cases b <;> rfl
    have hOnIff : ∀ x y, (onPred x y).inWhere = true ↔ (x = y ∧ isFullKey x = true) := by
      intro ⟨x1, x2⟩ ⟨y1, y2⟩
      dsimp only [onPred, isFullKey]
      rw [hInWhereAnd]
      cases x1 <;> cases x2 <;> cases y1 <;> cases y2 <;>
        simp only [Bool.and_eq_true, sqlEq_some_inWhere, Option.isSome, Option.some.injEq,
          Prod.mk.injEq, and_true] <;>
        simp [sqlEq, SqlBool.inWhere]
    have hFilterC : ∀ x,
        csci.filter (fun y => (onPred x y).inWhere) =
        if isFullKey x && cOk.contains x then [x] else [] := by
      intro x
      by_cases hMatch : isFullKey x && cOk.contains x
      · simp only [hMatch]
        simp only [Bool.and_eq_true, List.contains_eq_mem, decide_eq_true_eq] at hMatch
        have hxInC : x ∈ csci := (List.mem_filter.mp hMatch.2).1
        apply filter_unique_of_nodup_mem _ hNodupC hxInC
        · exact (hOnIff x x).mpr ⟨rfl, hMatch.1⟩
        · intro y _ hy
          exact ((hOnIff x y).mp hy).1.symm
      · simp only [hMatch]
        apply filter_eq_nil_of_forall_false
        intro y hy
        cases hVal : (onPred x y).inWhere
        · rfl
        · obtain ⟨rfl, hFull⟩ := (hOnIff x y).mp hVal
          have hxInCOk : x ∈ cOk := List.mem_filter.mpr ⟨hy, hFull⟩
          have : (isFullKey x && cOk.contains x) = true := by
            simp [hFull, List.contains_eq_mem, hxInCOk]
          exact absurd this hMatch
    have hLeftPart : ∀ xs : Rel (SqlVal Int × SqlVal Int),
        (xs.flatMap fun x =>
          let m := csci.filter (fun y => (onPred x y).inWhere)
          if m.isEmpty then [(some x, none)]
          else m.map (fun y => (some x, some y))) =
        xs.map (fun x =>
          if isFullKey x && cOk.contains x then (some x, some x) else (some x, none)) := by
      intro xs
      induction xs with
      | nil => rfl
      | cons x xs ih =>
        simp only [List.flatMap_cons, List.map_cons, ih]
        rw [hFilterC x]
        cases (isFullKey x && cOk.contains x) <;> simp
    have hAnyS : ∀ y,
        ssci.any (fun x => (onPred x y).inWhere) = (isFullKey y && sOk.contains y) := by
      intro y
      cases hR : isFullKey y && sOk.contains y
      · rw [Bool.eq_false_iff]
        intro hAny
        rw [List.any_eq_true] at hAny
        obtain ⟨x, hx, hxy⟩ := hAny
        obtain ⟨rfl, hFull⟩ := (hOnIff x y).mp hxy
        have : (isFullKey x && sOk.contains x) = true := by
          simp [hFull, List.contains_eq_mem, sOk, List.mem_filter, hx]
        rw [hR] at this
        contradiction
      · simp only [Bool.and_eq_true, List.contains_eq_mem, decide_eq_true_eq] at hR
        have hyInS : y ∈ ssci := (List.mem_filter.mp hR.2).1
        exact List.any_eq_true.mpr ⟨y, hyInS, (hOnIff y y).mpr ⟨rfl, hR.1⟩⟩
    have hInterSymm :
        (cOk.filter (fun y => sOk.contains y)).length = inter := by
      dsimp [inter]
      have hPerm : BagEq (cOk.filter (fun y => sOk.contains y))
          (sOk.filter (fun x => cOk.contains x)) := by
        apply perm_of_nodup_iff_mem ((hNodupC.filter _).filter _) ((hNodupS.filter _).filter _)
        intro z
        simp [sOk, cOk, List.mem_filter, List.contains_eq_mem, and_left_comm]
      exact hPerm.length_eq
    have hStoreOnly :
        (foj.filter (fun sc => sHasCust sc.1 && !sHasCust sc.2)).length =
        sOk.length - inter + sPart.length := by
      dsimp [foj, tpcdsFullOuterJoin]
      rw [hLeftPart ssci, List.filter_append]
      have hRightZero :
          (((csci.filter (fun y => !(ssci.any (fun x => (onPred x y).inWhere)))).map
            (fun y => (none, some y))).filter (fun sc => sHasCust sc.1 && !sHasCust sc.2)) = [] := by
        apply filter_eq_nil_of_forall_false
        intro sc hsc
        rw [List.mem_map] at hsc
        obtain ⟨_, _, rfl⟩ := hsc
        rfl
      rw [hRightZero, List.append_nil, List.filter_map, List.length_map]
      have hPredL :
          ssci.filter ((fun sc => sHasCust sc.1 && !sHasCust sc.2) ∘
            (fun x => if isFullKey x && cOk.contains x then (some x, some x) else (some x, none))) =
          ssci.filter (fun x => (isFullKey x && !cOk.contains x) || isPartKey x) := by
        apply List.filter_congr
        intro ⟨x1, x2⟩ _
        dsimp only [Function.comp_def, sHasCust, isFullKey, isPartKey]
        cases x1 <;> cases x2 <;> cases cOk.contains _ <;> rfl
      rw [hPredL]
      have hPartPerm :
          BagEq (ssci.filter (fun x => (isFullKey x && !cOk.contains x) || isPartKey x))
            (sOk.filter (fun x => !cOk.contains x) ++ sPart) := by
        apply perm_of_nodup_iff_mem (hNodupS.filter _)
        · rw [List.nodup_append]
          refine ⟨(hNodupS.filter _).filter _, hNodupS.filter _, ?_⟩
          intro p h1 q h2 hEq
          subst hEq
          rcases p with ⟨x1, x2⟩
          cases x2 <;> simp [sOk, sPart, isFullKey, isPartKey, List.mem_filter] at h1 h2
        · intro ⟨x1, x2⟩
          cases x1 <;> cases x2 <;>
            simp [sOk, sPart, isFullKey, isPartKey, List.mem_filter]
      rw [hPartPerm.length_eq, List.length_append, count_sub_inter_eq_diff]
    have hCatOnly :
        (foj.filter (fun sc => !sHasCust sc.1 && sHasCust sc.2)).length =
        cOk.length - inter + cPart.length := by
      dsimp [foj, tpcdsFullOuterJoin]
      rw [hLeftPart ssci, List.filter_append]
      have hLeftZero :
          ((ssci.map (fun x =>
            if isFullKey x && cOk.contains x then (some x, some x) else (some x, none))).filter
            (fun sc => !sHasCust sc.1 && sHasCust sc.2)) = [] := by
        apply filter_eq_nil_of_forall_false
        intro sc hsc
        rw [List.mem_map] at hsc
        obtain ⟨⟨x1, x2⟩, _, rfl⟩ := hsc
        cases x1 <;> cases x2 <;> cases cOk.contains _ <;> rfl
      rw [hLeftZero, List.nil_append, List.filter_map, List.length_map, List.filter_filter]
      have hPredR :
          csci.filter (fun y =>
            (((fun sc => !sHasCust sc.1 && sHasCust sc.2) ∘ fun y => (none, some y)) y) &&
            !(ssci.any (fun x => (onPred x y).inWhere))) =
          csci.filter (fun y => (isFullKey y && !sOk.contains y) || isPartKey y) := by
        apply List.filter_congr
        intro y _
        rw [hAnyS y]
        rcases y with ⟨y1, y2⟩
        dsimp only [Function.comp_def, sHasCust, isFullKey, isPartKey]
        cases y1 <;> cases y2 <;> cases sOk.contains _ <;> rfl
      rw [hPredR]
      have hPartPerm :
          BagEq (csci.filter (fun y => (isFullKey y && !sOk.contains y) || isPartKey y))
            (cOk.filter (fun y => !sOk.contains y) ++ cPart) := by
        apply perm_of_nodup_iff_mem (hNodupC.filter _)
        · rw [List.nodup_append]
          refine ⟨(hNodupC.filter _).filter _, hNodupC.filter _, ?_⟩
          intro p h1 q h2 hEq
          subst hEq
          rcases p with ⟨y1, y2⟩
          cases y2 <;> simp [cOk, cPart, isFullKey, isPartKey, List.mem_filter] at h1 h2
        · intro ⟨y1, y2⟩
          cases y1 <;> cases y2 <;>
            simp [cOk, cPart, isFullKey, isPartKey, List.mem_filter]
      rw [hPartPerm.length_eq, List.length_append, count_sub_inter_eq_diff, hInterSymm]
    have hBothCnt :
        (foj.filter (fun sc => sHasCust sc.1 && sHasCust sc.2)).length = inter := by
      dsimp [foj, tpcdsFullOuterJoin]
      rw [hLeftPart ssci, List.filter_append]
      have hRightZero :
          (((csci.filter (fun y => !(ssci.any (fun x => (onPred x y).inWhere)))).map
            (fun y => (none, some y))).filter (fun sc => sHasCust sc.1 && sHasCust sc.2)) = [] := by
        apply filter_eq_nil_of_forall_false
        intro sc hsc
        rw [List.mem_map] at hsc
        obtain ⟨_, _, rfl⟩ := hsc
        rfl
      rw [hRightZero, List.append_nil, List.filter_map, List.length_map]
      have hPredBoth :
          ssci.filter ((fun sc => sHasCust sc.1 && sHasCust sc.2) ∘
            (fun x => if isFullKey x && cOk.contains x then (some x, some x) else (some x, none))) =
          sOk.filter (fun x => cOk.contains x) := by
        simp only [sOk, List.filter_filter]
        apply List.filter_congr
        intro ⟨x1, x2⟩ _
        dsimp only [Function.comp_def, sHasCust, isFullKey]
        cases x1 <;> cases x2 <;> cases cOk.contains _ <;> rfl
      rw [hPredBoth]
    rw [hStoreOnly, hCatOnly, hBothCnt]
  · simp only [Bool.and_eq_true, List.isEmpty_iff] at hBothEmpty
    rcases hBothEmpty with ⟨rfl, rfl⟩
    rfl

end TinylambOpt
