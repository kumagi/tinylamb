import TinylambOpt.AvgMinMaxHavingDistinct
import TinylambOpt.CppRuleAudit

/-
# Step 7: 高リスク領域の網羅的 Lean 4 監査と C++ バグ発見・修復証明

未検証だった高リスク領域（集合演算 × `NULL`/`NaN`、外部結合結合律・完全外部結合分解、
準結合リオーダ、一意性制約による結合/重複消去、等価クラス・推移律、Sort/Limit 合成、
三値論理の `XOR` / `NOT IN (..., NULL)` / `IS TRUE = FALSE` / `COALESCE` / `NULLIF`）を
C++ 実装ごと Lean 4 に落とし、**新たに発見した実在バグ 6 件の反例証明** と
**修復後ルールの普遍的健全性定理（`sorry` ゼロ）** を与えます。
-/

namespace TinylambOpt

/-!
## 1. 集合演算と `IS NOT DISTINCT FROM` × `NaN` トラップ
対象 C++ ルール:
- `intersect_to_semijoin`, `except_to_antijoin` (`BuildEqualityOnAllColumns`)
- `setop_empty_simplification`, `setop_empty_identity`
- `union_to_union_all_plus_distinct`, `union_all_merge`
- `push_filter_past_setop`, `push_filter_through_distinct`, `distinct_over_distinct`
-/

/-- `DOUBLE` + `NULL` ドメイン（通常値 / `NaN` / `NULL`）。 -/
inductive DoubleSqlVal where
  | num  : Int → DoubleSqlVal
  | nan  : DoubleSqlVal
  | null : DoubleSqlVal
  deriving DecidableEq, Repr

/-- GoogleSQL / `tinylamb` の `SetOperationExecutor` (`executor/set_operation.hpp`) の同値判定:
    `NULL` 同士も `NaN` 同士も同一要素として扱う。 -/
def setOpSame : DoubleSqlVal → DoubleSqlVal → Bool
  | .null,  .null  => true
  | .nan,   .nan   => true
  | .num a, .num b => a == b
  | _,      _      => false

/-- 修正前の IS NOT DISTINCT FROM のモデル。現行 C++ は両辺が NaN の場合に真を返し、集合演算と一致する。 -/
def cppIsNotDistinctFromDouble : DoubleSqlVal → DoubleSqlVal → SqlBool
  | .null,  .null  => .tTrue
  | .null,  _      => .tFalse
  | _,      .null  => .tFalse
  | .nan,   _      => .tFalse
  | _,      .nan   => .tFalse
  | .num a, .num b => if a == b then .tTrue else .tFalse

/-- 🚨 **修正前の実装に対する反例 H1a (`intersect_to_semijoin` / `except_to_antijoin` の `NaN` 不整合)**:
    `DOUBLE` 列に `NaN` が含まれるとき、`INTERSECT` は `[NaN]` を返すが
    `IS NOT DISTINCT FROM` による `SemiJoin` は `[]` を返し、
    `EXCEPT` は `[]` を返すが `AntiJoin` は `[NaN]` を残してしまう！ -/
theorem cpp_intersect_except_nan_isNotDistinctFrom_bug :
    let L : Rel DoubleSqlVal := [.nan]
    let R : Rel DoubleSqlVal := [.nan]
    let trueIntersect := L.filter (fun l => R.any (fun r => setOpSame l r))
    let buggySemiJoin := semiJoin cppIsNotDistinctFromDouble L R
    let trueExcept    := L.filter (fun l => !(R.any (fun r => setOpSame l r)))
    let buggyAntiJoin := antiJoin cppIsNotDistinctFromDouble L R
    trueIntersect = [.nan] ∧ buggySemiJoin = [] ∧
    trueExcept = [] ∧ buggyAntiJoin = [.nan] := by
  decide

/-- 🚨 **実在バグ証明 H1b (`BuildEqualityOnAllColumns` の計算式エイリアス誤解決)**:
    左ブランチが `SELECT a + 10 AS a FROM t1` のように計算式に基底列と同じ別名 `a` を付けたとき、
    旧 `resolve_column` は非 `kColumnValue` を無視して基底列 `t1.a` で結合条件を組んでいた。 -/
theorem cpp_intersect_computed_alias_bug :
    let t1 : Rel Int := [1]
    let t2 : Rel Int := [1]
    let leftProj := t1.map (fun a => a + 10)
    let trueIntersect := distinctKeys (leftProj.filter (fun v => t2.any (fun r => v == r)))
    let buggySemiOnBaseCol :=
      distinctKeys ((t1.filter (fun a1 => t2.any (fun a2 => a1 == a2))).map (fun a => a + 10))
    trueIntersect = [] ∧ buggySemiOnBaseCol = [11] ∧ trueIntersect ≠ buggySemiOnBaseCol := by
  decide

def sqlUnion {Row : Type} [DecidableEq Row] (l r : Rel Row) : Rel Row :=
  distinctKeys (l ++ r)

def sqlIntersect {Row : Type} [DecidableEq Row] (l r : Rel Row) : Rel Row :=
  distinctKeys (l.filter (fun x => r.any (fun y => decide (x = y))))

def sqlExcept {Row : Type} [DecidableEq Row] (l r : Rel Row) : Rel Row :=
  distinctKeys (l.filter (fun x => !(r.any (fun y => decide (x = y)))))

private theorem sqlIsNotDistinctFrom_inWhere_eq_decide {α : Type} [DecidableEq α]
    (x r : SqlVal α) :
    (sqlIsNotDistinctFrom x r).inWhere = decide (x = r) := by
  cases x with
  | none => cases r <;> rfl
  | some vx =>
    cases r with
    | none => rfl
    | some vr => by_cases h : vx = vr <;> simp [sqlIsNotDistinctFrom, h, SqlBool.inWhere]

private theorem any_sqlIsNotDistinctFrom_eq_any_decide {α : Type} [DecidableEq α]
    (x : SqlVal α) (rs : Rel (SqlVal α)) :
    rs.any (fun r => (sqlIsNotDistinctFrom x r).inWhere) =
      rs.any (fun r => decide (x = r)) := by
  induction rs with
  | nil => rfl
  | cons r tl _ =>
    simp only [List.any_cons, sqlIsNotDistinctFrom_inWhere_eq_decide]

/-- ✅ **完全正当性定理 (`intersect_to_semijoin`)**:
    非 `DOUBLE` ドメインかつ各出力列がブランチ基底列へ正確に解決されるとき、
    `Distinct(SemiJoin(L, R, IS NOT DISTINCT FROM))` は `INTERSECT` と **全入力（`NULL` 含む）で完全一致** する。 -/
theorem repaired_intersect_to_semijoin_sound {α : Type} [DecidableEq α]
    (L R : Rel (SqlVal α)) :
    distinctKeys (semiJoin sqlIsNotDistinctFrom L R) = sqlIntersect L R := by
  unfold semiJoin sqlIntersect
  congr 1
  apply List.filter_congr
  intro x _
  exact any_sqlIsNotDistinctFrom_eq_any_decide x R

/-- ✅ **完全正当性定理 (`except_to_antijoin`)**:
    非 `DOUBLE` ドメインにおいて `Distinct(AntiJoin(L, R, IS NOT DISTINCT FROM))` は
    `EXCEPT` と **全入力（`NULL` 含む）で完全一致** する。 -/
theorem repaired_except_to_antijoin_sound {α : Type} [DecidableEq α]
    (L R : Rel (SqlVal α)) :
    distinctKeys (antiJoin sqlIsNotDistinctFrom L R) = sqlExcept L R := by
  unfold antiJoin sqlExcept
  congr 1
  apply List.filter_congr
  intro x _
  rw [any_sqlIsNotDistinctFrom_eq_any_decide]

/-- ✅ **完全正当性定理 (`setop_empty_simplification` & 右空 `EXCEPT` 罠の反例)**:
    `INTERSECT` は左右どちらが空でも空、`EXCEPT` は左が空のときのみ空（右が空なら非空のまま）。 -/
theorem setop_empty_simplification_sound {Row : Type} [DecidableEq Row] (L R : Rel Row) :
    sqlIntersect [] R = [] ∧
    sqlIntersect L [] = [] ∧
    sqlExcept [] R = [] ∧
    sqlExcept [1] ([] : Rel Int) = [1] := by
  refine ⟨rfl, ?_, rfl, by decide⟩
  unfold sqlIntersect
  have hf : L.filter (fun x => ([] : Rel Row).any (fun y => decide (x = y))) = [] := by
    induction L with
    | nil => rfl
    | cons _ _ ih => exact ih
  rw [hf]
  rfl

/-- ✅ **完全正当性定理 (`setop_empty_identity`, `union_to_union_all_plus_distinct`, `union_all_merge`)** -/
theorem setop_identity_and_union_rules_sound {Row : Type} [DecidableEq Row]
    (A B C : Rel Row) :
    unionAll A [] = A ∧
    unionAll [] B = B ∧
    sqlExcept A [] = distinctKeys A ∧
    sqlUnion A [] = distinctKeys A ∧
    sqlUnion A B = distinctKeys (unionAll A B) ∧
    unionAll (unionAll A B) C = unionAll A (unionAll B C) := by
  refine ⟨List.append_nil A, rfl, ?_, ?_, rfl, List.append_assoc A B C⟩
  · unfold sqlExcept
    have hf : A.filter (fun x => !(([] : Rel Row).any (fun y => decide (x = y)))) = A := by
      induction A with
      | nil => rfl
      | cons x tl ih =>
        change x :: tl.filter (fun x => !(([] : Rel Row).any (fun y => decide (x = y)))) = x :: tl
        rw [ih]
    rw [hf]
  · unfold sqlUnion
    rw [List.append_nil]

private theorem filter_ne_self_of_not_mem {Row : Type} [DecidableEq Row] (x : Row) :
    ∀ xs : Rel Row, x ∉ xs → xs.filter (fun y => decide (y ≠ x)) = xs := by
  intro xs hnin
  induction xs with
  | nil => rfl
  | cons y tl ih =>
    have hne : y ≠ x := fun h => hnin (h ▸ List.Mem.head tl)
    have hnin_tl : x ∉ tl := fun hm => hnin (List.Mem.tail y hm)
    simp only [List.filter, decide_eq_true hne, ih hnin_tl]

/-- ✅ **完全正当性定理 (`pk_unique_distinct_elimination`)**:
    主キーにより重複行が存在しない入力 `ls.Nodup` に対して `DISTINCT` の消去は恒等である。 -/
theorem pk_unique_distinct_elim_sound {Row : Type} [DecidableEq Row]
    (ls : Rel Row) (h_nodup : ls.Nodup) :
    distinctKeys ls = ls := by
  induction ls with
  | nil => rfl
  | cons x tl ih =>
    have hnd := List.nodup_cons.mp h_nodup
    simp only [distinctKeys, ih hnd.2, filter_ne_self_of_not_mem x tl hnd.1]

/-- ✅ **完全正当性定理 (`distinct_over_distinct` & `push_filter_through_distinct`)** -/
theorem distinct_over_distinct_sound {Row : Type} [DecidableEq Row] (r : Rel Row) :
    distinctKeys (distinctKeys r) = distinctKeys r :=
  pk_unique_distinct_elim_sound (distinctKeys r) (nodup_distinctKeys r)

private theorem filter_comm_pred {α : Type} (p q : α → Bool) (xs : List α) :
    (xs.filter p).filter q = (xs.filter q).filter p := by
  induction xs with
  | nil => rfl
  | cons x tl ih =>
    cases hp : p x <;> cases hq : q x <;> simp [List.filter, hp, hq, ih]

theorem push_filter_through_distinct_sound {Row : Type} [DecidableEq Row]
    (p : Row → Bool) (r : Rel Row) :
    (distinctKeys r).filter p = distinctKeys (r.filter p) := by
  induction r with
  | nil => rfl
  | cons x tl ih =>
    simp only [distinctKeys, List.filter]
    cases hp : p x
    · rw [filter_comm_pred (fun y => decide (y ≠ x)) p (distinctKeys tl), ih]
      have hnin : x ∉ distinctKeys (tl.filter p) := by
        intro hy
        have hmem := List.mem_filter.mp ((mem_distinctKeys (tl.filter p) x).mp hy)
        rw [hp] at hmem
        exact Bool.noConfusion hmem.2
      exact filter_ne_self_of_not_mem x (distinctKeys (tl.filter p)) hnin
    · rw [filter_comm_pred (fun y => decide (y ≠ x)) p (distinctKeys tl), ih]
      rfl

private theorem any_filter_self_eq {Row : Type} [DecidableEq Row]
    (p : Row → Bool) (x : Row) (hp : p x = true) (rs : Rel Row) :
    (rs.filter p).any (fun y => decide (x = y)) = rs.any (fun y => decide (x = y)) := by
  induction rs with
  | nil => rfl
  | cons r tl ih =>
    cases hpr : p r
    · have hne : x ≠ r := by
        intro h; subst h; rw [hp] at hpr; exact Bool.noConfusion hpr
      simp [List.filter, hpr, List.any_cons, hne, ih]
    · simp [List.filter, hpr, List.any_cons, ih]

/-- ✅ **完全正当性定理 (`push_filter_past_setop`)**:
    行単位フィルタ `p` は `UNION ALL` / `UNION` / `INTERSECT` / `EXCEPT` のすべてと可換である。 -/
theorem push_filter_past_setop_sound {Row : Type} [DecidableEq Row]
    (p : Row → Bool) (L R : Rel Row) :
    (unionAll L R).filter p = unionAll (L.filter p) (R.filter p) ∧
    (sqlUnion L R).filter p = sqlUnion (L.filter p) (R.filter p) ∧
    (sqlIntersect L R).filter p = sqlIntersect (L.filter p) (R.filter p) ∧
    (sqlExcept L R).filter p = sqlExcept (L.filter p) (R.filter p) := by
  refine ⟨List.filter_append (l₁ := L) (l₂ := R), ?_, ?_, ?_⟩
  · unfold sqlUnion
    rw [push_filter_through_distinct_sound, List.filter_append]
  · unfold sqlIntersect
    rw [push_filter_through_distinct_sound]
    congr 1
    induction L with
    | nil => rfl
    | cons x tl ih =>
      cases hp : p x
      · cases hm : R.any (fun y => decide (x = y)) <;> simp [List.filter, hp, hm, ih]
      · rw [List.filter_cons_of_pos hp]
        simp only [List.filter, any_filter_self_eq p x hp R]
        cases hm : R.any (fun y => decide (x = y)) <;> simp [hp, ih]
  · unfold sqlExcept
    rw [push_filter_through_distinct_sound]
    congr 1
    induction L with
    | nil => rfl
    | cons x tl ih =>
      cases hp : p x
      · cases hm : R.any (fun y => decide (x = y)) <;> simp [List.filter, hp, hm, ih]
      · rw [List.filter_cons_of_pos hp]
        simp only [List.filter, any_filter_self_eq p x hp R]
        cases hm : R.any (fun y => decide (x = y)) <;> simp [hp, ih]

/-!
## 2. 外部結合の結合律・完全外部結合分解・準結合リオーダ
対象 C++ ルール:
- `right_to_left_outer_join`, `full_outer_join_decomposition`, `outer_join_associativity`
- `semi_join_inner_join_reorder`, `semi_join_commutativity`, `push_semi_join_through_inner_join`
- `cross_to_inner_with_predicate`, `join_empty_simplification`
-/

def rightOuterJoin {L R : Type} (p : L → R → SqlBool) (ls : Rel L) (rs : Rel R)
    : Rel (Option L × R) :=
  rs.flatMap (fun r =>
    let matches_ := ls.filter (fun l => (p l r).inWhere)
    if matches_.isEmpty then [(none, r)]
    else matches_.map (fun l => (some l, r)))

def fullOuterJoin {L R : Type} (p : L → R → SqlBool) (ls : Rel L) (rs : Rel R)
    : Rel (Option L × Option R) :=
  (leftOuterJoin p ls rs).map (fun lr => (some lr.1, lr.2)) ++
  (antiJoin (fun r l => p l r) rs ls).map (fun r => (none, some r))

/-- ✅ **完全正当性定理 (`right_to_left_outer_join` & `full_outer_join_decomposition`)** -/
theorem right_to_left_outer_join_sound {L R : Type}
    (p : L → R → SqlBool) (ls : Rel L) (rs : Rel R) :
    rightOuterJoin p ls rs =
      (leftOuterJoin (fun r l => p l r) rs ls).map (fun rl => (rl.2, rl.1)) := by
  unfold rightOuterJoin leftOuterJoin
  induction rs with
  | nil => rfl
  | cons r tl ih =>
    simp only [List.flatMap_cons, List.map_append, ih]
    cases h : ls.filter (fun l => (p l r).inWhere) <;> simp [List.map_map, Function.comp]

theorem full_outer_join_decomposition_sound {L R : Type}
    (p : L → R → SqlBool) (ls : Rel L) (rs : Rel R) :
    fullOuterJoin p ls rs =
      unionAll ((leftOuterJoin p ls rs).map (fun lr => (some lr.1, lr.2)))
               ((antiJoin (fun r l => p l r) rs ls).map (fun r => (none, some r))) := rfl

/-- ❌ **反例証明 (`outer_join_associativity` で `p2` が `B` の `NULL` を弾かない場合)**:
    `p2` が `b IS NULL` のように `NULL` を通すとき、
    `(A ⟕ B) ⟕ C` は不一致行で `C` と結合するが、`A ⟕ (B ⟕ C)` は `(a, NULL, NULL)` になり不一致！ -/
theorem outer_join_assoc_fails_without_null_reject_bug :
    let A : Rel Int := [1]
    let B : Rel Int := []
    let C : Rel Int := [99]
    let p1 : Int → Int → SqlBool := fun a b => if a == b then .tTrue else .tFalse
    let p2 : Option Int → Int → SqlBool := fun ob _ => match ob with | none => .tTrue | some _ => .tFalse
    let lhs := (leftOuterJoin (fun (ab : Int × Option Int) c => p2 ab.2 c) (leftOuterJoin p1 A B) C).map
                 (fun abc => (abc.1.1, abc.1.2, abc.2))
    let rhs := (leftOuterJoin (fun a (bc : Int × Option Int) => p1 a bc.1) A
                 (leftOuterJoin (fun b c => p2 (some b) c) B C)).map
                 (fun abc => (abc.1, abc.2.map Prod.fst, abc.2.bind Prod.snd))
    lhs = [(1, none, some 99)] ∧ rhs = [(1, none, none)] ∧ lhs ≠ rhs := by
  decide

private theorem filter_false_nil {α : Type} (xs : List α) :
    xs.filter (fun _ => false) = [] := by
  induction xs with
  | nil => rfl
  | cons _ _ ih => exact ih

private theorem filter_leftOuterJoin_fst {B C : Type}
    (pred : B → Bool) (p2 : Option B → C → SqlBool) (bs : Rel B) (cs : Rel C) :
    (leftOuterJoin p2 (bs.map some) cs).filter (fun bc => match bc.1 with | some b => pred b | none => false) =
    leftOuterJoin p2 ((bs.filter pred).map some) cs := by
  induction bs with
  | nil => rfl
  | cons b tl ih =>
    have hcons : ∀ (x : Option B) (xs : List (Option B)),
        leftOuterJoin p2 (x :: xs) cs =
        (let m := cs.filter (fun y => (p2 x y).inWhere)
         if m.isEmpty then [(x, none)] else m.map (fun y => (x, some y))) ++
        leftOuterJoin p2 xs cs := fun _ _ => rfl
    simp only [List.map_cons, hcons, List.filter_append]
    cases hp : pred b
    · have hdrop : ∀ xs : List C,
          (xs.map (fun y => (some b, some y))).filter
            (fun bc : Option B × Option C => match bc.1 with | some b' => pred b' | none => false) = [] := by
        intro xs
        induction xs with
        | nil => rfl
        | cons _ _ xih => simp only [List.map_cons, List.filter, hp]; exact xih
      cases hm : cs.filter (fun y => (p2 (some b) y).inWhere) with
      | nil => simp only [List.isEmpty_nil, ↓reduceIte, List.filter, hp, List.nil_append, ih]
      | cons c0 ctl =>
        simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, hdrop (c0 :: ctl),
                   List.nil_append, List.filter, hp, ih]
    · have hkeep : ∀ xs : List C,
          (xs.map (fun y => (some b, some y))).filter
            (fun bc : Option B × Option C => match bc.1 with | some b' => pred b' | none => false) =
          xs.map (fun y => (some b, some y)) := by
        intro xs
        induction xs with
        | nil => rfl
        | cons x xtl xih => simp only [List.map_cons, List.filter, hp, xih]
      simp only [List.filter, hp, List.map_cons, hcons]
      cases hm : cs.filter (fun y => (p2 (some b) y).inWhere) with
      | nil => simp only [List.isEmpty_nil, ↓reduceIte, List.filter, hp, ih]
      | cons c0 ctl =>
        simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, hkeep (c0 :: ctl), ih]

/-- ✅ **完全正当性定理 (`outer_join_associativity`)**:
    `p2` が `B = NULL` を弾く（`∀ c, (p2 none c).inWhere = false`）とき、
    `(A ⟕_{p1} B) ⟕_{p2} C` と `A ⟕_{p1} (B ⟕_{p2} C)` は **全入力で順序・重複度まで完全一致** する！ -/
theorem outer_join_assoc_when_p2_rejects_null_sound {A B C : Type}
    (p1 : A → B → SqlBool) (p2 : Option B → C → SqlBool)
    (h_reject_b : ∀ c : C, (p2 none c).inWhere = false)
    (as_ : Rel A) (bs : Rel B) (cs : Rel C) :
    (leftOuterJoin (fun ab c => p2 ab.2 c) (leftOuterJoin p1 as_ bs) cs).map
      (fun abc => (abc.1.1, abc.1.2, abc.2)) =
    (leftOuterJoin (fun a bc => match bc.1 with | some b => p1 a b | none => .tNull)
      as_ (leftOuterJoin p2 (bs.map some) cs)).map
      (fun abc => (abc.1, abc.2.bind Prod.fst, abc.2.bind Prod.snd)) := by
  induction as_ with
  | nil => rfl
  | cons a atl ih =>
    have h_head :
      (leftOuterJoin (fun ab c => p2 ab.2 c)
        (let m := bs.filter (fun b => (p1 a b).inWhere)
         if m.isEmpty then [(a, none)] else m.map (fun b => (a, some b))) cs).map
        (fun abc => (abc.1.1, abc.1.2, abc.2)) =
      (let m2 := (leftOuterJoin p2 (bs.map some) cs).filter
                   (fun bc => match bc.1 with | some b => (p1 a b).inWhere | none => false)
       if m2.isEmpty then [(a, none)] else m2.map (fun bc => (a, some bc))).map
        (fun abc => (abc.1, abc.2.bind Prod.fst, abc.2.bind Prod.snd)) := by
      rw [filter_leftOuterJoin_fst (fun b => (p1 a b).inWhere) p2 bs cs]
      cases hm : bs.filter (fun b => (p1 a b).inWhere) with
      | nil =>
        have hc : cs.filter (fun c => (p2 none c).inWhere) = [] := by
          have hcongr : cs.filter (fun c => (p2 none c).inWhere) = cs.filter (fun _ => false) :=
            List.filter_congr (fun c _ => h_reject_b c)
          rw [hcongr, filter_false_nil]
        have hstep : leftOuterJoin (fun ab c => p2 ab.2 c) [(a, (none : Option B))] cs =
            [((a, none), none)] := by
          unfold leftOuterJoin
          simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
          rw [hc]
          rfl
        have hstep2 : leftOuterJoin p2 ([] : List (Option B)) cs = [] := rfl
        simp only [List.map_nil, hstep2, List.isEmpty_nil, ↓reduceIte, hstep, List.map_cons]
        rfl
      | cons b0 btl =>
        have hnonempty : (leftOuterJoin p2 ((b0 :: btl).map some) cs).isEmpty = false := by
          unfold leftOuterJoin
          simp only [List.map_cons, List.flatMap_cons]
          cases cs.filter (fun y => (p2 (some b0) y).inWhere) <;> simp
        simp only [List.isEmpty_cons, Bool.false_eq_true, ↓reduceIte, hnonempty]
        clear hm hnonempty
        have h_nonnull_list : ∀ xs : Rel B,
            (leftOuterJoin (fun ab c => p2 ab.2 c) (xs.map (fun b => (a, some b))) cs).map
              (fun abc => (abc.1.1, abc.1.2, abc.2)) =
            ((leftOuterJoin p2 (xs.map some) cs).map (fun bc => (a, some bc))).map
              (fun abc => (abc.1, abc.2.bind Prod.fst, abc.2.bind Prod.snd)) := by
          intro xs
          induction xs with
          | nil => rfl
          | cons b bbs bih =>
            have hc1 : leftOuterJoin (fun ab c => p2 ab.2 c) ((a, some b) :: bbs.map (fun b => (a, some b))) cs =
                (let m := cs.filter (fun y => (p2 (some b) y).inWhere)
                 if m.isEmpty then [((a, some b), none)] else m.map (fun y => ((a, some b), some y))) ++
                leftOuterJoin (fun ab c => p2 ab.2 c) (bbs.map (fun b => (a, some b))) cs := rfl
            have hc2 : leftOuterJoin p2 (some b :: bbs.map some) cs =
                (let m := cs.filter (fun y => (p2 (some b) y).inWhere)
                 if m.isEmpty then [(some b, none)] else m.map (fun y => (some b, some y))) ++
                leftOuterJoin p2 (bbs.map some) cs := rfl
            simp only [List.map_cons, hc1, hc2, List.map_append]
            rw [bih]
            cases cs.filter (fun y => (p2 (some b) y).inWhere) <;>
              simp [List.map_map, Function.comp]
        exact h_nonnull_list (b0 :: btl)
    have h_lhs_cons :
        leftOuterJoin p1 (a :: atl) bs =
        (let m := bs.filter (fun b => (p1 a b).inWhere)
         if m.isEmpty then [(a, none)] else m.map (fun b => (a, some b))) ++
        leftOuterJoin p1 atl bs := rfl
    have h_rhs_cons :
        leftOuterJoin (fun a bc => match bc.1 with | some b => p1 a b | none => .tNull)
          (a :: atl) (leftOuterJoin p2 (bs.map some) cs) =
        (let m2 := (leftOuterJoin p2 (bs.map some) cs).filter
                     (fun bc => (match bc.1 with | some b => p1 a b | none => SqlBool.tNull).inWhere)
         if m2.isEmpty then [(a, none)] else m2.map (fun bc => (a, some bc))) ++
        leftOuterJoin (fun a bc => match bc.1 with | some b => p1 a b | none => .tNull)
          atl (leftOuterJoin p2 (bs.map some) cs) := rfl
    have h_ljoin_append : ∀ (xs ys : Rel (A × Option B)),
        leftOuterJoin (fun ab c => p2 ab.2 c) (xs ++ ys) cs =
        leftOuterJoin (fun ab c => p2 ab.2 c) xs cs ++
        leftOuterJoin (fun ab c => p2 ab.2 c) ys cs := by
      intro xs ys; unfold leftOuterJoin; exact List.flatMap_append
    have h_p1_inWhere : ∀ bc : Option B × Option C,
        ((match bc.1 with | some b => p1 a b | none => SqlBool.tNull)).inWhere =
        (match bc.1 with | some b => (p1 a b).inWhere | none => false) := by
      intro bc; cases bc.1 <;> rfl
    rw [h_lhs_cons, h_ljoin_append, List.map_append, h_rhs_cons, List.map_append]
    simp only [h_p1_inWhere]
    rw [h_head, ih]

/-- ✅ **完全正当性定理 (`semi_join_inner_join_reorder`, `semi_join_commutativity`, `push_semi_join_through_inner_join`)** -/
theorem semi_join_reorder_and_pushdown_sound {R S T : Type}
    (pRS : R → S → SqlBool) (pRT : R → T → SqlBool) (pST : Option T → S → SqlBool)
    (rs : Rel R) (ss : Rel S) (ts : Rel T) :
    innerJoin pRT (semiJoin pRS rs ss) ts =
      semiJoin (fun rt s => pRS rt.1 s) (innerJoin pRT rs ts) ss ∧
    semiJoin pRT (semiJoin pRS rs ss) ts =
      semiJoin pRS (semiJoin pRT rs ts) ss ∧
    semiJoin (fun rt s => pRS rt.1 s) (innerJoin pRT rs ts) ss =
      innerJoin pRT (semiJoin pRS rs ss) ts ∧
    semiJoin (fun rt s => pST rt.2 s) (innerJoin pRT rs ts) ss =
      innerJoin pRT rs (semiJoin (fun t s => pST (some t) s) ts ss) := by
  have h1 : innerJoin pRT (semiJoin pRS rs ss) ts =
      semiJoin (fun rt s => pRS rt.1 s) (innerJoin pRT rs ts) ss := by
    unfold innerJoin semiJoin
    induction rs with
    | nil => rfl
    | cons r rtl ih =>
      by_cases hs : ss.any (fun s => (pRS r s).inWhere) = true
      · have hmap :
          ((ts.filter (fun t => (pRT r t).inWhere)).map (fun t => (r, some t))).filter
            (fun rt => ss.any (fun s => (pRS rt.1 s).inWhere)) =
          (ts.filter (fun t => (pRT r t).inWhere)).map (fun t => (r, some t)) := by
          induction ts.filter (fun t => (pRT r t).inWhere) with
          | nil => rfl
          | cons _ _ tih => simp [hs, tih]
        simp [List.filter, hs, List.flatMap_cons, List.filter_append, hmap, ih]
      · simp only [Bool.not_eq_true] at hs
        have hmap :
          ((ts.filter (fun t => (pRT r t).inWhere)).map (fun t => (r, some t))).filter
            (fun rt => ss.any (fun s => (pRS rt.1 s).inWhere)) = [] := by
          induction ts.filter (fun t => (pRT r t).inWhere) with
          | nil => rfl
          | cons _ _ tih => simp [hs, tih]
        simp [List.filter, hs, List.flatMap_cons, List.filter_append, hmap, ih]
  refine ⟨h1, filter_comm_pred _ _ rs, h1.symm, ?_⟩
  clear h1
  unfold innerJoin semiJoin
  have h_row : ∀ (r : R) (xs : Rel T),
      ((xs.filter (fun t => (pRT r t).inWhere)).map (fun t => (r, some t))).filter
        (fun rt => ss.any (fun s => (pST rt.2 s).inWhere)) =
      ((xs.filter (fun t => ss.any (fun s => (pST (some t) s).inWhere))).filter
        (fun t => (pRT r t).inWhere)).map (fun t => (r, some t)) := by
    intro r xs
    induction xs with
    | nil => rfl
    | cons t ttl tih =>
      cases hrt : (pRT r t).inWhere <;>
        cases hst : ss.any (fun s => (pST (some t) s).inWhere) <;>
        simp [List.filter, hrt, hst, tih]
  induction rs with
  | nil => rfl
  | cons r rtl ih =>
    simp only [List.flatMap_cons, List.filter_append, h_row r ts, ih]

/-- ✅ **完全正当性定理 (`cross_to_inner_with_predicate` & `join_empty_simplification`)** -/
theorem cross_and_empty_join_simplification_sound {L R : Type}
    (p : L → R → SqlBool) (ls : Rel L) (rs : Rel R) :
    (ls.flatMap (fun l => rs.map (fun r => (l, some r)))).filter
      (fun lr => match lr.2 with | some r => (p lr.1 r).inWhere | none => false) =
      innerJoin p ls rs ∧
    innerJoin p [] rs = [] ∧
    innerJoin p ls [] = [] ∧
    semiJoin p [] rs = [] ∧
    semiJoin p ls [] = [] ∧
    antiJoin p [] rs = [] ∧
    antiJoin p ls [] = ls := by
  have h_cross_row : ∀ (l : L) (xs : Rel R),
      (xs.map (fun r => (l, some r))).filter
        (fun lr : L × Option R => match lr.2 with | some r => (p lr.1 r).inWhere | none => false) =
      (xs.filter (fun y => (p l y).inWhere)).map (fun r => (l, some r)) := by
    intro l xs
    induction xs with
    | nil => rfl
    | cons r rtl rih =>
      cases hp : (p l r).inWhere <;> simp [List.filter, hp, rih]
  refine ⟨?_, rfl, ?_, rfl, ?_, rfl, ?_⟩
  · unfold innerJoin
    induction ls with
    | nil => rfl
    | cons l ltl ih =>
      simp only [List.flatMap_cons, List.filter_append, h_cross_row l rs, ih]
  · unfold innerJoin; induction ls with | nil => rfl | cons _ _ ih => exact ih
  · unfold semiJoin; induction ls with | nil => rfl | cons _ _ ih => exact ih
  · unfold antiJoin; induction ls with | nil => rfl | cons _ _ _ => simp [List.filter]

/-!
## 3. 一意性・主キーによる結合消去と `DISTINCT` 押し込み
対象 C++ ルール:
- `self_join_elimination`, `unique_semi_to_inner`, `pk_unique_distinct_elimination`
- `distinct_eager_over_join`, `not_null_is_not_null_elimination`
- `eliminate_false_selection`, `eliminate_true_selection`
-/

/-- ❌ **反例証明 (`self_join_elimination` で `UNIQUE` 列が `NULL` 許容の場合)**:
    `UNIQUE` でも `NULL` 行は自己結合で脱落するため、単体スキャンへの置換は不正！ -/
theorem self_join_elim_nullable_unique_bug :
    let R : Rel (Option Int × String) := [(none, "a")]
    let onEq : (Option Int × String) → (Option Int × String) → SqlBool :=
      fun l r => sqlEq l.1 r.1
    (innerJoin onEq R R).map Prod.fst = [] ∧ R ≠ [] := by
  decide

private theorem filter_not_mem_key_nil {Row K : Type} [DecidableEq K]
    (k : Row → K) (x : Row) :
    ∀ xs : Rel Row, k x ∉ xs.map k →
      xs.filter (fun y => decide (k x = k y)) = [] := by
  intro xs hnin
  induction xs with
  | nil => rfl
  | cons y tl ih =>
    have hxy : k x ≠ k y := fun h => hnin (h ▸ List.Mem.head _)
    have hnin_tl : k x ∉ tl.map k := fun hm => hnin (List.Mem.tail _ hm)
    simp only [List.filter, decide_eq_false hxy]
    exact ih hnin_tl

private theorem filter_self_mem_eq_singleton {Row K : Type} [DecidableEq K]
    (k : Row → K) (x : Row) :
    ∀ xs : Rel Row, (xs.map k).Nodup → x ∈ xs →
      xs.filter (fun y => decide (k x = k y)) = [x] := by
  intro xs hnd hmem
  induction xs with
  | nil => contradiction
  | cons y tl ih =>
    have hnd' := List.nodup_cons.mp hnd
    cases hmem with
    | head _ =>
      simp only [List.filter, decide_true]
      rw [filter_not_mem_key_nil k x tl hnd'.1]
    | tail _ htl =>
      have hxy : k x ≠ k y := by
        intro h
        apply hnd'.1
        rw [← h]
        exact List.mem_map_of_mem htl
      simp only [List.filter, decide_eq_false hxy]
      exact ih hnd'.2 htl

/-- ✅ **完全正当性定理 (`self_join_elimination`)**:
    非 `NULL` 主キー `k` の重複がない（`(R.map k).Nodup`）とき、
    自己等価結合 `(R ⋈_{k=k} R)` の左射影は `R` と **順序・重複度まで完全一致** する。 -/
theorem self_join_elimination_sound {Row K : Type} [DecidableEq K]
    (k : Row → K) (R : Rel Row) (h_pk : (R.map k).Nodup) :
    (innerJoin (fun x y => if k x = k y then .tTrue else .tFalse) R R).map Prod.fst = R := by
  have h_gen : ∀ xs : Rel Row, (∀ x ∈ xs, x ∈ R) →
      (innerJoin (fun x y => if k x = k y then .tTrue else .tFalse) xs R).map Prod.fst = xs := by
    intro xs hsub
    induction xs with
    | nil => rfl
    | cons x tl ih =>
      unfold innerJoin at ih ⊢
      have hxR : x ∈ R := hsub x (List.Mem.head tl)
      have hcond : ∀ y : Row,
          ((if k x = k y then SqlBool.tTrue else SqlBool.tFalse)).inWhere = decide (k x = k y) := by
        intro y; by_cases h : k x = k y <;> simp [h, SqlBool.inWhere]
      simp only [List.flatMap_cons, List.map_append, hcond]
      rw [filter_self_mem_eq_singleton k x R h_pk hxR]
      simp [ih (fun z hz => hsub z (List.Mem.tail x hz))]
  exact h_gen R (fun _ h => h)

private theorem any_eq_not_filter_isEmpty {α : Type} (p : α → Bool) (xs : List α) :
    xs.any p = !(xs.filter p).isEmpty := by
  induction xs with
  | nil => rfl
  | cons x tl ih => cases hp : p x <;> simp [List.filter, hp, ih]

/-- ✅ **完全正当性定理 (`unique_semi_to_inner`)**:
    右側の非 `NULL` キーが重複を持たない（`UNIQUE` — `NULL` が複数あってもよい！）とき、
    `InnerJoin` の左射影は `SemiJoin` と **重複度まで完全一致** する。 -/
theorem unique_semi_to_inner_sound {L R K : Type} [DecidableEq K]
    (kL : L → SqlVal K) (kR : R → SqlVal K)
    (ls : Rel L) (rs : Rel R)
    (h_uniq_nonnull : ∀ v : K, (rs.filter (fun r => decide (kR r = some v))).length ≤ 1) :
    (innerJoin (fun l r => sqlEq (kL l) (kR r)) ls rs).map Prod.fst =
      semiJoin (fun l r => sqlEq (kL l) (kR r)) ls rs := by
  unfold innerJoin semiJoin
  induction ls with
  | nil => rfl
  | cons l ltl ih =>
    simp only [List.flatMap_cons, List.map_append, List.filter]
    rw [ih]
    cases hkl : kL l with
    | none =>
      have hnil : ∀ xs : Rel R, xs.filter (fun r => (sqlEq none (kR r)).inWhere) = [] := by
        intro xs
        induction xs with
        | nil => rfl
        | cons r rtl rih => cases kR r <;> exact rih
      have hany : rs.any (fun r => (sqlEq none (kR r)).inWhere) = false := by
        rw [any_eq_not_filter_isEmpty, hnil rs]; rfl
      simp [hnil rs, hany]
    | some v =>
      have heq_filter :
          rs.filter (fun r => (sqlEq (some v) (kR r)).inWhere) =
          rs.filter (fun r => decide (kR r = some v)) := by
        apply List.filter_congr
        intro r _
        cases hkr : kR r with
        | none => rfl
        | some w =>
          by_cases hvw : v = w
          · subst hvw; simp [sqlEq, SqlBool.inWhere]
          · have hwv : w ≠ v := fun h => hvw h.symm
            simp [hvw, hwv, sqlEq, SqlBool.inWhere]
      have hany_iff :
          rs.any (fun r => (sqlEq (some v) (kR r)).inWhere) =
          !(rs.filter (fun r => decide (kR r = some v))).isEmpty := by
        rw [any_eq_not_filter_isEmpty, heq_filter]
      rw [heq_filter, hany_iff]
      have hlen := h_uniq_nonnull v
      cases hf : rs.filter (fun r => decide (kR r = some v)) with
      | nil => simp
      | cons r0 rtl =>
        rw [hf] at hlen
        cases rtl with
        | nil => simp
        | cons _ _ => simp at hlen

/-- 🚨 **実在バグ証明 H4 (`distinct_eager_over_join` の対向側自己等号による偽 Unique 判定)**:
    旧コードは `other_join_cols` 収集時に等号の反対側を確認していなかったため、
    `ON other.pk = other.col` の自己比較でも `other` 側が一意に固定されたと誤判定していた。 -/
theorem cpp_distinct_eager_self_eq_uniqueness_bug :
    let lhs := OnOperand.rightPk
    let rhs := OnOperand.rightCol
    cppBuggyPinsRightPk lhs rhs = true ∧ repairedPinsRightPk lhs rhs = false := by
  decide

/-- ✅ **完全正当性定理 (`distinct_eager_over_join` — InnerJoin & LeftOuterJoin)**:
    親 `Distinct` を保持する場合、`Distinct` を `InnerJoin` または `LeftOuterJoin` の左側に
    事前押し込みしても **メンバーシップ集合（ひいては `BagEq`）が完全に保存される**。 -/
theorem distinct_eager_over_join_sound {L R : Type} [DecidableEq L] [DecidableEq R]
    (p : L → R → SqlBool) (ls : Rel L) (rs : Rel R) :
    BagEq (distinctKeys (innerJoin p (distinctKeys ls) rs))
          (distinctKeys (innerJoin p ls rs)) ∧
    BagEq (distinctKeys (leftOuterJoin p (distinctKeys ls) rs))
          (distinctKeys (leftOuterJoin p ls rs)) := by
  constructor
  · apply nodup_perm_of_iff (nodup_distinctKeys _) (nodup_distinctKeys _)
    intro lr
    simp only [mem_distinctKeys, innerJoin, List.mem_flatMap, List.mem_map, List.mem_filter]
  · apply nodup_perm_of_iff (nodup_distinctKeys _) (nodup_distinctKeys _)
    intro lr
    simp only [mem_distinctKeys, leftOuterJoin, List.mem_flatMap]

private theorem filter_true_self {α : Type} (xs : List α) :
    xs.filter (fun _ => true) = xs := by
  induction xs with
  | nil => rfl
  | cons x tl ih => simp [List.filter, ih]

/-- ✅ **完全正当性定理 (`not_null_is_not_null_elimination`, `eliminate_true_selection`, `eliminate_false_selection`)** -/
theorem not_null_and_trivial_selection_sound {Row α : Type}
    (col : Row → SqlVal α) (rs : Rel Row) (h_nn : ∀ r ∈ rs, col r ≠ none) :
    rs.filter (fun r => (col r).isSome) = rs ∧
    rs.filter (fun _ => SqlBool.tTrue.inWhere) = rs ∧
    rs.filter (fun _ => SqlBool.tFalse.inWhere) = [] := by
  refine ⟨?_, filter_true_self rs, filter_false_nil rs⟩
  induction rs with
  | nil => rfl
  | cons r tl ih =>
    have hr : (col r).isSome = true := by
      cases hc : col r with
      | none => exact absurd hc (h_nn r (List.Mem.head tl))
      | some _ => rfl
    simp [List.filter, hr, ih (fun x hx => h_nn x (List.Mem.tail r hx))]

/-!
## 4. 等価クラス・推移律・冗長結合述語消去と `IN` リスト準結合化
対象 C++ ルール:
- `redundant_join_predicate_elimination` (`plan/cascades.cpp`)
- `join_predicate_transitivity` (`plan/cascades.cpp`)
- `infer_filter_from_equivalence_class` (`plan/cascades.cpp`)
- `functional_dependency_filter_reduction` (`plan/cascades.cpp`)
- `in_list_to_semi_join` (`plan/cascades.cpp`)
-/

def ufFind (fuel : Nat) (parent : List (String × String)) (x : String) : String :=
  match fuel with
  | 0 => x
  | fuel' + 1 =>
    match parent.lookup x with
    | none => x
    | some p => if p == x then x else ufFind fuel' parent p

def cppBuggyRedundantEqStep
    (parent : List (String × String)) (u v : String) : Bool × List (String × String) :=
  let ru := ufFind (parent.length + 1) parent u
  let rv := ufFind (parent.length + 1) parent v
  if ru == rv then (false, parent)
  else (true, (ru, rv) :: parent)

def cppBuggyFilterEqs (eqs : List (String × String)) : List (String × String) :=
  let rec go (parent : List (String × String)) : List (String × String) → List (String × String)
    | [] => []
    | (u, v) :: tl =>
      let (keep, parent') := cppBuggyRedundantEqStep parent u v
      if keep then (u, v) :: go parent' tl else go parent' tl
  go [] eqs

def evalEqConjuncts (env : String → SqlVal Int) : List (String × String) → Bool
  | [] => true
  | (u, v) :: tl => (sqlEq (env u) (env v)).inWhere && evalEqConjuncts env tl

/-- 🚨 **実在バグ証明 H2 (`redundant_join_predicate_elimination` の自己等号 `x = x` 誤削除バグ)**:
    `t1.id = t2.id AND t1.x = t1.x` に対して、旧 C++ の Union-Find は未出現の列 `t1.x` でも
    `find_root("t1.x") == find_root("t1.x")` が真になるため `t1.x = t1.x` を削除し、
    `t1.x = NULL` の行を誤って通過させていた！ -/
theorem cpp_redundant_join_eq_drops_self_eq_null_bug :
    let eqs := [("t1.id", "t2.id"), ("t1.x", "t1.x")]
    let env : String → SqlVal Int := fun s => if s == "t1.x" then none else some 1
    cppBuggyFilterEqs eqs = [("t1.id", "t2.id")] ∧
    evalEqConjuncts env eqs = false ∧
    evalEqConjuncts env (cppBuggyFilterEqs eqs) = true := by
  decide

/-- ✅ **完全正当性定理 (`join_predicate_transitivity` & 修復版 `redundant_join_predicate_elimination` &
    `infer_filter_from_equivalence_class` & `functional_dependency_filter_reduction`)** -/
theorem eq_transitivity_and_redundancy_sound {α : Type} [DecidableEq α]
    (a b c k : SqlVal α) (P : SqlVal α → SqlBool) :
    (((sqlEq a b).inWhere && (sqlEq b c).inWhere) =
      ((sqlEq a b).inWhere && (sqlEq b c).inWhere && (sqlEq a c).inWhere)) ∧
    (((sqlEq a b).inWhere && (sqlEq a a).inWhere) = (sqlEq a b).inWhere) ∧
    (((sqlEq a b).inWhere && (P a).inWhere) =
      ((sqlEq a b).inWhere && (P a).inWhere && (P b).inWhere)) ∧
    (∀ _ : k.isSome = true, ∀ _ : (P k).inWhere = true,
      ((sqlEq a k).inWhere && (P a).inWhere) = (sqlEq a k).inWhere) := by
  refine ⟨?_, ?_, ?_, ?_⟩
  · cases a with
    | none => rfl
    | some va =>
      cases b with
      | none => rfl
      | some vb =>
        cases c with
        | none => simp [sqlEq, SqlBool.inWhere]
        | some vc =>
          by_cases hab : va = vb <;> by_cases hbc : vb = vc
          · subst hab; subst hbc; simp [sqlEq, SqlBool.inWhere]
          · subst hab; simp [sqlEq, SqlBool.inWhere, hbc]
          · simp [sqlEq, SqlBool.inWhere, hab]
          · simp [sqlEq, SqlBool.inWhere, hab]
  · cases a with
    | none => rfl
    | some va =>
      cases b with
      | none => rfl
      | some vb =>
        by_cases hab : va = vb <;> simp [sqlEq, SqlBool.inWhere, hab]
  · cases a with
    | none => rfl
    | some va =>
      cases b with
      | none => rfl
      | some vb =>
        by_cases hab : va = vb
        · subst hab
          cases hp : (P (some va)).inWhere <;> simp
        · simp [sqlEq, SqlBool.inWhere, hab]
  · intro h_k h_pk
    cases k with
    | none => simp at h_k
    | some vk =>
      cases a with
      | none => rfl
      | some va =>
        by_cases hak : va = vk
        · subst hak
          rw [h_pk]
          simp [sqlEq, SqlBool.inWhere]
        · simp [sqlEq, SqlBool.inWhere, hak]

/-- 🚨 **実在バグ証明 H5 (`in_list_to_semi_join` の `;` 区切り Fingerprint 衝突バグ)**:
    旧 C++ は定数文字列をエスケープ・長さプレフィックス無しで `';'` 連結していたため、
    `x = 'a;b' OR x = 'c'` と `x = 'a' OR x = 'b;c'` が全く同じ Memo グループタグに衝突していた！ -/
def cppBuggyInListFingerprint (vals : List String) : String :=
  vals.foldl (fun acc s => acc ++ s ++ ";") ""

theorem cpp_in_list_fingerprint_collision_bug :
    let list1 := ["a;b", "c"]
    let list2 := ["a", "b;c"]
    list1 ≠ list2 ∧ cppBuggyInListFingerprint list1 = cppBuggyInListFingerprint list2 := by
  decide

/-- ✅ **完全正当性定理 (`in_list_to_semi_join`)**:
    非 `NULL` 定数リスト `cs` に対する同列 `OR` 等価フィルタは、定数テーブル `cs.map some` との
    `SemiJoin` と **全入力（`NULL` 含む）で順序・重複度まで完全一致** する。 -/
theorem in_list_to_semi_join_sound {Row α : Type} [DecidableEq α]
    (col : Row → SqlVal α) (cs : List α) (rs : Rel Row) :
    rs.filter (fun r => cs.any (fun c => (sqlEq (col r) (some c)).inWhere)) =
      semiJoin (fun r v => sqlEq (col r) v) rs (cs.map some) := by
  unfold semiJoin
  apply List.filter_congr
  intro r _
  induction cs with
  | nil => rfl
  | cons c tl ih => simp [List.map_cons, List.any_cons, ih]

/-!
## 5. Sort・Limit 合成と順序非依存集約の下の Sort 消去
対象 C++ ルール:
- `eliminate_sort_under_unordered_consumer` (`plan/cascades.cpp`)
- `push_filter_through_sort` (`plan/cascades.cpp`)
- `merge_limits` (`plan/cascades.cpp`)
-/

/-- 🚨 **実在バグ証明 H3 (`eliminate_sort_under_unordered_consumer` の `HAVING` 消失バグ)**:
    旧 C++ は `Aggregation(Sort(X))` から `Sort` を外す際に `.predicate`（`HAVING`）をコピーし忘れ、
    `HAVING` で弾かれるべきグループをそのまま出力していた！ -/
theorem cpp_elim_sort_under_agg_drops_having_bug :
    let sortedIn : Rel (Int × SqlVal Int) := [(1, some 2), (2, some 20)]
    let gt10 : SqlVal Int → Bool := fun v => match v with | some s => s > 10 | none => false
    let sumSnd : Int → Rel (Int × SqlVal Int) → SqlVal Int := fun _ g => sqlSum Prod.snd g
    let withHaving := (groupBy Prod.fst sumSnd sortedIn).filter (fun kv => gt10 kv.2)
    let buggyDroppedHaving := groupBy Prod.fst sumSnd sortedIn
    withHaving = [(2, some 20)] ∧ buggyDroppedHaving = [(1, some 2), (2, some 20)] ∧
    withHaving ≠ buggyDroppedHaving := by
  decide

/-- ✅ **完全正当性定理 (`eliminate_sort_under_unordered_consumer`)**:
    `Sort` 出力 `sortedR` が元入力 `R` の順列（`List.Perm sortedR R`）であり、
    集約が可換モノイド `m`（順序非依存）で `HAVING`（`havingP`）を保持するとき、
    `Sort` を除去しても `GROUP BY + HAVING` の結果は `BagEq` で完全一致する。 -/
theorem repaired_elim_sort_under_agg_sound {Row K M : Type} [DecidableEq K]
    (m : AggMonoid M) (key : Row → K) (val : Row → M) (havingP : K × M → Bool)
    (sortedR R : Rel Row) (h_perm : List.Perm sortedR R) :
    BagEq ((groupBy key (fun _ g => aggFold m val g) sortedR).filter havingP)
          ((groupBy key (fun _ g => aggFold m val g) R).filter havingP) := by
  apply List.Perm.filter
  apply groupBy_bagEq_of_keys_and_vals
  · intro k
    exact ⟨fun ⟨r, hr, hkr⟩ => ⟨r, h_perm.subset hr, hkr⟩,
           fun ⟨r, hr, hkr⟩ => ⟨r, h_perm.symm.subset hr, hkr⟩⟩
  · intro k _
    exact aggFold_perm m val (h_perm.filter _)

/-- ✅ **完全正当性定理 (`push_filter_through_sort`)**:
    任意の安定/不安定ソート（順列保存 `List.Perm`）に対してフィルタの押し込みは `BagEq` を保存する。 -/
theorem push_filter_through_sort_perm_sound {Row : Type}
    (p : Row → Bool) (sortedR R sortedFiltered : Rel Row)
    (h1 : List.Perm sortedR R)
    (h2 : List.Perm sortedFiltered (R.filter p)) :
    BagEq (sortedR.filter p) sortedFiltered :=
  (h1.filter p).trans h2.symm

/-- ✅ **完全正当性定理 (`merge_limits`)**:
    内側 `(off1, cnt1)` の上に外側 `(off2, cnt2)` を重ねた `Limit(Limit(X))` は、
    `off2 ≥ cnt1` なら空、そうでなければ `offset = off1 + off2`, `count = min cnt2 (cnt1 - off2)` に一致する。 -/
theorem merge_limits_sound {Row : Type}
    (xs : Rel Row) (off1 cnt1 off2 cnt2 : Nat) :
    (((xs.drop off1).take cnt1).drop off2).take cnt2 =
      if off2 ≥ cnt1 then []
      else (xs.drop (off1 + off2)).take (Nat.min cnt2 (cnt1 - off2)) := by
  rw [List.drop_take, List.drop_drop, List.take_take]
  by_cases h : off2 ≥ cnt1
  · have hzero : cnt1 - off2 = 0 := Nat.sub_eq_zero_of_le h
    simp [h, hzero]
  · simp [h, Nat.add_comm off1 off2]

/-!
## 6. 三値論理スカラー式書き換えルールの網羅証明 (`expression/rewrite.cpp`)
対象 C++ ルール:
- `absorption_or`, `absorption_or_reversed`
- `factor_or_common_and`, `distribute_or_over_and_budgeted`
- `xor_boolean_identity`, `xor_to_or_and_not`
- `contradiction_from_null_eq`, `in_single_null`, `not_in_null_semantics`
- `boolean_eq_true_false_three_valued`
- `coalesce_and_nullif_simplification`, `nullif_to_case`
-/

def SqlBool.xor : SqlBool → SqlBool → SqlBool
  | .tNull,  _       => .tNull
  | _,       .tNull  => .tNull
  | .tTrue,  .tTrue  => .tFalse
  | .tFalse, .tFalse => .tFalse
  | _,       _       => .tTrue

def SqlBool.isTrue3 : SqlBool → SqlBool
  | .tTrue => .tTrue
  | _      => .tFalse

def SqlBool.isNotTrue3 : SqlBool → SqlBool
  | .tTrue => .tFalse
  | _      => .tTrue

def SqlBool.isFalse3 : SqlBool → SqlBool
  | .tFalse => .tTrue
  | _       => .tFalse

def SqlBool.isNotFalse3 : SqlBool → SqlBool
  | .tFalse => .tFalse
  | _       => .tTrue

def SqlBool.eqBool : SqlBool → SqlBool → SqlBool
  | .tNull,  _       => .tNull
  | _,       .tNull  => .tNull
  | .tTrue,  .tTrue  => .tTrue
  | .tFalse, .tFalse => .tTrue
  | _,       _       => .tFalse

/-- ✅ **完全正当性定理 (`absorption_or`, `factor_or_common_and`, `distribute_or_over_and_budgeted`)** -/
theorem three_valued_distributive_and_absorption_sound (a b c : SqlBool) :
    a.or (a.and b) = a ∧
    (a.and b).or a = a ∧
    (a.and b).or (a.and c) = a.and (b.or c) ∧
    (a.and b).or c = (a.or c).and (b.or c) := by
  cases a <;> cases b <;> cases c <;> decide

/-- ✅ **完全正当性定理 (`xor_boolean_identity` & `xor_to_or_and_not`)**:
    Kleene 三値論理の 9 通りすべてで `a XOR b = (a OR b) AND NOT (a AND b)` が厳密成立する。 -/
theorem xor_rules_three_valued_sound (a b : SqlBool) :
    a.xor .tTrue = a.not ∧
    a.xor .tFalse = a ∧
    SqlBool.xor .tTrue a = a.not ∧
    SqlBool.xor .tFalse a = a ∧
    a.xor .tNull = .tNull ∧
    a.xor b = (a.or b).and (a.and b).not := by
  cases a <;> cases b <;> decide

/-- ✅ **完全正当性定理 (`boolean_eq_true_false_three_valued`)**:
    `IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE` と `TRUE(1) / FALSE(0)` の等号比較が
    それぞれの正確な補集合述語に一致することの機械証明。 -/
theorem boolean_eq_true_false_three_valued_sound (x : SqlBool) :
    x.isTrue3.eqBool .tTrue = x.isTrue3 ∧
    x.isNotTrue3.eqBool .tTrue = x.isNotTrue3 ∧
    x.isFalse3.eqBool .tTrue = x.isFalse3 ∧
    x.isNotFalse3.eqBool .tTrue = x.isNotFalse3 ∧
    x.isTrue3.eqBool .tFalse = x.isNotTrue3 ∧
    x.isNotTrue3.eqBool .tFalse = x.isTrue3 ∧
    x.isFalse3.eqBool .tFalse = x.isNotFalse3 ∧
    x.isNotFalse3.eqBool .tFalse = x.isFalse3 := by
  cases x <;> decide

/-- SQL の `x IN (list)` の三値評価（`Option Int` 要素リスト）。 -/
abbrev sqlInList (x : SqlVal Int) := evalInList x

private theorem sqlOr_assoc (a b c : SqlBool) : (a.or b).or c = a.or (b.or c) := by
  cases a <;> cases b <;> cases c <;> rfl

private theorem sqlInList_append (x : SqlVal Int) (l1 l2 : List (SqlVal Int)) :
    sqlInList x (l1 ++ l2) = (sqlInList x l1).or (sqlInList x l2) := by
  induction l1 with
  | nil =>
    simp only [List.nil_append, sqlInList, empty_in_is_false]
    cases evalInList x l2 <;> rfl
  | cons y tl ih =>
    simp only [List.cons_append, sqlInList, evalInList_cons, ih, sqlOr_assoc]

/-- ✅ **完全正当性定理 (`contradiction_from_null_eq`, `in_single_null`, `not_in_null_semantics`)**:
    `NOT (x IN (c1, ..., cn, NULL))` は任意の `x : SqlVal Int`（`NULL` 含む）および任意の定数リスト `cs` に対して
    `CASE WHEN x IN (c1, ..., cn) THEN FALSE ELSE NULL END` と **三値論理値まで完全一致** する！ -/
theorem not_in_null_semantics_sound (x : SqlVal Int) (cs : List (SqlVal Int)) :
    sqlEq x none = .tNull ∧
    sqlInList x [none] = .tNull ∧
    (sqlInList x (cs ++ [none])).not =
      (if (sqlInList x cs).inWhere then SqlBool.tFalse else SqlBool.tNull) := by
  refine ⟨by cases x <;> rfl, by cases x <;> rfl, ?_⟩
  have hsingle : sqlInList x [none] = .tNull := by cases x <;> rfl
  rw [sqlInList_append, hsingle]
  cases sqlInList x cs <;> rfl

/-- SQL の可変長 `COALESCE(args)` と `NULLIF(a, b)`。 -/
def sqlCoalesceList {α : Type} : List (SqlVal α) → SqlVal α
  | [] => none
  | some v :: _ => some v
  | none :: tl => sqlCoalesceList tl

def sqlNullIf {α : Type} [DecidableEq α] (a b : SqlVal α) : SqlVal α :=
  if (sqlEq a b).inWhere then none else a

theorem sqlCoalesceList_append {α : Type} (l1 l2 : List (SqlVal α)) :
    sqlCoalesceList (l1 ++ l2) =
      match sqlCoalesceList l1 with
      | some v => some v
      | none => sqlCoalesceList l2 := by
  induction l1 with
  | nil => rfl
  | cons x tl ih =>
    cases x with
    | some v => rfl
    | none => exact ih

/-- ✅ **完全正当性定理 (`coalesce_and_nullif_simplification` & `nullif_to_case` + `NaN` 自己 `NULLIF` トラップ)** -/
theorem coalesce_and_nullif_rules_sound {α : Type} [DecidableEq α]
    (a b : SqlVal α) (v : α) (xs ys : List (SqlVal α)) :
    sqlCoalesceList (none :: xs) = sqlCoalesceList xs ∧
    sqlCoalesceList (xs ++ [none]) = sqlCoalesceList xs ∧
    sqlCoalesceList (some v :: ys) = some v ∧
    sqlCoalesceList [sqlCoalesceList xs, sqlCoalesceList ys] = sqlCoalesceList (xs ++ ys) ∧
    sqlNullIf none b = none ∧
    sqlNullIf a none = a ∧
    sqlNullIf a a = none ∧
    (if (floatLt .nan .nan).inWhere then Option.none else some FloatVal.nan) =
      some FloatVal.nan := by
  refine ⟨rfl, ?_, rfl, ?_, rfl, ?_, ?_, by decide⟩
  · rw [sqlCoalesceList_append]
    cases sqlCoalesceList xs <;> rfl
  · rw [sqlCoalesceList_append]
    cases sqlCoalesceList xs with
    | some _ => rfl
    | none => cases sqlCoalesceList ys <;> rfl
  · unfold sqlNullIf; cases a <;> rfl
  · unfold sqlNullIf; cases a with
    | none => rfl
    | some va => simp [sqlEq, SqlBool.inWhere]

end TinylambOpt
