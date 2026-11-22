import Std.Tactic
import TinylambOpt.ThreeValued

/-
# 相関 NOT IN を NULL 対応マーク結合に置き換える

C++ 対応 (`executor/detail/subquery_runtime.cpp` のマーク結合 / `NOT IN` 実行):
  * 相関キーごとの build を有限な状態に要約する: 「NULL を含むか」と「非 NULL 値のリスト」。
    build が空かどうかは `!hasNull && values.isEmpty` で復元する。
  * この要約は相関キーごとに作るので、別の相関キーの bucket に NULL があっても
    外側の別の行の三値には影響しない。
  * 述語は単一 INT64 キーで全域・決定的（同じスナップショットでは同じ相関キーに対して
    同じ build が得られる）として、`bucket : OVal → List OVal` でモデル化する。
  * 相関キーが NULL なら SQL の等値比較は UNKNOWN となり build は 0 行なので、
    `bucket none = []` を仮定する（C++ の NULL 相関キーの扱い）。
  * 参照は既存の 3 値 `IN` (`evalInList`) に `NOT` を適用したもの。
    WHERE と射影の両方で同じ三値が観測されることを比較する（issue の「射影でも比較する」）。 -/

namespace TinylambOpt.NullableMarkJoin

abbrev OVal := Option Int

@[simp] theorem or_false (b : SqlBool) : SqlBool.tFalse.or b = b := by
  cases b <;> rfl
@[simp] theorem or_true (b : SqlBool) : SqlBool.tTrue.or b = .tTrue := by
  cases b <;> rfl
@[simp] theorem or_null (b : SqlBool) :
    SqlBool.tNull.or b = (if b = .tTrue then .tTrue else .tNull) := by
  cases b <;> rfl

/-- 相関キーごとの有限要約: (NULL を含むか, 非 NULL 値のリスト)。 -/
def markStateOfList : List OVal → Bool × List Int
  | [] => (false, [])
  | none :: xs => let s := markStateOfList xs; (true, s.2)
  | some v :: xs => let s := markStateOfList xs; (s.1, v :: s.2)

/-- 要約から `NOT IN` の三値を復元する。 -/
def markNotIn (x : OVal) (s : Bool × List Int) : SqlBool :=
  match x with
  | none => if !s.1 && s.2.isEmpty then .tTrue else .tNull
  | some v => if v ∈ s.2 then .tFalse else if s.1 then .tNull else .tTrue

/-- 補助: probe が NULL なら `IN` は決して TRUE にならない。 -/
theorem evalInList_none_not_true : ∀ xs : List OVal, evalInList none xs ≠ .tTrue
  | [] => by simp [evalInList]
  | o :: xs => by
      rw [evalInList_cons]
      cases hs : evalInList none xs with
      | tTrue => exfalso; exact evalInList_none_not_true xs hs
      | tFalse => cases o <;> simp [sqlEq, SqlBool.or]
      | tNull => cases o <;> simp [sqlEq, SqlBool.or]

/-- 補助: 非 NULL probe の `IN` が TRUE になるのは要約の値リストに hits があるときだけ。 -/
theorem mem_values_iff_true (v : Int) :
    ∀ xs : List OVal, v ∈ (markStateOfList xs).2 ↔ evalInList (some v) xs = .tTrue
  | [] => by simp [markStateOfList, evalInList]
  | none :: xs => by
      change v ∈ (markStateOfList xs).2 ↔ evalInList (some v) (none :: xs) = .tTrue
      rw [evalInList_cons]
      have hih := mem_values_iff_true v xs
      cases hs : evalInList (some v) xs with
      | tTrue => simp [sqlEq, hs] at hih ⊢; exact hih
      | tFalse => simp [sqlEq, hs] at hih ⊢; exact hih
      | tNull => simp [sqlEq, hs] at hih ⊢; exact hih
  | some w :: xs => by
      change v ∈ w :: (markStateOfList xs).2 ↔ evalInList (some v) (some w :: xs) = .tTrue
      rw [evalInList_cons]
      by_cases hvw : v = w
      · subst hvw
        simp [sqlEq]
      · have hih := mem_values_iff_true v xs
        simpa [sqlEq, hvw] using hih

/-- 補助: `IN` が FALSE なら build に NULL は無い。 -/
theorem hasNull_of_in_false (v : Int) :
    ∀ xs : List OVal, evalInList (some v) xs = .tFalse → (markStateOfList xs).1 = false := by
  intro xs
  induction xs with
  | nil => intro h; rfl
  | cons o xs ih =>
      intro h
      cases o with
      | none =>
          rw [evalInList_cons] at h
          cases hs : evalInList (some v) xs <;> simp [sqlEq, hs] at h
      | some w =>
          rw [evalInList_cons] at h
          by_cases hvw : v = w
          · subst hvw; simp [sqlEq] at h
          · have hrest : evalInList (some v) xs = .tFalse := by simpa [sqlEq, hvw] using h
            have hn := ih hrest
            simpa [markStateOfList] using hn

/-- 補助: `IN` が UNKNOWN なら build に NULL が有る。 -/
theorem hasNull_of_in_null (v : Int) :
    ∀ xs : List OVal, evalInList (some v) xs = .tNull → (markStateOfList xs).1 = true := by
  intro xs
  induction xs with
  | nil => intro h; cases h
  | cons o xs ih =>
      intro h
      cases o with
      | none => rfl
      | some w =>
          rw [evalInList_cons] at h
          by_cases hvw : v = w
          · subst hvw; simp [sqlEq] at h
          · have hrest : evalInList (some v) xs = .tNull := by simpa [sqlEq, hvw] using h
            have hn := ih hrest
            simpa [markStateOfList] using hn

/-- 核定理: 有限要約から復元した三値が、元の逐次 `IN` 評価の `NOT` と完全一致する。 -/
theorem mark_matches_reference_list (x : OVal) :
    ∀ b : List OVal, markNotIn x (markStateOfList b) = (evalInList x b).not
  | [] => by cases x <;> rfl
  | none :: xs => by
      cases x with
      | none =>
          have hn := evalInList_none_not_true xs
          rw [evalInList_cons]
          cases hs : evalInList none xs with
          | tTrue => exfalso; exact hn hs
          | tFalse => simp [markStateOfList, markNotIn, sqlEq, SqlBool.not]
          | tNull => simp [markStateOfList, markNotIn, sqlEq, SqlBool.not]
      | some v =>
          have hmem := mem_values_iff_true v xs
          rw [evalInList_cons]
          by_cases h : v ∈ (markStateOfList xs).2
          · have ht : evalInList (some v) xs = .tTrue := hmem.mp h
            simp [markStateOfList, markNotIn, sqlEq, SqlBool.not, ht, h]
          · have hnt : evalInList (some v) xs ≠ .tTrue := by
                intro ht; exact h (hmem.mpr ht)
            cases hs : evalInList (some v) xs with
            | tTrue => exfalso; exact hnt hs
            | tFalse => simp [markStateOfList, markNotIn, sqlEq, SqlBool.not, h]
            | tNull => simp [markStateOfList, markNotIn, sqlEq, SqlBool.not, h]
  | some w :: xs => by
      cases x with
      | none =>
          have hn := evalInList_none_not_true xs
          rw [evalInList_cons]
          cases hs : evalInList none xs with
          | tTrue => exfalso; exact hn hs
          | tFalse => simp [markStateOfList, markNotIn, sqlEq, SqlBool.not]
          | tNull => simp [markStateOfList, markNotIn, sqlEq, SqlBool.not]
      | some v =>
          by_cases hvw : v = w
          · subst hvw
            rw [evalInList_cons]
            simp [markStateOfList, markNotIn, sqlEq, SqlBool.not]
          · by_cases hmemv : v ∈ (markStateOfList xs).2
            · have ht : evalInList (some v) xs = .tTrue :=
                (mem_values_iff_true v xs).mp hmemv
              rw [evalInList_cons]
              simp [markStateOfList, markNotIn, sqlEq, hvw, hmemv, ht, SqlBool.not]
            · have hnt : evalInList (some v) xs ≠ .tTrue := by
                intro ht; exact hmemv ((mem_values_iff_true v xs).mpr ht)
              rw [evalInList_cons]
              cases hs : evalInList (some v) xs with
              | tTrue => exfalso; exact hnt hs
              | tFalse =>
                  have hnfalse := hasNull_of_in_false v xs hs
                  simp [markStateOfList, markNotIn, sqlEq, hvw, hmemv, hnfalse, SqlBool.not]
              | tNull =>
                  have hntrue := hasNull_of_in_null v xs hs
                  simp [markStateOfList, markNotIn, sqlEq, hvw, hmemv, hntrue, SqlBool.not]

/-- マーク結合が復元する三値と、参照の逐次 NOT IN。 -/
def markNotInFor (bucket : OVal → List OVal) (corrKey probe : OVal) : SqlBool :=
  markNotIn probe (markStateOfList (bucket corrKey))

def refNotIn (bucket : OVal → List OVal) (corrKey probe : OVal) : SqlBool :=
  (evalInList probe (bucket corrKey)).not

structure OuterRow where
  corrKey : OVal
  probe : OVal

/-- 三値を SQL の NULL 許容ブール値に写像（射影での観測用）。 -/
def sqlBoolToVal : SqlBool → SqlVal Bool
  | .tTrue => some true
  | .tFalse => some false
  | .tNull => none

/-- **定理 (`nullable_mark_join_sound`)**:
    NULL 相関キーの build が空 (`bucket none = []`) なら、
    (a) マーク状態から復元した三値が逐次 `NOT IN` と 1 行ごとに完全一致し、
    (b) WHERE の通過行リストが一致し、
    (c) 射影される三値（NULL 許容ブール）も一致する。 -/
theorem nullable_mark_join_sound (bucket : OVal → List OVal) (hNullKey : bucket none = [])
    (rows : List OuterRow) :
    (∀ r : OuterRow, markNotInFor bucket r.corrKey r.probe = refNotIn bucket r.corrKey r.probe) ∧
    rows.filter (fun r => (markNotInFor bucket r.corrKey r.probe).inWhere) =
      rows.filter (fun r => (refNotIn bucket r.corrKey r.probe).inWhere) ∧
    rows.map (fun r => sqlBoolToVal (markNotInFor bucket r.corrKey r.probe)) =
      rows.map (fun r => sqlBoolToVal (refNotIn bucket r.corrKey r.probe)) := by
  have hrow : ∀ r : OuterRow,
      markNotInFor bucket r.corrKey r.probe = refNotIn bucket r.corrKey r.probe := by
    intro r
    cases hk : r.corrKey with
    | none =>
        cases hp : r.probe <;>
          simp [markNotInFor, refNotIn, markStateOfList, markNotIn, hNullKey, evalInList,
                SqlBool.not]
    | some k =>
        exact mark_matches_reference_list r.probe (bucket (some k))
  refine ⟨hrow, ?_, ?_⟩
  · induction rows with
    | nil => rfl
    | cons x xs ih =>
        simp only [List.filter_cons]
        rw [hrow x, ih]
  · induction rows with
    | nil => rfl
    | cons x xs ih =>
        simp only [List.map_cons]
        rw [hrow x, ih]

/-- 境界例: probe が NULL で build が空なら TRUE、1 件でもあれば UNKNOWN。 -/
theorem boundary_examples :
    markNotIn none (markStateOfList []) = .tTrue ∧
    markNotIn none (markStateOfList [none]) = .tNull ∧
    markNotIn none (markStateOfList [some 5]) = .tNull ∧
    markNotIn (some 5) (markStateOfList [some 5, none]) = .tFalse ∧
    markNotIn (some 7) (markStateOfList [some 5, none]) = .tNull ∧
    markNotIn (some 7) (markStateOfList [some 5]) = .tTrue := by
  decide

end TinylambOpt.NullableMarkJoin
