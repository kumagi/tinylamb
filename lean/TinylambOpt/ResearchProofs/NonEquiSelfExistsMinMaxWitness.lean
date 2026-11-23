import TinylambOpt.ResearchGoals.NonEquiSelfExistsMinMaxWitness

namespace TinylambOpt.ResearchProofs.NonEquiSelfExistsMinMaxWitness

open TinylambOpt.ResearchGoals.NonEquiSelfExistsMinMaxWitness

/-! ## Characterising the `groupMinMax` fold -/

private def step (oid : Int) (acc : Option (Int × Int)) (r2 : Row) : Option (Int × Int) :=
  if r2.1 != oid then acc
  else
    match r2.2, acc with
    | none, _ => acc
    | some b, none => some (b, b)
    | some b, some (mn, mx) => some (min mn b, max mx b)

private def nonNulls (rows : List Row) (oid : Int) : List Int :=
  rows.filterMap fun r2 => if r2.1 == oid then r2.2 else none

private theorem groupMinMax_eq_fold (rows : List Row) (oid : Int) :
    groupMinMax rows oid = rows.foldl (step oid) none := rfl

/-- `IsSummary S acc` says that `acc` is `none` when `S` is empty, and otherwise
    stores the minimum and maximum of the non-empty list `S`. -/
private def IsSummary (S : List Int) (acc : Option (Int × Int)) : Prop :=
  match acc with
  | none => S = []
  | some (mn, mx) =>
      S ≠ [] ∧
      (∀ b ∈ S, mn ≤ b ∧ b ≤ mx) ∧
      mn ∈ S ∧ mx ∈ S

private theorem step_summary (oid : Int) (r2 : Row) (acc : Option (Int × Int))
    (S : List Int) : IsSummary S acc →
      IsSummary (S ++ nonNulls [r2] oid) (step oid acc r2) := by
  intro hacc
  by_cases hm : (r2.1 == oid) = true
  · have heq : r2.1 = oid := by simpa using hm
    subst oid
    cases h2 : r2.2 with
    | none =>
        simpa [step, nonNulls, h2] using hacc
    | some b =>
        cases acc with
        | none =>
            have hS : S = [] := hacc
            subst S
            simp [step, nonNulls, h2, IsSummary]
        | some sm =>
            rcases sm with ⟨mn, mx⟩
            rcases hacc with ⟨hSne, hbounds, hmnS, hmxS⟩
            have hstep : step r2.1 (some (mn, mx)) r2 =
                some (min mn b, max mx b) := by simp [step, h2]
            have hnn : nonNulls [r2] r2.1 = [b] := by simp [nonNulls, h2]
            rw [hstep, hnn]
            dsimp [IsSummary]
            refine ⟨?_, ?_, ?_, ?_⟩
            · simp [hSne]
            · intro x hx
              rcases List.mem_append.mp hx with hxS | hxb
              · have hbnds := hbounds x hxS
                exact ⟨Int.le_trans (Int.min_le_left mn b) hbnds.1,
                  Int.le_trans hbnds.2 (Int.le_max_left mx b)⟩
              · have hxb' : x = b := by simpa using hxb
                subst x
                exact ⟨Int.min_le_right mn b, Int.le_max_right mx b⟩
            · by_cases hle : mn ≤ b
              · rw [Int.min_eq_left hle]
                exact List.mem_append_left [b] hmnS
              · have hle' : b ≤ mn := by omega
                rw [Int.min_eq_right hle']
                exact List.mem_append.mpr (Or.inr (by simp))
            · by_cases hle : mx ≤ b
              · rw [Int.max_eq_right hle]
                exact List.mem_append.mpr (Or.inr (by simp))
              · have hle' : b ≤ mx := by omega
                rw [Int.max_eq_left hle']
                exact List.mem_append_left [b] hmxS
  · have hne : r2.1 ≠ oid := by simpa using hm
    simpa [step, nonNulls, hne] using hacc

private theorem fold_summary_aux (rows : List Row) (oid : Int)
    (acc : Option (Int × Int)) (S : List Int) :
    IsSummary S acc → IsSummary (S ++ nonNulls rows oid) (rows.foldl (step oid) acc) := by
  induction rows generalizing S acc with
  | nil =>
      intro hacc
      simpa [nonNulls] using hacc
  | cons r2 rows ih =>
      intro hacc
      rw [List.foldl_cons]
      have hsplit : nonNulls (r2 :: rows) oid = nonNulls [r2] oid ++ nonNulls rows oid := by
        unfold nonNulls
        by_cases h : r2.1 = oid <;> cases h2 : r2.2 <;> simp [h, h2]
      rw [hsplit, ← List.append_assoc]
      exact ih (acc := step oid acc r2) (S := S ++ nonNulls [r2] oid)
        (step_summary oid r2 acc S hacc)

private theorem fold_summary (rows : List Row) (oid : Int) :
    IsSummary (nonNulls rows oid) (rows.foldl (step oid) none) := by
  simpa using fold_summary_aux rows oid none [] (by simp [IsSummary])

/-! ## Reducing the quadratic reference to the same list -/

private theorem ref_none (rows : List Row) (oid : Int) :
    refSelfExists rows (oid, none) = false := by
  induction rows with
  | nil => simp [refSelfExists]
  | cons r2 rows ih => simp [refSelfExists]

private theorem ref_eq_any (rows : List Row) (oid : Int) (a : Int) :
    refSelfExists rows (oid, some a) = (nonNulls rows oid).any (fun b => a != b) := by
  induction rows with
  | nil => simp [refSelfExists, nonNulls]
  | cons r2 rows ih =>
      unfold refSelfExists at ih ⊢
      rw [List.any_cons, ih]
      by_cases hm : (r2.1 == oid) = true
      · have heq : r2.1 = oid := by simpa using hm
        cases h2 : r2.2 <;> simp [nonNulls, heq, h2]
      · have hne : r2.1 ≠ oid := by simpa using hm
        simp [nonNulls, hne]

/-! ## The `O(1)` decision agrees with the list search -/

private theorem decision_matches (S : List Int) (mn mx a : Int) :
    IsSummary S (some (mn, mx)) →
      decide (mn < mx || a != mn) = S.any (fun b => a != b) := by
  intro h
  rcases h with ⟨hSne, hbounds, hmnS, hmxS⟩
  by_cases hlt : mn < mx
  · have hlhs : decide (mn < mx || a != mn) = true := by simp [hlt]
    have hrhs : S.any (fun b => a != b) = true := by
      rw [List.any_eq_true]
      by_cases ha : a = mn
      · subst a
        refine ⟨mx, hmxS, ?_⟩
        simp
        omega
      · exact ⟨mn, hmnS, by simp [ha]⟩
    rw [hlhs, hrhs]
  · have hmn_le_mx : mn ≤ mx := (hbounds mx hmxS).1
    have hmnx : mn = mx := by omega
    have hall : ∀ b ∈ S, b = mn := by
      intro b hb
      have hbnd := hbounds b hb
      omega
    by_cases hane : (a != mn) = true
    · have hlhs : decide (mn < mx || a != mn) = true := by simp [hlt, hane]
      have hrhs : S.any (fun b => a != b) = true := by
        rw [List.any_eq_true]
        exact ⟨mn, hmnS, hane⟩
      rw [hlhs, hrhs]
    · have hane_f : (a != mn) = false := Bool.eq_false_of_ne_true hane
      have hlhs : decide (mn < mx || a != mn) = false := by simp [hlt, hane_f]
      have hrhs : S.any (fun b => a != b) = false := by
        apply Bool.eq_false_of_ne_true
        intro hany
        rw [List.any_eq_true] at hany
        rcases hany with ⟨b, hbS, hbne⟩
        have hb_eq : b = mn := hall b hbS
        subst b
        exact hane hbne
      rw [hlhs, hrhs]

theorem sound : TinylambOpt.ResearchGoals.NonEquiSelfExistsMinMaxWitness.Goal := by
  intro rows r
  rcases r with ⟨oid, wh⟩
  cases wh with
  | none =>
      rw [ref_none]
      simp [optMinMaxWitness]
  | some a =>
      rw [ref_eq_any]
      have hsum := fold_summary rows oid
      unfold optMinMaxWitness
      rw [show groupMinMax rows oid = rows.foldl (step oid) none from groupMinMax_eq_fold rows oid]
      cases hs : rows.foldl (step oid) none with
      | none =>
          have hS : nonNulls rows oid = [] := by simpa [hs, IsSummary] using hsum
          simp [hS]
      | some sm =>
          rcases sm with ⟨mn, mx⟩
          have hsum' : IsSummary (nonNulls rows oid) (some (mn, mx)) := by
            simpa [hs] using hsum
          simp
          exact decision_matches (nonNulls rows oid) mn mx a hsum'

end TinylambOpt.ResearchProofs.NonEquiSelfExistsMinMaxWitness
