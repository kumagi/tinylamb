import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_PriceRangeBlockAcceptance. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_PriceRangeBlockAcceptance

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# PriceRangeBlockAcceptance

A price page is accepted whole when its minimum already exceeds the threshold,
rejected whole when its maximum is at most the threshold, and otherwise scanned
with the threshold predicate. These three cases partition every possible page,
including the empty page, so the chosen path returns exactly the qualifying row
IDs in physical order.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem krs_read_page_eq_filter (p : List (Int × Nat)) :
    (if (p.map Prod.fst).min?.any (fun v => 100 < v) then p
     else if (p.map Prod.fst).max?.any (fun v => v ≤ 100) then []
     else p.filter (fun r => 100 < r.1)) = p.filter (fun r => 100 < r.1) := by
  by_cases hmin : (p.map Prod.fst).min?.any (fun v => 100 < v) = true
  · rcases Option.any_eq_true (fun v : Int => 100 < v) (p.map Prod.fst).min? |>.mp hmin with ⟨m, hmSome, hm100⟩
    have hprops := List.min?_eq_some_iff.mp hmSome
    have hm100p : 100 < m := by simpa using hm100
    have hall : ∀ r ∈ p, 100 < r.1 := by
      intro r hr
      have hrmem : r.1 ∈ p.map Prod.fst := List.mem_map.mpr ⟨r, hr, rfl⟩
      have hmle : m ≤ r.1 := hprops.2 r.1 hrmem
      omega
    have hfilt : p.filter (fun r => 100 < r.1) = p := by
      apply List.filter_eq_self.mpr
      intro r hr
      simpa using hall r hr
    simp [hmin, hfilt]
  · by_cases hmax : (p.map Prod.fst).max?.any (fun v => v ≤ 100) = true
    · rcases Option.any_eq_true (fun v : Int => v ≤ 100) (p.map Prod.fst).max? |>.mp hmax with ⟨m, hmSome, hm100⟩
      have hprops := List.max?_eq_some_iff.mp hmSome
      have hm100p : m ≤ 100 := by simpa using hm100
      have hall : ∀ r ∈ p, ¬ 100 < r.1 := by
        intro r hr
        have hrmem : r.1 ∈ p.map Prod.fst := List.mem_map.mpr ⟨r, hr, rfl⟩
        have hlem : r.1 ≤ m := hprops.2 r.1 hrmem
        omega
      have hfilt : p.filter (fun r => 100 < r.1) = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro r hr
        exact (hall r (List.mem_filter.mp hr).1) (by simpa using (List.mem_filter.mp hr).2)
      simp [hmin, hmax, hfilt]
    · simp [hmin, hmax]

theorem krs_flatMap_filter_snd (pages : List (List (Int × Nat))) :
    (pages.flatMap (fun p => p.filter (fun r => 100 < r.1))).map Prod.snd =
      (pages.flatten.filter (fun r => 100 < r.1)).map Prod.snd := by
  induction pages with
  | nil => simp
  | cons p ps ih => simp [ih, List.filter_append]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.PriceRangeBlockAcceptance.Goal := by
  intro pages
  dsimp
  let readPage := fun p : List (Int × Nat) =>
    if (p.map Prod.fst).min?.any (fun v => 100 < v) then p
    else if (p.map Prod.fst).max?.any (fun v => v ≤ 100) then []
    else p.filter (fun r => 100 < r.1)
  have hread : pages.flatMap readPage =
      pages.flatMap (fun p => p.filter (fun r => 100 < r.1)) := by
    induction pages with
    | nil => rfl
    | cons p ps ih => simp [readPage, krs_read_page_eq_filter, ih]
  change (pages.flatMap readPage).map Prod.snd =
    (pages.flatten.filter (fun r => 100 < r.1)).map Prod.snd
  rw [hread]
  exact krs_flatMap_filter_snd pages
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_PriceRangeBlockAcceptance
