import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_WitnessedGroupBoundaryBeforeExistenceProbes

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
A sample of distinct witnessed eligible groups certifies that at least the first
hundred eligible groups lie below the cutoff. Existence probes may therefore be
restricted to rows at or below that cutoff: the hundred smallest eligible keys
and their full counts are unchanged, so the modeled top-group state is identical.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- Two value-only filters commute. -/
theorem filter_filter_comm {α} (xs : List α) (p q : α → Bool) :
    (xs.filter p).filter q = (xs.filter q).filter p := by
  rw [List.filter_filter, List.filter_filter]
  apply congrArg (fun f : α → Bool => xs.filter f)
  funext b
  cases hp : p b <;> cases hq : q b <;> simp [hp, hq]

/-- Deduplication commutes with a value-only selection predicate. -/
theorem eraseDups_filter {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (p : α → Bool) :
    (xs.filter p).eraseDups = xs.eraseDups.filter p := by
  induction hlen : xs.length using Nat.strongRecOn generalizing xs with
  | ind n ih =>
      cases xs with
      | nil => simp
      | cons x xs =>
          have hn : n = xs.length + 1 := by simpa using hlen.symm
          have hsmall : (xs.filter (fun b => !b == x)).length < n := by
            have hle := List.length_filter_le (fun b => !b == x) xs
            omega
          by_cases hx : p x = true
          · rw [List.filter_cons_of_pos hx]
            simp only [List.eraseDups_cons]
            rw [List.filter_cons_of_pos hx]
            have ih' := ih ((xs.filter (fun b => !b == x)).length) hsmall
              (xs.filter (fun b => !b == x)) rfl
            rw [← filter_filter_comm xs (fun b => !b == x) p]
            exact congrArg (fun t => x :: t) ih'
          · rw [List.filter_cons_of_neg hx]
            simp only [List.eraseDups_cons]
            rw [List.filter_cons_of_neg hx]
            have hpneq : (xs.filter p).filter (fun b => !b == x) = xs.filter p := by
              apply List.filter_eq_self.mpr
              intro b hb
              have hbp : p b = true := (List.mem_filter.mp hb).2
              by_cases hbx : (b == x) = true
              · have hbeq : b = x := LawfulBEq.eq_of_beq hbx
                rw [hbeq] at hbp
                exact False.elim (hx hbp)
              · simp [hbx]
            have hyseq : (xs.filter (fun b => !b == x)).filter p = xs.filter p := by
              rw [filter_filter_comm]
              exact hpneq
            have ih' := ih ((xs.filter (fun b => !b == x)).length) hsmall
              (xs.filter (fun b => !b == x)) rfl
            rw [hyseq] at ih'
            exact ih'

/-- Projecting keys after a key-only filter commutes with filtering the keys. -/
theorem map_fst_filter {p : Nat → Bool} (rs : List (Nat × Nat)) :
    (rs.filter (fun r => p r.1)).map Prod.fst = (rs.map Prod.fst).filter p := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨k, b⟩
      by_cases hk : p k = true <;> simp [hk, ih]

/-- Eligible keys below the cutoff are exactly the filtered eligible keys. -/
theorem eligible_filter (customers : List (Nat × Nat)) (buys : Nat → Bool) (cut : Nat) :
    ((customers.filter (fun r => decide (r.1 ≤ cut))).filter (fun r => buys r.2)).map Prod.fst =
      ((customers.filter (fun r => buys r.2)).map Prod.fst).filter (fun k => decide (k ≤ cut)) := by
  rw [List.filter_filter]
  have hswap : (customers.filter (fun a => buys a.2 && decide (a.1 ≤ cut))) =
      (customers.filter (fun a => buys a.2)).filter (fun a => decide (a.1 ≤ cut)) := by
    rw [List.filter_filter]
    apply congrArg (fun f : Nat × Nat → Bool => customers.filter f)
    funext a
    cases hp : decide (a.1 ≤ cut) <;> cases hb : buys a.2 <;> simp [hp, hb, Bool.and_comm]
  rw [hswap]
  simpa using (map_fst_filter (p := fun k : Nat => decide (k ≤ cut))
    (customers.filter (fun a => buys a.2)))

/-- The default natural-number merge order is sorted. -/
theorem sortedNat (xs : List Nat) :
    (xs.mergeSort (fun a b : Nat => decide (a ≤ b))).Pairwise
      (fun a b => decide (a ≤ b) = true) := by
  apply List.pairwise_mergeSort
  · intro a b c h1 h2
    exact decide_eq_true (Nat.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2))
  · intro a b
    rcases Nat.le_total a b with h | h <;> simp [h]

/-- Two sorted permutations of the same natural-number list are equal. -/
theorem sorted_perm_eq {l₁ l₂ : List Nat}
    (h1 : l₁.Pairwise (fun a b => decide (a ≤ b) = true))
    (h2 : l₂.Pairwise (fun a b => decide (a ≤ b) = true))
    (hp : l₁.Perm l₂) : l₁ = l₂ := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Nat => a ≤ b)
  · intro a b ha hb hab hba
    omega
  · exact h1.imp (fun h => of_decide_eq_true h)
  · exact h2.imp (fun h => of_decide_eq_true h)
  · exact hp

/-- Filtering a sorted list by a cutoff keeps exactly a prefix. -/
theorem sorted_filter_prefix (l : List Nat) (cut : Nat)
    (hsorted : l.Pairwise (fun a b => decide (a ≤ b) = true)) :
    l.filter (fun k => decide (k ≤ cut)) =
      l.take ((l.filter (fun k => decide (k ≤ cut))).length) := by
  induction l with
  | nil => simp
  | cons a t ih =>
      have hpw := List.pairwise_cons.mp hsorted
      have ht : t.Pairwise (fun a b => decide (a ≤ b) = true) := hpw.2
      have hle_all : ∀ x ∈ t, a ≤ x := by
        intro x hx
        exact of_decide_eq_true (hpw.1 x hx)
      have iht := ih ht
      by_cases hcut : a ≤ cut
      · calc
          (a :: t).filter (fun k => decide (k ≤ cut)) =
              a :: t.filter (fun k => decide (k ≤ cut)) := by simp [hcut]
          _ = a :: t.take ((t.filter (fun k => decide (k ≤ cut))).length) := by
            exact congrArg (fun l => a :: l) iht
          _ = (a :: t).take (((a :: t).filter (fun k => decide (k ≤ cut))).length) := by
            have hlen : (((a :: t).filter (fun k => decide (k ≤ cut))).length) =
                ((t.filter (fun k => decide (k ≤ cut))).length) + 1 := by simp [hcut]
            rw [hlen]
            simp
      · have hnone : t.filter (fun k => decide (k ≤ cut)) = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro x hx
          have hx' := List.mem_filter.mp hx
          have hle : a ≤ x := hle_all x hx'.1
          exact hcut (Nat.le_trans hle (of_decide_eq_true hx'.2))
        simp [hcut, hnone]

theorem sound : WitnessedGroupBoundaryBeforeExistenceProbes.Goal := by
  intro customers buys witnesses cut
  dsimp
  intro hw h100
  let eligible := fun (rs : List (Nat × Nat)) => (rs.filter (fun r => buys r.2)).map Prod.fst
  let p := fun k : Nat => decide (k ≤ cut)
  let E := eligible customers
  let F := eligible (customers.filter (fun r => r.1 ≤ cut))
  have hF : F = E.filter p := by
    exact eligible_filter customers buys cut
  have hT : F.eraseDups = E.eraseDups.filter p := by
    rw [hF]
    exact eraseDups_filter E p
  have hpairE := sortedNat E.eraseDups
  have hpairF := sortedNat F.eraseDups
  have hpairEfilt : (E.eraseDups.mergeSort.filter p).Pairwise
      (fun a b => decide (a ≤ b) = true) := List.Pairwise.filter p hpairE
  have hpairMergeFilt := sortedNat (E.eraseDups.filter p)
  have hpermFilter : (E.eraseDups.filter p).mergeSort.Perm (E.eraseDups.mergeSort.filter p) := by
    exact (List.mergeSort_perm (E.eraseDups.filter p) (fun a b : Nat => decide (a ≤ b))).trans
      ((List.mergeSort_perm E.eraseDups (fun a b : Nat => decide (a ≤ b))).filter p).symm
  have hsortFilter : (E.eraseDups.filter p).mergeSort = E.eraseDups.mergeSort.filter p :=
    sorted_perm_eq hpairMergeFilt hpairEfilt hpermFilter
  have hmergeF : F.eraseDups.mergeSort = E.eraseDups.mergeSort.filter p := by
    rw [hT, hsortFilter]
  let W := (witnesses.filter (fun k : Nat => decide (k ≤ cut))).eraseDups
  have hWlen : 100 ≤ W.length := by
    simpa [W] using h100
  have hsub : W ⊆ E.eraseDups.filter p := by
    intro k hk
    have hkw : k ∈ witnesses.filter p := List.mem_eraseDups.mp hk
    have hkwit : k ∈ witnesses := (List.mem_filter.mp hkw).1
    have hkp : p k = true := (List.mem_filter.mp hkw).2
    have hkE : k ∈ E := hw k hkwit
    exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hkE, hkp⟩
  have hmLower : 100 ≤ (E.eraseDups.mergeSort.filter p).length := by
    have hWnodup : W.Nodup := nodup_eraseDups (witnesses.filter (fun k : Nat => decide (k ≤ cut)))
    have hle := List.Nodup.length_le_of_subset hWnodup hsub
    have hlenperm : (E.eraseDups.mergeSort.filter p).length = (E.eraseDups.filter p).length :=
      ((List.mergeSort_perm E.eraseDups (fun a b : Nat => decide (a ≤ b))).filter p).length_eq
    omega
  have hprefix := sorted_filter_prefix E.eraseDups.mergeSort cut hpairE
  have htake : (E.eraseDups.mergeSort.filter p).take 100 = E.eraseDups.mergeSort.take 100 := by
    rw [hprefix]
    rw [List.take_take, Nat.min_eq_left hmLower]
  have hkeys : F.eraseDups.mergeSort.take 100 = E.eraseDups.mergeSort.take 100 := by
    rw [hmergeF, htake]
  dsimp [best]
  change (F.eraseDups.mergeSort.take 100).map (fun k => (k, F.count k)) =
    (E.eraseDups.mergeSort.take 100).map (fun k => (k, E.count k))
  rw [hkeys]
  apply List.map_congr_left
  intro k hk
  have hkFmerge : k ∈ F.eraseDups.mergeSort.take 100 := by
    rw [hkeys]
    exact hk
  have hkFdup : k ∈ F.eraseDups := List.mem_mergeSort.mp (List.mem_of_mem_take hkFmerge)
  have hkF : k ∈ F := List.mem_eraseDups.mp hkFdup
  have hkp : p k = true := (List.mem_filter.mp (by rwa [hF] at hkF)).2
  have hcount : F.count k = E.count k := by
    rw [hF]
    exact List.count_filter hkp
  simp [hcount]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_WitnessedGroupBoundaryBeforeExistenceProbes
