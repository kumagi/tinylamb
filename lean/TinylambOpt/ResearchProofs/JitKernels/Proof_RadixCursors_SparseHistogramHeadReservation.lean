import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
The reservation below a bucket cutoff can be computed from only the bucket IDs
that actually occur: summing each distinct bucket's histogram gives the same
count as scanning all live lanes. This avoids iterating over absent bucket IDs
when a SQL scatter kernel reserves its output window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace Nov26.Proof_RadixCursors_SparseHistogramHeadReservation

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors
open TinylambOpt.ResearchGoals.JitKernels.RadixCursors.SparseHistogramHeadReservation

theorem count_filter_ne_add (l : List Nat) (a cutoff : Nat) (ha : a < cutoff) :
    l.countP (fun x => x < cutoff) =
      l.count a + (l.filter (fun b => !b == a)).countP (fun x => x < cutoff) := by
  induction l with
  | nil => simp
  | cons b l ih =>
      simp only [List.countP_cons, List.count_cons, List.filter_cons]
      by_cases hba : b == a
      · have hbeq : b = a := by simpa using hba
        subst b
        simp [ha]
        omega
      · simp [hba]
        rw [ih]
        simp [List.countP_cons]
        omega

theorem countP_filter_ne_of_not (l : List Nat) (a cutoff : Nat) (ha : ¬ a < cutoff) :
    l.countP (fun x => x < cutoff) =
      (l.filter (fun b => !b == a)).countP (fun x => x < cutoff) := by
  induction l with
  | nil => simp
  | cons b l ih =>
      simp only [List.countP_cons, List.filter_cons]
      by_cases hba : b == a
      · have hbeq : b = a := by simpa using hba
        subst b
        simp [ha]
        exact ih
      · simp [hba]
        rw [ih]
        simp [List.countP_cons]

theorem countP_eq_sum_counts (l : List Nat) (cutoff : Nat) :
    l.countP (fun x => x < cutoff) =
      ((l.eraseDups.filter (fun x => x < cutoff)).map (fun k => l.count k)).sum := by
  cases l with
  | nil => simp
  | cons a l =>
      rw [List.eraseDups_cons]
      let lf := l.filter (fun b => !b == a)
      have hsum_ne : ∀ k ∈ lf.eraseDups, (a :: l).count k = lf.count k := by
        intro k hk
        have hkin : k ∈ lf := List.mem_eraseDups.mp hk
        have hkin' : k ∈ l.filter (fun b => !b == a) := by simpa [lf] using hkin
        have hkne : (!k == a) = true := by
          have hp := (List.mem_filter.mp hkin').2
          simpa using hp
        have hne : k ≠ a := by
          intro h
          subst k
          simp at hkne
        have hcfl : lf.count k = l.count k := by
          dsimp [lf]
          exact List.count_filter (p := fun b => !b == a) (a := k) hkne
        have hne' : a ≠ k := fun h => hne h.symm
        simp [hne', hcfl]
      have hmap : ((lf.eraseDups.filter (fun x => x < cutoff)).map (fun k => (a :: l).count k)) =
          (lf.eraseDups.filter (fun x => x < cutoff)).map (fun k => lf.count k) := by
        apply List.map_congr_left
        intro k hk
        exact hsum_ne k (by simpa using (List.mem_filter.mp hk).1)
      have h2 := countP_eq_sum_counts lf cutoff
      by_cases hacut : a < cutoff
      · simp [hacut]
        rw [hmap]
        have h1 := count_filter_ne_add l a cutoff hacut
        change l.countP (fun x => x < cutoff) = l.count a + lf.countP (fun x => x < cutoff) at h1
        omega
      · simp [hacut]
        rw [hmap]
        have h1 := countP_filter_ne_of_not l a cutoff hacut
        change l.countP (fun x => x < cutoff) = lf.countP (fun x => x < cutoff) at h1
        omega
termination_by l.length
decreasing_by
  dsimp [lf]
  have hle := List.length_filter_le (fun b => !b == a) l
  omega

theorem sound : Goal := by
  intro xs cutoff
  unfold base
  exact countP_eq_sum_counts (active xs) cutoff

end Nov26.Proof_RadixCursors_SparseHistogramHeadReservation
