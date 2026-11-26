import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-! SOUND: A contiguous packet of `n` byte rows (stride 1) occupies at most
`(n + w − 1)/w + 1` lines of width `w`. The touched lines form one consecutive run from
`base / w` up to `(base + n − 1)/w`; with `base = w*q + r`, `r < w`, the run has length
`(r + n − 1)/w + 1 ≤ (n + w − 1)/w + 1` since `r ≤ w − 1`. The duplicate-free footprint is
a subset of that run, so it is no longer than the run. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_BroadwordLoadFootprintBound
open TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-- `eraseDups` of any list is duplicate-free. -/
private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  have key : ∀ n : Nat, ∀ l : List Nat, l.length ≤ n → l.eraseDups.Nodup := by
    intro n
    induction n with
    | zero =>
        intro m hle
        match m with
        | [] => exact List.Pairwise.nil
        | a :: as =>
          rw [List.length_cons] at hle
          exact absurd hle (by omega)
    | succ k ih =>
        intro m hle
        match m with
        | [] => exact List.Pairwise.nil
        | a :: as =>
          have has : as.length ≤ k := by rw [List.length_cons] at hle; omega
          rw [List.eraseDups_cons, List.nodup_cons]
          refine ⟨fun hx => ?_,
            ih (as.filter fun b => !(b == a)) (Nat.le_trans (List.length_filter_le _ as) has)⟩
          rw [List.mem_eraseDups, List.mem_filter] at hx
          obtain ⟨_, hp⟩ := hx
          rw [Nat.not_beq_eq_true_eq] at hp
          exact hp rfl
  exact key l.length l (Nat.le_refl l.length)

/-- Footprint of a contiguous packet is bounded by an explicit consecutive run. -/
theorem sound : BroadwordLoadFootprintBound.Goal := by
  intro base n w hw
  obtain ⟨q, r, hbase, hr⟩ : ∃ q r, base = w * q + r ∧ r < w :=
    ⟨base / w, base % w, (Nat.div_add_mod base w).symm, Nat.mod_lt base hw⟩
  cases n with
  | zero =>
    show ((List.map (lineOf base 1 w) (List.range 0)).eraseDups).length ≤ (0 + w - 1) / w + 1
    rw [List.range_zero, List.map_nil, List.eraseDups_nil, List.length_nil]
    exact Nat.zero_le _
  | succ n =>
    have hsub : footprint base 1 w (List.range (n + 1))
        ⊆ List.map (q + ·) (List.range ((r + n) / w + 1)) := by
      intro x hx
      rw [footprint, List.mem_eraseDups, List.mem_map] at hx
      obtain ⟨i, hi, rfl⟩ := hx
      rw [List.mem_range] at hi
      have hdiv : lineOf base 1 w i = q + (r + i) / w := by
        show (base + 1 * i) / w = q + (r + i) / w
        rw [Nat.one_mul, hbase,
          show (w * q + r) + i = (r + i) + w * q from by omega,
          Nat.add_mul_div_left _ _ hw]
        omega
      rw [List.mem_map]
      refine ⟨(r + i) / w, List.mem_range.mpr (Nat.lt_succ_of_le (Nat.div_le_div_right ?_)), ?_⟩
      · omega
      · exact hdiv.symm
    calc (footprint base 1 w (List.range (n + 1))).length
        ≤ (List.map (q + ·) (List.range ((r + n) / w + 1))).length :=
          List.Nodup.length_le_of_subset (nodup_eraseDups _) hsub
      _ = (r + n) / w + 1 := by rw [List.length_map, List.length_range]
      _ ≤ (n + 1 + w - 1) / w + 1 := by
          refine Nat.add_le_add_right (Nat.div_le_div_right ?_) 1
          omega

end TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_BroadwordLoadFootprintBound
