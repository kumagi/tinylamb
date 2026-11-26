import TinylambOpt.ResearchGoals.JitKernels.FusionTraffic

/-! SOUND: Reordering the gathers of a selection (a permutation of row indices) only
permutes the resulting cache-line footprint. The line map is a function, `List.map`
respects permutations, `eraseDups` yields a duplicate-free list, and two duplicate-free
lists with the same elements are permutations of each other — so the set of compulsory
line fetches for an independent consumer is reorder-invariant. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_ProbeReorderCacheLineParity
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

/-- Permutations are preserved by `eraseDups`. -/
private theorem eraseDups_perm {l₁ l₂ : List Nat} (p : List.Perm l₁ l₂) :
    List.Perm l₁.eraseDups l₂.eraseDups := by
  refine (List.perm_ext_iff_of_nodup (nodup_eraseDups _) (nodup_eraseDups _)).mpr (fun x => ?_)
  simp [List.mem_eraseDups, p.mem_iff]

/-- Reordering a selection permutes its footprint. -/
theorem sound : ProbeReorderCacheLineParity.Goal := by
  intro base stride w a b p
  show List.Perm ((List.map (lineOf base stride w) a).eraseDups)
      ((List.map (lineOf base stride w) b).eraseDups)
  exact eraseDups_perm (List.Perm.map _ p)

end TinylambOpt.ResearchProofs.JitKernels.FusionTraffic_ProbeReorderCacheLineParity
