import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary.RenumberedPackedIdWidth

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem peak_ids_eq (d xs : List Symbol) (hnd : d.Nodup)
    (hcov : ∀ x ∈ d, x ∈ xs) (hall : ∀ x ∈ xs, x ∈ d) :
    peak (ids d xs) = if d.length = 0 then 0 else d.length - 1 := by
  classical
  by_cases hd : d.length = 0
  · have hdnil : d = [] := List.eq_nil_of_length_eq_zero hd
    subst hdnil
    have hxs : xs = [] := by
      cases xs with
      | nil => rfl
      | cons x xs => exact (List.not_mem_nil (hall x List.mem_cons_self)).elim
    subst hxs
    simp [peak, ids]
  · rw [if_neg hd]
    apply Nat.le_antisymm
    · -- upper bound
      unfold peak
      cases hmax : (ids d xs).max? with
      | none => simp
      | some m =>
        simp only [Option.getD_some]
        have hm_mem : m ∈ ids d xs := List.max?_mem hmax
        rw [ids, List.mem_map] at hm_mem
        rcases hm_mem with ⟨x, hx, rfl⟩
        have hxd : x ∈ d := hall x hx
        have hlt : d.idxOf x < d.length := List.idxOf_lt_length_of_mem hxd
        omega
    · -- lower bound
      have hlast : d.length - 1 < d.length := by omega
      have hmemd : d[d.length - 1] ∈ d := List.getElem_mem hlast
      have hmemxs : d[d.length - 1] ∈ xs := hcov _ hmemd
      have hidx : d.idxOf (d[d.length - 1]) = d.length - 1 :=
        List.Nodup.idxOf_getElem hnd (d.length - 1) hlast
      have hmemids : d.length - 1 ∈ ids d xs := by
        rw [ids, List.mem_map]
        exact ⟨d[d.length - 1], hmemxs, hidx⟩
      unfold peak
      cases hmax : (ids d xs).max? with
      | none =>
        rw [List.max?_eq_none_iff] at hmax
        rw [hmax] at hmemids
        exact (List.not_mem_nil hmemids).elim
      | some m =>
        simp only [Option.getD_some]
        exact (List.max?_eq_some_iff.mp hmax).2 _ hmemids

theorem sound : RenumberedPackedIdWidth.Goal := by
  intro d e xs hperm hnd hcov hall
  have hnde : e.Nodup := hperm.nodup_iff.mp hnd
  have hlen : d.length = e.length := hperm.length_eq
  have hcov_e : ∀ x ∈ e, x ∈ xs := fun x hx => hcov x (hperm.mem_iff.mpr hx)
  have hall_e : ∀ x ∈ xs, x ∈ e := fun x hx => hperm.mem_iff.mp (hall x hx)
  have hp1 := peak_ids_eq d xs hnd hcov hall
  have hp2 := peak_ids_eq e xs hnde hcov_e hall_e
  simp only [packedSize]
  rw [show peak (ids d xs) = peak (ids e xs) by rw [hp1, hp2, hlen]]
  simp [ids]

end TinylambOpt.ResearchProofs.ResourcePolicies.SharedVocabulary.RenumberedPackedIdWidth
