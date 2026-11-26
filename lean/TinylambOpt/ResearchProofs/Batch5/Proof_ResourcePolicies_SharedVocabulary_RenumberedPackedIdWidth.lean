import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_RenumberedPackedIdWidth

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch proof for the RenumberedPackedIdWidth resource-policy goal. It records the
relationship between dictionary ids and SQL-visible scan results. -/


open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem idxOf_getElem_eq {l : List Symbol} (hnd : l.Nodup) {i : Nat}
    (hi : i < l.length) : l.idxOf (l[i]) = i := by
  induction l generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      cases i with
      | zero => simp
      | succ i =>
          have hixs : i < xs.length := by simpa using hi
          have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
          have hneq : (x == xs[i]) = false := by
            rw [beq_eq_false_iff_ne]
            intro heq
            exact hxnot (heq.symm ▸ List.getElem_mem hixs)
          simp [List.idxOf_cons, hneq, ih (List.nodup_cons.mp hnd).2 hixs]

private theorem peak_ids_eq (d xs : List Symbol) (hnd : d.Nodup)
    (hcov : ∀ x ∈ d, x ∈ xs) (hall : ∀ x ∈ xs, x ∈ d) :
    peak (ids d xs) = if d.length = 0 then 0 else d.length - 1 := by
  classical
  by_cases hd : d.length = 0
  · have hdnil : d = [] := List.eq_nil_of_length_eq_zero hd
    subst d
    have hxs : xs = [] := by
      cases xs with
      | nil => rfl
      | cons x xs => exact (List.not_mem_nil (hall x List.mem_cons_self)).elim
    subst xs
    simp [peak, ids]
  · simp only [ite_eq_right hd]
    have hpos : 0 < d.length := Nat.pos_of_ne_zero hd
    have hlast : d.length - 1 < d.length := by omega
    have hmemd : d[d.length - 1] ∈ d := List.getElem_mem hlast
    have hmemxs : d[d.length - 1] ∈ xs := hcov _ hmemd
    have hidx : d.idxOf (d[d.length - 1]) = d.length - 1 :=
      idxOf_getElem_eq hnd hlast
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
        apply Nat.le_antisymm
        · apply (List.max?_le_iff hmax).2
          intro b hb
          rw [ids, List.mem_map] at hb
          rcases hb with ⟨x, hx, rfl⟩
          have hxd : x ∈ d := hall x hx
          have hlt : d.idxOf x < d.length := List.idxOf_lt_length_of_mem hxd
          omega
        · exact (List.max?_eq_some_iff.mp hmax).2 _ hmemids

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.RenumberedPackedIdWidth.Goal := by
  intro d e xs hperm hnd hcov hall
  have hnde : e.Nodup := hperm.nodup_iff.mp hnd
  have hlen : d.length = e.length := hperm.length_eq
  have hcov_e : ∀ x ∈ e, x ∈ xs := fun x hx => hcov x ((hperm.mem_iff).mpr hx)
  have hall_e : ∀ x ∈ xs, x ∈ e := fun x hx => (hperm.mem_iff).mp (hall x hx)
  have hp1 := peak_ids_eq d xs hnd hcov hall
  have hp2 := peak_ids_eq e xs hnde hcov_e hall_e
  simp only [packedSize]
  rw [show peak (ids d xs) = peak (ids e xs) by rw [hp1, hp2, hlen]]
  simp [ids]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_RenumberedPackedIdWidth
