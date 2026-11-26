import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_SharedDictionaryOverlapCharge

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch proof for the SharedDictionaryOverlapCharge goal. Deduplicating and
partitioning dictionaries charges each shared symbol once in the merged group and
once as overlap, so the byte accounting is exact for SQL scans of either group. -/
open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

theorem nodup_eraseDups : ∀ l : List Symbol, l.eraseDups.Nodup
  | [] => by simp
  | x :: xs => by
      rw [List.eraseDups_cons]
      refine List.nodup_cons.mpr ⟨?_, ?_⟩
      · simp [List.mem_filter]
      · exact nodup_eraseDups (xs.filter fun b => !(b == x))
termination_by l => l.length
decreasing_by simpa using Nat.lt_succ_of_le (List.length_filter_le (fun b : Symbol => !(b == x)) xs)

theorem nodup_filter {p : Symbol → Bool} {l : List Symbol} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · rw [List.filter_cons_of_pos hp]
        apply List.nodup_cons.mpr
        constructor
        · intro hx
          exact (List.nodup_cons.mp h).1 (List.mem_filter.mp hx).1
        · exact ih (List.nodup_cons.mp h).2
      · rw [List.filter_cons_of_neg hp]
        exact ih (List.nodup_cons.mp h).2

theorem dictSize_append (u v : List Symbol) :
    dictSize (u ++ v) = dictSize u + dictSize v := by
  simp [dictSize]

theorem dictSize_perm {u v : List Symbol} (p : u.Perm v) : dictSize u = dictSize v := by
  unfold dictSize
  have pm := p.map (fun s : Symbol => ubytes s.length + s.length)
  exact pm.foldr_eq' (by intro x hx y hy z; exact Nat.add_left_comm y x z) 0

theorem dictSize_filter_partition (p : Symbol → Bool) (l : List Symbol) :
    dictSize (l.filter p) + dictSize (l.filter (fun s => !p s)) = dictSize l := by
  induction l with
  | nil => simp [dictSize]
  | cons x xs ih =>
      by_cases hp : p x = true
      · have hpneg : ¬ ((fun s : Symbol => !p s) x = true) := by simp [hp]
        rw [List.filter_cons_of_pos hp]
        rw [List.filter_cons_of_neg (p := fun s : Symbol => !p s) hpneg]
        simp [dictSize] at ih ⊢
        omega
      · have hp' : p x = false := Bool.eq_false_iff.mpr hp
        have hpneg : ((fun s : Symbol => !p s) x = true) := by simp [hp']
        rw [List.filter_cons_of_neg hp]
        rw [List.filter_cons_of_pos (p := fun s : Symbol => !p s) hpneg]
        simp [dictSize] at ih ⊢
        omega

theorem dictSize_removeAll_eraseDups (b a : List Symbol) :
    dictSize ((b.removeAll a).eraseDups) =
      dictSize ((vocab b).filter (fun s => !(vocab a).contains s)) := by
  apply dictSize_perm
  apply (List.perm_ext_iff_of_nodup (nodup_eraseDups _) (nodup_filter (nodup_eraseDups b))).2
  intro s
  simp [vocab, List.removeAll]

theorem overlap_symm (a b : List Symbol) :
    dictSize ((vocab a).filter (fun s => (vocab b).contains s)) =
      dictSize ((vocab b).filter (fun s => (vocab a).contains s)) := by
  apply dictSize_perm
  apply (List.perm_ext_iff_of_nodup
    (nodup_filter (nodup_eraseDups a)) (nodup_filter (nodup_eraseDups b))).2
  intro s
  simp [vocab, and_comm]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.SharedDictionaryOverlapCharge.Goal := by
  intro a b
  rw [show vocab (a ++ b) = vocab a ++ (b.removeAll a).eraseDups by simp [vocab, List.eraseDups_append]]
  rw [dictSize_append, dictSize_removeAll_eraseDups, overlap_symm]
  have hpart := dictSize_filter_partition (fun s : Symbol => (vocab a).contains s) (vocab b)
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_SharedDictionaryOverlapCharge
