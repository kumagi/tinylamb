import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

/-!
# NameDayBitmapTriplePopulation
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.NameDayBitmapTriplePopulation
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.NameDayBitmapTriplePopulation

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  match l with
  | [] => simp [List.eraseDups_nil]
  | a :: as =>
      rw [List.eraseDups_cons]
      simp only [List.nodup_cons]
      refine ⟨?_, nodup_eraseDups _⟩
      intro ha
      have hmem : a ∈ List.filter (fun b => !b == a) as := List.mem_eraseDups.mp ha
      have hb := (List.mem_filter.mp hmem).2
      simp at hb
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem eraseDups_filter_eq {α : Type} [BEq α] [LawfulBEq α] (l : List α) (p : α → Bool)
    (hl : l.Nodup) : (l.filter p).eraseDups = l.filter p := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hnd_as : as.Nodup := (List.nodup_cons.mp hl).2
      have ih' := ih hnd_as
      by_cases hp : p a = true
      · have ha_not : a ∉ as.filter p := fun ha => (List.nodup_cons.mp hl).1 (List.mem_filter.mp ha).1
        simp only [List.filter_cons, hp, ite_true]
        rw [List.eraseDups_cons]
        congr 1
        have hfilter : List.filter (fun b => !b == a) (as.filter p) = as.filter p := by
          rw [List.filter_eq_self]
          intro b hb
          have hne : b ≠ a := by intro heq; subst heq; exact ha_not hb
          simp [beq_eq_false_iff_ne.mpr hne]
        rw [hfilter, ih']
      · have hp' : p a = false := by cases h : p a <;> simp_all
        simp [List.filter_cons, hp', ih']

private theorem members_members (s c w : List Nat) :
    members (members s c) w = s.eraseDups.filter (fun d => c.contains d && w.contains d) := by
  unfold members
  rw [eraseDups_filter_eq _ _ (nodup_eraseDups s), List.filter_filter]
  congr 1
  funext d
  cases c.contains d <;> cases w.contains d <;> simp

theorem sound : Goal := by
  intro s c w n hs
  rw [members_members]
  unfold countWhere
  apply List.Perm.length_eq
  refine (List.perm_ext_iff_of_nodup ?_ ?_).mpr ?_
  · exact (List.nodup_range).filter _
  · exact (nodup_eraseDups s).filter _
  · intro d
    simp only [List.mem_filter, List.mem_range, List.mem_eraseDups, Bool.and_eq_true, List.contains_iff_mem]
    constructor
    · rintro ⟨hdr, ⟨hsd, hcd⟩, hwd⟩
      exact ⟨hsd, hcd, hwd⟩
    · rintro ⟨hds, hcd, hwd⟩
      exact ⟨hs d hds, ⟨hds, hcd⟩, hwd⟩

#print axioms sound
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.NameDayBitmapTriplePopulation
