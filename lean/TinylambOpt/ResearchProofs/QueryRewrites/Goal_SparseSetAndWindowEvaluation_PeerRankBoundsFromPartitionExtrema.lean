import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# PeerRankBoundsFromPartitionExtrema
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_PeerRankBoundsFromPartitionExtrema
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.PeerRankBoundsFromPartitionExtrema

private theorem le_max?_of_mem {l : List Nat} {x m : Nat} (hm : l.max? = some m) (hx : x ∈ l) : x ≤ m := by
  induction l generalizing m with
  | nil => cases hx
  | cons a as ih =>
      simp only [List.max?_cons] at hm
      cases h : as.max? with
      | none =>
          simp [h] at hm
          subst hm
          simp at hx
          rcases hx with rfl | hx
          · exact Nat.le_refl _
          · have : as = [] := by cases as with | nil => rfl | cons _ _ => simp at h
            subst this; simp at hx
      | some b =>
          simp [h] at hm
          subst hm
          simp at hx
          rcases hx with rfl | hx
          · exact Nat.le_max_left _ _
          · exact Nat.le_trans (ih h hx) (Nat.le_max_right _ _)

private theorem min?_le_of_mem {l : List Nat} {x m : Nat} (hm : l.min? = some m) (hx : x ∈ l) : m ≤ x := by
  induction l generalizing m with
  | nil => cases hx
  | cons a as ih =>
      simp only [List.min?_cons] at hm
      cases h : as.min? with
      | none =>
          simp [h] at hm
          subst hm
          simp at hx
          rcases hx with rfl | hx
          · exact Nat.le_refl _
          · have : as = [] := by cases as with | nil => rfl | cons _ _ => simp at h
            subst this; simp at hx
      | some b =>
          simp [h] at hm
          subst hm
          simp at hx
          rcases hx with rfl | hx
          · exact Nat.min_le_left _ _
          · exact Nat.le_trans (Nat.min_le_right _ _) (ih h hx)

private theorem smaller_eq_countWhere (p : List Nat) (probe : Nat) :
    (if p.max?.any (· < probe) then p.length
     else if p.min?.any (probe ≤ ·) then 0
     else countWhere p (· < probe)) = countWhere p (· < probe) := by
  split
  · rename_i hmax
    have hall : ∀ x ∈ p, x < probe := by
      intro x hx
      simp only [Option.any_eq_true, decide_eq_true_eq] at hmax
      rcases hmax with ⟨m, hm, hlt⟩
      have hxle : x ≤ m := le_max?_of_mem hm hx
      omega
    unfold countWhere
    have : List.filter (fun x => decide (x < probe)) p = p := by
      apply List.filter_eq_self.mpr
      intro x hx
      simp [hall x hx]
    simp [this]
  · split
    · rename_i _ hmin2
      have hall : ∀ x ∈ p, ¬ x < probe := by
        intro x hx hlt
        simp only [Option.any_eq_true, decide_eq_true_eq] at hmin2
        rcases hmin2 with ⟨m, hm, hle⟩
        have hmx : m ≤ x := min?_le_of_mem hm hx
        omega
      unfold countWhere
      have hemp : List.filter (fun x => decide (x < probe)) p = [] := by
        rw [List.filter_eq_nil_iff]
        intro x hx
        simp [hall x hx]
      simp [hemp]
    · rfl

theorem sound : Goal := by
  intro pages probe
  dsimp only
  have h : ∀ ps : List (List Nat),
      (ps.map (fun p => if p.max?.any (· < probe) then p.length
        else if p.min?.any (probe ≤ ·) then 0 else countWhere p (· < probe))).sum
      = countWhere ps.flatten (· < probe) := by
    intro ps
    induction ps with
    | nil => simp [countWhere]
    | cons p ps ih =>
        simp only [List.map_cons, List.sum_cons, List.flatten_cons]
        rw [smaller_eq_countWhere, ih]
        unfold countWhere
        simp [List.filter_append]
  rw [h pages]

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_PeerRankBoundsFromPartitionExtrema
