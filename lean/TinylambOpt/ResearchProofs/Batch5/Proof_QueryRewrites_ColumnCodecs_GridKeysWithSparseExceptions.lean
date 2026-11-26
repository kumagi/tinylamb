import TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.ColumnCodecs.Goal_GridKeysWithSparseExceptions

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs


/-!
# GridKeysWithSparseExceptions

A predicted product-warehouse grid plus position-indexed exceptions reconstructs the
original key sequence when the warehouse count is positive. The inventory scan can derive
regular keys from the grid and consult only the sparse exception list, preserving every
key observed by a full column expansion.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace GridCodecGoalHelpers

open TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

def pick (g : Nat → Nat × Nat) (o : Option (Nat × Nat)) (i : Nat) : Option (Nat × Nat) :=
  match o with
  | some v => if v != g i then some v else none
  | none => none

theorem Z (g : Nat → Nat × Nat) : ∀ (l : List (Nat × Nat)) (k i : Nat),
    (((l.zipIdx k).filter (fun (v,j) => v != g j)).find? (fun e => e.2 == i)).map Prod.fst
      = if k ≤ i then pick g l[i-k]? i else none := by
  intro l
  induction l with
  | nil => intro k i; simp [pick]
  | cons a l ih =>
    intro k i
    simp only [List.zipIdx_cons, List.filter_cons]
    rcases Nat.lt_trichotomy i k with h | h | h
    · have h1 : ¬ k ≤ i := by omega
      have h2 : ¬ k + 1 ≤ i := by omega
      have := ih (k+1) i
      have hk : (k == i) = false := by simp; omega
      by_cases hA : (a != g k) = true
      · simp [hA, hk, h1]
        simpa [h2] using this
      · simp [hA, h1]
        simpa [h2] using this
    · subst h
      have := ih (i+1) i
      have h2 : ¬ i + 1 ≤ i := by omega
      by_cases hA : (a != g i) = true
      · simp [hA, pick]
      · simp [hA, pick]
        simpa [h2] using this
    · obtain ⟨d, rfl⟩ : ∃ d, i = k + 1 + d := ⟨i - (k+1), by omega⟩
      have e1 : k + 1 + d - k = d + 1 := by omega
      have e2 : k + 1 + d - (k+1) = d := by omega
      have hk : (k == k + 1 + d) = false := by simp; omega
      have := ih (k+1) (k+1+d)
      rw [e2] at this
      have h3 : k ≤ k + 1 + d := by omega
      have h4 : k + 1 ≤ k + 1 + d := by omega
      by_cases hA : (a != g k) = true
      · simp [hA, hk, h3, e1]
        simpa [h4] using this
      · simp [hA, h3, e1]
        simpa [h4] using this

end GridCodecGoalHelpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs.GridKeysWithSparseExceptions.Goal := by
  intro rows start wh hw
  dsimp only
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp at h1
    simp only [List.getElem_map, List.getElem_range]
    have hz := GridCodecGoalHelpers.Z (fun i => ((start+i)/wh, (start+i)%wh)) rows 0 i
    simp only [Nat.zero_le, ite_true, Nat.sub_zero] at hz
    rw [hz, List.getElem?_eq_getElem h1]
    simp only [GridCodecGoalHelpers.pick]
    by_cases hb : (rows[i] != ((start+i)/wh, (start+i)%wh)) = true
    · simp [hb]
    · simp [hb]
      simp at hb
      exact hb.symm
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.ColumnCodecs.Goal_GridKeysWithSparseExceptions
