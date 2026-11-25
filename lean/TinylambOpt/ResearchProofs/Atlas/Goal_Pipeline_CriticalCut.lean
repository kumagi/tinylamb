import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_CriticalCut

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline Pipeline

abbrev Job := Nat × Nat

private def sumS (xs : List Job) : Nat := (xs.map Prod.snd).sum

/-- The candidate makespan terms: for each cut position `i`, the read prefix
    through `i` plus the evaluation suffix starting at `i`. -/
private def cutTerm (xs : List Job) (i : Nat) : Nat :=
  ((xs.take (i+1)).map Prod.fst).sum + ((xs.drop i).map Prod.snd).sum

private def cutMax (xs : List Job) : Nat :=
  (xs.zipIdx.map (fun (_, i) => cutTerm xs i)).max?.getD 0

/-- `zipIdx` started one position later is the successor map of `zipIdx`. -/
private theorem zipIdx_succ (i : Nat) (xs : List Job) :
    xs.zipIdx (i+1) = (xs.zipIdx i).map (fun (a, j) => (a, j+1)) := by
  induction xs generalizing i with
  | nil => simp [List.zipIdx]
  | cons a as ih =>
      simp [List.zipIdx_cons, ih, Nat.add_assoc]

/-- Advancing the cut by one job adds that job's read cost to the term. -/
private theorem cutTerm_cons (p s : Nat) (rest : List Job) (i : Nat) :
    cutTerm ((p,s)::rest) (i+1) = p + cutTerm rest i := by
  simp [cutTerm, Nat.add_assoc]

private theorem max?_cons_getD (x : Nat) (xs : List Nat) :
    (x :: xs).max?.getD 0 = max x (xs.max?.getD 0) := by
  rw [List.max?_cons]
  cases xs.max? with
  | none => simp
  | some m => simp

/-- Adding a constant to every element of a list adds it to the maximum,
    provided the head already dominates that constant. -/
private theorem max?_map_add_aux (p : Nat) : ∀ (a : Nat) (l : List Nat), p ≤ a →
    (a :: l.map (fun v => p + v)).max?.getD 0 = max a (p + l.max?.getD 0) := by
  intro a l hle
  induction l generalizing a with
  | nil =>
      simp [Nat.max_eq_left hle]
  | cons v vs ih =>
      rw [List.map_cons, max?_cons_getD]
      rw [ih (p+v) (by omega)]
      rw [max?_cons_getD]
      omega

/-- The cut maximum satisfies the same one-job recurrence as the pipeline:
    either the first job is the cut, or its read cost is added to the rest. -/
private theorem cutMax_cons (p s : Nat) (rest : List Job) :
    cutMax ((p,s)::rest) = max (p+s+sumS rest) (p + cutMax rest) := by
  unfold cutMax
  rw [List.zipIdx_cons]
  simp only [List.map_cons]
  have htail : (rest.zipIdx 1).map (fun x => cutTerm ((p,s)::rest) x.2) =
               (rest.zipIdx 0).map (fun x => p + cutTerm rest x.2) := by
    rw [show (1:Nat) = 0+1 by omega, zipIdx_succ]
    rw [List.map_map]
    apply List.map_congr_left
    intro x hx
    rcases x with ⟨a,j⟩
    simp [cutTerm_cons]
  rw [htail]
  have hzero : cutTerm ((p,s)::rest) 0 = p + s + sumS rest := by
    simp [cutTerm, sumS]
    omega
  rw [hzero, max?_cons_getD]
  have hm := max?_map_add_aux p (p+s+sumS rest)
      ((rest.zipIdx 0).map (fun x => cutTerm rest x.2)) (by omega)
  rw [max?_cons_getD] at hm
  simpa [cutMax, Function.comp_def] using hm

/-- Closed form for running the pipeline from an already synchronized state. -/
private theorem finishFrom : ∀ (xs : List Job) (c₁ c₂ : Nat), c₁ ≤ c₂ →
    (xs.foldl step (c₁,c₂)).2 = max (c₂ + sumS xs) (c₁ + cutMax xs) := by
  intro xs
  induction xs with
  | nil =>
      intro c₁ c₂ h
      simp [sumS, cutMax, Nat.max_eq_left h]
  | cons j rest ih =>
      intro c₁ c₂ h
      rcases j with ⟨p,s⟩
      simp only [List.foldl_cons]
      dsimp [step]
      simp only [sumS, List.map_cons, List.sum_cons]
      let c₁' := c₁ + p
      let c₂' := max c₁' c₂ + s
      have h' : c₁' ≤ c₂' := by dsimp [c₁', c₂']; omega
      have ih' := ih c₁' c₂' h'
      change (rest.foldl step (c₁', c₂')).2 =
        max (c₂ + (s + sumS rest)) (c₁ + cutMax ((p,s)::rest))
      rw [ih', cutMax_cons]
      dsimp [c₁', c₂']
      omega

private theorem sumS_le_cutMax : ∀ xs : List Job, sumS xs ≤ cutMax xs
  | [] => by simp [sumS, cutMax]
  | (p,s)::rest => by
      rw [cutMax_cons]
      simp [sumS]
      omega

theorem sound_Pipeline_CriticalCut : CriticalCut.Goal := by
  intro xs
  have h := finishFrom xs 0 0 (by omega)
  unfold finish
  rw [h]
  simpa [cutMax, cutTerm] using Nat.max_eq_right (sumS_le_cutMax xs)

end TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_CriticalCut
