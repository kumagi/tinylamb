import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReweightBound

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

private theorem length_weights_pos (t : Tree) : 0 < (weights t).length := by
  induction t with
  | leaf w => simp [weights]
  | fork l r ihl ihr =>
      simp [weights]
      omega

private theorem min?_getD_le {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    xs.min?.getD 0 ≤ x := by
  cases hm : xs.min? with
  | none =>
      have : xs = [] := (List.min?_eq_none_iff).mp hm
      simp [this] at hx
  | some a =>
      have ha := (List.min?_eq_some_iff).mp hm
      have hax : a ≤ x := ha.2 x hx
      simpa [hm] using hax

-- The interval DP, with any fuel at least the number of internal nodes needed,
-- never exceeds the write work of a concrete tree with those weights.
private theorem dp_le_work (t : Tree) (f : Nat)
    (hf : (weights t).length - 1 ≤ f) : dp f (weights t) ≤ work t := by
  induction t generalizing f with
  | leaf w =>
      cases f <;> simp [weights, dp, work]
  | fork l r ihl ihr =>
      have hwl : 0 < (weights l).length := length_weights_pos l
      have hwr : 0 < (weights r).length := length_weights_pos r
      cases f with
      | zero =>
          simp [weights] at hf
          omega
      | succ f' =>
          have hf_l : (weights l).length - 1 ≤ f' := by
            simp [weights] at hf
            omega
          have hf_r : (weights r).length - 1 ≤ f' := by
            simp [weights] at hf
            omega
          have ihl' : dp f' (weights l) ≤ work l := ihl f' hf_l
          have ihr' : dp f' (weights r) ≤ work r := ihr f' hf_r
          have hnot : ¬ (weights l ++ weights r).length ≤ 1 := by
            simp only [List.length_append]
            omega
          simp only [dp, weights, work, hnot, ↓reduceIte, List.sum_append]
          have hidx : (weights l).length - 1 ∈
              List.range ((weights l ++ weights r).length - 1) := by
            simp [List.mem_range]
            omega
          have hj1 : (weights l).length - 1 + 1 = (weights l).length := by omega
          have htake : (weights l ++ weights r).take ((weights l).length - 1 + 1) =
              weights l := by
            rw [hj1, List.take_left]
          have hdrop : (weights l ++ weights r).drop ((weights l).length - 1 + 1) =
              weights r := by
            rw [hj1, List.drop_left]
          let cand : List Nat :=
            (List.range ((weights l ++ weights r).length - 1)).map
              (fun i => dp f' ((weights l ++ weights r).take (i + 1)) +
                dp f' ((weights l ++ weights r).drop (i + 1)))
          have hmem : dp f' (weights l) + dp f' (weights r) ∈ cand := by
            dsimp [cand]
            rw [List.mem_map]
            refine ⟨(weights l).length - 1, hidx, ?_⟩
            simp [htake, hdrop]
          have hmin : cand.min?.getD 0 ≤ dp f' (weights l) + dp f' (weights r) :=
            min?_getD_le hmem
          dsimp [cand] at hmin
          omega

private theorem sum_modify_add (ws : List Nat) (i d : Nat) (hi : i < ws.length) :
    (ws.modify i (fun x => x + d)).sum = ws.sum + d := by
  induction ws generalizing i with
  | nil => simp at hi
  | cons w ws ih =>
      rw [List.modify_cons]
      split
      · rename_i h
        subst i
        simp
        omega
      · rename_i h
        have hi' : i - 1 < ws.length := by
          simp only [List.length_cons] at hi
          omega
        change w + (ws.modify (i - 1) (fun x => x + d)).sum = w + ws.sum + d
        rw [ih (i - 1) hi']
        rw [Nat.add_assoc]

private theorem modify_append_left (l r : List Nat) (i : Nat) (f : Nat → Nat)
    (hi : i < l.length) : (l ++ r).modify i f = l.modify i f ++ r := by
  induction l generalizing i with
  | nil => simp at hi
  | cons a l ih =>
      rw [List.modify_cons]
      split
      · rename_i h
        subst i
        simp [List.modify_cons]
      · rename_i h
        have hi' : i - 1 < l.length := by
          simp only [List.length_cons] at hi
          omega
        simp [List.modify_cons, h, ih (i - 1) hi']

private theorem modify_append_right (l r : List Nat) (j : Nat) (f : Nat → Nat) :
    (l ++ r).modify (l.length + j) f = l ++ r.modify j f := by
  induction l generalizing j with
  | nil => simp
  | cons a l ih =>
      simp only [List.length_cons, List.cons_append]
      rw [List.modify_cons]
      split
      · rename_i h
        omega
      · rename_i h
        have hidx : l.length + 1 + j - 1 = l.length + j := by omega
        rw [hidx]
        rw [ih]

private theorem modify_append_right' (l r : List Nat) (i : Nat) (f : Nat → Nat)
    (hi : l.length ≤ i) : (l ++ r).modify i f = l ++ r.modify (i - l.length) f := by
  have h := modify_append_right l r (i - l.length) f
  have heq : l.length + (i - l.length) = i := by omega
  simpa [heq] using h

private def bumpTree (t : Tree) (i d : Nat) : Tree :=
  match t with
  | .leaf w => .leaf (if i = 0 then w + d else w)
  | .fork l r =>
      if i < (weights l).length then .fork (bumpTree l i d) r
      else .fork l (bumpTree r (i - (weights l).length) d)

private theorem weights_bumpTree (t : Tree) (i d : Nat) (hi : i < (weights t).length) :
    weights (bumpTree t i d) = (weights t).modify i (fun x => x + d) := by
  induction t generalizing i d with
  | leaf w =>
      have hi0 : i = 0 := by simpa [weights] using hi
      simp [bumpTree, weights, hi0]
  | fork l r ihl ihr =>
      have hwl : 0 < (weights l).length := length_weights_pos l
      have hwr : 0 < (weights r).length := length_weights_pos r
      simp only [weights, bumpTree]
      simp only [weights] at hi
      split
      · rename_i h
        simp only [weights]
        have hil : i < (weights l).length := h
        rw [ihl i d hil]
        rw [modify_append_left (weights l) (weights r) i _ hil]
      · rename_i h
        simp only [weights]
        have hge : (weights l).length ≤ i := by omega
        have hir : i - (weights l).length < (weights r).length := by
          have hlen : (weights l ++ weights r).length =
              (weights l).length + (weights r).length := by simp
          omega
        rw [ihr (i - (weights l).length) d hir]
        rw [modify_append_right' (weights l) (weights r) i _ hge]

private theorem work_bumpTree (t : Tree) (i d : Nat) (hi : i < (weights t).length) :
    work (bumpTree t i d) = work t + d * depth t i := by
  induction t generalizing i d with
  | leaf w =>
      have hi0 : i = 0 := by simpa [weights] using hi
      simp [bumpTree, work, depth, hi0]
  | fork l r ihl ihr =>
      have hwl : 0 < (weights l).length := length_weights_pos l
      have hwr : 0 < (weights r).length := length_weights_pos r
      simp only [weights] at hi
      simp only [bumpTree]
      split
      · rename_i h
        simp only [work, depth]
        have hil : i < (weights l).length := h
        rw [ihl i d hil]
        have hwsum : (weights (bumpTree l i d)).sum = (weights l).sum + d := by
          rw [weights_bumpTree l i d hil]
          exact sum_modify_add (weights l) i d hil
        rw [hwsum]
        simp [h, Nat.mul_add]
        omega
      · rename_i h
        simp only [work, depth]
        have hir : i - (weights l).length < (weights r).length := by
          have hlen : (weights l ++ weights r).length =
              (weights l).length + (weights r).length := by simp
          omega
        rw [ihr (i - (weights l).length) d hir]
        have hwsum : (weights (bumpTree r (i - (weights l).length) d)).sum =
            (weights r).sum + d := by
          rw [weights_bumpTree r (i - (weights l).length) d hir]
          exact sum_modify_add (weights r) (i - (weights l).length) d hir
        rw [hwsum]
        simp [h, Nat.mul_add]
        omega

theorem sound_ReweightBound : AlphabeticMerge.ReweightBound.Goal := by
  intro t i d hi _hopt
  let t' := bumpTree t i d
  have hle : best (weights t') ≤ work t' := by
    simpa [best] using (dp_le_work t' (weights t').length (by omega))
  rw [weights_bumpTree t i d hi] at hle
  rw [work_bumpTree t i d hi] at hle
  exact hle

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_ReweightBound
