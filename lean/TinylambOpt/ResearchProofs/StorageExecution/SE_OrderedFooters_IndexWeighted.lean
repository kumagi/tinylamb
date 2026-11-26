import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.IndexWeighted
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open SE_ORF

def W (xs : List Nat) : Nat := (xs.zipIdx.map (fun (v,i) => i*v)).sum

theorem ref_eq (xs : List Nat) :
    IndexWeighted.agg.reference xs = (xs.length, xs.sum, W xs) := rfl

theorem join_eq (p q : Nat × Nat × Nat) :
    IndexWeighted.agg.join p q = (p.1 + q.1, p.2.1 + q.2.1, p.2.2 + q.2.2 + p.1 * q.2.1) := rfl

theorem W_append (a b : List Nat) : W (a ++ b) = W a + W b + a.length * b.sum := by
  have shift (l : List Nat) : ∀ k,
      ((l.zipIdx k).map (fun (v,i) => i*v)).sum = ((l.zipIdx).map (fun (v,i) => i*v)).sum + k * l.sum := by
    intro k
    induction l generalizing k with
    | nil => simp
    | cons y l ih =>
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
      rw [ih (k+1), ih 1]
      nring
  unfold W
  rw [List.zipIdx_append (k := 0), List.map_append, List.sum_append, shift b (0 + a.length)]
  nring

theorem W_cons (x : Nat) (l : List Nat) : W (x :: l) = W l + l.sum := by
  have := W_append [x] l
  rw [show ([x] : List Nat) ++ l = x :: l from rfl] at this
  have h1 : W ([x] : List Nat) = 0 := by simp [W]
  rw [h1, show ([x] : List Nat).length = 1 from rfl, Nat.one_mul] at this
  omega

theorem stitch_agg : stitch IndexWeighted.agg := by
  intro a b
  rw [ref_eq, ref_eq, ref_eq, join_eq]
  simp only [List.length_append, List.sum_append]
  rw [W_append]

theorem sound_Stitch : IndexWeighted.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : IndexWeighted.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : IndexWeighted.RepeatPower.Goal := repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : IndexWeighted.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : IndexWeighted.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : IndexWeighted.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : IndexWeighted.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : IndexWeighted.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

theorem sum_set (r : List Nat) : ∀ (k : Nat) (x : Nat), k < r.length →
    (r.set k x).sum + r[k]?.getD 0 = r.sum + x := by
  induction r with
  | nil => intro k x h; exact absurd h (by simp)
  | cons y l ih =>
    intro k x h
    cases k with
    | zero =>
      rw [List.set_cons_zero]
      have hg : (y :: l)[0]?.getD 0 = y := by simp
      rw [hg, List.sum_cons, List.sum_cons]
      omega
    | succ k' =>
      rw [List.set_cons_succ]
      have hg : (y :: l)[k'.succ]?.getD 0 = l[k']?.getD 0 := by simp
      rw [hg, List.sum_cons, List.sum_cons]
      specialize ih k' x (by simpa using h)
      omega

theorem sound_ReplaceAt : IndexWeighted.ReplaceAt.Goal := by
  intro xs
  induction xs with
  | nil => intro i x h; exact absurd h (by simp)
  | cons y l ih =>
    intro i x h
    cases i with
    | zero =>
      simp only [ref_eq, List.set_cons_zero]
      rw [W_cons, W_cons]
      omega
    | succ k =>
      have hk : k < l.length := by simpa using h
      have hi := ih k x hk
      simp only [ref_eq] at hi
      have hs := sum_set l k x hk
      have hget : (y :: l)[k.succ]?.getD 0 = l[k]?.getD 0 := by simp
      simp only [ref_eq, List.set_cons_succ, hget, Nat.add_mul, Nat.one_mul]
      rw [W_cons, W_cons]
      omega

theorem sound_ReverseMoment : IndexWeighted.ReverseMoment.Goal := by
  intro xs
  induction xs with
  | nil => simp [ref_eq, W]
  | cons y l ih =>
    have hi := ih
    simp only [ref_eq] at hi
    rw [Nat.sub_mul, Nat.one_mul] at hi
    rw [List.reverse_cons]
    simp only [ref_eq, List.length_cons, List.sum_cons, Nat.add_sub_cancel]
    have h2 : W ([y] : List Nat) = 0 := by simp [W]
    have h3 : ([y] : List Nat).sum = y := by simp
    have hW : W (y :: l) = W l + l.sum := W_cons y l
    rw [W_append, h2, h3, List.length_reverse, hW]
    simp only [Nat.add_zero, Nat.zero_add, Nat.mul_add]
    have hd : l.sum ≤ l.length * l.sum := by
      cases l with
      | nil => simp
      | cons z zs => exact Nat.le_mul_of_pos_left _ (by simp)
    omega

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.IndexWeighted
