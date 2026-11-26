import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common
set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.BitOccupancy
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open SE_ORF

def step (a x : Nat) : Nat := a ||| 2^(x%64)
def B (xs : List Nat) : Nat := xs.foldl step 0

theorem ref_eq (xs : List Nat) : BitOccupancy.agg.reference xs = (xs.length, B xs) := rfl
theorem join_eq (p q : Nat × Nat) : BitOccupancy.agg.join p q = (p.1 + q.1, p.2 ||| q.2) := rfl

theorem foldl_or : ∀ (acc : Nat) (l : List Nat), l.foldl step acc = acc ||| B l := by
  intro acc l
  induction l generalizing acc with
  | nil => simp [B]
  | cons y l ih =>
    show l.foldl step (step acc y) = acc ||| l.foldl step (step 0 y)
    simp only [step, Nat.zero_or]
    rw [ih (acc ||| 2^(y%64)), ih (2^(y%64)), Nat.or_assoc]

theorem B_cons (y : Nat) (l : List Nat) : B (y :: l) = 2^(y%64) ||| B l := by
  show l.foldl step (step 0 y) = _
  simp only [step, Nat.zero_or]
  rw [foldl_or]

theorem B_append (a b : List Nat) : B (a ++ b) = B a ||| B b := by
  show (a ++ b).foldl step 0 = _
  rw [List.foldl_append, foldl_or, ← B]

theorem bit_iff_any (xs : List Nat) (j : Nat) :
    (B xs).testBit j = xs.any (fun x => x % 64 == j) := by
  induction xs with
  | nil => simp [B]
  | cons y l ih =>
    rw [B_cons, Nat.testBit_or, Nat.testBit_two_pow, List.any_cons, ih]
    rfl

theorem stitch_agg : stitch BitOccupancy.agg := by
  intro a b
  rw [ref_eq, ref_eq, ref_eq, join_eq, List.length_append, B_append]

theorem sound_Stitch : BitOccupancy.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : BitOccupancy.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : BitOccupancy.RepeatPower.Goal := repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : BitOccupancy.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : BitOccupancy.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : BitOccupancy.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : BitOccupancy.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : BitOccupancy.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

theorem bool_ac (a b c d : Bool) : ((a || b) || (c || d)) = ((a || c) || (b || d)) := by
  cases a <;> cases b <;> cases c <;> cases d <;> rfl

theorem any_or (xs : List Nat) (p q : Nat → Bool) :
    ((xs.any p || xs.any q) = xs.any (fun x => p x || q x)) := by
  induction xs with
  | nil => rfl
  | cons y l ih =>
    rw [List.any_cons, List.any_cons, List.any_cons]
    rw [bool_ac (p y) (l.any p) (q y) (l.any q), ih]

theorem any_congr_of {p q : Nat → Bool} (xs : List Nat) (h : ∀ x ∈ xs, p x = q x) :
    xs.any p = xs.any q := by
  induction xs with
  | nil => rfl
  | cons y l ih =>
    rw [List.any_cons, List.any_cons, h y (by simp), ih (fun x hx => h x (by simp [hx]))]

theorem mod_key (k : Nat) (hk : k < 32) (y : Nat) :
    (y % 64 == k || y % 64 == k + 32) = (y % 32 == k) := by
  have h : y % 64 = y % 32 ∨ y % 64 = y % 32 + 32 := by omega
  rcases h with h | h
  · rw [h]
    have : ¬ (y % 32 = k + 32) := by omega
    simp [this]
  · rw [h]
    have : ¬ (y % 32 + 32 = k) := by omega
    simp [this]

theorem sound_FoldInto32 : BitOccupancy.FoldInto32.Goal := by
  intro xs k hk
  simp only [ref_eq]
  rw [bit_iff_any, bit_iff_any (j := k + 32), any_or]
  refine any_congr_of xs (fun x _ => ?_)
  simpa using mod_key k hk x

theorem any_mem {p : Nat → Bool} : ∀ (xs : List Nat) (x : Nat), x ∈ xs → p x = true →
    xs.any p = true := by
  intro xs
  induction xs with
  | nil => intro x h _; exact absurd h (by simp)
  | cons y l ih =>
    intro x h hp
    rw [List.mem_cons] at h
    rw [List.any_cons]
    rcases h with rfl | h
    · rw [hp]; simp
    · rw [ih x h hp]; simp

theorem sound_NegativeTest : BitOccupancy.NegativeTest.Goal := by
  intro xs x h hx
  rw [ref_eq] at h
  rw [bit_iff_any] at h
  have hb : xs.any (fun x' => x' % 64 == x % 64) = true := any_mem xs x hx (by simp)
  rw [hb, Bool.not_true] at h
  simpa using h

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.BitOccupancy
