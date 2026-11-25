import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_PrefixCollision

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check: every series of length ≤ 5 over [-3, 3]. -/
#eval do
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  pure ((lists 5).filter (fun x =>
    ZeroFrames.measure x != TiePairs.measure (0 :: prefs x)) |>.length)

/-! ### Counting helpers -/

private def tieCount (x : Series) : Nat := (pairs x).countP (fun p => p.1 == p.2)
private def zeroCount (x : Series) : Nat := (windows x).countP (fun w => w.sum == 0)

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using eq_comm

private theorem countP_beq_comm (a : Int) (ys : Series) :
    ys.countP (fun s => a == s) = ys.countP (fun s => s == a) := by
  apply List.countP_congr
  intro s hs
  simp only [beq_iff_eq]
  exact eq_comm

private theorem range_map_succ {α : Type} (n : Nat) (f : Nat → α) :
    (List.range (n + 1)).map f = f 0 :: (List.range n).map (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n.succ + 1 = (n + 1).succ by omega]
      rw [List.range_succ, List.map_append, ih]
      simp [List.range_succ]

private theorem flatMap_range_succ {α : Type} (f : Nat → List α) (n : Nat) :
    (List.range (n + 1)).flatMap f = f 0 ++ (List.range n).flatMap (fun i => f (i + 1)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n.succ + 1 = n.succ.succ by omega, List.range_succ, List.flatMap_append]
      rw [ih]
      rw [List.range_succ, List.flatMap_append]
      simp [List.append_assoc]

private theorem prefs_cons (a : Int) (xs : Series) :
    prefs (a :: xs) = a :: (prefs xs).map (fun s => a + s) := by
  unfold prefs
  simp only [List.length_cons]
  rw [range_map_succ]
  simp [List.take_succ_cons, List.map_map]

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem countP_map_add_left (a b : Int) (xs : Series) :
    (xs.map (fun s => a + s)).countP (fun s => s == b) = xs.countP (fun s => s == b - a) := by
  rw [countP_map_comp]
  apply List.countP_congr
  intro s hs
  simp only [beq_iff_eq]
  constructor <;> intro h <;> omega

private theorem countP_map_add_right (a b : Int) (xs : Series) :
    (xs.map (fun s => a + s)).countP (fun s => b == s) = xs.countP (fun s => b - a == s) := by
  rw [countP_map_comp]
  apply List.countP_congr
  intro s hs
  simp only [beq_iff_eq]
  constructor <;> intro h <;> omega

private theorem tieCount_cons (a : Int) (x : Series) :
    tieCount (a :: x) = x.countP (fun b => a == b) + tieCount x := by
  unfold tieCount
  rw [pairs_cons, List.countP_append, countP_map_comp]

private theorem tieCount_map_add (a : Int) (x : Series) :
    tieCount (x.map (fun s => a + s)) = tieCount x := by
  induction x with
  | nil => rfl
  | cons b bs ih =>
      simp only [List.map_cons]
      rw [tieCount_cons, tieCount_cons, ih]
      rw [countP_map_add_right a (a + b) bs]
      have h : (a + b) - a = b := by omega
      rw [h]

private theorem windows_cons (a : Int) (xs : Series) :
    windows (a :: xs) =
      (List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1)) ++ windows xs := by
  unfold windows
  simp only [List.length_cons]
  rw [flatMap_range_succ]
  have hsub : ∀ i : Nat, xs.length + 1 - (i + 1) = xs.length - i := by intro i; omega
  simp [List.drop_succ_cons, hsub]

private theorem zeroCount_cons (a : Int) (xs : Series) :
    zeroCount (a :: xs) = zeroCount xs +
      ((List.range (xs.length + 1)).map (fun j => a + (xs.take j).sum)).countP (fun s => s == 0) := by
  unfold zeroCount
  rw [windows_cons, List.countP_append]
  have hcongr :
      ((List.range (xs.length + 1)).map (fun j => (a :: xs).take (j + 1))).countP (fun w => w.sum == 0) =
      ((List.range (xs.length + 1)).map (fun j => a + (xs.take j).sum)).countP (fun s => s == 0) := by
    rw [countP_map_comp, countP_map_comp]
    apply List.countP_congr
    intro j hj
    simp only [beq_iff_eq]
    simp [List.take_succ_cons]
  rw [hcongr]
  omega

private theorem prefix_zero_count (a : Int) (xs : Series) :
    ((List.range (xs.length + 1)).map (fun j => a + (xs.take j).sum)).countP (fun s => s == 0) =
      (if a == 0 then 1 else 0) + (prefs xs).countP (fun s => s == -a) := by
  rw [range_map_succ]
  simp only [List.countP_cons, List.take_zero, List.sum_nil]
  have hfirst : (if (a + 0 == 0) = true then 1 else 0) = (if a == 0 then 1 else 0) := by simp
  rw [hfirst]
  have hmap : (List.range xs.length).map (fun i => a + (xs.take (i + 1)).sum) =
      (prefs xs).map (fun s => a + s) := by
    unfold prefs
    rw [List.map_map]
    rfl
  rw [hmap, countP_map_add_left a 0 (prefs xs)]
  have hneg : 0 - a = -a := by omega
  rw [hneg]
  omega

private theorem tieCount_zero_cons_shift (a : Int) (ys : Series) :
    tieCount (0 :: a :: (ys.map (fun s => a + s))) =
      (if a == 0 then 1 else 0) + ys.countP (fun s => s == -a) +
        ys.countP (fun s => s == 0) + tieCount ys := by
  simp only [tieCount_cons, List.countP_cons]
  rw [tieCount_map_add]
  rw [countP_map_add_right a 0 ys, countP_map_add_right a a ys]
  have hneg : 0 - a = -a := by omega
  have hzero : a - a = 0 := by omega
  rw [hneg, hzero]
  have hbeq : (if (0 == a) = true then 1 else 0) = (if a == 0 then 1 else 0) := by
    rw [beq_comm_int 0 a]
  rw [hbeq]
  rw [countP_beq_comm (-a) ys, countP_beq_comm 0 ys]
  omega

theorem sound_ZeroFrames_PrefixCollision : ZeroFrames.PrefixCollision.Goal := by
  intro x
  unfold ZeroFrames.measure TiePairs.measure
  change (zeroCount x : Int) = (tieCount (0 :: prefs x) : Int)
  have hnat : zeroCount x = tieCount (0 :: prefs x) := by
    induction x with
    | nil => simp [zeroCount, tieCount, windows, prefs, pairs]
    | cons a xs ih =>
        rw [zeroCount_cons, prefix_zero_count, prefs_cons]
        rw [tieCount_zero_cons_shift]
        rw [tieCount_cons] at ih
        have ih' : zeroCount xs = (prefs xs).countP (fun s => s == 0) + tieCount (prefs xs) := by
          rw [ih]
          rw [countP_beq_comm 0 (prefs xs)]
        rw [ih']
        omega
  exact_mod_cast hnat

end TinylambOpt.ResearchProofs.Atlas.Goal_ZeroFrames_PrefixCollision
