import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `LongestHole.BridgeRun` — `∀ (a b : List Nat), let s := (a.reverse.takeWhile (· ==
  0)).length let p := (b.takeWhile (· == 0)).length longestZero (a++b)=max (max
  (longestZero a) (longestZero b)) (s+p)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_BridgeRun

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole
open TinylambOpt.ResearchGoals.Compression.Wire

-- small exhaustive truth check
private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun b =>
    longestZero (a ++ b) ==
      max (max (longestZero a) (longestZero b))
        ((a.reverse.takeWhile (· == 0)).length + (b.takeWhile (· == 0)).length)))

#eval probe 5
#eval probe 6

private def lead (xs : List Nat) : Nat := (xs.takeWhile (fun x => x == 0)).length
private def trail (xs : List Nat) : Nat := lead xs.reverse

private theorem foldl_max_eq_peak_add (L : List Nat) (a : Nat) :
    L.foldl max a = max a (peak L) := by
  induction L generalizing a with
  | nil => simp [peak]
  | cons b bs ih =>
      simp only [List.foldl]
      rw [ih (max a b)]
      have hihb := ih b
      simp [peak, List.max?] at hihb ⊢
      omega

private theorem peak_cons (a : Nat) (L : List Nat) : peak (a :: L) = max a (peak L) := by
  change L.foldl max a = max a (peak L)
  exact foldl_max_eq_peak_add L a

private def lz : List Nat → Nat
  | [] => 0
  | x :: xs => if x == 0 then max (1 + lead xs) (lz xs) else lz xs

private def allZero (xs : List Nat) : Prop := ∀ x ∈ xs, (x == 0) = true

private theorem bool_false_of_not_true {b : Bool} (h : ¬ b = true) : b = false := by
  cases b <;> simp_all

private theorem lead_cons_zero (x : Nat) (xs : List Nat) (hx : (x == 0) = true) :
    lead (x :: xs) = 1 + lead xs := by
  simp [lead, hx]
  omega

private theorem lead_cons_ne (x : Nat) (xs : List Nat) (hx : (x == 0) = false) :
    lead (x :: xs) = 0 := by
  simp [lead, hx]

private theorem lead_eq_length_of_allZero {xs : List Nat} (h : allZero xs) :
    lead xs = xs.length := by
  induction xs with
  | nil => simp [lead]
  | cons x xs ih =>
      have hx : (x == 0) = true := h x (by simp)
      have hxs : allZero xs := fun y hy => h y (by simp [hy])
      rw [lead_cons_zero x xs hx, ih hxs]
      simp
      omega

private theorem lead_append_of_allZero (a b : List Nat) (h : allZero a) :
    lead (a ++ b) = a.length + lead b := by
  induction a with
  | nil => simp [lead]
  | cons x xs ih =>
      have hx : (x == 0) = true := h x (by simp)
      have hxs : allZero xs := fun y hy => h y (by simp [hy])
      rw [List.cons_append, lead_cons_zero x (xs ++ b) hx, ih hxs]
      simp
      omega

private theorem lead_append_of_not_allZero (a b : List Nat) (h : ¬ allZero a) :
    lead (a ++ b) = lead a := by
  induction a with
  | nil => simp [allZero] at h
  | cons x xs ih =>
      by_cases hx : (x == 0) = true
      · have hxs : ¬ allZero xs := by
          intro hz
          apply h
          intro y hy
          simp only [List.mem_cons] at hy
          rcases hy with rfl | hy
          · exact hx
          · exact hz y hy
        rw [List.cons_append, lead_cons_zero x (xs ++ b) hx,
          lead_cons_zero x xs hx, ih hxs]
      · have hx' : (x == 0) = false := bool_false_of_not_true hx
        rw [List.cons_append, lead_cons_ne x (xs ++ b) hx', lead_cons_ne x xs hx']

private theorem lead_le_lz : ∀ xs : List Nat, lead xs ≤ lz xs := by
  intro xs
  cases xs with
  | nil => simp [lead, lz]
  | cons x xs =>
      by_cases hx : (x == 0) = true
      · rw [lead_cons_zero x xs hx]
        simp [lz, hx]
        exact Nat.le_max_left _ _
      · have hx' : (x == 0) = false := bool_false_of_not_true hx
        rw [lead_cons_ne x xs hx']
        simp [lz, hx']

private theorem lead_append_left (a b : List Nat) : lead a ≤ lead (a ++ b) := by
  induction a with
  | nil => simp [lead]
  | cons x xs ih =>
      rw [List.cons_append]
      by_cases hx : (x == 0) = true
      · rw [lead_cons_zero x (xs ++ b) hx, lead_cons_zero x xs hx]
        omega
      · have hx' : (x == 0) = false := bool_false_of_not_true hx
        rw [lead_cons_ne x (xs ++ b) hx', lead_cons_ne x xs hx']
        omega

private theorem lz_append_left (a b : List Nat) : lz a ≤ lz (a ++ b) := by
  induction a with
  | nil => simp [lz]
  | cons x xs ih =>
      by_cases hx : (x == 0) = true
      · have hlead := lead_append_left xs b
        rw [List.cons_append]
        simp [lz, hx]
        omega
      · have hx' : (x == 0) = false := bool_false_of_not_true hx
        rw [List.cons_append]
        simp [lz, hx', ih]

private theorem lz_append_right (a b : List Nat) : lz b ≤ lz (a ++ b) := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      rw [List.cons_append]
      by_cases hx : (x == 0) = true
      · simp [lz, hx]
        exact Nat.le_trans ih (Nat.le_max_right _ _)
      · have hx' : (x == 0) = false := bool_false_of_not_true hx
        simpa [lz, hx'] using ih

private theorem lz_eq_length_of_allZero {xs : List Nat} (h : allZero xs) :
    lz xs = xs.length := by
  induction xs with
  | nil => simp [lz]
  | cons x xs ih =>
      have hx : (x == 0) = true := h x (by simp)
      have hxs : allZero xs := fun y hy => h y (by simp [hy])
      have hle := lead_eq_length_of_allZero hxs
      simp [lz, hx, ih hxs, hle]
      omega

private theorem allZero_reverse {xs : List Nat} : allZero xs.reverse ↔ allZero xs := by
  constructor <;> intro h x hx
  · exact h x (List.mem_reverse.mpr hx)
  · exact h x (List.mem_reverse.mp hx)

private theorem range_succ_shift (n : Nat) :
    List.range (n + 1) = 0 :: (List.range n).map (fun i => i + 1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        List.range (Nat.succ n + 1) = List.range (n + 1) ++ [n + 1] := by
          rw [show Nat.succ n + 1 = (n + 1) + 1 by omega, List.range_succ]
        _ = (0 :: (List.range n).map (fun i => i + 1)) ++ [n + 1] := by rw [ih]
        _ = 0 :: ((List.range n).map (fun i => i + 1) ++ [n + 1]) := rfl
        _ = 0 :: (List.range (Nat.succ n)).map (fun i => i + 1) := by
          rw [show List.range (Nat.succ n) = List.range n ++ [n] by rw [List.range_succ]]
          simp

private theorem longestZero_eq_lz : ∀ xs : List Nat, longestZero xs = lz xs := by
  intro xs
  induction xs with
  | nil => simp [longestZero, lz, peak]
  | cons x xs ih =>
      have hmap :
          (List.range ((x :: xs).length + 1)).map
              (fun i => (((x :: xs).drop i).takeWhile (fun y => y == 0)).length) =
            ((x :: xs).takeWhile (fun y => y == 0)).length ::
              (List.range (xs.length + 1)).map
                (fun i => ((xs.drop i).takeWhile (fun y => y == 0)).length) := by
        rw [show (x :: xs).length + 1 = (xs.length + 1) + 1 by simp]
        rw [range_succ_shift (xs.length + 1)]
        simp [List.map_map, Function.comp_def]
      have ih' : peak ((List.range (xs.length + 1)).map
            (fun i => ((xs.drop i).takeWhile (fun y => y == 0)).length)) = lz xs := by
        simpa [longestZero] using ih
      unfold longestZero
      rw [hmap, peak_cons, ih']
      by_cases hx : (x == 0) = true
      · simp [lz, hx, lead]
        omega
      · have hx' : (x == 0) = false := bool_false_of_not_true hx
        simp [lz, hx']

private theorem trail_cons_eq (x : Nat) (xs : List Nat) :
    trail (x :: xs) = lead (xs.reverse ++ [x]) := by
  simp [trail]

private theorem trail_cons_of_ne (x : Nat) (xs : List Nat) (hx : (x == 0) = false) :
    trail (x :: xs) = trail xs := by
  rw [trail_cons_eq]
  by_cases hz : allZero xs.reverse
  · have hr : lead xs.reverse = xs.length := by
      simpa [List.length_reverse] using lead_eq_length_of_allZero hz
    have hxlead : lead [x] = 0 := by simp [lead, hx]
    rw [lead_append_of_allZero xs.reverse [x] hz]
    rw [hxlead]
    rw [Nat.add_zero]
    simpa [trail, List.length_reverse] using hr.symm
  · exact lead_append_of_not_allZero xs.reverse [x] hz

private theorem trail_cons_of_not_allZero (x : Nat) (xs : List Nat) (hz : ¬ allZero xs) :
    trail (x :: xs) = trail xs := by
  rw [trail_cons_eq]
  apply lead_append_of_not_allZero xs.reverse [x]
  intro h
  exact hz (allZero_reverse.mp h)

private theorem trail_cons_of_zero (x : Nat) (xs : List Nat) (hx : (x == 0) = true)
    (hz : allZero xs) : trail (x :: xs) = trail xs + 1 := by
  rw [trail_cons_eq]
  have hzr : allZero xs.reverse := allZero_reverse.mpr hz
  have hr : lead xs.reverse = xs.length := by
    simpa [List.length_reverse] using lead_eq_length_of_allZero hzr
  have hxlead : lead [x] = 1 := by simp [lead, hx]
  rw [lead_append_of_allZero xs.reverse [x] hzr]
  rw [hxlead]
  have h4 : xs.reverse.length = trail xs := by
    simpa [trail, List.length_reverse] using hr.symm
  omega

private theorem bridge_lz : ∀ a b : List Nat,
    lz (a ++ b) = max (max (lz a) (lz b)) (trail a + lead b) := by
  intro a
  induction a with
  | nil =>
      intro b
      simpa [trail, lead, lz] using (Nat.max_eq_left (lead_le_lz b)).symm
  | cons x xs ih =>
      intro b
      by_cases hx : (x == 0) = true
      · by_cases hz : allZero xs
        · have hallx : allZero (x :: xs) := by
            intro y hy
            simp only [List.mem_cons] at hy
            rcases hy with rfl | hy
            · exact hx
            · exact hz y hy
          have hleadxb := lead_append_of_allZero xs b hz
          have htrail := trail_cons_of_zero x xs hx hz
          have hlzx := lz_eq_length_of_allZero hallx
          have hlzxs := lz_eq_length_of_allZero hz
          have htrailxs : trail xs = xs.length := by
            simpa [trail] using lead_eq_length_of_allZero (allZero_reverse.mpr hz)
          have hleadb := lead_le_lz b
          have ihb := ih b
          rw [List.cons_append]
          rw [hlzx, htrail]
          simp [lz, hx]
          rw [hleadxb, ihb, hlzxs, htrailxs]
          omega
        · have hleadxb := lead_append_of_not_allZero xs b hz
          have htrail := trail_cons_of_not_allZero x xs hz
          have ihb := ih b
          rw [List.cons_append]
          simp [lz, hx]
          rw [hleadxb, ihb, htrail]
          omega
      · have hx' : (x == 0) = false := bool_false_of_not_true hx
        have htrail := trail_cons_of_ne x xs hx'
        have ihb := ih b
        rw [List.cons_append]
        simp [lz, hx']
        rw [ihb, htrail]
        omega

theorem sound_BridgeRun : LongestHole.BridgeRun.Goal := by
  intro a b
  dsimp
  rw [longestZero_eq_lz a, longestZero_eq_lz b, longestZero_eq_lz (a ++ b)]
  simpa [trail, lead] using bridge_lz a b

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_BridgeRun
