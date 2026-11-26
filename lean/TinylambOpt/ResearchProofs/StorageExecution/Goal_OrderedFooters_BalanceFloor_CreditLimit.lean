import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BalanceFloor.CreditLimit` — `∀ xs (credit : Int), 0 ≤ credit+(agg.reference xs).2 ↔
  ∀ i≤xs.length, 0≤credit+(agg.reference (xs.take i)).1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_CreditLimit

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs, 3 :: xs])

private def probe (n : Nat) : Bool :=
  (gen n).all (fun xs =>
    (List.range 13).all (fun c =>
      let credit := (c : Int) - 6
      ((0 ≤ credit + (agg.reference xs).2) ==
       ((List.range (xs.length + 1)).all (fun i =>
          0 ≤ credit + (agg.reference (xs.take i)).1)))))

#eval probe 3
#eval probe 4

private def minFrom (start : Int) : List Int → Int
  | [] => start
  | x :: xs => min start (minFrom (start + x) xs)

private theorem scanl_min?_some (start : Int) (l : List Int) :
    ∃ m, (l.scanl (· + ·) start).min? = some m := by
  cases l with
  | nil => exact ⟨start, by simp⟩
  | cons x xs => simp [List.scanl_cons]

private theorem minFrom_eq_scanl_min (start : Int) (l : List Int) :
    minFrom start l = (l.scanl (· + ·) start).min?.getD start := by
  induction l generalizing start with
  | nil => simp [minFrom]
  | cons x xs ih =>
      obtain ⟨m, hm⟩ := scanl_min?_some (start + x) xs
      have hih : minFrom (start + x) xs = m := by
        rw [ih (start + x), hm]
        simp
      rw [List.scanl_cons]
      simp [List.min?_cons, hm, minFrom, hih]

private theorem minFrom_le_iff (start : Int) (l : List Int) (m : Int) :
    minFrom start l ≤ m ↔ ∃ i ≤ l.length, start + (l.take i).sum ≤ m := by
  induction l generalizing start with
  | nil => simp [minFrom]
  | cons x xs ih =>
      constructor
      · intro h
        by_cases hs : start ≤ m
        · refine ⟨0, by omega, ?_⟩
          simpa using hs
        · have hh : min start (minFrom (start + x) xs) ≤ m := by
            simpa [minFrom] using h
          have hmin : minFrom (start + x) xs ≤ m := by omega
          obtain ⟨i, hi, hsum⟩ := (ih (start + x)).mp hmin
          refine ⟨i + 1, by simpa using Nat.succ_le_succ hi, ?_⟩
          have htake : (x :: xs).take (i + 1) = x :: xs.take i := by
            simp [List.take]
          rw [htake]
          simp
          omega
      · rintro ⟨i, hi, hsum⟩
        cases i with
        | zero =>
            simp [minFrom] at hsum ⊢
            omega
        | succ j =>
            have hi' : j + 1 ≤ xs.length + 1 := by simpa using hi
            have hj : j ≤ xs.length := by omega
            have hs0 := hsum
            simp [List.take] at hs0
            have hsum' : (start + x) + (xs.take j).sum ≤ m := by omega
            have hmin : minFrom (start + x) xs ≤ m :=
              (ih (start + x)).mpr ⟨j, hj, hsum'⟩
            simp [minFrom]
            omega

theorem sound_CreditLimit : BalanceFloor.CreditLimit.Goal := by
  intro xs credit
  let steps : List Int := xs.map (fun (x : Nat) =>
    if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int))
  have hmin : (agg.reference xs).2 = minFrom 0 steps := by
    simp [agg, steps, minFrom_eq_scanl_min]
  have hsum : ∀ i, (agg.reference (xs.take i)).1 = (steps.take i).sum := by
    intro i
    simp [agg, steps, List.map_take]
  have hlen : steps.length = xs.length := by simp [steps]
  constructor
  · intro h i hi
    have hm : 0 ≤ credit + minFrom 0 steps := by omega
    have hle : minFrom 0 steps ≤ (steps.take i).sum :=
      (minFrom_le_iff 0 steps ((steps.take i).sum)).mpr ⟨i, by omega, by simp⟩
    rw [hsum]
    omega
  · intro h
    by_cases htot : 0 ≤ credit + (agg.reference xs).2
    · exact htot
    · exfalso
      rw [hmin] at htot
      have hlt : minFrom 0 steps ≤ -credit - 1 := by omega
      obtain ⟨i, hi, hsumi⟩ := (minFrom_le_iff 0 steps (-credit - 1)).mp hlt
      have hok := h i (by omega)
      rw [hsum] at hok
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_CreditLimit
