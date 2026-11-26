import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Proved (10 claims, including the following)

- `Alternating.Pairs` — `∀ n, advance model 0 ((List.replicate n [0,1]).flatten) = some
  0`
- `Alternating.RepeatedColor` — `∀ q x, q<2 → x<2 → advance model q [x,x] = none`
- `AtLeastOneRed.Absorbing` — `∀ ls, advance model 1 ls = some 1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_Absorbing

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

-- ============ EvenHops ============
theorem evenAdv (q : Nat) (hq : q < 2) : ∀ ls : List Nat,
    advance EvenHops.model q ls = some ((q + ls.length) % 2) := by
  intro ls
  induction ls generalizing q with
  | nil => simp [advance]; omega
  | cons l ls ih =>
      simp [advance, EvenHops.model]
      change advance EvenHops.model (1 - q) ls = some ((q + (ls.length + 1)) % 2)
      rw [ih (1 - q) (by omega)]
      have hq01 : q = 0 ∨ q = 1 := by omega
      rcases hq01 with rfl | rfl <;> simp [Nat.add_mod] <;> omega

theorem eh_parity : EvenHops.Parity.Goal := by
  intro ls
  simpa using evenAdv 0 (by omega) ls

theorem eh_two : EvenHops.TwoSteps.Goal := by
  intro q a b hq
  have hq0 : q = 0 ∨ q = 1 := by omega
  rcases hq0 with rfl | rfl <;> simp [advance, EvenHops.model]

-- ============ AtLeastOneRed ============
theorem redFold1 : ∀ ls : List Nat, List.foldlM (fun q x => some (if x = 1 then 1 else q)) 1 ls = some 1 := by
  intro ls
  induction ls with
  | nil => simp
  | cons l ls ih => simp [ih]

theorem redFold0 : ∀ ls : List Nat, List.foldlM (fun q x => some (if x = 1 then 1 else q)) 0 ls = some (if 1 ∈ ls then 1 else 0) := by
  intro ls
  induction ls with
  | nil => simp
  | cons l ls ih =>
      by_cases hl : l = 1
      · subst l
        simp [redFold1]
      · have hl' : ¬ 1 = l := by omega
        simp [hl, hl', ih]

theorem ar_red : AtLeastOneRed.RedWitness.Goal := by
  intro ls
  simp [advance, AtLeastOneRed.model, redFold0]
  by_cases hmem : 1 ∈ ls <;> simp [hmem]

theorem sound : AtLeastOneRed.Absorbing.Goal := by
  intro ls
  simp [advance, AtLeastOneRed.model, redFold1]

-- ============ Alternating ============
theorem alt_repeat : Alternating.RepeatedColor.Goal := by
  intro q x hq hx
  have hq' : q = 0 ∨ q = 1 := by omega
  have hx' : x = 0 ∨ x = 1 := by omega
  rcases hq' with rfl | rfl <;> rcases hx' with rfl | rfl <;> simp [advance, Alternating.model]

theorem altFoldPair : ∀ ls : List Nat,
    List.foldlM (fun q x => if q = x then some (1 - q) else none) 0 ([0,1] ++ ls) =
    List.foldlM (fun q x => if q = x then some (1 - q) else none) 0 ls := by
  intro ls
  simp

theorem altFoldN : ∀ n : Nat,
    List.foldlM (fun q x => if q = x then some (1 - q) else none) 0 (List.replicate n [0,1]).flatten = some 0 := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.flatten_cons]
      rw [altFoldPair]
      exact ih

theorem alt_pairs : Alternating.Pairs.Goal := by
  intro n
  simp [advance, Alternating.model, altFoldN]

-- ============ TollBudget ============
theorem tollFold : ∀ (q : Nat) (hq : q ≤ 3) (ls : List Nat),
    List.foldlM (fun q x => if q + x ≤ 3 then some (q + x) else none) q ls =
      (if q + ls.sum ≤ 3 then some (q + ls.sum) else none) := by
  intro q hq ls
  induction ls generalizing q with
  | nil => simp [hq]
  | cons x xs ih =>
      by_cases h : q + x ≤ 3
      · simp [h]
        rw [ih (q+x) (by omega)]
        by_cases h2 : q + x + xs.sum ≤ 3
        · have h3 : q + (x + xs.sum) ≤ 3 := by omega
          simp [h2, h3, Nat.add_assoc]
        · simp [h2]
          have h3 : ¬ q + (x + xs.sum) ≤ 3 := by omega
          simp [h3]
      · simp [h]
        have h3 : ¬ q + (x + xs.sum) ≤ 3 := by omega
        simp [h3]

theorem tb_budget : TollBudget.Budget.Goal := by
  intro ls
  simp [advance, TollBudget.model, tollFold 0 (by omega) ls]
  by_cases h : ls.sum ≤ 3 <;> simp [h]

theorem tb_exact : TollBudget.ExactCost.Goal := by
  intro ls q h
  simp [advance, TollBudget.model] at h
  rw [tollFold 0 (by omega) ls] at h
  by_cases hs : ls.sum ≤ 3
  · simp [hs] at h
    cases h
    rfl
  · simp [hs] at h

-- ============ ModuloCost ============
theorem modFold : ∀ (q : Nat) (hq : q < 3) (ls : List Nat),
    List.foldlM (fun q x => some ((q + x) % 3)) q ls = some ((q + ls.sum) % 3) := by
  intro q hq ls
  induction ls generalizing q with
  | nil => simp [Nat.mod_eq_of_lt hq]
  | cons x xs ih =>
      simp [ih ((q+x)%3) (Nat.mod_lt _ (by decide))]
      omega

theorem mc_res : ModuloCost.Residue.Goal := by
  intro ls
  simp [advance, ModuloCost.model, modFold]

theorem permSum : ∀ {a b : List Nat}, a.Perm b → a.sum = b.sum := by
  intro a b h
  induction h with
  | nil => rfl
  | cons x h ih => simp [ih]
  | swap x y => simp [Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

theorem mc_reorder : ModuloCost.Reorder.Goal := by
  intro a b h
  simp [advance, ModuloCost.model, modFold]
  rw [permSum h]

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_AtLeastOneRed_Absorbing
