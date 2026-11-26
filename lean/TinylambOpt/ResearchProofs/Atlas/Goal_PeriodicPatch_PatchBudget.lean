import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `PeriodicPatch.PatchBudget` — `∀ c, ((rows layout c).map Prod.snd).sum = ((rows
  LinearOrdinal.layout c).map Prod.snd).sum + c.bias*((c.n+width c-1)/width c:Nat)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PeriodicPatch_PatchBudget

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PeriodicPatch

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ]
      simp only [List.filterMap_append]
      rw [ih (fun i hi => h i (by omega))]
      simp [h n (by omega)]

private theorem rows_periodic_full (c : Config) :
    rows layout c = (List.range c.n).map (fun i : Nat =>
      (i, c.base + c.step * ((i : Nat) : Int) +
        (if i % width c == (0 : Nat) then c.bias else 0))) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  simp [layout]

private theorem rows_periodic (c : Config) :
    (rows layout c).map Prod.snd =
      (List.range c.n).map (fun i : Nat => c.base + c.step * ((i : Nat) : Int) +
        (if i % width c == (0 : Nat) then c.bias else 0)) := by
  rw [rows_periodic_full]
  simp

private theorem rows_linear_full (c : Config) :
    rows LinearOrdinal.layout c = (List.range c.n).map (fun i : Nat =>
      (i, c.base + c.step * ((i : Nat) : Int))) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  simp [LinearOrdinal.layout]

private theorem rows_linear (c : Config) :
    (rows LinearOrdinal.layout c).map Prod.snd =
      (List.range c.n).map (fun i : Nat => c.base + c.step * ((i : Nat) : Int)) := by
  rw [rows_linear_full]
  simp

private theorem div_n_add_w (n w : Nat) (hw : 0 < w) : (n + w) / w = n / w + 1 := by
  simpa using (Nat.add_mul_div_left n 1 hw)

private theorem ceil_succ (n w : Nat) (hw : 0 < w) :
    (n + 1 + w - 1) / w = (n + w - 1) / w + (if n % w == 0 then 1 else 0) := by
  have hsucc : n + 1 + w - 1 = n + w := by omega
  have hsucc_div : (n + 1 + w - 1) / w = n / w + 1 := by
    rw [hsucc, div_n_add_w n w hw]
  by_cases hmod : n % w = 0
  · have hn_eq : w * (n / w) = n := by
      have h := Nat.mod_add_div n w
      simpa [hmod] using h
    have hcur : n + w - 1 = (w - 1) + w * (n / w) := by
      calc
        n + w - 1 = w * (n / w) + w - 1 := by rw [hn_eq]
        _ = (w - 1) + w * (n / w) := by omega
    have hcur_div : (n + w - 1) / w = n / w := by
      rw [hcur, Nat.add_mul_div_left (w - 1) (n / w) hw,
        Nat.div_eq_of_lt (by omega : w - 1 < w)]
      simp
    have hm : (if n % w == 0 then 1 else 0) = 1 := by simp [hmod]
    rw [hsucc_div, hcur_div, hm]
  · let q := n / w
    let r := n % w
    have hdecomp : q * w + r = n := by
      dsimp [q, r]
      rw [Nat.mul_comm (n / w) w, Nat.div_add_mod]
    have hrpos : 0 < r := by
      dsimp [r]
      exact Nat.pos_of_ne_zero hmod
    have hrlt : r < w := by
      dsimp [r]
      exact Nat.mod_lt n hw
    have hcur : n + w - 1 = (r - 1) + w * (q + 1) := by
      rw [← hdecomp]
      rw [Nat.mul_add, Nat.mul_one, Nat.mul_comm q w]
      omega
    have hcur_div : (n + w - 1) / w = q + 1 := by
      rw [hcur, Nat.add_mul_div_left (r - 1) (q + 1) hw,
        Nat.div_eq_of_lt (by omega : r - 1 < w)]
      simp
    have hm : (if n % w == 0 then 1 else 0) = 0 := by simp [hmod]
    rw [hsucc_div, hcur_div, hm]

private theorem sum_patch (c : Config) : ∀ n : Nat,
    ((List.range n).map (fun i : Nat => c.base + c.step * ((i : Nat) : Int) +
      (if i % width c == (0 : Nat) then c.bias else 0))).sum =
    ((List.range n).map (fun i : Nat => c.base + c.step * ((i : Nat) : Int))).sum +
      c.bias * ((n + width c - 1) / width c : Nat) := by
  intro n
  induction n with
  | zero =>
      have hw : 0 < width c := by unfold width; omega
      have hz : (0 + width c - 1) / width c = 0 := by
        apply Nat.div_eq_of_lt
        omega
      rw [hz]
      simp
  | succ n ih =>
      rw [range_succ_map_sum, range_succ_map_sum, ih]
      by_cases hmod : n % width c == (0 : Nat)
      · have hs := ceil_succ n (width c) (by unfold width; omega)
        rw [hs]
        simp [hmod, Int.mul_add]
        omega
      · have hs := ceil_succ n (width c) (by unfold width; omega)
        rw [hs]
        simp [hmod]
        omega

theorem sound_PeriodicPatch_PatchBudget : PeriodicPatch.PatchBudget.Goal := by
  intro c
  rw [rows_periodic, rows_linear]
  exact sum_patch c c.n

end TinylambOpt.ResearchProofs.Atlas.Goal_PeriodicPatch_PatchBudget
