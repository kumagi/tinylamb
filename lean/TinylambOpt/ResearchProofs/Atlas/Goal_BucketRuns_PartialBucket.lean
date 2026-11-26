import TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-!
SQL scans and aggregates can use encoded page structure when positions and arithmetic
summaries agree with expanded rows.

Proved

- `BucketRuns.PartialBucket` — `∀ c (j : Nat), c.step ≠ 0 → occurrence layout c
  (c.base+c.step*j) = min (width c) (c.n-j*width c)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_PartialBucket

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.BucketRuns

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

private theorem rows_map (c : Config) :
    rows layout c = (List.range c.n).map (fun i =>
      (i, c.base + c.step * ((i / width c : Nat) : Int))) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  simp [layout]

private theorem countP_map_snd {α β : Type} [BEq β] (l : List α) (f : α → β) (v : β) :
    (l.map (fun x => (x, f x))).countP (fun r => r.2 == v) = l.countP (fun x => f x == v) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [List.countP_cons, ih]

private theorem value_eq_iff (c : Config) (j i : Nat) (hstep : c.step ≠ 0) :
    (c.base + c.step * ((i / width c : Nat) : Int) == c.base + c.step * (j : Int)) = true ↔
      (i / width c == j) = true := by
  rw [beq_iff_eq, beq_iff_eq]
  constructor
  · intro heq
    have hm : c.step * ((i / width c : Nat) : Int) = c.step * (j : Int) :=
      Int.add_left_cancel heq
    have hc : ((i / width c : Nat) : Int) = (j : Int) :=
      Int.eq_of_mul_eq_mul_left hstep hm
    exact Int.ofNat.inj hc
  · intro h
    simp [h]

private theorem count_div_eq (w n j : Nat) (hw : 0 < w) :
    (List.range n).countP (fun i => i / w == j) = min w (n - j * w) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      simp only [List.countP_singleton]
      rw [ih]
      by_cases hj : n / w = j
      · have hdecomp : n = j * w + n % w := by
          calc
            n = w * (n / w) + n % w := (Nat.div_add_mod n w).symm
            _ = j * w + n % w := by rw [hj, Nat.mul_comm w j]
        have hmod : n % w < w := Nat.mod_lt n hw
        have hsub : n - j * w = n % w := by
          calc
            n - j * w = (j * w + n % w) - j * w :=
              congrArg (fun x => x - j * w) hdecomp
            _ = n % w := Nat.add_sub_cancel_left _ _
        have hrhs : min w (n - j * w) = n % w := by
          rw [hsub]
          exact Nat.min_eq_right (Nat.le_of_lt hmod)
        have hsucc : (n + 1) - j * w = n % w + 1 := by
          calc
            (n + 1) - j * w = (j * w + n % w + 1) - j * w :=
              congrArg (fun x => (x + 1) - j * w) hdecomp
            _ = n % w + 1 := by omega
        have hmin : min w (n % w + 1) = n % w + 1 := by
          exact Nat.min_eq_right (by omega)
        simp [hj, hrhs, hsucc, hmin]
      · by_cases hlt : n < j * w
        · have hsub : n - j * w = 0 := Nat.sub_eq_zero_of_le (Nat.le_of_lt hlt)
          have hsucc : (n + 1) - j * w = 0 := by omega
          simp [hj, hsub, hsucc]
        · have hge : j * w ≤ n := Nat.le_of_not_gt hlt
          have hdivge : j ≤ n / w := (Nat.le_div_iff_mul_le hw).mpr hge
          have hdivgt : j < n / w := Nat.lt_of_le_of_ne hdivge (by
            intro heq
            exact hj heq.symm)
          have hmul : (j + 1) * w ≤ n := (Nat.le_div_iff_mul_le hw).mp (by omega)
          have hmul' : j * w + w ≤ n := by
            simpa [Nat.add_mul] using hmul
          have hle : w ≤ n - j * w := by omega
          have hle' : w ≤ (n + 1) - j * w := by omega
          have hsub : min w (n - j * w) = w := Nat.min_eq_left hle
          have hsucc : min w ((n + 1) - j * w) = w := Nat.min_eq_left hle'
          simp [hj, hsub, hsucc]

theorem sound_BucketRuns_PartialBucket : BucketRuns.PartialBucket.Goal := by
  intro c j hstep
  unfold occurrence
  rw [rows_map]
  rw [countP_map_snd]
  rw [List.countP_congr (fun i _ => value_eq_iff c j i hstep)]
  exact count_div_eq (width c) c.n j (by unfold width; omega)

end TinylambOpt.ResearchProofs.Atlas.Goal_BucketRuns_PartialBucket
