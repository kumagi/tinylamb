import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingSign_SliceState

open TinylambOpt.ResearchGoals.Atlas.EncodedPages
open TinylambOpt.ResearchGoals.Atlas.EncodedPages.AlternatingSign

/-! 一般のリスト補題 -/

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem filter_map_pair_range (n lo hi : Nat) (f : Nat → Int) :
    ((List.range n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi) =
      ((List.range n).filter (fun i => lo ≤ i && i < hi)).map (fun i => (i, f i)) := by
  simpa using (map_pair_filter (List.range n) (fun i : Nat => i) f (fun x : Nat => lo ≤ x && x < hi))

private theorem range_count_between (n tlo thi : Nat) (hle : tlo ≤ thi) :
    ((List.range n).filter (fun i => tlo ≤ i && i < thi)).length = min n thi - min n tlo := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h1 : tlo ≤ n <;> by_cases h2 : n < thi <;> simp [h1, h2, ih] <;> omega

private theorem range_sum_between (n tlo thi : Nat) (f : Nat → Int) (hle : tlo ≤ thi) :
    (((List.range n).filter (fun i => tlo ≤ i && i < thi)).map f).sum =
      ((List.range (min n thi)).map f).sum - ((List.range (min n tlo)).map f).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h1 : tlo ≤ n <;> by_cases h2 : n < thi
      · simp [h1, h2]
        rw [ih]
        have hmthi : min n thi = n := Nat.min_eq_left (by omega)
        have hmtlo : min n tlo = tlo := Nat.min_eq_right (by omega)
        have hmnthi : min (n + 1) thi = n + 1 := Nat.min_eq_left (by omega)
        have hmntlo : min (n + 1) tlo = tlo := Nat.min_eq_right (by omega)
        rw [hmthi, hmtlo, hmnthi, hmntlo]
        simp [List.range_succ]
        omega
      · simp [h1, h2]
        rw [ih]
        have hnthi : min (n + 1) thi = min n thi := by omega
        have hntlo : min (n + 1) tlo = min n tlo := by omega
        rw [hnthi, hntlo]
      · simp [h1, h2]
        rw [ih]
        have hmthi : min n thi = n := Nat.min_eq_left (by omega)
        have hmtlo : min n tlo = n := Nat.min_eq_left (by omega)
        have hmnthi : min (n + 1) thi = n + 1 := Nat.min_eq_left (by omega)
        have hmntlo : min (n + 1) tlo = n + 1 := Nat.min_eq_left (by omega)
        rw [hmthi, hmtlo, hmnthi, hmntlo]
        simp [List.range_succ]
      · simp [h1, h2]
        omega

private theorem range_succ_map_sum (f : Nat → Int) (n : Nat) :
    ((List.range (n + 1)).map f).sum = ((List.range n).map f).sum + f n := by
  rw [show n + 1 = Nat.succ n by omega, List.range_succ, List.map_append, List.sum_append]
  simp

/-! 等差レーンの prefix sum -/

private theorem choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by
      have := Nat.mul_le_mul_left k (show 1 ≤ k by omega)
      simpa using this
    have hcan : k * k - k + k = k * k := Nat.sub_add_cancel hle
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [choose2_succ]
  simp [Int.natCast_add]

private theorem partialSum_succ (a : Lane) (k : Nat) :
    partialSum a (k + 1) = partialSum a k + (a.base + (k : Int) * a.step) := by
  rw [partialSum, partialSum, tri_succ]
  simp [Int.natCast_add, Int.add_mul]
  omega

private theorem even_step (B S : Int) (p : Nat) (he : p % 2 = 0) :
    B + ((p : Int) / 2) * (2 * S) =
      (if p % 2 == 0 then (1 : Int) else -1) * (B + S * (p : Int)) := by
  have hd : p / 2 * 2 = p := by omega
  have hdI : ((p / 2 : Nat) : Int) * 2 = (p : Int) := by exact_mod_cast hd
  have hc : (p : Int) / 2 = ((p / 2 : Nat) : Int) := by
    rw [← hdI]
    omega
  rw [hc]
  simp [he]
  grind

private theorem odd_step (B S : Int) (p : Nat) (he : p % 2 = 1) :
    -(B + S) + ((p : Int) / 2) * (-2 * S) =
      (if p % 2 == 0 then (1 : Int) else -1) * (B + S * (p : Int)) := by
  have hd : p / 2 * 2 + 1 = p := by omega
  have hdI : ((p / 2 : Nat) : Int) * 2 + 1 = (p : Int) := by exact_mod_cast hd
  have hc : (p : Int) / 2 = ((p / 2 : Nat) : Int) := by
    have h2 : ((p / 2 : Nat) : Int) * 2 + 1 = (p : Int) := hdI
    omega
  rw [hc]
  simp [he]
  grind

/-! 交互符号ページのスライス状態 -/

theorem sound_AlternatingSign_SliceState : AlternatingSign.SliceState.Goal := by
  intro c lo hi hle
  dsimp only
  let n := c.n
  let B := c.base
  let S := c.step
  let f : Nat → Int := fun i => (if i % 2 == 0 then (1 : Int) else -1) * (B + S * (i : Int))
  let l0 : Lane := lane n 0 2 B (2 * S)
  let l1 : Lane := lane n 1 2 (-(B + S)) (-2 * S)
  let P : Nat → Int := fun m => ((List.range m).map f).sum
  let C0 : Nat → Nat := fun p => beforePos l0 p
  let C1 : Nat → Nat := fun p => beforePos l1 p
  let E : Nat → Int := fun p => partialSum l0 (C0 p)
  let O : Nat → Int := fun p => partialSum l1 (C1 p)

  have hrows : rows layout c = (List.range n).map (fun i => (i, f i)) := by
    simp [rows, layout, f, n, B, S]

  have hC0 : ∀ p, C0 p = min ((n + 1) / 2) ((p + 1) / 2) := by
    intro p
    dsimp [C0, l0, n, B, S]
    simp [beforePos, lane]

  have hC1 : ∀ p, C1 p = min (n / 2) (p / 2) := by
    intro p
    dsimp [C1, l1, n, B, S]
    simp [beforePos, lane]
    omega

  have hCsum : ∀ p, C0 p + C1 p = min n p := by
    intro p
    rw [hC0 p, hC1 p]
    omega

  have hEsum : ∀ p, E p + O p = P (min n p) := by
    intro p
    induction p with
    | zero =>
        have hb0 : C0 0 = 0 := by
          rw [hC0]
          omega
        have hb1 : C1 0 = 0 := by
          rw [hC1]
          omega
        simp [E, O, P, hb0, hb1, partialSum, tri,
          TinylambOpt.ResearchGoals.PairAggregates.choose2]
    | succ p ih =>
        by_cases hpn : p < n
        · rcases (show p % 2 = 0 ∨ p % 2 = 1 by omega) with he | ho
          · have hC0s : C0 (p + 1) = C0 p + 1 := by
              rw [hC0 (p + 1), hC0 p]
              omega
            have hC1s : C1 (p + 1) = C1 p := by
              rw [hC1 (p + 1), hC1 p]
              omega
            have hEs : E (p + 1) = E p + f p := by
              dsimp [E]
              rw [hC0s, partialSum_succ]
              have hk : C0 p = p / 2 := by
                rw [hC0 p]
                omega
              rw [hk]
              dsimp [l0, f, lane]
              rw [even_step B S p he]
            have hOs : O (p + 1) = O p := by
              dsimp [O]
              rw [hC1s]
            have hPs : P (p + 1) = P p + f p := by
              dsimp [P]
              exact range_succ_map_sum f p
            have hmn : min n p = p := Nat.min_eq_right (by omega)
            have hmn1 : min n (p + 1) = p + 1 := Nat.min_eq_right (by omega)
            calc
              E (p + 1) + O (p + 1) = (E p + f p) + O p := by rw [hEs, hOs]
              _ = (E p + O p) + f p := by omega
              _ = P (min n p) + f p := by rw [ih]
              _ = P p + f p := by rw [hmn]
              _ = P (p + 1) := by rw [hPs]
              _ = P (min n (p + 1)) := by rw [hmn1]
          · have hC0s : C0 (p + 1) = C0 p := by
              rw [hC0 (p + 1), hC0 p]
              omega
            have hC1s : C1 (p + 1) = C1 p + 1 := by
              rw [hC1 (p + 1), hC1 p]
              omega
            have hEs : E (p + 1) = E p := by
              dsimp [E]
              rw [hC0s]
            have hOs : O (p + 1) = O p + f p := by
              dsimp [O]
              rw [hC1s, partialSum_succ]
              have hk : C1 p = p / 2 := by
                rw [hC1 p]
                omega
              rw [hk]
              dsimp [l1, f, lane]
              rw [odd_step B S p ho]
            have hPs : P (p + 1) = P p + f p := by
              dsimp [P]
              exact range_succ_map_sum f p
            have hmn : min n p = p := Nat.min_eq_right (by omega)
            have hmn1 : min n (p + 1) = p + 1 := Nat.min_eq_right (by omega)
            calc
              E (p + 1) + O (p + 1) = E p + (O p + f p) := by rw [hEs, hOs]
              _ = (E p + O p) + f p := by omega
              _ = P (min n p) + f p := by rw [ih]
              _ = P p + f p := by rw [hmn]
              _ = P (p + 1) := by rw [hPs]
              _ = P (min n (p + 1)) := by rw [hmn1]
        · have hC0s : C0 (p + 1) = C0 p := by
            rw [hC0 (p + 1), hC0 p]
            omega
          have hC1s : C1 (p + 1) = C1 p := by
            rw [hC1 (p + 1), hC1 p]
            omega
          have hEs : E (p + 1) = E p := by
            dsimp [E]
            rw [hC0s]
          have hOs : O (p + 1) = O p := by
            dsimp [O]
            rw [hC1s]
          have hmn : min n (p + 1) = min n p := by omega
          calc
            E (p + 1) + O (p + 1) = E p + O p := by rw [hEs, hOs]
            _ = P (min n p) := ih
            _ = P (min n (p + 1)) := by rw [hmn]

  have hsel : ((List.range n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi) =
      ((List.range n).filter (fun i => lo ≤ i && i < hi)).map (fun i => (i, f i)) :=
    filter_map_pair_range n lo hi f

  have hlen : (((List.range n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi)).length =
      min n hi - min n lo := by
    rw [hsel]
    simp only [List.length_map]
    exact range_count_between n lo hi hle

  have hsum :
      ((((List.range n).map (fun i => (i, f i))).filter (fun r => lo ≤ r.1 && r.1 < hi)).map Prod.snd).sum =
        P (min n hi) - P (min n lo) := by
    rw [hsel]
    simp only [List.map_map]
    exact range_sum_between n lo hi f hle

  have hcountR :
      (beforePos l0 hi - beforePos l0 lo) + (beforePos l1 hi - beforePos l1 lo) =
        min n hi - min n lo := by
    change (C0 hi - C0 lo) + (C1 hi - C1 lo) = min n hi - min n lo
    rw [← hCsum hi, ← hCsum lo]
    have h0 : C0 lo ≤ C0 hi := by
      rw [hC0 lo, hC0 hi]
      omega
    have h1 : C1 lo ≤ C1 hi := by
      rw [hC1 lo, hC1 hi]
      omega
    omega

  have hsumR :
      (partialSum l0 (beforePos l0 hi) - partialSum l0 (beforePos l0 lo)) +
        (partialSum l1 (beforePos l1 hi) - partialSum l1 (beforePos l1 lo)) =
        P (min n hi) - P (min n lo) := by
    change (E hi - E lo) + (O hi - O lo) = P (min n hi) - P (min n lo)
    rw [← hEsum hi, ← hEsum lo]
    omega

  have hlanes : layout.lanes c = [l0, l1] := by
    simp [layout, l0, l1, n, B, S]

  rw [hrows, hlanes]
  apply Prod.ext
  · simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
    rw [hlen]
    omega
  · simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
    rw [hsum]
    omega

end TinylambOpt.ResearchProofs.Atlas.Goal_AlternatingSign_SliceState
