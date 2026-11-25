import TinylambOpt.ResearchGoals.Atlas.EncodedPages

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkD

open TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! ## Generic helpers -/

private theorem filterMap_some_eq_map {α β : Type} (l : List α) (f : α → β) :
    l.filterMap (fun x => some (f x)) = l.map f := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_congr' {α β : Type} {l : List α} {f g : α → Option β}
    (h : ∀ x ∈ l, f x = g x) : l.filterMap f = l.filterMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filterMap_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem range_filterMap_all {α : Type} (n : Nat) (f : Nat → Option α) (g : Nat → α)
    (h : ∀ i, i < n → f i = some (g i)) :
    (List.range n).filterMap f = (List.range n).map g := by
  rw [← filterMap_some_eq_map (List.range n) g]
  apply filterMap_congr'
  intro i hi
  exact h i (List.mem_range.mp hi)

private theorem list_sum_perm {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem perm_length {α : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) : l₁.length = l₂.length := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem flatMap_congr_left {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_natCast_eq' {α : Type} (l : List α) (g : α → Nat) :
    ((l.map (fun x => (g x : Int))).sum) = (((l.map g).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]

private theorem map_pair_filter {α β : Type} (xs : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    (xs.map (fun x => (f x, g x))).filter (fun r => p r.1) =
      (xs.filter (fun x => p (f x))).map (fun x => (f x, g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private def lanePoints (a : Lane) : List (Nat × Int) :=
  (List.range a.n).map (fun i : Nat => (a.start + i * a.stride, a.base + (i : Int) * a.step))

/-! ## DeletedSpan -/

namespace DeletedSpan

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan

private theorem rows_eq (c : Config) :
    rows layout c =
      ((List.range (min c.n c.split)).map (fun i => (i, c.base + c.step * (i : Int))) : List (Nat × Int)) ++
      ((List.range (c.n - (c.split + width c))).map
        (fun j => (c.split + width c + j, c.base + c.step * ((c.split + width c + j : Nat) : Int))) : List (Nat × Int)) := by
  unfold rows
  simp only [layout]
  -- split range into [0, split) and [split, split+width) and [split+width, n)
  have hsplit : List.range c.n =
      List.range (min c.n c.split) ++
      (List.range (min c.n (c.split + width c) - min c.n c.split)).map (fun j => min c.n c.split + j) ++
      (List.range (c.n - min c.n (c.split + width c))).map (fun j => min c.n (c.split + width c) + j) := by
    have h1len : min c.n c.split ≤ min c.n (c.split + width c) := by omega
    have h2len : min c.n (c.split + width c) ≤ c.n := Nat.min_le_left _ _
    have hsum : min c.n c.split + (min c.n (c.split + width c) - min c.n c.split) +
        (c.n - min c.n (c.split + width c)) = c.n := by omega
    have hEq : List.range c.n = List.range (min c.n c.split + (min c.n (c.split + width c) - min c.n c.split) +
        (c.n - min c.n (c.split + width c))) := by rw [hsum]
    rw [hEq, List.range_add, List.range_add]
    simp only [Nat.add_assoc, Nat.add_sub_cancel' h1len]
  rw [hsplit, List.filterMap_append, List.filterMap_append]
  -- first part: all kept
  have h1 : (List.range (min c.n c.split)).filterMap
      (fun i => (if c.split ≤ i && i < c.split + width c then none
        else some (c.base + c.step * (i : Int))).map (fun v => (i, v))) =
      ((List.range (min c.n c.split)).map (fun i => (i, c.base + c.step * (i : Int))) : List (Nat × Int)) := by
    rw [← filterMap_some_eq_map]
    apply filterMap_congr'
    intro i hi
    have hi' : i < c.split := Nat.lt_of_lt_of_le (List.mem_range.mp hi) (Nat.min_le_right _ _)
    have : ¬ (c.split ≤ i && i < c.split + width c) = true := by
      simp; omega
    simp [this]
  -- middle part: all dropped
  have h2 : ((List.range (min c.n (c.split + width c) - min c.n c.split)).map
      (fun j => min c.n c.split + j)).filterMap
      (fun i => (if c.split ≤ i && i < c.split + width c then none
        else some (c.base + c.step * (i : Int))).map (fun v => (i, v))) = [] := by
    rw [List.filterMap_eq_nil_iff]
    intro x hx
    simp only [List.mem_map, List.mem_range] at hx
    rcases hx with ⟨j, hj, rfl⟩
    have hjb : min c.n c.split + j < min c.n (c.split + width c) := by omega
    have hlo : c.split ≤ min c.n c.split + j := by
      by_cases hs : c.split ≤ c.n
      · have : min c.n c.split = c.split := Nat.min_eq_right hs
        omega
      · omega
    have hhi : min c.n c.split + j < c.split + width c := by
      have := Nat.min_le_right c.n (c.split + width c)
      omega
    simp [hlo, hhi]
  -- third part: all kept
  have h3 : ((List.range (c.n - min c.n (c.split + width c))).map
      (fun j => min c.n (c.split + width c) + j)).filterMap
      (fun i => (if c.split ≤ i && i < c.split + width c then none
        else some (c.base + c.step * (i : Int))).map (fun v => (i, v))) =
      ((List.range (c.n - (c.split + width c))).map
        (fun j => (c.split + width c + j, c.base + c.step * ((c.split + width c + j : Nat) : Int))) : List (Nat × Int)) := by
    by_cases hn : c.split + width c ≤ c.n
    · have hmn : min c.n (c.split + width c) = c.split + width c := Nat.min_eq_right hn
      simp only [hmn]
      rw [List.filterMap_map]
      rw [← filterMap_some_eq_map (List.range (c.n - (c.split + width c)))
            (fun j => (c.split + width c + j, c.base + c.step * ((c.split + width c + j : Nat) : Int)))]
      apply filterMap_congr'
      intro j hj
      simp only [Function.comp, Option.map]
      split
      · simp_all
      · rename_i h
        simp at h
    · have hmn : min c.n (c.split + width c) = c.n := Nat.min_eq_left (by omega)
      have hsub : c.n - (c.split + width c) = 0 := by omega
      simp [hmn, hsub]
  rw [h1, h2, h3]
  simp

theorem SeekSurvivor_sound : SeekSurvivor.Goal := by
  intro c k hk
  rw [rows_eq] at hk ⊢
  have hlen1 : (((List.range (min c.n c.split)).map (fun i => (i, c.base + c.step * (i : Int))) : List (Nat × Int))).length = min c.n c.split := by simp
  have hlenB : (((List.range (c.n - (c.split + width c))).map
      (fun j => (c.split + width c + j, c.base + c.step * ((c.split + width c + j : Nat) : Int))) : List (Nat × Int))).length = c.n - (c.split + width c) := by simp
  have hlen : ((((List.range (min c.n c.split)).map (fun i => (i, c.base + c.step * (i : Int))) : List (Nat × Int))) ++
      (((List.range (c.n - (c.split + width c))).map
        (fun j => (c.split + width c + j, c.base + c.step * ((c.split + width c + j : Nat) : Int))) : List (Nat × Int)))).length =
      min c.n c.split + (c.n - (c.split + width c)) := by rw [List.length_append, hlen1, hlenB]
  rw [hlen] at hk
  by_cases hk1 : k < min c.n c.split
  · have hget : ((((List.range (min c.n c.split)).map (fun i => (i, c.base + c.step * (i : Int))) : List (Nat × Int))) ++
        ((List.range (c.n - (c.split + width c))).map
          (fun j => (c.split + width c + j, c.base + c.step * ((c.split + width c + j : Nat) : Int)))))[k]? =
        some (k, c.base + c.step * (k : Int)) := by
      rw [List.getElem?_append_left (by rw [hlen1]; exact hk1)]
      simp [List.getElem?_map, List.getElem?_range, hk1]
    rw [hget]
    have hlt : k < c.split := Nat.lt_of_lt_of_le hk1 (Nat.min_le_right _ _)
    simp [hlt]
  · have hk2 : min c.n c.split ≤ k := Nat.not_lt.mp hk1
    have hk3 : k - min c.n c.split < c.n - (c.split + width c) := by omega
    have hget : ((((List.range (min c.n c.split)).map (fun i => (i, c.base + c.step * (i : Int))) : List (Nat × Int))) ++
        ((List.range (c.n - (c.split + width c))).map
          (fun j => (c.split + width c + j, c.base + c.step * ((c.split + width c + j : Nat) : Int)))))[k]? =
        some (c.split + width c + (k - min c.n c.split),
              c.base + c.step * ((c.split + width c + (k - min c.n c.split) : Nat) : Int)) := by
      rw [List.getElem?_append_right (by rw [hlen1]; omega)]
      rw [hlen1]
      simp only [List.getElem?_map, List.getElem?_range, hk3, ↓reduceIte, Option.map_some]
    rw [hget]
    have hge : ¬ k < c.split := by
      intro h
      have : k < min c.n c.split := Nat.lt_min.mpr ⟨by omega, h⟩
      omega
    have hAeq : min c.n c.split = c.split := by omega
    have hsk : c.split ≤ k := by omega
    simp [hge, hAeq]
    have hcancel := Nat.sub_add_cancel hsk
    omega

end DeletedSpan

/-! ## RotatedPage -/

namespace RotatedPage

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage

private theorem rows_map (c : Config) :
    rows layout c = (List.range c.n).map (fun i => (i, c.base + c.step * (((i + c.split % c.n) % c.n : Nat) : Int))) := by
  unfold rows
  apply range_filterMap_all
  intro i hi
  simp [layout]

theorem PermutationAggregate_sound : PermutationAggregate.Goal := by
  intro c
  by_cases hn : c.n = 0
  · simp [hn, rows, layout, LinearOrdinal.layout]
  · have hpos : 0 < c.n := Nat.pos_of_ne_zero hn
    let s := c.split % c.n
    have hs : s < c.n := Nat.mod_lt _ hpos
    let t := c.n - s
    have hts : t + s = c.n := by omega
    -- LinearOrdinal values
    have hlin : (rows LinearOrdinal.layout c).map Prod.snd =
        (List.range c.n).map (fun (i : Nat) => c.base + c.step * (i : Int)) := by
      unfold rows
      simp only [LinearOrdinal.layout, Option.map_some]
      rw [filterMap_some_eq_map]
      simp [List.map_map]
    -- RotatedPage values
    have hrot : (rows layout c).map Prod.snd =
        (List.range c.n).map (fun i => c.base + c.step * (((i + s) % c.n : Nat) : Int)) := by
      rw [rows_map]; simp [List.map_map, s]
    rw [hrot, hlin]
    -- split range n as range s ++ (range t).map (·+s)
    have hrange : List.range c.n = List.range s ++ (List.range t).map (fun x => s + x) := by
      have : List.range c.n = List.range (s + t) := by rw [Nat.add_comm, hts]
      rw [this, List.range_add]
    -- rotated index list is (range t).map (·+s) ++ range s
    have hidx : (List.range c.n).map (fun i => ((i + s) % c.n)) =
        (List.range t).map (fun i => s + i) ++ (List.range s) := by
      have hrange2 : List.range c.n = List.range t ++ (List.range s).map (fun x => t + x) := by
        have : List.range c.n = List.range (t + s) := by rw [hts]
        rw [this, List.range_add]
      rw [hrange2, List.map_append]
      congr 1
      · apply List.map_congr_left
        intro i hi
        have hi' : i < t := List.mem_range.mp hi
        have hlt : i + s < c.n := by omega
        rw [Nat.mod_eq_of_lt hlt]; omega
      · rw [List.map_map]
        refine (List.map_congr_left ?_).trans (List.map_id (List.range s))
        intro j hj
        have hj' : j < s := List.mem_range.mp hj
        have hval : t + j + s = c.n + j := by omega
        show (t + j + s) % c.n = j
        rw [hval, Nat.add_mod_left, Nat.mod_eq_of_lt (Nat.lt_trans hj' hs)]
    have hlinIdx : List.range c.n = List.range s ++ (List.range t).map (fun x => s + x) := hrange
    -- perm of index lists
    have hpermIdx : ((List.range c.n).map (fun i => ((i + s) % c.n))).Perm (List.range c.n) := by
      rw [hidx, hlinIdx]
      exact List.perm_append_comm
    -- lift to values
    have hval : ((List.range c.n).map (fun i => c.base + c.step * (((i + s) % c.n : Nat) : Int))) =
        ((List.range c.n).map (fun i => ((i + s) % c.n))).map (fun j => c.base + c.step * ((j : Nat) : Int)) := by
      simp [List.map_map]
    rw [hval]
    exact hpermIdx.map (fun j => c.base + c.step * ((j : Nat) : Int))

end RotatedPage


private theorem length_filterMap_isSome {α β : Type} (l : List α) (f : α → Option β) :
    (l.filterMap f).length = (l.filter (fun x => (f x).isSome)).length := by
  induction l with
  | nil => simp
  | cons x xs ih => cases h : f x <;> simp [h, ih]
/-! ## PagedTombstones -/

namespace PagedTombstones

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones

-- BEGIN ep-helpers
private theorem ep_succ_mod_step (n p : Nat) (hp : 0 < p) :
    (n + 1) % p = if n % p + 1 < p then n % p + 1 else 0 := by
  by_cases hp1 : p = 1
  · subst p
    have hm : n % 1 = 0 := by
      have := Nat.mod_lt n (by omega : 0 < 1)
      omega
    simp [hm]
  · have hp' : 1 < p := by omega
    have h1 : (n + 1) % p = (n % p + 1 % p) % p := Nat.add_mod n 1 p
    have h1p : 1 % p = 1 := Nat.mod_eq_of_lt hp'
    rw [h1, h1p]
    by_cases h : n % p + 1 < p
    · rw [Nat.mod_eq_of_lt h]
      simp [h]
    · have heq : n % p + 1 = p := by
        have hs := Nat.mod_lt n hp
        omega
      rw [heq, Nat.mod_self]
      simp [h]

private theorem ep_succ_div_step (n p : Nat) (hp : 0 < p) :
    (n + 1) / p = n / p + if n % p + 1 < p then 0 else 1 := by
  have hd := Nat.div_add_mod n p
  have hs := Nat.mod_lt n hp
  by_cases h : n % p + 1 < p
  · have hEq : n + 1 = p * (n / p) + (n % p + 1) := by grind
    rw [hEq, Nat.mul_add_div hp (n / p) (n % p + 1), Nat.div_eq_of_lt h]
    simp [h]
  · have heq : n % p + 1 = p := by omega
    have hEq : n + 1 = p * (n / p + 1) := by grind
    rw [hEq, Nat.mul_comm p (n / p + 1), Nat.mul_div_left _ hp]
    simp [h]
-- END ep-helpers

private theorem mod_two_mul_lt (i w : Nat) (hw : 0 < w) :
    ((i / w) % 2 == 0) = decide (i % (2 * w) < w) := by
  have hr : i % w < w := Nat.mod_lt i hw
  have h1 : i = w * (i / w) + i % w := (Nat.div_add_mod i w).symm
  by_cases hq : (i / w) % 2 = 0
  · have h2 : i / w = 2 * (i / w / 2) := by
      have hd := Nat.div_add_mod' (i / w) 2
      rw [hq, Nat.add_zero] at hd
      omega
    have hclaim : i % (2 * w) < w := by
      rw [h2] at h1
      rw [show w * (2 * (i / w / 2)) = 2 * w * (i / w / 2) from by
        rw [← Nat.mul_assoc, Nat.mul_comm w 2]] at h1
      rw [h1, Nat.mul_add_mod, Nat.mod_eq_of_lt (by omega : i % w < 2 * w)]
      exact hr
    rw [show ((i / w) % 2 == 0) = true from by simp [hq]]
    exact (decide_eq_true hclaim).symm
  · have h2 : i / w = 2 * (i / w / 2) + 1 := by
      have hd := Nat.div_add_mod' (i / w) 2
      have h1o : (i / w) % 2 = 1 := by
        have hmo : (i / w) % 2 < 2 := Nat.mod_lt _ (by omega : 0 < 2)
        omega
      omega
    have hclaim : i % (2 * w) = w + i % w := by
      rw [h2] at h1
      rw [show w * (2 * (i / w / 2) + 1) = 2 * w * (i / w / 2) + w from by
        rw [Nat.mul_add, ← Nat.mul_assoc, Nat.mul_comm w 2, Nat.mul_one]] at h1
      have he : (2 * w * (i / w / 2) + w + i % w) % (2 * w) = w + i % w := by
        rw [Nat.add_mod, Nat.mul_add_mod,
          Nat.mod_eq_of_lt (by omega : w < 2 * w),
          Nat.mod_eq_of_lt (by omega : i % w < 2 * w),
          Nat.mod_eq_of_lt (by omega : w + i % w < 2 * w)]
      rw [← he]
      exact congrArg (fun t => t % (2 * w)) h1
    rw [show ((i / w) % 2 == 0) = false from by simp [hq]]
    rw [hclaim]
    exact (decide_eq_false (by omega : ¬ (w + i % w < w))).symm

private theorem count_mod_lt (n w : Nat) (hw : 0 < w) :
    ((List.range n).filter (fun i => decide (i % (2 * w) < w))).length =
      (n / (2 * w)) * w + min w (n % (2 * w)) := by
  have hw2 : 0 < 2 * w := by omega
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append, List.length_append, ih]
      have hf : (([n] : List Nat).filter (fun i => decide (i % (2 * w) < w))).length =
          if decide (n % (2 * w) < w) then 1 else 0 := by
        by_cases hc : decide (n % (2 * w) < w) = true
        · simp [hc]
        · simp [hc]
      rw [hf, ep_succ_mod_step n (2 * w) hw2, ep_succ_div_step n (2 * w) hw2]
      by_cases h1 : n % (2 * w) + 1 < 2 * w
      · have e1 : (if n % (2 * w) + 1 < 2 * w then (0 : Nat) else 1) = 0 := by
          rw [if_pos h1]
        have e2 : (if n % (2 * w) + 1 < 2 * w then n % (2 * w) + 1 else 0) = n % (2 * w) + 1 := by
          rw [if_pos h1]
        by_cases h2 : n % (2 * w) < w
        · have h3 : n % (2 * w) + 1 ≤ w := by
            have hle : n % (2 * w) ≤ w - 1 := by omega
            omega
          have e3 : (if decide (n % (2 * w) < w) then (1 : Nat) else 0) = 1 := by
            simp [h2]
          have e4 : min w (n % (2 * w)) = n % (2 * w) := Nat.min_eq_right (by omega)
          have e5 : min w (n % (2 * w) + 1) = n % (2 * w) + 1 := Nat.min_eq_right h3
          rw [e1, e2, e3, e4, e5]
          simp only [Nat.add_zero]
          omega
        · have h3 : w ≤ n % (2 * w) + 1 := by omega
          have e3 : (if decide (n % (2 * w) < w) then (1 : Nat) else 0) = 0 := by
            simp [h2]
          have e4 : min w (n % (2 * w)) = w := Nat.min_eq_left (by omega)
          have e5 : min w (n % (2 * w) + 1) = w := Nat.min_eq_left h3
          rw [e1, e2, e3, e4, e5]
          simp only [Nat.add_zero]
      · have hs := Nat.mod_lt n hw2
        have hge : 2 * w ≤ n % (2 * w) + 1 := by omega
        have heq : n % (2 * w) = 2 * w - 1 := by omega
        have e1 : (if n % (2 * w) + 1 < 2 * w then (0 : Nat) else 1) = 1 := by
          rw [if_neg h1]
        have e2 : (if n % (2 * w) + 1 < 2 * w then n % (2 * w) + 1 else 0) = 0 := by
          rw [if_neg h1]
        have e3 : (if decide (n % (2 * w) < w) then (1 : Nat) else 0) = 0 := by
          have hnot : ¬ n % (2 * w) < w := by omega
          simp [hnot]
        have e4 : min w (n % (2 * w)) = w := by
          rw [heq]
          exact Nat.min_eq_left (by omega)
        have e5 : min w 0 = 0 := Nat.min_eq_right (by omega : (0 : Nat) ≤ w)
        rw [e1, e2, e3, e4, e5, Nat.add_mul, Nat.one_mul]

theorem SkipEpochs_sound : SkipEpochs.Goal := by
  intro c
  have hw : 0 < width c := by simp [width]
  unfold rows
  simp only [layout]
  have hlen : ((List.range c.n).filterMap (fun i =>
      (if (i / width c) % 2 == 0 then some (c.base + c.step * (i : Int)) else none).map (fun v => (i, v)))).length =
      ((List.range c.n).filter (fun i => (i / width c) % 2 == 0)).length := by
    rw [length_filterMap_isSome]
    apply congrArg List.length
    apply List.filter_congr
    intro i hi
    by_cases h : (i / width c) % 2 = 0 <;> simp [h, Option.isSome]
  rw [hlen]
  have hpred : ((List.range c.n).filter (fun i => (i / width c) % 2 == 0)).length =
      ((List.range c.n).filter (fun i => decide (i % (2 * width c) < width c))).length := by
    apply congrArg List.length
    apply List.filter_congr
    intro i hi
    exact mod_two_mul_lt i (width c) hw
  rw [hpred, count_mod_lt c.n (width c) hw]

end PagedTombstones

/-! ## GrayTile -/

namespace GrayTile

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile

-- BEGIN ep-helpers
private def ep_gray (r : Nat) : Nat := r ^^^ (r / 2)

private theorem ep_xor_div_two (a b : Nat) : (a ^^^ b) / 2 = (a / 2) ^^^ (b / 2) := by
  apply Nat.eq_of_testBit_eq
  intro i
  simp [Nat.testBit_div_two, Nat.testBit_xor]

private theorem ep_xor_eq_zero_iff_eq (a b : Nat) : a ^^^ b = 0 ↔ a = b := by
  constructor
  · intro h
    apply Nat.eq_of_testBit_eq
    intro i
    have ht := congrArg (fun n : Nat => n.testBit i) h
    simp [Nat.testBit_xor] at ht
    cases ha : a.testBit i <;> cases hb : b.testBit i <;> simp [ha, hb] at ht ⊢
  · intro h
    subst b
    exact Nat.xor_self a

private def ep_invGray (n : Nat) : Nat :=
  if n = 0 then 0 else n ^^^ ep_invGray (n / 2)
termination_by n
decreasing_by exact Nat.div_lt_self (by omega) (by decide : 1 < 2)

private theorem ep_invGray_half (n : Nat) : ep_invGray n / 2 = ep_invGray n ^^^ n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · rw [ep_invGray]
        simp [hn]
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hih := ih (n / 2) hdiv
        rw [ep_invGray]
        simp [hn]
        rw [ep_xor_div_two, hih]
        calc
          n / 2 ^^^ (ep_invGray (n / 2) ^^^ n / 2) =
              (ep_invGray (n / 2) ^^^ n / 2) ^^^ n / 2 := by rw [Nat.xor_comm]
          _ = ep_invGray (n / 2) ^^^ (n / 2 ^^^ n / 2) := by rw [Nat.xor_assoc]
          _ = ep_invGray (n / 2) := by rw [Nat.xor_self, Nat.xor_zero]
          _ = (n ^^^ ep_invGray (n / 2)) ^^^ n := by
            rw [Nat.xor_comm n (ep_invGray (n / 2))]
            rw [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

private theorem ep_gray_invGray (n : Nat) : ep_gray (ep_invGray n) = n := by
  unfold ep_gray
  rw [ep_invGray_half]
  rw [← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]

private theorem ep_gray_ne_zero_of_pos {n : Nat} (hn : 0 < n) : ep_gray n ≠ 0 := by
  intro h
  unfold ep_gray at h
  have heq : n = n / 2 := (ep_xor_eq_zero_iff_eq n (n / 2)).mp h
  omega

private theorem ep_invGray_gray (n : Nat) : ep_invGray (ep_gray n) = n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · rw [ep_invGray]
        simp [hn, ep_gray]
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hih := ih (n / 2) hdiv
        have hpos : ep_gray n ≠ 0 := ep_gray_ne_zero_of_pos (by omega)
        have hhalf : ep_gray n / 2 = ep_gray (n / 2) := by
          unfold ep_gray
          rw [ep_xor_div_two]
        rw [ep_invGray]
        simp [hpos]
        rw [hhalf, hih]
        unfold ep_gray
        rw [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

private theorem ep_invGray_lt_two_pow (d n : Nat) (hn : n < 2 ^ d) : ep_invGray n < 2 ^ d := by
  induction n using Nat.strongRecOn generalizing d with
  | ind n ih =>
      by_cases hn0 : n = 0
      · rw [ep_invGray]
        simp [hn0]
        exact Nat.two_pow_pos d
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hndiv : n / 2 < 2 ^ d := by omega
        have hih := ih (n / 2) hdiv d hndiv
        rw [ep_invGray]
        simp [hn0]
        exact Nat.xor_lt_two_pow hn hih

private theorem ep_gray_lt_two_pow (d n : Nat) (hn : n < 2 ^ d) : ep_gray n < 2 ^ d := by
  unfold ep_gray
  exact Nat.xor_lt_two_pow hn (by omega)



private theorem ep_gray_inj (a b : Nat) (h : ep_gray a = ep_gray b) : a = b := by
  have hh := congrArg ep_invGray h
  simpa [ep_invGray_gray] using hh

private def ep_grayIdx (w i : Nat) : Nat := (i / w) * w + ep_gray (i % w)

private theorem ep_grayIdx_div (d i : Nat) : ep_grayIdx (2 ^ d) i / 2 ^ d = i / 2 ^ d := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := ep_gray (i % w)
  have hs : s < w := by
    dsimp [s]
    exact ep_gray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) / w = i / w
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_div_left s (i / w) hw, Nat.div_eq_of_lt hs, Nat.zero_add]

private theorem ep_grayIdx_mod (d i : Nat) : ep_grayIdx (2 ^ d) i % 2 ^ d = ep_gray (i % 2 ^ d) := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := ep_gray (i % w)
  have hs : s < w := by
    dsimp [s]
    exact ep_gray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) % w = s
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hs]

private theorem ep_grayIdx_bound (d n i : Nat) (hn : n % 2 ^ d = 0) (hi : i < n) :
    ep_grayIdx (2 ^ d) i < n := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  have hnd : n = (n / w) * w := by
    have hd := Nat.div_add_mod n w
    rw [hn, Nat.add_zero] at hd
    rw [Nat.mul_comm] at hd
    exact hd.symm
  have hklt : i / w < n / w := by
    rw [Nat.div_lt_iff_lt_mul hw]
    rw [← hnd]
    exact hi
  calc
    ep_grayIdx w i = (i / w) * w + (ep_grayIdx w i) % w := by
      have hdec := Nat.div_add_mod (ep_grayIdx w i) w
      rw [show ep_grayIdx w i / w = i / w by simpa [w] using ep_grayIdx_div d i] at hdec
      simpa [Nat.mul_comm] using hdec.symm
    _ < ((i / w) + 1) * w := by
      rw [Nat.add_mul, Nat.one_mul]
      have hs : (ep_grayIdx w i) % w < w := Nat.mod_lt _ hw
      exact Nat.add_lt_add_left hs ((i / w) * w)
    _ ≤ n := by
      rw [hnd]
      exact Nat.mul_le_mul_right w (by omega)

private theorem ep_grayIdx_inj (d a b : Nat)
    (h : ep_grayIdx (2 ^ d) a = ep_grayIdx (2 ^ d) b) : a = b := by
  let w := 2 ^ d
  have hd := congrArg (fun x : Nat => x / w) h
  have hm := congrArg (fun x : Nat => x % w) h
  change ep_grayIdx w a / w = ep_grayIdx w b / w at hd
  change ep_grayIdx w a % w = ep_grayIdx w b % w at hm
  rw [show ep_grayIdx w a / w = a / w by simpa [w] using ep_grayIdx_div d a] at hd
  rw [show ep_grayIdx w b / w = b / w by simpa [w] using ep_grayIdx_div d b] at hd
  rw [show ep_grayIdx w a % w = ep_gray (a % w) by simpa [w] using ep_grayIdx_mod d a] at hm
  rw [show ep_grayIdx w b % w = ep_gray (b % w) by simpa [w] using ep_grayIdx_mod d b] at hm
  have hmod : a % w = b % w := ep_gray_inj _ _ hm
  calc
    a = (a / w) * w + a % w := by simpa [Nat.mul_comm] using (Nat.div_add_mod a w).symm
    _ = (b / w) * w + b % w := by rw [hd, hmod]
    _ = b := by simpa [Nat.mul_comm] using Nat.div_add_mod b w

private def ep_idxInv (w i : Nat) : Nat := (i / w) * w + ep_invGray (i % w)

private theorem ep_idxInv_div (d i : Nat) : ep_idxInv (2 ^ d) i / 2 ^ d = i / 2 ^ d := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := ep_invGray (i % w)
  have hs : s < w := by
    dsimp [s]
    exact ep_invGray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) / w = i / w
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_div_left s (i / w) hw, Nat.div_eq_of_lt hs, Nat.zero_add]

private theorem ep_idxInv_mod (d i : Nat) : ep_idxInv (2 ^ d) i % 2 ^ d = ep_invGray (i % 2 ^ d) := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := ep_invGray (i % w)
  have hs : s < w := by
    dsimp [s]
    exact ep_invGray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) % w = s
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hs]

private theorem ep_idxInv_bound (d n x : Nat) (hn : n % 2 ^ d = 0) (hx : x < n) :
    ep_idxInv (2 ^ d) x < n := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  have hnd : n = (n / w) * w := by
    have hd := Nat.div_add_mod n w
    rw [hn, Nat.add_zero] at hd
    rw [Nat.mul_comm] at hd
    exact hd.symm
  have hklt : x / w < n / w := by
    rw [Nat.div_lt_iff_lt_mul hw]
    rw [← hnd]
    exact hx
  calc
    ep_idxInv w x = (x / w) * w + (ep_idxInv w x) % w := by
      have hdec := Nat.div_add_mod (ep_idxInv w x) w
      rw [show ep_idxInv w x / w = x / w by simpa [w] using ep_idxInv_div d x] at hdec
      simpa [Nat.mul_comm] using hdec.symm
    _ < ((x / w) + 1) * w := by
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_lt_add_left (Nat.mod_lt _ hw) ((x / w) * w)
    _ ≤ n := by
      rw [hnd]
      exact Nat.mul_le_mul_right w (by omega)

private theorem ep_grayIdx_idxInv (d x : Nat) : ep_grayIdx (2 ^ d) (ep_idxInv (2 ^ d) x) = x := by
  let w := 2 ^ d
  have hiDiv : ep_idxInv w x / w = x / w := by simpa [w] using ep_idxInv_div d x
  have hiMod : ep_idxInv w x % w = ep_invGray (x % w) := by simpa [w] using ep_idxInv_mod d x
  have hgi : ep_gray (ep_invGray (x % w)) = x % w := ep_gray_invGray (x % w)
  unfold ep_grayIdx
  rw [hiDiv, hiMod, hgi]
  simpa [Nat.mul_comm] using Nat.div_add_mod x w

private theorem ep_grayIdx_perm (d n : Nat) (hn : n % 2 ^ d = 0) :
    List.Perm ((List.range n).map (ep_grayIdx (2 ^ d))) (List.range n) := by
  have hnd : ((List.range n).map (ep_grayIdx (2 ^ d))).Nodup := by
    exact List.Pairwise.map (R := fun a b : Nat => a ≠ b) (S := fun a b : Nat => a ≠ b)
      (ep_grayIdx (2 ^ d)) (fun a b hne hEq => hne (ep_grayIdx_inj d a b hEq)) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnd List.nodup_range).mpr
  intro x
  constructor
  · intro hx
    obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hx
    exact List.mem_range.mpr (ep_grayIdx_bound d n i hn (List.mem_range.mp hi))
  · intro hx
    refine List.mem_map.mpr ⟨ep_idxInv (2 ^ d) x,
      List.mem_range.mpr (ep_idxInv_bound d n x hn (List.mem_range.mp hx)), ?_⟩
    exact ep_grayIdx_idxInv d x



private theorem ep_rows_grayTile (c : Config) :
    (rows
        GrayTile.layout c).map Prod.snd =
      (List.range c.n).map (fun i : Nat =>
        c.base + c.step * ((ep_grayIdx (width c) i : Nat) : Int)) := by
  let w := width c
  have hrows : rows
        GrayTile.layout c =
      (List.range c.n).map (fun i : Nat =>
        (i, c.base + c.step * ((ep_grayIdx w i : Nat) : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * ((ep_grayIdx w i : Nat) : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [GrayTile.layout,
      ep_grayIdx, ep_gray, w]
  rw [hrows]
  simp [w, List.map_map, Function.comp_def]

private theorem ep_rows_linearOrdinal (c : Config) :
    (rows
        LinearOrdinal.layout c).map Prod.snd =
      (List.range c.n).map (fun (j : Nat) => c.base + c.step * (j : Int)) := by
  have hrows : rows
        LinearOrdinal.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * (i : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [LinearOrdinal.layout]
  rw [hrows]
  simp [List.map_map, Function.comp_def]
-- END ep-helpers

theorem PowerOfTwo_sound : PowerOfTwo.Goal := by
  intro c d hw hn
  rw [ep_rows_grayTile c, ep_rows_linearOrdinal c]
  rw [hw] at hn ⊢
  by_cases hs : c.step = 0
  · simp [hs]
  · have hp := ep_grayIdx_perm d c.n hn
    have hval := List.Perm.map (fun j : Nat => c.base + c.step * (j : Int)) hp
    simpa [ep_grayIdx, Function.comp_def] using hval

end GrayTile

/-! ## Generic lane decomposition for ResidueJoin / DeleteProgression / AlignedProduct -/

private theorem sum_flatMap_int {α β : Type} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [List.map_append, List.sum_append, ih]

private theorem sum_flatMap_nat {α β : Type} (l : List α) (f : α → List β) (g : β → Nat) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [List.map_append, List.sum_append, ih]

private theorem sum_filter_flatMap {α β : Type} (l : List α) (f : α → List β)
    (p : β → Bool) (g : β → Int) :
    (((l.flatMap f).filter p).map g).sum = (l.map (fun a => (((f a).filter p).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [List.filter_append, List.map_append, List.sum_append, ih]

-- BEGIN ep-helpers
private theorem ep_list_sum_append_int (l₁ l₂ : List Int) :
    (l₁ ++ l₂).sum = l₁.sum + l₂.sum := by
  induction l₁ with
  | nil => simp
  | cons x xs ih => simp [ih]; omega

private theorem ep_sum_map_append {α β : Type} (l₁ l₂ : List α) (f : α → List β) (g : β → Int) :
    ((l₁ ++ l₂).flatMap f |>.map g).sum = ((l₁.flatMap f).map g).sum + ((l₂.flatMap f).map g).sum := by
  simp [List.flatMap_append, List.map_append, ep_list_sum_append_int]

private theorem ep_map_flatMap_sum {α β : Type} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_cons, List.map_append, ep_list_sum_append_int, ih]

private theorem ep_map_flatMap_sum_nat {α β : Type} (l : List α) (f : α → List β) (g : β → Nat) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_cons, List.map_append, List.sum_append, ih]

private theorem ep_length_flatMap {α β : Type} (l : List α) (f : α → List β) :
    (l.flatMap f).length = (l.map (fun a => (f a).length)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, ih]

private theorem ep_filter_flatMap {α β : Type} (l : List α) (f : α → List β) (p : β → Bool) :
    (l.flatMap f).filter p = l.flatMap (fun a => (f a).filter p) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, List.filter_append, ih]

private theorem ep_sum_map_congr {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem ep_countP_eq_length_filter {α : Type} (p : α → Bool) (l : List α) :
    l.countP p = (l.filter p).length := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem ep_sum_natCast_eq {α : Type} (l : List α) (g : α → Nat) :
    ((l.map (fun x => (g x : Int))).sum) = (((l.map g).sum : Nat) : Int) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Int.natCast_add]

/-! ## Lane points -/

private def ep_allPoints (ls : List Lane) : List (Nat × Int) := ls.flatMap lanePoints

private theorem ep_allPoints_cons (a : Lane) (ls : List Lane) :
    ep_allPoints (a :: ls) = lanePoints a ++ ep_allPoints ls := by
  simp [ep_allPoints, List.flatMap_cons]

private theorem ep_allPoints_map_sum (ls : List Lane) (f : Nat × Int → Int) :
    ((ep_allPoints ls).map f).sum = (ls.map (fun a => ((lanePoints a).map f).sum)).sum := by
  unfold ep_allPoints
  rw [ep_map_flatMap_sum]

/-! ## Arithmetic helpers -/

private theorem ep_choose2_succ (k : Nat) :
    TinylambOpt.ResearchGoals.PairAggregates.choose2 (k + 1) =
      TinylambOpt.ResearchGoals.PairAggregates.choose2 k + k := by
  unfold TinylambOpt.ResearchGoals.PairAggregates.choose2
  by_cases hk : k = 0
  · simp [hk]
  · have hkm1 : k + 1 - 1 = k := by omega
    rw [hkm1]
    have hmul1 : (k + 1) * k = k * k + k := by rw [Nat.add_mul, Nat.one_mul]
    have hmul2 : k * (k - 1) = k * k - k := by rw [Nat.mul_sub_left_distrib, Nat.mul_one]
    have hle : k ≤ k * k := by simpa using Nat.mul_le_mul_left k (show 1 ≤ k by omega)
    have hEq : (k + 1) * k = k * (k - 1) + 2 * k := by rw [hmul1, hmul2]; omega
    rw [hEq]
    exact Nat.add_mul_div_left (k * (k - 1)) k (by decide : 0 < 2)

private theorem ep_tri_succ (k : Nat) : tri (k + 1) = tri k + (k : Int) := by
  dsimp [tri]
  rw [ep_choose2_succ]
  simp [Int.natCast_add]

private theorem ep_squares_succ (k : Nat) :
    squares (k + 1) = squares k + (k : Int) * (k : Int) := by
  unfold squares
  have hkm1 : k + 1 - 1 = k := by omega
  rw [hkm1]
  have h2 : 2 * (k + 1) - 1 = 2 * k + 1 := by omega
  rw [h2]
  have hp : (k + 1) * k * (2 * k + 1) = k * (k - 1) * (2 * k - 1) + 6 * (k * k) := by
    by_cases hk : k = 0
    · simp [hk]
    · obtain ⟨m, rfl⟩ := Nat.exists_eq_succ_of_ne_zero hk
      have hm : m + 1 - 1 = m := by omega
      have h2m : 2 * (m + 1) - 1 = 2 * m + 1 := by omega
      rw [hm, h2m]
      change (m + 1 + 1) * (m + 1) * (2 * (m + 1) + 1) =
        (m + 1) * m * (2 * m + 1) + 6 * ((m + 1) * (m + 1))
      grind
  rw [hp, Nat.add_mul_div_left _ _ (by decide : 0 < 6)]
  simp [Int.natCast_add, Int.natCast_mul]

private theorem ep_partialSum_succ (a : Lane) (k : Nat) :
    partialSum a (k + 1) = partialSum a k + (a.base + (k : Int) * a.step) := by
  rw [partialSum, partialSum, ep_tri_succ]
  simp [Int.natCast_add, Int.add_mul]
  omega

private theorem ep_partialSum_eq_sum (a : Lane) (k : Nat) :
    partialSum a k = ((List.range k).map (fun i : Nat => a.base + (i : Int) * a.step)).sum := by
  induction k with
  | zero => simp [partialSum, tri, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ k ih => rw [ep_partialSum_succ, ih, List.range_succ, List.map_append, List.sum_append]; simp


/-! ## Progression membership and intersection -/

private def ep_progVal (a : Progression) (k : Nat) : Int := a.lo + (k : Int) * a.step

private def ep_progMem (a : Progression) (v : Int) : Prop :=
  ∃ k, k < a.n ∧ v = ep_progVal a k

private theorem ep_contains_iff_mem (a : Progression) (v : Int) :
    contains a v = true ↔ ep_progMem a v := by
  unfold contains lastVal ep_progMem ep_progVal
  constructor
  · intro h
    have h' : 0 < a.n ∧ a.lo ≤ v ∧ v ≤ a.lo + ((a.n - 1 : Nat) : Int) * a.step ∧
        (a.step == 0 || (v - a.lo) % (a.step : Int) == 0) = true := by
      simpa only [Bool.and_eq_true, decide_eq_true_eq, and_assoc] using h
    rcases h' with ⟨hn, hlo, hhi, hmod⟩
    by_cases hs : a.step == 0
    · have hs' : a.step = 0 := Nat.eq_of_beq_eq_true hs
      refine ⟨0, hn, ?_⟩
      simp only [hs', Int.mul_zero, Int.add_zero] at hhi hlo ⊢
      omega
    · have hs' : a.step ≠ 0 := by simpa using hs
      have hpos : (0 : Int) < a.step := by exact_mod_cast Nat.pos_of_ne_zero hs'
      have hmod' : (v - a.lo) % (a.step : Int) = 0 := by
        simpa [hs] using hmod
      let q := (v - a.lo) / a.step
      have hqmul : a.step * q = v - a.lo := by
        simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod'
      have hqmul' : q * a.step = v - a.lo := by simpa only [Int.mul_comm] using hqmul
      have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hpos
      have hqle : q ≤ (a.n - 1 : Nat) := by
        apply Int.le_of_mul_le_mul_left (b := q) (c := (a.n - 1 : Nat)) _ hpos
        change a.step * ((v - a.lo) / a.step) ≤ a.step * (a.n - 1 : Nat)
        have hh : a.step * ((v - a.lo) / a.step) = v - a.lo := by
          simpa only [Int.mul_comm] using Int.ediv_mul_cancel_of_emod_eq_zero hmod'
        rw [hh]
        have hh2 := Int.sub_le_sub_right hhi a.lo
        have hcancel : a.lo + ((a.n - 1 : Nat) : Int) * a.step - a.lo =
            ((a.n - 1 : Nat) : Int) * a.step := by omega
        rw [hcancel] at hh2
        simpa only [Int.mul_comm] using hh2
      have hqnat : q.toNat < a.n := by
        have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
        omega
      refine ⟨q.toNat, hqnat, ?_⟩
      rw [Int.toNat_of_nonneg hqnon]
      symm
      calc
        a.lo + q * a.step = a.lo + (v - a.lo) := congrArg (fun x : Int => a.lo + x) hqmul'
        _ = v := by omega
  · rintro ⟨k, hk, rfl⟩
    change (decide (0 < a.n) && decide (a.lo ≤ a.lo + (k : Int) * a.step) &&
      decide (a.lo + (k : Int) * a.step ≤ a.lo + ((a.n - 1 : Nat) : Int) * a.step) &&
      (a.step == 0 || (a.lo + (k : Int) * a.step - a.lo) % (a.step : Int) == 0)) = true
    have hsub : a.lo + (k : Int) * a.step - a.lo = (k : Int) * a.step := by omega
    rw [hsub]
    by_cases hs : a.step = 0
    · simp [hs]
      omega
    · have hpos : (0 : Int) < a.step := by exact_mod_cast Nat.pos_of_ne_zero hs
      have hkint : (k : Int) ≤ (a.n - 1 : Nat) := by exact_mod_cast (by omega)
      have hmul : a.step * (k : Int) ≤ a.step * (a.n - 1 : Nat) := by
        simpa only [Int.mul_comm] using Int.mul_le_mul_of_nonneg_left hkint (Int.le_of_lt hpos)
      have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
        simpa only [Int.mul_comm] using hmul
      simp only [Bool.and_eq_true, decide_eq_true_eq, and_assoc]
      refine ⟨by omega, ?_, ?_, ?_⟩
      · exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hpos))
      · exact Int.add_le_add_left hmul' a.lo
      · rw [Bool.or_eq_true]
        right
        change (((k : Int) * a.step) % a.step == 0) = true
        rw [beq_iff_eq]
        apply Int.emod_eq_zero_of_dvd
        exact ⟨(k : Int), Int.mul_comm _ _⟩

private theorem ep_bezout_spec (a b : Nat) :
    (a.gcd b : Int) = (a : Int) * (bezout a b).1 + (b : Int) * (bezout a b).2 := by
  induction b using Nat.strongRecOn generalizing a with
  | ind b ih =>
      by_cases hb : b = 0
      · subst b
        simp [bezout]
      · have hmod : a % b < b := Nat.mod_lt a (Nat.pos_of_ne_zero hb)
        have hrec := ih (a % b) hmod b
        have hfst : (bezout a b).1 = (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        have hsnd : (bezout a b).2 =
            (bezout b (a % b)).1 - ((a / b : Nat) : Int) * (bezout b (a % b)).2 := by
          rw [bezout.eq_1]
          simp [hb]
        rw [hfst, hsnd]
        rcases hbez : bezout b (a % b) with ⟨x, y⟩
        simp only [hbez]
        have hgcd : a.gcd b = b.gcd (a % b) := by
          rw [Nat.gcd_comm a b, Nat.gcd_rec b a, Nat.gcd_comm]
        rw [hgcd, hrec]
        have hd : (a : Int) = (b : Int) * ((a / b : Nat) : Int) + ((a % b : Nat) : Int) := by
          exact_mod_cast (Nat.div_add_mod a b).symm
        rw [hd]
        grind

private theorem ep_reduced_coprime (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    (a / a.gcd b).gcd (b / a.gcd b) = 1 := by
  exact Nat.gcd_div_gcd_div_gcd_of_pos_left ha

private theorem ep_bezout_reduced_inverse (a b : Nat) (ha : 0 < a) (hb : 0 < b) :
    ((((a / a.gcd b : Nat) : Int) * (bezout (a / a.gcd b) (b / a.gcd b)).1) - 1) %
        (b / a.gcd b : Nat) = 0 := by
  let g := a.gcd b
  let A := a / g
  let B := b / g
  have hg : 0 < g := Nat.gcd_pos_of_pos_left b ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left b ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right a hb
  have hcop : A.gcd B = 1 := by
    simpa [A, B, g] using ep_reduced_coprime a b ha hb
  have hspec := ep_bezout_spec A B
  rw [hcop] at hspec
  simp only [Int.ofNat_one] at hspec
  apply Int.emod_eq_zero_of_dvd
  refine ⟨-(bezout A B).2, ?_⟩
  change (A : Int) * (bezout A B).1 - 1 = (B : Int) * -(bezout A B).2
  rw [hspec]
  grind

private theorem ep_mem_of_bounds_dvd (a : Progression) (v : Int) (hn : 0 < a.n) (hs : 0 < a.step)
    (hlo : a.lo ≤ v) (hhi : v ≤ lastVal a) (hdvd : (a.step : Int) ∣ v - a.lo) :
    contains a v = true := by
  obtain ⟨q, hq⟩ := hdvd
  have hpos : (0 : Int) < a.step := by exact_mod_cast hs
  have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hpos
  have hqle : q ≤ (a.n - 1 : Nat) := by
    apply Int.le_of_mul_le_mul_left (b := q) (c := (a.n - 1 : Nat)) _ hpos
    rw [← hq]
    unfold lastVal at hhi
    rw [Int.mul_comm ((a.n - 1 : Nat) : Int) a.step] at hhi
    omega
  apply (ep_contains_iff_mem a v).mpr
  refine ⟨q.toNat, ?_, ?_⟩
  · have hle : q.toNat ≤ a.n - 1 := (Int.toNat_le (m := q) (n := a.n - 1)).mpr hqle
    omega
  · unfold ep_progVal
    rw [Int.toNat_of_nonneg hqnon]
    change v = a.lo + q * a.step
    rw [Int.mul_comm a.step q] at hq
    omega

private theorem ep_span_contains_iff (first step high x : Int) (hs : 0 < step) :
    let n := if first ≤ high then ((high - first) / step).toNat + 1 else 0
    contains ⟨first, step.toNat, n⟩ x = true ↔
      first ≤ x ∧ x ≤ high ∧ (step : Int) ∣ x - first := by
  dsimp only
  constructor
  · intro hx
    have hm := (ep_contains_iff_mem ⟨first, step.toNat, if first ≤ high then ((high - first) / step).toNat + 1 else 0⟩ x).mp hx
    obtain ⟨k, hk, hval⟩ := hm
    unfold ep_progVal at hval
    have hstepcast : (step.toNat : Int) = step := Int.toNat_of_nonneg (by omega)
    simp only [hstepcast] at hval
    by_cases hfh : first ≤ high
    · change k < (if first ≤ high then ((high - first) / step).toNat + 1 else 0) at hk
      rw [if_pos hfh] at hk
      have ht : 0 ≤ (high - first) / step :=
        Int.ediv_nonneg (by omega) (Int.le_of_lt hs)
      have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
        Int.toNat_of_nonneg ht
      have hkq : (k : Int) ≤ (high - first) / step :=
        (Int.le_ediv_iff_mul_le hs).mpr (by
          have hkNat : k ≤ ((high - first) / step).toNat := by omega
          have hkInt : (k : Int) ≤ (((high - first) / step).toNat : Int) := by exact_mod_cast hkNat
          calc
            (k : Int) * step ≤ (((high - first) / step).toNat : Int) * step :=
              Int.mul_le_mul_of_nonneg_right hkInt (Int.le_of_lt hs)
            _ = ((high - first) / step) * step := by rw [htcast]
            _ ≤ high - first := Int.ediv_mul_le _ (by omega))
      have hkle : (k : Int) * step ≤ high - first :=
        (Int.le_ediv_iff_mul_le hs).mp hkq
      refine ⟨?_, ?_, ?_⟩
      · rw [hval]
        exact Int.le_add_of_nonneg_right (Int.mul_nonneg (by omega) (Int.le_of_lt hs))
      · calc
          x = first + (k : Int) * step := hval
          _ ≤ first + (high - first) := Int.add_le_add_left hkle first
          _ = high := by omega
      · refine ⟨(k : Int), ?_⟩
        calc
          x - first = (k : Int) * step := by omega
          _ = step * (k : Int) := Int.mul_comm _ _
    · simp [hfh] at hk
  · rintro ⟨hlo, hhi, hdvd⟩
    have hfh : first ≤ high := by omega
    rw [if_pos hfh]
    obtain ⟨q, hq⟩ := hdvd
    have hqnon : 0 ≤ q := Int.nonneg_of_mul_nonneg_right (by omega) hs
    have htcast : (((high - first) / step).toNat : Int) = (high - first) / step :=
      Int.toNat_of_nonneg (Int.ediv_nonneg (by omega) (Int.le_of_lt hs))
    have hqle : q ≤ (high - first) / step := by
      apply (Int.le_ediv_iff_mul_le hs).mpr
      calc
        q * step = x - first := by rw [Int.mul_comm q step]; exact hq.symm
        _ ≤ high - first := Int.sub_le_sub_right hhi first
    apply (ep_contains_iff_mem ⟨first, step.toNat, ((high - first) / step).toNat + 1⟩ x).mpr
    refine ⟨q.toNat, ?_, ?_⟩
    · have hle : q.toNat ≤ ((high - first) / step).toNat := by
        apply (Int.toNat_le (m := q) (n := ((high - first) / step).toNat)).mpr
        rw [htcast]
        exact hqle
      exact Nat.lt_succ_of_le hle
    · unfold ep_progVal
      rw [Int.toNat_of_nonneg hqnon]
      change x = first + q * (step.toNat : Int)
      rw [Int.toNat_of_nonneg (by omega : 0 ≤ step)]
      rw [Int.mul_comm step q] at hq
      omega

private theorem ep_root_dvd (as bs : Nat) (ha : 0 < as) (hb : 0 < bs) (alo blo gap : Int)
    (hgapdef : gap = blo - alo) (hgap : (as.gcd bs : Int) ∣ gap) :
    let g := as.gcd bs
    let A := as / g
    let B := bs / g
    let root := alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int))
    (as : Int) ∣ root - alo ∧ (bs : Int) ∣ root - blo := by
  dsimp only
  let g := as.gcd bs
  let A := as / g
  let B := bs / g
  let t := (gap / (g : Int) * (bezout A B).1) % (B : Int)
  have hg : 0 < g := Nat.gcd_pos_of_pos_left bs ha
  have hA : 0 < A := Nat.div_gcd_pos_of_pos_left bs ha
  have hB : 0 < B := Nat.div_gcd_pos_of_pos_right as hb
  have hasg : (as : Int) = (g : Int) * (A : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_left as bs)).symm
  have hbsg : (bs : Int) = (g : Int) * (B : Int) := by
    exact_mod_cast (Nat.mul_div_cancel' (Nat.gcd_dvd_right as bs)).symm
  obtain ⟨z, hz⟩ := hgap
  have hzdiv : gap / (g : Int) = z := Int.ediv_eq_of_eq_mul_right (by omega) hz
  have hinv := ep_bezout_reduced_inverse as bs ha hb
  have hinv' : (B : Int) ∣ (A : Int) * (bezout A B).1 - 1 :=
    Int.dvd_of_emod_eq_zero (by simpa [A, B, g] using hinv)
  have hmod := Int.mul_ediv_add_emod (z * (bezout A B).1) (B : Int)
  have hrem : z * (bezout A B).1 - t =
      (B : Int) * ((z * (bezout A B).1) / (B : Int)) := by
    dsimp [t]
    rw [hzdiv]
    omega
  have hdiff : (B : Int) ∣ z * ((A : Int) * (bezout A B).1 - 1) :=
    Int.dvd_mul_of_dvd_right hinv'
  have hmult : (B : Int) ∣ (A : Int) * (z * (bezout A B).1 - t) :=
    Int.dvd_mul_of_dvd_right (Int.dvd_of_emod_eq_zero (by
      rw [hrem]
      exact Int.emod_eq_zero_of_dvd ⟨_, rfl⟩))
  have hcomb : (B : Int) ∣ (A : Int) * t - z := by
    have heq : z * ((A : Int) * (bezout A B).1 - 1) -
        (A : Int) * (z * (bezout A B).1 - t) = (A : Int) * t - z := by grind
    rw [← heq]
    exact Int.dvd_sub hdiff hmult
  have hbs : (bs : Int) ∣ (as : Int) * t - gap := by
    rw [hbsg, hasg, hz]
    have hh := Int.mul_dvd_mul_left (g : Int) hcomb
    simpa only [Int.mul_sub, Int.mul_assoc] using hh
  constructor
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - alo =
        (as : Int) * t := by dsimp [t]; omega
    rw [hroot]
    exact ⟨t, rfl⟩
  · have hroot : alo + (as : Int) * ((gap / (g : Int) * (bezout A B).1) % (B : Int)) - blo =
        (as : Int) * t - gap := by dsimp [t]; omega
    rw [hroot]
    exact hbs

private theorem ep_first_root_dvd (low root first period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) : period ∣ first - root := by
  have hm := Int.mul_ediv_add_emod (root - low) period
  refine ⟨-((root - low) / period), ?_⟩
  grind

private theorem ep_first_le_of_congruence (low root first x period : Int) (hp : 0 < period)
    (hfirst : first = low + (root - low) % period) (hx : low ≤ x)
    (hxdvd : period ∣ x - root) : first ≤ x := by
  have hremnon : 0 ≤ (root - low) % period := Int.emod_nonneg _ (by omega)
  have hremlt : (root - low) % period < period := Int.emod_lt_of_pos _ hp
  have hflow : low ≤ first := by omega
  have hfhigh : first < low + period := by omega
  have hfroot := ep_first_root_dvd low root first period hp hfirst
  by_cases hle : first ≤ x
  · exact hle
  · exfalso
    have hlt : x < first := by omega
    have hpos : 0 < first - x := by omega
    have hltp : first - x < period := by omega
    have hdiff : period ∣ first - x := by
      have heq : (first - root) - (x - root) = first - x := by omega
      rw [← heq]
      exact Int.dvd_sub hfroot hxdvd
    obtain ⟨k, hk⟩ := hdiff
    have hkpos : 0 < k := Int.pos_of_mul_pos_right (by omega) hp
    have hkge : 1 ≤ k := by omega
    have hmul := Int.mul_le_mul_of_nonneg_left hkge (Int.le_of_lt hp)
    omega

private theorem ep_contains_const_iff (lo : Int) (n : Nat) (v : Int) :
    contains ⟨lo, 0, n⟩ v = true ↔ 0 < n ∧ v = lo := by
  constructor
  · intro h
    have h' : (0 < n ∧ lo ≤ v) ∧ v ≤ lo := by
      simpa [contains, lastVal] using h
    exact ⟨h'.1.1, by omega⟩
  · rintro ⟨hn, rfl⟩
    simp [contains, lastVal, hn]

private theorem ep_contains_bounds (a : Progression) (v : Int) (h : contains a v = true) :
    a.lo ≤ v ∧ v ≤ lastVal a := by
  have hm := (ep_contains_iff_mem a v).mp h
  obtain ⟨k, hk, hval⟩ := hm
  unfold ep_progVal at hval
  have hkint : (k : Int) ≤ ((a.n - 1 : Nat) : Int) := by
    exact_mod_cast (Nat.le_pred_of_lt hk)
  have hmul := Int.mul_le_mul_of_nonneg_left hkint (by omega : 0 ≤ (a.step : Int))
  have hmul' : (k : Int) * a.step ≤ ((a.n - 1 : Nat) : Int) * a.step := by
    simpa only [Int.mul_comm] using hmul
  unfold lastVal
  have hknon : (0 : Int) ≤ k := by omega
  have hstepnon : (0 : Int) ≤ (a.step : Int) := Int.natCast_nonneg _
  have hkmulnon : (0 : Int) ≤ (k : Int) * a.step := Int.mul_nonneg hknon hstepnon
  constructor
  · rw [hval]
    omega
  · rw [hval]
    exact Int.add_le_add_left hmul' a.lo

private theorem ep_contains_dvd (a : Progression) (v : Int) (h : contains a v = true) :
    (a.step : Int) ∣ v - a.lo := by
  obtain ⟨k, _, hval⟩ := (ep_contains_iff_mem a v).mp h
  unfold ep_progVal at hval
  refine ⟨(k : Int), ?_⟩
  rw [hval]
  rw [Int.mul_comm (k : Int) (a.step : Int)]
  omega

private theorem ep_common_gap_dvd (a b : Progression) (v : Int)
    (ha : contains a v = true) (hb : contains b v = true) :
    (a.step.gcd b.step : Int) ∣ b.lo - a.lo := by
  obtain ⟨i, _, hi⟩ := (ep_contains_iff_mem a v).mp ha
  obtain ⟨j, _, hj⟩ := (ep_contains_iff_mem b v).mp hb
  unfold ep_progVal at hi hj
  have hgd : ((a.step.gcd b.step : Nat) : Int) ∣ (b.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_right a.step b.step)
  have hgl : ((a.step.gcd b.step : Nat) : Int) ∣ (a.step : Int) := by
    exact_mod_cast (Nat.gcd_dvd_left a.step b.step)
  obtain ⟨tg, htg⟩ := hgd
  obtain ⟨tl, htl⟩ := hgl
  have h1 : (a.step.gcd b.step : Int) ∣ (b.step : Int) * (j : Int) := by
    refine ⟨tg * (j : Int), ?_⟩
    rw [htg, Int.mul_assoc]
  have h2 : (a.step.gcd b.step : Int) ∣ (a.step : Int) * (i : Int) := by
    refine ⟨tl * (i : Int), ?_⟩
    rw [htl, Int.mul_assoc]
  have hi' : (i : Int) * a.step = v - a.lo := by omega
  have hj' : (j : Int) * b.step = v - b.lo := by omega
  have heq : (b.step : Int) * (j : Int) - (a.step : Int) * (i : Int) = a.lo - b.lo := by
    have h1' : (b.step : Int) * (j : Int) = (j : Int) * b.step := Int.mul_comm _ _
    have h2' : (a.step : Int) * (i : Int) = (i : Int) * a.step := Int.mul_comm _ _
    rw [h1', h2', hj', hi']
    omega
  have hdvd : (a.step.gcd b.step : Int) ∣ a.lo - b.lo := by
    rw [← heq]
    exact Int.dvd_sub h1 h2
  have hneg : (a.step.gcd b.step : Int) ∣ -(a.lo - b.lo) := Int.dvd_neg.mpr hdvd
  simpa [Int.neg_sub] using hneg

private theorem ep_contains_unit_one (lo : Int) (v : Int) :
    contains ⟨lo, 1, 1⟩ v = true ↔ v = lo := by
  rw [ep_contains_iff_mem]
  constructor
  · rintro ⟨k, hk, hv⟩
    have hk1 : k < 1 := by simpa using hk
    have hk0 : k = 0 := by omega
    subst k
    unfold ep_progVal at hv
    simpa using hv
  · intro hv
    subst v
    refine ⟨0, ?_, ?_⟩
    · change 0 < (1 : Nat)
      decide
    · unfold ep_progVal
      simp

private theorem ep_contains_unit_zero (lo : Int) (v : Int) :
    contains ⟨lo, 1, 0⟩ v = true ↔ False := by
  rw [ep_contains_iff_mem]
  constructor
  · rintro ⟨k, hk, _⟩
    have hk0 : k < 0 := by simpa using hk
    omega
  · intro h
    exact h.elim

private theorem ep_mul_div_gcd_eq_lcm (a b : Nat) (ha : 0 < a) :
    a * (b / a.gcd b) = Nat.lcm a b := by
  have hg : 0 < a.gcd b := Nat.gcd_pos_of_pos_left b ha
  apply Nat.eq_of_mul_eq_mul_left (n := a.gcd b) hg
  rw [Nat.gcd_mul_lcm]
  calc
    a.gcd b * (a * (b / a.gcd b)) = a * (a.gcd b * (b / a.gcd b)) := by grind
    _ = a * b := by rw [Nat.mul_comm (a.gcd b), Nat.div_mul_cancel (Nat.gcd_dvd_right a b)]

private theorem ep_lcm_dvd_of_dvd_of_dvd (a b : Int) (X : Int) (ha : a ∣ X) (hb : b ∣ X) :
    (Int.lcm a b : Int) ∣ X := by
  have hxa : a ∣ (X.natAbs : Int) := Int.dvd_natAbs.mpr ha
  have hxb : b ∣ (X.natAbs : Int) := Int.dvd_natAbs.mpr hb
  have h : Int.lcm a b ∣ X.natAbs := Int.lcm_dvd hxa hxb
  exact Int.dvd_natAbs.mp (by exact_mod_cast h)

private theorem ep_intersection_contains (a b : Progression) (v : Int) :
    contains (intersection a b) v = true ↔ contains a v = true ∧ contains b v = true := by
  by_cases han : a.n = 0
  · cases a with
    | mk alo astep an =>
        simp at han
        subst an
        simp [intersection, contains]
  · by_cases hbn : b.n = 0
    · cases b with
      | mk blo bstep bn =>
          simp at hbn
          subst bn
          simp [intersection, contains]
    · have hanp : 0 < a.n := by omega
      have hbnp : 0 < b.n := by omega
      by_cases has : a.step = 0
      · cases a with
        | mk alo astep an =>
          simp at has han hanp
          subst astep
          have hInt : intersection ⟨alo, 0, an⟩ b =
              ⟨alo, 1, if contains b alo then 1 else 0⟩ := by
            simp [intersection, han, hbn]
          rw [hInt]
          by_cases hb : contains b alo = true
          · rw [if_pos hb]
            constructor
            · intro h
              have hvalo : v = alo := (ep_contains_unit_one alo v).mp h
              subst v
              exact ⟨(ep_contains_const_iff alo an alo).mpr ⟨hanp, rfl⟩, hb⟩
            · rintro ⟨ha, hbv⟩
              have hva := (ep_contains_const_iff alo an v).mp ha
              have hvalo : v = alo := hva.2
              subst v
              exact (ep_contains_unit_one alo alo).mpr rfl
          · rw [if_neg hb]
            constructor
            · intro h
              exact (ep_contains_unit_zero alo v).mp h |>.elim
            · rintro ⟨ha, hbv⟩
              have hva := (ep_contains_const_iff alo an v).mp ha
              have hvalo : v = alo := hva.2
              subst v
              exact (hb hbv).elim
      · by_cases hbs : b.step = 0
        · cases b with
          | mk blo bstep bn =>
            simp at hbs hbn hbnp
            subst bstep
            have hInt : intersection a ⟨blo, 0, bn⟩ =
                ⟨blo, 1, if contains a blo then 1 else 0⟩ := by
              simp [intersection, han, hbn, has]
            rw [hInt]
            by_cases ha' : contains a blo = true
            · rw [if_pos ha']
              constructor
              · intro h
                have hvlo : v = blo := (ep_contains_unit_one blo v).mp h
                subst v
                exact ⟨ha', (ep_contains_const_iff blo bn blo).mpr ⟨hbnp, rfl⟩⟩
              · rintro ⟨ha, hbv⟩
                have hvb := (ep_contains_const_iff blo bn v).mp hbv
                have hvlo : v = blo := hvb.2
                subst v
                exact (ep_contains_unit_one blo blo).mpr rfl
            · rw [if_neg ha']
              constructor
              · intro h
                exact (ep_contains_unit_zero blo v).mp h |>.elim
              · rintro ⟨ha, hbv⟩
                have hvb := (ep_contains_const_iff blo bn v).mp hbv
                have hvlo : v = blo := hvb.2
                subst v
                exact (ha' ha).elim
        · have hasp : 0 < a.step := Nat.pos_of_ne_zero has
          have hbsp : 0 < b.step := Nat.pos_of_ne_zero hbs
          let g := a.step.gcd b.step
          let gap := b.lo - a.lo
          let period := (a.step / g) * b.step
          let unit := (bezout (a.step / g) (b.step / g)).1
          let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % ((b.step / g : Nat) : Int))
          let low := max a.lo b.lo
          let first := low + (root - low) % (period : Int)
          let high := min (lastVal a) (lastVal b)
          have hgpos : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step hasp
          have hApos : 0 < a.step / a.step.gcd b.step := Nat.div_gcd_pos_of_pos_left b.step hasp
          have hpNat : 0 < period := by
            dsimp [period]
            exact Nat.mul_pos hApos hbsp
          have hp : (0 : Int) < period := by exact_mod_cast hpNat
          have hgapdef : gap = b.lo - a.lo := rfl
          by_cases hgapmod : gap % (g : Int) = 0
          · have hgap : (g : Int) ∣ gap := Int.dvd_of_emod_eq_zero hgapmod
            have hroot := ep_root_dvd a.step b.step hasp hbsp a.lo b.lo gap hgapdef hgap
            have hfirst : first = low + (root - low) % (period : Int) := rfl
            have hfroot := ep_first_root_dvd low root first period hp hfirst
            have hpaEq : period = a.step * (b.step / g) := by
              dsimp [period, g]
              have hbdvd : a.step.gcd b.step ∣ b.step := Nat.gcd_dvd_right a.step b.step
              have hadvd : a.step.gcd b.step ∣ a.step := Nat.gcd_dvd_left a.step b.step
              calc (a.step / a.step.gcd b.step) * b.step
                  = (a.step / a.step.gcd b.step) * (a.step.gcd b.step * (b.step / a.step.gcd b.step)) := by
                      rw [Nat.mul_div_cancel' hbdvd]
                _ = (a.step / a.step.gcd b.step * a.step.gcd b.step) * (b.step / a.step.gcd b.step) := by
                      rw [Nat.mul_assoc]
                _ = a.step * (b.step / a.step.gcd b.step) := by
                      rw [Nat.div_mul_cancel hadvd]
            have hpbEq : period = b.step * (a.step / g) := by
              dsimp [period, g]
              rw [Nat.mul_comm]
            have hpa : (a.step : Int) ∣ (period : Int) := by
              refine ⟨((b.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hpaEq
            have hpb : (b.step : Int) ∣ (period : Int) := by
              refine ⟨((a.step / g : Nat) : Int), ?_⟩
              exact_mod_cast hpbEq
            simp [intersection, han, hbn, has, hbs, hgapmod, Id.run, pure, g, gap]
            change contains ⟨first, period, if first ≤ high then ((high - first) / (↑period : Int)).toNat + 1 else 0⟩ v = true ↔
              contains a v = true ∧ contains b v = true
            have hsp := ep_span_contains_iff first (↑period) high v hp
            simp only [Int.toNat_natCast] at hsp
            rw [hsp]
            constructor
            · rintro ⟨hfirstx, hxhigh, hxfirst⟩
              have hxroot : (period : Int) ∣ v - root := by
                have heq : v - root = (v - first) + (first - root) := by omega
                rw [heq]
                exact Int.dvd_add hxfirst hfroot
              have hxalo : (a.step : Int) ∣ v - a.lo := by
                have h1 : (a.step : Int) ∣ v - root := Int.dvd_trans hpa hxroot
                have h2 : (a.step : Int) ∣ (v - root) + (root - a.lo) := Int.dvd_add h1 hroot.1
                simpa [show (v - root) + (root - a.lo) = v - a.lo by omega] using h2
              have hxblo : (b.step : Int) ∣ v - b.lo := by
                have h1 : (b.step : Int) ∣ v - root := Int.dvd_trans hpb hxroot
                have h2 : (b.step : Int) ∣ (v - root) + (root - b.lo) := Int.dvd_add h1 hroot.2
                simpa [show (v - root) + (root - b.lo) = v - b.lo by omega] using h2
              have hxlow : low ≤ v := by
                have hflow : low ≤ first := by
                  dsimp [first]
                  have := Int.emod_nonneg (root - low) (show (↑period : Int) ≠ 0 by omega)
                  omega
                omega
              constructor
              · exact ep_mem_of_bounds_dvd a v hanp hasp (by omega) (by omega) hxalo
              · exact ep_mem_of_bounds_dvd b v hbnp hbsp (by omega) (by omega) hxblo
            · rintro ⟨ha, hb⟩
              have hab := ep_contains_bounds a v ha
              have hbb := ep_contains_bounds b v hb
              have hxalo0 := ep_contains_dvd a v ha
              have hxblo0 := ep_contains_dvd b v hb
              have hvar : (a.step : Int) ∣ v - root := by
                have heq : v - root = (v - a.lo) - (root - a.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxalo0 hroot.1
              have hvbr : (b.step : Int) ∣ v - root := by
                have heq : v - root = (v - b.lo) - (root - b.lo) := by omega
                rw [heq]
                exact Int.dvd_sub hxblo0 hroot.2
              have hperLcmNat : period = Nat.lcm a.step b.step := by
                rw [hpaEq]
                exact ep_mul_div_gcd_eq_lcm a.step b.step hasp
              have hxroot : (period : Int) ∣ v - root := by
                have hl := ep_lcm_dvd_of_dvd_of_dvd (a.step : Int) (b.step : Int) (v - root) hvar hvbr
                rw [hperLcmNat]
                simpa using hl
              have hxfirst : (period : Int) ∣ v - first := by
                have heq : v - first = (v - root) - (first - root) := by omega
                rw [heq]
                exact Int.dvd_sub hxroot hfroot
              have hxlow : low ≤ v := by
                dsimp [low]
                omega
              have hfirstle : first ≤ v :=
                ep_first_le_of_congruence low root first v (period : Int) hp hfirst hxlow hxroot
              have hxhigh : v ≤ high := by
                dsimp [high]
                omega
              exact ⟨hfirstle, hxhigh, hxfirst⟩
          · have hgapne : gap % (g : Int) ≠ 0 := by simpa using hgapmod
            simp [intersection, han, hbn, has, hbs, hgapmod, Id.run, pure, g, gap]
            simp only [contains]
            constructor
            · intro h; simp at h
            · intro ⟨ha, hb⟩
              have hg := ep_common_gap_dvd a b v ha hb
              exact (hgapne (Int.emod_eq_zero_of_dvd hg)).elim

/-! ## Per-lane generic identities -/

private theorem ep_list_sum_congr {α : Type} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem ep_countP_range_eq_of_step0 (b : Lane) (hb : b.step = 0) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      if v == b.base then b.n else 0 := by
  simp only [hb, Int.mul_zero, Int.add_zero]
  by_cases hv : v == b.base
  · simp [hv, List.countP_true]
  · simp [hv]

-- Obstacle: needs careful case split on a.step < 0 vs >= 0 to map the
-- reversed `values` progression back to lane indices. True but proof-engineering heavy.
private theorem ep_contains_values_iff (a : Lane) (v : Int) :
    contains (values a) v = true ↔ ∃ i < a.n, v = a.base + (i : Int) * a.step := by
  have hlo : (values a).lo =
      if a.step < 0 then a.base + ((a.n - 1 : Nat) : Int) * a.step else a.base := rfl
  have hst : ((values a).step : Int) = (a.step.natAbs : Int) := rfl
  have hn : (values a).n = a.n := rfl
  constructor
  · intro h
    obtain ⟨k, hk, hv⟩ := (ep_contains_iff_mem (values a) v).mp h
    unfold ep_progVal at hv
    have hkn : k < a.n := by simpa [hn] using hk
    by_cases hs : a.step < 0
    · have hlo' : (values a).lo = a.base + ((a.n - 1 : Nat) : Int) * a.step := if_pos hs
      have hst' : ((values a).step : Int) = -a.step := by
        show (a.step.natAbs : Int) = -a.step
        omega
      rw [hlo', hst'] at hv
      rw [Int.mul_neg] at hv
      refine ⟨a.n - 1 - k, by omega, ?_⟩
      have h1 : ((a.n - 1 - k : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (k : Int) := by omega
      rw [h1, Int.sub_mul]
      omega
    · have hlo' : (values a).lo = a.base := if_neg hs
      have hst' : ((values a).step : Int) = a.step := by
        show (a.step.natAbs : Int) = a.step
        omega
      rw [hlo', hst'] at hv
      exact ⟨k, hkn, hv⟩
  · rintro ⟨i, hi, rfl⟩
    apply (ep_contains_iff_mem (values a) _).mpr
    refine ⟨if a.step < 0 then a.n - 1 - i else i, ?_, ?_⟩
    · by_cases hs : a.step < 0 <;> simp [hs, hn] <;> omega
    · unfold ep_progVal
      by_cases hs : a.step < 0
      · have hlo' : (values a).lo = a.base + ((a.n - 1 : Nat) : Int) * a.step := if_pos hs
        have hst' : ((values a).step : Int) = -a.step := by
          show (a.step.natAbs : Int) = -a.step
          omega
        rw [hlo', hst']
        simp only [hs, if_true]
        rw [Int.mul_neg]
        have h1 : ((a.n - 1 - i : Nat) : Int) = ((a.n - 1 : Nat) : Int) - (i : Int) := by omega
        rw [h1, Int.sub_mul]
        omega
      · have hlo' : (values a).lo = a.base := if_neg hs
        have hst' : ((values a).step : Int) = a.step := by
          show (a.step.natAbs : Int) = a.step
          omega
        rw [hlo', hst']
        simp only [hs, if_false]

private theorem ep_contains_values_of_lanePoint (a : Lane) (i : Nat) (hi : i < a.n) :
    contains (values a) (a.base + (i : Int) * a.step) = true := by
  rw [ep_contains_values_iff]
  exact ⟨i, hi, rfl⟩


-- BEGIN ep-helpers
private theorem ep_contains_step_one_ite (lo : Int) (c : Bool) (v : Int) :
    contains ⟨lo, 1, if c then 1 else 0⟩ v = true ↔ c = true ∧ v = lo := by
  cases c <;> simp [contains, lastVal] <;> omega

private theorem ep_intersection_step0_left (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step = 0) :
    intersection a b = ⟨a.lo, 1, if contains b a.lo then 1 else 0⟩ := by
  unfold intersection
  simp [han, hbn, has]

private theorem ep_intersection_step0_right (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step ≠ 0) (hbs : b.step = 0) :
    intersection a b = ⟨b.lo, 1, if contains a b.lo then 1 else 0⟩ := by
  unfold intersection
  simp [han, hbn, has, hbs]

private theorem ep_intersection_main_form (a b : Progression) (han : a.n ≠ 0) (hbn : b.n ≠ 0)
    (has : a.step ≠ 0) (hbs : b.step ≠ 0) :
    let g := a.step.gcd b.step
    let gap := b.lo - a.lo
    let period := (a.step / g) * b.step
    let unit := (bezout (a.step / g) (b.step / g)).1
    let root := a.lo + (a.step : Int) * ((gap / (g : Int) * unit) % (b.step / g : Nat))
    let low := max a.lo b.lo
    let first := low + (root - low) % (period : Int)
    let high := min (lastVal a) (lastVal b)
    intersection a b = if gap % (g : Int) != 0 then ⟨0, 1, 0⟩ else
      ⟨first, period, if first ≤ high then ((high - first) / (period : Int)).toNat + 1 else 0⟩ := by
  dsimp only
  unfold intersection
  simp [han, hbn, has, hbs]
  by_cases hgap : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 <;> simp [hgap]

private theorem ep_intersection_step_pos (a b : Progression) : 0 < (intersection a b).step := by
  by_cases han : a.n = 0
  · simp [intersection, han]
  · by_cases hbn : b.n = 0
    · simp [intersection, han, hbn]
    · by_cases has : a.step = 0
      · simp [intersection, han, hbn, has]
      · by_cases hbs : b.step = 0
        · simp [intersection, han, hbn, has, hbs]
        · have hg : 0 < a.step.gcd b.step := Nat.gcd_pos_of_pos_left b.step (Nat.pos_of_ne_zero has)
          have hdiv : 0 < a.step / a.step.gcd b.step :=
            Nat.div_pos (Nat.le_of_dvd (Nat.pos_of_ne_zero has) (Nat.gcd_dvd_left a.step b.step)) hg
          have hp : 0 < (a.step / a.step.gcd b.step) * b.step := Nat.mul_pos hdiv (Nat.pos_of_ne_zero hbs)
          rw [ep_intersection_main_form a b han hbn has hbs]
          by_cases hgap : (b.lo - a.lo) % (a.step.gcd b.step : Int) = 0 <;> simp [hgap, hp]

private theorem ep_nodup_map_of_injective {α β : Type} (l : List α) (f : α → β)
    (h : ∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) (hl : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hl
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hm
        rcases List.mem_map.mp hm with ⟨b, hb, he⟩
        exact hl.1 (by rw [← h b (by simp [hb]) a (by simp) he]; exact hb)
      · exact ih (fun x hx y hy => h x (by simp [hx]) y (by simp [hy])) hl.2

private theorem ep_laneVal_inj (a : Lane) (ha : a.step ≠ 0) (i j : Nat)
    (hi : i < a.n) (hj : j < a.n) (h : a.base + (i : Int) * a.step = a.base + (j : Int) * a.step) :
    i = j := by
  have hm0 : (i : Int) * a.step = (j : Int) * a.step := by omega
  have hm : a.step * (i : Int) = a.step * (j : Int) := by simpa [Int.mul_comm] using hm0
  have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff (by simpa using ha)).mp hm
  exact_mod_cast hij

private theorem ep_count_values_mem (a : Lane) (q : Progression) :
    (List.range a.n).countP (fun i : Nat => contains q (a.base + (i : Int) * a.step)) =
      (intersection (values a) q).n * (if a.step = 0 then a.n else 1) := by
  by_cases ha0 : a.step = 0
  · rw [if_pos ha0]
    by_cases han : a.n = 0
    · simp [han, contains]
    · by_cases hqn : q.n = 0
      · have hleft : (List.range a.n).countP
            (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = 0 := by
          apply List.countP_eq_zero.mpr
          intro i hi
          simp [contains, hqn]
        rw [hleft]
        simp [intersection, hqn, contains]
      · rw [ep_intersection_step0_left (values a) q
            (by simpa [values, ha0] using han) hqn (by simp [values, ha0])]
        have hvalo : (values a).lo = a.base := by simp [values, ha0]
        rw [hvalo]
        by_cases hc : contains q a.base
        · have hleft : (List.range a.n).countP
              (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = a.n := by
            rw [show (List.range a.n).countP (fun i : Nat => contains q (a.base + (i : Int) * a.step)) =
                (List.range a.n).countP (fun _ : Nat => true) by
              apply List.countP_congr
              intro i hi
              simp [ha0, hc]]
            simp
          rw [hleft]
          simp [ep_contains_step_one_ite, hc, Nat.mul_comm]
        · have hleft : (List.range a.n).countP
              (fun i : Nat => contains q (a.base + (i : Int) * a.step)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro i hi
            simp [ha0, hc]
          rw [hleft]
          simp [ep_contains_step_one_ite, hc]
  · rw [if_neg ha0, Nat.mul_one]
    let R := intersection (values a) q
    let L : List Int := ((List.range a.n).filter
      (fun i : Nat => contains q (a.base + (i : Int) * a.step))).map
        (fun i : Nat => a.base + (i : Int) * a.step)
    let L2 : List Int := (List.range R.n).map (ep_progVal R)
    have hnodL : L.Nodup := by
      dsimp [L]
      apply ep_nodup_map_of_injective
      · intro i hi j hj h
        exact ep_laneVal_inj a ha0 i j
          (List.mem_range.mp (List.mem_filter.mp hi).1)
          (List.mem_range.mp (List.mem_filter.mp hj).1) h
      · exact List.Sublist.nodup (List.filter_sublist (p := fun i : Nat =>
          contains q (a.base + (i : Int) * a.step)) (l := List.range a.n)) List.nodup_range
    have hnodL2 : L2.Nodup := by
      dsimp [L2]
      apply ep_nodup_map_of_injective
      · intro i hi j hj h
        have hm0 : (i : Int) * (R.step : Int) = (j : Int) * (R.step : Int) := by
          unfold ep_progVal at h
          omega
        have hm : R.step * (i : Int) = R.step * (j : Int) := by simpa [Int.mul_comm] using hm0
        have hRnz : (R.step : Int) ≠ 0 := by
          exact_mod_cast (Nat.ne_of_gt (ep_intersection_step_pos (values a) q))
        have hij : (i : Int) = (j : Int) := (Int.mul_eq_mul_left_iff hRnz).mp hm
        exact_mod_cast hij
      · exact List.nodup_range
    have hmem : ∀ x : Int, x ∈ L ↔ x ∈ L2 := by
      intro x
      constructor
      · intro hx
        rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
        have hxi : i < a.n := List.mem_range.mp (List.mem_filter.mp hi).1
        have hxv : contains q (a.base + (i : Int) * a.step) = true := (List.mem_filter.mp hi).2
        have hxa : contains (values a) (a.base + (i : Int) * a.step) = true :=
          ep_contains_values_of_lanePoint a i hxi
        have hxR : contains R (a.base + (i : Int) * a.step) = true :=
          (ep_intersection_contains (values a) q _).mpr ⟨hxa, hxv⟩
        obtain ⟨k, hk, hv⟩ := (ep_contains_iff_mem R _).mp hxR
        apply List.mem_map.mpr
        refine ⟨k, List.mem_range.mpr hk, ?_⟩
        exact hv.symm
      · intro hx
        rcases List.mem_map.mp hx with ⟨k, hk, hv⟩
        have hR : contains R (ep_progVal R k) = true := by
          apply (ep_contains_iff_mem R _).mpr
          exact ⟨k, List.mem_range.mp hk, rfl⟩
        have hboth := (ep_intersection_contains (values a) q _).mp hR
        obtain ⟨i, hi, hival⟩ := (ep_contains_values_iff a _).mp hboth.1
        apply List.mem_map.mpr
        refine ⟨i, ?_, ?_⟩
        · apply List.mem_filter.mpr
          refine ⟨List.mem_range.mpr hi, ?_⟩
          rw [hival] at hboth
          exact hboth.2
        · rw [← hival]
          exact hv
    have hperm : L.Perm L2 := (List.perm_ext_iff_of_nodup hnodL hnodL2).mpr hmem
    rw [List.countP_eq_length_filter]
    dsimp [L] at hperm
    dsimp [L2] at hperm
    have hlen := hperm.length_eq
    rw [List.length_map, List.length_map, List.length_range] at hlen
    exact hlen

private theorem ep_sum_congr_nat {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem ep_sum_eq_zero_nat {α : Type} (l : List α) (f : α → Nat)
    (h : ∀ x ∈ l, f x = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem ep_ite_sum_eq_length_filter {α : Type} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then 1 else 0)).sum = (l.filter p).length := by
  induction l with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih] <;> omega

private theorem ep_countP_eq_ite_sum {α : Type} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then 1 else 0)).sum = l.countP p := by
  rw [ep_ite_sum_eq_length_filter, ep_countP_eq_length_filter]

private theorem ep_sum_range_eq_one : ∀ n : Nat, ∀ i < n,
    ((List.range n).map (fun k => if k = i then (1 : Nat) else 0)).sum = 1 := by
  intro n
  induction n with
  | zero => intro i hi; exact absurd hi (by simp)
  | succ q ih =>
      intro i hi
      rw [List.range_succ, List.map_append, List.sum_append]
      rcases Nat.lt_or_ge i q with hij | hij
      · have hz : (if q = i then (1 : Nat) else 0) = 0 := by
          by_cases heq : q = i
          · exact absurd heq (by omega)
          · rw [if_neg heq]
        have ht : (([q] : List Nat).map (fun m => if m = i then (1 : Nat) else 0)).sum = 0 := by
          simp [hz]
        rw [ih i hij, ht]
      · have hq : i = q := by omega
        rw [hq]
        have hz : ∀ m ∈ List.range q, (if m = q then (1 : Nat) else 0) = 0 := by
          intro m hm
          rw [if_neg (by
            have hm' : m < q := List.mem_range.mp hm
            omega)]
        rw [ep_sum_congr_nat (List.range q) (fun m => if m = q then 1 else 0) (fun _ => 0) hz,
          ep_sum_eq_zero_nat (List.range q) (fun _ => 0) (by simp)]
        simp

private theorem ep_sum_map_mul_nat {α : Type} (l : List α) (c : Nat) (f : α → Nat) :
    ((l.map (fun x => c * f x)).sum) = c * (l.map f).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, Nat.mul_add, ih]

private theorem ep_countP_range_unique (b : Lane) (hb : b.step ≠ 0) (v : Int) (i : Nat)
    (hi : i < b.n) (hv : v = b.base + (i : Int) * b.step) :
    (List.range b.n).countP (fun k : Nat => v == b.base + b.step * (k : Int)) = 1 := by
  rw [← ep_countP_eq_ite_sum]
  have hel : ∀ k ∈ List.range b.n,
      (if v == b.base + b.step * (k : Int) then (1 : Nat) else 0) = (if k = i then 1 else 0) := by
    intro k hk
    have hk' : k < b.n := List.mem_range.mp hk
    by_cases hkeq : k = i
    · subst k
      simp [hv, Int.mul_comm]
    · rw [if_neg hkeq]
      rw [if_neg (by
        intro hbe
        have h2 := eq_of_beq hbe
        rw [Int.mul_comm] at h2
        rw [hv] at h2
        exact hkeq (ep_laneVal_inj b hb i k hi hk' h2).symm)]
  rw [ep_sum_congr_nat (List.range b.n)
    (fun k => if v == b.base + b.step * (k : Int) then 1 else 0) (fun k => if k = i then 1 else 0) hel]
  exact ep_sum_range_eq_one b.n i hi

private theorem ep_countP_range_none (b : Lane) (v : Int)
    (h : ∀ i < b.n, ¬(v = b.base + (i : Int) * b.step)) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) = 0 := by
  rw [← ep_countP_eq_ite_sum]
  have hel : ∀ k ∈ List.range b.n,
      (if v == b.base + b.step * (k : Int) then (1 : Nat) else 0) = 0 := by
    intro k hk
    rw [if_neg (by
      intro hbe
      have h2 := eq_of_beq hbe
      rw [Int.mul_comm] at h2
      exact h k (List.mem_range.mp hk) h2)]
  rw [ep_sum_congr_nat (List.range b.n)
    (fun k => if v == b.base + b.step * (k : Int) then 1 else 0) (fun _ => 0) hel]
  exact ep_sum_eq_zero_nat (List.range b.n) (fun _ => 0) (by simp)
-- END ep-helpers

-- Obstacle: injectivity of i ↦ b.base+i*b.step for b.step!=0 plus
-- ep_contains_values_iff; needs list counting/uniqueness plumbing.
private theorem ep_countP_range_eq_of_step_ne (b : Lane) (hb : b.step ≠ 0) (v : Int) :
    (List.range b.n).countP (fun i : Nat => v == b.base + b.step * (i : Int)) =
      if contains (values b) v then 1 else 0 := by
  by_cases hcv : contains (values b) v = true
  · rw [if_pos hcv]
    obtain ⟨i, hi, hv⟩ := (ep_contains_values_iff b v).mp hcv
    exact ep_countP_range_unique b hb v i hi hv
  · rw [if_neg hcv]
    refine ep_countP_range_none b v (fun i hi h2 => hcv ((ep_contains_values_iff b v).mpr ⟨i, hi, h2⟩))

-- Obstacle: reduce per-point count to contains(values b) with multiplicity
-- (b.step==0 => b.n else 1, a.step==0 => a.n else 1) and sum over intersection.
-- Depends on ep_contains_values_iff + ep_intersection_contains.
private theorem ep_joinCount_lane (a b : Lane) :
    ((lanePoints a).map (fun r => (List.range b.n).countP (fun i : Nat => r.2 == b.base + b.step * (i : Int)))).sum =
      joinCount a b := by
  unfold lanePoints
  rw [List.map_map]
  show ((List.range a.n).map (fun i : Nat => (List.range b.n).countP
      (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)))).sum = joinCount a b
  by_cases hbn : b.n = 0
  · rw [hbn, List.range_zero]
    have hsum : ((List.range a.n).map (fun i : Nat => List.countP
        (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)) [])).sum = 0 := by
      apply ep_sum_eq_zero_nat
      intro i hi
      rfl
    rw [hsum]
    unfold joinCount
    have hn : (intersection (values a) (values b)).n = (0 : Nat) := by
      unfold intersection
      rw [show ((values b).n) = (0 : Nat) from by simp [values, hbn]]
      simp
    rw [hn, Nat.zero_mul]
    simp
  · by_cases hb0 : b.step = 0
    · have hper : ∀ i : Nat, (List.range b.n).countP
          (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)) =
            b.n * (if (a.base + (i : Int) * a.step) == b.base then 1 else 0) := by
        intro i
        rw [ep_countP_range_eq_of_step0 b hb0]
        by_cases he : a.base + (i : Int) * a.step = b.base
        · simp [he]
        · simp [he]
      rw [ep_sum_congr_nat (List.range a.n)
        (fun i => (List.range b.n).countP (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)))
        (fun i => b.n * (if (a.base + (i : Int) * a.step) == b.base then 1 else 0)) (fun i _ => hper i)]
      rw [ep_sum_map_mul_nat, ep_countP_eq_ite_sum]
      have hc : ∀ i ∈ List.range a.n,
          ((a.base + (i : Int) * a.step) == b.base) = contains (values b) (a.base + (i : Int) * a.step) := by
        intro i hi
        have hin : i < a.n := List.mem_range.mp hi
        have hbn' : 0 < b.n := Nat.pos_of_ne_zero hbn
        have hx : ((a.base + (i : Int) * a.step) == b.base) =
            if a.base + (i : Int) * a.step = b.base then true else false := by
          by_cases he : a.base + (i : Int) * a.step = b.base <;> simp [he]
        have hy : contains (values b) (a.base + (i : Int) * a.step) =
            if a.base + (i : Int) * a.step = b.base then true else false := by
          by_cases he : a.base + (i : Int) * a.step = b.base
          · rw [if_pos he]
            refine (ep_contains_values_iff b _).mpr ⟨0, by omega, ?_⟩
            rw [hb0, Int.mul_zero]
            omega
          · rw [if_neg he]
            cases hT : contains (values b) (a.base + (i : Int) * a.step)
            · rfl
            · obtain ⟨j, _, hj⟩ := (ep_contains_values_iff b _).mp hT
              rw [hb0, Int.mul_zero, Int.add_zero] at hj
              exact (he hj).elim
        rw [hx, hy]
      rw [List.countP_congr (fun i hi => Iff.intro
            (fun h => (hc i hi).symm.trans h) (fun h => (hc i hi).trans h)),
        ep_count_values_mem a (values b)]
      unfold joinCount
      rw [show (if b.step == (0 : Int) then b.n else 1) = b.n from if_pos (by simpa using hb0)]
      by_cases has : a.step = 0
      · rw [if_pos has,
          show (if a.step == (0 : Int) then a.n else 1) = a.n from if_pos (by simpa using has)]
        exact Nat.mul_comm _ _
      · rw [if_neg has,
          show (if a.step == (0 : Int) then a.n else 1) = (1 : Nat) from
            if_neg (by intro h; exact has (by simpa using h)), Nat.mul_one]
        exact Nat.mul_comm _ _
    · have hper : ∀ i : Nat, (List.range b.n).countP
          (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)) =
            if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0 := by
        intro i
        exact ep_countP_range_eq_of_step_ne b hb0 _
      rw [ep_sum_congr_nat (List.range a.n)
        (fun i => (List.range b.n).countP (fun j : Nat => (a.base + (i : Int) * a.step) == b.base + b.step * (j : Int)))
        (fun i => if contains (values b) (a.base + (i : Int) * a.step) then 1 else 0) (fun i _ => hper i)]
      rw [ep_countP_eq_ite_sum, ep_count_values_mem a (values b)]
      unfold joinCount
      rw [show (if b.step == (0 : Int) then b.n else 1) = (1 : Nat) from
        if_neg (by intro h; exact hb0 (by simpa using h))]
      rw [Nat.mul_one]
      by_cases has : a.step = 0 <;> simp [has]

-- END ep-helpers

-- Per-lane arithmetic core. These identities follow from the definitions of
-- intersection/joinCount/deletedSum/aligned on arithmetic progressions, but the
-- CRT/bezout proof over the Id.run intersection is nontrivial to formalize here.
private theorem lane_joinCount (a b : Lane) :
    (((lanePoints a).map (fun r =>
      (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum : Int) =
    (joinCount a b : Int) := by
  exact congrArg (fun x : Nat => (x : Int)) (ep_joinCount_lane a b)

/-! ### Deleted-sum per-lane helpers (adapted from the LaneMajor proof) -/

private theorem ep_sum_filter_complement {α : Type} (l : List α) (p : α → Bool) (f : α → Int) :
    ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum = (l.map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
        omega
      · simp [h, ih]
        omega

private def ep_progVals (p : Progression) : List Int := (List.range p.n).map (ep_progVal p)

private theorem ep_contains_mem (p : Progression) (v : Int) :
    contains p v = true ↔ v ∈ ep_progVals p := by
  rw [ep_contains_iff_mem]
  simp [ep_progMem, ep_progVals, eq_comm]

private theorem ep_nodup_filter {α : Type} (l : List α) (p : α → Bool) (hl : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hl
      simp only [List.filter_cons]
      by_cases h : p x
      · rw [if_pos h, List.nodup_cons]
        constructor
        · intro hx
          exact hl.1 (List.mem_filter.mp hx).1
        · exact ih hl.2
      · simpa [if_neg h] using ih hl.2

private theorem ep_progVals_nodup_of_pos (p : Progression) (hs : 0 < p.step) :
    (ep_progVals p).Nodup := by
  unfold ep_progVals
  apply ep_nodup_map_of_injective
  · intro a ha b hb hEq
    unfold ep_progVal at hEq
    have hm' : (a : Int) * (p.step : Int) = (b : Int) * (p.step : Int) := by omega
    have hm : (p.step : Int) * (a : Int) = (p.step : Int) * (b : Int) := by
      rw [Int.mul_comm (p.step : Int) (a : Int), Int.mul_comm (p.step : Int) (b : Int)]
      exact hm'
    have hab : (a : Int) = (b : Int) :=
      (Int.mul_eq_mul_left_iff (by exact_mod_cast (Nat.ne_of_gt hs) : (p.step : Int) ≠ 0)).mp hm
    exact_mod_cast hab
  · exact List.nodup_range

private def ep_lanePosVals (a : Lane) : List Int :=
  (List.range a.n).map (fun j : Nat => ((a.start + j * a.stride : Nat) : Int))

private def ep_posF (a : Lane) (v : Int) : Int :=
  a.base + (((v - (a.start : Int)) / (a.stride : Int)).toNat : Int) * a.step

private theorem ep_posF_at (a : Lane) (ha : 0 < a.stride) (j : Nat) :
    ep_posF a ((a.start + j * a.stride : Nat) : Int) = a.base + (j : Int) * a.step := by
  unfold ep_posF
  have hsub : ((a.start + j * a.stride : Nat) : Int) - (a.start : Int) =
      (j : Int) * (a.stride : Int) := by
    push_cast
    omega
  rw [hsub]
  rw [Int.mul_ediv_cancel (j : Int) (by exact_mod_cast (Nat.ne_of_gt ha))]
  rfl

private theorem ep_inter_positions_perm (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    (ep_progVals (intersection (positions a) m)).Perm
      ((ep_lanePosVals a).filter (fun v => contains m v)) := by
  have hleft : (ep_progVals (intersection (positions a) m)).Nodup :=
    ep_progVals_nodup_of_pos _ (ep_intersection_step_pos (positions a) m)
  have hright : ((ep_lanePosVals a).filter (fun v => contains m v)).Nodup := by
    unfold ep_lanePosVals
    exact ep_nodup_filter _ _ (ep_nodup_map_of_injective _ _ (by
      intro i _ j _ hEq
      have hEq' : a.start + i * a.stride = a.start + j * a.stride := by exact_mod_cast hEq
      have hij : i * a.stride = j * a.stride := Nat.add_left_cancel hEq'
      exact Nat.eq_of_mul_eq_mul_left ha (by simpa [Nat.mul_comm] using hij)) List.nodup_range)
  apply (List.perm_ext_iff_of_nodup hleft hright).mpr
  intro v
  unfold ep_lanePosVals
  simp only [List.mem_filter]
  constructor
  · intro hz
    have hq := (ep_contains_mem (intersection (positions a) m) v).mpr hz
    have hb := (ep_intersection_contains (positions a) m v).mp hq
    rcases List.mem_map.mp ((ep_contains_mem (positions a) v).mp hb.1) with ⟨j, hj, hjv⟩
    have hj' : j ∈ List.range a.n := by simpa [positions] using hj
    have hjv' : ((a.start + j * a.stride : Nat) : Int) = v := by
      simpa [ep_progVal, positions] using hjv
    exact ⟨List.mem_map.mpr ⟨j, hj', hjv'⟩, hb.2⟩
  · rintro ⟨hz, hm⟩
    rcases List.mem_map.mp hz with ⟨j, hj, hjv⟩
    have hj' : j ∈ List.range (positions a).n := by simpa [positions] using hj
    have hjv' : ep_progVal (positions a) j = v := by
      simpa [ep_progVal, positions] using hjv
    apply (ep_contains_mem (intersection (positions a) m) v).mp
    apply (ep_intersection_contains (positions a) m v).mpr
    refine ⟨(ep_contains_mem (positions a) v).mpr ?_, hm⟩
    exact List.mem_map.mpr ⟨j, hj', hjv'⟩

private theorem ep_posF_inter_of (a : Lane) (ha : 0 < a.stride) (q : Progression)
    (hqa : ∀ v, contains q v = true → contains (positions a) v = true)
    (v : Int) (hv : v ∈ ep_progVals q) :
    ∃ k, k < q.n ∧
      v = q.lo + (k : Int) * (q.step : Int) ∧
      ep_posF a v = firstAt a q + (k : Int) * slopeAt a q := by
  have hqmem : v ∈ ep_progVals q := hv
  rcases List.mem_map.mp hqmem with ⟨k, hk, hkv⟩
  have hklt : k < q.n := List.mem_range.mp hk
  have hkval : v = q.lo + (k : Int) * (q.step : Int) := by
    simpa [ep_progVal] using hkv.symm
  have hqlo : q.lo ∈ ep_progVals q := by
    exact List.mem_map.mpr ⟨0, List.mem_range.mpr (by omega), by simp [ep_progVal]⟩
  have hqloPos : contains (positions a) q.lo = true :=
    hqa q.lo ((ep_contains_mem q q.lo).mpr hqlo)
  rcases List.mem_map.mp ((ep_contains_mem (positions a) q.lo).mp hqloPos) with ⟨j0, hj0, hj0v⟩
  have hj0val : q.lo = (a.start : Int) + (j0 : Int) * (a.stride : Int) := by
    have htmp : q.lo = (positions a).lo + (j0 : Int) * (positions a).step := by
      simpa [ep_progVal] using hj0v.symm
    change q.lo = (a.start : Int) + (j0 : Int) * (a.stride : Int) at htmp
    exact htmp
  have hsub0 : q.lo - (a.start : Int) = (j0 : Int) * (a.stride : Int) := by omega
  have hdiv0 : (((q.lo - (a.start : Int)) / (a.stride : Int)).toNat) = j0 := by
    rw [hsub0, Int.mul_ediv_cancel (j0 : Int) (by exact_mod_cast (Nat.ne_of_gt ha))]
    simp
  by_cases hk0 : k = 0
  · subst k
    refine ⟨0, hklt, by simpa using hkval, ?_⟩
    have hnon : 0 ≤ (q.lo - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsub0]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hdiv0' : (q.lo - (a.start : Int)) / (a.stride : Int) = (j0 : Int) := by
      rw [← Int.toNat_of_nonneg hnon, hdiv0]
    have hkval0 : v = q.lo := by simpa using hkval
    unfold ep_posF firstAt slopeAt
    rw [hkval0]
    rw [Int.toNat_of_nonneg hnon, hdiv0']
    simp
  · have hkpos : 0 < k := by omega
    have hq1mem : q.lo + (q.step : Int) ∈ ep_progVals q := by
      exact List.mem_map.mpr ⟨1, List.mem_range.mpr (by omega), by simp [ep_progVal]⟩
    have hq1Pos : contains (positions a) (q.lo + (q.step : Int)) = true :=
      hqa (q.lo + (q.step : Int)) ((ep_contains_mem q (q.lo + (q.step : Int))).mpr hq1mem)
    rcases List.mem_map.mp ((ep_contains_mem (positions a) (q.lo + (q.step : Int))).mp hq1Pos) with
      ⟨j1, hj1, hj1v⟩
    have hj1val : q.lo + (q.step : Int) = (a.start : Int) + (j1 : Int) * (a.stride : Int) := by
      have htmp : q.lo + (q.step : Int) = (positions a).lo + (j1 : Int) * (positions a).step := by
        simpa [ep_progVal] using hj1v.symm
      change q.lo + (q.step : Int) = (a.start : Int) + (j1 : Int) * (a.stride : Int) at htmp
      exact htmp
    have hqstep_nonneg : 0 ≤ (q.step : Int) := by exact_mod_cast (Nat.zero_le q.step)
    have hj0le1 : j0 ≤ j1 := by
      by_cases h : j0 ≤ j1
      · exact h
      · have hjlt : j1 < j0 := by omega
        have hmul : j1 * a.stride < j0 * a.stride := Nat.mul_lt_mul_of_pos_right hjlt ha
        omega
    have hstepEq : (q.step : Int) = ((j1 - j0 : Nat) : Int) * (a.stride : Int) := by
      have hsubcast : ((j1 - j0 : Nat) : Int) = (j1 : Int) - (j0 : Int) := by
        exact Int.ofNat_sub hj0le1
      rw [hsubcast, Int.sub_mul]
      grind
    have hstepNatEq : q.step = (j1 - j0) * a.stride := by exact_mod_cast hstepEq
    have hstepNat : q.step / a.stride = j1 - j0 := by
      rw [hstepNatEq]
      exact Nat.mul_div_cancel _ ha
    have hsubv : v - (a.start : Int) = ((j0 + k * (j1 - j0) : Nat) : Int) * (a.stride : Int) := by
      rw [hkval, hstepEq]
      grind
    have hdivv : (((v - (a.start : Int)) / (a.stride : Int)).toNat) = j0 + k * (j1 - j0) := by
      rw [hsubv, Int.mul_ediv_cancel ((j0 + k * (j1 - j0) : Nat) : Int)
        (by exact_mod_cast (Nat.ne_of_gt ha))]
      rw [Int.toNat_natCast]
    refine ⟨k, hklt, hkval, ?_⟩
    change ep_posF a v = firstAt a q + (k : Int) * slopeAt a q
    have hnon0 : 0 ≤ (q.lo - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsub0]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hnonv : 0 ≤ (v - (a.start : Int)) / (a.stride : Int) := by
      apply Int.ediv_nonneg
      · rw [hsubv]
        exact Int.mul_nonneg (Int.natCast_nonneg _) (Int.natCast_nonneg _)
      · exact_mod_cast (Nat.zero_le a.stride)
    have hdiv0' : (q.lo - (a.start : Int)) / (a.stride : Int) = (j0 : Int) := by
      rw [← Int.toNat_of_nonneg hnon0, hdiv0]
    have hdivv' : (v - (a.start : Int)) / (a.stride : Int) =
        ((j0 + k * (j1 - j0) : Nat) : Int) := by
      rw [← Int.toNat_of_nonneg hnonv, hdivv]
    unfold ep_posF firstAt slopeAt
    change a.base + (((v - (a.start : Int)) / (a.stride : Int)).toNat : Int) * a.step =
      a.base + (q.lo - (a.start : Int)) / (a.stride : Int) * a.step +
        (k : Int) * (((q.step / a.stride : Nat) : Int) * a.step)
    rw [Int.toNat_of_nonneg hnonv, hdivv', hdiv0', hstepNat]
    grind

private theorem ep_posF_inter (a : Lane) (ha : 0 < a.stride) (m : Progression)
    (v : Int) (hv : v ∈ ep_progVals (intersection (positions a) m)) :
    ∃ k, k < (intersection (positions a) m).n ∧
      v = (intersection (positions a) m).lo + (k : Int) * ((intersection (positions a) m).step : Int) ∧
      ep_posF a v = firstAt a (intersection (positions a) m) +
        (k : Int) * slopeAt a (intersection (positions a) m) := by
  exact ep_posF_inter_of a ha (intersection (positions a) m)
    (fun v h => ((ep_intersection_contains (positions a) m v).mp h).1) v hv

private theorem ep_progVal_inj_of_pos (p : Progression) (hs : 0 < p.step)
    (i j : Nat) (hi : i < p.n) (hj : j < p.n)
    (hEq : ep_progVal p i = ep_progVal p j) : i = j := by
  unfold ep_progVal at hEq
  have hm' : (i : Int) * (p.step : Int) = (j : Int) * (p.step : Int) := by omega
  have hm : (p.step : Int) * (i : Int) = (p.step : Int) * (j : Int) := by
    rw [Int.mul_comm (p.step : Int) (i : Int), Int.mul_comm (p.step : Int) (j : Int)]
    exact hm'
  have hij : (i : Int) = (j : Int) :=
    (Int.mul_eq_mul_left_iff (by exact_mod_cast (Nat.ne_of_gt hs) : (p.step : Int) ≠ 0)).mp hm
  exact_mod_cast hij

private theorem ep_sum_posF_inter (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    ((ep_progVals (intersection (positions a) m)).map (ep_posF a)).sum = deletedSum a m := by
  unfold deletedSum
  let q := intersection (positions a) m
  have hqstep : 0 < q.step := by
    dsimp [q]
    exact ep_intersection_step_pos (positions a) m
  change ((ep_progVals q).map (ep_posF a)).sum = (q.n : Int) * firstAt a q + tri q.n * slopeAt a q
  have hval : ∀ k, k < q.n →
      ep_posF a (ep_progVal q k) = firstAt a q + (k : Int) * slopeAt a q := by
    intro k hk
    obtain ⟨k', hk', hvk', heq⟩ := ep_posF_inter a ha m (ep_progVal q k)
      (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    have hvk'' : ep_progVal q k = q.lo + (k' : Int) * (q.step : Int) := by
      change ep_progVal q k = (intersection (positions a) m).lo +
        (k' : Int) * ((intersection (positions a) m).step : Int) at hvk'
      exact hvk'
    have heq' : ep_posF a (ep_progVal q k) = firstAt a q + (k' : Int) * slopeAt a q := by
      change ep_posF a (ep_progVal q k) = firstAt a q + (k' : Int) * slopeAt a q at heq
      exact heq
    have hEqv : ep_progVal q k' = ep_progVal q k := by
      change q.lo + (k' : Int) * (q.step : Int) = ep_progVal q k
      rw [hvk'']
    have hk'eq : k' = k := ep_progVal_inj_of_pos q hqstep k' k hk' hk hEqv
    rw [hk'eq] at heq'
    exact heq'
  let L : Lane := ⟨0, 1, q.n, firstAt a q, slopeAt a q⟩
  calc
    ((ep_progVals q).map (ep_posF a)).sum =
        ((List.range q.n).map (fun k : Nat => firstAt a q + (k : Int) * slopeAt a q)).sum := by
      rw [show ep_progVals q = (List.range q.n).map (fun k : Nat => ep_progVal q k) by rfl, List.map_map]
      change ((List.range q.n).map (fun k : Nat => ep_posF a (ep_progVal q k))).sum =
        ((List.range q.n).map (fun k : Nat => firstAt a q + (k : Int) * slopeAt a q)).sum
      apply congrArg List.sum
      apply List.map_congr_left
      intro k hk
      exact hval k (List.mem_range.mp hk)
    _ = partialSum L q.n := (ep_partialSum_eq_sum L q.n).symm
    _ = (q.n : Int) * firstAt a q + tri q.n * slopeAt a q := rfl

private theorem ep_filter_map_eq {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).filter p = (l.filter (fun x => p (f x))).map f := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem ep_filter_lanePosVals (a : Lane) (m : Progression) :
    (ep_lanePosVals a).filter (fun v => contains m v) =
      ((List.range a.n).filter (fun j : Nat => contains m ((a.start + j * a.stride : Nat) : Int))).map
        (fun j : Nat => ((a.start + j * a.stride : Nat) : Int)) := by
  unfold ep_lanePosVals
  rw [ep_filter_map_eq]

private theorem ep_deletedSum_lane (a : Lane) (ha : 0 < a.stride) (m : Progression) :
    (((List.range a.n).filter (fun j : Nat => !contains m ((a.start + j * a.stride : Nat) : Int))).map
      (fun j : Nat => a.base + (j : Int) * a.step)).sum =
      partialSum a a.n - deletedSum a m := by
  let p : Nat → Bool := fun j : Nat => contains m ((a.start + j * a.stride : Nat) : Int)
  let val : Nat → Int := fun j => a.base + (j : Int) * a.step
  have hcomp := ep_sum_filter_complement (List.range a.n) p val
  have hdel : (((List.range a.n).filter p).map val).sum = deletedSum a m := by
    rw [show ((List.range a.n).filter p).map val =
        (((ep_lanePosVals a).filter (fun v => contains m v)).map (ep_posF a)) by
      rw [ep_filter_lanePosVals, List.map_map]
      apply List.map_congr_left
      intro j hj
      exact (ep_posF_at a ha j).symm]
    exact (list_sum_perm ((ep_inter_positions_perm a ha m).map (ep_posF a))).symm.trans
      (ep_sum_posF_inter a ha m)
  have htot : ((List.range a.n).map val).sum = partialSum a a.n := (ep_partialSum_eq_sum a a.n).symm
  have hcomp' : (((List.range a.n).filter p).map val).sum +
      (((List.range a.n).filter (fun j : Nat => !p j)).map val).sum =
      ((List.range a.n).map val).sum := by simpa using hcomp
  have hgoal : (((List.range a.n).filter (fun j : Nat => !p j)).map val).sum =
      partialSum a a.n - deletedSum a m := by omega
  simpa [p, val] using hgoal

private theorem ep_filter_map_pair_snd {α β : Type} (l : List α) (f : α → Nat) (g : α → β)
    (p : Nat → Bool) :
    ((l.map (fun x => (f x, g x))).filter (fun r => p r.1)).map Prod.snd =
      (l.filter (fun x => p (f x))).map g := by
  induction l with
  | nil => simp
  | cons x xs ih => by_cases h : p (f x) <;> simp [h, ih]

private theorem lane_deleteProgression (a : Lane) (ha : 0 < a.stride) (mask : Progression) :
    (((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    partialSum a a.n - deletedSum a mask := by
  -- The hypothesis 0 < a.stride is needed: with stride 0 the same position is
  -- repeated and the filter no longer matches the compressed overlap progression.
  rw [show ((lanePoints a).filter (fun r => !contains mask r.1)).map Prod.snd =
      ((List.range a.n).filter (fun i : Nat => !contains mask ((a.start + i * a.stride : Nat) : Int))).map
        (fun i : Nat => a.base + (i : Int) * a.step) by
    unfold lanePoints
    simpa using ep_filter_map_pair_snd (List.range a.n)
      (fun i : Nat => a.start + i * a.stride)
      (fun i : Nat => a.base + (i : Int) * a.step)
      (fun v : Nat => !contains mask (v : Int))]
  exact ep_deletedSum_lane a ha mask


private def ep_bvalAt (b : Lane) (v : Int) : Int :=
  if contains (positions b) v then
    b.base + b.step * (((v - (b.start : Int)) / (b.stride : Int)).toNat : Int)
  else 0

private theorem ep_sum_map_zero_int {α : Type} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem ep_sum_eq_single_int (p r : Nat) (hr : r < p) (f : Nat → Int) :
    ((List.range p).map (fun x => if x == r then f x else 0)).sum = f r := by
  induction p with
  | zero => omega
  | succ p ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      by_cases heq : p = r
      · subst r
        have hpre : ((List.range p).map (fun x => if x == p then f x else 0)) =
            (List.range p).map (fun _ => (0 : Int)) := by
          apply List.map_congr_left
          intro x hx
          have hxp : x < p := List.mem_range.mp hx
          rw [beq_eq_false_iff_ne.mpr (by omega)]
          simp
        rw [hpre, ep_sum_map_zero_int]
        simp
      · rw [if_neg (by simp [heq])]
        rw [ih (by omega)]
        omega

private theorem ep_inner_aligned (a b : Lane) (hb : 0 < b.stride) (j : Nat) :
    ((List.range b.n).map (fun i : Nat =>
      if a.start + j * a.stride == b.start + i * b.stride then
        (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
      else 0)).sum =
    (a.base + (j : Int) * a.step) * ep_bvalAt b ((a.start + j * a.stride : Nat) : Int) := by
  let pos : Int := ((a.start + j * a.stride : Nat) : Int)
  let aval : Int := a.base + (j : Int) * a.step
  by_cases hc : contains (positions b) pos = true
  · rcases List.mem_map.mp ((ep_contains_mem (positions b) pos).mp hc) with ⟨i, hi, hival⟩
    have hival' : pos = (b.start : Int) + (i : Int) * (b.stride : Int) := by
      simpa [ep_progVal, positions] using hival.symm
    have huniq : ∀ k < b.n, pos == b.start + k * b.stride → k = i := by
      intro k hk heqk
      have hEq : (b.start : Int) + (k : Int) * (b.stride : Int) =
          (b.start : Int) + (i : Int) * (b.stride : Int) := by
        have hcast : ((b.start + k * b.stride : Nat) : Int) =
            (b.start : Int) + (k : Int) * (b.stride : Int) := by
          push_cast
          rfl
        rw [← hcast]
        exact (beq_iff_eq.mp heqk).symm.trans hival'
      have hmul : k * b.stride = i * b.stride := by
        exact_mod_cast (by omega : (k : Int) * (b.stride : Int) = (i : Int) * (b.stride : Int))
      exact Nat.eq_of_mul_eq_mul_left hb (by simpa [Nat.mul_comm] using hmul)
    have hsum : ((List.range b.n).map (fun k : Nat =>
        if a.start + j * a.stride == b.start + k * b.stride then
          aval * (b.base + b.step * (k : Int))
        else 0)).sum = aval * (b.base + b.step * (i : Int)) := by
      rw [show (List.range b.n).map (fun k : Nat =>
          if a.start + j * a.stride == b.start + k * b.stride then
            aval * (b.base + b.step * (k : Int))
          else 0) =
          (List.range b.n).map (fun k : Nat => if k == i then
            aval * (b.base + b.step * (i : Int)) else 0) by
        apply List.map_congr_left
        intro k hk
        by_cases hkEq : k == i
        · have hki : k = i := beq_iff_eq.mp hkEq
          subst k
          have hnat : a.start + j * a.stride = b.start + i * b.stride := by omega
          rw [if_pos (beq_iff_eq.mpr hnat)]
          simp
        · have hne : a.start + j * a.stride ≠ b.start + k * b.stride := by
            intro hEq
            have hbeq : pos == b.start + k * b.stride := by
              apply beq_iff_eq.mpr
              change ((a.start + j * a.stride : Nat) : Int) =
                ((b.start + k * b.stride : Nat) : Int)
              exact_mod_cast hEq
            have hki := huniq k (List.mem_range.mp hk) hbeq
            exact hkEq (by rw [hki]; simp)
          rw [beq_eq_false_iff_ne.mpr hne]
          by_cases hki : k = i
          · subst k
            exact (hkEq (by simp)).elim
          · simp [hki]]
      rw [ep_sum_eq_single_int b.n i (List.mem_range.mp hi)]
    have hbval : ep_bvalAt b pos = b.base + b.step * (i : Int) := by
      unfold ep_bvalAt
      rw [if_pos hc]
      have hdiv : (((pos - (b.start : Int)) / (b.stride : Int)).toNat) = i := by
        rw [hival']
        have hsub : ((b.start : Int) + (i : Int) * (b.stride : Int) - (b.start : Int)) =
            (i : Int) * (b.stride : Int) := by omega
        rw [hsub, Int.mul_ediv_cancel (i : Int) (by exact_mod_cast (Nat.ne_of_gt hb))]
        simp
      rw [hdiv]
    rw [hsum, hbval]
  · have hzero : ((List.range b.n).map (fun k : Nat =>
        if a.start + j * a.stride == b.start + k * b.stride then
          aval * (b.base + b.step * (k : Int))
        else 0)).sum = 0 := by
      rw [show (List.range b.n).map (fun k : Nat =>
          if a.start + j * a.stride == b.start + k * b.stride then
            aval * (b.base + b.step * (k : Int))
          else 0) = (List.range b.n).map (fun _ : Nat => (0 : Int)) by
        apply List.map_congr_left
        intro k hk
        by_cases hEq : a.start + j * a.stride == b.start + k * b.stride
        · have hEq' : a.start + j * a.stride = b.start + k * b.stride := beq_iff_eq.mp hEq
          have hc' : contains (positions b) pos = true := by
            apply (ep_contains_mem (positions b) pos).mpr
            unfold ep_progVals
            apply List.mem_map.mpr
            refine ⟨k, hk, ?_⟩
            have hcast : ((b.start + k * b.stride : Nat) : Int) = pos := by
              change ((b.start + k * b.stride : Nat) : Int) =
                ((a.start + j * a.stride : Nat) : Int)
              exact_mod_cast hEq'.symm
            change ((b.start + k * b.stride : Nat) : Int) = pos
            exact hcast
          exact (hc hc').elim
        · rw [if_neg hEq]]
      rw [ep_sum_map_zero_int]
    have hbval : ep_bvalAt b pos = 0 := by
      unfold ep_bvalAt
      rw [if_neg hc]
    rw [hzero, hbval]
    simp

private theorem ep_moment_sum (n : Nat) (x y dx dy : Int) :
    ((List.range n).map (fun k : Nat => (x + (k : Int) * dx) * (y + (k : Int) * dy))).sum =
      (n : Int) * x * y + tri n * (x * dy + y * dx) + squares n * dx * dy := by
  induction n with
  | zero => simp [tri, squares, TinylambOpt.ResearchGoals.PairAggregates.choose2]
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      rw [ep_tri_succ, ep_squares_succ]
      simp [Int.natCast_add, Int.natCast_mul]
      grind
private theorem lane_alignedProduct (a b : Lane) (ha : 0 < a.stride) (hb : 0 < b.stride) :
    ((lanePoints a).map (fun r =>
      ((List.range b.n).map (fun (i : Nat) =>
        if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    aligned a b := by
  unfold lanePoints
  rw [List.map_map]
  change ((List.range a.n).map (fun j : Nat =>
      ((List.range b.n).map (fun i : Nat =>
        if a.start + j * a.stride == b.start + i * b.stride then
          (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
        else 0)).sum)).sum = aligned a b
  rw [show (List.range a.n).map (fun j : Nat =>
      ((List.range b.n).map (fun i : Nat =>
        if a.start + j * a.stride == b.start + i * b.stride then
          (a.base + (j : Int) * a.step) * (b.base + b.step * (i : Int))
        else 0)).sum) =
      (List.range a.n).map (fun j : Nat =>
        (a.base + (j : Int) * a.step) * ep_bvalAt b ((a.start + j * a.stride : Nat) : Int)) by
    apply List.map_congr_left
    intro j hj
    exact ep_inner_aligned a b hb j]
  rw [show ((List.range a.n).map (fun j : Nat =>
      (a.base + (j : Int) * a.step) * ep_bvalAt b ((a.start + j * a.stride : Nat) : Int))).sum =
      ((ep_lanePosVals a).map (fun v => ep_posF a v * ep_bvalAt b v)).sum by
    rw [ep_lanePosVals, List.map_map]
    apply congrArg List.sum
    apply List.map_congr_left
    intro j hj
    change (a.base + (j : Int) * a.step) * ep_bvalAt b ((a.start + j * a.stride : Nat) : Int) =
      ep_posF a ((a.start + j * a.stride : Nat) : Int) *
        ep_bvalAt b ((a.start + j * a.stride : Nat) : Int)
    rw [ep_posF_at a ha j]]
  let q := intersection (positions a) (positions b)
  have hqstep : 0 < q.step := ep_intersection_step_pos (positions a) (positions b)
  have hsplit : ((ep_lanePosVals a).map (fun v => ep_posF a v * ep_bvalAt b v)).sum =
      (((ep_lanePosVals a).filter (fun v => contains (positions b) v)).map
        (fun v => ep_posF a v * ep_bvalAt b v)).sum := by
    have hc := ep_sum_filter_complement (ep_lanePosVals a) (fun v => contains (positions b) v)
      (fun v => ep_posF a v * ep_bvalAt b v)
    have hz : (((ep_lanePosVals a).filter (fun v => !contains (positions b) v)).map
        (fun v => ep_posF a v * ep_bvalAt b v)).sum = 0 := by
      rw [show ((ep_lanePosVals a).filter (fun v => !contains (positions b) v)).map
          (fun v => ep_posF a v * ep_bvalAt b v) =
          ((ep_lanePosVals a).filter (fun v => !contains (positions b) v)).map (fun _ => (0 : Int)) by
        apply List.map_congr_left
        intro v hv
        have hnv : ¬ contains (positions b) v := by
          have hb' := (List.mem_filter.mp hv).2
          simpa using hb'
        unfold ep_bvalAt
        rw [if_neg hnv]
        simp]
      exact ep_sum_map_zero_int _
    omega
  rw [hsplit]
  rw [← list_sum_perm ((ep_inter_positions_perm a ha (positions b)).map
    (fun v => ep_posF a v * ep_bvalAt b v))]
  have hbval_inter : ∀ v ∈ ep_progVals q, ep_bvalAt b v = ep_posF b v := by
    intro v hv
    have hc := (ep_contains_mem q v).mpr hv
    have hbboth := (ep_intersection_contains (positions a) (positions b) v).mp hc
    unfold ep_bvalAt ep_posF
    rw [if_pos hbboth.2]
    grind
  rw [show ((ep_progVals q).map (fun v => ep_posF a v * ep_bvalAt b v)).sum =
      ((ep_progVals q).map (fun v => ep_posF a v * ep_posF b v)).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro v hv
    rw [hbval_inter v hv]]
  have hval : ∀ k, k < q.n →
      ep_posF a (ep_progVal q k) * ep_posF b (ep_progVal q k) =
        (firstAt a q + (k : Int) * slopeAt a q) *
          (firstAt b q + (k : Int) * slopeAt b q) := by
    intro k hk
    obtain ⟨ka, hka, hvka, heqa⟩ := ep_posF_inter a ha (positions b) (ep_progVal q k)
      (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    change ep_progVal q k = q.lo + (ka : Int) * (q.step : Int) at hvka
    change ep_posF a (ep_progVal q k) = firstAt a q + (ka : Int) * slopeAt a q at heqa
    change ka < q.n at hka
    have hcq : contains q (ep_progVal q k) = true := (ep_contains_mem q (ep_progVal q k)).mpr
      (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    obtain ⟨kb, hkb, hvkb, heqb⟩ := ep_posF_inter_of b hb q
      (fun v h => ((ep_intersection_contains (positions a) (positions b) v).mp h).2)
      (ep_progVal q k) (List.mem_map.mpr ⟨k, List.mem_range.mpr hk, rfl⟩)
    have hkab : ka = kb := by
      have hEq : ep_progVal q ka = ep_progVal q kb := by
        change q.lo + (ka : Int) * (q.step : Int) = q.lo + (kb : Int) * (q.step : Int)
        rw [← hvka, ← hvkb]
      exact ep_progVal_inj_of_pos q hqstep ka kb hka hkb hEq
    have heqb' : ep_posF b (ep_progVal q k) = firstAt b q + (ka : Int) * slopeAt b q := by
      rw [← hkab] at heqb
      exact heqb
    have hkaka : ka = k := by
      apply ep_progVal_inj_of_pos q hqstep ka k hka hk
      change q.lo + (ka : Int) * (q.step : Int) = q.lo + (k : Int) * (q.step : Int)
      rw [← hvka]
      rfl
    rw [hkaka] at heqa heqb'
    rw [heqa, heqb']
  unfold aligned
  change ((ep_progVals q).map (fun v => ep_posF a v * ep_posF b v)).sum =
    (q.n : Int) * firstAt a (intersection (positions a) (positions b)) *
        firstAt b (intersection (positions a) (positions b)) +
      tri q.n * (firstAt a q * slopeAt b q + firstAt b q * slopeAt a q) +
      squares q.n * slopeAt a q * slopeAt b q
  rw [show ep_progVals q = (List.range q.n).map (fun k : Nat => ep_progVal q k) by rfl, List.map_map]
  change ((List.range q.n).map (fun k : Nat =>
    ep_posF a (ep_progVal q k) * ep_posF b (ep_progVal q k))).sum = _
  rw [show (List.range q.n).map (fun k : Nat =>
      ep_posF a (ep_progVal q k) * ep_posF b (ep_progVal q k)) =
      (List.range q.n).map (fun k : Nat =>
        (firstAt a q + (k : Int) * slopeAt a q) *
          (firstAt b q + (k : Int) * slopeAt b q)) by
    apply List.map_congr_left
    intro k hk
    exact hval k (List.mem_range.mp hk)]
  exact ep_moment_sum q.n (firstAt a q) (firstAt b q) (slopeAt a q) (slopeAt b q)


private theorem residueJoin_of_flatMap (rowsEq : List (Nat × Int)) (ls : List Lane) (b : Lane)
    (hrows : rowsEq = ls.flatMap lanePoints) :
    (rowsEq.map (fun r => (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
    (ls.map (joinCount · b)).sum := by
  rw [hrows, sum_flatMap_nat ls lanePoints (fun r => (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a _ha
  have h := lane_joinCount a b
  exact Int.ofNat.inj h

private theorem deleteProgression_of_flatMap (rowsEq : List (Nat × Int)) (ls : List Lane) (mask : Progression)
    (hrows : rowsEq = ls.flatMap lanePoints) (hs : ∀ a ∈ ls, 0 < a.stride) :
    (((rowsEq).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    (ls.map (fun a => partialSum a a.n - deletedSum a mask)).sum := by
  rw [hrows]
  rw [sum_filter_flatMap ls lanePoints (fun r => !contains mask r.1) Prod.snd]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact lane_deleteProgression a (hs a ha) mask

private theorem alignedProduct_of_flatMap (rowsEq : List (Nat × Int)) (ls : List Lane) (b : Lane)
    (hrows : rowsEq = ls.flatMap lanePoints) (hb : 0 < b.stride) (hs : ∀ a ∈ ls, 0 < a.stride) :
    (rowsEq.map (fun r =>
      ((List.range b.n).map (fun i =>
        if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    (ls.map (aligned · b)).sum := by
  rw [hrows]
  rw [sum_flatMap_int ls lanePoints (fun r =>
      ((List.range b.n).map (fun i =>
        if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact lane_alignedProduct a b (hs a ha) hb

/-! ## Row decompositions and generic goal proofs -/

private theorem lanePoints_span (c : Config) (lo hi : Nat) (z d : Int) :
    lanePoints (span c lo hi z d) =
      (List.range (min c.n hi - lo)).map (fun i => (lo + i, z + d * (i : Int))) := by
  unfold lanePoints span lane
  have hcount : (min c.n hi - lo + 1 - 1) / 1 = min c.n hi - lo := by omega
  simp only [Nat.mul_one, Nat.add_zero, hcount]
  apply List.map_congr_left
  intro i _hi
  apply Prod.ext
  · simp
  · simp [Int.mul_comm]

namespace DeletedSpan

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.DeletedSpan

private theorem rows_flatMap (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  rw [rows_eq]
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  rw [lanePoints_span, lanePoints_span]
  simp only [width, Nat.min_self, Nat.add_zero]
  congr 1
  · apply List.map_congr_left
    intro i _hi
    simp
  · apply List.map_congr_left
    intro i _hi
    apply Prod.ext
    · simp
    · push_cast
      simp [Int.mul_add, Int.add_assoc]

theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_flatMap (rows layout c) (layout.lanes c) b (rows_flatMap c)

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact deleteProgression_of_flatMap (rows layout c) (layout.lanes c) mask (rows_flatMap c) (by
    intro a ha
    simp [layout, span, lane] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_of_flatMap (rows layout c) (layout.lanes c) b (rows_flatMap c) hb (by
    intro a ha
    simp [layout, span, lane] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

end DeletedSpan

namespace RotatedPage

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage

private theorem rows_flatMap (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  rw [rows_map]
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  by_cases hn : c.n = 0
  · simp [hn, span, lane, lanePoints]
  · have hpos : 0 < c.n := Nat.pos_of_ne_zero hn
    let s := c.split % c.n
    have hs : s < c.n := Nat.mod_lt _ hpos
    have hts : c.n - s + s = c.n := by omega
    have h1 : span c 0 (c.n - s) (c.base + c.step * (s : Int)) c.step =
        ⟨0, 1, c.n - s, c.base + c.step * (s : Int), c.step⟩ := by
      unfold span lane
      simp [Nat.min_eq_right (show c.n - s ≤ c.n by omega)]
    have h2 : span c (c.n - s) c.n c.base c.step = ⟨c.n - s, 1, s, c.base, c.step⟩ := by
      unfold span lane
      have hmin : min c.n c.n = c.n := Nat.min_self c.n
      have hsub : c.n - (c.n - s) = s := by omega
      simp [hmin, hsub]
    rw [h1, h2]
    unfold lanePoints
    simp only [Nat.zero_add, Nat.one_mul, Nat.add_zero]
    rw [show List.range c.n = List.range (c.n - s + s) by rw [hts]]
    rw [List.range_add, List.map_append, List.map_map]
    congr 1
    · apply List.map_congr_left
      intro i hi
      have hi' : i < c.n - s := List.mem_range.mp hi
      have hlt : i + s < c.n := by omega
      have hmod : (i + s) % c.n = i + s := Nat.mod_eq_of_lt hlt
      rw [hmod]
      apply Prod.ext
      · simp
      · push_cast
        simp [Int.mul_add, Int.mul_comm, Int.add_assoc, Int.add_left_comm, Int.add_comm]
    · apply List.map_congr_left
      intro j hj
      have hj' : j < s := List.mem_range.mp hj
      have hval : c.n - s + j + s = c.n + j := by omega
      have hmod : (c.n - s + j + s) % c.n = j := by
        rw [hval, Nat.add_mod_left, Nat.mod_eq_of_lt (by omega : j < c.n)]
      simp only [Function.comp, Nat.one_mul]
      apply Prod.ext
      · simp
      · have hmod' : ((c.n - s + j) + c.split % c.n) % c.n = j := by simpa [s] using hmod
        simp [hmod', Int.mul_comm]

theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_flatMap (rows layout c) (layout.lanes c) b (rows_flatMap c)

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact deleteProgression_of_flatMap (rows layout c) (layout.lanes c) mask (rows_flatMap c) (by
    intro a ha
    simp [layout, span, lane] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_of_flatMap (rows layout c) (layout.lanes c) b (rows_flatMap c) hb (by
    intro a ha
    simp [layout, span, lane] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

end RotatedPage

namespace RestartedDelta

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

private theorem rows_flatMap (c : Config) :
    rows layout c = (layout.lanes c).flatMap lanePoints := by
  unfold rows
  simp only [layout, List.flatMap_cons, List.flatMap_nil, List.append_nil]
  rw [lanePoints_span, lanePoints_span]
  have hsplit : (List.range c.n).filterMap (fun i =>
      (if i < c.split then some (c.base + c.step * (i : Int))
       else some (c.bias + c.step * ((i - c.split : Nat) : Int))).map (fun v => (i, v))) =
      (List.range c.n).map (fun i =>
        (i, if i < c.split then c.base + c.step * (i : Int)
            else c.bias + c.step * ((i - c.split : Nat) : Int))) := by
    rw [← filterMap_some_eq_map]
    apply filterMap_congr'
    intro i _hi
    by_cases h : i < c.split <;> simp [h]
  rw [hsplit]
  by_cases hle : c.split ≤ c.n
  · have h1 : min c.n c.split = c.split := Nat.min_eq_right hle
    have h2 : min c.n c.n - c.split = c.n - c.split := by omega
    have hsum : c.split + (c.n - c.split) = c.n := Nat.add_sub_cancel' hle
    have hrange : List.range c.n = List.range c.split ++ (List.range (c.n - c.split)).map (fun j => c.split + j) := by
      rw [← hsum, List.range_add, Nat.add_sub_cancel_left]
    rw [hrange, List.map_append, List.map_map]
    simp only [h1, h2, Nat.add_zero]
    congr 1
    · apply List.map_congr_left
      intro i hi
      have hi' : i < c.split := List.mem_range.mp hi
      simp [hi']
    · have hmap : List.map
          ((fun i => (i, if i < c.split then c.base + c.step * (i : Int)
              else c.bias + c.step * ((i - c.split : Nat) : Int))) ∘ fun j => c.split + j)
          (List.range (c.n - c.split)) =
        List.map (fun i => (c.split + i, c.bias + c.step * (i : Int))) (List.range (c.n - c.split)) := by
        apply List.map_congr_left
        intro j hj
        have hj' : j < c.n - c.split := List.mem_range.mp hj
        have hnot : ¬ c.split + j < c.split := by omega
        have hsub : c.split + j - c.split = j := Nat.add_sub_cancel_left _ _
        simp [Function.comp, hnot, hsub]
      rw [hmap]
  · have hn : c.n ≤ c.split := by omega
    have h1 : min c.n c.split = c.n := Nat.min_eq_left hn
    have h2 : min c.n c.n - c.split = 0 := by omega
    simp only [h1, h2, List.range_zero, List.map_nil, List.append_nil, Nat.add_zero]
    apply List.map_congr_left
    intro i hi
    have hi' : i < c.n := List.mem_range.mp hi
    have : i < c.split := Nat.lt_of_lt_of_le hi' hn
    simp [this]

theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_flatMap (rows layout c) (layout.lanes c) b (rows_flatMap c)

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact deleteProgression_of_flatMap (rows layout c) (layout.lanes c) mask (rows_flatMap c) (by
    intro a ha
    simp [layout, span, lane] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_of_flatMap (rows layout c) (layout.lanes c) b (rows_flatMap c) hb (by
    intro a ha
    simp [layout, span, lane] at ha
    rcases ha with rfl | rfl <;> simp [span, lane])

end RestartedDelta

namespace GrayTile

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile

private theorem rows_range_map (lay : Layout) (c : Config) (f : Nat → Int)
    (h : ∀ i, i < c.n → lay.read c i = some (f i)) :
    rows lay c = (List.range c.n).map (fun i => (i, f i)) := by
  unfold rows
  exact range_filterMap_all c.n (fun i => (lay.read c i).map (fun v => (i, v)))
    (fun i => (i, f i)) (by intro i hi; simp [h i hi])

private theorem rows_gray_pairs (c : Config) :
    rows layout c =
      (List.range c.n).map (fun i => (i, c.base + c.step *
        ((i / width c * width c + ep_gray (i % width c) : Nat) : Int))) := by
  apply rows_range_map
  intro i hi
  simp [GrayTile.layout, ep_gray]

private theorem ceilDiv_eq (d s : Nat) (hs : 0 < s) : d + s - 1 - (s - 1) = d := by omega

private theorem pos_lt_iff (start stride i p : Nat) (hs : 0 < stride) :
    start + i * stride < p ↔ i < (p - start + stride - 1) / stride := by
  by_cases hps : p ≤ start
  · have hp0 : p - start = 0 := by omega
    have hzero : (p - start + stride - 1) / stride = 0 := by
      rw [hp0, Nat.zero_add]
      exact Nat.div_eq_of_lt (by omega)
    rw [hzero]
    omega
  · have hps' : start < p := by omega
    rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start :=
      ceilDiv_eq (p - start) stride hs
    rw [hrewrite]
    omega

private theorem mem_lanePoints (a : Lane) (x : Nat × Int) :
    x ∈ lanePoints a ↔ ∃ q, q < a.n ∧ x = (a.start + q * a.stride, a.base + (q : Int) * a.step) := by
  unfold lanePoints
  rw [List.mem_map]
  constructor
  · rintro ⟨q, hq, hval⟩
    exact ⟨q, List.mem_range.mp hq, hval.symm⟩
  · rintro ⟨q, hq, hval⟩
    exact ⟨q, List.mem_range.mpr hq, hval.symm⟩

private theorem lanePoints_nodup (a : Lane) (hs : 0 < a.stride) : (lanePoints a).Nodup := by
  unfold lanePoints
  apply ep_nodup_map_of_injective
  · intro i hi j hj h
    have hfst := congrArg Prod.fst h
    have hmul : i * a.stride = j * a.stride := Nat.add_left_cancel hfst
    exact Nat.eq_of_mul_eq_mul_right hs hmul
  · exact List.nodup_range

private theorem gray_cross_lane_ne (c : Config) :
    let w := width c
    ∀ r1 r2, r1 < min w c.n → r2 < min w c.n → r1 ≠ r2 →
      ∀ x ∈ lanePoints (lane c.n r1 w (c.base + c.step * ep_gray r1) (c.step * w)),
      ∀ y ∈ lanePoints (lane c.n r2 w (c.base + c.step * ep_gray r2) (c.step * w)), x ≠ y := by
  dsimp only
  intro r1 r2 hr1 hr2 hrne x hx y hy heq
  rcases (mem_lanePoints _ x).mp hx with ⟨j1, hj1, hxv⟩
  rcases (mem_lanePoints _ y).mp hy with ⟨j2, hj2, hyv⟩
  have hfst := congrArg Prod.fst (hxv.symm.trans (heq.trans hyv))
  dsimp [lane] at hfst
  have hw : 0 < width c := by dsimp [width]; omega
  have hm1 : (r1 + j1 * width c) % width c = r1 := by
    rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt (by omega)]
  have hm2 : (r2 + j2 * width c) % width c = r2 := by
    rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt (by omega)]
  have hr : r1 = r2 := by
    rw [← hm1, ← hm2, hfst]
  exact hrne hr

-- Gray tiles permute the values within each stripe, so the rows are a
-- permutation of the flat-mapped lanes rather than literally equal to them.
private theorem rows_flatMap (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  rw [rows_gray_pairs c]
  have hw : 0 < width c := by dsimp [width]; omega
  let laneAt : Nat → Lane := fun r => lane c.n r (width c) (c.base + c.step * ep_gray r) (c.step * width c)
  have hlanes : layout.lanes c = (List.range (min (width c) c.n)).map laneAt := by
    dsimp [GrayTile.layout, striped, laneAt]
    simp [ep_gray]
  rw [hlanes]
  have hL : ((List.range c.n).map (fun i => (i, c.base + c.step *
      ((i / width c * width c + ep_gray (i % width c) : Nat) : Int)))).Nodup := by
    apply ep_nodup_map_of_injective
    · intro i hi j hj h
      exact congrArg Prod.fst h
    · exact List.nodup_range
  have hcross := gray_cross_lane_ne c
  have hR2gen : ∀ xs : List Nat, xs.Nodup → (∀ a ∈ xs, a < min (width c) c.n) →
      xs.Pairwise (fun r1 r2 => ∀ x ∈ lanePoints (laneAt r1),
        ∀ y ∈ lanePoints (laneAt r2), x ≠ y) := by
    intro xs
    induction xs with
    | nil => simp
    | cons a as ih =>
        intro hnd hbound
        rw [List.nodup_cons] at hnd
        rcases hnd with ⟨ha_not, hnd_as⟩
        rw [List.pairwise_cons]
        constructor
        · intro b hb
          apply hcross a b (hbound a (by simp)) (hbound b (by simp [hb]))
          intro hr
          subst b
          exact ha_not hb
        · apply ih hnd_as
          intro b hb
          exact hbound b (by simp [hb])
  have hR2 := hR2gen (List.range (min (width c) c.n)) List.nodup_range (fun a ha => List.mem_range.mp ha)
  have hR : ((List.range (min (width c) c.n)).flatMap (fun r => lanePoints (laneAt r))).Nodup := by
    rw [List.nodup_iff_pairwise_ne, List.pairwise_flatMap]
    constructor
    · intro r hr
      apply lanePoints_nodup
      dsimp [laneAt, lane]
      simp [hw]
    · exact hR2
  rw [List.flatMap_map]
  apply (List.perm_ext_iff_of_nodup hL hR).mpr
  intro x
  constructor
  · intro hx
    rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
    have hin : i < c.n := List.mem_range.mp hi
    let r := i % width c
    let j := i / width c
    have hr : r < min (width c) c.n := by
      exact (Nat.lt_min).mpr ⟨Nat.mod_lt _ hw, Nat.lt_of_le_of_lt (Nat.mod_le _ _) hin⟩
    have hi_decomp : i = r + j * width c := by
      dsimp [r, j]
      have hd := Nat.div_add_mod i (width c)
      rw [Nat.mul_comm (width c) (i / width c)] at hd
      omega
    have hj : j < (lane c.n r (width c) (c.base + c.step * ep_gray r) (c.step * width c)).n := by
      unfold lane
      apply (pos_lt_iff r (width c) j c.n hw).mp
      omega
    apply List.mem_flatMap.mpr
    refine ⟨r, List.mem_range.mpr hr, ?_⟩
    apply (mem_lanePoints (laneAt r) _).mpr
    refine ⟨j, by simpa [laneAt, lane] using hj, ?_⟩
    apply Prod.ext
    · dsimp [laneAt, lane]
      omega
    · dsimp [laneAt, lane, r, j]
      simp only [Int.mul_add, Int.natCast_add, Int.natCast_mul]
      ac_rfl
  · intro hx
    rcases List.mem_flatMap.mp hx with ⟨r, hr, hx⟩
    rcases (mem_lanePoints (laneAt r) x).mp hx with ⟨j, hj, rfl⟩
    have hr' : r < min (width c) c.n := List.mem_range.mp hr
    have hjlt : j < (lane c.n r (width c) (c.base + c.step * ep_gray r) (c.step * width c)).n := by
      simpa [laneAt] using hj
    have hi : r + j * width c < c.n := by
      dsimp [lane] at hjlt
      exact (pos_lt_iff r (width c) j c.n hw).mpr hjlt
    apply List.mem_map.mpr
    refine ⟨r + j * width c, List.mem_range.mpr hi, ?_⟩
    apply Prod.ext
    · dsimp [laneAt, lane]
    · have hdivij : (r + j * width c) / width c = j := by
        rw [Nat.mul_comm j (width c), Nat.add_mul_div_left r j hw,
          Nat.div_eq_of_lt (by omega)]
        simp
      have hmodij : (r + j * width c) % width c = r := by
        rw [Nat.mul_comm j (width c), Nat.add_mul_mod_self_left,
          Nat.mod_eq_of_lt (by omega)]
      rw [hdivij, hmodij]
      dsimp [laneAt, lane]
      simp only [Int.mul_add, Int.natCast_add, Int.natCast_mul]
      ac_rfl

private theorem list_sum_perm_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem residueJoin_of_flatMap_perm (rowsEq : List (Nat × Int)) (ls : List Lane) (b : Lane)
    (hrows : rowsEq.Perm (ls.flatMap lanePoints)) :
    (rowsEq.map (fun r => (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
    (ls.map (joinCount · b)).sum := by
  have hp := hrows.map (fun r : Nat × Int =>
    (List.range b.n).countP (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))
  rw [list_sum_perm_nat hp]
  exact residueJoin_of_flatMap (ls.flatMap lanePoints) ls b rfl

private theorem deleteProgression_of_flatMap_perm (rowsEq : List (Nat × Int)) (ls : List Lane) (mask : Progression)
    (hrows : rowsEq.Perm (ls.flatMap lanePoints)) (hs : ∀ a ∈ ls, 0 < a.stride) :
    (((rowsEq).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    (ls.map (fun a => partialSum a a.n - deletedSum a mask)).sum := by
  have hp := (List.Perm.filter (fun r : Nat × Int => !contains mask r.1) hrows).map Prod.snd
  rw [list_sum_perm hp]
  exact deleteProgression_of_flatMap (ls.flatMap lanePoints) ls mask rfl hs

private theorem alignedProduct_of_flatMap_perm (rowsEq : List (Nat × Int)) (ls : List Lane) (b : Lane)
    (hrows : rowsEq.Perm (ls.flatMap lanePoints)) (hb : 0 < b.stride) (hs : ∀ a ∈ ls, 0 < a.stride) :
    (rowsEq.map (fun r =>
      ((List.range b.n).map (fun i =>
        if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    (ls.map (aligned · b)).sum := by
  have hp := hrows.map (fun r : Nat × Int =>
    ((List.range b.n).map (fun i : Nat =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)
  rw [list_sum_perm hp]
  exact alignedProduct_of_flatMap (ls.flatMap lanePoints) ls b rfl hb hs

theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_flatMap_perm (rows layout c) (layout.lanes c) b (rows_flatMap c)

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact deleteProgression_of_flatMap_perm (rows layout c) (layout.lanes c) mask (rows_flatMap c) (by
    intro a ha
    simp [layout, striped, lane, width] at ha
    rcases ha with ⟨r, hr, rfl⟩
    simp [lane, width])

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_of_flatMap_perm (rows layout c) (layout.lanes c) b (rows_flatMap c) hb (by
    intro a ha
    simp [layout, striped, lane, width] at ha
    rcases ha with ⟨r, hr, rfl⟩
    simp [lane, width])

end GrayTile

namespace PagedTombstones

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones

/-! The epoch lanes enumerate surviving ordinals in transposed (lane-major)
order, so only a permutation identity holds here, not list equality. -/

private theorem list_sum_perm_nat {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp; omega
  | trans p₁ p₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem pos_lt_iff (start stride i p : Nat) (hs : 0 < stride) :
    start + i * stride < p ↔ i < (p - start + stride - 1) / stride := by
  by_cases hps : p ≤ start
  · have hp0 : p - start = 0 := by omega
    have hzero : (p - start + stride - 1) / stride = 0 := by
      rw [hp0, Nat.zero_add]
      exact Nat.div_eq_of_lt (by omega)
    rw [hzero]
    omega
  · rw [Nat.lt_div_iff_mul_lt hs]
    have hrewrite : p - start + stride - 1 - (stride - 1) = p - start := by omega
    rw [hrewrite]
    omega

private theorem transpose_perm (P n : Nat) (hP : 0 < P) (keep : Nat → Bool) (V : Nat → Nat → Int) :
    (((List.range n).filter (fun i => keep (i % P))).map (fun i => (i, V (i % P) (i / P)))).Perm
    (((List.range (min P n)).filter keep).flatMap (fun r =>
      (List.range ((n - r + P - 1)/P)).map (fun j => (r + j*P, V r j)))) := by
  apply (List.perm_ext_iff_of_nodup ?_ ?_).mpr
  · intro a
    obtain ⟨i0, v0⟩ := a
    simp only [List.mem_map, List.mem_filter, List.mem_range, List.mem_flatMap, Prod.mk.injEq,
      decide_eq_true_eq]
    constructor
    · rintro ⟨i, ⟨hi, hk⟩, rfl, rfl⟩
      refine ⟨i % P, ⟨?_, hk⟩, i / P, ?_, ?_, rfl⟩
      · have := Nat.mod_lt i hP
        have := Nat.mod_le i P
        omega
      · have h := (pos_lt_iff (i % P) P (i / P) n hP).mp (by
          have := Nat.mod_add_div i P
          rw [Nat.mul_comm] at this
          omega)
        exact h
      · have := Nat.mod_add_div i P
        rw [Nat.mul_comm] at this
        omega
    · rintro ⟨r, ⟨hr, hk⟩, j, hj, rfl, rfl⟩
      have hrP : r < P := by omega
      have hlt := (pos_lt_iff r P j n hP).mpr hj
      have hmod : (r + j * P) % P = r := by
        rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrP]
      have hdiv : (r + j * P) / P = j := by
        rw [Nat.add_mul_div_right _ _ hP, Nat.div_eq_of_lt hrP]; omega
      refine ⟨r + j * P, ⟨hlt, ?_⟩, rfl, ?_⟩
      · rw [hmod]; exact hk
      · rw [hmod, hdiv]
  · have hnd : ((List.range n).filter (fun i => keep (i % P))).Pairwise (· ≠ ·) :=
      List.Pairwise.sublist List.filter_sublist List.nodup_range
    exact List.Pairwise.map _ (fun a b h hab => h (Prod.mk.inj hab).1) hnd
  · show List.Pairwise (· ≠ ·) _
    rw [List.pairwise_flatMap]
    constructor
    · intro r _
      have hnd : (List.range ((n - r + P - 1)/P)).Pairwise (· ≠ ·) := List.nodup_range
      exact List.Pairwise.map _ (fun a b h hab => by
        have := (Prod.mk.inj hab).1
        exact h (Nat.eq_of_mul_eq_mul_right hP (Nat.add_left_cancel this))) hnd
    · have hnd : ((List.range (min P n)).filter keep).Pairwise (· ≠ ·) :=
        List.Pairwise.sublist List.filter_sublist List.nodup_range
      apply hnd.imp_of_mem
      intro r r' hr hr' hne
      intro a ha b hb hab
      simp only [List.mem_map, List.mem_range] at ha hb
      obtain ⟨j, hj, rfl⟩ := ha
      obtain ⟨j', hj', rfl⟩ := hb
      have h1 := (Prod.mk.inj hab).1
      have hrP : r < P := by
        have := (List.mem_filter.mp hr).1
        simp only [List.mem_range] at this; omega
      have hrP' : r' < P := by
        have := (List.mem_filter.mp hr').1
        simp only [List.mem_range] at this; omega
      have e1 : (r + j * P) % P = r := by rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrP]
      have e2 : (r' + j' * P) % P = r' := by rw [Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hrP']
      rw [h1] at e1
      exact hne (by omega)

private theorem rows_eq_filter_map (lay : Layout) (c : Config) (p : Nat → Bool) (g : Nat → Int)
    (h : ∀ i, lay.read c i = if p i then some (g i) else none) :
    rows lay c = ((List.range c.n).filter p).map (fun i => (i, g i)) := by
  unfold rows
  induction (List.range c.n) with
  | nil => rfl
  | cons x xs ih =>
    simp only [List.filterMap_cons, List.filter_cons, h x]
    by_cases hx : p x = true <;> simp [hx, ih]

private theorem rows_perm_lanes (lay : Layout) (c : Config) (P : Nat) (hP : 0 < P)
    (p keep : Nat → Bool) (g : Nat → Int) (V : Nat → Nat → Int)
    (h : ∀ i, lay.read c i = if p i then some (g i) else none)
    (hp : ∀ i, i < c.n → p i = keep (i % P)) (hg : ∀ i, i < c.n → g i = V (i % P) (i / P))
    (hl : (lay.lanes c).flatMap lanePoints =
      ((List.range (min P c.n)).filter keep).flatMap (fun r =>
        (List.range ((c.n - r + P - 1)/P)).map (fun j => (r + j*P, V r j)))) :
    (rows lay c).Perm ((lay.lanes c).flatMap lanePoints) := by
  rw [hl, rows_eq_filter_map lay c p g h]
  have e : ((List.range c.n).filter p).map (fun i => (i, g i)) =
      ((List.range c.n).filter (fun i => keep (i % P))).map (fun i => (i, V (i % P) (i / P))) := by
    rw [List.filter_congr (fun i hi => hp i (List.mem_range.mp hi))]
    apply List.map_congr_left
    intro i hi
    have := (List.mem_filter.mp hi).1
    rw [hg i (List.mem_range.mp this)]
  rw [e]
  exact transpose_perm P c.n hP keep V

private theorem filter_lt_range (m w : Nat) :
    (List.range m).filter (fun r => decide (r < w)) = List.range (min m w) := by
  induction m with
  | zero => simp
  | succ m ih =>
    rw [List.range_succ, List.filter_append, ih]
    by_cases h : m < w
    · have h1 : min (m+1) w = m + 1 := by omega
      have h2 : min m w = m := by omega
      rw [h1, h2, List.range_succ]
      simp [h]
    · have h1 : min (m+1) w = min m w := by omega
      rw [h1]
      simp [h]

private theorem paged_iff (w i : Nat) (hw : 0 < w) : (i / w) % 2 = 0 ↔ i % (2 * w) < w := by
  have h := Nat.mod_mul (x := i) (a := w) (b := 2)
  rw [Nat.mul_comm 2 w]
  have hlt := Nat.mod_lt i hw
  rcases (by omega : i / w % 2 = 0 ∨ i / w % 2 = 1) with h0 | h1
  · rw [h0] at h; simp at h; omega
  · rw [h1] at h; simp at h; omega

private theorem rows_flatMap (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  have hw : 0 < width c := by unfold width; omega
  refine rows_perm_lanes layout c (2 * width c) (by omega)
    (fun i => (i / width c) % 2 == 0) (fun r => decide (r < width c))
    (fun i => c.base + c.step * (i : Int))
    (fun r j => (c.base + c.step * (r : Int)) + (j : Int) * (c.step * (2 * (width c : Int))))
    (fun i => by simp only [layout]) ?_ ?_ ?_
  · intro i _
    have := paged_iff (width c) i hw
    exact Bool.eq_iff_iff.mpr (by simpa using this)
  · intro i _
    have hi : (i : Int) = ((i % (2 * width c) : Nat) : Int) +
        ((2 * width c : Nat) : Int) * ((i / (2 * width c) : Nat) : Int) := by
      have := Nat.mod_add_div i (2 * width c)
      exact_mod_cast this.symm
    rw [hi]
    simp [Int.mul_add, Int.mul_comm, Int.mul_left_comm, Int.add_assoc]
  · simp only [layout, List.flatMap_map]
    rw [filter_lt_range]
    rw [show min (min (2 * width c) c.n) (width c) = min c.n (width c) by omega]
    simp [lanePoints, lane]

private theorem residueJoin_of_perm (rowsEq : List (Nat × Int)) (ls : List Lane) (b : Lane)
    (hrows : rowsEq.Perm (ls.flatMap lanePoints)) :
    (rowsEq.map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base + b.step * (i : Int)))).sum =
    (ls.map (joinCount · b)).sum := by
  let f : Nat × Int → Nat := fun r => (List.range b.n).countP
    (fun (i : Nat) => r.2 == b.base + b.step * (i : Int))
  have hp : List.Perm (rowsEq.map f) ((ls.flatMap lanePoints).map f) := hrows.map f
  rw [list_sum_perm_nat hp, sum_flatMap_nat ls lanePoints f]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  have h := lane_joinCount a b
  exact Int.ofNat.inj h

private theorem deleteProgression_of_perm (rowsEq : List (Nat × Int)) (ls : List Lane) (mask : Progression)
    (hrows : rowsEq.Perm (ls.flatMap lanePoints)) (hs : ∀ a ∈ ls, 0 < a.stride) :
    (((rowsEq).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    (ls.map (fun a => partialSum a a.n - deletedSum a mask)).sum := by
  have hp := hrows.filter (fun r : Nat × Int => !contains mask r.1)
  rw [list_sum_perm (hp.map Prod.snd)]
  rw [sum_filter_flatMap ls lanePoints (fun r => !contains mask r.1) Prod.snd]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact lane_deleteProgression a (hs a ha) mask

private theorem alignedProduct_of_perm (rowsEq : List (Nat × Int)) (ls : List Lane) (b : Lane)
    (hrows : rowsEq.Perm (ls.flatMap lanePoints)) (hb : 0 < b.stride) (hs : ∀ a ∈ ls, 0 < a.stride) :
    (rowsEq.map (fun r =>
      ((List.range b.n).map (fun i =>
        if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum)).sum =
    (ls.map (aligned · b)).sum := by
  let f : Nat × Int → Int := fun r =>
    ((List.range b.n).map (fun i =>
      if r.1 == b.start + i * b.stride then r.2 * (b.base + b.step * (i : Int)) else 0)).sum
  have hp : List.Perm (rowsEq.map f) ((ls.flatMap lanePoints).map f) := hrows.map f
  rw [list_sum_perm hp, sum_flatMap_int ls lanePoints f]
  apply congrArg List.sum
  apply List.map_congr_left
  intro a ha
  exact lane_alignedProduct a b (hs a ha) hb

theorem ResidueJoin_sound : ResidueJoin.Goal := by
  intro c b
  exact residueJoin_of_perm (rows layout c) (layout.lanes c) b (rows_flatMap c)

theorem DeleteProgression_sound : DeleteProgression.Goal := by
  intro c mask
  exact deleteProgression_of_perm (rows layout c) (layout.lanes c) mask (rows_flatMap c) (by
    intro a ha
    simp [layout, lane, width] at ha
    rcases ha with ⟨r, hr, rfl⟩
    simp [lane, width])

theorem AlignedProduct_sound : AlignedProduct.Goal := by
  intro c b hb
  exact alignedProduct_of_perm (rows layout c) (layout.lanes c) b (rows_flatMap c) hb (by
    intro a ha
    simp [layout, lane, width] at ha
    rcases ha with ⟨r, hr, rfl⟩
    simp [lane, width])

end PagedTombstones

end TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkD
