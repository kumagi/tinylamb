import TinylambOpt.ResearchGoals.Atlas.EncodedPages
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_GrayTile_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_PagedTombstones_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_DeletedSpan_Remaining
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_DeletedSpan_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_RotatedPage_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_RestartedDelta_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_EncodedPages_RestartedDelta_Remaining
import TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E7

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

private theorem nat_sum_perm {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
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

private theorem mod_two_mul_lt (i w : Nat) (hw : 0 < w) :
    ((i / w) % 2 == 0) = decide (i % (2 * w) < w) := by
  have h2 : 0 < 2 * w := by omega
  obtain ⟨q, r, hr, hrfl⟩ : ∃ q r, r < 2 * w ∧ i = 2 * w * q + r :=
    ⟨i / (2 * w), i % (2 * w), Nat.mod_lt _ h2, (Nat.div_add_mod i (2 * w)).symm⟩
  rw [hrfl]
  have hdivw : (2 * w * q + r) / w = 2 * q + r / w := by
    have : 2 * w * q + r = w * (2 * q) + r := by rw [Nat.mul_assoc, Nat.mul_left_comm]
    rw [this, Nat.mul_add_div hw]
  have hmodw : (2 * w * q + r) % (2 * w) = r := by
    rw [Nat.mul_add_mod]
    exact Nat.mod_eq_of_lt hr
  rw [hdivw, hmodw]
  by_cases hlt : r < w
  · have hrd : r / w = 0 := Nat.div_eq_of_lt hlt
    simp [hrd, hlt]
  · have hge : w ≤ r := Nat.le_of_not_gt hlt
    have h1 : r / w = 1 := by
      have hlt2 : r / w < 2 := (Nat.div_lt_iff_lt_mul hw).2 (by omega)
      have hge1 : 1 ≤ r / w := (Nat.le_div_iff_mul_le hw).2 (by omega)
      omega
    simp [h1, hlt]

private theorem count_mod_lt (n w : Nat) (hw : 0 < w) :
    ((List.range n).filter (fun i => decide (i % (2 * w) < w))).length =
      (n / (2 * w)) * w + min w (n % (2 * w)) := by
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.range_succ, List.filter_append, List.length_append, ih]
    have h2 : 0 < 2 * w := by omega
    obtain ⟨q, r, hr, rfl⟩ : ∃ q r, r < 2 * w ∧ n = 2 * w * q + r :=
      ⟨n / (2 * w), n % (2 * w), Nat.mod_lt _ h2, (Nat.div_add_mod n (2 * w)).symm⟩
    have e1 : (2 * w * q + r) / (2 * w) = q := by
      rw [Nat.mul_add_div h2]; simp [Nat.div_eq_of_lt hr]
    have e2 : (2 * w * q + r) % (2 * w) = r := by
      rw [Nat.mul_add_mod]; exact Nat.mod_eq_of_lt hr
    rw [e1, e2]
    simp only [List.filter_cons, List.filter_nil, List.length_append, List.length_cons,
      List.length_nil, e2]
    by_cases hlt : r < w
    · by_cases hr1 : r + 1 < 2 * w
      · have a1 : (2 * w * q + r + 1) / (2 * w) = q := by
          have : 2 * w * q + r + 1 = 2 * w * q + (r + 1) := Nat.add_assoc _ _ _
          rw [this, Nat.mul_add_div h2]; simp [Nat.div_eq_of_lt hr1]
        have a2 : (2 * w * q + r + 1) % (2 * w) = r + 1 := by
          have : 2 * w * q + r + 1 = 2 * w * q + (r + 1) := Nat.add_assoc _ _ _
          rw [this, Nat.mul_add_mod]; exact Nat.mod_eq_of_lt hr1
        have hminr : min w r = r := Nat.min_eq_right (Nat.le_of_lt hlt)
        have hminr1 : min w (r + 1) = r + 1 := Nat.min_eq_right (by omega)
        simp [a1, a2, hlt, hminr, hminr1]; omega
      · have hr' : r + 1 = 2 * w := by omega
        have a1 : (2 * w * q + r + 1) / (2 * w) = q + 1 := by
          have : 2 * w * q + r + 1 = 2 * w * (q + 1) := by rw [Nat.mul_add]; omega
          rw [this, Nat.mul_div_cancel_left _ h2]
        have a2 : (2 * w * q + r + 1) % (2 * w) = 0 := by
          have : 2 * w * q + r + 1 = 2 * w * (q + 1) := by rw [Nat.mul_add]; omega
          rw [this]; exact Nat.mul_mod_right _ _
        have hminr : min w r = r := Nat.min_eq_right (Nat.le_of_lt hlt)
        have hmin0 : min w 0 = 0 := Nat.min_eq_right (Nat.zero_le w)
        simp [a1, a2, hlt, hminr, hmin0]
        omega
    · have hge : w ≤ r := Nat.le_of_not_gt hlt
      have hr1 : r + 1 < 2 * w ∨ r + 1 = 2 * w := by omega
      rcases hr1 with hr1 | hr1
      · have a1 : (2 * w * q + r + 1) / (2 * w) = q := by
          have : 2 * w * q + r + 1 = 2 * w * q + (r + 1) := Nat.add_assoc _ _ _
          rw [this, Nat.mul_add_div h2]; simp [Nat.div_eq_of_lt hr1]
        have a2 : (2 * w * q + r + 1) % (2 * w) = r + 1 := by
          have : 2 * w * q + r + 1 = 2 * w * q + (r + 1) := Nat.add_assoc _ _ _
          rw [this, Nat.mul_add_mod]; exact Nat.mod_eq_of_lt hr1
        have hminr : min w r = w := Nat.min_eq_left hge
        have hminr1 : min w (r + 1) = w := Nat.min_eq_left (by omega)
        simp [a1, a2, hlt, hminr, hminr1]
      · have a1 : (2 * w * q + r + 1) / (2 * w) = q + 1 := by
          have : 2 * w * q + r + 1 = 2 * w * (q + 1) := by rw [Nat.mul_add]; omega
          rw [this, Nat.mul_div_cancel_left _ h2]
        have a2 : (2 * w * q + r + 1) % (2 * w) = 0 := by
          have : 2 * w * q + r + 1 = 2 * w * (q + 1) := by rw [Nat.mul_add]; omega
          rw [this]; exact Nat.mul_mod_right _ _
        have hminr : min w r = w := Nat.min_eq_left hge
        have hmin0 : min w 0 = 0 := Nat.min_eq_right (Nat.zero_le w)
        simp [a1, a2, hlt, hminr, hmin0]
        rw [Nat.add_mul, Nat.one_mul]

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

private def gray (r : Nat) : Nat := r ^^^ (r / 2)

private theorem xor_div_two (a b : Nat) : (a ^^^ b) / 2 = (a / 2) ^^^ (b / 2) := by
  apply Nat.eq_of_testBit_eq
  intro i
  simp [Nat.testBit_div_two, Nat.testBit_xor]

private theorem xor_eq_zero_iff_eq (a b : Nat) : a ^^^ b = 0 ↔ a = b := by
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

private def invGray (n : Nat) : Nat :=
  if n = 0 then 0 else n ^^^ invGray (n / 2)
termination_by n
decreasing_by exact Nat.div_lt_self (by omega) (by decide : 1 < 2)

private theorem invGray_half (n : Nat) : invGray n / 2 = invGray n ^^^ n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · rw [invGray]; simp [hn]
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hih := ih (n / 2) hdiv
        rw [invGray]; simp [hn]; rw [xor_div_two, hih]
        calc
          n / 2 ^^^ (invGray (n / 2) ^^^ n / 2) =
              (invGray (n / 2) ^^^ n / 2) ^^^ n / 2 := by rw [Nat.xor_comm]
          _ = invGray (n / 2) ^^^ (n / 2 ^^^ n / 2) := by rw [Nat.xor_assoc]
          _ = invGray (n / 2) := by rw [Nat.xor_self, Nat.xor_zero]
          _ = (n ^^^ invGray (n / 2)) ^^^ n := by
            rw [Nat.xor_comm n (invGray (n / 2)), Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

private theorem gray_invGray (n : Nat) : gray (invGray n) = n := by
  unfold gray
  rw [invGray_half, ← Nat.xor_assoc, Nat.xor_self, Nat.zero_xor]

private theorem gray_ne_zero_of_pos {n : Nat} (hn : 0 < n) : gray n ≠ 0 := by
  intro h
  unfold gray at h
  have heq : n = n / 2 := (xor_eq_zero_iff_eq n (n / 2)).mp h
  omega

private theorem invGray_gray (n : Nat) : invGray (gray n) = n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · rw [invGray]; simp [hn, gray]
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hih := ih (n / 2) hdiv
        have hpos : gray n ≠ 0 := gray_ne_zero_of_pos (by omega)
        have hhalf : gray n / 2 = gray (n / 2) := by unfold gray; rw [xor_div_two]
        rw [invGray]; simp [hpos]; rw [hhalf, hih]
        unfold gray
        rw [Nat.xor_assoc, Nat.xor_self, Nat.xor_zero]

private theorem invGray_lt_two_pow (d n : Nat) (hn : n < 2 ^ d) : invGray n < 2 ^ d := by
  induction n using Nat.strongRecOn generalizing d with
  | ind n ih =>
      by_cases hn0 : n = 0
      · rw [invGray]; simp [hn0]; exact Nat.two_pow_pos d
      · have hdiv : n / 2 < n := Nat.div_lt_self (by omega) (by decide : 1 < 2)
        have hih := ih (n / 2) hdiv d (by omega)
        rw [invGray]; simp [hn0]; exact Nat.xor_lt_two_pow hn hih

private theorem gray_lt_two_pow (d n : Nat) (hn : n < 2 ^ d) : gray n < 2 ^ d := by
  unfold gray
  exact Nat.xor_lt_two_pow hn (by omega)

private theorem gray_inj (a b : Nat) (h : gray a = gray b) : a = b := by
  have hh := congrArg invGray h
  simpa [invGray_gray] using hh

private def grayIdx (w i : Nat) : Nat := (i / w) * w + gray (i % w)

private theorem grayIdx_div (d i : Nat) : grayIdx (2 ^ d) i / 2 ^ d = i / 2 ^ d := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := gray (i % w)
  have hs : s < w := gray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) / w = i / w
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_div_left s (i / w) hw, Nat.div_eq_of_lt hs, Nat.zero_add]

private theorem grayIdx_mod (d i : Nat) : grayIdx (2 ^ d) i % 2 ^ d = gray (i % 2 ^ d) := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := gray (i % w)
  have hs : s < w := gray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) % w = s
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hs]

private theorem grayIdx_bound (d n i : Nat) (hn : n % 2 ^ d = 0) (hi : i < n) :
    grayIdx (2 ^ d) i < n := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  have hnd : n = (n / w) * w := by
    have hd' := Nat.div_add_mod n w
    rw [hn, Nat.add_zero] at hd'
    rw [Nat.mul_comm] at hd'
    exact hd'.symm
  have hklt : i / w < n / w := by rw [Nat.div_lt_iff_lt_mul hw, ← hnd]; exact hi
  calc
    grayIdx w i = (i / w) * w + (grayIdx w i) % w := by
      have hdec := Nat.div_add_mod (grayIdx w i) w
      rw [show grayIdx w i / w = i / w by simpa [w] using grayIdx_div d i] at hdec
      simpa [Nat.mul_comm] using hdec.symm
    _ < ((i / w) + 1) * w := by
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_lt_add_left (Nat.mod_lt _ hw) ((i / w) * w)
    _ ≤ n := by rw [hnd]; exact Nat.mul_le_mul_right w (by omega)

private theorem grayIdx_inj (d a b : Nat)
    (h : grayIdx (2 ^ d) a = grayIdx (2 ^ d) b) : a = b := by
  let w := 2 ^ d
  have hd' := congrArg (fun x : Nat => x / w) h
  have hm := congrArg (fun x : Nat => x % w) h
  rw [show grayIdx w a / w = a / w by simpa [w] using grayIdx_div d a] at hd'
  rw [show grayIdx w b / w = b / w by simpa [w] using grayIdx_div d b] at hd'
  rw [show grayIdx w a % w = gray (a % w) by simpa [w] using grayIdx_mod d a] at hm
  rw [show grayIdx w b % w = gray (b % w) by simpa [w] using grayIdx_mod d b] at hm
  have hmod : a % w = b % w := gray_inj _ _ hm
  calc
    a = (a / w) * w + a % w := by simpa [Nat.mul_comm] using (Nat.div_add_mod a w).symm
    _ = (b / w) * w + b % w := by rw [hd', hmod]
    _ = b := by simpa [Nat.mul_comm] using Nat.div_add_mod b w

private def idxInv (w i : Nat) : Nat := (i / w) * w + invGray (i % w)

private theorem idxInv_div (d i : Nat) : idxInv (2 ^ d) i / 2 ^ d = i / 2 ^ d := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := invGray (i % w)
  have hs : s < w := invGray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) / w = i / w
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_div_left s (i / w) hw, Nat.div_eq_of_lt hs, Nat.zero_add]

private theorem idxInv_mod (d i : Nat) : idxInv (2 ^ d) i % 2 ^ d = invGray (i % 2 ^ d) := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  let s := invGray (i % w)
  have hs : s < w := invGray_lt_two_pow d (i % w) (Nat.mod_lt i hw)
  change (i / w * w + s) % w = s
  rw [Nat.add_comm (i / w * w) s, Nat.mul_comm (i / w) w,
    Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hs]

private theorem idxInv_bound (d n x : Nat) (hn : n % 2 ^ d = 0) (hx : x < n) :
    idxInv (2 ^ d) x < n := by
  let w := 2 ^ d
  have hw : 0 < w := Nat.two_pow_pos d
  have hnd : n = (n / w) * w := by
    have hd' := Nat.div_add_mod n w
    rw [hn, Nat.add_zero] at hd'
    rw [Nat.mul_comm] at hd'
    exact hd'.symm
  calc
    idxInv w x = (x / w) * w + (idxInv w x) % w := by
      have hdec := Nat.div_add_mod (idxInv w x) w
      rw [show idxInv w x / w = x / w by simpa [w] using idxInv_div d x] at hdec
      simpa [Nat.mul_comm] using hdec.symm
    _ < ((x / w) + 1) * w := by
      rw [Nat.add_mul, Nat.one_mul]
      exact Nat.add_lt_add_left (Nat.mod_lt _ hw) ((x / w) * w)
    _ ≤ n := by
      rw [hnd]
      apply Nat.mul_le_mul_right w
      have hlt : x / w < n / w := by
        rw [Nat.div_lt_iff_lt_mul hw, ← hnd]
        exact hx
      omega

private theorem grayIdx_idxInv (d x : Nat) : grayIdx (2 ^ d) (idxInv (2 ^ d) x) = x := by
  let w := 2 ^ d
  have hiDiv : idxInv w x / w = x / w := by simpa [w] using idxInv_div d x
  have hiMod : idxInv w x % w = invGray (x % w) := by simpa [w] using idxInv_mod d x
  unfold grayIdx
  rw [hiDiv, hiMod, gray_invGray]
  simpa [Nat.mul_comm] using Nat.div_add_mod x w

private theorem grayIdx_perm (d n : Nat) (hn : n % 2 ^ d = 0) :
    List.Perm ((List.range n).map (grayIdx (2 ^ d))) (List.range n) := by
  have hnd : ((List.range n).map (grayIdx (2 ^ d))).Nodup := by
    exact List.Pairwise.map (R := fun a b : Nat => a ≠ b) (S := fun a b : Nat => a ≠ b)
      (grayIdx (2 ^ d)) (fun a b hne hEq => hne (grayIdx_inj d a b hEq)) List.nodup_range
  apply (List.perm_ext_iff_of_nodup hnd List.nodup_range).mpr
  intro x
  constructor
  · intro hx
    obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hx
    exact List.mem_range.mpr (grayIdx_bound d n i hn (List.mem_range.mp hi))
  · intro hx
    refine List.mem_map.mpr ⟨idxInv (2 ^ d) x,
      List.mem_range.mpr (idxInv_bound d n x hn (List.mem_range.mp hx)), ?_⟩
    exact grayIdx_idxInv d x

private theorem rows_grayTile (c : Config) :
    (rows layout c).map Prod.snd =
      (List.range c.n).map (fun i : Nat =>
        c.base + c.step * ((grayIdx (width c) i : Nat) : Int)) := by
  let w := width c
  have hrows : rows layout c =
      (List.range c.n).map (fun i : Nat =>
        (i, c.base + c.step * ((grayIdx w i : Nat) : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * ((grayIdx w i : Nat) : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [layout, grayIdx, gray, w]
  rw [hrows]
  simp [w, List.map_map, Function.comp_def]

private theorem rows_linearOrdinal (c : Config) :
    (rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout c).map Prod.snd =
      (List.range c.n).map (fun (j : Nat) => c.base + c.step * (j : Int)) := by
  have hrows : rows TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout c =
      (List.range c.n).map (fun i : Nat => (i, c.base + c.step * (i : Int))) := by
    unfold rows
    rw [← filterMap_some_eq_map (List.range c.n) (fun i : Nat =>
      (i, c.base + c.step * (i : Int)))]
    apply filterMap_congr'
    intro i hi
    simp [TinylambOpt.ResearchGoals.Atlas.EncodedPages.LinearOrdinal.layout]
  rw [hrows]
  simp [List.map_map, Function.comp_def]

theorem PowerOfTwo_sound : PowerOfTwo.Goal := by
  intro c d hw hn
  rw [rows_grayTile c, rows_linearOrdinal c]
  rw [hw] at hn ⊢
  by_cases hs : c.step = 0
  · simp [hs]
  · have hp := grayIdx_perm d c.n hn
    have hval := List.Perm.map (fun j : Nat => c.base + c.step * (j : Int)) hp
    simpa [grayIdx, Function.comp_def] using hval

end GrayTile

/-! ## Generic lane decomposition for ResidueJoin / DeleteProgression / AlignedProduct -/

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
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  refine List.Perm.of_eq ?_
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

theorem ResidueJoin_sound : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeletedSpan.Remaining.sound_ResidueJoin

theorem DeleteProgression_sound : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.DeletedSpan.Remaining.sound_DeleteProgression

theorem AlignedProduct_sound : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task47.sound_AlignedProduct

end DeletedSpan

namespace RotatedPage

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RotatedPage

private theorem rows_flatMap (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  refine List.Perm.of_eq ?_
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

theorem ResidueJoin_sound : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task48.sound_ResidueJoin

theorem DeleteProgression_sound : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task48.sound_DeleteProgression

theorem AlignedProduct_sound : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task48.sound_AlignedProduct

end RotatedPage

namespace RestartedDelta

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.RestartedDelta

private theorem rows_flatMap (c : Config) :
    (rows layout c).Perm ((layout.lanes c).flatMap lanePoints) := by
  refine List.Perm.of_eq ?_
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

theorem ResidueJoin_sound : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task50.sound_ResidueJoin

theorem DeleteProgression_sound : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_EncodedPages_E7.RD.RD_DeleteProgression

theorem AlignedProduct_sound : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Atlas.EncodedPages.RestartedDelta.Remaining.sound_AlignedProduct

end RestartedDelta

namespace GrayTile

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.GrayTile

theorem ResidueJoin_sound : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task49.sound_ResidueJoin

theorem DeleteProgression_sound : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task49.sound_DeleteProgression

theorem AlignedProduct_sound : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task49.sound_AlignedProduct

end GrayTile

namespace PagedTombstones

open TinylambOpt.ResearchGoals.Atlas.EncodedPages.PagedTombstones

theorem ResidueJoin_sound : ResidueJoin.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task51.sound_ResidueJoin

theorem DeleteProgression_sound : DeleteProgression.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task51.sound_DeleteProgression

theorem AlignedProduct_sound : AlignedProduct.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task51.sound_AlignedProduct

end PagedTombstones

end TinylambOpt.ResearchProofs.Atlas.EncodedPages_WorkD
