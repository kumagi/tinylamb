import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `StaggeredHandoff.RankSeek` — `∀ b p (rs : Rows) (lo hi : Int) (k : Nat), (∀ r ∈ rs,
  bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) → let scores := ((rs.filter (fun r =>
  shape.qualifies p r.1)).map Prod.snd).mergeSort k < scores.length → scores[k]? =
  some (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_StaggeredHandoff_RankSeek

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.StaggeredHandoff

/-! Shared finite-list and two-dimensional prefix-sum algebra. -/

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem point_beq_true_iff (a b : Point) :
    (a == b) = true ↔ a.x = b.x ∧ a.y = b.y := by
  change ((a.x == b.x) && (a.y == b.y)) = true ↔ _
  simp only [Bool.and_eq_true, beq_iff_eq]

private def stencilWeight (s : Shape) (b : Int) (p v : Point) : Int :=
  ((s.stencil b p).map (fun q => if boxIn q v then q.coeff else 0)).sum

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) :
    ((rs.filter f).map Prod.snd).sum =
      (rs.map (fun r => if f r then r.2 else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    by_cases h : f r = true <;> simp [h, ih]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if r.1.x < x && r.1.y < y then r.2 else 0) + prefixMass rs x y := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h]

private theorem prefixMass_append (rs ts : Rows) (x y : Int) :
    prefixMass (rs ++ ts) x y = prefixMass rs x y + prefixMass ts x y := by
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
    rw [List.cons_append, prefixMass_cons, prefixMass_cons, ih]
    omega

private theorem prefixMass_box (rs : Rows) (x0 x1 y0 y1 : Int) (hx : x0 < x1) (hy : y0 < y1) :
    prefixMass rs x1 y1 - prefixMass rs x0 y1 - prefixMass rs x1 y0 + prefixMass rs x0 y0 =
      ((rs.filter (fun r => boxIn (box x0 x1 y0 y1) r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  simp only [boxIn, box, decide_eq_true_eq]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
    rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
    simp only [List.map_cons, List.sum_cons]
    by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
      by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
      by_cases hxloP : x0 ≤ r.1.x <;> by_cases hyloP : y0 ≤ r.1.y <;>
      simp [hx1, hx0, hy1, hy0, hxloP, hyloP, decide_eq_true_eq] <;> first | exact ih | omega

private theorem filter_nil_of_forall_not {α : Type} (l : List α) (f : α → Bool)
    (h : ∀ a ∈ l, ¬ f a = true) : l.filter f = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    have ha : ¬ f a = true := h a List.mem_cons_self
    have has : ∀ b ∈ as, ¬ f b = true := fun b hb => h b (List.mem_cons_of_mem a hb)
    simp [ha, ih has]

private theorem corners_box_sum (q : Box) (rs : Rows) :
    corners q (prefixMass rs) =
      q.coeff * ((rs.filter (fun r => boxIn q r.1)).map Prod.snd).sum := by
  unfold corners
  by_cases h : (q.x0 < q.x1 && q.y0 < q.y1) = true
  · rw [if_pos h]
    exact congrArg (fun z : Int => q.coeff * z)
      (prefixMass_box rs q.x0 q.x1 q.y0 q.y1
        (by
          have hh : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
            simpa [Bool.and_eq_true, decide_eq_true_eq] using h
          exact hh.1)
        (by
          have hh : q.x0 < q.x1 ∧ q.y0 < q.y1 := by
            simpa [Bool.and_eq_true, decide_eq_true_eq] using h
          exact hh.2))
  · rw [if_neg h]
    have hnot : ¬ (q.x0 < q.x1 ∧ q.y0 < q.y1) := by
      intro hh
      apply h
      simpa using hh
    have hn : (rs.filter (fun r => boxIn q r.1)) = [] := by
      apply filter_nil_of_forall_not
      intro r _
      simp [boxIn, decide_eq_true_eq]
      omega
    simp [hn]

private theorem sum_map_mul_const {α : Type} (xs : List α) (f : α → Int) (w : Int) :
    (xs.map (fun x => w * f x)).sum = w * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.mul_add]

private theorem sum_map_snd_const (rs : Rows) (c : Int) :
    (rs.map (fun r => c * r.2)).sum = c * (rs.map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [ih, Int.mul_add]

private theorem sum_map_mul_const' {α : Type} (xs : List α) (f : α → Int) (w : Int) :
    (xs.map (fun x => f x * w)).sum = (xs.map f).sum * w := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih, Int.add_mul]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_map_zero {β : Type} (bs : List β) :
    (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem mul_sum_if (rs : Rows) (c : Int) (f : Point × Int → Bool) :
    c * (rs.map (fun r => if f r then r.2 else 0)).sum =
      (rs.map (fun r => if f r then c * r.2 else 0)).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
    by_cases h : f r = true <;> simp [h, ih, Int.mul_add]

private theorem sum_congr {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x = g x) : (xs.map f).sum = (xs.map g).sum :=
  congrArg List.sum (List.map_congr_left h)

private theorem sum_map_le {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x ≤ g x) : (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp only [List.map_cons, List.sum_cons]
    have hx := h x (by simp)
    have ht := ih (fun y hy => h y (by simp [hy]))
    omega

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x - g x)).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_swap {α β : Type} (as : List α) (bs : List β) (f : α → β → Int) :
    (as.map (fun a => (bs.map (fun b => f a b)).sum)).sum =
      (bs.map (fun b => (as.map (fun a => f a b)).sum)).sum := by
  induction as with
  | nil => simp [sum_map_zero]
  | cons a as ih =>
    simp only [List.map_cons, List.sum_cons]
    rw [ih, ← sum_map_add]

private theorem indexed_eq_sum_weight (s : Shape) (b : Int) (p : Point) (rs : Rows) :
    indexed s b p (prefixMass rs) =
      (rs.map (fun r => stencilWeight s b p r.1 * r.2)).sum := by
  unfold indexed
  have h1 : ((s.stencil b p).map (corners · (prefixMass rs))).sum =
      ((s.stencil b p).map
        (fun q => (rs.map (fun r => if boxIn q r.1 then q.coeff * r.2 else 0)).sum)).sum := by
    induction s.stencil b p with
    | nil => rfl
    | cons q qs ih =>
      rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
      rw [corners_box_sum, sum_filter_eq_sum_if, mul_sum_if, ih]
  rw [h1, sum_swap]
  apply sum_congr
  intro r _
  unfold stencilWeight
  induction s.stencil b p with
  | nil => simp
  | cons q qs ih =>
    by_cases h : boxIn q r.1 = true <;> simp [h, ih, Int.add_mul]

private theorem prefixIndex_of_weight (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (h : ∀ r ∈ rs, stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) :
    indexed s b p (prefixMass rs) = scan s p rs := by
  rw [indexed_eq_sum_weight]
  have hmap : rs.map (fun r => stencilWeight s b p r.1 * r.2) =
      rs.map (fun r => if s.qualifies p r.1 then r.2 else 0) := by
    apply List.map_congr_left
    intro r hr
    rw [h r hr]
    by_cases hc : s.qualifies p r.1 <;> simp [hc]
  rw [hmap]
  exact (sum_filter_eq_sum_if rs (fun r => s.qualifies p r.1)).symm

private theorem sum_if_one {α : Type} (xs : List α) (f : α → Bool) :
    (xs.map (fun x => if f x then (1 : Int) else 0)).sum =
      (xs.filter f).length := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    by_cases h : f x = true <;> simp [h, ih] <;> omega

private theorem sum_map_one {α : Type} (xs : List α) :
    (xs.map (fun _ => (1 : Int))).sum = xs.length := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih] <;> omega

/-! Rank seeking: a logarithmic-fuel search for the first integer whose rank
exceeds `k`.  The rank threshold lemma is proved for sorted lists, not by
computation. -/

private theorem lt_two_pow_log2_succ (n : Nat) : n < 2 ^ (n.log2 + 1) := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
    rw [Nat.log2_def]
    by_cases h2 : 2 ≤ n
    · rw [if_pos h2, Nat.pow_succ]
      have hhalf := ih (n / 2) (Nat.div_lt_self (Nat.le_of_lt h2) (by decide))
      have hmod := Nat.mod_lt n (by decide : 0 < 2)
      have hadd := Nat.mod_add_div n 2
      omega
    · have hn : n < 2 := by omega
      have hcases : n = 0 ∨ n = 1 := by omega
      rcases hcases with rfl | rfl <;> decide

private theorem seek_eq_target (rank : Int → Int) (k : Nat) (target : Int)
    (hrank : ∀ x, rank x ≤ (k : Int) ↔ x < target) :
    ∀ fuel lo hi, lo ≤ target → target ≤ hi → (hi - lo).toNat < 2 ^ fuel →
      seek rank k fuel lo hi = target := by
  intro fuel
  induction fuel with
  | zero =>
    intro lo hi hlo hhi hd
    have hzero : (hi - lo).toNat = 0 := by omega
    have heq : hi = lo := by omega
    simp only [seek]
    omega
  | succ fuel ih =>
    intro lo hi hlo hhi hd
    by_cases hstop : hi ≤ lo
    · simp [seek, hstop]
      omega
    · simp only [seek, hstop, if_false]
      let mid := (lo + hi) / 2
      have hd' : hi - lo < (2 : Int) * (2 ^ fuel : Nat) := by
        have hd2 : (hi - lo).toNat < 2 ^ fuel * 2 := by
          simpa [Nat.pow_succ, Nat.mul_comm] using hd
        have hd3 : ((hi - lo).toNat : Int) < ((2 ^ fuel * 2 : Nat) : Int) := by
          exact_mod_cast hd2
        have hnon : 0 ≤ hi - lo := by omega
        rw [Int.toNat_of_nonneg hnon] at hd3
        have htmp : hi - lo < (2 ^ fuel : Nat) * 2 := by
          simpa [Int.natCast_mul] using hd3
        omega
      have hm := Int.mul_ediv_add_emod (lo + hi) 2
      have hr0 := Int.emod_nonneg (lo + hi) (by decide : (2 : Int) ≠ 0)
      have hr2 := Int.emod_lt_of_pos (lo + hi) (by decide : (0 : Int) < 2)
      by_cases hk : (k : Int) < rank mid
      · rw [if_pos hk]
        apply ih lo mid hlo ?_ ?_
        · by_cases hlt : mid < target
          · have hle : rank mid ≤ (k : Int) := (hrank mid).mpr hlt
            omega
          · omega
        · change (mid - lo).toNat < 2 ^ fuel
          dsimp [mid] at *
          omega
      · rw [if_neg hk]
        have hle : rank mid ≤ (k : Int) := by omega
        have hmt : mid < target := (hrank mid).mp hle
        apply ih (mid + 1) hi ?_ hhi ?_
        · omega
        · change (hi - (mid + 1)).toNat < 2 ^ fuel
          dsimp [mid] at *
          omega

private theorem count_le_index_iff (xs : List Int) (hs : xs.Pairwise (· ≤ ·))
    (k : Nat) (hk : k < xs.length) (x : Int) :
    xs.countP (fun a => a ≤ x) ≤ k ↔ x < xs[k] := by
  induction xs generalizing k with
  | nil => simp at hk
  | cons a as ih =>
    rcases List.pairwise_cons.mp hs with ⟨ha, ht⟩
    cases k with
    | zero =>
      by_cases hax : a ≤ x
      · have hcount : (a :: as).countP (fun b => b ≤ x) =
          1 + as.countP (fun b => b ≤ x) := by
          simp [List.countP_cons, hax, Nat.add_comm]
        rw [hcount]
        change (1 + as.countP (fun b => b ≤ x) ≤ 0) ↔ x < a
        omega
      · have hnone : as.countP (fun b => b ≤ x) = 0 := by
          apply List.countP_eq_zero.mpr
          intro b hb
          have hab := ha b hb
          simp only [decide_eq_true_eq]
          omega
        have hcount : (a :: as).countP (fun b => b ≤ x) = 0 := by
          simp [List.countP_cons, hax, hnone]
        rw [hcount]
        change (0 ≤ 0) ↔ x < a
        omega
    | succ k =>
      have hk' : k < as.length := by simpa using hk
      by_cases hax : a ≤ x
      · have hcount : (a :: as).countP (fun b => b ≤ x) =
            1 + as.countP (fun b => b ≤ x) := by
          simp [List.countP_cons, hax, Nat.add_comm]
        rw [hcount]
        change (1 + as.countP (fun b => b ≤ x) ≤ k + 1) ↔ x < as[k]
        constructor
        · intro h
          exact (ih ht k hk').mp (by omega)
        · intro h
          have h' := (ih ht k hk').mpr h
          omega
      · have hnone : as.countP (fun b => b ≤ x) = 0 := by
          apply List.countP_eq_zero.mpr
          intro b hb
          have hab := ha b hb
          simp only [decide_eq_true_eq]
          omega
        have hgt : ¬ as[k] ≤ x := by
          have hz := List.countP_eq_zero.mp hnone as[k] (List.getElem_mem hk')
          simpa only [decide_eq_true_eq] using hz
        have hcount : (a :: as).countP (fun b => b ≤ x) = 0 := by
          simp [List.countP_cons, hax, hnone]
        rw [hcount]
        rw [List.getElem_cons_succ]
        change (0 ≤ k + 1) ↔ x < as[k]
        omega

private theorem beforeScore_eq_count (s : Shape) (b : Int) (p : Point) (rs : Rows) (x : Int)
    (P : Point → Prop)
    (hst : ∀ v, P v → bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hP : ∀ r ∈ rs, P r.1) (hb : ∀ r ∈ rs, bounded b r.1) :
    beforeScore s b p rs x =
      (((rs.filter (fun r => s.qualifies p r.1)).filter (fun r => r.2 ≤ x)).length : Int) := by
  unfold beforeScore
  let rs' := (rs.filter (fun r => r.2 ≤ x)).map (fun r => (r.1, (1 : Int)))
  have hi := prefixIndex_of_weight s b p rs' (fun r hr => by
    rcases List.mem_map.mp hr with ⟨r0, hr0, heq0⟩
    subst r
    exact hst r0.1 (hP r0 (List.mem_filter.mp hr0).1)
      (hb r0 (List.mem_filter.mp hr0).1))
  rw [hi]
  have hscan : scan s p rs' =
      (((rs.filter (fun r => s.qualifies p r.1)).filter (fun r => r.2 ≤ x)).length : Int) := by
    unfold scan rs'
    simp [List.filter_map, List.map_map, List.filter_filter, Function.comp_def, sum_map_one, Bool.and_comm]
  exact hscan

private theorem rankSeek_of_stencil (s : Shape) (b : Int) (p : Point) (rs : Rows)
    (lo hi : Int) (k : Nat) (P : Point → Prop)
    (hst : ∀ v, P v → bounded b v →
      stencilWeight s b p v = if s.qualifies p v then (1 : Int) else 0)
    (hP : ∀ r ∈ rs, P r.1)
    (hb : ∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi)
    (hk : k < ((rs.filter (fun r => s.qualifies p r.1)).map Prod.snd).mergeSort.length) :
    ((rs.filter (fun r => s.qualifies p r.1)).map Prod.snd).mergeSort[k]? = some
      (seek (beforeScore s b p rs) k ((hi - lo).toNat.log2 + 2) lo hi) := by
  let matched := rs.filter (fun r => s.qualifies p r.1)
  let scores := matched.map Prod.snd
  let sorted := scores.mergeSort
  have hsorted : sorted.Pairwise (· ≤ ·) := by
    have h := List.pairwise_mergeSort (le := fun a b : Int => decide (a ≤ b))
      (fun a b c hab hbc => decide_eq_true (Int.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc)))
      (fun a b => by simp [Bool.or_eq_true, decide_eq_true_eq]; omega) scores
    simpa [sorted] using h
  have hk' : k < sorted.length := by simpa [sorted, scores, matched] using hk
  have hmem : ∀ z ∈ sorted, lo ≤ z ∧ z ≤ hi := by
    intro z hz
    rw [List.mem_mergeSort] at hz
    rcases List.mem_map.mp hz with ⟨r, hr, rfl⟩
    exact ⟨(hb r (List.mem_filter.mp hr).1).2.1,
      (hb r (List.mem_filter.mp hr).1).2.2⟩
  have htarget := hmem sorted[k] (List.getElem_mem hk')
  have hthresh : ∀ x, beforeScore s b p rs x ≤ (k : Int) ↔ x < sorted[k] := by
    intro x
    rw [beforeScore_eq_count s b p rs x P hst hP (fun r hr => (hb r hr).1)]
    have hc : sorted.countP (fun z => z ≤ x) =
        matched.countP (fun r => r.2 ≤ x) := by
      simpa [sorted, scores, List.countP_map, Function.comp_def] using
        (List.mergeSort_perm scores (fun a b => decide (a ≤ b))).countP_eq
          (fun z => decide (z ≤ x))
    rw [← List.countP_eq_length_filter, ← hc]
    exact_mod_cast count_le_index_iff sorted hsorted k hk' x
  have hfuel : (hi - lo).toNat < 2 ^ ((hi - lo).toNat.log2 + 2) := by
    have hbase := lt_two_pow_log2_succ (hi - lo).toNat
    rw [show (hi - lo).toNat.log2 + 2 = ((hi - lo).toNat.log2 + 1) + 1 by omega,
      Nat.pow_succ]
    omega
  rw [List.getElem?_eq_getElem hk']
  apply congrArg some
  exact (seek_eq_target (beforeScore s b p rs) k sorted[k] hthresh
    ((hi - lo).toNat.log2 + 2) lo hi htarget.1 htarget.2 hfuel).symm

private theorem stencil_id (b : Int) (p v : Point) (hv : bounded b v) :
    stencilWeight shape b p v = if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, boxIn, stencilWeight, bounded] at hv ⊢
  omega

theorem sound_StaggeredHandoff_RankSeek : StaggeredHandoff.RankSeek.Goal := by
  intro b p rs lo hi k hb
  dsimp only
  intro hk
  exact rankSeek_of_stencil shape b p rs lo hi k (P := fun _ => True)
    (fun v _ hv => stencil_id b p v hv) (fun _ _ => True.intro) hb hk

end TinylambOpt.ResearchProofs.Atlas.Goal_StaggeredHandoff_RankSeek
