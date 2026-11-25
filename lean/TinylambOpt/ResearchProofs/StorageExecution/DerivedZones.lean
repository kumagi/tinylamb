import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

namespace TinylambOpt.ResearchProofs.StorageExecution.DerivedZones

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! ## Generic helpers -/

theorem inside_of_sound {f : Kernel} (hs : soundInterval f) {t : Nat} {p : List (Option Row)}
    {a b : Bounds} (hc : covers p a b) {r : Option Row} (hr : r ∈ p) {x y : Nat}
    (heq : r = some (x, y)) : inside (f.bounds t a b) (f.score t (x, y)) := by
  exact hs t a b x y (hc r hr x y heq).1 (hc r hr x y heq).2

theorem skipRange_of_sound {f : Kernel} (hs : soundInterval f) : skipRange f := by
  intro t p a b lo hi hc ha
  unfold selected scores
  rw [List.filter_eq_nil_iff]
  intro v hv
  simp only [List.mem_filterMap] at hv
  rcases hv with ⟨r, hrm, hr⟩
  rcases r with _ | ⟨x, y⟩
  · simp at hr
  · simp only [Option.map_some, Option.some.injEq] at hr
    subst hr
    have hin := inside_of_sound hs hc hrm rfl
    have hallow : allows (f.bounds t a b) lo hi = true := by
      unfold allows
      simp only [Bool.and_eq_true, decide_eq_true_eq]
      refine ⟨?_, ?_, ?_⟩
      · exact Nat.le_trans (by have := hin.1; omega) hin.2
      · -- lo <= v <= r.hi? no, need lo <= r.hi and r.lo < hi and lo < hi
        -- from allows=false we get negation. We want contradiction if v selected.
        omega
      · omega
    -- easier: show v cannot satisfy filter
    simp only [Bool.and_eq_true, decide_eq_true_eq] at ha ⊢
    intro hvsel
    have hlo : lo ≤ (f.bounds t a b).hi := Nat.le_trans hvsel.1 hin.2
    have hhi : (f.bounds t a b).lo < hi := Nat.lt_of_le_of_lt hin.1 hvsel.2
    have hlt : lo < hi := Nat.lt_of_le_of_lt hvsel.1 hvsel.2
    simp [allows, hlo, hhi, hlt] at ha

theorem takeAll_of_sound {f : Kernel} (hs : soundInterval f) : takeAll f := by
  intro t p a b lo hi hc hlo hhi
  unfold selected
  rw [List.filter_eq_self]
  intro v hv
  simp only [List.mem_filterMap, scores] at hv
  rcases hv with ⟨r, hrm, hr⟩
  rcases r with _ | ⟨x, y⟩
  · simp at hr
  · simp only [Option.map_some, Option.some.injEq] at hr
    subst hr
    have hin := inside_of_sound hs hc hrm rfl
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    exact ⟨Nat.le_trans hlo hin.1, Nat.lt_of_le_of_lt hin.2 hhi⟩

theorem crossPageJoin_of_sound {f : Kernel} (hs : soundInterval f) : crossPageJoin f := by
  intro t u p q a b c d hcp hcq hlt x hx hxq
  simp only [scores, List.mem_filterMap] at hx hxq
  rcases hx with ⟨r, hrm, hr⟩
  rcases hxq with ⟨r2, hrm2, hr2⟩
  rcases r with _ | ⟨x1, y1⟩ <;> simp at hr
  rcases r2 with _ | ⟨x2, y2⟩ <;> simp at hr2
  subst hr; subst hr2
  have h1 := (inside_of_sound hs hcp hrm rfl).2
  have h2 := (inside_of_sound hs hcq hrm2 rfl).1
  omega

theorem semijoinFilter_of_sound {f : Kernel} (hs : soundInterval f) : semijoinFilter f := by
  intro t p a b ks hc
  unfold scores
  simp only
  congr 1
  apply List.filter_congr
  intro v hv
  simp only [List.mem_filterMap] at hv
  rcases hv with ⟨r, hrm, hr⟩
  rcases r with _ | ⟨x, y⟩
  · simp at hr
  · simp only [Option.map_some, Option.some.injEq] at hr
    subst hr
    have hin := inside_of_sound hs hc hrm rfl
    -- (ks.filter (fun k => r.lo <= k && k <= r.hi)).contains v = ks.contains v
    -- when v is inside r
    simp only [List.contains_eq_mem, List.mem_filter, Bool.and_eq_true, decide_eq_true_eq]
    constructor
    · intro h; exact h.1
    · intro h; exact ⟨h, hin.1, hin.2⟩

theorem sumCeiling_of_sound {f : Kernel} (hs : soundInterval f) : sumCeiling f := by
  intro t p a b hc
  dsimp
  set vs := scores f t p
  set r := f.bounds t a b
  have hmem : ∀ v ∈ vs, r.lo ≤ v ∧ v ≤ r.hi := by
    intro v hv
    unfold scores at hv
    simp only [List.mem_filterMap] at hv
    rcases hv with ⟨row, hrm, hr⟩
    rcases row with _ | ⟨x, y⟩
    · simp at hr
    · simp only [Option.map_some, Option.some.injEq] at hr
      subst hr
      exact inside_of_sound hs hc hrm rfl
  constructor
  · have h : ∀ v ∈ vs, r.lo ≤ v := fun v hv => (hmem v hv).1
    clear hmem hc hs
    induction vs with
    | nil => simp
    | cons v vs ih =>
      simp only [List.length_cons, List.sum_cons]
      have hv := h v List.mem_cons_self
      have ih' := ih (fun w hw => h w (List.mem_cons_of_mem v hw))
      calc vs.length * r.lo + r.lo
          ≤ vs.length * r.lo + v := Nat.add_le_add_left hv _
        _ ≤ vs.sum + v := Nat.add_le_add_right ih' _
        _ = v + vs.sum := by omega
        _ = _ := rfl
      -- need: (vs.length + 1) * r.lo <= v + vs.sum
      -- (vs.length+1)*r.lo = vs.length*r.lo + r.lo <= vs.length*r.lo + v <= vs.sum + v
      omega
  · have h : ∀ v ∈ vs, v ≤ r.hi := fun v hv => (hmem v hv).2
    clear hmem hc hs
    induction vs with
    | nil => simp
    | cons v vs ih =>
      simp only [List.length_cons, List.sum_cons]
      have hv := h v List.mem_cons_self
      have ih' := ih (fun w hw => h w (List.mem_cons_of_mem v hw))
      omega

theorem thresholdCount_of_sound {f : Kernel} (hs : soundInterval f) : thresholdCount f := by
  intro t ps cut hcov
  dsimp
  set n := fun (e : List (Option Row) × Bounds × Bounds) => (e.1.filterMap id).length
  set actual := ((ps.map Prod.fst).flatten.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)
  constructor
  · -- lower bound: pages with cut <= r.lo contribute all their rows
    -- actual >= sum of lengths of those pages
    have hpage : ∀ e ∈ ps, cut ≤ (f.bounds t e.2.1 e.2.2).lo →
        n e ≤ ((e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)) := by
      intro e he hle
      have hcov_e : covers e.1 e.2.1 e.2.2 := hcov e he
      have hall : ∀ v ∈ e.1.filterMap (fun r => r.map (f.score t)), cut ≤ v := by
        intro v hv
        simp only [List.mem_filterMap] at hv
        rcases hv with ⟨r, hrm, hr⟩
        rcases r with _ | ⟨x, y⟩
        · simp at hr
        · simp only [Option.map_some, Option.some.injEq] at hr
          subst hr
          have hin := inside_of_sound hs hcov_e hrm rfl
          exact Nat.le_trans hle hin.1
      -- countP of all-true = length
      have hcount : ((e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)) =
          (e.1.filterMap (fun r => r.map (f.score t))).length := by
        rw [List.countP_eq_length_filter]
        rw [List.filter_eq_self]
        intro v hv
        simp only [decide_eq_true_eq]
        exact hall v hv
      rw [hcount]
      -- n e = (e.1.filterMap id).length <= (e.1.filterMap (fun r => r.map (f.score t))).length
      simp [n, List.length_filterMap]
      -- both are lengths of filterMap over same list, second maps through score
      have : (e.1.filterMap id).length ≤ (e.1.filterMap (fun r => r.map (f.score t))).length := by
        induction e.1 with
        | nil => simp
        | cons r rs ih =>
          cases r with
          | none => simp [ih]
          | some row =>
            simp only [List.filterMap_cons, Option.map_some, Option.some_bind, id_eq, List.length_cons]
            exact Nat.succ_le_succ ih
      exact this
    -- sum over filtered pages <= actual
    -- actual = countP over flatten of all pages
    have hactual_eq : actual = ((ps.map (fun e => e.1.filterMap (fun r => r.map (f.score t)))).flatten).countP (· ≥ cut) := by
      simp [actual, List.filterMap_flatten, List.filterMap_map, Function.comp]
    rw [hactual_eq]
    -- sum of per-page counts for selected pages <= total count
    have hsum : ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)).map n).sum ≤
        ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)).map (fun e => (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut))).sum := by
      apply List.sum_le_sum
      intro x hx
      simp only [List.mem_map, List.mem_filter] at hx
      rcases hx with ⟨e, ⟨he, hle⟩, rfl⟩
      simp only [decide_eq_true_eq] at hle
      exact hpage e he hle
    refine Nat.le_trans hsum ?_
    -- sum of countP of filtered subset of pages <= countP of flatten all
    have hsub : ((ps.filter (fun e => cut ≤ (f.bounds t e.2.1 e.2.2).lo)).map (fun e => (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut))).sum ≤
        (ps.map (fun e => (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut))).sum := by
      clear hsum hpage hactual_eq actual n
      induction ps with
      | nil => simp
      | cons e es ih =>
        simp only [List.filter_cons, List.map_cons, List.sum_cons]
        split
        · exact Nat.add_le_add_left ih _
        · exact Nat.le_trans ih (Nat.le_add_left _ _)
    refine Nat.le_trans hsub ?_
    -- sum of countP per page = countP of flatten
    have hflat : (ps.map (fun e => (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut))).sum =
        ((ps.map (fun e => e.1.filterMap (fun r => r.map (f.score t)))).flatten).countP (· ≥ cut) := by
      clear hsub hsum hpage hactual_eq actual n
      induction ps with
      | nil => simp
      | cons e es ih =>
        simp [List.countP_append, ih]
    exact le_of_eq hflat
  · -- upper bound: only pages with cut <= r.hi can contribute
    have hactual_eq : actual = ((ps.map (fun e => e.1.filterMap (fun r => r.map (f.score t)))).flatten).countP (· ≥ cut) := by
      simp [actual, List.filterMap_flatten, List.filterMap_map, Function.comp]
    rw [hactual_eq]
    -- each contributing row comes from a page with cut <= r.hi
    have hpage : ∀ e ∈ ps, ((e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)) ≤
        if cut ≤ (f.bounds t e.2.1 e.2.2).hi then n e else 0 := by
      intro e he
      split
      · rename_i hhi
        -- countP <= length <= n e
        have hle : ((e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)) ≤
            (e.1.filterMap (fun r => r.map (f.score t))).length := List.countP_le_length _
        have hlen : (e.1.filterMap (fun r => r.map (f.score t))).length ≤ n e := by
          simp [n]
          induction e.1 with
          | nil => simp
          | cons r rs ih =>
            cases r with
            | none => simp [ih]
            | some row =>
              simp only [List.filterMap_cons, Option.map_some, Option.some_bind, id_eq, List.length_cons]
              exact Nat.succ_le_succ ih
        exact Nat.le_trans hle hlen
      · rename_i hhi
        -- no row can satisfy cut <= score
        rw [Nat.not_le] at hhi
        have hall : ((e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut)) = 0 := by
          rw [List.countP_eq_zero]
          intro v hv
          simp only [decide_eq_true_eq]
          intro hle
          simp only [List.mem_filterMap] at hv
          rcases hv with ⟨r, hrm, hr⟩
          rcases r with _ | ⟨x, y⟩
          · simp at hr
          · simp only [Option.map_some, Option.some.injEq] at hr
            subst hr
            have hin := inside_of_sound hs (hcov e he) hrm rfl
            omega
        simp [hall]
    -- total <= sum of per-page bounds
    have hflat : ((ps.map (fun e => e.1.filterMap (fun r => r.map (f.score t)))).flatten).countP (· ≥ cut) =
        (ps.map (fun e => (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut))).sum := by
      induction ps with
      | nil => simp
      | cons e es ih =>
        simp [List.countP_append, ih]
    rw [hflat]
    have hsum : (ps.map (fun e => (e.1.filterMap (fun r => r.map (f.score t))).countP (· ≥ cut))).sum ≤
        (ps.map (fun e => if cut ≤ (f.bounds t e.2.1 e.2.2).hi then n e else 0)).sum := by
      apply List.sum_le_sum
      intro x hx
      simp only [List.mem_map] at hx
      rcases hx with ⟨e, he, rfl⟩
      exact hpage e he
    refine Nat.le_trans hsum ?_
    -- sum of if-then-n-else-0 = sum of n over filtered
    clear hsum hflat hpage hactual_eq actual
    induction ps with
    | nil => simp
    | cons e es ih =>
      simp only [List.filter_cons, List.map_cons, List.sum_cons]
      split
      · rename_i hhi
        simp only [decide_eq_true_eq] at hhi
        simp [hhi, ih]
      · rename_i hhi
        simp only [decide_eq_true_eq] at hhi
        have : ¬ (cut ≤ (f.bounds t e.2.1 e.2.2).hi) := hhi
        simp [this, ih]
