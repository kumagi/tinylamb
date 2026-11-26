import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Hash routing sends every occurrence of the same value to the same bucket, so the distinct
sets of different buckets are disjoint and their sizes sum to the global distinct count:
once a bucket's partial is finalized it can be reclaimed independently. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_DistinctPartitionFinalization
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

private theorem beq_false {α : Type} [BEq α] [LawfulBEq α] {a b : α}
    (h : ¬ ((a == b) = true)) : (a == b) = false := by
  cases hab : a == b with
  | true => exact absurd hab (by simpa using h)
  | false => exact rfl

private theorem filter_comm : ∀ (l : List Int) (p q : Int → Bool),
    (l.filter p).filter q = (l.filter q).filter p := by
  intro l p q
  rw [List.filter_filter, List.filter_filter]
  congr 1
  funext a
  exact Bool.and_comm _ _

/-- Filtering and deduplication commute. -/
private theorem filter_eraseDups_comm : ∀ (l : List Int) (p : Int → Bool),
    (l.filter p).eraseDups = l.eraseDups.filter p := by
  have key : ∀ n : Nat, ∀ (l : List Int), l.length ≤ n →
      ∀ p : Int → Bool, (l.filter p).eraseDups = l.eraseDups.filter p := by
    intro n
    induction n with
    | zero =>
        intro l hl p
        obtain rfl : l = [] := List.eq_nil_of_length_eq_zero (by omega)
        rfl
    | succ n ih =>
        intro l hl p
        cases l with
        | nil => rfl
        | cons x xs =>
            have hlen : xs.length ≤ n := by
              rw [List.length_cons] at hl
              omega
            have hlg : (xs.filter (fun b => !b == x)).length ≤ n := by
              calc List.length (List.filter (fun b => !b == x) xs)
                ≤ xs.length := List.length_filter_le _ _
                _ ≤ n := hlen
            by_cases hx : p x = true
            · rw [List.filter_cons, if_pos hx, List.eraseDups_cons, List.eraseDups_cons,
                List.filter_cons, if_pos hx]
              rw [filter_comm _ _ _, ih (xs.filter (fun b => !b == x)) hlg p]
            · have hident : (xs.eraseDups.filter p).filter (fun b => !b == x) =
                  xs.eraseDups.filter p := by
                refine List.filter_eq_self.mpr (fun b hb => ?_)
                have hp : p b := (List.mem_filter.mp hb).2
                have hne : (b == x) = false := by
                  refine beq_false (by
                    intro hh
                    have hbEq : b = x := LawfulBEq.eq_of_beq hh
                    rw [hbEq] at hp
                    exact absurd hp hx)
                simp [hne]
              have hgp : (xs.filter (fun b => !b == x)).filter p = xs.filter p := by
                rw [filter_comm]
                refine List.filter_eq_self.mpr (fun b hb => ?_)
                have hp : p b := (List.mem_filter.mp hb).2
                have hne : (b == x) = false := by
                  refine beq_false (by
                    intro hh
                    have hbEq : b = x := LawfulBEq.eq_of_beq hh
                    rw [hbEq] at hp
                    exact absurd hp hx)
                simp [hne]
              rw [List.filter_cons, if_neg hx, List.eraseDups_cons, List.filter_cons, if_neg hx]
              rw [ih xs hlen (fun b => !b == x), filter_comm _ _ _, hident, ih xs hlen p]
  intro l p
  exact key l.length l (Nat.le_refl _) p

private theorem bucket_eq (vs : List (Option Int)) (p : Int → Bool) :
    distinctCount (vs.filter (fun v => v.any p)) = (vs.filterMap id).eraseDups.countP p := by
  have h1 : (vs.filter (fun v => v.any p)).filterMap id = (vs.filterMap id).filter p := by
    induction vs with
    | nil => rfl
    | cons v rest ih =>
        cases v with
        | none => simp [List.filter_cons, List.filterMap_cons, ih]
        | some w =>
            by_cases hw : p w = true
            · simp [List.filter_cons, List.filterMap_cons, ih, hw]
            · simp [List.filter_cons, List.filterMap_cons, ih, hw]
  unfold distinctCount
  rw [h1, filter_eraseDups_comm, ← List.countP_eq_length_filter]

private theorem sum_map_add : ∀ (l : List Nat) (f g : Nat → Nat),
    ((l.map fun b => f b + g b)).sum = (l.map f).sum + (l.map g).sum := by
  intro l
  induction l with
  | nil => intro f g; rfl
  | cons a l ih =>
      intro f g
      rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons, List.map_cons,
        List.sum_cons, ih]
      omega

private theorem ite_range_sum : ∀ (n v : Nat),
    ((List.range n).map (fun b => if v == b then 1 else 0)).sum = if v < n then 1 else 0 := by
  intro n
  induction n with
  | zero => intro v; simp
  | succ n ih =>
      intro v
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      rcases Nat.lt_or_ge v n with h | h
      · have hne : (v == n) = false := by
          refine beq_false (by
            intro hh
            have hEq := LawfulBEq.eq_of_beq hh
            omega)
        have h1 : v < n + 1 := Nat.lt_succ_of_lt h
        have hne' : ¬ v = n := by omega
        simp [h, h1, hne, hne']
      · rcases Nat.eq_or_lt_of_le h with rfl | hgt
        · simp [beq_self_eq_true]
        · have hne : (v == n) = false := by
            refine beq_false (by
              intro hh
              have hEq := LawfulBEq.eq_of_beq hh
              omega)
          have hv1 : ¬ (v < n + 1) := by omega
          have hv0 : ¬ (v < n) := by omega
          have hne' : ¬ v = n := by omega
          simp [hne, hv0, hv1, hne']

private theorem sum_zero : ∀ n : Nat, ((List.range n).map (fun b => 0)).sum = 0 := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp

private theorem sum_countP : ∀ (D : List Int) (f : Int → Nat) (n : Nat),
    (∀ x ∈ D, f x < n) →
    ((List.range n).map (fun b => D.countP (fun x => f x == b))).sum = D.length := by
  intro D
  induction D with
  | nil => intro f n _; exact sum_zero n
  | cons x xs ih =>
      intro f n h
      have hmap : (List.range n).map (fun b => (x :: xs).countP (fun y => f y == b)) =
          (List.range n).map (fun b =>
            xs.countP (fun y => f y == b) + if f x == b then 1 else 0) := by
        refine List.map_congr_left (fun b _ => ?_)
        exact List.countP_cons
      have hxlt : f x < n := h x (by simp)
      have hite : ((List.range n).map (fun b => if f x == b then 1 else 0)).sum = 1 := by
        rw [ite_range_sum, if_pos hxlt]
      rw [hmap,
        sum_map_add (List.range n) (fun b => xs.countP (fun y => f y == b))
          (fun b => if f x == b then 1 else 0),
        hite, ih f n (fun y hy => h y (List.mem_cons_of_mem x hy)), List.length_cons]

theorem sound : DistinctPartitionFinalization.Goal := by
  intro vs bucketOf n h
  have hsumD : ∀ x ∈ (vs.filterMap id).eraseDups, bucketOf x < n := by
    intro x hx
    rw [List.mem_eraseDups, List.mem_filterMap] at hx
    obtain ⟨a, hav, rfl⟩ := hx
    exact h x hav
  have h1 : (List.range n).map (fun b =>
      distinctCount (vs.filter (fun v => v.any (fun x => bucketOf x == b)))) =
      (List.range n).map (fun b =>
        ((vs.filterMap id).eraseDups).countP (fun x => bucketOf x == b)) := by
    refine List.map_congr_left (fun b _ => ?_)
    exact bucket_eq vs (fun x => bucketOf x == b)
  have h2 := sum_countP (vs.filterMap id).eraseDups bucketOf n hsumD
  rw [h1, h2]
  rfl

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_AggregateReplay_DistinctPartitionFinalization
