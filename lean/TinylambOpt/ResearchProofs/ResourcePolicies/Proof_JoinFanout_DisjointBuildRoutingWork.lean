import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

/-!
Proof contribution for ResourcePolicies_JoinFanout_DisjointBuildRoutingWork. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout.Goal_DisjointBuildRoutingWork

open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem sum_map_filter_ne (f : Nat → Nat) (a : Nat) (as : List Nat) :
    (as.map f).sum = as.count a * f a + ((as.filter (fun b => !(b == a))).map f).sum := by
  induction as with
  | nil => simp
  | cons b t ih =>
      by_cases hba : (b == a) = true
      · have hbeq : b = a := by simpa using hba
        subst b
        simp only [List.map_cons, List.sum_cons, List.count_cons_self]
        have hfilt : (a :: t).filter (fun b => !(b == a)) = t.filter (fun b => !(b == a)) := by simp
        rw [hfilt, ih, Nat.add_mul]
        omega
      · have hbne : b ≠ a := by simpa using hba
        simp only [List.map_cons, List.sum_cons, List.count_cons_of_ne hbne]
        have hfilt : (b :: t).filter (fun b => !(b == a)) = b :: t.filter (fun b => !(b == a)) := by simp [hba]
        rw [hfilt]
        simp only [List.map_cons, List.sum_cons]
        rw [ih]
        omega

private theorem count_cons_filter (a x : Nat) (as : List Nat)
    (hx : x ∈ as.filter (fun b => !(b == a))) :
    (a :: as).count x = (as.filter (fun b => !(b == a))).count x := by
  simp only [List.mem_filter] at hx
  have hxa : (x == a) = false := by simpa using hx.2
  have hax : a ≠ x := by intro h; subst x; simp at hxa
  rw [List.count_cons_of_ne hax, List.count_filter]
  simp [hxa]

private theorem map_count_eq (a : Nat) (as : List Nat) : ∀ (l : List Nat),
    (∀ x ∈ l, x ∈ as.filter (fun b => !(b == a))) →
    l.map (fun x => (x, (a :: as).count x)) =
      l.map (fun x => (x, (as.filter (fun b => !(b == a))).count x))
  | [], _ => by simp
  | x :: t, h => by
      have hx := h x (by simp)
      simp only [List.map_cons]
      rw [count_cons_filter a x as hx]
      rw [map_count_eq a as t (fun y hy => h y (by simp [hy]))]

private theorem sum_group (f : Nat → Nat) : ∀ (xs : List Nat),
    ((frequency xs).map (fun (k, n) => n * f k)).sum = (xs.map f).sum
  | [] => by simp [frequency]
  | a :: as => by
      let r := as.filter (fun b => !(b == a))
      have ih_r := sum_group f r
      have hcons : (a :: as).count a = as.count a + 1 := by simp
      have hfreq : frequency (a :: as) = (a, (a :: as).count a) :: frequency r := by
        dsimp [r]
        unfold frequency
        rw [List.eraseDups_cons]
        simp only [List.map_cons]
        rw [map_count_eq a as (List.filter (fun b => !(b == a)) as).eraseDups
          (fun x hx => (List.mem_eraseDups).1 hx)]
      rw [hfreq]
      simp only [List.map_cons, List.sum_cons]
      rw [hcons, ih_r, sum_map_filter_ne f a as]
      change (as.count a + 1) * f a + ((as.filter (fun b => !(b == a))).map f).sum =
        f a + (as.count a * f a + ((as.filter (fun b => !(b == a))).map f).sum)
      rw [Nat.add_mul]
      omega
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_of_le_of_lt (by simpa [r] using List.length_filter_le (fun b => !(b == a)) as) (by simp)

private theorem product_eq_sum_count (a b : Keys) :
    product a b = ((a.filterMap id).map (fun k => b.count (some k))).sum := by
  unfold product hist
  exact sum_group (fun k => b.count (some k)) (a.filterMap id)

private theorem count_some_eq_filterMap_count (b : Keys) (k : Nat) :
    b.count (some k) = (b.filterMap id).count k := by
  induction b with
  | nil => simp
  | cons x xs ih =>
      cases x with
      | none => simp [ih]
      | some x =>
          by_cases hx : x = k
          · subst x; simp [ih]
          · simp [ih, hx]

private theorem count_cons_eq_add (a k : Nat) (A : List Nat) :
    (a :: A).count k = A.count k + (if (a == k) = true then 1 else 0) := by
  by_cases h : (a == k) = true <;> simp [List.count_cons, h]

private theorem indicator_sum (a : Nat) (B : List Nat) :
    (B.map (fun k => if (a == k) = true then 1 else 0)).sum = B.count a := by
  induction B with
  | nil => simp
  | cons b B ih =>
      simp only [List.map_cons, List.sum_cons, List.count_cons]
      by_cases h : (b == a) = true
      · have hab : (a == b) = true := by simpa [Bool.beq_comm] using h
        rw [if_pos h, if_pos hab, ih]
        omega
      · have hab : (a == b) = false := by simpa [Bool.beq_comm] using h
        have h' : ¬ (b == a) = true := h
        have hab' : ¬ (a == b) = true := by simpa using hab
        rw [if_neg h', if_neg hab', ih]
        omega

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem sum_zero (B : List Nat) : (B.map (fun _ => 0)).sum = 0 := by
  induction B with
  | nil => simp
  | cons b B ih => simp [ih]

private theorem sum_count_eq_sum_count (A B : List Nat) :
    (A.map (fun k => B.count k)).sum = (B.map (fun k => A.count k)).sum := by
  induction A with
  | nil =>
      simp [sum_zero]
  | cons a A ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      have hmap : (B.map (fun k => (a :: A).count k)).sum =
          (B.map (fun k => A.count k + (if (a == k) = true then 1 else 0))).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k hk
        exact count_cons_eq_add a k A
      rw [hmap, sum_map_add, indicator_sum]
      omega

private theorem product_comm (a b : Keys) : product a b = product b a := by
  rw [product_eq_sum_count, product_eq_sum_count]
  simp [count_some_eq_filterMap_count]
  exact sum_count_eq_sum_count (a.filterMap id) (b.filterMap id)

private theorem entry_le_maxFreq (b : Keys) (k n : Nat) (h : (k, n) ∈ hist b) :
    n ≤ ((hist b).map Prod.snd).max?.getD 0 := by
  let ns := (hist b).map Prod.snd
  have hnmem : n ∈ ns := List.mem_map.mpr ⟨(k, n), h, rfl⟩
  have hne : ns ≠ [] := List.ne_nil_of_mem hnmem
  obtain ⟨m, hm⟩ : ∃ m, ns.max? = some m := by
    cases hm0 : ns.max? with
    | none => have : ns = [] := (List.max?_eq_none_iff).mp hm0; contradiction
    | some m => exact ⟨m, rfl⟩
  have hle : n ≤ m := (List.max?_eq_some_iff.mp hm).2 n hnmem
  simp [ns, hm]
  omega

private theorem count_le_maxFreq (b : Keys) (k : Nat) :
    b.count (some k) ≤ ((hist b).map Prod.snd).max?.getD 0 := by
  by_cases hk : k ∈ b.filterMap id
  · have hentry : (k, (b.filterMap id).count k) ∈ hist b := by
      unfold hist frequency
      exact List.mem_map.mpr ⟨k, (List.mem_eraseDups).mpr hk, rfl⟩
    have hb := entry_le_maxFreq b k ((b.filterMap id).count k) hentry
    rw [count_some_eq_filterMap_count]
    exact hb
  · rw [count_some_eq_filterMap_count]
    rw [List.count_eq_countP, List.countP_eq_length_filter]
    have hf : List.filter (fun x => x == k) (b.filterMap id) = [] := by
      apply (List.filter_eq_nil_iff).mpr
      intro x hx hxk
      apply hk
      have hxeq : x = k := by simpa using hxk
      simpa [hxeq] using hx
    rw [hf]
    simp

private theorem sum_le_card_mul_max (l : List Nat) (c : Nat) (h : ∀ x ∈ l, x ≤ c) :
    l.sum ≤ l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.length_cons, List.sum_cons, Nat.succ_mul]
      have hx := h x (by simp)
      have htail := ih (fun y hy => h y (by simp [hy]))
      omega


private theorem filter_hit_none (b : Keys) : (b.filter (hit none)).length = 0 := by
  simp [hit]

private theorem filter_hit_some (k : Nat) (b : Keys) :
    (b.filter (hit (some k))).length = b.count (some k) := by
  rw [List.count_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  intro j hj
  cases j with
  | none => simp [hit]
  | some j => simp [hit, Bool.beq_comm]

private theorem product_as_row_counts (a b : Keys) :
    product a b = (a.map (fun k => (b.filter (hit k)).length)).sum := by
  rw [product_eq_sum_count]
  induction a with
  | nil => simp
  | cons k ks ih =>
      cases k with
      | none => simp [filter_hit_none, ih]
      | some k =>
          simp [filter_hit_some, ih]

private theorem any_hit_iff_count_pos (k : Option Nat) (b : Keys) :
    b.any (hit k) = true ↔ 0 < (b.filter (hit k)).length := by
  constructor
  · intro hany
    rcases List.any_eq_true.mp hany with ⟨x, hx, hpx⟩
    have hxfilter : x ∈ b.filter (hit k) := by simp [hx, hpx]
    have hne : b.filter (hit k) ≠ [] := List.ne_nil_of_mem hxfilter
    exact Nat.pos_of_ne_zero (by
      intro hzero
      exact hne ((List.length_eq_zero_iff).mp hzero))
  · intro hpos
    have hne : b.filter (hit k) ≠ [] := by
      intro hnil
      rw [hnil] at hpos
      simp at hpos
    rcases List.exists_mem_of_ne_nil _ hne with ⟨x, hxfilter⟩
    simp only [List.mem_filter] at hxfilter
    exact List.any_eq_true.mpr ⟨x, hxfilter.1, hxfilter.2⟩

private theorem key_left_outer (k : Option Nat) (b : Keys) :
    (b.filter (hit k)).length + (if !(b.any (hit k)) then 1 else 0) =
      max 1 (b.filter (hit k)).length := by
  by_cases hany : (b.any (hit k)) = true
  · have hpos := (any_hit_iff_count_pos k b).mp hany
    rw [Nat.max_eq_right (by omega)]
    simp [hany]
  · have hnpos : ¬ 0 < (b.filter (hit k)).length := by
      intro hp
      exact hany ((any_hit_iff_count_pos k b).mpr hp)
    have hzero : (b.filter (hit k)).length = 0 := Nat.eq_zero_of_not_pos hnpos
    simp [hany, hzero]

private theorem length_filter_eq_sum_indicator {α : Type} (p : α → Bool) (l : List α) :
    (l.filter p).length = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
        omega
      · have hf : p x = false := by simpa using h
        simp [hf, ih]


private theorem sum_filter_eq_map_indicator {α : Type} (p : α → Bool) (f : α → Nat) (l : List α) :
    ((l.filter p).map f).sum = (l.map (fun x => if p x then f x else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
      · have hf : p x = false := by simpa using h
        simp [hf, ih]

private theorem count_filter_route_eq (b : Keys) (k : Option Nat) (route : Nat → Bool) (flag : Bool)
    (hk : k.any (fun x => route x == flag) = true) :
    (b.filter (fun x => x.any (fun y => route y == flag))).count k = b.count k := by
  induction b with
  | nil => simp
  | cons x xs ih =>
      cases k with
      | none => simp at hk
      | some k =>
          cases x with
          | none => simp [ih, hk]
          | some x =>
              by_cases hx : x = k
              · subst x
                simp [ih, hk]
              · have hxbeq : ((some x : Option Nat) == some k) = false := by simp [hx]
                by_cases hkeep : (some x : Option Nat).any (fun y => route y == flag) = true
                · simp [List.count_cons, hkeep, ih, hxbeq]
                · simp [List.count_cons, hkeep, ih, hxbeq]

private theorem product_side_route (a b : Keys) (route : Nat → Bool) (flag : Bool) :
    product (a.filter (fun k => k.any (fun x => route x == flag)))
      (b.filter (fun k => k.any (fun x => route x == flag))) =
        (a.map (fun k => if k.any (fun x => route x == flag) then (b.filter (hit k)).length else 0)).sum := by
  rw [product_as_row_counts]
  have hmap :
      ((a.filter (fun k => k.any (fun x => route x == flag))).map
        (fun k => ((b.filter (fun j => j.any (fun x => route x == flag))).filter (hit k)).length)) =
      ((a.filter (fun k => k.any (fun x => route x == flag))).map (fun k => (b.filter (hit k)).length)) := by
    apply List.map_congr_left
    intro k hk
    have hkflag : k.any (fun x => route x == flag) = true := (List.mem_filter.mp hk).2
    cases k with
    | none => simp at hkflag
    | some k =>
        have hkroute : (route k == flag) = true := by simpa using hkflag
        rw [filter_hit_some, filter_hit_some]
        exact count_filter_route_eq b (some k) route flag hkflag
  rw [hmap]
  exact sum_filter_eq_map_indicator (fun k : Option Nat => k.any (fun x => route x == flag))
    (fun k => (b.filter (hit k)).length) a

private theorem route_indicator_add (k : Option Nat) (b : Keys) (route : Nat → Bool) :
    (if k.any (fun x => route x == true) then (b.filter (hit k)).length else 0) +
      (if k.any (fun x => route x == false) then (b.filter (hit k)).length else 0) =
        (b.filter (hit k)).length := by
  cases k with
  | none =>
      rw [filter_hit_none]
      simp
  | some k =>
      cases h : route k <;> simp [h]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.DisjointBuildRoutingWork.Goal := by
  intro a b route
  dsimp only
  rw [product_side_route, product_side_route, product_as_row_counts]
  rw [← sum_map_add]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  exact route_indicator_add k b route

end TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout.Goal_DisjointBuildRoutingWork
