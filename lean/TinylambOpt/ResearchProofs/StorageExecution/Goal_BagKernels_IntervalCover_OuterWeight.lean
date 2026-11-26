import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `IntervalCover.OuterWeight` — LEFT JOIN contributes one unmatched right placeholder
  or the complete right multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover
open TinylambOpt.ResearchGoals.Compression.Wire

private def lhs (a c : List (Nat × Nat)) : Nat :=
  weighted bag a (fun x => max 1 (bag.mass c x))

private def rhs (a c : List (Nat × Nat)) : Nat :=
  (pairs (bag.expand a) (bag.expand c) (· == ·)).length +
  ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).length

#eval lhs [(3,2),(7,3)] [(7,1)]
#eval rhs [(3,2),(7,3)] [(7,1)]
#eval lhs [(3,2),(3,4),(7,0)] [(3,1),(5,2)]
#eval rhs [(3,2),(3,4),(7,0)] [(3,1),(5,2)]
#eval lhs [(3,0),(7,3)] []
#eval rhs [(3,0),(7,3)] []
#eval lhs [] [(7,3)]
#eval rhs [] [(7,3)]

example : lhs [(3,2),(7,3)] [(7,1)] = rhs [(3,2),(7,3)] [(7,1)] := by decide
example : lhs [(3,2),(3,4),(7,0)] [(3,1),(5,2)] = rhs [(3,2),(3,4),(7,0)] [(3,1),(5,2)] := by decide
example : lhs [(3,0),(7,3)] [] = rhs [(3,0),(7,3)] [] := by decide
example : lhs [] [(7,3)] = rhs [] [(7,3)] := by decide

private def smallLists : List (List (Nat × Nat)) :=
  let ps := (List.range 2).flatMap (fun x => (List.range 3).map (fun y => (x,y)))
  ps.flatMap (fun p => ps.flatMap (fun q => ps.flatMap (fun r =>
    [[], [p], [p,q], [p,q,r]])))

#eval (smallLists.flatMap fun a =>
  smallLists.flatMap fun c =>
    if lhs a c == rhs a c then [] else [(a,c)]).length

private theorem max?_some_of_ne_nil : ∀ {xs : List Nat}, xs ≠ [] → ∃ m, xs.max? = some m := by
  intro xs
  cases xs with
  | nil => simp
  | cons a as =>
      intro _
      rw [List.max?_cons]
      exact ⟨as.max?.elim a (max a), rfl⟩

private theorem le_peak {xs : List Nat} {x : Nat} (hx : x ∈ xs) : x ≤ peak xs := by
  have hxsne : xs ≠ [] := by
    intro hnil
    subst xs
    simp at hx
  obtain ⟨m, hm⟩ := max?_some_of_ne_nil (xs := xs) hxsne
  have hmprops := List.max?_eq_some_iff.mp hm
  have hxm : x ≤ m := hmprops.2 x hx
  have hp : peak xs = m := by simp [peak, hm]
  rw [hp]
  exact hxm

private theorem count_map_add (s n k : Nat) :
    ((List.range n).map (fun i => s + i)).count k = (if s ≤ k && k < s + n then 1 else 0) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.count_append]
      simp only [List.map_singleton]
      rw [List.count_cons, ih]
      by_cases hp : s ≤ k
      · by_cases hk : k < s + n
        · have hkn : k < s + (n+1) := by omega
          have hne : s + n ≠ k := by omega
          have hbeq : (s + n == k) = false := by simp [hne]
          simp [hp, hk, hkn, hbeq]
        · have hle : s + n ≤ k := Nat.le_of_not_gt hk
          by_cases hkeq : k = s + n
          · subst k
            simp [hp]
          · have hkn : ¬ k < s + (n+1) := by omega
            have hne : s + n ≠ k := by omega
            have hbeq : (s + n == k) = false := by simp [hne]
            simp [hp, hk, hkn, hbeq]
      · have hks : k < s := Nat.lt_of_not_ge hp
        have hne : s + n ≠ k := by omega
        have hbeq : (s + n == k) = false := by simp [hne]
        simp [hp, hbeq]

private theorem mass_eq_count_expand (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass rs k = (bag.expand rs).count k := by
  induction rs with
  | nil => simp [bag]
  | cons r rs ih =>
      obtain ⟨s, n⟩ := r
      have hmass_cons : bag.mass ((s, n) :: rs) k =
          bag.mass rs k + (if s ≤ k && k < s + n then 1 else 0) := by
        simp [bag, List.countP_cons]
      rw [hmass_cons, ih]
      simp [bag, List.count_flatMap, count_map_add, Function.comp_def, Nat.add_comm]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_zero (l : List Nat) : (l.map fun _ => 0).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem filter_ne_eq_self (a : Nat) (l : List Nat) (h : a ∉ l) :
    l.filter (fun b => ! b == a) = l := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hba : b ≠ a := by
        intro hb
        subst b
        exact h (by simp)
      have hbs : a ∉ bs := by
        intro ha
        exact h (by simp [ha])
      simp [hba, ih hbs]

private theorem eraseDups_eq_self_of_nodup (l : List Nat) (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.eraseDups_cons]
      simp only [List.nodup_cons] at h
      rw [filter_ne_eq_self a as h.1]
      rw [ih h.2]

private theorem sum_range_indicator (P x : Nat) (f : Nat → Nat) :
    ((List.range P).map (fun k => (if (x == k) = true then 1 else 0) * f k)).sum =
      (if x < P then f x else 0) := by
  induction P with
  | zero => simp
  | succ P ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      by_cases hxP : x < P
      · have hxPs : x < P+1 := by omega
        have hne : x ≠ P := by omega
        simp [hxP, hxPs, hne]
      · by_cases hxeq : x = P
        · subst x
          simp
        · have hxPs : ¬ x < P+1 := by
            have hle : P ≤ x := Nat.le_of_not_gt hxP
            omega
          simp [hxP, hxPs, hxeq]

private theorem sum_count_eq_sum_map (xs : List Nat) (P : Nat) (f : Nat → Nat)
    (hbound : ∀ x ∈ xs, x < P) :
    ((List.range P).map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  induction xs with
  | nil =>
      simp
      exact sum_map_zero (List.range P)
  | cons x xs ih =>
      have hx : x < P := hbound x (by simp)
      have hxs : ∀ y ∈ xs, y < P := by
        intro y hy
        exact hbound y (by simp [hy])
      simp only [List.count_cons]
      calc
        ((List.range P).map (fun k => (xs.count k + (if (x == k) = true then 1 else 0)) * f k)).sum
            = ((List.range P).map (fun k => xs.count k * f k + (if (x == k) = true then 1 else 0) * f k)).sum := by
                congr 1
                apply List.map_congr_left
                intro k hk
                rw [Nat.add_mul]
        _ = ((List.range P).map (fun k => xs.count k * f k)).sum +
            ((List.range P).map (fun k => (if (x == k) = true then 1 else 0) * f k)).sum := by
                exact sum_map_add (List.range P) _ _
        _ = (xs.map f).sum + (if x < P then f x else 0) := by
                rw [ih hxs, sum_range_indicator]
        _ = ((x :: xs).map f).sum := by
                simp [hx, Nat.add_comm]

private theorem mem_expand_lt_peak (rs : List (Nat × Nat)) {x : Nat}
    (hx : x ∈ bag.expand rs) : x < peak (rs.map (fun r : Nat × Nat => r.1 + r.2)) := by
  change x ∈ rs.flatMap (fun r : Nat × Nat => (List.range r.2).map (r.1 + ·)) at hx
  rw [List.mem_flatMap] at hx
  obtain ⟨⟨s, n⟩, hrs, hmem⟩ := hx
  rw [List.mem_map] at hmem
  obtain ⟨i, hi, rfl⟩ := hmem
  rw [List.mem_range] at hi
  have hmem' : s + n ∈ rs.map (fun r : Nat × Nat => r.1 + r.2) :=
    List.mem_map.mpr ⟨(s, n), hrs, rfl⟩
  have hle : s + n ≤ peak (rs.map (fun r : Nat × Nat => r.1 + r.2)) := le_peak hmem'
  omega

private theorem weighted_eq_sum_expand (a : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag a f = ((bag.expand a).map f).sum := by
  unfold weighted
  have hkeys : (bag.keys a).eraseDups = bag.keys a := by
    change (List.range (peak (a.map fun r : Nat × Nat => r.1 + r.2))).eraseDups =
      List.range (peak (a.map fun r : Nat × Nat => r.1 + r.2))
    exact eraseDups_eq_self_of_nodup _ List.nodup_range
  rw [hkeys]
  change ((List.range (peak (a.map fun r : Nat × Nat => r.1 + r.2))).map
      (fun k => bag.mass a k * f k)).sum = ((bag.expand a).map f).sum
  simp only [mass_eq_count_expand]
  apply sum_count_eq_sum_map
  intro x hx
  exact mem_expand_lt_peak a hx

private theorem count_filter_eq_count (ys : List Nat) (x : Nat) :
    (ys.filter (fun y => x == y)).length = ys.count x := by
  rw [← List.countP_eq_length_filter]
  rw [List.count_eq_countP]
  apply List.countP_congr
  intro y hy
  constructor
  · intro h
    have hxy : x = y := by simpa using h
    simp [hxy]
  · intro h
    have hyx : y = x := by simpa using h
    simp [hyx]

private theorem not_contains_indicator (ys : List Nat) (x : Nat) :
    (if (!ys.contains x) = true then 1 else 0) = (if ys.count x == 0 then 1 else 0) := by
  by_cases hx : x ∈ ys
  · have hc : ys.contains x = true := List.contains_iff_mem.mpr hx
    have hn : ys.count x ≠ 0 := by
      intro hz
      exact ((List.count_eq_zero).mp hz) hx
    rw [hc]
    simp [hn]
  · have hc : ys.contains x = false := by
      cases hcy : ys.contains x with
      | false => rfl
      | true =>
          exfalso
          exact hx (List.contains_iff_mem.mp hcy)
    have hz : ys.count x = 0 := (List.count_eq_zero).mpr hx
    rw [hc]
    simp [hz]

private theorem count_add_indicator (n : Nat) :
    n + (if n == 0 then 1 else 0) = max 1 n := by
  by_cases h : n = 0
  · subst n
    simp
  · have hpos : 1 ≤ n := Nat.succ_le_of_lt (Nat.pos_of_ne_zero h)
    have hbeq : (n == 0) = false := by simp [h]
    rw [hbeq]
    simp [Nat.max_eq_right hpos]

private theorem pairs_len_eq_sum_count (xs ys : List Nat) :
    (pairs xs ys (· == ·)).length = (xs.map (fun x => ys.count x)).sum := by
  simp [pairs, List.length_flatMap, List.length_map, count_filter_eq_count]

private theorem filter_notmem_length (xs ys : List Nat) :
    (xs.filter (fun x => decide (¬ x ∈ ys))).length =
    (xs.map (fun x => if ys.count x == 0 then 1 else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.filter_cons, List.map_cons, List.sum_cons]
      by_cases hx : x ∈ ys
      · have hn : ys.count x ≠ 0 := by
          intro hz
          exact ((List.count_eq_zero).mp hz) hx
        have hbeq : (ys.count x == 0) = false := by simp [hn]
        rw [ite_eq_right (by simp [hx])]
        rw [ih]
        simp [hbeq]
      · have hz : ys.count x = 0 := (List.count_eq_zero).mpr hx
        have hbeq : (ys.count x == 0) = true := by simp [hz]
        rw [ite_eq_left (by simp [hx])]
        simp only [List.length_cons]
        rw [ih]
        simp [hbeq, Nat.add_comm]

private theorem filter_notcontains_eq (xs ys : List Nat) :
    xs.filter (fun x => !ys.contains x) = xs.filter (fun x => decide (¬ x ∈ ys)) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x ∈ ys <;> simp [hx]

private theorem filter_notcontains_length (xs ys : List Nat) :
    (xs.filter (fun x => !ys.contains x)).length =
    (xs.map (fun x => if ys.count x == 0 then 1 else 0)).sum := by
  rw [filter_notcontains_eq]
  exact filter_notmem_length xs ys

private theorem sum_count_plus_filter (xs ys : List Nat) :
    (xs.map (fun x => ys.count x)).sum + (xs.filter (fun x => !ys.contains x)).length =
    (xs.map (fun x => max 1 (ys.count x))).sum := by
  rw [filter_notcontains_length]
  rw [← sum_map_add]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  exact count_add_indicator (ys.count x)

private theorem pairs_len_and_filter (xs ys : List Nat) :
    (pairs xs ys (· == ·)).length + (xs.filter (fun x => !ys.contains x)).length =
    (xs.map (fun x => max 1 (ys.count x))).sum := by
  rw [pairs_len_eq_sum_count]
  exact sum_count_plus_filter xs ys

theorem sound_OuterWeight : IntervalCover.OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_sum_expand a (fun x => max 1 (bag.mass c x))]
  simp only [mass_eq_count_expand]
  rw [pairs_len_and_filter (bag.expand a) (bag.expand c)]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_IntervalCover_OuterWeight
