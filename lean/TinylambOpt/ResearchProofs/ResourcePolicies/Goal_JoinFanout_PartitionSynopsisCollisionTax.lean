import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.PartitionSynopsisCollisionTax.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_JoinFanout_PartitionSynopsisCollisionTax
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
open TinylambOpt.ResearchGoals.QueryRewrites

private theorem nodup_filterMap_eraseDups :
    ∀ (l : List Nat) (f : Nat → Option Nat), (l.filterMap f).eraseDups.Nodup := by
  intro l
  induction l with
  | nil => intro f; simp [List.filterMap]
  | cons z t ih =>
      intro f
      simp only [List.filterMap_cons]
      split
      · exact ih f
      · rename_i b _
        rw [List.eraseDups_cons]
        refine (List.nodup_cons).mpr ⟨?_, ?_⟩
        · intro hx
          rw [List.mem_eraseDups, List.mem_filter] at hx
          obtain ⟨hp, _⟩ := hx
          have hb : (b == b) = true := by simp
          simp_all
        · have hstep : (t.filterMap f).filter (fun c => !(c == b)) =
              t.filterMap (fun y => (f y).filter fun c => !(c == b)) := List.filter_filterMap
          have := ih (fun y => (f y).filter fun c => !(c == b))
          simpa [hstep] using this

/-- `eraseDups` of any list is duplicate-free. -/
private theorem nodup_eraseDups2 : ∀ (l : List Nat), l.eraseDups.Nodup := by
  intro l
  have key : l.filterMap some = l := by
    induction l with
    | nil => rfl
    | cons z t ih => simp [List.filterMap_cons, ih]
  have := nodup_filterMap_eraseDups l some
  rwa [key] at this

private theorem sum_map_add' {f g : Nat → Nat} : ∀ (l : List Nat),
    ((l.map fun y => f y + g y)).sum = (l.map f).sum + (l.map g).sum := by
  intro l
  induction l with
  | nil => simp
  | cons z t ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem sum_congr2 {f g : Nat → Nat} : ∀ (l : List Nat),
    (∀ y ∈ l, f y = g y) → (l.map f).sum = (l.map g).sum := by
  intro l
  induction l with
  | nil => intro h; rfl
  | cons z t ih =>
      intro h
      simp [List.map_cons, List.sum_cons,
        ih (fun y hy => h y ((List.mem_cons).mpr (Or.inr hy))),
        h z ((List.mem_cons).mpr (Or.inl rfl))]

private theorem sum_ite_count : ∀ (d : List Nat) (a : Nat),
    (d.map fun y => if a == y then 1 else 0).sum = d.count a := by
  intro d
  induction d with
  | nil => intro a; simp
  | cons z t ih =>
      intro a
      rw [List.map_cons, List.sum_cons, ih, List.count_cons]
      rcases Decidable.em (a = z) with haz | haz
      · subst haz
        simp
        omega
      · have h1 : ¬ (a = z) := haz
        have h2 : ¬ (z = a) := fun h => h1 h.symm
        simp [h1, h2]

private theorem sum_ite_mul {g : Nat → Nat} : ∀ (d : List Nat) (a : Nat), d.Nodup →
    (d.map fun y => (if a == y then 1 else 0) * g y).sum = if a ∈ d then g a else 0 := by
  intro d
  induction d with
  | nil => intro a _; simp
  | cons z t ih =>
      intro a hn
      obtain ⟨hz, htn⟩ := (List.nodup_cons).mp hn
      rw [List.map_cons, List.sum_cons, ih a htn]
      rcases Decidable.em (a = z) with haz | haz
      · subst haz
        simp [List.mem_cons, hz]
      · have h2 : ¬ z = a := fun h => haz h.symm
        simp [haz, h2, hz]

/-- A frequency synopsis weighted by `g` sums to the plain sum of `g`. -/
private theorem freq_sum_weighted {g : Nat → Nat} : ∀ (xs : List Nat),
    ((frequency xs).map (fun p => p.2 * g p.1)).sum = (xs.map g).sum := by
  intro xs
  have hform : (frequency xs).map (fun p => p.2 * g p.1) = xs.eraseDups.map (fun y => xs.count y * g y) := by
    simp [frequency, List.map_map]
  rw [hform]
  suffices h : ∀ (xs : List Nat), ((xs.reverse).eraseDups.map fun y =>
      xs.reverse.count y * g y).sum = (xs.reverse.map g).sum by
    have := h xs.reverse
    rwa [List.reverse_reverse] at this
  intro xs
  induction xs with
  | nil => simp
  | cons a t ih =>
      rw [List.reverse_cons]
      generalize t.reverse = l at ih ⊢
      have hih : (l.eraseDups.map fun y => l.count y * g y).sum = (l.map g).sum := ih
      have hn : l.eraseDups.Nodup := nodup_eraseDups2 l
      have hdedup : (l ++ [a]).eraseDups = if a ∈ l then l.eraseDups else l.eraseDups ++ [a] := by
        rw [List.eraseDups_append]
        by_cases hmem : a ∈ l
        · rw [if_pos hmem]
          have h1 : [a].removeAll l = [] := by
            simp [List.removeAll, List.elem_eq_mem, hmem]
          rw [h1, List.eraseDups_nil, List.append_nil]
        · rw [if_neg hmem]
          have h1 : [a].removeAll l = [a] := by
            simp [List.removeAll, List.elem, hmem]
          rw [h1]
          have h2 : ([a] : List Nat).eraseDups = [a] := by
            rw [List.eraseDups_cons]
            simp
          rw [h2]
      by_cases hmem : a ∈ l
      · rw [hdedup, if_pos hmem]
        have hcnt : (l.eraseDups.map fun y => (l ++ [a]).count y * g y).sum =
            (l.eraseDups.map fun y => (l.count y + if a == y then 1 else 0) * g y).sum := by
          refine sum_congr2 l.eraseDups ?_
          intro y hy
          rw [List.mem_eraseDups] at hy
          rw [List.count_append, List.count_singleton]
        have hdist : (l.eraseDups.map fun y => (l.count y + if a == y then 1 else 0) * g y) =
            (l.eraseDups.map fun y => l.count y * g y + (if a == y then 1 else 0) * g y) := by
          refine List.map_congr_left ?_
          intro y _
          simp [Nat.add_mul]
        rw [hcnt, hdist, sum_map_add', hih]
        have h1 : (l.eraseDups.map fun y => (if a == y then 1 else 0) * g y).sum = g a := by
          have hmem' : a ∈ l.eraseDups := List.mem_eraseDups.mpr hmem
          rw [sum_ite_mul (g := g) l.eraseDups a hn, if_pos hmem']
        have h2 : ((l ++ [a]).map g).sum = (l.map g).sum + g a := by
          simp [List.map_append, List.map_singleton, List.sum_append]
        rw [h1, h2]
      · rw [hdedup, if_neg hmem]
        have h0 : List.count a l = 0 := List.count_eq_zero_of_not_mem hmem
        have h2 : (l.eraseDups ++ [a]).map (fun y => (l ++ [a]).count y * g y) =
            (l.eraseDups.map (fun y => (l ++ [a]).count y * g y)) ++ [1 * g a] := by
          rw [List.map_append, List.map_singleton]
          congr 1
          rw [List.count_append, List.count_singleton]
          simp [h0]
        rw [h2, List.sum_append, List.sum_singleton]
        have hsplit : (l.eraseDups.map fun y => (l ++ [a]).count y * g y).sum =
            (l.eraseDups.map fun y => l.count y * g y).sum := by
          refine sum_congr2 l.eraseDups ?_
          intro y hy
          rw [List.mem_eraseDups] at hy
          rw [List.count_append, List.count_singleton]
          have hny : ¬ a = y := by intro hh; subst hh; exact hmem hy
          simp [hny]
        rw [hsplit, hih]
        have hmap : ((l ++ [a]).map g) = l.map g ++ [g a] := by
          rw [List.map_append, List.map_singleton]
        rw [hmap, List.sum_append]
        simp only [List.sum_singleton]
        omega

private theorem bcount_some : ∀ (b : List (Option Nat)) (k : Nat),
    List.count (some k) b = (b.filterMap id).count k := by
  intro b
  induction b with
  | nil => intro k; simp [List.filterMap]
  | cons z t ih =>
      intro k
      cases z with
      | none => simp [List.filterMap_cons, List.count_cons, ih]
      | some x =>
          simp only [List.filterMap_cons, List.count_cons, id_eq]
          rw [ih]
          rcases Decidable.em (k = x) with hk | hk
          · subst hk; simp
          · simp [hk]

private theorem prod_rw : ∀ (x y : List (Option Nat)),
    product x y = ((x.filterMap id).map fun v => (y.filterMap id).count v).sum := by
  intro x y
  have h1 : product x y =
      ((frequency (x.filterMap id)).map (fun p => p.2 * List.count (some p.1) y)).sum := by
    simp [product, hist]
  have h2 : ((frequency (x.filterMap id)).map (fun p => p.2 * List.count (some p.1) y)).sum =
      ((x.filterMap id).map fun v => List.count (some v) y).sum :=
    freq_sum_weighted (g := fun v => List.count (some v) y) (x.filterMap id)
  have h3 : ((x.filterMap id).map fun v => List.count (some v) y).sum =
      ((x.filterMap id).map fun v => (y.filterMap id).count v).sum :=
    sum_congr2 (x.filterMap id) (fun v _ => bcount_some y v)
  rw [h1, h2, h3]

private theorem filterMap_map_route : ∀ (l : List (Option Nat)) (route : Nat → Nat),
    (l.map (Option.map route)).filterMap id = (l.filterMap id).map route := by
  intro l route
  induction l with
  | nil => simp
  | cons k t ih =>
      cases k with
      | none => simp [ih]
      | some x => simp [ih]

private theorem sum_ite_filter {α : Type} : ∀ (l : List α) (p : α → Bool),
    (l.map fun x => if p x then 1 else 0).sum = (l.filter p).length := by
  intro l
  induction l with
  | nil => intro p; simp
  | cons z t ih =>
      intro p
      cases hz : p z with
      | true =>
          simp [List.map_cons, List.sum_cons, List.filter_cons, ih, hz]
          omega
      | false =>
          simp [List.map_cons, List.sum_cons, List.filter_cons, ih, hz]

private theorem sum_ite_count2 : ∀ (d : List Nat) (a : Nat),
    (d.map fun y => if y == a then 1 else 0).sum = d.count a := by
  intro d a
  rw [← sum_ite_count]
  refine congrArg (fun m : List Nat => m.sum) ?_
  refine List.map_congr_left ?_
  intro y _
  rcases Decidable.em (y = a) with hy | hy
  · subst hy; rfl
  · have h1 : (y == a) = false := by simpa using hy
    have h2 : (a == y) = false := by simpa using fun hh => hy hh.symm
    rw [h1, h2]

private theorem hcnt : ∀ (l : List Nat) (route : Nat → Nat) (k : Nat),
    (l.map route).count (route k) = (l.filter fun j => route j == route k).length := by
  intro l
  induction l with
  | nil => intro route k; simp
  | cons z t ih =>
      intro route k
      rw [List.map_cons, List.count_cons, List.filter_cons, ih]
      cases h : route z == route k with
      | true =>
          have h2 : (route k == route z) = true := by rw [Bool.beq_comm]; exact h
          simp [h, h2]
      | false =>
          have h2 : (route k == route z) = false := by rw [Bool.beq_comm]; exact h
          simp [h, h2]

private theorem ite_split : ∀ (j k : Nat) (route : Nat → Nat),
    (if route j == route k then 1 else 0) =
      (if j == k then 1 else 0) + (if k != j && route k == route j then 1 else 0) := by
  intro j k route
  rcases Decidable.em (j = k) with hjk | hjk
  · subst hjk
    simp
  · rw [Bool.beq_comm (a := route j) (b := route k)]
    simp [hjk]
    by_cases h : route k = route j
    · simp [h]
      exact fun hh => hjk hh.symm
    · simp [h]

/--
Bucket-level products overestimate the true pair count exactly by the number
of mismatching key pairs that collide under the routing function.
-/
theorem sound : PartitionSynopsisCollisionTax.Goal := by
  intro a b route
  have hL : product (a.map (Option.map route)) (b.map (Option.map route)) =
      ((a.filterMap id).map fun k => ((b.filterMap id).map route).count (route k)).sum := by
    rw [prod_rw, filterMap_map_route, filterMap_map_route, List.map_map]
    exact congrArg (fun m : List Nat => m.sum) (List.map_congr_left (fun k _ => rfl))
  have hper : ∀ k : Nat,
      ((b.filterMap id).map route).count (route k) =
        List.count k (b.filterMap id) +
        ((b.filterMap id).filter fun j => k != j && route k == route j).length := by
    intro k
    have h3 : (List.map (fun j => if route j == route k then 1 else 0) (b.filterMap id)).sum =
        (List.map (fun j => (if j == k then 1 else 0) +
          (if k != j && route k == route j then 1 else 0)) (b.filterMap id)).sum := by
      refine congrArg (fun m : List Nat => m.sum) ?_
      refine List.map_congr_left (fun j _ => ite_split j k route)
    calc ((b.filterMap id).map route).count (route k)
        = ((b.filterMap id).filter fun j => route j == route k).length :=
          hcnt (b.filterMap id) route k
      _ = (List.map (fun j => if route j == route k then 1 else 0) (b.filterMap id)).sum :=
          (sum_ite_filter (b.filterMap id) (fun j => route j == route k)).symm
      _ = (List.map (fun j => (if j == k then 1 else 0) +
          (if k != j && route k == route j then 1 else 0)) (b.filterMap id)).sum := h3
      _ = (List.map (fun j => if j == k then 1 else 0) (b.filterMap id)).sum +
          (List.map (fun j => if k != j && route k == route j then 1 else 0)
            (b.filterMap id)).sum :=
          (sum_map_add' (f := fun j => if j == k then 1 else 0)
            (g := fun j => if k != j && route k == route j then 1 else 0)
            (b.filterMap id))
      _ = List.count k (b.filterMap id) +
          ((b.filterMap id).filter fun j => k != j && route k == route j).length := by
          rw [sum_ite_count2, sum_ite_filter]
  rw [hL, sum_congr2 (a.filterMap id) (fun k _ => hper k)]
  rw [sum_map_add']
  rw [← prod_rw]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_JoinFanout_PartitionSynopsisCollisionTax
