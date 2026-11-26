import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.DisjointBuildRoutingWork.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_JoinFanout_DisjointBuildRoutingWork
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

private theorem sidefl : ∀ (l : List (Option Nat)) (route : Nat → Bool) (flag : Bool),
    ((l.filter fun k => k.any fun x => route x == flag).filterMap id) =
      (l.filterMap id).filter fun x => route x == flag := by
  intro l route flag
  induction l with
  | nil => simp
  | cons k t ih =>
      cases k with
      | none => simp [ih]
      | some x =>
          cases h : route x == flag with
          | true => simp [List.filter_cons, List.filterMap_cons, h, ih]
          | false => simp [List.filter_cons, List.filterMap_cons, h, ih]

private theorem count_split : ∀ (m : List Nat) (p q : Nat → Bool), (∀ x, q x = !(p x)) →
    ∀ v, (m.filter p).count v + (m.filter q).count v = m.count v := by
  intro m
  induction m with
  | nil => intro p q hq v; simp
  | cons z t ih =>
      intro p q hq v
      cases h : p z with
      | true =>
          have hqz : q z = false := by rw [hq]; simp [h]
          have hih := ih p q hq v
          simp [List.filter_cons, List.count_cons, h, hqz, hih]
          omega
      | false =>
          have hqz : q z = true := by rw [hq]; simp [h]
          have hih := ih p q hq v
          simp [List.filter_cons, List.count_cons, h, hqz, hih]
          omega

private theorem key' : ∀ (l m : List Nat) (p q : Nat → Bool), (∀ x, q x = !(p x)) →
    ((l.filter p).map fun v => (m.filter p).count v).sum +
      ((l.filter q).map fun v => (m.filter q).count v).sum =
    (l.map fun v => m.count v).sum := by
  intro l
  induction l with
  | nil => intro m p q hq; simp
  | cons z t ih =>
      intro m p q hq
      cases h : p z with
      | true =>
          have hqz : q z = false := by rw [hq]; simp [h]
          have hqn : ¬ (q z = true) := by rw [hqz]; simp
          have hnz : z ∉ List.filter q m := by
            intro hx
            obtain ⟨_, h2⟩ := (List.mem_filter).mp hx
            rw [hqz] at h2
            simp at h2
          have hAB := ih m p q hq
          have hcnt := count_split m p q hq z
          have hz0 : List.count z (List.filter q m) = 0 :=
            List.count_eq_zero_of_not_mem hnz
          rw [List.filter_cons, List.filter_cons, if_pos h, if_neg hqn,
            List.map_cons, List.map_cons, List.sum_cons, List.sum_cons]
          rw [hz0, Nat.add_zero] at hcnt
          rw [← hcnt, ← hAB]
          omega
      | false =>
          have hqz : q z = true := by rw [hq]; simp [h]
          have hpn : ¬ (p z = true) := by rw [h]; simp
          have hnz : z ∉ List.filter p m := by
            intro hx
            obtain ⟨_, h2⟩ := (List.mem_filter).mp hx
            rw [h] at h2
            simp at h2
          have hAB := ih m p q hq
          have hcnt := count_split m p q hq z
          have hz0 : List.count z (List.filter p m) = 0 :=
            List.count_eq_zero_of_not_mem hnz
          rw [List.filter_cons, List.filter_cons, if_neg hpn, if_pos hqz,
            List.map_cons, List.map_cons, List.sum_cons, List.sum_cons]
          rw [hz0, Nat.zero_add] at hcnt
          rw [← hcnt, ← hAB]
          omega

/--
Local work estimated by bucket products is additive after eliminating keys
routed to different buckets: the two same-bucket products sum exactly to the
global product, since a matching pair of keys always shares its route bit.
-/
theorem sound : DisjointBuildRoutingWork.Goal := by
  intro a b route
  have hflip : (fun x => route x == false) = (fun x => !(route x == true)) := by
    funext x
    cases route x <;> simp
  have hT : product (a.filter fun k => k.any fun x => route x == true)
      (b.filter fun k => k.any fun x => route x == true) =
      (((a.filterMap id).filter fun x => route x == true).map
        (fun v => ((b.filterMap id).filter fun x => route x == true).count v)).sum := by
    rw [prod_rw, sidefl, sidefl]
  have hF : product (a.filter fun k => k.any fun x => route x == false)
      (b.filter fun k => k.any fun x => route x == false) =
      (((a.filterMap id).filter fun x => route x == false).map
        (fun v => ((b.filterMap id).filter fun x => route x == false).count v)).sum := by
    rw [prod_rw, sidefl, sidefl]
  show product (a.filter fun k => k.any fun x => route x == true)
      (b.filter fun k => k.any fun x => route x == true) +
      product (a.filter fun k => k.any fun x => route x == false)
      (b.filter fun k => k.any fun x => route x == false) = product a b
  rw [hT, hF, hflip]
  rw [key' (a.filterMap id) (b.filterMap id) (fun x => route x == true)
    (fun x => !(route x == true)) (fun x => rfl)]
  rw [← prod_rw]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_JoinFanout_DisjointBuildRoutingWork
