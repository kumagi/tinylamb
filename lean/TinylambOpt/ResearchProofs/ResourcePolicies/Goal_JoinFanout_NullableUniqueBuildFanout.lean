import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.NullableUniqueBuildFanout.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_JoinFanout_NullableUniqueBuildFanout
open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout
open TinylambOpt.ResearchGoals.QueryRewrites

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

private theorem sum_le' {f g : Nat → Nat} : ∀ (l : List Nat),
    (∀ y ∈ l, f y ≤ g y) → (l.map f).sum ≤ (l.map g).sum := by
  intro l
  induction l with
  | nil => intro _; simp
  | cons z t ih =>
      intro h
      have h1 := h z (List.mem_cons.mpr (Or.inl rfl))
      have h2 := ih (fun y hy => h y (List.mem_cons.mpr (Or.inr hy)))
      simp only [List.map_cons, List.sum_cons]
      omega

private theorem nodup_count_le_one : ∀ (l : List Nat), l.Nodup → ∀ x, l.count x ≤ 1 := by
  intro l
  induction l with
  | nil => intro _ x; simp
  | cons z t ih =>
      intro hn x
      obtain ⟨hz, htn⟩ := (List.nodup_cons).mp hn
      rw [List.count_cons]
      rcases Decidable.em (x = z) with hx | hx
      · subst hx
        rw [List.count_eq_zero_of_not_mem hz]
        simp
      · have hz' : ¬ (z = x) := fun h => hx h.symm
        have hle := ih htn x
        simp [hx, hz'] at *
        omega

private theorem sum_one_eq_length : ∀ (l : List Nat), (l.map fun _ => 1).sum = l.length := by
  intro l
  induction l with
  | nil => simp
  | cons z t ih =>
      simp [List.map_cons, List.sum_cons, List.length_cons, ih]
      omega

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

/--
Unique build keys bound fanout on the inner path: with a duplicate-free
non-NULL synopsis on the build side, every non-NULL probe key matches at most
one build row, so the exact product is capped by the probe-side row count.
-/
theorem sound : NullableUniqueBuildFanout.Goal := by
  intro a b hnd
  have hprod : product a b = ((a.filterMap id).map fun y => (b.filterMap id).count y).sum := by
    have h1 : product a b =
        ((frequency (a.filterMap id)).map (fun p => p.2 * List.count (some p.1) b)).sum := by
      simp [product, hist]
    have h2 : ((frequency (a.filterMap id)).map (fun p => p.2 * List.count (some p.1) b)).sum =
        ((a.filterMap id).map fun y => List.count (some y) b).sum :=
      freq_sum_weighted (g := fun y => List.count (some y) b) (a.filterMap id)
    have h3 : ((a.filterMap id).map fun y => List.count (some y) b).sum =
        ((a.filterMap id).map fun y => (b.filterMap id).count y).sum :=
      sum_congr2 (a.filterMap id) (fun y _ => bcount_some b y)
    rw [h1, h2, h3]
  have hb1 : ((a.filterMap id).map fun y => (b.filterMap id).count y).sum ≤
      ((a.filterMap id).map fun _ => 1).sum :=
    sum_le' (a.filterMap id) (fun y _ => nodup_count_le_one _ hnd y)
  rw [hprod, ← sum_one_eq_length (a.filterMap id)]
  exact hb1

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_JoinFanout_NullableUniqueBuildFanout
