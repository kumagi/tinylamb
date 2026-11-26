import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

/-!
Proof contribution for ResourcePolicies_JoinFanout_StripedHotKeyCoverage. The theorem below
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
namespace TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout.Goal_StripedHotKeyCoverage

open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout


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
        simp

private theorem sum_indicator_mul (a C : Nat) (B : List Nat) :
    (B.map (fun s => if (a == s) = true then C else 0)).sum = B.count a * C := by
  induction B with
  | nil => simp
  | cons b B ih =>
      simp only [List.map_cons, List.sum_cons, List.count_cons]
      by_cases h : (b == a) = true
      · have hab : (a == b) = true := by simpa [Bool.beq_comm] using h
        rw [if_pos h, if_pos hab, ih]
        rw [Nat.add_mul]
        omega
      · have hab : (a == b) = false := by simpa [Bool.beq_comm] using h
        have h' : ¬ (b == a) = true := h
        have hab' : ¬ (a == b) = true := by simpa using hab
        rw [if_neg h', if_neg hab', ih]
        simp

private theorem count_filter_false {α : Type} [BEq α] [LawfulBEq α] (p : α → Bool) (a : α) (l : List α)
    (hp : p a = false) : (l.filter p).count a = 0 := by
  apply List.count_eq_zero_of_not_mem
  simp [hp]

private theorem count_group_range (stripes rows i : Nat) (hu : 0 < stripes) :
    ((List.range stripes).flatMap (fun s => (List.range rows).filter (fun j => j % stripes == s))).count i =
      (List.range rows).count i := by
  rw [List.count_flatMap]
  change ((List.range stripes).map (fun s => ((List.range rows).filter (fun j => j % stripes == s)).count i)).sum =
    (List.range rows).count i
  have hmap :
      ((List.range stripes).map (fun s => ((List.range rows).filter (fun j => j % stripes == s)).count i)).sum =
        ((List.range stripes).map (fun s => if (i % stripes == s) = true then (List.range rows).count i else 0)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro s hs
    by_cases hmod : (i % stripes == s) = true
    · rw [List.count_filter (p := fun j => j % stripes == s) (a := i) (l := List.range rows) hmod]
      simp [hmod]
    · have hmodf : (i % stripes == s) = false := by simpa using hmod
      rw [count_filter_false (fun j => j % stripes == s) i _ hmodf]
      simp [hmod]
  rw [hmap]
  have hsum :
      ((List.range stripes).map (fun s => if (i % stripes == s) = true then (List.range rows).count i else 0)).sum =
        (List.range stripes).count (i % stripes) * (List.range rows).count i := by
    exact sum_indicator_mul (i % stripes) ((List.range rows).count i) (List.range stripes)
  rw [hsum, List.count_range]
  have hmodlt : i % stripes < stripes := Nat.mod_lt i hu
  simp [hmodlt]

private theorem grouped_rows_perm (rows stripes : Nat) (hu : 0 < stripes) :
    ((List.range stripes).flatMap (fun s => (List.range rows).filter (fun i => i % stripes == s))).Perm (List.range rows) := by
  apply (List.perm_iff_count).mpr
  intro i
  exact count_group_range stripes rows i hu

private theorem perm_flatMap_right {α β : Type} {l₁ l₂ : List α} (p : l₁.Perm l₂) (f : α → List β) :
    (l₁.flatMap f).Perm (l₂.flatMap f) := by
  induction p with
  | nil => simp
  | cons x p ih =>
      simpa [List.flatMap] using List.Perm.append (List.Perm.refl (f x)) ih
  | swap x y l =>
      simpa [List.flatMap, List.append_assoc] using
        (List.Perm.append (List.perm_append_comm : (f y ++ f x).Perm (f x ++ f y)) (List.Perm.refl (l.flatMap f)))
  | trans p1 p2 ih1 ih2 =>
      exact ih1.trans ih2

private theorem flatMap_flatMap {α β γ : Type} (l : List α) (f : α → List β) (g : β → List γ) :
    (l.flatMap (fun a => (f a).flatMap g)) = (l.flatMap f).flatMap g := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [List.flatMap_append, ih]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.StripedHotKeyCoverage.Goal := by
  intro rows probes stripes hu
  rw [flatMap_flatMap]
  exact perm_flatMap_right (grouped_rows_perm rows stripes hu)
    (fun i => (List.range probes).map (fun j => (i, j)))

end TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout.Goal_StripedHotKeyCoverage
