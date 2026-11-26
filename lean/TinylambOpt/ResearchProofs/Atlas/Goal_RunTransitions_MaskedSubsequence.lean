import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `RunTransitions.MaskedSubsequence` — `∀ x (keep : Int → Bool), measure (x.filter
  keep) ≤ measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_MaskedSubsequence

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RunTransitions

/-! Brute-force truth check on all series of length <= 3 over [-3, 3] and
    the predicates `even`, `odd`, `== 1`, `!= 1`, `== -3`. -/
#eval
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let lists : List (List Int) :=
    [[]] ++
    vals.map (fun a => [a]) ++
    vals.flatMap (fun a => vals.flatMap (fun b => [[a, b]])) ++
    vals.flatMap (fun a => vals.flatMap (fun b => vals.flatMap (fun c => [[a, b, c]])))
  let keeps : List (Int → Bool) :=
    [fun v => v % 2 == 0, fun v => v % 2 != 0, fun v => v == 1,
     fun v => v != 1, fun v => v == -3]
  (lists.flatMap (fun x => keeps.map (fun k =>
    decide (RunTransitions.measure (x.filter k) ≤ RunTransitions.measure x)))).count false

private def tc (l : Series) : Nat := (l.zip (l.drop 1)).countP (fun (a, b) => a != b)

/-- Contribution of a virtual preceding value `anc` to the transition count. -/
private def ancDiff (anc : Option Int) (a : Int) : Nat :=
  match anc with
  | none => 0
  | some b => if b != a then 1 else 0

/-- Transitions of `l`, including the edge from the leading anchor `anc`. -/
private def H (l : Series) (anc : Option Int) : Nat :=
  match l with
  | [] => 0
  | a :: r => ancDiff anc a + H r (some a)

/-- Transitions of `l.filter p`, including the edge from the leading anchor `anc`. -/
private def G (l : Series) (anc : Option Int) (p : Int → Bool) : Nat :=
  match l with
  | [] => 0
  | a :: r => if p a then ancDiff anc a + G r (some a) p else G r anc p

private theorem H_none_eq_tc : ∀ l : Series, H l none = tc l := by
  intro l
  induction l with
  | nil => simp [H, tc]
  | cons a r ih =>
      cases r with
      | nil => simp [H, tc, ancDiff]
      | cons b s =>
          have ih' : H s (some b) = tc (b :: s) := by
            simpa [H, ancDiff] using ih
          simp [H, tc, ancDiff, ih', List.countP_cons, Nat.add_comm]

private theorem G_eq_H_filter : ∀ (l : Series) (anc : Option Int) (p : Int → Bool),
    G l anc p = H (l.filter p) anc := by
  intro l
  induction l with
  | nil => intro anc p; simp [G, H]
  | cons a r ih =>
      intro anc p
      by_cases hpa : p a = true <;> simp [G, H, hpa, ih]

private theorem H_anchor_le : ∀ (r : Series) (a : Int) (anc : Option Int),
    H r anc ≤ ancDiff anc a + H r (some a) := by
  intro r a anc
  cases r with
  | nil => simp [H]
  | cons b s =>
      cases anc with
      | none =>
          simp [H, ancDiff]
      | some c =>
          by_cases hca : c = a
          · subst a
            simp [ancDiff]
          · simp [H, ancDiff, hca]
            by_cases hcb : c = b <;> by_cases hab : a = b <;> simp [hcb, hab] <;> omega

private theorem G_le_H : ∀ (l : Series) (anc : Option Int) (p : Int → Bool),
    G l anc p ≤ H l anc := by
  intro l
  induction l with
  | nil => intro anc p; simp [G, H]
  | cons a r ih =>
      intro anc p
      by_cases hpa : p a = true
      · simpa [G, H, hpa] using (ih (some a) p)
      · simp [G, H, hpa]
        exact Nat.le_trans (ih anc p) (H_anchor_le r a anc)

theorem sound_RunTransitions_MaskedSubsequence : RunTransitions.MaskedSubsequence.Goal := by
  intro x keep
  unfold RunTransitions.measure
  change (tc (x.filter keep) : Int) ≤ (tc x : Int)
  rw [← H_none_eq_tc (x.filter keep), ← G_eq_H_filter x none keep, ← H_none_eq_tc x]
  exact_mod_cast (G_le_H x none keep)

end TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_MaskedSubsequence
