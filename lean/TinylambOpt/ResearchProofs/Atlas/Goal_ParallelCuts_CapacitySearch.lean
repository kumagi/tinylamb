import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `ParallelCuts.CapacitySearch` — `∀ a b xs, a ≤ b → (∀ w ∈ xs, w ≤ a) → required b xs
  ≤ required a xs`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ParallelCuts_CapacitySearch

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.ParallelCuts

/-! 小さい入力で真偽を総当たり確認する。長さ 0..4 の Nat 列と cap 0..5 の組を全部試す。 -/
def vecs : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => (List.range 5).flatMap (fun w => (vecs n).map (fun xs => w :: xs))

#eval! (vecs 4).all (fun xs =>
  (List.range 6).all (fun a =>
    (List.range 6).all (fun b =>
      decide (a ≤ b → (∀ w ∈ xs, w ≤ a) → required b xs ≤ required a xs))))

/-! `pack` 一段の遷移。 -/
private def step (cap : Nat) (s : Nat × Nat) (w : Nat) : Nat × Nat :=
  if s.2 + w ≤ cap then (s.1, s.2 + w) else (s.1 + 1, w)

private theorem pack_eq_foldl (cap : Nat) (xs : List Nat) :
    pack cap xs = xs.foldl (step cap) (0, 0) := by
  rfl

/-! cap を大きくすると (現在の群数, 現在の群の重み) が
    「群数が少ない、または群数が同じなら残り余裕が大きい」という順序で悪化しない。 -/
private theorem step_mono (a b w ka sa kb sb : Nat)
    (hab : a ≤ b) (hw : w ≤ a) (_hsa : sa ≤ a) (hsb : sb ≤ b)
    (hk : kb ≤ ka) (hgap : kb = ka → a - sa ≤ b - sb) :
    let A' := step a (ka, sa) w
    let B' := step b (kb, sb) w
    A'.2 ≤ a ∧ B'.2 ≤ b ∧ B'.1 ≤ A'.1 ∧ (B'.1 = A'.1 → a - A'.2 ≤ b - B'.2) := by
  dsimp [step]
  split <;> split <;> simp_all <;> omega

private theorem pack_mono_aux (a b : Nat) (xs : List Nat) (hab : a ≤ b)
    (hw : ∀ w ∈ xs, w ≤ a) (ka sa kb sb : Nat)
    (hsa : sa ≤ a) (hsb : sb ≤ b)
    (hk : kb ≤ ka) (hgap : kb = ka → a - sa ≤ b - sb) :
    let A := xs.foldl (step a) (ka, sa)
    let B := xs.foldl (step b) (kb, sb)
    B.1 ≤ A.1 ∧ (B.1 = A.1 → a - A.2 ≤ b - B.2) := by
  induction xs generalizing ka sa kb sb with
  | nil =>
      dsimp
      exact ⟨hk, hgap⟩
  | cons w ws ih =>
      simp only [List.foldl_cons]
      have hwmem : w ≤ a := hw w (by simp)
      have hws : ∀ x ∈ ws, x ≤ a := by
        intro x hx
        exact hw x (by simp [hx])
      have hs := step_mono a b w ka sa kb sb hab hwmem hsa hsb hk hgap
      dsimp [step] at hs
      rcases hs with ⟨hsa', hsb', hk', hgap'⟩
      exact ih hws (step a (ka, sa) w).1 (step a (ka, sa) w).2
        (step b (kb, sb) w).1 (step b (kb, sb) w).2 hsa' hsb' hk' hgap'

theorem sound_ParallelCuts_CapacitySearch : ParallelCuts.CapacitySearch.Goal := by
  intro a b xs hab hw
  by_cases hempty : xs.isEmpty
  · simp [required, hempty]
  · have hm := pack_mono_aux a b xs hab hw 0 0 0 0 (by omega) (by omega) (by omega)
      (by intro h; omega)
    simp [required, hempty]
    rw [pack_eq_foldl, pack_eq_foldl]
    have hbins : (xs.foldl (step b) (0, 0)).1 ≤ (xs.foldl (step a) (0, 0)).1 := by
      simpa using hm.1
    omega

end TinylambOpt.ResearchProofs.Atlas.Goal_ParallelCuts_CapacitySearch
