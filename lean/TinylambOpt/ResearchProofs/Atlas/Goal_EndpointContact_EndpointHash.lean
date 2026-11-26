import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `EndpointContact.EndpointHash` — `∀ p rs, scan shape p rs = ((rs.filter (fun r =>
  r.1.x == p.y)).map Prod.snd).sum + ((rs.filter (fun r => r.1.y == p.x)).map
  Prod.snd).sum - ((rs.filter (fun r => r.1 == ⟨p.y,p.x⟩)).map Prod.snd).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EndpointContact_EndpointHash

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.EndpointContact

/-! Finite-list algebra shared with the other `RangePlans` proofs. -/

private theorem point_beq_true_iff (a b : Point) :
    (a == b) = true ↔ a.x = b.x ∧ a.y = b.y := by
  change ((a.x == b.x) && (a.y == b.y)) = true ↔ _
  simp only [Bool.and_eq_true, beq_iff_eq]

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) :
    ((rs.filter f).map Prod.snd).sum =
      (rs.map (fun r => if f r then r.2 else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    by_cases h : f r = true <;> simp [h, ih]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x - g x)).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih] <;> omega

private theorem sum_congr {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x = g x) : (xs.map f).sum = (xs.map g).sum :=
  congrArg List.sum (List.map_congr_left h)

/-! `qualifies` is the union of the two endpoint strips; the corner
    `⟨p.y, p.x⟩` belongs to both, so the signed unit box subtracts that double
    count.  Inclusion-exclusion over each row gives the two hash probes minus
    the composite probe. -/

theorem sound_EndpointContact_EndpointHash : EndpointHash.Goal := by
  intro p rs
  rw [scan, sum_filter_eq_sum_if, sum_filter_eq_sum_if, sum_filter_eq_sum_if,
    sum_filter_eq_sum_if, ← sum_map_add, ← sum_map_sub]
  apply sum_congr
  intro r _
  rcases r with ⟨v, w⟩
  by_cases hboth : v = (⟨p.y, p.x⟩ : Point)
  · subst v
    have hb : (⟨p.y, p.x⟩ == (⟨p.y, p.x⟩ : Point)) = true :=
      (point_beq_true_iff _ _).mpr ⟨rfl, rfl⟩
    simp [shape, hb]
  · have hbf : (v == (⟨p.y, p.x⟩ : Point)) = false := by
      apply Bool.eq_false_of_ne_true
      intro hb
      rcases (point_beq_true_iff v (⟨p.y, p.x⟩ : Point)).mp hb with ⟨hxv, hyv⟩
      cases v
      subst hxv
      subst hyv
      exact hboth rfl
    by_cases hx : v.x = p.y <;> by_cases hy : v.y = p.x
    · have hv : v = (⟨p.y, p.x⟩ : Point) := by
        cases v
        subst hx
        subst hy
        rfl
      exact False.elim (hboth hv)
    · simp [shape, hx, hy, hbf] <;> omega
    · simp [shape, hx, hy, hbf] <;> omega
    · simp [shape, hx, hy, hbf] <;> omega

end TinylambOpt.ResearchProofs.Atlas.Goal_EndpointContact_EndpointHash
