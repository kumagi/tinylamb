import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AdjacentTiles_SparseNeighbors

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.AdjacentTiles

private theorem natAbs_add_one_iff (a b : Int) :
    a.natAbs + b.natAbs = 1 ↔
      (a = 0 ∧ b = 1) ∨ (a = 0 ∧ b = -1) ∨
        (a = 1 ∧ b = 0) ∨ (a = -1 ∧ b = 0) := by
  constructor
  · intro h
    have ha : a.natAbs = 0 ∨ a.natAbs = 1 := by omega
    have hb : b.natAbs = 0 ∨ b.natAbs = 1 := by omega
    rcases ha with ha | ha <;> rcases hb with hb | hb
    · simp [Int.natAbs_eq_zero.mp ha, Int.natAbs_eq_zero.mp hb] at h
    · have hb' : b = 1 ∨ b = -1 := by simpa using Int.natAbs_eq_iff.mp hb
      rcases hb' with rfl | rfl <;> simp [Int.natAbs_eq_zero.mp ha] <;> omega
    · have ha' : a = 1 ∨ a = -1 := by simpa using Int.natAbs_eq_iff.mp ha
      rcases ha' with rfl | rfl <;> simp [Int.natAbs_eq_zero.mp hb] <;> omega
    · have ha' : a = 1 ∨ a = -1 := by simpa using Int.natAbs_eq_iff.mp ha
      have hb' : b = 1 ∨ b = -1 := by simpa using Int.natAbs_eq_iff.mp hb
      omega
  · rintro (⟨rfl, rfl⟩ | ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩) <;> decide

private theorem qualifies_iff (p v : Point) :
    shape.qualifies p v = true ↔
      v = ⟨p.x - 1, p.y⟩ ∨ v = ⟨p.x + 1, p.y⟩ ∨
        v = ⟨p.x, p.y - 1⟩ ∨ v = ⟨p.x, p.y + 1⟩ := by
  simp only [shape, beq_iff_eq, natAbs_add_one_iff]
  constructor
  · cases v with
    | mk vx vy =>
      rintro (⟨hx, hy⟩ | ⟨hx, hy⟩ | ⟨hx, hy⟩ | ⟨hx, hy⟩)
      all_goals dsimp at hx hy
      · right; right; right
        simp only [Point.mk.injEq]
        constructor <;> omega
      · right; right; left
        simp only [Point.mk.injEq]
        constructor <;> omega
      · right; left
        simp only [Point.mk.injEq]
        constructor <;> omega
      · left
        simp only [Point.mk.injEq]
        constructor <;> omega
  · rintro (rfl | rfl | rfl | rfl) <;> grind

private theorem point_beq_eq (a b : Point) :
    (a == b) = decide (a.x = b.x ∧ a.y = b.y) := by
  cases a
  cases b
  rfl

private theorem point_beq_true_iff (a b : Point) : (a == b) = true ↔ a = b := by
  rw [point_beq_eq, decide_eq_true_eq]
  cases a
  cases b
  simp only [Point.mk.injEq]

private theorem point_beq_false_of_ne {a b : Point} (h : a ≠ b) : (a == b) = false := by
  apply Bool.eq_false_of_ne_true
  intro hb
  exact h (point_beq_true_iff a b |>.mp hb)

private theorem neighbor_ne (p : Point) :
    (⟨p.x - 1, p.y⟩ : Point) ≠ ⟨p.x + 1, p.y⟩ ∧
    (⟨p.x - 1, p.y⟩ : Point) ≠ ⟨p.x, p.y - 1⟩ ∧
    (⟨p.x - 1, p.y⟩ : Point) ≠ ⟨p.x, p.y + 1⟩ ∧
    (⟨p.x + 1, p.y⟩ : Point) ≠ ⟨p.x, p.y - 1⟩ ∧
    (⟨p.x + 1, p.y⟩ : Point) ≠ ⟨p.x, p.y + 1⟩ ∧
    (⟨p.x, p.y - 1⟩ : Point) ≠ ⟨p.x, p.y + 1⟩ := by
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩ <;> intro h <;>
    simp only [Point.mk.injEq] at h <;> omega

private theorem row_sparse (p : Point) (r : Point × Int) :
    (if shape.qualifies p r.1 then r.2 else 0) =
      (if r.1 == ⟨p.x - 1, p.y⟩ then r.2 else 0) +
      (if r.1 == ⟨p.x + 1, p.y⟩ then r.2 else 0) +
      (if r.1 == ⟨p.x, p.y - 1⟩ then r.2 else 0) +
      (if r.1 == ⟨p.x, p.y + 1⟩ then r.2 else 0) := by
  rcases r with ⟨v, w⟩
  rcases neighbor_ne p with ⟨n12, n13, n14, n23, n24, n34⟩
  by_cases h1 : v = ⟨p.x - 1, p.y⟩
  · subst v
    have hq : shape.qualifies p ⟨p.x - 1, p.y⟩ = true :=
      (qualifies_iff p _).mpr (Or.inl rfl)
    simp [point_beq_eq, hq, point_beq_false_of_ne n12,
      point_beq_false_of_ne n13, point_beq_false_of_ne n14]
  · have h1f := point_beq_false_of_ne h1
    by_cases h2 : v = ⟨p.x + 1, p.y⟩
    · subst v
      have hq : shape.qualifies p ⟨p.x + 1, p.y⟩ = true :=
        (qualifies_iff p _).mpr (Or.inr (Or.inl rfl))
      simp [point_beq_eq, hq, h1f, point_beq_false_of_ne n23,
        point_beq_false_of_ne n24]
    · have h2f := point_beq_false_of_ne h2
      by_cases h3 : v = ⟨p.x, p.y - 1⟩
      · subst v
        have hq : shape.qualifies p ⟨p.x, p.y - 1⟩ = true :=
          (qualifies_iff p _).mpr (Or.inr (Or.inr (Or.inl rfl)))
        simp [point_beq_eq, hq, h1f, h2f, point_beq_false_of_ne n34]
      · have h3f := point_beq_false_of_ne h3
        by_cases h4 : v = ⟨p.x, p.y + 1⟩
        · subst v
          have hq : shape.qualifies p ⟨p.x, p.y + 1⟩ = true :=
            (qualifies_iff p _).mpr (Or.inr (Or.inr (Or.inr rfl)))
          simp [point_beq_eq, hq, h1f, h2f, h3f]
        · have h4f := point_beq_false_of_ne h4
          have hq : shape.qualifies p v = false := by
            apply Bool.eq_false_of_ne_true
            intro ht
            rcases (qualifies_iff p v).mp ht with h | h | h | h
            · exact h1 h
            · exact h2 h
            · exact h3 h
            · exact h4 h
          simp [hq, h1f, h2f, h3f, h4f]

private theorem sum_filter_map_cons (r : Point × Int) (rs : Rows)
    (f : Point × Int → Bool) (g : Point × Int → Int) :
    (((r :: rs).filter f).map g).sum =
      (if f r then g r else 0) + ((rs.filter f).map g).sum := by
  by_cases h : f r = true <;> simp [h]

theorem sound_AdjacentTiles_SparseNeighbors :
    TinylambOpt.ResearchGoals.Atlas.RangePlans.AdjacentTiles.SparseNeighbors.Goal := by
  intro p rs
  dsimp only
  induction rs with
  | nil => simp [scan]
  | cons r rs ih =>
      simp only [scan] at ih ⊢
      rw [sum_filter_map_cons, sum_filter_map_cons, sum_filter_map_cons,
        sum_filter_map_cons, sum_filter_map_cons, row_sparse]
      omega

-- Alias matching the short `sound_<leaf>` naming convention.
theorem sound_SparseNeighbors : SparseNeighbors.Goal :=
  sound_AdjacentTiles_SparseNeighbors

end TinylambOpt.ResearchProofs.Atlas.Goal_AdjacentTiles_SparseNeighbors
