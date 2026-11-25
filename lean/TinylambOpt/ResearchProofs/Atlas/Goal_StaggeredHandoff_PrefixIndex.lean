import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_StaggeredHandoff_PrefixIndex

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.StaggeredHandoff

private def boxIn (q : Box) (v : Point) : Bool :=
  decide (q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1)

private theorem sum_filter_eq_sum_if (rs : Rows) (f : Point × Int → Bool) :
    ((rs.filter f).map Prod.snd).sum = (rs.map (fun r => if f r then r.2 else 0)).sum := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
    by_cases h : f r = true <;> simp [h, ih]

private theorem prefixMass_cons (r : Point × Int) (rs : Rows) (x y : Int) :
    prefixMass (r :: rs) x y =
      (if r.1.x < x && r.1.y < y then r.2 else 0) + prefixMass rs x y := by
  by_cases h : r.1.x < x && r.1.y < y <;> simp [prefixMass, h]

private theorem prefixMass_box (rs : Rows) (x0 x1 y0 y1 : Int) (hx : x0 < x1) (hy : y0 < y1) :
    prefixMass rs x1 y1 - prefixMass rs x0 y1 - prefixMass rs x1 y0 + prefixMass rs x0 y0 =
      ((rs.filter (fun r => boxIn (box x0 x1 y0 y1) r.1)).map Prod.snd).sum := by
  rw [sum_filter_eq_sum_if]
  simp only [boxIn, box]
  induction rs with
  | nil => simp [prefixMass]
  | cons r rs ih =>
    rw [prefixMass_cons, prefixMass_cons, prefixMass_cons, prefixMass_cons]
    simp only [List.map_cons, List.sum_cons]
    by_cases hx1 : r.1.x < x1 <;> by_cases hx0 : r.1.x < x0 <;>
      by_cases hy1 : r.1.y < y1 <;> by_cases hy0 : r.1.y < y0 <;>
      by_cases hxloP : x0 ≤ r.1.x <;> by_cases hyloP : y0 ≤ r.1.y <;>
        simp [hx1, hx0, hy1, hy0, hxloP, hyloP] <;>
        first | exact ih | omega

private theorem filter_nil_of_forall_not {α : Type} (l : List α) (f : α → Bool)
    (h : ∀ a ∈ l, ¬ f a = true) : l.filter f = [] := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    have ha : ¬ f a = true := h a List.mem_cons_self
    have has : ∀ b ∈ as, ¬ f b = true := fun b hb => h b (List.mem_cons_of_mem a hb)
    simp [ha, ih has]

theorem sound_StaggeredHandoff_PrefixIndex : PrefixIndex.Goal := by
  intro b p rs hb
  unfold indexed scan
  rw [show shape.stencil b p = [box (p.x + 1) p.y (p.y + 1) (b + 1)] by rfl]
  simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Int.add_zero]
  change corners (box (p.x + 1) p.y (p.y + 1) (b + 1)) (prefixMass rs) =
    ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).sum
  unfold corners
  split
  · rename_i hcond
    have hcbool := hcond
    simp only [Bool.and_eq_true] at hcbool
    have hc : (box (p.x + 1) p.y (p.y + 1) (b + 1)).x0 <
          (box (p.x + 1) p.y (p.y + 1) (b + 1)).x1 ∧
        (box (p.x + 1) p.y (p.y + 1) (b + 1)).y0 <
          (box (p.x + 1) p.y (p.y + 1) (b + 1)).y1 :=
      ⟨of_decide_eq_true hcbool.1, of_decide_eq_true hcbool.2⟩
    have hv' : p.x + 1 < p.y ∧ p.y + 1 < b + 1 := by
      simpa only [box] using hc
    simp only [box, Int.one_mul]
    rw [prefixMass_box rs (p.x + 1) p.y (p.y + 1) (b + 1) hv'.1 hv'.2]
    congr 1
    apply congrArg (List.map Prod.snd)
    apply List.filter_congr
    intro r hr
    have hbr := hb r hr
    simp [bounded] at hbr
    apply Bool.eq_iff_iff.mpr
    simp [boxIn, box, shape]
    constructor
    · intro h
      have hp := of_decide_eq_true h
      omega
    · intro h
      apply decide_eq_true
      omega
  · rename_i hcond
    have hv' : ¬ (p.x + 1 < p.y ∧ p.y + 1 < b + 1) := by
      intro hh
      apply hcond
      simp only [box, Bool.and_eq_true]
      exact ⟨decide_eq_true hh.1, decide_eq_true hh.2⟩
    have hn : (rs.filter (fun r => shape.qualifies p r.1)) = [] := by
      apply filter_nil_of_forall_not
      intro r hr
      intro hq
      have hbr := hb r hr
      simp [shape, bounded] at hq hbr
      omega
    simp [hn]

end TinylambOpt.ResearchProofs.Atlas.Goal_StaggeredHandoff_PrefixIndex
