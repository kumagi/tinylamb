import TinylambOpt.ResearchGoals.Atlas.RangePlans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RingNeighborhood_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.RingNeighborhood

private def out (q : Box) (g : Page) : Prop :=
  g.upper.x < q.x0 ∨ q.x1 ≤ g.lower.x ∨ g.upper.y < q.y0 ∨ q.y1 ≤ g.lower.y

private def full (q : Box) (g : Page) : Prop :=
  q.x0 ≤ g.lower.x ∧ g.upper.x < q.x1 ∧ q.y0 ≤ g.lower.y ∧ g.upper.y < q.y1

private def inBox (q : Box) (v : Point) : Prop :=
  q.x0 ≤ v.x ∧ v.x < q.x1 ∧ q.y0 ≤ v.y ∧ v.y < q.y1

private theorem uniform_cases (q : Box) (g : Page) (hx : q.x0 < q.x1) (hy : q.y0 < q.y1) :
    (uniform g q = some 0 ∧ out q g) ∨
    (uniform g q = some q.coeff ∧ full q g) ∨
    uniform g q = none := by
  have hx' : ¬ q.x1 ≤ q.x0 := by omega
  have hy' : ¬ q.y1 ≤ q.y0 := by omega
  unfold uniform
  simp only [hx', hy']
  by_cases h1 : g.upper.x < q.x0
  · exact Or.inl ⟨by simp [h1], by simp [out, h1]⟩
  · by_cases h2 : q.x1 ≤ g.lower.x
    · exact Or.inl ⟨by simp [h1, h2], by simp [out, h1, h2]⟩
    · by_cases h3 : g.upper.y < q.y0
      · exact Or.inl ⟨by simp [h1, h2, h3], by simp [out, h1, h2, h3]⟩
      · by_cases h4 : q.y1 ≤ g.lower.y
        · exact Or.inl ⟨by simp [h1, h2, h3, h4], by simp [out, h1, h2, h3, h4]⟩
        · by_cases hf1 : q.x0 ≤ g.lower.x
          · by_cases hf2 : g.upper.x < q.x1
            · by_cases hf3 : q.y0 ≤ g.lower.y
              · by_cases hf4 : g.upper.y < q.y1
                · exact Or.inr (Or.inl ⟨by simp [h1, h2, h3, h4, hf1, hf2, hf3, hf4],
                    by simp [full, hf1, hf2, hf3, hf4]⟩)
                · exact Or.inr (Or.inr (by simp [h1, h2, h3, h4, hf1, hf2, hf3, hf4]))
              · exact Or.inr (Or.inr (by simp [h1, h2, h3, h4, hf1, hf2, hf3]))
            · exact Or.inr (Or.inr (by simp [h1, h2, h3, h4, hf1, hf2]))
          · exact Or.inr (Or.inr (by simp [h1, h2, h3, h4, hf1]))

private theorem natAbs_le_iff (a : Int) (k : Nat) : a.natAbs ≤ k ↔ -(k : Int) ≤ a ∧ a ≤ k := by
  constructor
  · intro h
    rcases Int.natAbs_eq a with ha | ha
    · omega
    · omega
  · intro h
    rcases Int.natAbs_eq a with ha | ha
    · omega
    · omega

private theorem qualify_iff (p v : Point) : shape.qualifies p v = true ↔
    inBox (box (p.x - 5) (p.x + 6) (p.y - 5) (p.y + 6)) v ∧
    ¬ inBox (box (p.x - 1) (p.x + 2) (p.y - 1) (p.y + 2) (-1)) v := by
  unfold shape
  dsimp
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  unfold inBox
  simp only [box]
  let dx := (v.x - p.x).natAbs
  let dy := (v.y - p.y).natAbs
  have hle5 : max dx dy ≤ 5 ↔ dx ≤ 5 ∧ dy ≤ 5 := by exact Nat.max_le
  have hgt1 : 1 < max dx dy ↔ 1 < dx ∨ 1 < dy := by
    constructor
    · intro h
      by_cases hdx : dx ≤ dy
      · have hm : max dx dy = dy := Nat.max_eq_right hdx
        simp [hm] at h
        exact Or.inr h
      · have hdy : dy ≤ dx := by omega
        have hm : max dx dy = dx := Nat.max_eq_left hdy
        simp [hm] at h
        exact Or.inl h
    · intro h
      rcases h with h | h
      · have hm := Nat.le_max_left dx dy
        omega
      · have hm := Nat.le_max_right dx dy
        omega
  rw [hle5, hgt1]
  constructor
  · rintro ⟨hgt, hx5, hy5⟩
    constructor
    · constructor <;> omega
    · intro hin
      have hdx1 : dx ≤ 1 := (natAbs_le_iff (v.x - p.x) 1).2 (by omega)
      have hdy1 : dy ≤ 1 := (natAbs_le_iff (v.y - p.y) 1).2 (by omega)
      rcases hgt with hgt | hgt <;> omega
  · rintro ⟨hout, hin⟩
    have hx5 : dx ≤ 5 := (natAbs_le_iff (v.x - p.x) 5).2 (by omega)
    have hy5 : dy ≤ 5 := (natAbs_le_iff (v.y - p.y) 5).2 (by omega)
    refine ⟨?_, hx5, hy5⟩
    by_cases hdxgt : 1 < dx
    · exact Or.inl hdxgt
    · right
      by_cases hdygt : 1 < dy
      · exact hdygt
      · exfalso
        apply hin
        have hdx1 : dx ≤ 1 := by omega
        have hdy1 : dy ≤ 1 := by omega
        constructor <;> omega

private theorem row_in_of_full (q : Box) (g : Page) (hf : full q g)
    (r : Point × Int)
    (hrv : g.lower.x ≤ r.1.x ∧ r.1.x ≤ g.upper.x ∧ g.lower.y ≤ r.1.y ∧ r.1.y ≤ g.upper.y) :
    inBox q r.1 := by
  rcases hrv with ⟨hlx, hux, hly, huy⟩
  rcases hf with ⟨hfx0, hfx1, hfy0, hfy1⟩
  unfold inBox
  omega

private theorem row_not_in_of_out (q : Box) (g : Page) (ho : out q g)
    (r : Point × Int)
    (hrv : g.lower.x ≤ r.1.x ∧ r.1.x ≤ g.upper.x ∧ g.lower.y ≤ r.1.y ∧ r.1.y ≤ g.upper.y) :
    ¬ inBox q r.1 := by
  rcases hrv with ⟨hlx, hux, hly, huy⟩
  intro hin
  rcases hin with ⟨hix0, hix1, hiy0, hiy1⟩
  rcases ho with ho | ho | ho | ho <;> omega

private theorem filter_false (p : Point) (l : Rows)
    (h : ∀ r ∈ l, shape.qualifies p r.1 = false) :
    l.filter (fun r => shape.qualifies p r.1) = [] := by
  induction l with
  | nil => rfl
  | cons r rs ih =>
      have hr : shape.qualifies p r.1 = false := h r (by simp)
      have ht : ∀ x ∈ rs, shape.qualifies p x.1 = false := by
        intro x hx
        exact h x (by simp [hx])
      simp [hr, ih ht]

theorem sound_RingNeighborhood_PageVerdict : RingNeighborhood.PageVerdict.Goal := by
  intro b p g hv hb
  let qo : Box := box (p.x - 5) (p.x + 6) (p.y - 5) (p.y + 6)
  let qi : Box := box (p.x - 1) (p.x + 2) (p.y - 1) (p.y + 2) (-1)
  have hqox : qo.x0 < qo.x1 := by simp [qo, box]; omega
  have hqoy : qo.y0 < qo.y1 := by simp [qo, box]; omega
  have hqix : qi.x0 < qi.x1 := by simp [qi, box]; omega
  have hqiy : qi.y0 < qi.y1 := by simp [qi, box]; omega
  have hstencil : RingNeighborhood.shape.stencil b p = [qo, qi] := by rfl
  rcases uniform_cases qo g hqox hqoy with hu1 | hu1 | hu1
  · rcases hu1 with ⟨huo, ho⟩
    rcases uniform_cases qi g hqix hqiy with hu2 | hu2 | hu2
    · rcases hu2 with ⟨hui, _⟩
      have hscan : scan shape p g.rows = 0 := by
        unfold scan
        rw [filter_false p g.rows]
        · simp
        · intro r hr
          cases hval : shape.qualifies p r.1
          · rfl
          · have hq := (qualify_iff p r.1).1 hval
            exact False.elim ((row_not_in_of_out qo g ho r (hv r hr)) hq.1)
      simp [pageScan, hstencil, huo, hui, hscan]
    · rcases hu2 with ⟨hui, hfi⟩
      have hrows : g.rows = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro r hr
        have hrv := hv r hr
        rcases hrv with ⟨hlx, hux, hly, huy⟩
        have ho' : out qo g := ho
        have hfi' : full qi g := hfi
        simp [out, full, qo, qi, box] at ho' hfi'
        rcases hfi' with ⟨hfx0, hfx1, hfy0, hfy1⟩
        rcases ho' with ho' | ho' | ho' | ho' <;> omega
      have hui' : uniform g qi = some (-1) := by simpa [qi, box] using hui
      simp [pageScan, hstencil, huo, hui', hrows, scan]
    · simp [pageScan, hstencil, huo, hu2]
  · rcases hu1 with ⟨huo, hfo⟩
    rcases uniform_cases qi g hqix hqiy with hu2 | hu2 | hu2
    · rcases hu2 with ⟨hui, hoi⟩
      have hscan : scan shape p g.rows = (g.rows.map Prod.snd).sum := by
        unfold scan
        have hfilter : g.rows.filter (fun r => shape.qualifies p r.1) = g.rows :=
          (List.filter_eq_self).2 (by
            intro r hr
            exact (qualify_iff p r.1).2 ⟨row_in_of_full qo g hfo r (hv r hr),
              row_not_in_of_out qi g hoi r (hv r hr)⟩)
        simp [hfilter]
      have huo' : uniform g qo = some 1 := by simpa [qo, box] using huo
      simp [pageScan, hstencil, huo', hui, hscan]
    · rcases hu2 with ⟨hui, hfi⟩
      have hscan : scan shape p g.rows = 0 := by
        unfold scan
        rw [filter_false p g.rows]
        · simp
        · intro r hr
          cases hval : shape.qualifies p r.1
          · rfl
          · have hq := (qualify_iff p r.1).1 hval
            exact False.elim (hq.2 (row_in_of_full qi g hfi r (hv r hr)))
      have huo' : uniform g qo = some 1 := by simpa [qo, box] using huo
      have hui' : uniform g qi = some (-1) := by simpa [qi, box] using hui
      simp [pageScan, hstencil, huo', hui', hscan]
    · simp [pageScan, hstencil, huo, hu2]
  · simp [pageScan, hstencil, hu1]

end TinylambOpt.ResearchProofs.Atlas.Goal_RingNeighborhood_PageVerdict
