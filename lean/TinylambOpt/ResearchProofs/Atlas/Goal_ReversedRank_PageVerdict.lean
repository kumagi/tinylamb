import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `ReversedRank.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_ReversedRank_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.ReversedRank

private def d1 (b : Int) (p : Point) (g : Page) : Prop :=
  p.x ≤ -b ∨ b + 1 ≤ p.y + 1 ∨ g.upper.x < -b ∨ p.x ≤ g.lower.x ∨
    g.upper.y < p.y + 1 ∨ b + 1 ≤ g.lower.y

private def f1 (b : Int) (p : Point) (g : Page) : Prop :=
  -b ≤ g.lower.x ∧ g.upper.x < p.x ∧ p.y + 1 ≤ g.lower.y ∧ g.upper.y < b + 1

private def d2 (b : Int) (p : Point) (g : Page) : Prop :=
  b + 1 ≤ p.x + 1 ∨ p.y ≤ -b ∨ g.upper.x < p.x + 1 ∨ b + 1 ≤ g.lower.x ∨
    g.upper.y < -b ∨ p.y ≤ g.lower.y

private def f2 (b : Int) (p : Point) (g : Page) : Prop :=
  p.x + 1 ≤ g.lower.x ∧ g.upper.x < b + 1 ∧ -b ≤ g.lower.y ∧ g.upper.y < p.y

private theorem uniform_b1_cases (b : Int) (p : Point) (g : Page) :
    (uniform g (box (-b) p.x (p.y + 1) (b + 1)) = some 0 ∧ d1 b p g) ∨
    (uniform g (box (-b) p.x (p.y + 1) (b + 1)) = some 1 ∧ f1 b p g) ∨
    (uniform g (box (-b) p.x (p.y + 1) (b + 1)) = none) := by
  by_cases ha : p.x ≤ -b
  · exact Or.inl ⟨by simp [uniform, box, ha], by simp [d1, ha]⟩
  · by_cases hb' : b + 1 ≤ p.y + 1
    · exact Or.inl ⟨by simp [uniform, box, ha, hb'], by simp [d1, ha, hb']⟩
    · by_cases hc : g.upper.x < -b
      · exact Or.inl ⟨by simp [uniform, box, ha, hb', hc], by simp [d1, ha, hb', hc]⟩
      · by_cases hd : p.x ≤ g.lower.x
        · exact Or.inl ⟨by simp [uniform, box, ha, hb', hc, hd], by simp [d1, ha, hb', hc, hd]⟩
        · by_cases he : g.upper.y < p.y + 1
          · exact Or.inl ⟨by simp [uniform, box, ha, hb', hc, hd, he], by simp [d1, ha, hb', hc, hd, he]⟩
          · by_cases hf : b + 1 ≤ g.lower.y
            · exact Or.inl ⟨by simp [uniform, box, ha, hb', hc, hd, he, hf], by simp [d1, ha, hb', hc, hd, he, hf]⟩
            · by_cases hfa : -b ≤ g.lower.x
              · by_cases hfb : g.upper.x < p.x
                · by_cases hfc : p.y + 1 ≤ g.lower.y
                  · by_cases hfd : g.upper.y < b + 1
                    · exact Or.inr (Or.inl ⟨by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa, hfb, hfc, hfd],
                        by simp [f1, hfa, hfb, hfc, hfd]⟩)
                    · exact Or.inr (Or.inr (by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa, hfb, hfc, hfd]))
                  · exact Or.inr (Or.inr (by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa, hfb, hfc]))
                · exact Or.inr (Or.inr (by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa, hfb]))
              · exact Or.inr (Or.inr (by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa]))

private theorem uniform_b2_cases (b : Int) (p : Point) (g : Page) :
    (uniform g (box (p.x + 1) (b + 1) (-b) p.y) = some 0 ∧ d2 b p g) ∨
    (uniform g (box (p.x + 1) (b + 1) (-b) p.y) = some 1 ∧ f2 b p g) ∨
    (uniform g (box (p.x + 1) (b + 1) (-b) p.y) = none) := by
  by_cases ha : b + 1 ≤ p.x + 1
  · exact Or.inl ⟨by simp [uniform, box, ha], by simp [d2, ha]⟩
  · by_cases hb' : p.y ≤ -b
    · exact Or.inl ⟨by simp [uniform, box, ha, hb'], by simp [d2, ha, hb']⟩
    · by_cases hc : g.upper.x < p.x + 1
      · exact Or.inl ⟨by simp [uniform, box, ha, hb', hc], by simp [d2, ha, hb', hc]⟩
      · by_cases hd : b + 1 ≤ g.lower.x
        · exact Or.inl ⟨by simp [uniform, box, ha, hb', hc, hd], by simp [d2, ha, hb', hc, hd]⟩
        · by_cases he : g.upper.y < -b
          · exact Or.inl ⟨by simp [uniform, box, ha, hb', hc, hd, he], by simp [d2, ha, hb', hc, hd, he]⟩
          · by_cases hf : p.y ≤ g.lower.y
            · exact Or.inl ⟨by simp [uniform, box, ha, hb', hc, hd, he, hf], by simp [d2, ha, hb', hc, hd, he, hf]⟩
            · by_cases hfa : p.x + 1 ≤ g.lower.x
              · by_cases hfb : g.upper.x < b + 1
                · by_cases hfc : -b ≤ g.lower.y
                  · by_cases hfd : g.upper.y < p.y
                    · exact Or.inr (Or.inl ⟨by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa, hfb, hfc, hfd],
                        by simp [f2, hfa, hfb, hfc, hfd]⟩)
                    · exact Or.inr (Or.inr (by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa, hfb, hfc, hfd]))
                  · exact Or.inr (Or.inr (by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa, hfb, hfc]))
                · exact Or.inr (Or.inr (by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa, hfb]))
              · exact Or.inr (Or.inr (by simp [uniform, box, ha, hb', hc, hd, he, hf, hfa]))

private theorem filter_all_false (l : List (Point × Int))
    (h : ∀ r ∈ l, shape.qualifies p r.1 = false) :
    l.filter (fun r => shape.qualifies p r.1) = [] := by
  induction l with
  | nil => rfl
  | cons r rs ih =>
      have hr : shape.qualifies p r.1 = false := h r (by simp)
      have htail : ∀ x ∈ rs, shape.qualifies p x.1 = false := by
        intro x hx
        exact h x (by simp [hx])
      simp [hr, ih htail]

private theorem nw_false (b : Int) (p : Point) (g : Page) (hD1 : d1 b p g)
    (r : Point × Int) (hrv : g.lower.x ≤ r.1.x ∧ r.1.x ≤ g.upper.x ∧
      g.lower.y ≤ r.1.y ∧ r.1.y ≤ g.upper.y)
    (hrb : bounded b r.1) :
    (decide (r.1.x < p.x) && decide (p.y < r.1.y)) = false := by
  rcases hrv with ⟨hlx, hux, hly, huy⟩
  rcases hrb with ⟨bx0, bx1, by0, by1⟩
  rcases hD1 with ha | hb' | hc | hd | he | hf
  · have hn : ¬ r.1.x < p.x := by omega
    simp [hn]
  · have hn : ¬ p.y < r.1.y := by omega
    simp [hn]
  · exfalso
    omega
  · have hn : ¬ r.1.x < p.x := by omega
    simp [hn]
  · have hn : ¬ p.y < r.1.y := by omega
    simp [hn]
  · exfalso
    omega

private theorem se_false (b : Int) (p : Point) (g : Page) (hD2 : d2 b p g)
    (r : Point × Int) (hrv : g.lower.x ≤ r.1.x ∧ r.1.x ≤ g.upper.x ∧
      g.lower.y ≤ r.1.y ∧ r.1.y ≤ g.upper.y)
    (hrb : bounded b r.1) :
    (decide (p.x < r.1.x) && decide (r.1.y < p.y)) = false := by
  rcases hrv with ⟨hlx, hux, hly, huy⟩
  rcases hrb with ⟨bx0, bx1, by0, by1⟩
  rcases hD2 with ha | hb' | hc | hd | he | hf
  · have hn : ¬ p.x < r.1.x := by omega
    simp [hn]
  · have hn : ¬ r.1.y < p.y := by omega
    simp [hn]
  · have hn : ¬ p.x < r.1.x := by omega
    simp [hn]
  · exfalso
    omega
  · exfalso
    omega
  · have hn : ¬ r.1.y < p.y := by omega
    simp [hn]

theorem sound_ReversedRank_PageVerdict : ReversedRank.PageVerdict.Goal := by
  intro b p g hv hb
  unfold pageScan
  rcases uniform_b1_cases b p g with h1 | h1 | hu1n
  · rcases h1 with ⟨hu10, hD1⟩
    rcases uniform_b2_cases b p g with h2 | h2 | hu2n
    · rcases h2 with ⟨hu20, hD2⟩
      have hall : ∀ r ∈ g.rows, shape.qualifies p r.1 = false := by
        intro r hr
        have hrv := hv r hr
        have hrb := hb r hr
        have hnw := nw_false b p g hD1 r hrv hrb
        have hse := se_false b p g hD2 r hrv hrb
        simp [ReversedRank.shape, hnw, hse]
      have hfilter : g.rows.filter (fun r => shape.qualifies p r.1) = [] :=
        filter_all_false g.rows hall
      have hfilter' : g.rows.filter (fun r =>
          decide (r.1.x < p.x) && decide (p.y < r.1.y) ||
            decide (p.x < r.1.x) && decide (r.1.y < p.y)) = [] := by
        simpa [ReversedRank.shape] using hfilter
      simp [scan, ReversedRank.shape, hu10, hu20, hfilter']
    · rcases h2 with ⟨hu21, hF2⟩
      have hall : ∀ r ∈ g.rows, shape.qualifies p r.1 = true := by
        intro r hr
        have hrv := hv r hr
        rcases hrv with ⟨_, hux, _, huy⟩
        rcases hF2 with ⟨_, _, _, hfy⟩
        have hx : p.x < r.1.x := by omega
        have hy : r.1.y < p.y := by omega
        simp [ReversedRank.shape, hx, hy]
      have hfilter : g.rows.filter (fun r => shape.qualifies p r.1) = g.rows :=
        (List.filter_eq_self).2 hall
      have hfilter' : g.rows.filter (fun r =>
          decide (r.1.x < p.x) && decide (p.y < r.1.y) ||
            decide (p.x < r.1.x) && decide (r.1.y < p.y)) = g.rows := by
        simpa [ReversedRank.shape] using hfilter
      simp [scan, ReversedRank.shape, hu10, hu21, hfilter']
    · simp [ReversedRank.shape, hu10, hu2n]
  · rcases h1 with ⟨hu11, hF1⟩
    rcases uniform_b2_cases b p g with h2 | h2 | hu2n
    · rcases h2 with ⟨hu20, hD2⟩
      have hall : ∀ r ∈ g.rows, shape.qualifies p r.1 = true := by
        intro r hr
        have hrv := hv r hr
        rcases hrv with ⟨hlx, _, hly, _⟩
        rcases hF1 with ⟨_, hfx, _, _⟩
        have hx : r.1.x < p.x := by omega
        have hy : p.y < r.1.y := by omega
        simp [ReversedRank.shape, hx, hy]
      have hfilter : g.rows.filter (fun r => shape.qualifies p r.1) = g.rows :=
        (List.filter_eq_self).2 hall
      have hfilter' : g.rows.filter (fun r =>
          decide (r.1.x < p.x) && decide (p.y < r.1.y) ||
            decide (p.x < r.1.x) && decide (r.1.y < p.y)) = g.rows := by
        simpa [ReversedRank.shape] using hfilter
      simp [scan, ReversedRank.shape, hu11, hu20, hfilter']
    · rcases h2 with ⟨hu21, hF2⟩
      have hrows : g.rows = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro r hr
        have hrv := hv r hr
        rcases hrv with ⟨hlx, hux, _, _⟩
        rcases hF1 with ⟨_, hfx, _, _⟩
        rcases hF2 with ⟨hfx2, _, _, _⟩
        omega
      simp [scan, ReversedRank.shape, hu11, hu21, hrows]
    · simp [ReversedRank.shape, hu11, hu2n]
  · simp [ReversedRank.shape, hu1n]

end TinylambOpt.ResearchProofs.Atlas.Goal_ReversedRank_PageVerdict
