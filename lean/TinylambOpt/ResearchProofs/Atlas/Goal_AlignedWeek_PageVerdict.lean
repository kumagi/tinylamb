import TinylambOpt.ResearchGoals.Atlas.RangePlans

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.

Proved

- `AlignedWeek.PageVerdict` — `∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
  pageScan shape b p g = scan shape p g.rows`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_AlignedWeek_PageVerdict

open TinylambOpt.ResearchGoals.Atlas.RangePlans
open TinylambOpt.ResearchGoals.Atlas.RangePlans.AlignedWeek

private def inBox (q : Box) (r : Point) : Bool :=
  q.x0 ≤ r.x && r.x < q.x1 && q.y0 ≤ r.y && r.y < q.y1

private theorem inBox_iff (q : Box) (r : Point) :
    inBox q r = true ↔ q.x0 ≤ r.x ∧ r.x < q.x1 ∧ q.y0 ≤ r.y ∧ r.y < q.y1 := by
  unfold inBox
  simp only [Bool.and_eq_true]
  constructor
  · intro h
    exact ⟨of_decide_eq_true h.1.1.1, of_decide_eq_true h.1.1.2,
      of_decide_eq_true h.1.2, of_decide_eq_true h.2⟩
  · intro h
    exact And.intro (And.intro (And.intro (decide_eq_true h.1) (decide_eq_true h.2.1))
      (decide_eq_true h.2.2.1)) (decide_eq_true h.2.2.2)

private theorem sum_map_add {β : Type} (bs : List β) (f g : β → Int) :
    (bs.map (fun b => f b + g b)).sum = (bs.map f).sum + (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [ih]; omega

private theorem sum_map_zero {β : Type} (bs : List β) : (bs.map (fun _ => (0 : Int))).sum = 0 := by
  induction bs with
  | nil => rfl
  | cons b bs ih => simp [ih]

private theorem sum_map_left_mul (rs : Rows) (c : Int) :
    (rs.map (fun r => c * r.2)).sum = c * (rs.map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih => simp [ih, Int.mul_add]

private theorem uniform_weight (g : Page) (q : Box) (hg : valid g) :
    match uniform g q with
    | some c => ∀ r ∈ g.rows, c = if inBox q r.1 then q.coeff else 0
    | none => True := by
  cases hu : uniform g q with
  | none => trivial
  | some c =>
      unfold uniform at hu
      split at hu
      · rename_i hdisjoint
        simp only [Option.some.injEq] at hu
        subst c
        intro r hr
        have hv := hg r hr
        simp only [Bool.or_eq_true, decide_eq_true_eq] at hdisjoint
        by_cases hi : q.x0 ≤ r.1.x ∧ r.1.x < q.x1 ∧ q.y0 ≤ r.1.y ∧ r.1.y < q.y1
        · rw [ite_eq_left (by simpa [inBox, and_assoc, decide_eq_true_eq] using hi)]
          omega
        · rw [ite_eq_right (by simpa [inBox, and_assoc, decide_eq_true_eq] using hi)]
      · split at hu
        · rename_i hcover
          simp only [Option.some.injEq] at hu
          subst c
          intro r hr
          have hv := hg r hr
          simp only [Bool.and_eq_true, decide_eq_true_eq] at hcover
          have hi : q.x0 ≤ r.1.x ∧ r.1.x < q.x1 ∧ q.y0 ≤ r.1.y ∧ r.1.y < q.y1 := by
            omega
          rw [ite_eq_left (by simpa [inBox, and_assoc, decide_eq_true_eq] using hi)]
        · simp at hu

private theorem page_mass (g : Page) (st : List Box) (hg : valid g) :
    ∀ ws, st.mapM (uniform g) = some ws →
      ws.sum * (g.rows.map Prod.snd).sum =
        (g.rows.map (fun r => ((st.map (fun q => if inBox q r.1 then q.coeff else 0)).sum * r.2))).sum := by
  induction st with
  | nil =>
      intro ws hws
      simp at hws
      subst ws
      simp [sum_map_zero]
  | cons q qs ih =>
      intro ws hws
      cases hu : uniform g q with
      | none =>
          simp [List.mapM_cons, hu] at hws
      | some c =>
          cases hm : qs.mapM (uniform g) with
          | none => simp [List.mapM_cons, hu, hm] at hws
          | some vs =>
              simp [List.mapM_cons, hu, hm] at hws
              subst ws
              have hw0 := uniform_weight g q hg
              rw [hu] at hw0
              have hw : ∀ r ∈ g.rows, c = if inBox q r.1 then q.coeff else 0 := hw0
              have hq : c * (g.rows.map Prod.snd).sum =
                  (g.rows.map (fun r => (if inBox q r.1 then q.coeff else 0) * r.2)).sum := by
                rw [← sum_map_left_mul g.rows c]
                apply congrArg List.sum
                apply List.map_congr_left
                intro r hr
                rw [hw r hr]
              have ht := ih vs hm
              simp only [List.map_cons, List.sum_cons]
              rw [Int.add_mul, hq, ht, ← sum_map_add]
              apply congrArg List.sum
              apply List.map_congr_left
              intro r hr
              rw [Int.add_mul]

private def stencilWeight (s : Shape) (b : Int) (p : Point) (r : Point) : Int :=
  ((s.stencil b p).map (fun q => if inBox q r then q.coeff else 0)).sum

private theorem if_one_mul (rs : Rows) (f : Point × Int → Bool) :
    (rs.map (fun r => (if f r then (1 : Int) else 0) * r.2)).sum =
      ((rs.filter f).map Prod.snd).sum := by
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases h : f r = true <;> simp [h, ih] <;> omega

private theorem pageScan_of_stencil (s : Shape) (b : Int) (p : Point) (g : Page)
    (hg : valid g) (_hb : ∀ r ∈ g.rows, bounded b r.1)
    (hst : ∀ r ∈ g.rows, stencilWeight s b p r.1 = if s.qualifies p r.1 then (1 : Int) else 0) :
    pageScan s b p g = scan s p g.rows := by
  unfold pageScan
  cases hm : (s.stencil b p).mapM (uniform g) with
  | none => rfl
  | some ws =>
      change ws.sum * (g.rows.map Prod.snd).sum = scan s p g.rows
      rw [page_mass g (s.stencil b p) hg ws hm]
      rw [show (g.rows.map (fun r => ((s.stencil b p).map (fun q => if inBox q r.1 then q.coeff else 0)).sum * r.2)) =
          (g.rows.map (fun r => (if s.qualifies p r.1 then (1 : Int) else 0) * r.2)) by
        apply List.map_congr_left
        intro r hr
        rw [← stencilWeight, hst r hr]]
      exact if_one_mul g.rows (fun r => s.qualifies p r.1)

-- Euclidean division by a positive divisor: v / d = k exactly when v lies in
-- the half-open bucket [d*k, d*k+d).  Int.ediv_mul_add_emod supplies the
-- decomposition v = (v/d)*d + (v%d) with 0 ≤ v%d < d, so omega closes both
-- directions when d is the literal 7 or 10.
private theorem div7_eq (k v : Int) :
    v / 7 = k ↔ 7 * k ≤ v ∧ v < 7 * k + 7 := by
  have hdecomp : v / 7 * 7 + v % 7 = v := Int.ediv_mul_add_emod v 7
  have h7 : (7 : Int) ≠ 0 := by omega
  have hr0 : 0 ≤ v % 7 := Int.emod_nonneg v h7
  have hrd : v % 7 < 7 := Int.emod_lt_of_pos v (by omega)
  constructor <;> intro h <;> omega

private theorem div10_eq (k v : Int) :
    v / 10 = k ↔ 10 * k ≤ v ∧ v < 10 * k + 10 := by
  have hdecomp : v / 10 * 10 + v % 10 = v := Int.ediv_mul_add_emod v 10
  have h10 : (10 : Int) ≠ 0 := by omega
  have hr0 : 0 ≤ v % 10 := Int.emod_nonneg v h10
  have hrd : v % 10 < 10 := Int.emod_lt_of_pos v (by omega)
  constructor <;> intro h <;> omega

private theorem stencil_id (b : Int) (p v : Point) (_hv : bounded b v) :
    ((shape.stencil b p).map (fun q => if inBox q v then q.coeff else 0)).sum =
      if shape.qualifies p v then (1 : Int) else 0 := by
  simp [shape, box, inBox]
  by_cases h7b : 7 * (p.x / 7) ≤ v.x ∧ v.x < 7 * (p.x / 7) + 7
  · by_cases h10b : 10 * (p.y / 10) ≤ v.y ∧ v.y < 10 * (p.y / 10) + 10
    · have hd7 : v.x / 7 = p.x / 7 := (div7_eq (p.x / 7) v.x).2 h7b
      have hd10 : v.y / 10 = p.y / 10 := (div10_eq (p.y / 10) v.y).2 h10b
      simp [h7b, h10b, hd7, hd10]
    · have hd10 : v.y / 10 ≠ p.y / 10 := by
        intro h
        exact h10b ((div10_eq (p.y / 10) v.y).1 h)
      simp [h7b, h10b, hd10]
  · by_cases h10b : 10 * (p.y / 10) ≤ v.y ∧ v.y < 10 * (p.y / 10) + 10
    · have hd7 : v.x / 7 ≠ p.x / 7 := by
        intro h
        exact h7b ((div7_eq (p.x / 7) v.x).1 h)
      simp [h7b, h10b, hd7]
    · have hd7 : v.x / 7 ≠ p.x / 7 := by
        intro h
        exact h7b ((div7_eq (p.x / 7) v.x).1 h)
      have hd10 : v.y / 10 ≠ p.y / 10 := by
        intro h
        exact h10b ((div10_eq (p.y / 10) v.y).1 h)
      simp [h7b, hd7, hd10]

theorem sound_AlignedWeek_PageVerdict : AlignedWeek.PageVerdict.Goal := by
  intro b p g hg hb
  exact pageScan_of_stencil shape b p g hg hb (fun r hr => stencil_id b p r.1 (hb r hr))

end TinylambOpt.ResearchProofs.Atlas.Goal_AlignedWeek_PageVerdict
