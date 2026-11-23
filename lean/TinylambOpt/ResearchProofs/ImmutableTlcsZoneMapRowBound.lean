import TinylambOpt.ResearchGoals.ImmutableTlcsZoneMapRowBound

namespace TinylambOpt.ResearchProofs.ImmutableTlcsZoneMapRowBound
open ResearchGoals.ImmutableTlcsZoneMapRowBound

private theorem dtrue {p : Prop} [inst : Decidable p] (h : p) : decide p = true := by
  cases inst with
  | isTrue _ => rfl
  | isFalse hn => exact absurd h hn

/-! ## Per-row and per-block zone-map soundness -/

/-- A valid block whose row key passes the runtime filter must overlap the
    filter interval in its header, so the header-only prune cannot skip it. -/
private theorem mem_mayMatch (lo hi : Int) (b : Block) (hb : ValidBlockHeader b)
    {r : Option Int × Int} (hr : r ∈ b.rows) (hp : inFilter lo hi r.1 = true) :
    mayMatchHeader lo hi b = true := by
  rcases r with ⟨ok, v⟩
  cases ok with
  | none => simp [inFilter] at hp
  | some k =>
      have hrange : lo ≤ k ∧ k ≤ hi := by
        exact of_decide_eq_true (by simpa [inFilter] using hp)
      have hbound : b.allNull = false ∧ b.minKey ≤ k ∧ k ≤ b.maxKey := by
        simpa using hb (some k, v) hr
      exact dtrue (by
        obtain ⟨h1, h2⟩ := hrange
        obtain ⟨h3, h4, h5⟩ := hbound
        constructor
        · omega
        constructor
        · simp [h3]
        constructor
        · omega
        · omega)

/-- A pruned block contains no filtering row at all. -/
private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) (l : List α)
    (h : ∀ a ∈ l, p a = false) : l.filter p = [] := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ha : p a = false := h a (by simp)
      have hl : ∀ x ∈ l, p x = false := by
        intro x hx
        exact h x (by simp [hx])
      simp [ha, ih hl]

private theorem filter_length_le {α : Type} (p : α → Bool) (l : List α) :
    (l.filter p).length ≤ l.length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      cases h : p a <;> simp [h, ih] <;> omega

/-- The number of surviving rows of one valid block is at most its full row
    count when the header matches, and zero when the header is pruned. -/
private theorem block_bound (lo hi : Int) (b : Block) (hb : ValidBlockHeader b) :
    (b.rows.filter (fun r => inFilter lo hi r.1)).length ≤
      (if mayMatchHeader lo hi b then b.rows.length else 0) := by
  cases hm : mayMatchHeader lo hi b
  · have hnil : b.rows.filter (fun r => inFilter lo hi r.1) = [] := by
      apply filter_eq_nil_of_all_false
      intro r hr
      cases h : inFilter lo hi r.1
      · rfl
      · have hm' := mem_mayMatch lo hi b hb hr h
        rw [hm'] at hm
        cases hm
    simp [hnil]
  · simp
    exact filter_length_le (fun r => inFilter lo hi r.1) b.rows

/-! ## List-level bound -/

private theorem list_bound (lo hi : Int) :
    ∀ bs : List Block,
      (∀ b ∈ bs, ValidBlockHeader b) →
      ((bs.flatMap Block.rows).filter (fun r => inFilter lo hi r.1)).length ≤
        ((bs.filter (mayMatchHeader lo hi)).map (fun b => b.rows.length)).sum := by
  intro bs
  induction bs with
  | nil => simp
  | cons b bs ih =>
      intro hv
      have hb : ValidBlockHeader b := hv b (by simp)
      have ht : ∀ x ∈ bs, ValidBlockHeader x := by
        intro x hx
        exact hv x (by simp [hx])
      have hblock := block_bound lo hi b hb
      have htail := ih ht
      cases hm : mayMatchHeader lo hi b
      · have hb0 : (b.rows.filter (fun r => inFilter lo hi r.1)).length ≤ 0 := by
          simpa [hm] using hblock
        simp [hm]
        omega
      · have hb1 : (b.rows.filter (fun r => inFilter lo hi r.1)).length ≤ b.rows.length := by
          simpa [hm] using hblock
        simp [hm]
        omega

/-! ## Main theorem -/

theorem sound : TinylambOpt.ResearchGoals.ImmutableTlcsZoneMapRowBound.Goal := by
  constructor
  · intro lo hi bs buildFanout maxAbs exactLimit hv
    have hbound : survivingRowCount lo hi bs ≤ zmRowUpperBound lo hi bs := by
      unfold survivingRowCount zmRowUpperBound
      exact list_bound lo hi bs hv
    constructor
    · exact hbound
    · intro hlim
      unfold withinExactSumLimit at hlim ⊢
      exact Nat.le_trans (Nat.mul_le_mul_right maxAbs (Nat.mul_le_mul_right buildFanout hbound)) hlim
  · decide

end TinylambOpt.ResearchProofs.ImmutableTlcsZoneMapRowBound
