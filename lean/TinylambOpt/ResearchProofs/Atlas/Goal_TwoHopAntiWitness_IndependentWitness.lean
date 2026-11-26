import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `TwoHopAntiWitness.IndependentWitness` — `∀ n db b, (∀ r i j, 0 ≤ db r i j) → (0 <
  reference pattern n db b ↔ ∃ z < n, 0 < bridge n db 0 1 (b 0) z ∧ 0 < bridge n db 2
  1 (b 0) z ∧ 0 < total n (db 0 z))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TwoHopAntiWitness_IndependentWitness

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness
open TinylambOpt.ResearchGoals.Frontiers.Contractions (total)

/-! Finite sums over `List.range n`, used by `reference`, `total` and `bridge`.
All entries are nonnegative under the goal's hypothesis, so positivity of a
sum is equivalent to a positive summand. -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem sum_map_congr {α} (l : List α) (f g : α → Int)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (fun b => F a b)).sum)).sum =
      (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l with
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      exact (sum_zero l').symm
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, sum_add]

private theorem rsum_succ (n : Nat) (f : Nat → Int) :
    rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int}
    (h : ∀ i, i < n → f i = g i) : rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)),
        h n (Nat.lt_succ_self n)]

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  exact sum_mul_left (List.range n) c f

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  exact sum_mul_right (List.range n) f c

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) =
      rsum m (fun j => rsum n (fun i => f i j)) := by
  exact sum_swap (List.range n) (List.range m) f

private theorem rsum_swap34 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun i => rsum n (fun j => rsum n (fun k => rsum n (fun l => f i j k l)))) =
      rsum n (fun i => rsum n (fun j => rsum n (fun l => rsum n (fun k => f i j k l)))) := by
  apply rsum_congr
  intro i hi
  apply rsum_congr
  intro j hj
  exact rsum_comm n n (fun k l => f i j k l)

private theorem rsum_prod3 (n m k : Nat) (f g h : Nat → Int) :
    rsum n (fun i => rsum m (fun j => rsum k (fun l => f i * g j * h l))) =
      rsum n f * rsum m g * rsum k h := by
  calc
    rsum n (fun i => rsum m (fun j => rsum k (fun l => f i * g j * h l)))
        = rsum n (fun i => rsum m (fun j => (f i * g j) * rsum k h)) := by
            apply rsum_congr; intro i hi
            apply rsum_congr; intro j hj
            rw [rsum_const_mul]
    _ = rsum n (fun i => (rsum m (fun j => f i * g j)) * rsum k h) := by
            apply rsum_congr; intro i hi
            rw [rsum_mul_const]
    _ = rsum n (fun i => (f i * rsum m g) * rsum k h) := by
            apply rsum_congr; intro i hi
            rw [rsum_const_mul]
    _ = rsum n (fun i => f i * rsum m g) * rsum k h := rsum_mul_const n _ _
    _ = (rsum n f * rsum m g) * rsum k h := by rw [rsum_mul_const]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int)
    (h : ∀ i, i < n → 0 ≤ f i) : 0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_pos_iff (n : Nat) (f : Nat → Int)
    (h : ∀ i, i < n → 0 ≤ f i) :
    0 < rsum n f ↔ ∃ i, i < n ∧ 0 < f i := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have htail := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      constructor
      · intro hp
        have hnon := h n (Nat.lt_succ_self n)
        by_cases hn : 0 < f n
        · exact ⟨n, Nat.lt_succ_self n, hn⟩
        · have hz : f n = 0 := by omega
          have hp' : 0 < rsum n f := by simpa [hz] using hp
          rcases htail.mp hp' with ⟨i, hi, hfi⟩
          exact ⟨i, Nat.lt_succ_of_lt hi, hfi⟩
      · rintro ⟨i, hi, hfi⟩
        have hlast := h n (Nat.lt_succ_self n)
        have hr := rsum_nonneg n f (fun j hj => h j (Nat.lt_succ_of_lt hj))
        rcases Nat.lt_succ_iff_lt_or_eq.mp hi with hi | rfl
        · have hp := htail.mpr ⟨i, hi, hfi⟩
          omega
        · omega

private theorem int_mul_pos_of_nonneg {a b : Int} (ha : 0 ≤ a) (hb : 0 ≤ b) :
    0 < a * b ↔ 0 < a ∧ 0 < b := by
  constructor
  · intro h
    by_cases hza : a = 0
    · simp [hza] at h
    · by_cases hzb : b = 0
      · simp [hzb] at h
      · exact ⟨by omega, by omega⟩
  · rintro ⟨ha', hb'⟩
    by_cases hza : a = 0
    · simp [hza] at ha'
    · by_cases hzb : b = 0
      · simp [hzb] at hb'
      · exact Int.mul_pos ha' hb'

/-! The five-atom join factors as a product of two two-hop bridge masses and
an outgoing degree, with the shared endpoint `z` as the only summation key. -/

private theorem twohop_reference_factor (n : Nat) (db : DB) (b : Bind) :
    reference pattern n db b =
      rsum n (fun z => bridge n db 0 1 (b 0) z * bridge n db 2 1 (b 0) z *
        total n (db 0 z)) := by
  simp [reference, integrate, pattern, value, put, bridge, total]
  change rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x2 =>
    db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4))))))) =
    rsum n (fun z => (rsum n (fun k => db 0 (b 0) k * db 1 k z)) *
      (rsum n (fun k => db 2 (b 0) k * db 1 k z)) * rsum n (db 0 z))
  calc
    rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 => rsum n (fun x2 =>
        db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4)))))))
        = rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x4 =>
            db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4))))))) :=
          rsum_swap34 n _
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
          db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4))))))) := by
          apply rsum_congr; intro x1 hx1
          exact rsum_comm n n (fun x3 x2 => rsum n (fun x4 =>
            db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4)))))
    _ = rsum n (fun x2 => rsum n (fun x1 => rsum n (fun x3 => rsum n (fun x4 =>
          db 0 (b 0) x1 * (db 1 x1 x2 * (db 2 (b 0) x3 * (db 1 x3 x2 * db 0 x2 x4))))))) :=
          rsum_comm n n _
    _ = rsum n (fun z => (rsum n (fun x1 => db 0 (b 0) x1 * db 1 x1 z)) *
          (rsum n (fun x3 => db 2 (b 0) x3 * db 1 x3 z)) * rsum n (db 0 z)) := by
          apply rsum_congr
          intro z hz
          simpa only [Int.mul_assoc] using rsum_prod3 n n n
            (fun x1 => db 0 (b 0) x1 * db 1 x1 z)
            (fun x3 => db 2 (b 0) x3 * db 1 x3 z) (db 0 z)

theorem sound_TwoHopAntiWitness_IndependentWitness : IndependentWitness.Goal := by
  intro n db b hnonneg
  rw [twohop_reference_factor]
  change 0 < rsum n (fun z =>
    (rsum n (fun k => db 0 (b 0) k * db 1 k z)) *
      (rsum n (fun k => db 2 (b 0) k * db 1 k z)) * rsum n (db 0 z)) ↔
    ∃ z, z < n ∧ 0 < rsum n (fun k => db 0 (b 0) k * db 1 k z) ∧
      0 < rsum n (fun k => db 2 (b 0) k * db 1 k z) ∧ 0 < rsum n (db 0 z)
  rw [rsum_pos_iff]
  · constructor
    · rintro ⟨z, hz, hp⟩
      have hpos := int_mul_pos_of_nonneg
        (a := rsum n (fun i => db 0 (b 0) i * db 1 i z) *
          rsum n (fun i => db 2 (b 0) i * db 1 i z))
        (b := total n (db 0 z))
        (Int.mul_nonneg (rsum_nonneg n _ (fun i hi =>
          Int.mul_nonneg (hnonneg 0 (b 0) i) (hnonneg 1 i z)))
          (rsum_nonneg n _ (fun i hi =>
            Int.mul_nonneg (hnonneg 2 (b 0) i) (hnonneg 1 i z))))
        (rsum_nonneg n (db 0 z) (fun i hi => hnonneg 0 z i))
      have h1 := hpos.mp hp
      have hb := int_mul_pos_of_nonneg
        (rsum_nonneg n _ (fun i hi => Int.mul_nonneg (hnonneg 0 (b 0) i) (hnonneg 1 i z)))
        (rsum_nonneg n _ (fun i hi => Int.mul_nonneg (hnonneg 2 (b 0) i) (hnonneg 1 i z)))
      have hbridges := hb.mp (And.left h1)
      exact ⟨z, hz, And.left hbridges, And.right hbridges, And.right h1⟩
    · rintro ⟨z, hz, h1, h2, h3⟩
      have hb := int_mul_pos_of_nonneg
        (rsum_nonneg n _ (fun i hi => Int.mul_nonneg (hnonneg 0 (b 0) i) (hnonneg 1 i z)))
        (rsum_nonneg n _ (fun i hi => Int.mul_nonneg (hnonneg 2 (b 0) i) (hnonneg 1 i z)))
      have hpos := int_mul_pos_of_nonneg
        (a := rsum n (fun i => db 0 (b 0) i * db 1 i z) *
          rsum n (fun i => db 2 (b 0) i * db 1 i z))
        (b := total n (db 0 z))
        (Int.mul_nonneg (rsum_nonneg n _ (fun i hi =>
          Int.mul_nonneg (hnonneg 0 (b 0) i) (hnonneg 1 i z)))
          (rsum_nonneg n _ (fun i hi =>
            Int.mul_nonneg (hnonneg 2 (b 0) i) (hnonneg 1 i z))))
        (rsum_nonneg n (db 0 z) (fun i hi => hnonneg 0 z i))
      exact ⟨z, hz, hpos.mpr ⟨hb.mpr ⟨h1, h2⟩, h3⟩⟩
  · intro i hi
    exact Int.mul_nonneg
      (Int.mul_nonneg (rsum_nonneg n _ (fun j hj =>
        Int.mul_nonneg (hnonneg 0 (b 0) j) (hnonneg 1 j i)))
        (rsum_nonneg n _ (fun j hj =>
          Int.mul_nonneg (hnonneg 2 (b 0) j) (hnonneg 1 j i))))
      (rsum_nonneg n (db 0 i) (fun j hj => hnonneg 0 i j))

end TinylambOpt.ResearchProofs.Atlas.Goal_TwoHopAntiWitness_IndependentWitness
