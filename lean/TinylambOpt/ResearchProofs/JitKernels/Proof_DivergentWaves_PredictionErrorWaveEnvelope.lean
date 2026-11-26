import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! SOUND: A prediction that is pointwise off by at most `e` inflates the wave count by at
most `e` per wave. Take and drop preserve zip membership positionally: pairs taken or
dropped from equal-length lists are pairs of the original zip, so each aligned window of
predictions is still dominated by its actual counterpart plus `e`. The peak of such a
window grows by at most `e` (running-maximum induction), and summing the per-window
inequalities — one `+ e` term per wave — yields the envelope
`ticks w a ≤ ticks w b + (waves w b).length * e`. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_PredictionErrorWaveEnvelope
open TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

private theorem div_step (n w : Nat) (hw : 0 < w) : (n + w) / w = n / w + 1 := by
  rw [Nat.add_comm, Nat.add_div hw]
  rw [Nat.mod_self, Nat.div_self hw]
  have hlt : n % w < w := Nat.mod_lt n hw
  rw [if_neg (by omega)]
  omega

private theorem map_range_split {α : Type} (f : Nat → α) :
    ∀ m : Nat, List.map f (List.range (m + 1)) =
      f 0 :: List.map (fun j => f (j + 1)) (List.range m) := by
  intro m
  induction m with
  | zero => rw [Nat.zero_add, List.range_one]; simp
  | succ m ih =>
      rw [List.range_succ, List.map_append, ih,
        List.range_succ, List.map_append, List.map_cons]
      simp

/-- `map f (range (m+1)) = f 0 :: map g (range m)`. -/
private theorem map_range_eq_cons {α : Type} :
    ∀ (f g : Nat → α) (a : α), f 0 = a → (∀ j, f (j + 1) = g j) →
      ∀ m : Nat, List.map f (List.range (m + 1)) = a :: List.map g (List.range m) := by
  intro f g a h0 h1 m
  rw [map_range_split, h0]
  refine congrArg (a :: ·) (List.map_congr_left (fun j _ => ?_))
  exact h1 j

private theorem waves_nil (w : Nat) (hw : 0 < w) : waves w [] = [] := by
  unfold waves
  rw [List.length_nil, Nat.div_eq_of_lt (by omega), List.range_zero, List.map_nil]

private theorem waves_short (w : Nat) (xs : List Nat) (hw : 0 < w) (h1 : 0 < xs.length)
    (h2 : xs.length ≤ w) : waves w xs = [peak xs] := by
  unfold waves
  have hsub : xs.length + w - 1 = w + (xs.length - 1) := by omega
  rw [hsub, Nat.add_comm, div_step (xs.length - 1) w hw, Nat.div_eq_of_lt (by omega),
    Nat.zero_add, List.range_one]
  simp [List.drop_zero, List.take_of_length_le h2]

private theorem waves_long (w : Nat) (xs : List Nat) (hw : 0 < w) (h : w < xs.length) :
    waves w xs = peak (xs.take w) :: waves w (xs.drop w) := by
  have hlen : (xs.drop w).length = xs.length - w := List.length_drop
  have hdiv : (xs.length + w - 1) / w = (xs.length - w + w - 1) / w + 1 := by
    have h1 : xs.length - w + w - 1 = xs.length - 1 := by omega
    rw [h1, show xs.length + w - 1 = xs.length - 1 + w by omega, div_step _ _ hw]
  unfold waves
  rw [hlen, hdiv, map_range_eq_cons
    (fun j => peak ((xs.drop (j * w)).take w))
    (fun j => peak (((xs.drop w).drop (j * w)).take w))
    (peak (xs.take w)) (by simp [List.drop_zero])
    (fun j => by
      rw [List.drop_drop,
        show (j + 1) * w = w + j * w by rw [Nat.add_mul, Nat.one_mul, Nat.add_comm]])]

/-- The running maximum dominates the accumulator and every element. -/
private theorem le_foldl_max :
    ∀ (as : List Nat) (init : Nat), init ≤ as.foldl max init := by
  intro as
  induction as with
  | nil => intro init; exact Nat.le_refl init
  | cons a as ih =>
      intro init
      show init ≤ as.foldl max (max init a)
      exact Nat.le_trans (Nat.le_max_left init a) (ih (max init a))

private theorem mem_foldl_max :
    ∀ (as : List Nat) (init x : Nat), x ∈ as → x ≤ as.foldl max init := by
  intro as
  induction as with
  | nil => intro init x h; cases h
  | cons a as ih =>
      intro init x h
      show x ≤ as.foldl max (max init a)
      rw [List.mem_cons] at h
      cases h with
      | inl he =>
          rw [he]
          exact Nat.le_trans (Nat.le_max_right init a) (le_foldl_max as (max init a))
      | inr hx => exact ih (max init a) x hx

/-- Every element is at most the peak. -/
private theorem peak_le_mem : ∀ (x : Nat) (xs : List Nat), x ∈ xs → x ≤ peak xs := by
  intro x xs h
  cases xs with
  | nil => cases h
  | cons a as =>
      rw [List.mem_cons] at h
      cases h with
      | inl he => rw [he]; exact le_foldl_max as a
      | inr hx => exact mem_foldl_max as a x hx

private theorem foldlMaxLE :
    ∀ (as : List Nat) (init c : Nat), init ≤ c → (∀ x ∈ as, x ≤ c) → as.foldl max init ≤ c := by
  intro as
  induction as with
  | nil => intro init c h1 _; exact h1
  | cons b as ih =>
      intro init c h1 h2
      show as.foldl max (max init b) ≤ c
      exact ih (max init b) c (Nat.max_le.mpr ⟨h1, h2 b (by simp)⟩)
        (fun x hx => h2 x (by simp [hx]))

/-- Zip and `take` commute. -/
private theorem zip_take_eq : ∀ (a b : List Nat) (n : Nat),
    (a.take n).zip (b.take n) = (a.zip b).take n := by
  intro a
  induction a with
  | nil => intro b n; simp
  | cons x xs ih =>
      intro b n
      match b, n with
      | [], _ => simp
      | _, 0 => simp
      | y :: ys, k + 1 =>
          exact congrArg ((x, y) :: ·) (ih ys k)

/-- Zip and `drop` commute. -/
private theorem zip_drop_eq : ∀ (a b : List Nat) (n : Nat),
    (a.drop n).zip (b.drop n) = (a.zip b).drop n := by
  intro a
  induction a with
  | nil => intro b n; simp
  | cons x xs ih =>
      intro b n
      match b, n with
      | [], _ => simp
      | _, 0 => simp
      | y :: ys, k + 1 => exact ih ys k

private theorem zip_take_mem : ∀ (a b : List Nat) (n : Nat) (p : Nat × Nat),
    p ∈ (a.take n).zip (b.take n) → p ∈ a.zip b := by
  intro a b n p hp
  rw [zip_take_eq a b n] at hp
  exact List.mem_of_mem_take hp

private theorem zip_drop_mem : ∀ (a b : List Nat) (n : Nat) (p : Nat × Nat),
    p ∈ (a.drop n).zip (b.drop n) → p ∈ a.zip b := by
  intro a b n p hp
  rw [zip_drop_eq a b n] at hp
  exact List.mem_of_mem_drop hp

/-- Folded maxima transfer across zip-dominated lists. -/
private theorem peakAG : ∀ (a b : List Nat) (e : Nat), a.length = b.length →
    (∀ p ∈ a.zip b, p.1 ≤ p.2 + e) → ∀ ia ib : Nat, ia ≤ ib + e →
      a.foldl max ia ≤ b.foldl max ib + e := by
  intro a
  induction a with
  | nil =>
      intro b e hlen hz ia ib h3
      match b with
      | [] => exact h3
      | y :: ys => exact absurd hlen (by simp)
  | cons x xs ih =>
      intro b e hlen hz ia ib h3
      match b with
      | [] => exact absurd hlen (by simp)
      | y :: ys =>
          have hxy : x ≤ y + e := hz (x, y) (by rw [List.zip_cons_cons]; exact List.Mem.head _)
          have hlens : xs.length = ys.length := by simpa using hlen
          have hzs : ∀ p ∈ xs.zip ys, p.1 ≤ p.2 + e := fun p hp => hz p (by
            rw [List.zip_cons_cons]; exact List.Mem.tail _ hp)
          have hmax : max ia x ≤ max ib y + e := by
            refine Nat.max_le.mpr ⟨Nat.le_trans h3 (Nat.add_le_add_right (Nat.le_max_left ib y) e),
              Nat.le_trans hxy (Nat.add_le_add_right (Nat.le_max_right ib y) e)⟩
          show xs.foldl max (max ia x) ≤ ys.foldl max (max ib y) + e
          exact ih ys e hlens hzs (max ia x) (max ib y) hmax

/-- A window of predictions peaks at most `e` above the aligned window of actuals. -/
private theorem peakA : ∀ (a b : List Nat) (e : Nat), a.length = b.length →
    (∀ p ∈ a.zip b, p.1 ≤ p.2 + e) → peak a ≤ peak b + e := by
  intro a b e hlen hz
  match a, b with
  | [], [] => exact Nat.zero_le _
  | x :: xs, y :: ys =>
      exact peakAG xs ys e (by simpa using hlen)
        (fun p hp => hz p (by rw [List.zip_cons_cons]; exact List.Mem.tail _ hp)) x y
        (hz (x, y) (by rw [List.zip_cons_cons]; exact List.Mem.head _))
  | [], y :: ys => exact absurd hlen (by simp)
  | x :: xs, [] => exact absurd hlen (by simp)

/-- Windowed summation envelope: each of the (equal number of) waves adds at most `e`. -/
private theorem wavesSumLE :
    ∀ (w e : Nat), 0 < w → ∀ n : Nat, ∀ a b : List Nat, a.length ≤ n → b.length ≤ n →
      a.length = b.length → (∀ p ∈ a.zip b, p.1 ≤ p.2 + e) →
      (waves w a).sum ≤ (waves w b).sum + (waves w b).length * e := by
  intro w e hw n
  induction n with
  | zero =>
      intro a b ha hb hlen hz
      match a, b, ha, hb with
      | [], [], _, _ => simp
  | succ k ih =>
      intro a b ha hb hlen hz
      by_cases haz : a = []
      · subst haz
        rw [List.length_nil] at ha hlen
        match b, (by omega : b.length = 0) with
        | [], _ => simp
      · have hapos : 0 < a.length := List.length_pos_iff.mpr haz
        have hbpos : 0 < b.length := by omega
        by_cases hsmall : a.length ≤ w
        · have hbsmall : b.length ≤ w := by omega
          rw [waves_short w a hw hapos hsmall, waves_short w b hw hbpos hbsmall]
          simp only [List.sum_cons, List.sum_nil, List.length_cons, List.length_nil,
            Nat.add_zero, Nat.mul_zero, Nat.one_mul]
          have hle := peakA a b e hlen hz
          omega
        · have hage : w < a.length := by omega
          have hbge : w < b.length := by omega
          have hta : (a.take w).length = (b.take w).length := by
            rw [List.length_take, List.length_take]; omega
          have hdp : (a.drop w).length = (b.drop w).length := by
            rw [List.length_drop, List.length_drop]; omega
          have hza : ∀ p ∈ (a.take w).zip (b.take w), p.1 ≤ p.2 + e :=
            fun p hp => hz p (zip_take_mem a b w p hp)
          have hzb : ∀ p ∈ (a.drop w).zip (b.drop w), p.1 ≤ p.2 + e :=
            fun p hp => hz p (zip_drop_mem a b w p hp)
          have h1 : (a.drop w).length ≤ k := by rw [List.length_drop]; omega
          have h2 : (b.drop w).length ≤ k := by rw [List.length_drop]; omega
          have hpeak := peakA (a.take w) (b.take w) e hta hza
          have hrec := ih (a.drop w) (b.drop w) h1 h2 hdp hzb
          rw [waves_long w a hw hage, waves_long w b hw hbge]
          simp only [List.sum_cons, List.sum_nil, List.length_cons, List.length_nil,
            Nat.add_zero]
          have hmul : ((waves w (b.drop w)).length + 1) * e =
              (waves w (b.drop w)).length * e + e := by
            rw [Nat.add_mul, Nat.one_mul]
          omega

/-- Prediction error inflates barrier ticks by at most `e` per wave. -/
theorem sound : PredictionErrorWaveEnvelope.Goal := by
  intro a b w e hw hlen hz
  show (waves w a).sum ≤ (waves w b).sum + (waves w b).length * e
  exact wavesSumLE w e hw a.length a b (Nat.le_refl _) (by omega) hlen hz

end TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_PredictionErrorWaveEnvelope
