import TinylambOpt.ResearchGoals.JitKernels.DivergentWaves

/-! SOUND: Within one band of lengths, wasted lane steps are bounded by band width plus tail.
Decompose `w * ticks - sum` wave by wave. For a full or partial wave of `m ≤ w` tasks with
peak `p`, `w * p - sum = (w - m) * p + Σᵢ (p - xᵢ)`: the first term is idle lanes and the
second is per-lane padding below the wave peak. Every task lies in `[l, h]`, so
`p - xᵢ ≤ h - l` (band width) and `(w - m) * p ≤ (w - m) * h` (partial tail), which is
exactly `m * (h - l) + (1 * w - m) * h` for a single wave. For several waves, split off the
first full packet: `w * peak₁ - sum₁ ≤ w * (h - l)` since the packet has exactly `w` tasks,
and the idle-lane term of that packet vanishes; the remaining waves satisfy the bound
inductively. Summing uses `sum ≤ w * ticks` (each task is charged to at most one lane step
per wave) to keep every natural subtraction exact where needed, and
`length ≤ waves.length * w` to align the trailing idle multiplier. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_HomogeneousBucketPaddingBound
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

/-- If every element is bounded by `c`, so is the peak. -/
private theorem peak_le_bound :
    ∀ (xs : List Nat) (c : Nat), (∀ x ∈ xs, x ≤ c) → peak xs ≤ c := by
  intro xs c h
  cases xs with
  | nil => exact Nat.zero_le c
  | cons a as =>
      show as.foldl max a ≤ c
      exact foldlMaxLE as a c (h a (by simp)) (fun x hx => h x (by simp [hx]))

/-- A list whose elements are all `≤ c` has sum at most `length * c`. -/
private theorem sum_le_const :
    ∀ (as : List Nat) (c : Nat), (∀ x ∈ as, x ≤ c) → as.sum ≤ as.length * c := by
  intro as
  induction as with
  | nil => intro c h; exact Nat.zero_le _
  | cons b as ih =>
      intro c h
      have hb : b ≤ c := h b (by simp)
      have hrec : as.sum ≤ as.length * c := ih c (fun x hx => h x (by simp [hx]))
      rw [List.sum_cons, List.length_cons, Nat.add_mul, Nat.one_mul]
      omega

/-- Elementwise subtraction against a common upper bound telescopes to a total. -/
private theorem sum_sub_const :
    ∀ (as : List Nat) (p : Nat), (∀ x ∈ as, x ≤ p) →
      (as.map (fun x => p - x)).sum = as.length * p - as.sum := by
  intro as
  induction as with
  | nil => intro p h; simp
  | cons b as ih =>
      intro p h
      have hb : b ≤ p := h b (by simp)
      have hrec : (as.map (fun x => p - x)).sum = as.length * p - as.sum :=
        ih p (fun x hx => h x (by simp [hx]))
      have hSp : as.sum ≤ as.length * p :=
        sum_le_const as p (fun x hx => h x (by simp [hx]))
      have hlen : (b :: as).length * p = p + as.length * p := by
        rw [List.length_cons, Nat.add_mul, Nat.one_mul]
        omega
      have hmap : ((b :: as).map (fun x => p - x)).sum = (p - b) +
          (as.map (fun x => p - x)).sum := rfl
      rw [hmap, hrec, hlen, List.sum_cons]
      omega

/-- Scalar work is at most one lane step per wave: `sum ≤ w * ticks`. -/
private theorem sumWavesLE :
    ∀ (w : Nat), 0 < w → ∀ n : Nat, ∀ steps : List Nat, steps.length ≤ n →
      steps.sum ≤ w * ticks w steps := by
  intro w hw n
  induction n with
  | zero =>
      intro steps hlen
      match steps, hlen with
      | [], _ => unfold ticks; rw [waves_nil w hw]; simp
  | succ k ih =>
      intro steps hlen
      by_cases haz : steps = []
      · subst haz; unfold ticks; rw [waves_nil w hw]; simp
      · have hpos : 0 < steps.length := List.length_pos_iff.mpr haz
        by_cases hsmall : steps.length ≤ w
        · have t := sum_le_const steps (peak steps) (fun x hx => peak_le_mem x steps hx)
          have t2 : steps.sum ≤ w * peak steps :=
            Nat.le_trans t (Nat.mul_le_mul_right (peak steps) hsmall)
          unfold ticks
          rw [waves_short w steps hw hpos hsmall, List.sum_cons, List.sum_nil, Nat.add_zero]
          exact t2
        · have hge : w < steps.length := by omega
          have hta : (steps.take w).length = w := by
            rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hge)]
          have h1 : (steps.take w).sum ≤ w * peak (steps.take w) := by
            have t := sum_le_const (steps.take w) (peak (steps.take w))
              (fun x hx => peak_le_mem x (steps.take w) hx)
            rw [hta] at t; exact t
          have h2 : (steps.drop w).sum ≤ w * (waves w (steps.drop w)).sum := by
            have t := ih (steps.drop w) (by rw [List.length_drop]; omega)
            unfold ticks at t; exact t
          have hsum : steps.sum = (steps.take w).sum + (steps.drop w).sum := by
            rw [← List.sum_append, List.take_append_drop]
          unfold ticks
          rw [waves_long w steps hw hge, List.sum_cons, hsum, Nat.mul_add]
          omega

/-- The wave count times the width covers the task count. -/
private theorem wavesLenMul :
    ∀ (w : Nat), 0 < w → ∀ n : Nat, ∀ steps : List Nat, steps.length ≤ n →
      steps.length ≤ (waves w steps).length * w := by
  intro w hw n
  induction n with
  | zero =>
      intro steps hlen
      match steps, hlen with
      | [], _ => rw [waves_nil w hw, List.length_nil, Nat.zero_mul]; exact Nat.le_refl _
  | succ k ih =>
      intro steps hlen
      by_cases haz : steps = []
      · subst haz; rw [waves_nil w hw, List.length_nil, Nat.zero_mul]; exact Nat.le_refl _
      · have hpos : 0 < steps.length := List.length_pos_iff.mpr haz
        by_cases hsmall : steps.length ≤ w
        · rw [waves_short w steps hw hpos hsmall, List.length_cons, List.length_nil,
            Nat.add_zero, Nat.one_mul]
          exact hsmall
        · have hge : w < steps.length := by omega
          have hdp : (steps.drop w).length = steps.length - w := List.length_drop
          have hrec := ih (steps.drop w) (by rw [List.length_drop]; omega)
          rw [waves_long w steps hw hge, List.length_cons, Nat.add_zero, Nat.add_mul,
            Nat.one_mul]
          omega

/-- One wave: idle-lane padding plus below-peak padding fit the band bound. -/
private theorem shortBound :
    ∀ (w l h : Nat) (steps : List Nat) (p : Nat), 0 < w → 0 < steps.length →
      steps.length ≤ w → p = peak steps → (∀ x ∈ steps, l ≤ x ∧ x ≤ h) →
      w * p - steps.sum ≤ steps.length * (h - l) + (w - steps.length) * h := by
  intro w l h steps p hw hpos hsmall hp hbound
  obtain ⟨x, hx⟩ := List.length_pos_iff_exists_mem.mp hpos
  have hph : p ≤ h := by rw [hp]; exact peak_le_bound steps h (fun y hy => (hbound y hy).2)
  have hpl : l ≤ p := by rw [hp]; exact Nat.le_trans (hbound x hx).1 (peak_le_mem x steps hx)
  have hSp : steps.sum ≤ steps.length * p :=
    sum_le_const steps p (fun y hy => by rw [hp]; exact peak_le_mem y steps hy)
  have hmap : ∀ y ∈ steps.map (fun t => p - t), y ≤ h - l := by
    intro y hy
    obtain ⟨t, htm, rfl⟩ := List.mem_map.mp hy
    have ht1 : l ≤ t := (hbound t htm).1
    have ht2 : t ≤ h := (hbound t htm).2
    have ht3 : t ≤ p := by rw [hp]; exact peak_le_mem t steps htm
    omega
  have hsumle : (steps.map (fun t => p - t)).sum ≤ steps.length * (h - l) := by
    have t := sum_le_const (steps.map (fun t => p - t)) (h - l) hmap
    rw [List.length_map] at t; exact t
  have hsub : (steps.map (fun t => p - t)).sum = steps.length * p - steps.sum :=
    sum_sub_const steps p (fun y hy => by rw [hp]; exact peak_le_mem y steps hy)
  have hLp : steps.length * p ≤ w * p := Nat.mul_le_mul_right p hsmall
  have hdecomp : w * p - steps.sum = (w - steps.length) * p +
      (steps.map (fun t => p - t)).sum := by
    rw [Nat.sub_mul, hsub]
    omega
  rw [hdecomp]
  have h2 : (w - steps.length) * p ≤ (w - steps.length) * h :=
    Nat.mul_le_mul_left (w - steps.length) hph
  omega

/-- Band padding bound for one bucket of homogeneous work. -/
theorem sound : HomogeneousBucketPaddingBound.Goal := by
  intro steps w l h hw hbound
  show w * ticks w steps - steps.sum ≤
    steps.length * (h - l) + ((waves w steps).length * w - steps.length) * h
  have key : ∀ n : Nat, ∀ s : List Nat, s.length ≤ n → (∀ x ∈ s, l ≤ x ∧ x ≤ h) →
      w * ticks w s - s.sum ≤
        s.length * (h - l) + ((waves w s).length * w - s.length) * h := by
    intro n
    induction n with
    | zero =>
        intro s hlen hb
        match s, hlen with
        | [], _ => unfold ticks; rw [waves_nil w hw]; simp
    | succ k ih =>
        intro s hlen hb
        by_cases haz : s = []
        · subst haz; unfold ticks; rw [waves_nil w hw]; simp
        · have hpos : 0 < s.length := List.length_pos_iff.mpr haz
          by_cases hsmall : s.length ≤ w
          · unfold ticks
            rw [waves_short w s hw hpos hsmall, List.sum_cons, List.sum_nil, Nat.add_zero,
              List.length_cons, List.length_nil, Nat.add_zero, Nat.one_mul]
            exact shortBound w l h s (peak s) hw hpos hsmall rfl hb
          · have hge : w < s.length := by omega
            have hta : (s.take w).length = w := by
              rw [List.length_take, Nat.min_eq_left (Nat.le_of_lt hge)]
            have hdp : (s.drop w).length = s.length - w := List.length_drop
            have hlenPos : 0 < (s.take w).length := by rw [hta]; exact hw
            have hbTake : ∀ x ∈ s.take w, l ≤ x ∧ x ≤ h :=
              fun x hx => hb x (List.mem_of_mem_take hx)
            have hchunk := shortBound w l h (s.take w) (peak (s.take w)) hw hlenPos
              (by rw [hta]; exact Nat.le_refl w) rfl hbTake
            rw [hta, Nat.sub_self, Nat.zero_mul, Nat.add_zero] at hchunk
            have hih := ih (s.drop w) (by rw [List.length_drop]; omega)
              (fun x hx => hb x (List.mem_of_mem_drop hx))
            unfold ticks at hih
            have hsum : s.sum = (s.take w).sum + (s.drop w).sum := by
              rw [← List.sum_append, List.take_append_drop]
            have hS1 : (s.take w).sum ≤ w * peak (s.take w) := by
              have t := sum_le_const (s.take w) (peak (s.take w))
                (fun x hx => peak_le_mem x (s.take w) hx)
              rw [hta] at t; exact t
            have hS2 : (s.drop w).sum ≤ w * (waves w (s.drop w)).sum := by
              have t := sumWavesLE w hw k (s.drop w) (by rw [List.length_drop]; omega)
              unfold ticks at t; exact t
            have hL : s.length = w + (s.drop w).length := by omega
            have hR1 : s.length * (h - l) =
                w * (h - l) + (s.drop w).length * (h - l) := by
              rw [hL, Nat.add_mul]
            have hR2 : ((waves w (s.drop w)).length + 1) * w - s.length =
                (waves w (s.drop w)).length * w - (s.drop w).length := by
              rw [Nat.add_mul, Nat.one_mul, hL]
              omega
            unfold ticks
            rw [waves_long w s hw hge, List.sum_cons, List.length_cons, hsum, Nat.mul_add,
              hR1, hR2]
            omega
  exact key steps.length steps (Nat.le_refl _) hbound

end TinylambOpt.ResearchProofs.JitKernels.DivergentWaves_HomogeneousBucketPaddingBound
