import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_CountryReturn_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ParallelLineage_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_PromoInventory_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_SupplierClique_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ReturnedItem_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ChannelBridge_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_TwoHopAntiWitness_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_HubWithChords_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_PromoInventory_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ParallelLineage_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_SupplierClique_Remaining_Round2

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option maxErrors 30
set_option relaxedAutoImplicit true

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans_WorkB

open TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-! ## Wrappers for goals already proved in other Remaining files -/

theorem sound_CountryReturn_DeltaForecast :
    CountryReturn.DeltaForecast.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.CountryReturn.Remaining.sound_DeltaForecast

theorem sound_ParallelLineage_DeltaForecast :
    ParallelLineage.DeltaForecast.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ParallelLineage.Remaining.sound_DeltaForecast

theorem sound_SupplierClique_FractionalCap :
    SupplierClique.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.SupplierClique.Remaining.SupplierClique.sound_FractionalCap


/-! ## Common helpers -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum
private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]
private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]
private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]
private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega
private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega
private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]
private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]
private theorem rsum_neg (n : Nat) (f : Nat → Int) :
    rsum n (fun i => - f i) = - rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.neg_add]
private theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp
private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      have h : rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0:Int)) := by
        apply rsum_congr; intro j hj; rw [rsum_zero]
      rw [h, rsum_const_zero]
  | succ n ih =>
      rw [rsum_succ, ih]
      have h : rsum m (fun j => rsum (n+1) (fun i => f i j)) =
               rsum m (fun j => rsum n (fun i => f i j) + f n j) := by
        apply rsum_congr; intro j hj; rw [rsum_succ]
      rw [h, rsum_add]
private theorem rsum_eq_single (n a : Nat) (ha : a < n) (g : Nat → Int) :
    rsum n (fun x => if x == a then g x else 0) = g a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : n = a
      · rw [h]
        have hprev : rsum a (fun x => if x == a then g x else 0) = 0 := by
          rw [show rsum a (fun x => if x == a then g x else 0) = rsum a (fun _ => (0:Int)) by
            apply rsum_congr; intro x hx
            have hxa : (x == a) = false := by
              have hne' : x ≠ a := by omega
              simp [beq_iff_eq, hne']
            simp [hxa]]
          exact rsum_const_zero a
        rw [hprev]
        simp
      · have ha' : a < n := by omega
        have hne : (n == a) = false := by simp [beq_iff_eq, h]
        rw [ih ha']
        simp [hne]
private theorem rsum_eq_single_pair (n a : Nat) (ha : a < n) (g : Nat → Nat → Int) :
    rsum n (fun b => rsum n (fun d => if b == a && d == a then g b d else 0)) = g a a := by
  calc
    rsum n (fun b => rsum n (fun d => if b == a && d == a then g b d else 0))
        = rsum n (fun b => if b == a then rsum n (fun d => if d == a then g b d else 0) else 0) := by
            apply rsum_congr; intro b hb
            by_cases hba : b == a
            · simp [hba]
            · simp [hba, rsum_const_zero]
    _ = g a a := by
        rw [rsum_eq_single n a ha (fun b => rsum n (fun d => if d == a then g b d else 0))]
        rw [rsum_eq_single n a ha (fun d => g a d)]
private theorem rsum_eq_single_triple (n a : Nat) (ha : a < n) (g : Nat → Nat → Nat → Int) :
    rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      if b == a && c == a && d == a then g b c d else 0))) = g a a a := by
  calc
    rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      if b == a && c == a && d == a then g b c d else 0)))
        = rsum n (fun b => if b == a then rsum n (fun c => rsum n (fun d =>
            if c == a && d == a then g b c d else 0)) else 0) := by
            apply rsum_congr; intro b hb
            by_cases hba : b == a
            · simp [hba]
            · simp [hba, rsum_const_zero]
    _ = g a a a := by
        rw [rsum_eq_single n a ha (fun b => rsum n (fun c => rsum n (fun d =>
            if c == a && d == a then g b c d else 0)))]
        rw [rsum_eq_single_pair n a ha (fun c d => g a c d)]

private theorem rsum_ite_single (n a : Nat) (g : Nat → Int) :
    rsum n (fun b => if a = b then g b else 0) = if a < n then g a else 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih]
      by_cases h : a = n
      · subst n
        simp
      · by_cases han : a < n
        · simp [han, Nat.lt_succ_of_lt han, h]
        · have hange : ¬ a < n+1 := by omega
          simp [han, hange, h]

private theorem triple_inclusion (n : Nat) (f : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c =>
      if a ≠ b ∧ a ≠ c ∧ b ≠ c then f a b c else 0))) =
    rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c))) -
    rsum n (fun a => rsum n (fun c => f a a c)) -
    rsum n (fun a => rsum n (fun b => f a b a)) -
    rsum n (fun a => rsum n (fun b => f a b b)) +
    2 * rsum n (fun a => f a a a) := by
  let D := fun a b c => if a ≠ b ∧ a ≠ c ∧ b ≠ c then f a b c else 0
  let P := fun a b c => if a = b then f a b c else 0
  let Q := fun a b c => if a = c then f a b c else 0
  let R := fun a b c => if b = c then f a b c else 0
  let E := fun a b c => if a = b ∧ b = c then f a b c else 0
  have hpoint : ∀ a b c, D a b c = f a b c - P a b c - Q a b c - R a b c + 2 * E a b c := by
    intro a b c
    dsimp [D, P, Q, R, E]
    by_cases hab : a = b <;> by_cases hac : a = c <;> by_cases hbc : b = c <;>
      simp_all <;> omega
  have hinner : ∀ a b,
      rsum n (fun c => D a b c) =
        rsum n (fun c => f a b c) - rsum n (fun c => P a b c) -
        rsum n (fun c => Q a b c) - rsum n (fun c => R a b c) +
        2 * rsum n (fun c => E a b c) := by
    intro a b
    rw [show (fun c => D a b c) = (fun c => f a b c - P a b c - Q a b c - R a b c +
        2 * E a b c) by funext c; exact hpoint a b c]
    rw [rsum_add, rsum_sub, rsum_sub, rsum_sub, rsum_const_mul]
  have hmid : ∀ a, a < n →
      rsum n (fun b => rsum n (fun c => D a b c)) =
        rsum n (fun b => rsum n (fun c => f a b c)) -
        rsum n (fun c => f a a c) -
        rsum n (fun b => f a b a) -
        rsum n (fun b => f a b b) +
        2 * f a a a := by
    intro a ha
    rw [show (fun b => rsum n (fun c => D a b c)) =
        (fun b => rsum n (fun c => f a b c) - rsum n (fun c => P a b c) -
          rsum n (fun c => Q a b c) - rsum n (fun c => R a b c) +
          2 * rsum n (fun c => E a b c)) by funext b; exact hinner a b]
    rw [rsum_add, rsum_sub, rsum_sub, rsum_sub, rsum_const_mul]
    have hP : rsum n (fun b => rsum n (fun c => P a b c)) = rsum n (fun c => f a a c) := by
      rw [show (fun b => rsum n (fun c => P a b c)) =
          (fun b => if a = b then rsum n (fun c => f a b c) else 0) by
        funext b
        dsimp [P]
        by_cases hab : a = b
        · subst b; simp
        · simp [hab, rsum_const_zero]]
      rw [rsum_ite_single n a (fun b => rsum n (fun c => f a b c))]
      simp [ha]
    have hQ : rsum n (fun b => rsum n (fun c => Q a b c)) = rsum n (fun b => f a b a) := by
      rw [show (fun b => rsum n (fun c => Q a b c)) =
          (fun b => if a < n then f a b a else 0) by
        funext b
        dsimp [Q]
        rw [rsum_ite_single n a (fun c => f a b c)]]
      simp [ha]
    have hR : rsum n (fun b => rsum n (fun c => R a b c)) = rsum n (fun b => f a b b) := by
      apply rsum_congr
      intro b hb
      dsimp [R]
      rw [rsum_ite_single n b (fun c => f a b c)]
      simp [hb]
    have hE : rsum n (fun b => rsum n (fun c => E a b c)) = f a a a := by
      rw [show (fun b => rsum n (fun c => E a b c)) =
          (fun b => if a = b then (if b < n then f a b b else 0) else 0) by
        funext b
        dsimp [E]
        by_cases hab : a = b
        · subst hab
          simp [rsum_ite_single, ha]
        · simp [hab, rsum_const_zero]]
      rw [rsum_ite_single n a (fun b => if b < n then f a b b else 0)]
      simp [ha]
    rw [hP, hQ, hR, hE]
  calc
    rsum n (fun a => rsum n (fun b => rsum n (fun c => D a b c)))
        = rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c)) -
            rsum n (fun c => f a a c) - rsum n (fun b => f a b a) -
            rsum n (fun b => f a b b) + 2 * f a a a) := by
              apply rsum_congr
              intro a ha
              exact hmid a ha
    _ = rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c))) -
        rsum n (fun a => rsum n (fun c => f a a c)) -
        rsum n (fun a => rsum n (fun b => f a b a)) -
        rsum n (fun a => rsum n (fun b => f a b b)) +
        2 * rsum n (fun a => f a a a) := by
          rw [rsum_add, rsum_sub, rsum_sub, rsum_sub, rsum_const_mul]

/-! 4-var inclusion-exclusion -/
private theorem eq_coeff4 (a b c d : Nat) :
    (if [a,b,c,d].Nodup then (1:Int) else 0) =
      (1:Int) * (1:Int) * (1:Int) * (1:Int) +
      -((if (b == a) then (1:Int) else 0) * (1:Int) * (1:Int)) +
      -((1:Int) * (if (c == a) then (1:Int) else 0) * (1:Int)) +
      -((1:Int) * (1:Int) * (if (d == a) then (1:Int) else 0)) +
      -((1:Int) * (if (c == b) then (1:Int) else 0) * (1:Int)) +
      (2:Int) * ((if (b == a) && (c == a) then (1:Int) else 0) * (1:Int)) +
      (if (c == b) then (1:Int) else 0) * (if (d == a) then (1:Int) else 0) +
      -((1:Int) * (1:Int) * (if (d == b) then (1:Int) else 0)) +
      (if (c == a) then (1:Int) else 0) * (if (d == b) then (1:Int) else 0) +
      (2:Int) * ((1:Int) * (if (b == a) && (d == a) then (1:Int) else 0)) +
      -((1:Int) * (1:Int) * (if (d == c) then (1:Int) else 0)) +
      (if (b == a) then (1:Int) else 0) * (if (d == c) then (1:Int) else 0) +
      (2:Int) * ((1:Int) * (if (c == a) && (d == a) then (1:Int) else 0)) +
      (2:Int) * ((1:Int) * (if (c == b) && (d == b) then (1:Int) else 0)) +
      (-6:Int) * ((if (b == a) && (c == a) && (d == a) then (1:Int) else 0)) := by
  by_cases hab : b = a <;> by_cases hac : c = a <;> by_cases had : d = a <;>
    by_cases hbc : c = b <;> by_cases hbd : d = b <;> by_cases hcd : d = c <;>
      simp_all [List.Nodup, List.mem_cons, beq_iff_eq] <;> omega

-- BEGIN jpb-helpers
/-- Evaluate an innermost indicator sum whose condition mentions no other
    variable of the body. -/
private theorem jpb_ite_pull (n : Nat) (P : Prop) [inst : Decidable P] (g : Nat → Int) :
    rsum n (fun i => if P then g i else 0) = if P then rsum n g else 0 := by
  by_cases hP : P <;> simp [hP, rsum_const_zero]

/-- Drop a vacuous bound range filter. -/
private theorem jpb_range_ite (n : Nat) (g : Nat → Int) :
    rsum n (fun a => if a < n then g a else 0) = rsum n g := by
  apply rsum_congr; intro a ha; exact if_pos ha

/-- Collapse a sum over a variable constrained to equal an outer value. -/
private theorem jpb_rsum_ite_flip (n a : Nat) (g : Nat → Int) :
    rsum n (fun b => if b = a then g b else 0) = if a < n then g a else 0 := by
  have h : (fun b => if b = a then g b else 0) = (fun b => if a = b then g b else 0) := by
    funext b
    by_cases hab : b = a
    · subst hab; simp
    · rw [if_neg hab, if_neg (fun hh => hab hh.symm)]
  rw [h, rsum_ite_single]

/-- Pointwise inclusion-exclusion for the 4-var distinctness indicator:
    blocks in `partitions` positional order with Möbius coefficients. -/
private theorem jpb_pointwise (a b c d : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    (if (¬a = b ∧ ¬a = c ∧ ¬a = d) ∧ (¬b = c ∧ ¬b = d) ∧ ¬c = d then f a b c d else 0) =
      (f a b c d
      - (if b = a then f a b c d else 0)
      - (if c = a then f a b c d else 0)
      - (if d = a then f a b c d else 0)
      - (if c = b then f a b c d else 0)
      - (if d = b then f a b c d else 0)
      - (if d = c then f a b c d else 0)
      + 2 * (if b = a then (if c = a then f a b c d else 0) else 0)
      + 2 * (if b = a then (if d = a then f a b c d else 0) else 0)
      + 2 * (if c = a then (if d = a then f a b c d else 0) else 0)
      + 2 * (if c = b then (if d = b then f a b c d else 0) else 0)
      + (if c = b then (if d = a then f a b c d else 0) else 0)
      + (if c = a then (if d = b then f a b c d else 0) else 0)
      + (if b = a then (if d = c then f a b c d else 0) else 0)
      - 6 * (if b = a then (if c = a then (if d = a then f a b c d else 0) else 0) else 0)) := by
  by_cases h1 : a = b <;> by_cases h2 : a = c <;> by_cases h3 : a = d <;>
    by_cases h4 : b = c <;> by_cases h5 : b = d <;> by_cases h6 : c = d <;>
      simp_all <;>
      (try omega)

/-- Rewrite a 4-fold sum body pointwise. -/
private theorem jpb_body_rw (n : Nat) (φ ψ : Nat → Nat → Nat → Nat → Int)
    (h : ∀ a b c d, φ a b c d = ψ a b c d) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => φ a b c d)))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => ψ a b c d)))) := by
  refine rsum_congr n (fun a _ => ?_)
  refine rsum_congr n (fun b _ => ?_)
  refine rsum_congr n (fun c _ => ?_)
  refine rsum_congr n (fun d _ => ?_)
  exact h a b c d

/-- Master 4-var collision-correction identity: the injective (distinct) sum
    equals the Möbius combination over collapsed diagonals. RHS in natural
    binder order; the `jpb_sw*` lemmas below convert to `hc`-order. -/
private theorem jpb_quad (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
        if (¬a = b ∧ ¬a = c ∧ ¬a = d) ∧ (¬b = c ∧ ¬b = d) ∧ ¬c = d then f a b c d else 0)))) =
      (rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => f a b c d))))
      - rsum n (fun a => rsum n (fun c => rsum n (fun d => f a a c d)))
      - rsum n (fun a => rsum n (fun b => rsum n (fun d => f a b a d)))
      - rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c a)))
      - rsum n (fun a => rsum n (fun b => rsum n (fun d => f a b b d)))
      - rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c b)))
      - rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c c)))
      + 2 * rsum n (fun a => rsum n (fun d => f a a a d))
      + 2 * rsum n (fun a => rsum n (fun c => f a a c a))
      + 2 * rsum n (fun a => rsum n (fun b => f a b a a))
      + 2 * rsum n (fun a => rsum n (fun b => f a b b b))
      + rsum n (fun a => rsum n (fun b => f a b b a))
      + rsum n (fun a => rsum n (fun b => f a b a b))
      + rsum n (fun a => rsum n (fun c => f a a c c))
      - 6 * rsum n (fun a => f a a a a)) := by
  rw [jpb_body_rw n
    (fun a b c d => if (¬a = b ∧ ¬a = c ∧ ¬a = d) ∧ (¬b = c ∧ ¬b = d) ∧ ¬c = d
      then f a b c d else 0)
    (fun a b c d =>
      (f a b c d
      - (if b = a then f a b c d else 0)
      - (if c = a then f a b c d else 0)
      - (if d = a then f a b c d else 0)
      - (if c = b then f a b c d else 0)
      - (if d = b then f a b c d else 0)
      - (if d = c then f a b c d else 0)
      + 2 * (if b = a then (if c = a then f a b c d else 0) else 0)
      + 2 * (if b = a then (if d = a then f a b c d else 0) else 0)
      + 2 * (if c = a then (if d = a then f a b c d else 0) else 0)
      + 2 * (if c = b then (if d = b then f a b c d else 0) else 0)
      + (if c = b then (if d = a then f a b c d else 0) else 0)
      + (if c = a then (if d = b then f a b c d else 0) else 0)
      + (if b = a then (if d = c then f a b c d else 0) else 0)
      - 6 * (if b = a then (if c = a then (if d = a then f a b c d else 0) else 0) else 0)))
    (fun a b c d => jpb_pointwise a b c d f)]
  simp only [rsum_add, rsum_sub, rsum_const_mul, rsum_neg, jpb_ite_pull, jpb_range_ite,
    jpb_rsum_ite_flip]

-- Swap lemmas: natural binder order vs the `hc`/`partitions`-order atoms.
private theorem jpb_sw3 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun d => f a b a d))) =
      rsum n (fun b => rsum n (fun a => rsum n (fun d => f a b a d))) :=
  rsum_comm n n (fun x y => rsum n (fun d => f x y x d))

private theorem jpb_sw4 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c a))) =
      rsum n (fun b => rsum n (fun c => rsum n (fun a => f a b c a))) := by
  rw [rsum_comm n n (fun x y => rsum n (fun z => f x y z x))]
  rw [show (fun b => rsum n (fun a => rsum n (fun c => f a b c a))) =
        (fun b => rsum n (fun c => rsum n (fun a => f a b c a))) by
    funext b; exact rsum_comm n n (fun x y => f x b y x)]

private theorem jpb_sw7 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => f a b b a)) =
      rsum n (fun b => rsum n (fun a => f a b b a)) :=
  rsum_comm n n (fun x y => f x y y x)

private theorem jpb_sw8 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c b))) =
      rsum n (fun a => rsum n (fun c => rsum n (fun b => f a b c b))) := by
  rw [show (fun a => rsum n (fun b => rsum n (fun c => f a b c b))) =
        (fun a => rsum n (fun c => rsum n (fun b => f a b c b))) by
    funext a; exact rsum_comm n n (fun x y => f a x y x)]

private theorem jpb_sw10 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun c => f a a c a)) =
      rsum n (fun c => rsum n (fun a => f a a c a)) :=
  rsum_comm n n (fun x y => f x x y x)

private theorem jpb_sw13 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => f a b a a)) =
      rsum n (fun b => rsum n (fun a => f a b a a)) :=
  rsum_comm n n (fun x y => f x y x x)
-- END jpb-helpers

namespace CountryReturn_CC
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn
private def F (db : DB) (b : Bind) (a c d e : Nat) : Int := db 0 (b 0) a * (db 1 a c * (db 2 c d * (db 1 d e * db 2 (b 0) e)))
private theorem parts_eq : partitions pattern.freeVars = [[[1], [2], [3], [4]], [[1, 2], [3], [4]], [[2], [1, 3], [4]], [[2], [3], [1, 4]], [[1], [2, 3], [4]], [[1, 2, 3], [4]], [[2, 3], [1, 4]], [[1], [3], [2, 4]], [[1, 3], [2, 4]], [[3], [1, 2, 4]], [[1], [2], [3, 4]], [[1, 2], [3, 4]], [[2], [1, 3, 4]], [[1], [2, 3, 4]], [[1, 2, 3, 4]]] := by native_decide
private theorem hc1 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [3], [4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => F db b a c d e)))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [3], [4]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => rsum n (fun e => F db b a a d e))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1, 3], [4]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => rsum n (fun e => F db b a c a e))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3], [1, 4]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun a => F db b a c d a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2, 3], [4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun e => F db b a c c e))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2, 3], [4]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db b a a a e)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3], [1, 4]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b a c c a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [3], [2, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => rsum n (fun c => F db b a c d c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 3], [2, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a c a c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [1, 2, 4]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => F db b a a d a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [3, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun d => F db b a c d d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [3, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db b a a d d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1, 3, 4]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b a c a a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2, 3, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a c c c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2, 3, 4]] : List (List Nat)) = rsum n (fun a => F db b a a a a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if [a, c, d, e].Nodup then F db b a c d e else 0)))) := by
  simp [injective, pattern, integrate, value, put, rsum, F, List.Nodup, List.mem_cons]
private theorem hpoint (db : DB) (b : Bind) (a c d e : Nat) :
    (if (¬a = c ∧ ¬a = d ∧ ¬a = e) ∧ (¬c = d ∧ ¬c = e) ∧ ¬d = e then F db b a c d e else 0) =
      F db b a c d e
      - (if c = a then F db b a c d e else 0)
      - (if d = a then F db b a c d e else 0)
      - (if e = a then F db b a c d e else 0)
      - (if d = c then F db b a c d e else 0)
      - (if e = c then F db b a c d e else 0)
      - (if e = d then F db b a c d e else 0)
      + 2 * (if c = a then (if d = a then F db b a c d e else 0) else 0)
      + 2 * (if c = a then (if e = a then F db b a c d e else 0) else 0)
      + 2 * (if d = a then (if e = a then F db b a c d e else 0) else 0)
      + 2 * (if d = c then (if e = c then F db b a c d e else 0) else 0)
      + (if d = c then (if e = a then F db b a c d e else 0) else 0)
      + (if d = a then (if e = c then F db b a c d e else 0) else 0)
      + (if c = a then (if e = d then F db b a c d e else 0) else 0)
      - 6 * (if c = a then (if d = a then (if e = a then F db b a c d e else 0) else 0) else 0) := by
  exact jpb_pointwise a c d e (F db b)
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end CountryReturn_CC

theorem sound_CountryReturn_CollisionCorrection : CountryReturn.CollisionCorrection.Goal := CountryReturn_CC.sound_CollisionCorrection
namespace OrderSupplier_CC
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier
private def F (db : DB) (b : Bind) (a c e d : Nat) : Int := db 0 (b 0) a * (db 1 a c * (db 2 c d * (db 1 (b 0) e * db 2 e d)))
private theorem parts_eq : partitions pattern.freeVars = [[[1], [2], [4], [3]], [[1, 2], [4], [3]], [[2], [1, 4], [3]], [[2], [4], [1, 3]], [[1], [2, 4], [3]], [[1, 2, 4], [3]], [[2, 4], [1, 3]], [[1], [4], [2, 3]], [[1, 4], [2, 3]], [[4], [1, 2, 3]], [[1], [2], [4, 3]], [[1, 2], [4, 3]], [[2], [1, 4, 3]], [[1], [2, 4, 3]], [[1, 2, 4, 3]]] := by native_decide
private theorem hc1 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [4], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun e => rsum n (fun d => F db b a c e d)))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [4], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => rsum n (fun d => F db b a a e d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1, 4], [3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => rsum n (fun d => F db b a c a d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [4], [1, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => rsum n (fun a => F db b a c e a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2, 4], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun d => F db b a c c d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2, 4], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db b a a a d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 4], [1, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b a c c a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [4], [2, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => rsum n (fun c => F db b a c e c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 4], [2, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a c a c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1, 2, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => F db b a a e a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun e => F db b a c e e))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db b a a e e)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1, 4, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b a c a a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2, 4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a c c c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2, 4, 3]] : List (List Nat)) = rsum n (fun a => F db b a a a a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun a => rsum n (fun c => rsum n (fun e => rsum n (fun d => if [a, c, e, d].Nodup then F db b a c e d else 0)))) := by
  simp [injective, pattern, integrate, value, put, rsum, F, List.Nodup, List.mem_cons]
private theorem hpoint (db : DB) (b : Bind) (a c e d : Nat) :
    (if (¬a = c ∧ ¬a = e ∧ ¬a = d) ∧ (¬c = e ∧ ¬c = d) ∧ ¬e = d then F db b a c e d else 0) =
      F db b a c e d
      - (if c = a then F db b a c e d else 0)
      - (if e = a then F db b a c e d else 0)
      - (if d = a then F db b a c e d else 0)
      - (if e = c then F db b a c e d else 0)
      - (if d = c then F db b a c e d else 0)
      - (if d = e then F db b a c e d else 0)
      + 2 * (if c = a then (if e = a then F db b a c e d else 0) else 0)
      + 2 * (if c = a then (if d = a then F db b a c e d else 0) else 0)
      + 2 * (if e = a then (if d = a then F db b a c e d else 0) else 0)
      + 2 * (if e = c then (if d = c then F db b a c e d else 0) else 0)
      + (if e = c then (if d = a then F db b a c e d else 0) else 0)
      + (if e = a then (if d = c then F db b a c e d else 0) else 0)
      + (if c = a then (if d = e then F db b a c e d else 0) else 0)
      - 6 * (if c = a then (if e = a then (if d = a then F db b a c e d else 0) else 0) else 0) := by
  exact jpb_pointwise a c e d (F db b)
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end OrderSupplier_CC

theorem sound_OrderSupplier_CollisionCorrection : OrderSupplier.CollisionCorrection.Goal := OrderSupplier_CC.sound_CollisionCorrection
namespace ChannelBridge_CC
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ChannelBridge
private def F (db : DB) (b : Bind) (c d e a : Nat) : Int := db 0 (b 0) c * (db 0 a c * (db 1 (b 0) d * (db 1 a d * (db 2 (b 0) e * db 2 a e))))
private theorem parts_eq : partitions pattern.freeVars = [[[2], [3], [4], [1]], [[2, 3], [4], [1]], [[3], [2, 4], [1]], [[3], [4], [2, 1]], [[2], [3, 4], [1]], [[2, 3, 4], [1]], [[3, 4], [2, 1]], [[2], [4], [3, 1]], [[2, 4], [3, 1]], [[4], [2, 3, 1]], [[2], [3], [4, 1]], [[2, 3], [4, 1]], [[3], [2, 4, 1]], [[2], [3, 4, 1]], [[2, 3, 4, 1]]] := by native_decide
private theorem hc1 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3], [4], [1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun e => rsum n (fun a => F db b c d e a)))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3], [4], [1]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => rsum n (fun a => F db b c c e a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2, 4], [1]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => rsum n (fun a => F db b c d c a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [4], [2, 1]] : List (List Nat)) = rsum n (fun d => rsum n (fun e => rsum n (fun c => F db b c d e c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3, 4], [1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun a => F db b c d d a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3, 4], [1]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b c c c a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 4], [2, 1]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => F db b c d d c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [4], [3, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => rsum n (fun d => F db b c d e d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 4], [3, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => F db b c d c d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [2, 3, 1]] : List (List Nat)) = rsum n (fun e => rsum n (fun c => F db b c c e c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3], [4, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun e => F db b c d e e))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3], [4, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => F db b c c e e)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2, 4, 1]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => F db b c d c c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3, 4, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => F db b c d d d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3, 4, 1]] : List (List Nat)) = rsum n (fun c => F db b c c c c) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun c => rsum n (fun d => rsum n (fun e => rsum n (fun a => if [c, d, e, a].Nodup then F db b c d e a else 0)))) := by
  simp [injective, pattern, integrate, value, put, rsum, F, List.Nodup, List.mem_cons]
private theorem hpoint (db : DB) (b : Bind) (c d e a : Nat) :
    (if (¬c = d ∧ ¬c = e ∧ ¬c = a) ∧ (¬d = e ∧ ¬d = a) ∧ ¬e = a then F db b c d e a else 0) =
      F db b c d e a
      - (if d = c then F db b c d e a else 0)
      - (if e = c then F db b c d e a else 0)
      - (if a = c then F db b c d e a else 0)
      - (if e = d then F db b c d e a else 0)
      - (if a = d then F db b c d e a else 0)
      - (if a = e then F db b c d e a else 0)
      + 2 * (if d = c then (if e = c then F db b c d e a else 0) else 0)
      + 2 * (if d = c then (if a = c then F db b c d e a else 0) else 0)
      + 2 * (if e = c then (if a = c then F db b c d e a else 0) else 0)
      + 2 * (if e = d then (if a = d then F db b c d e a else 0) else 0)
      + (if e = d then (if a = c then F db b c d e a else 0) else 0)
      + (if e = c then (if a = d then F db b c d e a else 0) else 0)
      + (if d = c then (if a = e then F db b c d e a else 0) else 0)
      - 6 * (if d = c then (if e = c then (if a = c then F db b c d e a else 0) else 0) else 0) := by
  exact jpb_pointwise c d e a (F db b)
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end ChannelBridge_CC

theorem sound_ChannelBridge_CollisionCorrection : ChannelBridge.CollisionCorrection.Goal := ChannelBridge_CC.sound_CollisionCorrection
namespace PromoInventory_CC
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.PromoInventory
private def F (db : DB) (b : Bind) (a c x0 d : Nat) : Int := db 0 x0 a * (db 0 a c * (db 0 c d * (db 0 d x0 * (db 2 x0 c * db 1 a d))))
private theorem parts_eq : partitions pattern.freeVars = [[[1], [2], [0], [3]], [[1, 2], [0], [3]], [[2], [1, 0], [3]], [[2], [0], [1, 3]], [[1], [2, 0], [3]], [[1, 2, 0], [3]], [[2, 0], [1, 3]], [[1], [0], [2, 3]], [[1, 0], [2, 3]], [[0], [1, 2, 3]], [[1], [2], [0, 3]], [[1, 2], [0, 3]], [[2], [1, 0, 3]], [[1], [2, 0, 3]], [[1, 2, 0, 3]]] := by native_decide
private theorem hc1 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [0], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun x0 => rsum n (fun d => F db b a c x0 d)))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [0], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun x0 => rsum n (fun d => F db b a a x0 d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1, 0], [3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => rsum n (fun d => F db b a c a d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [0], [1, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun x0 => rsum n (fun a => F db b a c x0 a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2, 0], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun d => F db b a c c d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2, 0], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db b a a a d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 0], [1, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b a c c a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [0], [2, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun x0 => rsum n (fun c => F db b a c x0 c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 0], [2, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a c a c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[0], [1, 2, 3]] : List (List Nat)) = rsum n (fun x0 => rsum n (fun a => F db b a a x0 a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [0, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun x0 => F db b a c x0 x0))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [0, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun x0 => F db b a a x0 x0)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1, 0, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b a c a a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2, 0, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a c c c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2, 0, 3]] : List (List Nat)) = rsum n (fun a => F db b a a a a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun a => rsum n (fun c => rsum n (fun x0 => rsum n (fun d => if [a, c, x0, d].Nodup then F db b a c x0 d else 0)))) := by
  simp [injective, pattern, integrate, value, put, rsum, F, List.Nodup, List.mem_cons]
private theorem hpoint (db : DB) (b : Bind) (a c x0 d : Nat) :
    (if (¬a = c ∧ ¬a = x0 ∧ ¬a = d) ∧ (¬c = x0 ∧ ¬c = d) ∧ ¬x0 = d then F db b a c x0 d else 0) =
      F db b a c x0 d
      - (if c = a then F db b a c x0 d else 0)
      - (if x0 = a then F db b a c x0 d else 0)
      - (if d = a then F db b a c x0 d else 0)
      - (if x0 = c then F db b a c x0 d else 0)
      - (if d = c then F db b a c x0 d else 0)
      - (if d = x0 then F db b a c x0 d else 0)
      + 2 * (if c = a then (if x0 = a then F db b a c x0 d else 0) else 0)
      + 2 * (if c = a then (if d = a then F db b a c x0 d else 0) else 0)
      + 2 * (if x0 = a then (if d = a then F db b a c x0 d else 0) else 0)
      + 2 * (if x0 = c then (if d = c then F db b a c x0 d else 0) else 0)
      + (if x0 = c then (if d = a then F db b a c x0 d else 0) else 0)
      + (if x0 = a then (if d = c then F db b a c x0 d else 0) else 0)
      + (if c = a then (if d = x0 then F db b a c x0 d else 0) else 0)
      - 6 * (if c = a then (if x0 = a then (if d = a then F db b a c x0 d else 0) else 0) else 0) := by
  exact jpb_pointwise a c x0 d (F db b)
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end PromoInventory_CC

theorem sound_PromoInventory_CollisionCorrection : PromoInventory.CollisionCorrection.Goal := PromoInventory_CC.sound_CollisionCorrection
namespace TwoHopAntiWitness_CC
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.TwoHopAntiWitness
private def F (db : DB) (b : Bind) (a d e c : Nat) : Int := db 0 (b 0) a * (db 1 a c * (db 2 (b 0) d * (db 1 d c * db 0 c e)))
private theorem parts_eq : partitions pattern.freeVars = [[[1], [3], [4], [2]], [[1, 3], [4], [2]], [[3], [1, 4], [2]], [[3], [4], [1, 2]], [[1], [3, 4], [2]], [[1, 3, 4], [2]], [[3, 4], [1, 2]], [[1], [4], [3, 2]], [[1, 4], [3, 2]], [[4], [1, 3, 2]], [[1], [3], [4, 2]], [[1, 3], [4, 2]], [[3], [1, 4, 2]], [[1], [3, 4, 2]], [[1, 3, 4, 2]]] := by native_decide
private theorem hc1 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [3], [4], [2]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => rsum n (fun e => rsum n (fun c => F db b a d e c)))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 3], [4], [2]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => rsum n (fun c => F db b a a e c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [1, 4], [2]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => rsum n (fun c => F db b a d a c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [4], [1, 2]] : List (List Nat)) = rsum n (fun d => rsum n (fun e => rsum n (fun a => F db b a d e a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [3, 4], [2]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => rsum n (fun c => F db b a d d c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 3, 4], [2]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a a a c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 4], [1, 2]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => F db b a d d a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [4], [3, 2]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => rsum n (fun d => F db b a d e d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 4], [3, 2]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db b a d a d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1, 3, 2]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => F db b a a e a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [3], [4, 2]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => rsum n (fun e => F db b a d e e))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 3], [4, 2]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db b a a e e)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [1, 4, 2]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => F db b a d a a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [3, 4, 2]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db b a d d d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 3, 4, 2]] : List (List Nat)) = rsum n (fun a => F db b a a a a) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun a => rsum n (fun d => rsum n (fun e => rsum n (fun c => if [a, d, e, c].Nodup then F db b a d e c else 0)))) := by
  simp [injective, pattern, integrate, value, put, rsum, F, List.Nodup, List.mem_cons]
private theorem hpoint (db : DB) (b : Bind) (a d e c : Nat) :
    (if (¬a = d ∧ ¬a = e ∧ ¬a = c) ∧ (¬d = e ∧ ¬d = c) ∧ ¬e = c then F db b a d e c else 0) =
      F db b a d e c
      - (if d = a then F db b a d e c else 0)
      - (if e = a then F db b a d e c else 0)
      - (if c = a then F db b a d e c else 0)
      - (if e = d then F db b a d e c else 0)
      - (if c = d then F db b a d e c else 0)
      - (if c = e then F db b a d e c else 0)
      + 2 * (if d = a then (if e = a then F db b a d e c else 0) else 0)
      + 2 * (if d = a then (if c = a then F db b a d e c else 0) else 0)
      + 2 * (if e = a then (if c = a then F db b a d e c else 0) else 0)
      + 2 * (if e = d then (if c = d then F db b a d e c else 0) else 0)
      + (if e = d then (if c = a then F db b a d e c else 0) else 0)
      + (if e = a then (if c = d then F db b a d e c else 0) else 0)
      + (if d = a then (if c = e then F db b a d e c else 0) else 0)
      - 6 * (if d = a then (if e = a then (if c = a then F db b a d e c else 0) else 0) else 0) := by
  exact jpb_pointwise a d e c (F db b)
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end TwoHopAntiWitness_CC

theorem sound_TwoHopAntiWitness_CollisionCorrection : TwoHopAntiWitness.CollisionCorrection.Goal := TwoHopAntiWitness_CC.sound_CollisionCorrection
namespace SupplierClique_CC
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique
private def F (db : DB) (b : Bind) (d c a x0 : Nat) : Int := db 0 x0 a * (db 0 x0 c * (db 0 x0 d * (db 0 a c * (db 0 a d * db 0 c d))))
private theorem parts_eq : partitions pattern.freeVars = [[[3], [2], [1], [0]], [[3, 2], [1], [0]], [[2], [3, 1], [0]], [[2], [1], [3, 0]], [[3], [2, 1], [0]], [[3, 2, 1], [0]], [[2, 1], [3, 0]], [[3], [1], [2, 0]], [[3, 1], [2, 0]], [[1], [3, 2, 0]], [[3], [2], [1, 0]], [[3, 2], [1, 0]], [[2], [3, 1, 0]], [[3], [2, 1, 0]], [[3, 2, 1, 0]]] := by native_decide
private theorem hc1 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2], [1], [0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => rsum n (fun a => rsum n (fun x0 => F db b d c a x0)))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 2], [1], [0]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => rsum n (fun x0 => F db b d d a x0))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3, 1], [0]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun x0 => F db b d c d x0))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1], [3, 0]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => rsum n (fun d => F db b d c a d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2, 1], [0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => rsum n (fun x0 => F db b d c c x0))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 2, 1], [0]] : List (List Nat)) = rsum n (fun d => rsum n (fun x0 => F db b d d d x0)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 1], [3, 0]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => F db b d c c d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [1], [2, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => rsum n (fun c => F db b d c a c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 1], [2, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => F db b d c d c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [3, 2, 0]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db b d d a d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2], [1, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => rsum n (fun a => F db b d c a a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 2], [1, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => F db b d d a a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3, 1, 0]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => F db b d c d d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2, 1, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => F db b d c c c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 2, 1, 0]] : List (List Nat)) = rsum n (fun d => F db b d d d d) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun d => rsum n (fun c => rsum n (fun a => rsum n (fun x0 => if [d, c, a, x0].Nodup then F db b d c a x0 else 0)))) := by
  simp [injective, pattern, integrate, value, put, rsum, F, List.Nodup, List.mem_cons]
private theorem hpoint (db : DB) (b : Bind) (d c a x0 : Nat) :
    (if (¬d = c ∧ ¬d = a ∧ ¬d = x0) ∧ (¬c = a ∧ ¬c = x0) ∧ ¬a = x0 then F db b d c a x0 else 0) =
      F db b d c a x0
      - (if c = d then F db b d c a x0 else 0)
      - (if a = d then F db b d c a x0 else 0)
      - (if x0 = d then F db b d c a x0 else 0)
      - (if a = c then F db b d c a x0 else 0)
      - (if x0 = c then F db b d c a x0 else 0)
      - (if x0 = a then F db b d c a x0 else 0)
      + 2 * (if c = d then (if a = d then F db b d c a x0 else 0) else 0)
      + 2 * (if c = d then (if x0 = d then F db b d c a x0 else 0) else 0)
      + 2 * (if a = d then (if x0 = d then F db b d c a x0 else 0) else 0)
      + 2 * (if a = c then (if x0 = c then F db b d c a x0 else 0) else 0)
      + (if a = c then (if x0 = d then F db b d c a x0 else 0) else 0)
      + (if a = d then (if x0 = c then F db b d c a x0 else 0) else 0)
      + (if c = d then (if x0 = a then F db b d c a x0 else 0) else 0)
      - 6 * (if c = d then (if a = d then (if x0 = d then F db b d c a x0 else 0) else 0) else 0) := by
  exact jpb_pointwise d c a x0 (F db b)
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end SupplierClique_CC

theorem sound_SupplierClique_CollisionCorrection : SupplierClique.CollisionCorrection.Goal := SupplierClique_CC.sound_CollisionCorrection
namespace ParallelLineage_CC
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.ParallelLineage
private def F (db : DB) (b : Bind) (e a c d : Nat) : Int := db 0 (b 0) a * (db 0 a c * (db 0 (b 0) a * (db 1 c d * (db 1 d (b 0) * db 2 d e))))
private theorem parts_eq : partitions pattern.freeVars = [[[4], [1], [2], [3]], [[4, 1], [2], [3]], [[1], [4, 2], [3]], [[1], [2], [4, 3]], [[4], [1, 2], [3]], [[4, 1, 2], [3]], [[1, 2], [4, 3]], [[4], [2], [1, 3]], [[4, 2], [1, 3]], [[2], [4, 1, 3]], [[4], [1], [2, 3]], [[4, 1], [2, 3]], [[1], [4, 2, 3]], [[4], [1, 2, 3]], [[4, 1, 2, 3]]] := by native_decide
private theorem hc1 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1], [2], [3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => rsum n (fun c => rsum n (fun d => F db b e a c d)))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 1], [2], [3]] : List (List Nat)) = rsum n (fun e => rsum n (fun c => rsum n (fun d => F db b e e c d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [4, 2], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => rsum n (fun d => F db b e a e d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun e => F db b e a c e))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1, 2], [3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => rsum n (fun d => F db b e a a d))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 1, 2], [3]] : List (List Nat)) = rsum n (fun e => rsum n (fun d => F db b e e e d)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db b e a a e)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [2], [1, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun c => rsum n (fun a => F db b e a c a))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 2], [1, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => F db b e a e a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [4, 1, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => F db b e e c e)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1], [2, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => rsum n (fun c => F db b e a c c))) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 1], [2, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun c => F db b e e c c)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [4, 2, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db b e a e e)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1, 2, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => F db b e a a a)) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 1, 2, 3]] : List (List Nat)) = rsum n (fun e => F db b e e e e) := by
  simp [collapsed, pattern, integrate, value, put, rsum, F, List.filterMap, List.head?]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun e => rsum n (fun a => rsum n (fun c => rsum n (fun d => if [e, a, c, d].Nodup then F db b e a c d else 0)))) := by
  simp [injective, pattern, integrate, value, put, rsum, F, List.Nodup, List.mem_cons]
private theorem hpoint (db : DB) (b : Bind) (e a c d : Nat) :
    (if (¬e = a ∧ ¬e = c ∧ ¬e = d) ∧ (¬a = c ∧ ¬a = d) ∧ ¬c = d then F db b e a c d else 0) =
      F db b e a c d
      - (if a = e then F db b e a c d else 0)
      - (if c = e then F db b e a c d else 0)
      - (if d = e then F db b e a c d else 0)
      - (if c = a then F db b e a c d else 0)
      - (if d = a then F db b e a c d else 0)
      - (if d = c then F db b e a c d else 0)
      + 2 * (if a = e then (if c = e then F db b e a c d else 0) else 0)
      + 2 * (if a = e then (if d = e then F db b e a c d else 0) else 0)
      + 2 * (if c = e then (if d = e then F db b e a c d else 0) else 0)
      + 2 * (if c = a then (if d = a then F db b e a c d else 0) else 0)
      + (if c = a then (if d = e then F db b e a c d else 0) else 0)
      + (if c = e then (if d = a then F db b e a c d else 0) else 0)
      + (if a = e then (if d = c then F db b e a c d else 0) else 0)
      - 6 * (if a = e then (if c = e then (if d = e then F db b e a c d else 0) else 0) else 0) := by
  exact jpb_pointwise e a c d (F db b)
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end ParallelLineage_CC

theorem sound_ParallelLineage_CollisionCorrection : ParallelLineage.CollisionCorrection.Goal := ParallelLineage_CC.sound_CollisionCorrection


/-! ReturnedItem.CollisionCorrection -/
theorem sound_ReturnedItem_CollisionCorrection : ReturnedItem.CollisionCorrection.Goal := by
  intro n db b
  let W : Nat → Nat → Nat → Int := fun x y z =>
    db 0 (b 0) x * (db 1 x y * (db 2 y z * (db 3 z (b 0) * db 2 (b 0) y)))
  have hinj : injective ReturnedItem.pattern n db b =
      rsum n (fun x => rsum n (fun y => rsum n (fun z =>
        if (x ≠ y ∧ x ≠ z) ∧ y ≠ z then W x y z else 0))) := by
    simp [injective, ReturnedItem.pattern, integrate, value, put, rsum, W, List.Nodup, List.mem_cons, List.mem_nil_iff, not_or]
  have hc1 : collapsed ReturnedItem.pattern n db b ([[1], [2], [3]] : List (List Nat)) =
      rsum n (fun x => rsum n (fun y => rsum n (fun z => W x y z))) := by
    simp [collapsed, ReturnedItem.pattern, integrate, value, put, rsum, W, List.filterMap, List.head?]
  have hc2 : collapsed ReturnedItem.pattern n db b ([[1, 2], [3]] : List (List Nat)) =
      rsum n (fun x => rsum n (fun z => W x x z)) := by
    simp [collapsed, ReturnedItem.pattern, integrate, value, put, rsum, W, List.filterMap, List.head?]
  have hc3 : collapsed ReturnedItem.pattern n db b ([[2], [1, 3]] : List (List Nat)) =
      rsum n (fun y => rsum n (fun x => W x y x)) := by
    -- match partition [[2],[1,3]] gives representatives 2,1 mapping: 1->1,2->2,3->1 so x->1,y->2,z->1
    have : collapsed ReturnedItem.pattern n db b ([[2], [1, 3]] : List (List Nat)) =
        rsum n (fun y => rsum n (fun x => W x y x)) := by
      simp [collapsed, ReturnedItem.pattern, integrate, value, put, rsum, W, List.filterMap, List.head?]
    exact this
  have hc4 : collapsed ReturnedItem.pattern n db b ([[1], [2, 3]] : List (List Nat)) =
      rsum n (fun x => rsum n (fun y => W x y y)) := by
    simp [collapsed, ReturnedItem.pattern, integrate, value, put, rsum, W, List.filterMap, List.head?]
  have hc5 : collapsed ReturnedItem.pattern n db b ([[1, 2, 3]] : List (List Nat)) =
      rsum n (fun x => W x x x) := by
    simp [collapsed, ReturnedItem.pattern, integrate, value, put, rsum, W, List.filterMap, List.head?]
  have hparts : partitions ReturnedItem.pattern.freeVars =
      [[[1], [2], [3]], [[1, 2], [3]], [[2], [1, 3]], [[1], [2, 3]], [[1, 2, 3]]] := by native_decide
  rw [hinj, hparts]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5]
  have h := triple_inclusion n W
  have hfun : (fun x => rsum n (fun y => rsum n (fun z =>
      if (x ≠ y ∧ x ≠ z) ∧ y ≠ z then W x y z else 0))) =
      (fun x => rsum n (fun y => rsum n (fun z =>
      if x ≠ y ∧ x ≠ z ∧ y ≠ z then W x y z else 0))) := by
        funext x; apply rsum_congr; intro y hy; apply rsum_congr; intro z hz
        by_cases hxy : x = y <;> by_cases hxz : x = z <;> by_cases hyz : y = z <;> simp [hxy, hxz, hyz]
  rw [hfun, h]
  have hswap : rsum n (fun a => rsum n (fun b => W a b a)) =
      rsum n (fun b => rsum n (fun a => W a b a)) :=
    rsum_comm n n (fun a b => W a b a)
  rw [hswap]
  omega
-- BEGIN jpb-copies (verbatim from READ-ONLY reference proofs; inlined because
-- their .olean modules are missing or stale in the import path).

-- BEGIN jpb-copy Task72 (CountryReturn FractionalCap)
namespace TinylambOpt.ResearchProofs.Round2.Task72

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn

/-! ## Generic finite-range and integration helpers (private copies). -/

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n + 1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        rw [rsum_zero]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n + 1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_eq_zero_of_zero (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → f i = 0) :
    rsum n f = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]
      simp

private theorem le_rsum_of_mem (n i : Nat) (f : Nat → Int) (hi : i < n)
    (hf : ∀ j, j < n → 0 ≤ f j) : f i ≤ rsum n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases hin : i < n
      · have hrec := ih hin (fun j hj => hf j (Nat.lt_succ_of_lt hj))
        have hn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
        omega
      · have : i = n := by omega
        subst i
        have hpre : 0 ≤ rsum n f := rsum_nonneg n f (fun j hj => hf j (Nat.lt_succ_of_lt hj))
        omega

private theorem eq_zero_of_rsum_eq_zero (n : Nat) (f : Nat → Int)
    (hf : ∀ i, i < n → 0 ≤ f i) (hs : rsum n f = 0) : ∀ i, i < n → f i = 0 := by
  induction n with
  | zero => intro i hi; omega
  | succ n ih =>
      rw [rsum_succ] at hs
      have hpre : 0 ≤ rsum n f := rsum_nonneg n f (fun i hi => hf i (Nat.lt_succ_of_lt hi))
      have hn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
      have hpre0 : rsum n f = 0 := by omega
      have hn0 : f n = 0 := by omega
      intro i hi
      by_cases hin : i < n
      · exact ih (fun j hj => hf j (Nat.lt_succ_of_lt hj)) hpre0 i hin
      · have : i = n := by omega
        subst i
        exact hn0

private theorem sum_bool {α} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then (1 : Int) else 0)).sum = (l.countP p : Int) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h, ih]; omega
      · have h' : p a = false := by cases hpa : p a <;> simp_all
        simp [h', ih]

private theorem int_sum_nat (l : List Nat) : (l.map (fun x : Nat => (x : Int))).sum = (l.sum : Int) := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem int_countP_eq_rsum (n : Nat) (p : Nat → Bool) :
    ((List.range n).countP p : Int) = rsum n (fun x => if p x then (1 : Int) else 0) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      have hcast : ((List.countP p (List.range n) + List.countP p [n] : Nat) : Int) =
          (List.countP p (List.range n) : Int) + (List.countP p [n] : Int) := by omega
      rw [hcast, ih]
      rw [rsum_succ]
      cases h : p n <;> simp [h]

private theorem int_list_sum_countP (l : List Nat) (n : Nat) (f : Nat → Nat → Bool) :
    ((l.map (fun x => (List.range n).countP (f x))).sum : Int) =
      (l.map (fun x => rsum n (fun y => if f x y then (1 : Int) else 0))).sum := by
  induction l with
  | nil => simp
  | cons x l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hcast : ((List.countP (f x) (List.range n) +
          (List.map (fun x => List.countP (f x) (List.range n)) l).sum : Nat) : Int) =
          (List.countP (f x) (List.range n) : Int) +
          ((List.map (fun x => List.countP (f x) (List.range n)) l).sum : Int) := by omega
      rw [hcast, int_countP_eq_rsum, ih]

/-! ## CountryReturn indicator model. -/

private def indA (db : DB) (b : Bind) (x : Nat) : Int := if db 0 (b 0) x == 0 then 0 else 1
private def indB (db : DB) (x y : Nat) : Int := if db 1 x y == 0 then 0 else 1
private def indC (db : DB) (x y : Nat) : Int := if db 2 x y == 0 then 0 else 1
private def indD (db : DB) (x y : Nat) : Int := if db 1 x y == 0 then 0 else 1
private def indE (db : DB) (b : Bind) (x : Nat) : Int := if db 2 (b 0) x == 0 then 0 else 1

private theorem ne_zero_false_of_beq_zero {z : Int} (h : z == 0) : (z != 0) = false := by
  have hz : z = 0 := beq_iff_eq.mp h
  simp [hz]

private theorem ne_zero_true_of_not_beq_zero {z : Int} (h : ¬ z == 0) : (z != 0) = true := by
  cases h' : z != 0
  · have hz : z = 0 := by
      have : (z == 0) = true := by simpa using h'
      exact beq_iff_eq.mp this
    have hbeq : (z == 0) = true := by simp [hz]
    exact False.elim (h hbeq)
  · rfl

private theorem indA_spec (db : DB) (b : Bind) (x : Nat) :
    indA db b x = if db 0 (b 0) x != 0 then 1 else 0 := by
  by_cases h : db 0 (b 0) x == 0
  · simp [indA, h, ne_zero_false_of_beq_zero h]
  · simp [indA, h, ne_zero_true_of_not_beq_zero h]
private theorem indB_spec (db : DB) (x y : Nat) :
    indB db x y = if db 1 x y != 0 then 1 else 0 := by
  by_cases h : db 1 x y == 0
  · simp [indB, h, ne_zero_false_of_beq_zero h]
  · simp [indB, h, ne_zero_true_of_not_beq_zero h]
private theorem indC_spec (db : DB) (x y : Nat) :
    indC db x y = if db 2 x y != 0 then 1 else 0 := by
  by_cases h : db 2 x y == 0
  · simp [indC, h, ne_zero_false_of_beq_zero h]
  · simp [indC, h, ne_zero_true_of_not_beq_zero h]
private theorem indD_spec (db : DB) (x y : Nat) :
    indD db x y = if db 1 x y != 0 then 1 else 0 := by
  by_cases h : db 1 x y == 0
  · simp [indD, h, ne_zero_false_of_beq_zero h]
  · simp [indD, h, ne_zero_true_of_not_beq_zero h]
private theorem indE_spec (db : DB) (b : Bind) (x : Nat) :
    indE db b x = if db 2 (b 0) x != 0 then 1 else 0 := by
  by_cases h : db 2 (b 0) x == 0
  · simp [indE, h, ne_zero_false_of_beq_zero h]
  · simp [indE, h, ne_zero_true_of_not_beq_zero h]

private theorem indA_nonneg (db : DB) (b : Bind) (x : Nat) : 0 ≤ indA db b x := by
  rw [indA_spec]; split <;> omega
private theorem indB_nonneg (db : DB) (x y : Nat) : 0 ≤ indB db x y := by
  rw [indB_spec]; split <;> omega
private theorem indC_nonneg (db : DB) (x y : Nat) : 0 ≤ indC db x y := by
  rw [indC_spec]; split <;> omega
private theorem indD_nonneg (db : DB) (x y : Nat) : 0 ≤ indD db x y := by
  rw [indD_spec]; split <;> omega
private theorem indE_nonneg (db : DB) (b : Bind) (x : Nat) : 0 ≤ indE db b x := by
  rw [indE_spec]; split <;> omega
private theorem indA_le_one (db : DB) (b : Bind) (x : Nat) : indA db b x ≤ 1 := by
  rw [indA_spec]; split <;> omega
private theorem indB_le_one (db : DB) (x y : Nat) : indB db x y ≤ 1 := by
  rw [indB_spec]; split <;> omega
private theorem indC_le_one (db : DB) (x y : Nat) : indC db x y ≤ 1 := by
  rw [indC_spec]; split <;> omega
private theorem indD_le_one (db : DB) (x y : Nat) : indD db x y ≤ 1 := by
  rw [indD_spec]; split <;> omega
private theorem indE_le_one (db : DB) (b : Bind) (x : Nat) : indE db b x ≤ 1 := by
  rw [indE_spec]; split <;> omega

private def Rfull (n : Nat) (db : DB) (b : Bind) : Int :=
  rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))

private theorem ref_indicator (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b = Rfull n db b := by
  simp [reference, integrate, pattern, value, put, rsum, Rfull,
    indA, indB, indC, indD, indE]

private theorem Rfull_nonneg (n : Nat) (db : DB) (b : Bind) : 0 ≤ Rfull n db b := by
  dsimp [Rfull]
  apply rsum_nonneg; intro x1 hx1
  apply rsum_nonneg; intro x2 hx2
  apply rsum_nonneg; intro x3 hx3
  apply rsum_nonneg; intro x4 hx4
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

/-! ## Support counts and their rsum forms. -/

private def s0 (n : Nat) (db : DB) (b : Bind) : Nat := (List.range n).countP (fun x => db 0 (b 0) x != 0)
private def s1 (n : Nat) (db : DB) : Nat :=
  ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))).sum
private def s2 (n : Nat) (db : DB) : Nat :=
  ((List.range n).map (fun x => (List.range n).countP (fun y => db 2 x y != 0))).sum
private def s3 (n : Nat) (db : DB) : Nat :=
  ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))).sum
private def s4 (n : Nat) (db : DB) (b : Bind) : Nat := (List.range n).countP (fun x => db 2 (b 0) x != 0)

private theorem support0_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨0, 0, 1⟩ : Atom) pattern b = s0 n db b := by
  simp [support, pattern, s0]

private theorem support1_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨1, 1, 2⟩ : Atom) pattern b = s1 n db := by
  simp [support, pattern, s1]

private theorem support2_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨2, 2, 3⟩ : Atom) pattern b = s2 n db := by
  simp [support, pattern, s2]

private theorem support3_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨1, 3, 4⟩ : Atom) pattern b = s3 n db := by
  simp [support, pattern, s3]

private theorem support4_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨2, 0, 4⟩ : Atom) pattern b = s4 n db b := by
  simp [support, pattern, s4]

private theorem s1_eq_s3 (n : Nat) (db : DB) : s1 n db = s3 n db := by rfl

private theorem rsum_indA (n : Nat) (db : DB) (b : Bind) :
    rsum n (fun x => indA db b x) = (s0 n db b : Int) := by
  rw [show rsum n (fun x => indA db b x) =
      rsum n (fun x => if db 0 (b 0) x != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indA_spec db b i]
  unfold rsum s0
  exact sum_bool (List.range n) (fun x => db 0 (b 0) x != 0)

private theorem rsum_indB (n : Nat) (db : DB) (x : Nat) :
    rsum n (fun y => indB db x y) =
      ((List.range n).countP (fun y => db 1 x y != 0) : Int) := by
  rw [show rsum n (fun y => indB db x y) =
      rsum n (fun y => if db 1 x y != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indB_spec db x i]
  unfold rsum
  exact sum_bool (List.range n) (fun y => db 1 x y != 0)

private theorem rsum_indC (n : Nat) (db : DB) (x : Nat) :
    rsum n (fun y => indC db x y) =
      ((List.range n).countP (fun y => db 2 x y != 0) : Int) := by
  rw [show rsum n (fun y => indC db x y) =
      rsum n (fun y => if db 2 x y != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indC_spec db x i]
  unfold rsum
  exact sum_bool (List.range n) (fun y => db 2 x y != 0)

private theorem rsum_indD (n : Nat) (db : DB) (x : Nat) :
    rsum n (fun y => indD db x y) =
      ((List.range n).countP (fun y => db 1 x y != 0) : Int) := by
  rw [show rsum n (fun y => indD db x y) =
      rsum n (fun y => if db 1 x y != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indD_spec db x i]
  unfold rsum
  exact sum_bool (List.range n) (fun y => db 1 x y != 0)

private theorem rsum_indE (n : Nat) (db : DB) (b : Bind) :
    rsum n (fun x => indE db b x) = (s4 n db b : Int) := by
  rw [show rsum n (fun x => indE db b x) =
      rsum n (fun x => if db 2 (b 0) x != 0 then 1 else 0) by
    apply rsum_congr; intro i hi; exact indE_spec db b i]
  unfold rsum s4
  exact sum_bool (List.range n) (fun x => db 2 (b 0) x != 0)

private theorem rsum_indB2 (n : Nat) (db : DB) :
    rsum n (fun x => rsum n (fun y => indB db x y)) = (s1 n db : Int) := by
  simp only [rsum_indB]
  unfold rsum s1
  simpa [List.map_map, Function.comp_def] using
    (int_sum_nat ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))))

private theorem rsum_indC2 (n : Nat) (db : DB) :
    rsum n (fun x => rsum n (fun y => indC db x y)) = (s2 n db : Int) := by
  simp only [rsum_indC]
  unfold rsum s2
  simpa [List.map_map, Function.comp_def] using
    (int_sum_nat ((List.range n).map (fun x => (List.range n).countP (fun y => db 2 x y != 0))))

private theorem rsum_indD2 (n : Nat) (db : DB) :
    rsum n (fun x => rsum n (fun y => indD db x y)) = (s3 n db : Int) := by
  simp only [rsum_indD]
  unfold rsum s3
  simpa [List.map_map, Function.comp_def] using
    (int_sum_nat ((List.range n).map (fun x => (List.range n).countP (fun y => db 1 x y != 0))))

private theorem rowB_le_s3 (n : Nat) (db : DB) (x : Nat) (hx : x < n) :
    rsum n (fun y => indB db x y) ≤ (s3 n db : Int) := by
  rw [← rsum_indD2 n db]
  rw [show (fun y => indB db x y) = (fun y => indD db x y) by
    funext y
    rw [indB_spec, indD_spec]]
  exact le_rsum_of_mem n x (fun i => rsum n (fun y => indD db i y)) hx
    (fun i hi => rsum_nonneg n (fun y => indD db i y) (fun y hy => indD_nonneg db i y))

private theorem rowC_le_s2 (n : Nat) (db : DB) (x : Nat) (hx : x < n) :
    rsum n (fun y => indC db x y) ≤ (s2 n db : Int) := by
  rw [← rsum_indC2 n db]
  exact le_rsum_of_mem n x (fun i => rsum n (fun y => indC db i y)) hx
    (fun i hi => rsum_nonneg n (fun y => indC db i y) (fun y hy => indC_nonneg db i y))

/-! ## The four integral-cover bounds. -/

private theorem quad_product1 (n : Nat) (f : Nat → Int) (g : Nat → Nat → Int) (h : Nat → Int) :
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      f x1 * g x2 x3 * h x4)))) =
      (rsum n f) * (rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) * (rsum n h) := by
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      f x1 * g x2 x3 * h x4))))
        = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            (f x1 * g x2 x3) * rsum n h))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              apply rsum_congr; intro x3 hx3
              rw [rsum_const_mul]
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            (f x1 * rsum n h) * rsum n (fun x3 => g x2 x3))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              rw [show (fun x3 => (f x1 * g x2 x3) * rsum n h) =
                  (fun x3 => (f x1 * rsum n h) * g x2 x3) by
                funext x3; grind]
              rw [rsum_const_mul]
    _ = rsum n (fun x1 =>
            (f x1 * rsum n h) * rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) := by
              apply rsum_congr; intro x1 hx1
              rw [rsum_const_mul]
    _ = (rsum n f) * (rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) * (rsum n h) := by
              rw [show (fun x1 => (f x1 * rsum n h) *
                    rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) =
                  (fun x1 => ((rsum n (fun x2 => rsum n (fun x3 => g x2 x3))) *
                    rsum n h) * f x1) by
                funext x1; grind]
              rw [rsum_const_mul]
              grind

private theorem quad_product2 (n : Nat) (g : Nat → Nat → Int) (h : Nat → Nat → Int) :
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      g x1 x2 * h x3 x4)))) =
      (rsum n (fun x1 => rsum n (fun x2 => g x1 x2))) *
        (rsum n (fun x3 => rsum n (fun x4 => h x3 x4))) := by
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      g x1 x2 * h x3 x4))))
        = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            g x1 x2 * rsum n (fun x4 => h x3 x4)))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              apply rsum_congr; intro x3 hx3
              rw [rsum_const_mul]
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            g x1 x2 * rsum n (fun x3 => rsum n (fun x4 => h x3 x4)))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              rw [rsum_const_mul]
    _ = rsum n (fun x1 =>
          (rsum n (fun x2 => g x1 x2)) * (rsum n (fun x3 => rsum n (fun x4 => h x3 x4)))) := by
              apply rsum_congr; intro x1 hx1
              rw [rsum_mul_const]
    _ = (rsum n (fun x1 => rsum n (fun x2 => g x1 x2))) *
          (rsum n (fun x3 => rsum n (fun x4 => h x3 x4))) := by
              rw [rsum_mul_const]

private theorem term_le_ACE (db : DB) (b : Bind) (x1 x2 x3 x4 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4))) ≤
      indA db b x1 * indC db x2 x3 * indE db b x4 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

private theorem term_le_BD (db : DB) (b : Bind) (x1 x2 x3 x4 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4))) ≤
      indB db x1 x2 * indD db x3 x4 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

private theorem term_le_ACD (db : DB) (b : Bind) (x1 x2 x3 x4 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4))) ≤
      indA db b x1 * indC db x2 x3 * indD db x3 x4 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

private theorem term_le_BCE (db : DB) (b : Bind) (x1 x2 x3 x4 : Nat) :
    indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4))) ≤
      indB db x1 x2 * indC db x2 x3 * indE db b x4 := by
  rw [indA_spec, indB_spec, indC_spec, indD_spec, indE_spec]
  split <;> split <;> split <;> split <;> split <;> omega

private theorem full_le_s0s2s4 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ (s0 n db b : Int) * (s2 n db : Int) * (s4 n db b : Int) := by
  rw [Rfull]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indA db b x1 * indC db x2 x3 * indE db b x4)))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              apply rsum_le; intro x4 hx4
              exact term_le_ACE db b x1 x2 x3 x4
    _ = (s0 n db b : Int) * (s2 n db : Int) * (s4 n db b : Int) := by
              rw [quad_product1 n (fun x => indA db b x) (fun x y => indC db x y)
                (fun x => indE db b x)]
              rw [rsum_indA, rsum_indC2, rsum_indE]

private theorem full_le_s1s3 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ (s1 n db : Int) * (s3 n db : Int) := by
  rw [Rfull]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indB db x1 x2 * indD db x3 x4)))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              apply rsum_le; intro x4 hx4
              exact term_le_BD db b x1 x2 x3 x4
    _ = (s1 n db : Int) * (s3 n db : Int) := by
              rw [quad_product2 n (fun x y => indB db x y) (fun x y => indD db x y)]
              rw [rsum_indB2, rsum_indD2]

private theorem pull_const_triple (n : Nat) (c : Int) (f : Nat → Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y => rsum n (fun z => c * f x y z))) =
      c * rsum n (fun x => rsum n (fun y => rsum n (fun z => f x y z))) := by
  calc
    rsum n (fun x => rsum n (fun y => rsum n (fun z => c * f x y z)))
        = rsum n (fun x => rsum n (fun y => c * rsum n (fun z => f x y z))) := by
            apply rsum_congr; intro x hx
            apply rsum_congr; intro y hy
            rw [rsum_const_mul]
    _ = rsum n (fun x => c * rsum n (fun y => rsum n (fun z => f x y z))) := by
            apply rsum_congr; intro x hx
            rw [rsum_const_mul]
    _ = c * rsum n (fun x => rsum n (fun y => rsum n (fun z => f x y z))) := by
            rw [rsum_const_mul]

private theorem cd_triple_le (n : Nat) (db : DB) :
    rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indC db x2 x3 * indD db x3 x4))) ≤ (s2 n db : Int) * (s3 n db : Int) := by
  calc
    rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indC db x2 x3 * indD db x3 x4)))
        = rsum n (fun x2 => rsum n (fun x3 =>
            indC db x2 x3 * rsum n (fun x4 => indD db x3 x4))) := by
              apply rsum_congr; intro x2 hx2
              apply rsum_congr; intro x3 hx3
              rw [rsum_const_mul]
    _ ≤ rsum n (fun x2 => rsum n (fun x3 =>
            indC db x2 x3 * (s3 n db : Int))) := by
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              have hC : 0 ≤ indC db x2 x3 := indC_nonneg db x2 x3
              exact Int.mul_le_mul_of_nonneg_left (rowB_le_s3 n db x3 hx3) hC
    _ = rsum n (fun x2 => (rsum n (fun x3 => indC db x2 x3)) * (s3 n db : Int)) := by
              apply rsum_congr; intro x2 hx2
              rw [rsum_mul_const]
    _ = (rsum n (fun x2 => rsum n (fun x3 => indC db x2 x3))) * (s3 n db : Int) := by
              rw [rsum_mul_const]
    _ = (s2 n db : Int) * (s3 n db : Int) := by
              rw [rsum_indC2]

private theorem full_le_s0s2s3 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ (s0 n db b : Int) * (s2 n db : Int) * (s3 n db : Int) := by
  rw [Rfull]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indA db b x1 * indC db x2 x3 * indD db x3 x4)))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              apply rsum_le; intro x4 hx4
              exact term_le_ACD db b x1 x2 x3 x4
    _ = rsum n (fun x1 => indA db b x1 *
          rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indC db x2 x3 * indD db x3 x4)))) := by
              apply rsum_congr; intro x1 hx1
              rw [show (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                    indA db b x1 * indC db x2 x3 * indD db x3 x4))) =
                  (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                    indA db b x1 * (indC db x2 x3 * indD db x3 x4)))) by
                funext x2
                apply congrArg List.sum
                apply List.map_congr_left
                intro x3 hx3
                apply congrArg List.sum
                apply List.map_congr_left
                intro x4 hx4
                grind]
              rw [show (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                    indA db b x1 * (indC db x2 x3 * indD db x3 x4)))) =
                  (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
                    (indA db b x1) * (indC db x2 x3 * indD db x3 x4)))) by rfl]
              rw [pull_const_triple]
    _ ≤ rsum n (fun x1 => indA db b x1 * ((s2 n db : Int) * (s3 n db : Int))) := by
              apply rsum_le; intro x1 hx1
              have hA : 0 ≤ indA db b x1 := indA_nonneg db b x1
              exact Int.mul_le_mul_of_nonneg_left (cd_triple_le n db) hA
    _ = (s0 n db b : Int) * (s2 n db : Int) * (s3 n db : Int) := by
              rw [rsum_mul_const]
              rw [rsum_indA]
              grind

private theorem full_le_s1s2s4 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ (s1 n db : Int) * (s2 n db : Int) * (s4 n db b : Int) := by
  rw [Rfull]
  calc
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
      indA db b x1 * (indB db x1 x2 * (indC db x2 x3 * (indD db x3 x4 * indE db b x4)))))))
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 =>
            indB db x1 x2 * indC db x2 x3 * indE db b x4)))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x3 hx3
              apply rsum_le; intro x4 hx4
              exact term_le_BCE db b x1 x2 x3 x4
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            (indB db x1 x2 * rsum n (fun x3 => indC db x2 x3)) *
              (s4 n db b : Int))) := by
              apply rsum_congr; intro x1 hx1
              apply rsum_congr; intro x2 hx2
              rw [show (fun x3 => rsum n (fun x4 =>
                    indB db x1 x2 * indC db x2 x3 * indE db b x4)) =
                  (fun x3 => (indB db x1 x2 * indC db x2 x3) * rsum n (fun x4 => indE db b x4)) by
                funext x3
                rw [show (fun x4 => indB db x1 x2 * indC db x2 x3 * indE db b x4) =
                    (fun x4 => (indB db x1 x2 * indC db x2 x3) * indE db b x4) by
                  funext x4; grind]
                rw [rsum_const_mul]]
              rw [show (fun x3 => (indB db x1 x2 * indC db x2 x3) * rsum n (fun x4 => indE db b x4)) =
                  (fun x3 => (rsum n (fun x4 => indE db b x4)) * (indB db x1 x2 * indC db x2 x3)) by
                funext x3; grind]
              rw [rsum_const_mul]
              rw [rsum_const_mul]
              rw [rsum_indE]
              grind
    _ ≤ rsum n (fun x1 => rsum n (fun x2 =>
            (indB db x1 x2 * (s2 n db : Int)) * (s4 n db b : Int))) := by
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              have hB : 0 ≤ indB db x1 x2 := indB_nonneg db x1 x2
              have hs4 : 0 ≤ (s4 n db b : Int) := by exact_mod_cast (Nat.zero_le (s4 n db b))
              have hrow : rsum n (fun x3 => indC db x2 x3) ≤ (s2 n db : Int) := rowC_le_s2 n db x2 hx2
              have hm : indB db x1 x2 * rsum n (fun x3 => indC db x2 x3) ≤
                  indB db x1 x2 * (s2 n db : Int) :=
                Int.mul_le_mul_of_nonneg_left hrow hB
              exact Int.mul_le_mul_of_nonneg_right hm hs4
    _ = (s1 n db : Int) * (s2 n db : Int) * (s4 n db b : Int) := by
              have hinner : ∀ x1, rsum n (fun x2 =>
                    (indB db x1 x2 * (s2 n db : Int)) * (s4 n db b : Int)) =
                  (rsum n (fun x2 => indB db x1 x2)) * ((s2 n db : Int) * (s4 n db b : Int)) := by
                intro x1
                rw [show (fun x2 => (indB db x1 x2 * (s2 n db : Int)) * (s4 n db b : Int)) =
                    (fun x2 => indB db x1 x2 * ((s2 n db : Int) * (s4 n db b : Int))) by
                  funext x2; grind]
                rw [rsum_mul_const]
              rw [show (fun x1 => rsum n (fun x2 =>
                    (indB db x1 x2 * (s2 n db : Int)) * (s4 n db b : Int))) =
                  (fun x1 => (rsum n (fun x2 => indB db x1 x2)) *
                    ((s2 n db : Int) * (s4 n db b : Int))) by
                funext x1; exact hinner x1]
              rw [rsum_mul_const]
              rw [rsum_indB2]
              grind



/-! ## FractionalCap: four integral covers and the fractional decomposition. -/

private theorem rhs_prod (n : Nat) (db : DB) (b : Bind)
    (w0 w1 w2 w3 w4 : Nat) :
    ((pattern.atoms.zip [w0, w1, w2, w3, w4]).map
      (fun (a, w) => support n db a pattern b ^ w)).prod =
      support n db (⟨0, 0, 1⟩ : Atom) pattern b ^ w0 *
        (support n db (⟨1, 1, 2⟩ : Atom) pattern b ^ w1 *
          (support n db (⟨2, 2, 3⟩ : Atom) pattern b ^ w2 *
            (support n db (⟨1, 3, 4⟩ : Atom) pattern b ^ w3 *
              support n db (⟨2, 0, 4⟩ : Atom) pattern b ^ w4))) := by
  simp [pattern]

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn.FractionalCap.Goal := by
  intro n db b weights den hcover
  rcases hcover with ⟨hden, hwlen, hcov⟩
  cases weights with
  | nil => simp [pattern] at hwlen
  | cons w0 ws1 =>
    cases ws1 with
    | nil => simp [pattern] at hwlen
    | cons w1 ws2 =>
      cases ws2 with
      | nil => simp [pattern] at hwlen
      | cons w2 ws3 =>
        cases ws3 with
        | nil => simp [pattern] at hwlen
        | cons w3 ws4 =>
          cases ws4 with
          | nil => simp [pattern] at hwlen
          | cons w4 rest =>
            cases rest with
            | nil =>
              rw [rhs_prod n db b w0 w1 w2 w3 w4]
              rw [support0_eq, support1_eq, support2_eq, support3_eq, support4_eq]
              simp [pattern] at hcov
              rcases hcov with ⟨h01, h12, h23, h34⟩
              let N : Nat := (reference pattern n
                (fun r x y => if db r x y == 0 then 0 else 1) b).toNat
              have hNint : (N : Int) = reference pattern n
                  (fun r x y => if db r x y == 0 then 0 else 1) b := by
                dsimp [N]
                rw [ref_indicator]
                exact Int.toNat_of_nonneg (Rfull_nonneg n db b)
              have h025 : N ≤ s0 n db b * s2 n db * s4 n db b := by
                apply Int.ofNat_le.mp
                rw [hNint, ref_indicator]
                simpa [Int.natCast_mul] using full_le_s0s2s4 n db b
              have h13 : N ≤ s1 n db * s3 n db := by
                apply Int.ofNat_le.mp
                rw [hNint, ref_indicator]
                simpa [Int.natCast_mul] using full_le_s1s3 n db b
              have h023 : N ≤ s0 n db b * s2 n db * s3 n db := by
                apply Int.ofNat_le.mp
                rw [hNint, ref_indicator]
                simpa [Int.natCast_mul] using full_le_s0s2s3 n db b
              have h124 : N ≤ s1 n db * s2 n db * s4 n db b := by
                apply Int.ofNat_le.mp
                rw [hNint, ref_indicator]
                simpa [Int.natCast_mul] using full_le_s1s2s4 n db b
              change N ^ den ≤
                s0 n db b ^ w0 * (s1 n db ^ w1 *
                  (s2 n db ^ w2 * (s3 n db ^ w3 * s4 n db b ^ w4)))
              by_cases hz : s0 n db b = 0 ∨ s1 n db = 0 ∨ s2 n db = 0 ∨
                  s3 n db = 0 ∨ s4 n db b = 0
              · have hN0 : N = 0 := by
                  rcases hz with h | h | h | h | h
                  · have : N ≤ 0 := by simpa [h] using h025
                    omega
                  · have : N ≤ 0 := by simpa [h] using h13
                    omega
                  · have : N ≤ 0 := by simpa [h] using h023
                    omega
                  · have : N ≤ 0 := by simpa [h] using h13
                    omega
                  · have : N ≤ 0 := by simpa [h] using h025
                    omega
                simp [hN0, hden]
              · have hs0 : 1 ≤ s0 n db b := by
                  have hne : s0 n db b ≠ 0 := by intro h; exact hz (Or.inl h)
                  omega
                have hs1 : 1 ≤ s1 n db := by
                  have hne : s1 n db ≠ 0 := by intro h; exact hz (Or.inr (Or.inl h))
                  omega
                have hs2 : 1 ≤ s2 n db := by
                  have hne : s2 n db ≠ 0 := by intro h; exact hz (Or.inr (Or.inr (Or.inl h)))
                  omega
                have hs3 : 1 ≤ s3 n db := by
                  have hne : s3 n db ≠ 0 := by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inl h))))
                  omega
                have hs4 : 1 ≤ s4 n db b := by
                  have hne : s4 n db b ≠ 0 := by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inr h))))
                  omega
                let u := min w2 (min w4 den)
                let a := min u w0
                let d := den - w1 - a
                let c := u - a
                let bb := den - u - d
                have hu2 : u ≤ w2 := by dsimp [u]; exact Nat.min_le_left _ _
                have hu4 : u ≤ w4 := by
                  dsimp [u]
                  exact Nat.le_trans (Nat.min_le_right _ _) (Nat.min_le_left _ _)
                have hud : u ≤ den := by
                  dsimp [u]
                  exact Nat.le_trans (Nat.min_le_right _ _) (Nat.min_le_right _ _)
                have hau : a ≤ u := by dsimp [a]; exact Nat.min_le_left _ _
                have haw0 : a ≤ w0 := by dsimp [a]; exact Nat.min_le_right _ _
                have ha_d_le_w0 : a + d ≤ w0 := by
                  dsimp [d]
                  omega
                have hbc_le_w1 : bb + c ≤ w1 := by
                  dsimp [bb, c, d]
                  omega
                have hacd_le_w2 : a + c + d ≤ w2 := by
                  dsimp [a, c, d]
                  by_cases huw0 : u ≤ w0
                  · rw [Nat.min_eq_left huw0]
                    omega
                  · have hwu : w0 ≤ u := by omega
                    rw [Nat.min_eq_right hwu]
                    have hd0 : den - w1 - w0 = 0 := by omega
                    rw [hd0]
                    omega
                have hd_le_denu : d ≤ den - u := by
                  dsimp [d]
                  by_cases huw0 : u ≤ w0
                  · have ha_eq : a = u := by dsimp [a]; exact Nat.min_eq_left huw0
                    rw [ha_eq]
                    omega
                  · have hwu : w0 ≤ u := by omega
                    have ha_eq : a = w0 := by dsimp [a]; exact Nat.min_eq_right hwu
                    rw [ha_eq]
                    have hd0 : den - w1 - w0 = 0 := by omega
                    rw [hd0]
                    omega
                have hbd_eq : bb + d = den - u := by
                  dsimp [bb]
                  exact Nat.sub_add_cancel hd_le_denu
                have hdenu_le_w3 : den - u ≤ w3 := by
                  dsimp [u]
                  by_cases h2 : w2 ≤ min w4 den
                  · rw [Nat.min_eq_left h2]
                    omega
                  · have hminle : min w4 den ≤ w2 := by omega
                    rw [Nat.min_eq_right hminle]
                    by_cases h4 : w4 ≤ den
                    · rw [Nat.min_eq_left h4]
                      omega
                    · have hdle : den ≤ w4 := by omega
                      rw [Nat.min_eq_right hdle]
                      simp
                have hbd_le_w3 : bb + d ≤ w3 := by rw [hbd_eq]; exact hdenu_le_w3
                have hac_eq : a + c = u := by
                  dsimp [c]
                  omega
                have hac_le_w4 : a + c ≤ w4 := by rw [hac_eq]; exact hu4
                have hsum : a + bb + c + d = den := by
                  rw [show a + bb + c + d = (a + c) + (bb + d) by omega]
                  rw [hac_eq, hbd_eq]
                  omega
                have hpow : N ^ den = N ^ a * N ^ bb * N ^ c * N ^ d := by
                  rw [show den = a + bb + c + d by omega]
                  rw [show a + bb + c + d = (a + bb + c) + d by omega]
                  rw [Nat.pow_add]
                  rw [show a + bb + c = (a + bb) + c by omega]
                  rw [Nat.pow_add, Nat.pow_add]
                have hm0 : s0 n db b ^ (a + d) ≤ s0 n db b ^ w0 :=
                  Nat.pow_le_pow_right hs0 ha_d_le_w0
                have hm1 : s1 n db ^ (bb + c) ≤ s1 n db ^ w1 :=
                  Nat.pow_le_pow_right hs1 hbc_le_w1
                have hm2 : s2 n db ^ (a + c + d) ≤ s2 n db ^ w2 :=
                  Nat.pow_le_pow_right hs2 hacd_le_w2
                have hm3 : s3 n db ^ (bb + d) ≤ s3 n db ^ w3 :=
                  Nat.pow_le_pow_right hs3 hbd_le_w3
                have hm4 : s4 n db b ^ (a + c) ≤ s4 n db b ^ w4 :=
                  Nat.pow_le_pow_right hs4 hac_le_w4
                calc
                  N ^ den = N ^ a * N ^ bb * N ^ c * N ^ d := hpow
                  _ ≤ (((s0 n db b * s2 n db * s4 n db b) ^ a *
                        (s1 n db * s3 n db) ^ bb) *
                        (s1 n db * s2 n db * s4 n db b) ^ c) *
                      (s0 n db b * s2 n db * s3 n db) ^ d := by
                        apply Nat.mul_le_mul
                        · apply Nat.mul_le_mul
                          · apply Nat.mul_le_mul
                            · exact Nat.pow_le_pow_left h025 a
                            · exact Nat.pow_le_pow_left h13 bb
                          · exact Nat.pow_le_pow_left h124 c
                        · exact Nat.pow_le_pow_left h023 d
                  _ = s0 n db b ^ (a + d) * s1 n db ^ (bb + c) *
                        s2 n db ^ (a + c + d) * s3 n db ^ (bb + d) *
                        s4 n db b ^ (a + c) := by
                        simp only [Nat.mul_pow]
                        rw [show ((((s0 n db b ^ a * s2 n db ^ a) * s4 n db b ^ a) *
                              (s1 n db ^ bb * s3 n db ^ bb)) *
                                ((s1 n db ^ c * s2 n db ^ c) * s4 n db b ^ c)) *
                              ((s0 n db b ^ d * s2 n db ^ d) * s3 n db ^ d) =
                            (((s0 n db b ^ a * s0 n db b ^ d) *
                              (s1 n db ^ bb * s1 n db ^ c)) *
                              (((s2 n db ^ a * s2 n db ^ d) * s2 n db ^ c) *
                                ((s3 n db ^ bb * s3 n db ^ d) *
                                  (s4 n db b ^ a * s4 n db b ^ c)))) by ac_rfl]
                        rw [← Nat.pow_add (s0 n db b) a d]
                        rw [← Nat.pow_add (s1 n db) bb c]
                        rw [← Nat.pow_add (s2 n db) a d]
                        rw [← Nat.pow_add (s2 n db) (a + d) c]
                        rw [← Nat.pow_add (s3 n db) bb d]
                        rw [← Nat.pow_add (s4 n db b) a c]
                        rw [show a + d + c = a + c + d by omega]
                        ac_rfl
                  _ ≤ s0 n db b ^ w0 * s1 n db ^ w1 * s2 n db ^ w2 *
                        s3 n db ^ w3 * s4 n db b ^ w4 := by
                        apply Nat.mul_le_mul
                        · apply Nat.mul_le_mul
                          · apply Nat.mul_le_mul
                            · apply Nat.mul_le_mul
                              · exact hm0
                              · exact hm1
                            · exact hm2
                          · exact hm3
                        · exact hm4
                  _ = s0 n db b ^ w0 * (s1 n db ^ w1 *
                        (s2 n db ^ w2 * (s3 n db ^ w3 * s4 n db b ^ w4))) := by ac_rfl
            | cons _ _ => simp [pattern] at hwlen

end TinylambOpt.ResearchProofs.Round2.Task72
-- END jpb-copy Task72

-- BEGIN jpb-copy OrderSupplier Remaining (FractionalCap)
namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplier.Remaining

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.OrderSupplier

/--! Finite-range sum helpers (Std only). -/

def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

theorem rsum_zero (f : Nat → Int) : rsum 0 f = 0 := by simp [rsum]

theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) : rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ, ih]
      omega

theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.mul_add]

theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, ih, Int.add_mul]

theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn : 0 ≤ f n := h n (Nat.lt_succ_self n)
      have hi : 0 ≤ rsum n f := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

theorem rsum_eq_zero_of_zero (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → f i = 0) :
    rsum n f = 0 := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]
      simp

theorem rsum_const_zero (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [rsum_zero]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr
        intro j hj
        rw [rsum_zero]]
      exact (rsum_const_zero m).symm
  | succ n ih =>
      rw [rsum_succ]
      rw [ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

theorem rsum_product (n m : Nat) (f : Nat → Int) (g : Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i * g j)) = rsum n f * rsum m g := by
  calc
    rsum n (fun i => rsum m (fun j => f i * g j))
        = rsum n (fun i => f i * rsum m g) := by
            apply rsum_congr
            intro i hi
            rw [rsum_const_mul]
    _ = rsum n f * rsum m g := by rw [rsum_mul_const]

theorem filter_range_eq_single (n a : Nat) (ha : a < n) :
    (List.range n).filter (fun y => y == a) = [a] := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [List.range_succ]
      by_cases h : a == n
      · have han : a = n := Nat.eq_of_beq_eq_true h
        subst a
        have hnil : (List.range n).filter (fun y : Nat => y == n) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          have hylt : y < n := List.mem_range.mp hy
          cases h' : (y == n) <;> simp_all
        simp [hnil]
      · have hane : a ≠ n := by intro heq; subst n; simp at h
        have halt : a < n := by omega
        rw [List.filter_append, ih halt]
        have hna : n ≠ a := hane.symm
        simp [h, hna]

theorem rsum_filter_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    (((List.range n).filter (fun y => y == a)).map f).sum = f a := by
  rw [filter_range_eq_single n a ha]
  simp

private def ind (db : DB) (r x y : Nat) : Int := if db r x y != 0 then 1 else 0

private theorem ind_eq_indicator (db : DB) (r x y : Nat) :
    (if db r x y == 0 then (0 : Int) else 1) = ind db r x y := by
  unfold ind
  cases h : (db r x y == 0) <;> cases h' : (db r x y != 0) <;> simp_all

private theorem ind_nonneg (db : DB) (r x y : Nat) : 0 ≤ ind db r x y := by
  unfold ind
  split <;> omega

private theorem ind_le_one (db : DB) (r x y : Nat) : ind db r x y ≤ 1 := by
  unfold ind
  split <;> omega

private theorem reference_ind (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
    rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
      ind db 0 (b 0) x1 * (ind db 1 x1 x2 * (ind db 2 x2 x3 *
        (ind db 1 (b 0) x4 * ind db 2 x4 x3))))))) := by
  simp [reference, integrate, pattern, value, rsum, ind, put]

private theorem le_rsum_of_mem (n i : Nat) (f : Nat → Int) (hi : i < n)
    (hf : ∀ j, j < n → 0 ≤ f j) : f i ≤ rsum n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases hin : i < n
      · have hrec := ih hin (fun j hj => hf j (Nat.lt_succ_of_lt hj))
        have hn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
        omega
      · have : i = n := by omega
        subst i
        have hpre : 0 ≤ rsum n f := rsum_nonneg n f (fun j hj => hf j (Nat.lt_succ_of_lt hj))
        omega

private theorem eq_zero_of_rsum_eq_zero (n : Nat) (f : Nat → Int)
    (hf : ∀ i, i < n → 0 ≤ f i) (hs : rsum n f = 0) : ∀ i, i < n → f i = 0 := by
  induction n with
  | zero => intro i hi; omega
  | succ n ih =>
      rw [rsum_succ] at hs
      have hpre : 0 ≤ rsum n f := rsum_nonneg n f (fun i hi => hf i (Nat.lt_succ_of_lt hi))
      have hn : 0 ≤ f n := hf n (Nat.lt_succ_self n)
      have hpre0 : rsum n f = 0 := by omega
      have hn0 : f n = 0 := by omega
      intro i hi
      by_cases hin : i < n
      · exact ih (fun j hj => hf j (Nat.lt_succ_of_lt hj)) hpre0 i hin
      · have : i = n := by omega
        subst i
        exact hn0

private theorem drop_factor (p q : Int) (hp : 0 ≤ p) (hq0 : 0 ≤ q) (hq1 : q ≤ 1) : p * q ≤ p := by
  have := Int.mul_le_mul_of_nonneg_left hq1 hp
  simpa using this

private theorem five_le_BE (a b c d e : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) : a*b*c*d*e ≤ b*e := by
  have hp1 : 0 ≤ a*b*d*e := Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg ha0 hb0) hd0) he0
  have h1 : (a*b*d*e)*c ≤ a*b*d*e := drop_factor _ _ hp1 hc0 hc1
  have h1' : a*b*c*d*e ≤ a*b*d*e := by
    rw [show a*b*c*d*e = (a*b*d*e)*c by grind]
    exact h1
  have hp2 : 0 ≤ b*d*e := Int.mul_nonneg (Int.mul_nonneg hb0 hd0) he0
  have h2 : (b*d*e)*a ≤ b*d*e := drop_factor _ _ hp2 ha0 ha1
  have h2' : a*b*d*e ≤ b*d*e := by
    rw [show a*b*d*e = (b*d*e)*a by grind]
    exact h2
  have hp3 : 0 ≤ b*e := Int.mul_nonneg hb0 he0
  have h3 : (b*e)*d ≤ b*e := drop_factor _ _ hp3 hd0 hd1
  have h3' : b*d*e ≤ b*e := by
    rw [show b*d*e = (b*e)*d by grind]
    exact h3
  omega

private theorem five_le_ACD (a b c d e : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) : a*b*c*d*e ≤ a*c*d := by
  have hp1 : 0 ≤ a*c*d := Int.mul_nonneg (Int.mul_nonneg ha0 hc0) hd0
  have h1 : (a*c*d)*e ≤ a*c*d := drop_factor _ _ hp1 he0 he1
  have h1' : a*c*d*e ≤ a*c*d := by
    rw [show a*c*d*e = (a*c*d)*e by grind]
    exact h1
  have hp2 : 0 ≤ a*c*d := hp1
  have h2 : (a*c*d)*b ≤ a*c*d := drop_factor _ _ hp2 hb0 hb1
  have h2' : a*b*c*d ≤ a*c*d := by
    rw [show a*b*c*d = (a*c*d)*b by grind]
    exact h2
  have hfull1 : a*b*c*d*e ≤ a*c*d*e := by
    have := Int.mul_le_mul_of_nonneg_right h2' he0
    simpa [show a*c*d*e = (a*c*d)*e by grind] using this
  omega

private theorem five_le_ACE (a b c d e : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) : a*b*c*d*e ≤ a*c*e := by
  have hp1 : 0 ≤ a*c*e := Int.mul_nonneg (Int.mul_nonneg ha0 hc0) he0
  have h1 : (a*c*e)*d ≤ a*c*e := drop_factor _ _ hp1 hd0 hd1
  have h1' : a*c*d*e ≤ a*c*e := by
    rw [show a*c*d*e = (a*c*e)*d by grind]
    exact h1
  have h2 : (a*c*e)*b ≤ a*c*e := drop_factor _ _ hp1 hb0 hb1
  have h2' : a*b*c*e ≤ a*c*e := by
    rw [show a*b*c*e = (a*c*e)*b by grind]
    exact h2
  have habc0 : 0 ≤ a*b*c := Int.mul_nonneg (Int.mul_nonneg ha0 hb0) hc0
  have hddrop : (a*b*c)*d ≤ a*b*c := drop_factor _ _ habc0 hd0 hd1
  have habcd : a*b*c*d ≤ a*b*c := by
    rw [show a*b*c*d = (a*b*c)*d by grind]
    exact hddrop
  have hfull1 : a*b*c*d*e ≤ a*b*c*e := by
    have := Int.mul_le_mul_of_nonneg_right habcd he0
    simpa [show a*b*c*d*e = (a*b*c*d)*e by grind] using this
  omega

private theorem rsum_const_mul2 (n : Nat) (c : Int) (f : Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y => c * f x y)) =
      c * rsum n (fun x => rsum n (fun y => f x y)) := by
  have h : (fun x => rsum n (fun y => c * f x y)) =
      (fun x => c * rsum n (fun y => f x y)) := by
    funext x
    rw [rsum_const_mul]
  rw [h, rsum_const_mul]

private theorem rsum_mul_const2 (n : Nat) (f : Nat → Nat → Int) (c : Int) :
    rsum n (fun x => rsum n (fun y => f x y * c)) =
      rsum n (fun x => rsum n (fun y => f x y)) * c := by
  have h : (fun x => rsum n (fun y => f x y * c)) =
      (fun x => rsum n (fun y => f x y) * c) := by
    funext x
    rw [rsum_mul_const]
  rw [h, rsum_mul_const]

private theorem rsum_const_mul3 (n : Nat) (c : Int) (f : Nat → Nat → Nat → Int) :
    rsum n (fun x => rsum n (fun y => rsum n (fun z => c * f x y z))) =
      c * rsum n (fun x => rsum n (fun y => rsum n (fun z => f x y z))) := by
  have h : (fun x => rsum n (fun y => rsum n (fun z => c * f x y z))) =
      (fun x => c * rsum n (fun y => rsum n (fun z => f x y z))) := by
    funext x
    rw [rsum_const_mul2]
  rw [h, rsum_const_mul]

private theorem five_le_BCD (a b c d e : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b)
    (hc0 : 0 ≤ c) (hd0 : 0 ≤ d)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) : a*b*c*d*e ≤ b*c*d := by
  have hp : 0 ≤ b*c*d := Int.mul_nonneg (Int.mul_nonneg hb0 hc0) hd0
  have h1 : (b*c*d)*e ≤ b*c*d := drop_factor _ _ hp he0 he1
  have h2 : (b*c*d)*a ≤ b*c*d := drop_factor _ _ hp ha0 ha1
  have h1' : a*b*c*d*e ≤ (b*c*d)*e := by
    rw [show a*b*c*d*e = (b*c*d)*a*e by grind]
    have := Int.mul_le_mul_of_nonneg_right h2 he0
    simpa using this
  omega

private def A (db : DB) (b : Bind) (x : Nat) : Int := ind db 0 (b 0) x
private def Bf (db : DB) (x y : Nat) : Int := ind db 1 x y
private def Cf (db : DB) (x y : Nat) : Int := ind db 2 x y
private def Df (db : DB) (b : Bind) (x : Nat) : Int := ind db 1 (b 0) x
private def Ef (db : DB) (x y : Nat) : Int := ind db 2 x y

private def s0 (n : Nat) (db : DB) (b : Bind) : Int := rsum n (fun x => A db b x)
private def s1 (n : Nat) (db : DB) : Int := rsum n (fun x => rsum n (fun y => Bf db x y))
private def s2 (n : Nat) (db : DB) : Int := rsum n (fun x => rsum n (fun y => Cf db x y))
private def s3 (n : Nat) (db : DB) (b : Bind) : Int := rsum n (fun x => Df db b x)
private def s4 (n : Nat) (db : DB) : Int := rsum n (fun x => rsum n (fun y => Ef db x y))

private def Rfull (n : Nat) (db : DB) (b : Bind) : Int :=
  rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
    A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3)))))))

private theorem full_le_s1s4 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ s1 n db * s4 n db := by
  calc
    Rfull n db b
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            Bf db x1 x2 * Ef db x4 x3)))) := by
              dsimp [Rfull]
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x4 hx4
              apply rsum_le; intro x3 hx3
              have h := five_le_BE (A db b x1) (Bf db x1 x2) (Cf db x2 x3) (Df db b x4) (Ef db x4 x3)
                (by dsimp [A]; exact ind_nonneg db 0 (b 0) x1) (by dsimp [A]; exact ind_le_one db 0 (b 0) x1)
                (by dsimp [Bf]; exact ind_nonneg db 1 x1 x2) (by dsimp [Bf]; exact ind_le_one db 1 x1 x2)
                (by dsimp [Cf]; exact ind_nonneg db 2 x2 x3) (by dsimp [Cf]; exact ind_le_one db 2 x2 x3)
                (by dsimp [Df]; exact ind_nonneg db 1 (b 0) x4) (by dsimp [Df]; exact ind_le_one db 1 (b 0) x4)
                (by dsimp [Ef]; exact ind_nonneg db 2 x4 x3) (by dsimp [Ef]; exact ind_le_one db 2 x4 x3)
              simpa [show A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3))) =
                  A db b x1 * Bf db x1 x2 * Cf db x2 x3 * Df db b x4 * Ef db x4 x3 by grind] using h
    _ = s1 n db * s4 n db := by
      dsimp [s1, s4]
      let K : Int := rsum n (fun x4 => rsum n (fun x3 => Ef db x4 x3))
      calc
        rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            Bf db x1 x2 * Ef db x4 x3))))
            = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 =>
                (rsum n (fun x3 => Ef db x4 x3)) * Bf db x1 x2))) := by
                  apply rsum_congr; intro x1 h1
                  apply rsum_congr; intro x2 h2
                  apply rsum_congr; intro x4 h4
                  rw [show (fun x3 => Bf db x1 x2 * Ef db x4 x3) =
                      (fun x3 => Ef db x4 x3 * Bf db x1 x2) by funext x3; ac_rfl]
                  rw [rsum_mul_const]
        _ = rsum n (fun x1 => rsum n (fun x2 => Bf db x1 x2 * K)) := by
                  dsimp [K]
                  apply rsum_congr; intro x1 h1
                  apply rsum_congr; intro x2 h2
                  rw [show (fun x4 => (rsum n (fun x3 => Ef db x4 x3)) * Bf db x1 x2) =
                      (fun x4 => Bf db x1 x2 * (rsum n (fun x3 => Ef db x4 x3))) by funext x4; ac_rfl]
                  rw [rsum_const_mul]
        _ = rsum n (fun x1 => (rsum n (fun x2 => Bf db x1 x2)) * K) := by
                  apply rsum_congr; intro x1 h1
                  rw [rsum_mul_const]
        _ = (rsum n (fun x1 => rsum n (fun x2 => Bf db x1 x2))) * K := by
                  rw [rsum_mul_const]

private theorem inner_swap_le (n : Nat) (db : DB) :
    rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 => Cf db x2 x3 * Ef db x4 x3)))
      ≤ s2 n db * s4 n db := by
  dsimp [s2, s4]
  calc
    rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 => Cf db x2 x3 * Ef db x4 x3)))
        = rsum n (fun x2 => rsum n (fun x3 => rsum n (fun x4 => Cf db x2 x3 * Ef db x4 x3))) := by
            apply rsum_congr; intro x2 h2
            rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x2 => rsum n (fun x4 => Cf db x2 x3 * Ef db x4 x3))) := by
            rw [rsum_comm]
    _ = rsum n (fun x3 => rsum n (fun x2 => Cf db x2 x3) * rsum n (fun x4 => Ef db x4 x3)) := by
            apply rsum_congr; intro x3 h3
            rw [show (fun x2 => rsum n (fun x4 => Cf db x2 x3 * Ef db x4 x3)) =
                (fun x2 => rsum n (fun x4 => Ef db x4 x3 * Cf db x2 x3)) by
                  funext x2; congr 1; funext x4; ac_rfl]
            have hinner : (fun x2 => rsum n (fun x4 => Ef db x4 x3 * Cf db x2 x3)) =
                (fun x2 => (rsum n (fun x4 => Ef db x4 x3)) * Cf db x2 x3) := by
              funext x2
              rw [rsum_mul_const]
            rw [hinner, rsum_const_mul]
            grind
    _ ≤ rsum n (fun x3 => (rsum n (fun y => rsum n (fun z => Cf db y z))) *
            rsum n (fun x4 => Ef db x4 x3)) := by
            apply rsum_le; intro x3 h3
            have hcle : rsum n (fun x2 => Cf db x2 x3) ≤ rsum n (fun y => rsum n (fun z => Cf db y z)) := by
              apply rsum_le; intro x2 hx2
              exact le_rsum_of_mem n x3 (fun z => Cf db x2 z) h3
                (fun z hz => by dsimp [Cf]; exact ind_nonneg db 2 x2 z)
            have hEnon : 0 ≤ rsum n (fun x4 => Ef db x4 x3) :=
              rsum_nonneg n _ (fun x4 hx4 => by dsimp [Ef]; exact ind_nonneg db 2 x4 x3)
            exact Int.mul_le_mul_of_nonneg_right hcle hEnon
    _ = (rsum n (fun y => rsum n (fun z => Cf db y z))) *
          rsum n (fun x3 => rsum n (fun x4 => Ef db x4 x3)) := by
            rw [rsum_const_mul]
    _ = (rsum n (fun y => rsum n (fun z => Cf db y z))) *
          rsum n (fun x4 => rsum n (fun x3 => Ef db x4 x3)) := by
            congr 1
            rw [rsum_comm]

private theorem full_le_s0s2s4 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ s0 n db b * s2 n db * s4 n db := by
  calc
    Rfull n db b
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            A db b x1 * (Cf db x2 x3 * Ef db x4 x3)))) ):= by
              dsimp [Rfull]
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x4 hx4
              apply rsum_le; intro x3 hx3
              have h := five_le_ACE (A db b x1) (Bf db x1 x2) (Cf db x2 x3) (Df db b x4) (Ef db x4 x3)
                (by dsimp [A]; exact ind_nonneg db 0 (b 0) x1) (by dsimp [A]; exact ind_le_one db 0 (b 0) x1)
                (by dsimp [Bf]; exact ind_nonneg db 1 x1 x2) (by dsimp [Bf]; exact ind_le_one db 1 x1 x2)
                (by dsimp [Cf]; exact ind_nonneg db 2 x2 x3) (by dsimp [Cf]; exact ind_le_one db 2 x2 x3)
                (by dsimp [Df]; exact ind_nonneg db 1 (b 0) x4) (by dsimp [Df]; exact ind_le_one db 1 (b 0) x4)
                (by dsimp [Ef]; exact ind_nonneg db 2 x4 x3) (by dsimp [Ef]; exact ind_le_one db 2 x4 x3)
              simpa [show A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3))) =
                  A db b x1 * Bf db x1 x2 * Cf db x2 x3 * Df db b x4 * Ef db x4 x3 by grind,
                  show A db b x1 * (Cf db x2 x3 * Ef db x4 x3) = A db b x1 * Cf db x2 x3 * Ef db x4 x3 by grind] using h
    _ = rsum n (fun x1 => A db b x1 *
          rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 => Cf db x2 x3 * Ef db x4 x3)))) := by
            apply rsum_congr; intro x1 h1
            have h : (fun x2 => rsum n (fun x4 => rsum n (fun x3 => A db b x1 * (Cf db x2 x3 * Ef db x4 x3)))) =
                (fun x2 => A db b x1 * rsum n (fun x4 => rsum n (fun x3 => Cf db x2 x3 * Ef db x4 x3))) := by
              funext x2
              rw [rsum_const_mul2]
            rw [h, rsum_const_mul]
    _ ≤ rsum n (fun x1 => A db b x1 * (s2 n db * s4 n db)) := by
            apply rsum_le; intro x1 h1
            have hA : 0 ≤ A db b x1 := by dsimp [A]; exact ind_nonneg db 0 (b 0) x1
            exact Int.mul_le_mul_of_nonneg_left (inner_swap_le n db) hA
    _ = s0 n db b * s2 n db * s4 n db := by
            dsimp [s0]
            rw [show (fun x => A db b x * (s2 n db * s4 n db)) =
                (fun x => A db b x * (s2 n db * s4 n db)) by rfl]
            rw [rsum_mul_const]
            grind

private theorem full_le_s0s2s3 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ s0 n db b * s2 n db * s3 n db b := by
  calc
    Rfull n db b
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            A db b x1 * (Cf db x2 x3 * Df db b x4)))) ):= by
              dsimp [Rfull]
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x4 hx4
              apply rsum_le; intro x3 hx3
              have h := five_le_ACD (A db b x1) (Bf db x1 x2) (Cf db x2 x3) (Df db b x4) (Ef db x4 x3)
                (by dsimp [A]; exact ind_nonneg db 0 (b 0) x1) (by dsimp [A]; exact ind_le_one db 0 (b 0) x1)
                (by dsimp [Bf]; exact ind_nonneg db 1 x1 x2) (by dsimp [Bf]; exact ind_le_one db 1 x1 x2)
                (by dsimp [Cf]; exact ind_nonneg db 2 x2 x3) (by dsimp [Cf]; exact ind_le_one db 2 x2 x3)
                (by dsimp [Df]; exact ind_nonneg db 1 (b 0) x4) (by dsimp [Df]; exact ind_le_one db 1 (b 0) x4)
                (by dsimp [Ef]; exact ind_nonneg db 2 x4 x3) (by dsimp [Ef]; exact ind_le_one db 2 x4 x3)
              simpa [show A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3))) =
                  A db b x1 * Bf db x1 x2 * Cf db x2 x3 * Df db b x4 * Ef db x4 x3 by grind,
                  show A db b x1 * (Cf db x2 x3 * Df db b x4) = A db b x1 * Cf db x2 x3 * Df db b x4 by grind] using h
    _ = rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            A db b x1 * (Cf db x2 x3 * s3 n db b))) ):= by
            apply rsum_congr; intro x1 h1
            apply rsum_congr; intro x2 h2
            rw [rsum_comm]
            apply rsum_congr; intro x3 h3
            have hreshape : (fun i => A db b x1 * (Cf db x2 x3 * Df db b i)) =
                (fun i => (A db b x1 * Cf db x2 x3) * Df db b i) := by
              funext i
              grind
            rw [hreshape, rsum_const_mul]
            dsimp [s3]
            grind
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            A db b x1 * (rsum n (fun x3 => Cf db x2 x3) * s3 n db b)) ):= by
            apply rsum_congr; intro x1 h1
            apply rsum_congr; intro x2 h2
            rw [rsum_const_mul]
            congr 1
            rw [rsum_mul_const]
    _ = rsum n (fun x1 => A db b x1 * (s2 n db * s3 n db b)) := by
            apply rsum_congr; intro x1 h1
            rw [rsum_const_mul]
            congr 1
            dsimp [s2]
            rw [rsum_mul_const]
    _ = s0 n db b * s2 n db * s3 n db b := by
            dsimp [s0]
            rw [rsum_mul_const]
            grind

private theorem full_le_s1s2s3 (n : Nat) (db : DB) (b : Bind) :
    Rfull n db b ≤ s1 n db * s2 n db * s3 n db b := by
  calc
    Rfull n db b
        ≤ rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x4 => rsum n (fun x3 =>
            Bf db x1 x2 * (Cf db x2 x3 * Df db b x4))))) := by
              dsimp [Rfull]
              apply rsum_le; intro x1 hx1
              apply rsum_le; intro x2 hx2
              apply rsum_le; intro x4 hx4
              apply rsum_le; intro x3 hx3
              have h := five_le_BCD (A db b x1) (Bf db x1 x2) (Cf db x2 x3) (Df db b x4) (Ef db x4 x3)
                (by dsimp [A]; exact ind_nonneg db 0 (b 0) x1) (by dsimp [A]; exact ind_le_one db 0 (b 0) x1)
                (by dsimp [Bf]; exact ind_nonneg db 1 x1 x2)
                (by dsimp [Cf]; exact ind_nonneg db 2 x2 x3) (by dsimp [Df]; exact ind_nonneg db 1 (b 0) x4)
                (by dsimp [Ef]; exact ind_nonneg db 2 x4 x3) (by dsimp [Ef]; exact ind_le_one db 2 x4 x3)
              simpa [show A db b x1 * (Bf db x1 x2 * (Cf db x2 x3 * (Df db b x4 * Ef db x4 x3))) =
                  A db b x1 * Bf db x1 x2 * Cf db x2 x3 * Df db b x4 * Ef db x4 x3 by grind,
                  show Bf db x1 x2 * (Cf db x2 x3 * Df db b x4) = Bf db x1 x2 * Cf db x2 x3 * Df db b x4 by grind] using h
    _ = rsum n (fun x1 => rsum n (fun x2 =>
            Bf db x1 x2 * (rsum n (fun x3 => Cf db x2 x3) * s3 n db b)) ):= by
            apply rsum_congr; intro x1 h1
            apply rsum_congr; intro x2 h2
            rw [rsum_comm]
            have hreshape2 : (fun j => rsum n (fun i => Bf db x1 x2 * (Cf db x2 j * Df db b i))) =
                (fun j => Bf db x1 x2 * rsum n (fun i => Cf db x2 j * Df db b i)) := by
              funext j
              rw [rsum_const_mul]
            rw [hreshape2, rsum_const_mul]
            have h3reshape : (fun j => rsum n (fun i => Cf db x2 j * Df db b i)) =
                (fun j => Cf db x2 j * s3 n db b) := by
              funext j
              rw [rsum_const_mul]
              simp [s3]
            rw [h3reshape, rsum_mul_const]
    _ ≤ rsum n (fun x1 => rsum n (fun x2 =>
            Bf db x1 x2 * (s2 n db * s3 n db b)) ):= by
            apply rsum_le; intro x1 h1
            apply rsum_le; intro x2 h2
            have hB : 0 ≤ Bf db x1 x2 := by dsimp [Bf]; exact ind_nonneg db 1 x1 x2
            have hs3 : 0 ≤ s3 n db b := rsum_nonneg n _ (fun x h => by dsimp [Df]; exact ind_nonneg db 1 (b 0) x)
            have hs2x : rsum n (fun x3 => Cf db x2 x3) ≤ s2 n db := by
              dsimp [s2]
              rw [show rsum n (fun y => rsum n (fun z => Cf db y z)) =
                  rsum n (fun z => rsum n (fun y => Cf db y z)) by rw [rsum_comm]]
              apply rsum_le; intro z hz
              exact le_rsum_of_mem n x2 (fun y => Cf db y z) h2
                (fun y hy => by dsimp [Cf]; exact ind_nonneg db 2 y z)
            exact Int.mul_le_mul_of_nonneg_left (Int.mul_le_mul_of_nonneg_right hs2x hs3) hB
    _ = s1 n db * s2 n db * s3 n db b := by
            dsimp [s1]
            have h : (fun x1 => rsum n (fun x2 => Bf db x1 x2 * (s2 n db * s3 n db b))) =
                (fun x1 => (rsum n (fun x2 => Bf db x1 x2)) * (s2 n db * s3 n db b)) := by
              funext x1
              rw [rsum_mul_const]
            rw [h, rsum_mul_const]
            grind

private theorem int_countP_eq_rsum (n : Nat) (p : Nat → Bool) :
    ((List.range n).countP p : Int) = rsum n (fun x => if p x then (1 : Int) else 0) := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [List.range_succ, List.countP_append]
      have hcast : ((List.countP p (List.range n) + List.countP p [n] : Nat) : Int) =
          (List.countP p (List.range n) : Int) + (List.countP p [n] : Int) := by omega
      rw [hcast, ih]
      rw [rsum_succ]
      cases h : p n <;> simp [h]

private theorem int_list_sum_countP (l : List Nat) (n : Nat) (f : Nat → Nat → Bool) :
    ((l.map (fun x => (List.range n).countP (f x))).sum : Int) =
      (l.map (fun x => rsum n (fun y => if f x y then (1 : Int) else 0))).sum := by
  induction l with
  | nil => simp
  | cons x l ih =>
      simp only [List.map_cons, List.sum_cons]
      have hcast : ((List.countP (f x) (List.range n) +
          (List.map (fun x => List.countP (f x) (List.range n)) l).sum : Nat) : Int) =
          (List.countP (f x) (List.range n) : Int) +
          ((List.map (fun x => List.countP (f x) (List.range n)) l).sum : Int) := by omega
      rw [hcast, int_countP_eq_rsum, ih]

private theorem support0_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨0, 0, 1⟩ : Atom) pattern b : Int) = s0 n db b := by
  simp [support, pattern, s0, A, ind, int_countP_eq_rsum]

private theorem support1_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨1, 1, 2⟩ : Atom) pattern b : Int) = s1 n db := by
  simp [support, pattern, s1, Bf]
  rw [int_list_sum_countP]
  rfl

private theorem support2_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨2, 2, 3⟩ : Atom) pattern b : Int) = s2 n db := by
  simp [support, pattern, s2, Cf]
  rw [int_list_sum_countP]
  rfl

private theorem support3_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨1, 0, 4⟩ : Atom) pattern b : Int) = s3 n db b := by
  simp [support, pattern, s3, Df, ind, int_countP_eq_rsum]

private theorem support4_eq (n : Nat) (db : DB) (b : Bind) :
    (support n db (⟨2, 4, 3⟩ : Atom) pattern b : Int) = s4 n db := by
  simp [support, pattern, s4, Ef]
  rw [int_list_sum_countP]
  rfl

private theorem prod5_nonneg (a b c d e : Int) (ha : 0 ≤ a) (hb : 0 ≤ b) (hc : 0 ≤ c)
    (hd : 0 ≤ d) (he : 0 ≤ e) : 0 ≤ a*b*c*d*e :=
  Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg ha hb) hc) hd) he

private theorem Rfull_nonneg (n : Nat) (db : DB) (b : Bind) : 0 ≤ Rfull n db b := by
  dsimp [Rfull]
  apply rsum_nonneg; intro x1 hx1
  apply rsum_nonneg; intro x2 hx2
  apply rsum_nonneg; intro x4 hx4
  apply rsum_nonneg; intro x3 hx3
  have h := prod5_nonneg (ind db 0 (b 0) x1) (ind db 1 x1 x2) (ind db 2 x2 x3)
    (ind db 1 (b 0) x4) (ind db 2 x4 x3)
    (ind_nonneg db 0 (b 0) x1)
    (ind_nonneg db 1 x1 x2)
    (ind_nonneg db 2 x2 x3)
    (ind_nonneg db 1 (b 0) x4)
    (ind_nonneg db 2 x4 x3)
  simpa [A, Bf, Cf, Df, Ef, show ind db 0 (b 0) x1 * ind db 1 x1 x2 * ind db 2 x2 x3 *
      ind db 1 (b 0) x4 * ind db 2 x4 x3 =
      ind db 0 (b 0) x1 * (ind db 1 x1 x2 * (ind db 2 x2 x3 *
        (ind db 1 (b 0) x4 * ind db 2 x4 x3))) by grind] using h

private theorem reference_eq_Rfull (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b = Rfull n db b := by
  rw [reference_ind]
  simp [Rfull, A, Bf, Cf, Df, Ef, ind, ind_eq_indicator]

private theorem pow_mono_one (a : Int) (h1 : 1 ≤ a) : ∀ (m n : Nat), m ≤ n → a^m ≤ a^n
  | m, 0, hmn => by
      have : m = 0 := by omega
      subst m
      simp
  | m, n+1, hmn => by
      by_cases hm : m ≤ n
      · have ih := pow_mono_one a h1 m n hm
        have hnon : 0 ≤ a ^ n := Int.pow_nonneg (by omega)
        have hstep : a^n ≤ a^n * a := by
          have := Int.mul_le_mul_of_nonneg_left h1 hnon
          simpa using this
        rw [Int.pow_succ]
        omega
      · have : m = n+1 := by omega
        subst m
        simp

private theorem Rfull_zero_of_s1 (n : Nat) (db : DB) (b : Bind) (h : s1 n db = 0) :
    Rfull n db b = 0 := by
  have hB : ∀ x y, x < n → y < n → Bf db x y = 0 := by
    intro x y hx hy
    have houter : ∀ i, i < n → 0 ≤ rsum n (fun j => Bf db i j) := by
      intro i hi
      exact rsum_nonneg n _ (fun j hj => ind_nonneg db 1 i j)
    have hz : rsum n (fun i => rsum n (fun j => Bf db i j)) = 0 := by simpa [s1] using h
    have hrow : rsum n (fun y => Bf db x y) = 0 :=
      eq_zero_of_rsum_eq_zero n (fun i => rsum n (fun j => Bf db i j)) houter hz x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => Bf db x y)
      (fun j hj => ind_nonneg db 1 x j) hrow y hy
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hB x1 x2 hx1 hx2]

private theorem Rfull_zero_of_s2 (n : Nat) (db : DB) (b : Bind) (h : s2 n db = 0) :
    Rfull n db b = 0 := by
  have hC : ∀ x y, x < n → y < n → Cf db x y = 0 := by
    intro x y hx hy
    have houter : ∀ i, i < n → 0 ≤ rsum n (fun j => Cf db i j) := by
      intro i hi
      exact rsum_nonneg n _ (fun j hj => ind_nonneg db 2 i j)
    have hz : rsum n (fun i => rsum n (fun j => Cf db i j)) = 0 := by simpa [s2] using h
    have hrow : rsum n (fun y => Cf db x y) = 0 :=
      eq_zero_of_rsum_eq_zero n (fun i => rsum n (fun j => Cf db i j)) houter hz x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => Cf db x y)
      (fun j hj => ind_nonneg db 2 x j) hrow y hy
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hC x2 x3 hx2 hx3]

private theorem Rfull_zero_of_s4 (n : Nat) (db : DB) (b : Bind) (h : s4 n db = 0) :
    Rfull n db b = 0 := by
  have hE : ∀ x y, x < n → y < n → Ef db x y = 0 := by
    intro x y hx hy
    have houter : ∀ i, i < n → 0 ≤ rsum n (fun j => Ef db i j) := by
      intro i hi
      exact rsum_nonneg n _ (fun j hj => ind_nonneg db 2 i j)
    have hz : rsum n (fun i => rsum n (fun j => Ef db i j)) = 0 := by simpa [s4] using h
    have hrow : rsum n (fun y => Ef db x y) = 0 :=
      eq_zero_of_rsum_eq_zero n (fun i => rsum n (fun j => Ef db i j)) houter hz x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => Ef db x y)
      (fun j hj => ind_nonneg db 2 x j) hrow y hy
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hE x4 x3 hx4 hx3]

private theorem Rfull_zero_of_s0 (n : Nat) (db : DB) (b : Bind) (h : s0 n db b = 0) :
    Rfull n db b = 0 := by
  have hA : ∀ x, x < n → A db b x = 0 := by
    intro x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => A db b y)
      (fun j hj => ind_nonneg db 0 (b 0) j) (by simpa [s0] using h) x hx
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hA x1 hx1]

private theorem Rfull_zero_of_s3 (n : Nat) (db : DB) (b : Bind) (h : s3 n db b = 0) :
    Rfull n db b = 0 := by
  have hD : ∀ x, x < n → Df db b x = 0 := by
    intro x hx
    exact eq_zero_of_rsum_eq_zero n (fun y => Df db b y)
      (fun j hj => ind_nonneg db 1 (b 0) j) (by simpa [s3] using h) x hx
  dsimp [Rfull]
  apply rsum_eq_zero_of_zero; intro x1 hx1
  apply rsum_eq_zero_of_zero; intro x2 hx2
  apply rsum_eq_zero_of_zero; intro x4 hx4
  apply rsum_eq_zero_of_zero; intro x3 hx3
  simp [hD x4 hx4]

private theorem pow_le_pow_of_le {a c : Int} (h : a ≤ c) (ha : 0 ≤ a) :
    ∀ k : Nat, a^k ≤ c^k
  | 0 => by simp
  | k+1 => by
      rw [Int.pow_succ, Int.pow_succ]
      have hc : 0 ≤ c := by omega
      have hak : 0 ≤ a^k := Int.pow_nonneg ha
      exact Int.mul_le_mul (pow_le_pow_of_le h ha k) h ha (Int.pow_nonneg hc)

private theorem mul5_le {a b c d e A B C D E : Int}
    (ha : a ≤ A) (hb : b ≤ B) (hc : c ≤ C) (hd : d ≤ D) (he : e ≤ E)
    (ha0 : 0 ≤ a) (hb0 : 0 ≤ b) (hc0 : 0 ≤ c) (hd0 : 0 ≤ d) (he0 : 0 ≤ e) :
    a*b*c*d*e ≤ A*B*C*D*E := by
  have hA0 : 0 ≤ A := by omega
  have hB0 : 0 ≤ B := by omega
  have hC0 : 0 ≤ C := by omega
  have hD0 : 0 ≤ D := by omega
  have h12 : a*b ≤ A*B := Int.mul_le_mul ha hb hb0 hA0
  have hAB0 : 0 ≤ A*B := Int.mul_nonneg hA0 hB0
  have h123 : a*b*c ≤ A*B*C := Int.mul_le_mul h12 hc hc0 hAB0
  have hABC0 : 0 ≤ A*B*C := Int.mul_nonneg hAB0 hC0
  have h1234 : a*b*c*d ≤ A*B*C*D := Int.mul_le_mul h123 hd hd0 hABC0
  have hABCD0 : 0 ≤ A*B*C*D := Int.mul_nonneg hABC0 hD0
  exact Int.mul_le_mul h1234 he he0 hABCD0

private theorem s0_nonneg (n : Nat) (db : DB) (b : Bind) : 0 ≤ s0 n db b :=
  rsum_nonneg n _ (fun x hx => ind_nonneg db 0 (b 0) x)
private theorem s1_nonneg (n : Nat) (db : DB) : 0 ≤ s1 n db :=
  rsum_nonneg n _ (fun x hx => rsum_nonneg n _ (fun y hy => ind_nonneg db 1 x y))
private theorem s2_nonneg (n : Nat) (db : DB) : 0 ≤ s2 n db :=
  rsum_nonneg n _ (fun x hx => rsum_nonneg n _ (fun y hy => ind_nonneg db 2 x y))
private theorem s3_nonneg (n : Nat) (db : DB) (b : Bind) : 0 ≤ s3 n db b :=
  rsum_nonneg n _ (fun x hx => ind_nonneg db 1 (b 0) x)
private theorem s4_nonneg (n : Nat) (db : DB) : 0 ≤ s4 n db :=
  rsum_nonneg n _ (fun x hx => rsum_nonneg n _ (fun y hy => ind_nonneg db 2 x y))

theorem sound_FractionalCap : FractionalCap.Goal := by
  intro n db b weights den hcov
  have hlen : weights.length = 5 := by
    simpa [cover, pattern] using hcov.2.1
  rcases weights with _ | ⟨w0, _ | ⟨w1, _ | ⟨w2, _ | ⟨w3, _ | ⟨w4, rest⟩⟩⟩⟩⟩
  · simp at hlen
  · simp at hlen
  · simp at hlen
  · simp at hlen
  · simp at hlen
  · have hrest : rest = [] := by simpa using hlen
    subst rest
    unfold cover at hcov
    have hdenpos : 0 < den := hcov.1
    have hc1 : den ≤ w0 + w1 := by
      have := hcov.2.2 1 (by simp [pattern])
      simpa [pattern] using this
    have hc2 : den ≤ w1 + w2 := by
      have := hcov.2.2 2 (by simp [pattern])
      simpa [pattern] using this
    have hc3 : den ≤ w3 + w4 := by
      have := hcov.2.2 4 (by simp [pattern])
      simpa [pattern] using this
    have hc4 : den ≤ w2 + w4 := by
      have := hcov.2.2 3 (by simp [pattern])
      simpa [pattern] using this
    by_cases hz : s0 n db b = 0 ∨ s1 n db = 0 ∨ s2 n db = 0 ∨
        s3 n db b = 0 ∨ s4 n db = 0
    · have hR : Rfull n db b = 0 := by
        rcases hz with h | h | h | h | h
        · exact Rfull_zero_of_s0 n db b h
        · exact Rfull_zero_of_s1 n db b h
        · exact Rfull_zero_of_s2 n db b h
        · exact Rfull_zero_of_s3 n db b h
        · exact Rfull_zero_of_s4 n db b h
      have href0 : reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b = 0 := by
        rw [reference_eq_Rfull, hR]
      have href : reference pattern n (fun r x y => if db r x y = 0 then (0 : Int) else 1) b = 0 := by
        simpa using href0
      have hzeroPow : (0 : Nat)^den = 0 := by
        cases den with
        | zero => omega
        | succ k => simp
      have hrefb : reference pattern n (fun r x y => if (db r x y == 0) = true then 0 else 1) b = 0 := by
        simpa using href0
      rw [hrefb]
      simp [hzeroPow]
    · have hs0 : 1 ≤ s0 n db b := by
        have := s0_nonneg n db b
        omega
      have hs1 : 1 ≤ s1 n db := by
        have := s1_nonneg n db
        omega
      have hs2 : 1 ≤ s2 n db := by
        have := s2_nonneg n db
        omega
      have hs3 : 1 ≤ s3 n db b := by
        have := s3_nonneg n db b
        omega
      have hs4 : 1 ≤ s4 n db := by
        have := s4_nonneg n db
        omega
      let p := min w1 den
      let q := min w4 den
      have hpden : p ≤ den := Nat.min_le_right _ _
      have hqden : q ≤ den := Nat.min_le_right _ _
      have hw0' : den - p ≤ w0 := by
        dsimp [p]
        by_cases h : w1 ≤ den
        · rw [Nat.min_eq_left h]
          omega
        · have h' : den ≤ w1 := by omega
          rw [Nat.min_eq_right h']
          simp
      have hw1' : p ≤ w1 := Nat.min_le_left _ _
      have hdp2 : den - p ≤ w2 := by
        dsimp [p]
        by_cases h : w1 ≤ den
        · rw [Nat.min_eq_left h]
          omega
        · have h' : den ≤ w1 := by omega
          rw [Nat.min_eq_right h']
          simp
      have hdq2 : den - q ≤ w2 := by
        dsimp [q]
        by_cases h : w4 ≤ den
        · rw [Nat.min_eq_left h]
          omega
        · have h' : den ≤ w4 := by omega
          rw [Nat.min_eq_right h']
          simp
      have hw2' : den - min p q ≤ w2 := by
        by_cases hpq : p ≤ q
        · rw [Nat.min_eq_left hpq]
          exact hdp2
        · have hpq' : q ≤ p := by omega
          rw [Nat.min_eq_right hpq']
          exact hdq2
      have hw3' : den - q ≤ w3 := by
        dsimp [q]
        by_cases h : w4 ≤ den
        · rw [Nat.min_eq_left h]
          omega
        · have h' : den ≤ w4 := by omega
          rw [Nat.min_eq_right h']
          simp
      have hw4' : q ≤ w4 := Nat.min_le_left _ _
      have hmain : (Rfull n db b) ^ den ≤
          (s0 n db b) ^ w0 * (s1 n db) ^ w1 * (s2 n db) ^ w2 *
            (s3 n db b) ^ w3 * (s4 n db) ^ w4 := by
        by_cases hpq : p ≤ q
        · have hdecomp : den = (den - q) + (q - p) + p := by omega
          have h1 := pow_le_pow_of_le (full_le_s0s2s3 n db b) (Rfull_nonneg n db b) (den - q)
          have h2 := pow_le_pow_of_le (full_le_s0s2s4 n db b) (Rfull_nonneg n db b) (q - p)
          have h3 := pow_le_pow_of_le (full_le_s1s4 n db b) (Rfull_nonneg n db b) p
          have hprod : (Rfull n db b)^(den-q) * (Rfull n db b)^(q-p) * (Rfull n db b)^p ≤
              ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-q) *
                ((s0 n db b)*(s2 n db)*(s4 n db))^(q-p) *
                  ((s1 n db)*(s4 n db))^p := by
            have hR0 : 0 ≤ Rfull n db b := Rfull_nonneg n db b
            have hP1 : 0 ≤ (s0 n db b)*(s2 n db)*(s3 n db b) :=
              Int.mul_nonneg (Int.mul_nonneg (by omega) (by omega)) (by omega)
            have hP2 : 0 ≤ (s0 n db b)*(s2 n db)*(s4 n db) :=
              Int.mul_nonneg (Int.mul_nonneg (by omega) (by omega)) (by omega)
            have hP3 : 0 ≤ (s1 n db)*(s4 n db) := Int.mul_nonneg (by omega) (by omega)
            have hbc := Int.mul_le_mul h1 h2 (Int.pow_nonneg hR0) (Int.pow_nonneg hP1)
            have hcd0 : 0 ≤ ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-q) *
                ((s0 n db b)*(s2 n db)*(s4 n db))^(q-p) :=
              Int.mul_nonneg (Int.pow_nonneg hP1) (Int.pow_nonneg hP2)
            exact Int.mul_le_mul hbc h3 (Int.pow_nonneg hR0) hcd0
          have hexpand : ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-q) *
                ((s0 n db b)*(s2 n db)*(s4 n db))^(q-p) *
                  ((s1 n db)*(s4 n db))^p =
              (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-p) *
                (s3 n db b)^(den-q) * (s4 n db)^q := by
            repeat rw [Int.mul_pow]
            have he0 : (den - q) + (q - p) = den - p := by omega
            have he4 : (q - p) + p = q := by omega
            have hrearr : (s0 n db b)^(den-q) * (s2 n db)^(den-q) * (s3 n db b)^(den-q) *
                 ((s0 n db b)^(q-p) * (s2 n db)^(q-p) * (s4 n db)^(q-p)) *
                   ((s1 n db)^p * (s4 n db)^p) =
               ((s0 n db b)^(den-q) * (s0 n db b)^(q-p)) *
                 ((s2 n db)^(den-q) * (s2 n db)^(q-p)) * (s3 n db b)^(den-q) *
                   ((s4 n db)^(q-p) * (s4 n db)^p) * (s1 n db)^p := by grind
            rw [hrearr]
            rw [← Int.pow_add (s0 n db b) (den-q) (q-p)]
            rw [← Int.pow_add (s2 n db) (den-q) (q-p)]
            rw [← Int.pow_add (s4 n db) (q-p) p]
            rw [he0, he4]
            grind
          have hm0 := pow_mono_one (s0 n db b) hs0 (den-p) w0 hw0'
          have hm1 := pow_mono_one (s1 n db) hs1 p w1 hw1'
          have hm2 := pow_mono_one (s2 n db) hs2 (den-p) w2 (by omega)
          have hm3 := pow_mono_one (s3 n db b) hs3 (den-q) w3 hw3'
          have hm4 := pow_mono_one (s4 n db) hs4 q w4 hw4'
          have hmono : (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-p) *
                (s3 n db b)^(den-q) * (s4 n db)^q ≤
              (s0 n db b)^w0 * (s1 n db)^w1 * (s2 n db)^w2 *
                (s3 n db b)^w3 * (s4 n db)^w4 := by
            apply mul5_le hm0 hm1 hm2 hm3 hm4
            all_goals apply Int.pow_nonneg; omega
          calc
            (Rfull n db b)^den = (Rfull n db b)^((den-q)+(q-p)+p) := by
              rw [← hdecomp]
            _ = (Rfull n db b)^(den-q) * (Rfull n db b)^(q-p) * (Rfull n db b)^p := by
                  rw [Int.pow_add, Int.pow_add]
            _ ≤ ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-q) *
                  ((s0 n db b)*(s2 n db)*(s4 n db))^(q-p) *
                    ((s1 n db)*(s4 n db))^p := hprod
            _ = (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-p) *
                  (s3 n db b)^(den-q) * (s4 n db)^q := hexpand
            _ ≤ (s0 n db b)^w0 * (s1 n db)^w1 * (s2 n db)^w2 *
                  (s3 n db b)^w3 * (s4 n db)^w4 := hmono
        · have hpq' : q ≤ p := by omega
          have hdecomp : den = (den - p) + (p - q) + q := by omega
          have h1 := pow_le_pow_of_le (full_le_s0s2s3 n db b) (Rfull_nonneg n db b) (den - p)
          have h2 := pow_le_pow_of_le (full_le_s1s2s3 n db b) (Rfull_nonneg n db b) (p - q)
          have h3 := pow_le_pow_of_le (full_le_s1s4 n db b) (Rfull_nonneg n db b) q
          have hprod : (Rfull n db b)^(den-p) * (Rfull n db b)^(p-q) * (Rfull n db b)^q ≤
              ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-p) *
                ((s1 n db)*(s2 n db)*(s3 n db b))^(p-q) *
                  ((s1 n db)*(s4 n db))^q := by
            have hR0 : 0 ≤ Rfull n db b := Rfull_nonneg n db b
            have hP1 : 0 ≤ (s0 n db b)*(s2 n db)*(s3 n db b) :=
              Int.mul_nonneg (Int.mul_nonneg (by omega) (by omega)) (by omega)
            have hP2 : 0 ≤ (s1 n db)*(s2 n db)*(s3 n db b) :=
              Int.mul_nonneg (Int.mul_nonneg (by omega) (by omega)) (by omega)
            have hP3 : 0 ≤ (s1 n db)*(s4 n db) := Int.mul_nonneg (by omega) (by omega)
            have hbc := Int.mul_le_mul h1 h2 (Int.pow_nonneg hR0) (Int.pow_nonneg hP1)
            have hcd0 : 0 ≤ ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-p) *
                ((s1 n db)*(s2 n db)*(s3 n db b))^(p-q) :=
              Int.mul_nonneg (Int.pow_nonneg hP1) (Int.pow_nonneg hP2)
            exact Int.mul_le_mul hbc h3 (Int.pow_nonneg hR0) hcd0
          have hexpand : ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-p) *
                ((s1 n db)*(s2 n db)*(s3 n db b))^(p-q) *
                  ((s1 n db)*(s4 n db))^q =
              (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-q) *
                (s3 n db b)^(den-q) * (s4 n db)^q := by
            repeat rw [Int.mul_pow]
            have he1 : (p - q) + q = p := by omega
            have he2 : (den - p) + (p - q) = den - q := by omega
            have hrearr : (s0 n db b)^(den-p) * (s2 n db)^(den-p) * (s3 n db b)^(den-p) *
                 ((s1 n db)^(p-q) * (s2 n db)^(p-q) * (s3 n db b)^(p-q)) *
                   ((s1 n db)^q * (s4 n db)^q) =
               (s0 n db b)^(den-p) *
                 ((s1 n db)^(p-q) * (s1 n db)^q) *
                   ((s2 n db)^(den-p) * (s2 n db)^(p-q)) *
                     ((s3 n db b)^(den-p) * (s3 n db b)^(p-q)) * (s4 n db)^q := by grind
            rw [hrearr]
            rw [← Int.pow_add (s1 n db) (p-q) q]
            rw [← Int.pow_add (s2 n db) (den-p) (p-q)]
            rw [← Int.pow_add (s3 n db b) (den-p) (p-q)]
            rw [he1, he2]
          have hm0 := pow_mono_one (s0 n db b) hs0 (den-p) w0 hw0'
          have hm1 := pow_mono_one (s1 n db) hs1 p w1 hw1'
          have hm2 := pow_mono_one (s2 n db) hs2 (den-q) w2 (by omega)
          have hm3 := pow_mono_one (s3 n db b) hs3 (den-q) w3 hw3'
          have hm4 := pow_mono_one (s4 n db) hs4 q w4 hw4'
          have hmono : (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-q) *
                (s3 n db b)^(den-q) * (s4 n db)^q ≤
              (s0 n db b)^w0 * (s1 n db)^w1 * (s2 n db)^w2 *
                (s3 n db b)^w3 * (s4 n db)^w4 := by
            apply mul5_le hm0 hm1 hm2 hm3 hm4
            all_goals apply Int.pow_nonneg; omega
          calc
            (Rfull n db b)^den = (Rfull n db b)^((den-p)+(p-q)+q) := by
              rw [← hdecomp]
            _ = (Rfull n db b)^(den-p) * (Rfull n db b)^(p-q) * (Rfull n db b)^q := by
                  rw [Int.pow_add, Int.pow_add]
            _ ≤ ((s0 n db b)*(s2 n db)*(s3 n db b))^(den-p) *
                  ((s1 n db)*(s2 n db)*(s3 n db b))^(p-q) *
                    ((s1 n db)*(s4 n db))^q := hprod
            _ = (s0 n db b)^(den-p) * (s1 n db)^p * (s2 n db)^(den-q) *
                  (s3 n db b)^(den-q) * (s4 n db)^q := hexpand
            _ ≤ (s0 n db b)^w0 * (s1 n db)^w1 * (s2 n db)^w2 *
                  (s3 n db b)^w3 * (s4 n db)^w4 := hmono
      apply Int.ofNat_le.mp
      rw [show (((reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den : Nat) : Int) =
          ((reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat : Int) ^ den by simp]
      rw [Int.toNat_of_nonneg (by rw [reference_eq_Rfull]; exact Rfull_nonneg n db b)]
      rw [reference_eq_Rfull]
      simp [pattern]
      have h0 := support0_eq n db b
      have h1 := support1_eq n db b
      have h2 := support2_eq n db b
      have h3 := support3_eq n db b
      have h4 := support4_eq n db b
      simp only [pattern] at h0 h1 h2 h3 h4 ⊢
      rw [h0, h1, h2, h3, h4]
      simpa only [Int.mul_assoc] using hmain


end TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplier.Remaining
-- END jpb-copy OrderSupplier Remaining

-- BEGIN jpb-copy Task78 (HubWithChords CollisionCorrection)
namespace TinylambOpt.ResearchProofs.Round2.Task78

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.HubWithChords

example : pattern.freeVars = [1, 2, 5, 4, 3, 0] := by rfl

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

/-- Partition-helper definitions mirroring the `collapsed` definition. -/
private def headOf : List Nat → Nat
  | h :: _ => h
  | [] => 0

private def Heads (bs : List (List Nat)) : List Nat := bs.filterMap List.head?

private def Root (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun bl => bl.contains v)).bind List.head?).getD v

private def constOn (bs : List (List Nat)) (x : Bind) : Bool :=
  bs.all (fun bl => bl.all (fun v => x v == x (headOf bl)))

/-- Boolean structural spec of a partition of the fixed variable list. -/
private def PSb (V : List Nat) (bs : List (List Nat)) : Bool :=
  decide bs.Nodup &&
  bs.all (fun bl => decide bl.Nodup && bl.all (fun v => V.contains v)) &&
  bs.all (fun bl1 => bs.all (fun bl2 =>
    bl1.all (fun v => if bl2.contains v then bl1 == bl2 else true))) &&
  V.all (fun v => bs.any (fun bl => bl.contains v))

private theorem ps_all : (partitions pattern.freeVars).all (fun bs => PSb pattern.freeVars bs) = true := by
  native_decide

private theorem ps_of_mem {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    PSb pattern.freeVars bs = true :=
  List.all_eq_true.mp ps_all bs hbs

private theorem find_eq_some_unique {α} (p : α → Bool) (l : List α) (a : α)
    (ha : a ∈ l) (hp : p a = true) (huniq : ∀ b ∈ l, p b = true → b = a) :
    l.find? p = some a := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      by_cases hxa : x = a
      · subst x
        simp [hp]
      · have hax : a ≠ x := by intro h; exact hxa h.symm
        have haxs : a ∈ xs := by simpa [hax] using ha
        have hpx : p x = false := by
          cases hpx' : p x with
          | true =>
              have hxeq : x = a := huniq x (by simp) hpx'
              exact False.elim (hxa hxeq)
          | false => rfl
        simp [hpx, ih haxs (fun b hb hpb => huniq b (by simp [hb]) hpb)]

private theorem root_eq_head (bs : List (List Nat)) (hPS : PSb pattern.freeVars bs = true) :
    ∀ bl ∈ bs, ∀ v ∈ bl, Root bs v = headOf bl := by
  simp [PSb] at hPS
  rcases hPS with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro bl hbl v hv
  have hpbl : bl.contains v = true := by simpa using hv
  have huniq : ∀ b ∈ bs, b.contains v = true → b = bl := by
    intro b hb hbv
    have hdeq := hdisj bl hbl b hb v hv
    have hbmem : v ∈ b := by simpa using hbv
    rcases hdeq with hnot | heq
    · exact False.elim (hnot hbmem)
    · exact heq.symm
  have hfind : bs.find? (fun b : List Nat => b.contains v) = some bl :=
    find_eq_some_unique (fun b : List Nat => b.contains v) bs bl hbl hpbl huniq
  unfold Root
  rw [hfind]
  cases bl with
  | nil => simp at hv
  | cons h tl => rfl

private theorem head_mem_heads_cons (bl : List Nat) (bs : List (List Nat))
    (hs : bl.head?.isSome = true) : headOf bl ∈ Heads (bl :: bs) := by
  cases bl with
  | nil => simp at hs
  | cons h tl => simp [Heads, headOf]

private theorem head_mem_heads {bl : List Nat} {bs : List (List Nat)}
    (hbl : bl ∈ bs) : bl.head?.isSome = true → headOf bl ∈ Heads bs := by
  induction bs with
  | nil => simp at hbl
  | cons b bs ih =>
      intro hs
      rcases List.mem_cons.mp hbl with hbeq | hbl
      · subst bl
        exact head_mem_heads_cons b bs hs
      · have htail := ih hbl hs
        cases hb : b.head? with
        | none => simpa [Heads, hb] using htail
        | some h =>
            simp [Heads, hb]
            apply Or.inr
            exact List.mem_filterMap.mp htail

private def HPb (bs : List (List Nat)) : Bool :=
  decide (Heads bs).Nodup && (Heads bs).all (fun h => pattern.freeVars.contains h)

private theorem hp_all : (partitions pattern.freeVars).all HPb = true := by
  native_decide

private theorem heads_spec {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    (Heads bs).Nodup ∧ (∀ h ∈ Heads bs, h ∈ pattern.freeVars) := by
  have h := List.all_eq_true.mp hp_all bs hbs
  simp [HPb] at h
  exact h

private theorem find_none_of_all_false {α} (p : α → Bool) (l : List α)
    (h : ∀ a ∈ l, p a = false) : l.find? p = none := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [List.find?, h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem root_out {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v, v ∉ pattern.freeVars → Root bs v = v := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  have hfalse : ∀ bl ∈ bs, bl.contains v = false := by
    intro bl hbl
    have hbsub := hsub bl hbl
    by_cases hne : bl.contains v = true
    · have hvbl : v ∈ bl := by simpa using hne
      exact False.elim (hv (hbsub.2 v hvbl))
    · cases hne' : bl.contains v <;> simp_all
  unfold Root
  rw [find_none_of_all_false (fun bl : List Nat => bl.contains v) bs hfalse]
  simp

private theorem root_head {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ h ∈ Heads bs, Root bs h = h := by
  intro h hh
  rcases List.mem_filterMap.mp hh with ⟨bl, hbl, hhbl⟩
  have hhroot : headOf bl = h := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simpa [headOf] using hhbl
  have hvbl : headOf bl ∈ bl := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simp [headOf]
  rw [← hhroot]
  exact root_eq_head bs hPS bl hbl (headOf bl) hvbl

private theorem root_mem_heads {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v ∈ pattern.freeVars, Root bs v ∈ Heads bs := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  rcases hcov v hv with ⟨bl, hbl, hvbl⟩
  rw [root_eq_head bs hPS bl hbl v hvbl]
  exact head_mem_heads hbl (by cases bl with | nil => simp at hvbl | cons h t => simp)

private theorem constOn_iff {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) (x : Bind) :
    constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  constructor
  · intro hc v hv
    rcases hcov v hv with ⟨bl, hbl, hvbl⟩
    rw [root_eq_head bs hPS bl hbl v hvbl]
    have hbeq := List.all_eq_true.mp hc bl hbl
    have hveq := List.all_eq_true.mp hbeq v hvbl
    simpa using hveq
  · intro hx
    apply List.all_eq_true.mpr
    intro bl hbl
    apply List.all_eq_true.mpr
    intro v hvbl
    have hvV : v ∈ pattern.freeVars := (hsub bl hbl).2 v hvbl
    have hrv := hx v hvV
    rw [root_eq_head bs hPS bl hbl v hvbl] at hrv
    simpa using hrv

/-- Integration helpers (Std-only). -/
private theorem sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_eq_zero {α} (l : List α) (f : α → Int) (h : ∀ x ∈ l, f x = 0) :
    (l.map f).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]
      simp

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    simp [put]
  · simp [put, hi]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : (v != w) = true) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hv : i == v
  · by_cases hw : i == w
    · have hiv : i = v := Nat.eq_of_beq_eq_true hv
      have hiw : i = w := Nat.eq_of_beq_eq_true hw
      have hvw : v = w := by omega
      simp [hvw] at h
    · simp [put, hv, hw]
  · by_cases hw : i == w <;> simp [put, hv, hw]

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by cases h' : (v == w) <;> simp_all
    change ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
    change rsum n (fun x => rsum n (fun y => integrate n vs (put (put b v x) w y) f)) =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b w x) v y) f))
    rw [rsum_comm]
    apply sum_congr
    intro x hx
    apply sum_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

private theorem integrate_append (n : Nat) (xs ys : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (xs ++ ys) b f = integrate n xs b (fun b' => integrate n ys b' f) := by
  induction xs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n (vs ++ ys) (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) (fun b' => integrate n ys b' f))).sum
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_perm (n : Nat) {xs ys : List Nat} (h : xs.Perm ys)
    (b : Bind) (f : Bind → Int) : integrate n xs b f = integrate n ys b f := by
  induction h generalizing b with
  | nil => rfl
  | cons x h ih =>
      change ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum
      apply sum_congr
      intro t ht
      exact ih (put b x t)
  | swap x y l => exact (integrate_swap n x y l b f).symm
  | trans h1 h2 ih1 ih2 => exact (ih1 b).trans (ih2 b)

private def bindings (n : Nat) : List Nat → Bind → List Bind
  | [], b => [b]
  | v :: vs, b => (List.range n).flatMap (fun x => bindings n vs (put b v x))

private theorem bindings_sum (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b f = ((bindings n vs b).map f).sum := by
  induction vs generalizing b with
  | nil => simp [integrate, bindings]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        (((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f).sum
      rw [show ((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f =
          (List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f) by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.map_append, ih2]]
      rw [show ((List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f)).sum =
          ((List.range n).map (fun x => ((bindings n vs (put b v x)).map f).sum)).sum by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.sum_append, ih2]]
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem bindings_fixed (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∉ vs, y v = b v := by
  induction vs generalizing b with
  | nil =>
      intro y hy v hv
      simp [bindings] at hy
      subst y
      rfl
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      have hvw : v ≠ w := by intro h; subst v; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      rw [ih (put b w x) y hy v hvws]
      simp [put, hvw]

private theorem bindings_range (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∈ vs, y v ∈ List.range n := by
  induction vs generalizing b with
  | nil => simp
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      by_cases hvw : w = v
      · subst w
        by_cases htail : v ∈ ws
        · exact ih (put b v x) y hy v htail
        · have heq := bindings_fixed n ws (put b v x) y hy v htail
          rw [heq]
          simpa [put] using hx
      · have htail : v ∈ ws := by
          rcases List.mem_cons.mp hv with h | h
          · exact (hvw h.symm).elim
          · exact h
        exact ih (put b w x) y hy v htail

private theorem integrate_congr_bindings (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ y, y ∈ bindings n vs b → f y = g y) :
    integrate n vs b f = integrate n vs b g := by
  rw [bindings_sum, bindings_sum]
  apply sum_congr
  intro y hy
  exact h y hy

private theorem rsum_ite_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    rsum n (fun x => if x = a then f x else 0) = f a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : a = n
      · subst a
        have hzero : rsum n (fun x => if x = n then f x else 0) = 0 := by
          rw [show rsum n (fun x => if x = n then f x else 0) = rsum n (fun _ => (0 : Int)) by
            apply rsum_congr
            intro x hx
            have hxn : x ≠ n := by omega
            simp [hxn]]
          exact rsum_zero_const n
        rw [hzero]
        simp
      · have han : a < n := by omega
        rw [ih han]
        have hne : ¬ n = a := by omega
        simp [hne]

private theorem integrate_zero_of_false (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (h : ∀ y ∈ bindings n vs b, f y = 0) : integrate n vs b f = 0 := by
  rw [bindings_sum]
  apply sum_eq_zero
  intro y hy
  exact h y hy

private theorem inner_collapse (n : Nat) (M : List Nat) (Root : Nat → Nat) (y : Bind) (C : Int)
    (hM : M.Nodup) (hRnot : ∀ m ∈ M, Root m ∉ M)
    (hy : ∀ m ∈ M, y (Root m) ∈ List.range n) :
    integrate n M y (fun x => if M.all (fun m => x m == y (Root m)) then C else 0) = C := by
  induction M generalizing y with
  | nil => simp [integrate]
  | cons m Ms ih =>
      have hmMs : m ∉ Ms := (List.nodup_cons.mp hM).1
      have hMnd : Ms.Nodup := (List.nodup_cons.mp hM).2
      have hRnot' : ∀ v ∈ Ms, Root v ∉ Ms := by
        intro v hv hmem
        exact hRnot v (by simp [hv]) (by simp [hmem])
      have hrootne : ∀ v ∈ Ms, Root v ≠ m := by
        intro v hv h_eq
        exact hRnot v (by simp [hv]) (by simp [h_eq])
      have hpoint : ∀ t < n,
          integrate n Ms (put y m t)
            (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) =
          if t = y (Root m) then C else 0 := by
        intro t ht
        by_cases htm : t = y (Root m)
        · subst t
          have hyrange' : ∀ v ∈ Ms, (put y m (y (Root m))) (Root v) ∈ List.range n := by
            intro v hv
            have hne := hrootne v hv
            simpa [put, hne] using hy v (by simp [hv])
          calc
            integrate n Ms (put y m (y (Root m)))
                (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)
                = integrate n Ms (put y m (y (Root m)))
                    (fun x => if Ms.all (fun v => x v == (put y m (y (Root m))) (Root v)) then C else 0) := by
                    apply integrate_congr_bindings
                    intro x hx
                    have hxm : x m = y (Root m) := by
                      rw [bindings_fixed n Ms (put y m (y (Root m))) x hx m hmMs]
                      simp [put]
                    simp [hxm]
                    by_cases hleft : ∀ v ∈ Ms, x v = y (Root v)
                    · have hright : ∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v) := by
                        intro v hv
                        have hv' := hleft v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_left hleft, ite_eq_left hright]
                    · have hrightnot : ¬ (∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v)) := by
                        intro hr
                        apply hleft
                        intro v hv
                        have hv' := hr v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_right hleft, ite_eq_right hrightnot]
            _ = C := ih (put y m (y (Root m))) hMnd hRnot' hyrange'
            _ = (if y (Root m) = y (Root m) then C else 0) := by simp
        · have hzero : integrate n Ms (put y m t)
              (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) = 0 := by
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htm]
          simpa [htm] using hzero
      change rsum n (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) = C
      rw [show (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) =
        (fun t => if t = y (Root m) then C else 0) by
          funext t
          by_cases ht : t < n
          · exact hpoint t ht
          · have htn : y (Root m) < n := by
              have := hy m (by simp)
              simpa using this
            have htne : t ≠ y (Root m) := by omega
            simp [htne]
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htne]]
      rw [rsum_ite_eq_single n (y (Root m)) (by
        have := hy m (by simp)
        simpa using this)]

private theorem perm_append_left {α} {l1 l2 : List α} (h : l1.Perm l2) (l3 : List α) :
    (l1 ++ l3).Perm (l2 ++ l3) := by
  induction h with
  | nil => rfl
  | cons x h ih => exact .cons x ih
  | swap x y l => exact .swap x y (l ++ l3)
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem perm_append_right {α} (l1 : List α) {l3 l4 : List α} (h : l3.Perm l4) :
    (l1 ++ l3).Perm (l1 ++ l4) := by
  induction l1 with
  | nil => simpa using h
  | cons a l1 ih => exact .cons a ih

private theorem perm_append {α} {l1 l2 l3 l4 : List α} (h1 : l1.Perm l2) (h2 : l3.Perm l4) :
    (l1 ++ l3).Perm (l2 ++ l4) := by
  exact (perm_append_left h1 l3).trans (perm_append_right l2 h2)

private theorem cons_append_perm (a : Nat) (l : List Nat) : (a :: l).Perm (l ++ [a]) := by
  induction l with
  | nil => simp
  | cons x l ih => exact (List.Perm.swap x a l).trans (.cons x ih)

private theorem filter_append_compl (p : Nat → Bool) (l : List Nat) :
    l.Perm ((l.filter p) ++ (l.filter (fun x => !p x))) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : p a = true
      · simpa [ha] using (List.Perm.cons a ih)
      · have ha' : p a = false := by cases h : p a <;> simp_all
        have hstep := (List.Perm.cons a ih).trans
          (perm_append (cons_append_perm a (l.filter p))
            (List.Perm.refl (l.filter (fun x => !p x))))
        simpa [ha'] using hstep

private theorem perm_filter_of_nodup {l l' : List Nat} (hnd : l.Nodup) (hnd' : l'.Nodup)
    (hsub : ∀ x ∈ l, x ∈ l') : l.Perm (l'.filter (fun x => x ∈ l)) := by
  induction l' generalizing l with
  | nil =>
      have hl : l = [] := by
        cases l with
        | nil => rfl
        | cons a as => exact False.elim (by simpa using hsub a (by simp))
      subst l
      simp
  | cons a l' ih =>
      have hnot : a ∉ l' := (List.nodup_cons.mp hnd').1
      by_cases ha : a ∈ l
      · have htail := ih (hnd.erase a) (List.nodup_cons.mp hnd').2 (by
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnd.not_mem_erase hx
          have hxl := (List.mem_erase_of_ne hxa).mp hx
          simpa [hxa] using hsub x hxl)
        have hab : decide (a ∈ l) = true := by simp [ha]
        have hfilter : (a :: l').filter (fun x => x ∈ l) =
            a :: l'.filter (fun x : Nat => x ∈ l.erase a) := by
          rw [List.filter_cons, ite_eq_left hab]
          congr 1
          apply List.filter_congr
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnot hx
          simp [List.mem_erase_of_ne hxa]
        rw [hfilter]
        exact (List.perm_cons_erase ha).trans (.cons a htail)
      · rw [List.filter_cons]
        simp [ha]
        apply ih hnd (List.nodup_cons.mp hnd').2
        intro x hx
        have hxa : x ≠ a := by intro h; subst x; exact ha hx
        simpa [hxa] using hsub x hx

private theorem collapse_sum (n : Nat) (b : Bind) (F : Bind → Int)
    (bs : List (List Nat)) (hbs : bs ∈ partitions pattern.freeVars)
    (hF : ∀ x y, (∀ v ∈ pattern.freeVars, x v = y v) → F x = F y) :
    integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) =
      integrate n pattern.freeVars b (fun x => if constOn bs x then F x else 0) := by
  have hPS := ps_of_mem hbs
  have hspec := heads_spec hbs
  have hVnd : pattern.freeVars.Nodup := by native_decide
  let H := Heads bs
  let M := pattern.freeVars.filter (fun v => !H.contains v)
  have hHsub : ∀ v ∈ H, v ∈ pattern.freeVars := hspec.2
  have hrootV : ∀ v ∈ pattern.freeVars, Root bs v ∈ H := by
    intro v hv
    exact root_mem_heads hPS v hv
  have hrootH : ∀ h ∈ H, Root bs h = h := by
    intro h hh
    exact root_head hPS h hh
  have hrootOut : ∀ v, v ∉ pattern.freeVars → Root bs v = v := root_out hPS
  have hconst : ∀ x, constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := constOn_iff hPS
  have hfilt : (pattern.freeVars.filter (fun v => H.contains v)).Perm H := by
    have hp := perm_filter_of_nodup hspec.1 hVnd hHsub
    simpa using hp.symm
  have hpermV : pattern.freeVars.Perm (H ++ M) := by
    have h1 := filter_append_compl H.contains pattern.freeVars
    have h2 : (pattern.freeVars.filter (fun v => H.contains v) ++
        pattern.freeVars.filter (fun v => !H.contains v)).Perm
        (H ++ M) := perm_append hfilt (by rfl)
    exact h1.trans h2
  rw [integrate_perm n hpermV b (fun x => if constOn bs x then F x else 0)]
  rw [integrate_append]
  apply integrate_congr_bindings
  intro y hy
  have hyH : ∀ h ∈ H, y h ∈ List.range n := by
    intro h hh
    exact bindings_range n H b y hy h hh
  have hyfixed : ∀ v ∉ H, y v = b v := by
    intro v hv
    exact bindings_fixed n H b y hy v hv
  let x0 : Bind := fun v => y (Root bs v)
  have hx0V : ∀ v ∈ pattern.freeVars, x0 v ∈ List.range n := by
    intro v hv
    have hr := hrootV v hv
    simpa [x0] using hyH (Root bs v) hr
  have hMsub : ∀ v ∈ M, v ∈ pattern.freeVars := by
    intro v hv
    exact (List.mem_filter.mp hv).1
  have hMnotH : ∀ v ∈ M, v ∉ H := by
    intro v hv
    have hnot := (List.mem_filter.mp hv).2
    simpa using hnot
  have hMnd : M.Nodup := hVnd.filter _
  have hRnot : ∀ m ∈ M, Root bs m ∉ M := by
    intro m hm hmem
    have hr := hrootV m (hMsub m hm)
    exact hMnotH (Root bs m) hmem hr
  have hyrange : ∀ m ∈ M, y (Root bs m) ∈ List.range n := by
    intro m hm
    exact hyH (Root bs m) (hrootV m (hMsub m hm))
  have hinner : integrate n M y (fun x => if constOn bs x then F x else 0) = F x0 := by
    calc
      integrate n M y (fun x => if constOn bs x then F x else 0)
          = integrate n M y (fun x => if M.all (fun m => x m == y (Root bs m)) then F x0 else 0) := by
            apply integrate_congr_bindings
            intro x hx
            have hxfixedH : ∀ h ∈ H, x h = y h := by
              intro h hh
              have hnot : h ∉ M := by
                intro hm
                exact hMnotH h hm hh
              rw [bindings_fixed n M y x hx h hnot]
            by_cases hall : M.all (fun m => x m == y (Root bs m)) = true
            · rw [ite_eq_left hall]
              have hxFx0 : ∀ v ∈ pattern.freeVars, x v = x0 v := by
                intro v hv
                by_cases hvH : v ∈ H
                · have hrv := hrootH v hvH
                  change x v = y (Root bs v)
                  rw [hxfixedH v hvH, hrv]
                · have hvM : v ∈ M := by
                    simp [M, hv, hvH]
                  have hvEq := List.all_eq_true.mp hall v hvM
                  change x v = y (Root bs v)
                  simpa using hvEq
              have hcond : ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
                intro v hv
                have hxv := hxFx0 v hv
                have hxroot : x (Root bs v) = y (Root bs v) :=
                  hxfixedH (Root bs v) (hrootV v hv)
                exact hxv.trans (by simpa [x0] using hxroot.symm)
              rw [ite_eq_left ((hconst x).2 hcond)]
              exact hF x x0 hxFx0
            · have hallnot : ¬ M.all (fun m => x m == y (Root bs m)) = true := by
                intro h
                exact hall h
              rw [ite_eq_right hallnot]
              have hcondnot : ¬ (∀ v ∈ pattern.freeVars, x v = x (Root bs v)) := by
                intro hc
                apply hall
                apply List.all_eq_true.mpr
                intro m hm
                have hcv := hc m (hMsub m hm)
                have hrv := hrootV m (hMsub m hm)
                have hxrv : x (Root bs m) = y (Root bs m) := hxfixedH (Root bs m) hrv
                simp [hcv, hxrv]
              have hconstnot : ¬ constOn bs x = true := by
                intro hx'
                exact hcondnot ((hconst x).1 hx')
              rw [ite_eq_right hconstnot]
      _ = F x0 := inner_collapse n M (fun v => Root bs v) y (F x0) hMnd hRnot hyrange
  simpa [x0] using hinner.symm

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum = (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l' with
  | nil =>
      change (l.map (fun _ => (0 : Int))).sum = 0
      exact sum_zero l
  | cons b l' ih =>
      rw [show (fun a => (List.map (F a) (b :: l')).sum) =
          (fun a => F a b + (l'.map (F a)).sum) by funext a; rfl]
      rw [sum_add, ih]
      simp


/-- ## Generic Möbius inversion over equality partitions -/

private theorem sum_flatMap {α β : Type} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [List.flatMap_cons, ih]

private theorem sum_range_succ_shift (m : Nat) (f : Nat → Int) :
    f 0 + ((List.range m).map (fun i => f (i + 1))).sum =
      ((List.range (m + 1)).map f).sum := by
  induction m with
  | zero => simp
  | succ m ih =>
      calc
        f 0 + ((List.range (m + 1)).map (fun i => f (i + 1))).sum
            = f 0 + (((List.range m).map (fun i => f (i + 1))).sum + f (m + 1)) := by
                rw [List.range_succ]
                simp
        _ = (f 0 + ((List.range m).map (fun i => f (i + 1))).sum) + f (m + 1) := by
                rw [Int.add_assoc]
        _ = ((List.range (m + 1)).map f).sum + f (m + 1) := by rw [ih]
        _ = ((List.range (m + 2)).map f).sum := by
                rw [show List.range (m + 2) = List.range (m + 1) ++ [m + 1] by
                  simpa using (List.range_succ (n := m + 1))]
                simp

private theorem sum_zipIdx (bs : List (List Nat)) (f : Nat → Int) :
    (bs.zipIdx.map (fun p => f p.2)).sum = ((List.range bs.length).map f).sum := by
  induction bs generalizing f with
  | nil => simp
  | cons b bs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [show bs.zipIdx 1 = (bs.zipIdx).map (fun p => (p.1, p.2 + 1)) by
        simpa using (List.zipIdx_succ (l := bs) (i := 0))]
      rw [List.map_map]
      change f 0 + (bs.zipIdx.map (fun p => f (p.2 + 1))).sum =
        (List.map f (List.range (b :: bs).length)).sum
      rw [ih (fun i => f (i + 1))]
      rw [sum_range_succ_shift]
      simp

private theorem mobius_singleton (v : Nat) (bs : List (List Nat)) :
    mobius ([v] :: bs) = mobius bs := by
  simp [mobius, factorial]

private theorem mobius_cons (b : List Nat) (bs : List (List Nat)) :
    mobius (b :: bs) = ((-1 : Int)^(b.length-1) * (factorial (b.length-1) : Int)) * mobius bs := by
  simp [mobius]

private theorem block_weight_modify (k : Nat) (hk : 0 < k) :
    (-1 : Int)^k * (factorial k : Int) =
      -(k : Int) * ((-1 : Int)^(k-1) * (factorial (k-1) : Int)) := by
  cases k with
  | zero => omega
  | succ k =>
      cases k with
      | zero => simp [factorial]
      | succ k =>
          simp [factorial]
          rw [show k + 1 + 1 = (k + 1) + 1 by omega, Int.pow_succ]
          grind

private theorem modify_succ (b : List Nat) (bs : List (List Nat)) (i : Nat)
    (f : List Nat → List Nat) : (b :: bs).modify (i + 1) f = b :: bs.modify i f := by
  rw [List.modify_cons]
  simp

private theorem mobius_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (hn : ∀ bl ∈ bs, bl ≠ []) :
    mobius (bs.modify i (fun bl => v :: bl)) = -((bs.get ⟨i, hi⟩).length : Int) * mobius bs := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero =>
          have hb : b ≠ [] := hn b (by simp)
          cases b with
          | nil => simp at hb
          | cons a as =>
              simp [List.modify, mobius]
              rw [block_weight_modify (as.length + 1) (by omega)]
              grind
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          have hn' : ∀ bl ∈ bs, bl ≠ [] := by intro bl hbl; exact hn bl (by simp [hbl])
          rw [modify_succ]
          rw [mobius_cons, mobius_cons, ih i hi' hn']
          simp only [List.get_cons_succ]
          grind

private def cOnP (bs : List (List Nat)) (x : Bind) : Prop :=
  ∀ bl ∈ bs, ∀ u ∈ bl, x u = x (headOf bl)

private theorem constOn_eq_cOnP (bs : List (List Nat)) (x : Bind) :
    constOn bs x = true ↔ cOnP bs x := by
  simp [constOn, cOnP, List.all_eq_true, beq_iff_eq]

private theorem constOn_singleton (v : Nat) (bs : List (List Nat)) (x : Bind) :
    constOn ([v] :: bs) x = constOn bs x := by
  simp [constOn, headOf]

private theorem cOnP_cons (b : List Nat) (bs : List (List Nat)) (x : Bind) :
    cOnP (b :: bs) x ↔ (∀ u ∈ b, x u = x (headOf b)) ∧ cOnP bs x := by
  simp [cOnP]

private theorem cOnP_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (x : Bind) (hn : ∀ bl ∈ bs, bl ≠ []) :
    cOnP (bs.modify i (fun bl => v :: bl)) x ↔
      (cOnP bs x ∧ ∀ u ∈ bs.get ⟨i, hi⟩, x u = x v) := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero =>
          have hb : b ≠ [] := hn b (by simp)
          cases b with
          | nil => simp at hb
          | cons a as =>
              simp only [List.modify_cons]
              simp only [ite_true]
              unfold cOnP
              simp only [List.mem_cons, forall_eq_or_imp]
              constructor
              · intro h
                rcases h with ⟨hnew, hrest⟩
                simp [headOf] at hnew ⊢
                constructor
                · constructor
                  · intro u hu
                    exact (hnew.2 u hu).trans hnew.1.symm
                  · exact hrest
                · exact hnew
              · intro h
                rcases h with ⟨⟨_, hrest⟩, hnew⟩
                simp [headOf] at hnew ⊢
                constructor
                · exact hnew
                · exact hrest
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          have hn' : ∀ bl ∈ bs, bl ≠ [] := by intro bl hbl; exact hn bl (by simp [hbl])
          rw [modify_succ]
          rw [cOnP_cons, cOnP_cons, ih i hi' hn']
          simp only [List.get_cons_succ]
          constructor <;> intro h <;> rcases h with ⟨h1, h2⟩ <;> constructor <;> simp_all

private theorem constOn_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (x : Bind) (hn : ∀ bl ∈ bs, bl ≠ []) :
    constOn (bs.modify i (fun bl => v :: bl)) x =
      (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) := by
  apply Bool.eq_iff_iff.mpr
  rw [constOn_eq_cOnP]
  rw [Bool.and_eq_true]
  rw [List.all_eq_true]
  rw [constOn_eq_cOnP]
  constructor
  · intro h
    have hh := (cOnP_modify bs i hi v x hn).mp h
    exact ⟨hh.1, by intro u hu; exact beq_iff_eq.mpr (hh.2 u hu)⟩
  · intro h
    apply (cOnP_modify bs i hi v x hn).mpr
    exact ⟨h.1, by intro u hu; exact beq_iff_eq.mp (h.2 u hu)⟩

private theorem partitions_nonempty : ∀ (V : List Nat) (bs : List (List Nat)),
    bs ∈ partitions V → ∀ bl ∈ bs, bl ≠ [] := by
  intro V
  induction V with
  | nil =>
      intro bs hbs
      simp [partitions] at hbs
      subst bs
      intro bl hbl
      simp at hbl
  | cons v vs ih =>
      intro bs hbs
      simp only [partitions, List.mem_flatMap] at hbs
      rcases hbs with ⟨bls, hbls, hmem⟩
      simp only [List.mem_cons, List.mem_map] at hmem
      rcases hmem with rfl | ⟨p, hp, rfl⟩
      · intro bl hbl
        simp only [List.mem_cons] at hbl
        rcases hbl with rfl | hbl
        · simp
        · exact ih bls hbls bl hbl
      · rcases p with ⟨_, i⟩
        have hi : i < bls.length := by simpa using (List.mem_zipIdx hp).2.1
        intro bl hbl
        rcases List.mem_iff_getElem.mp hbl with ⟨j, hj, hjEq⟩
        have hjbs : j < bls.length := by simpa using hj
        rw [List.getElem_modify] at hjEq
        by_cases hij : i = j
        · rw [if_pos hij] at hjEq
          rw [← hjEq]
          simp
        · rw [if_neg hij] at hjEq
          rw [← hjEq]
          exact ih bls hbls (bls[j]) (List.getElem_mem hjbs)

private theorem perm_append_cons (b : List Nat) (v : Nat) (l : List Nat) :
    (b ++ (v :: l)).Perm (v :: (b ++ l)) := by
  induction b with
  | nil => simp
  | cons x xs ih =>
      simp only [List.cons_append]
      exact (List.Perm.cons x ih).trans (List.Perm.swap v x (xs ++ l))

private theorem modify_flatten_perm (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) : (bs.modify i (fun bl => v :: bl)).flatten.Perm (v :: bs.flatten) := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero => simp [List.modify]
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          rw [modify_succ]
          simp only [List.flatten_cons]
          exact (List.Perm.append_left b (ih i hi')).trans (perm_append_cons b v bs.flatten)

private theorem partitions_flatten_perm : ∀ (V : List Nat) (bs : List (List Nat)),
    bs ∈ partitions V → bs.flatten.Perm V := by
  intro V
  induction V with
  | nil =>
      intro bs hbs
      simp [partitions] at hbs
      simpa [hbs]
  | cons v vs ih =>
      intro bs hbs
      simp only [partitions, List.mem_flatMap] at hbs
      rcases hbs with ⟨bls, hbls, hmem⟩
      simp only [List.mem_cons, List.mem_map] at hmem
      rcases hmem with rfl | ⟨p, hp, rfl⟩
      · simpa using (List.Perm.cons v (ih bls hbls))
      · rcases p with ⟨_, i⟩
        have hi : i < bls.length := by simpa using (List.mem_zipIdx hp).2.1
        exact (modify_flatten_perm bls i hi v).trans (List.Perm.cons v (ih bls hbls))

private theorem countP_of_all_eq (l : List Nat) (x : Nat → Nat) (c : Nat)
    (h : ∀ u ∈ l, x u = c) : l.countP (fun u => x u == c) = l.length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ha : x a = c := h a (by simp)
      have ht : ∀ u ∈ l, x u = c := by intro u hu; exact h u (by simp [hu])
      simp [ha, ih ht]

private theorem sum_block_count (bs : List (List Nat)) (x : Nat → Nat) (v : Nat)
    (hc : constOn bs x = true) :
    (bs.map (fun bl => if x v == x (headOf bl) then (bl.length : Int) else 0)).sum =
      (bs.flatten.countP (fun u => x u == x v) : Int) := by
  have hcp : cOnP bs x := (constOn_eq_cOnP bs x).mp hc
  induction bs with
  | nil => simp
  | cons b bs ih =>
      have hcb : ∀ u ∈ b, x u = x (headOf b) := by
        intro u hu
        exact hcp b (by simp) u hu
      have htailP : cOnP bs x := by
        intro bl hbl u hu
        exact hcp bl (by simp [hbl]) u hu
      have htail : constOn bs x = true := (constOn_eq_cOnP bs x).mpr htailP
      simp only [List.map_cons, List.sum_cons, List.flatten_cons]
      rw [ih htail htailP]
      by_cases h : x v == x (headOf b)
      · have hvh : x (headOf b) = x v := (beq_iff_eq.mp h).symm
        rw [if_pos h]
        have hcnt : b.countP (fun u => x u == x v) = b.length := by
          apply countP_of_all_eq
          intro u hu
          exact (hcb u hu).trans hvh
        simp [hcnt, Int.natCast_add]
      · rw [if_neg h]
        have hcnt : b.countP (fun u => x u == x v) = 0 := by
          rw [List.countP_eq_zero.mpr]
          intro u hu heq
          have huv : x u = x v := beq_iff_eq.mp heq
          have hub : x u = x (headOf b) := hcb u hu
          apply h
          simp [← hub, huv]
        simp [hcnt]

private theorem countP_le_one_of_map_nodup (l : List Nat) (v : Nat) (x : Nat → Nat)
    (h : (l.map x).Nodup) : l.countP (fun u => x u == x v) ≤ 1 := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ht : (l.map x).Nodup := (List.nodup_cons.mp h).2
      have hi := ih ht
      have hnot : x a ∉ l.map x := (List.nodup_cons.mp h).1
      simp only [List.map_cons, List.countP_cons]
      by_cases ha : x a == x v
      · have hzero : l.countP (fun u => x u == x v) = 0 := by
          rw [List.countP_eq_zero.mpr]
          intro u hu heq
          have huv : x u = x v := beq_iff_eq.mp heq
          have hva : x a = x v := beq_iff_eq.mp ha
          have : x u ∈ l.map x := List.mem_map.mpr ⟨u, hu, rfl⟩
          exact hnot (by simpa [huv, hva] using this)
        simp [ha, hzero]
      · simp [ha]
        omega

private theorem countP_pos_mem (l : List Nat) (v : Nat) (x : Nat → Nat)
    (h : 0 < l.countP (fun u => x u == x v)) : x v ∈ l.map x := by
  rcases List.countP_pos_iff.mp h with ⟨u, hu, huv⟩
  apply List.mem_map.mpr
  refine ⟨u, hu, ?_⟩
  exact beq_iff_eq.mp huv

private theorem zipIdx_fst (bs : List (List Nat)) : bs.zipIdx.map (fun p => p.1) = bs := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [List.zipIdx_cons, ih]

private theorem sum_zipIdx_fst (bs : List (List Nat)) (g : List Nat → Int) :
    (bs.zipIdx.map (fun p => g p.1)).sum = (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [List.zipIdx_succ]
      simp only [List.map_map]
      change g b + (bs.zipIdx.map (fun p => g p.1)).sum = g b + (bs.map g).sum
      rw [ih]

private theorem mobius_point_gen (V : List Nat) (hV : V.Nodup) (x : Bind) :
    ((partitions V).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
      (if (V.map x).Nodup then (1 : Int) else 0) := by
  induction V with
  | nil => simp [partitions, mobius, constOn]
  | cons v vs ih =>
      have hvs : vs.Nodup := (List.nodup_cons.mp hV).2
      have ih' := ih hvs
      let k : Int := (vs.countP (fun u => x u == x v) : Int)
      have hmod (bs : List (List Nat)) (hn : ∀ bl ∈ bs, bl ≠ [])
          (i : Nat) (hi : i < bs.length) :
          mobius (bs.modify i (fun bl => v :: bl)) *
              (if constOn (bs.modify i (fun bl => v :: bl)) x then (1 : Int) else 0) =
            mobius bs * (if constOn bs x then
              (if x v == x (headOf (bs.get ⟨i, hi⟩)) then
                -((bs.get ⟨i, hi⟩).length : Int) else 0) else 0) := by
        rw [mobius_modify bs i hi v hn, constOn_modify bs i hi v x hn]
        by_cases hc : constOn bs x = true
        · have hcp : cOnP bs x := (constOn_eq_cOnP bs x).mp hc
          have hblock : bs.get ⟨i, hi⟩ ≠ [] := hn _ (List.getElem_mem hi)
          have hheadmem : headOf (bs.get ⟨i, hi⟩) ∈ bs.get ⟨i, hi⟩ := by
            cases hblk : bs.get ⟨i, hi⟩ with
            | nil =>
                rw [hblk] at hblock
                exact False.elim (hblock rfl)
            | cons a as => simp [headOf]
          by_cases hhd : x v == x (headOf (bs.get ⟨i, hi⟩))
          · have hall : (bs.get ⟨i, hi⟩).all (fun u => x u == x v) = true := by
              apply List.all_eq_true.mpr
              intro u hu
              have huEq := hcp (bs.get ⟨i, hi⟩) (List.getElem_mem hi) u hu
              have hhv : x (headOf (bs.get ⟨i, hi⟩)) = x v := (beq_iff_eq.mp hhd).symm
              exact beq_iff_eq.mpr (huEq.trans hhv)
            rw [if_pos hc, if_pos hhd]
            have hand : (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
              rw [hc, hall]
              rfl
            rw [if_pos hand]
            grind
          · have hallfalse : (bs.get ⟨i, hi⟩).all (fun u => x u == x v) = false := by
              cases hall' : (bs.get ⟨i, hi⟩).all (fun u => x u == x v)
              · rfl
              · have hhv' := List.all_eq_true.mp hall' (headOf (bs.get ⟨i, hi⟩)) hheadmem
                have hhv : x v = x (headOf (bs.get ⟨i, hi⟩)) := (beq_iff_eq.mp hhv').symm
                exact False.elim (hhd (by simp [hhv]))
            rw [if_pos hc, if_neg hhd]
            have hand : (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = false := by
              rw [hc, hallfalse]
              rfl
            have handnot : ¬ (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
              rw [hand]
              decide
            rw [if_neg handnot]
            grind
        · rw [if_neg hc]
          have handnot : ¬ (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
            intro hh
            rw [Bool.and_eq_true] at hh
            exact hc hh.1
          rw [if_neg handnot]
          grind
      have hterm (bs : List (List Nat)) (hbs : bs ∈ partitions vs) :
          ((([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))).map
              (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))).sum =
            (mobius bs * (if constOn bs x then (1 : Int) else 0)) * (1 - k) := by
        have hne := partitions_nonempty vs bs hbs
        simp only [List.map_cons, List.sum_cons]
        rw [mobius_singleton, constOn_singleton]
        rw [show (List.map (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))
                  (List.map (fun p => bs.modify p.2 (fun bl => v :: bl)) bs.zipIdx)).sum =
              (bs.zipIdx.map (fun p => mobius (bs.modify p.2 (fun bl => v :: bl)) *
                (if constOn (bs.modify p.2 (fun bl => v :: bl)) x then (1 : Int) else 0))).sum by
              rw [List.map_map]
              rfl]
        rw [show (bs.zipIdx.map (fun p => mobius (bs.modify p.2 (fun bl => v :: bl)) *
                (if constOn (bs.modify p.2 (fun bl => v :: bl)) x then (1 : Int) else 0))) =
              (bs.zipIdx.map (fun p => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf p.1) then -(p.1.length : Int) else 0) else 0))) by
              apply List.map_congr_left
              intro p hp
              have hi : p.2 < bs.length := by simpa using (List.mem_zipIdx hp).2.1
              have hpval : p.1 = bs.get ⟨p.2, hi⟩ := by simpa using (List.mem_zipIdx hp).2.2
              rw [hmod bs hne p.2 hi]
              rw [← hpval]]
        rw [show (bs.zipIdx.map (fun p => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf p.1) then -(p.1.length : Int) else 0) else 0))).sum =
              (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum by
            exact sum_zipIdx_fst bs (fun bl => mobius bs *
              (if constOn bs x then
                (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))]
        by_cases hc : constOn bs x = true
        · rw [if_pos hc]
          rw [show (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum =
                (bs.map (fun bl => mobius bs *
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0))).sum by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bl hbl
              rw [if_pos hc]]
          rw [show (bs.map (fun bl => mobius bs *
                (if x v == x (headOf bl) then -(bl.length : Int) else 0))).sum =
                (-mobius bs) *
                  (bs.map (fun bl => if x v == x (headOf bl) then (bl.length : Int) else 0)).sum by
              rw [show (fun bl => mobius bs *
                    (if x v == x (headOf bl) then -(bl.length : Int) else 0)) =
                    (fun bl => (-mobius bs) *
                      (if x v == x (headOf bl) then (bl.length : Int) else 0)) by
                    funext bl
                    by_cases hhd : x v == x (headOf bl) <;> simp [hhd] <;> grind]
              rw [sum_mul_left]]
          rw [sum_block_count bs x v hc]
          have hperm := partitions_flatten_perm vs bs hbs
          have hcperm := List.Perm.countP_eq (fun u => x u == x v) hperm
          dsimp [k]
          rw [hcperm]
          grind
        · rw [if_neg hc]
          have hzero : (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum = 0 := by
            rw [show (fun bl => mobius bs *
                  (if constOn bs x then
                    (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0)) =
                  (fun _ => (0 : Int)) by
                  funext bl
                  rw [if_neg hc]
                  simp]
            exact sum_zero bs
          rw [hzero]
          grind
      rw [show partitions (v :: vs) =
          (partitions vs).flatMap (fun bs =>
            ([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))) by rfl]
      rw [sum_flatMap]
      rw [show ((partitions vs).map (fun bs =>
            ((([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))).map
              (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))).sum)).sum =
            ((partitions vs).map (fun bs =>
              (mobius bs * (if constOn bs x then (1 : Int) else 0)) * (1 - k))).sum by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          exact hterm bs hbs]
      rw [sum_mul_right]
      rw [ih']
      by_cases hN : (vs.map x).Nodup
      · have hk_le : vs.countP (fun u => x u == x v) ≤ 1 := countP_le_one_of_map_nodup vs v x hN
        have hk01 : vs.countP (fun u => x u == x v) = 0 ∨ vs.countP (fun u => x u == x v) = 1 := by omega
        rcases hk01 with hk0 | hk1
        · have hnot : x v ∉ vs.map x := by
            intro hm
            rcases List.mem_map.mp hm with ⟨u, hu, huv⟩
            have heq : x u == x v := by simp [huv]
            exact (List.countP_eq_zero.mp hk0) u hu heq
          have hnot' : ∀ u ∈ vs, x u ≠ x v := by
            intro u hu heq
            exact hnot (List.mem_map.mpr ⟨u, hu, heq⟩)
          have hnodup : ((v :: vs).map x).Nodup := by
            simp only [List.map_cons, List.nodup_cons]
            exact ⟨hnot, hN⟩
          rw [if_pos hN, if_pos hnodup]
          simp [k, hk0]
        · have hpos : 0 < vs.countP (fun u => x u == x v) := by omega
          have hmem := countP_pos_mem vs v x hpos
          have hnotnodup : ¬ ((v :: vs).map x).Nodup := by
            intro hn
            have hnot : x v ∉ vs.map x := (List.nodup_cons.mp (by simpa using hn)).1
            exact hnot hmem
          rcases List.mem_map.mp hmem with ⟨u, hu, huv⟩
          rw [if_pos hN, if_neg hnotnodup]
          simp [k, hk1]
      · have hnotnodup : ¬ ((v :: vs).map x).Nodup := by
          intro hn
          exact hN (List.nodup_cons.mp (by simpa using hn)).2
        rw [if_neg hN, if_neg hnotnodup]
        simp

private theorem mobius_point (x : Bind) :
    ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
    (if (pattern.freeVars.map x).Nodup then (1 : Int) else 0) := by
  exact mobius_point_gen pattern.freeVars (by native_decide) x
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  let V := pattern.freeVars
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hF : ∀ x y, (∀ v ∈ V, x v = y v) → F x = F y := by
    intro x y hxy
    dsimp only [F, V]
    simp [pattern, value]
    rw [hxy 0 (by native_decide), hxy 2 (by native_decide),
      hxy 1 (by native_decide), hxy 3 (by native_decide),
      hxy 4 (by native_decide), hxy 5 (by native_decide)]
  have hcoll : ∀ bs ∈ partitions V,
      collapsed pattern n db b bs =
        integrate n V b (fun x => if constOn bs x then F x else 0) := by
    intro bs hbs
    unfold collapsed
    change integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) = _
    exact collapse_sum n b F bs hbs hF
  change integrate n V b (fun x => if (V.map x).Nodup then F x else 0) =
    ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum
  calc
    integrate n V b (fun x => if (V.map x).Nodup then F x else 0)
        = ((bindings n V b).map (fun x => if (V.map x).Nodup then F x else 0)).sum :=
            bindings_sum n V b _
    _ = ((bindings n V b).map (fun x => F x *
          ((partitions V).map (fun bs => mobius bs *
            (if constOn bs x then (1 : Int) else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [mobius_point x]
          by_cases hN : (V.map x).Nodup <;> simp [hN, V]
    _ = ((bindings n V b).map (fun x => ((partitions V).map (fun bs =>
          mobius bs * (if constOn bs x then F x else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [show (fun bs => mobius bs * (if constOn bs x then F x else 0)) =
              (fun bs => (mobius bs * (if constOn bs x then (1 : Int) else 0)) * F x) by
            funext bs
            by_cases h : constOn bs x <;> simp [h]]
          rw [sum_mul_right]
          ac_rfl
    _ = ((partitions V).map (fun bs => mobius bs *
          ((bindings n V b).map (fun x => if constOn bs x then F x else 0)).sum)).sum := by
          rw [show (fun x => ((partitions V).map (fun bs =>
              mobius bs * (if constOn bs x then F x else 0))).sum) =
              (fun x => ((partitions V).map (fun bs =>
                (fun x => mobius bs * (if constOn bs x then F x else 0)) x)).sum) by rfl]
          rw [sum_swap]
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [sum_mul_left]
    _ = ((partitions V).map (fun bs => mobius bs *
          integrate n V b (fun x => if constOn bs x then F x else 0))).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [bindings_sum]
    _ = ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [hcoll bs hbs]


end TinylambOpt.ResearchProofs.Round2.Task78
-- END jpb-copy Task78

-- BEGIN jpb-copy Task78IC (IntersectingCycles CollisionCorrection; Task78 machinery re-pointed at the IC pattern)
namespace TinylambOpt.ResearchProofs.Round2.Task78IC

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles

example : pattern.freeVars = [4, 5, 1, 2, 3, 0] := by rfl

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr
        intro j hj
        rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hi => h i (Nat.lt_succ_of_lt hi))
      omega

/-- Partition-helper definitions mirroring the `collapsed` definition. -/
private def headOf : List Nat → Nat
  | h :: _ => h
  | [] => 0

private def Heads (bs : List (List Nat)) : List Nat := bs.filterMap List.head?

private def Root (bs : List (List Nat)) (v : Nat) : Nat :=
  ((bs.find? (fun bl => bl.contains v)).bind List.head?).getD v

private def constOn (bs : List (List Nat)) (x : Bind) : Bool :=
  bs.all (fun bl => bl.all (fun v => x v == x (headOf bl)))

/-- Boolean structural spec of a partition of the fixed variable list. -/
private def PSb (V : List Nat) (bs : List (List Nat)) : Bool :=
  decide bs.Nodup &&
  bs.all (fun bl => decide bl.Nodup && bl.all (fun v => V.contains v)) &&
  bs.all (fun bl1 => bs.all (fun bl2 =>
    bl1.all (fun v => if bl2.contains v then bl1 == bl2 else true))) &&
  V.all (fun v => bs.any (fun bl => bl.contains v))

private theorem ps_all : (partitions pattern.freeVars).all (fun bs => PSb pattern.freeVars bs) = true := by
  native_decide

private theorem ps_of_mem {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    PSb pattern.freeVars bs = true :=
  List.all_eq_true.mp ps_all bs hbs

private theorem find_eq_some_unique {α} (p : α → Bool) (l : List α) (a : α)
    (ha : a ∈ l) (hp : p a = true) (huniq : ∀ b ∈ l, p b = true → b = a) :
    l.find? p = some a := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
      by_cases hxa : x = a
      · subst x
        simp [hp]
      · have hax : a ≠ x := by intro h; exact hxa h.symm
        have haxs : a ∈ xs := by simpa [hax] using ha
        have hpx : p x = false := by
          cases hpx' : p x with
          | true =>
              have hxeq : x = a := huniq x (by simp) hpx'
              exact False.elim (hxa hxeq)
          | false => rfl
        simp [hpx, ih haxs (fun b hb hpb => huniq b (by simp [hb]) hpb)]

private theorem root_eq_head (bs : List (List Nat)) (hPS : PSb pattern.freeVars bs = true) :
    ∀ bl ∈ bs, ∀ v ∈ bl, Root bs v = headOf bl := by
  simp [PSb] at hPS
  rcases hPS with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro bl hbl v hv
  have hpbl : bl.contains v = true := by simpa using hv
  have huniq : ∀ b ∈ bs, b.contains v = true → b = bl := by
    intro b hb hbv
    have hdeq := hdisj bl hbl b hb v hv
    have hbmem : v ∈ b := by simpa using hbv
    rcases hdeq with hnot | heq
    · exact False.elim (hnot hbmem)
    · exact heq.symm
  have hfind : bs.find? (fun b : List Nat => b.contains v) = some bl :=
    find_eq_some_unique (fun b : List Nat => b.contains v) bs bl hbl hpbl huniq
  unfold Root
  rw [hfind]
  cases bl with
  | nil => simp at hv
  | cons h tl => rfl

private theorem head_mem_heads_cons (bl : List Nat) (bs : List (List Nat))
    (hs : bl.head?.isSome = true) : headOf bl ∈ Heads (bl :: bs) := by
  cases bl with
  | nil => simp at hs
  | cons h tl => simp [Heads, headOf]

private theorem head_mem_heads {bl : List Nat} {bs : List (List Nat)}
    (hbl : bl ∈ bs) : bl.head?.isSome = true → headOf bl ∈ Heads bs := by
  induction bs with
  | nil => simp at hbl
  | cons b bs ih =>
      intro hs
      rcases List.mem_cons.mp hbl with hbeq | hbl
      · subst bl
        exact head_mem_heads_cons b bs hs
      · have htail := ih hbl hs
        cases hb : b.head? with
        | none => simpa [Heads, hb] using htail
        | some h =>
            simp [Heads, hb]
            apply Or.inr
            exact List.mem_filterMap.mp htail

private def HPb (bs : List (List Nat)) : Bool :=
  decide (Heads bs).Nodup && (Heads bs).all (fun h => pattern.freeVars.contains h)

private theorem hp_all : (partitions pattern.freeVars).all HPb = true := by
  native_decide

private theorem heads_spec {bs : List (List Nat)} (hbs : bs ∈ partitions pattern.freeVars) :
    (Heads bs).Nodup ∧ (∀ h ∈ Heads bs, h ∈ pattern.freeVars) := by
  have h := List.all_eq_true.mp hp_all bs hbs
  simp [HPb] at h
  exact h

private theorem find_none_of_all_false {α} (p : α → Bool) (l : List α)
    (h : ∀ a ∈ l, p a = false) : l.find? p = none := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [List.find?, h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem root_out {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v, v ∉ pattern.freeVars → Root bs v = v := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  have hfalse : ∀ bl ∈ bs, bl.contains v = false := by
    intro bl hbl
    have hbsub := hsub bl hbl
    by_cases hne : bl.contains v = true
    · have hvbl : v ∈ bl := by simpa using hne
      exact False.elim (hv (hbsub.2 v hvbl))
    · cases hne' : bl.contains v <;> simp_all
  unfold Root
  rw [find_none_of_all_false (fun bl : List Nat => bl.contains v) bs hfalse]
  simp

private theorem root_head {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ h ∈ Heads bs, Root bs h = h := by
  intro h hh
  rcases List.mem_filterMap.mp hh with ⟨bl, hbl, hhbl⟩
  have hhroot : headOf bl = h := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simpa [headOf] using hhbl
  have hvbl : headOf bl ∈ bl := by
    cases bl with
    | nil => simp at hhbl
    | cons a t => simp [headOf]
  rw [← hhroot]
  exact root_eq_head bs hPS bl hbl (headOf bl) hvbl

private theorem root_mem_heads {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) :
    ∀ v ∈ pattern.freeVars, Root bs v ∈ Heads bs := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  intro v hv
  rcases hcov v hv with ⟨bl, hbl, hvbl⟩
  rw [root_eq_head bs hPS bl hbl v hvbl]
  exact head_mem_heads hbl (by cases bl with | nil => simp at hvbl | cons h t => simp)

private theorem constOn_iff {bs : List (List Nat)} (hPS : PSb pattern.freeVars bs = true) (x : Bind) :
    constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
  have hPS' := hPS
  simp [PSb] at hPS'
  rcases hPS' with ⟨⟨⟨hnd, hsub⟩, hdisj⟩, hcov⟩
  constructor
  · intro hc v hv
    rcases hcov v hv with ⟨bl, hbl, hvbl⟩
    rw [root_eq_head bs hPS bl hbl v hvbl]
    have hbeq := List.all_eq_true.mp hc bl hbl
    have hveq := List.all_eq_true.mp hbeq v hvbl
    simpa using hveq
  · intro hx
    apply List.all_eq_true.mpr
    intro bl hbl
    apply List.all_eq_true.mpr
    intro v hvbl
    have hvV : v ∈ pattern.freeVars := (hsub bl hbl).2 v hvbl
    have hrv := hx v hvV
    rw [root_eq_head bs hPS bl hbl v hvbl] at hrv
    simpa using hrv

/-- Integration helpers (Std-only). -/
private theorem sum_congr {α} (l : List α) (f g : α → Int) (h : ∀ x ∈ l, f x = g x) :
    (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem sum_eq_zero {α} (l : List α) (f : α → Int) (h : ∀ x ∈ l, f x = 0) :
    (l.map f).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]
      simp

private theorem put_self (b : Bind) (v x : Nat) : put b v x v = x := by simp [put]

private theorem put_same (b : Bind) (v : Nat) : put b v (b v) = b := by
  funext i
  by_cases hi : i == v
  · have hiv : i = v := Nat.eq_of_beq_eq_true hi
    subst i
    simp [put]
  · simp [put, hi]

private theorem put_comm_ne (b : Bind) (v w x y : Nat) (h : (v != w) = true) :
    put (put b v x) w y = put (put b w y) v x := by
  funext i
  by_cases hv : i == v
  · by_cases hw : i == w
    · have hiv : i = v := Nat.eq_of_beq_eq_true hv
      have hiw : i = w := Nat.eq_of_beq_eq_true hw
      have hvw : v = w := by omega
      simp [hvw] at h
    · simp [put, hv, hw]
  · by_cases hw : i == w <;> simp [put, hv, hw]

private theorem integrate_swap (n v w : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (v :: w :: vs) b f = integrate n (w :: v :: vs) b f := by
  by_cases h : v == w
  · have hvw : v = w := Nat.eq_of_beq_eq_true h
    subst w
    rfl
  · have hne : (v != w) = true := by cases h' : (v == w) <;> simp_all
    change ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b v x) w y) f)).sum)).sum =
      ((List.range n).map (fun x =>
        ((List.range n).map (fun y => integrate n vs (put (put b w x) v y) f)).sum)).sum
    change rsum n (fun x => rsum n (fun y => integrate n vs (put (put b v x) w y) f)) =
      rsum n (fun x => rsum n (fun y => integrate n vs (put (put b w x) v y) f))
    rw [rsum_comm]
    apply sum_congr
    intro x hx
    apply sum_congr
    intro y hy
    rw [put_comm_ne b v w y x hne]

private theorem integrate_append (n : Nat) (xs ys : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n (xs ++ ys) b f = integrate n xs b (fun b' => integrate n ys b' f) := by
  induction xs generalizing b with
  | nil => rfl
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n (vs ++ ys) (put b v x) f)).sum =
        ((List.range n).map (fun x => integrate n vs (put b v x) (fun b' => integrate n ys b' f))).sum
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem integrate_perm (n : Nat) {xs ys : List Nat} (h : xs.Perm ys)
    (b : Bind) (f : Bind → Int) : integrate n xs b f = integrate n ys b f := by
  induction h generalizing b with
  | nil => rfl
  | cons x h ih =>
      change ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum =
        ((List.range n).map (fun t => integrate n _ (put b x t) f)).sum
      apply sum_congr
      intro t ht
      exact ih (put b x t)
  | swap x y l => exact (integrate_swap n x y l b f).symm
  | trans h1 h2 ih1 ih2 => exact (ih1 b).trans (ih2 b)

private def bindings (n : Nat) : List Nat → Bind → List Bind
  | [], b => [b]
  | v :: vs, b => (List.range n).flatMap (fun x => bindings n vs (put b v x))

private theorem bindings_sum (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int) :
    integrate n vs b f = ((bindings n vs b).map f).sum := by
  induction vs generalizing b with
  | nil => simp [integrate, bindings]
  | cons v vs ih =>
      change ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum =
        (((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f).sum
      rw [show ((List.range n).flatMap (fun x => bindings n vs (put b v x))).map f =
          (List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f) by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.map_append, ih2]]
      rw [show ((List.range n).flatMap (fun x => (bindings n vs (put b v x)).map f)).sum =
          ((List.range n).map (fun x => ((bindings n vs (put b v x)).map f).sum)).sum by
            induction List.range n with
            | nil => rfl
            | cons a l ih2 => simp [List.sum_append, ih2]]
      apply sum_congr
      intro x hx
      exact ih (put b v x)

private theorem bindings_fixed (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∉ vs, y v = b v := by
  induction vs generalizing b with
  | nil =>
      intro y hy v hv
      simp [bindings] at hy
      subst y
      rfl
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      have hvw : v ≠ w := by intro h; subst v; exact hv (by simp)
      have hvws : v ∉ ws := by intro h; exact hv (by simp [h])
      rw [ih (put b w x) y hy v hvws]
      simp [put, hvw]

private theorem bindings_range (n : Nat) (vs : List Nat) (b : Bind) :
    ∀ y ∈ bindings n vs b, ∀ v ∈ vs, y v ∈ List.range n := by
  induction vs generalizing b with
  | nil => simp
  | cons w ws ih =>
      intro y hy v hv
      rcases List.mem_flatMap.mp hy with ⟨x, hx, hy⟩
      by_cases hvw : w = v
      · subst w
        by_cases htail : v ∈ ws
        · exact ih (put b v x) y hy v htail
        · have heq := bindings_fixed n ws (put b v x) y hy v htail
          rw [heq]
          simpa [put] using hx
      · have htail : v ∈ ws := by
          rcases List.mem_cons.mp hv with h | h
          · exact (hvw h.symm).elim
          · exact h
        exact ih (put b w x) y hy v htail

private theorem integrate_congr_bindings (n : Nat) (vs : List Nat) (b : Bind) (f g : Bind → Int)
    (h : ∀ y, y ∈ bindings n vs b → f y = g y) :
    integrate n vs b f = integrate n vs b g := by
  rw [bindings_sum, bindings_sum]
  apply sum_congr
  intro y hy
  exact h y hy

private theorem rsum_ite_eq_single (n a : Nat) (ha : a < n) (f : Nat → Int) :
    rsum n (fun x => if x = a then f x else 0) = f a := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsum_succ]
      by_cases h : a = n
      · subst a
        have hzero : rsum n (fun x => if x = n then f x else 0) = 0 := by
          rw [show rsum n (fun x => if x = n then f x else 0) = rsum n (fun _ => (0 : Int)) by
            apply rsum_congr
            intro x hx
            have hxn : x ≠ n := by omega
            simp [hxn]]
          exact rsum_zero_const n
        rw [hzero]
        simp
      · have han : a < n := by omega
        rw [ih han]
        have hne : ¬ n = a := by omega
        simp [hne]

private theorem integrate_zero_of_false (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (h : ∀ y ∈ bindings n vs b, f y = 0) : integrate n vs b f = 0 := by
  rw [bindings_sum]
  apply sum_eq_zero
  intro y hy
  exact h y hy

private theorem inner_collapse (n : Nat) (M : List Nat) (Root : Nat → Nat) (y : Bind) (C : Int)
    (hM : M.Nodup) (hRnot : ∀ m ∈ M, Root m ∉ M)
    (hy : ∀ m ∈ M, y (Root m) ∈ List.range n) :
    integrate n M y (fun x => if M.all (fun m => x m == y (Root m)) then C else 0) = C := by
  induction M generalizing y with
  | nil => simp [integrate]
  | cons m Ms ih =>
      have hmMs : m ∉ Ms := (List.nodup_cons.mp hM).1
      have hMnd : Ms.Nodup := (List.nodup_cons.mp hM).2
      have hRnot' : ∀ v ∈ Ms, Root v ∉ Ms := by
        intro v hv hmem
        exact hRnot v (by simp [hv]) (by simp [hmem])
      have hrootne : ∀ v ∈ Ms, Root v ≠ m := by
        intro v hv h_eq
        exact hRnot v (by simp [hv]) (by simp [h_eq])
      have hpoint : ∀ t < n,
          integrate n Ms (put y m t)
            (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) =
          if t = y (Root m) then C else 0 := by
        intro t ht
        by_cases htm : t = y (Root m)
        · subst t
          have hyrange' : ∀ v ∈ Ms, (put y m (y (Root m))) (Root v) ∈ List.range n := by
            intro v hv
            have hne := hrootne v hv
            simpa [put, hne] using hy v (by simp [hv])
          calc
            integrate n Ms (put y m (y (Root m)))
                (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)
                = integrate n Ms (put y m (y (Root m)))
                    (fun x => if Ms.all (fun v => x v == (put y m (y (Root m))) (Root v)) then C else 0) := by
                    apply integrate_congr_bindings
                    intro x hx
                    have hxm : x m = y (Root m) := by
                      rw [bindings_fixed n Ms (put y m (y (Root m))) x hx m hmMs]
                      simp [put]
                    simp [hxm]
                    by_cases hleft : ∀ v ∈ Ms, x v = y (Root v)
                    · have hright : ∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v) := by
                        intro v hv
                        have hv' := hleft v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_left hleft, ite_eq_left hright]
                    · have hrightnot : ¬ (∀ v ∈ Ms, x v = (put y m (y (Root m))) (Root v)) := by
                        intro hr
                        apply hleft
                        intro v hv
                        have hv' := hr v hv
                        have hneq : (Root v == m) = false := by
                          cases hb : Root v == m <;> simp_all
                        simpa [put, hneq] using hv'
                      rw [ite_eq_right hleft, ite_eq_right hrightnot]
            _ = C := ih (put y m (y (Root m))) hMnd hRnot' hyrange'
            _ = (if y (Root m) = y (Root m) then C else 0) := by simp
        · have hzero : integrate n Ms (put y m t)
              (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0) = 0 := by
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htm]
          simpa [htm] using hzero
      change rsum n (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) = C
      rw [show (fun t => integrate n Ms (put y m t)
        (fun x => if (m :: Ms).all (fun v => x v == y (Root v)) then C else 0)) =
        (fun t => if t = y (Root m) then C else 0) by
          funext t
          by_cases ht : t < n
          · exact hpoint t ht
          · have htn : y (Root m) < n := by
              have := hy m (by simp)
              simpa using this
            have htne : t ≠ y (Root m) := by omega
            simp [htne]
            apply integrate_zero_of_false
            intro x hx
            have hxm : x m = t := by
              rw [bindings_fixed n Ms (put y m t) x hx m hmMs]
              simp [put]
            simp [hxm, htne]]
      rw [rsum_ite_eq_single n (y (Root m)) (by
        have := hy m (by simp)
        simpa using this)]

private theorem perm_append_left {α} {l1 l2 : List α} (h : l1.Perm l2) (l3 : List α) :
    (l1 ++ l3).Perm (l2 ++ l3) := by
  induction h with
  | nil => rfl
  | cons x h ih => exact .cons x ih
  | swap x y l => exact .swap x y (l ++ l3)
  | trans h1 h2 ih1 ih2 => exact ih1.trans ih2

private theorem perm_append_right {α} (l1 : List α) {l3 l4 : List α} (h : l3.Perm l4) :
    (l1 ++ l3).Perm (l1 ++ l4) := by
  induction l1 with
  | nil => simpa using h
  | cons a l1 ih => exact .cons a ih

private theorem perm_append {α} {l1 l2 l3 l4 : List α} (h1 : l1.Perm l2) (h2 : l3.Perm l4) :
    (l1 ++ l3).Perm (l2 ++ l4) := by
  exact (perm_append_left h1 l3).trans (perm_append_right l2 h2)

private theorem cons_append_perm (a : Nat) (l : List Nat) : (a :: l).Perm (l ++ [a]) := by
  induction l with
  | nil => simp
  | cons x l ih => exact (List.Perm.swap x a l).trans (.cons x ih)

private theorem filter_append_compl (p : Nat → Bool) (l : List Nat) :
    l.Perm ((l.filter p) ++ (l.filter (fun x => !p x))) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases ha : p a = true
      · simpa [ha] using (List.Perm.cons a ih)
      · have ha' : p a = false := by cases h : p a <;> simp_all
        have hstep := (List.Perm.cons a ih).trans
          (perm_append (cons_append_perm a (l.filter p))
            (List.Perm.refl (l.filter (fun x => !p x))))
        simpa [ha'] using hstep

private theorem perm_filter_of_nodup {l l' : List Nat} (hnd : l.Nodup) (hnd' : l'.Nodup)
    (hsub : ∀ x ∈ l, x ∈ l') : l.Perm (l'.filter (fun x => x ∈ l)) := by
  induction l' generalizing l with
  | nil =>
      have hl : l = [] := by
        cases l with
        | nil => rfl
        | cons a as => exact False.elim (by simpa using hsub a (by simp))
      subst l
      simp
  | cons a l' ih =>
      have hnot : a ∉ l' := (List.nodup_cons.mp hnd').1
      by_cases ha : a ∈ l
      · have htail := ih (hnd.erase a) (List.nodup_cons.mp hnd').2 (by
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnd.not_mem_erase hx
          have hxl := (List.mem_erase_of_ne hxa).mp hx
          simpa [hxa] using hsub x hxl)
        have hab : decide (a ∈ l) = true := by simp [ha]
        have hfilter : (a :: l').filter (fun x => x ∈ l) =
            a :: l'.filter (fun x : Nat => x ∈ l.erase a) := by
          rw [List.filter_cons, ite_eq_left hab]
          congr 1
          apply List.filter_congr
          intro x hx
          have hxa : x ≠ a := by intro h; subst x; exact hnot hx
          simp [List.mem_erase_of_ne hxa]
        rw [hfilter]
        exact (List.perm_cons_erase ha).trans (.cons a htail)
      · rw [List.filter_cons]
        simp [ha]
        apply ih hnd (List.nodup_cons.mp hnd').2
        intro x hx
        have hxa : x ≠ a := by intro h; subst x; exact ha hx
        simpa [hxa] using hsub x hx

private theorem collapse_sum (n : Nat) (b : Bind) (F : Bind → Int)
    (bs : List (List Nat)) (hbs : bs ∈ partitions pattern.freeVars)
    (hF : ∀ x y, (∀ v ∈ pattern.freeVars, x v = y v) → F x = F y) :
    integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) =
      integrate n pattern.freeVars b (fun x => if constOn bs x then F x else 0) := by
  have hPS := ps_of_mem hbs
  have hspec := heads_spec hbs
  have hVnd : pattern.freeVars.Nodup := by native_decide
  let H := Heads bs
  let M := pattern.freeVars.filter (fun v => !H.contains v)
  have hHsub : ∀ v ∈ H, v ∈ pattern.freeVars := hspec.2
  have hrootV : ∀ v ∈ pattern.freeVars, Root bs v ∈ H := by
    intro v hv
    exact root_mem_heads hPS v hv
  have hrootH : ∀ h ∈ H, Root bs h = h := by
    intro h hh
    exact root_head hPS h hh
  have hrootOut : ∀ v, v ∉ pattern.freeVars → Root bs v = v := root_out hPS
  have hconst : ∀ x, constOn bs x = true ↔ ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := constOn_iff hPS
  have hfilt : (pattern.freeVars.filter (fun v => H.contains v)).Perm H := by
    have hp := perm_filter_of_nodup hspec.1 hVnd hHsub
    simpa using hp.symm
  have hpermV : pattern.freeVars.Perm (H ++ M) := by
    have h1 := filter_append_compl H.contains pattern.freeVars
    have h2 : (pattern.freeVars.filter (fun v => H.contains v) ++
        pattern.freeVars.filter (fun v => !H.contains v)).Perm
        (H ++ M) := perm_append hfilt (by rfl)
    exact h1.trans h2
  rw [integrate_perm n hpermV b (fun x => if constOn bs x then F x else 0)]
  rw [integrate_append]
  apply integrate_congr_bindings
  intro y hy
  have hyH : ∀ h ∈ H, y h ∈ List.range n := by
    intro h hh
    exact bindings_range n H b y hy h hh
  have hyfixed : ∀ v ∉ H, y v = b v := by
    intro v hv
    exact bindings_fixed n H b y hy v hv
  let x0 : Bind := fun v => y (Root bs v)
  have hx0V : ∀ v ∈ pattern.freeVars, x0 v ∈ List.range n := by
    intro v hv
    have hr := hrootV v hv
    simpa [x0] using hyH (Root bs v) hr
  have hMsub : ∀ v ∈ M, v ∈ pattern.freeVars := by
    intro v hv
    exact (List.mem_filter.mp hv).1
  have hMnotH : ∀ v ∈ M, v ∉ H := by
    intro v hv
    have hnot := (List.mem_filter.mp hv).2
    simpa using hnot
  have hMnd : M.Nodup := hVnd.filter _
  have hRnot : ∀ m ∈ M, Root bs m ∉ M := by
    intro m hm hmem
    have hr := hrootV m (hMsub m hm)
    exact hMnotH (Root bs m) hmem hr
  have hyrange : ∀ m ∈ M, y (Root bs m) ∈ List.range n := by
    intro m hm
    exact hyH (Root bs m) (hrootV m (hMsub m hm))
  have hinner : integrate n M y (fun x => if constOn bs x then F x else 0) = F x0 := by
    calc
      integrate n M y (fun x => if constOn bs x then F x else 0)
          = integrate n M y (fun x => if M.all (fun m => x m == y (Root bs m)) then F x0 else 0) := by
            apply integrate_congr_bindings
            intro x hx
            have hxfixedH : ∀ h ∈ H, x h = y h := by
              intro h hh
              have hnot : h ∉ M := by
                intro hm
                exact hMnotH h hm hh
              rw [bindings_fixed n M y x hx h hnot]
            by_cases hall : M.all (fun m => x m == y (Root bs m)) = true
            · rw [ite_eq_left hall]
              have hxFx0 : ∀ v ∈ pattern.freeVars, x v = x0 v := by
                intro v hv
                by_cases hvH : v ∈ H
                · have hrv := hrootH v hvH
                  change x v = y (Root bs v)
                  rw [hxfixedH v hvH, hrv]
                · have hvM : v ∈ M := by
                    simp [M, hv, hvH]
                  have hvEq := List.all_eq_true.mp hall v hvM
                  change x v = y (Root bs v)
                  simpa using hvEq
              have hcond : ∀ v ∈ pattern.freeVars, x v = x (Root bs v) := by
                intro v hv
                have hxv := hxFx0 v hv
                have hxroot : x (Root bs v) = y (Root bs v) :=
                  hxfixedH (Root bs v) (hrootV v hv)
                exact hxv.trans (by simpa [x0] using hxroot.symm)
              rw [ite_eq_left ((hconst x).2 hcond)]
              exact hF x x0 hxFx0
            · have hallnot : ¬ M.all (fun m => x m == y (Root bs m)) = true := by
                intro h
                exact hall h
              rw [ite_eq_right hallnot]
              have hcondnot : ¬ (∀ v ∈ pattern.freeVars, x v = x (Root bs v)) := by
                intro hc
                apply hall
                apply List.all_eq_true.mpr
                intro m hm
                have hcv := hc m (hMsub m hm)
                have hrv := hrootV m (hMsub m hm)
                have hxrv : x (Root bs m) = y (Root bs m) := hxfixedH (Root bs m) hrv
                simp [hcv, hxrv]
              have hconstnot : ¬ constOn bs x = true := by
                intro hx'
                exact hcondnot ((hconst x).1 hx')
              rw [ite_eq_right hconstnot]
      _ = F x0 := inner_collapse n M (fun v => Root bs v) y (F x0) hMnd hRnot hyrange
  simpa [x0] using hinner.symm

private theorem sum_zero {α} (l : List α) : (l.map (fun _ => (0 : Int))).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private theorem sum_add {α} (l : List α) (f g : α → Int) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_left_comm]

private theorem sum_mul_left {α} (l : List α) (c : Int) (f : α → Int) :
    (l.map (fun x => c * f x)).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.mul_add]

private theorem sum_mul_right {α} (l : List α) (f : α → Int) (c : Int) :
    (l.map (fun x => f x * c)).sum = (l.map f).sum * c := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.add_mul]

private theorem sum_swap {α β} (l : List α) (l' : List β) (F : α → β → Int) :
    (l.map (fun a => (l'.map (F a)).sum)).sum = (l'.map (fun b => (l.map (fun a => F a b)).sum)).sum := by
  induction l' with
  | nil =>
      change (l.map (fun _ => (0 : Int))).sum = 0
      exact sum_zero l
  | cons b l' ih =>
      rw [show (fun a => (List.map (F a) (b :: l')).sum) =
          (fun a => F a b + (l'.map (F a)).sum) by funext a; rfl]
      rw [sum_add, ih]
      simp


/-- ## Generic Möbius inversion over equality partitions -/

private theorem sum_flatMap {α β : Type} (l : List α) (f : α → List β) (g : β → Int) :
    ((l.flatMap f).map g).sum = (l.map (fun a => ((f a).map g).sum)).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [List.flatMap_cons, ih]

private theorem sum_range_succ_shift (m : Nat) (f : Nat → Int) :
    f 0 + ((List.range m).map (fun i => f (i + 1))).sum =
      ((List.range (m + 1)).map f).sum := by
  induction m with
  | zero => simp
  | succ m ih =>
      calc
        f 0 + ((List.range (m + 1)).map (fun i => f (i + 1))).sum
            = f 0 + (((List.range m).map (fun i => f (i + 1))).sum + f (m + 1)) := by
                rw [List.range_succ]
                simp
        _ = (f 0 + ((List.range m).map (fun i => f (i + 1))).sum) + f (m + 1) := by
                rw [Int.add_assoc]
        _ = ((List.range (m + 1)).map f).sum + f (m + 1) := by rw [ih]
        _ = ((List.range (m + 2)).map f).sum := by
                rw [show List.range (m + 2) = List.range (m + 1) ++ [m + 1] by
                  simpa using (List.range_succ (n := m + 1))]
                simp

private theorem sum_zipIdx (bs : List (List Nat)) (f : Nat → Int) :
    (bs.zipIdx.map (fun p => f p.2)).sum = ((List.range bs.length).map f).sum := by
  induction bs generalizing f with
  | nil => simp
  | cons b bs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [show bs.zipIdx 1 = (bs.zipIdx).map (fun p => (p.1, p.2 + 1)) by
        simpa using (List.zipIdx_succ (l := bs) (i := 0))]
      rw [List.map_map]
      change f 0 + (bs.zipIdx.map (fun p => f (p.2 + 1))).sum =
        (List.map f (List.range (b :: bs).length)).sum
      rw [ih (fun i => f (i + 1))]
      rw [sum_range_succ_shift]
      simp

private theorem mobius_singleton (v : Nat) (bs : List (List Nat)) :
    mobius ([v] :: bs) = mobius bs := by
  simp [mobius, factorial]

private theorem mobius_cons (b : List Nat) (bs : List (List Nat)) :
    mobius (b :: bs) = ((-1 : Int)^(b.length-1) * (factorial (b.length-1) : Int)) * mobius bs := by
  simp [mobius]

private theorem block_weight_modify (k : Nat) (hk : 0 < k) :
    (-1 : Int)^k * (factorial k : Int) =
      -(k : Int) * ((-1 : Int)^(k-1) * (factorial (k-1) : Int)) := by
  cases k with
  | zero => omega
  | succ k =>
      cases k with
      | zero => simp [factorial]
      | succ k =>
          simp [factorial]
          rw [show k + 1 + 1 = (k + 1) + 1 by omega, Int.pow_succ]
          grind

private theorem modify_succ (b : List Nat) (bs : List (List Nat)) (i : Nat)
    (f : List Nat → List Nat) : (b :: bs).modify (i + 1) f = b :: bs.modify i f := by
  rw [List.modify_cons]
  simp

private theorem mobius_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (hn : ∀ bl ∈ bs, bl ≠ []) :
    mobius (bs.modify i (fun bl => v :: bl)) = -((bs.get ⟨i, hi⟩).length : Int) * mobius bs := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero =>
          have hb : b ≠ [] := hn b (by simp)
          cases b with
          | nil => simp at hb
          | cons a as =>
              simp [List.modify, mobius]
              rw [block_weight_modify (as.length + 1) (by omega)]
              grind
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          have hn' : ∀ bl ∈ bs, bl ≠ [] := by intro bl hbl; exact hn bl (by simp [hbl])
          rw [modify_succ]
          rw [mobius_cons, mobius_cons, ih i hi' hn']
          simp only [List.get_cons_succ]
          grind

private def cOnP (bs : List (List Nat)) (x : Bind) : Prop :=
  ∀ bl ∈ bs, ∀ u ∈ bl, x u = x (headOf bl)

private theorem constOn_eq_cOnP (bs : List (List Nat)) (x : Bind) :
    constOn bs x = true ↔ cOnP bs x := by
  simp [constOn, cOnP, List.all_eq_true, beq_iff_eq]

private theorem constOn_singleton (v : Nat) (bs : List (List Nat)) (x : Bind) :
    constOn ([v] :: bs) x = constOn bs x := by
  simp [constOn, headOf]

private theorem cOnP_cons (b : List Nat) (bs : List (List Nat)) (x : Bind) :
    cOnP (b :: bs) x ↔ (∀ u ∈ b, x u = x (headOf b)) ∧ cOnP bs x := by
  simp [cOnP]

private theorem cOnP_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (x : Bind) (hn : ∀ bl ∈ bs, bl ≠ []) :
    cOnP (bs.modify i (fun bl => v :: bl)) x ↔
      (cOnP bs x ∧ ∀ u ∈ bs.get ⟨i, hi⟩, x u = x v) := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero =>
          have hb : b ≠ [] := hn b (by simp)
          cases b with
          | nil => simp at hb
          | cons a as =>
              simp only [List.modify_cons]
              simp only [ite_true]
              unfold cOnP
              simp only [List.mem_cons, forall_eq_or_imp]
              constructor
              · intro h
                rcases h with ⟨hnew, hrest⟩
                simp [headOf] at hnew ⊢
                constructor
                · constructor
                  · intro u hu
                    exact (hnew.2 u hu).trans hnew.1.symm
                  · exact hrest
                · exact hnew
              · intro h
                rcases h with ⟨⟨_, hrest⟩, hnew⟩
                simp [headOf] at hnew ⊢
                constructor
                · exact hnew
                · exact hrest
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          have hn' : ∀ bl ∈ bs, bl ≠ [] := by intro bl hbl; exact hn bl (by simp [hbl])
          rw [modify_succ]
          rw [cOnP_cons, cOnP_cons, ih i hi' hn']
          simp only [List.get_cons_succ]
          constructor <;> intro h <;> rcases h with ⟨h1, h2⟩ <;> constructor <;> simp_all

private theorem constOn_modify (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) (x : Bind) (hn : ∀ bl ∈ bs, bl ≠ []) :
    constOn (bs.modify i (fun bl => v :: bl)) x =
      (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) := by
  apply Bool.eq_iff_iff.mpr
  rw [constOn_eq_cOnP]
  rw [Bool.and_eq_true]
  rw [List.all_eq_true]
  rw [constOn_eq_cOnP]
  constructor
  · intro h
    have hh := (cOnP_modify bs i hi v x hn).mp h
    exact ⟨hh.1, by intro u hu; exact beq_iff_eq.mpr (hh.2 u hu)⟩
  · intro h
    apply (cOnP_modify bs i hi v x hn).mpr
    exact ⟨h.1, by intro u hu; exact beq_iff_eq.mp (h.2 u hu)⟩

private theorem partitions_nonempty : ∀ (V : List Nat) (bs : List (List Nat)),
    bs ∈ partitions V → ∀ bl ∈ bs, bl ≠ [] := by
  intro V
  induction V with
  | nil =>
      intro bs hbs
      simp [partitions] at hbs
      subst bs
      intro bl hbl
      simp at hbl
  | cons v vs ih =>
      intro bs hbs
      simp only [partitions, List.mem_flatMap] at hbs
      rcases hbs with ⟨bls, hbls, hmem⟩
      simp only [List.mem_cons, List.mem_map] at hmem
      rcases hmem with rfl | ⟨p, hp, rfl⟩
      · intro bl hbl
        simp only [List.mem_cons] at hbl
        rcases hbl with rfl | hbl
        · simp
        · exact ih bls hbls bl hbl
      · rcases p with ⟨_, i⟩
        have hi : i < bls.length := by simpa using (List.mem_zipIdx hp).2.1
        intro bl hbl
        rcases List.mem_iff_getElem.mp hbl with ⟨j, hj, hjEq⟩
        have hjbs : j < bls.length := by simpa using hj
        rw [List.getElem_modify] at hjEq
        by_cases hij : i = j
        · rw [if_pos hij] at hjEq
          rw [← hjEq]
          simp
        · rw [if_neg hij] at hjEq
          rw [← hjEq]
          exact ih bls hbls (bls[j]) (List.getElem_mem hjbs)

private theorem perm_append_cons (b : List Nat) (v : Nat) (l : List Nat) :
    (b ++ (v :: l)).Perm (v :: (b ++ l)) := by
  induction b with
  | nil => simp
  | cons x xs ih =>
      simp only [List.cons_append]
      exact (List.Perm.cons x ih).trans (List.Perm.swap v x (xs ++ l))

private theorem modify_flatten_perm (bs : List (List Nat)) (i : Nat) (hi : i < bs.length)
    (v : Nat) : (bs.modify i (fun bl => v :: bl)).flatten.Perm (v :: bs.flatten) := by
  induction bs generalizing i with
  | nil => simp at hi
  | cons b bs ih =>
      cases i with
      | zero => simp [List.modify]
      | succ i =>
          have hi' : i < bs.length := by simpa using hi
          rw [modify_succ]
          simp only [List.flatten_cons]
          exact (List.Perm.append_left b (ih i hi')).trans (perm_append_cons b v bs.flatten)

private theorem partitions_flatten_perm : ∀ (V : List Nat) (bs : List (List Nat)),
    bs ∈ partitions V → bs.flatten.Perm V := by
  intro V
  induction V with
  | nil =>
      intro bs hbs
      simp [partitions] at hbs
      simpa [hbs]
  | cons v vs ih =>
      intro bs hbs
      simp only [partitions, List.mem_flatMap] at hbs
      rcases hbs with ⟨bls, hbls, hmem⟩
      simp only [List.mem_cons, List.mem_map] at hmem
      rcases hmem with rfl | ⟨p, hp, rfl⟩
      · simpa using (List.Perm.cons v (ih bls hbls))
      · rcases p with ⟨_, i⟩
        have hi : i < bls.length := by simpa using (List.mem_zipIdx hp).2.1
        exact (modify_flatten_perm bls i hi v).trans (List.Perm.cons v (ih bls hbls))

private theorem countP_of_all_eq (l : List Nat) (x : Nat → Nat) (c : Nat)
    (h : ∀ u ∈ l, x u = c) : l.countP (fun u => x u == c) = l.length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ha : x a = c := h a (by simp)
      have ht : ∀ u ∈ l, x u = c := by intro u hu; exact h u (by simp [hu])
      simp [ha, ih ht]

private theorem sum_block_count (bs : List (List Nat)) (x : Nat → Nat) (v : Nat)
    (hc : constOn bs x = true) :
    (bs.map (fun bl => if x v == x (headOf bl) then (bl.length : Int) else 0)).sum =
      (bs.flatten.countP (fun u => x u == x v) : Int) := by
  have hcp : cOnP bs x := (constOn_eq_cOnP bs x).mp hc
  induction bs with
  | nil => simp
  | cons b bs ih =>
      have hcb : ∀ u ∈ b, x u = x (headOf b) := by
        intro u hu
        exact hcp b (by simp) u hu
      have htailP : cOnP bs x := by
        intro bl hbl u hu
        exact hcp bl (by simp [hbl]) u hu
      have htail : constOn bs x = true := (constOn_eq_cOnP bs x).mpr htailP
      simp only [List.map_cons, List.sum_cons, List.flatten_cons]
      rw [ih htail htailP]
      by_cases h : x v == x (headOf b)
      · have hvh : x (headOf b) = x v := (beq_iff_eq.mp h).symm
        rw [if_pos h]
        have hcnt : b.countP (fun u => x u == x v) = b.length := by
          apply countP_of_all_eq
          intro u hu
          exact (hcb u hu).trans hvh
        simp [hcnt, Int.natCast_add]
      · rw [if_neg h]
        have hcnt : b.countP (fun u => x u == x v) = 0 := by
          rw [List.countP_eq_zero.mpr]
          intro u hu heq
          have huv : x u = x v := beq_iff_eq.mp heq
          have hub : x u = x (headOf b) := hcb u hu
          apply h
          simp [← hub, huv]
        simp [hcnt]

private theorem countP_le_one_of_map_nodup (l : List Nat) (v : Nat) (x : Nat → Nat)
    (h : (l.map x).Nodup) : l.countP (fun u => x u == x v) ≤ 1 := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ht : (l.map x).Nodup := (List.nodup_cons.mp h).2
      have hi := ih ht
      have hnot : x a ∉ l.map x := (List.nodup_cons.mp h).1
      simp only [List.map_cons, List.countP_cons]
      by_cases ha : x a == x v
      · have hzero : l.countP (fun u => x u == x v) = 0 := by
          rw [List.countP_eq_zero.mpr]
          intro u hu heq
          have huv : x u = x v := beq_iff_eq.mp heq
          have hva : x a = x v := beq_iff_eq.mp ha
          have : x u ∈ l.map x := List.mem_map.mpr ⟨u, hu, rfl⟩
          exact hnot (by simpa [huv, hva] using this)
        simp [ha, hzero]
      · simp [ha]
        omega

private theorem countP_pos_mem (l : List Nat) (v : Nat) (x : Nat → Nat)
    (h : 0 < l.countP (fun u => x u == x v)) : x v ∈ l.map x := by
  rcases List.countP_pos_iff.mp h with ⟨u, hu, huv⟩
  apply List.mem_map.mpr
  refine ⟨u, hu, ?_⟩
  exact beq_iff_eq.mp huv

private theorem zipIdx_fst (bs : List (List Nat)) : bs.zipIdx.map (fun p => p.1) = bs := by
  induction bs with
  | nil => simp
  | cons b bs ih => simp [List.zipIdx_cons, ih]

private theorem sum_zipIdx_fst (bs : List (List Nat)) (g : List Nat → Int) :
    (bs.zipIdx.map (fun p => g p.1)).sum = (bs.map g).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [List.zipIdx_succ]
      simp only [List.map_map]
      change g b + (bs.zipIdx.map (fun p => g p.1)).sum = g b + (bs.map g).sum
      rw [ih]

private theorem mobius_point_gen (V : List Nat) (hV : V.Nodup) (x : Bind) :
    ((partitions V).map (fun bs => mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
      (if (V.map x).Nodup then (1 : Int) else 0) := by
  induction V with
  | nil => simp [partitions, mobius, constOn]
  | cons v vs ih =>
      have hvs : vs.Nodup := (List.nodup_cons.mp hV).2
      have ih' := ih hvs
      let k : Int := (vs.countP (fun u => x u == x v) : Int)
      have hmod (bs : List (List Nat)) (hn : ∀ bl ∈ bs, bl ≠ [])
          (i : Nat) (hi : i < bs.length) :
          mobius (bs.modify i (fun bl => v :: bl)) *
              (if constOn (bs.modify i (fun bl => v :: bl)) x then (1 : Int) else 0) =
            mobius bs * (if constOn bs x then
              (if x v == x (headOf (bs.get ⟨i, hi⟩)) then
                -((bs.get ⟨i, hi⟩).length : Int) else 0) else 0) := by
        rw [mobius_modify bs i hi v hn, constOn_modify bs i hi v x hn]
        by_cases hc : constOn bs x = true
        · have hcp : cOnP bs x := (constOn_eq_cOnP bs x).mp hc
          have hblock : bs.get ⟨i, hi⟩ ≠ [] := hn _ (List.getElem_mem hi)
          have hheadmem : headOf (bs.get ⟨i, hi⟩) ∈ bs.get ⟨i, hi⟩ := by
            cases hblk : bs.get ⟨i, hi⟩ with
            | nil =>
                rw [hblk] at hblock
                exact False.elim (hblock rfl)
            | cons a as => simp [headOf]
          by_cases hhd : x v == x (headOf (bs.get ⟨i, hi⟩))
          · have hall : (bs.get ⟨i, hi⟩).all (fun u => x u == x v) = true := by
              apply List.all_eq_true.mpr
              intro u hu
              have huEq := hcp (bs.get ⟨i, hi⟩) (List.getElem_mem hi) u hu
              have hhv : x (headOf (bs.get ⟨i, hi⟩)) = x v := (beq_iff_eq.mp hhd).symm
              exact beq_iff_eq.mpr (huEq.trans hhv)
            rw [if_pos hc, if_pos hhd]
            have hand : (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
              rw [hc, hall]
              rfl
            rw [if_pos hand]
            grind
          · have hallfalse : (bs.get ⟨i, hi⟩).all (fun u => x u == x v) = false := by
              cases hall' : (bs.get ⟨i, hi⟩).all (fun u => x u == x v)
              · rfl
              · have hhv' := List.all_eq_true.mp hall' (headOf (bs.get ⟨i, hi⟩)) hheadmem
                have hhv : x v = x (headOf (bs.get ⟨i, hi⟩)) := (beq_iff_eq.mp hhv').symm
                exact False.elim (hhd (by simp [hhv]))
            rw [if_pos hc, if_neg hhd]
            have hand : (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = false := by
              rw [hc, hallfalse]
              rfl
            have handnot : ¬ (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
              rw [hand]
              decide
            rw [if_neg handnot]
            grind
        · rw [if_neg hc]
          have handnot : ¬ (constOn bs x && (bs.get ⟨i, hi⟩).all (fun u => x u == x v)) = true := by
            intro hh
            rw [Bool.and_eq_true] at hh
            exact hc hh.1
          rw [if_neg handnot]
          grind
      have hterm (bs : List (List Nat)) (hbs : bs ∈ partitions vs) :
          ((([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))).map
              (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))).sum =
            (mobius bs * (if constOn bs x then (1 : Int) else 0)) * (1 - k) := by
        have hne := partitions_nonempty vs bs hbs
        simp only [List.map_cons, List.sum_cons]
        rw [mobius_singleton, constOn_singleton]
        rw [show (List.map (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))
                  (List.map (fun p => bs.modify p.2 (fun bl => v :: bl)) bs.zipIdx)).sum =
              (bs.zipIdx.map (fun p => mobius (bs.modify p.2 (fun bl => v :: bl)) *
                (if constOn (bs.modify p.2 (fun bl => v :: bl)) x then (1 : Int) else 0))).sum by
              rw [List.map_map]
              rfl]
        rw [show (bs.zipIdx.map (fun p => mobius (bs.modify p.2 (fun bl => v :: bl)) *
                (if constOn (bs.modify p.2 (fun bl => v :: bl)) x then (1 : Int) else 0))) =
              (bs.zipIdx.map (fun p => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf p.1) then -(p.1.length : Int) else 0) else 0))) by
              apply List.map_congr_left
              intro p hp
              have hi : p.2 < bs.length := by simpa using (List.mem_zipIdx hp).2.1
              have hpval : p.1 = bs.get ⟨p.2, hi⟩ := by simpa using (List.mem_zipIdx hp).2.2
              rw [hmod bs hne p.2 hi]
              rw [← hpval]]
        rw [show (bs.zipIdx.map (fun p => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf p.1) then -(p.1.length : Int) else 0) else 0))).sum =
              (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum by
            exact sum_zipIdx_fst bs (fun bl => mobius bs *
              (if constOn bs x then
                (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))]
        by_cases hc : constOn bs x = true
        · rw [if_pos hc]
          rw [show (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum =
                (bs.map (fun bl => mobius bs *
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0))).sum by
              apply congrArg List.sum
              apply List.map_congr_left
              intro bl hbl
              rw [if_pos hc]]
          rw [show (bs.map (fun bl => mobius bs *
                (if x v == x (headOf bl) then -(bl.length : Int) else 0))).sum =
                (-mobius bs) *
                  (bs.map (fun bl => if x v == x (headOf bl) then (bl.length : Int) else 0)).sum by
              rw [show (fun bl => mobius bs *
                    (if x v == x (headOf bl) then -(bl.length : Int) else 0)) =
                    (fun bl => (-mobius bs) *
                      (if x v == x (headOf bl) then (bl.length : Int) else 0)) by
                    funext bl
                    by_cases hhd : x v == x (headOf bl) <;> simp [hhd] <;> grind]
              rw [sum_mul_left]]
          rw [sum_block_count bs x v hc]
          have hperm := partitions_flatten_perm vs bs hbs
          have hcperm := List.Perm.countP_eq (fun u => x u == x v) hperm
          dsimp [k]
          rw [hcperm]
          grind
        · rw [if_neg hc]
          have hzero : (bs.map (fun bl => mobius bs *
                (if constOn bs x then
                  (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0))).sum = 0 := by
            rw [show (fun bl => mobius bs *
                  (if constOn bs x then
                    (if x v == x (headOf bl) then -(bl.length : Int) else 0) else 0)) =
                  (fun _ => (0 : Int)) by
                  funext bl
                  rw [if_neg hc]
                  simp]
            exact sum_zero bs
          rw [hzero]
          grind
      rw [show partitions (v :: vs) =
          (partitions vs).flatMap (fun bs =>
            ([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))) by rfl]
      rw [sum_flatMap]
      rw [show ((partitions vs).map (fun bs =>
            ((([v] :: bs) :: bs.zipIdx.map (fun (_, i) => bs.modify i (fun bl => v :: bl))).map
              (fun bs' => mobius bs' * (if constOn bs' x then (1 : Int) else 0))).sum)).sum =
            ((partitions vs).map (fun bs =>
              (mobius bs * (if constOn bs x then (1 : Int) else 0)) * (1 - k))).sum by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          exact hterm bs hbs]
      rw [sum_mul_right]
      rw [ih']
      by_cases hN : (vs.map x).Nodup
      · have hk_le : vs.countP (fun u => x u == x v) ≤ 1 := countP_le_one_of_map_nodup vs v x hN
        have hk01 : vs.countP (fun u => x u == x v) = 0 ∨ vs.countP (fun u => x u == x v) = 1 := by omega
        rcases hk01 with hk0 | hk1
        · have hnot : x v ∉ vs.map x := by
            intro hm
            rcases List.mem_map.mp hm with ⟨u, hu, huv⟩
            have heq : x u == x v := by simp [huv]
            exact (List.countP_eq_zero.mp hk0) u hu heq
          have hnot' : ∀ u ∈ vs, x u ≠ x v := by
            intro u hu heq
            exact hnot (List.mem_map.mpr ⟨u, hu, heq⟩)
          have hnodup : ((v :: vs).map x).Nodup := by
            simp only [List.map_cons, List.nodup_cons]
            exact ⟨hnot, hN⟩
          rw [if_pos hN, if_pos hnodup]
          simp [k, hk0]
        · have hpos : 0 < vs.countP (fun u => x u == x v) := by omega
          have hmem := countP_pos_mem vs v x hpos
          have hnotnodup : ¬ ((v :: vs).map x).Nodup := by
            intro hn
            have hnot : x v ∉ vs.map x := (List.nodup_cons.mp (by simpa using hn)).1
            exact hnot hmem
          rcases List.mem_map.mp hmem with ⟨u, hu, huv⟩
          rw [if_pos hN, if_neg hnotnodup]
          simp [k, hk1]
      · have hnotnodup : ¬ ((v :: vs).map x).Nodup := by
          intro hn
          exact hN (List.nodup_cons.mp (by simpa using hn)).2
        rw [if_neg hN, if_neg hnotnodup]
        simp

private theorem mobius_point (x : Bind) :
    ((partitions pattern.freeVars).map (fun bs =>
      mobius bs * (if constOn bs x then (1 : Int) else 0))).sum =
    (if (pattern.freeVars.map x).Nodup then (1 : Int) else 0) := by
  exact mobius_point_gen pattern.freeVars (by native_decide) x
theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  let V := pattern.freeVars
  let F : Bind → Int := fun x => (pattern.atoms.map (fun a => value db a x)).prod
  have hF : ∀ x y, (∀ v ∈ V, x v = y v) → F x = F y := by
    intro x y hxy
    dsimp only [F, V]
    simp [pattern, value]
    rw [hxy 0 (by native_decide), hxy 2 (by native_decide),
      hxy 1 (by native_decide), hxy 3 (by native_decide),
      hxy 4 (by native_decide), hxy 5 (by native_decide)]
  have hcoll : ∀ bs ∈ partitions V,
      collapsed pattern n db b bs =
        integrate n V b (fun x => if constOn bs x then F x else 0) := by
    intro bs hbs
    unfold collapsed
    change integrate n (Heads bs) b (fun y => F (fun v => y (Root bs v))) = _
    exact collapse_sum n b F bs hbs hF
  change integrate n V b (fun x => if (V.map x).Nodup then F x else 0) =
    ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum
  calc
    integrate n V b (fun x => if (V.map x).Nodup then F x else 0)
        = ((bindings n V b).map (fun x => if (V.map x).Nodup then F x else 0)).sum :=
            bindings_sum n V b _
    _ = ((bindings n V b).map (fun x => F x *
          ((partitions V).map (fun bs => mobius bs *
            (if constOn bs x then (1 : Int) else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [mobius_point x]
          by_cases hN : (V.map x).Nodup <;> simp [hN, V]
    _ = ((bindings n V b).map (fun x => ((partitions V).map (fun bs =>
          mobius bs * (if constOn bs x then F x else 0))).sum)).sum := by
          apply sum_congr
          intro x hx
          rw [show (fun bs => mobius bs * (if constOn bs x then F x else 0)) =
              (fun bs => (mobius bs * (if constOn bs x then (1 : Int) else 0)) * F x) by
            funext bs
            by_cases h : constOn bs x <;> simp [h]]
          rw [sum_mul_right]
          ac_rfl
    _ = ((partitions V).map (fun bs => mobius bs *
          ((bindings n V b).map (fun x => if constOn bs x then F x else 0)).sum)).sum := by
          rw [show (fun x => ((partitions V).map (fun bs =>
              mobius bs * (if constOn bs x then F x else 0))).sum) =
              (fun x => ((partitions V).map (fun bs =>
                (fun x => mobius bs * (if constOn bs x then F x else 0)) x)).sum) by rfl]
          rw [sum_swap]
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [sum_mul_left]
    _ = ((partitions V).map (fun bs => mobius bs *
          integrate n V b (fun x => if constOn bs x then F x else 0))).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [bindings_sum]
    _ = ((partitions V).map (fun bs => mobius bs * collapsed pattern n db b bs)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro bs hbs
          rw [hcoll bs hbs]


end TinylambOpt.ResearchProofs.Round2.Task78IC
-- END jpb-copy Task78IC

-- BEGIN jpb-copy Task80 (IntersectingCycles FractionalCap)
namespace TinylambOpt.ResearchProofs.Round2.Task80

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles

/-- Sanity checks on the concrete pattern. -/
example : pattern.freeVars = [4, 5, 1, 2, 3, 0] := by rfl
example : pattern.atoms =
    [⟨0, 0, 1⟩, ⟨0, 1, 2⟩, ⟨0, 2, 3⟩, ⟨0, 3, 0⟩,
     ⟨1, 2, 4⟩, ⟨2, 4, 5⟩, ⟨2, 5, 3⟩] := by rfl

private abbrev fv : List Nat := pattern.freeVars

private def rsum (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem rsum_succ (n : Nat) (f : Nat → Int) : rsum (n+1) f = rsum n f + f n := by
  simp [rsum, List.range_succ, List.sum_append]

private theorem rsum_zero_const (m : Nat) : rsum m (fun _ => (0 : Int)) = 0 := by
  induction m with
  | zero => simp [rsum]
  | succ m ih => rw [rsum_succ, ih]; simp

private theorem rsum_congr (n : Nat) {f g : Nat → Int} (h : ∀ i, i < n → f i = g i) :
    rsum n f = rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi))]
      rw [h n (Nat.lt_succ_self n)]

private theorem rsum_add (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i + g i) = rsum n f + rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, rsum_succ, ih]; omega

private theorem rsum_neg (n : Nat) (f : Nat → Int) : rsum n (fun i => -f i) = -rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih]; omega

private theorem rsum_sub (n : Nat) (f g : Nat → Int) :
    rsum n (fun i => f i - g i) = rsum n f - rsum n g := by
  rw [show (fun i => f i - g i) = (fun i => f i + (-g i)) by
    funext i; rw [Int.sub_eq_add_neg]]
  rw [rsum_add, rsum_neg]
  omega

private theorem rsum_const_mul (n : Nat) (c : Int) (f : Nat → Int) :
    rsum n (fun i => c * f i) = c * rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.mul_add]

private theorem rsum_mul_const (n : Nat) (f : Nat → Int) (c : Int) :
    rsum n (fun i => f i * c) = rsum n f * c := by
  induction n with
  | zero => simp [rsum]
  | succ n ih => rw [rsum_succ, rsum_succ, ih, Int.add_mul]

private theorem rsum_comm (n m : Nat) (f : Nat → Nat → Int) :
    rsum n (fun i => rsum m (fun j => f i j)) = rsum m (fun j => rsum n (fun i => f i j)) := by
  induction n with
  | zero =>
      rw [show rsum 0 (fun i => rsum m (fun j => f i j)) = 0 by simp [rsum]]
      rw [show rsum m (fun j => rsum 0 (fun i => f i j)) = rsum m (fun _ => (0 : Int)) by
        apply rsum_congr; intro j hj; simp [rsum]]
      exact (rsum_zero_const m).symm
  | succ n ih =>
      rw [rsum_succ, ih]
      rw [show rsum m (fun j => rsum (n+1) (fun i => f i j)) =
          rsum m (fun j => rsum n (fun i => f i j) + f n j) by
        apply rsum_congr; intro j hj; rw [rsum_succ]]
      rw [rsum_add]

private theorem rsum_nonneg (n : Nat) (f : Nat → Int) (h : ∀ i, i < n → 0 ≤ f i) :
    0 ≤ rsum n f := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hii => h i (Nat.lt_succ_of_lt hii))
      omega

private theorem rsum_le (n : Nat) (f g : Nat → Int) (h : ∀ i, i < n → f i ≤ g i) :
    rsum n f ≤ rsum n g := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ]
      have hn := h n (Nat.lt_succ_self n)
      have hi := ih (fun i hii => h i (Nat.lt_succ_of_lt hii))
      omega

private theorem sum_bool {α} (l : List α) (p : α → Bool) :
    (l.map (fun x => if p x then (1 : Int) else 0)).sum = (l.countP p : Int) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases h : p a = true
      · simp [h, ih]; omega
      · have h' : p a = false := by cases hpa : p a <;> simp_all
        simp [h', ih]

private theorem int_sum_nat (l : List Nat) : (l.map (fun x : Nat => (x : Int))).sum = (l.sum : Int) := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

/-! ## FractionalCap: indicator expansion and integral cover bounds. -/

private def I (db : DB) (r x y : Nat) : Int := if db r x y == 0 then 0 else 1

private theorem I_nonneg (db : DB) (r x y : Nat) : 0 ≤ I db r x y := by
  by_cases h : db r x y == 0 <;> simp [I, h]

private theorem I_le_one (db : DB) (r x y : Nat) : I db r x y ≤ 1 := by
  by_cases h : db r x y == 0 <;> simp [I, h]

private theorem I_eq_ne (db : DB) (r x y : Nat) :
    I db r x y = if db r x y != 0 then 1 else 0 := by
  by_cases h : db r x y == 0
  · have hz : db r x y = 0 := beq_iff_eq.mp h
    simp [I, h, hz]
  · have hn : db r x y ≠ 0 := by intro he; exact h (by simp [he])
    have ht : (db r x y != 0) = true := by simp [hn]
    simp [I, h, ht]

private theorem countP_eq_sumI (n : Nat) (db : DB) (r x : Nat) :
    ((List.range n).countP (fun y => db r x y != 0) : Int) =
      ((List.range n).map (fun y => I db r x y)).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, List.map_append, List.sum_append]
      rw [show ((List.countP (fun y => db r x y != 0) (List.range n) +
          [n].countP (fun y => db r x y != 0) : Nat) : Int) =
          (List.countP (fun y => db r x y != 0) (List.range n) : Int) +
          ([n].countP (fun y => db r x y != 0) : Int) by omega]
      rw [ih]
      by_cases hz : db r x n = 0 <;> simp [I, hz]

private def S (n : Nat) (db : DB) (r : Nat) : Int :=
  rsum n (fun x => rsum n (fun y => I db r x y))

private theorem support_eq_S (n : Nat) (db : DB) (b : Bind) (r src dst : Nat)
    (hs : src ∈ fv) (hd : dst ∈ fv) :
    (support n db ⟨r, src, dst⟩ pattern b : Int) = S n db r := by
  have hsrc2 : [4, 5, 1, 2, 3, 0].contains src = true := by
    simpa [fv, pattern] using hs
  have hdst2 : [4, 5, 1, 2, 3, 0].contains dst = true := by
    simpa [fv, pattern] using hd
  unfold support S rsum
  simp only [pattern, Atom.src, Atom.dst, hsrc2, hdst2, ↓reduceIte]
  rw [← int_sum_nat ((List.range n).map (fun x => List.countP (fun y => db r x y != 0) (List.range n)))]
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  exact countP_eq_sumI n db r x

private theorem ref_eq_sum (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
      rsum n (fun v4 => rsum n (fun v5 => rsum n (fun v1 => rsum n (fun v2 =>
        rsum n (fun v3 => rsum n (fun v0 =>
          I db 0 v0 v1 * I db 0 v1 v2 * I db 0 v2 v3 * I db 0 v3 v0 *
            I db 1 v2 v4 * I db 2 v4 v5 * I db 2 v5 v3)))))) := by
  simp [reference, pattern, integrate, value, put, rsum, I, Int.mul_assoc]

private theorem ref_nonneg (n : Nat) (db : DB) (b : Bind) :
    0 ≤ reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b := by
  rw [ref_eq_sum]
  apply rsum_nonneg; intro v4 h4
  apply rsum_nonneg; intro v5 h5
  apply rsum_nonneg; intro v1 h1
  apply rsum_nonneg; intro v2 h2
  apply rsum_nonneg; intro v3 h3
  apply rsum_nonneg; intro v0 h0
  have h01 := I_nonneg db 0 v0 v1
  have h12 := I_nonneg db 0 v1 v2
  have h23 := I_nonneg db 0 v2 v3
  have h30 := I_nonneg db 0 v3 v0
  have h24 := I_nonneg db 1 v2 v4
  have h45 := I_nonneg db 2 v4 v5
  have h53 := I_nonneg db 2 v5 v3
  exact Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg
    (Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg h01 h12) h23) h30) h24) h45) h53

private theorem integrate_mono (n : Nat) (vs : List Nat) (b : Bind) {f g : Bind → Int}
    (h : ∀ x, f x ≤ g x) : integrate n vs b f ≤ integrate n vs b g := by
  induction vs generalizing b with
  | nil => exact h b
  | cons v vs ih =>
      simp only [integrate]
      apply rsum_le
      intro x hx
      exact ih (put b v x)

private theorem mul_le_one_nonneg {a b : Int} (ha0 : 0 ≤ a) (ha1 : a ≤ 1)
    (hb0 : 0 ≤ b) (hb1 : b ≤ 1) : a * b ≤ 1 := by
  have h := Int.mul_le_mul ha1 hb1 hb0 (by omega : (0 : Int) ≤ 1)
  simpa using h

private theorem drop_mul {a b : Int} (ha0 : 0 ≤ a) (hb0 : 0 ≤ b) (hb1 : b ≤ 1) :
    a * b ≤ a := by
  have h := Int.mul_le_mul_of_nonneg_left hb1 ha0
  simpa using h

private theorem rsum6_prod (n : Nat) (f1 f2 f3 : Nat → Nat → Int) :
    rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => rsum n (fun x5 => f1 x0 x1 * f2 x2 x3 * f3 x4 x5)))))) =
    (rsum n (fun x => rsum n (fun y => f1 x y))) *
      (rsum n (fun x => rsum n (fun y => f2 x y))) *
        (rsum n (fun x => rsum n (fun y => f3 x y))) := by
  let S3 := rsum n (fun x => rsum n (fun y => f3 x y))
  let S2 := rsum n (fun x => rsum n (fun y => f2 x y))
  let S1 := rsum n (fun x => rsum n (fun y => f1 x y))
  calc
    rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
      rsum n (fun x4 => rsum n (fun x5 => f1 x0 x1 * f2 x2 x3 * f3 x4 x5))))))
        = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
            rsum n (fun x4 => (f1 x0 x1 * f2 x2 x3) *
              rsum n (fun x5 => f3 x4 x5)))))) := by
              apply rsum_congr; intro x0 h0
              apply rsum_congr; intro x1 h1
              apply rsum_congr; intro x2 h2
              apply rsum_congr; intro x3 h3
              apply rsum_congr; intro x4 h4
              rw [rsum_const_mul]
    _ = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 => rsum n (fun x3 =>
          (f1 x0 x1 * f2 x2 x3) * S3)))) := by
              apply rsum_congr; intro x0 h0
              apply rsum_congr; intro x1 h1
              apply rsum_congr; intro x2 h2
              apply rsum_congr; intro x3 h3
              dsimp [S3]
              rw [rsum_const_mul]
    _ = rsum n (fun x0 => rsum n (fun x1 => rsum n (fun x2 =>
          (f1 x0 x1 * S3) * rsum n (fun x3 => f2 x2 x3)))) := by
              apply rsum_congr; intro x0 h0
              apply rsum_congr; intro x1 h1
              apply rsum_congr; intro x2 h2
              rw [show (fun x3 => (f1 x0 x1 * f2 x2 x3) * S3) =
                  (fun x3 => (f1 x0 x1 * S3) * f2 x2 x3) by
                funext x3; ac_rfl]
              rw [rsum_const_mul]
    _ = rsum n (fun x0 => rsum n (fun x1 =>
          (f1 x0 x1 * S3) * S2)) := by
              apply rsum_congr; intro x0 h0
              apply rsum_congr; intro x1 h1
              dsimp [S2]
              rw [rsum_const_mul]
    _ = rsum n (fun x0 => (S3 * S2) * rsum n (fun x1 => f1 x0 x1)) := by
              apply rsum_congr; intro x0 h0
              rw [show (fun x1 => (f1 x0 x1 * S3) * S2) =
                  (fun x1 => (S3 * S2) * f1 x0 x1) by
                funext x1; ac_rfl]
              rw [rsum_const_mul]
    _ = (S3 * S2) * S1 := by
              dsimp [S1]
              rw [rsum_const_mul]
    _ = S1 * S2 * S3 := by ac_rfl

private theorem integrate_pairs_canonical (n : Nat) (db : DB) (b : Bind) (r1 r2 r3 : Nat) :
    integrate n [0, 1, 2, 3, 4, 5] b
      (fun x => I db r1 (x 0) (x 1) * I db r2 (x 2) (x 3) * I db r3 (x 4) (x 5)) =
        S n db r1 * S n db r2 * S n db r3 := by
  simp [integrate, S, rsum, put]
  exact rsum6_prod n (fun x y => I db r1 x y) (fun x y => I db r2 x y) (fun x y => I db r3 x y)

private theorem prod7_le_keep3 (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*c*f := by
  have hkeep : 0 ≤ a*c*f := Int.mul_nonneg (Int.mul_nonneg ha0 hc0) hf0
  have hbd : b*d ≤ 1 := mul_le_one_nonneg hb0 hb1 hd0 hd1
  have heg : e*g ≤ 1 := mul_le_one_nonneg he0 he1 hg0 hg1
  have hdrop : b*d*(e*g) ≤ 1 :=
    mul_le_one_nonneg (Int.mul_nonneg hb0 hd0) hbd (Int.mul_nonneg he0 hg0) heg
  have hdrop' : b*d*e*g ≤ 1 := by
    rw [show b*d*e*g = b*d*(e*g) by ac_rfl]
    exact hdrop
  calc
    a*b*c*d*e*f*g = (a*c*f)*(b*d*e*g) := by ac_rfl
    _ ≤ (a*c*f)*1 := Int.mul_le_mul_of_nonneg_left hdrop' hkeep
    _ = a*c*f := by simp

private theorem prod7_le_keep4 (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*b*e*g := by
  have hab : 0 ≤ a*b := Int.mul_nonneg ha0 hb0
  have habeg : 0 ≤ a*b*e := Int.mul_nonneg hab he0
  have hkeep : 0 ≤ a*b*e*g := Int.mul_nonneg habeg hg0
  have hcd : c*d ≤ 1 := mul_le_one_nonneg hc0 hc1 hd0 hd1
  have hf1' : f ≤ 1 := hf1
  have hdrop : c*d*f ≤ 1 := by
    calc
      c*d*f = (c*d)*f := by ac_rfl
      _ ≤ 1*1 := Int.mul_le_mul hcd hf1' hf0 (by omega : (0 : Int) ≤ 1)
      _ = 1 := by simp
  calc
    a*b*c*d*e*f*g = (a*b*e*g)*(c*d*f) := by ac_rfl
    _ ≤ (a*b*e*g)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = a*b*e*g := by simp

private theorem rsum_mul_le_products_aux (n : Nat) (a b : Nat → Int)
    (ha : ∀ i, i < n → 0 ≤ a i) (hb : ∀ i, i < n → 0 ≤ b i) :
    rsum n (fun i => a i * b i) ≤ rsum n a * rsum n b := by
  induction n with
  | zero => simp [rsum]
  | succ n ih =>
      rw [rsum_succ, rsum_succ, rsum_succ]
      have han : 0 ≤ a n := ha n (Nat.lt_succ_self n)
      have hbn : 0 ≤ b n := hb n (Nat.lt_succ_self n)
      have hia := ih (fun i hii => ha i (Nat.lt_succ_of_lt hii))
        (fun i hii => hb i (Nat.lt_succ_of_lt hii))
      have hsa : 0 ≤ rsum n a := rsum_nonneg n a (fun i hi => ha i (Nat.lt_succ_of_lt hi))
      have hsb : 0 ≤ rsum n b := rsum_nonneg n b (fun i hi => hb i (Nat.lt_succ_of_lt hi))
      simp only [Int.add_mul, Int.mul_add]
      have hp1 : 0 ≤ rsum n a * b n := Int.mul_nonneg hsa hbn
      have hp2 : 0 ≤ a n * rsum n b := Int.mul_nonneg han hsb
      omega

private theorem rsum_path_le (n : Nat) (f g : Nat → Nat → Int)
    (hf0 : ∀ m a, 0 ≤ f m a) (hf1 : ∀ m a, f m a ≤ 1)
    (hg0 : ∀ m b, 0 ≤ g m b) (hg1 : ∀ m b, g m b ≤ 1) :
    rsum n (fun m => rsum n (fun a => rsum n (fun b => f m a * g m b))) ≤
      (rsum n (fun m => rsum n (fun a => f m a))) *
        (rsum n (fun m => rsum n (fun b => g m b))) := by
  calc
    rsum n (fun m => rsum n (fun a => rsum n (fun b => f m a * g m b)))
        = rsum n (fun m => (rsum n (fun a => f m a)) *
            rsum n (fun b => g m b)) := by
            apply rsum_congr; intro m hm
            rw [show (fun a => rsum n (fun b => f m a * g m b)) =
                (fun a => f m a * rsum n (fun b => g m b)) by
              funext a
              rw [rsum_const_mul]]
            rw [rsum_mul_const]
    _ ≤ (rsum n (fun m => rsum n (fun a => f m a))) *
          (rsum n (fun m => rsum n (fun b => g m b))) := by
            apply rsum_mul_le_products_aux
            · intro m hm
              exact rsum_nonneg n (fun a => f m a) (fun i hi => hf0 m i)
            · intro m hm
              exact rsum_nonneg n (fun b => g m b) (fun i hi => hg0 m i)

private theorem integrate_path_canonical (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [0, 1, 2] b
      (fun x => I db r1 (x 0) (x 1) * I db r2 (x 1) (x 2)) ≤
        S n db r1 * S n db r2 := by
  simp only [integrate, S, put, List.map_cons, List.sum_cons, List.map_nil, List.sum_nil]
  change rsum n (fun x => rsum n (fun y => rsum n (fun z =>
      I db r1 x y * I db r2 y z))) ≤
    (rsum n (fun x => rsum n (fun y => I db r1 x y))) *
      (rsum n (fun y => rsum n (fun z => I db r2 y z)))
  rw [rsum_comm]
  rw [show (rsum n (fun x => rsum n (fun y => I db r1 x y))) =
      rsum n (fun y => rsum n (fun x => I db r1 x y)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 a m) (fun m b => I db r2 m b)
    (fun m a => I_nonneg db r1 a m) (fun m a => I_le_one db r1 a m)
    (fun m b => I_nonneg db r2 m b) (fun m b => I_le_one db r2 m b)

private theorem rsum6_split (n : Nat) (f g : Nat → Nat → Nat → Int) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      rsum n (fun e => rsum n (fun h => f a b c * g d e h)))))) =
    (rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c)))) *
      (rsum n (fun d => rsum n (fun e => rsum n (fun h => g d e h)))) := by
  let Gtot := rsum n (fun d => rsum n (fun e => rsum n (fun h => g d e h)))
  let Ftot := rsum n (fun a => rsum n (fun b => rsum n (fun c => f a b c)))
  calc
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
      rsum n (fun e => rsum n (fun h => f a b c * g d e h))))))
        = rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d =>
            f a b c * rsum n (fun e => rsum n (fun h => g d e h)))))) := by
              apply rsum_congr; intro a ha
              apply rsum_congr; intro b hb
              apply rsum_congr; intro c hc
              apply rsum_congr; intro d hd
              rw [show (fun e => rsum n (fun h => f a b c * g d e h)) =
                  (fun e => f a b c * rsum n (fun h => g d e h)) by
                funext e
                rw [rsum_const_mul]]
              rw [rsum_const_mul]
    _ = rsum n (fun a => rsum n (fun b => rsum n (fun c =>
            f a b c * Gtot))) := by
              apply rsum_congr; intro a ha
              apply rsum_congr; intro b hb
              apply rsum_congr; intro c hc
              dsimp [Gtot]
              rw [rsum_const_mul]
    _ = rsum n (fun a => rsum n (fun b =>
            Gtot * rsum n (fun c => f a b c))) := by
              apply rsum_congr; intro a ha
              apply rsum_congr; intro b hb
              rw [show (fun c => f a b c * Gtot) =
                  (fun c => Gtot * f a b c) by
                funext c; ac_rfl]
              rw [rsum_const_mul]
    _ = rsum n (fun a => Gtot * (rsum n (fun b => rsum n (fun c => f a b c)))) := by
              apply rsum_congr; intro a ha
              rw [rsum_const_mul]
    _ = Gtot * Ftot := by
              dsimp [Ftot]
              rw [rsum_const_mul]
    _ = Ftot * Gtot := by ac_rfl

private theorem integrate_split_canonical (n : Nat) (b : Bind) (f g : Nat → Nat → Nat → Int) :
    integrate n [0, 1, 2, 3, 4, 5] b
      (fun x => f (x 0) (x 1) (x 2) * g (x 3) (x 4) (x 5)) =
      integrate n [0, 1, 2] b (fun x => f (x 0) (x 1) (x 2)) *
        integrate n [3, 4, 5] b (fun x => g (x 3) (x 4) (x 5)) := by
  simp [integrate, rsum, put]
  exact rsum6_split n f g

private theorem integrate_perm (n : Nat) {vs ws : List Nat} (h : vs.Perm ws)
    (b : Bind) (f : Bind → Int) : integrate n vs b f = integrate n ws b f := by
  induction h generalizing b with
  | nil => rfl
  | cons v h ih =>
      simp only [integrate]
      apply congrArg List.sum
      apply List.map_congr_left
      intro x hx
      exact ih (put b v x)
  | swap v w vs =>
      by_cases hvw : v = w
      · subst w; rfl
      · change rsum n (fun c => rsum n (fun a => integrate n vs (put (put b w c) v a) f)) =
            rsum n (fun a => rsum n (fun c => integrate n vs (put (put b v a) w c) f))
        rw [rsum_comm]
        apply congrArg List.sum
        apply List.map_congr_left
        intro c hc
        apply congrArg List.sum
        apply List.map_congr_left
        intro a ha
        congr 1
        funext i
        by_cases hiv : i == v
        · by_cases hiw : i == w
          · have hiv' : i = v := beq_iff_eq.mp hiv
            have hiw' : i = w := beq_iff_eq.mp hiw
            exfalso
            exact hvw (hiv'.symm.trans hiw')
          · simp [put, hiv, hiw]
        · by_cases hiw : i == w <;> simp [put, hiv, hiw]
  | trans h1 h2 ih1 ih2 => exact (ih1 b).trans (ih2 b)


/-! ## Cover bounds for FractionalCap. -/

private def Fprod (db : DB) (x : Bind) : Int :=
  I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2) * I db 0 (x 2) (x 3) * I db 0 (x 3) (x 0) *
    I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3)

private theorem ref_eq_integrate (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b =
      integrate n fv b (Fprod db) := by
  simp only [reference, fv, pattern, value]
  apply congrArg (integrate n fv b)
  funext x
  simp [Fprod, I]
  ac_rfl

private theorem prod3_le_one (a b c : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) : a*b*c ≤ 1 := by
  have hab : a*b ≤ 1 := mul_le_one_nonneg ha0 ha1 hb0 hb1
  have hab0 : 0 ≤ a*b := Int.mul_nonneg ha0 hb0
  calc
    a*b*c = (a*b)*c := by ac_rfl
    _ ≤ 1*1 := Int.mul_le_mul hab hc1 hc0 (by omega : (0 : Int) ≤ 1)
    _ = 1 := by simp

private theorem prod4_le_one (a b c d : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1) : a*b*c*d ≤ 1 := by
  have hab : a*b ≤ 1 := mul_le_one_nonneg ha0 ha1 hb0 hb1
  have hab0 : 0 ≤ a*b := Int.mul_nonneg ha0 hb0
  have habc : a*b*c ≤ 1 := mul_le_one_nonneg hab0 hab hc0 hc1
  have habc0 : 0 ≤ a*b*c := Int.mul_nonneg hab0 hc0
  calc
    a*b*c*d = (a*b*c)*d := by ac_rfl
    _ ≤ 1*1 := Int.mul_le_mul habc hd1 hd0 (by omega : (0 : Int) ≤ 1)
    _ = 1 := by simp

private theorem prod7_le_keep3_aeg (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*e*g := by
  have hkeep : 0 ≤ a*e*g := Int.mul_nonneg (Int.mul_nonneg ha0 he0) hg0
  have hdrop : b*c*d*f ≤ 1 := prod4_le_one b c d f hb0 hb1 hc0 hc1 hd0 hd1 hf0 hf1
  calc
    a*b*c*d*e*f*g = (a*e*g)*(b*c*d*f) := by ac_rfl
    _ ≤ (a*e*g)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = a*e*g := by simp

private theorem prod7_le_keep3_bdf (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ b*d*f := by
  have hkeep : 0 ≤ b*d*f := Int.mul_nonneg (Int.mul_nonneg hb0 hd0) hf0
  have hdrop : a*c*e*g ≤ 1 := prod4_le_one a c e g ha0 ha1 hc0 hc1 he0 he1 hg0 hg1
  calc
    a*b*c*d*e*f*g = (b*d*f)*(a*c*e*g) := by ac_rfl
    _ ≤ (b*d*f)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = b*d*f := by simp

private theorem prod7_le_keep4_abfg (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*b*f*g := by
  have hkeep : 0 ≤ a*b*f*g := Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg ha0 hb0) hf0) hg0
  have hdrop : c*d*e ≤ 1 := prod3_le_one c d e hc0 hc1 hd0 hd1 he0 he1
  calc
    a*b*c*d*e*f*g = (a*b*f*g)*(c*d*e) := by ac_rfl
    _ ≤ (a*b*f*g)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = a*b*f*g := by simp

private theorem prod7_le_keep4_adef (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ a*d*e*f := by
  have hkeep : 0 ≤ a*d*e*f := Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg ha0 hd0) he0) hf0
  have hdrop : b*c*g ≤ 1 := prod3_le_one b c g hb0 hb1 hc0 hc1 hg0 hg1
  calc
    a*b*c*d*e*f*g = (a*d*e*f)*(b*c*g) := by ac_rfl
    _ ≤ (a*d*e*f)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = a*d*e*f := by simp

private theorem prod7_le_keep4_bdeg (a b c d e f g : Int)
    (ha0 : 0 ≤ a) (ha1 : a ≤ 1) (hb0 : 0 ≤ b) (hb1 : b ≤ 1)
    (hc0 : 0 ≤ c) (hc1 : c ≤ 1) (hd0 : 0 ≤ d) (hd1 : d ≤ 1)
    (he0 : 0 ≤ e) (he1 : e ≤ 1) (hf0 : 0 ≤ f) (hf1 : f ≤ 1)
    (hg0 : 0 ≤ g) (hg1 : g ≤ 1) : a*b*c*d*e*f*g ≤ b*d*e*g := by
  have hkeep : 0 ≤ b*d*e*g := Int.mul_nonneg (Int.mul_nonneg (Int.mul_nonneg hb0 hd0) he0) hg0
  have hdrop : a*c*f ≤ 1 := prod3_le_one a c f ha0 ha1 hc0 hc1 hf0 hf1
  calc
    a*b*c*d*e*f*g = (b*d*e*g)*(a*c*f) := by ac_rfl
    _ ≤ (b*d*e*g)*1 := Int.mul_le_mul_of_nonneg_left hdrop hkeep
    _ = b*d*e*g := by simp

private theorem I_all_nonneg_le_one (db : DB) (x : Bind) :
    (0 ≤ I db 0 (x 0) (x 1) ∧ I db 0 (x 0) (x 1) ≤ 1) ∧
    (0 ≤ I db 0 (x 1) (x 2) ∧ I db 0 (x 1) (x 2) ≤ 1) ∧
    (0 ≤ I db 0 (x 2) (x 3) ∧ I db 0 (x 2) (x 3) ≤ 1) ∧
    (0 ≤ I db 0 (x 3) (x 0) ∧ I db 0 (x 3) (x 0) ≤ 1) ∧
    (0 ≤ I db 1 (x 2) (x 4) ∧ I db 1 (x 2) (x 4) ≤ 1) ∧
    (0 ≤ I db 2 (x 4) (x 5) ∧ I db 2 (x 4) (x 5) ≤ 1) ∧
    (0 ≤ I db 2 (x 5) (x 3) ∧ I db 2 (x 5) (x 3) ≤ 1) := by
  refine ⟨⟨I_nonneg db 0 (x 0) (x 1), I_le_one db 0 (x 0) (x 1)⟩,
    ⟨I_nonneg db 0 (x 1) (x 2), I_le_one db 0 (x 1) (x 2)⟩,
    ⟨I_nonneg db 0 (x 2) (x 3), I_le_one db 0 (x 2) (x 3)⟩,
    ⟨I_nonneg db 0 (x 3) (x 0), I_le_one db 0 (x 3) (x 0)⟩,
    ⟨I_nonneg db 1 (x 2) (x 4), I_le_one db 1 (x 2) (x 4)⟩,
    ⟨I_nonneg db 2 (x 4) (x 5), I_le_one db 2 (x 4) (x 5)⟩,
    ⟨I_nonneg db 2 (x 5) (x 3), I_le_one db 2 (x 5) (x 3)⟩⟩

private theorem integrate_zero (n : Nat) (vs : List Nat) (b : Bind) :
    integrate n vs b (fun _ => (0 : Int)) = 0 := by
  induction vs generalizing b with
  | nil => simp [integrate]
  | cons v vs ih =>
      change rsum n (fun x => integrate n vs (put b v x) (fun _ => (0 : Int))) = 0
      rw [show (fun x => integrate n vs (put b v x) (fun _ => (0 : Int))) =
          (fun _ => (0 : Int)) by
        funext x
        exact ih (put b v x)]
      exact rsum_zero_const n

private theorem S_nonneg (n : Nat) (db : DB) (r : Nat) : 0 ≤ S n db r := by
  unfold S
  apply rsum_nonneg; intro x hx
  apply rsum_nonneg; intro y hy
  exact I_nonneg db r x y

private theorem integrate_nonneg (n : Nat) (vs : List Nat) (b : Bind) (f : Bind → Int)
    (hf : ∀ x, 0 ≤ f x) : 0 ≤ integrate n vs b f := by
  rw [← integrate_zero n vs b]
  apply integrate_mono
  intro x
  exact hf x


/-! ## Specialized factorization and path bounds. -/

private theorem pairF_046 (n : Nat) (db : DB) (b : Bind) :
    integrate n [0, 1, 2, 4, 5, 3] b
      (fun x => I db 0 (x 0) (x 1) * I db 1 (x 2) (x 4) * I db 2 (x 5) (x 3)) =
        S n db 0 * S n db 1 * S n db 2 := by
  simp [integrate, S, rsum, put]
  exact rsum6_prod n (fun x y => I db 0 x y) (fun x y => I db 1 x y) (fun x y => I db 2 x y)

private theorem pairF_135 (n : Nat) (db : DB) (b : Bind) :
    integrate n [1, 2, 3, 0, 4, 5] b
      (fun x => I db 0 (x 1) (x 2) * I db 0 (x 3) (x 0) * I db 2 (x 4) (x 5)) =
        S n db 0 * S n db 0 * S n db 2 := by
  simp [integrate, S, rsum, put]
  exact rsum6_prod n (fun x y => I db 0 x y) (fun x y => I db 0 x y) (fun x y => I db 2 x y)

private theorem split_0156 (n : Nat) (db : DB) (b : Bind) :
    integrate n [0, 1, 2, 4, 5, 3] b
      (fun x => (I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2)) *
        (I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3))) =
      integrate n [0, 1, 2] b (fun x => I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2)) *
        integrate n [4, 5, 3] b (fun x => I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3)) := by
  simp [integrate, rsum, put]
  exact rsum6_split n (fun a c d => I db 0 a c * I db 0 c d)
    (fun e f h => I db 2 e f * I db 2 f h)

private theorem split_0345 (n : Nat) (db : DB) (b : Bind) :
    integrate n [0, 1, 3, 2, 4, 5] b
      (fun x => (I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0)) *
        (I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5))) =
      integrate n [0, 1, 3] b (fun x => I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0)) *
        integrate n [2, 4, 5] b (fun x => I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5)) := by
  simp [integrate, rsum, put]
  exact rsum6_split n (fun a c d => I db 0 a c * I db 0 d a)
    (fun e f h => I db 1 e f * I db 2 f h)

private theorem split_1346 (n : Nat) (db : DB) (b : Bind) :
    integrate n [1, 2, 4, 0, 3, 5] b
      (fun x => (I db 0 (x 1) (x 2) * I db 1 (x 2) (x 4)) *
        (I db 0 (x 3) (x 0) * I db 2 (x 5) (x 3))) =
      integrate n [1, 2, 4] b (fun x => I db 0 (x 1) (x 2) * I db 1 (x 2) (x 4)) *
        integrate n [0, 3, 5] b (fun x => I db 0 (x 3) (x 0) * I db 2 (x 5) (x 3)) := by
  simp [integrate, rsum, put]
  exact rsum6_split n (fun a c d => I db 0 a c * I db 1 c d)
    (fun e f h => I db 0 f e * I db 2 h f)

private theorem path_453 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [4, 5, 3] b
      (fun x => I db r1 (x 4) (x 5) * I db r2 (x 5) (x 3)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun a => rsum n (fun m => rsum n (fun z =>
      I db r1 a m * I db r2 m z))) ≤
    (rsum n (fun a => rsum n (fun m => I db r1 a m))) *
      (rsum n (fun m => rsum n (fun z => I db r2 m z)))
  rw [rsum_comm]
  rw [show (rsum n (fun a => rsum n (fun m => I db r1 a m))) =
      rsum n (fun m => rsum n (fun a => I db r1 a m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 a m) (fun m z => I db r2 m z)
    (fun m a => I_nonneg db r1 a m) (fun m a => I_le_one db r1 a m)
    (fun m z => I_nonneg db r2 m z) (fun m z => I_le_one db r2 m z)

private theorem path_013 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [0, 1, 3] b
      (fun x => I db r1 (x 0) (x 1) * I db r2 (x 3) (x 0)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun m => rsum n (fun a => rsum n (fun z =>
      I db r1 m a * I db r2 z m))) ≤
    (rsum n (fun x => rsum n (fun y => I db r1 x y))) *
      (rsum n (fun x => rsum n (fun y => I db r2 x y)))
  rw [show (rsum n (fun x => rsum n (fun y => I db r2 x y))) =
      rsum n (fun m => rsum n (fun z => I db r2 z m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 m a) (fun m z => I db r2 z m)
    (fun m a => I_nonneg db r1 m a) (fun m a => I_le_one db r1 m a)
    (fun m z => I_nonneg db r2 z m) (fun m z => I_le_one db r2 z m)

private theorem path_245 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [2, 4, 5] b
      (fun x => I db r1 (x 2) (x 4) * I db r2 (x 4) (x 5)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun a => rsum n (fun m => rsum n (fun z =>
      I db r1 a m * I db r2 m z))) ≤
    (rsum n (fun a => rsum n (fun m => I db r1 a m))) *
      (rsum n (fun m => rsum n (fun z => I db r2 m z)))
  rw [rsum_comm]
  rw [show (rsum n (fun a => rsum n (fun m => I db r1 a m))) =
      rsum n (fun m => rsum n (fun a => I db r1 a m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 a m) (fun m z => I db r2 m z)
    (fun m a => I_nonneg db r1 a m) (fun m a => I_le_one db r1 a m)
    (fun m z => I_nonneg db r2 m z) (fun m z => I_le_one db r2 m z)

private theorem path_124 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [1, 2, 4] b
      (fun x => I db r1 (x 1) (x 2) * I db r2 (x 2) (x 4)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun a => rsum n (fun m => rsum n (fun z =>
      I db r1 a m * I db r2 m z))) ≤
    (rsum n (fun a => rsum n (fun m => I db r1 a m))) *
      (rsum n (fun m => rsum n (fun z => I db r2 m z)))
  rw [rsum_comm]
  rw [show (rsum n (fun a => rsum n (fun m => I db r1 a m))) =
      rsum n (fun m => rsum n (fun a => I db r1 a m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 a m) (fun m z => I db r2 m z)
    (fun m a => I_nonneg db r1 a m) (fun m a => I_le_one db r1 a m)
    (fun m z => I_nonneg db r2 m z) (fun m z => I_le_one db r2 m z)

private theorem path_035 (n : Nat) (db : DB) (b : Bind) (r1 r2 : Nat) :
    integrate n [0, 3, 5] b
      (fun x => I db r1 (x 3) (x 0) * I db r2 (x 5) (x 3)) ≤
        S n db r1 * S n db r2 := by
  simp [integrate, put, S]
  change rsum n (fun a => rsum n (fun m => rsum n (fun z =>
      I db r1 m a * I db r2 z m))) ≤
    (rsum n (fun x => rsum n (fun y => I db r1 x y))) *
      (rsum n (fun x => rsum n (fun y => I db r2 x y)))
  rw [rsum_comm]
  rw [show (rsum n (fun x => rsum n (fun y => I db r1 x y))) =
      rsum n (fun m => rsum n (fun a => I db r1 m a)) by rw [rsum_comm]]
  rw [show (rsum n (fun x => rsum n (fun y => I db r2 x y))) =
      rsum n (fun m => rsum n (fun z => I db r2 z m)) by rw [rsum_comm]]
  exact rsum_path_le n (fun m a => I db r1 m a) (fun m z => I db r2 z m)
    (fun m a => I_nonneg db r1 m a) (fun m a => I_le_one db r1 m a)
    (fun m z => I_nonneg db r2 z m) (fun m z => I_le_one db r2 z m)

private theorem prod_nonneg_of_S (n : Nat) (db : DB) (r s : Nat) :
    0 ≤ S n db r * S n db s := Int.mul_nonneg (S_nonneg n db r) (S_nonneg n db s)

private theorem bound_025 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 2 := by
  rw [ref_eq_integrate]
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b
        (fun x => I db 0 (x 0) (x 1) * I db 0 (x 2) (x 3) * I db 2 (x 4) (x 5)) := by
          apply integrate_mono
          intro x
          dsimp [Fprod]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          exact prod7_le_keep3 (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = S n db 0 * S n db 0 * S n db 2 := by
          rw [integrate_perm n (by native_decide : fv.Perm [0,1,2,3,4,5]) b]
          exact integrate_pairs_canonical n db b 0 0 2

private theorem bound_046 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 1 * S n db 2 := by
  rw [ref_eq_integrate]
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b
        (fun x => I db 0 (x 0) (x 1) * I db 1 (x 2) (x 4) * I db 2 (x 5) (x 3)) := by
          apply integrate_mono
          intro x
          dsimp [Fprod]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          exact prod7_le_keep3_aeg (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = S n db 0 * S n db 1 * S n db 2 := by
          rw [integrate_perm n (by native_decide : fv.Perm [0,1,2,4,5,3]) b]
          exact pairF_046 n db b

private theorem bound_135 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 2 := by
  rw [ref_eq_integrate]
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b
        (fun x => I db 0 (x 1) (x 2) * I db 0 (x 3) (x 0) * I db 2 (x 4) (x 5)) := by
          apply integrate_mono
          intro x
          dsimp [Fprod]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          exact prod7_le_keep3_bdf (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = S n db 0 * S n db 0 * S n db 2 := by
          rw [integrate_perm n (by native_decide : fv.Perm [1,2,3,0,4,5]) b]
          exact pairF_135 n db b

private theorem bound_0156 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 2 * S n db 2 := by
  rw [ref_eq_integrate]
  let A : Bind → Int := fun x => I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2)
  let B : Bind → Int := fun x => I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3)
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b (fun x => A x * B x) := by
          apply integrate_mono
          intro x
          dsimp [Fprod, A, B]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          rw [show (I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2)) *
              (I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3)) =
              I db 0 (x 0) (x 1) * I db 0 (x 1) (x 2) * I db 2 (x 4) (x 5) * I db 2 (x 5) (x 3) by ac_rfl]
          exact prod7_le_keep4_abfg (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = integrate n [0,1,2,4,5,3] b (fun x => A x * B x) := by
          rw [integrate_perm n (by native_decide : fv.Perm [0,1,2,4,5,3]) b]
    _ = integrate n [0,1,2] b A * integrate n [4,5,3] b B := by
          dsimp [A, B]
          exact split_0156 n db b
    _ ≤ (S n db 0 * S n db 0) * (S n db 2 * S n db 2) := by
          apply Int.mul_le_mul
          · exact integrate_path_canonical n db b 0 0
          · exact path_453 n db b 2 2
          · apply integrate_nonneg
            intro x
            dsimp [B]
            exact Int.mul_nonneg (I_nonneg db 2 (x 4) (x 5)) (I_nonneg db 2 (x 5) (x 3))
          · exact Int.mul_nonneg (S_nonneg n db 0) (S_nonneg n db 0)
    _ = S n db 0 * S n db 0 * S n db 2 * S n db 2 := by ac_rfl

private theorem bound_0345 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 1 * S n db 2 := by
  rw [ref_eq_integrate]
  let A : Bind → Int := fun x => I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0)
  let B : Bind → Int := fun x => I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5)
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b (fun x => A x * B x) := by
          apply integrate_mono
          intro x
          dsimp [Fprod, A, B]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          rw [show (I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0)) *
              (I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5)) =
              I db 0 (x 0) (x 1) * I db 0 (x 3) (x 0) * I db 1 (x 2) (x 4) * I db 2 (x 4) (x 5) by ac_rfl]
          exact prod7_le_keep4_adef (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = integrate n [0,1,3,2,4,5] b (fun x => A x * B x) := by
          rw [integrate_perm n (by native_decide : fv.Perm [0,1,3,2,4,5]) b]
    _ = integrate n [0,1,3] b A * integrate n [2,4,5] b B := by
          dsimp [A, B]
          exact split_0345 n db b
    _ ≤ (S n db 0 * S n db 0) * (S n db 1 * S n db 2) := by
          apply Int.mul_le_mul
          · exact path_013 n db b 0 0
          · exact path_245 n db b 1 2
          · apply integrate_nonneg
            intro x
            dsimp [B]
            exact Int.mul_nonneg (I_nonneg db 1 (x 2) (x 4)) (I_nonneg db 2 (x 4) (x 5))
          · exact Int.mul_nonneg (S_nonneg n db 0) (S_nonneg n db 0)
    _ = S n db 0 * S n db 0 * S n db 1 * S n db 2 := by ac_rfl

private theorem bound_1346 (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b ≤
      S n db 0 * S n db 0 * S n db 1 * S n db 2 := by
  rw [ref_eq_integrate]
  let A : Bind → Int := fun x => I db 0 (x 1) (x 2) * I db 1 (x 2) (x 4)
  let B : Bind → Int := fun x => I db 0 (x 3) (x 0) * I db 2 (x 5) (x 3)
  calc
    integrate n fv b (Fprod db) ≤ integrate n fv b (fun x => A x * B x) := by
          apply integrate_mono
          intro x
          dsimp [Fprod, A, B]
          rcases I_all_nonneg_le_one db x with ⟨h1,h2,h3,h4,h5,h6,h7⟩
          rw [show (I db 0 (x 1) (x 2) * I db 1 (x 2) (x 4)) *
              (I db 0 (x 3) (x 0) * I db 2 (x 5) (x 3)) =
              I db 0 (x 1) (x 2) * I db 0 (x 3) (x 0) * I db 1 (x 2) (x 4) * I db 2 (x 5) (x 3) by ac_rfl]
          exact prod7_le_keep4_bdeg (I db 0 (x 0) (x 1)) (I db 0 (x 1) (x 2))
            (I db 0 (x 2) (x 3)) (I db 0 (x 3) (x 0)) (I db 1 (x 2) (x 4))
            (I db 2 (x 4) (x 5)) (I db 2 (x 5) (x 3)) h1.1 h1.2 h2.1 h2.2 h3.1 h3.2
            h4.1 h4.2 h5.1 h5.2 h6.1 h6.2 h7.1 h7.2
    _ = integrate n [1,2,4,0,3,5] b (fun x => A x * B x) := by
          rw [integrate_perm n (by native_decide : fv.Perm [1,2,4,0,3,5]) b]
    _ = integrate n [1,2,4] b A * integrate n [0,3,5] b B := by
          dsimp [A, B]
          exact split_1346 n db b
    _ ≤ (S n db 0 * S n db 1) * (S n db 0 * S n db 2) := by
          apply Int.mul_le_mul
          · exact path_124 n db b 0 1
          · exact path_035 n db b 0 2
          · apply integrate_nonneg
            intro x
            dsimp [B]
            exact Int.mul_nonneg (I_nonneg db 0 (x 3) (x 0)) (I_nonneg db 2 (x 5) (x 3))
          · exact Int.mul_nonneg (S_nonneg n db 0) (S_nonneg n db 1)
    _ = S n db 0 * S n db 0 * S n db 1 * S n db 2 := by ac_rfl


private def vcov7 (w0 w1 w2 w3 w4 w5 w6 d : Nat) : Prop :=
  d ≤ w0 + w3 ∧ d ≤ w0 + w1 ∧ d ≤ w1 + w2 + w4 ∧
    d ≤ w2 + w3 + w6 ∧ d ≤ w4 + w5 ∧ d ≤ w5 + w6

private theorem peel7 (w0 w1 w2 w3 w4 w5 w6 d : Nat) (hd : 0 < d)
    (hc : vcov7 w0 w1 w2 w3 w4 w5 w6 d) :
    (0 < w0 ∧ 0 < w2 ∧ 0 < w5 ∧ vcov7 (w0-1) w1 (w2-1) w3 w4 (w5-1) w6 (d-1)) ∨
    (0 < w0 ∧ 0 < w4 ∧ 0 < w6 ∧ vcov7 (w0-1) w1 w2 w3 (w4-1) w5 (w6-1) (d-1)) ∨
    (0 < w1 ∧ 0 < w3 ∧ 0 < w5 ∧ vcov7 w0 (w1-1) w2 (w3-1) w4 (w5-1) w6 (d-1)) ∨
    (0 < w0 ∧ 0 < w1 ∧ 0 < w5 ∧ 0 < w6 ∧
      vcov7 (w0-1) (w1-1) w2 w3 w4 (w5-1) (w6-1) (d-1)) ∨
    (0 < w0 ∧ 0 < w3 ∧ 0 < w4 ∧ 0 < w5 ∧
      vcov7 (w0-1) w1 w2 (w3-1) (w4-1) (w5-1) w6 (d-1)) ∨
    (0 < w1 ∧ 0 < w3 ∧ 0 < w4 ∧ 0 < w6 ∧
      vcov7 w0 (w1-1) w2 (w3-1) (w4-1) w5 (w6-1) (d-1)) := by
  unfold vcov7 at hc ⊢
  by_cases h0 : w0 = 0 <;> by_cases h1 : w1 = 0 <;> by_cases h2 : w2 = 0 <;>
  by_cases h3 : w3 = 0 <;> by_cases h4 : w4 = 0 <;> by_cases h5 : w5 = 0 <;>
  by_cases h6 : w6 = 0
  all_goals
    first
    | exact Or.inl ⟨by omega, by omega, by omega, by omega⟩
    | exact Or.inr (Or.inl ⟨by omega, by omega, by omega, by omega⟩)
    | exact Or.inr (Or.inr (Or.inl ⟨by omega, by omega, by omega, by omega⟩))
    | exact Or.inr (Or.inr (Or.inr (Or.inl ⟨by omega, by omega, by omega, by omega, by omega⟩)))
    | exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inl ⟨by omega, by omega, by omega, by omega, by omega⟩))))
    | exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inr ⟨by omega, by omega, by omega, by omega, by omega⟩))))

private theorem N_zero_of_s_zero (N s0 s1 s2 s3 s4 s5 s6 : Nat)
    (h025 : N ≤ s0 * s2 * s5) (h046 : N ≤ s0 * s4 * s6)
    (h135 : N ≤ s1 * s3 * s5) (h0156 : N ≤ s0 * s1 * s5 * s6)
    (h0345 : N ≤ s0 * s3 * s4 * s5) (h1346 : N ≤ s1 * s3 * s4 * s6)
    (hz : s0 = 0 ∨ s1 = 0 ∨ s2 = 0 ∨ s3 = 0 ∨ s4 = 0 ∨ s5 = 0 ∨ s6 = 0) : N = 0 := by
  rcases hz with h0 | h1 | h2 | h3 | h4 | h5 | h6
  all_goals
    first
    | have hh := h025; simp [h0] at hh; omega
    | have hh := h135; simp [h1] at hh; omega
    | have hh := h025; simp [h2] at hh; omega
    | have hh := h135; simp [h3] at hh; omega
    | have hh := h046; simp [h4] at hh; omega
    | have hh := h025; simp [h5] at hh; omega
    | have hh := h046; simp [h6] at hh; omega

private theorem prod7_pos (s0 s1 s2 s3 s4 s5 s6 w0 w1 w2 w3 w4 w5 w6 : Nat)
    (h0 : 0 < s0) (h1 : 0 < s1) (h2 : 0 < s2) (h3 : 0 < s3)
    (h4 : 0 < s4) (h5 : 0 < s5) (h6 : 0 < s6) :
    0 < s0^w0 * s1^w1 * s2^w2 * s3^w3 * s4^w4 * s5^w5 * s6^w6 := by
  have p0 : 0 < s0^w0 := Nat.pow_pos h0
  have p1 : 0 < s1^w1 := Nat.pow_pos h1
  have p2 : 0 < s2^w2 := Nat.pow_pos h2
  have p3 : 0 < s3^w3 := Nat.pow_pos h3
  have p4 : 0 < s4^w4 := Nat.pow_pos h4
  have p5 : 0 < s5^w5 := Nat.pow_pos h5
  have p6 : 0 < s6^w6 := Nat.pow_pos h6
  exact Nat.mul_pos (Nat.mul_pos (Nat.mul_pos (Nat.mul_pos (Nat.mul_pos (Nat.mul_pos p0 p1) p2) p3) p4) p5) p6

private theorem frac_bound (N s0 s1 s2 s3 s4 s5 s6 : Nat)
    (hs0 : 0 < s0) (hs1 : 0 < s1) (hs2 : 0 < s2) (hs3 : 0 < s3)
    (hs4 : 0 < s4) (hs5 : 0 < s5) (hs6 : 0 < s6)
    (d w0 w1 w2 w3 w4 w5 w6 : Nat)
    (h025 : N ≤ s0 * s2 * s5) (h046 : N ≤ s0 * s4 * s6)
    (h135 : N ≤ s1 * s3 * s5) (h0156 : N ≤ s0 * s1 * s5 * s6)
    (h0345 : N ≤ s0 * s3 * s4 * s5) (h1346 : N ≤ s1 * s3 * s4 * s6)
    (hc : vcov7 w0 w1 w2 w3 w4 w5 w6 d) :
    N ^ d ≤ s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
  induction d using Nat.strongRecOn generalizing w0 w1 w2 w3 w4 w5 w6 with
  | ind d ih =>
      by_cases hd0 : d = 0
      · subst d
        simp
        have hp := prod7_pos s0 s1 s2 s3 s4 s5 s6 w0 w1 w2 w3 w4 w5 w6
          hs0 hs1 hs2 hs3 hs4 hs5 hs6
        exact Nat.succ_le_of_lt hp
      · have hd : 0 < d := by omega
        rcases peel7 w0 w1 w2 w3 w4 w5 w6 d hd hc with hp | hp | hp | hp | hp | hp
        · rcases hp with ⟨hw0, hw2, hw5, hc'⟩
          have hi := ih (d-1) (by omega) (w0-1) w1 (w2-1) w3 w4 (w5-1) w6 hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ (w0-1) * s1 ^ w1 * s2 ^ (w2-1) * s3 ^ w3 * s4 ^ w4 *
                s5 ^ (w5-1) * s6 ^ w6) * (s0 * s2 * s5) := Nat.mul_le_mul hi h025
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e0 : w0 - 1 + 1 = w0 := Nat.sub_add_cancel (by omega)
            have e2 : w2 - 1 + 1 = w2 := Nat.sub_add_cancel (by omega)
            have e5 : w5 - 1 + 1 = w5 := Nat.sub_add_cancel (by omega)
            rw [← e0, ← e2, ← e5]
            simp only [show (w0 - 1 + 1) - 1 = w0 - 1 by omega]
            simp only [show (w2 - 1 + 1) - 1 = w2 - 1 by omega]
            simp only [show (w5 - 1 + 1) - 1 = w5 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw0, hw4, hw6, hc'⟩
          have hi := ih (d-1) (by omega) (w0-1) w1 w2 w3 (w4-1) w5 (w6-1) hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ (w0-1) * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ (w4-1) * s5 ^ w5 *
                s6 ^ (w6-1)) * (s0 * s4 * s6) := Nat.mul_le_mul hi h046
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e0 : w0 - 1 + 1 = w0 := Nat.sub_add_cancel (by omega)
            have e4 : w4 - 1 + 1 = w4 := Nat.sub_add_cancel (by omega)
            have e6 : w6 - 1 + 1 = w6 := Nat.sub_add_cancel (by omega)
            rw [← e0, ← e4, ← e6]
            simp only [show (w0 - 1 + 1) - 1 = w0 - 1 by omega]
            simp only [show (w4 - 1 + 1) - 1 = w4 - 1 by omega]
            simp only [show (w6 - 1 + 1) - 1 = w6 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw1, hw3, hw5, hc'⟩
          have hi := ih (d-1) (by omega) w0 (w1-1) w2 (w3-1) w4 (w5-1) w6 hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ w0 * s1 ^ (w1-1) * s2 ^ w2 * s3 ^ (w3-1) * s4 ^ w4 *
                s5 ^ (w5-1) * s6 ^ w6) * (s1 * s3 * s5) := Nat.mul_le_mul hi h135
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e1 : w1 - 1 + 1 = w1 := Nat.sub_add_cancel (by omega)
            have e3 : w3 - 1 + 1 = w3 := Nat.sub_add_cancel (by omega)
            have e5 : w5 - 1 + 1 = w5 := Nat.sub_add_cancel (by omega)
            rw [← e1, ← e3, ← e5]
            simp only [show (w1 - 1 + 1) - 1 = w1 - 1 by omega]
            simp only [show (w3 - 1 + 1) - 1 = w3 - 1 by omega]
            simp only [show (w5 - 1 + 1) - 1 = w5 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw0, hw1, hw5, hw6, hc'⟩
          have hi := ih (d-1) (by omega) (w0-1) (w1-1) w2 w3 w4 (w5-1) (w6-1) hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ (w0-1) * s1 ^ (w1-1) * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 *
                s5 ^ (w5-1) * s6 ^ (w6-1)) * (s0 * s1 * s5 * s6) := Nat.mul_le_mul hi h0156
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e0 : w0 - 1 + 1 = w0 := Nat.sub_add_cancel (by omega)
            have e1 : w1 - 1 + 1 = w1 := Nat.sub_add_cancel (by omega)
            have e5 : w5 - 1 + 1 = w5 := Nat.sub_add_cancel (by omega)
            have e6 : w6 - 1 + 1 = w6 := Nat.sub_add_cancel (by omega)
            rw [← e0, ← e1, ← e5, ← e6]
            simp only [show (w0 - 1 + 1) - 1 = w0 - 1 by omega]
            simp only [show (w1 - 1 + 1) - 1 = w1 - 1 by omega]
            simp only [show (w5 - 1 + 1) - 1 = w5 - 1 by omega]
            simp only [show (w6 - 1 + 1) - 1 = w6 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw0, hw3, hw4, hw5, hc'⟩
          have hi := ih (d-1) (by omega) (w0-1) w1 w2 (w3-1) (w4-1) (w5-1) w6 hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ (w0-1) * s1 ^ w1 * s2 ^ w2 * s3 ^ (w3-1) * s4 ^ (w4-1) *
                s5 ^ (w5-1) * s6 ^ w6) * (s0 * s3 * s4 * s5) := Nat.mul_le_mul hi h0345
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e0 : w0 - 1 + 1 = w0 := Nat.sub_add_cancel (by omega)
            have e3 : w3 - 1 + 1 = w3 := Nat.sub_add_cancel (by omega)
            have e4 : w4 - 1 + 1 = w4 := Nat.sub_add_cancel (by omega)
            have e5 : w5 - 1 + 1 = w5 := Nat.sub_add_cancel (by omega)
            rw [← e0, ← e3, ← e4, ← e5]
            simp only [show (w0 - 1 + 1) - 1 = w0 - 1 by omega]
            simp only [show (w3 - 1 + 1) - 1 = w3 - 1 by omega]
            simp only [show (w4 - 1 + 1) - 1 = w4 - 1 by omega]
            simp only [show (w5 - 1 + 1) - 1 = w5 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl
        · rcases hp with ⟨hw1, hw3, hw4, hw6, hc'⟩
          have hi := ih (d-1) (by omega) w0 (w1-1) w2 (w3-1) (w4-1) w5 (w6-1) hc'
          calc
          N ^ d = N ^ (d - 1 + 1) := by congr 1; omega
          _ = N ^ (d-1) * N := by rw [Nat.pow_succ]
          _ ≤ (s0 ^ w0 * s1 ^ (w1-1) * s2 ^ w2 * s3 ^ (w3-1) * s4 ^ (w4-1) *
                s5 ^ w5 * s6 ^ (w6-1)) * (s1 * s3 * s4 * s6) := Nat.mul_le_mul hi h1346
          _ = s0 ^ w0 * s1 ^ w1 * s2 ^ w2 * s3 ^ w3 * s4 ^ w4 * s5 ^ w5 * s6 ^ w6 := by
            have e1 : w1 - 1 + 1 = w1 := Nat.sub_add_cancel (by omega)
            have e3 : w3 - 1 + 1 = w3 := Nat.sub_add_cancel (by omega)
            have e4 : w4 - 1 + 1 = w4 := Nat.sub_add_cancel (by omega)
            have e6 : w6 - 1 + 1 = w6 := Nat.sub_add_cancel (by omega)
            rw [← e1, ← e3, ← e4, ← e6]
            simp only [show (w1 - 1 + 1) - 1 = w1 - 1 by omega]
            simp only [show (w3 - 1 + 1) - 1 = w3 - 1 by omega]
            simp only [show (w4 - 1 + 1) - 1 = w4 - 1 by omega]
            simp only [show (w6 - 1 + 1) - 1 = w6 - 1 by omega]
            simp only [Nat.pow_succ]
            ac_rfl


theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.IntersectingCycles.FractionalCap.Goal := by
  intro n db b weights den hcover
  rcases hcover with ⟨hden, hwlen, hcov⟩
  cases weights with
  | nil => simp [pattern] at hwlen
  | cons w0 ws1 =>
    cases ws1 with
    | nil => simp [pattern] at hwlen
    | cons w1 ws2 =>
      cases ws2 with
      | nil => simp [pattern] at hwlen
      | cons w2 ws3 =>
        cases ws3 with
        | nil => simp [pattern] at hwlen
        | cons w3 ws4 =>
          cases ws4 with
          | nil => simp [pattern] at hwlen
          | cons w4 ws5 =>
            cases ws5 with
            | nil => simp [pattern] at hwlen
            | cons w5 ws6 =>
              cases ws6 with
              | nil => simp [pattern] at hwlen
              | cons w6 ws7 =>
                cases ws7 with
                | nil =>
                    let N : Nat := (reference pattern n
                      (fun r x y => if db r x y == 0 then 0 else 1) b).toNat
                    let s0 := support n db ⟨0,0,1⟩ pattern b
                    let s1 := support n db ⟨0,1,2⟩ pattern b
                    let s2 := support n db ⟨0,2,3⟩ pattern b
                    let s3 := support n db ⟨0,3,0⟩ pattern b
                    let s4 := support n db ⟨1,2,4⟩ pattern b
                    let s5 := support n db ⟨2,4,5⟩ pattern b
                    let s6 := support n db ⟨2,5,3⟩ pattern b
                    have hNint : (N : Int) = reference pattern n
                        (fun r x y => if db r x y == 0 then 0 else 1) b := by
                      dsimp [N]
                      exact Int.toNat_of_nonneg (ref_nonneg n db b)
                    have hs0 : (s0 : Int) = S n db 0 := by
                      dsimp [s0]; apply support_eq_S <;> native_decide
                    have hs1 : (s1 : Int) = S n db 0 := by
                      dsimp [s1]; apply support_eq_S <;> native_decide
                    have hs2 : (s2 : Int) = S n db 0 := by
                      dsimp [s2]; apply support_eq_S <;> native_decide
                    have hs3 : (s3 : Int) = S n db 0 := by
                      dsimp [s3]; apply support_eq_S <;> native_decide
                    have hs4 : (s4 : Int) = S n db 1 := by
                      dsimp [s4]; apply support_eq_S <;> native_decide
                    have hs5 : (s5 : Int) = S n db 2 := by
                      dsimp [s5]; apply support_eq_S <;> native_decide
                    have hs6 : (s6 : Int) = S n db 2 := by
                      dsimp [s6]; apply support_eq_S <;> native_decide
                    have h025 : N ≤ s0 * s2 * s5 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs0, hs2, hs5]
                      simpa using bound_025 n db b
                    have h046 : N ≤ s0 * s4 * s6 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs0, hs4, hs6]
                      simpa using bound_046 n db b
                    have h135 : N ≤ s1 * s3 * s5 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs1, hs3, hs5]
                      simpa using bound_135 n db b
                    have h0156 : N ≤ s0 * s1 * s5 * s6 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs0, hs1, hs5, hs6]
                      simpa using bound_0156 n db b
                    have h0345 : N ≤ s0 * s3 * s4 * s5 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs0, hs3, hs4, hs5]
                      simpa using bound_0345 n db b
                    have h1346 : N ≤ s1 * s3 * s4 * s6 := by
                      apply Int.ofNat_le.mp
                      rw [hNint]
                      simp only [Int.natCast_mul]
                      rw [hs1, hs3, hs4, hs6]
                      simpa using bound_1346 n db b
                    have hvc : vcov7 w0 w1 w2 w3 w4 w5 w6 den := by
                      simp [pattern] at hcov
                      rcases hcov with ⟨hv0, hv1, hv2, hv3, hv4, hv5⟩
                      exact ⟨by omega, by omega, by omega, by omega, by omega, by omega⟩
                    have hNdef : (reference pattern n
                        (fun r x y => if db r x y == 0 then 0 else 1) b).toNat = N := rfl
                    rw [hNdef]
                    by_cases hz : s0 = 0 ∨ s1 = 0 ∨ s2 = 0 ∨ s3 = 0 ∨ s4 = 0 ∨ s5 = 0 ∨ s6 = 0
                    · have hN0 := N_zero_of_s_zero N s0 s1 s2 s3 s4 s5 s6
                        h025 h046 h135 h0156 h0345 h1346 hz
                      rw [hN0]
                      have hzpow : (0 : Nat) ^ den = 0 := by
                        cases den with
                        | zero => omega
                        | succ k => simp
                      rw [hzpow]
                      exact Nat.zero_le _
                    · have hs0p : 0 < s0 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inl h))
                      have hs1p : 0 < s1 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inl h)))
                      have hs2p : 0 < s2 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inl h))))
                      have hs3p : 0 < s3 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inl h)))))
                      have hs4p : 0 < s4 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl h))))))
                      have hs5p : 0 < s5 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl h)))))))
                      have hs6p : 0 < s6 := Nat.pos_of_ne_zero (by intro h; exact hz (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr h)))))))
                      simpa [s0, s1, s2, s3, s4, s5, s6, pattern, Nat.mul_assoc] using
                        (frac_bound N s0 s1 s2 s3 s4 s5 s6 hs0p hs1p hs2p hs3p hs4p hs5p hs6p den w0 w1 w2 w3 w4 w5 w6
                          h025 h046 h135 h0156 h0345 h1346 hvc)
                | cons w7 _ => simp [pattern] at hwlen

/-! ## CollisionCorrection: Möbius inversion on equality partitions. -/
end TinylambOpt.ResearchProofs.Round2.Task80
-- END jpb-copy Task80

-- END jpb-copies

/-! ## Remaining goals (to be filled) -/

theorem sound_CountryReturn_FractionalCap : CountryReturn.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task72.sound_FractionalCap

theorem sound_OrderSupplier_FractionalCap : OrderSupplier.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.OrderSupplier.Remaining.sound_FractionalCap

theorem sound_ReturnedItem_FractionalCap : ReturnedItem.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ReturnedItem.Remaining.sound_FractionalCap

theorem sound_ChannelBridge_FractionalCap : ChannelBridge.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.ChannelBridge.Remaining.sound_FractionalCap

theorem sound_PromoInventory_FractionalCap : PromoInventory.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task76.sound_FractionalCap

theorem sound_PromoInventory_DeltaForecast : PromoInventory.DeltaForecast.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventory.Remaining.sound_DeltaForecast

theorem sound_TwoHopAntiWitness_FractionalCap : TwoHopAntiWitness.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.TwoHopAntiWitness.Remaining.sound_FractionalCap

theorem sound_HubWithChords_FractionalCap : HubWithChords.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.HubWithChords.Remaining.sound_FractionalCap

theorem sound_HubWithChords_CollisionCorrection : HubWithChords.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task78.sound_CollisionCorrection

theorem sound_SupplierClique_OrbitRepresentative : SupplierClique.OrbitRepresentative.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task79.sound_OrbitRepresentative

theorem sound_IntersectingCycles_FractionalCap : IntersectingCycles.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task80.sound_FractionalCap

theorem sound_IntersectingCycles_CollisionCorrection : IntersectingCycles.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task78IC.sound_CollisionCorrection

theorem sound_ParallelLineage_FractionalCap : ParallelLineage.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task81.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.JoinPlans_WorkB
