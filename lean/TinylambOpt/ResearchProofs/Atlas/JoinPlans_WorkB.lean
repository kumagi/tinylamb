import TinylambOpt.ResearchGoals.Atlas.JoinPlans
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_CountryReturn_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ParallelLineage_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_PromoInventory_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_SupplierClique_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_TwoHopAntiWitness_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_OrderSupplier_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ChannelBridge_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ReturnedItem_Remaining
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_HubWithChords_Remaining
import TinylambOpt.ResearchProofs.Atlas.Goal_CountryReturn_FractionalCap
import TinylambOpt.ResearchProofs.Atlas.Goal_SupplierClique_OrbitRepresentative
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_ParallelLineage_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas_JoinPlans_PromoInventory_Remaining_Round2
import TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_FractionalCap
import TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J5

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
    simp_all [List.Nodup, List.mem_cons, List.mem_nil_iff, beq_iff_eq] <;> try omega

-- BEGIN jpb-helpers
private theorem jpb_ite_pull (n : Nat) (P : Prop) [inst : Decidable P] (g : Nat → Int) :
    rsum n (fun i => if P then g i else 0) = if P then rsum n g else 0 := by
  by_cases hP : P <;> simp [hP, rsum_const_zero]

private theorem jpb_range_ite (n : Nat) (g : Nat → Int) :
    rsum n (fun a => if a < n then g a else 0) = rsum n g := by
  apply rsum_congr; intro a ha; exact if_pos ha

private theorem jpb_rsum_ite_flip (n a : Nat) (g : Nat → Int) :
    rsum n (fun b => if b = a then g b else 0) = if a < n then g a else 0 := by
  have h : (fun b => if b = a then g b else 0) = (fun b => if a = b then g b else 0) := by
    funext b
    by_cases hab : b = a
    · subst hab; simp
    · rw [if_neg hab, if_neg (fun hh => hab hh.symm)]
  rw [h, rsum_ite_single]

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
      simp_all <;> (try omega)

private theorem jpb_body_rw (n : Nat) (φ ψ : Nat → Nat → Nat → Nat → Int)
    (h : ∀ a b c d, φ a b c d = ψ a b c d) :
    rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => φ a b c d)))) =
      rsum n (fun a => rsum n (fun b => rsum n (fun c => rsum n (fun d => ψ a b c d)))) := by
  refine rsum_congr n (fun a _ => ?_)
  refine rsum_congr n (fun b _ => ?_)
  refine rsum_congr n (fun c _ => ?_)
  refine rsum_congr n (fun d _ => ?_)
  exact h a b c d

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

private theorem hc1 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1], [2], [3], [4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => F db bnd a c d e)))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc2 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1, 2], [3], [4]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => rsum n (fun e => F db bnd a a d e))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc3 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[2], [1, 3], [4]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => rsum n (fun e => F db bnd a c a e))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc4 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[2], [3], [1, 4]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun a => F db bnd a c d a))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc5 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1], [2, 3], [4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun e => F db bnd a c c e))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc6 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1, 2, 3], [4]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db bnd a a a e)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc7 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[2, 3], [1, 4]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db bnd a c c a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc8 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1], [3], [2, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => rsum n (fun c => F db bnd a c d c))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc9 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1, 3], [2, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db bnd a c a c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc10 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[3], [1, 2, 4]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => F db bnd a a d a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc11 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1], [2], [3, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun d => F db bnd a c d d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc12 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1, 2], [3, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db bnd a a d d)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc13 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[2], [1, 3, 4]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db bnd a c a a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc14 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1], [2, 3, 4]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db bnd a c c c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc15 (n : Nat) (db : DB) (bnd : Bind) : collapsed pattern n db bnd ([[1, 2, 3, 4]] : List (List Nat)) = rsum n (fun a => F db bnd a a a a) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hinj (n : Nat) (db : DB) (bnd : Bind) : injective pattern n db bnd = rsum n (fun a => rsum n (fun c => rsum n (fun d => rsum n (fun e => if [a, c, d, e].Nodup then F db bnd a c d e else 0)))) := by
  unfold injective
  simp [pattern, integrate, value, put, rsum, F, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, beq_iff_eq, ite_true, ite_false, reduceIte]

private theorem hpoint (db : DB) (bnd : Bind) (a c d e : Nat) :
    (if [a,c,d,e].Nodup then F db bnd a c d e else 0) =
      F db bnd a c d e
      - (if c == a then F db bnd a c d e else 0)
      - (if d == a then F db bnd a c d e else 0)
      - (if e == a then F db bnd a c d e else 0)
      - (if d == c then F db bnd a c d e else 0)
      - (if e == c then F db bnd a c d e else 0)
      - (if e == d then F db bnd a c d e else 0)
      + (if d == c && e == a then F db bnd a c d e else 0)
      + (if d == a && e == c then F db bnd a c d e else 0)
      + (if c == a && e == d then F db bnd a c d e else 0)
      + 2 * (if c == a && d == a then F db bnd a c d e else 0)
      + 2 * (if d == c && e == c then F db bnd a c d e else 0)
      + 2 * (if d == a && e == a then F db bnd a c d e else 0)
      + 2 * (if c == a && e == a then F db bnd a c d e else 0)
      - 6 * (if c == a && d == a && e == a then F db bnd a c d e else 0) := by
  by_cases hca : c = a <;> by_cases hda : d = a <;> by_cases hea : e = a <;>
    by_cases hdc : d = c <;> by_cases hec : e = c <;> by_cases hed : e = d <;>
    simp_all [List.Nodup, List.mem_cons, List.mem_nil_iff, beq_iff_eq] <;> try omega

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15, List.Nodup, List.mem_cons]
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
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [4], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => rsum n (fun d => F db b a a e d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1, 4], [3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => rsum n (fun d => F db b a c a d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [4], [1, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => rsum n (fun a => F db b a c e a))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2, 4], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun d => F db b a c c d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2, 4], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db b a a a d)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 4], [1, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b a c c a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [4], [2, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => rsum n (fun c => F db b a c e c))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 4], [2, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a c a c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1, 2, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => F db b a a e a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun e => F db b a c e e))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db b a a e e)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1, 4, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b a c a a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2, 4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => F db b a c c c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2, 4, 3]] : List (List Nat)) = rsum n (fun a => F db b a a a a) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun a => rsum n (fun c => rsum n (fun e => rsum n (fun d => if [a, c, e, d].Nodup then F db b a c e d else 0)))) := by
  unfold injective
  simp [pattern, integrate, value, put, rsum, F, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, beq_iff_eq, ite_true, ite_false, reduceIte]

private theorem hpoint (db : DB) (b : Bind) (a c e d : Nat) :
    (if [a,c,e,d].Nodup then F db b a c e d else 0) =
      F db b a c e d
      - (if c == a then F db b a c e d else 0)
      - (if e == a then F db b a c e d else 0)
      - (if d == a then F db b a c e d else 0)
      - (if e == c then F db b a c e d else 0)
      - (if d == c then F db b a c e d else 0)
      - (if d == e then F db b a c e d else 0)
      + (if e == c && d == a then F db b a c e d else 0)
      + (if e == a && d == c then F db b a c e d else 0)
      + (if c == a && d == e then F db b a c e d else 0)
      + 2 * (if c == a && e == a then F db b a c e d else 0)
      + 2 * (if e == c && d == c then F db b a c e d else 0)
      + 2 * (if e == a && d == a then F db b a c e d else 0)
      + 2 * (if c == a && d == a then F db b a c e d else 0)
      - 6 * (if c == a && e == a && d == a then F db b a c e d else 0) := by
  by_cases hca : c = a <;> by_cases hea : e = a <;> by_cases hda : d = a <;>
    by_cases hec : e = c <;> by_cases hdc : d = c <;> by_cases hde : d = e <;>
    simp_all [List.Nodup, List.mem_cons, List.mem_nil_iff, beq_iff_eq] <;> try omega

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15, List.Nodup, List.mem_cons]
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
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3], [4], [1]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => rsum n (fun a => F db b c c e a))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2, 4], [1]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => rsum n (fun a => F db b c d c a))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [4], [2, 1]] : List (List Nat)) = rsum n (fun d => rsum n (fun e => rsum n (fun c => F db b c d e c))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3, 4], [1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun a => F db b c d d a))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3, 4], [1]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => F db b c c c a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 4], [2, 1]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => F db b c d d c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [4], [3, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => rsum n (fun d => F db b c d e d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 4], [3, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => F db b c d c d)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [2, 3, 1]] : List (List Nat)) = rsum n (fun e => rsum n (fun c => F db b c c e c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3], [4, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun e => F db b c d e e))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3], [4, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => F db b c c e e)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2, 4, 1]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => F db b c d c c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3, 4, 1]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => F db b c d d d)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 3, 4, 1]] : List (List Nat)) = rsum n (fun c => F db b c c c c) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun c => rsum n (fun d => rsum n (fun e => rsum n (fun a => if [c, d, e, a].Nodup then F db b c d e a else 0)))) := by
  unfold injective
  simp [pattern, integrate, value, put, rsum, F, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, beq_iff_eq, ite_true, ite_false, reduceIte]

private theorem hpoint (db : DB) (b : Bind) (c d e a : Nat) :
    (if [c,d,e,a].Nodup then F db b c d e a else 0) =
      F db b c d e a
      - (if d == c then F db b c d e a else 0)
      - (if e == c then F db b c d e a else 0)
      - (if a == c then F db b c d e a else 0)
      - (if e == d then F db b c d e a else 0)
      - (if a == d then F db b c d e a else 0)
      - (if a == e then F db b c d e a else 0)
      + (if e == d && a == c then F db b c d e a else 0)
      + (if e == c && a == d then F db b c d e a else 0)
      + (if d == c && a == e then F db b c d e a else 0)
      + 2 * (if d == c && e == c then F db b c d e a else 0)
      + 2 * (if e == d && a == d then F db b c d e a else 0)
      + 2 * (if e == c && a == c then F db b c d e a else 0)
      + 2 * (if d == c && a == c then F db b c d e a else 0)
      - 6 * (if d == c && e == c && a == c then F db b c d e a else 0) := by
  by_cases hdc : d = c <;> by_cases hec : e = c <;> by_cases hac : a = c <;>
    by_cases hed : e = d <;> by_cases had : a = d <;> by_cases hae : a = e <;>
    simp_all [List.Nodup, List.mem_cons, List.mem_nil_iff, beq_iff_eq] <;> try omega

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15, List.Nodup, List.mem_cons]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end ChannelBridge_CC

theorem sound_ChannelBridge_CollisionCorrection : ChannelBridge.CollisionCorrection.Goal := ChannelBridge_CC.sound_CollisionCorrection
namespace SupplierClique_CC
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique
private def F (db : DB) (b : Bind) (d c a x0 : Nat) : Int := db 0 x0 a * (db 0 x0 c * (db 0 x0 d * (db 0 a c * (db 0 a d * db 0 c d))))
private theorem parts_eq : partitions pattern.freeVars = [[[3], [2], [1], [0]], [[3, 2], [1], [0]], [[2], [3, 1], [0]], [[2], [1], [3, 0]], [[3], [2, 1], [0]], [[3, 2, 1], [0]], [[2, 1], [3, 0]], [[3], [1], [2, 0]], [[3, 1], [2, 0]], [[1], [3, 2, 0]], [[3], [2], [1, 0]], [[3, 2], [1, 0]], [[2], [3, 1, 0]], [[3], [2, 1, 0]], [[3, 2, 1, 0]]] := by native_decide

private theorem hc1 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2], [1], [0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => rsum n (fun a => rsum n (fun x0 => F db b d c a x0)))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 2], [1], [0]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => rsum n (fun x0 => F db b d d a x0))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3, 1], [0]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => rsum n (fun x0 => F db b d c d x0))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [1], [3, 0]] : List (List Nat)) = rsum n (fun c => rsum n (fun a => rsum n (fun d => F db b d c a d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2, 1], [0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => rsum n (fun x0 => F db b d c c x0))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 2, 1], [0]] : List (List Nat)) = rsum n (fun d => rsum n (fun x0 => F db b d d d x0)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2, 1], [3, 0]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => F db b d c c d)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [1], [2, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => rsum n (fun c => F db b d c a c))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 1], [2, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => F db b d c d c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [3, 2, 0]] : List (List Nat)) = rsum n (fun a => rsum n (fun d => F db b d d a d)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2], [1, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => rsum n (fun a => F db b d c a a))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 2], [1, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun a => F db b d d a a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [3, 1, 0]] : List (List Nat)) = rsum n (fun c => rsum n (fun d => F db b d c d d)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3], [2, 1, 0]] : List (List Nat)) = rsum n (fun d => rsum n (fun c => F db b d c c c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[3, 2, 1, 0]] : List (List Nat)) = rsum n (fun d => F db b d d d d) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun d => rsum n (fun c => rsum n (fun a => rsum n (fun x0 => if [d, c, a, x0].Nodup then F db b d c a x0 else 0)))) := by
  unfold injective
  simp [pattern, integrate, value, put, rsum, F, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, beq_iff_eq, ite_true, ite_false, reduceIte]

private theorem hpoint (db : DB) (b : Bind) (d c a x0 : Nat) :
    (if [d,c,a,x0].Nodup then F db b d c a x0 else 0) =
      F db b d c a x0
      - (if c == d then F db b d c a x0 else 0)
      - (if a == d then F db b d c a x0 else 0)
      - (if x0 == d then F db b d c a x0 else 0)
      - (if a == c then F db b d c a x0 else 0)
      - (if x0 == c then F db b d c a x0 else 0)
      - (if x0 == a then F db b d c a x0 else 0)
      + (if a == c && x0 == d then F db b d c a x0 else 0)
      + (if a == d && x0 == c then F db b d c a x0 else 0)
      + (if c == d && x0 == a then F db b d c a x0 else 0)
      + 2 * (if c == d && a == d then F db b d c a x0 else 0)
      + 2 * (if a == c && x0 == c then F db b d c a x0 else 0)
      + 2 * (if a == d && x0 == d then F db b d c a x0 else 0)
      + 2 * (if c == d && x0 == d then F db b d c a x0 else 0)
      - 6 * (if c == d && a == d && x0 == d then F db b d c a x0 else 0) := by
  by_cases hcd : c = d <;> by_cases had : a = d <;> by_cases hx0d : x0 = d <;>
    by_cases hac : a = c <;> by_cases hx0c : x0 = c <;> by_cases hx0a : x0 = a <;>
    simp_all [List.Nodup, List.mem_cons, List.mem_nil_iff, beq_iff_eq] <;> try omega

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15, List.Nodup, List.mem_cons]
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
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc2 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 1], [2], [3]] : List (List Nat)) = rsum n (fun e => rsum n (fun c => rsum n (fun d => F db b e e c d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc3 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [4, 2], [3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => rsum n (fun d => F db b e a e d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc4 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [2], [4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun c => rsum n (fun e => F db b e a c e))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc5 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1, 2], [3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => rsum n (fun d => F db b e a a d))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc6 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 1, 2], [3]] : List (List Nat)) = rsum n (fun e => rsum n (fun d => F db b e e e d)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc7 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1, 2], [4, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db b e a a e)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc8 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [2], [1, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun c => rsum n (fun a => F db b e a c a))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc9 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 2], [1, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => F db b e a e a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc10 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[2], [4, 1, 3]] : List (List Nat)) = rsum n (fun c => rsum n (fun e => F db b e e c e)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc11 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1], [2, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => rsum n (fun c => F db b e a c c))) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc12 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 1], [2, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun c => F db b e e c c)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc13 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[1], [4, 2, 3]] : List (List Nat)) = rsum n (fun a => rsum n (fun e => F db b e a e e)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc14 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4], [1, 2, 3]] : List (List Nat)) = rsum n (fun e => rsum n (fun a => F db b e a a a)) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hc15 (n : Nat) (db : DB) (b : Bind) : collapsed pattern n db b ([[4, 1, 2, 3]] : List (List Nat)) = rsum n (fun e => F db b e e e e) := by
  unfold collapsed
  simp [pattern, integrate, value, put, rsum, F, List.filterMap, List.head?, List.find?,
    List.mem_cons, List.mem_nil_iff, List.contains_cons, List.contains_nil,
    beq_iff_eq, ite_true, ite_false, reduceIte, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, Option.getD_some, Option.bind_some, Option.bind_none, Option.getD_none]
private theorem hinj (n : Nat) (db : DB) (b : Bind) : injective pattern n db b = rsum n (fun e => rsum n (fun a => rsum n (fun c => rsum n (fun d => if [e, a, c, d].Nodup then F db b e a c d else 0)))) := by
  unfold injective
  simp [pattern, integrate, value, put, rsum, F, List.map_cons, List.map_nil,
    List.prod_cons, List.prod_nil, beq_iff_eq, ite_true, ite_false, reduceIte]

private theorem hpoint (db : DB) (b : Bind) (e a c d : Nat) :
    (if [e,a,c,d].Nodup then F db b e a c d else 0) =
      F db b e a c d
      - (if a == e then F db b e a c d else 0)
      - (if c == e then F db b e a c d else 0)
      - (if d == e then F db b e a c d else 0)
      - (if c == a then F db b e a c d else 0)
      - (if d == a then F db b e a c d else 0)
      - (if d == c then F db b e a c d else 0)
      + (if c == a && d == e then F db b e a c d else 0)
      + (if c == e && d == a then F db b e a c d else 0)
      + (if a == e && d == c then F db b e a c d else 0)
      + 2 * (if a == e && c == e then F db b e a c d else 0)
      + 2 * (if c == a && d == a then F db b e a c d else 0)
      + 2 * (if c == e && d == e then F db b e a c d else 0)
      + 2 * (if a == e && d == e then F db b e a c d else 0)
      - 6 * (if a == e && c == e && d == e then F db b e a c d else 0) := by
  by_cases hae : a = e <;> by_cases hce : c = e <;> by_cases hde : d = e <;>
    by_cases hca : c = a <;> by_cases hda : d = a <;> by_cases hdc : d = c <;>
    simp_all [List.Nodup, List.mem_cons, List.mem_nil_iff, beq_iff_eq] <;> try omega

theorem sound_CollisionCorrection : CollisionCorrection.Goal := by
  intro n db b
  rw [hinj, parts_eq]
  simp [mobius, factorial, hc1, hc2, hc3, hc4, hc5, hc6, hc7, hc8, hc9, hc10, hc11, hc12, hc13, hc14, hc15, List.Nodup, List.mem_cons]
  refine Eq.trans (jpb_quad n (F db b)) ?_
  rw [← jpb_sw3 n (F db b), ← jpb_sw4 n (F db b), ← jpb_sw7 n (F db b), ← jpb_sw8 n (F db b),
    ← jpb_sw10 n (F db b), ← jpb_sw13 n (F db b)]
  omega
end ParallelLineage_CC

theorem sound_ParallelLineage_CollisionCorrection : ParallelLineage.CollisionCorrection.Goal := ParallelLineage_CC.sound_CollisionCorrection

theorem sound_PromoInventory_CollisionCorrection : PromoInventory.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.PromoInventory.Remaining.sound_CollisionCorrection

theorem sound_TwoHopAntiWitness_CollisionCorrection : TwoHopAntiWitness.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.JoinPlans.TwoHopAntiWitness.Remaining.sound_CollisionCorrection


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


/-! ## Remaining goals -/

theorem sound_CountryReturn_FractionalCap : CountryReturn.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Goal_CountryReturn_FractionalCap.sound_CountryReturn_FractionalCap

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
  TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J5.HubWithChords_CollisionCorrection

theorem sound_SupplierClique_OrbitRepresentative : SupplierClique.OrbitRepresentative.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Goal_SupplierClique_OrbitRepresentative.sound_SupplierClique_OrbitRepresentative

theorem sound_IntersectingCycles_FractionalCap : IntersectingCycles.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Goal_IntersectingCycles_FractionalCap.sound_IntersectingCycles_FractionalCap

theorem sound_IntersectingCycles_CollisionCorrection : IntersectingCycles.CollisionCorrection.Goal :=
  TinylambOpt.ResearchProofs.Atlas.Auto_JoinPlans_J5.IntersectingCycles_CollisionCorrection

theorem sound_ParallelLineage_FractionalCap : ParallelLineage.FractionalCap.Goal :=
  TinylambOpt.ResearchProofs.Round2.Task81.sound_FractionalCap

end TinylambOpt.ResearchProofs.Atlas.JoinPlans_WorkB

