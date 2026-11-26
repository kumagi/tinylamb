import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `SupplierClique.OrbitRepresentative` — `∀ n db b, (∀ i < n, ∀ j < n, db 0 i j = db 0
  j i) → injective pattern n db b = 24*integrate n pattern.freeVars b (fun x => if x
  0 < x 1 && x 1 < x 2 && x 2 < x 3 then product (factors pattern db) x else 0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_SupplierClique_OrbitRepresentative

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.SupplierClique

/-! Orbit counting by induction on the domain bound. `R` is a `List.range` sum;
`inj₄` counts all distinct quadruples and `inc₄` only strictly decreasing ones.
A fully symmetric weight has exactly 4! orderings per unordered quadruple. -/

def R : Nat → (Nat → Int) → Int
  | 0, _ => 0
  | n+1, g => R n g + g n

theorem R_congr {n : Nat} {g h : Nat → Int} (H : ∀ i, i < n → g i = h i) : R n g = R n h := by
  induction n with
  | zero => rfl
  | succ n ih => simp only [R]; rw [ih (fun i hi => H i (by omega)), H n (by omega)]

@[congr] theorem R_congr' {n m : Nat} {g h : Nat → Int} (e : n = m)
    (H : ∀ i, i < n → g i = h i) : R n g = R m h := by subst e; exact R_congr H

theorem R_add (n : Nat) (g h : Nat → Int) : R n (fun i => g i + h i) = R n g + R n h := by
  induction n with
  | zero => rfl
  | succ n ih => simp only [R, ih]; omega

theorem R_mul (n : Nat) (c : Int) (g : Nat → Int) : R n (fun i => c * g i) = c * R n g := by
  induction n with
  | zero => simp [R]
  | succ n ih => simp only [R, ih, Int.mul_add]

theorem R_zero (n : Nat) : R n (fun _ => 0) = 0 := by
  induction n with
  | zero => rfl
  | succ n ih => simp only [R, ih]; rfl

theorem R_const_ite (n : Nat) (p : Prop) [Decidable p] (g : Nat → Int) :
    R n (fun i => if p then g i else 0) = if p then R n g else 0 := by
  by_cases h : p <;> simp [h, R_zero]

def inj2 (n : Nat) (f : Nat → Nat → Int) := R n fun a => R n fun b => if a ≠ b then f a b else 0
def inc2 (n : Nat) (f : Nat → Nat → Int) := R n fun a => R n fun b => if b < a then f a b else 0

theorem lvl2 (f : Nat → Nat → Int) (hs : ∀ a b, f a b = f b a) (n : Nat) :
    inj2 n f = 2 * inc2 n f := by
  induction n with
  | zero => simp [inj2, inc2, R]
  | succ n ih =>
    simp only [inj2, inc2] at ih ⊢
    simp only [R]
    simp (config := {contextual := true}) [R_add, Nat.ne_of_lt, Nat.ne_of_gt, Nat.lt_irrefl,
      show ∀ a, a < n → ¬ n < a from fun a h => by omega]
    simp only [ne_eq, ite_not] at ih
    have e : (R n fun i => f i n) = R n fun i => f n i := R_congr (fun i _ => hs i n)
    omega

def inj3 (n : Nat) (f : Nat → Nat → Nat → Int) :=
  R n fun a => R n fun b => R n fun c => if a ≠ b ∧ a ≠ c ∧ b ≠ c then f a b c else 0
def inc3 (n : Nat) (f : Nat → Nat → Nat → Int) :=
  R n fun a => R n fun b => R n fun c => if c < b ∧ b < a then f a b c else 0

theorem lvl3 (f : Nat → Nat → Nat → Int) (h1 : ∀ a b c, f a b c = f b a c)
    (h2 : ∀ a b c, f a b c = f a c b) (n : Nat) : inj3 n f = 6 * inc3 n f := by
  induction n with
  | zero => simp [inj3, inc3, R]
  | succ n ih =>
    simp only [inj3, inc3, ne_eq] at ih ⊢
    simp only [R]
    simp (config := {contextual := true}) [R_add, R_zero, Nat.ne_of_lt, Nat.ne_of_gt, Nat.lt_irrefl,
      show ∀ a, a < n → ¬ n < a from fun a h => by omega]
    have L := lvl2 (fun a b => f n a b) (fun a b => h2 n a b) n
    simp only [inj2, inc2, ne_eq] at L
    have t1 : (R n fun i => R n fun j => if i = j then 0 else f i j n)
        = R n fun a => R n fun b => if ¬ a = b then f n a b else 0 := by
      apply R_congr; intro i _; apply R_congr; intro j _
      by_cases hij : i = j <;> simp [hij]
      rw [h2 i j n, h1 i n j]
    have t2 : (R n fun i => R n fun j => if i = j then 0 else f i n j)
        = R n fun a => R n fun b => if ¬ a = b then f n a b else 0 := by
      apply R_congr; intro i _; apply R_congr; intro j _
      by_cases hij : i = j <;> simp [hij]
      rw [h1 i n j]
    have t3 : (R n fun i => R n fun j => if i = j then 0 else f n i j)
        = R n fun a => R n fun b => if ¬ a = b then f n a b else 0 := by
      apply R_congr; intro i _; apply R_congr; intro j _
      by_cases hij : i = j <;> simp [hij]
    omega

def inj4 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :=
  R n fun a => R n fun b => R n fun c => R n fun d =>
    if a ≠ b ∧ a ≠ c ∧ a ≠ d ∧ b ≠ c ∧ b ≠ d ∧ c ≠ d then f a b c d else 0
def inc4 (n : Nat) (f : Nat → Nat → Nat → Nat → Int) :=
  R n fun a => R n fun b => R n fun c => R n fun d => if d < c ∧ c < b ∧ b < a then f a b c d else 0

theorem lvl4 (f : Nat → Nat → Nat → Nat → Int) (h1 : ∀ a b c d, f a b c d = f b a c d)
    (h2 : ∀ a b c d, f a b c d = f a c b d) (h3 : ∀ a b c d, f a b c d = f a b d c) (n : Nat) :
    inj4 n f = 24 * inc4 n f := by
  induction n with
  | zero => simp [inj4, inc4, R]
  | succ n ih =>
    simp only [inj4, inc4, ne_eq] at ih ⊢
    simp only [R]
    simp (config := {contextual := true}) [R_add, R_zero, Nat.ne_of_lt, Nat.ne_of_gt, Nat.lt_irrefl,
      show ∀ a, a < n → ¬ n < a from fun a h => by omega]
    have L := lvl3 (fun a b c => f n a b c) (fun a b c => h2 n a b c) (fun a b c => h3 n a b c) n
    simp only [inj3, inc3, ne_eq] at L
    have key : ∀ (u : Nat → Nat → Nat → Int), (∀ a b c, u a b c = f n a b c) →
        (R n fun i => R n fun j => R n fun k => if ¬i = j ∧ ¬i = k ∧ ¬j = k then u i j k else 0)
        = R n fun a => R n fun b => R n fun c => if ¬a = b ∧ ¬a = c ∧ ¬b = c then f n a b c else 0 := by
      intro u hu
      apply R_congr; intro i _; apply R_congr; intro j _; apply R_congr; intro k _
      rw [hu]
    have t1 := key (fun i j k => f i j k n) (fun a b c => by
      show f a b c n = f n a b c
      rw [h3 a b c n, h2 a b n c, h1 a n b c])
    have t2 := key (fun i j k => f i j n k) (fun a b c => by
      show f a b n c = f n a b c
      rw [h2 a b n c, h1 a n b c])
    have t3 := key (fun i j k => f i n j k) (fun a b c => by
      show f a n b c = f n a b c
      rw [h1 a n b c])
    omega

theorem sum_eq_R (n : Nat) (g : Nat → Int) : ((List.range n).map g).sum = R n g := by
  induction n with
  | zero => rfl
  | succ n ih => simp [List.range_succ, R, ih]

theorem integrate4 (n : Nat) (b : Bind) (F : Bind → Int) :
    integrate n [3,2,1,0] b F = R n fun w => R n fun x => R n fun y => R n fun z =>
      F (put (put (put (put b 3 w) 2 x) 1 y) 0 z) := by
  simp [integrate, sum_eq_R]

def S (n : Nat) (db : DB) (i j : Nat) : Int := if i < n ∧ j < n then db 0 i j else 0
def F4 (n : Nat) (db : DB) (a b c d : Nat) : Int :=
  S n db d c * (S n db d b * (S n db d a * (S n db c b * (S n db c a * (S n db b a * 1)))))

theorem sound_SupplierClique_OrbitRepresentative : SupplierClique.OrbitRepresentative.Goal := by
  intro n db b hsym
  have hS : ∀ i j, S n db i j = S n db j i := by
    intro i j; unfold S
    by_cases h : i < n ∧ j < n
    · have h' : j < n ∧ i < n := ⟨h.2, h.1⟩
      simp only [h, and_self, ↓reduceIte]; exact hsym i h.1 j h.2
    · have h' : ¬ (j < n ∧ i < n) := fun h' => h ⟨h'.2, h'.1⟩
      simp only [h, h', ↓reduceIte]
  have h1 : ∀ a b c d, F4 n db a b c d = F4 n db b a c d := by
    intro a b c d; unfold F4; rw [hS a b]; ac_rfl
  have h2 : ∀ a b c d, F4 n db a b c d = F4 n db a c b d := by
    intro a b c d; unfold F4; rw [hS b c]; ac_rfl
  have h3 : ∀ a b c d, F4 n db a b c d = F4 n db a b d c := by
    intro a b c d; unfold F4; rw [hS c d]; ac_rfl
  have L := lvl4 (F4 n db) h1 h2 h3 n
  have hpr : ∀ x : Bind, product (factors pattern db) x =
      (pattern.atoms.map (fun a => value db a x)).prod := by
    intro x; simp [product, factors, List.map_map, Function.comp_def]
  have hp : ∀ w x y z, w < n → x < n → y < n → z < n →
      (pattern.atoms.map (fun a => value db a (put (put (put (put b 3 w) 2 x) 1 y) 0 z))).prod
        = F4 n db w x y z := by
    intro w x y z hw hx hy hz
    simp [pattern, value, put, F4, S, hw, hx, hy, hz]
  have e1 : injective pattern n db b = inj4 n (F4 n db) := by
    unfold injective inj4
    rw [show pattern.freeVars = [3,2,1,0] from rfl, integrate4]
    apply R_congr; intro w hw; apply R_congr; intro x hx; apply R_congr; intro y hy
    apply R_congr; intro z hz
    have := hp w x y z hw hx hy hz
    simp only [put, List.map_cons, List.map_nil, List.nodup_cons, List.mem_cons,
      List.not_mem_nil, or_false]
    simp at *
    split <;> split <;> first | exact this | rfl | omega
  have e2 : integrate n pattern.freeVars b (fun x =>
      if x 0 < x 1 && x 1 < x 2 && x 2 < x 3 then product (factors pattern db) x else 0)
      = inc4 n (F4 n db) := by
    unfold inc4
    rw [show pattern.freeVars = [3,2,1,0] from rfl, integrate4]
    apply R_congr; intro w hw; apply R_congr; intro x hx; apply R_congr; intro y hy
    apply R_congr; intro z hz
    have := hp w x y z hw hx hy hz
    rw [hpr]
    simp [put]
    split <;> split <;> first | exact this | rfl | omega
  rw [e1, e2, L]

end TinylambOpt.ResearchProofs.Atlas.Goal_SupplierClique_OrbitRepresentative
