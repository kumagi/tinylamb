import TinylambOpt.ThompsonFragment

/-!
Packing a boolean function into a BitVec, with bit-level readback.

Reference NFA: finite states 0..states-1, epsilon and labeled edges.

Named results include `packNat_testBit`, `pack_getLsbD`, `pack_high_zero`.
-/

/-
  TinylambOpt/RegexBitsetNfa.lean

  Bitset execution of a small byte regex NFA, proved against a reference NFA.

  Two independent levels:
    (a) reference NFA (`NFA`) with explicit epsilon and labeled edges.  A state
        set is a predicate `SSet = Nat -> Bool` observed only on `q < states`.
        `epsClose` is the stabilization of `epsFwd` after `states` steps.
        Termination is proved by a cardinality argument: the iteration is
        monotone, stays inside `range states`, and any strict step raises the
        cardinality, so at most `states` strict steps are possible.  This also
        covers the epsilon cycles created by `star`.
    (b) bitset machine: `BitVec W` with `states <= W`; bits at `>= states` are
        always zero.  `epsStepBits`, `epsCloseBits`, `transBits`, `stepBits`,
        `searchStepBits` are direct bitwise implementations.

  The main theorem is a genuine set-vs-bitset connection: `fromBits` of the
  machine state after every byte equals the reference reachability set with
  epsilon closure.  Existence search injects the start state (and its epsilon
  closure) at EVERY position; the counterexample theorem shows that injecting
  it only once loses `(ab|b)c` on `xbc`.  `a*` accepts the empty input.

  Model limits: byte semantics only, no captures, no flags, no anchors, and
  state count bounded by the word width W.  C++ realization: the small regex
  compiler and the bit-parallel NFA step in `expression/jit.cpp`.
-/

namespace TinylambOpt.RegexBitsetNfa


/-- Packing a boolean function into a BitVec, with bit-level readback. -/
def packNat (f : Nat → Bool) : Nat → Nat
  | 0 => 0
  | W + 1 => (if f W then 2 ^ W else 0) ||| packNat f W

theorem packNat_testBit (f : Nat → Bool) (W i : Nat) :
    (packNat f W).testBit i = if i < W then f i else false := by
  induction W with
  | zero => simp [packNat]
  | succ W ih =>
      simp only [packNat]
      rw [Nat.testBit_or, ih]
      by_cases hfW : f W
      · simp only [hfW, ↓reduceIte]
        by_cases hiW : i < W
        · have hpow : (2 ^ W).testBit i = false := by
            simp [Nat.testBit_two_pow, show W ≠ i by omega]
          rw [hpow, Bool.false_or, if_pos hiW, if_pos (show i < W + 1 by omega)]
        · by_cases hiEq : i = W
          · subst i
            rw [if_neg (by omega : ¬ W < W), if_pos (by omega : W < W + 1)]
            have hp : (2 ^ W).testBit W = true := by simp
            rw [hp, Bool.true_or, hfW]
          · have hpow : (2 ^ W).testBit i = false := by
              simp [Nat.testBit_two_pow, show W ≠ i by omega]
            rw [hpow, Bool.false_or, if_neg hiW, if_neg (show ¬ i < W + 1 by omega)]
      · simp only [hfW]
        rw [if_neg (by decide : ¬ (false = true)), Nat.zero_testBit, Bool.false_or]
        by_cases hiW : i < W
        · rw [if_pos hiW, if_pos (show i < W + 1 by omega)]
        · by_cases hiEq : i = W
          · subst i
            rw [if_pos (by omega : W < W + 1)]
            simp [hfW]
          · rw [if_neg hiW, if_neg (show ¬ i < W + 1 by omega)]

theorem pack_getLsbD (W i : Nat) (hi : i < W) (f : Nat → Bool) :
    (BitVec.ofNat W (packNat f W)).getLsbD i = f i := by
  rw [BitVec.getLsbD_ofNat, packNat_testBit]
  simp [hi]

theorem pack_high_zero (W i : Nat) (hi : W ≤ i) (f : Nat → Bool) :
    (BitVec.ofNat W (packNat f W)).getLsbD i = false := by
  rw [BitVec.getLsbD_ofNat]
  simp [show ¬ i < W by omega]

/-- Reference NFA: finite states 0..states-1, epsilon and labeled edges. -/
structure NFA where
  states : Nat
  start : Nat
  eps : Nat → List Nat
  trans : Nat → Byte → List Nat
  accept : Nat → Bool

/-- A state set is a predicate on states; only q < N.states is observable. -/
abbrev SSet := Nat → Bool

def startSet (N : NFA) : SSet := fun q => q = N.start && N.start < N.states

def anyMono {α} (l : List α) (f g : α → Bool) (h : ∀ x ∈ l, f x = true → g x = true) :
    l.any f = true → l.any g = true := by
  intro hf
  rw [List.any_eq_true] at hf ⊢
  rcases hf with ⟨x, hx, hfx⟩
  exact ⟨x, hx, h x hx hfx⟩

/-- One epsilon-closure step: add all epsilon successors, keeping in range. -/
def epsStep (N : NFA) (S : SSet) (i : Nat) : Bool :=
  if i < N.states then
    S i || (List.range N.states).any (fun q => S q && i ∈ N.eps q)
  else false

def epsFwd (N : NFA) (S : SSet) : Nat → SSet
  | 0, i => if i < N.states then S i else false
  | t + 1, i => epsStep N (epsFwd N S t) i

def setCard (N : NFA) (f : SSet) : Nat := ((List.range N.states).filter f).length

theorem filter_length_mono (l : List Nat) (f g : Nat → Bool)
    (h : ∀ x ∈ l, f x = true → g x = true) :
    (l.filter f).length ≤ (l.filter g).length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hf : f x = true
      · have hg : g x = true := h x (by simp) hf
        simp [hf, hg]
        apply ih
        intro y hy hyf
        exact h y (by simp [hy]) hyf
      · by_cases hg : g x = true
        · simp [hf, hg]
          have hih := ih (by intro y hy hyf; exact h y (by simp [hy]) hyf)
          omega
        · simp [hf, hg]
          apply ih
          intro y hy hyf
          exact h y (by simp [hy]) hyf

theorem filter_length_lt_of_witness (l : List Nat) (f g : Nat → Bool) (q : Nat)
    (hq : q ∈ l) (hfq : f q = false) (hgq : g q = true)
    (h : ∀ x ∈ l, f x = true → g x = true) :
    (l.filter f).length < (l.filter g).length := by
  induction l generalizing q with
  | nil => simp at hq
  | cons x xs ih =>
      by_cases hqx : x = q
      · subst x
        simp [hfq, hgq]
        have hle := filter_length_mono xs f g (by
          intro y hy hyf
          exact h y (by simp [hy]) hyf)
        omega
      · have hqxs : q ∈ xs := by simpa [List.mem_cons, Ne.symm hqx] using hq
        by_cases hfx : f x = true
        · have hgx : g x = true := h x (by simp) hfx
          simp [hfx, hgx]
          exact ih q hqxs hfq hgq (by
            intro y hy hyf
            exact h y (by simp [hy]) hyf)
        · by_cases hgx : g x = true
          · simp [hfx, hgx]
            have hlt := ih q hqxs hfq hgq (by
              intro y hy hyf
              exact h y (by simp [hy]) hyf)
            omega
          · simp [hfx, hgx]
            exact ih q hqxs hfq hgq (by
              intro y hy hyf
              exact h y (by simp [hy]) hyf)

theorem epsFwd_in_range (N S t q) (h : epsFwd N S t q = true) : q < N.states := by
  cases t with
  | zero =>
      by_cases hq : q < N.states
      · exact hq
      · simp [epsFwd, hq] at h
  | succ u =>
      unfold epsFwd epsStep at h
      by_cases hq : q < N.states
      · exact hq
      · rw [if_neg hq] at h
        contradiction

theorem epsFwd_mono_step (N S t q) (h : epsFwd N S t q = true) :
    epsFwd N S (t + 1) q = true := by
  unfold epsFwd
  unfold epsStep
  by_cases hq : q < N.states
  · rw [if_pos hq]
    simp [h, anyMono]
  · rw [if_neg hq]
    have hfalse : epsFwd N S t q = false := by
      cases t with
      | zero => simp [epsFwd, hq]
      | succ u => simp [epsFwd, epsStep, hq]
    simp [hfalse] at h

theorem epsFwd_mono (N S t u) (h : t ≤ u) (q : Nat)
    (hq : epsFwd N S t q = true) : epsFwd N S u q = true := by
  obtain ⟨d, hd⟩ := Nat.exists_eq_add_of_le h
  subst u
  clear h
  induction d with
  | zero => simpa using hq
  | succ d ih => exact epsFwd_mono_step N S (t + d) q ih

theorem setCard_le_states (N f) : setCard N f ≤ N.states := by
  unfold setCard
  simpa using List.length_filter_le f (List.range N.states)

theorem epsFwd_strict_card (N S t)
    (hne : epsFwd N S t ≠ epsFwd N S (t + 1)) :
    setCard N (epsFwd N S t) + 1 ≤ setCard N (epsFwd N S (t + 1)) := by
  have hle : ∀ q, epsFwd N S t q = true → epsFwd N S (t + 1) q = true :=
    fun q hq => epsFwd_mono N S t (t + 1) (by omega) q hq
  have hex : ∃ q, epsFwd N S t q ≠ epsFwd N S (t + 1) q := by
    by_cases hx : ∃ q, epsFwd N S t q ≠ epsFwd N S (t + 1) q
    · exact hx
    · exfalso
      apply hne
      funext q
      by_cases hq : epsFwd N S t q = epsFwd N S (t + 1) q
      · exact hq
      · exact False.elim (hx ⟨q, hq⟩)
  rcases hex with ⟨q, hneq⟩
  have hqt : epsFwd N S t q = false ∧ epsFwd N S (t + 1) q = true := by
    cases htf : epsFwd N S t q <;> cases htg : epsFwd N S (t + 1) q <;> simp_all
  have hqrange : q < N.states := by
    by_cases hqr : q < N.states
    · exact hqr
    · exfalso
      have hf : epsFwd N S (t + 1) q = false := by
        cases t with
        | zero =>
            unfold epsFwd epsStep
            rw [if_neg hqr]
        | succ u =>
            change (if q < N.states then _ else false) = false
            rw [if_neg hqr]
      simp [hf] at hqt
  unfold setCard
  have hlt := filter_length_lt_of_witness (List.range N.states)
    (epsFwd N S t) (epsFwd N S (t + 1)) q (List.mem_range.mpr hqrange)
    hqt.1 hqt.2 (by
      intro x hx hfx
      exact hle x hfx)
  omega

theorem epsFwd_eq_of_eq (N S u) (h : epsFwd N S u = epsFwd N S (u + 1)) :
    ∀ d, epsFwd N S (u + d) = epsFwd N S u := by
  intro d
  induction d with
  | zero => rfl
  | succ d ih =>
      change epsStep N (epsFwd N S (u + d)) = epsFwd N S u
      rw [ih]
      have hstep : epsFwd N S (u + 1) = epsStep N (epsFwd N S u) := rfl
      rw [← hstep]
      exact h.symm

theorem epsFwd_card_growth (N S) : ∀ t,
    (∀ u, u ≤ t → epsFwd N S u ≠ epsFwd N S (u + 1)) →
    setCard N (epsFwd N S 0) + (t + 1) ≤ setCard N (epsFwd N S (t + 1)) := by
  intro t
  induction t with
  | zero =>
      intro h
      have hlt := epsFwd_strict_card N S 0 (h 0 (by omega))
      omega
  | succ t ih =>
      intro h
      have hprev := ih (by intro u hu; exact h u (by omega))
      have hlt := epsFwd_strict_card N S (t + 1) (h (t + 1) (by omega))
      omega

theorem epsFwd_stable (N S) :
    epsFwd N S (N.states + 1) = epsFwd N S N.states := by
  by_cases hstable : epsFwd N S (N.states + 1) = epsFwd N S N.states
  · exact hstable
  · exfalso
    have hne := hstable
    have hall : ∀ u, u ≤ N.states → epsFwd N S u ≠ epsFwd N S (u + 1) := by
      intro u hu heq
      have hn := epsFwd_eq_of_eq N S u heq (N.states - u)
      have hn1 := epsFwd_eq_of_eq N S u heq (N.states + 1 - u)
      have hu1 : u + (N.states - u) = N.states := by omega
      have hu2 : u + (N.states + 1 - u) = N.states + 1 := by omega
      rw [hu1] at hn
      rw [hu2] at hn1
      exact hne (hn1.trans hn.symm)
    have hg := epsFwd_card_growth N S N.states hall
    have hb := setCard_le_states N (epsFwd N S (N.states + 1))
    omega

def epsClose (N : NFA) (S : SSet) : SSet := epsFwd N S N.states

theorem epsClose_contains (N S q) (hq : q < N.states) (h : S q = true) :
    epsClose N S q = true := by
  unfold epsClose
  exact epsFwd_mono N S 0 N.states (by omega) q (by simpa [epsFwd, hq] using h)

theorem epsClose_closed (N S q r) (hq : epsClose N S q = true)
    (hr : r ∈ N.eps q) (hrange : r < N.states) : epsClose N S r = true := by
  unfold epsClose at hq ⊢
  have hqrange : q < N.states := by
    exact epsFwd_in_range N S N.states q hq
  have hnext : epsFwd N S (N.states + 1) r = true := by
    unfold epsFwd epsStep
    rw [if_pos hrange]
    by_cases hleft : epsFwd N S N.states r = true
    · simp [hleft]
    · simp [hleft, List.any_eq_true]
      exact ⟨q, hqrange, hq, hr⟩
  rw [epsFwd_stable N S] at hnext
  exact hnext

theorem epsClose_least (N : NFA) (S T : SSet)
    (hS : ∀ q, q < N.states → S q = true → T q = true)
    (hT : ∀ q, q < N.states → T q = true → ∀ r ∈ N.eps q, r < N.states → T r = true) :
    ∀ q, epsClose N S q = true → T q = true := by
  unfold epsClose
  intro q hq
  have hiter : ∀ t r, epsFwd N S t r = true → T r = true := by
    intro t
    induction t with
    | zero =>
        intro r hr
        have hrange : r < N.states := by
          by_cases hb : r < N.states
          · exact hb
          · simp [epsFwd, hb] at hr
        apply hS r hrange
        simpa [epsFwd, hrange] using hr
    | succ t ih =>
        intro r hr
        unfold epsFwd epsStep at hr
        by_cases hrange : r < N.states
        · rw [if_pos hrange] at hr
          rw [Bool.or_eq_true] at hr
          cases hr with
          | inl hr => exact ih r hr
          | inr hr =>
              rw [List.any_eq_true] at hr
              rcases hr with ⟨u, hu, hru⟩
              rw [Bool.and_eq_true] at hru
              rcases hru with ⟨hru', hir⟩
              apply hT u
              · exact List.mem_range.mp hu
              · exact ih u hru'
              · simpa using hir
              · exact hrange
        · rw [if_neg hrange] at hr
          contradiction
  exact hiter N.states q hq

/-- Boolean predicate -> BitVec (bits >= n are zero). -/
def toBits (n W : Nat) (S : SSet) : BitVec W :=
  BitVec.ofNat W (packNat (fun i => if i < n then S i else false) W)

/-- BitVec -> predicate restricted to the first n bits. -/
def fromBits (n W : Nat) (x : BitVec W) : SSet :=
  fun i => decide (i < n) && x.getLsbD i

theorem toBits_get (n W i : Nat) (hi : i < W) (hin : i < n) (S : SSet) :
    (toBits n W S).getLsbD i = S i := by
  unfold toBits
  rw [pack_getLsbD W i hi]
  simp [hin]

theorem toBits_high_zero (n W i : Nat) (hi : W ≤ i) (S : SSet) :
    (toBits n W S).getLsbD i = false := by
  unfold toBits
  exact pack_high_zero W i hi _

theorem toBits_zero_above (n W i : Nat) (hi : i < W) (hn : n ≤ i) (S : SSet) :
    (toBits n W S).getLsbD i = false := by
  unfold toBits
  rw [pack_getLsbD W i hi]
  simp [show ¬ i < n by omega]

theorem fromBits_toBits (n W : Nat) (hNW : n ≤ W) (S : SSet) (i : Nat) (hi : i < n) :
    fromBits n W (toBits n W S) i = S i := by
  unfold fromBits
  rw [toBits_get n W i (by omega) hi S]
  simp [hi]

theorem toBits_fromBits (n W : Nat) (hNW : n ≤ W) (x : BitVec W)
    (hx : ∀ i, n ≤ i → x.getLsbD i = false) :
    toBits n W (fromBits n W x) = x := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  unfold toBits fromBits
  rw [pack_getLsbD W i hi]
  by_cases hin : i < n
  · simp [hin]
  · have hxi := hx i (by omega)
    simp [hin, hxi]

theorem or_toBits (n W : Nat) (hNW : n ≤ W) (S T : SSet) :
    toBits n W S ||| toBits n W T = toBits n W (fun i => S i || T i) := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  rw [BitVec.getLsbD_or]
  by_cases hin : i < n
  · rw [toBits_get n W i hi hin, toBits_get n W i hi hin, toBits_get n W i hi hin]
  · rw [toBits_zero_above n W i hi (by omega), toBits_zero_above n W i hi (by omega),
        toBits_zero_above n W i hi (by omega)]
    simp

/-- Bitset implementation of one epsilon-closure step. -/
def epsStepBits (N : NFA) (W : Nat) (x : BitVec W) : BitVec W :=
  BitVec.ofNat W (packNat (fun i =>
    if _hi : i < W then
      x.getLsbD i || (List.range N.states).any (fun q =>
        x.getLsbD q && decide (i < N.states) && decide (i ∈ N.eps q))
    else false) W)

theorem epsStepBits_get (N : NFA) (W i : Nat) (x : BitVec W) :
    (epsStepBits N W x).getLsbD i =
      if hi : i < W then
        x.getLsbD i || (List.range N.states).any (fun q =>
          x.getLsbD q && decide (i < N.states) && decide (i ∈ N.eps q))
      else false := by
  unfold epsStepBits
  by_cases hi : i < W
  · rw [pack_getLsbD W i hi, dif_pos hi]
  · rw [pack_high_zero W i (by omega), dif_neg hi]

theorem epsStepBits_toBits (N : NFA) (W : Nat) (hNW : N.states ≤ W)
    (S : SSet) (i : Nat) :
    (epsStepBits N W (toBits N.states W S)).getLsbD i =
      if i < W then epsStep N S i else false := by
  rw [epsStepBits_get]
  by_cases hi : i < W
  · rw [dif_pos hi, if_pos hi]
    unfold epsStep
    by_cases hin : i < N.states
    · rw [if_pos hin]
      have hto : (toBits N.states W S).getLsbD i = S i :=
        toBits_get N.states W i hi hin S
      rw [hto]
      have hany :
          (List.range N.states).any (fun q =>
            (toBits N.states W S).getLsbD q && decide (i < N.states) && decide (i ∈ N.eps q)) =
          (List.range N.states).any (fun q => S q && decide (i ∈ N.eps q)) := by
        apply Bool.eq_iff_iff.mpr
        constructor <;> intro h
        · rw [List.any_eq_true] at h ⊢
          rcases h with ⟨q, hq, hqb⟩
          have hq' : q < N.states := List.mem_range.mp hq
          rw [toBits_get N.states W q (by omega) hq' S] at hqb
          simp only [Bool.and_eq_true] at hqb
          rcases hqb with ⟨⟨hSq, _⟩, hmem⟩
          exact ⟨q, hq, by simp only [Bool.and_eq_true]; exact ⟨hSq, by simpa using hmem⟩⟩
        · rw [List.any_eq_true] at h ⊢
          rcases h with ⟨q, hq, hqb⟩
          have hq' : q < N.states := List.mem_range.mp hq
          rw [Bool.and_eq_true] at hqb
          rcases hqb with ⟨hSq, hmem⟩
          have hmemProp : i ∈ N.eps q := by simpa using hmem
          refine ⟨q, hq, ?_⟩
          rw [toBits_get N.states W q (by omega) hq' S]
          simp [hin, hSq, hmemProp]
      rw [hany]
    · rw [if_neg hin]
      have hzero : (List.range N.states).any (fun q =>
            (toBits N.states W S).getLsbD q && decide (i < N.states) && decide (i ∈ N.eps q)) = false := by
        apply Bool.eq_false_iff.mpr
        intro h
        rw [List.any_eq_true] at h
        rcases h with ⟨q, hq, hqb⟩
        rw [Bool.and_eq_true] at hqb
        rw [Bool.and_eq_true] at hqb
        rcases hqb with ⟨left, _⟩
        simp [hin] at left
      have hto : (toBits N.states W S).getLsbD i = false := by
        unfold toBits
        rw [pack_getLsbD W i hi]
        simp [hin]
      simp [hto, hzero]
  · simp [hi]

theorem epsStepBits_toBits_bv (N : NFA) (W : Nat) (hNW : N.states ≤ W) (S : SSet) :
    epsStepBits N W (toBits N.states W S) = toBits N.states W (epsStep N S) := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  rw [epsStepBits_toBits N W hNW S i, if_pos hi]
  by_cases hin : i < N.states
  · rw [toBits_get N.states W i hi hin]
  · rw [toBits_zero_above N.states W i hi (by omega)]
    unfold epsStep
    rw [if_neg hin]

def epsCloseBits (N : NFA) (W : Nat) : Nat → BitVec W → BitVec W
  | 0, x => x
  | t + 1, x => epsStepBits N W (epsCloseBits N W t x)

theorem epsCloseBits_eq (N : NFA) (W : Nat) (hNW : N.states ≤ W)
    (S : SSet) (t : Nat) :
    epsCloseBits N W t (toBits N.states W S) = toBits N.states W (epsFwd N S t) := by
  induction t generalizing S with
  | zero =>
      apply BitVec.eq_of_getLsbD_eq
      intro i hi
      change (toBits N.states W S).getLsbD i = (toBits N.states W (epsFwd N S 0)).getLsbD i
      by_cases hin : i < N.states
      · have hef : epsFwd N S 0 i = S i := by simp [epsFwd, hin]
        rw [toBits_get N.states W i hi hin, toBits_get N.states W i hi hin]
        rw [hef]
      · rw [toBits_zero_above N.states W i hi (by omega),
            toBits_zero_above N.states W i hi (by omega)]
  | succ t ih =>
      unfold epsCloseBits
      rw [ih]
      rw [epsStepBits_toBits_bv N W hNW (epsFwd N S t)]
      rfl

theorem epsCloseBits_epsClose (N : NFA) (W : Nat) (hNW : N.states ≤ W) (S : SSet) :
    epsCloseBits N W N.states (toBits N.states W S) =
      toBits N.states W (epsClose N S) := by
  unfold epsClose
  exact epsCloseBits_eq N W hNW S N.states

/-- Labeled transition, set semantics. -/
def transStep (N : NFA) (S : SSet) (b : Byte) (i : Nat) : Bool :=
  if i < N.states then
    (List.range N.states).any (fun q => S q && i ∈ N.trans q b)
  else false

def transBits (N : NFA) (W : Nat) (x : BitVec W) (b : Byte) : BitVec W :=
  BitVec.ofNat W (packNat (fun i =>
    if _hi : i < W then
      (List.range N.states).any (fun q =>
        x.getLsbD q && decide (i < N.states) && decide (i ∈ N.trans q b))
    else false) W)

theorem transBits_toBits (N : NFA) (W : Nat) (hNW : N.states ≤ W) (S : SSet) (b : Byte) :
    transBits N W (toBits N.states W S) b = toBits N.states W (transStep N S b) := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  unfold transBits toBits transStep
  rw [pack_getLsbD W i hi, pack_getLsbD W i hi]
  by_cases hin : i < N.states
  · rw [dif_pos hi, if_pos hin, if_pos hin]
    have hany :
        (List.range N.states).any (fun q =>
          (BitVec.ofNat W (packNat (fun i => if i < N.states then S i else false) W)).getLsbD q &&
            decide (i < N.states) && decide (i ∈ N.trans q b)) =
        (List.range N.states).any (fun q => S q && decide (i ∈ N.trans q b)) := by
      apply Bool.eq_iff_iff.mpr
      constructor <;> intro h
      · rw [List.any_eq_true] at h ⊢
        rcases h with ⟨q, hq, hqb⟩
        have hq' : q < N.states := List.mem_range.mp hq
        rw [pack_getLsbD W q (by omega)] at hqb
        simp [hq'] at hqb
        rcases hqb with ⟨⟨hSq, _⟩, hmem⟩
        refine ⟨q, hq, ?_⟩
        simp [hSq, hmem]
      · rw [List.any_eq_true] at h ⊢
        rcases h with ⟨q, hq, hqb⟩
        have hq' : q < N.states := List.mem_range.mp hq
        rw [Bool.and_eq_true] at hqb
        rcases hqb with ⟨hSq, hmem⟩
        refine ⟨q, hq, ?_⟩
        rw [pack_getLsbD W q (by omega)]
        simp [hq', hin, hSq, hmem]
    rw [hany]
  · rw [dif_pos hi]
    simp [hin]

def reachStep (N : NFA) (S : SSet) (b : Byte) : SSet := epsClose N (transStep N S b)

def stepBits (N : NFA) (W : Nat) (x : BitVec W) (b : Byte) : BitVec W :=
  epsCloseBits N W N.states (transBits N W x b)

theorem stepBits_spec (N : NFA) (W : Nat) (hNW : N.states ≤ W) (S : SSet) (b : Byte) :
    stepBits N W (toBits N.states W S) b = toBits N.states W (reachStep N S b) := by
  unfold stepBits reachStep
  rw [transBits_toBits N W hNW S b]
  exact epsCloseBits_epsClose N W hNW (transStep N S b)

/-- Unanchored search: inject the start state before every byte. -/
def startBits (N : NFA) (W : Nat) : BitVec W := toBits N.states W (startSet N)

def searchStep (N : NFA) (S : SSet) (b : Byte) : SSet :=
  reachStep N (epsClose N (fun q => S q || startSet N q)) b

def searchStepBits (N : NFA) (W : Nat) (x : BitVec W) (b : Byte) : BitVec W :=
  stepBits N W (epsCloseBits N W N.states (x ||| startBits N W)) b

theorem searchStepBits_spec (N : NFA) (W : Nat) (hNW : N.states ≤ W)
    (S : SSet) (b : Byte) :
    searchStepBits N W (toBits N.states W S) b =
      toBits N.states W (searchStep N S b) := by
  unfold searchStepBits searchStep startBits
  rw [or_toBits N.states W hNW S (startSet N)]
  rw [epsCloseBits_epsClose N W hNW (fun q => S q || startSet N q)]
  exact stepBits_spec N W hNW (epsClose N (fun q => S q || startSet N q)) b

def bitsSearchRun (N : NFA) (W : Nat) (init : BitVec W) : List Byte → BitVec W
  | [] => init
  | b :: bs => bitsSearchRun N W (searchStepBits N W init b) bs

def setSearchRun (N : NFA) (S : SSet) : List Byte → SSet
  | [] => S
  | b :: bs => setSearchRun N (searchStep N S b) bs

theorem bitsSearchRun_eq (N : NFA) (W : Nat) (hNW : N.states ≤ W)
    (S : SSet) : ∀ input : List Byte,
    bitsSearchRun N W (toBits N.states W S) input =
      toBits N.states W (setSearchRun N S input) := by
  intro input
  induction input generalizing S with
  | nil => rfl
  | cons b bs ih =>
      change bitsSearchRun N W (searchStepBits N W (toBits N.states W S) b) bs =
        toBits N.states W (setSearchRun N (searchStep N S b) bs)
      rw [searchStepBits_spec N W hNW S b]
      exact ih (searchStep N S b)

theorem epsClose_out (N : NFA) (S : SSet) (q : Nat) (hq : N.states ≤ q) :
    epsClose N S q = false := by
  unfold epsClose
  have hno : ¬ epsFwd N S N.states q = true := by
    intro h
    have hr := epsFwd_in_range N S N.states q h
    omega
  cases hv : epsFwd N S N.states q <;> simp [hv] at hno ⊢

theorem reachStep_out (N : NFA) (S : SSet) (b : Byte) (q : Nat) (hq : N.states ≤ q) :
    reachStep N S b q = false := by
  unfold reachStep
  apply epsClose_out
  exact hq

theorem searchStep_out (N : NFA) (S : SSet) (b : Byte) (q : Nat) (hq : N.states ≤ q) :
    searchStep N S b q = false := by
  unfold searchStep
  exact reachStep_out N (epsClose N (fun r => S r || startSet N r)) b q hq

theorem setSearchRun_out (N : NFA) (S : SSet) (hS : ∀ q, N.states ≤ q → S q = false) :
    ∀ input q, N.states ≤ q → setSearchRun N S input q = false := by
  intro input
  induction input generalizing S with
  | nil => exact hS
  | cons b bs ih =>
      intro q hq
      apply ih
      · intro r hr
        exact searchStep_out N S b r hr
      · exact hq

def initSet (N : NFA) : SSet := epsClose N (startSet N)
def initBits (N : NFA) (W : Nat) : BitVec W :=
  epsCloseBits N W N.states (startBits N W)

theorem initBits_eq (N : NFA) (W : Nat) (hNW : N.states ≤ W) :
    initBits N W = toBits N.states W (initSet N) := by
  unfold initBits initSet
  unfold startBits
  rw [epsCloseBits_epsClose N W hNW (startSet N)]

theorem initSet_out (N : NFA) (q : Nat) (hq : N.states ≤ q) : initSet N q = false := by
  unfold initSet
  apply epsClose_out
  exact hq

/-- MAIN: the bitset machine represents exactly the reference NFA state set
    after every input byte (existence search injects the start state each time). -/
theorem regex_bitset_nfa_sound (N : NFA) (W : Nat) (hNW : N.states ≤ W)
    (input : List Byte) :
    fromBits N.states W (bitsSearchRun N W (initBits N W) input) =
      setSearchRun N (initSet N) input := by
  rw [initBits_eq N W hNW]
  rw [bitsSearchRun_eq N W hNW (initSet N)]
  funext i
  by_cases hi : i < N.states
  · exact fromBits_toBits N.states W hNW (setSearchRun N (initSet N) input) i hi
  · unfold fromBits
    have hb : (toBits N.states W (setSearchRun N (initSet N) input)).getLsbD i = false := by
      by_cases hiW : i < W
      · exact toBits_zero_above N.states W i hiW (by omega) (setSearchRun N (initSet N) input)
      · exact toBits_high_zero N.states W i (by omega) (setSearchRun N (initSet N) input)
    have hge : N.states ≤ i := by omega
    have hs := setSearchRun_out N (initSet N) (initSet_out N) input i hge
    simp [hi, hb, hs]

theorem regex_bitset_nfa_accepts (N : NFA) (W : Nat) (hNW : N.states ≤ W)
    (input : List Byte) :
    (List.range N.states).any (fun q =>
      (bitsSearchRun N W (initBits N W) input).getLsbD q && N.accept q) =
    (List.range N.states).any (fun q =>
      setSearchRun N (initSet N) input q && N.accept q) := by
  have hstate := regex_bitset_nfa_sound N W hNW input
  apply Bool.eq_iff_iff.mpr
  constructor <;> intro h
  · rw [List.any_eq_true] at h ⊢
    rcases h with ⟨q, hq, hqv⟩
    have hq' : q < N.states := List.mem_range.mp hq
    rw [Bool.and_eq_true] at hqv
    rcases hqv with ⟨hqb, hacc⟩
    have hqset : setSearchRun N (initSet N) input q = true := by
      have heq := congrFun hstate q
      unfold fromBits at heq
      have heq' : (bitsSearchRun N W (initBits N W) input).getLsbD q =
          setSearchRun N (initSet N) input q := by simpa [hq'] using heq
      rw [← heq']
      exact hqb
    exact ⟨q, hq, by rw [Bool.and_eq_true]; exact ⟨hqset, hacc⟩⟩
  · rw [List.any_eq_true] at h ⊢
    rcases h with ⟨q, hq, hqv⟩
    have hq' : q < N.states := List.mem_range.mp hq
    rw [Bool.and_eq_true] at hqv
    rcases hqv with ⟨hqset, hacc⟩
    refine ⟨q, hq, ?_⟩
    rw [Bool.and_eq_true]
    have heq := congrFun hstate q
    unfold fromBits at heq
    have heq' : (bitsSearchRun N W (initBits N W) input).getLsbD q =
        setSearchRun N (initSet N) input q := by simpa [hq'] using heq
    have hqb : (bitsSearchRun N W (initBits N W) input).getLsbD q = true := by
      rw [heq']
      exact hqset
    exact ⟨hqb, hacc⟩

/-- Small byte regex: literal, byte class, concatenation, alternation, star.
    No captures, no flags, no anchors; matching is ASCII/byte based. -/
def shiftList (k : Nat) (l : List Nat) : List Nat := l.map (fun x => x + k)

def shiftNFA (N : NFA) (k : Nat) : NFA where
  states := N.states + k
  start := N.start + k
  eps i := if k ≤ i then shiftList k (N.eps (i - k)) else []
  trans i b := if k ≤ i then shiftList k (N.trans (i - k) b) else []
  accept i := if k ≤ i then N.accept (i - k) else false

namespace Thompson.Program

def node (p : Program l) (i : Nat) : p.Q := p.nodes[i]?.getD p.graph.entry

def index (p : Program l) (q : p.Q) : Nat :=
  letI := p.eqQ
  p.nodes.idxOf q

theorem index_lt (p : Program l) (q : p.Q) : p.index q < p.nodes.length := by
  letI := p.eqQ
  exact List.idxOf_lt_length_of_mem (p.cover q)

@[simp] theorem node_index (p : Program l) (q : p.Q) : p.node (p.index q) = q := by
  letI := p.eqQ
  have h := p.index_lt q
  unfold node
  rw [List.getElem?_eq_getElem h]
  exact List.getElem_idxOf _

def targets (p : Program l) (i : Nat) (c : Option Byte) : List Nat :=
  (List.range p.nodes.length).filter fun j => p.graph.edge (p.node i) c (p.node j)

def machine (p : Program l) : NFA :=
  letI := p.eqQ
  { states := p.nodes.length, start := p.index p.graph.entry,
    eps := fun i => p.targets i none, trans := fun i b => p.targets i (some b),
    accept := fun i => p.node i == p.graph.exit }

end Thompson.Program

/-- Flatten the tagged Thompson graph into the numeric transition table. -/
def compile (r : Regex) : NFA := (Thompson.assemble r).machine

private def acceptedBits (N : NFA) (bits : BitVec W) : Bool :=
  (List.range N.states).any fun q => bits.getLsbD q && N.accept q

private def searchBoundaries (N : NFA) (W : Nat) (st : BitVec W) : List Byte → Bool
  | [] => acceptedBits N st
  | b :: bs => acceptedBits N st || searchBoundaries N W (searchStepBits N W st b) bs

/-- Observe acceptance at every input boundary, including the empty prefix. -/
def containsMatch (N : NFA) (W : Nat) (input : List Byte) : Bool :=
  searchBoundaries N W (initBits N W) input

private theorem searchBoundaries_spec (N : NFA) (W : Nat) (st : BitVec W) (input : List Byte) :
    searchBoundaries N W st input = true ↔ ∃ pre rest, input = pre ++ rest ∧
      acceptedBits N (bitsSearchRun N W st pre) = true := by
  induction input generalizing st with
  | nil => simp [searchBoundaries, bitsSearchRun]
  | cons b bs ih =>
    rw [searchBoundaries, Bool.or_eq_true, ih]
    constructor
    · rintro (h | ⟨pre, rest, rfl, h⟩)
      · exact ⟨[], b :: bs, rfl, h⟩
      · exact ⟨b :: pre, rest, rfl, h⟩
    · rintro ⟨pre, rest, eq, h⟩
      cases pre with
      | nil => exact Or.inl h
      | cons c ps =>
        obtain ⟨rfl, ht⟩ := List.cons.inj eq
        exact Or.inr ⟨ps, rest, ht, h⟩

/-- Unanchored bit search succeeds exactly when the independent reachable-set
    evaluator has an accepting state at some boundary of the input. -/
theorem containsMatch_iff_reference (N : NFA) (W : Nat) (hNW : N.states ≤ W)
    (input : List Byte) : containsMatch N W input = true ↔
    ∃ pre rest, input = pre ++ rest ∧
      (List.range N.states).any (fun q => setSearchRun N (initSet N) pre q && N.accept q) = true := by
  simp only [containsMatch, searchBoundaries_spec, acceptedBits, regex_bitset_nfa_accepts N W hNW]

set_option maxRecDepth 100000 in
theorem alternation_does_not_accept_left_prefix :
    let n := compile (.alt (.concat (.lit 97) (.lit 98)) (.lit 99))
    containsMatch n 8 [97] = false ∧ containsMatch n 8 [98] = false ∧
      containsMatch n 8 [97,98,120] = true ∧
      containsMatch n 8 [99] = true := by decide

theorem alternation_keeps_both_singletons :
    let n := compile (.alt (.lit 97) (.lit 98))
    containsMatch n 6 [97] = true ∧ containsMatch n 6 [98] = true ∧
      containsMatch n 6 [] = false := by decide

theorem star_eps_cycle_closure_stabilizes :
    let N := compile (Regex.star (Regex.lit 97))
    epsFwd N (startSet N) (N.states + 1) = epsFwd N (startSet N) N.states := by
  exact epsFwd_stable _ _

theorem a_star_accepts_empty :
    let N := compile (Regex.star (Regex.lit 97))
    (List.range N.states).any (fun q => (initBits N N.states).getLsbD q && N.accept q) = true := by
  decide

/-- `a*` has an epsilon cycle; the closure computation still stops. -/
theorem a_star_has_epsilon_cycle :
    let N := compile (Regex.star (Regex.lit 97))
    2 ∈ N.eps (N.states - 1) := by
  decide

/-- Counterexample: `(ab|b)c` matches `xbc` only if the start state is
    injected at EVERY position.  Injecting it once (at the row start) loses
    the match that begins at the second byte. -/
theorem start_injection_once_is_wrong :
    let r := Regex.concat
      (Regex.alt (Regex.concat (Regex.lit 97) (Regex.lit 98)) (Regex.lit 98))
      (Regex.lit 99)
    let N := compile r
    let W := N.states
    let input : List Byte := [120, 98, 99]
    let every := bitsSearchRun N W (initBits N W) input
    let once := input.foldl (fun x b => stepBits N W x b) (initBits N W)
    (List.range N.states).any (fun q => every.getLsbD q && N.accept q) = true ∧
    (List.range N.states).any (fun q => once.getLsbD q && N.accept q) = false := by
  set_option maxRecDepth 100000 in
    decide
end TinylambOpt.RegexBitsetNfa

-- C++ references to this model or its proof.
-- common/regex_guard.cpp
-- common/regex_guard.hpp
