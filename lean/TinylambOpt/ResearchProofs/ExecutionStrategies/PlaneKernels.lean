import TinylambOpt.ResearchGoals.ExecutionStrategies.PlaneKernels

/-!
# Soundness of bit-sliced plane kernels

Each `Goal` states that a bit-plane kernel over 64 lanes of unsigned byte
codes agrees with the corresponding scalar (per-lane) computation.

Proof strategy shared by all goals:

* `pack_getLsbD`: bit `i` of `pack f` is `f i` for `i < 64` (fold induction).
* `planes_getLsbD`: bit `i` of `planes x` at plane `k` is bit `k` of `x i`.
* `mod_two_pow_succ`: `x % 2^(n+1) = x % 2^n + 2^n * (x.testBit n)` — the
  bridge between `BitVec` lanes and their bit-plane decomposition.
* Per-bit Boolean recurrences (`ltStep`, the ripple-carry step, the
  `changed` fold, ...) are verified by finite case analysis and lifted over
  `List.range 8` / `List.range 64` by fold inductions of the shape
  `*_foldl_getLsbD`.

`ParallelLess` is proved first: the low-to-high `lessMask` fold is shown,
bit by bit, to compute the unsigned `<` of each lane pair, hence
`lessMask (planes x) (planes y) = pack (fun i => decide (x i < y i))` and the
masked equality follows.
-/

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.PlaneKernels

open TinylambOpt.ResearchGoals.PlaneKernels
open TinylambOpt.ResearchGoals.BitPlaneSum


private def pstep (f : Nat → Bool) (w : W) (i : Nat) : W :=
  w ||| ((BitVec.ofNat 64 (f i).toNat) <<< i)

private theorem foldl_pstep (f : Nat → Bool) :
    ∀ (n : Nat) (acc : W) (i : Nat), i < 64 →
      ((List.range n).foldl (pstep f) acc).getLsbD i = (acc.getLsbD i || (decide (i < n) && f i)) := by
  intro n acc
  induction n with
  | zero =>
      intro i _
      simp
  | succ n ih =>
      intro j hj
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil]
      rw [pstep, BitVec.getLsbD_or, ih j hj]
      have hsh : ((BitVec.ofNat 64 (f n).toNat) <<< n).getLsbD j = (decide (j = n) && f n) := by
        rw [BitVec.getLsbD_shiftLeft, BitVec.getLsbD_ofNat]
        cases h : f n with
        | false =>
            have h0 : (0 : Nat).testBit (j - n) = false :=
              Nat.testBit_lt_two_pow (Nat.two_pow_pos _)
            rw [Bool.toNat_false, h0]
            simp
        | true =>
            have h1 : (1 : Nat).testBit (j - n) = decide (j - n = 0) := by
              show (2^0 : Nat).testBit (j - n) = decide (j - n = 0)
              rw [Nat.testBit_two_pow]
              by_cases hz : j - n = 0
              · rw [hz]
              · rw [decide_eq_false hz, decide_eq_false (fun h2 => hz h2.symm)]
            rw [Bool.toNat_true, h1, decide_eq_true hj, Bool.true_and]
            rcases Nat.lt_trichotomy j n with hlt | heq | hgt
            · rw [decide_eq_true hlt,
                decide_eq_true (Nat.sub_eq_zero_of_le (Nat.le_of_lt hlt)),
                decide_eq_false (by omega : ¬ (j = n))]
              simp
            · subst heq
              rw [decide_eq_false (Nat.lt_irrefl j), decide_eq_true (Nat.sub_self j),
                decide_eq_true rfl]
              simp
            · rw [decide_eq_false (by omega : ¬ (j < n)),
                decide_eq_false (by omega : ¬ (j - n = 0)),
                decide_eq_false (by omega : ¬ (j = n))]
              simp
      rw [hsh]
      have hA : decide (j < n+1) = (decide (j < n) || decide (j = n)) := by
        rcases Nat.lt_trichotomy j n with hlt | heq | hgt
        · rw [decide_eq_true (Nat.lt_succ_of_lt hlt), decide_eq_true hlt,
            decide_eq_false (by omega : ¬ (j = n))]
          simp
        · subst heq
          rw [decide_eq_true (Nat.lt_succ_self j), decide_eq_false (Nat.lt_irrefl j),
            decide_eq_true rfl]
          simp
        · rw [decide_eq_false (by omega : ¬ (j < n)), decide_eq_false (by omega : ¬ (j = n)),
            decide_eq_false (by omega : ¬ (j < n+1))]
          simp
      rw [hA]
      rcases Nat.lt_trichotomy j n with hlt | heq | hgt
      · rw [decide_eq_true hlt, decide_eq_false (by omega : ¬ (j = n))]
        simp
      · subst heq
        rw [decide_eq_false (Nat.lt_irrefl j), decide_eq_true rfl]
        simp
      · rw [decide_eq_false (by omega : ¬ (j < n)), decide_eq_false (by omega : ¬ (j = n))]
        simp

private theorem pack_getLsbD (f : Nat → Bool) (i : Nat) (hi : i < 64) :
    (pack f).getLsbD i = f i := by
  show ((List.range 64).foldl (pstep f) 0).getLsbD i = f i
  rw [foldl_pstep f 64 0 i hi]
  simp [hi]

private def ltStep (ak bk lt : Bool) : Bool :=
  (!(ak) && bk) || (!(ak ^^ bk) && lt)

private theorem ltStep_ft (d : Bool) : ltStep false true d = true := by simp [ltStep]
private theorem ltStep_tf (d : Bool) : ltStep true false d = false := by simp [ltStep]
private theorem ltStep_ff (d : Bool) : ltStep false false d = d := by simp [ltStep]
private theorem ltStep_tt (d : Bool) : ltStep true true d = d := by simp [ltStep]

private theorem mod_two_pow_succ (x n : Nat) :
    x % 2^(n+1) = x % 2^n + 2^n * (x.testBit n).toNat := by
  have hpow : 2^(n+1) = 2^n * 2 := by rw [Nat.pow_add, Nat.pow_one]
  have hlt : x % 2^n < 2^n := Nat.mod_lt _ (Nat.two_pow_pos _)
  have hq : x / 2^n = 2 * (x / 2^(n+1)) + (x / 2^n) % 2 := by
    have h1 : (x / 2^n) % 2 + 2 * ((x / 2^n) / 2) = x / 2^n := Nat.mod_add_div _ _
    have h2 : (x / 2^n) / 2 = x / 2^(n+1) := by
      rw [Nat.div_div_eq_div_mul, ← hpow]
    omega
  have htb : (x.testBit n).toNat = (x / 2^n) % 2 := by
    have h3 : x.testBit n = (x / 2^n).testBit 0 := by
      have h := Nat.testBit_add x 0 n
      rwa [Nat.zero_add] at h
    rw [h3, Nat.testBit_zero]
    have h4 : (x / 2^n) % 2 < 2 := Nat.mod_lt _ (by decide : (0:Nat) < 2)
    by_cases h5 : (x / 2^n) % 2 = 1
    · rw [decide_eq_true h5]
      simp [h5]
    · rw [decide_eq_false h5]
      have h6 : (x / 2^n) % 2 = 0 := by omega
      simp [h6]
  have hx : x = x % 2^n + 2^n * (x / 2^n) := by
    have := Nat.mod_add_div x (2^n)
    omega
  have hx2 : x % 2^(n+1) = (x % 2^n + 2^n * (x / 2^n)) % 2^(n+1) :=
    congrArg (fun t => t % 2^(n+1)) hx
  rw [hx2]
  have hq' : 2 ^ n * (x / 2 ^ n)
      = 2 ^ (n + 1) * (x / 2 ^ (n + 1)) + 2 ^ n * ((x / 2 ^ n) % 2) := by
    have e := congrArg (fun t => 2 ^ n * t) hq
    rw [Nat.mul_add] at e
    have e2 : 2 ^ n * (2 * (x / 2 ^ (n + 1))) = 2 ^ (n + 1) * (x / 2 ^ (n + 1)) := by
      rw [← Nat.mul_assoc, ← hpow]
    rw [e2] at e
    exact e
  have hdecomp : x % 2^n + 2^n * (x / 2^n)
      = (x % 2^n + 2^n * ((x / 2^n) % 2)) + 2^(n+1) * (x / 2^(n+1)) := by
    rw [hq']
    omega
  rw [hdecomp, Nat.add_mul_mod_self_left, htb]
  apply Nat.mod_eq_of_lt
  have h2m : (x / 2^n) % 2 < 2 := Nat.mod_lt _ (by decide : (0:Nat) < 2)
  have h2 : (x / 2^n) % 2 ≤ 1 := by omega
  have h3 : 2^n * ((x / 2^n) % 2) ≤ 2^n := by
    calc 2^n * ((x / 2^n) % 2) ≤ 2^n * 1 := Nat.mul_le_mul_left _ h2
      _ = 2^n := by simp
  have hpow2 : 2^(n+1) = 2^n + 2^n := Nat.two_pow_succ n
  omega

private theorem ltStep_foldl_spec (xn yn : Nat) (n : Nat) :
    (List.range n).foldl (fun lt k => ltStep (xn.testBit k) (yn.testBit k) lt) false
      = decide (xn % 2^n < yn % 2^n) := by
  induction n with
  | zero =>
      simp [Nat.mod_one]
  | succ n ih =>
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil]
      rw [ih, mod_two_pow_succ xn n, mod_two_pow_succ yn n]
      have hlt1 : xn % 2^n < 2^n := Nat.mod_lt _ (Nat.two_pow_pos _)
      have hlt2 : yn % 2^n < 2^n := Nat.mod_lt _ (Nat.two_pow_pos _)
      have hpos : 0 < 2^n := Nat.two_pow_pos _
      cases hxb : xn.testBit n <;> cases hyb : yn.testBit n <;>
        simp only [hxb, hyb, ltStep_ft, ltStep_tf, ltStep_ff, ltStep_tt,
          Bool.toNat_true, Bool.toNat_false, Nat.mul_zero, Nat.mul_one,
          Nat.add_zero] <;>
        first
        | rfl
        | (apply (decide_eq_true _).symm; omega)
        | (apply (decide_eq_false _).symm; omega)
        | (rw [decide_eq_decide]; exact (Nat.add_lt_add_iff_right).symm)

/-- bit `i` of `planes x` at plane `k` is bit `k` of `x i`. -/
private theorem planes_getLsbD (x : Nat → B) (k i : Nat) (hi : i < 64) :
    (planes x k).getLsbD i = (x i).getLsbD k := by
  show (pack fun i => (x i).getLsbD k).getLsbD i = _
  exact pack_getLsbD _ _ hi

/-- `getLsbD` commutes with the `lessMask` fold (for lanes `i < 64`). -/
private theorem lessMask_foldl_getLsbD (a b : Planes) (i : Nat) (hi : i < 64) (n : Nat) :
    ((List.range n).foldl (fun lt k => (~~~(a k) &&& b k) ||| (~~~(a k ^^^ b k) &&& lt)) 0).getLsbD i
      = (List.range n).foldl (fun lt k => ltStep ((a k).getLsbD i) ((b k).getLsbD i) lt) false := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.range_succ, List.foldl_append, List.foldl_cons, List.foldl_nil]
      have hstep : ∀ lt : W,
          ((~~~(a n) &&& b n) ||| (~~~(a n ^^^ b n) &&& lt)).getLsbD i
            = ltStep ((a n).getLsbD i) ((b n).getLsbD i) (lt.getLsbD i) := by
        intro lt
        rw [BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_and,
            BitVec.getLsbD_not, BitVec.getLsbD_not, BitVec.getLsbD_xor]
        simp [hi, ltStep]
      rw [hstep, ih]

private theorem foldl_ltStep_congr {f g : Bool → Nat → Bool} (l : List Nat) (init : Bool)
    (h : ∀ acc k, f acc k = g acc k) :
    l.foldl f init = l.foldl g init := by
  induction l generalizing init with
  | nil => rfl
  | cons k t ih =>
      rw [List.foldl_cons, List.foldl_cons, h init k]
      exact ih (g init k)

private theorem lessMask_getLsbD (x y : Nat → B) (i : Nat) (hi : i < 64) :
    (lessMask (planes x) (planes y)).getLsbD i = decide (x i < y i) := by
  have h1 := lessMask_foldl_getLsbD (planes x) (planes y) i hi 8
  -- rewrite plane bits to testBit of lane values
  have h2 : (List.range 8).foldl (fun lt k => ltStep ((planes x k).getLsbD i) ((planes y k).getLsbD i) lt) false
      = (List.range 8).foldl (fun lt k => ltStep ((x i).toNat.testBit k) ((y i).toNat.testBit k) lt) false := by
    apply foldl_ltStep_congr
    intro acc k
    rw [planes_getLsbD x k i hi, planes_getLsbD y k i hi]
    rfl
  rw [h2] at h1
  rw [ltStep_foldl_spec ((x i).toNat) ((y i).toNat) 8] at h1
  -- h1 : lessMask...getLsbD i = decide (xn % 256 < yn % 256)
  have hlt : (x i).toNat < 256 := (x i).isLt
  have hlt2 : (y i).toNat < 256 := (y i).isLt
  rw [Nat.mod_eq_of_lt hlt, Nat.mod_eq_of_lt hlt2] at h1
  -- h1 : ... = decide ((x i).toNat < (y i).toNat)
  exact h1

theorem sound_ParallelLess : ParallelLess.Goal := by
  intro x y m
  have hmask : lessMask (planes x) (planes y) = pack (fun i => decide (x i < y i)) := by
    apply BitVec.eq_of_getLsbD_eq
    intro i hi
    rw [lessMask_getLsbD x y i hi, pack_getLsbD _ _ hi]
  rw [hmask]

private theorem foldl_or_decide (g : Nat → Bool) (n : Nat) :
    (List.range n).foldl (fun acc k => acc || g k) false
      = decide (∃ k ∈ List.range n, g k) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.foldl_append]
      simp only [List.foldl_cons, List.foldl_nil, ih]
      have hmem : (∃ k ∈ List.range n ++ [n], g k) ↔
          (∃ k ∈ List.range n, g k) ∨ g n := by
        constructor
        · rintro ⟨k, hk, hg⟩
          rw [List.mem_append] at hk
          rcases hk with hk | hk
          · exact Or.inl ⟨k, hk, hg⟩
          · rw [List.mem_singleton] at hk; subst hk; exact Or.inr hg
        · rintro (⟨k, hk, hg⟩ | hg)
          · exact ⟨k, List.mem_append.mpr (Or.inl hk), hg⟩
          · exact ⟨n, List.mem_append.mpr (Or.inr (List.mem_singleton_self n)), hg⟩
      rw [decide_eq_decide.mpr hmem, Bool.eq_iff_iff, Bool.or_eq_true_iff,
        decide_eq_true_iff, decide_eq_true_iff]

private theorem exists_bit_diff_iff_ne (x v : B) :
    (∃ k ∈ List.range 8, (x.getLsbD k) ≠ (v.getLsbD k)) ↔ x ≠ v := by
  have hext : x = v ↔ ∀ k, k < 8 → x.getLsbD k = v.getLsbD k :=
    BitVec.eq_of_getLsbD_eq_iff (w := 8)
  constructor
  · rintro ⟨k, hk, hne⟩ rfl
    have hk8 : k < 8 := List.mem_range.mp hk
    exact hne (hext.mp rfl k hk8)
  · intro hne
    by_cases hcon : ∃ k ∈ List.range 8, x.getLsbD k ≠ v.getLsbD k
    · exact hcon
    · exfalso
      apply hne
      apply hext.mpr
      intro k hk
      by_cases hdiff : x.getLsbD k = v.getLsbD k
      · exact hdiff
      · exact absurd ⟨k, List.mem_range.mpr hk, hdiff⟩ hcon



-- getLsbD of an ite
private theorem getLsbD_ite {c : Prop} [Decidable c] (x y : W) (i : Nat) :
    BitVec.getLsbD (if c then x else y) i = if c then BitVec.getLsbD x i else BitVec.getLsbD y i := by
  by_cases hc : c <;> simp [hc]

-- contribution of one plane of `changed` at lane i, as a Bool
private theorem changed_contrib (a : Planes) (v : B) (i n : Nat) (hi : i < 64) :
    BitVec.getLsbD (if v.getLsbD n = true then ~~~(a n) else a n) i
      = (if v.getLsbD n = true then !(a n).getLsbD i else (a n).getLsbD i) := by
  rw [getLsbD_ite, BitVec.getLsbD_not, decide_eq_true hi, Bool.true_and]

-- the ite contribution is true iff the two bits differ
private theorem contrib_iff_diff (xb vb : Bool) :
    ((if vb = true then !xb else xb) = true) ↔ (xb ≠ vb) := by
  cases xb <;> cases vb <;> simp

-- the `changed` fold commutes with getLsbD
private theorem changed_foldl_getLsbD (a : Planes) (v : B) (i : Nat) (hi : i < 64) (n : Nat) :
    BitVec.getLsbD
        ((List.range n).foldl
          (fun w k => w ||| (if v.getLsbD k = true then ~~~(a k) else a k)) 0) i
      = (List.range n).foldl
          (fun acc k => acc || (if v.getLsbD k = true then !(a k).getLsbD i else (a k).getLsbD i))
          false := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.range_succ, List.foldl_append, List.foldl_cons, List.foldl_nil]
      rw [BitVec.getLsbD_or, changed_contrib a v i n hi, ih]

private theorem exists_mem_congr {P Q : Nat → Prop} (l : List Nat) (h : ∀ k ∈ l, P k ↔ Q k) :
    (∃ k ∈ l, P k) ↔ (∃ k ∈ l, Q k) := by
  constructor
  · rintro ⟨k, hk, hp⟩; exact ⟨k, hk, (h k hk).mp hp⟩
  · rintro ⟨k, hk, hq⟩; exact ⟨k, hk, (h k hk).mpr hq⟩

private theorem changed_planes_getLsbD (x : Nat → B) (v : B) (i : Nat) (hi : i < 64) :
    BitVec.getLsbD (changed (planes x) v) i = decide (x i != v) := by
  show BitVec.getLsbD
      ((List.range 8).foldl
        (fun w k => w ||| (if v.getLsbD k = true then ~~~(planes x k) else planes x k)) 0) i
      = decide (x i != v)
  rw [changed_foldl_getLsbD (planes x) v i hi 8]
  have hcongr : (List.range 8).foldl
        (fun acc k => acc || (if v.getLsbD k = true then !((planes x k).getLsbD i) else (planes x k).getLsbD i)) false
      = (List.range 8).foldl
        (fun acc k => acc || (if v.getLsbD k = true then !((x i).getLsbD k) else (x i).getLsbD k)) false := by
    apply foldl_ltStep_congr
    intro acc k
    rw [planes_getLsbD x k i hi]
  rw [hcongr, foldl_or_decide _ 8]
  apply decide_eq_decide.mpr
  have h1 : ∀ k ∈ List.range 8,
      ((if v.getLsbD k = true then !((x i).getLsbD k) else (x i).getLsbD k) = true) ↔
        ((x i).getLsbD k ≠ v.getLsbD k) := by
    intro k _
    exact contrib_iff_diff _ _
  rw [exists_mem_congr _ h1]
  exact Iff.trans (exists_bit_diff_iff_ne (x i) v) (bne_iff_ne).symm

-- mask bit = lane selected AND differs
private theorem mask_changed_getLsbD (x : Nat → B) (v : B) (m : W) (i : Nat) (hi : i < 64) :
    BitVec.getLsbD (m &&& changed (planes x) v) i = (m.getLsbD i && decide (x i != v)) := by
  rw [BitVec.getLsbD_and, changed_planes_getLsbD x v i hi]



private theorem pairwise_range (n : Nat) : List.Pairwise (· < ·) (List.range n) := by
  induction n with
  | zero => exact List.Pairwise.nil
  | succ n ih =>
      rw [List.range_succ, List.pairwise_append]
      refine ⟨ih, List.pairwise_singleton _ _, ?_⟩
      intro a ha b hb
      rw [List.mem_singleton] at hb
      subst hb
      exact List.mem_range.mp ha

private theorem pairwise_lanes (m : W) : List.Pairwise (· < ·) (lanes m) := by
  show List.Pairwise (· < ·) ((List.range 64).filter m.getLsbD)
  exact List.Pairwise.filter _ (pairwise_range 64)

private theorem find?_eq_iff_of_pairwise {p : Nat → Bool} {l : List Nat}
    (hs : List.Pairwise (· < ·) l) {i : Nat} :
    l.find? p = some i ↔ i ∈ l ∧ p i = true ∧ ∀ j ∈ l, j < i → ¬ (p j = true) := by
  induction l with
  | nil => simp [List.find?]
  | cons a t ih =>
      have hst : List.Pairwise (· < ·) t := List.Pairwise.of_cons hs
      have hat : ∀ b ∈ t, a < b := (List.pairwise_cons.mp hs).1
      have hat_not : a ∉ t := by
        intro ha
        exact Nat.lt_irrefl a (hat a ha)
      rw [List.find?_cons]
      cases hpa : p a with
      | true =>
          dsimp only
          rw [Option.some.injEq]
          constructor
          · intro hai
            subst i
            refine ⟨List.mem_cons_self, hpa, ?_⟩
            intro j hj hjlt
            rcases List.mem_cons.mp hj with heq | hjt
            · subst j
              exact absurd hjlt (Nat.lt_irrefl a)
            · exact absurd (Nat.lt_trans hjlt (hat j hjt)) (Nat.lt_irrefl j)
          · rintro ⟨hmem, -, hforall⟩
            rcases List.mem_cons.mp hmem with heq | hit
            · subst i
              rfl
            · have hai : a < i := hat i hit
              have hna := hforall a List.mem_cons_self hai
              rw [hpa] at hna
              exact absurd rfl hna
      | false =>
          dsimp only
          rw [ih hst]
          have hpaF : p a = false := hpa
          constructor
          · rintro ⟨hit, hp, htf⟩
            have hmem : i ∈ a :: t := List.mem_cons.mpr (Or.inr hit)
            refine ⟨hmem, hp, ?_⟩
            intro j hj hjlt
            rcases List.mem_cons.mp hj with heq | hjt
            · subst j
              rw [hpaF]
              exact fun h => Bool.noConfusion h
            · exact htf j hjt hjlt
          · rintro ⟨hmem, hp, hcall⟩
            have hnot_a : i ≠ a := by
              intro hia
              subst i
              rw [hpaF] at hp
              exact Bool.noConfusion hp
            have hit : i ∈ t := by
              rcases List.mem_cons.mp hmem with h | h
              · exact absurd h hnot_a
              · exact h
            refine ⟨hit, hp, ?_⟩
            intro j hjt hjlt
            exact hcall j (List.mem_cons.mpr (Or.inr hjt)) hjlt


private theorem natCast64_toNat : ((↑64 : W)).toNat = 64 := by
  show (BitVec.ofNat 64 64).toNat = 64
  rw [BitVec.toNat_ofNat, Nat.mod_eq_of_lt (Nat.lt_pow_self (a := 2) (n := 64) (by decide))]

private theorem ctz_zero_toNat : ((0 : W).ctz).toNat = 64 := by
  have hrev : ((0 : W).reverse) = 0 := (BitVec.reverse_eq_zero_iff (x := (0 : W))).mpr rfl
  have h : (0 : W).ctz = (↑64 : W) := by
    rw [BitVec.ctz_eq_reverse_clz, hrev]
    exact (BitVec.clz_eq_iff_eq_zero (x := (0 : W))).mpr rfl
  rw [h, natCast64_toNat]

private theorem ne_zero_of_getLsbD {w : W} {i : Nat} (hi : i < 64)
    (h : w.getLsbD i = true) : w ≠ 0 := by
  intro hw
  rw [hw] at h
  simp at h

private theorem eq_zero_of_getLsbD {w : W} (h : ∀ i, i < 64 → w.getLsbD i = false) : w = 0 := by
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp [h i hi]

/-- If `i₀` is a set bit and all lower bits are clear, `ctz` lands on `i₀`. -/
private theorem ctz_toNat_eq_of_bits {w : W} (hw : w ≠ 0) {i₀ : Nat} (hi₀ : i₀ < 64)
    (hbit : w.getLsbD i₀ = true) (hbelow : ∀ j, j < i₀ → w.getLsbD j = false) :
    w.ctz.toNat = i₀ := by
  have hc : w.ctz.toNat < 64 := by
    have hlt := (BitVec.ctz_lt_iff_ne_zero (x := w)).mpr hw
    have h2 : (w.ctz).toNat < ((↑64 : W)).toNat := hlt
    rw [natCast64_toNat] at h2
    exact h2
  have hct : w.getLsbD w.ctz.toNat = true := BitVec.getLsbD_true_ctz_of_ne_zero hw
  have hcl : ∀ j, j < w.ctz.toNat → w.getLsbD j = false :=
    fun j hj => BitVec.getLsbD_false_of_lt_ctz hj
  rcases Nat.lt_trichotomy w.ctz.toNat i₀ with hlt | heq | hgt
  · have hb := hbelow _ hlt
    rw [hct] at hb
    simp at hb
  · exact heq
  · have hb := hcl _ hgt
    rw [hbit] at hb
    simp at hb

/-- bit `i` of `m &&& changed` is `m_i && (x_i ≠ v)` for `i < 64`. -/
private theorem maskbit_iff_diff (x : Nat → B) (v : B) (m : W) (i : Nat) (hi : i < 64) :
    ((m &&& changed (planes x) v).getLsbD i = true) ↔
      (m.getLsbD i = true ∧ (x i != v) = true) := by
  rw [mask_changed_getLsbD x v m i hi]
  have hd : decide (x i != v) = (x i != v) := Bool.decide_eq_true
  rw [hd, Bool.and_eq_true]

theorem sound_RunBoundary : RunBoundary.Goal := by
  intro x v m
  cases hfind : (lanes m).find? (fun i => x i != v) with
  | none =>
      have hnone := (List.find?_eq_none).mp hfind
      have hw0 : m &&& changed (planes x) v = 0 := by
        apply eq_zero_of_getLsbD
        intro i hi
        rw [mask_changed_getLsbD x v m i hi]
        have hd : decide (x i != v) = (x i != v) := Bool.decide_eq_true
        rw [hd]
        by_cases hmi : m.getLsbD i = true
        · have himem : i ∈ lanes m := by
            show i ∈ (List.range 64).filter m.getLsbD
            exact List.mem_filter.mpr ⟨List.mem_range.mpr hi, hmi⟩
          have hnp := hnone i himem
          have hdf : (x i != v) = false := by
            cases h : (x i != v) with
            | true => exact absurd h hnp
            | false => rfl
          rw [hmi, hdf]
          rfl
        · have hmif : m.getLsbD i = false := by
            cases hc : m.getLsbD i <;> simp_all
          rw [hmif]
          simp
      rw [hw0, ctz_zero_toNat]
      rfl
  | some i₀ =>
      have hprop := (find?_eq_iff_of_pairwise (pairwise_lanes m)
        (p := fun i => x i != v)).mp hfind
      obtain ⟨hi₀mem, hp₀, hleast⟩ := hprop
      have hi₀64 : i₀ < 64 := List.mem_range.mp ((List.mem_filter.mp hi₀mem).1)
      have hm₀ : m.getLsbD i₀ = true := (List.mem_filter.mp hi₀mem).2
      have hbit₀ : (m &&& changed (planes x) v).getLsbD i₀ = true := by
        rw [maskbit_iff_diff x v m i₀ hi₀64]
        exact ⟨hm₀, hp₀⟩
      have hw : m &&& changed (planes x) v ≠ 0 := ne_zero_of_getLsbD hi₀64 hbit₀
      have hctz : (m &&& changed (planes x) v).ctz.toNat = i₀ := by
        apply ctz_toNat_eq_of_bits hw hi₀64 hbit₀
        intro j hj
        rw [mask_changed_getLsbD x v m j (by omega : j < 64)]
        have hj64 : j < 64 := by omega
        by_cases hmj : m.getLsbD j = true
        · have hjmem : j ∈ lanes m := by
            show j ∈ (List.range 64).filter m.getLsbD
            exact List.mem_filter.mpr ⟨List.mem_range.mpr hj64, hmj⟩
          have hnotp := hleast j hjmem hj
          have hdf : (x j != v) = false := by
            cases h : (x j != v) with
            | true => exact absurd h hnotp
            | false => rfl
          rw [hmj]
          have hd : decide (x j != v) = (x j != v) := Bool.decide_eq_true
          rw [hd, hdf]
          rfl
        · have hmjf : m.getLsbD j = false := by
            cases hc : m.getLsbD j <;> simp_all
          rw [hmjf]
          simp
      rw [hctz]
      rfl

/-! ### MedianOfThree: per-lane median mux over pairwise `lessMask` bits -/

private theorem bv_le_of_lt {a b : BitVec 8} (h : a < b) : a ≤ b := by
  have h' : a.toNat < b.toNat := h
  have : a.toNat ≤ b.toNat := by omega
  exact this

private theorem bv_ge_of_not_lt {a b : BitVec 8} (h : ¬ a < b) : b ≤ a := by
  have h' : ¬ (a.toNat < b.toNat) := h
  have : b.toNat ≤ a.toNat := by omega
  exact this

private theorem bv_eq_of_le_ge {a b : BitVec 8} (h1 : a ≤ b) (h2 : b ≤ a) : a = b := by
  have e1 : a.toNat ≤ b.toNat := h1
  have e2 : b.toNat ≤ a.toNat := h2
  have : a.toNat = b.toNat := by omega
  exact BitVec.eq_of_toNat_eq this

private theorem bv_min_eq_of_le {a b : BitVec 8} (h : a ≤ b) : min a b = a := ite_eq_left h
private theorem bv_min_eq_of_not_le {a b : BitVec 8} (h : ¬ a ≤ b) : min a b = b := ite_eq_right h
private theorem bv_min_eq_of_ge {a b : BitVec 8} (h : b ≤ a) : min a b = b := by
  by_cases hab : a ≤ b
  · have he : a = b := bv_eq_of_le_ge hab h
    rw [bv_min_eq_of_le hab]
    exact he
  · exact bv_min_eq_of_not_le hab
private theorem bv_max_eq_of_le {a b : BitVec 8} (h : a ≤ b) : max a b = b := ite_eq_left h
private theorem bv_max_eq_of_not_le {a b : BitVec 8} (h : ¬ a ≤ b) : max a b = a := ite_eq_right h
private theorem bv_max_eq_of_ge {a b : BitVec 8} (h : b ≤ a) : max a b = a := by
  by_cases hab : a ≤ b
  · have he : a = b := bv_eq_of_le_ge hab h
    rw [bv_max_eq_of_le hab]
    exact he.symm
  · exact bv_max_eq_of_not_le hab

/-- Per-lane median bit: the mux of the three comparison masks picks the bit of the
median byte `max (min x y) (min (max x y) z)`. -/
private theorem median3_lane (xv yv zv : BitVec 8) (k : Nat) :
    (((decide (xv < yv) ^^ decide (xv < zv)) && (xv.getLsbD k)) ||
    (!(decide (xv < yv) ^^ decide (xv < zv)) &&
      ((!decide (xv < yv)) ^^ decide (yv < zv)) && (yv.getLsbD k)) ||
    (!(decide (xv < yv) ^^ decide (xv < zv)) &&
      !((!decide (xv < yv)) ^^ decide (yv < zv)) && (zv.getLsbD k)))
    = (max (min xv yv) (min (max xv yv) zv)).getLsbD k := by
  by_cases hxy : xv < yv
  · by_cases hxz : xv < zv
    · by_cases hyz : yv < zv
      · -- x < y < z : median is y
        rw [decide_eq_true hxy, decide_eq_true hxz, decide_eq_true hyz]
        have hxy' := bv_le_of_lt hxy
        have hyz' := bv_le_of_lt hyz
        have hmed : max (min xv yv) (min (max xv yv) zv) = yv := by
          rw [bv_min_eq_of_le hxy', bv_max_eq_of_le hxy', bv_min_eq_of_le hyz',
            bv_max_eq_of_le hxy']
        rw [hmed]
        simp
      · -- x < z ≤ y : median is z
        rw [decide_eq_true hxy, decide_eq_true hxz, decide_eq_false hyz]
        have hxy' := bv_le_of_lt hxy
        have hxz' := bv_le_of_lt hxz
        have hzy := bv_ge_of_not_lt hyz
        have hmed : max (min xv yv) (min (max xv yv) zv) = zv := by
          rw [bv_min_eq_of_le hxy', bv_max_eq_of_le hxy', bv_min_eq_of_ge hzy,
            bv_max_eq_of_le hxz']
        rw [hmed]
        simp
    · by_cases hyz : yv < zv
      · -- z ≤ x < y and y < z : inconsistent
        have e1 : xv.toNat < yv.toNat := hxy
        have e2 : zv.toNat ≤ xv.toNat := bv_ge_of_not_lt hxz
        have e3 : yv.toNat < zv.toNat := hyz
        have hF : False := by omega
        exact hF.elim
      · -- z ≤ x < y : median is x
        rw [decide_eq_true hxy, decide_eq_false hxz, decide_eq_false hyz]
        have hxy' := bv_le_of_lt hxy
        have hzx := bv_ge_of_not_lt hxz
        have hzy := bv_ge_of_not_lt hyz
        have hmed : max (min xv yv) (min (max xv yv) zv) = xv := by
          rw [bv_min_eq_of_le hxy', bv_max_eq_of_le hxy', bv_min_eq_of_ge hzy,
            bv_max_eq_of_ge hzx]
        rw [hmed]
        simp
  · by_cases hxz : xv < zv
    · by_cases hyz : yv < zv
      · -- y ≤ x < z : median is x
        rw [decide_eq_false hxy, decide_eq_true hxz, decide_eq_true hyz]
        have hyx := bv_ge_of_not_lt hxy
        have hxz' := bv_le_of_lt hxz
        have hmed : max (min xv yv) (min (max xv yv) zv) = xv := by
          rw [bv_min_eq_of_ge hyx, bv_max_eq_of_ge hyx, bv_min_eq_of_le hxz',
            bv_max_eq_of_le hyx]
        rw [hmed]
        simp
      · -- y ≤ x < z and z ≤ y : inconsistent
        have e1 : yv.toNat ≤ xv.toNat := bv_ge_of_not_lt hxy
        have e2 : xv.toNat < zv.toNat := hxz
        have e3 : zv.toNat ≤ yv.toNat := bv_ge_of_not_lt hyz
        have hF : False := by omega
        exact hF.elim
    · by_cases hyz : yv < zv
      · -- y < z ≤ x : median is z
        rw [decide_eq_false hxy, decide_eq_false hxz, decide_eq_true hyz]
        have hyx := bv_ge_of_not_lt hxy
        have hzx := bv_ge_of_not_lt hxz
        have hyz' := bv_le_of_lt hyz
        have hmed : max (min xv yv) (min (max xv yv) zv) = zv := by
          rw [bv_min_eq_of_ge hyx, bv_max_eq_of_ge hyx, bv_min_eq_of_ge hzx,
            bv_max_eq_of_le hyz']
        rw [hmed]
        simp
      · -- z ≤ y ≤ x : median is y
        rw [decide_eq_false hxy, decide_eq_false hxz, decide_eq_false hyz]
        have hyx := bv_ge_of_not_lt hxy
        have hzx := bv_ge_of_not_lt hxz
        have hzy := bv_ge_of_not_lt hyz
        have hmed : max (min xv yv) (min (max xv yv) zv) = yv := by
          rw [bv_min_eq_of_ge hyx, bv_max_eq_of_ge hyx, bv_min_eq_of_ge hzx,
            bv_max_eq_of_ge hzy]
        rw [hmed]
        simp

theorem sound_MedianOfThree : MedianOfThree.Goal := by
  intro x y z k
  apply BitVec.eq_of_getLsbD_eq
  intro i hi
  simp only [median3, BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_xor,
    BitVec.getLsbD_not, planes_getLsbD x k i hi, planes_getLsbD y k i hi,
    planes_getLsbD z k i hi,
    planes_getLsbD (fun i => max (min (x i) (y i)) (min (max (x i) (y i)) (z i))) k i hi,
    lessMask_getLsbD x y i hi, lessMask_getLsbD x z i hi, lessMask_getLsbD y z i hi,
    decide_eq_true hi, Bool.true_and]
  exact median3_lane (x i) (y i) (z i) k

/-! ### PlaneDot helpers -/

private theorem sum_map_zero {α : Type} (l : List α) : (l.map fun _ => (0 : Nat)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a t ih => simp [ih]

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map fun a => f a + g a).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem mul_sum_const {α : Type} (l : List α) (f : α → Nat) (c : Nat) :
    c * (l.map f).sum = (l.map fun a => c * f a).sum := by
  induction l with
  | nil => simp
  | cons a t ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [Nat.mul_add, ih]

private theorem countP_congr_bool {α : Type} {p q : α → Bool} {l : List α}
    (h : ∀ i ∈ l, p i = q i) : l.countP p = l.countP q := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      have hat : p a = q a := h a List.mem_cons_self
      have ht : ∀ i ∈ t, p i = q i := fun i hi => h i (List.mem_cons_of_mem a hi)
      rw [List.countP_cons, List.countP_cons, ih ht, hat]

private theorem sum_congr {α : Type} {l : List α} {f g : α → Nat}
    (h : ∀ a ∈ l, f a = g a) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      rw [List.map_cons, List.map_cons, List.sum_cons, List.sum_cons,
        h a List.mem_cons_self, ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

private theorem sum_sum_comm {α β : Type} (l1 : List α) (l2 : List β) (f : α → β → Nat) :
    (l1.map fun a => (l2.map fun b => f a b).sum).sum
      = (l2.map fun b => (l1.map fun a => f a b).sum).sum := by
  induction l1 with
  | nil =>
      simp only [List.map_nil, List.sum_nil]
      -- RHS: (l2.map fun b => ([].map ...).sum).sum = (l2.map fun _ => 0).sum = 0
      simp [sum_map_zero]
  | cons a t ih =>
      simp only [List.map_cons, List.sum_cons]
      have hsplit : (l2.map fun b => f a b + (t.map fun a' => f a' b).sum).sum
          = (l2.map fun b => f a b).sum + (l2.map fun b => (t.map fun a' => f a' b).sum).sum := by
        exact sum_map_add l2 (fun b => f a b) (fun b => (t.map fun a' => f a' b).sum)
      rw [hsplit, ih]

private theorem sum_mul_sum {α β : Type} (l1 : List α) (l2 : List β) (f : α → Nat) (g : β → Nat) :
    (l1.map f).sum * (l2.map g).sum
      = (l1.map fun a => (l2.map fun b => f a * g b).sum).sum := by
  induction l1 with
  | nil => simp
  | cons a t ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [Nat.add_mul, ih, mul_sum_const]

private theorem sum_testBit : ∀ (w n : Nat), n < 2^w →
    n = ((List.range w).map fun k => 2^k * (n.testBit k).toNat).sum := by
  intro w
  induction w with
  | zero =>
      intro n hn
      have hn0 : n = 0 := by omega
      subst hn0
      simp
  | succ w ih =>
      intro n hn
      have hltw : n % 2^w < 2^w := Nat.mod_lt _ (Nat.two_pow_pos _)
      have hih := ih (n % 2^w) hltw
      have htb : ∀ k ∈ List.range w, ((n % 2^w).testBit k) = n.testBit k := by
        intro k hk
        have hk' : k < w := List.mem_range.mp hk
        rw [Nat.testBit_mod_two_pow, decide_eq_true hk', Bool.true_and]
      have hmap : ((List.range w).map fun k => 2^k * ((n % 2^w).testBit k).toNat).sum
          = ((List.range w).map fun k => 2^k * (n.testBit k).toNat).sum := by
        apply sum_congr
        intro k hk
        rw [htb k hk]
      rw [hmap] at hih
      calc n = n % 2^(w+1) := (Nat.mod_eq_of_lt hn).symm
        _ = n % 2^w + 2^w * (n.testBit w).toNat := mod_two_pow_succ n w
        _ = ((List.range w).map fun k => 2^k * (n.testBit k).toNat).sum
              + 2^w * (n.testBit w).toNat := by rw [hih]
        _ = ((List.range (w+1)).map fun k => 2^k * (n.testBit k).toNat).sum := by
            rw [List.range_succ, List.map_append, List.sum_append_nat]
            simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil,
              Nat.add_zero]

private theorem sum_testBit_ite (w n : Nat) (hn : n < 2^w) :
    n = ((List.range w).map fun k => if n.testBit k = true then 2^k else 0).sum := by
  calc n = ((List.range w).map fun k => 2^k * (n.testBit k).toNat).sum :=
        sum_testBit w n hn
    _ = ((List.range w).map fun k => if n.testBit k = true then 2^k else 0).sum := by
        apply sum_congr
        intro k _
        cases n.testBit k <;> simp

private theorem bv_sum_bits (v : B) :
    v.toNat = ((List.range 8).map fun j => if v.getLsbD j = true then 2^j else 0).sum := by
  have h := sum_testBit_ite 8 v.toNat v.isLt
  exact h

private theorem ite_mul_pow (b c : Bool) (j k : Nat) :
    (if b = true then 2^j else 0) * (if c = true then 2^k else 0)
      = if (b && c) = true then 2^(j+k) else 0 := by
  cases b <;> cases c <;> simp [Nat.pow_add]

private theorem ite_ite_and3 (m bx bz : Bool) (W : Nat) :
    (if m = true then (if (bx && bz) = true then W else 0) else 0)
      = if ((m && bx) && bz) = true then W else 0 := by
  cases m <;> cases bx <;> cases bz <;> rfl

private theorem ite_sum {c : Prop} [Decidable c] :
    ∀ (l : List Nat) (f : Nat → Nat),
    (if c then (l.map f).sum else 0) = (l.map fun a => if c then f a else 0).sum := by
  intro l f
  induction l with
  | nil => by_cases c <;> simp
  | cons a t ih =>
      simp only [List.map_cons, List.sum_cons]
      by_cases hc : c
      · simp only [ite_eq_left hc]
      · simp only [ite_eq_right hc, sum_map_zero]

private theorem sum_ite_const {α : Type} (l : List α) (p : α → Bool) (C : Nat) :
    (l.map fun a => if p a = true then C else 0).sum = C * l.countP p := by
  induction l with
  | nil => simp
  | cons a t ih =>
      rw [List.map_cons, List.sum_cons, List.countP_cons]
      by_cases hpa : p a = true
      · rw [ite_eq_left hpa]
        rw [ite_eq_left hpa]
        rw [ih, Nat.mul_add_one]
        omega
      · rw [ite_eq_right hpa]
        rw [ite_eq_right hpa]
        rw [ih]
        simp

private theorem sum_filter_map_ite {α : Type} (l : List α) (p : α → Bool) (f : α → Nat) :
    ((l.filter p).map f).sum = (l.map fun a => if p a = true then f a else 0).sum := by
  induction l with
  | nil => rfl
  | cons a t ih =>
      by_cases hpa : p a = true
      · rw [List.filter_cons_of_pos hpa, List.map_cons, List.map_cons, List.sum_cons,
          List.sum_cons, ih, ite_eq_left hpa]
      · rw [List.filter_cons_of_neg hpa, List.map_cons, List.sum_cons, ih,
          ite_eq_right hpa]
        simp

private theorem count_planes_dot (x y : Nat → B) (m : W) (j k : Nat) :
    count (m &&& planes x j &&& planes y k)
      = (List.range 64).countP (fun i => m.getLsbD i && (x i).getLsbD j && (y i).getLsbD k) := by
  show ((List.range 64).filter (m &&& planes x j &&& planes y k).getLsbD).length = _
  rw [← List.countP_eq_length_filter]
  apply countP_congr_bool
  intro i hi
  have hi64 : i < 64 := List.mem_range.mp hi
  rw [BitVec.getLsbD_and, BitVec.getLsbD_and,
    planes_getLsbD x j i hi64, planes_getLsbD y k i hi64]

private theorem sum_sum_sum_comm (l : List Nat) (f : Nat → Nat → Nat → Nat) :
    (l.map fun a => ((List.range 8).map fun j =>
        ((List.range 8).map fun k => f a j k).sum).sum).sum
      = ((List.range 8).map fun j => ((List.range 8).map fun k =>
          (l.map fun a => f a j k).sum).sum).sum := by
  calc (l.map fun a => ((List.range 8).map fun j =>
          ((List.range 8).map fun k => f a j k).sum).sum).sum
      = ((List.range 8).map fun j => (l.map fun a =>
          ((List.range 8).map fun k => f a j k).sum).sum).sum :=
        sum_sum_comm l (List.range 8)
          (fun a j => ((List.range 8).map fun k => f a j k).sum)
    _ = ((List.range 8).map fun j => ((List.range 8).map fun k =>
          (l.map fun a => f a j k).sum).sum).sum := by
        apply sum_congr
        intro j _
        exact sum_sum_comm l (List.range 8) (fun a k => f a j k)

theorem sound_PlaneDot : PlaneDot.Goal := by
  intro x y m
  have hR : ((lanes m).map fun i => (x i).toNat * (y i).toNat).sum
      = ((List.range 64).map fun i =>
          if m.getLsbD i = true then (x i).toNat * (y i).toNat else 0).sum := by
    show (((List.range 64).filter m.getLsbD).map fun i => (x i).toNat * (y i).toNat).sum = _
    exact sum_filter_map_ite _ _ _
  rw [hR]
  have hlane : ∀ i : Nat,
      (if m.getLsbD i = true then (x i).toNat * (y i).toNat else 0)
        = ((List.range 8).map fun j => ((List.range 8).map fun k =>
            if (m.getLsbD i && (x i).getLsbD j && (y i).getLsbD k) = true
            then 2^(j+k) else 0).sum).sum := by
    intro i
    have e1 : (x i).toNat * (y i).toNat
        = ((List.range 8).map fun j => ((List.range 8).map fun k =>
            (if (x i).getLsbD j = true then 2^j else 0) *
            (if (y i).getLsbD k = true then 2^k else 0)).sum).sum := by
      rw [bv_sum_bits (x i), bv_sum_bits (y i)]
      exact sum_mul_sum _ _ _ _
    have e2 : ∀ j k : Nat,
        (if (x i).getLsbD j = true then 2^j else 0) *
        (if (y i).getLsbD k = true then 2^k else 0)
          = if ((x i).getLsbD j && (y i).getLsbD k) = true then 2^(j+k) else 0 :=
      fun j k => ite_mul_pow _ _ _ _
    have e3 : ((List.range 8).map fun j => ((List.range 8).map fun k =>
            (if (x i).getLsbD j = true then 2^j else 0) *
            (if (y i).getLsbD k = true then 2^k else 0)).sum).sum
        = ((List.range 8).map fun j => ((List.range 8).map fun k =>
            if ((x i).getLsbD j && (y i).getLsbD k) = true then 2^(j+k) else 0).sum).sum := by
      apply sum_congr
      intro j _
      apply sum_congr
      intro k _
      exact e2 j k
    have e4 : (if m.getLsbD i = true then
            ((List.range 8).map fun j => ((List.range 8).map fun k =>
              if ((x i).getLsbD j && (y i).getLsbD k) = true then 2^(j+k) else 0).sum).sum else 0)
        = ((List.range 8).map fun j => ((List.range 8).map fun k =>
            if (m.getLsbD i && (x i).getLsbD j && (y i).getLsbD k) = true
            then 2^(j+k) else 0).sum).sum := by
      rw [ite_sum]
      apply sum_congr
      intro j _
      rw [ite_sum]
      apply sum_congr
      intro k _
      exact ite_ite_and3 _ _ _ _
    rw [e1, e3]
    exact e4
  have hmap : ((List.range 64).map fun i =>
        if m.getLsbD i = true then (x i).toNat * (y i).toNat else 0).sum
      = ((List.range 64).map fun i => ((List.range 8).map fun j =>
          ((List.range 8).map fun k =>
            if (m.getLsbD i && (x i).getLsbD j && (y i).getLsbD k) = true
            then 2^(j+k) else 0).sum).sum).sum :=
    sum_congr (fun i _ => hlane i)
  rw [hmap, sum_sum_sum_comm]
  apply sum_congr
  intro j _
  apply sum_congr
  intro k _
  rw [sum_ite_const]
  rw [← count_planes_dot x y m j k]


/-! ### List eraseDups counting infrastructure -/

private theorem nodup_eraseDups_aux {α : Type} [BEq α] [LawfulBEq α] :
    ∀ n : Nat, ∀ l : List α, l.length = n → (l.eraseDups).Nodup := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hl
      cases l with
      | nil => exact List.nodup_nil
      | cons a t =>
          rw [List.eraseDups_cons, List.nodup_cons]
          have hlt : (t.filter fun b => !b == a).length < n := by
            have hle : (t.filter fun b => !b == a).length ≤ t.length :=
              List.length_filter_le _ _
            simp at hl
            omega
          have ih' := ih _ hlt _ rfl
          constructor
          · intro hmem
            rw [List.mem_eraseDups, List.mem_filter] at hmem
            obtain ⟨_, hne⟩ := hmem
            have haa : (a == a) = true := beq_iff_eq.mpr rfl
            simp_all
          · exact ih'

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    (l.eraseDups).Nodup := nodup_eraseDups_aux _ _ rfl

private theorem eraseDups_length_perm {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (h : List.Perm l₁ l₂) :
    l₁.eraseDups.length = l₂.eraseDups.length := by
  have hperm : List.Perm l₁.eraseDups l₂.eraseDups := by
    rw [List.perm_ext_iff_of_nodup (nodup_eraseDups l₁) (nodup_eraseDups l₂)]
    intro a
    rw [List.mem_eraseDups, List.mem_eraseDups, h.mem_iff]
  exact hperm.length_eq

private theorem filter_map_pred {α β : Type} (g : α → β) (p : β → Bool) :
    ∀ l : List α, (l.map g).filter p = (l.filter fun a => p (g a)).map g := by
  intro l
  induction l with
  | nil => rfl
  | cons a t ih =>
      simp only [List.map_cons, List.filter_cons]
      by_cases h : p (g a) <;> simp [ih, h]

private theorem filter_map_beq {α β : Type} [BEq α] [LawfulBEq α]
    [BEq β] [LawfulBEq β] (g : α → β) (hg : Function.Injective g) (a : α) :
    ∀ l : List α, (l.map g).filter (fun b => !b == g a) =
      (l.filter fun b => !b == a).map g := by
  intro l
  rw [filter_map_pred]
  congr 1
  apply List.filter_congr
  intro c _
  have hbeq : (g c == g a) = (c == a) := by
    apply Bool.eq_iff_iff.mpr
    rw [beq_iff_eq, beq_iff_eq, hg.eq_iff]
  simp only [hbeq]

private theorem eraseDups_map_inj_aux {α β : Type} [BEq α] [LawfulBEq α]
    [BEq β] [LawfulBEq β] (g : α → β) (hg : Function.Injective g) :
    ∀ n : Nat, ∀ l : List α, l.length = n →
      (l.map g).eraseDups = l.eraseDups.map g := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hl
      cases l with
      | nil => rfl
      | cons a t =>
          have hlt : (t.filter fun b => !b == a).length < n := by
            have hle : (t.filter fun b => !b == a).length ≤ t.length :=
              List.length_filter_le _ _
            simp at hl
            omega
          rw [List.map_cons, List.eraseDups_cons, List.eraseDups_cons, List.map_cons]
          congr 1
          rw [filter_map_beq g hg a t]
          exact ih _ hlt _ rfl

private theorem eraseDups_map_inj {α β : Type} [BEq α] [LawfulBEq α]
    [BEq β] [LawfulBEq β] (g : α → β) (hg : Function.Injective g) (l : List α) :
    (l.map g).eraseDups = l.eraseDups.map g :=
  eraseDups_map_inj_aux g hg _ _ rfl

private theorem eraseDups_length_map_inj {α β : Type} [BEq α] [LawfulBEq α]
    [BEq β] [LawfulBEq β] (g : α → β) (hg : Function.Injective g) (l : List α) :
    (l.map g).eraseDups.length = l.eraseDups.length := by
  rw [eraseDups_map_inj g hg l, List.length_map]

private theorem removeAll_eq_self {α : Type} [BEq α] [LawfulBEq α]
    {xs ys : List α} (h : ∀ x ∈ ys, x ∉ xs) : ys.removeAll xs = ys := by
  show ys.filter (fun x => !xs.elem x) = ys
  rw [List.filter_eq_self]
  intro x hx
  have hxn : x ∉ xs := h x hx
  rw [List.elem_eq_mem]
  simp [hxn]

private theorem eraseDups_length_map_const {α : Type} [BEq α] [LawfulBEq α]
    (c : α) : ∀ l : List α, ((l.map fun _ => c).eraseDups).length =
      if l = [] then 0 else 1 := by
  intro l
  induction l with
  | nil => simp
  | cons a t ih =>
      rw [List.map_cons, List.eraseDups_cons]
      have hfil : ((t.map fun _ => c).filter fun b => !b == c) = [] := by
        rw [List.filter_eq_nil_iff]
        intro x hx
        rw [List.mem_map] at hx
        obtain ⟨y, -, rfl⟩ := hx
        simp
      rw [hfil]
      simp

/-! ### OccupiedCodes -/

private theorem mem_lanes_iff (m : W) (i : Nat) :
    i ∈ lanes m ↔ i < 64 ∧ m.getLsbD i = true := by
  show i ∈ (List.range 64).filter m.getLsbD ↔ i < 64 ∧ m.getLsbD i = true
  rw [List.mem_filter, List.mem_range]

private theorem lanes_eq_nil_iff (m : W) : lanes m = [] ↔ m == 0 := by
  show (List.range 64).filter m.getLsbD = [] ↔ m == 0
  rw [List.filter_eq_nil_iff]
  constructor
  · intro h
    have h0 : m = 0 := by
      apply eq_zero_of_getLsbD
      intro i hi
      have hh := h i (List.mem_range.mpr hi)
      cases hb : m.getLsbD i <;> simp_all
    rw [h0]
    simp
  · intro h
    have hm : m = 0 := beq_iff_eq.mp h
    subst hm
    intro i hi
    simp

private theorem mem_lanes_and_plane (x : Nat → B) (m : W) (n i : Nat) (hi : i < 64) :
    i ∈ lanes (m &&& planes x n) ↔ i ∈ lanes m ∧ (x i).getLsbD n = true := by
  rw [mem_lanes_iff, mem_lanes_iff]
  constructor
  · rintro ⟨hi64, hmi⟩
    rw [BitVec.getLsbD_and, planes_getLsbD x n i hi64, Bool.and_eq_true] at hmi
    exact ⟨⟨hi64, hmi.1⟩, hmi.2⟩
  · rintro ⟨⟨hi64, hmi⟩, hbit⟩
    refine ⟨hi64, ?_⟩
    rw [BitVec.getLsbD_and, planes_getLsbD x n i hi64, Bool.and_eq_true]
    exact ⟨hmi, hbit⟩

private theorem mem_lanes_and_not_plane (x : Nat → B) (m : W) (n i : Nat) (hi : i < 64) :
    i ∈ lanes (m &&& ~~~(planes x n)) ↔ i ∈ lanes m ∧ (x i).getLsbD n = false := by
  rw [mem_lanes_iff, mem_lanes_iff]
  constructor
  · rintro ⟨hi64, hmi⟩
    rw [BitVec.getLsbD_and, BitVec.getLsbD_not, planes_getLsbD x n i hi64,
        decide_eq_true hi64, Bool.true_and, Bool.and_eq_true] at hmi
    refine ⟨⟨hi64, hmi.1⟩, ?_⟩
    cases hb : (x i).getLsbD n <;> simp_all
  · rintro ⟨⟨hi64, hmi⟩, hbit⟩
    refine ⟨hi64, ?_⟩
    rw [BitVec.getLsbD_and, BitVec.getLsbD_not, planes_getLsbD x n i hi64,
        decide_eq_true hi64, Bool.true_and, hmi]
    cases hb : (x i).getLsbD n <;> simp_all

private theorem lanes_and_plane_eq_filter (x : Nat → B) (m : W) (n : Nat) :
    lanes (m &&& planes x n) = (lanes m).filter (fun i => (x i).getLsbD n) := by
  show (List.range 64).filter ((m &&& planes x n).getLsbD)
      = ((List.range 64).filter m.getLsbD).filter (fun i => (x i).getLsbD n)
  rw [List.filter_filter]
  apply List.filter_congr
  intro i hi
  have hi64 : i < 64 := List.mem_range.mp hi
  rw [BitVec.getLsbD_and, planes_getLsbD x n i hi64]
  exact Bool.and_comm _ _

private theorem lanes_and_not_plane_eq_filter (x : Nat → B) (m : W) (n : Nat) :
    lanes (m &&& ~~~(planes x n)) = (lanes m).filter (fun i => !(x i).getLsbD n) := by
  show (List.range 64).filter ((m &&& ~~~(planes x n)).getLsbD)
      = ((List.range 64).filter m.getLsbD).filter (fun i => !(x i).getLsbD n)
  rw [List.filter_filter]
  apply List.filter_congr
  intro i hi
  have hi64 : i < 64 := List.mem_range.mp hi
  rw [BitVec.getLsbD_and, BitVec.getLsbD_not, planes_getLsbD x n i hi64,
      decide_eq_true hi64, Bool.true_and]
  exact Bool.and_comm _ _

private theorem pat_succ_bit_val (x : Nat → B) (i n : Nat) :
    (x i).toNat % 2^(n+1) =
      (x i).toNat % 2^n + 2^n * (((x i).getLsbD n).toNat) := by
  have h := mod_two_pow_succ ((x i).toNat) n
  rwa [BitVec.testBit_toNat] at h

private theorem pat_succ_eq_add (x : Nat → B) (i n : Nat)
    (hb : (x i).getLsbD n = true) :
    (x i).toNat % 2^(n+1) = (x i).toNat % 2^n + 2^n := by
  rw [pat_succ_bit_val, hb]
  simp

private theorem pat_succ_eq (x : Nat → B) (i n : Nat)
    (hb : (x i).getLsbD n = false) :
    (x i).toNat % 2^(n+1) = (x i).toNat % 2^n := by
  rw [pat_succ_bit_val, hb]
  simp

private theorem occupied_eq_length (x : Nat → B) :
    ∀ n : Nat, ∀ m : W,
      occupied (planes x) m n =
        (((lanes m).map fun i => (x i).toNat % 2^n).eraseDups).length := by
  intro n
  induction n with
  | zero =>
      intro m
      show (if m == 0 then 0 else 1) = _
      have hmap : ((lanes m).map fun i => (x i).toNat % 2^0)
          = (lanes m).map fun _ => (0 : Nat) := by
        apply List.map_congr_left
        intro i _
        omega
      rw [hmap, eraseDups_length_map_const]
      by_cases h : lanes m = []
      · have hm : m == 0 := (lanes_eq_nil_iff m).mp h
        rw [ite_eq_left h, ite_eq_left hm]
      · rw [ite_eq_right h]
        have hm : ¬(m == 0) := fun h' => h ((lanes_eq_nil_iff m).mpr h')
        rw [ite_eq_right hm]
  | succ n ih =>
      intro m
      show (if m == 0 then 0 else
          occupied (planes x) (m &&& planes x n) n +
          occupied (planes x) (m &&& ~~~(planes x n)) n) = _
      by_cases hm : m == 0
      · rw [ite_eq_left hm]
        have he : lanes m = [] := (lanes_eq_nil_iff m).mpr hm
        rw [he]
        simp
      · rw [ite_eq_right hm]
        have hperm : List.Perm (lanes m)
            (List.filter (fun i => (x i).getLsbD n) (lanes m) ++
             List.filter (fun i => !(x i).getLsbD n) (lanes m)) := by
          have h := List.filter_append_perm (fun i => (x i).getLsbD n) (lanes m)
          exact h.symm
        have hmap_perm : List.Perm
            (List.map (fun i => (x i).toNat % 2^(n+1)) (lanes m))
            (List.map (fun i => (x i).toNat % 2^(n+1))
                (List.filter (fun i => (x i).getLsbD n) (lanes m)) ++
             List.map (fun i => (x i).toNat % 2^(n+1))
                (List.filter (fun i => !(x i).getLsbD n) (lanes m))) := by
          have h := hperm.map (fun i => (x i).toNat % 2^(n+1))
          rwa [List.map_append] at h
        have hA : (((lanes m).filter fun i => (x i).getLsbD n).map
            fun i => (x i).toNat % 2^(n+1)).eraseDups.length
            = occupied (planes x) (m &&& planes x n) n := by
          rw [← lanes_and_plane_eq_filter x m n]
          have hmap : ((lanes (m &&& planes x n)).map fun i => (x i).toNat % 2^(n+1))
              = ((lanes (m &&& planes x n)).map fun i => (x i).toNat % 2^n).map (· + 2^n) := by
            rw [List.map_map]
            apply List.map_congr_left
            intro i hi
            have hi64 : i < 64 := (mem_lanes_iff _ i).mp hi |>.1
            have hb : (x i).getLsbD n = true :=
              ((mem_lanes_and_plane x m n i hi64).mp hi).2
            exact pat_succ_eq_add x i n hb
          rw [hmap]
          have hinj : Function.Injective (· + 2^n) :=
            fun a b h => Nat.add_right_cancel h
          rw [eraseDups_length_map_inj _ hinj]
          exact (ih _).symm
        have hB : (((lanes m).filter fun i => !(x i).getLsbD n).map
            fun i => (x i).toNat % 2^(n+1)).eraseDups.length
            = occupied (planes x) (m &&& ~~~(planes x n)) n := by
          rw [← lanes_and_not_plane_eq_filter x m n]
          have hmap : ((lanes (m &&& ~~~(planes x n))).map fun i => (x i).toNat % 2^(n+1))
              = (lanes (m &&& ~~~(planes x n))).map fun i => (x i).toNat % 2^n := by
            apply List.map_congr_left
            intro i hi
            have hi64 : i < 64 := (mem_lanes_iff _ i).mp hi |>.1
            have hb : (x i).getLsbD n = false :=
              ((mem_lanes_and_not_plane x m n i hi64).mp hi).2
            exact pat_succ_eq x i n hb
          rw [hmap]
          exact (ih _).symm
        have hdisj : ∀ v ∈ ((lanes m).filter fun i => !(x i).getLsbD n).map
            (fun i => (x i).toNat % 2^(n+1)),
            v ∉ ((lanes m).filter fun i => (x i).getLsbD n).map
              (fun i => (x i).toNat % 2^(n+1)) := by
          intro v hv hvmem
          rw [List.mem_map] at hv hvmem
          obtain ⟨i, hi, rfl⟩ := hv
          obtain ⟨j, hj, hij⟩ := hvmem
          rw [List.mem_filter] at hi hj
          obtain ⟨-, hni⟩ := hi
          obtain ⟨-, hbj⟩ := hj
          have hbi : (x i).getLsbD n = false := by
            cases hb : (x i).getLsbD n <;> simp_all
          rw [pat_succ_eq x i n hbi] at hij
          rw [pat_succ_eq_add x j n hbj] at hij
          have hlt : (x i).toNat % 2^n < 2^n := Nat.mod_lt _ (Nat.two_pow_pos _)
          omega
        rw [eraseDups_length_perm hmap_perm, List.eraseDups_append,
            removeAll_eq_self hdisj, List.length_append, hA, hB]


theorem sound_OccupiedCodes : OccupiedCodes.Goal := by
  intro x m
  have h := occupied_eq_length x 8 m
  -- h : occupied (planes x) m 8 = (((lanes m).map fun i => (x i).toNat % 2^8).eraseDups).length
  rw [h]
  -- need: ((lanes m).map fun i => (x i).toNat % 256).eraseDups.length
  --     = ((lanes m).map x).eraseDups.length
  have hmod : ((lanes m).map fun i => (x i).toNat % 2^8)
      = ((lanes m).map x).map BitVec.toNat := by
    rw [List.map_map]
    apply List.map_congr_left
    intro i _
    have hlt : (x i).toNat < 2^8 := (x i).isLt
    exact Nat.mod_eq_of_lt hlt
  rw [hmod, eraseDups_map_inj _ (fun a b h => BitVec.toNat_inj.mp h)]
  rw [List.length_map]

/-! ### PlaneAdd: ripple-carry adder spec -/

/-- Carry into bit `k` of the ripple adder over planes `a b`. -/
private def carrySeq (a b : Planes) : Nat → W
  | 0 => 0
  | k+1 => (a k &&& b k) ||| ((a k ^^^ b k) &&& carrySeq a b k)

/-- Plane `j` of the sum: `a j ^^^ b j` plus carry-in. -/
private def sumBit (a b : Planes) (j : Nat) : W := a j ^^^ b j ^^^ carrySeq a b j

private theorem add_foldl_spec (a b : Planes) (k : Nat) :
    (List.range k).foldl
        (fun (out, c) j =>
          let p := a j ^^^ b j
          ((p ^^^ c)::out, (a j &&& b j) ||| (p &&& c)))
        ([], 0)
      = ((List.range k).reverse.map (sumBit a b), carrySeq a b k) := by
  induction k with
  | zero => rfl
  | succ k ih =>
      rw [List.range_succ, List.foldl_append, ih]
      simp only [List.foldl_cons, List.foldl_nil]
      show ((a k ^^^ b k ^^^ carrySeq a b k) :: ((List.range k).reverse.map (sumBit a b)),
            (a k &&& b k) ||| ((a k ^^^ b k) &&& carrySeq a b k))
        = (List.map (sumBit a b) (List.range k ++ [k]).reverse, carrySeq a b (k+1))
      refine Prod.ext_iff.mpr ⟨?_, rfl⟩
      rw [List.reverse_append, List.map_append]
      simp [List.map_cons, List.map_nil, sumBit]


/-- Bit `i` of the carry into plane `k` is `1` iff the low `k` bits of the
two lane values already overflow `2^k`. -/
private theorem carrySeq_planes_getLsbD (x y : Nat → B) (k i : Nat) (hi : i < 64) :
    (carrySeq (planes x) (planes y) k).getLsbD i
      = decide (2^k ≤ (x i).toNat % 2^k + (y i).toNat % 2^k) := by
  induction k with
  | zero =>
      show ((0 : W).getLsbD i) = decide (2^0 ≤ (x i).toNat % 2^0 + (y i).toNat % 2^0)
      simp [Nat.mod_one]
  | succ k ih =>
      show (((planes x k &&& planes y k) |||
          ((planes x k ^^^ planes y k) &&& carrySeq (planes x) (planes y) k))).getLsbD i
        = decide (2^(k+1) ≤ (x i).toNat % 2^(k+1) + (y i).toNat % 2^(k+1))
      rw [BitVec.getLsbD_or, BitVec.getLsbD_and, BitVec.getLsbD_and, BitVec.getLsbD_xor,
          planes_getLsbD x k i hi, planes_getLsbD y k i hi, ih]
      rw [show (x i).getLsbD k = (x i).toNat.testBit k from rfl,
          show (y i).getLsbD k = (y i).toNat.testBit k from rfl]
      rw [mod_two_pow_succ (x i).toNat k, mod_two_pow_succ (y i).toNat k]
      have hpow : 2^(k+1) = 2^k + 2^k := Nat.two_pow_succ k
      have hbx : (x i).toNat % 2^k < 2^k := Nat.mod_lt _ (Nat.two_pow_pos _)
      have hby : (y i).toNat % 2^k < 2^k := Nat.mod_lt _ (Nat.two_pow_pos _)
      cases hA : (x i).toNat.testBit k <;> cases hB : (y i).toNat.testBit k <;>
        simp [hA, hB, Bool.toNat_true, Bool.toNat_false] <;>
        first
        | rfl
        | omega
        | (constructor <;> intro h <;> omega)
        | exact ((decide_eq_true _).mpr (by omega)).symm
        | exact ((decide_eq_false _).mpr (by omega)).symm
        | (rw [decide_eq_decide]; constructor <;> intro h <;> omega)


/-- Bit `j` of `xn + yn` equals `xn_j ^^ yn_j ^^ carry_j`, where `carry_j` is
the carry into bit `j` generated by the low `j` bits. -/
private theorem add_testBit (xn yn j : Nat) :
    (xn + yn).testBit j =
      (((xn.testBit j) ^^ (yn.testBit j)) ^^ decide (2^j ≤ xn % 2^j + yn % 2^j)) := by
  have hpow : 2^(j+1) = 2^j + 2^j := Nat.two_pow_succ j
  generalize hsdef : xn % 2^j + yn % 2^j = s
  have hslt : s < 2^(j+1) := by
    rw [← hsdef]
    have h1 : xn % 2^j < 2^j := Nat.mod_lt _ (Nat.two_pow_pos _)
    have h2 : yn % 2^j < 2^j := Nat.mod_lt _ (Nat.two_pow_pos _)
    omega
  have hrlt : s % 2^j < 2^j := Nat.mod_lt s (Nat.two_pow_pos _)
  -- the carry predicate equals `s / 2^j` as a natural number
  have hcb_eq : (decide (2^j ≤ s)).toNat = s / 2^j := by
    by_cases hP : 2^j ≤ s
    · have hc1 : s / 2^j = 1 := by
        apply (Nat.div_eq_iff (Nat.two_pow_pos _)).mpr
        refine ⟨?_, ?_⟩ <;> omega
      rw [decide_eq_true hP, hc1]
      rfl
    · have hc0 : s / 2^j = 0 := by
        apply Nat.div_eq_of_lt
        have := Nat.not_le.mp hP
        omega
      rw [decide_eq_false hP, hc0]
      rfl
  -- mod-2^{j+1} decompositions
  have hmodx : xn % 2^(j+1) = xn % 2^j + 2^j * (xn.testBit j).toNat := mod_two_pow_succ xn j
  have hmody : yn % 2^(j+1) = yn % 2^j + 2^j * (yn.testBit j).toNat := mod_two_pow_succ yn j
  have hxn : xn = xn % 2^(j+1) + 2^(j+1) * (xn / 2^(j+1)) := (Nat.mod_add_div xn _).symm
  have hyn : yn = yn % 2^(j+1) + 2^(j+1) * (yn / 2^(j+1)) := (Nat.mod_add_div yn _).symm
  have hlow : xn % 2^(j+1) + yn % 2^(j+1)
      = s + 2^j * ((xn.testBit j).toNat + (yn.testBit j).toNat) := by
    rw [hmodx, hmody, ← hsdef, Nat.mul_add]
    ac_rfl
  have hs_eq : s = s % 2^j + 2^j * (s / 2^j) := (Nat.mod_add_div s (2^j)).symm
  have hsum1 : xn + yn = (xn % 2^(j+1) + yn % 2^(j+1))
      + 2^(j+1) * (xn / 2^(j+1) + yn / 2^(j+1)) := by
    conv => lhs; rw [hxn, hyn]
    rw [Nat.mul_add]
    ac_rfl
  have hdecomp : xn + yn = s % 2^j
      + 2^j * ((xn.testBit j).toNat + (yn.testBit j).toNat + s / 2^j)
      + 2^(j+1) * (xn / 2^(j+1) + yn / 2^(j+1)) := by
    have h1 := hsum1
    rw [hlow] at h1
    rw [hs_eq] at h1
    refine h1.trans ?_
    rw [Nat.mul_add (2^j) ((xn.testBit j).toNat + (yn.testBit j).toNat) (s / 2^j)]
    ac_rfl
  -- division by 2^j
  have hQ : (2^(j+1) * (xn / 2^(j+1) + yn / 2^(j+1)))
      = 2^j * ((xn / 2^(j+1) + yn / 2^(j+1)) + (xn / 2^(j+1) + yn / 2^(j+1))) := by
    rw [hpow, Nat.add_mul, ← Nat.mul_add]
  have hnum : s % 2^j
      + 2^j * ((xn.testBit j).toNat + (yn.testBit j).toNat + s / 2^j)
      + 2^j * ((xn / 2^(j+1) + yn / 2^(j+1)) + (xn / 2^(j+1) + yn / 2^(j+1)))
      = 2^j * (((xn.testBit j).toNat + (yn.testBit j).toNat + s / 2^j)
          + ((xn / 2^(j+1) + yn / 2^(j+1)) + (xn / 2^(j+1) + yn / 2^(j+1))))
        + s % 2^j := by
    rw [Nat.add_assoc, ← Nat.mul_add]
    exact Nat.add_comm _ _
  have hdiv : (xn + yn) / 2^j
      = (xn.testBit j).toNat + (yn.testBit j).toNat + s / 2^j
        + (xn / 2^(j+1) + yn / 2^(j+1)) + (xn / 2^(j+1) + yn / 2^(j+1)) := by
    rw [hdecomp, hQ, hnum]
    rw [Nat.mul_add_div (Nat.two_pow_pos _) _ _]
    have hr0 : s % 2^j / 2^j = 0 := Nat.div_eq_of_lt hrlt
    rw [hr0, Nat.add_zero]
    omega
  have hmod2 : (xn + yn) / 2^j % 2
      = ((xn.testBit j).toNat + (yn.testBit j).toNat + s / 2^j) % 2 := by
    rw [hdiv]
    omega
  rw [Nat.testBit_eq_decide_div_mod_eq, hmod2]
  -- replace `s / 2^j` by the Bool carry's toNat
  rw [← hcb_eq]
  -- finite case analysis on the three bits
  cases hA : xn.testBit j <;> cases hB : yn.testBit j <;>
    cases hcb : decide (2^j ≤ s) <;>
    simp [hA, hB, hcb, Bool.toNat_true, Bool.toNat_false]

/-- Bit `i` of plane `j` of the bit-sliced sum equals bit `j` of `x i + y i`. -/
private theorem sumBit_planes_getLsbD (x y : Nat → B) (j i : Nat)
    (hi : i < 64) (hj : j < 8) :
    (sumBit (planes x) (planes y) j).getLsbD i
      = (planes (fun i => x i + y i) j).getLsbD i := by
  show (((planes x j ^^^ planes y j) ^^^ carrySeq (planes x) (planes y) j).getLsbD i
      = (planes (fun i => x i + y i) j).getLsbD i)
  rw [BitVec.getLsbD_xor, BitVec.getLsbD_xor,
      planes_getLsbD x j i hi, planes_getLsbD y j i hi,
      carrySeq_planes_getLsbD x y j i hi]
  have hRHS : (planes (fun i => x i + y i) j).getLsbD i
      = ((x i + y i).toNat).testBit j := by
    show (pack fun i => (x i + y i).getLsbD j).getLsbD i = _
    exact pack_getLsbD _ _ hi
  rw [hRHS]
  show (((x i).toNat.testBit j) ^^ ((y i).toNat.testBit j) ^^
      (decide (2^j ≤ (x i).toNat % 2^j + (y i).toNat % 2^j)))
    = (((x i + y i).toNat).testBit j)
  rw [BitVec.toNat_add, Nat.testBit_mod_two_pow, decide_eq_true hj]
  simp only [Bool.true_and]
  exact (add_testBit ((x i).toNat) ((y i).toNat) j).symm

private theorem sumBit_planes (x y : Nat → B) (j : Nat) (hj : j < 8) :
    sumBit (planes x) (planes y) j = planes (fun i => x i + y i) j := by
  apply (BitVec.eq_of_getLsbD_eq_iff).mpr
  intro i hi
  exact sumBit_planes_getLsbD x y j i hi hj

/-- `add` produces sum planes `sumBit` and final carry `carrySeq`. -/
private theorem add_eq_sumBit_carry (a b : Planes) :
    add a b = ((List.range 8).map (sumBit a b), carrySeq a b 8) := by
  unfold add
  rw [add_foldl_spec a b 8]
  show (((List.range 8).reverse.map (sumBit a b)).reverse, carrySeq a b 8)
      = ((List.range 8).map (sumBit a b), carrySeq a b 8)
  have hrev : ((List.range 8).reverse.map (sumBit a b)).reverse
      = (List.range 8).map (sumBit a b) := by
    rw [List.map_reverse, List.reverse_reverse]
  rw [hrev]

theorem sound_PlaneAdd : PlaneAdd.Goal := by
  intro x y
  rw [add_eq_sumBit_carry (planes x) (planes y)]
  constructor
  · refine List.map_congr_left ?_
    intro j hj
    exact sumBit_planes x y j (List.mem_range.mp hj)
  · apply (BitVec.eq_of_getLsbD_eq_iff).mpr
    intro i hi
    rw [carrySeq_planes_getLsbD x y 8 i hi, pack_getLsbD _ _ hi]
    have hx : (x i).toNat < 256 := (x i).isLt
    have hy : (y i).toNat < 256 := (y i).isLt
    rw [Nat.mod_eq_of_lt hx, Nat.mod_eq_of_lt hy]


end TinylambOpt.ResearchProofs.ExecutionStrategies.PlaneKernels
