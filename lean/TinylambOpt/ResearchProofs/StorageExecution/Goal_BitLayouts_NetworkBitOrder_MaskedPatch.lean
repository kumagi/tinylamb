import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_MaskedPatch

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder

-- Truth checks on small inputs (masked patch preserves every bit cell).
#eval image layout 3 ([1, 2, 3].set 1 7) ==
  ((image layout 3 [1, 2, 3] ^^^ (image layout 3 [1, 2, 3] &&& rowMask layout 3 3 1)) ||| rowBits layout 3 3 1 7)
#eval image layout 0 [] == 0
#eval image layout 5 ([31, 0, 7, 255, 16].set 2 0) ==
  ((image layout 5 [31, 0, 7, 255, 16] ^^^ (image layout 5 [31, 0, 7, 255, 16] &&& rowMask layout 5 5 2)) ||| rowBits layout 5 5 2 0)
#eval image layout 4 ([0, 0, 0, 0].set 3 15) ==
  ((image layout 4 [0, 0, 0, 0] ^^^ (image layout 4 [0, 0, 0, 0] &&& rowMask layout 4 4 3)) ||| rowBits layout 4 4 3 15)

/-! ## Generic test-bit infrastructure -/

private theorem testBit_foldl_if_or {α : Type} (l : List α) (f : α → Nat)
    (c : α → Bool) (acc p : Nat) :
    (l.foldl (fun a x => if c x then a ||| f x else a) acc).testBit p =
      (acc.testBit p || l.any (fun x => c x && (f x).testBit p)) := by
  induction l generalizing acc with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons, List.any_cons]
      have hih := ih (if c x then acc ||| f x else acc)
      exact hih.trans (by
        by_cases hc : c x = true
        · simp [hc, Nat.testBit_or, Bool.or_assoc]
        · have hc' : c x = false := by cases h : c x <;> simp_all
          simp [hc'])

private theorem testBit_foldl_or {α : Type} (l : List α) (f : α → Nat)
    (acc p : Nat) :
    (l.foldl (fun a x => a ||| f x) acc).testBit p =
      (acc.testBit p || l.any (fun x => (f x).testBit p)) := by
  simpa using (testBit_foldl_if_or l f (fun _ => true) acc p)

/-- A conditional-OR fold started from `acc` is `acc` OR the same fold from zero. -/
private theorem foldl_cond_or_acc {α : Type} (l : List α) (p : α → Bool)
    (f : α → Nat) (acc : Nat) :
    l.foldl (fun a b => if p b then a ||| f b else a) acc =
      acc ||| l.foldl (fun a b => if p b then a ||| f b else a) 0 := by
  induction l generalizing acc with
  | nil => simp
  | cons b bs ih =>
      by_cases hp : p b = true
      · simp only [List.foldl_cons, hp, ite_true]
        rw [ih (acc ||| f b), ih (0 ||| f b)]
        simp only [Nat.zero_or, Nat.or_assoc]
      · simp only [List.foldl_cons, hp, ite_false]
        exact ih acc

private theorem rowMask_testBit (l : Layout) (n w i p : Nat) :
    (rowMask l n w i).testBit p =
      (lanes w).any (fun b => decide (l.bit n w i b = p)) := by
  unfold rowMask
  simpa [Nat.testBit_two_pow] using
    (testBit_foldl_or (lanes w) (fun b : Nat => 2 ^ l.bit n w i b) 0 p)

private theorem rowBits_testBit (l : Layout) (n w i x p : Nat) :
    (rowBits l n w i x).testBit p =
      (lanes w).any (fun b => x.testBit b && decide (l.bit n w i b = p)) := by
  unfold rowBits
  simpa [Nat.testBit_two_pow] using
    (testBit_foldl_if_or (lanes w) (fun b : Nat => 2 ^ l.bit n w i b)
      (fun b : Nat => x.testBit b) 0 p)

private theorem any_zipIdx_eq_range {α : Type} (xs : List α) (f : α → Nat → Bool) :
    xs.zipIdx.any (fun z => f z.1 z.2) =
      (List.range xs.length).any (fun j => (xs[j]?).any (fun x => f x j)) := by
  apply Bool.eq_iff_iff.mpr
  simp only [List.any_eq_true]
  constructor
  · rintro ⟨⟨x, j⟩, hz, hf⟩
    have hmem := List.mem_zipIdx_iff_getElem?.mp hz
    have hlt : j < xs.length := (List.getElem?_eq_some_iff.mp hmem).1
    refine ⟨j, List.mem_range.mpr hlt, ?_⟩
    rw [Option.any_eq_true]
    exact ⟨x, hmem, hf⟩
  · rintro ⟨j, hj, hopt⟩
    have hlt : j < xs.length := List.mem_range.mp hj
    rw [Option.any_eq_true] at hopt
    rcases hopt with ⟨x, hmem, hf⟩
    exact ⟨(x, j), List.mem_zipIdx_iff_getElem?.mpr hmem, hf⟩

private theorem image_eq_foldl_or (l : Layout) (w : Nat) (xs : List Nat) :
    image l w xs =
      xs.zipIdx.foldl (fun acc q => acc ||| rowBits l xs.length w q.2 q.1) 0 := by
  unfold image rowBits
  symm
  refine List.foldl_hom id ?_
  intro acc q
  exact (foldl_cond_or_acc (lanes w) (fun b => q.1.testBit b)
    (fun b => 2 ^ l.bit xs.length w q.2 b) acc).symm

private theorem image_testBit (l : Layout) (w : Nat) (xs : List Nat) (p : Nat) :
    (image l w xs).testBit p =
      (List.range xs.length).any (fun i => (lanes w).any
        (fun b => (xs[i]?.getD 0).testBit b && decide (l.bit xs.length w i b = p))) := by
  rw [image_eq_foldl_or]
  have hfold := testBit_foldl_or xs.zipIdx
    (fun z : Nat × Nat => rowBits l xs.length w z.2 z.1) 0 p
  simp at hfold
  refine hfold.trans ?_
  calc
    xs.zipIdx.any (fun z => (rowBits l xs.length w z.2 z.1).testBit p)
        = xs.zipIdx.any (fun z => (lanes w).any
          (fun b => z.1.testBit b && decide (l.bit xs.length w z.2 b = p))) := by
            congr 1
            funext z
            exact rowBits_testBit l xs.length w z.2 z.1 p
    _ = (List.range xs.length).any (fun i => (xs[i]?).any (fun x => (lanes w).any
          (fun b => x.testBit b && decide (l.bit xs.length w i b = p)))) := by
            exact any_zipIdx_eq_range xs (fun x i => (lanes w).any
              (fun b => x.testBit b && decide (l.bit xs.length w i b = p)))
    _ = (List.range xs.length).any (fun i => (lanes w).any
          (fun b => (xs[i]?.getD 0).testBit b && decide (l.bit xs.length w i b = p))) := by
            congr 1
            funext i
            by_cases hi : i < xs.length
            · have hs : xs[i]? = some (xs[i]?.getD 0) := by
                rw [List.getElem?_eq_getElem hi]
                simp
              rw [hs]
              simp
            · have hn : xs[i]? = none := by
                rw [List.getElem?_eq_none]
                omega
              rw [hn]
              simp

/-! ## NetworkBitOrder injectivity -/

/-- Reversing the three low bits of each byte is an involution. -/
private def rev8 (p : Nat) : Nat := p / 8 * 8 + (7 - p % 8)

private theorem rev8_involutive (p : Nat) : rev8 (rev8 p) = p := by
  unfold rev8
  have hmod : p % 8 < 8 := Nat.mod_lt p (by omega)
  have h1 : (p / 8 * 8 + (7 - p % 8)) / 8 = p / 8 := by omega
  have h2 : (p / 8 * 8 + (7 - p % 8)) % 8 = 7 - p % 8 := by omega
  rw [h1, h2]
  have h3 : 7 - (7 - p % 8) = p % 8 := by omega
  rw [h3]
  simpa [Nat.mul_comm] using Nat.div_add_mod p 8

/-- For rows below `n`, the tight position `i*w + b` is unique, and the byte-wise
bit reversal is injective, so distinct (row, lane) pairs own distinct bit cells. -/
private theorem injective_bits : ∀ n w i j a b, i < n → j < n → a < w → b < w →
    layout.bit n w i a = layout.bit n w j b → i = j ∧ a = b := by
  intro n w i j a b hi hj ha hb h
  change rev8 (i * w + a) = rev8 (j * w + b) at h
  have hpq : i * w + a = j * w + b := by
    have hh := congrArg rev8 h
    simpa [rev8_involutive] using hh
  have hw : 0 < w := by omega
  have hab : a = b := by
    have hm := congrArg (fun t : Nat => t % w) hpq
    rw [show (i * w + a) % w = a by
          rw [Nat.add_comm, Nat.mul_comm i w, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt ha],
        show (j * w + b) % w = b by
          rw [Nat.add_comm, Nat.mul_comm j w, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hb]] at hm
    exact hm
  subst b
  have hiw : i * w = j * w := by omega
  exact ⟨Nat.eq_of_mul_eq_mul_right hw hiw, rfl⟩

/-! ## Reading a known bit cell from the image -/

private theorem image_bit_at (w : Nat) (xs : List Nat) {i b p : Nat}
    (hi : i < xs.length) (hb : b < w)
    (hno : ∀ j k a c, j < xs.length → k < xs.length → a < w → c < w →
      layout.bit xs.length w j a = layout.bit xs.length w k c → j = k ∧ a = c)
    (hp : layout.bit xs.length w i b = p) :
    (image layout w xs).testBit p = (xs[i]?.getD 0).testBit b := by
  rw [image_testBit]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    obtain ⟨j, hj, hrow⟩ := List.any_eq_true.mp h
    have hjlen : j < xs.length := List.mem_range.mp hj
    obtain ⟨a, ha, hval⟩ := List.any_eq_true.mp hrow
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hval
    rcases hval with ⟨hxa, heq⟩
    have haw : a < w := by simpa [lanes] using ha
    have hji : j = i ∧ a = b := hno j i a b hjlen hi haw hb (heq.trans hp.symm)
    obtain ⟨rfl, rfl⟩ := hji
    simpa using hxa
  · intro h
    rw [List.any_eq_true]
    refine ⟨i, List.mem_range.mpr hi, ?_⟩
    rw [List.any_eq_true]
    refine ⟨b, by simp [lanes, hb], ?_⟩
    simp [h, hp]

/-- If row `i` has no bit cell at `p`, changing it leaves `p` untouched. -/
private theorem image_away_from_row (w : Nat) (xs : List Nat) (i x p : Nat)
    (hi : i < xs.length) (haway : ∀ b, b < w → layout.bit xs.length w i b ≠ p) :
    (image layout w (xs.set i x)).testBit p = (image layout w xs).testBit p := by
  have hlen : (xs.set i x).length = xs.length := List.length_set ..
  rw [image_testBit, image_testBit]
  rw [hlen]
  have hrowAway : ∀ y : Nat,
      (lanes w).any (fun b => y.testBit b && decide (layout.bit xs.length w i b = p)) = false := by
    intro y
    apply Bool.eq_false_iff.mpr
    intro h
    obtain ⟨b, hb, hval⟩ := List.any_eq_true.mp h
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hval
    exact haway b (by simpa [lanes] using hb) hval.2
  congr 1
  funext j
  by_cases hji : j = i
  · subst j
    rw [show (xs.set i x)[i]?.getD 0 = x by simp [List.getElem?_set, hi]]
    rw [hrowAway x, hrowAway (xs[i]?.getD 0)]
  · have hget : (xs.set i x)[j]?.getD 0 = xs[j]?.getD 0 := by
      simp [List.getElem?_set, hji, Ne.symm hji]
    simp [hget]

/-! ## The goal -/

theorem sound_MaskedPatch : NetworkBitOrder.MaskedPatch.Goal := by
  intro w xs i x hi
  let n := xs.length
  have hlen : (xs.set i x).length = n := by simp [n]
  have hno : ∀ j k a c, j < n → k < n → a < w → c < w →
      layout.bit n w j a = layout.bit n w k c → j = k ∧ a = c := by
    intro j k a c hj hk ha hc h
    exact injective_bits n w j k a c hj hk ha hc h
  apply Nat.eq_of_testBit_eq
  intro p
  rw [Nat.testBit_or, Nat.testBit_xor, Nat.testBit_and]
  rw [rowMask_testBit, rowBits_testBit]
  by_cases hm : (lanes w).any (fun b => decide (layout.bit n w i b = p)) = true
  · obtain ⟨b0, hb0mem, hdec⟩ := List.any_eq_true.mp hm
    have hb0 : b0 < w := by simpa [lanes] using hb0mem
    have hp0 : layout.bit n w i b0 = p := by simpa using hdec
    have hIold : (image layout w xs).testBit p = (xs[i]?.getD 0).testBit b0 := by
      exact image_bit_at w xs hi hb0 hno hp0
    have hInew : (image layout w (xs.set i x)).testBit p = x.testBit b0 := by
      have hres := image_bit_at w (xs.set i x) (by simpa [hlen] using hi) hb0
        (by simpa [hlen] using hno) hp0
      simpa [List.getElem?_set, hi] using hres
    have hRnew : (lanes w).any
        (fun b => x.testBit b && decide (layout.bit n w i b = p)) = x.testBit b0 := by
      apply Bool.eq_iff_iff.mpr
      constructor
      · intro h
        obtain ⟨b, hbmem, hval⟩ := List.any_eq_true.mp h
        simp only [Bool.and_eq_true, decide_eq_true_eq] at hval
        rcases hval with ⟨hxb, heq⟩
        have hb : b < w := by simpa [lanes] using hbmem
        have hbb0 : b = b0 := (hno i i b b0 hi hi hb hb0 (heq.trans hp0.symm)).2
        simpa [hbb0] using hxb
      · intro h
        exact List.any_eq_true.mpr ⟨b0, hb0mem, by simp [h, hp0]⟩
    rw [hInew, hIold, hRnew, hm]
    cases (xs[i]?.getD 0).testBit b0 <;> cases x.testBit b0 <;> rfl
  · have hmf : (lanes w).any (fun b => decide (layout.bit n w i b = p)) = false := by
      cases h : (lanes w).any (fun b => decide (layout.bit n w i b = p)) <;> simp_all
    have hRnew : (lanes w).any
        (fun b => x.testBit b && decide (layout.bit n w i b = p)) = false := by
      apply Bool.eq_false_iff.mpr
      intro h
      obtain ⟨b, hbmem, hval⟩ := List.any_eq_true.mp h
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hval
      have hmtrue : (lanes w).any (fun b => decide (layout.bit n w i b = p)) = true :=
        List.any_eq_true.mpr ⟨b, hbmem, by simp [hval.2]⟩
      rw [hmf] at hmtrue
      cases hmtrue
    have haway : ∀ b, b < w → layout.bit n w i b ≠ p := by
      intro b hb heq
      have : (lanes w).any (fun a => decide (layout.bit n w i a = p)) = true :=
        List.any_eq_true.mpr ⟨b, by simp [lanes, hb], by simp [heq]⟩
      rw [hmf] at this
      cases this
    have hIagree : (image layout w (xs.set i x)).testBit p =
        (image layout w xs).testBit p := by
      exact image_away_from_row w xs i x p hi haway
    rw [hIagree, hmf, hRnew]
    simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_NetworkBitOrder_MaskedPatch
