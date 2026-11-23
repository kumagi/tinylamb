import Std.Tactic

/-! Translation validation for the short filter kernels emitted by the JIT
    (C++ counterpart: `expression/jit.cpp`).

    Actual textual LLVM parsing is out of scope: the emitted IR is modelled as
    *structured data* (`Instr`).  We define

      * `spec`  -- the scalar reference (mathematical interval test, masked
                   reads, allocation-range check, first error, read addresses);
      * `check` -- a checker that accepts only the one canonical safe kernel
                   shape (plain wrapping `sub`, unsigned compare, *masked*
                   load, guarded `select`), with explicit parameter validity;
      * `interp` -- an interpreter for the IR including the dangerous forms
                   (`sub nsw`, unguarded full-width load, `cttz` with its
                   zero-time contract, unknown instructions).

    A lane is one `Int`.  `W >= 2` is the lane width, `M = 2^W`,
    `H = 2^(W-1)`.  Lane inputs and the interval endpoints must lie in the
    signed range `[-H, H-1]`.  Wrap-around subtraction is `z % M`.

    Soundness: if `check` accepts a program, then for **all** lane inputs the
    interpreter refines the scalar spec (same values, same first error, same
    read-address sequence).  Two negative variants are rejected and are
    concretely refuted: replacing a wrapping `sub` by `sub nsw` over the full
    MIN..MAX interval, and doing a full-width load followed by `select`.
-/

namespace TinylambOpt.Research.IrTranslationValidation

/-- Error kinds observable at a lane. -/
inductive Err where
  | oob (addr : Nat)
  | nswOverflow
  | cttzZero
  | unknown
  deriving DecidableEq, Repr

/-- The tiny IR.  Registers are addressed by natural indices; register `0`
    holds the lane input.  Every instruction appends one register except
    `halt`, which publishes a register as the lane result. -/
inductive Instr where
  | const (v : Int)
  | subWrap (a b : Nat)
  | subNsw (a b : Nat)
  | icmpUle (a b : Nat)
  | loadMasked (base : Nat) (maskReg : Nat)
  | loadFull (base : Nat)
  | select (c t f : Nat)
  | cttz (a : Nat)
  | halt (r : Nat)
  | unknown (name : String)
  deriving DecidableEq, Repr

/-- The full observation: result vector, first error (lane index + reason),
    and the sequence of memory addresses read. -/
structure Obs where
  values : List Int
  firstError : Option (Nat × Err)
  reads : List Nat
  deriving DecidableEq, Repr

/-- Per-lane intermediate state of the interpreter. -/
structure LState where
  regs : List Int
  out : Int
  err : Option Err
  read : Option Nat

/-- Per-lane observation. -/
structure LResult where
  out : Int
  err : Option Err
  read : Option Nat

/-- `z mod M`, the unsigned view of a wrapped two's-complement lane. -/
def wrap (M z : Int) : Int := z % M

/-- Signed lane range predicate. -/
def inSigned (W : Nat) (z : Int) : Prop :=
  -((2 : Int) ^ (W - 1)) ≤ z ∧ z ≤ (2 : Int) ^ (W - 1) - 1

/-! ## `cttz` semantics, including the zero-time contract -/

def cttzGo : Nat → Int → Nat
  | 0, _ => 0
  | n + 1, z => if z % 2 = 0 then 1 + cttzGo n (z / 2) else 0

/-- `cttz 0 = W` is the specified zero-time contract; a guard must exclude it
    before the result is trusted. -/
def cttzVal (W : Nat) (z : Int) : Nat :=
  if z = 0 then W else cttzGo W z

theorem cttzVal_zero (W : Nat) : cttzVal W 0 = W := by
  simp [cttzVal]

theorem cttzGo_nonneg (n : Nat) (z : Int) : 0 ≤ cttzGo n z := by
  induction n with
  | zero => simp [cttzGo]
  | succ n ih =>
      by_cases h : z % 2 = 0
      · simp [cttzGo, h]
      · simp [cttzGo, h]

/-! ## Wrapped-subtraction range lemma -/

/-- If `-M < z < M` then `z mod M` is `z` or `z + M`. -/
theorem emod_eq_self_or_add (z M : Int) (hM : 0 < M) (hl : -M < z) (hu : z < M) :
    z % M = z ∨ z % M = z + M := by
  by_cases hz : 0 ≤ z
  · exact Or.inl (Int.emod_eq_of_lt hz hu)
  · right
    have hnonneg : 0 ≤ z + M := by omega
    have hlt : z + M < M := by omega
    have h1 : (z + M) % M = z + M := Int.emod_eq_of_lt hnonneg hlt
    have h2 : (z + M) % M = z % M := by
      rw [show z + M = z + 1 * M by omega, Int.add_mul_emod_self_right]
    omega

instance (W : Nat) (z : Int) : Decidable (inSigned W z) := by
  unfold inSigned
  infer_instance

theorem pow_split (W : Nat) (hW : 2 ≤ W) :
    (2 : Int) ^ W = 2 * (2 : Int) ^ (W - 1) := by
  have hp := Int.pow_succ (2 : Int) (W - 1)
  have hs : (W - 1).succ = W := by omega
  rw [hs] at hp
  rw [hp]
  omega

/-- The range check over wrapped subtractions is exact on the full signed
    interval: `lo <= x <= hi` iff `U(x-lo) <= U(hi-lo)`. -/
theorem range_iff (W : Nat) (hW : 2 ≤ W) (lo hi x : Int)
    (hloL : -((2 : Int) ^ (W - 1)) ≤ lo)
    (hloH : lo ≤ (2 : Int) ^ (W - 1) - 1)
    (hhiL : -((2 : Int) ^ (W - 1)) ≤ hi)
    (hhiH : hi ≤ (2 : Int) ^ (W - 1) - 1)
    (hxL : -((2 : Int) ^ (W - 1)) ≤ x)
    (hxH : x ≤ (2 : Int) ^ (W - 1) - 1)
    (hord : lo ≤ hi) :
    (lo ≤ x ∧ x ≤ hi) ↔
      wrap ((2 : Int) ^ W) (x - lo) ≤ wrap ((2 : Int) ^ W) (hi - lo) := by
  let M : Int := (2 : Int) ^ W
  let H : Int := (2 : Int) ^ (W - 1)
  have hMH : M = 2 * H := by
    dsimp [M, H]
    exact pow_split W hW
  have hHpos : 0 < H := by
    dsimp [H]
    exact Int.pow_pos (show (0 : Int) < 2 by decide)
  have hMpos : 0 < M := by omega
  have hd0 : 0 ≤ hi - lo := by omega
  have hdM : hi - lo < M := by omega
  change (lo ≤ x ∧ x ≤ hi) ↔ (x - lo) % M ≤ (hi - lo) % M
  have hdu : (hi - lo) % M = hi - lo := Int.emod_eq_of_lt hd0 hdM
  constructor
  · rintro ⟨hxl, hxh⟩
    have hx0 : 0 ≤ x - lo := by omega
    have hxM : x - lo < M := by omega
    rw [Int.emod_eq_of_lt hx0 hxM, hdu]
    omega
  · intro hcmp
    have hzlo : -M < x - lo := by omega
    have hzhi : x - lo < M := by omega
    have hcases := emod_eq_self_or_add (x - lo) M hMpos hzlo hzhi
    have hcmp' : (x - lo) % M ≤ hi - lo := by simpa [hdu] using hcmp
    have hmod_nonneg : 0 ≤ (x - lo) % M := Int.emod_nonneg _ (by omega : M ≠ 0)
    rcases hcases with hself | hwrap
    · rw [hself] at hcmp'
      constructor <;> omega
    · rw [hwrap] at hcmp'
      have hxplus : x + M ≤ hi := by omega
      omega

/-! ## The canonical safe kernel and the checker -/

/-- The only program shape the checker accepts.  The `select` is guarded by
    the comparison mask, and the load is a *masked* load. -/
def goodProg (lo hi : Int) (base : Nat) : List Instr :=
  [ Instr.const lo
  , Instr.const hi
  , Instr.subWrap 0 1
  , Instr.subWrap 2 1
  , Instr.icmpUle 3 4
  , Instr.loadMasked base 5
  , Instr.const 0
  , Instr.select 5 6 7
  , Instr.halt 8 ]

/-- Static side conditions on the kernel parameters. -/
def paramsOk (W : Nat) (lo hi : Int) (len : Nat) : Prop :=
  2 ≤ W ∧ inSigned W lo ∧ inSigned W hi ∧ lo ≤ hi ∧ len ≤ 2 ^ W

instance (W : Nat) (lo hi : Int) (len : Nat) : Decidable (paramsOk W lo hi len) := by
  unfold paramsOk
  infer_instance

/-- The checker: exact shape plus parameter validity.  Any flag or instruction
    change makes the `decide` conjunct false. -/
def check (W : Nat) (lo hi : Int) (base len : Nat) (prog : List Instr) : Bool :=
  decide (prog = goodProg lo hi base ∧ paramsOk W lo hi len)

/-- Invalid variants used as regression scenarios. -/
def badNswProg (lo hi : Int) (base : Nat) : List Instr :=
  [ Instr.const lo, Instr.const hi, Instr.subNsw 0 1, Instr.subWrap 2 1
  , Instr.icmpUle 3 4, Instr.loadMasked base 5, Instr.const 0
  , Instr.select 5 6 7, Instr.halt 8 ]

def badFullLoadProg (lo hi : Int) (base : Nat) : List Instr :=
  [ Instr.const lo, Instr.const hi, Instr.subWrap 0 1, Instr.subWrap 2 1
  , Instr.icmpUle 3 4, Instr.loadFull base, Instr.const 0
  , Instr.select 5 6 7, Instr.halt 8 ]

/-! ## The IR interpreter -/

def recordErr (i : Nat) (e : Err) (st : LState) : LState :=
  match st.err with
  | none => { st with err := some e }
  | some _ => st

def stepLane (W : Nat) (mem : Nat → Int) (base len i : Nat)
    (st : LState) (instr : Instr) : LState :=
  let M : Int := (2 : Int) ^ W
  let g (r : Nat) : Int := st.regs.getD r 0
  let oob (a : Nat) : LState :=
    if a < base + len then st else recordErr i (Err.oob a) st
  match instr with
  | Instr.const v => { st with regs := st.regs ++ [v] }
  | Instr.subWrap a b => { st with regs := st.regs ++ [wrap M (g a - g b)] }
  | Instr.subNsw a b =>
      let z := g a - g b
      let st' := if inSigned W z then st else recordErr i Err.nswOverflow st
      { st' with regs := st'.regs ++ [z] }
  | Instr.icmpUle a b =>
      { st with regs := st.regs ++ [if wrap M (g a) ≤ wrap M (g b) then 1 else 0] }
  | Instr.loadMasked b m =>
      if g m ≠ 0 then
        let a := base + i
        let st' := oob a
        { st' with regs := st'.regs ++ [mem a], read := some a }
      else { st with regs := st.regs ++ [0] }
  | Instr.loadFull b =>
      let a := base + i
      let st' := oob a
      { st' with regs := st'.regs ++ [mem a], read := some a }
  | Instr.select c t f => { st with regs := st.regs ++ [if g c ≠ 0 then g t else g f] }
  | Instr.cttz a => { st with regs := st.regs ++ [(cttzVal W (g a) : Int)] }
  | Instr.halt r => { st with out := g r }
  | Instr.unknown _ => recordErr i Err.unknown st

def interpLane (W : Nat) (mem : Nat → Int) (base len i : Nat)
    (prog : List Instr) (x : Int) : LResult :=
  let st := prog.foldl (fun s instr => stepLane W mem base len i s instr)
    { regs := [x], out := 0, err := none, read := none }
  { out := st.out, err := st.err, read := st.read }

def firstErrFrom (i : Nat) : List (Option Err) → Option (Nat × Err)
  | [] => none
  | none :: xs => firstErrFrom (i + 1) xs
  | some e :: _ => some (i, e)

def mkObs (rs : List LResult) : Obs :=
  { values := rs.map (fun r => r.out)
  , firstError := firstErrFrom 0 (rs.map (fun r => r.err))
  , reads := rs.filterMap (fun r => r.read) }

def interpFrom (W : Nat) (mem : Nat → Int) (base len : Nat)
    (prog : List Instr) (i : Nat) : List Int → List LResult
  | [] => []
  | x :: xs => interpLane W mem base len i prog x :: interpFrom W mem base len prog (i + 1) xs

def interp (W : Nat) (mem : Nat → Int) (base len : Nat)
    (prog : List Instr) (lanes : List Int) : Obs :=
  mkObs (interpFrom W mem base len prog 0 lanes)

/-! ## The scalar reference -/

/-- Direct scalar specification of one lane: mathematical interval test,
    masked read, allocation-range check. -/
def specLane (W : Nat) (lo hi : Int) (base len : Nat) (mem : Nat → Int)
    (i : Nat) (x : Int) : LResult :=
  if lo ≤ x ∧ x ≤ hi then
    let a := base + i
    { out := mem a
    , err := if a < base + len then none else some (Err.oob a)
    , read := some a }
  else { out := 0, err := none, read := none }

def specFrom (W : Nat) (lo hi : Int) (base len : Nat) (mem : Nat → Int)
    (i : Nat) : List Int → List LResult
  | [] => []
  | x :: xs => specLane W lo hi base len mem i x :: specFrom W lo hi base len mem (i + 1) xs

def spec (W : Nat) (lo hi : Int) (base len : Nat) (mem : Nat → Int)
    (lanes : List Int) : Obs :=
  mkObs (specFrom W lo hi base len mem 0 lanes)

/-! ## Good kernel = scalar spec -/

theorem interpLane_good (W : Nat) (hW : 2 ≤ W) (lo hi : Int) (base len : Nat)
    (mem : Nat → Int) (i : Nat) (x : Int)
    (hpars : paramsOk W lo hi len) (hx : inSigned W x) :
    interpLane W mem base len i (goodProg lo hi base) x =
      specLane W lo hi base len mem i x := by
  rcases hpars with ⟨hW', hlo, hhi, hord, hlen⟩
  have hr := range_iff W hW lo hi x hlo.1 hlo.2 hhi.1 hhi.2 hx.1 hx.2 hord
  dsimp [interpLane, goodProg, stepLane, specLane, wrap, inSigned] at hr ⊢
  by_cases hir : lo ≤ x ∧ x ≤ hi
  · have hm : (if (x - lo) % (2 : Int) ^ W ≤ (hi - lo) % (2 : Int) ^ W then (1 : Int) else 0) ≠ 0 := by
      have : (x - lo) % (2 : Int) ^ W ≤ (hi - lo) % (2 : Int) ^ W := hr.mp hir
      simp [this]
    by_cases ha : base + i < base + len
    · simp [hir, hm, ha, recordErr]
    · simp [hir, hm, ha, recordErr]
  · have hm : (if (x - lo) % (2 : Int) ^ W ≤ (hi - lo) % (2 : Int) ^ W then (1 : Int) else 0) = 0 := by
      have : ¬ (x - lo) % (2 : Int) ^ W ≤ (hi - lo) % (2 : Int) ^ W := by
        intro hle
        exact hir (hr.mpr hle)
      simp [this]
    simp [hir, hm, recordErr]

theorem interpFrom_eq_specFrom (W : Nat) (hW : 2 ≤ W) (lo hi : Int)
    (base len : Nat) (mem : Nat → Int) (lanes : List Int) (i : Nat)
    (hpars : paramsOk W lo hi len)
    (hx : ∀ x ∈ lanes, inSigned W x) :
    interpFrom W mem base len (goodProg lo hi base) i lanes =
      specFrom W lo hi base len mem i lanes := by
  induction lanes generalizing i with
  | nil => rfl
  | cons x xs ih =>
      simp [interpFrom, specFrom]
      constructor
      · exact interpLane_good W hW lo hi base len mem i x hpars (hx x (by simp))
      · apply ih
        intro y hy
        exact hx y (by simp [hy])

theorem goodProg_from_spec (W : Nat) (hW : 2 ≤ W) (lo hi : Int) (base len : Nat)
    (mem : Nat → Int) (lanes : List Int)
    (hpars : paramsOk W lo hi len)
    (hx : ∀ x ∈ lanes, inSigned W x) :
    interp W mem base len (goodProg lo hi base) lanes = spec W lo hi base len mem lanes := by
  simpa [interp, spec] using
    congrArg mkObs (interpFrom_eq_specFrom W hW lo hi base len mem lanes 0 hpars hx)

/-- **MAIN THEOREM.** Successful validation refines the scalar spec for every
    input vector: identical values, first error and read addresses. -/
theorem ir_translation_validation_sound (W : Nat) (hW : 2 ≤ W)
    (lo hi : Int) (base len : Nat) (prog : List Instr)
    (mem : Nat → Int) (lanes : List Int)
    (hx : ∀ x ∈ lanes, inSigned W x)
    (hcheck : check W lo hi base len prog = true) :
    interp W mem base len prog lanes = spec W lo hi base len mem lanes := by
  have hp : prog = goodProg lo hi base ∧ paramsOk W lo hi len := of_decide_eq_true hcheck
  rcases hp with ⟨rfl, hpars⟩
  exact goodProg_from_spec W hW lo hi base len mem lanes hpars hx

/-! ## Positive acceptance and the two rejected regressions -/

theorem good_ir_accepted : check 4 0 0 0 1 (goodProg 0 0 0) = true := by
  native_decide

/-- Replacing one wrapping `sub` by `sub nsw` is rejected, and on the full
    MIN..MAX interval of W=4 the emitted program raises a spurious overflow
    where the scalar spec reads a value. -/
theorem nsw_sub_counterexample :
    check 4 (-8) 7 0 8 (badNswProg (-8) 7 0) = false ∧
    interp 4 (fun _ => 42) 0 8 (badNswProg (-8) 7 0) [7] ≠
      spec 4 (-8) 7 0 8 (fun _ => 42) [7] := by
  native_decide

/-- A full-width load followed by `select` is rejected, and it reads an
    out-of-allocation address at the tail lane (the spec's masked load does
    not), changing the first error and the read sequence. -/
theorem full_load_select_counterexample :
    check 4 0 0 0 1 (badFullLoadProg 0 0 0) = false ∧
    interp 4 (fun _ => 42) 0 1 (badFullLoadProg 0 0 0) [0, 1] ≠
      spec 4 0 0 0 1 (fun _ => 42) [0, 1] := by
  native_decide

/-- Unknown instructions abort validation. -/
theorem unknown_instr_counterexample :
    check 4 0 0 0 1 (goodProg 0 0 0 ++ [Instr.unknown "llvm.foo"]) = false ∧
    (interp 4 (fun _ => 42) 0 1 (goodProg 0 0 0 ++ [Instr.unknown "llvm.foo"]) [0]).firstError =
      some (0, Err.unknown) := by
  native_decide

/-- A `cttz` on zero follows the zero-time contract; the checker still refuses
    a kernel containing it (it is outside the validated whitelist). -/
theorem cttz_zero_contract_and_rejection :
    cttzVal 4 0 = 4 ∧ cttzVal 4 1 = 0 ∧
    check 4 0 0 0 1 (goodProg 0 0 0 ++ [Instr.cttz 8]) = false := by
  native_decide

end TinylambOpt.Research.IrTranslationValidation
