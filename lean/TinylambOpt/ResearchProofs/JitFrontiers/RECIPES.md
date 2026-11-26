# JitFrontiers proof recipes (learned on WordPromises/SignedAdd)

Environment: `cd /home/kumagi/develop/tinylamb/lean && export PATH="$HOME/.elan/bin:$PATH"`.
Verify ONE file at a time: `lake env lean TinylambOpt/ResearchProofs/JitFrontiers/<Domain>/Goal_<Model>_<Prop>.lean`
(no output = success). Do NOT run `lake build`. Scratch files go in `/tmp/opencode/`.
Put `set_option maxHeartbeats 0` and `set_option maxRecDepth 10000` at top of each file.

## Lean 4.36 core-only reality (no Mathlib!)

- NO `push_neg`, `tauto`, `bool_congr`, `omega` is available and powerful (Nat/Int
  linear arithmetic incl. `Nat.mod`/`Nat.div` by *constant* literals, `∧ ∨ ¬ ↔`
  combinations). `norm_num`, `simp`, `omega`, `bv_decide`, `grind`, `aesop` exist.
- `lemma` keyword does NOT exist; use `theorem`.

## Bool/Prop coercion traps (biggest time sink)

- `(b : Prop)` for `b : Bool` elaborates to the *displayed* form `b = true`
  (`Bool.boolToSort`). So a hypothesis `h : passes o p` is literally
  `passes o p = true`, and `¬(passes o p)` is `¬(passes o p = true)`.
- `f ✏' !decide p` traps: `!decide` can be read as function composition;
  parenthesize: `(!decide p)`.
- `rw [h]` where `h : a = b` rewrites occurrences of `a` with `b`. If goal is
  `b-mentioning`, use `rw [← h]`. Typeclass-lifted equalities can make `rw`
  fail confusingly; the escape hatch is `Eq.mpr`-style `▸`:
  `exact (hgo p.1 p.2).symm ▸ hde`.
- To turn a Bool equality goal into iff: prove once and reuse
  `theorem beq_iff (a b : Bool) : (a = b) ↔ (a ↔ b) := by cases a <;> cases b <;> simp`
  (WordPromises/Common.lean has this; every domain should have one).
- `¬(b = true) → b = false`: `Bool.not_eq_true'` in WordPromises/Common.lean.
- For `decide`-heavy Bool goals: `simp only [decide_eq_true_eq, Bool.and_eq_true,
  Bool.or_eq_true, Bool.not_eq_true]` converts to Prop, then `omega` finishes
  linear atoms. Careful: these simp lemmas are Prop-versions; run on an Iff
  goal after `rw [beq_iff]`.

## bv_decide limits

- Works great on pure bit formulas (`^^^ &&& ~~~ <<< msb getLsbD + -` on BitVec).
- FAILS (abstracts to opaque) on `decide (x.toInt + y.toInt < 2^63)` and
  `decide (x.toNat + y.toNat < 2^64)` — i.e. `toInt`/`toNat` of ONE operand are
  OK-ish but arithmetic *combinations* of two widened operands overflow its
  preprocessing. Workaround patterns that WORK:
  - `x ≤ x+y` style: `simp only [BitVec.le_def, BitVec.toNat_add]` (gives Nat
    `% 2^64` atoms) then `omega`.
  - Signed overflow: `rw [BitVec.saddOverflow_eq]` / `ssubOverflow_eq` /
    `smulOverflow_eq` / `uaddOverflow_eq` to pure bits, then `bv_decide`.
  - `BitVec.saddOverflow` def unfolds to `decide`-atoms of the same bounds as
    `signed` — bridge `signed (x.toInt + y.toInt) = !x.saddOverflow y` with
    `Bool.and_eq_decide / Bool.or_eq_decide / ← decide_not / decide_eq_decide`
    + `simp only [decide_eq_true_eq]` + `omega` (see Common.lean /
    SignedAdd GuardExact probe output in git history).
  - Variable exponent `2 ^ y.toNat` is nonlinear: bv_decide/omega can't see
    inside; case-split on the count, or relate mask to mod by other means.
- `Int.bmod` is NOT handled by omega; use `Int.bmod_eq_emod` + `split` +
  `Int.emod_nonneg`/`Int.emod_lt_of_pos` facts + omega, or std lemmas
  `BitVec.toInt_add : (x+y).toInt = (x.toInt+y.toInt).bmod 2^w` etc.

## Structural strategy per domain (10 models x 10 goals)

1. Read your `TinylambOpt/ResearchGoals/JitFrontiers/<Domain>.lean` fully.
2. Write ONE `Common.lean` in your dir proving the shared contracts once at the
   level of the domain's model *structure/definitions* (e.g. `guardExact o →
   dropFlag o`), NOT per model. Instantiate per model with one-liners.
3. Per model: prove the "hard" contract (usually `GuardExact`) in
   `Goal_<Model>_GuardExact.lean` as `theorem sound`, other files import it.
4. Deliberately strong hypotheses (`Unguarded`, `Substitute`, `EarlyAdvance`,
   `ChunkLocal`, ...) are usually FALSE: `theorem refute : ¬ Goal := ...` with
   the counterexample from `issues/research/jit-frontiers.md` or found by
   hand (check with `#eval`/`#guard` in a scratch file first).
5. Special boundary goals: first `#eval` candidate answers in scratch; often
   provable by `simp [ir, op]; decide` (closed BitVec 64 computations are fast)
   or `intro; bv_decide` / `omega` after unfolding.

## File template (STRICT)

```lean
import TinylambOpt.ResearchGoals.JitFrontiers.<Domain>
-- plus your Common / GuardExact file when needed

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.<Domain>.Goal_<Model>_<Prop>

open TinylambOpt.ResearchGoals.JitFrontiers.<Domain>

theorem sound : <Model>.<Prop>.Goal := by ...

end TinylambOpt.ResearchProofs.JitFrontiers.<Domain>.Goal_<Model>_<Prop>
```

Add `import` line for EVERY created file to
`TinylambOpt/ResearchProofs/JitFrontiers/<Domain>_All.lean` (overwrite it;
keep its header comment). The audit (`CheckJitFrontierProofs.lean`) counts a
Goal covered iff some theorem has type exactly `Goal` or `¬ Goal`, and fails
on `sorryAx` or exotic axioms (allowed: propext, Classical.choice, Quot.sound,
native_decide helpers).

`native_decide` is allowed for finite `Decidable` props (small domains only —
`∀ x : BitVec 64` is NOT small; `∀ x : Bool × Vec ...` over tiny types is).

Keep each file's verify time under ~30 s. If a GuardExact resist all tactics
after a reasonable effort, move on to other goals of that model (the 8 generic
ones may still be refutable/provable without it) and report the blocker.
