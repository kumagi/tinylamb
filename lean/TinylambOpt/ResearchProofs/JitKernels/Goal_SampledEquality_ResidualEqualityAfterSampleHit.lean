import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
When two strings share a sampled signature, they agree on every covered byte.
The remaining bytes are exactly the residual list in index order, so comparing
residuals is equivalent to comparing the full SQL grouping keys.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ResidualEqualityAfterSampleHit

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality
open TinylambOpt.SimdGroupHash (byteSamples)

/-- Bytes omitted by a predicate-dependent mask, in original order. -/
def keep {α : Type} (p : Nat → Bool) (l : List α) (start : Nat) : List α :=
  ((l.zipIdx start).filter (fun q => !p q.2)).map Prod.fst

theorem keep_cons {α : Type} (p : Nat → Bool) (x : α) (xs : List α) (start : Nat) :
    keep p (x :: xs) start =
      if p start = false then x :: keep p xs (start + 1) else keep p xs (start + 1) := by
  unfold keep
  simp [List.zipIdx]
  by_cases h : p start = false <;> simp [h]

/-- A list is determined by its covered positions and its masked residual. -/
theorem list_eq_of_partition (a b : List UInt8) (p : Nat → Bool) (start : Nat)
    (hlen : a.length = b.length)
    (hcov : ∀ i, start ≤ i → i < start + a.length → p i = true →
      a[i - start]? = b[i - start]?)
    (hres : keep p a start = keep p b start) : a = b := by
  induction a generalizing b p start with
  | nil =>
      have hb : b.length = 0 := by simpa using hlen.symm
      exact (List.eq_nil_of_length_eq_zero hb).symm
  | cons x xs ih =>
      cases b with
      | nil => simp at hlen
      | cons y ys =>
          have hylen : xs.length = ys.length := by simpa using hlen
          by_cases hp : p start = true
          · have hpnotfalse : ¬ p start = false := by cases h : p start <;> simp_all
            have htail : keep p xs (start + 1) = keep p ys (start + 1) := by
              simpa [keep_cons, hpnotfalse] using hres
            have hxy : x = y := by
              have hh := hcov start (by omega) (by simp) hp
              simpa using hh
            subst y
            congr
            apply ih ys p (start + 1)
            · simpa using hylen
            · intro i hle hlt hpi
              have hlen' : (x :: xs).length = ys.length + 1 := by simpa using hylen
              have hi_bound : i < start + (x :: xs).length := by omega
              have hh := hcov i (by omega) hi_bound hpi
              have hidx : i - start = (i - (start + 1)) + 1 := by omega
              simpa [hidx, List.getElem?_cons_succ] using hh
            · exact htail
          · have hpfalse : p start = false := by cases h : p start <;> simp_all
            have hhres : x = y ∧ keep p xs (start + 1) = keep p ys (start + 1) := by
              simpa [keep_cons, hpfalse] using hres
            have hxy := hhres.1
            subst y
            congr
            apply ih ys p (start + 1)
            · simpa using hylen
            · intro i hle hlt hpi
              have hlen' : (x :: xs).length = ys.length + 1 := by simpa using hylen
              have hi_bound : i < start + (x :: xs).length := by omega
              have hh := hcov i (by omega) hi_bound hpi
              have hidx : i - start = (i - (start + 1)) + 1 := by omega
              simpa [hidx, List.getElem?_cons_succ] using hh
            · exact hhres.2

theorem sound : ResidualEqualityAfterSampleHit.Goal := by
  intro a b hs
  constructor
  · intro h
    subst b
    rfl
  · intro hres
    have hlen : a.length = b.length := by
      have := congrArg Prod.fst hs
      simpa [sig] using this
    apply list_eq_of_partition a b (fun i => covered a.length i) 0 hlen
    · intro i hle hlt hc
      have hbs : byteSamples a = byteSamples b := by
        have := congrArg Prod.snd hs
        simpa [sig] using this
      simp [byteSamples, ← hlen] at hbs
      simp [covered, offsets] at hc
      rcases hc with h0 | h1 | h2
      · have hi4 : i < 4 := by simpa using h0
        have hh := congrArg (fun l : List UInt8 => l[i]?) hbs.1
        simpa [List.getElem?_take, hi4] using hh
      · have hmle : (a.length - 4) / 2 ≤ i := by simpa using h1.1
        have hj : i - (a.length - 4) / 2 < 4 := by omega
        have hh := congrArg (fun l : List UInt8 => l[i - (a.length - 4) / 2]?) hbs.2.1
        simp [hj, List.getElem?_drop] at hh
        have hmi : (a.length - 4) / 2 + (i - (a.length - 4) / 2) = i := Nat.add_sub_of_le hmle
        simpa [hmi] using hh
      · have hlle : a.length - 4 ≤ i := by simpa using h2.1
        have hj : i - (a.length - 4) < 4 := by omega
        have hh := congrArg (fun l : List UInt8 => l[i - (a.length - 4)]?) hbs.2.2
        simp [hj, List.getElem?_drop] at hh
        have hli : a.length - 4 + (i - (a.length - 4)) = i := Nat.add_sub_of_le hlle
        simpa [hli] using hh
    · simpa [residual, keep, ← hlen] using hres

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ResidualEqualityAfterSampleHit
