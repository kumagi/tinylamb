import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-! When two strings share a sampled signature, they agree on every covered
    byte. The remaining bytes are exactly the residual list in index order, so
    comparing residuals is equivalent to comparing the full SQL grouping key. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ResidualEqualityAfterSampleHit

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality
open TinylambOpt.SimdGroupHash (byteSamples)

theorem sound : TinylambOpt.ResearchGoals.JitKernels.SampledEquality.ResidualEqualityAfterSampleHit.Goal := by
  let keep : (Nat → Bool) → List UInt8 → Nat → List UInt8 := fun p l start =>
    ((l.zipIdx start).filter (fun q => !p q.2)).map Prod.fst
  have keep_cons : ∀ (p : Nat → Bool) (x : UInt8) (xs : List UInt8) (start : Nat),
      keep p (x :: xs) start =
        if p start = false then x :: keep p xs (start + 1) else keep p xs (start + 1) := by
    intro p x xs start
    dsimp [keep]
    by_cases h : p start = false <;> simp [h]
  have list_eq_of_partition : ∀ (a b : List UInt8) (p : Nat → Bool) (start : Nat),
      a.length = b.length →
      (∀ i, start ≤ i → i < start + a.length → p i = true →
        a[i - start]? = b[i - start]?) →
      keep p a start = keep p b start → a = b := by
    intro a
    induction a with
    | nil =>
        intro b p start hlen hcov hres
        have hb : b.length = 0 := by simpa using hlen.symm
        exact (List.eq_nil_of_length_eq_zero hb).symm
    | cons x xs ih =>
        intro b p start hlen hcov hres
        cases b with
        | nil => simp at hlen
        | cons y ys =>
            have hylen : xs.length = ys.length := by simpa using hlen
            by_cases hp : p start = true
            · have hpnotfalse : ¬ p start = false := by
                cases h : p start <;> simp_all
              have htail : keep p xs (start + 1) = keep p ys (start + 1) := by
                simpa [keep_cons, hpnotfalse] using hres
              have hxy : x = y := by
                have hh := hcov start (by omega) (by simp) hp
                simpa using hh
              rw [← hxy]
              congr
              apply ih ys p (start + 1)
              · simpa using hylen
              · intro i hle hlt hpi
                have hi_bound : i < start + (x :: xs).length := by
                  simpa [List.length_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hlt
                have hh := hcov i (by omega) hi_bound hpi
                have hidx : i - start = (i - (start + 1)) + 1 := by omega
                simpa [hidx, List.getElem?_cons_succ] using hh
              · exact htail
            · have hpfalse : p start = false := by
                cases h : p start <;> simp_all
              have hhres : x = y ∧ keep p xs (start + 1) = keep p ys (start + 1) := by
                simpa [keep_cons, hpfalse] using hres
              have hxy := hhres.1
              rw [← hxy]
              congr
              apply ih ys p (start + 1)
              · simpa using hylen
              · intro i hle hlt hpi
                have hi_bound : i < start + (x :: xs).length := by
                  simpa [List.length_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using hlt
                have hh := hcov i (by omega) hi_bound hpi
                have hidx : i - start = (i - (start + 1)) + 1 := by omega
                simpa [hidx, List.getElem?_cons_succ] using hh
              · exact hhres.2
  intro a b hs
  constructor
  · intro h
    rw [h]
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
        have hmi : (a.length - 4) / 2 + (i - (a.length - 4) / 2) = i :=
          Nat.add_sub_of_le hmle
        simpa [hmi] using hh
      · have hlle : a.length - 4 ≤ i := by simpa using h2.1
        have hj : i - (a.length - 4) < 4 := by omega
        have hh := congrArg (fun l : List UInt8 => l[i - (a.length - 4)]?) hbs.2.2
        simp [hj, List.getElem?_drop] at hh
        have hli : a.length - 4 + (i - (a.length - 4)) = i := Nat.add_sub_of_le hlle
        simpa [hli] using hh
    · simpa [residual, keep, ← hlen] using hres

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ResidualEqualityAfterSampleHit
