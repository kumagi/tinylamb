import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

/-!
Up to length twelve the first, middle, and last four-byte sample windows cover
every byte of a string. Equal length and equal raw windows therefore imply full
string equality, which justifies skipping exact verification for short SQL
grouping keys.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ThreeWindowCoverageCutoff

open TinylambOpt.ResearchGoals.JitKernels.SampledEquality
open TinylambOpt.SimdGroupHash (byteSamples)

theorem sound : ThreeWindowCoverageCutoff.Goal := by
  intro a b halen hsig
  let n := a.length
  have hlen : b.length = n := by
    have := congrArg Prod.fst hsig
    simpa [sig, n] using this.symm
  have hbs : byteSamples a = byteSamples b := by
    have := congrArg Prod.snd hsig
    simpa [sig] using this
  simp [byteSamples, hlen] at hbs
  have hfirst : a.take 4 = b.take 4 := hbs.1
  have hmid : (a.drop ((n - 4) / 2)).take 4 =
      (b.drop ((n - 4) / 2)).take 4 := hbs.2.1
  have hlast : (a.drop (n - 4)).take 4 =
      (b.drop (n - 4)).take 4 := hbs.2.2
  apply List.ext_getElem?
  intro i
  by_cases hi : i < n
  · have hcov : covered n i = true := by
      simp [covered, offsets]
      omega
    simp [covered, offsets] at hcov
    rcases hcov with h0 | h1 | h2
    · have hi4 : i < 4 := by simpa using h0
      have hh := congrArg (fun l : List UInt8 => l[i]?) hfirst
      simpa [List.getElem?_take, hi4] using hh
    · have hmle : (n - 4) / 2 ≤ i := by simpa using h1.1
      have hlt : i < (n - 4) / 2 + 4 := by simpa using h1.2
      have hj : i - (n - 4) / 2 < 4 := by omega
      have hh := congrArg (fun l : List UInt8 => l[i - (n - 4) / 2]?) hmid
      simp [hj, List.getElem?_drop] at hh
      have hmi : (n - 4) / 2 + (i - (n - 4) / 2) = i := Nat.add_sub_of_le hmle
      simpa [hmi] using hh
    · have hlle : n - 4 ≤ i := by simpa using h2.1
      have hj : i - (n - 4) < 4 := by omega
      have hh := congrArg (fun l : List UInt8 => l[i - (n - 4)]?) hlast
      simp [hj, List.getElem?_drop] at hh
      have hli : n - 4 + (i - (n - 4)) = i := Nat.add_sub_of_le hlle
      simpa [hli] using hh
  · have hai : a.length ≤ i := by
      exact Nat.le_of_not_gt (by simpa [n] using hi)
    have hbi : b.length ≤ i := by omega
    rw [List.getElem?_eq_none hai, List.getElem?_eq_none hbi]

end TinylambOpt.ResearchProofs.JitKernels.Goal_SampledEquality_ThreeWindowCoverageCutoff
