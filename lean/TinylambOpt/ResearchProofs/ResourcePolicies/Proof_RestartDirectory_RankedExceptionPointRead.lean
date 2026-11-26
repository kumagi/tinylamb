import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
import TinylambOpt.ResearchProofs.Compression.Wire

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_RankedExceptionPointRead

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
A single value can be read through the exception bitmap and the exclusive rank
of preceding exceptions without materialising the expanded frame: inline lanes
are read from the packed bits, and exception lanes from the sparse payload.

The proof keeps the observed value at every row identical; it only changes the
representation used to fetch it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_RankedExceptionPointRead

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

theorem pack_map_mod (w : Nat) (xs : List Nat) :
    pack w xs = pack w (xs.map (fun x => x % 2 ^ w)) := by
  unfold pack
  congr 1
  · induction xs with
    | nil => simp [word]
    | cons x xs ih => simp [word, ih]
  · simp

theorem filter_rank_get? (p : Nat → Bool) (xs : List Nat) (i : Nat)
    (hi : i < xs.length) (hp : p xs[i] = true) :
    (xs.filter p)[((xs.take i).filter p).length]? = xs[i]? := by
  induction xs generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      cases i with
      | zero =>
          simp at hp ⊢
          simp [hp]
      | succ i =>
          have hi' : i < xs.length := by simpa using hi
          have hp' : p xs[i] = true := by simpa using hp
          have hrec := ih i hi' hp'
          by_cases hx : p x
          · simp [hx, hi', hrec]
          · simp [hx, hi', hrec]

theorem pack_bytes_lt_256 (w : Nat) (xs : List Nat) :
    ∀ b ∈ pack w xs, b < 256 := by
  intro b hb
  simp only [pack, little, List.mem_map] at hb
  obtain ⟨j, hj, rfl⟩ := hb
  exact Nat.mod_lt _ (by decide : 0 < 256)

theorem sound : RankedExceptionPointRead.Goal := by
  intro w xs i
  unfold patchedRead
  by_cases hi : i < xs.length
  · have hflags : (xs.map (flagged w))[i]? = some (flagged w xs[i]) := by
      rw [List.getElem?_map]
      simp [List.getElem?_eq_getElem hi]
    rw [hflags]
    simp only
    by_cases hflag : flagged w xs[i] = true
    · simp [hflag]
      exact filter_rank_get? (flagged w) xs i hi hflag
    · simp [hflag]
      have hlt : xs[i] < 2 ^ w := by
        have hnot : ¬ 2 ^ w ≤ xs[i] := by
          intro hle
          apply hflag
          unfold flagged
          simp [hle]
        omega
      have hlocal := sound_LocalRead (pack w xs) w i (pack_bytes_lt_256 w xs)
      let ys : List Nat := xs.map (fun x => x % 2 ^ w)
      have hbound : ∀ y ∈ ys, y < 2 ^ w := by
        intro y hy
        dsimp [ys] at hy
        obtain ⟨z, hz, rfl⟩ := List.mem_map.mp hy
        exact Nat.mod_lt z (Nat.two_pow_pos w)
      have hinv := sound_PackedInverse w ys hbound
      have hlane : get (pack w ys) w i = xs[i] % 2 ^ w := by
        have hc := congrArg (fun l : List Nat => l[i]?) hinv
        dsimp [ys] at hc
        simp [hi] at hc
        simpa [ys] using hc
      have htake : takeAt (pack w xs) w i = xs[i] := by
        calc
          takeAt (pack w xs) w i = get (pack w xs) w i := hlocal
          _ = get (pack w ys) w i := by rw [pack_map_mod w xs]
          _ = xs[i] % 2 ^ w := hlane
          _ = xs[i] := Nat.mod_eq_of_lt hlt
      rw [htake]
      simp [List.getElem?_eq_getElem hi]
  · have hlen : (xs.map (flagged w)).length ≤ i := by
      rw [List.length_map]
      omega
    have hflagval : (xs.map (flagged w))[i]? = none := List.getElem?_eq_none hlen
    rw [hflagval]
    simp
    omega

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_RankedExceptionPointRead
end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_RankedExceptionPointRead
