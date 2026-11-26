import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_TableBytes

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Every transition of `ContainsSlash.reader` stays in the two-state space, so
every column of a compiled table fits in the bit width chosen from `states-1`.
The inverse law for bit packing then gives back the original table. -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length] using hq
  simp [applyT, List.getElem?_eq_getElem hq, chain]

private theorem power_unfold (n : Nat) (t : Table) (hn : n ≠ 0) :
    power n t =
      let h := power (n / 2) t
      let sq := chain h h
      if n % 2 == 0 then sq else chain sq t := by
  rw [power.eq_def n t]
  simp [hn]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · subst n
        simp [power]
      · rw [power_unfold n t hn]
        have hhalf := ih (n / 2) (Nat.div_lt_self (by omega) (by decide))
        dsimp
        split <;> simp [chain_length, hhalf]

private theorem compile_length (x : Rope) : (compile reader x).length = 2 := by
  induction x with
  | lit s => simp [compile, reader]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA =>
      simpa [compile, ihA] using power_length n (compile reader a)

private theorem foldl_lt_two (s : Bytes) (q : Nat) (hq : q < 2) :
    s.foldl reader.step q < 2 := by
  induction s generalizing q with
  | nil => simpa using hq
  | cons b bs ih =>
      simp only [List.foldl_cons]
      apply ih
      simp [reader]
      split <;> omega

private theorem power_entry_lt_two (n : Nat) (t : Table) (htlen : t.length = 2)
    (ht : ∀ r, r < 2 → applyT t r < 2) : ∀ q, q < 2 → applyT (power n t) q < 2 := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      intro q hq
      by_cases hzero : n = 0
      · subst n
        simp [power, applyT, htlen, hq]
      · have hhalf : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        have hboundHalf : ∀ r, r < 2 → applyT (power (n / 2) t) r < 2 := ih (n / 2) hhalf
        have hlenHalf : (power (n / 2) t).length = 2 := by
          simpa [htlen] using power_length (n / 2) t
        have hlenSq : (chain (power (n / 2) t) (power (n / 2) t)).length = 2 := by
          rw [chain_length, hlenHalf]
        have hboundSq : ∀ r, r < 2 →
            applyT (chain (power (n / 2) t) (power (n / 2) t)) r < 2 := by
          intro r hr
          rw [applyT_chain _ _ r (by simpa [hlenHalf] using hr)]
          have hr' : applyT (power (n / 2) t) r < 2 := hboundHalf r hr
          exact hboundHalf _ hr'
        by_cases heven : n % 2 == 0
        · rw [power_unfold n t hzero]
          dsimp
          simp [heven]
          exact hboundSq q hq
        · rw [power_unfold n t hzero]
          dsimp
          simp [heven]
          rw [applyT_chain _ _ q (by simpa [hlenSq] using hq)]
          exact ht _ (hboundSq q hq)

private theorem compile_entry_lt_two (x : Rope) : ∀ q, q < 2 → applyT (compile reader x) q < 2 := by
  induction x with
  | lit s =>
      intro q hq
      change applyT ((List.range 2).map (fun r : Nat => s.foldl reader.step r)) q < 2
      simp [applyT, hq, foldl_lt_two s q hq]
  | cat a b ihA ihB =>
      intro q hq
      have ha : (compile reader a).length = 2 := compile_length a
      change applyT (chain (compile reader a) (compile reader b)) q < 2
      rw [applyT_chain _ _ q (by simpa [ha] using hq)]
      exact ihB _ (ihA q hq)
  | rep n a ihA =>
      intro q hq
      change applyT (power n (compile reader a)) q < 2
      exact power_entry_lt_two n (compile reader a) (compile_length a) ihA q hq

theorem sound_TableBytes : ContainsSlash.TableBytes.Goal := by
  intro x
  change (List.range reader.states).map
      (fun q => get (pack (width (reader.states - 1)) (compile reader x))
        (width (reader.states - 1)) q) = compile reader x
  let t := compile reader x
  have htlen : t.length = 2 := by simpa [t] using compile_length x
  have hpow : 2 ^ width (reader.states - 1) = 2 := by native_decide
  have hbound : ∀ v ∈ t, v < 2 ^ width (reader.states - 1) := by
    intro v hv
    rw [hpow]
    rw [List.mem_iff_getElem] at hv
    rcases hv with ⟨i, hi, rfl⟩
    have hi2 : i < 2 := by simpa [htlen] using hi
    have heq : applyT t i = t[i] := by simp [applyT, List.getElem?_eq_getElem hi]
    rw [← heq]
    simpa [t] using compile_entry_lt_two x i hi2
  have hpi := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
    (width (reader.states - 1)) t hbound
  rw [htlen] at hpi
  simpa [t, reader] using hpi

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_TableBytes
