import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `LineBoundary.TableBytes` — Every successor state fits the packed dictionary of state
  identifiers.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_TableBytes

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Every transition of `LineBoundary.reader` lands in {0,1,2}, so every entry
of a compiled table is below 3 <= 2 ^ width (states-1) = 4. The bit-packing
inverse law then recovers the table from its packed bytes. -/

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

private theorem compile_length (x : Rope) : (compile reader x).length = 3 := by
  induction x with
  | lit s => simp [compile, reader]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA =>
      simpa [compile, ihA] using power_length n (compile reader a)

private theorem foldl_lt_three (s : Bytes) (q : Nat) (hq : q < 3) :
    s.foldl reader.step q < 3 := by
  induction s generalizing q with
  | nil => simpa using hq
  | cons b bs ih =>
      simp only [List.foldl_cons]
      apply ih
      by_cases h13 : b = 13
      · simp [reader, h13]
      · by_cases hb : q = 1 ∧ b = 10
        · simp [reader, hb]
        · simp [reader, h13, hb]

private theorem power_entry_lt_three (n : Nat) (t : Table) (htlen : t.length = 3)
    (ht : ∀ r, r < 3 → applyT t r < 3) : ∀ q, q < 3 → applyT (power n t) q < 3 := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      intro q hq
      by_cases hzero : n = 0
      · subst n
        simp [power, applyT, htlen, hq]
      · have hhalf : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        have hboundHalf : ∀ r, r < 3 → applyT (power (n / 2) t) r < 3 := ih (n / 2) hhalf
        have hlenHalf : (power (n / 2) t).length = 3 := by
          simpa [htlen] using power_length (n / 2) t
        have hlenSq : (chain (power (n / 2) t) (power (n / 2) t)).length = 3 := by
          rw [chain_length, hlenHalf]
        have hboundSq : ∀ r, r < 3 →
            applyT (chain (power (n / 2) t) (power (n / 2) t)) r < 3 := by
          intro r hr
          rw [applyT_chain _ _ r (by simpa [hlenHalf] using hr)]
          have hr' : applyT (power (n / 2) t) r < 3 := hboundHalf r hr
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

private theorem compile_entry_lt_three (x : Rope) : ∀ q, q < 3 → applyT (compile reader x) q < 3 := by
  induction x with
  | lit s =>
      intro q hq
      change applyT ((List.range 3).map (fun r : Nat => s.foldl reader.step r)) q < 3
      simp [applyT, hq, foldl_lt_three s q hq]
  | cat a b ihA ihB =>
      intro q hq
      have ha : (compile reader a).length = 3 := compile_length a
      change applyT (chain (compile reader a) (compile reader b)) q < 3
      rw [applyT_chain _ _ q (by simpa [ha] using hq)]
      exact ihB _ (ihA q hq)
  | rep n a ihA =>
      intro q hq
      change applyT (power n (compile reader a)) q < 3
      exact power_entry_lt_three n (compile reader a) (compile_length a) ihA q hq

theorem sound_TableBytes : LineBoundary.TableBytes.Goal := by
  intro x
  change (List.range reader.states).map
      (fun q => get (pack (width (reader.states - 1)) (compile reader x))
        (width (reader.states - 1)) q) = compile reader x
  let t := compile reader x
  have htlen : t.length = 3 := by simpa [t] using compile_length x
  have hpow : 2 ^ width (reader.states - 1) = 4 := by native_decide
  have hbound : ∀ v ∈ t, v < 2 ^ width (reader.states - 1) := by
    intro v hv
    rw [hpow]
    rw [List.mem_iff_getElem] at hv
    rcases hv with ⟨i, hi, rfl⟩
    have hi3 : i < 3 := by simpa [htlen] using hi
    change (compile reader x)[i] < 4
    have hi' : i < (compile reader x).length := by simpa [t] using hi
    have hlt : (compile reader x)[i] < 3 := by
      simpa [applyT, List.getElem?_eq_getElem hi'] using compile_entry_lt_three x i hi3
    omega
  have hpi := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
    (width (reader.states - 1)) t hbound
  rw [htlen] at hpi
  simpa [t, reader] using hpi

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_TableBytes
