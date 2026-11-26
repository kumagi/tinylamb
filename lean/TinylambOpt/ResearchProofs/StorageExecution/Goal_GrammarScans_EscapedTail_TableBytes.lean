import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EscapedTail.TableBytes` — Every successor state fits the packed dictionary of state
  identifiers.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_TableBytes

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EscapedTail

/-- A transition table is good when it has exactly two lanes and every
    successor fits in them. -/
private def Good (t : Table) : Prop := t.length = 2 ∧ ∀ v ∈ t, v < 2

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem step_bounded {q b : Nat} (_hq : q < EscapedTail.reader.states) :
    EscapedTail.reader.step q b < EscapedTail.reader.states := by
  simp only [EscapedTail.reader]
  split
  · omega
  · omega

private theorem foldl_step_bounded (xs : Bytes) {q : Nat}
    (hq : q < EscapedTail.reader.states) :
    xs.foldl EscapedTail.reader.step q < EscapedTail.reader.states := by
  induction xs generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (step_bounded hq)

private theorem applyT_of_good {t : Table} (hg : Good t) {q : Nat} (hq : q < 2) :
    applyT t q < 2 := by
  dsimp [Good] at hg
  have hq' : q < t.length := by omega
  rw [applyT, List.getElem?_eq_getElem hq']
  simp only [Option.getD_some]
  exact hg.2 (t[q]) (List.getElem_mem hq')

private theorem chain_good {a b : Table} (ha : Good a) (hb : Good b) :
    Good (chain a b) := by
  dsimp [Good] at ha hb ⊢
  constructor
  · simpa [chain_length] using ha.1
  · intro v hv
    simp [chain] at hv
    rcases hv with ⟨q, hqmem, rfl⟩
    exact applyT_of_good hb (ha.2 q hqmem)

private theorem good_power {t : Table} (hg : Good t) : ∀ n, Good (power n t) := by
  dsimp [Good] at hg
  intro n
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · subst n
        dsimp [Good]
        constructor
        · simp [power, hg.1]
        · intro v hv
          simp [power, List.mem_range] at hv
          omega
      · have hpos : 0 < n := Nat.pos_of_ne_zero hn
        have hdiv : n / 2 < n := Nat.div_lt_self hpos (by decide)
        have hgHalf : Good (power (n / 2) t) := ih (n / 2) hdiv
        have hgSq : Good (chain (power (n / 2) t) (power (n / 2) t)) :=
          chain_good hgHalf hgHalf
        unfold power
        simp only [hn, dite_false]
        by_cases heven : n % 2 = 0
        · simpa [heven] using hgSq
        · have hodd : n % 2 = 1 := by omega
          simpa [hodd] using chain_good hgSq hg

private theorem compile_good (x : Rope) : Good (compile EscapedTail.reader x) := by
  induction x with
  | lit s =>
      constructor
      · simp [compile, EscapedTail.reader]
      · intro v hv
        change v ∈ (List.range (2 : Nat)).map (fun q => s.foldl EscapedTail.reader.step q) at hv
        rw [List.mem_map] at hv
        rcases hv with ⟨q, hqmem, rfl⟩
        rw [List.mem_range] at hqmem
        exact foldl_step_bounded s hqmem
  | cat a b ihA ihB =>
      simpa [compile] using chain_good ihA ihB
  | rep n a ihA =>
      simpa [compile] using good_power ihA n

theorem sound_TableBytes : EscapedTail.TableBytes.Goal := by
  change tableBytes EscapedTail.reader
  intro x
  have htlen : (compile EscapedTail.reader x).length = EscapedTail.reader.states :=
    (compile_good x).1
  have hbound : ∀ v ∈ compile EscapedTail.reader x,
      v < 2 ^ width (EscapedTail.reader.states - 1) := by
    intro v hv
    have hv2 : v < 2 := (compile_good x).2 v hv
    have hw : width (EscapedTail.reader.states - 1) = 1 := by decide
    rw [hw]
    simpa using hv2
  have hpi := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
    (width (EscapedTail.reader.states - 1)) (compile EscapedTail.reader x) hbound
  simpa [htlen] using hpi

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_TableBytes
