import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_TableBytes

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.Compression.Wire

/-- A transition table is good when it has exactly three lanes and every
    successor fits in them. -/
private def Good (t : Table) : Prop := t.length = 3 ∧ ∀ v ∈ t, v < 3

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem step_bounded {q b : Nat} (hq : q < SlashDepth.reader.states) :
    SlashDepth.reader.step q b < SlashDepth.reader.states := by
  simp only [SlashDepth.reader]
  split
  · have hle : min 2 (q + 1) ≤ 2 := Nat.min_le_left _ _
    omega
  · exact hq

private theorem foldl_step_bounded (xs : Bytes) {q : Nat}
    (hq : q < SlashDepth.reader.states) :
    xs.foldl SlashDepth.reader.step q < SlashDepth.reader.states := by
  induction xs generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (step_bounded hq)

private theorem applyT_of_good {t : Table} (hg : Good t) {q : Nat} (hq : q < 3) :
    applyT t q < 3 := by
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
  dsimp [Good] at hg ⊢
  intro n
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · subst n
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
        · simpa [Good, heven] using hgSq
        · have hodd : n % 2 = 1 := by omega
          simpa [Good, hodd] using chain_good hgSq hg

private theorem compile_good (x : Rope) : Good (compile SlashDepth.reader x) := by
  dsimp [Good]
  induction x with
  | lit s =>
      constructor
      · simp [compile, SlashDepth.reader]
      · intro v hv
        simp [compile] at hv
        rcases hv with ⟨q, hqmem, rfl⟩
        exact foldl_step_bounded s hqmem
  | cat a b ihA ihB =>
      simpa [Good, compile] using chain_good ihA ihB
  | rep n a ihA =>
      simpa [Good, compile] using good_power ihA n

theorem sound_TableBytes : SlashDepth.TableBytes.Goal := by
  change tableBytes SlashDepth.reader
  intro x
  let t := compile SlashDepth.reader x
  have htlen : t.length = SlashDepth.reader.states := by
    dsimp [t]
    exact (compile_good x).1
  have hbound : ∀ v ∈ t, v < 2 ^ width (SlashDepth.reader.states - 1) := by
    intro v hv
    have hv3 : v < 3 := (compile_good x).2 v (by simpa [t] using hv)
    have hw : width (SlashDepth.reader.states - 1) = 2 := by decide
    rw [hw]
    change v < 4
    omega
  have hpi := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
    (width (SlashDepth.reader.states - 1)) t hbound
  simpa [t, htlen] using hpi

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_TableBytes
