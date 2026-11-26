import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `CStringEnd.TableBytes` — Every successor state fits the packed dictionary of state
  identifiers.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_CStringEnd_TableBytes

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.CStringEnd

/-- Small-input truth check: a three-state table packed at width 2
    (`width (states-1) = width 2 = 2`) must round-trip byte for byte. -/
private def samples : List Rope :=
  [ .lit [], .lit [0], .lit [1], .lit [0, 0], .lit [65, 0, 66],
    .cat (.lit [65]) (.lit [0]), .cat (.lit [0]) (.lit [65]),
    .rep 0 (.lit [0]), .rep 2 (.lit [0]), .rep 3 (.lit [65, 0]),
    .cat (.rep 2 (.lit [65])) (.lit [0]),
    .rep 5 (.cat (.lit [65]) (.lit [0, 66])) ]

private def check : Bool :=
  samples.all fun x =>
    let t := compile reader x
    (List.range reader.states).map
        (get (pack (width (reader.states - 1)) t) (width (reader.states - 1))) == t

#eval check

example : check := by native_decide

/-- A transition table is good when it has exactly three lanes and every
    successor fits in them. -/
private def Good (t : Table) : Prop := t.length = 3 ∧ ∀ v ∈ t, v < 3

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem step_bounded {q b : Nat} (_hq : q < reader.states) :
    reader.step q b < reader.states := by
  dsimp [reader] at _hq ⊢
  by_cases hq0 : q == 0 <;> by_cases hb0 : b == 0 <;> simp [hq0, hb0] <;> omega

private theorem foldl_step_bounded (xs : Bytes) {q : Nat}
    (hq : q < reader.states) :
    xs.foldl reader.step q < reader.states := by
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

private theorem compile_good (x : Rope) : Good (compile reader x) := by
  dsimp [Good]
  induction x with
  | lit s =>
      constructor
      · simp [compile, reader]
      · intro v hv
        simp [compile] at hv
        rcases hv with ⟨q, hqmem, rfl⟩
        have hq : q < reader.states := by simpa using hqmem
        have hb : s.foldl reader.step q < reader.states := foldl_step_bounded s hq
        simpa [reader] using hb
  | cat a b ihA ihB =>
      simpa [Good, compile] using chain_good ihA ihB
  | rep n a ihA =>
      simpa [Good, compile] using good_power ihA n

theorem sound_TableBytes : CStringEnd.TableBytes.Goal := by
  change tableBytes reader
  intro x
  let t := compile reader x
  have htlen : t.length = reader.states := by
    dsimp [t]
    have h := (compile_good x).1
    simpa [reader] using h
  have hbound : ∀ v ∈ t, v < 2 ^ width (reader.states - 1) := by
    intro v hv
    have hv3 : v < 3 := by
      apply (compile_good x).2
      simpa [t] using hv
    have hw : width (reader.states - 1) = 2 := by decide
    rw [hw]
    change v < 4
    omega
  have hpi := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
    (width (reader.states - 1)) t hbound
  simpa [t, htlen] using hpi

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_CStringEnd_TableBytes
