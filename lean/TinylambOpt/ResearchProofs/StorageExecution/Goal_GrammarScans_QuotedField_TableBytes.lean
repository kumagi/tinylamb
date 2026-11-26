import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_QuotedField_TableBytes

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField

/-! Small-input truth check: the quote-parity machine has two states, hence
every compiled table entry is a single bit; re-reading the packed table at
width 1 must reproduce it. -/
private def samples : List Rope :=
  [ .lit [], .lit [34], .lit [65], .lit [34, 34], .lit [34, 65, 34],
    .cat (.lit [34]) (.lit [65]), .cat (.lit [65, 34]) (.lit [34]),
    .rep 0 (.lit [34]), .rep 2 (.lit [34]), .rep 3 (.lit []),
    .rep 2 (.lit [34, 65, 34]), .cat (.rep 2 (.lit [65])) (.lit [34]),
    .rep 7 (.cat (.lit [34]) (.lit [65, 34, 66])) ]

private def check : Bool :=
  samples.all fun x =>
    let t := compile reader x
    (List.range reader.states).map
      (get (pack (width (reader.states - 1)) t) (width (reader.states - 1))) == t

#eval check

example : check := by native_decide

/-! Generic compile-table length and bound lemmas (self-contained, since the
shared StorageExecution proof bundle is not available here). -/

private theorem gs_power_length (n : Nat) (t : Table) :
    (power n t).length = t.length := by
  induction n using Nat.strongRecOn generalizing t with
  | ind n ih =>
      unfold power
      split
      · simp [List.length_range]
      · rename_i hn
        split <;> simp only [chain, List.length_map] <;>
          exact ih (n / 2) (Nat.div_lt_self (by omega) (by decide)) t

private theorem gs_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
  | rep k a ih => simp [compile, gs_power_length, ih]

private def GsClosed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem gs_foldl_closed (r : Reader) (h : GsClosed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem gs_chain_bound (r : Reader) (_h : GsClosed r) (a b : Table)
    (ha : ∀ q ∈ a, q < r.states) (hb : ∀ q ∈ b, q < r.states) :
    ∀ q ∈ chain a b, q < r.states := by
  intro q hq
  rw [chain, List.mem_map] at hq
  obtain ⟨i, hi, heq⟩ := hq
  rw [← heq]
  unfold applyT
  by_cases hib : i < b.length
  · rw [List.getElem?_eq_getElem hib]
    exact hb b[i] (List.getElem_mem hib)
  · rw [List.getElem?_eq_none (by omega)]
    exact ha i hi

private theorem gs_compile_bound (r : Reader) (h : GsClosed r) (x : Rope) :
    ∀ q ∈ compile r x, q < r.states := by
  induction x with
  | lit s =>
      intro q hq
      simp only [compile, List.mem_map] at hq
      obtain ⟨i, hi, rfl⟩ := hq
      exact gs_foldl_closed r h s i (List.mem_range.mp hi)
  | cat a b iha ihb =>
      exact gs_chain_bound r h (compile r a) (compile r b) iha ihb
  | rep k a ih =>
      change ∀ q, q ∈ power k (compile r a) → q < r.states
      induction k using Nat.strongRecOn with
      | ind k ihk =>
          unfold power
          split
          · intro q hq
            exact List.mem_range.mp (by simpa [gs_compile_length] using hq)
          · rename_i hk
            split
            · exact gs_chain_bound r h _ _
                (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
                (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
            · exact gs_chain_bound r h (chain (power (k / 2) (compile r a))
                  (power (k / 2) (compile r a))) (compile r a)
                (gs_chain_bound r h _ _
                  (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
                  (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))) ih

private theorem gs_quote_closed : GsClosed reader := by
  intro q b hq
  dsimp [reader] at hq ⊢
  by_cases h : b == 34
  · simp [h]
    omega
  · simpa [h] using hq

theorem sound_TableBytes : QuotedField.TableBytes.Goal := by
  intro x
  let t := compile reader x
  have htlen : t.length = reader.states := by
    dsimp [t]
    exact gs_compile_length reader x
  have hbound : ∀ v ∈ t, v < 2 ^ width (reader.states - 1) := by
    intro v hv
    have hv2 : v < 2 := by
      dsimp [t] at hv
      simpa [reader] using gs_compile_bound reader gs_quote_closed x v hv
    simpa [reader, width] using hv2
  have hpi := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
    (width (reader.states - 1)) t hbound
  simpa [t, htlen] using hpi

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_QuotedField_TableBytes
