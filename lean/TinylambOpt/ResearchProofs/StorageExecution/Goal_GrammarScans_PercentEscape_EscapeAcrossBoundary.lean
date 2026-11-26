import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_PercentEscape_EscapeAcrossBoundary

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape

/-! ## Truth check on small inputs

`[37, 52, 70]` is `%`, `4`, `F`.  From *any* of the four states the first byte
drives the reader to state 1, the (hex digit) `4` to state 2, and the (hex
digit) `F` to state 3, which is accepting.  Hence the goal holds for every
rope `x`, regardless of the state reached after `x`. -/

private def samples : List Rope :=
  [ .lit []
  , .lit [37]
  , .lit [65, 37, 48]
  , .lit [37, 52, 70]
  , .rep 5 (.lit [37])
  , .rep 3 (.lit [37, 52, 70])
  , .rep 0 (.lit [1, 2, 3])
  , .cat (.lit [37, 52]) (.lit [70, 37])
  , .cat (.rep 2 (.lit [37, 70])) (.lit [52])
  , .cat (.lit [37]) (.rep 4 (.lit [65, 66]))
  ]

#eval samples.all (fun x => matched reader (Rope.cat x (Rope.lit [37, 52, 70])) 0)

example : samples.all (fun x => matched reader (Rope.cat x (Rope.lit [37, 52, 70])) 0) = true := by
  native_decide

#guard matched reader (Rope.cat (Rope.lit []) (Rope.lit [37, 52, 70])) 0
#guard matched reader (Rope.cat (Rope.lit [37]) (Rope.lit [37, 52, 70])) 0

/-! ## Generic transition-table facts (self-contained) -/

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · simp [power, hn]
      · have hpos : 0 < n := by omega
        have hhalf : (power (n / 2) t).length = t.length :=
          ih (n / 2) (Nat.div_lt_self hpos (by decide : 1 < 2))
        unfold power
        rw [dite_eq_right hn]
        dsimp only
        by_cases heven : n % 2 == 0
        · simp [chain, heven, hhalf]
        · simp [chain, heven, hhalf]

private theorem compile_length (r : Reader) (x : Rope) : (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain, ihA]
  | rep n a ihA =>
      change (power n (compile r a)).length = r.states
      rw [power_length, ihA]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain] using hq
  simp [applyT, chain, List.getElem?_eq_getElem hq', List.getElem?_eq_getElem hq]

private def Closed (r : Reader) : Prop := ∀ q b, q < r.states → r.step q b < r.states

private theorem foldl_closed (r : Reader) (h : Closed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem chain_bound (r : Reader) (_h : Closed r) (a b : Table)
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

private theorem compile_bound (r : Reader) (h : Closed r) (x : Rope) :
    ∀ q ∈ compile r x, q < r.states := by
  induction x with
  | lit s =>
      intro q hq
      simp only [compile, List.mem_map] at hq
      obtain ⟨i, hi, rfl⟩ := hq
      exact foldl_closed r h s i (List.mem_range.mp hi)
  | cat a b iha ihb =>
      exact chain_bound r h (compile r a) (compile r b) iha ihb
  | rep k a ih =>
      change ∀ q, q ∈ power k (compile r a) → q < r.states
      induction k using Nat.strongRecOn with
      | ind k ihk =>
          unfold power
          split
          · intro q hq
            exact List.mem_range.mp (by simpa [compile_length] using hq)
          · rename_i hk
            split
            · exact chain_bound r h _ _
                (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
                (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
            · exact chain_bound r h (chain (power (k / 2) (compile r a))
                  (power (k / 2) (compile r a))) (compile r a)
                (chain_bound r h _ _
                  (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
                  (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))) ih

/-! ## `PercentEscape.reader` specifics -/

private theorem percent_closed : Closed reader := by
  intro q b hq
  change (if b == 37 then 1
    else if (48 ≤ b && b ≤ 57) || (65 ≤ b && b ≤ 70) || (97 ≤ b && b ≤ 102) then
      if q == 1 || q == 2 then q + 1 else 0
    else 0) < 4
  split
  · omega
  · split
    · rename_i hhex
      split
      · rename_i hq12
        have hq' : q = 1 ∨ q = 2 := by simpa using hq12
        omega
      · omega
    · omega

private theorem percent_escape (q : Nat) : [37, 52, 70].foldl reader.step q = 3 := by
  simp [reader]

/-! ## The theorem -/

theorem sound_EscapeAcrossBoundary : PercentEscape.EscapeAcrossBoundary.Goal := by
  intro x
  unfold matched
  change reader.accept
    (applyT (chain (compile reader x) (compile reader (Rope.lit [37, 52, 70]))) 0) = true
  rw [applyT_chain (compile reader x) (compile reader (Rope.lit [37, 52, 70])) 0
    (by rw [compile_length]; decide)]
  let q0 := applyT (compile reader x) 0
  have hq0 : q0 < reader.states := by
    dsimp [q0]
    unfold applyT
    have hpos : 0 < (compile reader x).length := by rw [compile_length]; decide
    rw [List.getElem?_eq_getElem hpos]
    exact compile_bound reader percent_closed x (compile reader x)[0] (List.getElem_mem hpos)
  change reader.accept (applyT (compile reader (Rope.lit [37, 52, 70])) q0) = true
  rw [compile]
  unfold applyT
  have hlen : q0 < (List.range reader.states).length := by simpa using hq0
  have hget : (List.range reader.states)[q0]? = some (List.range reader.states)[q0] :=
    List.getElem?_eq_getElem hlen
  rw [List.getElem?_map, hget]
  simp [percent_escape, reader]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_PercentEscape_EscapeAcrossBoundary
