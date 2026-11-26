import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
A parallel move bundle may be lowered in program order when every move's source
is untouched by all earlier destinations. The proof first converts the indexed
`freeSrc` condition into a structural "no later source is clobbered" predicate,
then shows each serial step reads the same original value as its parallel
counterpart and folding the remaining moves from the updated file agrees.

Proved

- `NoEarlierClobberSchedule` — a serial machine-move sequence implements the
  parallel transfer under the no-earlier-clobber guard.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_ParallelMoves_NoEarlierClobberSchedule

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-- No destination of an earlier move is the source of a later one. -/
def Safe : List Move → Prop
| [] => True
| m :: ms => (∀ m' ∈ ms, m'.2 ≠ m.1) ∧ Safe ms

/-- Membership in a list yields an index whose `get?` finds the element. -/
theorem mem_getQ {a : Move} {l : List Move} (h : a ∈ l) : ∃ i : Nat, l[i]? = some a := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      simp only [List.mem_cons] at h
      rcases h with rfl | h
      · exact ⟨0, rfl⟩
      · rcases ih h with ⟨i, hi⟩
        exact ⟨i + 1, by simpa using hi⟩

/-- The indexed guard implies the structural one: shifting indices by one
exposes the head destination in the `take` prefix. -/
theorem hyp_to_safe {ms : List Move}
    (h : ∀ i m, ms[i]? = some m → freeSrc (ms.take i) m = true) : Safe ms := by
  induction ms with
  | nil => trivial
  | cons m rest ih =>
      constructor
      · intro m' hm'
        rcases mem_getQ hm' with ⟨i, hi⟩
        have hidx : (m :: rest)[i + 1]? = some m' := by simpa using hi
        have hf := h (i + 1) m' hidx
        simp [freeSrc] at hf
        exact hf.1
      · apply ih
        intro j m' hj
        have hidx : (m :: rest)[j + 1]? = some m' := by simpa using hj
        have hf := h (j + 1) m' hidx
        simp [freeSrc] at hf
        simpa [freeSrc, List.map_take] using hf.2

/-- Folding parallel moves only depends on the source file at the moves'
sources, provided the accumulator is fixed. -/
theorem foldl_src_congr {src₁ src₂ : File} {ms : List Move}
    (h : ∀ m ∈ ms, src₁ m.2 = src₂ m.2) : ∀ acc : File,
    ms.foldl (fun t m => put t m.1 (src₁ m.2)) acc =
      ms.foldl (fun t m => put t m.1 (src₂ m.2)) acc := by
  induction ms with
  | nil => intro acc; rfl
  | cons m rest ih =>
      intro acc
      simp only [List.foldl_cons]
      rw [h m (by simp)]
      apply ih
      intro m' hm'
      exact h m' (by simp [hm'])

theorem safe_serial_eq_parallel : ∀ ms : List Move, Safe ms → ∀ s : File,
    serial s ms = parallelCopy s ms := by
  intro ms
  induction ms with
  | nil => intro hs s; rfl
  | cons m rest ih =>
      intro hs s
      let t : File := put s m.1 (s m.2)
      have ht : ∀ m' ∈ rest, t m'.2 = s m'.2 := by
        intro m' hm'
        simp [t, put, hs.1 m' hm']
      have hih := ih hs.2 t
      have hcongr := foldl_src_congr ht (put s m.1 (s m.2))
      simpa [serial, parallelCopy, t] using hih.trans hcongr

theorem sound : NoEarlierClobberSchedule.Goal := by
  intro ms s h
  exact safe_serial_eq_parallel ms (hyp_to_safe h) s

end Nov26.Proof_ParallelMoves_NoEarlierClobberSchedule
