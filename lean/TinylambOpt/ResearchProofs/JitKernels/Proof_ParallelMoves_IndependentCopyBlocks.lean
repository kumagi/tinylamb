import TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-!
Two serial move bundles commute when neither writes a register the other reads
or writes. The proof uses two invariants: a move list never changes a register
outside its destinations, and two runs that agree on all sources of a list
agree on all of its destinations. Applying both in each direction shows the two
composed bundles agree on every register.

Proved

- `IndependentCopyBlocks` — non-interfering copy blocks may be scheduled in
  either order without changing the final register file.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace Nov26.Proof_ParallelMoves_IndependentCopyBlocks

open TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

/-- A serial bundle leaves registers outside its destinations untouched. -/
theorem serial_not_written {ms : List Move} {r : Nat} (h : r ∉ ms.map Prod.fst) :
    ∀ s : File, serial s ms r = s r := by
  induction ms with
  | nil => intro s; rfl
  | cons m rest ih =>
      intro s
      have hrest : r ∉ rest.map Prod.fst := by
        intro hmem
        exact h (by simp [hmem])
      have hne : r ≠ m.1 := by
        intro heq
        exact h (by simp [heq])
      have hih := ih hrest (put s m.1 (s m.2))
      simpa [serial, put, hne] using hih

/-- Two runs that agree on a bundle's sources agree on its destinations. -/
theorem serial_dest_congr {ms : List Move} {t u : File}
    (hsrc : ∀ m ∈ ms, t m.2 = u m.2) :
    ∀ d ∈ ms.map Prod.fst, serial t ms d = serial u ms d := by
  induction ms generalizing t u with
  | nil => simp
  | cons m rest ih =>
      have hsrc' : ∀ m' ∈ rest,
          (put t m.1 (t m.2)) m'.2 = (put u m.1 (u m.2)) m'.2 := by
        intro m' hm'
        by_cases heq : m'.2 = m.1
        · rw [heq]
          simp [put, hsrc m (by simp)]
        · simp [put, heq, hsrc m' (by simp [hm'])]
      intro d hd
      simp only [List.map_cons, List.mem_cons] at hd
      rcases hd with rfl | hdrest
      · by_cases hd0 : m.1 ∈ rest.map Prod.fst
        · exact ih hsrc' m.1 (by simp [hd0])
        · have ht := serial_not_written hd0 (put t m.1 (t m.2))
          have hu := serial_not_written hd0 (put u m.1 (u m.2))
          calc
            serial t (m :: rest) m.1 = (put t m.1 (t m.2)) m.1 := by simpa [serial] using ht
            _ = t m.2 := by simp [put]
            _ = u m.2 := hsrc m (by simp)
            _ = (put u m.1 (u m.2)) m.1 := by simp [put]
            _ = serial u (m :: rest) m.1 := by simpa [serial] using hu.symm
      · exact ih hsrc' d hdrest

theorem sound : IndependentCopyBlocks.Goal := by
  intro s a b h
  have hb_not_a_src : ∀ x ∈ a, ∀ y ∈ b, x.2 ≠ y.1 := by
    intro x hx y hy
    exact fun heq => (h x hx y hy).2.2 heq.symm
  have ha_not_b_src : ∀ y ∈ b, ∀ x ∈ a, y.2 ≠ x.1 := by
    intro y hy x hx
    exact fun heq => (h x hx y hy).2.1 heq.symm
  have hdest_disj : ∀ x ∈ a, x.1 ∉ b.map Prod.fst := by
    intro x hx hmem
    rcases List.mem_map.mp hmem with ⟨y, hy, heq⟩
    exact (h x hx y hy).1 heq.symm
  have hsrc_a : ∀ m ∈ a, (serial s b) m.2 = s m.2 := by
    intro m hm
    exact serial_not_written (by
      intro hmem
      rcases List.mem_map.mp hmem with ⟨y, hy, heq⟩
      exact hb_not_a_src m hm y hy heq.symm) s
  have hsrc_b : ∀ m ∈ b, (serial s a) m.2 = s m.2 := by
    intro m hm
    exact serial_not_written (by
      intro hmem
      rcases List.mem_map.mp hmem with ⟨x, hx, heq⟩
      exact ha_not_b_src m hm x hx heq.symm) s
  simp [serial, List.foldl_append]
  funext r
  by_cases hra : r ∈ a.map Prod.fst
  · have hrnb : r ∉ b.map Prod.fst := by
      rcases List.mem_map.mp hra with ⟨x, hx, rfl⟩
      exact hdest_disj x hx
    calc
      serial (serial s a) b r = serial s a r := serial_not_written hrnb (serial s a)
      _ = serial (serial s b) a r := (serial_dest_congr hsrc_a r hra).symm
  · by_cases hrb : r ∈ b.map Prod.fst
    · calc
        serial (serial s a) b r = serial s b r := serial_dest_congr hsrc_b r hrb
        _ = serial (serial s b) a r := (serial_not_written hra (serial s b)).symm
    · have hleft : serial (serial s a) b r = s r :=
        (serial_not_written hrb (serial s a)).trans (serial_not_written hra s)
      have hright : serial (serial s b) a r = s r :=
        (serial_not_written hra (serial s b)).trans (serial_not_written hrb s)
      exact hleft.trans hright.symm

end Nov26.Proof_ParallelMoves_IndependentCopyBlocks
