import TinylambOpt.ResearchGoals.S3Durability.History

/-!
Proofs for the S3 WAL history goals. A create leaves every occupied key alone,
so a walk that reads only occupied keys is unchanged and a walk that reaches a
free key can only grow. These results let COMMIT be acknowledged from the
contiguous S3 prefix while dead writers' requests keep landing, and let the
seal marker end an epoch for every reader.
-/

namespace TinylambOpt.ResearchProofs.S3Durability.History

open TinylambOpt.ResearchGoals.S3Durability.History

/-- A conditional create never changes an occupied key. -/
theorem create_of_some {st : Store} {j : Nat} {x : Obj} (k : Nat) (o : Obj)
    (h : st j = some x) : create st k o j = some x := by
  simp [create, h]

/-- The restore walk only grows under conditional creates. -/
theorem walk_prefix_create (fuel : Nat) :
    ∀ (st : Store) (k : Nat) (o : Obj) (s lo : Nat),
      walk fuel st s lo <+: walk fuel (create st k o) s lo := by
  induction fuel with
  | zero => intro st k o s lo; simp [walk]
  | succ n ih =>
    intro st k o s lo
    cases hs : st s with
    | none =>
      have h1 : walk (n + 1) st s lo = [] := by simp [walk, hs]
      rw [h1]; exact List.nil_prefix
    | some x =>
      have hc : create st k o s = some x := create_of_some k o hs
      cases x with
      | terminal =>
        have h1 : walk (n + 1) st s lo = [] := by simp [walk, hs]
        rw [h1]; exact List.nil_prefix
      | chunk c =>
        by_cases hl : c.lo = lo
        · have h1 : walk (n + 1) st s lo = c.recs ++ walk n st (s + 1) (c.hi + 1) := by
            simp [walk, hs, hl]
          have h2 : walk (n + 1) (create st k o) s lo =
              c.recs ++ walk n (create st k o) (s + 1) (c.hi + 1) := by
            simp [walk, hc, hl]
          rw [h1, h2]
          exact (List.prefix_append_right_inj c.recs).mpr (ih st k o (s + 1) (c.hi + 1))
        · have h1 : walk (n + 1) st s lo = [] := by simp [walk, hs, hl]
          rw [h1]; exact List.nil_prefix

theorem walk_monotone : WalkMonotone.Goal :=
  fun fuel st k o s lo => walk_prefix_create fuel st k o s lo

/-- The records of a stored contiguous chain are a prefix of the walk. -/
theorem acked_prefix (cs : List Chunk) :
    ∀ (st : Store) (s lo fuel : Nat), Chained cs lo → Placed st s cs →
      cs.length ≤ fuel → (cs.flatMap Chunk.recs) <+: walk fuel st s lo := by
  induction cs with
  | nil => intro st s lo fuel _ _ _; simp
  | cons c cs ih =>
    intro st s lo fuel hch hpl hlen
    simp only [Chained] at hch
    simp only [Placed] at hpl
    obtain ⟨hlo, hch'⟩ := hch
    obtain ⟨hst, hpl'⟩ := hpl
    cases fuel with
    | zero => simp at hlen
    | succ n =>
      have h1 : walk (n + 1) st s lo = c.recs ++ walk n st (s + 1) (c.hi + 1) := by
        simp [walk, hst, hlo]
      have hlen' : cs.length ≤ n := by simp at hlen; omega
      rw [h1, List.flatMap_cons]
      exact (List.prefix_append_right_inj c.recs).mpr
        (ih st (s + 1) (c.hi + 1) n hch' hpl' hlen')

theorem acked_prefix_official : AckedPrefixOfficial.Goal :=
  fun cs st s lo fuel hch hpl hlen => acked_prefix cs st s lo fuel hch hpl hlen

/-- Behind a seal the walk reads only occupied keys, so no create changes it. -/
theorem seal_final (cs : List Chunk) :
    ∀ (st : Store) (s lo fuel k : Nat) (o : Obj), Chained cs lo → Placed st s cs →
      st (s + cs.length) = some .terminal →
      walk fuel (create st k o) s lo = walk fuel st s lo := by
  induction cs with
  | nil =>
    intro st s lo fuel k o _ _ hseal
    have hs : st s = some .terminal := by simpa using hseal
    have hc := create_of_some k o hs
    cases fuel with
    | zero => simp [walk]
    | succ n => simp [walk, hs, hc]
  | cons c cs ih =>
    intro st s lo fuel k o hch hpl hseal
    simp only [Chained] at hch
    simp only [Placed] at hpl
    obtain ⟨hlo, hch'⟩ := hch
    obtain ⟨hst, hpl'⟩ := hpl
    have hc := create_of_some k o hst
    cases fuel with
    | zero => simp [walk]
    | succ n =>
      have hseal' : st (s + 1 + cs.length) = some .terminal := by
        have e : s + 1 + cs.length = s + (c :: cs).length := by simp; omega
        rw [e]; exact hseal
      have ih' := ih st (s + 1) (c.hi + 1) n k o hch' hpl' hseal'
      simp [walk, hst, hc, hlo, ih']

theorem seal_final_goal : SealFinal.Goal :=
  fun cs st s lo fuel k o hch hpl hseal => seal_final cs st s lo fuel k o hch hpl hseal

/-- Counterexample: key 0 is free and key 1 holds a chunk. A restore stops at
key 0, so the chunk's record is not in the history. -/
theorem non_contiguous_ack_unsafe : ¬ NonContiguousAck.Goal := by
  intro h
  have := h (fun j => if j = 1 then some (.chunk ⟨1, 1, [7]⟩) else none)
    ⟨1, 1, [7]⟩ 3 7 (by simp) (by simp)
  simp [walk] at this

end TinylambOpt.ResearchProofs.S3Durability.History
