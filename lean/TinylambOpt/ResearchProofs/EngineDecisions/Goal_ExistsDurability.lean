import TinylambOpt.ResearchGoals.EngineDecisions.MVCC

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `ExistsDurability` — The WAL end of the last *committed* truth change is the minimum
  fence that makes this reply reproducible at every possible subsequent WAL prefix.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.MVCC_ExistsDurability

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.ExistsDurability

private theorem pairwise_tail {a : Batch} {bs : List Batch}
    (h : (a :: bs).Pairwise (fun x y => x.walEnd < y.walEnd)) :
    (∀ c ∈ bs, a.walEnd < c.walEnd) ∧
      bs.Pairwise (fun x y => x.walEnd < y.walEnd) := by
  rw [List.pairwise_cons] at h
  exact h

private theorem observation_cons (live : List Nat) (b : Batch) (bs : List Batch) :
    observation live (b :: bs) = observation (applyBatch live b) bs := by
  simp [observation]

private theorem observation_filter_cons_of_le (live : List Nat) (b : Batch) (bs : List Batch)
    (d : Nat) (hbd : b.walEnd ≤ d) :
    observation live ((b :: bs).filter (fun c => c.walEnd ≤ d)) =
      observation (applyBatch live b) (bs.filter (fun c => c.walEnd ≤ d)) := by
  simp [hbd, observation]

private theorem filter_cons_eq_of_gt (live : List Nat) (b : Batch) (bs : List Batch)
    (d : Nat) (hsorted : (b :: bs).Pairwise (fun x y => x.walEnd < y.walEnd))
    (hbd : d < b.walEnd)
    (hempty : (applyBatch live b).isEmpty == live.isEmpty) :
    observation live ((b :: bs).filter (fun c => c.walEnd ≤ d)) =
      observation (applyBatch live b) (bs.filter (fun c => c.walEnd ≤ d)) := by
  have htail := (pairwise_tail hsorted).1
  have hnone : bs.filter (fun c => c.walEnd ≤ d) = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro c hc
    simp only [List.mem_filter] at hc
    have hbc : b.walEnd < c.walEnd := htail c hc.1
    have hnot : ¬ c.walEnd ≤ d := by omega
    simp [hnot] at hc
  have hhead : (b :: bs).filter (fun c => c.walEnd ≤ d) = [] := by
    simp [hbd, hnone]
  rw [hhead, hnone]
  have heq : (applyBatch live b).isEmpty = live.isEmpty := by
    simpa using (of_decide_eq_true hempty)
  simp [observation, heq]

private theorem observation_cons_filter_transfer (live : List Nat) (b : Batch) (bs : List Batch)
    (f d : Nat) (hsorted : (b :: bs).Pairwise (fun x y => x.walEnd < y.walEnd))
    (hbf : f < b.walEnd) (hfd : f ≤ d)
    (hempty : (applyBatch live b).isEmpty == live.isEmpty) :
    observation live ((b :: bs).filter (fun c => c.walEnd ≤ d)) =
      observation (applyBatch live b) (bs.filter (fun c => c.walEnd ≤ d)) := by
  by_cases hbd : b.walEnd ≤ d
  · exact observation_filter_cons_of_le live b bs d hbd
  · exact filter_cons_eq_of_gt live b bs d hsorted (by omega) hempty

private theorem lastFlip_ge_of_lt (live : List Nat) (log : List Batch) (m : Nat)
    (hsorted : log.Pairwise (fun x y => x.walEnd < y.walEnd))
    (h : ∀ b ∈ log, m < b.walEnd) : m ≤ lastFlip live m log := by
  induction log generalizing live m with
  | nil => simp [lastFlip]
  | cons b bs ih =>
      have hb : m < b.walEnd := h b (by simp)
      have hbtail : ∀ c ∈ bs, b.walEnd < c.walEnd := (pairwise_tail hsorted).1
      have hstail := (pairwise_tail hsorted).2
      by_cases he : (applyBatch live b).isEmpty == live.isEmpty
      · simp [lastFlip, he]
        exact ih (applyBatch live b) m hstail (by
          intro c hc
          exact h c (by simp [hc]))
      · simp [lastFlip, he]
        have hb' := ih (applyBatch live b) b.walEnd hstail hbtail
        omega

private theorem char (live : List Nat) (log : List Batch) (m f : Nat) (hm : m ≤ f)
    (hsorted : log.Pairwise (fun x y => x.walEnd < y.walEnd)) :
    (lastFlip live m log ≤ f → ∀ d, f ≤ d →
      observation live (log.filter (fun c => c.walEnd ≤ d)) = observation live log) ∧
    (f < lastFlip live m log →
      ∃ d, f ≤ d ∧
        observation live (log.filter (fun c => c.walEnd ≤ d)) ≠ observation live log) := by
  induction log generalizing live m f with
  | nil =>
      constructor
      · intro _ d _; simp [lastFlip, observation]
      · intro hf
        simp [lastFlip] at hf
        omega
  | cons b bs ih =>
      have htail := pairwise_tail hsorted
      have hbtail : ∀ c ∈ bs, b.walEnd < c.walEnd := htail.1
      have hsortedTail : bs.Pairwise (fun x y => x.walEnd < y.walEnd) := htail.2
      by_cases hflip : (applyBatch live b).isEmpty == live.isEmpty
      · -- no flip at b
        by_cases hbf : b.walEnd ≤ f
        · have ih' := ih (applyBatch live b) m f hm hsortedTail
          constructor
          · intro hle d hfd
            have hle' : lastFlip (applyBatch live b) m bs ≤ f := by
              simpa [lastFlip, hflip] using hle
            have hres := (ih'.1 hle') d hfd
            rw [observation_cons]
            rw [← hres]
            exact observation_filter_cons_of_le live b bs d (by omega)
          · intro hgt
            have hgt' : f < lastFlip (applyBatch live b) m bs := by
              simpa [lastFlip, hflip] using hgt
            rcases ih'.2 hgt' with ⟨d, hfd, hdne⟩
            refine ⟨d, hfd, ?_⟩
            intro heq
            apply hdne
            rw [← observation_filter_cons_of_le live b bs d (by omega), ← observation_cons]
            exact heq
        · have hfb : f < b.walEnd := by omega
          have ih' := ih (applyBatch live b) m f hm hsortedTail
          constructor
          · intro hle d hfd
            have hle' : lastFlip (applyBatch live b) m bs ≤ f := by
              simpa [lastFlip, hflip] using hle
            have hres := (ih'.1 hle') d hfd
            rw [observation_cons]
            rw [← hres]
            exact observation_cons_filter_transfer live b bs f d hsorted hfb hfd hflip
          · intro hgt
            have hgt' : f < lastFlip (applyBatch live b) m bs := by
              simpa [lastFlip, hflip] using hgt
            rcases ih'.2 hgt' with ⟨d, hfd, hdne⟩
            refine ⟨d, hfd, ?_⟩
            intro heq
            apply hdne
            rw [← observation_cons_filter_transfer live b bs f d hsorted hfb hfd hflip, ← observation_cons]
            exact heq
      · -- flip at b
        by_cases hbf : b.walEnd ≤ f
        · have ih' := ih (applyBatch live b) b.walEnd f (by omega) hsortedTail
          constructor
          · intro hle d hfd
            have hle' : lastFlip (applyBatch live b) b.walEnd bs ≤ f := by
              simpa [lastFlip, hflip] using hle
            have hres := (ih'.1 hle') d hfd
            rw [observation_cons]
            rw [← hres]
            exact observation_filter_cons_of_le live b bs d (by omega)
          · intro hgt
            have hgt' : f < lastFlip (applyBatch live b) b.walEnd bs := by
              simpa [lastFlip, hflip] using hgt
            rcases ih'.2 hgt' with ⟨d, hfd, hdne⟩
            refine ⟨d, hfd, ?_⟩
            intro heq
            apply hdne
            rw [← observation_filter_cons_of_le live b bs d (by omega), ← observation_cons]
            exact heq
        · have hfb : f < b.walEnd := by omega
          have hge : b.walEnd ≤ lastFlip (applyBatch live b) b.walEnd bs :=
            lastFlip_ge_of_lt (applyBatch live b) bs b.walEnd hsortedTail hbtail
          constructor
          · intro hle
            have : lastFlip (applyBatch live b) b.walEnd bs ≤ f := by
              simpa [lastFlip, hflip] using hle
            omega
          · intro _
            by_cases hgt : b.walEnd < lastFlip (applyBatch live b) b.walEnd bs
            · have ih' := ih (applyBatch live b) b.walEnd b.walEnd (Nat.le_refl _) hsortedTail
              rcases ih'.2 hgt with ⟨d, hfd, hdne⟩
              refine ⟨d, by omega, ?_⟩
              intro heq
              apply hdne
              rw [← observation_filter_cons_of_le live b bs d (by omega), ← observation_cons]
              exact heq
            · have hle' : lastFlip (applyBatch live b) b.walEnd bs ≤ b.walEnd := by omega
              have ih' := ih (applyBatch live b) b.walEnd b.walEnd (Nat.le_refl _) hsortedTail
              have hstab := ih'.1 hle' b.walEnd (Nat.le_refl _)
              have hnone : bs.filter (fun c => c.walEnd ≤ b.walEnd) = [] := by
                apply List.eq_nil_iff_forall_not_mem.mpr
                intro c hc
                simp only [List.mem_filter] at hc
                have hbc := hbtail c hc.1
                have hnot : ¬ c.walEnd ≤ b.walEnd := by omega
                simp [hnot] at hc
              have hstab' : observation (applyBatch live b) [] = observation (applyBatch live b) bs := by
                simpa [hnone] using hstab
              have hnoneF : (b :: bs).filter (fun c => c.walEnd ≤ f) = [] := by
                apply List.eq_nil_iff_forall_not_mem.mpr
                intro c hc
                have hcMem : c ∈ b :: bs := (List.mem_filter.mp hc).1
                have hcLe : c.walEnd ≤ f := of_decide_eq_true (List.mem_filter.mp hc).2
                have hcCons : c = b ∨ c ∈ bs := by simpa using hcMem
                rcases hcCons with hcb | hcbs
                · subst c
                  exact hbf hcLe
                · have hbc := hbtail c hcbs
                  omega
              refine ⟨f, Nat.le_refl f, ?_⟩
              rw [hnoneF]
              have hne : (applyBatch live b).isEmpty ≠ live.isEmpty := by
                intro he
                apply hflip
                simp [he]
              have hstab'' : (applyBatch live b).isEmpty =
                  (bs.foldl applyBatch (applyBatch live b)).isEmpty := by
                simpa [observation] using hstab'
              have hfull : observation live (b :: bs) = observation (applyBatch live b) bs :=
                observation_cons live b bs
              rw [hfull]
              simp [observation]
              intro hlf
              exact hne (hstab''.trans hlf.symm)

theorem sound : Goal := by
  intro initial log fence hpos hsorted
  have hc := char initial log 0 fence (Nat.zero_le _) hsorted
  constructor
  · intro hle durable hd
    exact hc.1 hle durable hd
  · intro hall
    by_cases hle : lastFlip initial 0 log ≤ fence
    · exact hle
    · have hgt : fence < lastFlip initial 0 log := by omega
      rcases hc.2 hgt with ⟨d, hfd, hdne⟩
      exact False.elim (hdne (hall d hfd))

end TinylambOpt.ResearchProofs.EngineDecisions.MVCC_ExistsDurability
