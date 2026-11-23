import Std.Tactic

/-!
# Publishing a Bloom summary during spill re-partitioning (`spill-bloom-epoch`)

A build side is split into partitions.  Each partition carries the epoch and
hash salt inherited from its parent, a sealed flag, an observation (`fresh`,
`stale`, `unsealed`, `rereadFail`) and a Bloom summary of keys.  A partition is
safe to skip for a probe key only when it is **sealed**, observed **fresh**,
has **no unsealed descendant** sharing the inherited `(epoch, salt)`, and the
key is absent from its summary.  Deferred child pages therefore force a full
probe even though the parent already reported empty.

The reference computes the full build join.  The optimized computation skips
exactly the safely-sealed-empty partitions, and is proved equal for every
state satisfying `Valid` — which encodes every valid sealing / spill / probe
ordering.  A false positive in a summary can only add a partition-level I/O
event; it never changes the output bag.

C++ counterpart: `executor/detail/pipeline/ooc_hash_join_sink.cpp`.
-/

namespace TinylambOpt.Research.SpillBloomEpoch

/-- Observation attached to a partition summary. -/
inductive Obs where
  | fresh | stale | unsealed | rereadFail
  deriving DecidableEq, Repr

/-- Partition metadata: the epoch and salt are inherited by children. -/
structure Part where
  pid : Nat
  epoch : Nat
  salt : Nat
  sealed : Bool
  summary : List Nat
  obs : Obs
  deriving DecidableEq, Repr

/-- A build row occurrence, placed in a partition at a given epoch. -/
structure BRow where
  id : Nat
  part : Nat
  key : Nat
  epoch : Nat
  deriving DecidableEq, Repr

/-- A probe row occurrence. -/
structure PRow where
  id : Nat
  key : Nat
  deriving DecidableEq, Repr

abbrev Combo := Nat × Nat

/-- An unsealed sibling/cousin with the same inherited `(epoch, salt)` means
    a deferred page may still arrive: the lineage is not complete. -/
def noPending (parts : List Part) (pt : Part) : Bool :=
  !(parts.any (fun q => q.epoch == pt.epoch && q.salt == pt.salt && q.sealed == false))

/-- A partition may be skipped for `key` only with a sealed, fresh, complete
    lineage and an empty Bloom summary for this key. -/
def skippablePart (parts : List Part) (pt : Part) (key : Nat) : Bool :=
  pt.sealed && decide (pt.obs = Obs.fresh) && noPending parts pt &&
    decide (key ∉ pt.summary)

def rowKept (parts : List Part) (p : PRow) (b : BRow) : Bool :=
  match parts.find? (fun pt => pt.pid == b.part) with
  | some pt => !(skippablePart parts pt p.key)
  | none => true   -- unknown partition: never skip

/-- Reference: full build join, no skipping. -/
def refJoin (build : List BRow) (probes : List PRow) : List Combo :=
  probes.flatMap (fun p => (build.filter (fun b => b.key == p.key)).map (fun b => (b.id, p.id)))

/-- Optimized: skip only the certified empty partitions. -/
def optJoin (parts : List Part) (build : List BRow) (probes : List PRow) : List Combo :=
  probes.flatMap (fun p =>
    (build.filter (fun b => b.key == p.key && rowKept parts p b)).map (fun b => (b.id, p.id)))

/-- Naive: trust any summary, ignoring sealing, epoch and deferred children. -/
def naiveJoin (parts : List Part) (build : List BRow) (probes : List PRow) : List Combo :=
  probes.flatMap (fun p =>
    build.filterMap (fun b =>
      match parts.find? (fun pt => pt.pid == b.part) with
      | some pt => if p.key ∉ pt.summary then none else some (b.id, p.id)
      | none => some (b.id, p.id)))

/-! ## Valid states -/

/-- A sealed, fresh, complete partition's summary contains every key present
    in that partition.  Quantifying over all such states covers every valid
    ordering of seal / spill / probe events. -/
def Valid (parts : List Part) (build : List BRow) : Prop :=
  ∀ pt ∈ parts, pt.sealed = true → pt.obs = Obs.fresh → noPending parts pt = true →
    ∀ b ∈ build, b.part = pt.pid → b.key ∈ pt.summary

theorem mem_of_find_some {α : Type} (f : α → Bool) :
    ∀ (l : List α) (x : α), l.find? f = some x → x ∈ l
  | [], _, h => by simp at h
  | y :: ys, x, h => by
      by_cases hy : f y = true
      · have hxy : y = x := by simpa [hy] using h
        subst x
        simp
      · have htail : ys.find? f = some x := by simpa [hy] using h
        exact List.mem_cons_of_mem _ (mem_of_find_some f ys x htail)

theorem pred_of_find_some {α : Type} (f : α → Bool) :
    ∀ (l : List α) (x : α), l.find? f = some x → f x = true
  | [], _, h => by simp at h
  | y :: ys, x, h => by
      by_cases hy : f y = true
      · have hxy : y = x := by simpa [hy] using h
        subst x
        simpa using hy
      · have htail : ys.find? f = some x := by simpa [hy] using h
        exact pred_of_find_some f ys x htail

/-- The skip predicate is exactly the sealed, fresh, fully built, absent-key case. -/
theorem skippable_true_iff (parts : List Part) (pt : Part) (key : Nat) :
    skippablePart parts pt key = true ↔
      pt.sealed = true ∧ pt.obs = Obs.fresh ∧ noPending parts pt = true ∧ key ∉ pt.summary := by
  simp [skippablePart]
  constructor
  · intro h
    exact ⟨h.1.1.1, h.1.1.2, h.1.2, h.2⟩
  · intro h
    exact ⟨⟨⟨h.1, h.2.1⟩, h.2.2.1⟩, h.2.2.2⟩

/-- A row that the optimized path drops cannot be a match. -/
theorem kept_or_no_match (parts : List Part) (build : List BRow)
    (hv : Valid parts build) (b : BRow) (hb : b ∈ build) (p : PRow) :
    rowKept parts p b = true ∨ b.key ≠ p.key := by
  cases hfind : parts.find? (fun pt => pt.pid == b.part) with
  | none => simp [rowKept, hfind]
  | some pt =>
      by_cases hskip : skippablePart parts pt p.key = true
      · right
        have hptmem : pt ∈ parts := mem_of_find_some (fun pt : Part => pt.pid == b.part) parts pt hfind
        have hpred : (pt.pid == b.part) = true := pred_of_find_some (fun pt : Part => pt.pid == b.part) parts pt hfind
        have hpid : pt.pid = b.part := Nat.eq_of_beq_eq_true hpred
        have hs := (skippable_true_iff parts pt p.key).mp hskip
        have hbkey : b.key ∈ pt.summary := hv pt hptmem hs.1 hs.2.1 hs.2.2.1 b hb hpid.symm
        intro hkeq
        exact hs.2.2.2 (by simpa [hkeq] using hbkey)
      · left
        simp [rowKept, hfind, hskip]

theorem kept_filter_eq_match_filter (parts : List Part) (build : List BRow)
    (hv : Valid parts build) (p : PRow) :
    build.filter (fun b => b.key == p.key && rowKept parts p b) =
      build.filter (fun b => b.key == p.key) := by
  induction build with
  | nil => rfl
  | cons b bs ih =>
      have ih' : bs.filter (fun b => b.key == p.key && rowKept parts p b) =
          bs.filter (fun b => b.key == p.key) :=
        ih (fun pt hpt hs hf hp bb hbb hbp => hv pt hpt hs hf hp bb (by simp [hbb]) hbp)
      by_cases hm : (b.key == p.key) = true
      · have hk : rowKept parts p b = true ∨ b.key ≠ p.key :=
          kept_or_no_match parts (b :: bs) hv b (by simp) p
        have hrow : rowKept parts p b = true := by
          rcases hk with hk | hne
          · exact hk
          · exact False.elim (hne (Nat.eq_of_beq_eq_true hm))
        simp only [List.filter_cons]
        simp [hm, hrow, ih']
      · simp only [List.filter_cons]
        simp [hm, ih']

/-! ## Main theorem -/

theorem opt_eq_ref (parts : List Part) (build : List BRow) (probes : List PRow)
    (hv : Valid parts build) : optJoin parts build probes = refJoin build probes := by
  simp [optJoin, refJoin, kept_filter_eq_match_filter parts build hv]

theorem spill_bloom_epoch_sound
    (parts : List Part) (build : List BRow) (probes : List PRow)
    (hv : Valid parts build) :
    optJoin parts build probes = refJoin build probes ∧
    (refJoin build probes).length = (optJoin parts build probes).length := by
  have h := opt_eq_ref parts build probes hv
  exact ⟨h, by rw [h]⟩

/-! ## I/O events: false positives only add I/O -/

/-- One I/O event per sealed fresh complete partition that a probe must read. -/
def sealedIO (parts : List Part) (_build : List BRow) (probes : List PRow) : Nat :=
  (probes.map (fun p =>
    (parts.filter (fun pt =>
      pt.sealed = true ∧ pt.obs = Obs.fresh ∧ noPending parts pt = true ∧
        skippablePart parts pt p.key = false)).length)).sum

/-- For certified partitions, an I/O event happens exactly when the probe key
    is present in the summary; false positives therefore add I/O only. -/
theorem sealedIO_eq_summary_hits (parts : List Part) (build : List BRow)
    (probes : List PRow) :
    sealedIO parts build probes =
      (probes.map (fun p =>
        (parts.filter (fun pt =>
          pt.sealed = true ∧ pt.obs = Obs.fresh ∧ noPending parts pt = true ∧
            p.key ∈ pt.summary)).length)).sum := by
  simp only [sealedIO]
  apply congrArg List.sum
  apply congrArg (fun f => List.map f probes)
  funext p
  apply congrArg List.length
  apply List.filter_congr
  intro pt hpt
  by_cases hseal : pt.sealed = true
  · by_cases hfresh : pt.obs = Obs.fresh
    · by_cases hpend : noPending parts pt = true
      · have hskip : skippablePart parts pt p.key = false ↔ p.key ∈ pt.summary := by
          constructor
          · intro hn
            by_cases hmem : p.key ∈ pt.summary
            · exact hmem
            · exfalso
              have ht : skippablePart parts pt p.key = true := by
                simp [skippablePart, hseal, hfresh, hpend, hmem]
              rw [ht] at hn
              cases hn
          · intro hmem
            simp [skippablePart, hseal, hfresh, hpend, hmem]
        simp [hseal, hfresh, hpend, hskip]
      · simp [hseal, hfresh, hpend]
    · simp [hseal, hfresh]
  · simp [hseal]

/-! ## Regression cases -/

/-- The parent reports an empty summary, but the matching build row lives on
    a deferred child page that inherited `(epoch, salt)` and is still
    unsealed.  Trusting the parent summary drops the row; certifying only
    sealed complete partitions keeps it. -/
theorem parent_empty_child_deferred_counterexample :
    let parts : List Part :=
      [⟨0, 1, 42, true, [], Obs.fresh⟩, ⟨1, 1, 42, false, [], Obs.unsealed⟩]
    let build : List BRow := [⟨7, 1, 77, 1⟩]
    let probes : List PRow := [⟨1, 77⟩]
    naiveJoin parts build probes = [] ∧
    refJoin build probes = [(7, 1)] ∧
    optJoin parts build probes = [(7, 1)] := by
  dsimp
  decide

/-- A false positive `99` in the summary causes one extra partition I/O event
    but leaves the output unchanged. -/
theorem false_positive_extra_io_example :
    let clean : List Part := [⟨0, 1, 42, true, [77], Obs.fresh⟩]
    let fp : List Part := [⟨0, 1, 42, true, [77, 99], Obs.fresh⟩]
    let build : List BRow := [⟨7, 0, 77, 1⟩]
    let probes : List PRow := [⟨1, 99⟩]
    refJoin build probes = [] ∧
    optJoin clean build probes = [] ∧ optJoin fp build probes = [] ∧
    sealedIO clean build probes = 0 ∧ sealedIO fp build probes = 1 := by
  dsimp
  decide

end TinylambOpt.Research.SpillBloomEpoch
