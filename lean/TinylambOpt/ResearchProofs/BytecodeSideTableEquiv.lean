import TinylambOpt.ResearchGoals.BytecodeSideTableEquiv

/-!
# Soundness of full-program-aligned aggregate-slot deduplication

`evalSlotsWith sameFullProgram progs rows` deduplicates the aggregate programs
of a fused group-by join: `assignSlots` hands out one slot number per program
that is new w.r.t. an association list `seen` of first occurrences (compared
with `sameFullProgram`, i.e. *all* fields including the side tables
`ranges` / `recips` / `stringSets` and `resultTypeTag`); the `uniqueProgs`
`foldl` keeps one entry per slot id, and every slot id is finally resolved
through `slotSums.find?`.

We prove this equals `evalRefSlots progs rows`, i.e. independently evaluating
every program.  The proof is an induction on the program list generalising the
`seen` environment (the `assignSlots` accumulator, captured by the `SlotInv`
invariant that the `k`-th entry of `seen` carries slot number `k`, so fresh
slots never collide), plus per-row-congruence index-remap lemmas for the
`foldl` side table and the `find?`/`getElem?` chains:

* `foldl_dedup_eq_append` — the `foldl` never reorders: it appends fresh
  entries at the end, so the initial table stays a prefix.
* `swapSeen_find` — after deduplication, a slot-keyed lookup of slot `s`
  returns exactly the first program that owned `s`.
* `swapSeen_snoc_find` — freshly created slots look up the new program.
* `go_correct` — the main induction generalising `seen`.
-/

namespace TinylambOpt.ResearchProofs.BytecodeSideTableEquiv

open TinylambOpt.ResearchGoals.BytecodeSideTableEquiv

/-- Reference value of one aggregate slot (one row-wise `evalProg` sweep). -/
private def sumEval (rows : List DataRow) (p : Program) : Int :=
  (rows.map (evalProg p)).sum

private def swapPair (x : Program × Nat) : Nat × Program := (x.2, x.1)

/-- The first-occurrence table `seen : List (Program × Nat)` viewed the way
    the `uniqueProgs` fold stores it, as `List (Nat × Program)`. -/
private def swapSeen (seen : List (Program × Nat)) : List (Nat × Program) :=
  seen.map swapPair

/-- One `foldl` step of `evalSlotsWith`'s `uniqueProgs` build: keep the first
    entry for each slot id. -/
private def dedupStep (acc : List (Nat × Program)) (x : Program × Nat) :
    List (Nat × Program) :=
  if acc.any (fun q => q.1 == x.2) then acc else acc ++ [(x.2, x.1)]

/-- `evalSlotsWith`'s final side-table lookup. -/
private def slotLookup (slotSums : List (Nat × Int)) (sid : Nat) : Int :=
  match slotSums.find? (fun q => q.1 == sid) with
  | some (_, s) => s
  | none => 0

private theorem beq_eq_false_of_ne {m n : Nat} (h : ¬(m = n)) : (m == n) = false := by
  cases h' : m == n with
  | true => exact absurd (Nat.eq_of_beq_eq_true h') h
  | false => exact rfl

/-- Invariant of the `assignSlots` accumulator: the entry at index `k` carries
    slot number `k`. -/
private def SlotInv (seen : List (Program × Nat)) : Prop :=
  ∀ (k : Nat) (y : Program × Nat), seen[k]? = some y → y.2 = k

private theorem slotInv_nil : SlotInv ([] : List (Program × Nat)) := by
  intro k y h
  exact absurd h (by simp)

private theorem slotInv_snoc (seen : List (Program × Nat)) (hseen : SlotInv seen)
    (p : Program) : SlotInv (seen ++ [(p, seen.length)]) := by
  intro k y h
  rw [List.getElem?_append] at h
  rcases Nat.lt_or_ge k seen.length with hk | hk
  · rw [if_pos hk] at h
    exact hseen k y h
  · rw [if_neg (by omega)] at h
    have hk' : seen.length ≤ k := by omega
    rcases Nat.eq_or_lt_of_le hk' with hkeq | hkg
    · have hidx : k - seen.length = 0 := by omega
      rw [hidx, List.getElem?_cons_zero] at h
      have hy : y = (p, seen.length) := (Option.some.inj h).symm
      subst hy
      exact hkeq
    · have hne : ([(p, seen.length)] : List (Program × Nat)).length ≤ k - seen.length := by
        rw [show ([(p, seen.length)] : List (Program × Nat)).length = 1 from rfl]
        omega
      rw [List.getElem?_eq_none hne] at h
      exact absurd h (by simp)

/-- Index-remap lemma for the first-occurrence table: if the `sameFullProgram`
    lookup in `seen` returns `(q, s)` and every entry of `seen` at index `k`
    (shifted by a constant offset) carries slot number `k + off`, then the
    slot-keyed lookup in the deduplicated table finds exactly `(s, q)`. -/
private theorem swapSeen_find :
    ∀ (seen : List (Program × Nat)) (off : Nat),
      (∀ (k : Nat) (y : Program × Nat), seen[k]? = some y → y.2 = k + off) →
      ∀ (p q : Program) (s : Nat),
        seen.find? (fun pair => sameFullProgram pair.1 p) = some (q, s) →
        (swapSeen seen).find? (fun e => e.1 == s) = some (s, q) := by
  intro seen
  induction seen with
  | nil =>
    intro off hseen p q s h
    exact absurd h (by simp)
  | cons w tail ih =>
    cases w with
    | mk r t =>
      intro off hseen p q s h
      have htail : ∀ k y, tail[k]? = some y → y.2 = k + (off + 1) := by
        intro k y hy
        have := hseen (k + 1) y (by rw [List.getElem?_cons_succ]; exact hy)
        omega
      obtain hres := List.find?_cons_eq_some.mp h
      cases hres with
      | inl hboth =>
        obtain ⟨_, heq⟩ := hboth
        cases heq
        -- goal: (swapSeen ((r, t) :: tail)).find? (fun e => e.1 == t) = some (t, r)
        exact List.find?_cons_of_pos (Nat.beq_refl t)
      | inr hboth =>
        obtain ⟨_, htailf⟩ := hboth
        have ht : t = 0 + off := hseen 0 (r, t) rfl
        rw [Nat.zero_add] at ht
        obtain ⟨i, hi⟩ := (List.mem_iff_getElem?).mp (List.mem_of_find?_eq_some htailf)
        have hs : s = i + (off + 1) := htail i (q, s) hi
        refine (List.find?_cons_of_neg ?_).trans (ih (off + 1) htail p q s htailf)
        intro hp
        have : t = s := Nat.eq_of_beq_eq_true hp
        omega

/-- Swapping the first-occurrence table commutes with appending the new
    program's entry. -/
private theorem swapSeen_snoc (seen : List (Program × Nat)) (p : Program) :
    swapSeen (seen ++ [(p, seen.length)]) = swapSeen seen ++ [(seen.length, p)] := by
  unfold swapSeen
  rw [List.map_append]
  show seen.map swapPair ++ [(p, seen.length)].map swapPair
    = seen.map swapPair ++ [(seen.length, p)]
  rw [List.map_cons, List.map_nil]
  rfl

/-- No entry of a valid first-occurrence table carries the fresh slot
    `seen.length`. -/
private theorem swapSeen_lt_find (seen : List (Program × Nat)) (hseen : SlotInv seen) :
    (swapSeen seen).find? (fun x => x.1 == seen.length) = none := by
  rw [List.find?_eq_none]
  intro e he
  obtain ⟨a, ham, rfl⟩ := List.mem_map.mp he
  obtain ⟨i, hi⟩ := (List.mem_iff_getElem?).mp ham
  obtain ⟨hlt, _⟩ := List.getElem?_eq_some_iff.mp hi
  have h2 : a.2 = i := hseen i a hi
  intro ht
  have h3 : a.2 = seen.length := Nat.eq_of_beq_eq_true ht
  omega

/-- Index-remap lemma for the extended table: after `p` got the fresh slot
    `seen.length`, the slot-keyed lookup returns `(seen.length, p)`. -/
private theorem swapSeen_snoc_find (seen : List (Program × Nat)) (hseen : SlotInv seen)
    (p : Program) :
    (swapSeen (seen ++ [(p, seen.length)])).find? (fun x => x.1 == seen.length) =
      some (seen.length, p) := by
  rw [swapSeen_snoc, List.find?_append, swapSeen_lt_find seen hseen, List.find?_singleton]
  simp

/-- The `uniqueProgs` fold only ever appends at the end: the initial table is
    a prefix of the result. -/
private theorem foldl_dedup_eq_append (xs : List (Program × Nat))
    (acc : List (Nat × Program)) :
    ∃ adds, xs.foldl dedupStep acc = acc ++ adds := by
  induction xs generalizing acc with
  | nil => exact ⟨[], by simp⟩
  | cons x xs ih =>
    have hstep : ∃ adds0, dedupStep acc x = acc ++ adds0 := by
      by_cases hc : acc.any (fun q => q.1 == x.2)
      · exact ⟨[], by unfold dedupStep; rw [if_pos hc, List.append_nil]⟩
      · exact ⟨[(x.2, x.1)], by unfold dedupStep; rw [if_neg hc]⟩
    obtain ⟨adds0, h0⟩ := hstep
    obtain ⟨adds1, h1⟩ := ih (dedupStep acc x)
    exact ⟨adds0 ++ adds1, by rw [List.foldl_cons, h1, h0, List.append_assoc]⟩

/-- Main induction on the program list, generalising the `assignSlots`
    accumulator `seen`: resolving every assigned slot through the deduplicated
    table reproduces the independent per-program evaluation. -/
private theorem go_correct (rows : List DataRow) :
    ∀ (progs : List Program) (seen : List (Program × Nat)), SlotInv seen →
      ((assignSlots.go sameFullProgram seen progs).map
          fun sid =>
            slotLookup
              (((progs.zip (assignSlots.go sameFullProgram seen progs)).foldl
                      dedupStep (swapSeen seen)).map
                  fun x => (x.1, sumEval rows x.2))
              sid) =
        progs.map (fun p => sumEval rows p) := by
  intro progs
  induction progs with
  | nil => intro seen hseen; rfl
  | cons p rest ih =>
    intro seen hseen
    cases h : List.find? (fun pair => sameFullProgram pair.1 p) seen with
    | none =>
      have hcond : (swapSeen seen).any (fun q => q.1 == seen.length) = false := by
        rw [List.any_eq_false]
        intro e he
        obtain ⟨a, ham, rfl⟩ := List.mem_map.mp he
        obtain ⟨i, hi⟩ := (List.mem_iff_getElem?).mp ham
        obtain ⟨hlt, _⟩ := List.getElem?_eq_some_iff.mp hi
        have h2 : a.2 = i := hseen i a hi
        intro ht
        have h3 : a.2 = seen.length := Nat.eq_of_beq_eq_true ht
        omega
      have hstep : dedupStep (swapSeen seen) (p, seen.length) = swapSeen seen
          ++ [(seen.length, p)] := by
        show (if (swapSeen seen).any (fun q => q.1 == seen.length) then _ else _) = _
        rw [hcond]
        simp
      have hfind :
          (((rest.zip (assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest)).foldl
                  dedupStep (swapSeen (seen ++ [(p, seen.length)]))).map
              fun x => (x.1, sumEval rows x.2)).find? (fun q => q.1 == seen.length)
            = some (seen.length, sumEval rows p) := by
        obtain ⟨adds, hadds⟩ := foldl_dedup_eq_append
          (rest.zip (assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest))
          (swapSeen (seen ++ [(p, seen.length)]))
        rw [hadds, List.find?_map]
        have hcomp :
            ((fun (q : Nat × Int) => q.1 == seen.length)
                ∘ fun (x : Nat × Program) => (x.1, sumEval rows x.2))
              = fun x => x.1 == seen.length := rfl
        rw [hcomp, List.find?_append, swapSeen_snoc_find seen hseen p]
        rfl
      have hsum : slotLookup
          (((rest.zip (assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest)).foldl
                  dedupStep (swapSeen (seen ++ [(p, seen.length)]))).map
              fun x => (x.1, sumEval rows x.2))
          seen.length = sumEval rows p := by
        unfold slotLookup
        rw [hfind]
      have hids : assignSlots.go sameFullProgram seen (p :: rest)
          = seen.length
              :: assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest := by
        rw [assignSlots.go.eq_2, h]
      have hseen' : SlotInv (seen ++ [(p, seen.length)]) := slotInv_snoc seen hseen p
      rw [hids]
      rw [show (p :: rest).zip
              (seen.length :: assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest)
              = (p, seen.length)
                  :: rest.zip (assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest)
            from rfl]
      rw [List.foldl_cons, hstep, ← swapSeen_snoc]
      show slotLookup
          (((rest.zip (assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest)).foldl
                  dedupStep (swapSeen (seen ++ [(p, seen.length)]))).map
              fun x => (x.1, sumEval rows x.2))
            seen.length
          :: (assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest).map
            (fun sid =>
              slotLookup
                (((rest.zip (assignSlots.go sameFullProgram (seen ++ [(p, seen.length)]) rest)).foldl
                        dedupStep (swapSeen (seen ++ [(p, seen.length)]))).map
                    fun x => (x.1, sumEval rows x.2))
                  sid)
        = sumEval rows p :: rest.map (fun p => sumEval rows p)
      rw [hsum, ih (seen ++ [(p, seen.length)]) hseen']
    | some w =>
      cases w with
      | mk q s =>
        have hq : q = p := by
          simpa [sameFullProgram] using
            (List.find?_eq_some_iff_getElem.mp h).1
        have hf : (swapSeen seen).find? (fun e => e.1 == s) = some (s, q) :=
          swapSeen_find seen 0 hseen p q s h
        have hstep : dedupStep (swapSeen seen) (p, s) = swapSeen seen := by
          show (if (swapSeen seen).any (fun e => e.1 == s) then _ else _) = _
          refine if_pos ?_
          rw [List.any_eq_true]
          exact ⟨(s, q), List.mem_of_find?_eq_some hf, Nat.beq_refl s⟩
        have hfind :
            (((rest.zip (assignSlots.go sameFullProgram seen rest)).foldl
                    dedupStep (swapSeen seen)).map
                fun x => (x.1, sumEval rows x.2)).find? (fun q => q.1 == s)
              = some (s, sumEval rows q) := by
          obtain ⟨adds, hadds⟩ := foldl_dedup_eq_append
            (rest.zip (assignSlots.go sameFullProgram seen rest)) (swapSeen seen)
          rw [hadds, List.find?_map]
          have hcomp :
              ((fun (q : Nat × Int) => q.1 == s)
                  ∘ fun (x : Nat × Program) => (x.1, sumEval rows x.2))
                = fun x => x.1 == s := rfl
          rw [hcomp, List.find?_append, hf]
          rfl
        have hsum : slotLookup
            (((rest.zip (assignSlots.go sameFullProgram seen rest)).foldl
                    dedupStep (swapSeen seen)).map
                fun x => (x.1, sumEval rows x.2))
            s = sumEval rows p := by
          unfold slotLookup
          rw [hfind, hq]
        have hids : assignSlots.go sameFullProgram seen (p :: rest)
            = s :: assignSlots.go sameFullProgram seen rest := by
          rw [assignSlots.go.eq_2, h]
        rw [hids]
        rw [show (p :: rest).zip (s :: assignSlots.go sameFullProgram seen rest)
                = (p, s) :: rest.zip (assignSlots.go sameFullProgram seen rest)
              from rfl]
        rw [List.foldl_cons, hstep]
        show slotLookup
            (((rest.zip (assignSlots.go sameFullProgram seen rest)).foldl
                    dedupStep (swapSeen seen)).map
                fun x => (x.1, sumEval rows x.2))
              s
            :: (assignSlots.go sameFullProgram seen rest).map
              (fun sid =>
                slotLookup
                  (((rest.zip (assignSlots.go sameFullProgram seen rest)).foldl
                          dedupStep (swapSeen seen)).map
                      fun x => (x.1, sumEval rows x.2))
                    sid)
          = sumEval rows p :: rest.map (fun p => sumEval rows p)
        rw [hsum, ih seen hseen]

theorem sound : TinylambOpt.ResearchGoals.BytecodeSideTableEquiv.Goal := by
  intro progs rows
  have h := go_correct rows progs [] slotInv_nil
  show ((assignSlots.go sameFullProgram [] progs).map
          fun sid =>
            slotLookup
              (((progs.zip (assignSlots.go sameFullProgram [] progs)).foldl
                      dedupStep (swapSeen ([] : List (Program × Nat)))).map
                  fun x => (x.1, sumEval rows x.2))
              sid) =
        progs.map (fun p => sumEval rows p)
  exact h

end TinylambOpt.ResearchProofs.BytecodeSideTableEquiv

-- C++ references to this model or its proof.
-- executor/detail/fused_raw_aggregates.cpp
