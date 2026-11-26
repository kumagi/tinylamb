import TinylambOpt.ResearchGoals.WalCheckpointTlcsBoundary

/-!
Proved

- `Goal` — WAL checkpoint vs persistent TLCS boundary
  (`recovery/checkpoint_manager.cpp`, `recovery/recovery_manager.cpp`): under
  writes-before-commit and MVCC write-intent commit serialization, TLCS snapshot at
  `cpLsn` plus WAL replay from `minActiveWriteLsn` agrees with full WAL replay from
  LSN 0.

The reference model is defined in `TinylambOpt.ResearchGoals.WalCheckpointTlcsBoundary`.
-/

namespace TinylambOpt.ResearchProofs.WalCheckpointTlcsBoundary
open TinylambOpt.ResearchGoals.WalCheckpointTlcsBoundary
open List

/-! ## Option-level "first success" combinator and the last matching write -/

private def orOpt (a b : Option Int) : Option Int :=
  match a with
  | some v => some v
  | none => b

private theorem orOpt_some (v : Int) (b : Option Int) : orOpt (some v) b = some v := rfl

private theorem orOpt_none (b : Option Int) : orOpt none b = b := rfl

private theorem orOpt_none_right (a : Option Int) : orOpt a none = a := by
  rcases a with _ | v
  · rfl
  · rfl

private def stepW (txs : List Nat) (k : Nat) (rec : Record) (acc : Option Int) : Option Int :=
  match rec with
  | .write tx k' v => if txs.contains tx && k' == k then some v else acc
  | _ => acc

private def lastWriteFrom (txs : List Nat) (rs : List Record) (k : Nat) (acc : Option Int) :
    Option Int :=
  rs.foldl (fun a rec => stepW txs k rec a) acc

private def lastWrite (txs : List Nat) (rs : List Record) (k : Nat) : Option Int :=
  lastWriteFrom txs rs k none

private theorem lwF_cons (txs : List Nat) (k : Nat) (r : Record) (rs : List Record)
    (acc : Option Int) :
    lastWriteFrom txs (r :: rs) k acc = lastWriteFrom txs rs k (stepW txs k r acc) := rfl

private theorem stepW_commit (txs : List Nat) (k c : Nat) (acc : Option Int) :
    stepW txs k (Record.commit c) acc = acc := rfl

private theorem stepW_abort (txs : List Nat) (k c : Nat) (acc : Option Int) :
    stepW txs k (Record.abort c) acc = acc := rfl

private theorem lastWriteFrom_getD (txs : List Nat) :
    ∀ (rs : List Record) (k : Nat) (acc : Option Int),
      lastWriteFrom txs rs k acc = orOpt (lastWrite txs rs k) acc := by
  intro rs
  induction rs with
  | nil => intro k acc; rfl
  | cons r rs ih =>
      intro k acc
      have hL : lastWrite txs (r :: rs) k = lastWriteFrom txs rs k (stepW txs k r none) := rfl
      rw [lwF_cons, ih k (stepW txs k r acc), hL, ih k (stepW txs k r none)]
      rcases lastWrite txs rs k with _ | w
      · match r with
        | .commit _ => simp only [stepW_commit, orOpt_none]
        | .abort _ => simp only [stepW_abort, orOpt_none]
        | .write tx k' v =>
            by_cases hc : (txs.contains tx && k' == k)
            · simp only [stepW, if_pos hc, orOpt_none, orOpt_some]
            · simp only [stepW, if_neg hc, orOpt_none]
      · simp only [orOpt_some]

/-! ## `NoWrite` / `LastW` predicates and the exact fold specification -/

private def NoWrite (txs : List Nat) (rs : List Record) (k : Nat) : Prop :=
  ∀ (i tx : Nat) (v : Int), i < rs.length → rs[i]? = some (.write tx k v) → ¬ txs.contains tx

private def LastW (txs : List Nat) (rs : List Record) (k : Nat) (i : Nat) (v : Int) : Prop :=
  ∃ (tx : Nat), i < rs.length ∧ rs[i]? = some (.write tx k v) ∧ txs.contains tx ∧
    ∀ (j tx2 : Nat) (v2 : Int), j < rs.length → rs[j]? = some (.write tx2 k v2) →
      txs.contains tx2 → j ≤ i

private theorem noWrite_commit (txs : List Nat) (k c : Nat) (rs : List Record) :
    NoWrite txs (Record.commit c :: rs) k ↔ NoWrite txs rs k := by
  constructor
  · intro h i tx v hi he
    exact h (i + 1) tx v (Nat.succ_lt_succ hi) (by rw [List.getElem?_cons_succ]; exact he)
  · intro h i tx v hi he
    rcases i with _|j
    · rw [List.getElem?_cons_zero] at he
      exact absurd (Option.some.inj he) (by simp)
    · rw [List.getElem?_cons_succ] at he
      exact h j tx v (Nat.lt_of_succ_lt_succ hi) he

private theorem noWrite_abort (txs : List Nat) (k c : Nat) (rs : List Record) :
    NoWrite txs (Record.abort c :: rs) k ↔ NoWrite txs rs k := by
  constructor
  · intro h i tx v hi he
    exact h (i + 1) tx v (Nat.succ_lt_succ hi) (by rw [List.getElem?_cons_succ]; exact he)
  · intro h i tx v hi he
    rcases i with _|j
    · rw [List.getElem?_cons_zero] at he
      exact absurd (Option.some.inj he) (by simp)
    · rw [List.getElem?_cons_succ] at he
      exact h j tx v (Nat.lt_of_succ_lt_succ hi) he

private theorem and_eq_true {b1 b2 : Bool} : (b1 && b2) = true ↔ b1 = true ∧ b2 = true := by
  cases b1 <;> cases b2 <;> simp

private theorem noWrite_write (txs : List Nat) (k tx k' : Nat) (v : Int) (rs : List Record) :
    NoWrite txs (Record.write tx k' v :: rs) k ↔
      (¬ (txs.contains tx && k' == k)) ∧ NoWrite txs rs k := by
  constructor
  · intro h
    refine ⟨?_, fun i tx1 v1 hi he => h (i + 1) tx1 v1 (Nat.succ_lt_succ hi)
        (by rw [List.getElem?_cons_succ]; exact he)⟩
    · intro hc
      obtain ⟨hb1, hb2⟩ := and_eq_true.mp hc
      have hkk : k' = k := by simpa using hb2
      exact absurd hb1 (h 0 tx v (Nat.zero_lt_succ _) (by rw [List.getElem?_cons_zero, hkk]))
  · rintro ⟨hnc, hrs⟩ i tx1 v1 hi he
    rcases i with _|j
    · rw [List.getElem?_cons_zero] at he
      obtain ⟨rfl, hkk, rfl⟩ := Option.some.inj he
      intro hc1
      exact absurd (and_eq_true.mpr ⟨hc1, by simpa using hkk⟩) hnc
    · rw [List.getElem?_cons_succ] at he
      exact hrs j tx1 v1 (Nat.lt_of_succ_lt_succ hi) he

private theorem lastW_commit (txs : List Nat) (k c : Nat) (rs : List Record) (i : Nat) (v : Int) :
    LastW txs (Record.commit c :: rs) k i v ↔
      ∃ (i' : Nat), i = i' + 1 ∧ LastW txs rs k i' v := by
  constructor
  · intro h
    obtain ⟨tx, hlen, hget, hmem, hmax⟩ := h
    rcases i with _|j
    · rw [List.getElem?_cons_zero] at hget
      exact absurd (Option.some.inj hget) (by simp)
    · rw [List.getElem?_cons_succ] at hget
      exact ⟨j, rfl, tx, Nat.lt_of_succ_lt_succ hlen, hget, hmem,
        fun l tx2 v2 hl hl' hm =>
          Nat.succ_le_succ_iff.mp (hmax (l + 1) tx2 v2 (Nat.succ_lt_succ hl)
            (by rw [List.getElem?_cons_succ]; exact hl') hm)⟩
  · rintro ⟨j, rfl, tx, hlen, hget, hmem, hmax⟩
    refine ⟨tx, Nat.succ_lt_succ hlen, ?_, hmem, ?_⟩
    · rw [List.getElem?_cons_succ]; exact hget
    · intro l tx2 v2 hl hl' hm
      rcases l with _|l'
      · exact Nat.zero_le _
      · rw [List.getElem?_cons_succ] at hl'
        exact Nat.succ_le_succ (hmax l' tx2 v2 (Nat.lt_of_succ_lt_succ hl) hl' hm)

private theorem lastW_abort (txs : List Nat) (k c : Nat) (rs : List Record) (i : Nat) (v : Int) :
    LastW txs (Record.abort c :: rs) k i v ↔
      ∃ (i' : Nat), i = i' + 1 ∧ LastW txs rs k i' v := by
  constructor
  · intro h
    obtain ⟨tx, hlen, hget, hmem, hmax⟩ := h
    rcases i with _|j
    · rw [List.getElem?_cons_zero] at hget
      exact absurd (Option.some.inj hget) (by simp)
    · rw [List.getElem?_cons_succ] at hget
      exact ⟨j, rfl, tx, Nat.lt_of_succ_lt_succ hlen, hget, hmem,
        fun l tx2 v2 hl hl' hm =>
          Nat.succ_le_succ_iff.mp (hmax (l + 1) tx2 v2 (Nat.succ_lt_succ hl)
            (by rw [List.getElem?_cons_succ]; exact hl') hm)⟩
  · rintro ⟨j, rfl, tx, hlen, hget, hmem, hmax⟩
    refine ⟨tx, Nat.succ_lt_succ hlen, ?_, hmem, ?_⟩
    · rw [List.getElem?_cons_succ]; exact hget
    · intro l tx2 v2 hl hl' hm
      rcases l with _|l'
      · exact Nat.zero_le _
      · rw [List.getElem?_cons_succ] at hl'
        exact Nat.succ_le_succ (hmax l' tx2 v2 (Nat.lt_of_succ_lt_succ hl) hl' hm)

private theorem lastW_write (txs : List Nat) (k tx k' : Nat) (v : Int) (rs : List Record)
    (i : Nat) (vv : Int) :
    LastW txs (Record.write tx k' v :: rs) k i vv ↔
      ((txs.contains tx && k' == k) ∧ i = 0 ∧ vv = v ∧ NoWrite txs rs k) ∨
      (∃ (i' : Nat), i = i' + 1 ∧ LastW txs rs k i' vv) := by
  constructor
  · intro h
    obtain ⟨tx1, hlen, hget, hmem, hmax⟩ := h
    rcases i with _|j
    · rw [List.getElem?_cons_zero] at hget
      obtain ⟨rfl, hkk, rfl⟩ := Option.some.inj hget
      left
      refine ⟨and_eq_true.mpr ⟨hmem, by simpa using hkk⟩, rfl, rfl,
        fun jj tx2 v2 hjj hjj2 h2 =>
          absurd (hmax (jj + 1) tx2 v2 (Nat.succ_lt_succ hjj)
            (by rw [List.getElem?_cons_succ]; exact hjj2) h2) (Nat.not_succ_le_zero jj)⟩
    · rw [List.getElem?_cons_succ] at hget
      right
      exact ⟨j, rfl, tx1, Nat.lt_of_succ_lt_succ hlen, hget, hmem,
        fun l tx2 v2 hl hl' hm =>
          Nat.succ_le_succ_iff.mp (hmax (l + 1) tx2 v2 (Nat.succ_lt_succ hl)
            (by rw [List.getElem?_cons_succ]; exact hl') hm)⟩
  · rintro (⟨hc, rfl, rfl, hn⟩ | ⟨j, rfl, tx1, hlen, hget, hmem, hmax⟩)
    · refine ⟨tx, Nat.zero_lt_succ _, ?_, (and_eq_true.mp hc).1, ?_⟩
      · have hkk : k' = k := by simpa using (and_eq_true.mp hc).2
        rw [List.getElem?_cons_zero, hkk]
      · intro l tx2 v2 hl hl' hm
        rcases l with _|j'
        · exact Nat.zero_le _
        · rw [List.getElem?_cons_succ] at hl'
          exact absurd hm (hn j' tx2 v2 (Nat.lt_of_succ_lt_succ hl) hl')
    · refine ⟨tx1, Nat.succ_lt_succ hlen, ?_, hmem, ?_⟩
      · rw [List.getElem?_cons_succ]; exact hget
      · intro l tx2 v2 hl hl' hm
        rcases l with _|l'
        · exact Nat.zero_le _
        · rw [List.getElem?_cons_succ] at hl'
          exact Nat.succ_le_succ (hmax l' tx2 v2 (Nat.lt_of_succ_lt_succ hl) hl' hm)

private def HasW (txs : List Nat) (rs : List Record) (k : Nat) (v : Int) : Prop :=
  ∃ (i : Nat), LastW txs rs k i v

private theorem hasW_commit (txs : List Nat) (k c : Nat) (rs : List Record) (v : Int) :
    HasW txs (Record.commit c :: rs) k v ↔ HasW txs rs k v := by
  constructor
  · rintro ⟨i, h⟩
    obtain ⟨i', _, hw⟩ := (lastW_commit txs k c rs i v).1 h
    exact ⟨i', hw⟩
  · rintro ⟨i, hw⟩
    exact ⟨i + 1, (lastW_commit txs k c rs (i + 1) v).2 ⟨i, rfl, hw⟩⟩

private theorem hasW_abort (txs : List Nat) (k c : Nat) (rs : List Record) (v : Int) :
    HasW txs (Record.abort c :: rs) k v ↔ HasW txs rs k v := by
  constructor
  · rintro ⟨i, h⟩
    obtain ⟨i', _, hw⟩ := (lastW_abort txs k c rs i v).1 h
    exact ⟨i', hw⟩
  · rintro ⟨i, hw⟩
    exact ⟨i + 1, (lastW_abort txs k c rs (i + 1) v).2 ⟨i, rfl, hw⟩⟩

private theorem hasW_write (txs : List Nat) (k tx k' : Nat) (v0 v : Int) (rs : List Record) :
    HasW txs (Record.write tx k' v0 :: rs) k v ↔
      ((txs.contains tx && k' == k) ∧ v = v0 ∧ NoWrite txs rs k) ∨ HasW txs rs k v := by
  constructor
  · rintro ⟨i, h⟩
    rcases (lastW_write txs k tx k' v0 rs i v).1 h with ⟨hmv, _, hvv, hn⟩ | ⟨i', _, hw⟩
    · exact Or.inl ⟨hmv, hvv, hn⟩
    · exact Or.inr ⟨i', hw⟩
  · rintro (⟨hmv, hvv, hn⟩ | ⟨i, hw⟩)
    · exact ⟨0, (lastW_write txs k tx k' v0 rs 0 v).2 (Or.inl ⟨hmv, rfl, hvv, hn⟩)⟩
    · exact ⟨i + 1, (lastW_write txs k tx k' v0 rs (i + 1) v).2 (Or.inr ⟨i, rfl, hw⟩)⟩

private theorem lastWriteFrom_spec (txs : List Nat) :
    ∀ (rs : List Record) (k : Nat) (acc : Option Int),
      (lastWriteFrom txs rs k acc = none ↔ acc = none ∧ NoWrite txs rs k) ∧
      (∀ (v : Int), lastWriteFrom txs rs k acc = some v ↔
        HasW txs rs k v ∨ (acc = some v ∧ NoWrite txs rs k)) := by
  intro rs
  induction rs with
  | nil =>
      intro k acc
      have hnil : lastWriteFrom txs [] k acc = acc := rfl
      have hnw : NoWrite txs [] k := fun i tx v hi _ => absurd hi (Nat.not_lt_zero i)
      rw [hnil]
      refine ⟨⟨fun h => ⟨h, hnw⟩, fun ⟨h, _⟩ => h⟩, ?_⟩
      intro v
      constructor
      · intro hv
        exact Or.inr ⟨hv, hnw⟩
      · rintro (⟨i, tx, hi, _, _, _⟩ | ⟨hacc, _⟩)
        · exact absurd hi (Nat.not_lt_zero i)
        · exact hacc
  | cons r rs ih =>
      intro k acc
      have hA : ∀ (a : Option Int),
          lastWriteFrom txs rs k a = none ↔ a = none ∧ NoWrite txs rs k := fun a => (ih k a).1
      have hB : ∀ (a : Option Int) (w : Int), lastWriteFrom txs rs k a = some w ↔
          HasW txs rs k w ∨ (a = some w ∧ NoWrite txs rs k) := fun a w => (ih k a).2 w
      constructor
      · -- no-write
        match r with
        | .commit c => rw [lwF_cons, stepW_commit, hA, noWrite_commit]
        | .abort c => rw [lwF_cons, stepW_abort, hA, noWrite_abort]
        | .write tx k' v =>
            rw [lwF_cons]
            by_cases hc : (txs.contains tx && k' == k)
            · rw [show stepW txs k (Record.write tx k' v) acc = some v from if_pos hc, hA]
              constructor
              · rintro ⟨hs, _⟩
                exact absurd hs (by simp)
              · rintro ⟨_, hnw⟩
                exact absurd hc ((noWrite_write txs k tx k' v rs).1 hnw).1
            · rw [show stepW txs k (Record.write tx k' v) acc = acc from if_neg hc, hA,
                noWrite_write]
              exact ⟨fun ⟨h1, h2⟩ => ⟨h1, ⟨hc, h2⟩⟩, fun ⟨h1, ⟨_, h2⟩⟩ => ⟨h1, h2⟩⟩
      · -- some-value
        intro v
        match r with
        | .commit c => rw [lwF_cons, stepW_commit, hB, hasW_commit, noWrite_commit]
        | .abort c => rw [lwF_cons, stepW_abort, hB, hasW_abort, noWrite_abort]
        | .write tx k' vw =>
            rw [lwF_cons]
            by_cases hc : (txs.contains tx && k' == k)
            · rw [show stepW txs k (Record.write tx k' vw) acc = some vw from if_pos hc, hB]
              constructor
              · rintro (hw | ⟨he, hn⟩)
                · exact Or.inl ((hasW_write txs k tx k' vw v rs).2 (Or.inr hw))
                · exact Or.inl ((hasW_write txs k tx k' vw v rs).2
                    (Or.inl ⟨hc, (Option.some.inj he).symm, hn⟩))
              · rintro (hw | ⟨_, hnw⟩)
                · rcases (hasW_write txs k tx k' vw v rs).1 hw with ⟨_, hvv, hn⟩ | hw2
                  · exact Or.inr ⟨by rw [hvv], hn⟩
                  · exact Or.inl hw2
                · exact absurd hc ((noWrite_write txs k tx k' vw rs).1 hnw).1
            · rw [show stepW txs k (Record.write tx k' vw) acc = acc from if_neg hc, hB,
                noWrite_write]
              constructor
              · rintro (hw | ⟨ha, hn⟩)
                · exact Or.inl ((hasW_write txs k tx k' vw v rs).2 (Or.inr hw))
                · exact Or.inr ⟨ha, ⟨hc, hn⟩⟩
              · rintro (hw | ⟨ha, hnc, hn⟩)
                · rcases (hasW_write txs k tx k' vw v rs).1 hw with ⟨hmv, _, _⟩ | hw2
                  · exact absurd hmv hc
                  · exact Or.inl hw2
                · exact Or.inr ⟨ha, hn⟩

private def stepS (txs : List Nat) (rec : Record) (st : State) : State :=
  match rec with
  | .write tx k v => if txs.contains tx then applyWrite st k v else st
  | _ => st

private theorem stepS_write_match (txs : List Nat) (tx k : Nat) (v : Int) (st : State)
    (h : txs.contains tx) : stepS txs (Record.write tx k v) st = applyWrite st k v := by
  show (if txs.contains tx then applyWrite st k v else st) = applyWrite st k v
  rw [if_pos h]

private theorem stepS_write_skip (txs : List Nat) (tx k : Nat) (v : Int) (st : State)
    (h : ¬ txs.contains tx) : stepS txs (Record.write tx k v) st = st := by
  show (if txs.contains tx then applyWrite st k v else st) = st
  rw [if_neg h]

private theorem foldWritesFor_pointwise (txs : List Nat) :
    ∀ (rs : List Record) (st0 : State) (k : Nat),
      foldWritesFor txs rs st0 k = orOpt (lastWrite txs rs k) (st0 k) := by
  intro rs
  induction rs with
  | nil => intro st0 k; rfl
  | cons r rs ih =>
      intro st0 k
      have hcons : foldWritesFor txs (r :: rs) st0 = foldWritesFor txs rs (stepS txs r st0) := by
        cases r <;> rfl
      have hLW : lastWrite txs (r :: rs) k = lastWriteFrom txs rs k (stepW txs k r none) := rfl
      rw [hcons, ih, hLW, lastWriteFrom_getD]
      rcases lastWrite txs rs k with _ | w
      · match r with
        | .commit _ => simp only [stepS, stepW_commit, orOpt_none]
        | .abort _ => simp only [stepS, stepW_abort, orOpt_none]
        | .write tx k' vw =>
            by_cases hA : txs.contains tx
            · by_cases hB : k' = k
              · have hw : stepW txs k (Record.write tx k' vw) none = some vw :=
                  if_pos (and_eq_true.mpr ⟨hA, by simpa using hB⟩)
                have hs : stepS txs (Record.write tx k' vw) st0 = applyWrite st0 k' vw :=
                  stepS_write_match txs tx k' vw st0 hA
                rw [hw, hs, orOpt_none, orOpt_none, orOpt_some]
                simp only [applyWrite]
                rw [if_pos (by simpa using hB.symm)]
              · have hw : stepW txs k (Record.write tx k' vw) none = none :=
                  if_neg (fun hab => hB (by simpa using (and_eq_true.mp hab).2))
                have hs : stepS txs (Record.write tx k' vw) st0 = applyWrite st0 k' vw :=
                  stepS_write_match txs tx k' vw st0 hA
                rw [hw, hs]
                show orOpt none (applyWrite st0 k' vw k) = orOpt (orOpt none none) (st0 k)
                simp only [applyWrite, orOpt_none]
                rw [if_neg (by simpa using Ne.symm hB)]
            · have hw : stepW txs k (Record.write tx k' vw) none = none :=
                if_neg (fun hab => hA (and_eq_true.mp hab).1)
              have hs : stepS txs (Record.write tx k' vw) st0 = st0 :=
                stepS_write_skip txs tx k' vw st0 hA
              rw [hw, hs]
              simp only [orOpt_none]
      · simp only [orOpt_some]

/-! ## Splitting machinery: append, congruence, and the `startLsn` bounds -/

private abbrev cpCL (log : List Record) (cpLsn : Nat) := commitsIn (log.take cpLsn)

private abbrev allCL (log : List Record) := commitsIn log

private abbrev postCL (log : List Record) (cpLsn : Nat) :=
  (allCL log).filter (fun tx => !(cpCL log cpLsn).contains tx)

private abbrev sL (log : List Record) (cpLsn : Nat) : Nat :=
  minActiveWriteLsn (postCL log cpLsn) cpLsn log

private theorem orOpt_assoc (a b c : Option Int) : orOpt (orOpt a b) c = orOpt a (orOpt b c) := by
  rcases a with _ | v <;> rfl

private theorem foldWritesFor_eq_pointwise (txs : List Nat) (rs : List Record) (st0 : State) :
    foldWritesFor txs rs st0 = fun k => orOpt (lastWrite txs rs k) (st0 k) :=
  funext fun k => foldWritesFor_pointwise txs rs st0 k

private theorem lastWrite_append (txs : List Nat) (u v : List Record) (k : Nat) :
    lastWrite txs (u ++ v) k = orOpt (lastWrite txs v k) (lastWrite txs u k) := by
  have h1 : lastWrite txs (u ++ v) k = lastWriteFrom txs v k (lastWrite txs u k) := by
    show List.foldl (fun a rec => stepW txs k rec a) none (u ++ v) = _
    rw [List.foldl_append]
    rfl
  rw [h1, lastWriteFrom_getD]

private theorem lastWrite_congr (txs txs' : List Nat) (rs : List Record) (k : Nat)
    (h : ∀ tx, (∃ (i : Nat) (v : Int), rs[i]? = some (Record.write tx k v)) →
      txs.contains tx = txs'.contains tx) :
    lastWrite txs rs k = lastWrite txs' rs k := by
  cases h1 : lastWrite txs rs k with
  | none =>
      have hnw : NoWrite txs rs k := ((lastWriteFrom_spec txs rs k none).1).mp h1 |>.2
      have hnw' : NoWrite txs' rs k := fun i tx vv hi hget hc =>
        hnw i tx vv hi hget (by rw [h tx ⟨i, vv, hget⟩]; exact hc)
      have h2 : lastWrite txs' rs k = none := ((lastWriteFrom_spec txs' rs k none).1).mpr ⟨rfl, hnw'⟩
      exact h2.symm
  | some v =>
      have hsw : lastWriteFrom txs rs k none = some v := h1
      have hs := ((lastWriteFrom_spec txs rs k none).2 v).mp hsw
      have hs' : HasW txs rs k v :=
        hs.resolve_right (fun ⟨hne, _⟩ => by cases hne)
      rcases hs' with ⟨i, tx, hlen, hget, hmem, hmax⟩
      refine (((lastWriteFrom_spec txs' rs k none).2 v).mpr (Or.inl
        ⟨i, tx, hlen, hget, by rw [← h tx ⟨i, v, hget⟩]; exact hmem, ?_⟩)).symm
      intro j tx2 v2 hj hj' hm2
      exact hmax j tx2 v2 hj hj' (by rw [h tx2 ⟨j, v2, hj'⟩]; exact hm2)

private theorem mem_commitsIn_iff (rs : List Record) (tx : Nat) :
    tx ∈ commitsIn rs ↔ ∃ (i : Nat), i < rs.length ∧ rs[i]? = some (Record.commit tx) := by
  induction rs with
  | nil =>
      constructor
      · intro h; exact absurd h (by simp [commitsIn])
      · rintro ⟨i, hi, _⟩; exact absurd hi (Nat.not_lt_zero _)
  | cons r rs ih =>
      constructor
      · intro h
        match r with
        | .commit c =>
            rw [show commitsIn (Record.commit c :: rs) = c :: commitsIn rs from rfl] at h
            rcases List.mem_cons.mp h with heq | h2
            · exact ⟨0, Nat.zero_lt_succ _, by rw [List.getElem?_cons_zero]; exact heq ▸ rfl⟩
            · obtain ⟨i, hi, hg⟩ := ih.1 h2
              exact ⟨i + 1, Nat.succ_lt_succ hi, by rw [List.getElem?_cons_succ]; exact hg⟩
        | .abort c =>
            rw [show commitsIn (Record.abort c :: rs) = commitsIn rs from rfl] at h
            obtain ⟨i, hi, hg⟩ := ih.1 h
            exact ⟨i + 1, Nat.succ_lt_succ hi, by rw [List.getElem?_cons_succ]; exact hg⟩
        | .write t kk vv =>
            rw [show commitsIn (Record.write t kk vv :: rs) = commitsIn rs from rfl] at h
            obtain ⟨i, hi, hg⟩ := ih.1 h
            exact ⟨i + 1, Nat.succ_lt_succ hi, by rw [List.getElem?_cons_succ]; exact hg⟩
      · rintro ⟨i, hi, hg⟩
        match r with
        | .commit c =>
            rw [show commitsIn (Record.commit c :: rs) = c :: commitsIn rs from rfl]
            rcases i with _|j
            · rw [List.getElem?_cons_zero] at hg
              have hc : c = tx := Record.commit.inj (Option.some.inj hg)
              exact hc ▸ List.mem_cons.mpr (Or.inl rfl)
            · rw [List.getElem?_cons_succ] at hg
              exact List.mem_cons.mpr (Or.inr (ih.2 ⟨j, Nat.lt_of_succ_lt_succ hi, hg⟩))
        | .abort c =>
            rw [show commitsIn (Record.abort c :: rs) = commitsIn rs from rfl]
            rcases i with _|j
            · rw [List.getElem?_cons_zero] at hg; exact absurd hg (by simp)
            · rw [List.getElem?_cons_succ] at hg
              exact ih.2 ⟨j, Nat.lt_of_succ_lt_succ hi, hg⟩
        | .write t kk vv =>
            rw [show commitsIn (Record.write t kk vv :: rs) = commitsIn rs from rfl]
            rcases i with _|j
            · rw [List.getElem?_cons_zero] at hg; exact absurd hg (by simp)
            · rw [List.getElem?_cons_succ] at hg
              exact ih.2 ⟨j, Nat.lt_of_succ_lt_succ hi, hg⟩

private theorem commitsIn_append (u v : List Record) :
    commitsIn (u ++ v) = commitsIn u ++ commitsIn v := by
  induction u with
  | nil => rfl
  | cons r rs ih =>
      match r with
      | .commit c =>
          exact show commitsIn (Record.commit c :: rs ++ v) =
              commitsIn (Record.commit c :: rs) ++ commitsIn v
            from congrArg (List.cons c) ih
      | .abort c =>
          exact show commitsIn (Record.abort c :: rs ++ v) =
              commitsIn (Record.abort c :: rs) ++ commitsIn v
            from ih
      | .write t kk vv =>
          exact show commitsIn (Record.write t kk vv :: rs ++ v) =
              commitsIn (Record.write t kk vv :: rs) ++ commitsIn v
            from ih

private theorem mem_commitsIn_take (log : List Record) (cpLsn tx : Nat)
    (h : tx ∈ commitsIn (log.take cpLsn)) : tx ∈ commitsIn log := by
  have he : log = log.take cpLsn ++ log.drop cpLsn := (List.take_append_drop cpLsn log).symm
  rw [he, commitsIn_append, List.mem_append]
  exact Or.inl h

private theorem contains_of_mem (l : List Nat) (a : Nat) (h : a ∈ l) : l.contains a = true := by
  rw [List.contains_eq_mem]
  exact decide_eq_true h

private def minStepL (postC : List Nat) (start : Nat) (p : Record × Nat) : Nat :=
  match p with
  | (rec, idx) =>
      match rec with
      | .write tx _ _ => if postC.contains tx then Nat.min start idx else start
      | _ => start

private def minAcc (postC : List Nat) (l : List (Record × Nat)) (start : Nat) : Nat :=
  l.foldl (fun acc (rec, idx) =>
    match rec with
    | .write tx _ _ => if postC.contains tx then Nat.min acc idx else acc
    | _ => acc) start

private theorem minActive_eq (postC : List Nat) (cpLsn : Nat) (log : List Record) :
    minActiveWriteLsn postC cpLsn log = minAcc postC log.zipIdx cpLsn := rfl

private theorem minAcc_nil (postC : List Nat) (start : Nat) : minAcc postC [] start = start := rfl

private theorem minAcc_cons (postC : List Nat) (p : Record × Nat) (l : List (Record × Nat))
    (start : Nat) :
    minAcc postC (p :: l) start = minAcc postC l (minStepL postC start p) := by
  unfold minAcc minStepL
  rw [List.foldl_cons]

private theorem minStepL_write (postC : List Nat) (tx kk : Nat) (vv : Int) (idx start : Nat) :
    minStepL postC start (Record.write tx kk vv, idx) =
      if postC.contains tx then Nat.min start idx else start := rfl

private theorem minStepL_commit (postC : List Nat) (c idx start : Nat) :
    minStepL postC start (Record.commit c, idx) = start := rfl

private theorem minStepL_abort (postC : List Nat) (c idx start : Nat) :
    minStepL postC start (Record.abort c, idx) = start := rfl

private theorem minStepL_le (postC : List Nat) (p : Record × Nat) (start : Nat) :
    minStepL postC start p ≤ start := by
  match p with
  | (.write tx kk vv, idx) =>
      by_cases hc : postC.contains tx
      · have h1 : minStepL postC start (Record.write tx kk vv, idx) = Nat.min start idx := by
          rw [minStepL_write, if_pos hc]
        rw [h1]; exact Nat.min_le_left _ _
      · have h1 : minStepL postC start (Record.write tx kk vv, idx) = start := by
          rw [minStepL_write, if_neg hc]
        rw [h1]; exact Nat.le_refl _
  | (.commit c, idx) =>
      rw [minStepL_commit postC c idx start]; exact Nat.le_refl _
  | (.abort c, idx) =>
      rw [minStepL_abort postC c idx start]; exact Nat.le_refl _

private theorem minAcc_le (postC : List Nat) :
    ∀ (l : List (Record × Nat)) (start : Nat), minAcc postC l start ≤ start := by
  intro l
  induction l with
  | nil => intro start; rw [minAcc_nil]; exact Nat.le_refl _
  | cons p l ih =>
      intro start
      rw [minAcc_cons]
      exact Nat.le_trans (ih (minStepL postC start p)) (minStepL_le postC p start)

private theorem minAcc_widx (postC : List Nat) :
    ∀ (rs : List Record) (off start : Nat), ∀ (i tx kk : Nat) (vv : Int),
      i < rs.length → rs[i]? = some (Record.write tx kk vv) → postC.contains tx = true →
        minAcc postC (List.zipIdx rs off) start ≤ i + off := by
  intro rs
  induction rs with
  | nil => intro off start i tx kk vv hi _ _; exact absurd hi (Nat.not_lt_zero _)
  | cons r rs ih =>
      intro off start i tx kk vv hi hget hc
      rw [List.zipIdx_cons, minAcc_cons]
      rcases i with _|j
      · have hr : r = Record.write tx kk vv := Option.some.inj
          (by rw [List.getElem?_cons_zero] at hget; exact hget)
        subst hr
        have h1 : minStepL postC start (Record.write tx kk vv, off) = Nat.min start off := by
          rw [minStepL_write, if_pos hc]
        rw [h1]
        refine Nat.le_trans (minAcc_le postC (List.zipIdx rs (off + 1)) (Nat.min start off)) ?_
        exact Nat.le_trans (Nat.min_le_right start off) (Nat.le_of_eq (Nat.zero_add off).symm)
      · rw [List.getElem?_cons_succ] at hget
        have hjl : j < rs.length := Nat.lt_of_succ_lt_succ hi
        have hstep := ih (off + 1) (minStepL postC start (r, off)) j tx kk vv hjl hget hc
        have hconv : j + (off + 1) ≤ j + 1 + off := by omega
        exact Nat.le_trans hstep hconv

private theorem sL_le (log : List Record) (cpLsn : Nat) : sL log cpLsn ≤ cpLsn := by
  show minActiveWriteLsn (postCL log cpLsn) cpLsn log ≤ cpLsn
  rw [minActive_eq]
  exact minAcc_le (postCL log cpLsn) log.zipIdx cpLsn

private theorem sL_le_write (log : List Record) (cpLsn i tx kk : Nat) (vv : Int)
    (hi : i < log.length) (hget : log[i]? = some (Record.write tx kk vv))
    (hc : (postCL log cpLsn).contains tx = true) : sL log cpLsn ≤ i := by
  show minActiveWriteLsn (postCL log cpLsn) cpLsn log ≤ i
  rw [minActive_eq]
  have hle := minAcc_widx (postCL log cpLsn) log 0 cpLsn i tx kk vv hi hget hc
  exact Nat.le_trans hle (Nat.le_of_eq (Nat.add_zero i))

private theorem getElem?_some_lt {α : Type} (rs : List α) (i : Nat) (a : α)
    (h : rs[i]? = some a) : i < rs.length := by
  cases rs with
  | nil => exact absurd h (by simp)
  | cons a' as =>
      cases i with
      | zero => exact Nat.zero_lt_succ _
      | succ j =>
          rw [List.getElem?_cons_succ] at h
          exact Nat.succ_lt_succ (getElem?_some_lt as j a h)

private theorem noWrite_sL_TL (log : List Record) (cpLsn k i tx : Nat) (v : Int)
    (hi : i < (log.take (sL log cpLsn)).length)
    (hget : (log.take (sL log cpLsn))[i]? = some (Record.write tx k v)) :
    ¬ (postCL log cpLsn).contains tx := by
  intro hc
  have hi' : i < sL log cpLsn := Nat.lt_of_lt_of_le hi (List.length_take_le _ _)
  have hlog : log[i]? = some (Record.write tx k v) := by
    rw [List.getElem?_take] at hget
    rw [if_pos hi'] at hget
    exact hget
  have hleni : i < log.length := getElem?_some_lt log i (Record.write tx k v) hlog
  exact absurd (sL_le_write log cpLsn i tx k v hleni hlog hc) (Nat.not_le_of_gt hi')

private theorem hsplit (log : List Record) (cpLsn : Nat) (hlen : cpLsn ≤ log.length) :
    log.drop (sL log cpLsn) =
      (log.take cpLsn).drop (sL log cpLsn) ++ log.drop cpLsn := by
  have he : log = log.take cpLsn ++ log.drop cpLsn := (List.take_append_drop cpLsn log).symm
  have h0 : log.drop (sL log cpLsn) =
      (log.take cpLsn ++ log.drop cpLsn).drop (sL log cpLsn) :=
    congrArg (fun l => l.drop (sL log cpLsn)) he
  rw [h0, List.drop_append]
  rw [List.length_take_of_le hlen, Nat.sub_eq_zero_of_le (sL_le log cpLsn), List.drop_zero]

private theorem hsplit2 (log : List Record) (cpLsn : Nat) :
    log.take cpLsn =
      log.take (sL log cpLsn) ++ (log.take cpLsn).drop (sL log cpLsn) := by
  have htake : log.take (sL log cpLsn) = (log.take cpLsn).take (sL log cpLsn) := by
    rw [List.take_take, Nat.min_eq_left (sL_le log cpLsn)]
  rw [htake, List.take_append_drop]

private theorem aget (log : List Record) (cpLsn j : Nat) (hlen : cpLsn ≤ log.length)
    (hj : j < ((log.take cpLsn).drop (sL log cpLsn)).length) (tx2 : Nat) (k : Nat) (v2 : Int)
    (hget : ((log.take cpLsn).drop (sL log cpLsn))[j]? = some (Record.write tx2 k v2)) :
    log[sL log cpLsn + j]? = some (Record.write tx2 k v2) := by
  have hlenA : ((log.take cpLsn).drop (sL log cpLsn)).length = cpLsn - sL log cpLsn := by
    rw [List.length_drop, List.length_take_of_le hlen]
  have hsum : sL log cpLsn + ((log.take cpLsn).drop (sL log cpLsn)).length = cpLsn := by
    rw [hlenA]; exact Nat.add_sub_of_le (sL_le log cpLsn)
  have hbj : sL log cpLsn + j < cpLsn :=
    Nat.lt_of_lt_of_le (Nat.add_lt_add_left hj _) (Nat.le_of_eq hsum)
  rw [List.getElem?_drop] at hget
  rw [List.getElem?_take, if_pos hbj] at hget
  exact hget

/-! ## Pointwise forms of `recover` and `refState` -/

private theorem recover_or (log : List Record) (cpLsn : Nat) (k : Nat) :
    recover log cpLsn k =
      orOpt (lastWrite (postCL log cpLsn) (log.drop (sL log cpLsn)) k)
        (lastWrite (cpCL log cpLsn) (log.take cpLsn) k) := by
  unfold recover
  simp only [foldWritesFor_eq_pointwise, orOpt_none_right]

private theorem ref_or (log : List Record) (k : Nat) :
    refState log k = lastWrite (allCL log) log k := by
  unfold refState
  simp only [foldWritesFor_eq_pointwise, orOpt_none_right]

private theorem recover_split (log : List Record) (cpLsn : Nat) (hlen : cpLsn ≤ log.length)
    (k : Nat) :
    recover log cpLsn k =
      orOpt (orOpt (lastWrite (postCL log cpLsn) (log.drop cpLsn) k)
        (lastWrite (postCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn)) k))
        (orOpt (lastWrite (cpCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn)) k)
          (lastWrite (cpCL log cpLsn) (log.take (sL log cpLsn)) k)) := by
  have h1 : lastWrite (postCL log cpLsn) (log.drop (sL log cpLsn)) k =
      orOpt (lastWrite (postCL log cpLsn) (log.drop cpLsn) k)
        (lastWrite (postCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn)) k) :=
    Eq.trans (congrArg (fun l => lastWrite (postCL log cpLsn) l k) (hsplit log cpLsn hlen))
      (lastWrite_append (postCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn))
        (log.drop cpLsn) k)
  have h2 : lastWrite (cpCL log cpLsn) (log.take cpLsn) k =
      orOpt (lastWrite (cpCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn)) k)
        (lastWrite (cpCL log cpLsn) (log.take (sL log cpLsn)) k) :=
    Eq.trans (congrArg (fun l => lastWrite (cpCL log cpLsn) l k) (hsplit2 log cpLsn))
      (lastWrite_append (cpCL log cpLsn) (log.take (sL log cpLsn))
        ((log.take cpLsn).drop (sL log cpLsn)) k)
  rw [recover_or, h1, h2]

private theorem ref_split (log : List Record) (cpLsn : Nat) (k : Nat) :
    refState log k =
      orOpt (lastWrite (allCL log) (log.drop cpLsn) k)
        (orOpt (lastWrite (allCL log) ((log.take cpLsn).drop (sL log cpLsn)) k)
          (lastWrite (allCL log) (log.take (sL log cpLsn)) k)) := by
  have he : log = log.take cpLsn ++ log.drop cpLsn := (List.take_append_drop cpLsn log).symm
  have h1 : lastWrite (allCL log) log k =
      orOpt (lastWrite (allCL log) (log.drop cpLsn) k)
        (lastWrite (allCL log) (log.take cpLsn) k) :=
    Eq.trans (congrArg (fun l => lastWrite (allCL log) l k) he)
      (lastWrite_append (allCL log) (log.take cpLsn) (log.drop cpLsn) k)
  have h2 : lastWrite (allCL log) (log.take cpLsn) k =
      orOpt (lastWrite (allCL log) ((log.take cpLsn).drop (sL log cpLsn)) k)
        (lastWrite (allCL log) (log.take (sL log cpLsn)) k) :=
    Eq.trans (congrArg (fun l => lastWrite (allCL log) l k) (hsplit2 log cpLsn))
      (lastWrite_append (allCL log) (log.take (sL log cpLsn))
        ((log.take cpLsn).drop (sL log cpLsn)) k)
  rw [ref_or, h1, h2]

/-! ## Transaction-set congruences on each WAL region -/

private theorem lw_drop_cp_congr (log : List Record) (cpLsn : Nat) (hlen : cpLsn ≤ log.length)
    (hW : WritesBeforeCommit log) (k : Nat) :
    lastWrite (postCL log cpLsn) (log.drop cpLsn) k = lastWrite (allCL log) (log.drop cpLsn) k := by
  refine lastWrite_congr (postCL log cpLsn) (allCL log) (log.drop cpLsn) k ?_
  intro tx htx
  rcases htx with ⟨i, v, hget⟩
  have hcp : (cpCL log cpLsn).contains tx = false := by
    cases hc : (cpCL log cpLsn).contains tx with
    | false => rfl
    | true =>
        exfalso
        have hlog : log[cpLsn + i]? = some (Record.write tx k v) := by
          rw [← List.getElem?_drop]
          exact hget
        obtain ⟨i', hi', hcommit⟩ := ((mem_commitsIn_iff (log.take cpLsn) tx)).mp
          (of_decide_eq_true hc)
        have hi't : i' < cpLsn := Nat.lt_of_lt_of_le hi' (List.length_take_le cpLsn log)
        rw [List.getElem?_take] at hcommit
        rw [if_pos hi't] at hcommit
        have hj : i' < cpLsn + i := Nat.lt_of_lt_of_le hi't (Nat.le_add_right _ _)
        exact hW i' (cpLsn + i) tx k v hj hcommit hlog
  cases hpost : (postCL log cpLsn).contains tx with
  | false =>
      cases hall : (allCL log).contains tx with
      | false => rfl
      | true =>
          exfalso
          have hmem : tx ∈ postCL log cpLsn :=
            List.mem_filter.mpr ⟨of_decide_eq_true hall, by rw [hcp]; decide⟩
          have ht2 : (postCL log cpLsn).contains tx = true :=
            contains_of_mem _ _ hmem
          exact absurd ht2 (by rw [hpost]; decide)
  | true =>
      have hmem : tx ∈ postCL log cpLsn := of_decide_eq_true hpost
      symm
      rw [List.contains_eq_mem]
      exact decide_eq_true ((List.mem_filter).mp hmem).1

private theorem cpC_allC_congr (log : List Record) (cpLsn k : Nat) (rs : List Record)
    (hnw : NoWrite (postCL log cpLsn) rs k) :
    lastWrite (cpCL log cpLsn) rs k = lastWrite (allCL log) rs k := by
  refine lastWrite_congr (cpCL log cpLsn) (allCL log) rs k ?_
  intro tx htx
  rcases htx with ⟨i, v, hget⟩
  cases hcp : (cpCL log cpLsn).contains tx with
  | false =>
      cases hall : (allCL log).contains tx with
      | false => rfl
      | true =>
          exfalso
          have hmem : tx ∈ postCL log cpLsn :=
            List.mem_filter.mpr ⟨of_decide_eq_true hall, by rw [hcp]; decide⟩
          have ht2 : (postCL log cpLsn).contains tx = true :=
            contains_of_mem _ _ hmem
          exact hnw i tx v (getElem?_some_lt rs i (Record.write tx k v) hget) hget ht2
  | true =>
      have hall : (allCL log).contains tx = true :=
        contains_of_mem _ _
          (mem_commitsIn_take log cpLsn tx (of_decide_eq_true hcp))
      exact hall.symm

private theorem noWrite_postCL_TL (log : List Record) (cpLsn k : Nat) :
    NoWrite (postCL log cpLsn) (log.take (sL log cpLsn)) k := by
  intro i tx v hi hget
  exact noWrite_sL_TL log cpLsn k i tx v hi hget

/-! ## Main assembly -/

private theorem sound_main (log : List Record) (cpLsn : Nat) (hlen : cpLsn ≤ log.length)
    (hW : WritesBeforeCommit log) (hS : SameKeyIntentOrder log cpLsn) (k : Nat) :
    recover log cpLsn k = refState log k := by
  unfold SameKeyIntentOrder at hS
  rw [recover_split log cpLsn hlen k, ref_split log cpLsn k]
  rw [lw_drop_cp_congr log cpLsn hlen hW k]
  cases hX : lastWrite (allCL log) (log.drop cpLsn) k with
  | some v =>
      simp only [orOpt_some]
  | none =>
      rw [orOpt_none]
      cases hQ : lastWrite (postCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn)) k with
      | none =>
          have hnwA : NoWrite (postCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn)) k :=
            ((lastWriteFrom_spec (postCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn)) k
              none).1).mp hQ |>.2
          rw [orOpt_none]
          rw [cpC_allC_congr log cpLsn k _ hnwA,
            cpC_allC_congr log cpLsn k _ (noWrite_postCL_TL log cpLsn k)]
          rw [orOpt_none]
      | some w =>
          have hs := ((lastWriteFrom_spec (postCL log cpLsn)
            ((log.take cpLsn).drop (sL log cpLsn)) k none).2 w).mp hQ
          have hs' : HasW (postCL log cpLsn) ((log.take cpLsn).drop (sL log cpLsn)) k w :=
            hs.resolve_right (fun ⟨hne, _⟩ => by cases hne)
          rcases hs' with ⟨i, tx, hlenA, hgetA, hmemA, hmaxA⟩
          have hmemA' : tx ∈ postCL log cpLsn := of_decide_eq_true hmemA
          have hmem' : (allCL log).contains tx = true :=
            contains_of_mem _ _ ((List.mem_filter).mp hmemA').1
          have hmax' : ∀ (j tx2 : Nat) (v2 : Int),
              j < ((log.take cpLsn).drop (sL log cpLsn)).length →
              ((log.take cpLsn).drop (sL log cpLsn))[j]? = some (Record.write tx2 k v2) →
              (allCL log).contains tx2 = true → j ≤ i := by
            intro j tx2 v2 hj hj' hm2
            by_cases hij : i < j
            · exfalso
              have hli : log[sL log cpLsn + i]? = some (Record.write tx k w) :=
                aget log cpLsn i hlen hlenA tx k w hgetA
              have hlj : log[sL log cpLsn + j]? = some (Record.write tx2 k v2) :=
                aget log cpLsn j hlen hj tx2 k v2 hj'
              cases hcp2 : (cpCL log cpLsn).contains tx2 with
              | false =>
                  have hp : tx2 ∈ postCL log cpLsn :=
                    List.mem_filter.mpr ⟨of_decide_eq_true hm2, by rw [hcp2]; decide⟩
                  exact absurd hij
                    (Nat.not_lt_of_le (hmaxA j tx2 v2 hj hj' (contains_of_mem _ _ hp)))
              | true =>
                  have hpost : tx ∈ postCL log cpLsn := of_decide_eq_true hmemA
                  exact hS (sL log cpLsn + i) (sL log cpLsn + j) tx2 tx k v2 w
                    (Nat.add_lt_add_left hij _) hli hlj hpost (of_decide_eq_true hcp2)
            · exact Nat.le_of_not_lt hij
          have hY1 : lastWrite (allCL log) ((log.take cpLsn).drop (sL log cpLsn)) k = some w :=
            Iff.mpr
              ((lastWriteFrom_spec (allCL log) ((log.take cpLsn).drop (sL log cpLsn)) k none).2 w)
              (Or.inl ⟨i, tx, hlenA, hgetA, hmem', hmax'⟩)
          rw [hY1]
          simp only [orOpt_some, orOpt_none]

theorem sound : TinylambOpt.ResearchGoals.WalCheckpointTlcsBoundary.Goal :=
  fun log cpLsn k hlen hW hS => sound_main log cpLsn hlen hW hS k

end TinylambOpt.ResearchProofs.WalCheckpointTlcsBoundary
