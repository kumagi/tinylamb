import TinylambOpt.ResearchGoals.RestartRaces.Recovery

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Proved

- `CommitDrain` — Can a single-pass owner-buffered redo replace two-pass WAL-order
  replay even for interleaved writers? A cheap boundary to test is whether the owner
  of the last eligible image is also the last committer touching this key.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Recovery_CommitDrain

open TinylambOpt.ResearchGoals.RestartRaces.Recovery.CommitDrain

private theorem zipIdx_add {α : Type} (l : List α) (k n : Nat) :
    l.zipIdx (k + n) = (l.zipIdx k).map (fun p => (p.1, p.2 + n)) := by
  induction l generalizing k with
  | nil => simp [List.zipIdx_nil]
  | cons a l ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      rw [show k + n + 1 = (k + 1) + n by omega]
      rw [ih (k + 1)]
      simp [Nat.add_assoc, Nat.add_comm]

private def lastIdx {α : Type} (xs : List α) (P : α → Bool) : Option Nat :=
  match xs with
  | [] => none
  | x :: xt =>
      match lastIdx xt P with
      | some i => some (i + 1)
      | none => if P x then some 0 else none

private theorem lastIdx_cons {α : Type} (x : α) (xs : List α) (P : α → Bool) :
    lastIdx (x :: xs) P = match lastIdx xs P with
      | some i => some (i + 1)
      | none => if P x then some 0 else none := rfl

private theorem filter_last_eq_lastIdx {α : Type} (xs : List α) (P : α → Bool) :
    ((xs.zipIdx.filter (fun y : α × Nat => P y.1)).getLast?).map Prod.snd = lastIdx xs P := by
  induction xs with
  | nil => simp [lastIdx, List.zipIdx_nil]
  | cons x xs ih =>
      rw [List.zipIdx_cons]
      rw [show xs.zipIdx (0 + 1) = (xs.zipIdx 0).map (fun p : α × Nat => (p.1, p.2 + 1)) by
        simpa using zipIdx_add xs 0 1]
      simp only [List.filter_cons]
      rw [List.filter_map]
      have hcomp : ((fun y : α × Nat => P y.1) ∘
          (fun p : α × Nat => (p.1, p.2 + 1))) = (fun y : α × Nat => P y.1) := by rfl
      rw [hcomp]
      by_cases hx : P x = true
      · simp only [hx, ↓reduceIte]
        rw [List.getLast?_cons]
        rw [List.getLast?_map]
        cases hG : (xs.zipIdx.filter (fun y : α × Nat => P y.1)).getLast? with
        | none =>
            have hli : lastIdx xs P = none := by rw [hG] at ih; exact ih.symm
            simpa [lastIdx, hli, hx]
        | some p =>
            have hli : lastIdx xs P = some p.2 := by rw [hG] at ih; exact ih.symm
            simpa [lastIdx, hli, hx]
      · have hxProp : P x = false := by cases hpx : P x <;> simp_all
        simp [hxProp]
        unfold lastIdx
        cases hG : List.find? (fun y : α × Nat => P y.1) xs.zipIdx.reverse with
        | none =>
            have hG'' : (xs.zipIdx.filter (fun y : α × Nat => P y.1)).getLast? = none := by
              rw [List.getLast?_filter]; exact hG
            have hli : lastIdx xs P = none := by rw [hG''] at ih; exact ih.symm
            simpa [hG, hli, lastIdx, hx]
        | some p =>
            have hG'' : (xs.zipIdx.filter (fun y : α × Nat => P y.1)).getLast? = some p := by
              rw [List.getLast?_filter]; exact hG
            have hli : lastIdx xs P = some p.2 := by rw [hG''] at ih; exact ih.symm
            simpa [hG, hli, lastIdx, hx]

private theorem lastIdx_none_iff {α : Type} (xs : List α) (P : α → Bool) :
    lastIdx xs P = none ↔ ∀ x ∈ xs, P x = false := by
  induction xs with
  | nil => simp [lastIdx]
  | cons x xs ih =>
      rw [lastIdx_cons]
      cases hli : lastIdx xs P with
      | none =>
          constructor
          · intro hc
            have hxfalse : P x = false := by
              cases hpx : P x <;> simp_all
            have hrest : ∀ y ∈ xs, P y = false := ih.mp hli
            intro y hy
            simp only [List.mem_cons] at hy
            rcases hy with rfl | hy
            · exact hxfalse
            · exact hrest y hy
          · intro hall
            have hxfalse : P x = false := hall x (by simp)
            simp [hli, hxfalse]
      | some k =>
          constructor
          · intro hc
            simp [hli] at hc
          · intro hall
            have hrest : ∀ y ∈ xs, P y = false := by
              intro y hy
              exact hall y (by simp [hy])
            have hn : lastIdx xs P = none := ih.mpr hrest
            rw [hn] at hli
            contradiction

private theorem lastIdx_some_mem {α : Type} (xs : List α) (P : α → Bool) {i : Nat}
    (hi : i < xs.length) (h : lastIdx xs P = some i) : P (xs[i]'hi) = true := by
  induction xs generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      simp only [List.length_cons] at hi
      rw [lastIdx_cons] at h
      by_cases hn : lastIdx xs P = none
      · rw [hn] at h
        have hx : P x = true := by
          by_cases hp : P x = true
          · exact hp
          · have : P x = false := by cases P x <;> simp_all
            simp [this] at h
        have hi0 : i = 0 := (by simpa using h.symm : P x = true ∧ i = 0).2
        subst i
        simpa using hx
      · cases hl : lastIdx xs P with
        | none => contradiction
        | some k =>
            rw [hl] at h
            have hik : i = k + 1 := by simpa using h.symm
            subst i
            have hklen : k < xs.length := by omega
            have hmem := ih hklen hl
            simpa using hmem

private theorem lastIdx_sub {α : Type} (xs : List α) (P Q : α → Bool)
    (hle : ∀ x, Q x = true → P x = true) {i : Nat}
    (hi : i < xs.length) (hP : lastIdx xs P = some i) (hQ : Q (xs[i]'hi) = true) :
    lastIdx xs Q = some i := by
  induction xs generalizing i with
  | nil => simp at hi
  | cons x xs ih =>
      simp only [List.length_cons] at hi
      rw [lastIdx_cons] at hP ⊢
      by_cases hn : lastIdx xs P = none
      · rw [hn] at hP
        have hx : P x = true := by
          by_cases hp : P x = true
          · exact hp
          · have : P x = false := by cases P x <;> simp_all
            simp [this] at hP
        have hi0 : i = 0 := (by simpa using hP.symm : P x = true ∧ i = 0).2
        subst i
        have hQx : Q x = true := by simpa using hQ
        have hnQ : lastIdx xs Q = none := by
          rw [lastIdx_none_iff]
          intro y hy
          by_cases hyQ : Q y = true
          · have hPyfalse : P y = false := ((lastIdx_none_iff xs P).mp hn y hy)
            have hPytrue : P y = true := hle y hyQ
            rw [hPytrue] at hPyfalse
            contradiction
          · cases hqy : Q y with
            | true => exact (hyQ hqy).elim
            | false => rfl
        simp [hnQ, hQx]
      · cases hl : lastIdx xs P with
        | none => contradiction
        | some k =>
            rw [hl] at hP
            have hik : i = k + 1 := by simpa using hP.symm
            subst i
            have hklen : k < xs.length := by omega
            have hQk : Q (xs[k]'hklen) = true := by simpa using hQ
            have hres := ih hklen hl hQk
            simp [hres]

private def drainRec (xs : List Image) (commits : List Nat) (k : Nat) : Option Nat :=
  match commits with
  | [] => none
  | o :: os =>
      match drainRec xs os k with
      | some i => some i
      | none =>
          match lastIdx xs (fun x : Image => x.key == k && x.owner == o) with
          | some i => some i
          | none => none

private def lastOwn (xs : List Image) (commits : List Nat) (k : Nat) : Option Nat :=
  (commits.filter (fun o => xs.any (fun x : Image => x.key == k && x.owner == o))).getLast?

private theorem lastIdx_some_length {α : Type} (xs : List α) (P : α → Bool) {i : Nat}
    (h : lastIdx xs P = some i) : i < xs.length := by
  induction xs generalizing i with
  | nil => simp [lastIdx] at h
  | cons x xs ih =>
      rw [lastIdx_cons] at h
      by_cases hn : lastIdx xs P = none
      · rw [hn] at h
        have hi : i = 0 := (by simpa using h.symm : P x = true ∧ i = 0).2
        subst i
        simp
      · cases hl : lastIdx xs P with
        | none => contradiction
        | some k =>
            rw [hl] at h
            have hi : i = k + 1 := by simpa using h.symm
            subst i
            have hk := ih hl
            simp only [List.length_cons]
            omega

private theorem any_eq_lastIdx_some (xs : List Image) (k o : Nat) :
    xs.any (fun x : Image => x.key == k && x.owner == o) = true ↔
      (lastIdx xs (fun x : Image => x.key == k && x.owner == o)).isSome = true := by
  rw [List.any_eq_true]
  constructor
  · rintro ⟨x, hx, hp⟩
    cases hl : lastIdx xs (fun x : Image => x.key == k && x.owner == o) with
    | none =>
        have hpfalse := (lastIdx_none_iff xs (fun x : Image => x.key == k && x.owner == o)).mp hl x hx
        rw [hp] at hpfalse
        contradiction
    | some i => simp [hl]
  · intro h
    cases hl : lastIdx xs (fun x : Image => x.key == k && x.owner == o) with
    | none => simp [hl] at h
    | some i =>
        have hi := lastIdx_some_length xs (fun x : Image => x.key == k && x.owner == o) hl
        refine ⟨xs[i], List.getElem_mem hi, ?_⟩
        exact lastIdx_some_mem xs (fun x : Image => x.key == k && x.owner == o) hi hl

private theorem drain_eq_drainRec (xs : List Image) (commits : List Nat) (k : Nat) :
    drain xs commits k = drainRec xs commits k := by
  induction commits with
  | nil => simp [drain, drainRec]
  | cons o os ih =>
      unfold drain
      rw [List.flatMap_cons, List.getLast?_append]
      unfold drainRec
      cases hrest : (os.flatMap (fun o => xs.zipIdx.filter (fun (x, _) => x.key == k && x.owner == o))).getLast? with
      | none =>
          have ihNone : drainRec xs os k = none := by
            rw [← ih]
            simpa [drain, hrest]
          rw [ihNone]
          simp only [Option.none_or, Option.map_none]
          have hfl := filter_last_eq_lastIdx xs (fun x : Image => x.key == k && x.owner == o)
          cases hli : lastIdx xs (fun x : Image => x.key == k && x.owner == o) <;> simpa [hli] using hfl
      | some p =>
          have ihSome : drainRec xs os k = some p.2 := by
            rw [← ih]
            simpa [drain, hrest]
          rw [ihSome]
          simp [hrest]

private theorem drainRec_length (xs : List Image) (commits : List Nat) (k : Nat) {i : Nat}
    (h : drainRec xs commits k = some i) : i < xs.length := by
  induction commits with
  | nil => simp [drainRec] at h
  | cons o os ih =>
      unfold drainRec at h
      cases hd : drainRec xs os k with
      | some j =>
          rw [hd] at h
          have hji : j = i := by simpa using h
          subst i
          exact ih hd
      | none =>
          rw [hd] at h
          cases hl : lastIdx xs (fun x : Image => x.key == k && x.owner == o) with
          | none => simp [hl] at h
          | some j =>
              rw [hl] at h
              have hji : j = i := by simpa using h
              subst i
              exact lastIdx_some_length xs (fun x : Image => x.key == k && x.owner == o) hl

private theorem drain_owner_eq_lastOwn (xs : List Image) (commits : List Nat) (k : Nat) :
    (drainRec xs commits k).bind (fun i => xs[i]?.map Image.owner) = lastOwn xs commits k := by
  induction commits with
  | nil => rfl
  | cons o os ih =>
      cases hd : drainRec xs os k with
      | none =>
          unfold drainRec
          rw [hd]
          have hlo : lastOwn xs os k = none := by simpa [hd] using ih.symm
          cases hlo' : lastIdx xs (fun x : Image => x.key == k && x.owner == o) with
          | none =>
              have hQfalse : xs.any (fun x : Image => x.key == k && x.owner == o) = false := by
                cases hany : xs.any (fun x : Image => x.key == k && x.owner == o) with
                | true =>
                    have hs : (lastIdx xs (fun x : Image => x.key == k && x.owner == o)).isSome = true :=
                      (any_eq_lastIdx_some xs k o).mp hany
                    simp [hlo'] at hs
                | false => rfl
              have hfindnone : List.find? (fun o => xs.any (fun x : Image => x.key == k && x.owner == o)) os.reverse = none := by
                rw [← List.getLast?_filter]
                simpa [lastOwn] using hlo
              simp [lastOwn, List.filter_cons, hQfalse, hfindnone]
          | some i =>
              have hi := lastIdx_some_length xs (fun x : Image => x.key == k && x.owner == o) hlo'
              have hmem := lastIdx_some_mem xs (fun x : Image => x.key == k && x.owner == o) hi hlo'
              have hiidx : xs[i]? = some xs[i] := List.getElem?_eq_getElem hi
              have hQtrue : xs.any (fun x : Image => x.key == k && x.owner == o) = true :=
                (any_eq_lastIdx_some xs k o).mpr (by simp [hlo'])
              have hmem' : xs[i].key = k ∧ xs[i].owner = o := by simpa using hmem
              have hfilnone : (List.filter (fun o => xs.any (fun x : Image => x.key == k && x.owner == o)) os).getLast? = none := by
                simpa [lastOwn] using hlo
              simp [lastOwn, List.filter_cons, hQtrue, hd, List.getLast?_cons, hfilnone, hiidx, hmem'.2]
      | some i =>
          unfold drainRec
          rw [hd]
          have hi := drainRec_length xs os k hd
          have hiidx : xs[i]? = some xs[i] := List.getElem?_eq_getElem hi
          have hown : (xs[i]?.map Image.owner) = some xs[i].owner := by simp [hiidx]
          have hlone : lastOwn xs os k = some xs[i].owner := by
            simpa [hd, hiidx] using ih.symm
          cases hQ : xs.any (fun x : Image => x.key == k && x.owner == o) with
          | true =>
              have hlast : (List.filter (fun o => xs.any (fun x : Image => x.key == k && x.owner == o)) os).getLast? =
                  some xs[i].owner := by simpa [lastOwn] using hlone
              simp [lastOwn, List.filter_cons, hQ, List.getLast?_cons, hlast, hd, hiidx]
          | false =>
              have hfil : (List.filter (fun o => xs.any (fun x : Image => x.key == k && x.owner == o)) os).getLast? = some xs[i].owner := by
                simpa [lastOwn] using hlone
              have hfind : List.find? (fun o => xs.any (fun x : Image => x.key == k && x.owner == o)) os.reverse = some xs[i].owner := by
                rw [← List.getLast?_filter]
                exact hfil
              simp [hd, hiidx, lastOwn, List.filter_cons, hQ, hfind]

private theorem lastIdx_of_subset_none {α : Type} (xs : List α) (P Q : α → Bool)
    (hle : ∀ x, Q x = true → P x = true) (h : lastIdx xs P = none) :
    lastIdx xs Q = none := by
  rw [lastIdx_none_iff] at h ⊢
  intro x hx
  cases hQx : Q x with
  | true =>
      have hpfalse := h x hx
      have hptrue := hle x hQx
      rw [hptrue] at hpfalse
      contradiction
  | false => rfl

private theorem none_elim_all (xs : List Image) (commits : List Nat) (k : Nat)
    (h : lastIdx xs (fun x : Image => x.key == k && commits.contains x.owner) = none) :
    lastOwn xs commits k = none ∧ drainRec xs commits k = none := by
  induction commits with
  | nil => simp [lastOwn, drainRec]
  | cons o os ih =>
      let Q : Nat → Bool := fun o => xs.any (fun x : Image => x.key == k && x.owner == o)
      let Po : Image → Bool := fun x => x.key == k && x.owner == o
      have hle : ∀ x, Po x = true → (fun x : Image => x.key == k && (o :: os).contains x.owner) x = true := by
        intro x hx
        have hx' : x.key = k ∧ x.owner = o := by simpa [Po] using hx
        simp [hx'.1, hx'.2]
      have hnoneo : lastIdx xs Po = none := by
        simpa [Po] using lastIdx_of_subset_none xs (fun x : Image => x.key == k && (o :: os).contains x.owner) Po hle h
      have hQo : Q o = false := by
        have hnot : ¬(lastIdx xs Po).isSome = true := by simp [hnoneo]
        cases hany : Q o with
        | true =>
            have hs : (lastIdx xs Po).isSome = true := by
              simpa [Po, Q] using (any_eq_lastIdx_some xs k o).mp hany
            simp [hnot] at hs
        | false => rfl
      have hnoneos : lastIdx xs (fun x : Image => x.key == k && os.contains x.owner) = none := by
        apply lastIdx_of_subset_none xs (fun x : Image => x.key == k && (o :: os).contains x.owner)
          (fun x : Image => x.key == k && os.contains x.owner)
        · intro x hx
          have hx' : x.key = k ∧ x.owner ∈ os := by simpa using hx
          simp [hx'.1, hx'.2]
        · exact h
      have ihos := ih hnoneos
      constructor
      · have hfilnone : (List.filter Q os).getLast? = none := by
          simpa [lastOwn, Q] using ihos.1
        have hnoex : ¬∃ x, x ∈ xs ∧ x.key = k ∧ x.owner = o := by
          simpa [Q, List.any_eq_true] using hQo
        have hrestprop : ∀ x, x ∈ os → ∀ x1 ∈ xs, x1.key = k → x1.owner ≠ x := by
          simpa [lastOwn, Q] using ihos.1
        simpa [lastOwn, List.filter_cons, Q, hQo, hfilnone, hnoex, hrestprop]
      · unfold drainRec
        rw [ihos.2, hnoneo]

private theorem drainRec_index_last_owner (xs : List Image) (commits : List Nat) (k : Nat)
    {d : Nat} (hd : drainRec xs commits k = some d) (hi : d < xs.length) :
    lastIdx xs (fun x : Image => x.key == k && x.owner == xs[d].owner) = some d := by
  induction commits with
  | nil => simp [drainRec] at hd
  | cons o os ih =>
      cases hdos : drainRec xs os k with
      | some j =>
          unfold drainRec at hd
          rw [hdos] at hd
          have hjd : j = d := by simpa using hd
          subst d
          exact ih hdos
      | none =>
          unfold drainRec at hd
          rw [hdos] at hd
          cases hlo : lastIdx xs (fun x : Image => x.key == k && x.owner == o) with
          | none => simp [hlo] at hd
          | some j =>
              rw [hlo] at hd
              have hjd : j = d := by simpa using hd
              subst d
              have hmem := lastIdx_some_mem xs (fun x : Image => x.key == k && x.owner == o) hi hlo
              have howner : xs[j].owner = o := by
                have hm : xs[j].key = k ∧ xs[j].owner = o := by simpa using hmem
                exact hm.2
              simpa [howner] using hlo

theorem sound : Goal := by
  intro xs commits k hnod
  let P : Image → Bool := fun x => x.key == k && commits.contains x.owner
  let Q : Nat → Bool := fun o => xs.any (fun x : Image => x.key == k && x.owner == o)
  change ((serial xs commits k).bind (fun i => xs[i]?.map Image.owner) = lastOwn xs commits k) ↔
    (∀ base payload, read base payload (serial xs commits k) = read base payload (drain xs commits k))
  have hser : serial xs commits k = lastIdx xs P := by
    simpa [P, serial] using filter_last_eq_lastIdx xs P
  have hdr : drain xs commits k = drainRec xs commits k := drain_eq_drainRec xs commits k
  have hown := drain_owner_eq_lastOwn xs commits k
  have hreadIff :
      (∀ base payload, read base payload (serial xs commits k) = read base payload (drain xs commits k)) ↔
        serial xs commits k = drain xs commits k := by
    constructor
    · intro H
      by_cases hEq : serial xs commits k = drain xs commits k
      · exact hEq
      · exfalso
        have hbad := H 0 (fun n => n + 1)
        cases hS' : serial xs commits k with
        | none =>
            cases hD' : drain xs commits k with
            | none => exact hEq (by simp [hS', hD'])
            | some j =>
                simp [TinylambOpt.ResearchGoals.RestartRaces.Recovery.CommitDrain.read, hS', hD'] at hbad
        | some i =>
            cases hD' : drain xs commits k with
            | none =>
                simp [TinylambOpt.ResearchGoals.RestartRaces.Recovery.CommitDrain.read, hS', hD'] at hbad
            | some j =>
                simp [TinylambOpt.ResearchGoals.RestartRaces.Recovery.CommitDrain.read, hS', hD'] at hbad
                have hij : i = j := by omega
                exact hEq (by simp [hS', hD', hij])
    · intro h base payload
      simp [h]
  have hmain : ((serial xs commits k).bind (fun i => xs[i]?.map Image.owner) = lastOwn xs commits k) ↔
      serial xs commits k = drain xs commits k := by
    constructor
    · intro hL
      cases hS : lastIdx xs P with
      | none =>
          have hnone := none_elim_all xs commits k (by simpa [P] using hS)
          have hserNone : serial xs commits k = none := by simp [hser, hS]
          have hdrNone : drain xs commits k = none := by simp [hdr, hnone.2]
          exact hserNone.trans hdrNone.symm
      | some i =>
          have hi := lastIdx_some_length xs P hS
          have hmem := lastIdx_some_mem xs P hi hS
          have hiidx : xs[i]? = some xs[i] := List.getElem?_eq_getElem hi
          have hPi' : xs[i].key = k ∧ commits.contains xs[i].owner := by simpa [P] using hmem
          cases hD : drainRec xs commits k with
          | none =>
              have hlastNone : lastOwn xs commits k = none := by simpa [hD] using hown.symm
              have hQtrue : xs.any (fun x : Image => x.key == k && x.owner == xs[i].owner) = true := by
                rw [List.any_eq_true]
                exact ⟨xs[i], List.getElem_mem hi, by simp [hPi'.1]⟩
              have hmemOwner : xs[i].owner ∈ commits := by simpa using hPi'.2
              have hQs : ∃ x, x ∈ commits ∧ Q x = true :=
                ⟨xs[i].owner, hmemOwner, by simpa [Q] using hQtrue⟩
              have hfind : (List.find? Q commits.reverse).isSome = true := by
                rw [List.find?_isSome]
                simpa using hQs
              have hlastSome : (lastOwn xs commits k).isSome = true := by
                unfold lastOwn
                change (List.filter Q commits).getLast?.isSome = true
                rw [List.getLast?_filter, List.find?_isSome]
                simpa [Q] using hQs
              rw [hlastNone] at hlastSome
              simp at hlastSome
          | some d =>
              have hdlen := drainRec_length xs commits k hD
              have hdidx : xs[d]? = some xs[d] := List.getElem?_eq_getElem hdlen
              have hlast : lastOwn xs commits k = some xs[d].owner := by
                simpa [hD, hdidx] using hown.symm
              have hownerIeq : xs[i].owner = xs[d].owner := by
                have : some xs[i].owner = lastOwn xs commits k := by
                  simpa [hser, hS, hiidx] using hL
                simpa [hlast] using this
              have hleSub : ∀ x : Image, (x.key == k && x.owner == xs[i].owner) = true → P x = true := by
                intro x hx
                have hx' : x.key = k ∧ x.owner = xs[i].owner := by simpa using hx
                have hcProp : x.owner ∈ commits := by simpa [hx'.2] using hPi'.2
                simp [P, hx'.1, hcProp]
              have hli : lastIdx xs (fun x : Image => x.key == k && x.owner == xs[i].owner) = some i :=
                lastIdx_sub xs P (fun x : Image => x.key == k && x.owner == xs[i].owner) hleSub hi hS (by simp [hPi'.1])
              have hld : lastIdx xs (fun x : Image => x.key == k && x.owner == xs[i].owner) = some d := by
                have htmp := drainRec_index_last_owner xs commits k hD hdlen
                simpa [hownerIeq.symm] using htmp
              have hid : i = d := by
                have : some i = some d := hli.symm.trans hld
                simpa using this
              simp [hser, hdr, hS, hD, hid]
    · intro hEq
      cases hS : lastIdx xs P with
      | none =>
          have hnone := none_elim_all xs commits k (by simpa [P] using hS)
          have hserNone : serial xs commits k = none := by simp [hser, hS]
          have hdrNone : drain xs commits k = none := by
            rw [hdr, hnone.2]
          have hEq' : drain xs commits k = none := by simpa [hserNone] using hEq.symm
          simp [hserNone, hEq', hnone.1]
      | some i =>
          have hi := lastIdx_some_length xs P hS
          have hiidx : xs[i]? = some xs[i] := List.getElem?_eq_getElem hi
          have hserSome : serial xs commits k = some i := by simp [hser, hS]
          have hD : drainRec xs commits k = some i := by
            rw [← hdr]
            simpa [hserSome] using hEq.symm
          have hlone : lastOwn xs commits k = some xs[i].owner := by
            simpa [hD, hiidx] using hown.symm
          simp [hserSome, hiidx, hlone]

  constructor
  · intro hL
    exact hreadIff.mpr (hmain.mp hL)
  · intro hR
    exact hmain.mpr (hreadIff.mp hR)

end TinylambOpt.ResearchProofs.RestartRaces.Recovery_CommitDrain
