import TinylambOpt.ProposedGoals.GroupingSetsReuse

namespace TinylambOpt.ProposedProofs.GroupingSetsReuse

open TinylambOpt.ProposedGoals.GroupingSetsReuse

private def fineKey (r : Row) : Key := masked true true r

private def step (acc : List (Key × State)) (r : Row) : List (Key × State) :=
  match acc.find? (fun p => p.1 == fineKey r) with
  | none => acc ++ [(fineKey r, addRow empty r)]
  | some (_, s) => acc.map (fun p => if p.1 == fineKey r then (fineKey r, addRow s r) else p)

private def accFold (f : Key → Key) (k : Key) (acc : List (Key × State)) : State :=
  acc.foldl (fun s p => if f p.1 == k then combine s p.2 else s) empty

private def rowFold (f : Key → Key) (k : Key) (rows : List Row) : State :=
  rows.foldl (fun s r => if f (fineKey r) == k then addRow s r else s) empty

private theorem finest_eq_foldl (rows : List Row) : finest rows = rows.foldl step [] := by
  rfl

private theorem combine_assoc (a b c : State) : combine (combine a b) c = combine a (combine b c) := by
  ext <;> simp [combine] <;> omega

private theorem combine_empty_left (a : State) : combine empty a = a := by
  ext <;> simp [combine, empty]

private theorem combine_empty_right (a : State) : combine a empty = a := by
  cases a <;> simp [combine, empty]

private theorem foldl_combine_init (Q : (Key × State) → Bool)
    (acc : List (Key × State)) (s0 : State) :
    acc.foldl (fun s p => if Q p then combine s p.2 else s) s0 =
      combine s0 (acc.foldl (fun s p => if Q p then combine s p.2 else s) empty) := by
  induction acc generalizing s0 with
  | nil => simp [combine_empty_right]
  | cons p ps ih =>
      by_cases hq : Q p = true
      · have ih1 := ih (combine s0 p.2)
        have ih2 := ih p.2
        rw [List.foldl_cons, List.foldl_cons, ite_eq_left hq, ite_eq_left hq, combine_empty_left]
        rw [ih1, ih2]
        exact combine_assoc _ _ _
      · rw [List.foldl_cons, List.foldl_cons, ite_eq_right hq, ite_eq_right hq]
        exact ih s0

private theorem foldl_addRow_init (P : Row → Bool) (rows : List Row) (s0 : State) :
    rows.foldl (fun s r => if P r then addRow s r else s) s0 =
      combine s0 (rows.foldl (fun s r => if P r then addRow s r else s) empty) := by
  induction rows generalizing s0 with
  | nil => simp [combine_empty_right]
  | cons r rs ih =>
      by_cases hp : P r = true
      · have ih1 := ih (addRow s0 r)
        have ih2 := ih (addRow empty r)
        rw [List.foldl_cons, List.foldl_cons, ite_eq_left hp, ite_eq_left hp]
        rw [ih1, ih2]
        ext <;> simp [addRow, combine, empty, Nat.add_assoc, Nat.add_comm]
      · rw [List.foldl_cons, List.foldl_cons, ite_eq_right hp, ite_eq_right hp]
        exact ih s0

private theorem accFold_append (f : Key → Key) (k : Key)
    (acc : List (Key × State)) (kk : Key) (s : State) :
    accFold f k (acc ++ [(kk, s)]) = combine (accFold f k acc) (if f kk == k then s else empty) := by
  unfold accFold
  rw [List.foldl_append]
  by_cases h : f kk == k
  · have hk : f kk = k := beq_iff_eq.mp h
    simp [hk]
  · have hk : f kk ≠ k := by intro heq; exact h (beq_iff_eq.mpr heq)
    simp [h, hk, combine_empty_right]

private theorem rowFold_append (f : Key → Key) (k : Key) (rows : List Row) (r : Row) :
    rowFold f k (rows ++ [r]) =
      combine (rowFold f k rows) (if f (fineKey r) == k then addRow empty r else empty) := by
  unfold rowFold
  rw [List.foldl_append]
  by_cases h : f (fineKey r) == k
  · have hk : f (fineKey r) = k := beq_iff_eq.mp h
    simp [hk, addRow, combine, empty]
  · have hk : f (fineKey r) ≠ k := by intro heq; exact h (beq_iff_eq.mpr heq)
    simp [h, hk, combine_empty_right]

private theorem nodup_keys_append (acc : List (Key × State)) (kk : Key) (s : State)
    (hnd : List.Nodup (acc.map Prod.fst))
    (hnone : ∀ p ∈ acc, ¬ ((p.1 == kk) = true)) :
    List.Nodup ((acc ++ [(kk, s)]).map Prod.fst) := by
  rw [List.map_append]
  simp only [List.map_cons, List.map_nil]
  apply List.nodup_append.mpr
  refine ⟨hnd, by simp, ?_⟩
  intro a ha b hb
  have hb' : b = kk := by simpa using hb
  subst b
  rcases (List.mem_map.mp ha) with ⟨p, hpacc, rfl⟩
  intro heq
  apply hnone p hpacc
  exact beq_iff_eq.mpr heq

private theorem nodup_keys_map (acc : List (Key × State)) (kk : Key) (r : Row) (s : State)
    (hnd : List.Nodup (acc.map Prod.fst)) :
    List.Nodup ((acc.map (fun p => if p.1 == kk then (kk, addRow s r) else p)).map Prod.fst) := by
  have hkeys : (acc.map (fun p => if p.1 == kk then (kk, addRow s r) else p)).map Prod.fst =
      acc.map Prod.fst := by
    rw [List.map_map]
    apply List.map_congr_left
    intro p hp
    by_cases h : p.1 == kk
    · have hp1 : p.1 = kk := beq_iff_eq.mp h
      simp [hp1]
    · have hne : p.1 ≠ kk := by intro heq; exact h (beq_iff_eq.mpr heq)
      simp [hne]
  rw [hkeys]
  exact hnd

private theorem find_state_unique : ∀ (acc : List (Key × State)) (kk : Key) (found : Key × State),
    acc.find? (fun p => p.1 == kk) = some found →
    List.Nodup (acc.map Prod.fst) →
    ∀ p ∈ acc, p.1 == kk → p.2 = found.2 := by
  intro acc
  induction acc with
  | nil => intro kk found hfind; cases hfind
  | cons q qs ih =>
      intro kk found hfind hnd p hpacc hpk
      by_cases hq : q.1 == kk
      · have hfind_head : (q :: qs).find? (fun p => p.1 == kk) = some q := by simp [hq]
        have hfound : found = q := by
          rw [hfind_head] at hfind
          cases hfind
          rfl
        subst found
        rcases (List.mem_cons.mp hpacc) with hp | hp
        · subst p
          rfl
        · have hnd' : List.Nodup (q.1 :: qs.map Prod.fst) := by
            simpa [List.map_cons] using hnd
          have hnot := (List.nodup_cons.mp hnd').1
          exfalso
          apply hnot
          rw [List.mem_map]
          refine ⟨p, hp, ?_⟩
          have hq1 : q.1 = kk := beq_iff_eq.mp hq
          have hp1 : p.1 = kk := beq_iff_eq.mp hpk
          exact hp1.trans hq1.symm
      · have hfind_tail : qs.find? (fun p => p.1 == kk) = some found := by
          simpa [hq] using hfind
        have hnd_tail : List.Nodup (qs.map Prod.fst) := by
          have hnd' : List.Nodup (q.1 :: qs.map Prod.fst) := by
            simpa [List.map_cons] using hnd
          exact (List.nodup_cons.mp hnd').2
        rcases (List.mem_cons.mp hpacc) with hp | hp
        · subst p
          exact False.elim (hq hpk)
        · exact ih kk found hfind_tail hnd_tail p hp hpk

private theorem accFold_map_replace (acc : List (Key × State)) (kk : Key) (r : Row)
    (f : Key → Key) (k : Key)
    (hnd : List.Nodup (acc.map Prod.fst))
    (hex : ∃ p ∈ acc, p.1 == kk) :
    accFold f k (acc.map (fun p => if p.1 == kk then (kk, addRow p.2 r) else p)) =
      combine (accFold f k acc) (if f kk == k then addRow empty r else empty) := by
  induction acc with
  | nil => simp at hex
  | cons p ps ih =>
      have hnd' : List.Nodup (p.1 :: ps.map Prod.fst) := by
        simpa [List.map_cons] using hnd
      have hnd_tail : List.Nodup (ps.map Prod.fst) := (List.nodup_cons.mp hnd').2
      by_cases hp : p.1 == kk
      · have hnot : ∀ q ∈ ps, ¬ ((q.1 == kk) = true) := by
          intro q hq hqkk
          apply (List.nodup_cons.mp hnd').1
          rw [List.mem_map]
          refine ⟨q, hq, ?_⟩
          have hp1 : p.1 = kk := beq_iff_eq.mp hp
          have hq1 : q.1 = kk := beq_iff_eq.mp hqkk
          exact hq1.trans hp1.symm
        have hmap : (p :: ps).map (fun q => if q.1 == kk then (kk, addRow q.2 r) else q) =
            (kk, addRow p.2 r) :: ps := by
          simp only [List.map_cons]
          rw [ite_eq_left hp]
          congr
          have hcongr : ps.map (fun q => if q.1 == kk then (kk, addRow q.2 r) else q) = ps.map id := by
            apply List.map_congr_left
            intro q hq
            have hqfalse : (q.1 == kk) = false := by
              cases h : q.1 == kk
              · rfl
              · exfalso
                exact hnot q hq h
            simp [hqfalse]
          simpa using hcongr
        rw [hmap]
        have hp1 : p.1 = kk := beq_iff_eq.mp hp
        have hleft : accFold f k ((kk, addRow p.2 r) :: ps) =
            combine (if f kk == k then addRow p.2 r else empty) (accFold f k ps) := by
          unfold accFold
          rw [List.foldl_cons]
          rw [foldl_combine_init (fun q => f q.1 == k) ps]
          simp [combine_empty_left]
        have hright : combine (accFold f k (p :: ps)) (if f kk == k then addRow empty r else empty) =
            combine (combine (if f kk == k then p.2 else empty) (accFold f k ps))
              (if f kk == k then addRow empty r else empty) := by
          unfold accFold
          rw [List.foldl_cons]
          rw [hp1]
          rw [foldl_combine_init (fun q => f q.1 == k) ps]
          simp [combine_empty_left]
        rw [hleft, hright]
        cases h : f kk == k <;> simp [addRow, combine, empty] <;> omega
      · have hex_tail : ∃ q ∈ ps, q.1 == kk := by
          rcases hex with ⟨q, hqmem, hqkk⟩
          rcases (List.mem_cons.mp hqmem) with hq | hq
          · subst q
            exact False.elim (hp hqkk)
          · exact ⟨q, hq, hqkk⟩
        have ih' := ih hnd_tail hex_tail
        have hmap : (p :: ps).map (fun q => if q.1 == kk then (kk, addRow q.2 r) else q) =
            p :: ps.map (fun q => if q.1 == kk then (kk, addRow q.2 r) else q) := by
          simp only [List.map_cons, ite_eq_right hp]
        rw [hmap]
        unfold accFold at ih' ⊢
        rw [List.foldl_cons, List.foldl_cons]
        simp only [combine_empty_left]
        rw [foldl_combine_init (fun q => f q.1 == k)
              (ps.map (fun q => if q.1 == kk then (kk, addRow q.2 r) else q))
              (if f p.1 == k then p.2 else empty)]
        rw [ih']
        rw [foldl_combine_init (fun q => f q.1 == k) ps (if f p.1 == k then p.2 else empty)]
        exact (combine_assoc _ _ _).symm

private theorem step_inv (acc : List (Key × State)) (prior : List Row)
    (hnd : List.Nodup (acc.map Prod.fst))
    (hinv : ∀ f k, accFold f k acc = rowFold f k prior)
    (r : Row) :
    List.Nodup ((step acc r).map Prod.fst) ∧
    ∀ f k, accFold f k (step acc r) = rowFold f k (prior ++ [r]) := by
  cases hfind : acc.find? (fun p => p.1 == fineKey r) with
  | none =>
      have hstep : step acc r = acc ++ [(fineKey r, addRow empty r)] := by
        simp [step, hfind]
      rw [hstep]
      constructor
      · apply nodup_keys_append acc (fineKey r) (addRow empty r) hnd
        intro p hpacc
        exact (List.find?_eq_none.mp hfind) p hpacc
      · intro f k
        rw [accFold_append, rowFold_append, hinv f k]
  | some found =>
      have hstep : step acc r =
          acc.map (fun p => if p.1 == fineKey r then (fineKey r, addRow found.2 r) else p) := by
        simp [step, hfind]
      rw [hstep]
      constructor
      · exact nodup_keys_map acc (fineKey r) r found.2 hnd
      · intro f k
        have hfound_pred := List.find?_some hfind
        have hfound_mem : found ∈ acc := List.mem_of_find?_eq_some hfind
        have hstate : ∀ p ∈ acc, p.1 == fineKey r → p.2 = found.2 :=
          find_state_unique acc (fineKey r) found hfind hnd
        have hmap : acc.map (fun p => if p.1 == fineKey r then (fineKey r, addRow found.2 r) else p) =
            acc.map (fun p => if p.1 == fineKey r then (fineKey r, addRow p.2 r) else p) := by
          apply List.map_congr_left
          intro p hp
          by_cases h : p.1 == fineKey r
          · rw [hstate p hp h]
          · simp [h]
        rw [hmap]
        have hex : ∃ p ∈ acc, p.1 == fineKey r := ⟨found, hfound_mem, hfound_pred⟩
        rw [accFold_map_replace acc (fineKey r) r f k hnd hex]
        rw [rowFold_append, hinv f k]

private theorem finestFrom_inv : ∀ (acc : List (Key × State)) (prior : List Row) (rows : List Row),
    List.Nodup (acc.map Prod.fst) →
    (∀ f k, accFold f k acc = rowFold f k prior) →
    List.Nodup ((rows.foldl step acc).map Prod.fst) ∧
    ∀ f k, accFold f k (rows.foldl step acc) = rowFold f k (prior ++ rows) := by
  intro acc prior rows
  induction rows generalizing acc prior with
  | nil =>
      intro hnd hinv
      simpa using And.intro hnd hinv
  | cons r rs ih =>
      intro hnd hinv
      have hs := step_inv acc prior hnd hinv r
      have ih' := ih (step acc r) (prior ++ [r]) hs.1 hs.2
      simpa [List.foldl_cons, List.append_assoc] using ih'

private theorem finest_inv (rows : List Row) :
    List.Nodup ((finest rows).map Prod.fst) ∧
    ∀ f k, accFold f k (finest rows) = rowFold f k rows := by
  rw [finest_eq_foldl]
  have h := finestFrom_inv [] [] rows (by simp) (by intro f k; simp [accFold, rowFold])
  simpa using h

private theorem coarsen_fineKey (ga gb : Bool) (r : Row) :
    coarsen ga gb (fineKey r) = masked ga gb r := by
  simp [fineKey, coarsen, masked]

private theorem rowFold_eq_direct (ga gb : Bool) (rows : List Row) (k : Key) :
    rowFold (coarsen ga gb) k rows = direct (ga, gb) rows k := by
  unfold rowFold direct
  have hstep : (fun s r => if coarsen ga gb (fineKey r) == k then addRow s r else s) =
      (fun s r => if masked ga gb r == k then addRow s r else s) := by
    funext s r
    rw [coarsen_fineKey]
  rw [hstep]

theorem sound : Goal := by
  intro rows mask hmask
  constructor
  · funext k
    have h := (finest_inv rows).2 (coarsen mask.1 mask.2) k
    change accFold (coarsen mask.1 mask.2) k (finest rows) = direct mask rows k
    rw [h]
    rcases mask with ⟨ga, gb⟩
    exact rowFold_eq_direct ga gb rows k
  · intro hne
    have hlen : 0 < rows.length := List.length_pos_iff.mpr hne
    simp [reuseReads, directReads, masks]
    omega
