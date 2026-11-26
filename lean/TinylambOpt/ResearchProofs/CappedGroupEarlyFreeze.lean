import TinylambOpt.ResearchGoals.CappedGroupEarlyFreeze

/-!
Proved

- `Goal` — Early-frozen hash aggregation for un-ordered `GROUP BY ... LIMIT K`
  (ClickBench Q18): stopping new key allocation at `K` distinct keys while continuing
  updates for existing keys yields `min K (#distinct)` unique groups whose counts
  match exact full-table `COUNT(*)`, whereas stopping the scan prematurely misses
  trailing increments (and adding `ORDER BY`/`HAVING` breaks early freeze).

The reference model is defined in `TinylambOpt.ResearchGoals.CappedGroupEarlyFreeze`.
-/

namespace TinylambOpt.ResearchProofs.CappedGroupEarlyFreeze

open TinylambOpt.ResearchGoals.CappedGroupEarlyFreeze

private def keys (m : List (Int × Nat)) : List Int := m.map Prod.fst

private def predKeys (m : List (Int × Nat)) (p : Int × Nat) : Bool :=
  decide (p.1 ∈ keys m)

private theorem fst_mem_keys {p : Int × Nat} {m : List (Int × Nat)} (hp : p ∈ m) :
    p.1 ∈ keys m := by
  change p.1 ∈ m.map Prod.fst
  exact List.mem_map.mpr ⟨p, hp, rfl⟩

private theorem mem_keys_cons_ne {k k' : Int} {c : Nat} {rest : List (Int × Nat)}
    (hne : k' ≠ k) : (k ∈ keys ((k', c) :: rest)) ↔ k ∈ keys rest := by
  have hne' : k ≠ k' := by intro h; exact hne h.symm
  simp [keys, hne']

private theorem any_eq_mem (m : List (Int × Nat)) (k : Int) :
    (m.any (fun p => p.1 == k)) = true ↔ k ∈ keys m := by
  induction m with
  | nil => simp [keys]
  | cons p rest ih =>
      rcases p with ⟨k', c⟩
      by_cases heq : k' == k
      · have hk : k' = k := (beq_iff_eq).mp heq
        subst k'
        simp [keys]
      · have hkne : k' ≠ k := by
          intro h
          subst k'
          simp at heq
        have hmc := mem_keys_cons_ne (k := k) (k' := k') (c := c) (rest := rest) hkne
        simp [heq, ih, hmc]

private theorem keys_addGroupCount (m : List (Int × Nat)) (k : Int) :
    keys (addGroupCount m k) = if k ∈ keys m then keys m else keys m ++ [k] := by
  induction m with
  | nil => simp [keys, addGroupCount]
  | cons p rest ih =>
      rcases p with ⟨k', c⟩
      by_cases heq : k' == k
      · have hk : k' = k := (beq_iff_eq).mp heq
        subst k'
        simp [addGroupCount, keys]
      · have hkne : k' ≠ k := by
          intro h
          subst k'
          simp at heq
        have hne' : k ≠ k' := by intro h; exact hkne h.symm
        by_cases hmr : k ∈ keys rest
        · have hrest : keys (addGroupCount rest k) = keys rest := by simpa [hmr] using ih
          simp only [addGroupCount, heq]
          change k' :: keys (addGroupCount rest k) =
            (if k ∈ k' :: keys rest then k' :: keys rest else k' :: (keys rest ++ [k]))
          simp [hne', hmr, hrest]
        · have hrest : keys (addGroupCount rest k) = keys rest ++ [k] := by simpa [hmr] using ih
          simp only [addGroupCount, heq]
          change k' :: keys (addGroupCount rest k) =
            (if k ∈ k' :: keys rest then k' :: keys rest else k' :: (keys rest ++ [k]))
          simp [hne', hmr, hrest]

private theorem keys_addGroupCount_of_mem (m : List (Int × Nat)) (k : Int)
    (h : k ∈ keys m) : keys (addGroupCount m k) = keys m := by
  rw [keys_addGroupCount]
  simp [h]

private theorem keys_addGroupCount_of_notMem (m : List (Int × Nat)) (k : Int)
    (h : k ∉ keys m) : keys (addGroupCount m k) = keys m ++ [k] := by
  rw [keys_addGroupCount]
  simp [h]

private theorem filter_addGroupCount (P : Int → Bool) (m : List (Int × Nat)) (k : Int) :
    (addGroupCount m k).filter (fun p => P p.1) =
      if P k then addGroupCount (m.filter (fun p => P p.1)) k
      else m.filter (fun p => P p.1) := by
  induction m with
  | nil =>
      by_cases hPk : P k = true <;> simp [addGroupCount, hPk]
  | cons p rest ih =>
      rcases p with ⟨k', c⟩
      by_cases heq : k' == k
      · have hk : k' = k := (beq_iff_eq).mp heq
        subst k'
        by_cases hPk : P k = true <;> simp [addGroupCount, hPk]
      · have hkne : k' ≠ k := by
          intro h
          subst k'
          simp at heq
        by_cases hPk : P k = true <;> by_cases hPk' : P k' = true <;>
          simp [addGroupCount, heq, hPk, hPk', ih]

private theorem filter_predKeys_self (m : List (Int × Nat)) :
    m.filter (predKeys m) = m := by
  apply List.filter_eq_self.mpr
  intro p hp
  simp [predKeys, fst_mem_keys hp]

private theorem take_eq_self_of_le {α : Type} (l : List α) {n : Nat} (h : l.length ≤ n) :
    l.take n = l := by
  induction l generalizing n with
  | nil => simp
  | cons a t ih =>
      cases n with
      | zero => simp at h
      | succ n =>
          simp only [List.length_cons] at h
          have h' : t.length ≤ n := by omega
          rw [List.take_cons (Nat.succ_pos n)]
          simp [ih h']

private theorem stepFrozen_of_mem (lim : Nat) (m : List (Int × Nat)) (k : Int)
    (h : k ∈ keys m) : stepFrozen lim m k = addGroupCount m k := by
  have ha : m.any (fun p => p.1 == k) = true := (any_eq_mem m k).mpr h
  simp [stepFrozen, ha]

private theorem stepFrozen_of_notMem_of_lt (lim : Nat) (m : List (Int × Nat)) (k : Int)
    (h : k ∉ keys m) (hlt : m.length < lim) : stepFrozen lim m k = addGroupCount m k := by
  have ha : m.any (fun p => p.1 == k) = false := by
    cases hb : m.any (fun p => p.1 == k)
    · rfl
    · exact False.elim (h ((any_eq_mem m k).mp hb))
  simp [stepFrozen, ha, hlt]

private theorem stepFrozen_of_notMem_of_not_lt (lim : Nat) (m : List (Int × Nat)) (k : Int)
    (h : k ∉ keys m) (hlt : ¬ m.length < lim) : stepFrozen lim m k = m := by
  have ha : m.any (fun p => p.1 == k) = false := by
    cases hb : m.any (fun p => p.1 == k)
    · rfl
    · exact False.elim (h ((any_eq_mem m k).mp hb))
  simp [stepFrozen, ha, hlt]

private def Inv (lim : Nat) (rows : List Int) (m : List (Int × Nat)) : Prop :=
  keys m = (keys (fullGroupCount rows)).take lim ∧
  m = (fullGroupCount rows).filter (predKeys m)

private theorem step_preserves (lim : Nat) (pref : List Int) (m : List (Int × Nat))
    (k : Int) (hInv : Inv lim pref m) : Inv lim (pref ++ [k]) (stepFrozen lim m k) := by
  let F := fullGroupCount pref
  have hF : fullGroupCount (pref ++ [k]) = addGroupCount F k := by
    simp [fullGroupCount, List.foldl_append, F]
  rcases hInv with ⟨hkeys, hm⟩
  by_cases hmem : k ∈ keys m
  · have hfmem : k ∈ keys F := by
      apply List.mem_of_mem_take
      simpa [hkeys] using hmem
    have hstep : stepFrozen lim m k = addGroupCount m k := stepFrozen_of_mem lim m k hmem
    have hkm : keys (addGroupCount m k) = keys m := keys_addGroupCount_of_mem m k hmem
    have hkf : keys (addGroupCount F k) = keys F := keys_addGroupCount_of_mem F k hfmem
    constructor
    · simpa [hstep, hkm, hF, hkf] using hkeys
    · have hmem' : k ∈ keys (addGroupCount m k) := by simpa [hkm] using hmem
      have hPk : decide (k ∈ keys (addGroupCount m k)) = true := by simp [hmem']
      have hFiltEq : F.filter (fun p : Int × Nat => decide (p.1 ∈ keys (addGroupCount m k))) = m := by
        change F.filter (predKeys (addGroupCount m k)) = m
        have hp : predKeys (addGroupCount m k) = predKeys m := by
          funext p
          simp [predKeys, hkm]
        rw [hp]
        simpa [F] using hm.symm
      have hfilt := filter_addGroupCount
        (fun x : Int => decide (x ∈ keys (addGroupCount m k))) F k
      rw [hstep, hF]
      change addGroupCount m k =
        (addGroupCount F k).filter (fun p : Int × Nat => decide (p.1 ∈ keys (addGroupCount m k)))
      simpa [hPk, hFiltEq] using hfilt.symm
  · by_cases hlt : m.length < lim
    · have hstep : stepFrozen lim m k = addGroupCount m k :=
        stepFrozen_of_notMem_of_lt lim m k hmem hlt
      have hlen_take : ((keys F).take lim).length = m.length := by
        rw [← hkeys]
        simp [keys]
      have hKlt : (keys F).length < lim := by
        by_cases hle : lim ≤ (keys F).length
        · have htake : ((keys F).take lim).length = lim := by
            simp [List.length_take, Nat.min_eq_left hle]
          omega
        · exact Nat.lt_of_not_ge hle
      have htake_all : (keys F).take lim = keys F :=
        take_eq_self_of_le (keys F) (Nat.le_of_lt hKlt)
      have hkeys_eq : keys m = keys F := by simpa [F, htake_all] using hkeys
      have hm_eq_F : m = F := by
        rw [hm]
        apply List.filter_eq_self.mpr
        intro p hp
        have hpkeys : p.1 ∈ keys m := by simpa [hkeys_eq] using fst_mem_keys hp
        simp [predKeys, hpkeys]
      have hfmem : k ∉ keys F := by simpa [hkeys_eq] using hmem
      have hkm : keys (addGroupCount m k) = keys F ++ [k] := by
        simpa [hkeys_eq] using keys_addGroupCount_of_notMem m k hmem
      have hkf : keys (addGroupCount F k) = keys F ++ [k] :=
        keys_addGroupCount_of_notMem F k hfmem
      have hsize : (keys F ++ [k]).length ≤ lim := by
        have hmlen : m.length = (keys F).length := by simp [keys, hm_eq_F]
        have happ : (keys F ++ [k]).length = (keys F).length + 1 := by simp
        omega
      constructor
      · simp [F, hF, hstep, hkm, hkf,
          take_eq_self_of_le (keys F ++ [k]) hsize]
      · rw [hstep, hF, hm_eq_F]
        exact (filter_predKeys_self (addGroupCount F k)).symm
    · have hstep : stepFrozen lim m k = m :=
        stepFrozen_of_notMem_of_not_lt lim m k hmem hlt
      have hlen_take : ((keys F).take lim).length = m.length := by
        rw [← hkeys]
        simp [keys]
      have hlen_le : m.length ≤ lim := by
        rw [← hlen_take, List.length_take]
        exact Nat.min_le_left lim (keys F).length
      have hlen_eq : m.length = lim := by omega
      have htake_len : ((keys F).take lim).length = lim := by omega
      constructor
      · by_cases hfmem : k ∈ keys F
        · simpa [hstep, hF, keys_addGroupCount_of_mem F k hfmem] using hkeys
        · have hleK : lim ≤ (keys F).length := by
            by_cases hltK : (keys F).length < lim
            · have htake : (keys F).take lim = keys F :=
                take_eq_self_of_le (keys F) (Nat.le_of_lt hltK)
              have : (keys F).length = lim := by simpa [htake] using htake_len
              omega
            · exact Nat.le_of_not_gt hltK
          have htake_app : ((keys F) ++ [k]).take lim = (keys F).take lim :=
            List.take_append_of_le_length hleK
          simpa [hstep, hF, keys_addGroupCount_of_notMem F k hfmem, htake_app] using hkeys
      · have hPk : decide (k ∈ keys m) = false := by simp [hmem]
        have hfilt := filter_addGroupCount (fun x : Int => decide (x ∈ keys m)) F k
        have hleft : F.filter (fun p : Int × Nat => decide (p.1 ∈ keys m)) = m := by
          change F.filter (predKeys m) = m
          simpa [F] using hm.symm
        have hright : F.filter (fun p : Int × Nat => decide (p.1 ∈ keys m)) =
            (addGroupCount F k).filter (fun p : Int × Nat => decide (p.1 ∈ keys m)) := by
          simpa [hPk] using hfilt.symm
        rw [hstep, hF]
        change m = (addGroupCount F k).filter (fun p : Int × Nat => decide (p.1 ∈ keys m))
        exact hleft.symm.trans hright

private theorem inv_aux (lim : Nat) : ∀ (pref rest : List Int) (m : List (Int × Nat)),
    Inv lim pref m → Inv lim (pref ++ rest) (rest.foldl (stepFrozen lim) m) := by
  intro pref rest
  induction rest generalizing pref with
  | nil =>
      intro m hInv
      simpa using hInv
  | cons k rest ih =>
      intro m hInv
      have hs := step_preserves lim pref m k hInv
      have hrest := ih (pref ++ [k]) (stepFrozen lim m k) hs
      simpa [List.append_assoc] using hrest

private theorem inv_frozen (lim : Nat) (rows : List Int) :
    Inv lim rows (frozenGroupCount lim rows) := by
  have h := inv_aux lim [] rows [] (by simp [Inv, keys, fullGroupCount])
  simpa [frozenGroupCount] using h

private theorem keys_full_nodup : ∀ rows : List Int, (keys (fullGroupCount rows)).Nodup := by
  have haux : ∀ (rows : List Int) (m : List (Int × Nat)),
      (keys m).Nodup → (keys (rows.foldl addGroupCount m)).Nodup := by
    intro rows
    induction rows with
    | nil =>
        intro m hm
        simpa using hm
    | cons a rest ih =>
        intro m hm
        simp only [List.foldl_cons]
        apply ih
        by_cases hmem : a ∈ keys m
        · simpa [keys_addGroupCount_of_mem m a hmem] using hm
        · rw [keys_addGroupCount_of_notMem m a hmem]
          rw [List.nodup_append]
          refine ⟨hm, by simp, ?_⟩
          intro x hx y hy
          have hy' : y = a := by simpa using hy
          subst y
          intro hxy
          apply hmem
          simpa [hxy] using hx
  intro rows
  simpa [fullGroupCount] using haux rows [] (by simp [keys])

theorem sound : TinylambOpt.ResearchGoals.CappedGroupEarlyFreeze.Goal := by
  constructor
  · intro lim rows
    let out := frozenGroupCount lim rows
    let full := fullGroupCount rows
    have hInv := inv_frozen lim rows
    have hkeys : keys out = (keys full).take lim := by
      simpa [out, full] using hInv.1
    have hm : out = full.filter (predKeys out) := by
      simpa [out, full] using hInv.2
    have hlen : out.length = min lim full.length := by
      calc
        out.length = (keys out).length := by simp [keys]
        _ = ((keys full).take lim).length := by rw [hkeys]
        _ = min lim (keys full).length := List.length_take
        _ = min lim full.length := by simp [keys]
    have hnodup : (out.map Prod.fst).Nodup := by
      change (keys out).Nodup
      rw [hkeys]
      exact List.Pairwise.take (keys_full_nodup rows)
    have hmem : ∀ p ∈ out, p ∈ full := by
      intro p hp
      rw [hm] at hp
      exact (List.mem_filter.mp hp).1
    refine ⟨hlen, hnodup, hmem, ?_⟩
    rw [hlen]
    exact Nat.min_le_left lim full.length
  · refine ⟨2, [1, 2, 1], (1, 1), ?_, ?_⟩
    · decide
    · decide

end TinylambOpt.ResearchProofs.CappedGroupEarlyFreeze
