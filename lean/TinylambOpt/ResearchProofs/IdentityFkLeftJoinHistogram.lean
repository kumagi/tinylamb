import TinylambOpt.ResearchGoals.IdentityFkLeftJoinHistogram

namespace TinylambOpt.ResearchProofs.IdentityFkLeftJoinHistogram

open TinylambOpt.ResearchGoals.IdentityFkLeftJoinHistogram

private def rowMatches (i : Nat) (r : RRow) : Bool :=
  r.pred && r.val.isSome && (r.fk == some (↑(i + 1) : Int))

private theorem refPerKeyCounts_eq (n : Nat) (rs : List RRow) :
    refPerKeyCounts n rs = (List.range n).map (fun i => (rs.filter (rowMatches i)).length) := by
  apply List.ext_getElem?
  intro j
  by_cases hj : j < n
  · simp [refPerKeyCounts, hj]
    congr 1
    apply List.filter_congr
    intro r hr
    simp [rowMatches, Bool.and_assoc, Bool.and_comm]
  · simp [refPerKeyCounts, hj]

private def step (n : Nat) (acc : List Nat) (r : RRow) : List Nat :=
  if !r.pred || !r.val.isSome then acc
  else
    match r.fk with
    | some k =>
        if 1 ≤ k && k ≤ Int.ofNat n then incSlot acc (k.toNat - 1) else acc
    | none => acc

private theorem incSlot_getElem (acc : List Nat) (idx j : Nat) :
    (incSlot acc idx)[j]? = Option.map (fun v => if j == idx then v + 1 else v) acc[j]? := by
  simp [incSlot, List.getElem?_map, List.getElem?_zipIdx, Function.comp_def]

private theorem keyEq_iff_slot (n i : Nat) (k : Int) (hk1 : 1 ≤ k) (hkn : k ≤ Int.ofNat n) :
    (k == (↑(i + 1) : Int)) = (i == k.toNat - 1) := by
  have hk_nonneg : 0 ≤ k := by omega
  have hk_toNat : (k.toNat : Int) = k := Int.toNat_of_nonneg hk_nonneg
  by_cases h : k = (↑(i + 1) : Int)
  · subst k
    simp
  · have hslot : k.toNat - 1 ≠ i := by
      intro hs
      have hge : 1 ≤ k.toNat := by omega
      have hto : k.toNat = i + 1 := by omega
      have hk_eq : k = (↑(i + 1) : Int) := by
        simpa [hto] using hk_toNat.symm
      exact h hk_eq
    have hA : (k == (↑(i + 1) : Int)) = false := by simpa using h
    have hB : (i == k.toNat - 1) = false := by
      have hne : i ≠ k.toNat - 1 := by intro hi; exact hslot hi.symm
      simp [hne]
    rw [hA, hB]

private theorem step_getElem (n : Nat) (r : RRow) (acc : List Nat) (i : Nat) (hi : i < n) :
    (step n acc r)[i]? = Option.map (fun v => v + if rowMatches i r then 1 else 0) acc[i]? := by
  unfold step
  cases hp : r.pred
  · simp [hp, rowMatches]
  · cases hv : r.val.isSome
    · simp [hp, hv, rowMatches]
    · cases hr : r.fk with
      | none =>
          simp [hp, hv, hr, rowMatches]
      | some k =>
          by_cases hc : (decide (1 ≤ k) && decide (k ≤ Int.ofNat n)) = true
          · have h1 : 1 ≤ k := by
              by_cases h1' : 1 ≤ k
              · exact h1'
              · have hc' : (decide (1 ≤ k) && decide (k ≤ Int.ofNat n)) = false := by
                  simp [h1']
                rw [hc'] at hc
                contradiction
            have h2 : k ≤ Int.ofNat n := by
              by_cases h2' : k ≤ Int.ofNat n
              · exact h2'
              · have h2not : ¬ k ≤ (↑n : Int) := by
                  intro hkn
                  exact h2' (by simpa using hkn)
                have hc' : (decide (1 ≤ k) && decide (k ≤ Int.ofNat n)) = false := by
                  simp [h2not]
                rw [hc'] at hc
                contradiction
            have h2n : k ≤ (↑n : Int) := by simpa using h2
            have hkey : (k == (↑(i + 1) : Int)) = (i == k.toNat - 1) :=
              keyEq_iff_slot n i k h1 h2
            cases hb : (k == (↑(i + 1) : Int))
            · have hs : (i == k.toNat - 1) = false := by
                rw [← hkey]
                exact hb
              have hkNe : k ≠ (↑i + 1 : Int) := by simpa using hb
              simp [hp, hv, hr, rowMatches, h1, h2n, incSlot_getElem, hs, hkNe]
            · have hs : (i == k.toNat - 1) = true := by
                rw [← hkey]
                exact hb
              have hkEq : k = (↑i + 1 : Int) := by simpa using hb
              have hge : (1 : Int) ≤ (↑i + 1 : Int) := by omega
              have hle2 : (↑i + 1 : Int) ≤ (↑n : Int) := by simpa [hkEq] using h2n
              simp [hp, hv, hr, rowMatches, hge, hle2, incSlot_getElem, hkEq]
          · have hle' : (↑(i + 1) : Int) ≤ (↑n : Int) :=
              (Int.ofNat_le).2 (Nat.succ_le_of_lt hi)
            have hle : (↑i + 1 : Int) ≤ Int.ofNat n := by simpa using hle'
            have hle2 : (↑i + 1 : Int) ≤ (↑n : Int) := by simpa using hle'
            have hge : (1 : Int) ≤ (↑i + 1 : Int) := by omega
            have hne : k ≠ (↑i + 1 : Int) := by
              intro heq
              apply hc
              simp [heq, hge, hle2]
            have hnot : ¬ (1 ≤ k ∧ k ≤ (↑n : Int)) := by
              intro hk
              apply hc
              simp [hk.1, hk.2]
            have hbf : (k == (↑(i + 1) : Int)) = false := by simp [hne]
            simp [hp, hv, hr, rowMatches, hnot, hne]

private theorem fold_getElem (n : Nat) (rs : List RRow) (acc : List Nat) (i : Nat) (hi : i < n) :
    (rs.foldl (step n) acc)[i]? =
      Option.map (fun v => v + (rs.filter (rowMatches i)).length) acc[i]? := by
  induction rs generalizing acc with
  | nil => simp
  | cons r rs ih =>
      rw [List.foldl_cons, ih, step_getElem n r acc i hi]
      have hlen : (List.filter (rowMatches i) (r :: rs)).length =
          (if rowMatches i r then 1 else 0) + (List.filter (rowMatches i) rs).length := by
        by_cases hm : rowMatches i r = true <;> simp [hm] <;> omega
      rw [hlen]
      cases h : acc[i]? with
      | none => simp
      | some v =>
          simp
          omega

private theorem step_length (n : Nat) (acc : List Nat) (r : RRow) :
    (step n acc r).length = acc.length := by
  unfold step
  cases hp : r.pred <;> cases hv : r.val.isSome
  · simp
  · simp
  · simp
  · cases hr : r.fk with
    | none => simp
    | some k =>
        by_cases hcond : 1 ≤ k ∧ k ≤ (↑n : Int) <;> simp [hcond, incSlot]

private theorem fold_length (n : Nat) (rs : List RRow) (acc : List Nat) :
    (rs.foldl (step n) acc).length = acc.length := by
  induction rs generalizing acc with
  | nil => rfl
  | cons r rs ih => rw [List.foldl_cons, ih, step_length]

private theorem counts_eq_refPerKeyCounts (n : Nat) (rs : List RRow) :
    rs.foldl (step n) (List.replicate n 0) = refPerKeyCounts n rs := by
  apply List.ext_getElem?
  intro i
  by_cases hi : i < n
  · rw [fold_getElem n rs _ i hi, refPerKeyCounts_eq]
    simp [hi]
  · have hlen : (rs.foldl (step n) (List.replicate n 0)).length = n := by
      simpa using fold_length n rs (List.replicate n 0)
    simp [refPerKeyCounts, hi]
    omega

private theorem zeros_add_pos_eq_length (l : List Nat) :
    (l.filter (· == 0)).length + (l.filter (0 < ·)).length = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : x = 0
      · subst x
        simp
        omega
      · have hxpos : 0 < x := Nat.pos_of_ne_zero hx
        have hxeq : (x == 0) = false := by simp [hx]
        simp [hxpos, hxeq]
        omega

private theorem pos_filter_eq_filter (l : List Nat) (c : Nat) (hc : 0 < c) :
    (l.filter (fun x => x == c && 0 < x)).length = (l.filter (· == c)).length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      by_cases hx : x == c
      · have hxc : x = c := by simpa using hx
        subst x
        simp [ih, hc]
      · have hxeq : (x == c) = false := by simp [hx]
        simp [ih, hxeq]

theorem sound : TinylambOpt.ResearchGoals.IdentityFkLeftJoinHistogram.Goal := by
  intro n rs c
  unfold optHist refHist
  change (let counts := rs.foldl (step n) (List.replicate n 0);
          let nonZero := counts.filter (0 < ·);
          if c == 0 then n - nonZero.length else (nonZero.filter (· == c)).length) =
          ((refPerKeyCounts n rs).filter (· == c)).length
  dsimp
  rw [counts_eq_refPerKeyCounts]
  by_cases hc : c == 0
  · have hc0 : c = 0 := by simpa using hc
    subst c
    simp
    have hsum := zeros_add_pos_eq_length (refPerKeyCounts n rs)
    have hlen : (refPerKeyCounts n rs).length = n := by simp [refPerKeyCounts]
    omega
  · have hcpos : 0 < c := by
      have hcne : c ≠ 0 := by
        intro h
        apply hc
        simp [h]
      omega
    simp [hc]
    rw [pos_filter_eq_filter _ _ hcpos]

end TinylambOpt.ResearchProofs.IdentityFkLeftJoinHistogram
