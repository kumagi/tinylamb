import TinylambOpt.ResearchGoals.QueryRewrites.PivotedAggregatesAndSemijoinFilters
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
These claims cover pivoting two years into aggregate states, filtering groups
by their averages, and propagating selective dimension membership. They
support SQL plans that remove modeled self-joins or filter before expansion
while preserving the relevant groups and totals.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.PivotedAggregatesAndSemijoinFilters

open TinylambOpt.ResearchGoals.AnalyticsWorkloads (Totals zero weighted plus summarize)
open TinylambOpt.ResearchGoals.QueryRewrites.PivotedAggregatesAndSemijoinFilters

set_option maxRecDepth 100000
set_option maxHeartbeats 0

/-! Shared helpers: a HashMap fold seen through one fixed key. -/

private theorem hmFoldAt {α β ρ : Type} [BEq α] [Hashable α] [DecidableEq α] [EquivBEq α]
    [LawfulHashable α] [LawfulBEq α]
    (default : β) (key : ρ → α) (keep : ρ → Bool) (step : β → ρ → β)
    (rs : List ρ) (h0 : Std.HashMap α β) (id : α) :
    (rs.foldl (fun h r => if keep r then h.insert (key r) (step (h.getD (key r) default) r) else h) h0)[id]? =
      let rel := rs.filter (fun r => keep r && (key r == id))
      match h0[id]? with
      | some v0 => some (rel.foldl step v0)
      | none => if rel.length == 0 then none else some (rel.foldl step default) := by
  induction rs generalizing h0 with
  | nil =>
      cases h : h0[id]? <;> simp [h]
  | cons r rs ih =>
      by_cases hkeep : keep r = true
      · simp only [hkeep, List.foldl_cons, ↓reduceIte]
        rw [ih]
        by_cases hk : key r == id
        · have hkid : key r = id := beq_iff_eq.mp hk
          rw [Std.HashMap.getElem?_insert]
          simp only [hk, ↓reduceIte]
          rw [Std.HashMap.getD_eq_getD_getElem?]
          cases hh : h0[id]? with
          | none => simp [hh, hkeep, hkid]
          | some v0 => simp [hh, hkeep, hkid]
        · have hkf : (key r == id) = false := Bool.eq_false_of_ne_true hk
          rw [Std.HashMap.getElem?_insert]
          simp only [hkf]
          simp [hkeep, hkf]
      · have hkf : keep r = false := Bool.eq_false_of_ne_true hkeep
        simp [hkf, ih]

private theorem foldl_map_fold {α β γ : Type} (f : α → β) (g : γ → β → γ)
    (xs : List α) (init : γ) :
    (xs.map f).foldl g init = xs.foldl (fun a x => g a (f x)) init := by
  induction xs generalizing init with
  | nil => rfl
  | cons x xs ih => simp [ih]

/-! ## YearPairPivotSelfJoinElim -/

private def yrKeep (y : Int) (r : YrRow) : Bool :=
  let (_, yr, _) := r
  (yr == y) || (yr == y + 1)

private def yrStep (y : Int) (st : Totals × Totals) (r : YrRow) : Totals × Totals :=
  let (_, yr, v) := r
  if yr == y then (plus st.1 (weighted 1 v), st.2)
  else (st.1, plus st.2 (weighted 1 v))

private theorem yr_stepfn_eq (y : Int) (h : Std.HashMap Nat (Totals × Totals)) (r : YrRow) :
    (if r.2.1 == y then
       h.insert r.1 (plus (h.getD r.1 (zero, zero)).1 (weighted 1 r.2.2), (h.getD r.1 (zero, zero)).2)
     else if r.2.1 == y + 1 then
       h.insert r.1 ((h.getD r.1 (zero, zero)).1, plus (h.getD r.1 (zero, zero)).2 (weighted 1 r.2.2))
     else h) =
    if yrKeep y r then h.insert r.1 (yrStep y (h.getD r.1 (zero, zero)) r) else h := by
  rcases r with ⟨k, yr, v⟩
  unfold yrKeep yrStep
  by_cases h1 : yr == y
  · simp [h1]
  · by_cases h2 : yr == y + 1
    · simp [h1, h2]
    · simp [h1, h2]

private theorem pivotFold_eq_genericAux (rows : List YrRow) (y : Int)
    (h0 : Std.HashMap Nat (Totals × Totals)) :
    rows.foldl (fun h r =>
      if r.2.1 == y then
        h.insert r.1 (plus (h.getD r.1 (zero, zero)).1 (weighted 1 r.2.2), (h.getD r.1 (zero, zero)).2)
      else if r.2.1 == y + 1 then
        h.insert r.1 ((h.getD r.1 (zero, zero)).1, plus (h.getD r.1 (zero, zero)).2 (weighted 1 r.2.2))
      else h) h0 =
    rows.foldl (fun h r => if yrKeep y r then h.insert r.1 (yrStep y (h.getD r.1 (zero, zero)) r) else h) h0 := by
  induction rows generalizing h0 with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.foldl_cons]
      rw [yr_stepfn_eq]
      exact ih _

private theorem yrStep_fold (rs : List YrRow) (y : Int) (a b : Totals)
    (hall : ∀ r ∈ rs, yrKeep y r = true) :
    rs.foldl (yrStep y) (a, b) =
      ((rs.filter fun r => r.2.1 == y).foldl (fun s r => plus s (weighted 1 r.2.2)) a,
       (rs.filter fun r => r.2.1 == y + 1).foldl (fun s r => plus s (weighted 1 r.2.2)) b) := by
  induction rs generalizing a b with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨k, yr, v⟩
      have hh : yrKeep y (k, yr, v) = true := hall (k, yr, v) (by simp)
      unfold yrKeep at hh
      by_cases h1 : yr == y
      · have hy : yr = y := by simpa using h1
        have h2f : (yr == y + 1) = false := by
          apply Bool.eq_false_of_ne_true
          intro hh'
          have : yr = y + 1 := by simpa using hh'
          omega
        simp [yrStep, h1, h2f]
        exact ih (plus a (weighted 1 v)) b (fun r hr => hall r (by simp [hr]))
      · have h2 : yr == y + 1 := by
          simpa [h1] using hh
        simp [yrStep, h1, h2]
        exact ih a (plus b (weighted 1 v)) (fun r hr => hall r (by simp [hr]))

private theorem yrStep_fold_summarize (rs : List YrRow) (y : Int)
    (hall : ∀ r ∈ rs, yrKeep y r = true) :
    rs.foldl (yrStep y) (zero, zero) =
      (summarize ((rs.filter fun r => r.2.1 == y).map fun r => r.2.2),
       summarize ((rs.filter fun r => r.2.1 == y + 1).map fun r => r.2.2)) := by
  rw [yrStep_fold rs y zero zero hall]
  rw [summarize, summarize]
  rw [foldl_map_fold, foldl_map_fold]

private theorem filter_rel_y (rows : List YrRow) (y : Int) (id : Nat) :
    (rows.filter (fun r => yrKeep y r && r.1 == id)).filter (fun r => r.2.1 == y) =
      rows.filter (fun r => r.1 == id && r.2.1 == y) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  rcases r with ⟨k, yr, v⟩
  unfold yrKeep
  by_cases h1 : yr == y <;> by_cases h2 : yr == y + 1 <;> by_cases h3 : k == id <;>
    simp [h1, h2, h3, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm]

private theorem filter_rel_yp1 (rows : List YrRow) (y : Int) (id : Nat) :
    (rows.filter (fun r => yrKeep y r && r.1 == id)).filter (fun r => r.2.1 == y + 1) =
      rows.filter (fun r => r.1 == id && r.2.1 == y + 1) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  rcases r with ⟨k, yr, v⟩
  unfold yrKeep
  by_cases h1 : yr == y <;> by_cases h2 : yr == y + 1 <;> by_cases h3 : k == id <;>
    simp [h1, h2, h3, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm]

theorem YearPairPivotSelfJoinElim_sound :
    TinylambOpt.ResearchGoals.QueryRewrites.PivotedAggregatesAndSemijoinFilters.YearPairPivotSelfJoinElim.Goal := by
  intro rows y id
  dsimp [pivotYears, refYearSelfJoin]
  rw [pivotFold_eq_genericAux rows y ({} : Std.HashMap Nat (Totals × Totals))]
  rw [hmFoldAt (default := (zero, zero)) (key := fun r : YrRow => r.1)
    (keep := yrKeep y) (step := yrStep y) rows ({} : Std.HashMap Nat (Totals × Totals)) id]
  simp only [Std.HashMap.getElem?_empty, ↓reduceIte]
  let rel := rows.filter (fun r => yrKeep y r && r.1 == id)
  change (match (if rel.length == 0 then none else some (rel.foldl (yrStep y) (zero, zero))) with
    | some (a, b) => if a.cnt > 0 && b.cnt > 0 && a.nn > 0 && a.sum > 0 then some (a, b) else none
    | none => none) =
    (let a := summarize ((rows.filter fun (k, yr, _) => k == id && yr == y).map fun r => r.2.2)
     let b := summarize ((rows.filter fun (k, yr, _) => k == id && yr == y + 1).map fun r => r.2.2)
     if a.cnt > 0 && b.cnt > 0 && a.nn > 0 && a.sum > 0 then some (a, b) else none)
  have hrel_keep : ∀ r ∈ rel, yrKeep y r = true := by
    intro r hr
    dsimp [rel] at hr
    have hb := (List.mem_filter.mp hr).2
    exact (Bool.and_eq_true_iff.mp hb).1
  rw [yrStep_fold_summarize rel y hrel_keep]
  rw [filter_rel_y, filter_rel_yp1]
  by_cases hrel : rel.length == 0
  · have hrel_nil : rel = [] := List.length_eq_zero_iff.mp (by simpa using hrel : rel.length = 0)
    have hzy : (rows.filter (fun r => yrKeep y r && r.1 == id)).filter (fun r => r.2.1 == y) = [] := by
      simpa [rel] using congrArg (fun l : List YrRow => l.filter (fun r => r.2.1 == y)) hrel_nil
    have hzp : (rows.filter (fun r => yrKeep y r && r.1 == id)).filter (fun r => r.2.1 == y + 1) = [] := by
      simpa [rel] using congrArg (fun l : List YrRow => l.filter (fun r => r.2.1 == y + 1)) hrel_nil
    have hfy : (rows.filter fun (k, yr, _) => k == id && yr == y) = [] := by
      rw [← filter_rel_y]
      exact hzy
    have hfp : (rows.filter fun (k, yr, _) => k == id && yr == y + 1) = [] := by
      rw [← filter_rel_yp1]
      exact hzp
    simp [hrel, hfy, hfp, summarize, zero, weighted, plus]
  · simp [hrel]

/-! ## SelectiveDimSemijoinFilterChain -/

private theorem semijoin_fold_aux (lines : List Line) (orderYear : Nat → Option Int)
    (orderOk partOk : Nat → Bool) (suppNation : Nat → Nat) (targetNation : Nat) (yr : Int)
    (num den : Int) :
    lines.foldl (fun (num, den) l =>
      if !orderOk l.orderKey || !partOk l.partKey then (num, den) else
        match orderYear l.orderKey with
        | some y =>
          if y == yr then
            let addNum := if suppNation l.suppKey == targetNation then l.vol else 0
            (num + addNum, den + l.vol)
          else (num, den)
        | none => (num, den)) (num, den) =
      (num + ((lines.filter fun l =>
          partOk l.partKey && orderOk l.orderKey && orderYear l.orderKey == some yr).map fun l =>
            if suppNation l.suppKey == targetNation then l.vol else 0).sum,
       den + ((lines.filter fun l =>
          partOk l.partKey && orderOk l.orderKey && orderYear l.orderKey == some yr).map Line.vol).sum) := by
  induction lines generalizing num den with
  | nil => simp
  | cons l ls ih =>
      simp only [List.foldl_cons]
      rw [ih]
      by_cases hok : orderOk l.orderKey
      · by_cases hpk : partOk l.partKey
        · cases ho : orderYear l.orderKey with
          | none => simp [hok, hpk, ho]
          | some y =>
              by_cases hy : y == yr
              · have hsome : (some y : Option Int) == some yr := by simp [hy]
                simp [hok, hpk, ho, hy, hsome]
                omega
              · have hsome : ¬((some y : Option Int) == some yr) = true := by
                  intro hh
                  have : y = yr := by simpa using hh
                  exact hy (by simpa using this)
                simp [hok, hpk, ho, hy, hsome]
        · simp [hpk]
      · simp [hok]

theorem SelectiveDimSemijoinFilterChain_sound :
    TinylambOpt.ResearchGoals.QueryRewrites.PivotedAggregatesAndSemijoinFilters.SelectiveDimSemijoinFilterChain.Goal := by
  intro lines orderYear orderOk partOk suppNation targetNation yr
  calc semijoinMarketShare lines orderYear orderOk partOk suppNation targetNation yr
      = (0 + ((lines.filter fun l => partOk l.partKey && orderOk l.orderKey && orderYear l.orderKey == some yr).map fun l => if suppNation l.suppKey == targetNation then l.vol else 0).sum,
         0 + ((lines.filter fun l => partOk l.partKey && orderOk l.orderKey && orderYear l.orderKey == some yr).map Line.vol).sum) :=
        semijoin_fold_aux lines orderYear orderOk partOk suppNation targetNation yr 0 0
    _ = refMarketShare lines orderYear orderOk partOk suppNation targetNation yr := by
        unfold refMarketShare
        dsimp
        simp [Int.zero_add]

/-! ## CteVarianceCoeffSelfJoinPivot -/

private abbrev momDefault : Mom2 × Mom2 := (⟨0, 0, 0⟩, ⟨0, 0, 0⟩)

private def momKeep (m : Nat) (r : Nat × Nat × Int) : Bool :=
  r.2.1 == m || r.2.1 == m + 1

private def momStep (m : Nat) (st : Mom2 × Mom2) (r : Nat × Nat × Int) : Mom2 × Mom2 :=
  if r.2.1 == m then (mom2Add st.1 r.2.2, st.2) else (st.1, mom2Add st.2 r.2.2)

private theorem mom_stepfn_eq (m : Nat) (h : Std.HashMap Nat (Mom2 × Mom2))
    (r : Nat × Nat × Int) :
    (if r.2.1 == m then
       h.insert r.1 (mom2Add (h.getD r.1 momDefault).1 r.2.2, (h.getD r.1 momDefault).2)
     else if r.2.1 == m + 1 then
       h.insert r.1 ((h.getD r.1 momDefault).1, mom2Add (h.getD r.1 momDefault).2 r.2.2)
     else h) =
    if momKeep m r then h.insert r.1 (momStep m (h.getD r.1 momDefault) r) else h := by
  unfold momKeep momStep
  by_cases h1 : r.2.1 == m
  · simp [h1]
  · by_cases h2 : r.2.1 == m + 1
    · simp [h1, h2]
    · simp [h1, h2]

private theorem momFold_eq_genericAux (rows : List (Nat × Nat × Int)) (m : Nat)
    (h0 : Std.HashMap Nat (Mom2 × Mom2)) :
    rows.foldl (fun h r =>
      if r.2.1 == m then
        h.insert r.1 (mom2Add (h.getD r.1 momDefault).1 r.2.2, (h.getD r.1 momDefault).2)
      else if r.2.1 == m + 1 then
        h.insert r.1 ((h.getD r.1 momDefault).1, mom2Add (h.getD r.1 momDefault).2 r.2.2)
      else h) h0 =
    rows.foldl (fun h r => if momKeep m r then h.insert r.1 (momStep m (h.getD r.1 momDefault) r) else h) h0 := by
  induction rows generalizing h0 with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.foldl_cons]
      rw [mom_stepfn_eq]
      exact ih _

private theorem momStep_fold (rs : List (Nat × Nat × Int)) (m : Nat) (a b : Mom2)
    (hall : ∀ r ∈ rs, momKeep m r = true) :
    rs.foldl (momStep m) (a, b) =
      ((rs.filter fun r => r.2.1 == m).foldl (fun acc r => mom2Add acc r.2.2) a,
       (rs.filter fun r => r.2.1 == m + 1).foldl (fun acc r => mom2Add acc r.2.2) b) := by
  induction rs generalizing a b with
  | nil => simp
  | cons r rs ih =>
      have hh : momKeep m r = true := hall r (by simp)
      unfold momKeep at hh
      by_cases h1 : r.2.1 == m
      · have h2f : (r.2.1 == m + 1) = false := by
          apply Bool.eq_false_of_ne_true
          intro hh'
          have hm : r.2.1 = m := by simpa using h1
          have hp : r.2.1 = m + 1 := by simpa using hh'
          omega
        simp [momStep, h1, h2f]
        exact ih (mom2Add a r.2.2) b (fun r hr => hall r (by simp [hr]))
      · have h2 : r.2.1 == m + 1 := by simpa [h1] using hh
        simp [momStep, h1, h2]
        exact ih a (mom2Add b r.2.2) (fun r hr => hall r (by simp [hr]))

private theorem filter_rel_mom (rows : List (Nat × Nat × Int)) (m key : Nat) :
    (rows.filter (fun r => momKeep m r && r.1 == key)).filter (fun r => r.2.1 == m) =
      rows.filter (fun r => r.1 == key && r.2.1 == m) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  rcases r with ⟨k, moy, v⟩
  unfold momKeep
  by_cases h1 : moy == m <;> by_cases h2 : moy == m + 1 <;> by_cases h3 : k == key <;>
    simp [h1, h2, h3, Bool.and_comm]

private theorem filter_rel_momp1 (rows : List (Nat × Nat × Int)) (m key : Nat) :
    (rows.filter (fun r => momKeep m r && r.1 == key)).filter (fun r => r.2.1 == m + 1) =
      rows.filter (fun r => r.1 == key && r.2.1 == m + 1) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  rcases r with ⟨k, moy, v⟩
  unfold momKeep
  by_cases h1 : moy == m <;> by_cases h2 : moy == m + 1 <;> by_cases h3 : k == key <;>
    simp [h1, h2, h3, Bool.and_comm]

theorem CteVarianceCoeffSelfJoinPivot_sound :
    TinylambOpt.ResearchGoals.QueryRewrites.PivotedAggregatesAndSemijoinFilters.CteVarianceCoeffSelfJoinPivot.Goal := by
  intro rows m key
  dsimp [fusedTwoMonthMom, refTwoMonthMom]
  rw [momFold_eq_genericAux rows m ({} : Std.HashMap Nat (Mom2 × Mom2))]
  rw [Std.HashMap.getD_eq_getD_getElem?]
  rw [hmFoldAt (default := momDefault) (key := fun r : Nat × Nat × Int => r.1)
    (keep := momKeep m) (step := momStep m) rows ({} : Std.HashMap Nat (Mom2 × Mom2)) key]
  simp only [Std.HashMap.getElem?_empty]
  let rel := rows.filter (fun r => momKeep m r && r.1 == key)
  have hrel_keep : ∀ r ∈ rel, momKeep m r = true := by
    intro r hr
    dsimp [rel] at hr
    exact (Bool.and_eq_true_iff.mp (List.mem_filter.mp hr).2).1
  change (if rel.length == 0 then none else some (rel.foldl (momStep m) momDefault)).getD momDefault = _
  by_cases hrel : rel.length == 0
  · have hrel_nil : rel = [] := List.length_eq_zero_iff.mp (by simpa using hrel : rel.length = 0)
    have hfy : (rows.filter (fun r => r.1 == key && r.2.1 == m)) = [] := by
      have : (rows.filter (fun r => momKeep m r && r.1 == key)).filter (fun r => r.2.1 == m) = [] := by
        simpa [rel] using congrArg (fun l => l.filter (fun r => r.2.1 == m)) hrel_nil
      rw [← filter_rel_mom rows m key, this]
    have hp : (rows.filter (fun r => r.1 == key && r.2.1 == m + 1)) = [] := by
      have : (rows.filter (fun r => momKeep m r && r.1 == key)).filter (fun r => r.2.1 == m + 1) = [] := by
        simpa [rel] using congrArg (fun l => l.filter (fun r => r.2.1 == m + 1)) hrel_nil
      rw [← filter_rel_momp1 rows m key, this]
    simp [hrel_nil, hfy, hp, momDefault]
  · have hfold := momStep_fold rel m (⟨0,0,0⟩ : Mom2) (⟨0,0,0⟩ : Mom2) hrel_keep
    dsimp [momDefault] at hfold ⊢
    rw [hfold]
    rw [filter_rel_mom, filter_rel_momp1]
    simp [hrel]


/-! ## GroupAvgThresholdTwoPassSplit -/

private def salesByPair (sales : List SaleRev) : Std.HashMap (Nat × Nat) Int :=
  sales.foldl (fun m (s, i, v) => m.insert (s, i) (m.getD (s, i) 0 + v)) {}

private theorem pair_beq_eq_and (a b : Nat × Nat) :
    (a == b) = (a.1 == b.1 && a.2 == b.2) := by
  cases a with
  | mk a1 a2 =>
      cases b with
      | mk b1 b2 => rfl

private theorem salesByPair_get (sales : List SaleRev) (s i : Nat) :
    (salesByPair sales)[(s, i)]? =
      let rel := sales.filter (fun r => r.1 == s && r.2.1 == i)
      if rel.length == 0 then none else some ((rel.map fun r => r.2.2).sum) := by
  dsimp [salesByPair]
  have h := hmFoldAt (α := Nat × Nat) (β := Int) (ρ := SaleRev)
    (default := 0) (key := fun r : SaleRev => (r.1, r.2.1))
    (keep := fun _ : SaleRev => true) (step := fun acc : Int => fun r : SaleRev => acc + r.2.2)
    sales ({} : Std.HashMap (Nat × Nat) Int) (s, i)
  dsimp at h
  rw [h]
  simp only [Std.HashMap.getElem?_empty, ↓reduceIte]
  have hfilt : (sales.filter fun r => true && (r.fst, r.snd.fst) == (s, i)) =
      sales.filter (fun r => r.1 == s && r.2.1 == i) := by
    apply List.filter_congr
    intro r hr
    rw [pair_beq_eq_and]
    simp only [Bool.true_and]
  rw [hfilt]
  rw [List.sum_eq_foldl, foldl_map_fold]

private theorem mem_itemsSingle (sales : List SaleRev) (store i : Nat) (rev : Int) :
    (i, rev) ∈ (salesByPair sales).toList.filterMap (fun p =>
      if p.1.1 == store then some (p.1.2, p.2) else none) ↔
    (salesByPair sales)[(store, i)]? = some rev := by
  constructor
  · intro hm
    rcases List.mem_filterMap.mp hm with ⟨p, hp, hf⟩
    by_cases hs : p.1.1 == store
    · simp [hs] at hf
      have hkey : p.1.1 = store := by simpa using hs
      have hi : p.1.2 = i := hf.1
      have hr : p.2 = rev := hf.2
      have hm2 : (p.1, p.2) ∈ (salesByPair sales).toList := hp
      have hget : (salesByPair sales)[p.1]? = some p.2 :=
        Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hm2
      have hp1 : p.1 = (store, i) := by
        apply Prod.ext
        · exact hkey
        · exact hi
      rw [hp1, hr] at hget
      simpa using hget
    · simp [hs] at hf
  · intro hget
    have hp : ((store, i), rev) ∈ (salesByPair sales).toList :=
      Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hget
    apply List.mem_filterMap.mpr
    refine ⟨((store, i), rev), hp, ?_⟩
    simp

private theorem filterMap_eq_filter_map {α β : Type} (p : α → Bool) (g : α → β) (xs : List α) :
    xs.filterMap (fun a => if p a then some (g a) else none) = (xs.filter p).map g := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : p x <;> simp [h, ih]

private theorem pairwise_map_nodup_of_store (store : Nat)
    (l : List ((Nat × Nat) × Int))
    (hall : ∀ p ∈ l, p.1.1 == store)
    (hpw : l.Pairwise (fun a b => (a.1 == b.1) = false)) :
    (l.map fun p => (p.1.2, p.2)).Nodup := by
  induction l with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons]
      rw [List.nodup_cons]
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨q, hq, hqp⟩
        have hne : (p.1 == q.1) = false := (List.pairwise_cons.mp hpw).1 q hq
        have hps : p.1.1 == store := hall p (by simp)
        have hqs : q.1.1 == store := hall q (by simp [hq])
        have hkey : p.1 = q.1 := by
          apply Prod.ext
          · have hp' : p.1.1 = store := by simpa using hps
            have hq' : q.1.1 = store := by simpa using hqs
            omega
          · exact (congrArg Prod.fst hqp).symm
        have hbeq : (p.1 == q.1) = true := by simpa [hkey]
        rw [hbeq] at hne
        contradiction
      · exact ih (fun q hq => hall q (by simp [hq])) (List.pairwise_cons.mp hpw).2

private theorem nodup_itemsSingle (sales : List SaleRev) (store : Nat) :
    ((salesByPair sales).toList.filterMap fun p =>
      if p.1.1 == store then some (p.1.2, p.2) else none).Nodup := by
  let fl := (salesByPair sales).toList.filter (fun p => p.1.1 == store)
  have hpw := Std.HashMap.distinct_keys_toList (m := salesByPair sales)
  have hpw_fl : fl.Pairwise (fun a b => (a.1 == b.1) = false) := by
    dsimp [fl]
    exact hpw.filter (fun p => p.1.1 == store)
  rw [filterMap_eq_filter_map]
  apply pairwise_map_nodup_of_store store
  · intro p hp
    have hp' : p ∈ (salesByPair sales).toList ∧ p.1.1 == store := by simpa [fl] using hp
    exact hp'.2
  · exact hpw_fl

private def refItemIds (sales : List SaleRev) (store : Nat) : List Nat :=
  ((sales.filter fun r => r.1 == store).map fun r => r.2.1).eraseDups

private def refRevs (sales : List SaleRev) (store : Nat) : List (Nat × Int) :=
  (refItemIds sales store).map fun i =>
    (i, (((sales.filter fun r => r.1 == store).filter fun r => r.2.1 == i).map fun r => r.2.2).sum)

private theorem refRel_eq (sales : List SaleRev) (store i : Nat) :
    (sales.filter fun r => r.1 == store).filter (fun r => r.2.1 == i) =
      sales.filter fun r => r.1 == store && r.2.1 == i := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  simp [Bool.and_comm]

private theorem mem_refRevs (sales : List SaleRev) (store i : Nat) (rev : Int) :
    (i, rev) ∈ refRevs sales store ↔
      let rel := sales.filter (fun r => r.1 == store && r.2.1 == i)
      rel.length ≠ 0 ∧ rev = (rel.map fun r => r.2.2).sum := by
  dsimp [refRevs, refItemIds]
  constructor
  · intro hm
    rcases List.mem_map.mp hm with ⟨j, hj, hji⟩
    have hji_eq : j = i := congrArg Prod.fst hji
    have hjrev : rev = (((sales.filter fun r => r.1 == store).filter fun r => r.2.1 == j).map fun r => r.2.2).sum := by
      simpa using (congrArg Prod.snd hji).symm
    subst j
    rw [refRel_eq] at hjrev
    have hi_ids : i ∈ (((sales.filter fun r => r.1 == store).map fun r => r.2.1).eraseDups) := hj
    have hi_map : i ∈ (sales.filter fun r => r.1 == store).map fun r => r.2.1 :=
      List.mem_eraseDups.mp hi_ids
    rcases List.mem_map.mp hi_map with ⟨r, hr, hri⟩
    have hr' : r ∈ sales.filter (fun x => x.1 == store && x.2.1 == i) := by
      apply List.mem_filter.mpr
      constructor
      · exact (List.mem_filter.mp hr).1
      · have hs : r.1 == store := (List.mem_filter.mp hr).2
        have hi' : r.2.1 == i := by simpa [hri]
        exact Bool.and_eq_true_iff.mpr ⟨hs, hi'⟩
    exact ⟨fun hlen => by
      have hnil : sales.filter (fun x => x.1 == store && x.2.1 == i) = [] :=
        List.length_eq_zero_iff.mp (by simpa using hlen)
      simpa [hnil] using hr', hjrev⟩
  · intro h
    rcases h with ⟨hrel, hrev⟩
    have hpos : 0 < (sales.filter fun r => r.1 == store && r.2.1 == i).length :=
      Nat.pos_of_ne_zero hrel
    rcases List.exists_mem_of_length_pos hpos with ⟨r, hr⟩
    have hi_ids : i ∈ (((sales.filter fun r => r.1 == store).map fun r => r.2.1).eraseDups) := by
      apply List.mem_eraseDups.mpr
      apply List.mem_map.mpr
      refine ⟨r, ?_, ?_⟩
      · exact List.mem_filter.mpr
          ⟨(List.mem_filter.mp hr).1, (Bool.and_eq_true_iff.mp (List.mem_filter.mp hr).2).1⟩
      · have := (Bool.and_eq_true_iff.mp (List.mem_filter.mp hr).2).2
        simpa using this
    apply List.mem_map.mpr
    refine ⟨i, hi_ids, ?_⟩
    apply Prod.ext
    · simp
    · rw [refRel_eq]
      exact hrev.symm

private theorem not_mem_erase_self {α : Type} [DecidableEq α] {a : α} {l : List α}
    (h : l.Nodup) : a ∉ l.erase a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b = a
      · subst b
        rw [List.erase_cons_head]
        exact (List.nodup_cons.mp h).1
      · have hbeq : ¬(b == a) = true := by
          intro hb
          have heq : b = a := by simpa using hb
          exact hba heq
        rw [List.erase_cons_tail hbeq]
        intro hmem
        have hab : a ≠ b := by
          intro heq
          exact hba heq.symm
        have hmem' : a ∈ bs.erase a := by simpa [hab] using hmem
        exact ih (List.nodup_cons.mp h).2 hmem'

private theorem perm_cons_erase {α : Type} [DecidableEq α] (a : α) (l : List α) (h : a ∈ l) :
    (a :: l.erase a).Perm l := by
  induction l with
  | nil => simp at h
  | cons b bs ih =>
      by_cases hba : b = a
      · subst b
        simp
      · have hab : a ≠ b := by
          intro heq
          exact hba heq.symm
        have hb : a ∈ bs := by simpa [hab] using h
        have ih' := ih hb
        simp [hba, hab] at ih' ⊢
        exact (List.Perm.swap a b (bs.erase a)).symm.trans (List.Perm.cons b ih')

private theorem perm_of_nodup_same_mem {α : Type} [DecidableEq α] {l₁ l₂ : List α}
    (h1 : l₁.Nodup) (h2 : l₂.Nodup) (hm : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      cases l₂ with
      | nil => exact List.Perm.nil
      | cons b bs =>
          have : b ∈ ([] : List α) := (hm b).mpr (by simp)
          simp at this
  | cons a as ih =>
      have ha2 : a ∈ l₂ := (hm a).mp (by simp)
      have hperm2 : l₂.Perm (a :: l₂.erase a) := (perm_cons_erase a l₂ ha2).symm
      apply (List.Perm.cons a ?_).trans hperm2.symm
      apply ih (List.nodup_cons.mp h1).2 (h2.erase a)
      intro x
      constructor
      · intro hxas
        have hxl2 : x ∈ l₂ := (hm x).mp (by simp [hxas])
        have hxne : x ≠ a := by
          intro heq
          exact (List.nodup_cons.mp h1).1 (by simpa [heq] using hxas)
        simpa [hxne] using hxl2
      · intro hxerase
        have hxne : x ≠ a := by
          intro heq
          subst x
          exact (not_mem_erase_self h2) hxerase
        have hxl2 : x ∈ l₂ := by simpa [hxne] using hxerase
        have hxcons : x ∈ a :: as := (hm x).mpr hxl2
        simpa [hxne] using hxcons

private theorem items_perm (sales : List SaleRev) (store : Nat) :
    ((salesByPair sales).toList.filterMap fun p =>
      if p.1.1 == store then some (p.1.2, p.2) else none).Perm (refRevs sales store) := by
  apply perm_of_nodup_same_mem (nodup_itemsSingle sales store)
  · dsimp [refRevs, refItemIds]
    exact List.nodup_iff_pairwise_ne.mpr
      (List.Pairwise.map
        (fun i : Nat => (i, (((sales.filter fun r => r.1 == store).filter fun r => r.2.1 == i).map fun r => r.2.2).sum))
        (by
          intro a b hne heq
          exact hne (congrArg Prod.fst heq))
        (List.nodup_iff_pairwise_ne.mp
          (KeyGrouping.nodup_eraseDups ((sales.filter fun r => r.1 == store).map fun r => r.2.1))))
  · intro x
    rcases x with ⟨i, rev⟩
    rw [mem_itemsSingle, mem_refRevs, salesByPair_get]
    dsimp
    constructor
    · intro h
      by_cases hrel : (sales.filter fun r => r.1 == store && r.2.1 == i).length == 0
      · simp [hrel] at h
      · simp [hrel] at h ⊢
        exact ⟨by simpa using hrel, h.symm⟩
    · intro h
      rcases h with ⟨hrel, hrev⟩
      have hrelb : ((sales.filter fun r => r.1 == store && r.2.1 == i).length == 0) = false := by
        apply Bool.eq_false_of_ne_true
        intro hb
        exact hrel (by simpa using hb)
      simp [hrelb, hrev]

private theorem perm_sum_int {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons x p ih => simp [ih]
  | swap x y l => simp [Int.add_comm, Int.add_left_comm, Int.add_assoc]
  | trans p1 p2 ih1 ih2 => exact ih1.trans ih2

theorem GroupAvgThresholdTwoPassSplit_sound :
    TinylambOpt.ResearchGoals.QueryRewrites.PivotedAggregatesAndSemijoinFilters.GroupAvgThresholdTwoPassSplit.Goal := by
  intro sales store
  let itemsA := (salesByPair sales).toList.filterMap fun p =>
    if p.1.1 == store then some (p.1.2, p.2) else none
  let itemsB := refRevs sales store
  have hp : itemsA.Perm itemsB := items_perm sales store
  have hlen : itemsA.length = itemsB.length := List.Perm.length_eq hp
  have hsum : (itemsA.map Prod.snd).sum = (itemsB.map Prod.snd).sum := perm_sum_int (hp.map Prod.snd)
  change (itemsA.filter fun p => 10 * p.2 * (itemsA.length : Int) ≤ (itemsA.map Prod.snd).sum).Perm
    (itemsB.filter fun p => 10 * p.2 * (itemsB.length : Int) ≤ (itemsB.map Prod.snd).sum)
  rw [hlen, hsum]
  exact hp.filter _


end TinylambOpt.ResearchProofs.QueryRewrites.PivotedAggregatesAndSemijoinFilters

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join_test.cpp
-- executor/relational_preaggregation.cpp
-- executor/relational_regression_test_part3.cpp
-- query/select_grouped_rewrites.cpp
-- query/select_rewrites.cpp
-- query/select_rewrites.hpp
-- query/select_rewrites_test.cpp
-- query/select_set_and_join_rewrites.cpp
-- query/sql_engine.cpp
