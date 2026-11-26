import TinylambOpt.ResearchGoals.StarHubMultiwayWeightedProduct

/-!
Proved

- `Goal` — Star-hub 3-way FK-FK weighted-product factorization (STATS-CEB C1, C5 / JOB
  J5 / LDBC L2 / DictionaryJoinsAndLateLabels B3): independent pre-aggregation of
  value branch `B1` and multiplicity branch `B2` keyed by hub FK `k`, combined in a
  single scan over `H`, avoids intermediate FK-FK tuple blowup while preserving exact
  `(COUNT(*), COUNT(B1.v), SUM(B1.v))`.

The reference model is defined in
`TinylambOpt.ResearchGoals.StarHubMultiwayWeightedProduct`.
-/

namespace TinylambOpt.ResearchProofs.StarHubMultiwayWeightedProduct

open TinylambOpt.ResearchGoals.StarHubMultiwayWeightedProduct

private def fOpt (a b : Option Int) : Option Int :=
  match a, b with
  | none, y => y
  | x, none => x
  | some a', some b' => some (a' + b')

private theorem fOpt_assoc (a b c : Option Int) : fOpt (fOpt a b) c = fOpt a (fOpt b c) := by
  cases a <;> cases b <;> cases c <;> simp [fOpt] <;> omega

private theorem fOpt_none_left (a : Option Int) : fOpt none a = a := by
  cases a <;> simp [fOpt]

private theorem fOpt_none_right (a : Option Int) : fOpt a none = a := by
  cases a <;> simp [fOpt]

private theorem foldl_fOpt (xs : List (Option Int)) (acc : Option Int) :
    xs.foldl fOpt acc = fOpt acc (xs.foldl fOpt none) := by
  induction xs generalizing acc with
  | nil => simp [fOpt_none_right]
  | cons x xs ih =>
      change xs.foldl fOpt (fOpt acc x) = fOpt acc (xs.foldl fOpt (fOpt none x))
      rw [ih (fOpt acc x), ih (fOpt none x), fOpt_none_left, fOpt_assoc]

private def sumInt (xs : List (Option Int)) : Int :=
  (xs.filterMap id).sum

private theorem foldl_fOpt_eq (xs : List (Option Int)) (acc : Option Int) :
    xs.foldl fOpt acc =
      match acc with
      | none => if (xs.filter (fun o : Option Int => o.isSome)).length == 0 then none else some (sumInt xs)
      | some a => some (a + sumInt xs) := by
  induction xs generalizing acc with
  | nil =>
      cases acc <;> simp [sumInt]
  | cons x xs ih =>
      cases x with
      | none =>
          change xs.foldl fOpt (fOpt acc none) = _
          rw [fOpt_none_right]
          cases acc with
          | none => simpa [sumInt] using ih none
          | some a => simpa [sumInt] using ih (some a)
      | some v =>
          change xs.foldl fOpt (fOpt acc (some v)) = _
          cases acc with
          | none =>
              rw [fOpt_none_left]
              simpa [sumInt] using ih (some v)
          | some a =>
              rw [show fOpt (some a) (some v) = some (a + v) by simp [fOpt]]
              simpa [sumInt, Int.add_assoc] using ih (some (a + v))

private theorem sumOptList_eq_sumInt (xs : List (Option Int)) :
    sumOptList xs =
      if (xs.filter (fun o : Option Int => o.isSome)).length == 0 then none else some (sumInt xs) := by
  unfold sumOptList
  exact foldl_fOpt_eq xs none

private def addSt (a b : Nat × Nat × Int) : Nat × Nat × Int :=
  (a.1 + b.1, a.2.1 + b.2.1, a.2.2 + b.2.2)

private def zeroSt : Nat × Nat × Int := (0, 0, 0)

private theorem addSt_zero_left (a : Nat × Nat × Int) : addSt zeroSt a = a := by
  rcases a with ⟨a1, ⟨a2, a3⟩⟩
  simp [addSt, zeroSt]

private theorem addSt_assoc (a b c : Nat × Nat × Int) : addSt (addSt a b) c = addSt a (addSt b c) := by
  rcases a with ⟨a1, ⟨a2, a3⟩⟩
  rcases b with ⟨b1, ⟨b2, b3⟩⟩
  rcases c with ⟨c1, ⟨c2, c3⟩⟩
  simp [addSt]
  omega

private def meas (xs : List (Option Int)) : Nat × Nat × Int :=
  (xs.length, (xs.filter (fun o : Option Int => o.isSome)).length, sumInt xs)

private theorem meas_append (xs ys : List (Option Int)) : meas (xs ++ ys) = addSt (meas xs) (meas ys) := by
  simp [meas, sumInt, addSt]

private theorem fold_addSt {α : Type} (xs : List α) (f : α → Nat × Nat × Int) (acc : Nat × Nat × Int) :
    xs.foldl (fun st x => addSt st (f x)) acc = addSt acc (xs.foldl (fun st x => addSt st (f x)) zeroSt) := by
  induction xs generalizing acc with
  | nil => simp [addSt, zeroSt]
  | cons x xs ih =>
      change xs.foldl (fun st x => addSt st (f x)) (addSt acc (f x)) = _
      rw [ih (addSt acc (f x))]
      change addSt (addSt acc (f x)) (xs.foldl (fun st x => addSt st (f x)) zeroSt) =
        addSt acc (xs.foldl (fun st x => addSt st (f x)) (addSt zeroSt (f x)))
      rw [show addSt zeroSt (f x) = f x from addSt_zero_left (f x)]
      rw [ih (f x), addSt_assoc]

private theorem meas_flatMap {α : Type} (xs : List α) (f : α → List (Option Int)) :
    meas (xs.flatMap f) = xs.foldl (fun st x => addSt st (meas (f x))) zeroSt := by
  induction xs with
  | nil => simp [meas, sumInt, addSt, zeroSt]
  | cons x xs ih =>
      change meas (f x ++ xs.flatMap f) =
        xs.foldl (fun st x => addSt st (meas (f x))) (addSt zeroSt (meas (f x)))
      rw [meas_append, ih]
      rw [show addSt zeroSt (meas (f x)) = meas (f x) from addSt_zero_left (meas (f x))]
      rw [fold_addSt xs (fun y => meas (f y)) (meas (f x))]

private def valInt (v : Option Int) : Int :=
  match v with
  | some x => x
  | none => 0

private theorem meas_singleton (x : Option Int) :
    meas [x] = (1, if x.isSome then 1 else 0, valInt x) := by
  cases x <;> simp [meas, sumInt, valInt]

private theorem meas_cons (x : Option Int) (xs : List (Option Int)) :
    meas (x :: xs) = addSt (meas [x]) (meas xs) := by
  cases x <;> simp [meas, sumInt, addSt] <;> omega

private theorem meas_filterMap_key (b2F : List B2Row) (kh : Int) (v : Option Int) :
    meas (b2F.filterMap fun b2 => if b2.k = some kh then some v else none) =
      ( (b2F.filter (fun r : B2Row => r.k == some kh)).length
      , if v.isSome then (b2F.filter (fun r : B2Row => r.k == some kh)).length else 0
      , valInt v * Int.ofNat (b2F.filter (fun r : B2Row => r.k == some kh)).length ) := by
  induction b2F with
  | nil => simp [meas, sumInt, valInt]
  | cons b2 b2F ih =>
      cases b2 with
      | mk k2 pr =>
          by_cases h : k2 = some kh
          · have hb : k2 == some kh := by simp [h]
            rw [show (({ k := k2, pred := pr } : B2Row) :: b2F).filterMap
                    (fun b2 => if b2.k = some kh then some v else none) =
                  v :: (b2F.filterMap fun b2 => if b2.k = some kh then some v else none) by simp [h]]
            rw [meas_cons, meas_singleton, ih]
            cases v with
            | none => simp [valInt, hb, addSt, Nat.add_comm, Int.mul_add]
            | some x => simp [valInt, hb, addSt, Int.mul_add, Nat.add_comm] <;> omega
          · have hb : ¬(k2 == some kh) := by simp [h]
            rw [show (({ k := k2, pred := pr } : B2Row) :: b2F).filterMap
                    (fun b2 => if b2.k = some kh then some v else none) =
                  (b2F.filterMap fun b2 => if b2.k = some kh then some v else none) by simp [h]]
            simpa [hb] using ih

private def innerList (b1 : B1Row) (b2F : List B2Row) (kh : Int) : List (Option Int) :=
  b2F.filterMap fun b2 =>
    match b1.k, b2.k with
    | some k1, some k2 => if kh == k1 && kh == k2 then some b1.v else none
    | _, _ => none

private def stContrib (b1 : B1Row) (b2F : List B2Row) (kh : Int) : Nat × Nat × Int :=
  if b1.k == some kh then
    let c2 := countB2 b2F kh
    (c2, if b1.v.isSome then c2 else 0, valInt b1.v * Int.ofNat c2)
  else (0, 0, 0)

private theorem innerList_meas (b1 : B1Row) (b2F : List B2Row) (kh : Int) :
    meas (innerList b1 b2F kh) = stContrib b1 b2F kh := by
  cases b1 with
  | mk k v p =>
      by_cases hk1 : k == some kh
      · have hkval : k = some kh := by simpa using hk1
        have hfilter : innerList { k := k, v := v, pred := p } b2F kh =
            b2F.filterMap fun b2 => if b2.k == some kh then some v else none := by
          unfold innerList
          rw [hkval]
          have hfun : (fun b2 : B2Row =>
              match (some kh : Option Int), b2.k with
              | some k1, some k2 => if kh == k1 && kh == k2 then some v else none
              | _, _ => none) =
            (fun b2 : B2Row => if b2.k == some kh then some v else none) := by
            funext b2
            cases b2 with
            | mk k2 pr =>
                cases k2 with
                | none => simp
                | some k2 =>
                    by_cases h : k2 = kh
                    · simp [h]
                    · have h' : kh ≠ k2 := by omega
                      simp [h, h']
          rw [hfun]
        rw [hfilter]
        simpa [stContrib, countB2, hk1] using meas_filterMap_key b2F kh v
      · have hk1' : k ≠ some kh := by simpa using hk1
        have hempty : innerList { k := k, v := v, pred := p } b2F kh = [] := by
          cases k with
          | none =>
              induction b2F with
              | nil => rfl
              | cons b2 b2F ih =>
                  cases b2 with
                  | mk k2 pr =>
                      cases k2 with
                      | none => simp [innerList]
                      | some k2 => simp [innerList]
          | some k1 =>
              have hne : k1 ≠ kh := by
                intro h
                apply hk1'
                simp [h]
              induction b2F with
              | nil => rfl
              | cons b2 b2F ih =>
                  cases b2 with
                  | mk k2 pr =>
                      cases k2 with
                      | none => simpa [innerList] using ih
                      | some k2 =>
                          have hne' : kh ≠ k1 := by omega
                          have hcond' : ¬(kh = k1 ∧ kh = k2) := by omega
                          rw [show innerList { k := some k1, v := v, pred := p }
                                ({ k := some k2, pred := pr } :: b2F) kh =
                              innerList { k := some k1, v := v, pred := p } b2F kh by
                                simp [innerList, hcond']]
                          exact ih
        rw [hempty]
        simp [hk1, stContrib, meas, sumInt]

private def stOfSummary (s : B1Summary) : Nat × Nat × Int := (s.cnt, s.nn, s.sum)

private def summaryContribSt (kh : Int) (r : B1Row) : Nat × Nat × Int :=
  if r.k == some kh then
    match r.v with
    | none => (1, 0, 0)
    | some x => (1, 1, x)
  else (0, 0, 0)

private def summaryStep (kh : Int) (st : B1Summary) (r : B1Row) : B1Summary :=
  if r.k == some kh then
    match r.v with
    | none => { st with cnt := st.cnt + 1 }
    | some x => { cnt := st.cnt + 1, nn := st.nn + 1, sum := st.sum + x }
  else st

private theorem summaryStep_st (kh : Int) (st : B1Summary) (r : B1Row) :
    stOfSummary (summaryStep kh st r) = addSt (stOfSummary st) (summaryContribSt kh r) := by
  cases r with
  | mk k v p =>
      cases v <;> by_cases h : k == some kh <;>
        simp [summaryStep, summaryContribSt, stOfSummary, addSt, h]

private theorem summarizeB1_fold_general (b1F : List B1Row) (kh : Int) (acc : B1Summary) :
    stOfSummary (b1F.foldl (summaryStep kh) acc) =
      addSt (stOfSummary acc) (b1F.foldl (fun st r => addSt st (summaryContribSt kh r)) zeroSt) := by
  induction b1F generalizing acc with
  | nil => simp [addSt, zeroSt]
  | cons r rs ih =>
      change stOfSummary (rs.foldl (summaryStep kh) (summaryStep kh acc r)) = _
      rw [ih (summaryStep kh acc r), summaryStep_st]
      change addSt (addSt (stOfSummary acc) (summaryContribSt kh r))
          (rs.foldl (fun st x => addSt st (summaryContribSt kh x)) zeroSt) =
        addSt (stOfSummary acc)
          (rs.foldl (fun st x => addSt st (summaryContribSt kh x)) (addSt zeroSt (summaryContribSt kh r)))
      rw [show addSt zeroSt (summaryContribSt kh r) = summaryContribSt kh r from addSt_zero_left (summaryContribSt kh r)]
      have hf := fold_addSt rs (fun x => summaryContribSt kh x) (summaryContribSt kh r)
      rw [hf, addSt_assoc]

private theorem stOfSummary_summarizeB1 (b1F : List B1Row) (kh : Int) :
    stOfSummary (summarizeB1 b1F kh) =
      b1F.foldl (fun st r => addSt st (summaryContribSt kh r)) zeroSt := by
  unfold summarizeB1
  change stOfSummary (b1F.foldl (summaryStep kh) { cnt := 0, nn := 0, sum := 0 }) = _
  have h := summarizeB1_fold_general b1F kh { cnt := 0, nn := 0, sum := 0 }
  exact h.trans (addSt_zero_left (b1F.foldl (fun st r => addSt st (summaryContribSt kh r)) zeroSt))

private def scaleSt (c : Nat) (st : Nat × Nat × Int) : Nat × Nat × Int :=
  (st.1 * c, st.2.1 * c, st.2.2 * Int.ofNat c)

private theorem scaleSt_add (c : Nat) (a b : Nat × Nat × Int) :
    scaleSt c (addSt a b) = addSt (scaleSt c a) (scaleSt c b) := by
  rcases a with ⟨a1, ⟨a2, a3⟩⟩
  rcases b with ⟨b1, ⟨b2, b3⟩⟩
  simp [scaleSt, addSt, Nat.add_mul, Int.add_mul]

private theorem stContrib_eq (b1 : B1Row) (b2F : List B2Row) (kh : Int) :
    stContrib b1 b2F kh = scaleSt (countB2 b2F kh) (summaryContribSt kh b1) := by
  cases b1 with
  | mk k v p =>
      cases v <;> by_cases h : k == some kh <;>
        simp [stContrib, summaryContribSt, scaleSt, countB2, valInt, h]

private theorem fold_scaleSt (b1F : List B1Row) (kh : Int) (c : Nat) (acc : Nat × Nat × Int) :
    b1F.foldl (fun st r => addSt st (scaleSt c (summaryContribSt kh r))) acc =
      addSt acc (scaleSt c (b1F.foldl (fun st r => addSt st (summaryContribSt kh r)) zeroSt)) := by
  induction b1F generalizing acc with
  | nil => simp [addSt, zeroSt, scaleSt]
  | cons r rs ih =>
      change rs.foldl (fun st r => addSt st (scaleSt c (summaryContribSt kh r)))
          (addSt acc (scaleSt c (summaryContribSt kh r))) = _
      have h1 := ih (addSt acc (scaleSt c (summaryContribSt kh r)))
      rw [h1]
      change _ = addSt acc (scaleSt c
          (rs.foldl (fun st x => addSt st (summaryContribSt kh x))
            (addSt zeroSt (summaryContribSt kh r))))
      rw [show addSt zeroSt (summaryContribSt kh r) = summaryContribSt kh r from addSt_zero_left (summaryContribSt kh r)]
      have hf := fold_addSt rs (fun x => summaryContribSt kh x) (summaryContribSt kh r)
      rw [hf, scaleSt_add, addSt_assoc]

private theorem meas_flatMap_inner (b1F : List B1Row) (b2F : List B2Row) (kh : Int) :
    meas (b1F.flatMap fun b1 => innerList b1 b2F kh) =
      scaleSt (countB2 b2F kh) (stOfSummary (summarizeB1 b1F kh)) := by
  rw [meas_flatMap]
  have hinner : ∀ b1 : B1Row,
      meas (innerList b1 b2F kh) = scaleSt (countB2 b2F kh) (summaryContribSt kh b1) := by
    intro b1
    rw [innerList_meas, stContrib_eq]
  simp only [hinner]
  have hraw := fold_scaleSt b1F kh (countB2 b2F kh) zeroSt
  have h : b1F.foldl (fun st r => addSt st (scaleSt (countB2 b2F kh) (summaryContribSt kh r))) zeroSt =
      scaleSt (countB2 b2F kh) (b1F.foldl (fun st r => addSt st (summaryContribSt kh r)) zeroSt) := by
    rw [hraw]
    exact addSt_zero_left (scaleSt (countB2 b2F kh)
      (b1F.foldl (fun st r => addSt st (summaryContribSt kh r)) zeroSt))
  rw [h, stOfSummary_summarizeB1]

private def triplesFor (h : HRow) (b1F : List B1Row) (b2F : List B2Row) : List (Option Int) :=
  match h.k with
  | none => []
  | some kh => b1F.flatMap fun b1 => innerList b1 b2F kh

private def refTriples (hsF : List HRow) (b1F : List B1Row) (b2F : List B2Row) : List (Option Int) :=
  hsF.flatMap fun h =>
    b1F.flatMap fun b1 =>
      b2F.filterMap fun b2 =>
        match h.k, b1.k, b2.k with
        | some kh, some k1, some k2 => if kh == k1 && kh == k2 then some b1.v else none
        | _, _, _ => none

private theorem refHead_some (b1F : List B1Row) (b2F : List B2Row) (kh g : Int) (p : Bool) :
    (b1F.flatMap fun b1 => b2F.filterMap fun b2 =>
        match (some kh : Option Int), b1.k, b2.k with
        | some kh', some k1, some k2 => if kh' == k1 && kh' == k2 then some b1.v else none
        | _, _, _ => none) =
      triplesFor { k := some kh, g := g, pred := p } b1F b2F := by
  change (b1F.flatMap fun b1 => b2F.filterMap fun b2 =>
      match (some kh : Option Int), b1.k, b2.k with
      | some kh', some k1, some k2 => if kh' == k1 && kh' == k2 then some b1.v else none
      | _, _, _ => none) =
    (b1F.flatMap fun b1 => b2F.filterMap fun b2 =>
      match b1.k, b2.k with
      | some k1, some k2 => if kh == k1 && kh == k2 then some b1.v else none
      | _, _ => none)
  induction b1F with
  | nil => rfl
  | cons b1 b1F ih1 =>
      simp only [List.flatMap_cons]
      have hf : (fun b2 : B2Row =>
          match (some kh : Option Int), b1.k, b2.k with
          | some kh', some k1, some k2 => if kh' == k1 && kh' == k2 then some b1.v else none
          | _, _, _ => none) =
        (fun b2 : B2Row =>
          match b1.k, b2.k with
          | some k1, some k2 => if kh == k1 && kh == k2 then some b1.v else none
          | _, _ => none) := by
        funext b2
        cases b1.k <;> cases b2.k <;> simp
      rw [hf, ih1]

private theorem refTriples_eq (hsF : List HRow) (b1F : List B1Row) (b2F : List B2Row) :
    refTriples hsF b1F b2F = hsF.flatMap fun h => triplesFor h b1F b2F := by
  induction hsF with
  | nil => rfl
  | cons h hs ih =>
      have hhead : (b1F.flatMap fun b1 =>
            b2F.filterMap fun b2 =>
              match h.k, b1.k, b2.k with
              | some kh, some k1, some k2 => if kh == k1 && kh == k2 then some b1.v else none
              | _, _, _ => none) = triplesFor h b1F b2F := by
        cases h with
        | mk k g p =>
            cases k with
            | none =>
                unfold triplesFor
                induction b1F with
                | nil => rfl
                | cons b1 b1F ih1 => simp
            | some kh =>
                dsimp
                exact refHead_some b1F b2F kh g p
      simp only [refTriples, List.flatMap_cons]
      rw [hhead]
      change triplesFor h b1F b2F ++ refTriples hs b1F b2F =
        triplesFor h b1F b2F ++ hs.flatMap (fun h => triplesFor h b1F b2F)
      rw [ih]

private theorem meas_triplesFor (h : HRow) (b1F : List B1Row) (b2F : List B2Row) :
    meas (triplesFor h b1F b2F) =
      match h.k with
      | none => zeroSt
      | some kh => scaleSt (countB2 b2F kh) (stOfSummary (summarizeB1 b1F kh)) := by
  cases h with
  | mk k g p =>
      cases k with
      | none => simp [triplesFor, meas, sumInt, zeroSt]
      | some kh => simp [triplesFor, meas_flatMap_inner]

private def optStep (b1F : List B1Row) (b2F : List B2Row) (acc : Nat × Nat × Int) (h : HRow) : Nat × Nat × Int :=
  match h.k with
  | none => acc
  | some kh =>
      let s1 := summarizeB1 b1F kh
      let c2 := countB2 b2F kh
      ( acc.1 + s1.cnt * c2
      , acc.2.1 + s1.nn * c2
      , acc.2.2 + s1.sum * Int.ofNat c2 )

private theorem optStep_eq (b1F : List B1Row) (b2F : List B2Row) (acc : Nat × Nat × Int) (h : HRow) :
    optStep b1F b2F acc h = addSt acc (meas (triplesFor h b1F b2F)) := by
  cases h with
  | mk k g p =>
      cases k with
      | none => simp [optStep, triplesFor, meas, sumInt, addSt]
      | some kh =>
          simp [optStep, triplesFor, meas_flatMap_inner, scaleSt, stOfSummary, addSt]

private theorem fold_optStep (hsF : List HRow) (b1F : List B1Row) (b2F : List B2Row) (acc : Nat × Nat × Int) :
    hsF.foldl (optStep b1F b2F) acc = addSt acc (meas (hsF.flatMap fun h => triplesFor h b1F b2F)) := by
  induction hsF generalizing acc with
  | nil => simp [meas, sumInt, addSt]
  | cons h hs ih =>
      change hs.foldl (optStep b1F b2F) (optStep b1F b2F acc h) = _
      rw [optStep_eq]
      rw [ih (addSt acc (meas (triplesFor h b1F b2F)))]
      change _ = addSt acc (meas (triplesFor h b1F b2F ++ hs.flatMap fun h => triplesFor h b1F b2F))
      rw [meas_append]
      exact addSt_assoc acc (meas (triplesFor h b1F b2F)) (meas (hs.flatMap fun h => triplesFor h b1F b2F))

theorem sound : TinylambOpt.ResearchGoals.StarHubMultiwayWeightedProduct.Goal := by
  intro hs b1s b2s g
  let hsF := hs.filter (fun h : HRow => h.pred && h.g == g)
  let b1F := b1s.filter B1Row.pred
  let b2F := b2s.filter B2Row.pred
  change
    (let st := hsF.foldl (optStep b1F b2F) zeroSt
     (st.1, st.2.1, if st.2.1 == 0 then none else some st.2.2)) =
    (let triples := refTriples hsF b1F b2F
     (triples.length, (triples.filter (fun o : Option Int => o.isSome)).length, sumOptList triples))
  have hfold : hsF.foldl (optStep b1F b2F) zeroSt = meas (hsF.flatMap fun h => triplesFor h b1F b2F) := by
    have hraw := fold_optStep hsF b1F b2F zeroSt
    rw [hraw]
    exact addSt_zero_left _
  dsimp only
  rw [refTriples_eq]
  rw [hfold]
  simp [meas]
  rw [sumOptList_eq_sumInt]
  rfl

end TinylambOpt.ResearchProofs.StarHubMultiwayWeightedProduct

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.cpp
-- executor/detail/analytics_factorized_join.hpp
