import TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown

namespace TinylambOpt.ResearchProofs.BilateralEagerAggJoinPushdown

open TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown

set_option maxRecDepth 100000
set_option maxHeartbeats 0

private structure Stats where
  cnt : Nat
  nnL : Nat
  sumL : Int
  nnR : Nat
  sumR : Int

private def zeroStats : Stats := ⟨0, 0, 0, 0, 0⟩

private def statsAdd (a b : Stats) : Stats :=
  ⟨a.cnt + b.cnt, a.nnL + b.nnL, a.sumL + b.sumL, a.nnR + b.nnR, a.sumR + b.sumR⟩

private theorem statsAdd_comm (a b : Stats) : statsAdd a b = statsAdd b a := by
  cases a
  cases b
  simp [statsAdd]
  omega

private theorem statsAdd_assoc (a b c : Stats) :
    statsAdd (statsAdd a b) c = statsAdd a (statsAdd b c) := by
  cases a
  cases b
  cases c
  simp [statsAdd]
  omega

private theorem statsAdd_left_comm (a b c : Stats) :
    statsAdd a (statsAdd b c) = statsAdd b (statsAdd a c) := by
  cases a
  cases b
  cases c
  simp [statsAdd]
  omega

private theorem statsAdd_zero (a : Stats) : statsAdd zeroStats a = a := by
  cases a
  simp [statsAdd, zeroStats]

private theorem statsAdd_zero_right (a : Stats) : statsAdd a zeroStats = a := by
  rw [statsAdd_comm, statsAdd_zero]

private def sfold (l : List Stats) (st : Stats) : Stats := l.foldl statsAdd st

private theorem sfold_singleton (a : Stats) (st : Stats) : sfold [a] st = statsAdd st a := by
  simp [sfold]

private theorem sfold_start_add (l : List Stats) (st a : Stats) :
    sfold l (statsAdd st a) = statsAdd a (sfold l st) := by
  induction l generalizing st a with
  | nil => simp [sfold, statsAdd_comm]
  | cons x xs ih =>
      change sfold xs (statsAdd (statsAdd st a) x) = statsAdd a (sfold xs (statsAdd st x))
      rw [ih (statsAdd st a) x, ih st a, ih st x]
      simp [statsAdd_assoc, statsAdd_comm, statsAdd_left_comm]

private theorem sfold_cons_add (a : Stats) (l : List Stats) (st : Stats) :
    sfold (a :: l) st = statsAdd a (sfold l st) := by
  change sfold l (statsAdd st a) = statsAdd a (sfold l st)
  exact sfold_start_add l st a

private theorem sfold_start_add_left (l : List Stats) (st a : Stats) :
    sfold l (statsAdd a st) = statsAdd a (sfold l st) := by
  rw [statsAdd_comm a st]
  exact sfold_start_add l st a

private theorem fold_sfold_start_add (ks : List Int) (g : Int → List Stats) (st a : Stats) :
    ks.foldl (fun acc k => sfold (g k) acc) (statsAdd a st) =
      statsAdd a (ks.foldl (fun acc k => sfold (g k) acc) st) := by
  induction ks generalizing st a with
  | nil => simp [statsAdd_comm]
  | cons k ks ih =>
      change ks.foldl (fun acc k' => sfold (g k') acc) (sfold (g k) (statsAdd a st)) =
        statsAdd a (ks.foldl (fun acc k' => sfold (g k') acc) (sfold (g k) st))
      rw [sfold_start_add_left]
      exact ih (sfold (g k) st) a

private theorem sfold_insert_tail_eq (ks : List Int) (k0 : Int) (g : Int → List Stats)
    (a : Stats) (st : Stats) (h : ∀ k ∈ ks, k ≠ k0) :
    ks.foldl (fun acc k => sfold (if k = k0 then a :: g k else g k) acc) st =
      ks.foldl (fun acc k => sfold (g k) acc) st := by
  induction ks generalizing st with
  | nil => rfl
  | cons k ks ih =>
      have hk : k ≠ k0 := h k (by simp)
      have ih' := ih (st := sfold (g k) st) (fun k' hk' => h k' (by simp [hk']))
      simp [hk, ih']

private theorem sfold_insert_one (ks : List Int) (k0 : Int) (a : Stats)
    (g : Int → List Stats) (st : Stats) (hnodup : ks.Nodup) (hmem : k0 ∈ ks) :
    ks.foldl (fun acc k => sfold (if k = k0 then a :: g k else g k) acc) st =
      statsAdd a (ks.foldl (fun acc k => sfold (g k) acc) st) := by
  induction ks generalizing st with
  | nil => simp at hmem
  | cons k ks ih =>
      simp at hnodup hmem
      rcases hmem with hk0 | hk0
      · subst k0
        simp only [List.foldl_cons, ite_true]
        rw [sfold_cons_add]
        have ht : ks.foldl (fun acc k' => sfold (if k' = k then a :: g k' else g k') acc)
              (statsAdd a (sfold (g k) st)) =
            ks.foldl (fun acc k' => sfold (g k') acc) (statsAdd a (sfold (g k) st)) :=
          sfold_insert_tail_eq ks k g a (statsAdd a (sfold (g k) st))
            (fun k' hk' heq => hnodup.1 (by simpa [heq] using hk'))
        rw [ht]
        exact fold_sfold_start_add ks g (sfold (g k) st) a
      · have hkne : k ≠ k0 := by
          intro hkeq
          subst k0
          exact hnodup.1 hk0
        simp only [List.foldl_cons]
        simp [hkne]
        exact ih (st := sfold (g k) st) hnodup.2 hk0

private theorem beq_int_comm (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hab : a = b := by simpa using h
    simp [hab]
  · intro h
    have hba : b = a := by simpa using h
    simp [hba]

private theorem beq_opt_some_some (a b : Int) : ((some a : Option Int) == some b) = (b == a) := by
  calc
    ((some a : Option Int) == some b) = (a == b) := by simp
    _ = (b == a) := beq_int_comm a b

private theorem mem_addUniqueInt {acc : List Int} {x k : Int} :
    k ∈ addUniqueInt acc x ↔ k = x ∨ k ∈ acc := by
  unfold addUniqueInt
  by_cases hc : acc.contains x
  · rw [ite_eq_left hc]
    have hx : x ∈ acc := List.contains_iff_mem.mp hc
    constructor
    · intro hk
      exact Or.inr hk
    · intro hk
      rcases hk with rfl | hk
      · exact hx
      · exact hk
  · rw [ite_eq_right hc]
    constructor
    · intro hk
      simp at hk
      rcases hk with hkeq | hkin
      · exact Or.inl hkeq
      · exact Or.inr hkin
    · intro hk
      rcases hk with rfl | hk
      · simp
      · simp [hk]

private theorem nodup_addUniqueInt {acc : List Int} (h : acc.Nodup) (x : Int) :
    (addUniqueInt acc x).Nodup := by
  unfold addUniqueInt
  by_cases hc : acc.contains x
  · rw [ite_eq_left hc]
    exact h
  · rw [ite_eq_right hc]
    have hx : x ∉ acc := by
      intro hx'
      exact hc (List.contains_iff_mem.mpr hx')
    simp [hx, h]

private theorem distinctSomeKeysAux_nodup {α : Type} (rows : List α) (getKey : α → Option Int)
    (acc : List Int) (hacc : acc.Nodup) :
    (rows.foldl (fun acc r => match getKey r with | some k => addUniqueInt acc k | none => acc) acc).Nodup := by
  induction rows generalizing acc with
  | nil => exact hacc
  | cons r rows ih =>
      cases hg : getKey r with
      | none =>
          simpa [hg] using ih (acc := acc) hacc
      | some k =>
          simpa [hg] using ih (acc := addUniqueInt acc k) (nodup_addUniqueInt hacc k)

private theorem distinctSomeKeys_nodup {α : Type} (rows : List α) (getKey : α → Option Int) :
    (distinctSomeKeys rows getKey).Nodup := by
  exact distinctSomeKeysAux_nodup rows getKey [] (by simp)

private theorem mem_distinctSomeKeysAux {α : Type} (rows : List α) (getKey : α → Option Int)
    (acc : List Int) {k : Int} :
    k ∈ rows.foldl (fun acc r => match getKey r with | some k => addUniqueInt acc k | none => acc) acc ↔
      k ∈ acc ∨ ∃ r ∈ rows, getKey r = some k := by
  induction rows generalizing acc with
  | nil => simp
  | cons r rows ih =>
      cases hg : getKey r with
      | none =>
          simpa [hg] using ih (acc := acc)
      | some k' =>
          have ih' := ih (acc := addUniqueInt acc k')
          simp [hg] at ih' ⊢
          rw [ih']
          constructor
          · intro h
            rcases h with h | h
            · rcases (mem_addUniqueInt.mp h) with hkeq | hkacc
              · right; left; exact hkeq.symm
              · left; exact hkacc
            · right; right; exact h
          · intro h
            rcases h with hkacc | h
            · left; exact mem_addUniqueInt.mpr (Or.inr hkacc)
            · rcases h with hk'eq | h
              · left; exact mem_addUniqueInt.mpr (Or.inl hk'eq.symm)
              · right; exact h

private theorem mem_distinctSomeKeys {α : Type} (rows : List α) (getKey : α → Option Int) {k : Int} :
    k ∈ distinctSomeKeys rows getKey ↔ ∃ r ∈ rows, getKey r = some k := by
  constructor
  · intro hk
    rcases (mem_distinctSomeKeysAux rows getKey ([] : List Int) (k := k)).mp hk with hnil | hex
    · cases hnil
    · exact hex
  · intro hex
    exact (mem_distinctSomeKeysAux rows getKey ([] : List Int) (k := k)).mpr (Or.inr hex)

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
          have : b = a := by simpa using hb
          exact hba this
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

private theorem sfold_perm {l₁ l₂ : List Stats} (h : l₁.Perm l₂) : ∀ st, sfold l₁ st = sfold l₂ st := by
  induction h with
  | nil => intro st; rfl
  | cons x h ih =>
      intro st
      change sfold _ (statsAdd st x) = sfold _ (statsAdd st x)
      exact ih (statsAdd st x)
  | swap x y l =>
      intro st
      simp [sfold, statsAdd_assoc, statsAdd_comm, statsAdd_left_comm]
  | trans h₁ h₂ ih₁ ih₂ =>
      intro st
      exact (ih₁ st).trans (ih₂ st)

private theorem sfold_eq_add_sfold_zero (l : List Stats) (st : Stats) :
    sfold l st = statsAdd st (sfold l zeroStats) := by
  have h := sfold_start_add_left l zeroStats st
  rw [statsAdd_zero_right] at h
  exact h

private theorem fold_sfold_eq_sfold_map (ks : List Int) (g : Int → List Stats) (st : Stats) :
    ks.foldl (fun acc k => sfold (g k) acc) st =
      sfold (ks.map fun k => sfold (g k) zeroStats) st := by
  induction ks generalizing st with
  | nil => simp [sfold]
  | cons k ks ih =>
      change ks.foldl (fun acc k' => sfold (g k') acc) (sfold (g k) st) =
        sfold (sfold (g k) zeroStats :: (ks.map fun k' => sfold (g k') zeroStats)) st
      rw [sfold_cons_add]
      rw [show sfold (g k) st = statsAdd st (sfold (g k) zeroStats) from
        sfold_eq_add_sfold_zero (g k) st]
      rw [show statsAdd st (sfold (g k) zeroStats) = statsAdd (sfold (g k) zeroStats) st from
        statsAdd_comm st (sfold (g k) zeroStats)]
      rw [show ks.foldl (fun acc k' => sfold (g k') acc) (statsAdd (sfold (g k) zeroStats) st) =
            statsAdd (sfold (g k) zeroStats)
              (ks.foldl (fun acc k' => sfold (g k') acc) st) from
          fold_sfold_start_add ks g st (sfold (g k) zeroStats)]
      rw [ih]

private def nnCount : List (Option Int) → Nat
| [] => 0
| none :: xs => nnCount xs
| some _ :: xs => nnCount xs + 1

private def nnSum : List (Option Int) → Int
| [] => 0
| none :: xs => nnSum xs
| some v :: xs => v + nnSum xs

private theorem nnSum_eq_zero_of_nnCount_eq_zero {xs : List (Option Int)}
    (h : nnCount xs = 0) : nnSum xs = 0 := by
  induction xs with
  | nil => simp [nnSum]
  | cons x xs ih =>
      cases x with
      | none =>
          simpa [nnCount, nnSum] using ih h
      | some v =>
          simp [nnCount] at h

private def sumStep (a v : Option Int) : Option Int :=
  match a, v with
  | none, y => y
  | x, none => x
  | some a, some b => some (a + b)

private theorem sumOptListAux (acc : Option Int) (xs : List (Option Int)) :
    xs.foldl sumStep acc =
      match acc with
      | none => if nnCount xs = 0 then none else some (nnSum xs)
      | some a => if nnCount xs = 0 then some a else some (a + nnSum xs) := by
  induction xs generalizing acc with
  | nil => cases acc <;> simp [nnCount, nnSum]
  | cons x xs ih =>
      cases acc with
      | none =>
          cases x with
          | none =>
              change xs.foldl sumStep none = (if nnCount xs = 0 then none else some (nnSum xs))
              exact ih (acc := none)
          | some v =>
              simp [sumStep, nnCount, nnSum]
              rw [ih (acc := some v)]
              by_cases h : nnCount xs = 0
              · rw [nnSum_eq_zero_of_nnCount_eq_zero h]
                simp [h]
              · simp [h]
      | some a =>
          cases x with
          | none =>
              change xs.foldl sumStep (some a) =
                (if nnCount xs = 0 then some a else some (a + nnSum xs))
              exact ih (acc := some a)
          | some v =>
              simp [sumStep, nnCount, nnSum]
              rw [ih (acc := some (a + v))]
              by_cases h : nnCount xs = 0
              · rw [nnSum_eq_zero_of_nnCount_eq_zero h]
                simp [h]
              · simp [h]
                omega

private theorem sumOptList_char (xs : List (Option Int)) :
    sumOptList xs = if nnCount xs = 0 then none else some (nnSum xs) := by
  change xs.foldl sumStep none = if nnCount xs = 0 then none else some (nnSum xs)
  exact sumOptListAux none xs

private def preStep {α : Type} (getKey : α → Option Int) (getVal : α → Option Int) (k : Int)
    (st : PreAgg) (row : α) : PreAgg :=
  if getKey row == some k then
    match getVal row with
    | none => { st with cnt := st.cnt + 1 }
    | some x => { cnt := st.cnt + 1, nn := st.nn + 1, sum := st.sum + x }
  else st

private theorem preAggRows_eq_fold {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) :
    preAggRows rows getKey getVal k =
      rows.foldl (preStep getKey getVal k) { cnt := 0, nn := 0, sum := 0 } := by
  rfl

private theorem preAggRowsAux_cnt {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (st : PreAgg) :
    (rows.foldl (preStep getKey getVal k) st).cnt =
      st.cnt + (rows.filter (fun r => getKey r == some k)).length := by
  induction rows generalizing st with
  | nil => simp
  | cons r rows ih =>
      by_cases h : getKey r == some k
      · cases hv : getVal r with
        | none =>
            have ih' := ih { st with cnt := st.cnt + 1 }
            simp [preStep, h, hv] at ih' ⊢
            omega
        | some x =>
            have ih' := ih { cnt := st.cnt + 1, nn := st.nn + 1, sum := st.sum + x }
            simp [preStep, h, hv] at ih' ⊢
            omega
      · have ih' := ih st
        simp [preStep, h] at ih' ⊢
        omega

private theorem preAggRowsAux_nn {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (st : PreAgg) :
    (rows.foldl (preStep getKey getVal k) st).nn =
      st.nn + nnCount ((rows.filter (fun r => getKey r == some k)).map getVal) := by
  induction rows generalizing st with
  | nil => simp [nnCount]
  | cons r rows ih =>
      by_cases h : getKey r == some k
      · cases hv : getVal r with
        | none =>
            have ih' := ih { st with cnt := st.cnt + 1 }
            simp [preStep, h, hv, nnCount] at ih' ⊢
            omega
        | some x =>
            have ih' := ih { cnt := st.cnt + 1, nn := st.nn + 1, sum := st.sum + x }
            simp [preStep, h, hv, nnCount] at ih' ⊢
            omega
      · have ih' := ih st
        simp [preStep, h, nnCount] at ih' ⊢
        omega

private theorem preAggRowsAux_sum {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (st : PreAgg) :
    (rows.foldl (preStep getKey getVal k) st).sum =
      st.sum + nnSum ((rows.filter (fun r => getKey r == some k)).map getVal) := by
  induction rows generalizing st with
  | nil => simp [nnSum]
  | cons r rows ih =>
      by_cases h : getKey r == some k
      · cases hv : getVal r with
        | none =>
            have ih' := ih { st with cnt := st.cnt + 1 }
            simp [preStep, h, hv, nnSum] at ih' ⊢
            omega
        | some x =>
            have ih' := ih { cnt := st.cnt + 1, nn := st.nn + 1, sum := st.sum + x }
            simp [preStep, h, hv, nnSum] at ih' ⊢
            omega
      · have ih' := ih st
        simp [preStep, h, nnSum] at ih' ⊢
        omega

private theorem preAggRows_cnt {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) :
    (preAggRows rows getKey getVal k).cnt = (rows.filter (fun r => getKey r == some k)).length := by
  rw [preAggRows_eq_fold]
  simpa using preAggRowsAux_cnt rows getKey getVal k { cnt := 0, nn := 0, sum := 0 }

private theorem preAggRows_nn {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) :
    (preAggRows rows getKey getVal k).nn =
      nnCount ((rows.filter (fun r => getKey r == some k)).map getVal) := by
  rw [preAggRows_eq_fold]
  simpa using preAggRowsAux_nn rows getKey getVal k { cnt := 0, nn := 0, sum := 0 }

private theorem preAggRows_sum {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) :
    (preAggRows rows getKey getVal k).sum =
      nnSum ((rows.filter (fun r => getKey r == some k)).map getVal) := by
  rw [preAggRows_eq_fold]
  simpa using preAggRowsAux_sum rows getKey getVal k { cnt := 0, nn := 0, sum := 0 }

private structure Agg where
  cnt : Nat
  nn : Nat
  sum : Int

private def keyAgg {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) : Agg :=
  let p := preAggRows rows getKey getVal k
  ⟨p.cnt, p.nn, p.sum⟩

private def pairStep (st : Stats) (p : LRow × RRow) : Stats :=
  { cnt := st.cnt + 1
    nnL := match p.1.v with | none => st.nnL | some _ => st.nnL + 1
    sumL := match p.1.v with | none => st.sumL | some v => st.sumL + v
    nnR := match p.2.v with | none => st.nnR | some _ => st.nnR + 1
    sumR := match p.2.v with | none => st.sumR | some v => st.sumR + v }

private def pairStats (ps : List (LRow × RRow)) : Stats := ps.foldl pairStep zeroStats

private theorem pairStats_cnt (ps : List (LRow × RRow)) (st : Stats) :
    (ps.foldl pairStep st).cnt = st.cnt + ps.length := by
  induction ps generalizing st with
  | nil => simp
  | cons p ps ih =>
      change (ps.foldl pairStep (pairStep st p)).cnt = st.cnt + (p :: ps).length
      have ih' := ih (pairStep st p)
      simp [pairStep] at ih' ⊢
      omega

private theorem pairStats_nnL (ps : List (LRow × RRow)) (st : Stats) :
    (ps.foldl pairStep st).nnL = st.nnL + nnCount (ps.map fun p => p.1.v) := by
  induction ps generalizing st with
  | nil => simp [nnCount]
  | cons p ps ih =>
      change (ps.foldl pairStep (pairStep st p)).nnL =
        st.nnL + nnCount (p.1.v :: (ps.map fun p => p.1.v))
      have ih' := ih (pairStep st p)
      cases hv : p.1.v with
      | none =>
          simp [pairStep, hv, nnCount] at ih' ⊢
          omega
      | some v =>
          simp [pairStep, hv, nnCount] at ih' ⊢
          omega

private theorem pairStats_sumL (ps : List (LRow × RRow)) (st : Stats) :
    (ps.foldl pairStep st).sumL = st.sumL + nnSum (ps.map fun p => p.1.v) := by
  induction ps generalizing st with
  | nil => simp [nnSum]
  | cons p ps ih =>
      change (ps.foldl pairStep (pairStep st p)).sumL =
        st.sumL + nnSum (p.1.v :: (ps.map fun p => p.1.v))
      have ih' := ih (pairStep st p)
      cases hv : p.1.v with
      | none =>
          simp [pairStep, hv, nnSum] at ih' ⊢
          omega
      | some v =>
          simp [pairStep, hv, nnSum] at ih' ⊢
          omega

private theorem pairStats_nnR (ps : List (LRow × RRow)) (st : Stats) :
    (ps.foldl pairStep st).nnR = st.nnR + nnCount (ps.map fun p => p.2.v) := by
  induction ps generalizing st with
  | nil => simp [nnCount]
  | cons p ps ih =>
      change (ps.foldl pairStep (pairStep st p)).nnR =
        st.nnR + nnCount (p.2.v :: (ps.map fun p => p.2.v))
      have ih' := ih (pairStep st p)
      cases hv : p.2.v with
      | none =>
          simp [pairStep, hv, nnCount] at ih' ⊢
          omega
      | some v =>
          simp [pairStep, hv, nnCount] at ih' ⊢
          omega

private theorem pairStats_sumR (ps : List (LRow × RRow)) (st : Stats) :
    (ps.foldl pairStep st).sumR = st.sumR + nnSum (ps.map fun p => p.2.v) := by
  induction ps generalizing st with
  | nil => simp [nnSum]
  | cons p ps ih =>
      change (ps.foldl pairStep (pairStep st p)).sumR =
        st.sumR + nnSum (p.2.v :: (ps.map fun p => p.2.v))
      have ih' := ih (pairStep st p)
      cases hv : p.2.v with
      | none =>
          simp [pairStep, hv, nnSum] at ih' ⊢
          omega
      | some v =>
          simp [pairStep, hv, nnSum] at ih' ⊢
          omega

private theorem refGroup_char (ls : List LRow) (rs : List RRow) (g : Int) :
    refGroup ls rs g =
      let st := pairStats (refJoinedPairs ls rs g)
      (st.cnt, if st.nnL == 0 then none else some st.sumL,
        if st.nnR == 0 then none else some st.sumR) := by
  unfold refGroup
  dsimp only
  let ps := refJoinedPairs ls rs g
  change (ps.length, sumOptList (ps.map fun p => p.1.v),
      sumOptList (ps.map fun p => p.2.v)) =
      let st := pairStats ps
      (st.cnt, if st.nnL == 0 then none else some st.sumL,
        if st.nnR == 0 then none else some st.sumR)
  rw [sumOptList_char (ps.map fun p => p.1.v)]
  rw [sumOptList_char (ps.map fun p => p.2.v)]
  have hc : ps.length = (pairStats ps).cnt := by
    simpa [pairStats, zeroStats] using (pairStats_cnt ps zeroStats).symm
  have hnl : nnCount (ps.map fun p => p.1.v) = (pairStats ps).nnL := by
    simpa [pairStats, zeroStats] using (pairStats_nnL ps zeroStats).symm
  have hsl : nnSum (ps.map fun p => p.1.v) = (pairStats ps).sumL := by
    simpa [pairStats, zeroStats] using (pairStats_sumL ps zeroStats).symm
  have hnr : nnCount (ps.map fun p => p.2.v) = (pairStats ps).nnR := by
    simpa [pairStats, zeroStats] using (pairStats_nnR ps zeroStats).symm
  have hsr : nnSum (ps.map fun p => p.2.v) = (pairStats ps).sumR := by
    simpa [pairStats, zeroStats] using (pairStats_sumR ps zeroStats).symm
  rw [hc, hnl, hsl, hnr, hsr]
  by_cases hl : (pairStats ps).nnL = 0 <;> by_cases hr : (pairStats ps).nnR = 0 <;> simp [hl, hr]

private theorem beq_eq_false_of_ne {a b : Int} (h : a ≠ b) : (a == b) = false := by
  apply Bool.eq_false_of_ne_true
  intro hb
  have heq : a = b := by simpa using hb
  exact h heq

private theorem beq_eq_true_of_eq {a b : Int} (h : a = b) : (a == b) = true := by
  subst h
  simp

private theorem stats_ext {a b : Stats} (hc : a.cnt = b.cnt) (hnl : a.nnL = b.nnL)
    (hsl : a.sumL = b.sumL) (hnr : a.nnR = b.nnR) (hsr : a.sumR = b.sumR) : a = b := by
  cases a
  cases b
  simp at hc hnl hsl hnr hsr ⊢
  omega

private theorem nnCount_map_const_none {α : Type} (xs : List α) :
    nnCount (xs.map fun _ => (none : Option Int)) = 0 := by
  induction xs with
  | nil => simp [nnCount]
  | cons x xs ih => simp [nnCount, ih]

private theorem nnSum_map_const_none {α : Type} (xs : List α) :
    nnSum (xs.map fun _ => (none : Option Int)) = 0 := by
  induction xs with
  | nil => simp [nnSum]
  | cons x xs ih => simp [nnSum, ih]

private theorem nnCount_map_const_some {α : Type} (xs : List α) (v : Int) :
    nnCount (xs.map fun _ => some v) = xs.length := by
  induction xs with
  | nil => simp [nnCount]
  | cons x xs ih => simp [nnCount, ih]

private theorem nnSum_map_const_some {α : Type} (xs : List α) (v : Int) :
    nnSum (xs.map fun _ => some v) = v * Int.ofNat xs.length := by
  induction xs with
  | nil => simp [nnSum]
  | cons x xs ih =>
      simp only [List.map_cons, nnSum, List.length_cons]
      rw [ih]
      simp [Int.mul_add, Int.add_comm, Int.add_left_comm, Int.add_assoc]

private theorem fold_filterStats_start_add {α : Type} (rows : List α) (getKey : α → Option Int)
    (f : α → Stats) (st a : Stats) :
    rows.foldl (fun acc r => match getKey r with | some _ => statsAdd acc (f r) | none => acc)
        (statsAdd a st) =
      statsAdd a (rows.foldl (fun acc r => match getKey r with | some _ => statsAdd acc (f r) | none => acc) st) := by
  induction rows generalizing st with
  | nil => simp [statsAdd_comm]
  | cons r rows ih =>
      simp only [List.foldl_cons]
      cases hg : getKey r with
      | none =>
          change rows.foldl (fun acc r' => match getKey r' with | some _ => statsAdd acc (f r') | none => acc)
              (statsAdd a st) =
            statsAdd a (rows.foldl (fun acc r' => match getKey r' with | some _ => statsAdd acc (f r') | none => acc) st)
          exact ih st
      | some k =>
          change rows.foldl (fun acc r' => match getKey r' with | some _ => statsAdd acc (f r') | none => acc)
              (statsAdd (statsAdd a st) (f r)) =
            statsAdd a (rows.foldl (fun acc r' => match getKey r' with | some _ => statsAdd acc (f r') | none => acc)
              (statsAdd st (f r)))
          have hcomm : statsAdd (statsAdd a st) (f r) = statsAdd a (statsAdd st (f r)) := by
            cases a
            cases st
            cases f r
            simp [statsAdd]
            omega
          rw [hcomm]
          exact ih (statsAdd st (f r))

private def contrib (rs : List RRow) (l : LRow) : Stats :=
  match l.k with
  | none => zeroStats
  | some k =>
      let rc := keyAgg rs RRow.k RRow.v k
      { cnt := rc.cnt
        nnL := match l.v with | none => 0 | some _ => rc.cnt
        sumL := match l.v with | none => 0 | some v => v * Int.ofNat rc.cnt
        nnR := rc.nn
        sumR := rc.sum }

private def pairsOf (rs : List RRow) (l : LRow) : List (LRow × RRow) :=
  rs.filterMap fun r =>
    match l.k, r.k with
    | some lk, some rk => if lk == rk then some (l, r) else none
    | _, _ => none

private theorem refJoinedPairs_eq_flatMap (ls : List LRow) (rs : List RRow) (g : Int) :
    refJoinedPairs ls rs g = (ls.filter (fun l => l.g == g)).flatMap (pairsOf rs) := by
  rfl

private theorem filterMap_none_eq_nil {α β : Type} (xs : List α) :
    xs.filterMap (fun _ => (none : Option β)) = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem filterMap_eq_filter_map {α β : Type} (p : α → Bool) (y : α → β) (xs : List α) :
    xs.filterMap (fun a => if p a then some (y a) else none) = (xs.filter p).map y := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h, ih]
      · have hf : p x = false := Bool.eq_false_of_ne_true h
        simp [hf, ih]

private theorem emit_pair_some {l : LRow} {k : Int} (hl : l.k = some k) (r : RRow) :
    (match l.k, r.k with
     | some lk, some rk => if lk == rk then some (l, r) else none
     | _, _ => none) =
      if r.k == some k then some (l, r) else none := by
  rw [hl]
  cases hr : r.k with
  | none => simp
  | some rk =>
      rw [beq_opt_some_some rk k]

private theorem pairsOf_eq_filter {rs : List RRow} {l : LRow} {k : Int} (hl : l.k = some k) :
    pairsOf rs l = (rs.filter (fun r => r.k == some k)).map (fun r => (l, r)) := by
  unfold pairsOf
  rw [hl]
  have hem : (fun r : RRow =>
      match some k, r.k with
      | some lk, some rk => if lk == rk then some (l, r) else none
      | _, _ => none) =
      (fun r : RRow => if r.k == some k then some (l, r) else none) := by
    funext r
    simpa [hl] using (emit_pair_some hl r)
  rw [hem]
  exact filterMap_eq_filter_map (fun r : RRow => r.k == some k) (fun r => (l, r)) rs

private theorem map_pair_snd_v (xs : List RRow) (l : LRow) :
    (xs.map fun r => (l, r)).map (fun p => p.2.v) = xs.map fun r => r.v := by
  induction xs with
  | nil => rfl
  | cons r rs ih => simp [ih]

private theorem map_pair_fst_v (xs : List RRow) (l : LRow) :
    (xs.map fun r => (l, r)).map (fun p => p.1.v) = xs.map fun _ => l.v := by
  induction xs with
  | nil => rfl
  | cons r rs ih => simp [ih]

private theorem stats_pairsOf (rs : List RRow) (l : LRow) :
    pairStats (pairsOf rs l) = contrib rs l := by
  cases hl : l.k with
  | none =>
      have hp : pairsOf rs l = [] := by
        unfold pairsOf
        rw [hl]
        exact filterMap_none_eq_nil rs
      rw [hp]
      simp [contrib, hl, zeroStats, pairStats]
  | some k =>
      rw [pairsOf_eq_filter hl]
      let rsK := rs.filter (fun r => r.k == some k)
      have hcnt : (pairStats (rsK.map fun r => (l, r))).cnt =
          (preAggRows rs RRow.k RRow.v k).cnt := by
        simpa [pairStats, zeroStats, rsK, preAggRows_cnt] using
          (pairStats_cnt (rsK.map fun r => (l, r)) zeroStats)
      have hnnR : (pairStats (rsK.map fun r => (l, r))).nnR =
          (preAggRows rs RRow.k RRow.v k).nn := by
        have h := pairStats_nnR (rsK.map fun r => (l, r)) zeroStats
        rw [map_pair_snd_v rsK l] at h
        simpa [pairStats, zeroStats, rsK, preAggRows_nn] using h
      have hsumR : (pairStats (rsK.map fun r => (l, r))).sumR =
          (preAggRows rs RRow.k RRow.v k).sum := by
        have h := pairStats_sumR (rsK.map fun r => (l, r)) zeroStats
        rw [map_pair_snd_v rsK l] at h
        simpa [pairStats, zeroStats, rsK, preAggRows_sum] using h
      cases hv : l.v with
      | none =>
          have hnnL : (pairStats (rsK.map fun r => (l, r))).nnL = 0 := by
            have h := pairStats_nnL (rsK.map fun r => (l, r)) zeroStats
            rw [map_pair_fst_v rsK l] at h
            simpa [pairStats, zeroStats, hv, nnCount_map_const_none] using h
          have hsumL : (pairStats (rsK.map fun r => (l, r))).sumL = 0 := by
            have h := pairStats_sumL (rsK.map fun r => (l, r)) zeroStats
            rw [map_pair_fst_v rsK l] at h
            simpa [pairStats, zeroStats, hv, nnSum_map_const_none] using h
          apply stats_ext
          · simpa [contrib, hl, hv, keyAgg] using hcnt
          · simpa [contrib, hl, hv, keyAgg] using hnnL
          · simpa [contrib, hl, hv, keyAgg] using hsumL
          · simpa [contrib, hl, hv, keyAgg] using hnnR
          · simpa [contrib, hl, hv, keyAgg] using hsumR
      | some v =>
          have hnnL : (pairStats (rsK.map fun r => (l, r))).nnL = rsK.length := by
            have h := pairStats_nnL (rsK.map fun r => (l, r)) zeroStats
            rw [map_pair_fst_v rsK l] at h
            simpa [pairStats, zeroStats, hv, nnCount_map_const_some] using h
          have hsumL : (pairStats (rsK.map fun r => (l, r))).sumL =
              v * Int.ofNat rsK.length := by
            have h := pairStats_sumL (rsK.map fun r => (l, r)) zeroStats
            rw [map_pair_fst_v rsK l] at h
            simpa [pairStats, zeroStats, hv, nnSum_map_const_some] using h
          have hcntlen : (preAggRows rs RRow.k RRow.v k).cnt = rsK.length := by
            simp [rsK, preAggRows_cnt]
          apply stats_ext
          · simpa [contrib, hl, hv, keyAgg] using hcnt
          · simpa [contrib, hl, hv, keyAgg, hcntlen] using hnnL
          · simpa [contrib, hl, hv, keyAgg, hcntlen] using hsumL
          · simpa [contrib, hl, hv, keyAgg] using hnnR
          · simpa [contrib, hl, hv, keyAgg] using hsumR

private theorem preAggRows_cnt_cons_false {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (r : α) (h : (getKey r == some k) = false) :
    (preAggRows (r :: rows) getKey getVal k).cnt = (preAggRows rows getKey getVal k).cnt := by
  rw [preAggRows_cnt, preAggRows_cnt]
  simp [h]

private theorem preAggRows_nn_cons_false {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (r : α) (h : (getKey r == some k) = false) :
    (preAggRows (r :: rows) getKey getVal k).nn = (preAggRows rows getKey getVal k).nn := by
  rw [preAggRows_nn, preAggRows_nn]
  simp [h]

private theorem preAggRows_sum_cons_false {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (r : α) (h : (getKey r == some k) = false) :
    (preAggRows (r :: rows) getKey getVal k).sum = (preAggRows rows getKey getVal k).sum := by
  rw [preAggRows_sum, preAggRows_sum]
  simp [h]

private theorem preAggRows_cnt_cons_some {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (r : α) (h : getKey r == some k) :
    (preAggRows (r :: rows) getKey getVal k).cnt = (preAggRows rows getKey getVal k).cnt + 1 := by
  rw [preAggRows_cnt, preAggRows_cnt]
  simp [h]

private theorem preAggRows_nn_cons_none {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (r : α) (h : getKey r == some k)
    (hv : getVal r = none) :
    (preAggRows (r :: rows) getKey getVal k).nn = (preAggRows rows getKey getVal k).nn := by
  rw [preAggRows_nn, preAggRows_nn]
  simp [h, hv, nnCount]

private theorem preAggRows_nn_cons_some {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (r : α) (h : getKey r == some k) (v : Int)
    (hv : getVal r = some v) :
    (preAggRows (r :: rows) getKey getVal k).nn = (preAggRows rows getKey getVal k).nn + 1 := by
  rw [preAggRows_nn, preAggRows_nn]
  simp [h, hv, nnCount]

private theorem preAggRows_sum_cons_none {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (r : α) (h : getKey r == some k)
    (hv : getVal r = none) :
    (preAggRows (r :: rows) getKey getVal k).sum = (preAggRows rows getKey getVal k).sum := by
  rw [preAggRows_sum, preAggRows_sum]
  simp [h, hv, nnSum]

private theorem preAggRows_sum_cons_some {α : Type} (rows : List α) (getKey : α → Option Int)
    (getVal : α → Option Int) (k : Int) (r : α) (h : getKey r == some k) (v : Int)
    (hv : getVal r = some v) :
    (preAggRows (r :: rows) getKey getVal k).sum = (preAggRows rows getKey getVal k).sum + v := by
  rw [preAggRows_sum, preAggRows_sum]
  simp [h, hv, nnSum]
  omega

private def perKeyStats (lsG : List LRow) (rs : List RRow) (k : Int) : Stats :=
  let pL := preAggRows lsG LRow.k LRow.v k
  let pR := preAggRows rs RRow.k RRow.v k
  ⟨ pL.cnt * pR.cnt
  , pL.nn * pR.cnt
  , pL.sum * Int.ofNat pR.cnt
  , pL.cnt * pR.nn
  , Int.ofNat pL.cnt * pR.sum ⟩

private theorem perKeyStats_cons_some (l : LRow) (lsG : List LRow) (rs : List RRow) (k : Int)
    (hl : l.k = some k) :
    perKeyStats (l :: lsG) rs k = statsAdd (perKeyStats lsG rs k) (contrib rs l) := by
  have hmatch : LRow.k l == some k := by simpa [hl]
  cases hv : l.v with
  | none =>
      have hc := preAggRows_cnt_cons_some lsG LRow.k LRow.v k l hmatch
      have hn := preAggRows_nn_cons_none lsG LRow.k LRow.v k l hmatch hv
      have hs := preAggRows_sum_cons_none lsG LRow.k LRow.v k l hmatch hv
      apply stats_ext <;>
        simp [perKeyStats, statsAdd, contrib, hl, hv, keyAgg, hc, hn, hs,
          Nat.add_mul, Int.add_mul, Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]
  | some v =>
      have hc := preAggRows_cnt_cons_some lsG LRow.k LRow.v k l hmatch
      have hn := preAggRows_nn_cons_some lsG LRow.k LRow.v k l hmatch v hv
      have hs := preAggRows_sum_cons_some lsG LRow.k LRow.v k l hmatch v hv
      apply stats_ext <;>
        simp [perKeyStats, statsAdd, contrib, hl, hv, keyAgg, hc, hn, hs,
          Nat.add_mul, Int.add_mul, Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]

private theorem perKeyStats_cons_other (l : LRow) (lsG : List LRow) (rs : List RRow) (k : Int)
    (h : (l.k == some k) = false) :
    perKeyStats (l :: lsG) rs k = perKeyStats lsG rs k := by
  have hc := preAggRows_cnt_cons_false lsG LRow.k LRow.v k l h
  have hn := preAggRows_nn_cons_false lsG LRow.k LRow.v k l h
  have hs := preAggRows_sum_cons_false lsG LRow.k LRow.v k l h
  apply stats_ext <;> simp [perKeyStats, hc, hn, hs]

private theorem perKeyStatsAux (lsG : List LRow) (rs : List RRow) (k : Int) (st : Stats) :
    sfold ((lsG.filter (fun l => l.k == some k)).map (contrib rs)) st =
      statsAdd st (perKeyStats lsG rs k) := by
  induction lsG generalizing st with
  | nil =>
      simp [sfold, perKeyStats, preAggRows_eq_fold]
      change st = statsAdd st zeroStats
      rw [statsAdd_zero_right]
  | cons l lsG ih =>
      cases hl : l.k with
      | none =>
          have hcond : (l.k == some k) = false := by simp [hl]
          simp [hcond] at ih ⊢
          rw [ih]
          rw [perKeyStats_cons_other l lsG rs k hcond]
      | some k' =>
          by_cases hkeq : k' = k
          · subst k'
            have hcond : (l.k == some k) = true := by simpa [hl]
            simp [hcond]
            rw [sfold_cons_add]
            rw [show statsAdd (contrib rs l)
                  (sfold ((lsG.filter (fun l' => l'.k == some k)).map (contrib rs)) st) =
                sfold ((lsG.filter (fun l' => l'.k == some k)).map (contrib rs))
                  (statsAdd st (contrib rs l)) from by
              rw [statsAdd_comm st (contrib rs l)]
              exact (sfold_start_add_left _ st (contrib rs l)).symm]
            rw [ih]
            rw [perKeyStats_cons_some l lsG rs k hl]
            simp [statsAdd_assoc, statsAdd_comm, statsAdd_left_comm]
          · have hcond : (l.k == some k) = false := by
              simpa [hl, beq_opt_some_some, beq_eq_false_of_ne hkeq]
            simp [hcond] at ih ⊢
            rw [ih]
            rw [perKeyStats_cons_other l lsG rs k hcond]

private theorem perKeyStats_eq_sfold (lsG : List LRow) (rs : List RRow) (k : Int) :
    perKeyStats lsG rs k = sfold ((lsG.filter (fun l => l.k == some k)).map (contrib rs)) zeroStats := by
  have h := perKeyStatsAux lsG rs k zeroStats
  simpa [statsAdd_zero] using h.symm

private theorem filter_cons_map_of_key {α : Type} (rows : List α) (getKey : α → Option Int)
    (f : α → Stats) (r : α) {k0 : Int} (hg : getKey r = some k0) (k : Int) :
    ((r :: rows).filter (fun r' => getKey r' == some k)).map f =
      if k = k0 then f r :: (rows.filter (fun r' => getKey r' == some k)).map f
      else (rows.filter (fun r' => getKey r' == some k)).map f := by
  by_cases hkeq : k = k0
  · subst k0
    simp [hg, beq_opt_some_some]
  · have hkeq' : k0 ≠ k := by
      intro heq
      exact hkeq heq.symm
    simp [hg, hkeq, hkeq']

private theorem filter_eq_nil_of_no_key {α : Type} (rows : List α) (getKey : α → Option Int)
    (k : Int) (hno : ∀ r ∈ rows, getKey r ≠ some k) :
    rows.filter (fun r => getKey r == some k) = [] := by
  induction rows with
  | nil => rfl
  | cons a as ih =>
      have hane : getKey a ≠ some k := hno a (by simp)
      have hcond : (getKey a == some k) = false := by
        apply Bool.eq_false_of_ne_true
        intro hb
        have heq : getKey a = some k := by simpa using hb
        exact hane heq
      have ih' := ih (fun r' hr' => hno r' (List.mem_cons_of_mem a hr'))
      simp [hcond, ih']

private theorem distinctSomeKeys_cons_perm {α : Type} [DecidableEq α]
    (rows : List α) (getKey : α → Option Int) {k0 : Int} (r : α) (hg : getKey r = some k0) :
    (distinctSomeKeys (r :: rows) getKey).Perm
      (if k0 ∈ distinctSomeKeys rows getKey then distinctSomeKeys rows getKey
       else k0 :: distinctSomeKeys rows getKey) := by
  by_cases hmem : k0 ∈ distinctSomeKeys rows getKey
  · simp [hmem]
    apply perm_of_nodup_same_mem
    · exact distinctSomeKeys_nodup (r :: rows) getKey
    · exact distinctSomeKeys_nodup rows getKey
    · intro k
      constructor
      · intro hk
        rcases (mem_distinctSomeKeys (r :: rows) getKey).mp hk with ⟨r', hr', hgr'⟩
        simp at hr'
        rcases hr' with hhead | htail
        · subst r'
          have hkeq : k0 = k := by simpa [hg] using hgr'
          simpa [hkeq] using hmem
        · exact (mem_distinctSomeKeys rows getKey).mpr ⟨r', htail, hgr'⟩
      · intro hk
        rcases (mem_distinctSomeKeys rows getKey).mp hk with ⟨r', hr', hgr'⟩
        exact (mem_distinctSomeKeys (r :: rows) getKey).mpr ⟨r', by simp [hr'], hgr'⟩
  · simp [hmem]
    apply perm_of_nodup_same_mem
    · exact distinctSomeKeys_nodup (r :: rows) getKey
    · exact List.nodup_cons.mpr ⟨hmem, distinctSomeKeys_nodup rows getKey⟩
    · intro k
      constructor
      · intro hk
        rcases (mem_distinctSomeKeys (r :: rows) getKey).mp hk with ⟨r', hr', hgr'⟩
        simp at hr'
        rcases hr' with hhead | htail
        · subst r'
          have hkeq : k0 = k := by simpa [hg] using hgr'
          simp [hkeq]
        · have hkin : k ∈ distinctSomeKeys rows getKey :=
            (mem_distinctSomeKeys rows getKey).mpr ⟨r', htail, hgr'⟩
          simp [hkin]
      · intro hk
        simp at hk
        rcases hk with hkeq | hkin
        · subst k
          exact (mem_distinctSomeKeys (r :: rows) getKey).mpr ⟨r, by simp, hg⟩
        · rcases (mem_distinctSomeKeys rows getKey).mp hkin with ⟨r', hr', hgr'⟩
          exact (mem_distinctSomeKeys (r :: rows) getKey).mpr ⟨r', by simp [hr'], hgr'⟩

private theorem keyGroupFold {α : Type} [DecidableEq α] (rows : List α) (getKey : α → Option Int)
    (f : α → Stats) (st : Stats) :
    (distinctSomeKeys rows getKey).foldl
        (fun acc k => sfold ((rows.filter (fun r => getKey r == some k)).map f) acc) st =
      rows.foldl (fun acc r => match getKey r with | some _ => statsAdd acc (f r) | none => acc) st := by
  induction rows generalizing st with
  | nil => rfl
  | cons r rows ih =>
      cases hg : getKey r with
      | none =>
          simpa [distinctSomeKeys, hg] using ih (st := st)
      | some k0 =>
          let g : Int → List Stats := fun k => (rows.filter (fun r' => getKey r' == some k)).map f
          let g' : Int → List Stats := fun k => ((r :: rows).filter (fun r' => getKey r' == some k)).map f
          have hgmap : ∀ k, ((r :: rows).filter (fun r' => getKey r' == some k)).map f =
              if k = k0 then f r :: g k else g k := by
            intro k
            exact filter_cons_map_of_key rows getKey f r hg k
          have hdskperm0 := distinctSomeKeys_cons_perm rows getKey r hg
          by_cases hmem : k0 ∈ distinctSomeKeys rows getKey
          · have hdskperm : (distinctSomeKeys (r :: rows) getKey).Perm
                (distinctSomeKeys rows getKey) := by simpa [hmem] using hdskperm0
            rw [fold_sfold_eq_sfold_map]
            rw [sfold_perm (hdskperm.map (fun k => sfold (g' k) zeroStats)) st]
            rw [← fold_sfold_eq_sfold_map]
            simp only [g', hgmap]
            rw [sfold_insert_one (distinctSomeKeys rows getKey) k0 (f r) g st
              (distinctSomeKeys_nodup rows getKey) hmem]
            rw [ih]
            simp only [List.foldl_cons]
            simp [hg]
            simpa [statsAdd_comm] using (fold_filterStats_start_add rows getKey f st (f r)).symm
          · have hdskperm : (distinctSomeKeys (r :: rows) getKey).Perm
                (k0 :: distinctSomeKeys rows getKey) := by simpa [hmem] using hdskperm0
            have hno : ∀ r' ∈ rows, getKey r' ≠ some k0 := by
              intro r' hr' heq
              exact hmem ((mem_distinctSomeKeys rows getKey).mpr ⟨r', hr', heq⟩)
            have hfilternil : rows.filter (fun r' => getKey r' == some k0) = [] :=
              filter_eq_nil_of_no_key rows getKey k0 hno
            have hg0nil : g k0 = [] := by
              simpa [g] using hfilternil
            rw [fold_sfold_eq_sfold_map]
            rw [sfold_perm (hdskperm.map (fun k => sfold (g' k) zeroStats)) st]
            rw [← fold_sfold_eq_sfold_map]
            simp only [g', hgmap]
            simp only [List.foldl_cons]
            have hhead : sfold (if True then f r :: g k0 else g k0) st = statsAdd (f r) st := by
              simp [hg0nil, sfold_singleton, statsAdd_comm]
            rw [hhead]
            simp [hg]
            have htail : (distinctSomeKeys rows getKey).foldl
                  (fun acc k => sfold (if k = k0 then f r :: g k else g k) acc)
                  (statsAdd (f r) st) =
                (distinctSomeKeys rows getKey).foldl (fun acc k => sfold (g k) acc)
                  (statsAdd (f r) st) :=
              sfold_insert_tail_eq (distinctSomeKeys rows getKey) k0 g (f r)
                (statsAdd (f r) st) (fun k hk heq => hmem (by simpa [heq] using hk))
            rw [htail]
            rw [fold_sfold_start_add (distinctSomeKeys rows getKey) g st (f r)]
            have hfoldg : (distinctSomeKeys rows getKey).foldl (fun acc k => sfold (g k) acc) st =
                (distinctSomeKeys rows getKey).foldl
                  (fun acc k => sfold ((rows.filter (fun r' => getKey r' == some k)).map f) acc) st := by
              rfl
            rw [hfoldg]
            rw [ih]
            simpa [statsAdd_comm] using (fold_filterStats_start_add rows getKey f st (f r)).symm

private theorem pairStepFrom_eq (st : Stats) (p : LRow × RRow) :
    pairStep st p = statsAdd st (pairStep zeroStats p) := by
  cases hv1 : p.1.v with
  | none =>
      cases hv2 : p.2.v with
      | none => apply stats_ext <;> simp [pairStep, statsAdd, zeroStats, hv1, hv2] <;> omega
      | some x => apply stats_ext <;> simp [pairStep, statsAdd, zeroStats, hv1, hv2] <;> omega
  | some v =>
      cases hv2 : p.2.v with
      | none => apply stats_ext <;> simp [pairStep, statsAdd, zeroStats, hv1, hv2] <;> omega
      | some x => apply stats_ext <;> simp [pairStep, statsAdd, zeroStats, hv1, hv2] <;> omega

private theorem pairStatsFrom (ps : List (LRow × RRow)) (st : Stats) :
    ps.foldl pairStep st = statsAdd st (pairStats ps) := by
  induction ps generalizing st with
  | nil =>
      simp [pairStats]
      rw [statsAdd_zero_right]
  | cons p ps ih =>
      change ps.foldl pairStep (pairStep st p) = statsAdd st (pairStats (p :: ps))
      rw [ih (pairStep st p), pairStepFrom_eq]
      rw [show pairStats (p :: ps) = ps.foldl pairStep (pairStep zeroStats p) from rfl]
      rw [ih (pairStep zeroStats p)]
      simp [statsAdd_assoc, statsAdd_comm, statsAdd_left_comm]

private theorem fold_add_right_start {α : Type} (xs : List α) (f : α → Stats) (st a : Stats) :
    xs.foldl (fun acc x => statsAdd acc (f x)) (statsAdd a st) =
      statsAdd a (xs.foldl (fun acc x => statsAdd acc (f x)) st) := by
  induction xs generalizing st with
  | nil => simp [statsAdd_comm]
  | cons x xs ih =>
      change xs.foldl (fun acc y => statsAdd acc (f y)) (statsAdd (statsAdd a st) (f x)) =
        statsAdd a (xs.foldl (fun acc y => statsAdd acc (f y)) (statsAdd st (f x)))
      rw [show statsAdd (statsAdd a st) (f x) = statsAdd a (statsAdd st (f x)) from by
        cases a
        cases st
        cases f x
        simp [statsAdd]
        omega]
      exact ih (statsAdd st (f x))

private theorem pairStats_flatMap (xs : List LRow) (rs : List RRow) :
    pairStats (xs.flatMap (pairsOf rs)) =
      xs.foldl (fun st l => statsAdd st (contrib rs l)) zeroStats := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.flatMap_cons]
      rw [show pairStats (pairsOf rs x ++ xs.flatMap (pairsOf rs)) =
            statsAdd (pairStats (pairsOf rs x)) (pairStats (xs.flatMap (pairsOf rs))) from by
        rw [show pairStats (pairsOf rs x ++ xs.flatMap (pairsOf rs)) =
          (xs.flatMap (pairsOf rs)).foldl pairStep ((pairsOf rs x).foldl pairStep zeroStats) from by
            simp [pairStats, List.foldl_append]]
        exact pairStatsFrom (xs.flatMap (pairsOf rs)) (pairStats (pairsOf rs x))]
      rw [stats_pairsOf, ih]
      rw [← fold_add_right_start xs (contrib rs) zeroStats (contrib rs x)]
      rw [statsAdd_zero_right]
      simp [statsAdd_zero]

private theorem refStats_eq (ls : List LRow) (rs : List RRow) (g : Int) :
    pairStats (refJoinedPairs ls rs g) =
      (ls.filter (fun l => l.g == g)).foldl (fun st l => statsAdd st (contrib rs l)) zeroStats := by
  rw [refJoinedPairs_eq_flatMap]
  exact pairStats_flatMap (ls.filter (fun l => l.g == g)) rs

private abbrev Tup := Nat × Nat × Int × Nat × Int

private def statsFromTuple (t : Tup) : Stats :=
  ⟨t.1, t.2.1, t.2.2.1, t.2.2.2.1, t.2.2.2.2⟩

private theorem statsFromTuple_tupleAdd (a b : Tup) :
    statsFromTuple
        (a.1 + b.1, a.2.1 + b.2.1, a.2.2.1 + b.2.2.1,
          a.2.2.2.1 + b.2.2.2.1, a.2.2.2.2 + b.2.2.2.2) =
      statsAdd (statsFromTuple a) (statsFromTuple b) := by
  apply stats_ext <;> simp [statsFromTuple, statsAdd] <;> omega

private def optStep (lsG : List LRow) (rs : List RRow) (st : Tup) (k : Int) : Tup :=
  let pL := preAggRows lsG LRow.k LRow.v k
  let pR := preAggRows rs RRow.k RRow.v k
  ( st.1 + pL.cnt * pR.cnt
  , st.2.1 + pL.nn * pR.cnt
  , st.2.2.1 + pL.sum * Int.ofNat pR.cnt
  , st.2.2.2.1 + pL.cnt * pR.nn
  , st.2.2.2.2 + Int.ofNat pL.cnt * pR.sum )

private theorem optStep_stats (lsG : List LRow) (rs : List RRow) (st : Tup) (k : Int) :
    statsFromTuple (optStep lsG rs st k) = statsAdd (statsFromTuple st) (perKeyStats lsG rs k) := by
  apply stats_ext <;> simp [optStep, statsFromTuple, statsAdd, perKeyStats] <;> omega

private theorem optTupleFold_stats_aux (ks : List Int) (lsG : List LRow) (rs : List RRow) (t : Tup) :
    statsFromTuple (ks.foldl (optStep lsG rs) t) =
      ks.foldl (fun acc k => statsAdd acc (perKeyStats lsG rs k)) (statsFromTuple t) := by
  induction ks generalizing t with
  | nil => rfl
  | cons k ks ih =>
      change statsFromTuple (ks.foldl (optStep lsG rs) (optStep lsG rs t k)) =
        ks.foldl (fun acc k' => statsAdd acc (perKeyStats lsG rs k'))
          (statsAdd (statsFromTuple t) (perKeyStats lsG rs k))
      rw [ih (optStep lsG rs t k), optStep_stats]

private def optStats (ls : List LRow) (rs : List RRow) (g : Int) : Stats :=
  let lsG := ls.filter (fun l => l.g == g)
  let ks := distinctSomeKeys lsG LRow.k
  statsFromTuple (ks.foldl (optStep lsG rs) (0, 0, 0, 0, 0))

private theorem fold_perKeyStats_eq (ks : List Int) (lsG : List LRow) (rs : List RRow) (st : Stats) :
    ks.foldl (fun acc k => statsAdd acc (perKeyStats lsG rs k)) st =
      ks.foldl (fun acc k => sfold ((lsG.filter (fun l => l.k == some k)).map (contrib rs)) acc) st := by
  induction ks generalizing st with
  | nil => rfl
  | cons k ks ih =>
      change ks.foldl (fun acc k' => statsAdd acc (perKeyStats lsG rs k'))
          (statsAdd st (perKeyStats lsG rs k)) =
        ks.foldl (fun acc k' => sfold ((lsG.filter (fun l => l.k == some k')).map (contrib rs)) acc)
          (sfold ((lsG.filter (fun l => l.k == some k)).map (contrib rs)) st)
      have hstart : statsAdd st (perKeyStats lsG rs k) =
          sfold ((lsG.filter (fun l => l.k == some k)).map (contrib rs)) st := by
        rw [perKeyStats_eq_sfold]
        exact (sfold_eq_add_sfold_zero _ st).symm
      rw [hstart]
      exact ih _

private theorem fold_contrib_match_aux (lsG : List LRow) (rs : List RRow) (st : Stats) :
    lsG.foldl (fun acc l => match l.k with | some _ => statsAdd acc (contrib rs l) | none => acc) st =
      lsG.foldl (fun acc l => statsAdd acc (contrib rs l)) st := by
  induction lsG generalizing st with
  | nil => rfl
  | cons l lsG ih =>
      simp only [List.foldl_cons]
      cases hl : l.k with
      | none =>
          rw [show statsAdd st (contrib rs l) = st by simp [hl, contrib, statsAdd_zero_right]]
          exact ih st
      | some k =>
          simpa [hl] using ih (statsAdd st (contrib rs l))

private theorem fold_contrib_match (lsG : List LRow) (rs : List RRow) :
    lsG.foldl (fun acc l => match l.k with | some _ => statsAdd acc (contrib rs l) | none => acc) zeroStats =
      lsG.foldl (fun acc l => statsAdd acc (contrib rs l)) zeroStats :=
  fold_contrib_match_aux lsG rs zeroStats

private theorem optStats_eq (ls : List LRow) (rs : List RRow) (g : Int) :
    optStats ls rs g =
      (ls.filter (fun l => l.g == g)).foldl (fun st l => statsAdd st (contrib rs l)) zeroStats := by
  unfold optStats
  change statsFromTuple ((distinctSomeKeys (ls.filter (fun l => l.g == g)) LRow.k).foldl
      (optStep (ls.filter (fun l => l.g == g)) rs) (0, 0, 0, 0, 0)) = _
  rw [optTupleFold_stats_aux]
  rw [fold_perKeyStats_eq]
  rw [keyGroupFold]
  exact fold_contrib_match (ls.filter (fun l => l.g == g)) rs

theorem sound : TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown.Goal := by
  intro ls rs g
  have hoptTriple : optGroup ls rs g =
      let st := optStats ls rs g
      (st.cnt, if st.nnL == 0 then none else some st.sumL,
        if st.nnR == 0 then none else some st.sumR) := by
    unfold optGroup optStats
    rfl
  rw [hoptTriple, refGroup_char]
  have hstats : optStats ls rs g = pairStats (refJoinedPairs ls rs g) := by
    rw [optStats_eq, refStats_eq]
  rw [hstats]
end TinylambOpt.ResearchProofs.BilateralEagerAggJoinPushdown
