import TinylambOpt.ResearchGoals.QuantifiedRangeSummary

/-!
Proved

- `Goal` — `∀ q rows key x, probe q (index rows) key x = reference q rows key x`

The reference model is defined in `TinylambOpt.ResearchGoals.QuantifiedRangeSummary`.
-/

namespace TinylambOpt.ResearchProofs.QuantifiedRangeSummary

open TinylambOpt.ResearchGoals.QuantifiedRangeSummary

/-! ## Fold characterisation of the range summary -/

private def foldSumFrom (s : Summary) (vs : List Cell) : Summary :=
  vs.foldl (fun s c => push s c) s

private def foldSum (vs : List Cell) : Summary := foldSumFrom {} vs

private theorem foldSumFrom_nil (s : Summary) : foldSumFrom s [] = s := rfl

private theorem foldSumFrom_cons (s : Summary) (v : Cell) (vs : List Cell) :
    foldSumFrom s (v :: vs) = foldSumFrom (push s v) vs := rfl

private theorem push_hasNull (s : Summary) (v : Cell) :
    (push s v).hasNull = (s.hasNull || Option.isNone v) := by
  cases v <;> simp [push]

private theorem push_low (s : Summary) (v : Cell) :
    (push s v).low = match v with
      | none => s.low
      | some w => some (min w (s.low.getD w)) := by
  cases v <;> rfl

private theorem push_high (s : Summary) (v : Cell) :
    (push s v).high = match v with
      | none => s.high
      | some w => some (max w (s.high.getD w)) := by
  cases v <;> rfl

private theorem foldSumFrom_hasNull (s : Summary) :
    ∀ vs : List Cell, (foldSumFrom s vs).hasNull = (s.hasNull || vs.any Option.isNone)
  | [] => by cases s <;> simp [foldSumFrom]
  | (v :: vs) => by
      rw [foldSumFrom_cons, foldSumFrom_hasNull (push s v), push_hasNull, List.any_cons]
      cases Option.isNone v <;> simp

private theorem foldSum_hasNull_iff (vs : List Cell) :
    (foldSum vs).hasNull = true ↔ ∃ v, v ∈ vs ∧ v = none := by
  rw [show foldSum vs = foldSumFrom ({} : Summary) vs from rfl, foldSumFrom_hasNull]
  simp only [Bool.false_or]
  rw [List.any_eq_true]
  constructor
  · rintro ⟨v, hv, h⟩
    cases v with
    | none => exact ⟨none, hv, rfl⟩
    | some w => exact absurd h (by simp [Option.isNone])
  · rintro ⟨v, hv, rfl⟩
    exact ⟨none, hv, rfl⟩

/-! ## min/max folds -/

private def lowStep (o : Option Int) (b : Cell) : Option Int :=
  match b with
  | none => o
  | some v => some (min v (o.getD v))

private def highStep (o : Option Int) (b : Cell) : Option Int :=
  match b with
  | none => o
  | some v => some (max v (o.getD v))

private theorem foldSumFrom_low (s : Summary) :
    ∀ vs : List Cell, (foldSumFrom s vs).low = vs.foldl lowStep s.low
  | [] => rfl
  | (v :: vs) => by
      rw [foldSumFrom_cons, foldSumFrom_low (push s v), List.foldl_cons, push_low]
      cases v <;> rfl

private theorem foldSumFrom_high (s : Summary) :
    ∀ vs : List Cell, (foldSumFrom s vs).high = vs.foldl highStep s.high
  | [] => rfl
  | (v :: vs) => by
      rw [foldSumFrom_cons, foldSumFrom_high (push s v), List.foldl_cons, push_high]
      cases v <;> rfl

private theorem foldSum_low (vs : List Cell) :
    (foldSum vs).low = vs.foldl lowStep none := by
  show (foldSumFrom ({} : Summary) vs).low = _
  rw [foldSumFrom_low]

private theorem foldSum_high (vs : List Cell) :
    (foldSum vs).high = vs.foldl highStep none := by
  show (foldSumFrom ({} : Summary) vs).high = _
  rw [foldSumFrom_high]

private theorem lowStep_none (o : Option Int) : lowStep o none = o := rfl

private theorem lowStep_some (o : Option Int) (v : Int) :
    lowStep o (some v) = some (min v (o.getD v)) := rfl

private theorem highStep_none (o : Option Int) : highStep o none = o := rfl

private theorem highStep_some (o : Option Int) (v : Int) :
    highStep o (some v) = some (max v (o.getD v)) := rfl

private theorem foldl_lowStep_none_iff (vs : List Cell) :
    ∀ o : Option Int, (vs.foldl lowStep o = none) ↔
      (o = none ∧ ∀ n, (some n : Cell) ∉ vs) := by
  induction vs with
  | nil => intro o; simp
  | cons b vs ih =>
      intro o
      rw [List.foldl_cons, ih (lowStep o b)]
      cases b with
      | none => simp [lowStep_none]
      | some v =>
          refine ⟨fun h => absurd h.1 (by cases o <;> simp [lowStep_some]), ?_⟩
          intro h
          exact (h.2 v (List.mem_cons_self (a := some v) (l := vs))).elim

private theorem foldl_lowStep_some_mem (vs : List Cell) :
    ∀ (o : Option Int) (m : Int), vs.foldl lowStep o = some m →
      (some m : Cell) ∈ vs ∨ (∃ l, o = some l ∧ m = l) := by
  induction vs with
  | nil => intro o m h; exact Or.inr ⟨m, h, rfl⟩
  | cons b vs ih =>
      intro o m h
      rw [List.foldl_cons] at h
      cases b with
      | none =>
          rw [lowStep_none] at h
          cases ih o m h with
          | inl hm => exact Or.inl (List.mem_cons_of_mem none hm)
          | inr ex => exact Or.inr ex
      | some v =>
          rw [lowStep_some] at h
          cases ih (some (min v (o.getD v))) m h with
          | inl hm => exact Or.inl (List.mem_cons_of_mem (some v) hm)
          | inr ex =>
              obtain ⟨l', hl', hm⟩ := ex
              have hmin : min v (o.getD v) = l' := Option.some.inj hl'
              cases o with
              | none =>
                  rw [show (none : Option Int).getD v = v from rfl] at hmin
                  have hv : l' = v := hmin.symm.trans (Int.min_eq_left (Int.le_refl v))
                  rw [hm, hv]
                  exact Or.inl (List.mem_cons_self (a := some v) (l := vs))
              | some l =>
                  have hmn : min v l = l' := by simpa using hmin
                  by_cases hle : v ≤ l
                  · have hv : l' = v := hmn.symm.trans (Int.min_eq_left hle)
                    rw [hm, hv]
                    exact Or.inl (List.mem_cons_self (a := some v) (l := vs))
                  · have hl : l' = l :=
                      hmn.symm.trans (Int.min_eq_right (Int.le_of_lt (Int.not_le.mp hle)))
                    exact Or.inr ⟨l, rfl, hm.trans hl⟩

private theorem foldl_lowStep_some_le (vs : List Cell) :
    ∀ (o : Option Int) (m : Int), vs.foldl lowStep o = some m →
      (∀ n, (some n : Cell) ∈ vs → m ≤ n) ∧ (∀ l, o = some l → m ≤ l) := by
  induction vs with
  | nil =>
      intro o m h
      refine ⟨fun n hn => absurd hn (List.not_mem_nil (a := (some n : Cell))), fun l hl => ?_⟩
      cases o with
      | none => exact absurd h (by simp)
      | some l₀ =>
          have hl₀ : l₀ = l := Option.some.inj hl
          have hm : l₀ = m := Option.some.inj h
          rw [← hm, hl₀]
          exact Int.le_refl l
  | cons b vs ih =>
      intro o m h
      rw [List.foldl_cons] at h
      cases b with
      | none =>
          rw [lowStep_none] at h
          obtain ⟨h1, h2⟩ := ih o m h
          exact ⟨fun n hn => h1 n ((List.mem_cons.mp hn).resolve_left (by simp)),
            fun l hl => h2 l hl⟩
      | some v =>
          rw [lowStep_some] at h
          obtain ⟨h1, h2⟩ := ih (some (min v (o.getD v))) m h
          refine ⟨fun n hn => ?_, fun l hl => ?_⟩
          · cases List.mem_cons.mp hn with
            | inl he =>
                have hv : n = v := Option.some.inj he
                rw [hv]
                exact Int.le_trans (h2 _ rfl) (Int.min_le_left v (o.getD v))
            | inr hmem => exact h1 n hmem
          · cases o with
            | none => exact absurd hl (by simp)
            | some l₀ =>
                have h2' := h2 (min v l₀) rfl
                have hl₀ : l₀ = l := Option.some.inj hl
                rw [← hl₀]
                exact Int.le_trans h2' (Int.min_le_right v l₀)

private theorem foldl_highStep_none_iff (vs : List Cell) :
    ∀ o : Option Int, (vs.foldl highStep o = none) ↔
      (o = none ∧ ∀ n, (some n : Cell) ∉ vs) := by
  induction vs with
  | nil => intro o; simp
  | cons b vs ih =>
      intro o
      rw [List.foldl_cons, ih (highStep o b)]
      cases b with
      | none => simp [highStep_none]
      | some v =>
          refine ⟨fun h => absurd h.1 (by cases o <;> simp [highStep_some]), ?_⟩
          intro h
          exact (h.2 v (List.mem_cons_self (a := some v) (l := vs))).elim

private theorem foldl_highStep_some_mem (vs : List Cell) :
    ∀ (o : Option Int) (m : Int), vs.foldl highStep o = some m →
      (some m : Cell) ∈ vs ∨ (∃ l, o = some l ∧ m = l) := by
  induction vs with
  | nil => intro o m h; exact Or.inr ⟨m, h, rfl⟩
  | cons b vs ih =>
      intro o m h
      rw [List.foldl_cons] at h
      cases b with
      | none =>
          rw [highStep_none] at h
          cases ih o m h with
          | inl hm => exact Or.inl (List.mem_cons_of_mem none hm)
          | inr ex => exact Or.inr ex
      | some v =>
          rw [highStep_some] at h
          cases ih (some (max v (o.getD v))) m h with
          | inl hm => exact Or.inl (List.mem_cons_of_mem (some v) hm)
          | inr ex =>
              obtain ⟨l', hl', hm⟩ := ex
              have hmax : max v (o.getD v) = l' := Option.some.inj hl'
              cases o with
              | none =>
                  rw [show (none : Option Int).getD v = v from rfl] at hmax
                  have hv : l' = v := hmax.symm.trans (Int.max_self v)
                  rw [hm, hv]
                  exact Or.inl (List.mem_cons_self (a := some v) (l := vs))
              | some l =>
                  have hmn : max v l = l' := by simpa using hmax
                  by_cases hle : v ≤ l
                  · have hl : l' = l := hmn.symm.trans (Int.max_eq_right hle)
                    exact Or.inr ⟨l, rfl, hm.trans hl⟩
                  · have hv : l' = v :=
                      hmn.symm.trans (Int.max_eq_left (Int.le_of_lt (Int.not_le.mp hle)))
                    rw [hm, hv]
                    exact Or.inl (List.mem_cons_self (a := some v) (l := vs))

private theorem foldl_highStep_some_le (vs : List Cell) :
    ∀ (o : Option Int) (m : Int), vs.foldl highStep o = some m →
      (∀ n, (some n : Cell) ∈ vs → n ≤ m) ∧ (∀ l, o = some l → l ≤ m) := by
  induction vs with
  | nil =>
      intro o m h
      refine ⟨fun n hn => absurd hn (List.not_mem_nil (a := (some n : Cell))), fun l hl => ?_⟩
      cases o with
      | none => exact absurd h (by simp)
      | some l₀ =>
          have hl₀ : l₀ = l := Option.some.inj hl
          have hm : l₀ = m := Option.some.inj h
          rw [← hm, hl₀]
          exact Int.le_refl l
  | cons b vs ih =>
      intro o m h
      rw [List.foldl_cons] at h
      cases b with
      | none =>
          rw [highStep_none] at h
          obtain ⟨h1, h2⟩ := ih o m h
          exact ⟨fun n hn => h1 n ((List.mem_cons.mp hn).resolve_left (by simp)),
            fun l hl => h2 l hl⟩
      | some v =>
          rw [highStep_some] at h
          obtain ⟨h1, h2⟩ := ih (some (max v (o.getD v))) m h
          refine ⟨fun n hn => ?_, fun l hl => ?_⟩
          · cases List.mem_cons.mp hn with
            | inl he =>
                have hv : n = v := Option.some.inj he
                rw [hv]
                exact Int.le_trans (Int.le_max_left v (o.getD v)) (h2 _ rfl)
            | inr hmem => exact h1 n hmem
          · cases o with
            | none => exact absurd hl (by simp)
            | some l₀ =>
                have h2' := h2 (max v l₀) rfl
                have hl₀ : l₀ = l := Option.some.inj hl
                rw [← hl₀]
                exact Int.le_trans (Int.le_max_right v l₀) h2'

private theorem foldSum_low_none_iff (vs : List Cell) :
    (foldSum vs).low = none ↔ ∀ n, (some n : Cell) ∉ vs := by
  rw [foldSum_low, foldl_lowStep_none_iff vs none]
  exact ⟨fun h n hn => h.2 n hn, fun h => ⟨rfl, h⟩⟩

private theorem foldSum_low_some (vs : List Cell) (m : Int) (h : (foldSum vs).low = some m) :
    (some m : Cell) ∈ vs ∧ ∀ n, (some n : Cell) ∈ vs → m ≤ n := by
  rw [foldSum_low] at h
  obtain ⟨h1, _⟩ := foldl_lowStep_some_le vs none m h
  have hmem := foldl_lowStep_some_mem vs none m h
  cases hmem with
  | inl hm => exact ⟨hm, h1⟩
  | inr ex => exact absurd ex (fun ⟨l, hl, _⟩ => absurd hl (by simp))

private theorem foldSum_high_none_iff (vs : List Cell) :
    (foldSum vs).high = none ↔ ∀ n, (some n : Cell) ∉ vs := by
  rw [foldSum_high, foldl_highStep_none_iff vs none]
  exact ⟨fun h n hn => h.2 n hn, fun h => ⟨rfl, h⟩⟩

private theorem foldSum_high_some (vs : List Cell) (m : Int) (h : (foldSum vs).high = some m) :
    (some m : Cell) ∈ vs ∧ ∀ n, (some n : Cell) ∈ vs → n ≤ m := by
  rw [foldSum_high] at h
  obtain ⟨h1, _⟩ := foldl_highStep_some_le vs none m h
  have hmem := foldl_highStep_some_mem vs none m h
  cases hmem with
  | inl hm => exact ⟨hm, h1⟩
  | inr ex => exact absurd ex (fun ⟨l, hl, _⟩ => absurd hl (by simp))

/-! ## Three-valued folds over arbitrary accumulators -/

private theorem exists_cons_sql (y : SqlBool) (ts : List SqlBool) (p : SqlBool → Prop) :
    (∃ t ∈ y :: ts, p t) ↔ p y ∨ (∃ t ∈ ts, p t) := by
  constructor
  · rintro ⟨t, hmem, ht⟩
    cases List.mem_cons.mp hmem with
    | inl he => exact Or.inl (he ▸ ht)
    | inr h => exact Or.inr ⟨t, h, ht⟩
  · rintro (h | ⟨t, h, ht⟩)
    · exact ⟨y, List.mem_cons_self (a := y) (l := ts), h⟩
    · exact ⟨t, List.mem_cons.mpr (Or.inr h), ht⟩

private theorem foldl_or_spec (ts : List SqlBool) (acc : SqlBool) :
    ts.foldl SqlBool.or acc =
      if (∃ t ∈ ts, t = SqlBool.tTrue) ∨ acc = SqlBool.tTrue then SqlBool.tTrue
      else if (∃ t ∈ ts, t = SqlBool.tNull) ∨ acc = SqlBool.tNull then SqlBool.tNull
      else SqlBool.tFalse := by
  induction ts generalizing acc with
  | nil => cases acc <;> simp
  | cons y ts ih =>
      rw [List.foldl_cons, ih]
      cases acc <;> cases y <;>
        by_cases hT : (SqlBool.tTrue : SqlBool) ∈ ts <;>
        by_cases hN : SqlBool.tNull ∈ ts <;>
        simp [hT, hN, SqlBool.or]

private theorem foldl_and_spec (ts : List SqlBool) (acc : SqlBool) :
    ts.foldl SqlBool.and acc =
      if (∃ t ∈ ts, t = SqlBool.tFalse) ∨ acc = SqlBool.tFalse then SqlBool.tFalse
      else if (∃ t ∈ ts, t = SqlBool.tNull) ∨ acc = SqlBool.tNull then SqlBool.tNull
      else SqlBool.tTrue := by
  induction ts generalizing acc with
  | nil => cases acc <;> simp
  | cons y ts ih =>
      rw [List.foldl_cons, ih]
      cases acc <;> cases y <;>
        by_cases hF : (SqlBool.tFalse : SqlBool) ∈ ts <;>
        by_cases hN : SqlBool.tNull ∈ ts <;>
        simp [hF, hN, SqlBool.and]

/-! ## ltCell facts -/

private theorem ltCell_none_left (r : Cell) : ltCell (none : Cell) r = SqlBool.tNull := by
  cases r <;> rfl

private theorem ltCell_some_none (xv : Int) : ltCell (some xv) (none : Cell) = SqlBool.tNull := rfl

private theorem ltCell_some_some (xv yv : Int) :
    ltCell (some xv) (some yv) = if xv < yv then SqlBool.tTrue else SqlBool.tFalse := rfl

private theorem ltCell_high_true (xv : Int) (vs : List Cell)
    (hA : ∃ n, (some n : Cell) ∈ vs ∧ xv < n) :
    ltCell (some xv) (foldSum vs).high = SqlBool.tTrue := by
  obtain ⟨n, hn, hlt⟩ := hA
  cases h : (foldSum vs).high with
  | none =>
      have := (foldSum_high_none_iff vs).mp h
      exact absurd hn (this n)
  | some m =>
      have ⟨_, hb⟩ := foldSum_high_some vs m h
      have hmn : n ≤ m := hb n hn
      have hc : xv < m := by omega
      simpa [ltCell, h, hc]

private theorem ltCell_high_nottrue (xv : Int) (vs : List Cell)
    (hA : ¬ ∃ n, (some n : Cell) ∈ vs ∧ xv < n) :
    ltCell (some xv) (foldSum vs).high ≠ SqlBool.tTrue := by
  cases h : (foldSum vs).high with
  | none =>
      simp [ltCell]
  | some m =>
      have ⟨hm, _⟩ := foldSum_high_some vs m h
      by_cases hlt : xv < m
      · exact absurd (⟨m, hm, hlt⟩ : ∃ n, (some n : Cell) ∈ vs ∧ xv < n) hA
      · rw [ltCell_some_some, if_neg hlt]
        simp

private theorem ltCell_low_false (xv : Int) (vs : List Cell) :
    ltCell (some xv) (foldSum vs).low = SqlBool.tFalse ↔
      ∃ n, (some n : Cell) ∈ vs ∧ ¬ xv < n := by
  constructor
  · intro h
    cases hh : (foldSum vs).low with
    | none => exact absurd h (by simp [ltCell, hh])
    | some m =>
        have ⟨hm, hb⟩ := foldSum_low_some vs m hh
        refine ⟨m, hm, ?_⟩
        intro hlt
        rw [hh] at h
        rw [ltCell_some_some] at h
        rw [if_pos hlt] at h
        exact absurd h (by simp)
  · rintro ⟨n, hn, hn'⟩
    cases hh : (foldSum vs).low with
    | none =>
        have hnone := (foldSum_low_none_iff vs).mp hh
        exact absurd hn (hnone n)
    | some m =>
        have ⟨_, hb⟩ := foldSum_low_some vs m hh
        have hm : m ≤ n := hb n hn
        have hmn : ¬ xv < m := fun hlt => hn' (by omega)
        rw [ltCell_some_some, if_neg hmn]

/-! ## Existence of True / False / Null terms in a mapped group -/

private theorem exists_map_ltCell_true (xv : Int) (g : List Cell) :
    (∃ t ∈ g.map (fun v => ltCell (some xv) v), t = SqlBool.tTrue) ↔
      ∃ n, (some n : Cell) ∈ g ∧ xv < n := by
  induction g with
  | nil => simp
  | cons v gs ih =>
      rw [List.map_cons, exists_cons_sql, ih]
      cases v with
      | none => simp [ltCell, List.mem_cons]
      | some w =>
          rw [ltCell_some_some]
          by_cases h : xv < w <;> simp [h, List.mem_cons]

private theorem exists_map_ltCell_false (xv : Int) (g : List Cell) :
    (∃ t ∈ g.map (fun v => ltCell (some xv) v), t = SqlBool.tFalse) ↔
      ∃ n, (some n : Cell) ∈ g ∧ ¬ xv < n := by
  induction g with
  | nil => simp
  | cons v gs ih =>
      rw [List.map_cons, exists_cons_sql, ih]
      cases v with
      | none => simp [ltCell, List.mem_cons]
      | some w =>
          rw [ltCell_some_some]
          by_cases h : xv < w
          · rw [if_pos h]
            constructor
            · rintro (he | h2)
              · exact absurd he (by simp)
              · obtain ⟨n, hn, hnn⟩ := h2
                exact ⟨n, List.mem_cons_of_mem (some w) hn, hnn⟩
            · rintro ⟨n, hn, hnn⟩
              cases List.mem_cons.mp hn with
              | inl he =>
                  have hw : n = w := Option.some.inj he
                  rw [hw] at hnn
                  exact absurd h hnn
              | inr hmem => exact Or.inr ⟨n, hmem, hnn⟩
          · rw [if_neg h]
            constructor
            · rintro (he | h2)
              · exact ⟨w, List.mem_cons_self (a := (some w : Cell)) (l := gs), h⟩
              · obtain ⟨n, hn, hnn⟩ := h2
                exact ⟨n, List.mem_cons_of_mem (some w) hn, hnn⟩
            · rintro ⟨n, hn, hnn⟩
              cases List.mem_cons.mp hn with
              | inl he => exact Or.inl rfl
              | inr hmem => exact Or.inr ⟨n, hmem, hnn⟩

private theorem ltCell_some_some_ne_null (xv yv : Int) :
    ltCell (some xv) (some yv) ≠ SqlBool.tNull := by
  rw [ltCell_some_some]
  by_cases hlt : xv < yv
  · rw [if_pos hlt]
    simp
  · rw [if_neg hlt]
    simp

private theorem exists_map_ltCell_null (xv : Int) (g : List Cell) :
    (∃ t ∈ g.map (fun v => ltCell (some xv) v), t = SqlBool.tNull) ↔
      ∃ v, v ∈ g ∧ v = none := by
  induction g with
  | nil => simp
  | cons v gs ih =>
      rw [List.map_cons, exists_cons_sql, ih]
      cases v with
      | none =>
          constructor
          · rintro (h | ⟨w, hw, hwn⟩)
            · exact ⟨none, List.mem_cons_self (a := (none : Cell)) (l := gs), rfl⟩
            · exact ⟨none, List.mem_cons_self (a := (none : Cell)) (l := gs), rfl⟩
          · rintro ⟨w, hw, hwn⟩
            rw [hwn] at hw
            rcases List.mem_cons.mp hw with _ | hmem
            · exact Or.inl rfl
            · exact Or.inr ⟨none, hmem, rfl⟩
      | some z =>
          constructor
          · rintro (h | ⟨w, hw, hwn⟩)
            · exact absurd h (ltCell_some_some_ne_null xv z)
            · exact ⟨w, List.mem_cons_of_mem (some z) hw, hwn⟩
          · rintro ⟨w, hw, hwn⟩
            rcases List.mem_cons.mp hw with he | hmem
            · exact absurd (he.symm.trans hwn) (by simp)
            · exact Or.inr ⟨w, hmem, hwn⟩

private theorem not_exists_true_map_tNull (g : List Cell) :
    ¬ (∃ t ∈ g.map fun _ => SqlBool.tNull, t = SqlBool.tTrue) := by
  induction g with
  | nil => simp
  | cons v gs ih =>
      intro h
      rw [List.map_cons, exists_cons_sql] at h
      cases h with
      | inl h => exact absurd h (by simp)
      | inr h => exact ih h

private theorem not_exists_false_map_tNull (g : List Cell) :
    ¬ (∃ t ∈ g.map fun _ => SqlBool.tNull, t = SqlBool.tFalse) := by
  induction g with
  | nil => simp
  | cons v gs ih =>
      intro h
      rw [List.map_cons, exists_cons_sql] at h
      cases h with
      | inl h => exact absurd h (by simp)
      | inr h => exact ih h

private theorem exists_tNull_map_of_ne_nil (g : List Cell) (hg : g ≠ []) :
    ∃ t ∈ g.map fun _ => SqlBool.tNull, t = SqlBool.tNull := by
  cases g with
  | nil => exact absurd rfl hg
  | cons v gs =>
      exact ⟨SqlBool.tNull, List.mem_cons_self (a := SqlBool.tNull) (l := gs.map fun _ => SqlBool.tNull),
        rfl⟩

private theorem foldl_or_map_tNull (g : List Cell) (hg : g ≠ []) :
    (g.map fun _ => SqlBool.tNull).foldl SqlBool.or SqlBool.tFalse = SqlBool.tNull := by
  rw [foldl_or_spec]
  have h1 : ¬ ((∃ t ∈ g.map fun _ => SqlBool.tNull, t = SqlBool.tTrue) ∨
      SqlBool.tFalse = SqlBool.tTrue) := by
    rintro (h | h)
    · exact not_exists_true_map_tNull g h
    · exact absurd h (by simp)
  rw [if_neg h1]
  have h2 : (∃ t ∈ g.map fun _ => SqlBool.tNull, t = SqlBool.tNull) ∨
      SqlBool.tFalse = SqlBool.tNull :=
    Or.inl (exists_tNull_map_of_ne_nil g hg)
  rw [if_pos h2]

private theorem foldl_and_map_tNull (g : List Cell) (hg : g ≠ []) :
    (g.map fun _ => SqlBool.tNull).foldl SqlBool.and SqlBool.tTrue = SqlBool.tNull := by
  rw [foldl_and_spec]
  have h1 : ¬ ((∃ t ∈ g.map fun _ => SqlBool.tNull, t = SqlBool.tFalse) ∨
      SqlBool.tTrue = SqlBool.tFalse) := by
    rintro (h | h)
    · exact not_exists_false_map_tNull g h
    · exact absurd h (by simp)
  rw [if_neg h1]
  have h2 : (∃ t ∈ g.map fun _ => SqlBool.tNull, t = SqlBool.tNull) ∨
      SqlBool.tTrue = SqlBool.tNull :=
    Or.inl (exists_tNull_map_of_ne_nil g hg)
  rw [if_pos h2]

/-! ## The index map over rows -/

private def idxFold (m : Std.HashMap Int Summary) (r : Cell × Cell) : Std.HashMap Int Summary :=
  match r with
  | (none, _) => m
  | (some k, v) => m.insert k (push (m.getD k {}) v)

private theorem idxFold_none (b : Cell) (m : Std.HashMap Int Summary) :
    idxFold m (none, b) = m := rfl

private theorem idxFold_some (j : Int) (b : Cell) (m : Std.HashMap Int Summary) :
    idxFold m (some j, b) = m.insert j (push (m.getD j {}) b) := rfl

private theorem foldl_idxFold (m : Std.HashMap Int Summary) :
    ∀ rows : Rows,
      List.foldl (fun m (key,v) => match key with
        | none => m
        | some k => m.insert k (push (m.getD k {}) v)) m rows
        = List.foldl idxFold m rows := by
  intro rows
  have hfun : (fun (m : Std.HashMap Int Summary) p => match p with
      | (key, v) => match key with
        | none => m
        | some k => m.insert k (push (m.getD k {}) v)) = idxFold := by
    funext m p
    obtain ⟨a, b⟩ := p
    cases a <;> rfl
  rw [hfun]

private theorem index_eq (rows : Rows) : index rows = List.foldl idxFold {} rows := by
  unfold index
  exact foldl_idxFold {} rows

private def gvals (rows : Rows) (k : Int) : List Cell :=
  (rows.filter fun r => (some k : Cell) == r.1).map Prod.snd

private theorem gvals_cons_none (b : Cell) (rows : Rows) (k : Int) :
    gvals ((none, b) :: rows) k = gvals rows k := by
  unfold gvals
  rw [List.filter_cons]
  simp

private theorem gvals_cons_eq (k : Int) (b : Cell) (rows : Rows) :
    gvals ((some k, b) :: rows) k = b :: gvals rows k := by
  unfold gvals
  rw [List.filter_cons]
  rw [if_pos (by simp : ((some k : Cell) == (some k, b).1) = true)]
  rw [List.map_cons]

private theorem gvals_cons_ne (j k : Int) (h : j ≠ k) (b : Cell) (rows : Rows) :
    gvals ((some j, b) :: rows) k = gvals rows k := by
  unfold gvals
  rw [List.filter_cons]
  have hc : ¬ (((some k : Cell) == (some j, b).1) = true) := by
    intro hh
    rw [beq_iff_eq] at hh
    exact h (Option.some.inj hh).symm
  rw [if_neg hc]

private theorem foldl_index (m : Std.HashMap Int Summary) (rows : Rows) (k : Int) :
    ((rows.foldl idxFold m)[k]? =
        if gvals rows k = [] then m[k]?
        else some (foldSumFrom (m.getD k {}) (gvals rows k))) ∧
    ((rows.foldl idxFold m).getD k {} = foldSumFrom (m.getD k {}) (gvals rows k)) := by
  induction rows generalizing m with
  | nil => simp [gvals, foldSumFrom]
  | cons r rows ih =>
      rw [List.foldl_cons]
      obtain ⟨ih₁, ih₂⟩ := ih (idxFold m r)
      obtain ⟨a, b⟩ := r
      rw [ih₁, ih₂]
      cases a with
      | none =>
          rw [idxFold_none, gvals_cons_none]
          exact ⟨rfl, rfl⟩
      | some j =>
          rw [idxFold_some]
          by_cases hj : j = k
          · subst j
            rw [gvals_cons_eq, Std.HashMap.getElem?_insert_self, Std.HashMap.getD_insert_self]
            cases gvals rows k with
            | nil =>
                rw [if_pos (rfl : ([] : List Cell) = []),
                  if_neg (by simp : ¬ ((b :: ([] : List Cell)) = [])),
                  foldSumFrom_cons, foldSumFrom_nil]
                exact ⟨rfl, rfl⟩
            | cons c cs =>
                rw [if_neg (by simp : ¬ ((c :: cs : List Cell) = [])),
                  if_neg (by simp : ¬ ((b :: c :: cs : List Cell) = [])),
                  foldSumFrom_cons]
                exact ⟨rfl, rfl⟩
          · rw [gvals_cons_ne j k hj, Std.HashMap.getElem?_insert, Std.HashMap.getD_insert]
            have hbne1 : ¬ ((j == k : Bool) = true) := by
              intro hh
              rw [beq_iff_eq] at hh
              exact hj hh
            have hbne2 : ¬ ((k == j : Bool) = true) := by
              intro hh
              rw [beq_iff_eq] at hh
              exact hj hh.symm
            simp only [if_neg hbne1, if_neg hbne2]
            exact ⟨trivial, trivial⟩

private theorem index_getElem?_eq (rows : Rows) (k : Int) :
    (index rows)[k]? =
      if gvals rows k = [] then none else some (foldSum (gvals rows k)) := by
  rw [index_eq]
  have := (foldl_index {} rows k).1
  rw [show ({} : Std.HashMap Int Summary)[k]? = none from Std.HashMap.getElem?_empty,
    show ({} : Std.HashMap Int Summary).getD k {} = ({}) from Std.HashMap.getD_empty] at this
  rw [show foldSum (gvals rows k) = foldSumFrom ({} : Summary) (gvals rows k) from rfl]
  exact this

private theorem index_getD (rows : Rows) (k : Int) :
    (index rows).getD k {} = foldSum (gvals rows k) := by
  rw [index_eq]
  have := (foldl_index {} rows k).2
  rw [show ({} : Std.HashMap Int Summary).getD k {} = ({}) from Std.HashMap.getD_empty] at this
  rw [show foldSum (gvals rows k) = foldSumFrom ({} : Summary) (gvals rows k) from rfl]
  exact this

/-! ## Probe and reference specialisations -/

private theorem filter_false (rows : Rows) :
    List.filter (fun _ : Cell × Cell => false) rows = [] := by
  induction rows with
  | nil => rfl
  | cons a as ih => exact ih

private theorem map_snd_map (rows : List (Cell × Cell)) (x : Cell) :
    List.map (fun r : Cell × Cell => ltCell x r.2) rows =
      List.map (fun v : Cell => ltCell x v) (List.map Prod.snd rows) := by
  induction rows with
  | nil => rfl
  | cons a as ih =>
      obtain ⟨p, q⟩ := a
      simp [ih]

private theorem probe_any_none (m : Std.HashMap Int Summary) (x : Cell) :
    probe .any m none x = SqlBool.tFalse := rfl

private theorem probe_all_none (m : Std.HashMap Int Summary) (x : Cell) :
    probe .all m none x = SqlBool.tTrue := rfl

private theorem reference_any_none (rows : Rows) (x : Cell) :
    reference .any rows none x = SqlBool.tFalse := by
  show (List.foldl SqlBool.or SqlBool.tFalse
          (List.map (fun r : Cell × Cell => ltCell x r.2)
            (List.filter (fun _ : Cell × Cell => false) rows))) = SqlBool.tFalse
  rw [filter_false, List.map_nil]
  rfl

private theorem reference_all_none (rows : Rows) (x : Cell) :
    reference .all rows none x = SqlBool.tTrue := by
  show (List.foldl SqlBool.and SqlBool.tTrue
          (List.map (fun r : Cell × Cell => ltCell x r.2)
            (List.filter (fun _ : Cell × Cell => false) rows))) = SqlBool.tTrue
  rw [filter_false, List.map_nil]
  rfl

private theorem reference_any_some (rows : Rows) (k : Int) (x : Cell) :
    reference .any rows (some k) x =
      List.foldl SqlBool.or SqlBool.tFalse ((gvals rows k).map fun v => ltCell x v) := by
  show List.foldl SqlBool.or SqlBool.tFalse
        (List.map (fun r : Cell × Cell => ltCell x r.2)
          (List.filter (fun r : Cell × Cell => (some k : Cell) == r.1) rows)) =
      List.foldl SqlBool.or SqlBool.tFalse
        (List.map (fun v : Cell => ltCell x v) (gvals rows k))
  rw [map_snd_map]
  rfl

private theorem reference_all_some (rows : Rows) (k : Int) (x : Cell) :
    reference .all rows (some k) x =
      List.foldl SqlBool.and SqlBool.tTrue ((gvals rows k).map fun v => ltCell x v) := by
  show List.foldl SqlBool.and SqlBool.tTrue
        (List.map (fun r : Cell × Cell => ltCell x r.2)
          (List.filter (fun r : Cell × Cell => (some k : Cell) == r.1) rows)) =
      List.foldl SqlBool.and SqlBool.tTrue
        (List.map (fun v : Cell => ltCell x v) (gvals rows k))
  rw [map_snd_map]
  rfl

/-! ## Soundness -/

theorem sound : TinylambOpt.ResearchGoals.QuantifiedRangeSummary.Goal := by
  intro q rows key x
  cases key with
  | none =>
      cases q with
      | any => rw [probe_any_none, reference_any_none]
      | all => rw [probe_all_none, reference_all_none]
  | some k =>
      have hidx : (index rows)[k]? =
          if gvals rows k = [] then none else some (foldSum (gvals rows k)) :=
        index_getElem?_eq rows k
      cases q with
      | any =>
          show (match (index rows)[k]? with
            | none => SqlBool.tFalse
            | some s =>
                if ltCell x s.high = SqlBool.tTrue then SqlBool.tTrue
                else if s.hasNull || x.isNone then SqlBool.tNull
                else SqlBool.tFalse) = reference .any rows (some k) x
          rw [hidx]
          by_cases hg : gvals rows k = []
          · rw [if_pos hg, reference_any_some, hg]
            rfl
          · rw [if_neg hg, reference_any_some]
            cases x with
            | none =>
                show (if ltCell (none : Cell) (foldSum (gvals rows k)).high = SqlBool.tTrue
                    then SqlBool.tTrue
                  else if (foldSum (gvals rows k)).hasNull || (none : Cell).isNone
                    then SqlBool.tNull
                    else SqlBool.tFalse) = _
                have hlhs : (if ltCell (none : Cell) (foldSum (gvals rows k)).high
                      = SqlBool.tTrue then SqlBool.tTrue
                    else if (foldSum (gvals rows k)).hasNull || (none : Cell).isNone
                      then SqlBool.tNull
                      else SqlBool.tFalse) = SqlBool.tNull := by
                  rw [ltCell_none_left]
                  have h1 : ¬ (SqlBool.tNull = SqlBool.tTrue) := by simp
                  rw [if_neg h1]
                  have h2 : ((foldSum (gvals rows k)).hasNull || (none : Cell).isNone) = true :=
                    by simp
                  rw [if_pos h2]
                rw [hlhs]
                have hmap : (gvals rows k).map (fun v => ltCell (none : Cell) v) =
                    (gvals rows k).map fun _ => SqlBool.tNull := by
                  simp only [ltCell_none_left]
                rw [hmap, foldl_or_map_tNull (gvals rows k) hg]
            | some xv =>
                show (if ltCell (some xv) (foldSum (gvals rows k)).high = SqlBool.tTrue
                    then SqlBool.tTrue
                  else if (foldSum (gvals rows k)).hasNull || (some xv : Cell).isNone
                    then SqlBool.tNull
                    else SqlBool.tFalse) =
                    List.foldl SqlBool.or SqlBool.tFalse
                      ((gvals rows k).map fun v => ltCell (some xv) v)
                rw [foldl_or_spec]
                by_cases hA : ∃ n, (some n : Cell) ∈ gvals rows k ∧ xv < n
                · have hc : ltCell (some xv) (foldSum (gvals rows k)).high = SqlBool.tTrue :=
                    ltCell_high_true xv (gvals rows k) hA
                  have hR : (∃ t ∈ (gvals rows k).map (fun v => ltCell (some xv) v),
                      t = SqlBool.tTrue) ∨ SqlBool.tFalse = SqlBool.tTrue :=
                    Or.inl ((exists_map_ltCell_true xv (gvals rows k)).mpr hA)
                  rw [hc, if_pos rfl, if_pos hR]
                · have hc1 : ¬ ltCell (some xv) (foldSum (gvals rows k)).high = SqlBool.tTrue :=
                    ltCell_high_nottrue xv (gvals rows k) hA
                  have hR1 : ¬ ((∃ t ∈ (gvals rows k).map (fun v => ltCell (some xv) v),
                      t = SqlBool.tTrue) ∨ SqlBool.tFalse = SqlBool.tTrue) := by
                    rintro (h | h)
                    · exact hA ((exists_map_ltCell_true xv (gvals rows k)).mp h)
                    · exact absurd h (by simp)
                  by_cases hB : (foldSum (gvals rows k)).hasNull = true
                  · obtain ⟨v0, hv0, rfl⟩ := (foldSum_hasNull_iff (gvals rows k)).mp hB
                    have hcond : ((foldSum (gvals rows k)).hasNull
                        || ((some xv : Cell).isNone)) = true := by
                      rw [Option.isNone_some, Bool.or_false]
                      exact hB
                    have hR2 : (∃ t ∈ (gvals rows k).map (fun v => ltCell (some xv) v),
                        t = SqlBool.tNull) ∨ SqlBool.tFalse = SqlBool.tNull :=
                      Or.inl ((exists_map_ltCell_null xv (gvals rows k)).mpr ⟨none, hv0, rfl⟩)
                    rw [if_neg hc1, if_pos hcond, if_neg hR1, if_pos hR2]
                  · have hc2 : ¬ ((foldSum (gvals rows k)).hasNull
                      || ((some xv : Cell).isNone)) := by
                      intro h
                      rw [Option.isNone_some, Bool.or_false] at h
                      exact hB h
                    have hR2 : ¬ ((∃ t ∈ (gvals rows k).map (fun v => ltCell (some xv) v),
                        t = SqlBool.tNull) ∨ SqlBool.tFalse = SqlBool.tNull) := by
                      rintro (h | h)
                      · exact hB ((foldSum_hasNull_iff (gvals rows k)).mpr
                          ((exists_map_ltCell_null xv (gvals rows k)).mp h))
                      · exact absurd h (by simp)
                    rw [if_neg hc1, if_neg hc2, if_neg hR1, if_neg hR2]
      | all =>
          show (match (index rows)[k]? with
            | none => SqlBool.tTrue
            | some s =>
                if ltCell x s.low = SqlBool.tFalse then SqlBool.tFalse
                else if s.hasNull || x.isNone then SqlBool.tNull
                else SqlBool.tTrue) = reference .all rows (some k) x
          rw [hidx]
          by_cases hg : gvals rows k = []
          · rw [if_pos hg, reference_all_some, hg]
            rfl
          · rw [if_neg hg, reference_all_some]
            cases x with
            | none =>
                show (if ltCell (none : Cell) (foldSum (gvals rows k)).low = SqlBool.tFalse
                    then SqlBool.tFalse
                  else if (foldSum (gvals rows k)).hasNull || (none : Cell).isNone
                    then SqlBool.tNull
                    else SqlBool.tTrue) = _
                have hlhs : (if ltCell (none : Cell) (foldSum (gvals rows k)).low
                      = SqlBool.tFalse then SqlBool.tFalse
                    else if (foldSum (gvals rows k)).hasNull || (none : Cell).isNone
                      then SqlBool.tNull
                      else SqlBool.tTrue) = SqlBool.tNull := by
                  rw [ltCell_none_left]
                  have h1 : ¬ (SqlBool.tNull = SqlBool.tFalse) := by simp
                  rw [if_neg h1]
                  have h2 : ((foldSum (gvals rows k)).hasNull || (none : Cell).isNone) = true :=
                    by simp
                  rw [if_pos h2]
                rw [hlhs]
                have hmap : (gvals rows k).map (fun v => ltCell (none : Cell) v) =
                    (gvals rows k).map fun _ => SqlBool.tNull := by
                  simp only [ltCell_none_left]
                rw [hmap, foldl_and_map_tNull (gvals rows k) hg]
            | some xv =>
                show (if ltCell (some xv) (foldSum (gvals rows k)).low = SqlBool.tFalse
                    then SqlBool.tFalse
                  else if (foldSum (gvals rows k)).hasNull || (some xv : Cell).isNone
                    then SqlBool.tNull
                    else SqlBool.tTrue) =
                    List.foldl SqlBool.and SqlBool.tTrue
                      ((gvals rows k).map fun v => ltCell (some xv) v)
                rw [foldl_and_spec]
                by_cases hA : ∃ n, (some n : Cell) ∈ gvals rows k ∧ ¬ xv < n
                · have hc : ltCell (some xv) (foldSum (gvals rows k)).low = SqlBool.tFalse :=
                    (ltCell_low_false xv (gvals rows k)).mpr hA
                  have hR : (∃ t ∈ (gvals rows k).map (fun v => ltCell (some xv) v),
                      t = SqlBool.tFalse) ∨ SqlBool.tTrue = SqlBool.tFalse :=
                    Or.inl ((exists_map_ltCell_false xv (gvals rows k)).mpr hA)
                  rw [hc, if_pos rfl, if_pos hR]
                · have hc1 : ¬ ltCell (some xv) (foldSum (gvals rows k)).low = SqlBool.tFalse :=
                    fun hh => hA ((ltCell_low_false xv (gvals rows k)).mp hh)
                  have hR1 : ¬ ((∃ t ∈ (gvals rows k).map (fun v => ltCell (some xv) v),
                      t = SqlBool.tFalse) ∨ SqlBool.tTrue = SqlBool.tFalse) := by
                    rintro (h | h)
                    · exact hA ((exists_map_ltCell_false xv (gvals rows k)).mp h)
                    · exact absurd h (by simp)
                  by_cases hB : (foldSum (gvals rows k)).hasNull = true
                  · obtain ⟨v0, hv0, rfl⟩ := (foldSum_hasNull_iff (gvals rows k)).mp hB
                    have hcond : ((foldSum (gvals rows k)).hasNull
                        || ((some xv : Cell).isNone)) = true := by
                      rw [Option.isNone_some, Bool.or_false]
                      exact hB
                    have hR2 : (∃ t ∈ (gvals rows k).map (fun v => ltCell (some xv) v),
                        t = SqlBool.tNull) ∨ SqlBool.tTrue = SqlBool.tNull :=
                      Or.inl ((exists_map_ltCell_null xv (gvals rows k)).mpr ⟨none, hv0, rfl⟩)
                    rw [if_neg hc1, if_pos hcond, if_neg hR1, if_pos hR2]
                  · have hc2 : ¬ ((foldSum (gvals rows k)).hasNull
                      || ((some xv : Cell).isNone)) := by
                      intro h
                      rw [Option.isNone_some, Bool.or_false] at h
                      exact hB h
                    have hR2 : ¬ ((∃ t ∈ (gvals rows k).map (fun v => ltCell (some xv) v),
                        t = SqlBool.tNull) ∨ SqlBool.tTrue = SqlBool.tNull) := by
                      rintro (h | h)
                      · exact hB ((foldSum_hasNull_iff (gvals rows k)).mpr
                          ((exists_map_ltCell_null xv (gvals rows k)).mp h))
                      · exact absurd h (by simp)
                    rw [if_neg hc1, if_neg hc2, if_neg hR1, if_neg hR2]

end TinylambOpt.ResearchProofs.QuantifiedRangeSummary

-- C++ references to this model or its proof.
-- executor/detail/subquery_runtime.cpp
-- executor/detail/subquery_runtime.hpp
-- executor/relational_regression_test_part11.cpp
