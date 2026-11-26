import TinylambOpt.ResearchGoals.MonoContiguousFactSlice

/-!
Proved

- `Goal` — Monotone contiguous compressed fact binary-search slice
  (`executor/detail/fused_grouped_join.cpp` `mono_contiguous_rf`, TPC-DS Q65): over a
  non-null key-sorted fact column, `bsLower lo .. bsUpper hi` selects a contiguous
  range covering every row satisfying any predicate whose support lies within `[lo,
  hi]` (even when `lo > hi`).

The reference model is defined in `TinylambOpt.ResearchGoals.MonoContiguousFactSlice`.
-/

namespace TinylambOpt.ResearchProofs.MonoContiguousFactSlice

open TinylambOpt.ResearchGoals.MonoContiguousFactSlice

private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) {l : List α}
    (h : ∀ a ∈ l, p a = false) : l.filter p = [] := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      have hx : p x = false := h x (by simp)
      have ht : ∀ a ∈ xs, p a = false := by intro a ha; exact h a (by simp [ha])
      simp [hx, ih ht]

private theorem filter_length_mono {α : Type} (p q : α → Bool) (xs : List α)
    (h : ∀ a, p a = true → q a = true) :
    (xs.filter p).length ≤ (xs.filter q).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · have hq : q x = true := h x hp
        rw [List.filter_cons_of_pos hp, List.filter_cons_of_pos hq, List.length_cons,
          List.length_cons]
        omega
      · by_cases hq : q x = true
        · rw [List.filter_cons_of_neg (by simpa using hp),
            List.filter_cons_of_pos hq, List.length_cons]
          omega
        · rw [List.filter_cons_of_neg (by simpa using hp),
            List.filter_cons_of_neg (by simpa using hq)]
          exact ih

private theorem bool_eq_false (b : Bool) (h : ¬(b = true)) : b = false := by
  cases b with
  | true => exact absurd rfl h
  | false => rfl

private theorem feq_false (b : Bool) (h : b = true → False) : b = false := by
  cases b with
  | true => exact absurd rfl h
  | false => rfl

private def blt (target : Int) : Int × Int → Bool := fun r => decide (r.1 < target)
private def ble (target : Int) : Int × Int → Bool := fun r => decide (r.1 ≤ target)

private theorem tail_sorted (x : Int × Int) (xs : List (Int × Int))
    (hs : SortedByKey (x :: xs)) : SortedByKey xs := by
  simp only [SortedByKey, List.map_cons] at hs ⊢
  exact List.Pairwise.of_cons hs

private theorem head_le_of_sorted (x : Int × Int) (xs : List (Int × Int))
    (hs : SortedByKey (x :: xs)) : ∀ z ∈ xs, x.1 ≤ z.1 := by
  simp only [SortedByKey, List.map_cons] at hs
  have h := (List.pairwise_cons.mp hs).1
  intro z hz
  exact h z.1 (List.mem_map_of_mem hz)

private theorem take_eq_filter_lt : ∀ (rows : List (Int × Int)) (target : Int),
    SortedByKey rows →
    rows.take (rows.filter (blt target)).length = rows.filter (blt target) := by
  intro rows
  induction rows with
  | nil => intro _ _; rfl
  | cons x xs ih =>
      intro target hs
      have hs' := tail_sorted x xs hs
      have hxl := head_le_of_sorted x xs hs
      by_cases hx : blt target x = true
      · have hf : (x :: xs).filter (blt target) = x :: xs.filter (blt target) := by
          rw [List.filter_cons_of_pos hx]
        rw [hf, List.length_cons, List.take_succ_cons, ih target hs']
      · have hx' : ¬ x.1 < target := by simpa [blt] using hx
        have hnilt : ∀ a ∈ (x :: xs), blt target a = false := by
          intro a ha
          have hmem : a = x ∨ a ∈ xs := by simpa using ha
          cases hmem with
          | inl heq =>
              subst heq
              exact bool_eq_false _ hx
          | inr heq =>
              have hge : x.1 ≤ a.1 := hxl a heq
              exact decide_eq_false (by omega)
        have hnil : (x :: xs).filter (blt target) = [] :=
          filter_eq_nil_of_all_false _ hnilt
        rw [hnil]
        rfl

private theorem take_eq_filter_le : ∀ (rows : List (Int × Int)) (target : Int),
    SortedByKey rows →
    rows.take (rows.filter (ble target)).length = rows.filter (ble target) := by
  intro rows
  induction rows with
  | nil => intro _ _; rfl
  | cons x xs ih =>
      intro target hs
      have hs' := tail_sorted x xs hs
      have hxl := head_le_of_sorted x xs hs
      by_cases hx : ble target x = true
      · have hf : (x :: xs).filter (ble target) = x :: xs.filter (ble target) := by
          rw [List.filter_cons_of_pos hx]
        rw [hf, List.length_cons, List.take_succ_cons, ih target hs']
      · have hx' : target < x.1 := by simpa [ble] using hx
        have hnilt : ∀ a ∈ (x :: xs), ble target a = false := by
          intro a ha
          have hmem : a = x ∨ a ∈ xs := by simpa using ha
          cases hmem with
          | inl heq =>
              subst heq
              exact bool_eq_false _ hx
          | inr heq =>
              have hge : x.1 ≤ a.1 := hxl a heq
              exact decide_eq_false (by omega)
        have hnil : (x :: xs).filter (ble target) = [] :=
          filter_eq_nil_of_all_false _ hnilt
        rw [hnil]
        rfl

private theorem get_lt_split : ∀ (rows : List (Int × Int)) (target : Int),
    SortedByKey rows → ∀ (j : Nat) (hj : j < rows.length),
      (rows.get ⟨j, hj⟩).1 < target ↔ j < (rows.filter (blt target)).length := by
  intro rows
  induction rows with
  | nil => intro _ _ j hj; exact absurd hj (by simp)
  | cons x xs ih =>
      intro target hs j hj
      have hs' := tail_sorted x xs hs
      have hxl := head_le_of_sorted x xs hs
      cases j with
      | zero =>
          show (x.1 < target) ↔ 0 < ((x :: xs).filter (blt target)).length
          by_cases hx : blt target x = true
          · have hlen : 0 < ((x :: xs).filter (blt target)).length := by
              rw [List.filter_cons_of_pos hx, List.length_cons]
              omega
            exact ⟨fun _ => hlen, fun _ => of_decide_eq_true hx⟩
          · have hx' : ¬ x.1 < target := by simpa [blt] using hx
            have hnil : (x :: xs).filter (blt target) = [] := by
              apply filter_eq_nil_of_all_false
              intro a ha
              have hmem : a = x ∨ a ∈ xs := by simpa using ha
              cases hmem with
              | inl heq =>
                  subst heq
                  exact bool_eq_false _ hx
              | inr heq =>
                  have hge : x.1 ≤ a.1 := hxl a heq
                  exact decide_eq_false (by omega)
            rw [hnil]
            exact ⟨fun hh => absurd (decide_eq_true hh) hx, fun hh => absurd hh (by simp)⟩
      | succ j =>
          have hj' : j < xs.length := by simpa using hj
          have hih := ih target hs' j hj'
          show (xs.get ⟨j, hj'⟩).1 < target ↔ j + 1 < ((x :: xs).filter (blt target)).length
          by_cases hx : blt target x = true
          · have hf : (x :: xs).filter (blt target) = x :: xs.filter (blt target) := by
              rw [List.filter_cons_of_pos hx]
            rw [hf, List.length_cons, Nat.succ_lt_succ_iff]
            exact hih
          · have hx' : ¬ x.1 < target := by simpa [blt] using hx
            have hnil : (x :: xs).filter (blt target) = [] := by
              apply filter_eq_nil_of_all_false
              intro a ha
              have hmem : a = x ∨ a ∈ xs := by simpa using ha
              cases hmem with
              | inl heq =>
                  subst heq
                  exact bool_eq_false _ hx
              | inr heq =>
                  have hge : x.1 ≤ a.1 := hxl a heq
                  exact decide_eq_false (by omega)
            rw [hnil]
            refine ⟨fun hh => absurd hh (by
              have hge : x.1 ≤ (xs.get ⟨j, hj'⟩).1 := hxl _ (List.get_mem xs ⟨j, hj'⟩)
              omega), fun hh => absurd hh (by simp)⟩

private theorem get_le_split : ∀ (rows : List (Int × Int)) (target : Int),
    SortedByKey rows → ∀ (j : Nat) (hj : j < rows.length),
      (rows.get ⟨j, hj⟩).1 ≤ target ↔ j < (rows.filter (ble target)).length := by
  intro rows
  induction rows with
  | nil => intro _ _ j hj; exact absurd hj (by simp)
  | cons x xs ih =>
      intro target hs j hj
      have hs' := tail_sorted x xs hs
      have hxl := head_le_of_sorted x xs hs
      cases j with
      | zero =>
          show (x.1 ≤ target) ↔ 0 < ((x :: xs).filter (ble target)).length
          by_cases hx : ble target x = true
          · have hlen : 0 < ((x :: xs).filter (ble target)).length := by
              rw [List.filter_cons_of_pos hx, List.length_cons]
              omega
            exact ⟨fun _ => hlen, fun _ => of_decide_eq_true hx⟩
          · have hx' : target < x.1 := by simpa [ble] using hx
            have hnil : (x :: xs).filter (ble target) = [] := by
              apply filter_eq_nil_of_all_false
              intro a ha
              have hmem : a = x ∨ a ∈ xs := by simpa using ha
              cases hmem with
              | inl heq =>
                  subst heq
                  exact bool_eq_false _ hx
              | inr heq =>
                  have hge : x.1 ≤ a.1 := hxl a heq
                  exact decide_eq_false (by omega)
            rw [hnil]
            exact ⟨fun hh => absurd (decide_eq_true hh) hx, fun hh => absurd hh (by simp)⟩
      | succ j =>
          have hj' : j < xs.length := by simpa using hj
          have hih := ih target hs' j hj'
          show (xs.get ⟨j, hj'⟩).1 ≤ target ↔ j + 1 < ((x :: xs).filter (ble target)).length
          by_cases hx : ble target x = true
          · have hf : (x :: xs).filter (ble target) = x :: xs.filter (ble target) := by
              rw [List.filter_cons_of_pos hx]
            rw [hf, List.length_cons, Nat.succ_lt_succ_iff]
            exact hih
          · have hx' : target < x.1 := by simpa [ble] using hx
            have hnil : (x :: xs).filter (ble target) = [] := by
              apply filter_eq_nil_of_all_false
              intro a ha
              have hmem : a = x ∨ a ∈ xs := by simpa using ha
              cases hmem with
              | inl heq =>
                  subst heq
                  exact bool_eq_false _ hx
              | inr heq =>
                  have hge : x.1 ≤ a.1 := hxl a heq
                  exact decide_eq_false (by omega)
            rw [hnil]
            refine ⟨fun hh => absurd hh (by
              have hge : x.1 ≤ (xs.get ⟨j, hj'⟩).1 := hxl _ (List.get_mem xs ⟨j, hj'⟩)
              omega), fun hh => absurd hh (by simp)⟩

private theorem drop_eq_filter_not_le : ∀ (rows : List (Int × Int)) (target : Int),
    SortedByKey rows →
    rows.drop (rows.filter (ble target)).length =
      rows.filter (fun r => ble target r = false) := by
  intro rows
  induction rows with
  | nil => intro _ _; rfl
  | cons x xs ih =>
      intro target hs
      have hs' := tail_sorted x xs hs
      have hxl := head_le_of_sorted x xs hs
      by_cases hx : ble target x = true
      · have hf : (x :: xs).filter (ble target) = x :: xs.filter (ble target) := by
          rw [List.filter_cons_of_pos hx]
        have hfn : (x :: xs).filter (fun r => ble target r = false) =
            xs.filter (fun r => ble target r = false) := by
          rw [List.filter_cons_of_neg (by simp [hx])]
        rw [hf, List.length_cons, List.drop_succ_cons, ih target hs', hfn]
      · have hx' : target < x.1 := by simpa [ble] using hx
        have hallp : ∀ a ∈ (x :: xs), ble target a = false := by
          intro a ha
          have hmem : a = x ∨ a ∈ xs := by simpa using ha
          cases hmem with
          | inl heq =>
              subst heq
              exact bool_eq_false _ hx
          | inr heq =>
              have hge : x.1 ≤ a.1 := hxl a heq
              exact decide_eq_false (by omega)
        have hself : (x :: xs).filter (fun r => ble target r = false) = x :: xs := by
          rw [List.filter_eq_self]
          intro a ha
          exact decide_eq_true (hallp a ha)
        have hnil : (x :: xs).filter (ble target) = [] :=
          filter_eq_nil_of_all_false _ hallp
        rw [hnil, hself]
        rfl

private theorem bsLower_go_succ (rows : List (Int × Int)) (target : Int) (l r fuel : Nat) :
    bsLower.go rows target l r (fuel + 1) =
      if r ≤ l then l
      else match rows[(l + r) / 2]? with
        | some q =>
            if q.1 < target then bsLower.go rows target ((l + r) / 2 + 1) r fuel
            else bsLower.go rows target l ((l + r) / 2) fuel
        | none => l := by
  simp [bsLower.go]
  rfl

private theorem bsUpper_go_succ (rows : List (Int × Int)) (target : Int) (l r fuel : Nat) :
    bsUpper.go rows target l r (fuel + 1) =
      if r ≤ l then l
      else match rows[(l + r) / 2]? with
        | some q =>
            if q.1 ≤ target then bsUpper.go rows target ((l + r) / 2 + 1) r fuel
            else bsUpper.go rows target l ((l + r) / 2) fuel
        | none => l := by
  simp [bsUpper.go]
  rfl

private theorem bsLower_go_spec (rows : List (Int × Int)) (target : Int) (n : Nat)
    (hchar : ∀ (j : Nat) (hj : j < rows.length),
        (rows.get ⟨j, hj⟩).1 < target ↔ j < n) :
    ∀ fuel l r : Nat,
      l ≤ n → n ≤ r → r ≤ rows.length → r ≤ l + fuel →
        bsLower.go rows target l r fuel = n := by
  intro fuel
  induction fuel with
  | zero =>
      intro l r h1 h2 _ _
      have h6 : l = n := by omega
      subst h6
      simp [bsLower.go]
  | succ fuel ih =>
      intro l r h1 h2 h3 h4
      by_cases hrl : r ≤ l
      · have h6 : l = n := by omega
        subst h6
        simp [bsLower.go, hrl]
      · have hgt : l < r := by omega
        have hm1 : l ≤ (l + r) / 2 := by omega
        have hm2 : (l + r) / 2 < r := by omega
        have hmid : (l + r) / 2 < rows.length := by omega
        have hmL : rows[(l + r) / 2]? = some (rows.get ⟨(l + r) / 2, hmid⟩) := by
          rw [List.getElem?_eq_some_iff]
          exact ⟨hmid, rfl⟩
        have hunfold : bsLower.go rows target l r (fuel + 1) =
            if (rows.get ⟨(l + r) / 2, hmid⟩).1 < target
              then bsLower.go rows target ((l + r) / 2 + 1) r fuel
              else bsLower.go rows target l ((l + r) / 2) fuel := by
          simp [bsLower_go_succ, hmL, hrl]
        by_cases hb : (rows.get ⟨(l + r) / 2, hmid⟩).1 < target
        · have hmn : (l + r) / 2 < n := (hchar _ hmid).mp hb
          have hrec : bsLower.go rows target ((l + r) / 2 + 1) r fuel = n :=
            ih ((l + r) / 2 + 1) r (by omega) h2 h3 (by omega)
          rw [hunfold, ite_eq_left hb, hrec]
        · have hge : n ≤ (l + r) / 2 := by
            have hne : ¬ ((l + r) / 2 < n) := fun hh => hb ((hchar _ hmid).mpr hh)
            omega
          have hrec : bsLower.go rows target l ((l + r) / 2) fuel = n :=
            ih l ((l + r) / 2) h1 hge (by omega) (by omega)
          rw [hunfold, ite_eq_right hb, hrec]

private theorem bsUpper_go_spec (rows : List (Int × Int)) (target : Int) (n : Nat)
    (hchar : ∀ (j : Nat) (hj : j < rows.length),
        (rows.get ⟨j, hj⟩).1 ≤ target ↔ j < n) :
    ∀ fuel l r : Nat,
      l ≤ n → n ≤ r → r ≤ rows.length → r ≤ l + fuel →
        bsUpper.go rows target l r fuel = n := by
  intro fuel
  induction fuel with
  | zero =>
      intro l r h1 h2 _ _
      have h6 : l = n := by omega
      subst h6
      simp [bsUpper.go]
  | succ fuel ih =>
      intro l r h1 h2 h3 h4
      by_cases hrl : r ≤ l
      · have h6 : l = n := by omega
        subst h6
        simp [bsUpper.go, hrl]
      · have hgt : l < r := by omega
        have hm1 : l ≤ (l + r) / 2 := by omega
        have hm2 : (l + r) / 2 < r := by omega
        have hmid : (l + r) / 2 < rows.length := by omega
        have hmL : rows[(l + r) / 2]? = some (rows.get ⟨(l + r) / 2, hmid⟩) := by
          rw [List.getElem?_eq_some_iff]
          exact ⟨hmid, rfl⟩
        have hunfold : bsUpper.go rows target l r (fuel + 1) =
            if (rows.get ⟨(l + r) / 2, hmid⟩).1 ≤ target
              then bsUpper.go rows target ((l + r) / 2 + 1) r fuel
              else bsUpper.go rows target l ((l + r) / 2) fuel := by
          simp [bsUpper_go_succ, hmL, hrl]
        by_cases hb : (rows.get ⟨(l + r) / 2, hmid⟩).1 ≤ target
        · have hmn : (l + r) / 2 < n := (hchar _ hmid).mp hb
          have hrec : bsUpper.go rows target ((l + r) / 2 + 1) r fuel = n :=
            ih ((l + r) / 2 + 1) r (by omega) h2 h3 (by omega)
          rw [hunfold, ite_eq_left hb, hrec]
        · have hge : n ≤ (l + r) / 2 := by
            have hne : ¬ ((l + r) / 2 < n) := fun hh => hb ((hchar _ hmid).mpr hh)
            omega
          have hrec : bsUpper.go rows target l ((l + r) / 2) fuel = n :=
            ih l ((l + r) / 2) h1 hge (by omega) (by omega)
          rw [hunfold, ite_eq_right hb, hrec]

private theorem bsLower_correct (rows : List (Int × Int)) (target : Int)
    (hs : SortedByKey rows) :
    bsLower rows target = (rows.filter (blt target)).length := by
  unfold bsLower
  exact bsLower_go_spec rows target (rows.filter (blt target)).length
    (get_lt_split rows target hs) (rows.length + 1) 0 rows.length
    (by omega) (List.length_filter_le _ _) (by omega) (by omega)

private theorem bsUpper_correct (rows : List (Int × Int)) (target : Int)
    (hs : SortedByKey rows) :
    bsUpper rows target = (rows.filter (ble target)).length := by
  unfold bsUpper
  exact bsUpper_go_spec rows target (rows.filter (ble target)).length
    (get_le_split rows target hs) (rows.length + 1) 0 rows.length
    (by omega) (List.length_filter_le _ _) (by omega) (by omega)

private theorem take_split {α : Type} (xs : List α) : ∀ {n m : Nat}, n ≤ m →
    xs.take m = xs.take n ++ (xs.drop n).take (m - n) := by
  induction xs with
  | nil => intro n m _; simp
  | cons x xs ih =>
      intro n m h
      cases n with
      | zero => simp
      | succ n =>
          cases m with
          | zero => exact absurd h (by omega)
          | succ m =>
              have hnm : n ≤ m := by omega
              rw [List.take_succ_cons, List.take_succ_cons, List.drop_succ_cons,
                Nat.succ_sub_succ, ih hnm]
              exact List.cons_append.symm

theorem sound : TinylambOpt.ResearchGoals.MonoContiguousFactSlice.Goal := by
  intro rows lo hi pred hs hsupp
  dsimp only
  have hlb : bsLower rows lo = (rows.filter (blt lo)).length := bsLower_correct rows lo hs
  have hub : bsUpper rows hi = (rows.filter (ble hi)).length := bsUpper_correct rows hi hs
  refine ⟨?_, ?_⟩
  · simp only [sliceFilter, refFilter]
    rw [hlb, hub]
    have htakeU : rows.take (rows.filter (ble hi)).length = rows.filter (ble hi) :=
      take_eq_filter_le rows hi hs
    have hdropU : rows.drop (rows.filter (ble hi)).length =
        rows.filter (fun r => ble hi r = false) := drop_eq_filter_not_le rows hi hs
    have hdropNil :
        (rows.drop (rows.filter (ble hi)).length).filter (fun r => pred r.1) = [] := by
      rw [hdropU]
      apply filter_eq_nil_of_all_false
      intro r hr
      have h2 := (List.mem_filter.mp hr).2
      have hnot : ¬ (r.1 ≤ hi) := by simpa [ble] using h2
      exact feq_false (pred r.1) (fun hp => absurd (hsupp r.1 hp).2 hnot)
    have href : rows.filter (fun r => pred r.1) =
        (rows.take (rows.filter (ble hi)).length).filter (fun r => pred r.1) := by
      have h := congrArg (fun l => (l : List (Int × Int)).filter (fun r => pred r.1))
        (List.take_append_drop (rows.filter (ble hi)).length rows)
      rw [List.filter_append, hdropNil, List.append_nil] at h
      exact h.symm
    by_cases hle : (rows.filter (blt lo)).length ≤ (rows.filter (ble hi)).length
    · have htakeL : rows.take (rows.filter (blt lo)).length = rows.filter (blt lo) :=
        take_eq_filter_lt rows lo hs
      have htakeLnil :
          (rows.take (rows.filter (blt lo)).length).filter (fun r => pred r.1) = [] := by
        rw [htakeL]
        apply filter_eq_nil_of_all_false
        intro r hr
        have h2 := (List.mem_filter.mp hr).2
        have hlt : r.1 < lo := by simpa [blt] using h2
        exact feq_false (pred r.1) (fun hp => absurd (hsupp r.1 hp).1 (by omega))
      have htsplit : rows.take (rows.filter (ble hi)).length =
          rows.take (rows.filter (blt lo)).length ++
            (rows.drop (rows.filter (blt lo)).length).take
              ((rows.filter (ble hi)).length - (rows.filter (blt lo)).length) :=
        take_split rows hle
      have hslice_eq :
          (rows.take (rows.filter (ble hi)).length).filter (fun r => pred r.1) =
            ((rows.drop (rows.filter (blt lo)).length).take
              ((rows.filter (ble hi)).length - (rows.filter (blt lo)).length)).filter
                (fun r => pred r.1) := by
        rw [htsplit, List.filter_append, htakeLnil, List.nil_append]
      rw [href, hslice_eq]
    · have hgt : hi < lo := by
        by_cases h : lo ≤ hi
        · have hmono : (rows.filter (blt lo)).length ≤ (rows.filter (ble hi)).length := by
            apply filter_length_mono (blt lo) (ble hi) rows
            intro r hr
            have h1 : r.1 < lo := of_decide_eq_true hr
            exact decide_eq_true (by omega)
          exact absurd hmono hle
        · omega
      have htakeUnil :
          (rows.take (rows.filter (ble hi)).length).filter (fun r => pred r.1) = [] := by
        rw [htakeU]
        apply filter_eq_nil_of_all_false
        intro r hr
        have h2 := (List.mem_filter.mp hr).2
        have hle' : r.1 ≤ hi := by simpa [ble] using h2
        exact feq_false (pred r.1) (fun hp => absurd (hsupp r.1 hp).1 (by omega))
      have hsub : (rows.filter (ble hi)).length - (rows.filter (blt lo)).length = 0 := by omega
      rw [hsub, List.take_zero, List.filter_nil, href, htakeUnil]
  · intro i r hget hcase
    obtain ⟨hilt, hreq⟩ := List.getElem?_eq_some_iff.mp hget
    subst hreq
    rcases hcase with hlt | hge
    · rw [hlb] at hlt
      have key_lt : (rows.get ⟨i, hilt⟩).1 < lo :=
        (get_lt_split rows lo hs i hilt).mpr hlt
      exact feq_false (pred (rows.get ⟨i, hilt⟩).1)
        (fun hp => absurd (hsupp _ hp).1 (by omega))
    · rw [hub] at hge
      have hnot : ¬ ((rows.get ⟨i, hilt⟩).1 ≤ hi) := by
        intro hc
        have h := (get_le_split rows hi hs i hilt).mp hc
        omega
      exact feq_false (pred (rows.get ⟨i, hilt⟩).1)
        (fun hp => absurd (hsupp _ hp).2 hnot)

end TinylambOpt.ResearchProofs.MonoContiguousFactSlice

-- C++ references to this model or its proof.
-- executor/detail/fused_group_probe.cpp
