import TinylambOpt.ResearchGoals.NthNonNullFrame

/-!
Proved

- `Goal` — `∀ xs (requests : List Request), requests.map (selectNth (build xs)) =
  requests.map (reference xs)`

The reference model is defined in `TinylambOpt.ResearchGoals.NthNonNullFrame`.
-/

namespace TinylambOpt.ResearchProofs.NthNonNullFrame

open ResearchGoals.NthNonNullFrame
open List

/-! ## Generic helpers -/

private theorem snoc_induction {α : Type} {P : List α → Prop}
    (h0 : P []) (hs : ∀ (xs : List α) (c : α), P xs → P (xs ++ [c])) :
    ∀ xs : List α, P xs := by
  intro xs
  have hrev : ∀ ys : List α, P ys.reverse := by
    intro ys
    induction ys with
    | nil => simpa using h0
    | cons a ys ih => simpa using (hs ys.reverse a ih)
  simpa using hrev xs.reverse

private theorem filterMap_congr' {α β : Type} (f g : α → Option β) :
    ∀ (l : List α), (∀ x ∈ l, f x = g x) → l.filterMap f = l.filterMap g := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a l ih =>
      intro h
      rw [List.filterMap_cons, h a List.mem_cons_self,
        ih (fun x hx => h x (List.mem_cons_of_mem a hx)), List.filterMap_cons]

private theorem filterMap_fst {α : Type} (l : List (Option α × Nat)) :
    l.filterMap (fun p => p.1) = (l.map Prod.fst).filterMap (fun o => o) := by
  induction l with
  | nil => rfl
  | cons p l ih =>
      obtain ⟨c, k⟩ := p
      cases c <;> simp [ih]

/-! ## The list mirror of `build` -/

private def valsOf (xs : Cells) : List Int := xs.filterMap (fun o => o)

private def cnt (xs : Cells) (i : Nat) : Nat := (valsOf (xs.take i)).length

private theorem cnt_zero (xs : Cells) : cnt xs 0 = 0 := rfl

private theorem cnt_length (xs : Cells) : cnt xs xs.length = (valsOf xs).length := by
  simp [cnt]

private theorem valsOf_snoc (xs : Cells) (c : Option Int) :
    valsOf (xs ++ [c]) = valsOf xs ++ c.toList := by
  cases c <;> simp [valsOf, List.filterMap_append]

private theorem cnt_snoc_last (xs : Cells) (c : Option Int) :
    cnt (xs ++ [c]) (xs.length + 1) = cnt xs xs.length + (if c.isSome then 1 else 0) := by
  have ht : List.take (xs.length + 1) (xs ++ [c]) = xs ++ [c] := by
    rw [List.take_append]
    have h1 : xs.length + 1 - xs.length = 1 := by omega
    rw [h1]
    have h2 : List.take (xs.length + 1) xs = xs := by
      rw [List.take_eq_take_min, Nat.min_eq_right (by omega : xs.length ≤ xs.length + 1),
        List.take_length]
    rw [h2]
    simp
  unfold cnt
  rw [ht]
  change (List.filterMap (fun o => o) (xs ++ [c])).length =
    (List.filterMap (fun o => o) (xs.take xs.length)).length +
      (if c.isSome then 1 else 0)
  rw [List.filterMap_append, List.take_length]
  cases c <;> simp

private theorem cnt_snoc_before (xs : Cells) (c : Option Int) {i : Nat}
    (hi : i ≤ xs.length) : cnt (xs ++ [c]) i = cnt xs i := by
  unfold cnt valsOf
  rw [List.take_append, Nat.sub_eq_zero_of_le hi]
  simp

private theorem cnt_cons_none (xs : Cells) (i : Nat) : cnt (none :: xs) (i + 1) = cnt xs i := by
  rfl

private theorem cnt_cons_some (v : Int) (xs : Cells) (i : Nat) :
    cnt (some v :: xs) (i + 1) = cnt xs i + 1 := by
  rfl

private def istep (s : Index) (c : Option Int) : Index :=
  let vs := c.elim s.values s.values.push
  ⟨s.ranks.push vs.size, vs⟩

private theorem build_snoc (xs : Cells) (c : Option Int) :
    build (xs ++ [c]) = istep (build xs) c := by
  simp [build, istep, List.foldl_append]

private theorem build_mirror (xs : Cells) :
    (build xs).ranks.toList = (List.range (xs.length + 1)).map (fun i => cnt xs i) ∧
      (build xs).values.toList = valsOf xs := by
  refine snoc_induction (P := fun ys : Cells =>
    (build ys).ranks.toList = (List.range (ys.length + 1)).map (fun i => cnt ys i) ∧
      (build ys).values.toList = valsOf ys) ?_ ?_ xs
  · constructor <;> simp [build, valsOf, cnt]
  · intro xs c ih
    rcases ih with ⟨hr, hv⟩
    have hs : build (xs ++ [c]) = istep (build xs) c := build_snoc xs c
    have hsz : (build xs).values.size = (valsOf xs).length := by
      rw [← Array.length_toList, hv]
    constructor
    · rw [hs]
      cases c with
      | none =>
          rw [show (istep (build xs) none).ranks.toList =
            (build xs).ranks.toList ++ [(build xs).values.size] by simp [istep], hr]
          have hc : cnt (xs ++ [none]) (xs.length + 1) = (valsOf xs).length := by
            rw [cnt_snoc_last, cnt_length]
            simp
          have hrhs :
              (List.range ((xs ++ [none]).length + 1)).map (fun i => cnt (xs ++ [none]) i) =
                (List.range (xs.length + 1)).map (fun i => cnt (xs ++ [none]) i) ++
                  [cnt (xs ++ [none]) (xs.length + 1)] := by
            simp only [List.length_append, List.length_singleton]
            rw [List.range_succ, List.map_append, List.map_singleton]
          rw [hrhs, hc, hsz]
          congr 1
          apply List.map_congr_left
          intro i hi
          have hi' : i < xs.length + 1 := List.mem_range.mp hi
          exact (cnt_snoc_before xs none (by omega : i ≤ xs.length)).symm
      | some v =>
          rw [show (istep (build xs) (some v)).ranks.toList =
            (build xs).ranks.toList ++ [(build xs).values.size + 1] by simp [istep], hr]
          have hc : cnt (xs ++ [some v]) (xs.length + 1) = (valsOf xs).length + 1 := by
            rw [cnt_snoc_last, cnt_length]
            simp
          have hrhs :
              (List.range ((xs ++ [some v]).length + 1)).map (fun i => cnt (xs ++ [some v]) i) =
                (List.range (xs.length + 1)).map (fun i => cnt (xs ++ [some v]) i) ++
                  [cnt (xs ++ [some v]) (xs.length + 1)] := by
            simp only [List.length_append, List.length_singleton]
            rw [List.range_succ, List.map_append, List.map_singleton]
          rw [hrhs, hc, hsz]
          congr 1
          apply List.map_congr_left
          intro i hi
          have hi' : i < xs.length + 1 := List.mem_range.mp hi
          exact (cnt_snoc_before xs (some v) (by omega : i ≤ xs.length)).symm
    · rw [hs]
      cases c with
      | none =>
          simpa [istep, valsOf_snoc] using hv
      | some v =>
          rw [show (istep (build xs) (some v)).values.toList =
            (build xs).values.toList ++ [v] by simp [istep]]
          rw [hv, valsOf_snoc]
          rfl

private theorem rankAt_build (xs : Cells) (i : Nat) :
    rankAt (build xs) i = cnt xs (min i xs.length) := by
  obtain ⟨hr, hv⟩ := build_mirror xs
  have hsz : (build xs).values.size = cnt xs xs.length := by
    rw [← Array.length_toList, hv, ← cnt_length]
  unfold rankAt
  rw [← Array.getElem?_toList, hr, hsz]
  by_cases hi : i ≤ xs.length
  · have hlt : i < xs.length + 1 := by omega
    rw [List.getElem?_map, List.getElem?_range hlt]
    simp [Nat.min_eq_left hi]
  · have hge : (List.range (xs.length + 1)).length ≤ i := by simp; omega
    have hnone : (List.range (xs.length + 1))[i]? = none :=
      List.getElem?_eq_none_iff.mpr hge
    rw [List.getElem?_map, hnone]
    simp [Nat.min_eq_right (by omega : xs.length ≤ i)]

private theorem values_get (xs : Cells) (i : Nat) :
    (build xs).values[i]? = (valsOf xs)[i]? := by
  obtain ⟨_, hv⟩ := build_mirror xs
  rw [← Array.getElem?_toList, hv]

/-! ## Basic `cnt` / `valsOf` calculus -/

private theorem valsOf_take (xs : Cells) (m : Nat) :
    valsOf (xs.take m) = (valsOf xs).take (cnt xs m) := by
  induction xs generalizing m with
  | nil => simp [cnt, valsOf]
  | cons c xs ih =>
      cases m with
      | zero => rfl
      | succ m =>
          cases c with
          | none =>
              change valsOf (xs.take m) = (valsOf xs).take (cnt xs m)
              exact ih m
          | some v =>
              change v :: valsOf (xs.take m) = v :: (valsOf xs).take (cnt xs m)
              rw [ih]

private theorem cnt_mono (xs : Cells) {i j : Nat} (hij : i ≤ j) :
    cnt xs i ≤ cnt xs j := by
  have hdecomp : xs.take i ++ (xs.take j).drop i = xs.take j := by
    have h := List.take_append_drop i (xs.take j)
    rw [List.take_take, Nat.min_eq_left hij] at h
    exact h
  unfold cnt valsOf
  rw [← hdecomp, List.filterMap_append]
  simp

private theorem cnt_le_vals (xs : Cells) (i : Nat) : cnt xs i ≤ (valsOf xs).length := by
  unfold cnt
  rw [valsOf_take, List.length_take]
  exact Nat.min_le_right _ _

private theorem cnt_succ (xs : Cells) (i : Nat) :
    cnt xs (min (i + 1) xs.length) =
      cnt xs (min i xs.length) + (if (xs[i]?.getD none).isSome then 1 else 0) := by
  unfold cnt
  by_cases hi : i < xs.length
  · have hmini : min i xs.length = i := Nat.min_eq_left (Nat.le_of_lt hi)
    have hmini1 : min (i + 1) xs.length = i + 1 := Nat.min_eq_left (by omega)
    rw [hmini, hmini1, List.take_add_one]
    change (List.filterMap (fun o => o) (xs.take i ++ xs[i]?.toList)).length =
      (List.filterMap (fun o => o) (xs.take i)).length +
        (if (xs[i]?.getD none).isSome then 1 else 0)
    rw [List.filterMap_append]
    cases hx : xs[i]? with
    | none => simp [hx]
    | some cell =>
        cases cell with
        | none => simp [hx]
        | some v => simp [hx]
  · have hmini : min i xs.length = xs.length := Nat.min_eq_right (by omega)
    have hmini1 : min (i + 1) xs.length = xs.length := Nat.min_eq_right (by omega)
    rw [hmini, hmini1]
    have hx : xs[i]? = none := List.getElem?_eq_none_iff.mpr (by omega)
    simp [hx]

private theorem valsOf_drop (xs : Cells) {m : Nat} (hm : m ≤ xs.length) :
    valsOf (xs.drop m) = (valsOf xs).drop (cnt xs m) := by
  induction xs generalizing m with
  | nil =>
      have hm0 : m = 0 := Nat.eq_zero_of_le_zero (by simpa using hm)
      subst m
      rfl
  | cons c xs ih =>
      cases m with
      | zero =>
          change valsOf (c :: xs) = (valsOf (c :: xs)).drop 0
          rfl
      | succ m =>
          have hm' : m ≤ xs.length := by simpa using hm
          cases c <;> simpa [cnt, valsOf] using (ih hm')

/-! ## The window `Y` and its values -/

private def Y (xs : Cells) (lo hi : Nat) : Cells :=
  (xs.drop lo).take (min hi xs.length - lo)

private theorem Y_length (xs : Cells) (lo hi : Nat)
    (hlo : lo < xs.length) (hhi : lo < hi) :
    (Y xs lo hi).length = min hi xs.length - lo := by
  unfold Y
  rw [List.length_take, List.length_drop]
  omega

private theorem Y_take (xs : Cells) (lo hi : Nat)
    (hlo : lo < xs.length) (hhi : lo < hi) :
    ∀ t : Nat, t ≤ min hi xs.length - lo →
      (Y xs lo hi).take t = (xs.take (lo + t)).drop lo := by
  intro t ht
  unfold Y
  rw [List.take_take, Nat.min_eq_left ht, List.take_drop]

private theorem cntY (xs : Cells) (lo hi : Nat)
    (hlo : lo < xs.length) (hhi : lo < hi) :
    ∀ t : Nat, t ≤ min hi xs.length - lo →
      cnt (Y xs lo hi) t = cnt xs (lo + t) - cnt xs lo := by
  intro t ht
  unfold cnt
  rw [Y_take xs lo hi hlo hhi t ht]
  have hmle : lo ≤ (xs.take (lo + t)).length := by
    rw [List.length_take]
    omega
  rw [valsOf_drop _ hmle, List.length_drop]
  have hcnt : cnt (xs.take (lo + t)) lo = (valsOf (xs.take lo)).length := by
    unfold cnt
    rw [List.take_take, Nat.min_eq_left (by omega : lo ≤ lo + t)]
  rw [hcnt]

private theorem valsOf_Y (xs : Cells) (lo hi : Nat)
    (hlo : lo < xs.length) (hhi : lo < hi) :
    valsOf (Y xs lo hi) =
      ((valsOf xs).take (cnt xs (min hi xs.length))).drop (cnt xs lo) := by
  unfold Y
  rw [valsOf_take]
  rw [valsOf_drop xs (Nat.le_of_lt hlo)]
  have hcnt : cnt (xs.drop lo) (min hi xs.length - lo) =
      cnt xs (min hi xs.length) - cnt xs lo := by
    have h := cntY xs lo hi hlo hhi (min hi xs.length - lo) (Nat.le_refl _)
    have hloadd : lo + (min hi xs.length - lo) = min hi xs.length := by omega
    rw [hloadd] at h
    unfold cnt
    unfold cnt at h
    simp [Y, List.take_take, Nat.min_eq_left (Nat.le_refl _)] at h
    exact h
  rw [hcnt, List.take_drop]
  have hle : cnt xs lo ≤ cnt xs (min hi xs.length) :=
    cnt_mono xs ((Nat.le_min).mpr ⟨Nat.le_of_lt hhi, Nat.le_of_lt hlo⟩)
  rw [Nat.add_sub_cancel' hle]

/-! ## The reference window as a `zipIdx` filter -/

private theorem zipIdx_filterMap (xs : Cells) (g : Option Int → Nat → Option Int) :
    xs.zipIdx.filterMap (fun p => g p.1 p.2)
      = (List.range xs.length).filterMap (fun j => g (xs[j]?.getD none) j) := by
  induction xs generalizing g with
  | nil => rfl
  | cons c xs ih =>
      rw [List.zipIdx_cons']
      simp only [List.length_cons]
      rw [List.range_succ_eq_map]
      simp only [List.filterMap_cons, List.filterMap_map]
      simp only [List.getElem?_cons_zero, Option.getD_some]
      have hL :
          filterMap ((fun p : Option Int × Nat => g p.1 p.2) ∘
              Prod.map id (fun j => j + 1)) xs.zipIdx =
            filterMap (fun p : Option Int × Nat => g p.1 (p.2 + 1)) xs.zipIdx := by
        apply filterMap_congr'
        intro p hp
        obtain ⟨a, j⟩ := p
        simp
      have hR :
          filterMap ((fun j : Nat => g ((c :: xs)[j]?.getD none) j) ∘ Nat.succ)
              (List.range xs.length) =
            filterMap (fun j : Nat => g (xs[j]?.getD none) (j + 1))
              (List.range xs.length) := by
        apply filterMap_congr'
        intro j hj
        simp only [Function.comp_apply]
        rw [List.getElem?_cons_succ]
      rw [hL, hR, ih (fun a j => g a (j + 1))]

private theorem filterMap_range_trim (g : Nat → Option Int) (lo m n : Nat)
    (h1 : lo ≤ m) (h2 : m ≤ n)
    (hlo : ∀ j, j < lo → g j = none)
    (hhi : ∀ j, m ≤ j → j < n → g j = none) :
    (List.range n).filterMap g = (List.range (m - lo)).filterMap (fun k => g (lo + k)) := by
  have hln : lo ≤ n := Nat.le_trans h1 h2
  have hn : n = lo + (n - lo) := (Nat.add_sub_cancel' hln).symm
  have hsplit : n - lo = (m - lo) + (n - m) := by omega
  rw [hn, List.range_add, List.filterMap_append]
  have hA : (List.range lo).filterMap g = [] := by
    apply List.filterMap_eq_nil_iff.mpr
    intro j hj
    exact hlo j (List.mem_range.mp hj)
  rw [hA, List.nil_append]
  rw [hsplit, List.range_add, List.map_append, List.filterMap_append]
  have hfirst :
      filterMap g (List.map (fun x => lo + x) (List.range (m - lo))) =
        filterMap (fun k => g (lo + k)) (List.range (m - lo)) := by
    rw [List.filterMap_map]
    rfl
  have htail :
      filterMap g (List.map (fun x => lo + x)
        (List.map (fun x => m - lo + x) (List.range (n - m)))) = [] := by
    rw [List.filterMap_map, List.filterMap_map]
    apply List.filterMap_eq_nil_iff.mpr
    intro x hx
    simp only [Function.comp_apply]
    have hxlt : x < n - m := List.mem_range.mp hx
    exact hhi (lo + ((m - lo) + x)) (by omega) (by omega)
  rw [hfirst, htail, List.append_nil]

private def W (xs : Cells) (lo hi cur : Nat) : List Int :=
  xs.zipIdx.filterMap fun (c, i) =>
    if lo ≤ i && i < hi && i != cur then c else none

private theorem W_range (xs : Cells) (lo hi cur : Nat)
    (hlo : lo < xs.length) (hhi : lo < hi) :
    W xs lo hi cur =
      (List.range (min hi xs.length - lo)).filterMap
        (fun k => if lo + k != cur then xs[lo + k]?.getD none else none) := by
  let G : Option Int → Nat → Option Int := fun a j =>
    if lo ≤ j && j < hi && j != cur then a else none
  have h1 : xs.zipIdx.filterMap (fun p => G p.1 p.2) =
      (List.range xs.length).filterMap (fun j => G (xs[j]?.getD none) j) :=
    zipIdx_filterMap xs G
  unfold W
  change xs.zipIdx.filterMap (fun p => G p.1 p.2) =
    (List.range (min hi xs.length - lo)).filterMap
      (fun k => if lo + k != cur then xs[lo + k]?.getD none else none)
  rw [h1]
  let g : Nat → Option Int := fun j => G (xs[j]?.getD none) j
  have htrim := filterMap_range_trim g lo (min hi xs.length) xs.length
    ((Nat.le_min).mpr ⟨Nat.le_of_lt hhi, Nat.le_of_lt hlo⟩)
    (Nat.min_le_right hi xs.length)
    (fun j hj => by
      dsimp [g, G]
      have : ¬ lo ≤ j := by omega
      simp [this])
    (fun j hj1 hj2 => by
      dsimp [g, G]
      have hnot : ¬ j < hi := by
        by_cases hhile : hi ≤ xs.length
        · have : min hi xs.length = hi := Nat.min_eq_left hhile
          omega
        · have : min hi xs.length = xs.length := Nat.min_eq_right (by omega)
          omega
      simp [hnot])
  dsimp [g, G] at htrim
  rw [htrim]
  apply filterMap_congr'
  intro k hk
  have hklt : k < min hi xs.length - lo := List.mem_range.mp hk
  have hlole : lo ≤ lo + k := by omega
  have hlt : lo + k < hi := by omega
  by_cases hcur : lo + k != cur
  · simp [hlole, hlt, hcur]
  · simp [hlole, hlt, hcur]

private theorem W_Y (xs : Cells) (lo hi cur : Nat)
    (hlo : lo < xs.length) (hhi : lo < hi) :
    W xs lo hi cur =
      (Y xs lo hi).zipIdx.filterMap fun (c, k) =>
        if lo + k != cur then c else none := by
  rw [W_range xs lo hi cur hlo hhi]
  have hlen : (Y xs lo hi).length = min hi xs.length - lo := Y_length xs lo hi hlo hhi
  have hget : ∀ k : Nat, k < min hi xs.length - lo → (Y xs lo hi)[k]? = xs[lo + k]? := by
    intro k hk
    unfold Y
    rw [List.getElem?_take_of_lt hk, ← List.getElem?_drop]
  have hf : (List.range (min hi xs.length - lo)).filterMap
      (fun k => if lo + k != cur then xs[lo + k]?.getD none else none)
      = (List.range (min hi xs.length - lo)).filterMap
      (fun k => if lo + k != cur then (Y xs lo hi)[k]?.getD none else none) := by
    apply filterMap_congr'
    intro k hk
    rw [hget k (List.mem_range.mp hk)]
  rw [hf, ← hlen]
  exact (zipIdx_filterMap (Y xs lo hi) (fun a j => if lo + j != cur then a else none)).symm

/-! ## Removing one position from a value list -/

private def Wk (Y : Cells) (base kk : Nat) : List Int :=
  Y.zipIdx.filterMap fun (c, j) => if base + j != kk then c else none

private theorem Wk_of_lt (Y : Cells) {base kk : Nat} (h : kk < base) :
    Wk Y base kk = valsOf Y := by
  have hv : Y.zipIdx.filterMap (fun p => p.1) = valsOf Y := by
    rw [filterMap_fst, List.zipIdx_map_fst]
    rfl
  unfold Wk
  rw [← hv]
  apply filterMap_congr'
  intro p hp
  obtain ⟨c, j⟩ := p
  have hj : 0 ≤ j := (List.mem_zipIdx hp).1
  have hne : base + j ≠ kk := by omega
  simp [hne]

private theorem Wk_slice (Y : Cells) (base kk : Nat) (h : base ≤ kk) :
    Wk Y base kk =
      (valsOf Y).take (cnt Y (kk - base)) ++ (valsOf Y).drop (cnt Y (kk - base + 1)) := by
  induction Y generalizing base kk with
  | nil =>
      simp [Wk, valsOf, cnt]
  | cons c Y ih =>
      unfold Wk
      rw [List.zipIdx_cons']
      simp only [List.filterMap_cons, List.filterMap_map]
      have htail :
          filterMap ((fun x : Option Int × Nat =>
            if base + x.snd != kk then x.fst else none) ∘
              Prod.map id (fun x => x + 1)) Y.zipIdx = Wk Y (base + 1) kk := by
        unfold Wk
        apply filterMap_congr'
        intro p hp
        obtain ⟨a, j⟩ := p
        simp
        have : base + (j + 1) = (base + 1) + j := by omega
        rw [this]
      rw [htail]
      by_cases hhead : base = kk
      · subst kk
        have htailnil : Wk Y (base + 1) base = valsOf Y := Wk_of_lt Y (by omega)
        rw [htailnil, Nat.sub_self base]
        have hb0 : base + 0 = base := Nat.add_zero base
        cases c with
        | none =>
            simp [hb0, cnt_cons_none, cnt_zero, valsOf]
        | some v =>
            simp [hb0, cnt_cons_some, cnt_zero, valsOf]
      · have htail_le : base + 1 ≤ kk := by omega
        have hsub : kk - (base + 1) + 1 = kk - base := by omega
        have hsub1 : kk - (base + 1) = kk - base - 1 := by omega
        have ht : kk - base = (kk - base - 1) + 1 := by omega
        rw [ih (base + 1) kk htail_le]
        rw [hsub1]
        have hbne : (base + 0 != kk) = true := by simp [hhead]
        rw [hbne]
        cases c with
        | none =>
            rw [show kk - base = (kk - base - 1) + 1 from ht]
            rw [cnt_cons_none, cnt_cons_none]
            rfl
        | some v =>
            rw [show kk - base = (kk - base - 1) + 1 from ht]
            rw [cnt_cons_some, cnt_cons_some]
            rfl

/-! ## Access to the reference window -/

private theorem access_plain (V : List Int) (a b k : Nat) :
    ((V.take b).drop a)[k]? = if a + k < b then V[a + k]? else none := by
  rw [List.getElem?_drop, List.getElem?_take]

private theorem W_of_outside (xs : Cells) (lo hi cur : Nat)
    (hlo : lo < xs.length) (hhi : lo < hi)
    (hout : cur < lo ∨ hi ≤ cur ∨ xs.length ≤ cur) :
    W xs lo hi cur = valsOf (Y xs lo hi) := by
  rw [W_Y xs lo hi cur hlo hhi]
  have hv : (Y xs lo hi).zipIdx.filterMap (fun p => p.1) = valsOf (Y xs lo hi) := by
    rw [filterMap_fst, List.zipIdx_map_fst]
    rfl
  rw [← hv]
  apply filterMap_congr'
  intro p hp
  obtain ⟨c, k⟩ := p
  have hklt : k < (Y xs lo hi).length := by simpa using (List.mem_zipIdx hp).2.1
  have hne : lo + k ≠ cur := by
    have hupper : lo + k < hi := by
      have hlen := Y_length xs lo hi hlo hhi
      rw [hlen] at hklt
      by_cases hhile : hi ≤ xs.length
      · have hmin : min hi xs.length = hi := Nat.min_eq_left hhile
        rw [hmin] at hklt
        omega
      · have hmin : min hi xs.length = xs.length := Nat.min_eq_right (by omega)
        rw [hmin] at hklt
        omega
    have hupper2 : lo + k < xs.length := by
      have hlen := Y_length xs lo hi hlo hhi
      rw [hlen] at hklt
      have hminle : min hi xs.length ≤ xs.length := Nat.min_le_right hi xs.length
      omega
    omega
  simp [hne]

private theorem W_nth (xs : Cells) (lo hi cur k : Nat)
    (hlo : lo < xs.length) (hhi : lo < hi) :
    let vs := valsOf xs
    let a := cnt xs lo
    let b := cnt xs (min hi xs.length)
    let cut := cnt xs (min cur xs.length)
    let cut1 := cnt xs (min (cur + 1) xs.length)
    let remove : Bool := lo ≤ cur && cur < hi && decide (cut < cut1)
    let pos := a + k
    let target := if remove && cut ≤ pos then pos + 1 else pos
    (W xs lo hi cur)[k]? = (if target < b then vs[target]? else none) := by
  intro vs a b cut cut1 remove pos target
  by_cases hout : cur < lo ∨ hi ≤ cur ∨ xs.length ≤ cur
  · rw [W_of_outside xs lo hi cur hlo hhi hout]
    have hrm : remove = false := by
      apply Bool.eq_false_of_ne_true
      intro htrue
      simp only [remove, Bool.and_eq_true, decide_eq_true_eq] at htrue
      rcases hout with hltlo | hgehi | hgel
      · omega
      · omega
      · have hx : xs[cur]?.getD none = none := by
          rw [List.getElem?_eq_none_iff.mpr (by omega : xs.length ≤ cur)]
          rfl
        have hcut : cut = cut1 := by
          dsimp [cut, cut1]
          rw [cnt_succ xs cur]
          simp [hx]
        omega
    simp [target, hrm, pos]
    rw [valsOf_Y xs lo hi hlo hhi]
    exact access_plain vs a b k
  · have hlole : lo ≤ cur := by omega
    have hlt_hi : cur < hi := by omega
    have hcur_len : cur < xs.length := by omega
    have ha_le_cut : a ≤ cut := by
      dsimp [a, cut]
      apply cnt_mono
      exact (Nat.le_min).mpr ⟨hlole, Nat.le_of_lt hlo⟩
    have hcut_le_b : cut ≤ b := by
      dsimp [cut, b]
      apply cnt_mono
      exact (Nat.le_min).mpr
        ⟨Nat.le_trans (Nat.min_le_left cur xs.length) (Nat.le_of_lt hlt_hi),
         Nat.min_le_right cur xs.length⟩
    have hb_le_vs : b ≤ vs.length := by
      dsimp [b, vs]
      rw [← cnt_length]
      exact cnt_mono xs (Nat.min_le_right hi xs.length)
    have hLlen : ((vs.take b).drop a).length = b - a := by
      rw [List.length_drop, List.length_take, Nat.min_eq_left hb_le_vs]
    have hm_le : cut - a ≤ b - a := by omega
    have hcs : cut1 = cut + (if (xs[cur]?.getD none).isSome then 1 else 0) := by
      dsimp [cut1, cut]
      exact cnt_succ xs cur
    rw [W_Y xs lo hi cur hlo hhi]
    change (Wk (Y xs lo hi) lo cur)[k]? = _
    rw [Wk_slice (Y xs lo hi) lo cur hlole]
    have hcur_le_min : cur ≤ min hi xs.length :=
      (Nat.le_min).mpr ⟨Nat.le_of_lt hlt_hi, Nat.le_of_lt hcur_len⟩
    have hcur1_le_min : cur + 1 ≤ min hi xs.length :=
      (Nat.le_min).mpr ⟨by omega, by omega⟩
    have hkk1 : cur - lo ≤ min hi xs.length - lo := by
      exact Nat.sub_le_sub_right hcur_le_min lo
    have hkk2 : cur - lo + 1 ≤ min hi xs.length - lo := by
      have htmp : cur + 1 - lo ≤ min hi xs.length - lo :=
        Nat.sub_le_sub_right hcur1_le_min lo
      have heq : cur - lo + 1 = cur + 1 - lo := by omega
      rw [heq]
      exact htmp
    have hcnt1 : cnt (Y xs lo hi) (cur - lo) = cut - a := by
      rw [cntY xs lo hi hlo hhi (cur - lo) hkk1]
      have h1 : lo + (cur - lo) = cur := Nat.add_sub_cancel' hlole
      rw [h1]
      simp [cut, a, Nat.min_eq_left (Nat.le_of_lt hcur_len),
        Nat.min_eq_left (Nat.le_of_lt hlo)]
    have hcnt2 : cnt (Y xs lo hi) (cur - lo + 1) = cut1 - a := by
      rw [cntY xs lo hi hlo hhi (cur - lo + 1) hkk2]
      have h1 : lo + (cur - lo + 1) = cur + 1 := by omega
      rw [h1]
      simp [cut1, a, Nat.min_eq_left (by omega : cur + 1 ≤ xs.length),
        Nat.min_eq_left (Nat.le_of_lt hlo)]
    rw [hcnt1, hcnt2]
    have hY : valsOf (Y xs lo hi) = (vs.take b).drop a := valsOf_Y xs lo hi hlo hhi
    rw [hY]
    rw [List.getElem?_append]
    simp only [List.length_take, hLlen, Nat.min_eq_left hm_le]
    by_cases hklt : k < cut - a
    · simp only [hklt, if_true, List.getElem?_take_of_lt hklt]
      rw [access_plain vs a b k]
      have hnot : ¬ cut ≤ pos := by
        dsimp [pos]
        omega
      have hltb : a + k < b := by omega
      simp [target, pos, hnot, hltb]
    · have hkge : cut - a ≤ k := by omega
      simp [hkge]
      have hcut1_le : cut1 ≤ cut + 1 := by
        rw [hcs]
        split <;> omega
      by_cases hrm : remove = true
      · have htbool : (lo ≤ cur && cur < hi && decide (cut < cut1)) = true := by
          simpa [remove] using hrm
        have ht : cut < cut1 := by
          have hh := htbool
          simp only [Bool.and_eq_true, decide_eq_true_eq] at hh
          exact hh.2
        have hcut1 : cut1 = cut + 1 := by omega
        rw [hcut1]
        have hidx : a + (cut + 1 - a) + (k - (cut - a)) = a + (k + 1) := by omega
        rw [hidx]
        rw [List.getElem?_take]
        have hcutpos : cut ≤ pos := by
          dsimp [pos]
          omega
        simp [hklt]
        rw [List.getElem?_take]
        simp [target, hrm, hcutpos]
        dsimp [pos]
        simp [Nat.add_assoc]
      · have hrmfalse : remove = false := Bool.eq_false_of_ne_true hrm
        have hnotlt : ¬ cut < cut1 := by
          intro hlt
          have hrmt : remove = true := by
            simpa [remove, hlole, hlt_hi, hlt]
          exact hrm hrmt
        have hcut1 : cut1 = cut := by omega
        rw [hcut1]
        have hidx : a + (cut - a) + (k - (cut - a)) = a + k := by omega
        rw [hidx]
        simp [hklt]
        rw [List.getElem?_take]
        simp [target, hrmfalse]
        dsimp [pos]

/-! ## Per-request and overall soundness -/

private theorem reference_eq_W (xs : Cells) (r : Request)
    (hlo : r.lo < xs.length) (hhi : r.lo < r.hi) (hnth : ¬ r.nth ≤ 0) :
    reference xs r = .value (W xs r.lo r.hi r.current)[r.nth.toNat - 1]? := by
  unfold reference W
  have hcond : ¬ ((r.hi ≤ r.lo || xs.length ≤ r.lo) = true) := by
    simp [hlo, hhi]
  rw [if_neg hcond, if_neg hnth]

private theorem selectNth_eq (xs : Cells) (r : Request)
    (hlo : r.lo < xs.length) (hhi : r.lo < r.hi) (hnth : ¬ r.nth ≤ 0) :
    selectNth (build xs) r =
      let vs := valsOf xs
      let a := cnt xs r.lo
      let b := cnt xs (min r.hi xs.length)
      let cut := cnt xs (min r.current xs.length)
      let cut1 := cnt xs (min (r.current + 1) xs.length)
      let remove : Bool := r.lo ≤ r.current && r.current < r.hi && decide (cut < cut1)
      let pos := a + (r.nth.toNat - 1)
      let target := if remove && cut ≤ pos then pos + 1 else pos
      .value (if target < b then vs[target]? else none) := by
  have hranks_size : (build xs).ranks.size = xs.length + 1 := by
    obtain ⟨hr, _⟩ := build_mirror xs
    rw [← Array.length_toList, hr, List.length_map, List.length_range]
  unfold selectNth
  have hcond : ¬ ((r.hi ≤ r.lo || (build xs).ranks.size - 1 ≤ r.lo) = true) := by
    simpa [hranks_size] using (show ¬ ((r.hi ≤ r.lo || xs.length ≤ r.lo) = true) by
      simp [hlo, hhi])
  rw [if_neg hcond, if_neg hnth]
  rw [rankAt_build xs r.lo, rankAt_build xs r.hi,
    rankAt_build xs r.current, rankAt_build xs (r.current + 1)]
  simp only [Nat.min_eq_left (Nat.le_of_lt hlo)]
  rw [values_get]

private theorem request_eq (xs : Cells) (r : Request) :
    selectNth (build xs) r = reference xs r := by
  have hranks_size : (build xs).ranks.size = xs.length + 1 := by
    obtain ⟨hr, _⟩ := build_mirror xs
    rw [← Array.length_toList, hr, List.length_map, List.length_range]
  by_cases hempty : (r.hi ≤ r.lo || xs.length ≤ r.lo) = true
  · have hs : selectNth (build xs) r = .value none := by
      unfold selectNth
      have hcond : (r.hi ≤ r.lo || (build xs).ranks.size - 1 ≤ r.lo) = true := by
        simpa [hranks_size] using hempty
      rw [if_pos hcond]
    have hr : reference xs r = .value none := by
      unfold reference
      rw [if_pos hempty]
    rw [hs, hr]
  · have hlo : r.lo < xs.length := by
      by_cases h : r.lo < xs.length
      · exact h
      · have hle : xs.length ≤ r.lo := by omega
        exfalso
        apply hempty
        simp [hle]
    have hhi : r.lo < r.hi := by
      by_cases h : r.lo < r.hi
      · exact h
      · have hle : r.hi ≤ r.lo := by omega
        exfalso
        apply hempty
        simp [hle]
    by_cases hnth : r.nth ≤ 0
    · have hs : selectNth (build xs) r = .invalid := by
        unfold selectNth
        have hcond : ¬ ((r.hi ≤ r.lo || (build xs).ranks.size - 1 ≤ r.lo) = true) := by
          simpa [hranks_size] using hempty
        rw [if_neg hcond, if_pos hnth]
      have hr : reference xs r = .invalid := by
        unfold reference
        rw [if_neg hempty, if_pos hnth]
      rw [hs, hr]
    · rw [selectNth_eq xs r hlo hhi hnth, reference_eq_W xs r hlo hhi hnth]
      rw [W_nth xs r.lo r.hi r.current (r.nth.toNat - 1) hlo hhi]

theorem sound : TinylambOpt.ResearchGoals.NthNonNullFrame.Goal := by
  intro xs requests
  induction requests with
  | nil => rfl
  | cons r rs ih => simp [ih, request_eq xs r]

end TinylambOpt.ResearchProofs.NthNonNullFrame

-- C++ references to this model or its proof.
-- executor/detail/window_eval.cpp
-- executor/relational_regression_test_part11.cpp
