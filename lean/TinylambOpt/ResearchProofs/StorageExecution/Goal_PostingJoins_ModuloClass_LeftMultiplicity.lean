import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_LeftMultiplicity

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## Lower bound on a sorted key column -/

private def Cut (ys : List Nat) (v i : Nat) : Prop :=
  i ≤ ys.length ∧
  (∀ j, j < i → ys.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < ys.length → v ≤ ys.getD j 0)

private theorem array_mono {xs : Array Nat} (hs : xs.toList.Pairwise (· ≤ ·))
    {i j : Nat} (hi : i < xs.size) (hj : j < xs.size) (hij : i < j) :
    xs[i]?.getD 0 ≤ xs[j]?.getD 0 := by
  rw [← Array.getElem?_toList (xs := xs) (i := i),
    ← Array.getElem?_toList (xs := xs) (i := j)]
  have hi' : i < xs.toList.length := by simpa using hi
  have hj' : j < xs.toList.length := by simpa using hj
  have hrel : xs.toList[i] ≤ xs.toList[j] := hs.rel_getElem_of_lt hi' hj' hij
  simpa [List.getD, List.getElem?_eq_getElem hi', List.getElem?_eq_getElem hj'] using hrel

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs[j]?.getD 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.size → v ≤ xs[j]?.getD 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
      have heq : lo = hi := by omega
      subst hi
      simp [searchBound]
      refine ⟨by simpa using hhi, ?_, ?_⟩
      · intro j hj
        simpa [List.getD, Array.getElem?_toList] using hbelow j hj
      · intro j hj hjlen
        simpa [List.getD, Array.getElem?_toList] using habove j hj (by simpa using hjlen)
  | succ fuel ih =>
      by_cases hge : lo ≥ hi
      · have heq : lo = hi := by omega
        subst hi
        simp [searchBound]
        refine ⟨by simpa using hhi, ?_, ?_⟩
        · intro j hj
          simpa [List.getD, Array.getElem?_toList] using hbelow j hj
        · intro j hj hjlen
          simpa [List.getD, Array.getElem?_toList] using habove j hj (by simpa using hjlen)
      · have hlt : lo < hi := by omega
        let mid := (lo + hi) / 2
        have hml : lo ≤ mid := by omega
        have hmh : mid < hi := by omega
        have hmid : mid < xs.size := by omega
        rw [searchBound]
        simp only [hge, ↓reduceIte]
        by_cases hv : xs[mid]?.getD 0 < v
        · rw [if_pos hv]
          have hbelow' : ∀ j, j < mid + 1 → xs[j]?.getD 0 < v := by
            intro j hj
            by_cases hjmid : j = mid
            · subst j
              exact hv
            · have hjlt : j < mid := by omega
              by_cases hjlo : j < lo
              · exact hbelow j hjlo
              · have hjltSize : j < xs.size := by omega
                have hmono := array_mono hs hjltSize hmid hjlt
                omega
          have hf' : hi - (mid + 1) < 2 ^ fuel := by
            have hp : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by
              rw [Nat.pow_succ, Nat.mul_comm]
            rw [hp] at hf
            omega
          have hres := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
          obtain ⟨hc, hlb, hub⟩ := hres
          refine ⟨hc, ?_, hub⟩
          exact Nat.le_trans hml (Nat.le_trans (Nat.le_succ mid) hlb)
        · rw [if_neg hv]
          have habove' : ∀ j, mid ≤ j → j < xs.size → v ≤ xs[j]?.getD 0 := by
            intro j hj hjlen
            by_cases hjmid : j = mid
            · subst j
              omega
            · have hmj : mid < j := by omega
              by_cases hjhi : hi ≤ j
              · exact habove j hjhi hjlen
              · have hmono := array_mono hs hmid hjlen hmj
                omega
          have hf' : mid - lo < 2 ^ fuel := by
            have hp : 2 ^ (fuel + 1) = 2 * 2 ^ fuel := by
              rw [Nat.pow_succ, Nat.mul_comm]
            rw [hp] at hf
            omega
          have hres := ih lo mid hml (by omega) hf' hbelow habove'
          obtain ⟨hc, hlb, hub⟩ := hres
          refine ⟨hc, hlb, ?_⟩
          exact Nat.le_trans hub (Nat.le_of_lt hmh)

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn0 : n = 0 := by simpa using h
    subst n
    simp
  · simp [h]
    have hn : n ≠ 0 := by simpa using h
    have hlt : n < 2 ^ (n.log2 + 1) :=
      (Nat.log2_lt (n := n) (k := n.log2 + 1) hn).mp (by omega)
    exact Nat.lt_of_lt_of_le hlt
      (Nat.pow_le_pow_right (by decide : 0 < 2) (by omega : n.log2 + 1 ≤ n.log2 + 1 + 1))

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) := by
  unfold lower
  have hsize : ys.toArray.size = ys.length := by simp
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by omega) hf
    (by intro j hj; omega)
    (by intro j hj hjlen; omega)
    (by simpa using hs)
  simpa [List.getD, Array.getElem?_toList] using h.1

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).1

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).2.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : lower ys v ≤ i) (hj : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).2.2 i hi hj

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have hla := lower_le_length hs a
  have ha := lower_le_of_ge hs b (Nat.le_refl _) (by omega)
  omega

/-! ## The cut at the filter length -/

private theorem filter_count_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v ((ys.filter (fun y => decide (y < v))).length) := by
  induction ys with
  | nil => simp [Cut]
  | cons a tl ih =>
      have hs' : tl.Pairwise (· ≤ ·) := hs.tail
      have hcut := ih hs'
      by_cases ha : a < v
      · simp only [List.filter_cons, decide_eq_true_eq, ha, ↓reduceIte, List.length_cons]
        have hfl : (tl.filter (fun y => decide (y < v))).length ≤ tl.length :=
          List.length_filter_le _ _
        refine ⟨?_, ?_, ?_⟩
        · simpa [List.length_cons, Nat.add_comm] using (Nat.add_le_add_right hfl 1)
        · intro j hj
          cases j with
          | zero => simpa using ha
          | succ j =>
            have hj' : j < (tl.filter (fun y => decide (y < v))).length := by omega
            simpa using hcut.2.1 j hj'
        · intro j hj hjlen
          cases j with
          | zero => omega
          | succ j =>
            have hj' : j < tl.length := by simpa using hjlen
            have hge : (tl.filter (fun y => decide (y < v))).length ≤ j := by omega
            simpa using hcut.2.2 j hge hj'
      · have hgeav : v ≤ a := by omega
        have hnil : tl.filter (fun y => decide (y < v)) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          simp only [decide_eq_true_eq]
          have hay : a ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp only [List.filter_cons, decide_eq_true_eq, ha, ↓reduceIte, hnil, List.length_nil, Nat.zero_add]
        refine ⟨by omega, ?_, ?_⟩
        · intro j hj
          omega
        · intro j hj hjlen
          cases j with
          | zero => simpa using hgeav
          | succ j =>
            have hj' : j < tl.length := by simpa using hjlen
            have hge : (tl.filter (fun y => decide (y < v))).length ≤ j := by
              simpa [hnil]
            simpa using hcut.2.2 j hge hj'

private theorem cut_unique {ys : List Nat} {v i k : Nat}
    (hi : Cut ys v i) (hk : Cut ys v k) : i = k := by
  have hil := hi.1
  have hkl := hk.1
  apply Nat.le_antisymm
  · apply Nat.le_of_not_gt
    intro hki
    have hl := hi.2.1 k hki
    have hg := hk.2.2 k (Nat.le_refl _) (by omega)
    omega
  · apply Nat.le_of_not_gt
    intro hik
    have hl := hk.2.1 i hik
    have hg := hi.2.2 i (Nat.le_refl _) (by omega)
    omega

private theorem lower_eq_filter_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun y => decide (y < v))).length :=
  cut_unique (lower_cut hs v) (filter_count_cut hs v)

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun y => decide (y < b)) =
      ys.filter (fun y => decide (y < a)) ++
        ys.filter (fun y => decide (a ≤ y && y < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs']
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hax, hnil, ih hs']
        · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          have hnil_ab : xs.filter (fun y => decide (a ≤ y && y < b)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq, Bool.and_eq_true]
            intro hay
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

private theorem lower_sub_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    (a b : Nat) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_length hs b, lower_eq_filter_length hs a]
  by_cases hab : a ≤ b
  · have hsplit := filter_lt_prefix_filter_lt hs hab
    rw [hsplit, List.length_append]
    omega
  · have hb : b ≤ a := by omega
    have hsub : (ys.filter (fun y => decide (y < b))).length ≤
        (ys.filter (fun y => decide (y < a))).length := by
      have hsname : (ys.filter (fun y => decide (y < b))).length =
          lower ys b := (lower_eq_filter_length hs b).symm
      have hsname2 : (ys.filter (fun y => decide (y < a))).length =
          lower ys a := (lower_eq_filter_length hs a).symm
      rw [hsname, hsname2]
      exact lower_mono hs hb
    rw [Nat.sub_eq_zero_of_le hsub]
    have hnil : ys.filter (fun y => decide (a ≤ y && y < b)) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro y hy
      simp only [decide_eq_true_eq, Bool.and_eq_true]
      intro hay
      omega
    rw [hnil]
    rfl
private theorem member_cons {a b : Nat} {rs : Ranges} {y : Nat} :
    member ((a, b) :: rs) y = (decide (a ≤ y && y < b) || member rs y) := by
  simp [member]

private theorem length_filter_or_append {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ x ∈ xs, p x = true → q x = false) :
    (xs.filter (fun x => p x || q x)).length =
      (xs.filter p).length + (xs.filter q).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have htail : ∀ y ∈ xs, p y = true → q y = false :=
        fun y hy => hdisj y (by simp [hy])
      by_cases hp : p x = true
      · have hq : q x = false := hdisj x (by simp) hp
        simp [hp, hq, ih htail] <;> omega
      · have hp' : p x = false := by cases hpx : p x <;> simp_all
        by_cases hq : q x = true
        · simp [hp', hq, ih htail] <;> omega
        · have hq' : q x = false := by cases hqx : q x <;> simp_all
          simp [hp', hq', ih htail] <;> omega

private theorem length_filter_member_cons {ys : List Nat} {a b : Nat} {rs : Ranges}
    (hdisj : ∀ y ∈ ys, (decide (a ≤ y && y < b)) = true → member rs y = false) :
    (ys.filter (fun y => member ((a, b) :: rs) y)).length =
      (ys.filter (fun y => decide (a ≤ y && y < b))).length +
        (ys.filter (fun y => member rs y)).length := by
  change (ys.filter (fun y => (a ≤ y && y < b) || member rs y)).length =
    (ys.filter (fun y => decide (a ≤ y && y < b))).length +
      (ys.filter (fun y => member rs y)).length
  exact length_filter_or_append _ _ _ hdisj

private theorem length_filter_member_eq_sum (ys : List Nat) (rs : Ranges)
    (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1)) :
    (ys.filter (fun y => member rs y)).length =
      ((rs.map (fun p => (ys.filter (fun y => decide (p.1 ≤ y && y < p.2))).length))).sum := by
  induction rs with
  | nil => simp [member]
  | cons p rs ih =>
      obtain ⟨a, b⟩ := p
      have hb : ∀ q ∈ rs, b ≤ q.1 := (List.pairwise_cons.mp hrs).1
      have hrs' : rs.Pairwise (fun p q => p.2 ≤ q.1) := (List.pairwise_cons.mp hrs).2
      have hdisj : ∀ y ∈ ys, (decide (a ≤ y && y < b)) = true → member rs y = false := by
        intro y hy hp
        simp only [decide_eq_true_eq, Bool.and_eq_true] at hp
        by_cases hm : member rs y = true
        · unfold member at hm
          rw [List.any_eq_true] at hm
          rcases hm with ⟨q, hqr, hqpred⟩
          have hbq := hb q hqr
          simp only [decide_eq_true_eq, Bool.and_eq_true] at hqpred
          omega
        · cases h : member rs y <;> simp_all
      rw [length_filter_member_cons hdisj, ih hrs']
      simp

/-! ## Interval membership for the modulo-class spans -/

private def SpansSpec (s : Search) : Prop :=
  ∀ u t x y, y < u → (member (s.spans u t x) y ↔ s.test t x y = true)

private def SpansOrdered (s : Search) : Prop :=
  ∀ u t x, (s.spans u t x).Pairwise (fun p q => p.2 ≤ q.1) ∧
    ∀ p ∈ s.spans u t x, p.1 ≤ p.2

private theorem lt_range_div (u t x y : Nat) (hyu : y < u)
    (hmod : y % (t + 1) = x % (t + 1)) :
    y / (t + 1) < (u + (t + 1) - 1 - x % (t + 1)) / (t + 1) := by
  let w := t + 1
  change y / w < (u + w - 1 - x % w) / w
  have hw : 0 < w := by omega
  have hdecomp : w * (y / w) + y % w = y := Nat.div_add_mod y w
  have hmod' : x % w = y % w := by simpa [w] using hmod.symm
  have hru : y % w ≤ u := by omega
  have harith : u + w - 1 - x % w - (w - 1) = u - y % w := by
    rw [hmod']
    omega
  rw [Nat.lt_div_iff_mul_lt hw]
  rw [harith]
  rw [Nat.mul_comm (y / w) w]
  omega

private theorem spec : SpansSpec search := by
  intro u t x y hyu
  let w := t + 1
  let r := x % w
  let n := (u + w - 1 - r) / w
  change member ((List.range n).map (fun i => (r + i * w, r + i * w + 1))) y ↔
    (r == y % w) = true
  constructor
  · intro hm
    unfold member at hm
    rw [List.any_eq_true] at hm
    rcases hm with ⟨p, hp, hpred⟩
    rw [List.mem_map] at hp
    rcases hp with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
    have hyr : y = r + i * w := by omega
    subst y
    have hw : 0 < w := by omega
    have hrlt : r < w := by simpa [r, w] using Nat.mod_lt x hw
    have hmod : (r + i * w) % w = r := by
      rw [Nat.mul_comm i w, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hrlt]
    simp [hmod]
  · intro ht
    have hrt : r = y % w := by simpa using ht
    have hi : y / w < n := by
      dsimp [n, r]
      exact lt_range_div u t x y hyu hrt.symm
    unfold member
    rw [List.any_eq_true]
    refine ⟨(r + (y / w) * w, r + (y / w) * w + 1), ?_, ?_⟩
    · rw [List.mem_map]
      refine ⟨y / w, by simpa using hi, rfl⟩
    · simp only [decide_eq_true_eq, Bool.and_eq_true]
      have hdecomp : w * (y / w) + y % w = y := Nat.div_add_mod y w
      rw [Nat.mul_comm (y / w) w]
      constructor <;> omega

private theorem ordered : SpansOrdered search := by
  intro u t x
  let w := t + 1
  let r := x % w
  let n := (u + w - 1 - r) / w
  change ((List.range n).map (fun i => (r + i * w, r + i * w + 1))).Pairwise
      (fun p q => p.2 ≤ q.1) ∧
    ∀ p ∈ (List.range n).map (fun i => (r + i * w, r + i * w + 1)), p.1 ≤ p.2
  constructor
  · rw [List.pairwise_map]
    rw [List.pairwise_iff_getElem]
    intro i j hi hj hij
    rw [List.getElem_range (by simpa using hi : i < (List.range n).length),
        List.getElem_range (by simpa using hj : j < (List.range n).length)]
    have hw : 1 ≤ w := by omega
    have hijle : i + 1 ≤ j := by omega
    have hmul := Nat.mul_le_mul_right w hijle
    have hsucc : (i + 1) * w = i * w + w := by rw [Nat.succ_mul]
    rw [hsucc] at hmul
    omega
  · intro p hp
    rw [List.mem_map] at hp
    rcases hp with ⟨i, hi, rfl⟩
    omega

private theorem fan_eq_countP {u t v : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t v ys = ys.countP (search.test t v) := by
  have hlen_fan :
      (ys.filter (fun y => member (search.spans u t v) y)).length =
        fan search u t v ys := by
    unfold fan
    rw [length_filter_member_eq_sum ys (search.spans u t v) (ordered u t v).1]
    congr 1
    apply List.map_congr_left
    intro p hp
    obtain ⟨a, b⟩ := p
    exact (lower_sub_eq_filter hv.1 a b).symm
  rw [← hlen_fan, List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  intro y hy
  apply Bool.eq_iff_iff.mpr
  exact spec u t v y (hv.2 y hy)

theorem sound_LeftMultiplicity : LeftMultiplicity.Goal := by
  intro u t xs ys hv
  let zs := ys.filterMap id
  have hfan : ∀ v : Nat, fan search u t v zs = zs.countP (search.test t v) := by
    intro v
    exact fan_eq_countP hv
  unfold zipOuter
  rw [List.length_flatMap]
  congr 1
  apply List.map_congr_left
  intro x hx
  cases x with
  | none =>
      simp [Option.any]
  | some v =>
      simp only [Option.any, Option.map_some, Option.getD_some]
      let L := zs.filter (search.test t v)
      have houter :
          (let hits := zs.filter (search.test t v);
             if hits.isEmpty then [(some v, none)] else hits.map (fun y => (some v, some y))).length =
            (if L.isEmpty then 1 else L.length) := by
        dsimp [L]
        by_cases he : (zs.filter (search.test t v)).isEmpty <;> simp [he]
      have hcount : L.length = fan search u t v zs := by
        dsimp [L]
        rw [← List.countP_eq_length_filter, ← hfan v]
      rw [houter, ← hcount]
      change (if L.isEmpty then 1 else L.length) = max 1 L.length
      by_cases he : L.isEmpty
      · have hz : L.length = 0 := by simpa using he
        simp [he, hz]
      · have hp : 0 < L.length := by
          apply Nat.pos_of_ne_zero
          intro hz
          apply he
          simpa using hz
        rw [if_neg he, Nat.max_eq_right (by omega)]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_LeftMultiplicity
