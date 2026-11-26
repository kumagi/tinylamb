import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_OrderedProbe

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass
open TinylambOpt.ResearchGoals.Compression.Wire

private theorem valid_pairwise {u : Nat} {ys : List Nat} (hv : valid u ys) : ys.Pairwise (· ≤ ·) := hv.1
private theorem valid_lt {u : Nat} {ys : List Nat} (hv : valid u ys) {y : Nat} (hy : y ∈ ys) : y < u := hv.2 y hy

private def Cut (xs : List Nat) (v i : Nat) : Prop :=
  (∀ j, j < i → xs.getD j 0 < v) ∧
  (∀ j, i ≤ j → j < xs.length → v ≤ xs.getD j 0)

private theorem getD_eq_getElem {xs : List Nat} {i : Nat} (hi : i < xs.length) :
    xs.getD i 0 = xs[i] := by
  simp [List.getD, List.getElem?_eq_getElem hi]

private theorem getD_toArray (xs : List Nat) (i : Nat) :
    xs.toArray[i]?.getD 0 = xs.getD i 0 := by
  by_cases hi : i < xs.length
  · have ha : i < xs.toArray.size := by simpa using hi
    rw [Array.getElem?_eq_getElem ha, Option.getD_some, List.getElem_toArray ha]
    simp [List.getD, List.getElem?_eq_getElem hi]
  · have ha : xs.toArray.size ≤ i := by simpa using hi
    rw [Array.getElem?_eq_none ha]
    simp [List.getD, List.getElem?_eq_none (by omega : xs.length ≤ i)]

private theorem searchBound_cut {xs : Array Nat} (v fuel lo hi : Nat)
    (hlo : lo ≤ hi) (hhi : hi ≤ xs.size) (hf : hi - lo < 2 ^ fuel)
    (hbelow : ∀ j, j < lo → xs.toList.getD j 0 < v)
    (habove : ∀ j, hi ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0)
    (hs : xs.toList.Pairwise (· ≤ ·)) :
    Cut xs.toList v (searchBound xs v fuel lo hi) ∧
      lo ≤ searchBound xs v fuel lo hi ∧ searchBound xs v fuel lo hi ≤ hi := by
  induction fuel generalizing lo hi with
  | zero =>
    have h2 : (2 : Nat) ^ 0 = 1 := by simp
    have heq : lo = hi := by omega
    subst hi
    exact ⟨⟨hbelow, habove⟩, ⟨by simp [searchBound], by simp [searchBound]⟩⟩
  | succ fuel ih =>
    by_cases hge : lo ≥ hi
    · have heq : lo = hi := by omega
      subst hi
      simp only [searchBound, hge, ↓reduceIte]
      exact ⟨⟨hbelow, habove⟩, ⟨by simp [searchBound], by simp [searchBound]⟩⟩
    · have hlt : lo < hi := by omega
      let mid := (lo + hi) / 2
      have hml : lo ≤ mid := by omega
      have hmh : mid < hi := by omega
      have hmid : mid < xs.size := by omega
      have hmidlen : mid < xs.toList.length := by simpa using hmid
      have hval : xs[mid]?.getD 0 = xs[mid] := by
        rw [Array.getElem?_eq_getElem hmid, Option.getD_some]
      have hlistval : xs.toList.getD mid 0 = xs[mid] := by
        rw [← getD_toArray xs.toList mid, Array.getElem?_eq_getElem hmid, Option.getD_some]
      have hpow : (2 : Nat) ^ (fuel + 1) = 2 * 2 ^ fuel := by
        simp [Nat.pow_succ, Nat.mul_comm]
      rw [searchBound]
      simp only [hge, ↓reduceIte]
      change Cut xs.toList v
          (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
            else searchBound xs v fuel lo mid) ∧
        lo ≤ (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
          else searchBound xs v fuel lo mid) ∧
        (if xs[mid]?.getD 0 < v then searchBound xs v fuel (mid + 1) hi
          else searchBound xs v fuel lo mid) ≤ hi
      rw [hval]
      by_cases hv : xs[mid] < v
      · rw [ite_eq_left hv]
        have hbelow' : ∀ j, j < mid + 1 → xs.toList.getD j 0 < v := by
          intro j hj
          by_cases heq : j = mid
          · subst j
            rw [hlistval]
            exact hv
          · have hjm : j < mid := by omega
            have hjlen : j < xs.toList.length := by
              simpa using (by omega : j < xs.size)
            by_cases hjl : j < lo
            · exact hbelow j hjl
            · have hrel : xs.toList.getD j 0 ≤ xs.toList.getD mid 0 := by
                have hpair := hs.rel_getElem_of_lt hjlen hmidlen hjm
                rw [← getD_eq_getElem hjlen, ← getD_eq_getElem hmidlen] at hpair
                exact hpair
              rw [hlistval] at hrel
              omega
        have hf' : hi - (mid + 1) < 2 ^ fuel := by
          rw [hpow] at hf
          omega
        have hres := ih (mid + 1) hi (by omega) (by omega) hf' hbelow' habove
        obtain ⟨hc, hlb, hub⟩ := hres
        exact ⟨hc, by omega, hub⟩
      · rw [ite_eq_right hv]
        have habove' : ∀ j, mid ≤ j → j < xs.toList.length → v ≤ xs.toList.getD j 0 := by
          intro j hj hjlen
          by_cases heq : j = mid
          · subst j
            rw [hlistval]
            omega
          · have hmj : mid < j := by omega
            by_cases hjh : hi ≤ j
            · exact habove j hjh hjlen
            · have hrel : xs.toList.getD mid 0 ≤ xs.toList.getD j 0 := by
                have hpair := hs.rel_getElem_of_lt hmidlen hjlen hmj
                rw [← getD_eq_getElem hmidlen, ← getD_eq_getElem hjlen] at hpair
                exact hpair
              rw [hlistval] at hrel
              omega
        have hf' : mid - lo < 2 ^ fuel := by
          rw [hpow] at hf
          omega
        have hres := ih lo mid hml (by omega) hf' hbelow habove'
        obtain ⟨hc, hlb, hub⟩ := hres
        exact ⟨hc, hlb, by omega⟩

private theorem width_pow_bound (n : Nat) : n < 2 ^ (width n + 1) := by
  unfold width
  by_cases h : n == 0
  · have hn : n = 0 := by simpa using h
    subst hn
    simp
  · have hne : n ≠ 0 := by intro hz; simp [hz] at h
    simp [h]
    have hlt : n < 2 ^ (n.log2 + 1) :=
      (Nat.log2_lt hne).mp (Nat.lt_succ_self _)
    have hpow : (2 : Nat) ^ (n.log2 + 1 + 1) = 2 * 2 ^ (n.log2 + 1) := by
      rw [Nat.pow_succ]; omega
    rw [hpow]
    omega

private theorem lower_cut {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    Cut ys v (lower ys v) ∧ lower ys v ≤ ys.length := by
  unfold lower
  have hf : ys.length - 0 < 2 ^ (width ys.length + 1) := by
    have := width_pow_bound ys.length
    omega
  have h := searchBound_cut (xs := ys.toArray) v (width ys.length + 1) 0 ys.length
    (Nat.zero_le _) (by simp) hf
    (by intro j hj; omega)
    (by intro j hj hjlen
        have hjl : j < ys.length := by simpa using hjlen
        omega)
    (by simpa using hs)
  simpa using h

private theorem lower_le_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v ≤ ys.length := (lower_cut hs v).2

private theorem lower_lt_of_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hi : i < lower ys v) : ys.getD i 0 < v := (lower_cut hs v).1.1 i hi

private theorem lower_le_of_ge {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) {i : Nat}
    (hj : lower ys v ≤ i) (hi : i < ys.length) : v ≤ ys.getD i 0 :=
  (lower_cut hs v).1.2 i hj hi

private theorem lower_mono {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) : lower ys a ≤ lower ys b := by
  apply Nat.le_of_not_gt
  intro h
  have hb := lower_lt_of_lt hs a h
  have ha := lower_le_of_ge hs b (Nat.le_refl _)
    (Nat.lt_of_lt_of_le h (lower_le_length hs a))
  omega

private theorem lower_eq_filter_lt_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    lower ys v = (ys.filter (fun x => decide (x < v))).length := by
  have hle : lower ys v ≤ ys.length := lower_le_length hs v
  have htake : (ys.take (lower ys v)).filter (fun x => decide (x < v)) = ys.take (lower ys v) := by
    rw [List.filter_eq_self]
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_take]
    have hiys : i < ys.length := by omega
    have : ys.getD i 0 < v := lower_lt_of_lt hs v (Nat.lt_of_lt_of_le hi (Nat.min_le_left _ _))
    rwa [getD_eq_getElem hiys] at this
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := hi
      rw [List.length_drop] at hlen
      omega
    intro hlt
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) hiys
    rw [getD_eq_getElem hiys] at hge
    omega
  have hsplit := (List.take_append_drop (lower ys v) ys).symm
  calc
    lower ys v = (ys.take (lower ys v)).length := by
      rw [List.length_take]; simp [Nat.min_eq_left hle]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = ((ys.take (lower ys v)).filter (fun x => decide (x < v)) ++
          (ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [hdrop, List.append_nil]
    _ = ((ys.take (lower ys v) ++ ys.drop (lower ys v)).filter (fun x => decide (x < v))).length := by
        rw [List.filter_append]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [← hsplit]

private theorem drop_take_eq_take_drop {α : Type} (xs : List α) (i j : Nat) (hij : i ≤ j) :
    (xs.drop i).take (j - i) = (xs.take j).drop i := by
  induction xs generalizing i j with
  | nil => simp
  | cons x xs ih =>
    cases i with
    | zero => simp
    | succ i =>
      cases j with
      | zero => omega
      | succ j =>
        simp only [List.drop_succ_cons, List.take_succ_cons, Nat.succ_sub_succ]
        exact ih i j (by omega)

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) {a b : Nat}
    (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++
        ys.filter (fun y => decide (a ≤ y) && decide (y < b)) := by
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
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hxa, hxb, hax, hnil, ih hs']
      · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        have hnil_ab : xs.filter (fun y => decide (a ≤ y) && decide (y < b)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy
          simp only [Bool.and_eq_true, decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

private theorem take_filter_prefix {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    ys.take (ys.filter (fun x => decide (x < v))).length =
      ys.filter (fun x => decide (x < v)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
    have hs' := hs.tail
    by_cases hx : x < v
    · simp [hx, ih hs']
    · have hnil : xs.filter (fun y => decide (y < v)) = [] := by
        rw [List.filter_eq_nil_iff]
        intro y hy
        simp only [decide_eq_true_eq]
        have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
        omega
      simp [hx, hnil]

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => decide (a ≤ y) && decide (y < b)) := by
  unfold slice
  by_cases hab : a ≤ b
  · rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) (lower_mono hs hab),
      lower_eq_filter_lt_length hs a, lower_eq_filter_lt_length hs b,
      take_filter_prefix hs b, filter_lt_prefix_filter_lt hs hab, List.drop_left]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero]
    symm
    rw [List.filter_eq_nil_iff]
    intro y hy
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    omega

private theorem filter_member_cons_of_ordered {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} {rs : Ranges}
    (hb : ∀ p ∈ rs, b ≤ p.1) (_hrs : rs.Pairwise (fun p q => p.2 ≤ q.1)) :
    ys.filter (fun y => member ((a, b) :: rs) y) =
      ys.filter (fun y => decide (a ≤ y) && decide (y < b)) ++ ys.filter (fun y => member rs y) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
    have ih' := ih hs.tail
    have hPy : member ((a, b) :: rs) y =
        ((decide (a ≤ y) && decide (y < b)) || member rs y) := by
      rfl
    by_cases hp : (decide (a ≤ y) && decide (y < b)) = true
    · have hq : member rs y = false := by
        cases hmem : member rs y with
        | false => rfl
        | true =>
          simp only [member] at hmem
          rw [List.any_eq_true] at hmem
          rcases hmem with ⟨p, hpm, hpred⟩
          simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
          have hbp := hb p hpm
          have hp' : a ≤ y ∧ y < b := by simpa [Bool.and_eq_true] using hp
          omega
      simp only [List.filter_cons]
      rw [ih']
      simp [hPy, hp, hq]
    · have hp' : (decide (a ≤ y) && decide (y < b)) = false := by
        cases h : (decide (a ≤ y) && decide (y < b)) <;> simp_all
      by_cases hq : member rs y = true
      · have hyge : b ≤ y := by
          have hqmem := hq
          simp only [member] at hqmem
          rw [List.any_eq_true] at hqmem
          rcases hqmem with ⟨p, hpm, hpred⟩
          have hbp := hb p hpm
          simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
          omega
        have hnil : ys.filter (fun z => decide (a ≤ z) && decide (z < b)) = [] := by
          rw [List.filter_eq_nil_iff]
          intro z hz hzmem
          simp only [Bool.and_eq_true, decide_eq_true_eq] at hzmem
          have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
          omega
        simp only [List.filter_cons]
        rw [ih']
        simp [hPy, hp', hq, hnil]
      · have hq' : member rs y = false := by
          cases h : member rs y <;> simp_all
        simp only [List.filter_cons]
        rw [ih']
        simp [hPy, hp', hq']

private theorem filter_member_eq_flatMap {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    ys.filter (fun y => member rs y) =
      rs.flatMap (fun p => ys.filter (fun y => decide (p.1 ≤ y) && decide (y < p.2))) := by
  induction rs with
  | nil => simp [member]
  | cons p rs ih =>
    obtain ⟨a, b⟩ := p
    simp only [List.flatMap_cons]
    have hb : ∀ q ∈ rs, b ≤ q.1 := fun q hq => (List.pairwise_cons.mp hrs).1 q hq
    have hrs' := (List.pairwise_cons.mp hrs).2
    have hrs_wf' : ∀ q ∈ rs, q.1 ≤ q.2 := fun q hq => hrs_wf q (by simp [hq])
    rw [filter_member_cons_of_ordered hs hb hrs', ih hrs' hrs_wf']

private theorem seek_eq_filter_member {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    rs.flatMap (fun p => slice ys p.1 p.2) = ys.filter (fun y => member rs y) := by
  rw [filter_member_eq_flatMap hs hrs hrs_wf]
  congr 1
  funext p
  obtain ⟨a, b⟩ := p
  simp only
  exact slice_eq_filter hs a b

private theorem mem_mod_spans {u t x y : Nat} (hyu : y < u) :
    member (search.spans u t x) y ↔ (x % (t + 1) == y % (t + 1)) = true := by
  unfold search
  simp only [member]
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨p, hp, hpred⟩
    simp only [List.mem_map, List.mem_range] at hp
    rcases hp with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
    have hy : y = x % (t + 1) + i * (t + 1) := by omega
    subst hy
    simp [Nat.add_mul_mod_self_right]
  · intro h
    simp only [beq_iff_eq] at h
    have hw : 0 < t + 1 := by omega
    have hmul : (t + 1) * (y / (t + 1)) = (y / (t + 1)) * (t + 1) := Nat.mul_comm _ _
    have hdm := Nat.div_add_mod y (t + 1)
    rw [← h, hmul] at hdm
    have hy_eq : y = x % (t + 1) + (y / (t + 1)) * (t + 1) := by omega
    rw [List.any_eq_true]
    refine ⟨(x % (t + 1) + (y / (t + 1)) * (t + 1),
             x % (t + 1) + (y / (t + 1)) * (t + 1) + 1), ?_, ?_⟩
    · simp only [List.mem_map, List.mem_range]
      refine ⟨y / (t + 1), ?_, rfl⟩
      apply (Nat.lt_div_iff_mul_lt hw).mpr
      omega
    · simp only [decide_eq_true_eq, Bool.and_eq_true]
      constructor <;> omega

private theorem spans_ordered (u t x : Nat) :
    (search.spans u t x).Pairwise (fun p q => p.2 ≤ q.1) ∧
      ∀ p ∈ search.spans u t x, p.1 ≤ p.2 := by
  simp only [search]
  let w := t + 1
  let r := x % w
  have hw : 0 < w := by omega
  constructor
  · rw [List.pairwise_map]
    simp only [Function.comp]
    rw [List.pairwise_iff_getElem]
    intro i j hi hj hij
    rw [List.getElem_range hi, List.getElem_range hj]
    have hmul : (i + 1) * (t + 1) ≤ j * (t + 1) := Nat.mul_le_mul_right (t + 1) hij
    rw [Nat.succ_mul] at hmul
    omega
  · intro p hp
    simp only [List.mem_map, List.mem_range] at hp
    rcases hp with ⟨i, _, rfl⟩
    simp

theorem sound_OrderedProbe : ModuloClass.OrderedProbe.Goal := by
  change orderedProbe search
  unfold orderedProbe
  intro u t x ys hv
  have hs := valid_pairwise hv
  unfold seek
  obtain ⟨hrs, hwf⟩ := spans_ordered u t x
  rw [seek_eq_filter_member hs hrs hwf]
  apply List.filter_congr
  intro y hy
  have hyu := valid_lt hv hy
  have h := mem_mod_spans (u := u) (t := t) (x := x) (y := y) hyu
  cases hm : member (search.spans u t x) y <;>
    cases ht : search.test t x y <;> simp_all [search]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_OrderedProbe
