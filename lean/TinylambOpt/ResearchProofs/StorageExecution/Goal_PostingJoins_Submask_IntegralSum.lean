import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Submask.IntegralSum` — Subtraction on a stored prefix-sum column yields the right
  payload sum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_IntegralSum

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask
open TinylambOpt.ResearchGoals.Compression.Wire

/-! A self-contained certificate for `Submask.IntegralSum`.
The binary lower bound cuts the sorted key column exactly at each singleton
submask posting.  The fold that generates `submasks x` is shown to enumerate
exactly the submasks of `x`, in strictly increasing order; hence the spans are
ordered and disjoint and their coverage agrees with the predicate on `y < u`.
Then the generic interval/prefix-sum reduction applies. -/

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
        · rw [if_pos hv]
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
        · rw [if_neg hv]
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
      rw [Nat.pow_succ]
      omega
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
    have hlt : ys.getD i 0 < v := lower_lt_of_lt hs v (by simpa [Nat.min_eq_left hle] using hi)
    rwa [getD_eq_getElem hiys] at hlt
  have hdrop : (ys.drop (lower ys v)).filter (fun x => decide (x < v)) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_drop]
    have hiys : lower ys v + i < ys.length := by
      have hlen : i < (ys.drop (lower ys v)).length := by simpa using hi
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

private abbrev inR (a b y : Nat) : Bool := a ≤ y && y < b

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++
        ys.filter (fun x => inR a b x) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [inR, hxa, hxb, ih hs']
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [inR, hxa, hxb, hax, hnil, ih hs']
        · have hnil_a : xs.filter (fun y => decide (y < a)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [decide_eq_true_eq]
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          have hnil_ab : xs.filter (fun y => inR a b y) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro y hy
            simp only [inR, decide_eq_true_eq, Bool.and_eq_true]
            intro hay
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [inR, hxa, hxb, hnil_a, hnil_ab, ih hs']

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
          apply List.filter_eq_nil_iff.mpr
          intro y hy
          simp only [decide_eq_true_eq]
          have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
          omega
        simp [hx, hnil]

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => inR a b y) := by
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
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    simp only [inR, decide_eq_true_eq, Bool.and_eq_true]
    intro hay
    omega

private theorem sum_flatMap_nat {α : Type} (xs : List α) (f : α → List Nat) :
    (xs.flatMap f).sum = (xs.map (fun x => (f x).sum)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [List.flatMap_cons, List.sum_cons, ih, Nat.add_assoc]

private theorem take_sum_split (ys : List Nat) (p q : Nat) (hpq : p ≤ q) :
    (ys.take q).sum = (ys.take p).sum + ((ys.drop p).take (q - p)).sum := by
  have hlist : ys.take q = ys.take p ++ (ys.drop p).take (q - p) := by
    calc
      ys.take q = (ys.take q).take p ++ (ys.take q).drop p :=
        (List.take_append_drop p (ys.take q)).symm
      _ = ys.take p ++ (ys.drop p).take (q - p) := by
        rw [List.take_take, Nat.min_eq_left hpq, List.drop_take]
  rw [hlist, List.sum_append]

private theorem slice_sum {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    (slice ys a b).sum = sumAt ys (lower ys b) - sumAt ys (lower ys a) := by
  unfold slice sumAt
  by_cases hab : a ≤ b
  · have hmono := lower_mono hs hab
    have hsplit := take_sum_split ys (lower ys a) (lower ys b) hmono
    omega
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz, List.take_zero, List.sum_nil]
    have hsplit := take_sum_split ys (lower ys b) (lower ys a) hmono
    omega

private theorem total_eq_seek_sum {s : Search} {u t x : Nat} {ys : List Nat}
    (hs : ys.Pairwise (· ≤ ·)) :
    total s u t x ys = (seek s u t x ys).sum := by
  unfold total seek
  rw [sum_flatMap_nat]
  congr 1
  apply List.map_congr_left
  intro p hp
  obtain ⟨a, b⟩ := p
  simp [slice_sum hs]

private def SpansSpec (s : Search) : Prop :=
  ∀ u t x y, y < u → (member (s.spans u t x) y ↔ s.test t x y = true)

private def SpansOrdered (s : Search) : Prop :=
  ∀ u t x, (s.spans u t x).Pairwise (fun p q => p.2 ≤ q.1) ∧
    ∀ p ∈ s.spans u t x, p.1 ≤ p.2

private theorem member_cons {a b : Nat} {rs : Ranges} {y : Nat} :
    member ((a, b) :: rs) y = (inR a b y || member rs y) := by
  simp [member]

private theorem member_lower_bound {rs : Ranges} {b y : Nat}
    (hb : ∀ p ∈ rs, b ≤ p.1) (hm : member rs y = true) : b ≤ y := by
  unfold member at hm
  rw [List.any_eq_true] at hm
  rcases hm with ⟨p, hpm, hpred⟩
  have hbp := hb p hpm
  simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
  omega

private theorem filter_member_cons_of_ordered {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} {rs : Ranges}
    (hb : ∀ p ∈ rs, b ≤ p.1) (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1)) :
    ys.filter (fun y => member ((a, b) :: rs) y) =
      ys.filter (fun y => inR a b y) ++ ys.filter (fun y => member rs y) := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have hs' : ys.Pairwise (· ≤ ·) := hs.tail
      have ih' := ih hs'
      by_cases h₁ : inR a b y = true
      · have hnot : ¬ member rs y = true := by
          intro hm
          have hby := member_lower_bound hb hm
          simp only [inR, decide_eq_true_eq, Bool.and_eq_true] at h₁
          omega
        have h₂ : member rs y = false := by simpa [hnot]
        simp [member_cons, h₁, h₂]
        exact ih'
      · by_cases h₂ : member rs y = true
        · have hby : b ≤ y := member_lower_bound hb h₂
          have hnil : ys.filter (fun z => inR a b z) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro z hz
            simp only [inR, decide_eq_true_eq, Bool.and_eq_true]
            intro haz
            have hyz : y ≤ z := List.rel_of_pairwise_cons hs hz
            omega
          simpa [member_cons, h₁, h₂, hnil] using ih'
        · simp [member_cons, h₁, h₂]
          exact ih'

private theorem filter_member_eq_flatMap {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    ys.filter (fun y => member rs y) =
      rs.flatMap (fun (a, b) => ys.filter (fun y => inR a b y)) := by
  induction rs with
  | nil => simp [member]
  | cons p rs ih =>
      obtain ⟨a, b⟩ := p
      simp only [List.flatMap_cons]
      have hb : ∀ q ∈ rs, b ≤ q.1 := fun q hq => (List.pairwise_cons.mp hrs).1 q hq
      have hrs' := (List.pairwise_cons.mp hrs).2
      have hrs_wf' : ∀ q ∈ rs, q.1 ≤ q.2 :=
        fun q hq => hrs_wf q (List.mem_cons_of_mem _ hq)
      rw [filter_member_cons_of_ordered hs hb hrs', ih hrs' hrs_wf']

private theorem seek_eq_filter_member {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {rs : Ranges} (hrs : rs.Pairwise (fun p q => p.2 ≤ q.1))
    (hrs_wf : ∀ p ∈ rs, p.1 ≤ p.2) :
    rs.flatMap (fun (a, b) => slice ys a b) = ys.filter (fun y => member rs y) := by
  rw [filter_member_eq_flatMap hs hrs hrs_wf]
  congr 1
  funext p
  obtain ⟨a, b⟩ := p
  exact slice_eq_filter hs a b

private theorem integralSum_of_spec {s : Search}
    (hspec : SpansSpec s) (hord : SpansOrdered s) : integralSum s := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  rw [total_eq_seek_sum hs]
  unfold seek
  obtain ⟨hrs, hwf⟩ := hord u t x
  rw [seek_eq_filter_member hs hrs hwf]
  congr 1
  apply List.filter_congr
  intro y hy
  by_cases hm : member (s.spans u t x) y
  · have ht := (hspec u t x y (valid_lt hv hy)).mp hm
    simp [hm, ht]
  · have ht : s.test t x y ≠ true := by
      intro ht
      exact hm ((hspec u t x y (valid_lt hv hy)).mpr ht)
    simp [hm, ht]

/-! The generation of submasks. -/

private def Good (bs acc : List Nat) : Prop :=
  acc.Pairwise (· < ·) ∧
  (∀ y ∈ acc, ∀ i, y.testBit i = true → i ∈ bs) ∧
  (∀ y, (∀ i, y.testBit i = true → i ∈ bs) → y ∈ acc)

private theorem good_nil : Good [] [0] := by
  constructor
  · simp
  constructor
  · intro y hy i hi
    simp only [List.mem_singleton] at hy
    subst y
    simpa using hi
  · intro y hy
    have hy0 : y = 0 := by
      apply Nat.eq_of_testBit_eq
      intro i
      have hfalse : y.testBit i = false := by
        by_cases h : y.testBit i = true
        · have := hy i h
          simpa using this
        · simpa using h
      simp [hfalse]
    simp [hy0]

private theorem good_step {bs acc : List Nat} {b : Nat}
    (hbsb : ∀ i ∈ bs, i < b) (hgood : Good bs acc) :
    Good (bs ++ [b]) (acc ++ acc.map (fun y => y + 2 ^ b)) := by
  obtain ⟨hinc, hsub, hcompl⟩ := hgood
  have hsmall : ∀ y ∈ acc, y < 2 ^ b := by
    intro y hy
    apply Nat.lt_pow_two_of_testBit
    intro i hib
    by_cases hti : y.testBit i = true
    · have hibs := hsub y hy i hti
      have hilt := hbsb i hibs
      omega
    · simpa using hti
  have hmapinc : (acc.map (fun y => y + 2 ^ b)).Pairwise (· < ·) := by
    rw [List.pairwise_map]
    exact hinc.imp (by intro a c hac; omega)
  have hcross : ∀ a ∈ acc, ∀ c ∈ acc.map (fun y => y + 2 ^ b), a < c := by
    intro a ha c hc
    rcases List.mem_map.mp hc with ⟨z, hz, rfl⟩
    have haz := hsmall a ha
    omega
  constructor
  · rw [List.pairwise_append]
    exact ⟨hinc, hmapinc, hcross⟩
  constructor
  · intro y hy i hi
    simp only [List.mem_append] at hy
    rcases hy with hy | hy
    · have hib := hsub y hy i hi
      simp [hib]
    · rcases List.mem_map.mp hy with ⟨z, hz, rfl⟩
      have hzor : z + 2 ^ b = z ||| 2 ^ b := (Nat.or_two_pow_eq_add_of_lt (hsmall z hz)).symm
      rw [hzor, Nat.testBit_or, Nat.testBit_two_pow] at hi
      by_cases hbi : b = i
      · simp [hbi]
      · have hzi : z.testBit i = true := by
          cases h1 : z.testBit i <;> cases h2 : decide (b = i) <;> simp_all
        have hiz := hsub z hz i hzi
        simp [hiz, hbi]
  · intro y hybits
    by_cases hyb : y.testBit b = true
    · let z := y &&& (2 ^ b - 1)
      have hztwo : 2 ^ b - 1 < 2 ^ b := by
        have := Nat.two_pow_pos b
        omega
      have hzsmall : z < 2 ^ b := by
        exact Nat.and_lt_two_pow y hztwo
      have hzsub : ∀ i, z.testBit i = true → i ∈ bs := by
        intro i hzi
        have hzi' : i < b ∧ y.testBit i = true := by
          simpa [z, Nat.testBit_and] using hzi
        have hyi : y.testBit i = true := hzi'.2
        have hyibs : i ∈ bs ++ [b] := hybits i hyi
        have hib : i < b := hzi'.1
        simp only [List.mem_append, List.mem_singleton] at hyibs
        rcases hyibs with h | h
        · exact h
        · omega
      have hzmem : z ∈ acc := hcompl z hzsub
      have hyeq : y = z + 2 ^ b := by
        apply Nat.eq_of_testBit_eq
        intro i
        have hzor : z + 2 ^ b = z ||| 2 ^ b := (Nat.or_two_pow_eq_add_of_lt hzsmall).symm
        rw [hzor, Nat.testBit_or, Nat.testBit_two_pow]
        by_cases hib : i < b
        · have hne : b ≠ i := by omega
          have hzbit : z.testBit i = y.testBit i := by
            simp [z, Nat.testBit_and, hib]
          simp [hzbit, hne]
        · by_cases hbi : b = i
          · subst i
            simp [z, Nat.testBit_and, hyb]
          · have hyi : y.testBit i = false := by
              by_cases hbit : y.testBit i = true
              · have hiin := hybits i hbit
                simp only [List.mem_append, List.mem_singleton] at hiin
                rcases hiin with h | h
                · have := hbsb i h
                  omega
                · omega
              · simpa using hbit
            have hzbit : z.testBit i = false := by
              simp [z, Nat.testBit_and, hib]
            simp [hyi, hzbit, hbi]
      have hymap : y ∈ acc.map (fun w => w + 2 ^ b) := List.mem_map.mpr ⟨z, hzmem, hyeq.symm⟩
      exact List.mem_append_right acc hymap
    · have hybs : ∀ i, y.testBit i = true → i ∈ bs := by
        intro i hi
        have hiin := hybits i hi
        simp only [List.mem_append, List.mem_singleton] at hiin
        rcases hiin with h | h
        · exact h
        · subst i
          exact False.elim (by simpa [hyb] using hi)
      have hyacc : y ∈ acc := hcompl y hybs
      exact List.mem_append_left (acc.map (fun w => w + 2 ^ b)) hyacc

private theorem good_fold_from {bs bl acc : List Nat}
    (hinc_bs : bs.Pairwise (· < ·)) (hinc_bl : bl.Pairwise (· < ·))
    (hsep : ∀ a ∈ bs, ∀ c ∈ bl, a < c) (hacc : Good bs acc) :
    Good (bs ++ bl) (bl.foldl (fun s bit => s ++ s.map (fun y => y + 2 ^ bit)) acc) := by
  induction bl generalizing bs acc with
  | nil => simpa using hacc
  | cons b bl ih =>
      have hb_bs : ∀ a ∈ bs, a < b := fun a ha => hsep a ha b (by simp)
      have hstep : Good (bs ++ [b])
          (acc ++ acc.map (fun y => y + 2 ^ b)) := good_step hb_bs hacc
      have hinc_bs' : (bs ++ [b]).Pairwise (· < ·) := by
        rw [List.pairwise_append]
        exact ⟨hinc_bs, by simp, by
          intro a ha c hc
          simp at hc
          subst c
          exact hb_bs a ha⟩
      have hinc_bl' : bl.Pairwise (· < ·) := (List.pairwise_cons.mp hinc_bl).2
      have hsep' : ∀ a ∈ bs ++ [b], ∀ c ∈ bl, a < c := by
        intro a ha c hc
        simp only [List.mem_append, List.mem_singleton] at ha
        rcases ha with ha | ha
        · exact hsep a ha c (by simp [hc])
        · subst a
          exact (List.pairwise_cons.mp hinc_bl).1 c hc
      have hres := ih hinc_bs' hinc_bl' hsep' hstep
      simpa [List.append_assoc] using hres

private theorem pairwise_range_lt (n : Nat) : (List.range n).Pairwise (· < ·) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ]
      rw [List.pairwise_append]
      exact ⟨ih, by simp, by intro a ha c hc; simp at hc; subst c; simpa using ha⟩

private theorem submasks_good (x : Nat) :
    Good ((List.range (width x)).filter (fun b => x.testBit b)) (submasks x) := by
  unfold submasks
  simpa only [List.nil_append] using
    (good_fold_from (bs := []) (bl := (List.range (width x)).filter (fun b => x.testBit b))
      (acc := [0]) (by simp)
      ((pairwise_range_lt (width x)).filter (fun b => x.testBit b))
      (by simp) good_nil)

private theorem bit_lt_width {x i : Nat} (hi : x.testBit i = true) : i < width x := by
  have h2le : 2 ^ i ≤ x := Nat.ge_two_pow_of_testBit hi
  have hpos : 0 < 2 ^ i := Nat.two_pow_pos i
  have hxne : x ≠ 0 := by omega
  unfold width
  simp [hxne]
  have hlog : i ≤ x.log2 := (Nat.le_log2 hxne).2 h2le
  omega

private theorem mem_submasks {x y : Nat} :
    y ∈ submasks x ↔ ∀ i, y.testBit i = true → x.testBit i = true := by
  let bs := (List.range (width x)).filter (fun b => x.testBit b)
  have hgood := submasks_good x
  constructor
  · intro hy i hi
    have hib := hgood.2.1 y hy i hi
    have hib' : i < width x ∧ x.testBit i = true := by simpa [bs] using hib
    exact hib'.2
  · intro h
    have hsub : ∀ i, y.testBit i = true → i ∈ bs := by
      intro i hi
      have hxi := h i hi
      have hiw := bit_lt_width hxi
      simpa [bs, hiw, hxi]
    have hyin := hgood.2.2 y hsub
    simpa [bs] using hyin

private theorem land_eq_self_of_sub {x y : Nat}
    (hsub : ∀ i, y.testBit i = true → x.testBit i = true) : y &&& x = y := by
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_and]
  by_cases hy : y.testBit i
  · simp [hy, hsub i hy]
  · simp [hy]

private theorem sub_of_land_eq_self {x y : Nat} (h : y &&& x = y) :
    ∀ i, y.testBit i = true → x.testBit i = true := by
  intro i hi
  have ht := congrArg (fun n : Nat => n.testBit i) h
  rw [Nat.testBit_and] at ht
  by_cases hxi : x.testBit i
  · exact hxi
  · have : y.testBit i = false := by simpa [hxi] using ht.symm
    simp_all

private theorem mem_submask_spans {u t x y : Nat} (hyu : y < u) :
    member (search.spans u t x) y ↔ search.test t x y = true := by
  unfold search
  constructor
  · intro hm
    unfold member at hm
    rw [List.any_eq_true] at hm
    rcases hm with ⟨p, hp, hpred⟩
    simp only [List.mem_map] at hp
    rcases hp with ⟨z, hz, rfl⟩
    have hz' : z ∈ submasks x ∧ z < u ∧ z ≤ t := by
      simpa [decide_eq_true_eq, Bool.and_eq_true] using hz
    simp only [decide_eq_true_eq, Bool.and_eq_true] at hpred
    have hzy : z = y := by omega
    subst z
    have hzsub := (mem_submasks (x := x) (y := y)).mp hz'.1
    have hland : y &&& x = y := land_eq_self_of_sub hzsub
    have hyt : y ≤ t := by simpa using hz'.2.2
    simp [hland, hyt]
  · intro ht
    simp only [Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at ht
    have hland : y &&& x = y := ht.1
    have hyt : y ≤ t := ht.2
    have hzsub := sub_of_land_eq_self hland
    have hzmem : y ∈ submasks x := (mem_submasks (x := x) (y := y)).mpr hzsub
    unfold member
    rw [List.any_eq_true]
    refine ⟨(y, y + 1), ?_, ?_⟩
    · rw [List.mem_map]
      refine ⟨y, ?_, by simp⟩
      simp [hzmem, hyu, hyt]
    · simp

private theorem spec : SpansSpec search := by
  intro u t x y hyu
  exact mem_submask_spans hyu

private theorem ordered : SpansOrdered search := by
  intro u t x
  have hgood := submasks_good x
  constructor
  · unfold search
    rw [List.pairwise_map]
    have hfilt := hgood.1.filter (fun y : Nat => decide (y < u && y ≤ t))
    exact hfilt.imp (by
      intro a c hac
      dsimp
      omega)
  · intro p hp
    simp only [search, List.mem_map] at hp
    rcases hp with ⟨y, hy, rfl⟩
    simp

theorem sound_IntegralSum :
    TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask.IntegralSum.Goal :=
  integralSum_of_spec spec ordered

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_IntegralSum
