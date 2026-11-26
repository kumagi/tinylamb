import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_OrderedProbe

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask
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

-- Enumerating submasks by the foldl pass.

private def step (acc : List Nat) (b : Nat) : List Nat :=
  acc ++ acc.map (fun z => z + 2 ^ b)

private def GenInv (acc done rest : List Nat) : Prop :=
  (∀ z ∈ acc, ∀ i, z.testBit i = true → i ∈ done) ∧
  (∀ z, (∀ i, z.testBit i = true → i ∈ done) → z ∈ acc) ∧
  acc.Pairwise (· < ·) ∧
  done.Pairwise (· < ·) ∧
  (∀ d ∈ done, ∀ r ∈ rest, d < r) ∧
  rest.Pairwise (· < ·)

private theorem gen_step {acc done rest : List Nat} {b : Nat}
    (h : GenInv acc done (b :: rest)) :
    GenInv (step acc b) (done ++ [b]) rest := by
  rcases h with ⟨hA, hB, hC, hD, hE, hF⟩
  have hb_lt : ∀ r ∈ rest, b < r := (List.pairwise_cons.mp hF).1
  have hdone_lt_b : ∀ d ∈ done, d < b := fun d hd => hE d hd b (by simp)
  have bound_of_bits : ∀ z, (∀ i, z.testBit i = true → i ∈ done) → z < 2 ^ b := by
    intro z hz
    apply Nat.lt_pow_two_of_testBit
    intro i hi
    by_cases hzi : z.testBit i = true
    · have hib := hdone_lt_b i (hz i hzi)
      omega
    · simpa using hzi
  have hA' : ∀ z ∈ step acc b, ∀ i, z.testBit i = true → i ∈ done ++ [b] := by
    intro z hz i hzi
    simp only [step, List.mem_append, List.mem_map] at hz
    rcases hz with hza | ⟨a, ha, rfl⟩
    · exact List.mem_append_left _ (hA z hza i hzi)
    · have habound : a < 2 ^ b := bound_of_bits a (hA a ha)
      have hor : a ||| 2 ^ b = a + 2 ^ b := Nat.or_two_pow_eq_add_of_lt habound
      rw [← hor] at hzi
      simp only [Nat.testBit_or, Bool.or_eq_true] at hzi
      rcases hzi with hzi | hzi
      · exact List.mem_append_left _ (hA a ha i hzi)
      · have hib : b = i := by simpa [Nat.testBit_two_pow] using hzi
        simp [hib]
  have hB' : ∀ z, (∀ i, z.testBit i = true → i ∈ done ++ [b]) → z ∈ step acc b := by
    intro z hz
    by_cases hzb : z.testBit b = true
    · let a := z ^^^ 2 ^ b
      have ha_bits : ∀ i, a.testBit i = true → i ∈ done := by
        intro i hi
        have hxor : a.testBit i = (z.testBit i ^^ (2 ^ b).testBit i) := by
          dsimp [a]
          exact Nat.testBit_xor z (2 ^ b) i
        rw [hxor] at hi
        by_cases hib : i = b
        · subst i
          simp [hzb] at hi
        · have hbi : b ≠ i := by omega
          have hpowfalse : (2 ^ b).testBit i = false := by
            simp [Nat.testBit_two_pow, hbi]
          have hzi : z.testBit i = true := by simpa [hpowfalse] using hi
          have hi_done' := hz i hzi
          simp only [List.mem_append, List.mem_singleton] at hi_done'
          rcases hi_done' with hid | hibeq
          · exact hid
          · omega
      have ha_in : a ∈ acc := hB a ha_bits
      have habound : a < 2 ^ b := bound_of_bits a ha_bits
      have hor : a ||| 2 ^ b = a + 2 ^ b := Nat.or_two_pow_eq_add_of_lt habound
      have haeq : a + 2 ^ b = z := by
        apply Nat.eq_of_testBit_eq
        intro i
        have hxor : a.testBit i = (z.testBit i ^^ (2 ^ b).testBit i) := by
          dsimp [a]
          exact Nat.testBit_xor z (2 ^ b) i
        by_cases hipow : (2 ^ b).testBit i = true
        · have hib : b = i := by simpa [Nat.testBit_two_pow] using hipow
          subst i
          rw [← hor, Nat.testBit_or]
          have hab : a.testBit b = false := by
            have hx := hxor
            simpa [hzb] using hx
          simp [hab, hzb]
        · have haeq_i : a.testBit i = z.testBit i := by simp [hxor, hipow]
          rw [← hor, Nat.testBit_or]
          simp [haeq_i, hipow]
      simp only [step, List.mem_append, List.mem_map]
      exact Or.inr ⟨a, ha_in, haeq⟩
    · have hbits_done : ∀ i, z.testBit i = true → i ∈ done := by
        intro i hi
        have hid' := hz i hi
        simp only [List.mem_append, List.mem_singleton] at hid'
        rcases hid' with hid | hib
        · exact hid
        · subst i
          exact (hzb hi).elim
      have hz_in := hB z hbits_done
      simp only [step, List.mem_append]
      exact Or.inl hz_in
  have hC' : (step acc b).Pairwise (· < ·) := by
    simp only [step]
    rw [List.pairwise_append]
    refine ⟨hC, ?_, ?_⟩
    · rw [List.pairwise_map]
      exact hC.imp (by intro x y hxy; omega)
    · intro a ha m hm
      simp only [List.mem_map] at hm
      rcases hm with ⟨a', ha', rfl⟩
      have hab := bound_of_bits a (hA a ha)
      omega
  have hD' : (done ++ [b]).Pairwise (· < ·) := by
    rw [List.pairwise_append]
    refine ⟨hD, by simp, ?_⟩
    intro d hd s hs
    simp only [List.mem_singleton] at hs
    subst s
    exact hdone_lt_b d hd
  have hE' : ∀ d ∈ done ++ [b], ∀ r ∈ rest, d < r := by
    intro d hd r hr
    simp only [List.mem_append, List.mem_singleton] at hd
    rcases hd with hd | rfl
    · have hdb := hE d hd b (by simp)
      have hbr := hb_lt r hr
      omega
    · exact hb_lt r hr
  exact ⟨hA', hB', hC', hD', hE', (List.pairwise_cons.mp hF).2⟩

private theorem genInv_base {bs : List Nat} (hbs : bs.Pairwise (· < ·)) : GenInv [0] [] bs := by
  refine ⟨?_, ?_, by simp, by simp, by simp, hbs⟩
  · intro z hz i hi
    simp at hz
    subst z
    simp at hi
  · intro z hz
    have zeq : z = 0 := by
      apply Nat.eq_of_testBit_eq
      intro i
      by_cases hzi : z.testBit i = true
      · exact False.elim (by simpa using hz i hzi)
      · simpa using hzi
    simp [zeq]

private theorem genInv_foldl {acc done bs : List Nat}
    (hbs : bs.Pairwise (· < ·)) (hinv : GenInv acc done bs) :
    GenInv (bs.foldl step acc) (done ++ bs) [] := by
  induction bs generalizing acc done with
  | nil => simpa using hinv
  | cons b bs ih =>
      have hbs' : bs.Pairwise (· < ·) := (List.pairwise_cons.mp hbs).2
      have hinv' : GenInv (step acc b) (done ++ [b]) bs := gen_step hinv
      have happ : (done ++ [b]) ++ bs = done ++ (b :: bs) := by simp [List.append_assoc]
      simpa [happ] using ih hbs' hinv'

private theorem gen_bits_spec {bs : List Nat} (hbs : bs.Pairwise (· < ·)) :
    GenInv (bs.foldl step [0]) bs [] := by
  simpa using genInv_foldl hbs (genInv_base hbs)

private theorem mem_gen_bits {bs : List Nat} (hbs : bs.Pairwise (· < ·)) (z : Nat) :
    z ∈ bs.foldl step [0] ↔ ∀ i, z.testBit i = true → i ∈ bs := by
  have h := gen_bits_spec hbs
  exact ⟨fun hz i hi => h.1 z hz i hi, fun hz => h.2.1 z hz⟩

private theorem pairwise_gen_bits {bs : List Nat} (hbs : bs.Pairwise (· < ·)) :
    (bs.foldl step [0]).Pairwise (· < ·) := (gen_bits_spec hbs).2.2.1

private theorem range_pairwise (n : Nat) : (List.range n).Pairwise (· < ·) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.pairwise_append]
      refine ⟨ih, by simp, ?_⟩
      intro a ha b hb
      simp only [List.mem_singleton] at hb
      subst b
      exact List.mem_range.mp ha

private theorem bits_pairwise (x : Nat) :
    ((List.range (width x)).filter x.testBit).Pairwise (· < ·) := by
  rw [List.pairwise_filter]
  exact (range_pairwise (width x)).imp (by intro a b h _ _; exact h)

private theorem width_bound (x : Nat) : x < 2 ^ width x := by
  unfold width
  by_cases h : x == 0
  · have hx : x = 0 := by simpa using h
    subst x
    simp
  · have hxne : x ≠ 0 := by intro hz; simp [hz] at h
    simpa [h] using (Nat.log2_lt hxne).mp (Nat.lt_succ_self x.log2)

private theorem and_eq_self_iff_bits (x y : Nat) :
    (y &&& x = y) ↔ ∀ i, y.testBit i = true → x.testBit i = true := by
  constructor
  · intro h i hi
    have htest := congrArg (fun n : Nat => n.testBit i) h
    simpa [Nat.testBit_and, hi] using htest
  · intro h
    apply Nat.eq_of_testBit_eq
    intro i
    simp only [Nat.testBit_and]
    by_cases hi : y.testBit i = true
    · simp [hi, h i hi]
    · simp [hi]

private theorem submasks_eq_step (x : Nat) :
    submasks x = ((List.range (width x)).filter x.testBit).foldl step [0] := by
  rfl

private theorem mem_submasks_iff_bits (x y : Nat) :
    y ∈ submasks x ↔ ∀ i, y.testBit i = true → x.testBit i = true := by
  let bs := (List.range (width x)).filter x.testBit
  have hbs : bs.Pairwise (· < ·) := bits_pairwise x
  have hspec : ∀ z, z ∈ submasks x ↔ ∀ i, z.testBit i = true → i ∈ bs := by
    intro z
    rw [submasks_eq_step]
    simpa [bs] using mem_gen_bits hbs z
  constructor
  · intro hy
    have hbits := (hspec y).mp hy
    intro i hi
    have hi_bs := hbits i hi
    exact (List.mem_filter.mp hi_bs).2
  · intro hbits
    apply (hspec y).mpr
    intro i hi
    rw [List.mem_filter]
    refine ⟨?_, hbits i hi⟩
    rw [List.mem_range]
    have hge := Nat.ge_two_pow_of_testBit (hbits i hi)
    have hxlt := width_bound x
    have hp : 2 ^ i < 2 ^ width x := by omega
    exact (Nat.pow_lt_pow_iff_right (by decide : 1 < 2)).mp hp

private theorem mem_spans_iff (u t x y : Nat) (hyu : y < u) :
    member (search.spans u t x) y ↔ (y &&& x) == y && y ≤ t := by
  unfold search
  unfold member
  rw [List.any_eq_true]
  constructor
  · intro h
    rcases h with ⟨p, hp, hpred⟩
    simp only [List.mem_map] at hp
    rcases hp with ⟨z, hz, rfl⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
    have hzy : z = y := by omega
    subst z
    have hz' := List.mem_filter.mp hz
    have hbits := (mem_submasks_iff_bits x y).mp hz'.1
    have hand : y &&& x = y := (and_eq_self_iff_bits x y).2 hbits
    have hle : y ≤ t := by
      have hpair : y < u ∧ y ≤ t := by simpa [Bool.and_eq_true] using hz'.2
      exact hpair.2
    simp [hand, hle]
  · intro h
    simp only [Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h
    have hmem : y ∈ submasks x := (mem_submasks_iff_bits x y).mpr ((and_eq_self_iff_bits x y).1 h.1)
    refine ⟨(y, y + 1), ?_, ?_⟩
    · simp only [List.mem_map]
      exact ⟨y, List.mem_filter.mpr ⟨hmem, by simp [hyu, h.2]⟩, rfl⟩
    · simp only [Bool.and_eq_true, decide_eq_true_eq]
      omega

private theorem spans_ordered (u t x : Nat) :
    (search.spans u t x).Pairwise (fun p q => p.2 ≤ q.1) ∧
      ∀ p ∈ search.spans u t x, p.1 ≤ p.2 := by
  unfold search
  let bs := (List.range (width x)).filter x.testBit
  have hbs : bs.Pairwise (· < ·) := bits_pairwise x
  have hpair_sub : (submasks x).Pairwise (· < ·) := by
    rw [submasks_eq_step]
    simpa [bs] using pairwise_gen_bits hbs
  have hpair_filt : ((submasks x).filter (fun y => y < u && y ≤ t)).Pairwise (· < ·) := by
    rw [List.pairwise_filter]
    exact hpair_sub.imp (by intro a b h _ _; exact h)
  constructor
  · rw [List.pairwise_map]
    exact hpair_filt.imp (by intro a b hab; simp; omega)
  · intro p hp
    simp only [List.mem_map] at hp
    rcases hp with ⟨y, hy, rfl⟩
    simp

theorem sound_OrderedProbe : Submask.OrderedProbe.Goal := by
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
  have h := mem_spans_iff u t x y hyu
  cases hm : member (search.spans u t x) y <;>
    cases ht : search.test t x y <;> simp_all [search]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_OrderedProbe
