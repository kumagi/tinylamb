import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_RepeatedProbe

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Submask

private def allLists (n u : Nat) : List (List Nat) :=
  match n with
  | 0 => [[]]
  | k + 1 => [[]] ++ (List.range u).flatMap (fun y => (allLists k u).map (fun ys => y :: ys))

private def checkSmall : Bool :=
  (List.range 7).all fun u =>
  (List.range 4).all fun t =>
  (allLists 3 u).all fun ys =>
  (allLists 3 7).all fun xs =>
    if ys.Pairwise (· ≤ ·) && ys.all (fun y => y < u) then
      (xs.eraseDups.map (fun x => xs.count x * fan search u t x ys)).sum ==
        (xs.flatMap (fun x => ys.filter (search.test t x))).length
    else true

#eval checkSmall
#eval fan search 16 2 11 [0,2,3,7,8,9,11,15]

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

private theorem filter_lt_prefix_filter_lt {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.filter (fun x => decide (x < b)) =
      ys.filter (fun x => decide (x < a)) ++
        ys.filter (fun x => decide (a ≤ x && x < b)) := by
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
          rintro ⟨hay1, hay2⟩
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
        apply List.filter_eq_nil_iff.mpr
        intro y hy
        simp only [decide_eq_true_eq]
        have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
        omega
      simp [hx, hnil]

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => decide (a ≤ y && y < b)) := by
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
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    rintro ⟨hay1, hay2⟩
    omega

private theorem slice_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    (slice ys a b).length = lower ys b - lower ys a := by
  unfold slice
  rw [List.length_take, List.length_drop]
  have hla := lower_le_length hs a
  have hlb := lower_le_length hs b
  by_cases hab : a ≤ b
  · have hmono := lower_mono hs hab
    rw [Nat.min_eq_left (by omega : lower ys b - lower ys a ≤ ys.length - lower ys a)]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    have hz : lower ys b - lower ys a = 0 := by omega
    rw [hz]
    simp

private theorem eraseDups_nodup {α : Type} [DecidableEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := by
  induction h : xs.length using Nat.strongRecOn generalizing xs with
  | ind n ih =>
    cases xs with
    | nil => simp
    | cons a xs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro ha
        have hf := List.mem_eraseDups.mp ha
        have hm := List.mem_filter.mp hf
        simp at hm
      · apply ih ((xs.filter fun b => !(b == a)).length)
        · have hle := List.length_filter_le (l := xs) (p := fun b => !(b == a))
          have hn : n = xs.length + 1 := by simpa using h.symm
          omega
        · rfl

private theorem sum_map_perm {α : Type} (f : α → Nat) {l₁ l₂ : List α}
    (h : l₁.Perm l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction h with
  | nil => rfl
  | cons a h ih => simp [ih]
  | swap a b l => simp [Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_zero_map {α : Type} (xs : List α) (f : α → Nat)
    (h : ∀ x ∈ xs, f x = 0) : (xs.map f).sum = 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have hx := h x (by simp)
    have ht := ih (fun y hy => h y (by simp [hy]))
    simp [hx, ht]

private theorem sum_indicator_nodup {α : Type} [DecidableEq α] (xs : List α)
    (hnd : xs.Nodup) (a : α) (v : Nat) (ha : a ∈ xs) :
    (xs.map (fun x => if x = a then v else 0)).sum = v := by
  induction xs with
  | nil => simp at ha
  | cons b bs ih =>
    have hnd' := List.nodup_cons.mp hnd
    by_cases hba : b = a
    · subst b
      have hz := sum_zero_map bs (fun x => if x = a then v else 0)
        (fun x hx => by
          have hxa : x ≠ a := by
            intro heq
            subst x
            exact hnd'.1 hx
          simp [hxa])
      simp [hz]
    · have hmem : a ∈ bs := by
        grind
      simp [hba, ih hnd'.2 hmem]

private theorem sum_beq_indicator_nodup {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (hnd : xs.Nodup) (a : α) (v : Nat) (ha : a ∈ xs) :
    (xs.map (fun x => if a == x then v else 0)).sum = v := by
  calc
    (xs.map (fun x => if a == x then v else 0)).sum =
        (xs.map (fun x => if x = a then v else 0)).sum := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro x hx
      by_cases hxa : x = a
      · subst x
        simp
      · by_cases hbeq : a == x
        · exact False.elim (hxa (beq_iff_eq.mp hbeq).symm)
        · simp [hxa, hbeq]
    _ = v := sum_indicator_nodup xs hnd a v ha

private theorem count_cons_add {α : Type} [DecidableEq α] [LawfulBEq α]
    (a : α) (xs : List α) (x : α) :
    (a :: xs).count x = xs.count x + if x = a then 1 else 0 := by
  by_cases hxa : x = a
  · subst x
    simp [List.count_cons_self]
  · rw [List.count_cons_of_ne (Ne.symm hxa)]
    simp [hxa]

private theorem sum_count_mul_eq {α : Type} [DecidableEq α] [LawfulBEq α]
    (ds xs : List α) (f : α → Nat) (hnd : ds.Nodup)
    (hmem : ∀ x ∈ xs, x ∈ ds) :
    (ds.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  induction xs with
  | nil =>
      simpa using (sum_zero_map ds (fun _ => 0) (by simp))
  | cons a xs ih =>
    have hmem' : ∀ x ∈ xs, x ∈ ds := fun x hx => hmem x (by simp [hx])
    have ha : a ∈ ds := hmem a (by simp)
    have ih' := ih hmem'
    rw [List.map_cons, List.sum_cons, ← ih']
    rw [show ds.map (fun x => (a :: xs).count x * f x) =
        ds.map (fun x => xs.count x * f x + (if x = a then f a else 0)) from by
      apply List.map_congr_left
      intro x hx
      rw [count_cons_add]
      by_cases hxa : x = a <;> simp [hxa, Nat.add_mul]]
    rw [sum_map_add_nat, sum_indicator_nodup ds hnd a (f a) ha]
    omega

private theorem sum_map_eraseDups_count_mul {α : Type} [DecidableEq α] [LawfulBEq α]
    (xs : List α) (f : α → Nat) :
    (xs.eraseDups.map (fun x => xs.count x * f x)).sum = (xs.map f).sum := by
  exact sum_count_mul_eq xs.eraseDups xs f (eraseDups_nodup xs)
    (fun x hx => by simpa [List.mem_eraseDups] using hx)

private theorem mod_two_pow_add_lt {z b : Nat} (hmod : (z % 2^(b+1)).testBit b = false) :
    z % 2^(b+1) + 2^b < 2^(b+1) := by
  rw [Nat.testBit_eq_decide_div_mod_eq] at hmod
  have hp : ¬ z % 2^(b+1) / 2^b % 2 = 1 := by simpa using hmod
  have hlt := Nat.mod_lt z (Nat.pow_pos (by decide) : 0 < 2^(b+1))
  have hpow : 2^(b+1) = 2^b * 2 := by rw [Nat.pow_succ]
  rw [hpow] at hlt hp ⊢
  by_cases hc : z % (2^b * 2) < 2^b
  · omega
  · have hge : 1 * 2^b ≤ z % (2^b*2) := by simpa using (show 2^b ≤ z % (2^b*2) by omega)
    have hdiv : z % (2^b*2) / 2^b = 1 := by
      apply Nat.div_eq_of_lt_le hge
      simpa [Nat.succ_eq_add_one, Nat.mul_comm] using hlt
    rw [hdiv] at hp
    simp at hp

private theorem testBit_add_two_pow {z b i : Nat} (hz : z.testBit b = false) :
    (z + 2^b).testBit i = if i = b then true else z.testBit i := by
  have hzmod : (z % 2^(b+1)).testBit b = false := by
    rw [Nat.testBit_mod_two_pow]
    rw [show decide (b < b+1) = true by simp]
    simp [hz]
  have hrem : z % 2^(b+1) + 2^b < 2^(b+1) := mod_two_pow_add_lt hzmod
  have hsum : z + 2^b = 2^(b+1) * (z / 2^(b+1)) + (z % 2^(b+1) + 2^b) := by
    have hdm := Nat.div_add_mod z (2^(b+1))
    omega
  rw [hsum]
  rw [Nat.testBit_two_pow_mul_add (z / 2^(b+1)) hrem]
  by_cases hib : i < b + 1
  · rw [if_pos hib]
    by_cases hib' : i = b
    · subst i
      rw [if_pos rfl]
      rw [Nat.add_comm, Nat.testBit_two_pow_add_eq]
      simp [hzmod]
    · have hil : i < b := by omega
      have h1 : (z % 2^(b+1)).testBit i = z.testBit i := by
        rw [Nat.testBit_mod_two_pow]
        rw [show decide (i < b+1) = true by simp [Nat.lt_succ_of_lt hil]]
        simp
      have h2 : (z % 2^(b+1) + 2^b).testBit i = (z % 2^(b+1)).testBit i := by
        rw [Nat.add_comm, Nat.testBit_two_pow_add_gt hil]
      rw [h2, h1]
      rw [if_neg hib']
  · rw [if_neg hib]
    have hibge : b + 1 ≤ i := by omega
    have hne : i ≠ b := by omega
    rw [if_neg hne]
    rw [Nat.testBit_div_two_pow]
    simpa [Nat.sub_add_cancel hibge]

private theorem testBit_sub_two_pow_self {z b : Nat} (hz : z.testBit b = true) :
    (z - 2^b).testBit b = false := by
  rw [Nat.testBit_eq_decide_div_mod_eq]
  have hzdec : z / 2^b % 2 = 1 := by
    have := Nat.testBit_eq_decide_div_mod_eq (i:=b) (x:=z)
    simpa [hz] using this
  have hdiv : (z - 2^b) / 2^b = z / 2^b - 1 := by
    simpa [Nat.mul_one] using (Nat.sub_mul_div z (2^b) 1)
  rw [hdiv]
  have hbad : (z / 2^b - 1) % 2 ≠ 1 := by omega
  simp [hbad]

private theorem testBit_sub_two_pow_of_ne {z b i : Nat} (hz : z.testBit b = true) (hib : i ≠ b) :
    (z - 2^b).testBit i = z.testBit i := by
  have hz' : (z - 2^b).testBit b = false := testBit_sub_two_pow_self hz
  have hadd : (z - 2^b) + 2^b = z := Nat.sub_add_cancel (Nat.ge_two_pow_of_testBit hz)
  have h := testBit_add_two_pow (z := z - 2^b) (b := b) (i := i) hz'
  rw [hadd] at h
  rw [show (if i = b then true else (z - 2^b).testBit i) = (z - 2^b).testBit i by simp [hib]] at h
  exact h.symm

private abbrev step (acc : List Nat) (bit : Nat) : List Nat := acc ++ acc.map (fun z => z + 2^bit)

private theorem fold_sound (bs acc : List Nat) (top : Nat) (all : List Nat)
    (hmem : ∀ z ∈ acc, ∀ i, z.testBit i = true → i ∈ all)
    (hlt : ∀ z ∈ acc, z < 2^top)
    (hble : ∀ b ∈ bs, top ≤ b)
    (hbs : bs.Pairwise (· < ·))
    (hsub : ∀ b ∈ bs, b ∈ all) :
    ∀ y ∈ bs.foldl step acc, ∀ i, y.testBit i = true → i ∈ all := by
  revert acc top
  induction bs with
  | nil =>
      intro acc top hmem hlt hble y hy i hi
      exact hmem y hy i hi
  | cons b bs ih =>
      intro acc top hmem hlt hble y hy i hi
      have hbtop : top ≤ b := hble b (by simp)
      have hbmem : b ∈ all := hsub b (by simp)
      have hmem' : ∀ z ∈ step acc b, ∀ i, z.testBit i = true → i ∈ all := by
        intro z hz i hi
        simp only [step, List.mem_append, List.mem_map] at hz
        rcases hz with hz | ⟨z', hz', rfl⟩
        · exact hmem z hz i hi
        · by_cases hib : i = b
          · subst i; exact hbmem
          · have hz'lt : z' < 2^b := Nat.lt_of_lt_of_le (hlt z' hz') (Nat.pow_le_pow_right (by decide) hbtop)
            have hz'b : z'.testBit b = false := Nat.testBit_lt_two_pow hz'lt
            have hbit := testBit_add_two_pow (z := z') (b := b) (i := i) hz'b
            have hif : (if i = b then true else z'.testBit i) = z'.testBit i := by simp [hib]
            rw [hif] at hbit
            rw [hbit] at hi
            exact hmem z' hz' i hi
      have hlt' : ∀ z ∈ step acc b, z < 2^(b+1) := by
        intro z hz
        simp only [step, List.mem_append, List.mem_map] at hz
        rcases hz with hz | ⟨z', hz', rfl⟩
        · exact Nat.lt_of_lt_of_le (hlt z hz) (Nat.pow_le_pow_right (by decide) (by omega))
        · have hz'lt : z' < 2^b := Nat.lt_of_lt_of_le (hlt z' hz') (Nat.pow_le_pow_right (by decide) hbtop)
          have hp : 2^(b+1) = 2^b + 2^b := by rw [Nat.pow_succ]; omega
          rw [hp]
          omega
      have hble' : ∀ b' ∈ bs, b + 1 ≤ b' := by
        intro b' hb'
        have : b < b' := (List.pairwise_cons.mp hbs).1 b' hb'
        omega
      have hsub' : ∀ b' ∈ bs, b' ∈ all := by
        intro b' hb'
        exact hsub b' (List.mem_cons_of_mem b hb')
      exact ih (List.pairwise_cons.mp hbs).2 hsub' (acc := step acc b) (top := b+1) hmem' hlt' hble' y hy i hi

private theorem fold_nodup (bs acc : List Nat) (top : Nat) (hnd : acc.Nodup)
    (hlt : ∀ z ∈ acc, z < 2^top)
    (hble : ∀ b ∈ bs, top ≤ b)
    (hbs : bs.Pairwise (· < ·)) :
    (bs.foldl step acc).Nodup := by
  revert acc top
  induction bs with
  | nil => intro acc top hnd hlt hble; simpa [step] using hnd
  | cons b bs ih =>
      intro acc top hnd hlt hble
      have hbtop : top ≤ b := hble b (by simp)
      have hnd' : (step acc b).Nodup := by
        rw [step, List.nodup_append]
        refine ⟨hnd, ?_, ?_⟩
        · exact List.Pairwise.map (fun z => z + 2^b) (fun a c h => by omega) hnd
        · intro a ha c hc
          simp only [List.mem_map] at hc
          rcases hc with ⟨c', hc', rfl⟩
          have halt : a < 2^b := Nat.lt_of_lt_of_le (hlt a ha) (Nat.pow_le_pow_right (by decide) hbtop)
          have hbpos : 0 < 2^b := Nat.pow_pos (by decide)
          omega
      have hlt' : ∀ z ∈ step acc b, z < 2^(b+1) := by
        intro z hz
        simp only [step, List.mem_append, List.mem_map] at hz
        rcases hz with hz | ⟨z', hz', rfl⟩
        · exact Nat.lt_of_lt_of_le (hlt z hz) (Nat.pow_le_pow_right (by decide) (by omega))
        · have hz'lt : z' < 2^b := Nat.lt_of_lt_of_le (hlt z' hz') (Nat.pow_le_pow_right (by decide) hbtop)
          have hp : 2^(b+1) = 2^b + 2^b := by rw [Nat.pow_succ]; omega
          rw [hp]
          omega
      have hble' : ∀ b' ∈ bs, b + 1 ≤ b' := by
        intro b' hb'
        have : b < b' := (List.pairwise_cons.mp hbs).1 b' hb'
        omega
      exact ih (List.pairwise_cons.mp hbs).2 (acc := step acc b) (top := b+1) hnd' hlt' hble'

private theorem step_step_comm (acc : List Nat) (b c : Nat) :
    (step (step acc b) c).Perm (step (step acc c) b) := by
  simp [step, List.map_append, List.map_map, Function.comp_def, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
  have hp := List.perm_append_comm (l₁ := List.map (fun z => z + 2 ^ b) acc)
    (l₂ := List.map (fun z => z + 2 ^ c) acc)
  have hp2 := List.Perm.append_right (List.map (fun x => x + (2 ^ b + 2 ^ c)) acc) hp
  have hp3 := List.Perm.append_left acc hp2
  simpa [List.append_assoc] using hp3

private theorem fold_perm (bs : List Nat) (acc1 acc2 : List Nat) (h : acc1.Perm acc2) :
    (bs.foldl step acc1).Perm (bs.foldl step acc2) := by
  induction bs generalizing acc1 acc2 with
  | nil => simpa using h
  | cons b bs ih =>
      apply ih
      exact List.Perm.append h (h.map (fun z => z + 2^b))

private theorem step_fold_mem (acc : List Nat) (b : Nat) (bs : List Nat) :
    ∀ z, z ∈ bs.foldl step (step acc b) ↔ z ∈ step (bs.foldl step acc) b := by
  induction bs generalizing acc with
  | nil => intro z; simp [step]
  | cons c bs ih =>
      intro z
      simp only [List.foldl_cons]
      have hperm : (step (step acc b) c).Perm (step (step acc c) b) := step_step_comm acc b c
      have hz1 : z ∈ bs.foldl step (step (step acc b) c) ↔
          z ∈ bs.foldl step (step (step acc c) b) := (fold_perm bs _ _ hperm).mem_iff
      rw [hz1]
      rw [ih (acc := step acc c)]

private theorem foldl_step_mono (bs acc acc' : List Nat)
    (hsub : ∀ z, z ∈ acc → z ∈ acc') :
    ∀ z, z ∈ bs.foldl step acc → z ∈ bs.foldl step acc' := by
  induction bs generalizing acc acc' with
  | nil => intro z hz; exact hsub z hz
  | cons b bs ih =>
      intro z hz
      apply ih (acc := step acc b) (acc' := step acc' b)
      · intro w hw
        simp only [step, List.mem_append, List.mem_map] at hw ⊢
        rcases hw with hw | ⟨w', hw', rfl⟩
        · exact Or.inl (hsub w hw)
        · exact Or.inr ⟨w', hsub w' hw', rfl⟩
      · simpa using hz

private theorem fold_complete (bs : List Nat) :
    ∀ z, (∀ i, z.testBit i = true → i ∈ bs) → z ∈ bs.foldl step [0] := by
  induction bs with
  | nil =>
      intro z hz
      have hz0 : z = 0 := by
        by_cases hzero : z = 0
        · exact hzero
        · rcases Nat.exists_testBit_of_ne_zero hzero with ⟨i, hi⟩
          have hmem := hz i hi
          simp at hmem
      simp [hz0]
  | cons b bs ih =>
      intro z hz
      simp only [List.foldl_cons, step, List.mem_append, List.mem_map]
      by_cases hzb : z.testBit b = true
      · let z' := z - 2^b
        have hz'set : ∀ i, z'.testBit i = true → i ∈ bs := by
          intro i hi
          by_cases hib : i = b
          · subst i
            have hb' := testBit_sub_two_pow_self hzb
            rw [hb'] at hi
            contradiction
          · have hzi : z.testBit i = true := by
              have h := testBit_sub_two_pow_of_ne hzb hib
              rwa [h] at hi
            have hbm := hz i hzi
            simp only [List.mem_cons] at hbm
            rcases hbm with hbi | hbmem
            · exact False.elim (hib hbi)
            · exact hbmem
        have hsum : z' + 2^b = z := Nat.sub_add_cancel (Nat.ge_two_pow_of_testBit hzb)
        have hright : z ∈ step (bs.foldl step [0]) b := by
          simp only [step, List.mem_append, List.mem_map]
          exact Or.inr ⟨z', ih z' hz'set, hsum⟩
        exact (step_fold_mem [0] b bs z).mpr hright
      · have hbase := ih z (by
          intro i hi
          have hbm := hz i hi
          simp only [List.mem_cons] at hbm
          rcases hbm with hbi | hbmem
          · exact False.elim (hzb (by simpa [hbi] using hi))
          · exact hbmem)
        have hmono := foldl_step_mono bs [0] (step [0] b) (by
          intro w hw
          simp at hw
          subst w
          simp [step])
        exact hmono z hbase

private theorem land_eq_iff_bits {x y : Nat} :
    ((y &&& x) == y) = true ↔ ∀ i, y.testBit i = true → x.testBit i = true := by
  constructor
  · intro h i hi
    have heq : y &&& x = y := beq_iff_eq.mp h
    have hb := congrArg (fun n => Nat.testBit n i) heq
    rw [Nat.testBit_and] at hb
    simp [hi] at hb
    exact hb
  · intro h
    apply beq_iff_eq.mpr
    apply Nat.eq_of_testBit_eq
    intro i
    rw [Nat.testBit_and]
    by_cases hi : y.testBit i = true
    · simp [Nat.testBit_and, hi, h i hi]
    · simp [Nat.testBit_and, hi]

private theorem range_pairwise_lt (n : Nat) : (List.range n).Pairwise (· < ·) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.pairwise_append]
      refine ⟨ih, by simp, ?_⟩
      intro a ha b hb
      simp at hb
      subst b
      simp at ha
      omega

private def xbits (x : Nat) : List Nat := (List.range (width x)).filter x.testBit

private theorem submasks_sound {x y : Nat} (hy : y ∈ submasks x) :
    ∀ i, y.testBit i = true → i ∈ xbits x := by
  let bits := xbits x
  have hpair : bits.Pairwise (· < ·) := by
    dsimp [bits, xbits]
    exact List.Pairwise.filter x.testBit (range_pairwise_lt (width x))
  have hmem : ∀ z ∈ [0], ∀ i, z.testBit i = true → i ∈ bits := by
    intro z hz
    simp at hz
    subst z
    intro i hi
    simp at hi
  have hlt : ∀ z ∈ [0], z < 2 ^ 0 := by
    intro z hz
    simp at hz
    subst z
    simp
  have hle : ∀ b ∈ bits, 0 ≤ b := by intro b hb; omega
  have hy' : y ∈ bits.foldl step [0] := by simpa [submasks, bits, xbits, step] using hy
  exact fold_sound bits [0] 0 bits hmem hlt hle hpair (fun b hb => hb) y hy'

private theorem submasks_nodup (x : Nat) : (submasks x).Nodup := by
  let bits := xbits x
  have hpair : bits.Pairwise (· < ·) := by
    dsimp [bits, xbits]
    exact List.Pairwise.filter x.testBit (range_pairwise_lt (width x))
  have hlt : ∀ z ∈ [0], z < 2 ^ 0 := by
    intro z hz
    simp at hz
    subst z
    simp
  have hle : ∀ b ∈ bits, 0 ≤ b := by intro b hb; omega
  have hres := fold_nodup bits [0] 0 (by simp : ([0] : List Nat).Nodup) hlt hle hpair
  simpa [submasks, bits, xbits, step] using hres

private theorem submasks_complete {x y : Nat} (hy : (y &&& x) == y) : y ∈ submasks x := by
  have hbits : ∀ i, y.testBit i = true → i ∈ xbits x := by
    intro i hi
    have hxi : x.testBit i = true := (land_eq_iff_bits.mp hy) i hi
    have hiw : i < width x := by
      by_cases hltw : i < width x
      · exact hltw
      · have hge : width x ≤ i := by omega
        have hxlt : x < 2^i := by
          unfold width at hge
          by_cases hx0 : x = 0
          · simp [hx0] at hxi
          · simp [hx0] at hge
            have hlog : x < 2^(x.log2+1) := (Nat.log2_lt hx0).mp (Nat.lt_succ_self _)
            exact Nat.lt_of_lt_of_le hlog (Nat.pow_le_pow_right (by decide) hge)
        have hf := Nat.testBit_lt_two_pow hxlt
        rw [hf] at hxi
        contradiction
    simp [xbits, List.mem_filter, List.mem_range, hxi, hiw]
  let bits := xbits x
  have hres := fold_complete bits y (by simpa [bits] using hbits)
  simpa [submasks, bits, xbits, step] using hres



private theorem sum_count_mem_eq_filter_length (zs ys : List Nat) (hnd : zs.Nodup)
    (p : Nat → Bool) (hiff : ∀ z ∈ ys, p z = true ↔ z ∈ zs) :
    (zs.map (fun y => ys.count y)).sum = (ys.filter p).length := by
  induction ys with
  | nil =>
      induction zs with
      | nil => simp
      | cons z zs ih =>
          have hih := ih (List.nodup_cons.mp hnd).2 (fun w hw => False.elim (by simpa using hw))
          simpa using hih
  | cons a ys ih =>
    have ih' := ih (fun z hz => hiff z (by simp [hz]))
    have hpiff := hiff a (by simp)
    have hstep : (zs.map (fun y => (a :: ys).count y)).sum =
        (zs.map (fun y => ys.count y)).sum + if p a = true then 1 else 0 := by
      rw [show zs.map (fun y => (a :: ys).count y) =
          zs.map (fun y => ys.count y + if y = a then 1 else 0) from by
        apply List.map_congr_left
        intro y hy
        rw [count_cons_add]]
      rw [sum_map_add_nat]
      by_cases hp : p a = true
      · have ha : a ∈ zs := hpiff.mp hp
        rw [sum_indicator_nodup zs hnd a 1 ha]
        simp [hp]
      · have ha : a ∉ zs := by intro hm; exact hp (hpiff.mpr hm)
        have hz := sum_zero_map zs (fun y => if y = a then 1 else 0) (by
          intro y hy
          have hya : y ≠ a := by intro heq; exact ha (heq ▸ hy)
          simp [hya])
        rw [hz]
        simp [hp]
    rw [hstep, ih']
    by_cases hp : p a = true <;> simp [hp]

private theorem submask_fan (u t x : Nat) (ys : List Nat) (hv : valid u ys) :
    fan search u t x ys = (ys.filter (search.test t x)).length := by
  have hs : ys.Pairwise (· ≤ ·) := hv.1
  let zs := (submasks x).filter (fun y => y < u && y ≤ t)
  have hzsnd : zs.Nodup := (submasks_nodup x).sublist List.filter_sublist
  have hterm : ∀ y ∈ zs, lower ys (y + 1) - lower ys y = ys.count y := by
    intro y hy
    rw [← slice_length hs y (y + 1), slice_eq_filter hs y (y + 1)]
    rw [List.count_eq_length_filter]
    apply congrArg List.length
    apply List.filter_congr
    intro z hz
    apply Bool.eq_iff_iff.mpr
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    constructor
    · rintro ⟨hyz, hzy⟩
      exact beq_iff_eq.mpr (by omega)
    · intro h
      have hzy : z = y := beq_iff_eq.mp h
      subst z
      omega
  have hiff : ∀ z ∈ ys, (search.test t x z) = true ↔ z ∈ zs := by
    intro z hzys
    have hzu : z < u := hv.2 z hzys
    constructor
    · intro ht
      simp only [search, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at ht
      rcases ht with ⟨hsub, hzt⟩
      have hmem : z ∈ submasks x := submasks_complete (beq_of_eq hsub)
      simp only [zs, List.mem_filter]
      constructor
      · exact hmem
      · simp [hzu, hzt]
    · intro hm
      simp only [zs, List.mem_filter] at hm
      rcases hm with ⟨hzsub, hmb⟩
      have hpair : z < u ∧ z ≤ t := by
        simpa only [decide_eq_true_eq, Bool.and_eq_true] using hmb
      have hzbits := submasks_sound hzsub
      have hland : ((z &&& x) == z) = true := by
        apply land_eq_iff_bits.mpr
        intro i hi
        have hib := hzbits i hi
        dsimp [xbits] at hib
        exact (List.mem_filter.mp hib).2
      simp only [search, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq]
      exact ⟨beq_iff_eq.mp hland, hpair.2⟩
  unfold fan
  dsimp [search]
  rw [List.map_map]
  change (zs.map (fun y => lower ys (y + 1) - lower ys y)).sum =
    (ys.filter (search.test t x)).length
  rw [show zs.map (fun y => lower ys (y + 1) - lower ys y) =
      zs.map (fun y => ys.count y) from by
    apply List.map_congr_left
    intro y hy
    exact hterm y hy]
  exact sum_count_mem_eq_filter_length zs ys hzsnd (search.test t x) hiff

theorem sound_RepeatedProbe : Submask.RepeatedProbe.Goal := by
  intro u t xs ys hv
  have hfan : ∀ x, fan search u t x ys = (ys.filter (search.test t x)).length := by
    intro x
    exact submask_fan u t x ys hv
  have hmap : xs.eraseDups.map (fun x => xs.count x * fan search u t x ys) =
      xs.eraseDups.map (fun x => xs.count x * (ys.filter (search.test t x)).length) := by
    apply List.map_congr_left
    intro x hx
    rw [hfan x]
  rw [hmap, List.length_flatMap]
  exact sum_map_eraseDups_count_mul xs (fun x => (ys.filter (search.test t x)).length)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Submask_RepeatedProbe
