import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_MaskedJoin

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass

/-! ## Small-input truth check -/

private def pow (xs : List Nat) : List (List Nat) :=
  xs.foldr (fun a acc => acc.flatMap (fun ys => [ys, a :: ys])) [[]]

private def eqcase (u t x : Nat) (ys ds : List Nat) : Bool :=
  fan search u t x ys - ds.eraseDups.countP (fun i => ys[i]?.any (search.test t x)) ==
    ((ys.zipIdx.filter (fun p => !ds.contains p.2)).map Prod.fst).countP (search.test t x)

private def check : Bool :=
  (List.range 6).all fun u =>
  (List.range 4).all fun t =>
  (List.range (u + 2)).all fun x =>
  (pow (List.range u)).all fun ys =>
  (pow (List.range (u + 2))).all fun ds =>
    eqcase u t x ys ds

#eval check
example : check := by native_decide

/-! ## The binary lower bound on a sorted right key column -/

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
                  have hpair := List.Pairwise.rel_getElem_of_lt hjlen hmidlen hs hjm
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
                  have hpair := List.Pairwise.rel_getElem_of_lt hmidlen hjlen hs hmj
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
    subst n
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
    rw [List.filter_eq_self.mpr]
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
            intro hay
            have hxy : x ≤ y := List.rel_of_pairwise_cons hs hy
            omega
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs']

private theorem lower_sub_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = (ys.filter (fun y => decide (a ≤ y && y < b))).length := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a]
  rw [filter_lt_prefix_filter_lt hs hab, List.length_append]
  omega

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

private theorem take_filter_prefix {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (v : Nat) :
    ys.take (lower ys v) = ys.filter (fun x => decide (x < v)) := by
  let k := lower ys v
  have hdrop : (ys.drop k).filter (fun x => decide (x < v)) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq]
    have hiys : k + i < ys.length := by
      have hlen : i < (ys.drop k).length := hi
      rw [List.length_drop] at hlen
      omega
    have hge := lower_le_of_ge hs v (by dsimp [k]; omega) hiys
    rw [getD_eq_getElem hiys] at hge
    rw [List.getElem_drop]
    omega
  have hftake : (ys.take k).filter (fun x => decide (x < v)) = ys.take k := by
    apply List.filter_eq_self.mpr
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq, List.getElem_take]
    have hiys : i < ys.length := by
      have hklen : k ≤ ys.length := by dsimp [k]; exact lower_le_length hs v
      omega
    have hi' : i < k := Nat.lt_of_lt_of_le hi (Nat.min_le_left _ _)
    have hlt := lower_lt_of_lt hs v (by simpa [k] using hi')
    rwa [getD_eq_getElem hiys] at hlt
  have hfilt : ys.filter (fun x => decide (x < v)) = ys.take k := by
    calc
      ys.filter (fun x => decide (x < v)) =
          (ys.take k ++ ys.drop k).filter (fun x => decide (x < v)) := by
        rw [List.take_append_drop]
      _ = (ys.take k).filter (fun x => decide (x < v)) ++
          (ys.drop k).filter (fun x => decide (x < v)) := by rw [List.filter_append]
      _ = ys.take k := by rw [hftake, hdrop, List.append_nil]
  simpa [k] using hfilt.symm

private theorem slice_eq_filter {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    slice ys a b = ys.filter (fun y => decide (a ≤ y && y < b)) := by
  unfold slice
  by_cases hab : a ≤ b
  · rw [drop_take_eq_take_drop ys (lower ys a) (lower ys b) (lower_mono hs hab),
        take_filter_prefix hs b]
    have hla : lower ys a = (ys.filter (fun x => decide (x < a))).length :=
      lower_eq_filter_lt_length hs a
    rw [hla, filter_lt_prefix_filter_lt hs hab, List.drop_left]
  · have hba : b ≤ a := by omega
    rw [Nat.sub_eq_zero_of_le (lower_mono hs hba), List.take_zero]
    symm
    apply List.filter_eq_nil_iff.mpr
    intro y hy
    simp only [decide_eq_true_eq, Bool.and_eq_true]
    omega

private theorem slice_length {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a b : Nat) :
    (slice ys a b).length = lower ys b - lower ys a := by
  unfold slice
  rw [List.length_take]
  have hla := lower_le_length hs a
  have hlb := lower_le_length hs b
  by_cases hab : a ≤ b
  · have hmono := lower_mono hs hab
    rw [List.length_drop]
    rw [Nat.min_eq_left (by omega : lower ys b - lower ys a ≤ ys.length - lower ys a)]
  · have hba : b ≤ a := by omega
    have hmono := lower_mono hs hba
    rw [Nat.sub_eq_zero_of_le hmono]
    simp

/-! ## Arithmetic of the residue classes -/

private theorem mem_of_mod {u w r y : Nat} (hw : 0 < w) (hyu : y < u) :
    (y % w == r) = true →
      ∃ i, i < (u + w - 1 - r) / w ∧ y = r + i * w := by
  intro h
  have hyr : y % w = r := by simpa using h
  have hrw : r < w := by rw [← hyr]; exact Nat.mod_lt _ hw
  have hry : r ≤ y := by rw [← hyr]; exact Nat.mod_le _ _
  have hdiv : w * (y / w) + y % w = y := Nat.div_add_mod y w
  refine ⟨y / w, ?_, ?_⟩
  · have hd : w * (y / w) ≤ u - 1 - r := by omega
    have hd' : y / w ≤ (u - 1 - r) / w := by
      exact (Nat.le_div_iff_mul_le hw).2 (by simpa [Nat.mul_comm] using hd)
    have heq : u + w - 1 - r = (u - 1 - r) + w := by omega
    rw [heq]
    have hdiv_eq : ((u - 1 - r) + w) / w = (u - 1 - r) / w + 1 := by
      simpa using (Nat.add_mul_div_left (u - 1 - r) 1 hw)
    rw [hdiv_eq]
    omega
  · have hcalc : r + (y / w) * w = y := by
      rw [← hyr, Nat.mul_comm (y / w) w, Nat.add_comm]
      exact hdiv
    exact hcalc.symm

private theorem mod_of_mem {w r y i : Nat} (hr : r < w) (h : y = r + i * w) :
    (y % w == r) = true := by
  subst y
  rw [Nat.mul_comm i w, Nat.add_mul_mod_self_left, Nat.mod_eq_of_lt hr]
  simp

private theorem index_unique {w r i j y : Nat} (hw : 0 < w)
    (hi : y = r + i * w) (hj : y = r + j * w) : i = j := by
  have heq : i * w = j * w := by omega
  have heq' : w * i = w * j := by simpa [Nat.mul_comm] using heq
  exact Nat.eq_of_mul_eq_mul_left hw heq'

private theorem sum_map_ite_bool {α : Type} (l : List α) (f : α → Bool) :
    (l.map (fun x => if f x then 1 else 0)).sum = l.countP f := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.map_cons, List.sum_cons, List.countP_cons, ih]
      by_cases h : f x = true <;> simp [h] <;> omega

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem countP_eq_zero_of_all_false {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ x ∈ l, p x = false) : l.countP p = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.countP_cons]
      have hx : p x = false := h x (by simp)
      have ht : ∀ y ∈ xs, p y = false := fun y hy => h y (by simp [hy])
      rw [ih ht, hx]
      simp

private theorem countP_eq_one_of_unique {α : Type} (l : List α) (p : α → Bool)
    (hnodup : l.Nodup) (hex : ∃ a, a ∈ l ∧ p a = true)
    (huniq : ∀ a b, a ∈ l → b ∈ l → p a = true → p b = true → a = b) :
    l.countP p = 1 := by
  induction l with
  | nil =>
      rcases hex with ⟨a, ha, _⟩
      simp at ha
  | cons a l ih =>
      have hn : l.Nodup := (List.nodup_cons.mp hnodup).2
      have hna : a ∉ l := (List.nodup_cons.mp hnodup).1
      rw [List.countP_cons]
      by_cases hp : p a = true
      · have hzero : l.countP p = 0 := by
          apply countP_eq_zero_of_all_false
          intro b hb
          by_cases hpb : p b = true
          · have hbmem : b ∈ a :: l := by simp [hb]
            have : b = a := huniq b a hbmem (by simp) hpb hp
            exact (hna (by simpa [this] using hb)).elim
          · exact Bool.eq_false_of_ne_true hpb
        rw [hzero, hp]
        simp
      · have hp' : (if p a = true then 1 else 0) = 0 := by simp [hp]
        rw [hp']
        have hex' : ∃ b, b ∈ l ∧ p b = true := by
          rcases hex with ⟨b, hb, hpb⟩
          have hb' : b ∈ l := by
            simp only [List.mem_cons] at hb
            rcases hb with rfl | hb
            · exact (hp hpb).elim
            · exact hb
          exact ⟨b, hb', hpb⟩
        have huniq' : ∀ b c, b ∈ l → c ∈ l → p b = true → p c = true → b = c := by
          intro b c hb hc hpb hpc
          exact huniq b c (by simp [hb]) (by simp [hc]) hpb hpc
        exact ih hn hex' huniq'

private theorem sum_map_zero (l : List Nat) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem countP_decomp (ys : List Nat) (n w r : Nat) (_hw : 0 < w)
    (hpoint : ∀ y ∈ ys, (y % w == r) = true ↔ ∃ i, i < n ∧ y = r + i * w)
    (huniq : ∀ i j y, i < n → j < n → y = r + i * w → y = r + j * w → i = j) :
    ys.countP (fun y => y % w == r) =
      ((List.range n).map (fun i => ys.count (r + i * w))).sum := by
  induction ys with
  | nil =>
      simp only [List.countP_eq_length_filter, List.filter_nil, List.length_nil, List.count_nil]
      exact (sum_map_zero (List.range n)).symm
  | cons y ys ih =>
      have htail : ∀ z ∈ ys, (z % w == r) = true ↔ ∃ i, i < n ∧ z = r + i * w :=
        fun z hz => hpoint z (by simp [hz])
      have ih' := ih htail
      rw [List.countP_cons, ih']
      have hcnt : ∀ i, (y :: ys).count (r + i * w) =
          ys.count (r + i * w) + if (y == r + i * w) then 1 else 0 := by
        intro i
        rw [List.count_cons]
      have hsum :
          ((List.range n).map (fun i => (y :: ys).count (r + i * w))).sum =
            ((List.range n).map (fun i => ys.count (r + i * w))).sum +
              ((List.range n).map (fun i => if (y == r + i * w) then 1 else 0)).sum := by
        have hmap : (List.range n).map (fun i => (y :: ys).count (r + i * w)) =
            (List.range n).map (fun i => ys.count (r + i * w) +
              if (y == r + i * w) then 1 else 0) := by
          apply List.map_congr_left
          intro i hi
          exact hcnt i
        rw [hmap]
        exact sum_map_add _ _ _
      rw [hsum]
      rw [sum_map_ite_bool]
      have hidx : (List.range n).countP (fun i => y == r + i * w) =
          if (y % w == r) then 1 else 0 := by
        by_cases hy : (y % w == r) = true
        · have hex0 := (hpoint y (by simp)).mp hy
          rcases hex0 with ⟨i0, hi0, hi0eq⟩
          have hone : (List.range n).countP (fun i => y == r + i * w) = 1 := by
            apply countP_eq_one_of_unique
            · exact List.nodup_range
            · exact ⟨i0, by simpa [List.mem_range] using hi0, by simp [hi0eq]⟩
            · intro a b ha hb hpa hpb
              have ha' : a < n := by simpa [List.mem_range] using ha
              have hb' : b < n := by simpa [List.mem_range] using hb
              have hpa' : y = r + a * w := by simpa using hpa
              have hpb' : y = r + b * w := by simpa using hpb
              exact huniq a b y ha' hb' hpa' hpb'
          simp [hy, hone]
        · have hzero : (List.range n).countP (fun i => y == r + i * w) = 0 := by
            apply countP_eq_zero_of_all_false
            intro i hi
            have hin : i < n := by simpa [List.mem_range] using hi
            by_cases hval : (y == r + i * w) = true
            · have hyval : (y % w == r) = true :=
                (hpoint y (by simp)).mpr ⟨i, hin, by simpa using hval⟩
              exact (hy hyval).elim
            · exact Bool.eq_false_of_ne_true hval
          simp [hy, hzero]
      rw [hidx]

/-! ## Counting the fan of the modulo-class search -/

private theorem fan_mod_eq_countP {u t x : Nat} {ys : List Nat} (hv : valid u ys) :
    fan search u t x ys = ys.countP (fun y => search.test t x y) := by
  let w := t + 1
  let r := x % w
  let n := (u + w - 1 - r) / w
  have hw : 0 < w := by dsimp [w]; omega
  have hr : r < w := Nat.mod_lt _ hw
  have hs := hv.1
  have htest : ys.countP (fun y => search.test t x y) =
      ys.countP (fun y => y % w == r) := by
    apply List.countP_congr
    intro y hy
    dsimp [search, w, r]
    simp only [beq_iff_eq]
    exact eq_comm
  have hterm : ∀ i, lower ys (r + i * w + 1) - lower ys (r + i * w) =
      ys.count (r + i * w) := by
    intro i
    rw [lower_sub_eq_filter hs (by omega : r + i * w ≤ r + i * w + 1)]
    rw [← List.countP_eq_length_filter]
    have hcongr : ys.countP (fun y =>
        decide ((decide (r + i * w ≤ y) && decide (y < r + i * w + 1)) = true)) =
        ys.countP (fun y => y == r + i * w) := by
      apply List.countP_congr
      intro y hy
      by_cases hle : r + i * w ≤ y
      · by_cases hlt : y < r + i * w + 1
        · have heq : y = r + i * w := by omega
          subst y
          simp
        · have hne : y ≠ r + i * w := by omega
          simp [hle, hlt, hne]
      · have hne : y ≠ r + i * w := by omega
        simp [hle, hne]
    rw [hcongr, ← List.count_eq_countP]
  have hpoint : ∀ y ∈ ys, ((y % w == r) = true) ↔ ∃ i, i < n ∧ y = r + i * w := by
    intro y hy
    constructor
    · intro h
      simpa [n] using mem_of_mod (w := w) (r := r) hw (hv.2 y hy) h
    · rintro ⟨i, hi, hiy⟩
      exact mod_of_mem hr hiy
  have huniq : ∀ i j y, i < n → j < n → y = r + i * w → y = r + j * w → i = j :=
    fun i j y _ _ hi hj => index_unique hw hi hj
  rw [htest]
  dsimp [fan, search]
  rw [List.map_map]
  change ((List.range n).map (fun i => lower ys (r + i * w + 1) - lower ys (r + i * w))).sum =
    ys.countP (fun y => y % w == r)
  have hmap : (List.range n).map (fun i => lower ys (r + i * w + 1) - lower ys (r + i * w)) =
      (List.range n).map (fun i => ys.count (r + i * w)) := by
    apply List.map_congr_left
    intro i hi
    exact hterm i
  rw [hmap]
  exact (countP_decomp ys n w r hw hpoint huniq).symm

/-! ## Deleted positions versus distinct deleted indices -/

private theorem eraseDups_nodup : ∀ xs : List Nat, xs.eraseDups.Nodup
  | [] => by simp
  | a :: xs => by
      rw [List.eraseDups_cons]
      refine List.nodup_cons.mpr ⟨?_, ?_⟩
      · intro ha
        have hf := List.mem_filter.mp (List.mem_eraseDups.mp ha)
        simpa using hf.2
      · exact eraseDups_nodup (xs.filter (fun b => ! b == a))
termination_by xs => xs.length
decreasing_by
  have hle := List.length_filter_le (fun b : Nat => ! b == a) xs
  simp at hle ⊢
  omega

private theorem filter_not_perm_filter {α : Type} (q : α → Bool) (l : List α) :
    (l.filter (fun x => ! q x) ++ l.filter q).Perm l := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hq : q a = true
      · simp [hq]
        have hswap : (l.filter (fun x => ! q x) ++ [a] ++ l.filter q).Perm
            (a :: (l.filter (fun x => ! q x) ++ l.filter q)) := by
          have hcomm := (List.perm_append_comm (l₁ := l.filter (fun x => ! q x)) (l₂ := [a])).append_right
            (l.filter q)
          simpa [List.cons_append, List.append_assoc] using hcomm
        simpa [List.cons_append] using hswap.trans (List.Perm.cons a ih)
      · have hqf : q a = false := Bool.eq_false_of_ne_true hq
        simp [hq, hqf]
        exact ih

private theorem countP_filter_add {α : Type} (l : List α) (q p : α → Bool) :
    l.countP p = (l.filter q).countP p + (l.filter (fun x => ! q x)).countP p := by
  have hperm := filter_not_perm_filter q l
  rw [← hperm.countP_eq p, List.countP_append]
  omega

private theorem countP_zipIdx_off (ys : List Nat) (off : Nat) (p : Nat → Bool) :
    (ys.zipIdx off).countP (fun q => p q.1) = ys.countP p := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      simp [List.zipIdx_cons, List.countP_cons, ih]

private theorem countP_map_succ (l : List Nat) (f : Nat → Bool) :
    (l.map Nat.succ).countP f = l.countP (fun i => f (i + 1)) := by
  induction l with
  | nil => simp
  | cons a l ih => rw [List.map_cons, List.countP_cons, List.countP_cons, ih]

private theorem zipIdx_filter_countP_off (ys : List Nat) (off : Nat) (ds : List Nat)
    (p : Nat → Bool) :
    ((ys.zipIdx off).filter (fun q => ds.contains q.2)).countP (fun q => p q.1) =
      (List.range ys.length).countP (fun i => ds.contains (off + i) && p (ys.getD i 0)) := by
  induction ys generalizing off with
  | nil => simp [List.zipIdx]
  | cons y ys ih =>
      rw [List.zipIdx_cons]
      simp only [List.length_cons]
      have hcongr : (fun i => ds.contains (off + (i + 1)) && p ((y :: ys).getD (i + 1) 0)) =
          (fun i => ds.contains ((off + 1) + i) && p (ys.getD i 0)) := by
        funext i
        simp only [List.getD_cons_succ]
        congr 2 <;> omega
      by_cases hhead : (ds.contains off && p y) = true
      · rw [Bool.and_eq_true] at hhead
        have hc : ds.contains off = true := hhead.1
        have hp : p y = true := hhead.2
        rw [List.filter_cons, ite_eq_left hc, List.countP_cons, ite_eq_left hp]
        rw [List.range_succ_eq_map, List.countP_cons, countP_map_succ]
        have hP0 : (ds.contains (off + 0) && p ((y :: ys).getD 0 0)) = true := by
          rw [Bool.and_eq_true]
          simpa using hhead
        rw [ite_eq_left hP0, hcongr, ih]
      · rw [Bool.and_eq_true] at hhead
        have hP0false : ¬ ((ds.contains (off + 0) && p ((y :: ys).getD 0 0)) = true) := by
          intro hp0
          rw [Bool.and_eq_true] at hp0
          exact hhead (by simpa using hp0)
        by_cases hc : ds.contains off = true
        · have hp : p y = false := Bool.eq_false_of_ne_true (fun hpy => hhead ⟨hc, hpy⟩)
          rw [List.filter_cons, ite_eq_left hc, List.countP_cons,
            ite_eq_right (by simp [hp])]
          rw [List.range_succ_eq_map, List.countP_cons, countP_map_succ]
          rw [ite_eq_right hP0false, hcongr, ih]
        · rw [List.filter_cons, ite_eq_right hc]
          rw [List.range_succ_eq_map, List.countP_cons, countP_map_succ]
          rw [ite_eq_right hP0false, hcongr, ih]
          omega

private theorem filter_mem_length_eq (xs ys : List Nat) (p : Nat → Bool)
    (hxs : xs.Nodup) (hys : ys.Nodup) :
    (xs.filter (fun i => decide (i ∈ ys) && p i)).length =
      (ys.filter (fun i => decide (i ∈ xs) && p i)).length := by
  have hm : (xs.filter (fun i => decide (i ∈ ys))).Perm
      (ys.filter (fun i => decide (i ∈ xs))) := by
    apply (List.perm_ext_iff_of_nodup (List.Pairwise.filter _ hxs) (List.Pairwise.filter _ hys)).2
    intro i
    simp only [List.mem_filter]
    constructor
    · intro h
      exact ⟨by simpa using h.2, by simpa using h.1⟩
    · intro h
      exact ⟨by simpa using h.2, by simpa using h.1⟩
  have hp := hm.filter p
  have hl : xs.filter (fun i => decide (i ∈ ys) && p i) =
      (xs.filter (fun i => decide (i ∈ ys))).filter p := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro i hi
    by_cases hmem : i ∈ ys <;> by_cases hpi : p i <;> simp [hmem, hpi]
  have hr : ys.filter (fun i => decide (i ∈ xs) && p i) =
      (ys.filter (fun i => decide (i ∈ xs))).filter p := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro i hi
    by_cases hmem : i ∈ xs <;> by_cases hpi : p i <;> simp [hmem, hpi]
  rw [hl, hr]
  exact hp.length_eq

private theorem deleted_count_eq (ys ds : List Nat) (p : Nat → Bool) :
    ds.eraseDups.countP (fun i => ys[i]?.any p) =
      (List.range ys.length).countP (fun i => ds.contains i && p (ys.getD i 0)) := by
  have hlhs : ds.eraseDups.filter (fun i => ys[i]?.any p) =
      ds.eraseDups.filter (fun i => decide (i < ys.length) && p (ys.getD i 0)) := by
    apply List.filter_congr
    intro i hi
    by_cases h : i < ys.length
    · simp [h, List.getElem?_eq_getElem h, getD_eq_getElem h]
    · simp [h, List.getElem?_eq_none (by omega : ys.length ≤ i)]
  rw [List.countP_eq_length_filter]
  rw [hlhs]
  rw [← List.countP_eq_length_filter]
  have hlen := filter_mem_length_eq ds.eraseDups (List.range ys.length)
    (fun i => decide (i < ys.length) && p (ys.getD i 0)) (eraseDups_nodup ds) List.nodup_range
  have hlen' :
      ds.eraseDups.countP (fun i => decide (i < ys.length) && p (ys.getD i 0)) =
        (List.range ys.length).countP
          (fun i => decide (i ∈ ds.eraseDups) && decide (i < ys.length) &&
            p (ys.getD i 0)) := by
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
    simpa [List.mem_range, Bool.and_assoc] using hlen
  rw [hlen']
  apply List.countP_congr
  intro i hi
  have hin : i < ys.length := by simpa [List.mem_range] using hi
  by_cases hdi : ds.contains i = true
  · have hmem : i ∈ ds.eraseDups := by
      rw [List.mem_eraseDups]
      simpa [List.contains_eq_mem] using hdi
    have hids : i ∈ ds := by simpa [List.contains_eq_mem] using hdi
    simp [hmem, hdi, hin, hids]
  · have hmem : i ∉ ds.eraseDups := by
      rw [List.mem_eraseDups]
      simpa [List.contains_eq_mem] using hdi
    have hids : i ∉ ds := by simpa [List.contains_eq_mem] using hdi
    simp [hmem, hdi, hids]

/-! ## MaskedJoin -/

theorem sound_MaskedJoin : ModuloClass.MaskedJoin.Goal := by
  intro u t x ys ds hv
  rw [fan_mod_eq_countP hv]
  let p := fun y => search.test t x y
  change ys.countP p - ds.eraseDups.countP (fun i => ys[i]?.any p) =
    ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP p
  have hkeep : ((ys.zipIdx.filter (fun q => !ds.contains q.2)).map Prod.fst).countP p =
      (ys.zipIdx.filter (fun q => !ds.contains q.2)).countP (fun q => p q.1) := by
    rw [List.countP_map]
    rfl
  rw [hkeep]
  have hzip := countP_zipIdx_off ys 0 p
  have hpart := countP_filter_add (ys.zipIdx 0) (fun q => ds.contains q.2) (fun q => p q.1)
  have hzdel := zipIdx_filter_countP_off ys 0 ds p
  have hdel' : (ys.zipIdx.filter (fun q => ds.contains q.2)).countP (fun q => p q.1) =
      ds.eraseDups.countP (fun i => ys[i]?.any p) := by
    rw [hzdel]
    simpa using (deleted_count_eq ys ds p).symm
  have hsum : ys.countP p =
      ds.eraseDups.countP (fun i => ys[i]?.any p) +
        (ys.zipIdx.filter (fun q => !ds.contains q.2)).countP (fun q => p q.1) := by
    rw [← hzip]
    change (ys.zipIdx).countP (fun q => p q.1) = _
    rw [hpart, hdel']
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_MaskedJoin
