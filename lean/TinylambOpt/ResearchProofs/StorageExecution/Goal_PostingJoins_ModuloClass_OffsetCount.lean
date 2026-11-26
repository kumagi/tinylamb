import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `ModuloClass.OffsetCount` — Two lower-bound offsets per interval compute the exact
  join multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_OffsetCount

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ModuloClass

/-! Self-contained certificate for `ModuloClass.OffsetCount`.

The binary lower bound on a sorted right-key column cuts exactly at a key
boundary, so each singleton posting `[r+i*w, r+i*w+1)` contributes the
multiplicity of the right key `r+i*w`.  For right keys below the domain end
`u`, lying in one of these singletons is equivalent to having residue `r`
modulo `w = t+1`, which is precisely what `search.test` checks. -/

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
  let k := lower ys v
  have hle : k ≤ ys.length := by simpa [k] using lower_le_length hs v
  have htake : (ys.take k).filter (fun x => decide (x < v)) = ys.take k := by
    apply List.filter_eq_self.mpr
    intro x hx
    rw [List.mem_take_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq]
    have hiys : i < ys.length := by omega
    have hik : i < lower ys v := by
      dsimp [k] at hi ⊢
      omega
    have hlt := lower_lt_of_lt hs v hik
    rw [getD_eq_getElem hiys] at hlt
    simpa [k] using hlt
  have hdrop : (ys.drop k).filter (fun x => decide (x < v)) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro x hx
    rw [List.mem_iff_getElem] at hx
    rcases hx with ⟨i, hi, rfl⟩
    simp only [decide_eq_true_eq]
    intro hlt
    have hiys : k + i < ys.length := by
      have hlen : i < (ys.drop k).length := hi
      rw [List.length_drop] at hlen
      omega
    have hge := lower_le_of_ge hs v (Nat.le_add_right _ _) (by simpa [k] using hiys)
    rw [getD_eq_getElem hiys] at hge
    rw [List.getElem_drop] at hlt
    omega
  have hfull_list : ys.filter (fun x => decide (x < v)) =
      (ys.take k).filter (fun x => decide (x < v)) := by
    calc
      ys.filter (fun x => decide (x < v)) =
          (ys.take k ++ ys.drop k).filter (fun x => decide (x < v)) := by rw [List.take_append_drop]
      _ = (ys.take k).filter (fun x => decide (x < v)) ++
          (ys.drop k).filter (fun x => decide (x < v)) := by rw [List.filter_append]
      _ = (ys.take k).filter (fun x => decide (x < v)) := by rw [hdrop, List.append_nil]
  calc
    lower ys v = (ys.take k).length := by simp [k, List.length_take, Nat.min_eq_left hle]
    _ = ((ys.take k).filter (fun x => decide (x < v))).length := by rw [htake]
    _ = (ys.filter (fun x => decide (x < v))).length := by rw [hfull_list]

private theorem countP_lt_split {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    ys.countP (fun y => decide (y < b)) =
      ys.countP (fun y => decide (y < a)) + ys.countP (fun y => decide (a ≤ y && y < b)) := by
  induction ys with
  | nil => simp
  | cons x xs ih =>
      have hs' := hs.tail
      by_cases hxa : x < a
      · have hxb : x < b := by omega
        simp [hxa, hxb, ih hs'] <;> omega
      · by_cases hxb : x < b
        · have hax : a ≤ x := by omega
          have hnil_a : xs.countP (fun y => decide (y < a)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro y hy
            have hxy := List.rel_of_pairwise_cons hs hy
            simp [show ¬ y < a by omega]
          simp [hxa, hxb, hax, hnil_a, ih hs'] <;> omega
        · have hnil_a : xs.countP (fun y => decide (y < a)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro y hy
            have hxy := List.rel_of_pairwise_cons hs hy
            simp [show ¬ y < a by omega]
          have hnil_ab : xs.countP (fun y => decide (a ≤ y && y < b)) = 0 := by
            apply List.countP_eq_zero.mpr
            intro y hy
            have hxy := List.rel_of_pairwise_cons hs hy
            simpa using (show ¬ (a ≤ y ∧ y < b) by omega)
          simp [hxa, hxb, hnil_a, hnil_ab, ih hs'] <;> omega

private theorem lower_interval_count {ys : List Nat} (hs : ys.Pairwise (· ≤ ·))
    {a b : Nat} (hab : a ≤ b) :
    lower ys b - lower ys a = ys.countP (fun y => decide (a ≤ y && y < b)) := by
  rw [lower_eq_filter_lt_length hs b, lower_eq_filter_lt_length hs a,
    ← List.countP_eq_length_filter, ← List.countP_eq_length_filter]
  rw [countP_lt_split hs hab]
  omega

private theorem countP_or_of_disjoint {α : Type} (p q : α → Bool) (xs : List α)
    (hdisj : ∀ x ∈ xs, p x = true → q x = false) :
    xs.countP p + xs.countP q = xs.countP (fun x => p x || q x) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have htail : ∀ y ∈ xs, p y = true → q y = false := fun y hy => hdisj y (by simp [hy])
      have hih := ih htail
      by_cases hp : p x = true
      · have hq : q x = false := hdisj x (by simp) hp
        simp [hp, hq] at hih ⊢
        omega
      · by_cases hq : q x = true
        · simp [hp, hq] at hih ⊢
          omega
        · simp [hp, hq] at hih ⊢
          omega

/-! A singleton interval contributes the multiplicity of exactly one key. -/
private theorem lower_singleton_count {ys : List Nat} (hs : ys.Pairwise (· ≤ ·)) (a : Nat) :
    lower ys (a + 1) - lower ys a = ys.count a := by
  rw [lower_interval_count hs (by omega : a ≤ a + 1)]
  unfold List.count
  apply List.countP_congr
  intro y hy
  cases h1 : decide (a ≤ y && y < a + 1) <;> cases h2 : y == a <;> simp_all <;> omega

/-! The membership test of a mapped range of singleton intervals is exactly
    equality with one of the enumerated keys. -/
private theorem member_range_singleton_iff (r w n y : Nat) :
    member ((List.range n).map (fun i => (r + i * w, r + i * w + 1))) y ↔
      (List.range n).any (fun i => y == r + i * w) := by
  simp only [member, List.any_eq_true, List.mem_map, List.mem_range]
  constructor
  · rintro ⟨p, ⟨i, hi, hp⟩, hpy⟩
    subst p
    simp at hpy ⊢
    refine ⟨i, hi, ?_⟩
    omega
  · rintro ⟨i, hi, hiy⟩
    refine ⟨(r + i * w, r + i * w + 1), ⟨i, hi, rfl⟩, ?_⟩
    simp at hiy ⊢
    omega

/-! For a right key below the domain end, a singleton posting matches exactly
    when its residue is the left key's residue. -/
private theorem spans_spec (u t x y : Nat) (hyu : y < u) :
    member (search.spans u t x) y ↔ search.test t x y = true := by
  let w := t + 1
  let r := x % w
  let n := (u + w - 1 - r) / w
  have hw : 0 < w := by dsimp [w]; omega
  have hr : r < w := by dsimp [r]; exact Nat.mod_lt x hw
  have hspans : search.spans u t x = (List.range n).map (fun i => (r + i * w, r + i * w + 1)) := by rfl
  rw [hspans, member_range_singleton_iff]
  change ((List.range n).any (fun i => y == r + i * w)) ↔ (x % w == y % w) = true
  rw [show x % w = r by rfl]
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨i, hi, hiy⟩
    have hy_eq : y = r + i * w := by simpa using hiy
    rw [hy_eq, Nat.add_mod]
    simp [Nat.mod_eq_of_lt hr, Nat.mul_mod_left]
  · intro h
    have hr_eq : r = y % w := by simpa using h
    have hmod : y % w = r := hr_eq.symm
    have hdecomp : r + (y / w) * w = y := by
      calc
        r + (y / w) * w = r + w * (y / w) := by rw [Nat.mul_comm]
        _ = y := by rw [← hmod]; exact Nat.mod_add_div y w
    have hineq : (y / w + 1) * w ≤ u + w - 1 - r := by
      rw [Nat.add_mul, Nat.one_mul]
      omega
    have hle : y / w + 1 ≤ n := (Nat.le_div_iff_mul_le hw).mpr hineq
    have hi : y / w < n := by omega
    rw [List.any_eq_true]
    refine ⟨y / w, by simpa using hi, ?_⟩
    simpa using (show y = r + (y / w) * w by omega)

/-! Disjoint singleton postings: the sum of their multiplicities counts the
    union. -/
private theorem sum_count_range (ys : List Nat) (r w n : Nat) (hw : 0 < w) :
    ((List.range n).map (fun i => ys.count (r + i * w))).sum =
      ys.countP (fun y => (List.range n).any (fun i => y == r + i * w)) := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append, ih]
      simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
      have hdisj : ∀ y ∈ ys, (y == r + n * w) = true →
          ((List.range n).any (fun i => y == r + i * w)) = false := by
        intro y hy hp
        cases hany : (List.range n).any (fun i => y == r + i * w)
        · rfl
        · rw [List.any_eq_true] at hany
          rcases hany with ⟨i, hi, hiy⟩
          simp at hp hiy
          have hin : i < n := by simpa using hi
          have hmul : i * w < n * w := Nat.mul_lt_mul_of_pos_right hin hw
          omega
      have hsplit := countP_or_of_disjoint (fun y => y == r + n * w)
        (fun y => (List.range n).any (fun i => y == r + i * w)) ys hdisj
      have hcongr :
          ys.countP (fun y => (List.range n ++ [n]).any (fun i => y == r + i * w)) =
            ys.countP (fun y => (y == r + n * w) ||
              (List.range n).any (fun i => y == r + i * w)) := by
        apply List.countP_congr
        intro y hy
        rw [List.any_append]
        cases h1 : (List.range n).any (fun i => y == r + i * w) <;>
          cases h2 : y == r + n * w <;> simp_all
      rw [hcongr, ← hsplit]
      unfold List.count
      omega

theorem sound_OffsetCount : ModuloClass.OffsetCount.Goal := by
  intro u t x ys hv
  have hs := valid_pairwise hv
  let w := t + 1
  let r := x % w
  let n := (u + w - 1 - r) / w
  have hw : 0 < w := by dsimp [w]; omega
  have hspans : search.spans u t x = (List.range n).map (fun i => (r + i * w, r + i * w + 1)) := by rfl
  unfold fan
  rw [hspans]
  rw [List.map_map]
  change ((List.range n).map (fun i => lower ys (r + i * w + 1) - lower ys (r + i * w))).sum =
    ys.countP (search.test t x)
  have hterms :
      ((List.range n).map (fun i => lower ys (r + i * w + 1) - lower ys (r + i * w))) =
        ((List.range n).map (fun i => ys.count (r + i * w))) := by
    apply List.map_congr_left
    intro i hi
    exact lower_singleton_count hs (r + i * w)
  rw [hterms, sum_count_range ys r w n hw]
  apply List.countP_congr
  intro y hy
  have h := spans_spec u t x y (valid_lt hv hy)
  rw [hspans, member_range_singleton_iff] at h
  exact h

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ModuloClass_OffsetCount
