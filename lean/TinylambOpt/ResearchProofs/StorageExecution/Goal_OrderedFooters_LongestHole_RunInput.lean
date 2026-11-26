import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_RunInput

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## Generic consequences of the stitch law -/

private theorem repeatPower_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) :
    repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
    unfold repeatState
    by_cases hk : k = 0
    · simp [hk]
    · simp only [hk, dite_false]
      have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
      have ih2 := ih (k / 2) hdiv
      let a := repeatState f (f.reference xs) (k / 2)
      have ha' : a = f.reference (List.replicate (k / 2) xs).flatten := by simpa [a] using ih2
      have hjoin : f.join a a =
          f.reference ((List.replicate (k / 2) xs).flatten ++
                       (List.replicate (k / 2) xs).flatten) := by
        rw [ha']
        exact h _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++
          (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append]
        congr 1
        rw [show k / 2 * 2 = k / 2 + k / 2 by omega, ← List.replicate_append_replicate]
      by_cases he : k % 2 = 0
      · have hk2 : k / 2 * 2 = k := by omega
        have hcond : (k % 2 == 0) = true := by simp [he]
        simp only [hcond]
        change f.join a a = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, hk2]
      · have hodd : k % 2 = 1 := by omega
        have hk1 : k = k / 2 * 2 + 1 := by omega
        have hcond : (k % 2 == 0) = false := by simp [hodd]
        simp only [hcond]
        change f.join (f.join a a) (f.reference xs) =
          f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, h]
        congr 1
        rw [hk1, show k / 2 * 2 + 1 = (k / 2 + k / 2) + 1 by omega,
            ← List.replicate_append_replicate, List.flatten_append,
            List.replicate_one, List.flatten_singleton]
        have heq : (k / 2 + k / 2 + 1) / 2 * 2 = k / 2 + k / 2 := by omega
        simp [heq]

private theorem runInput_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) :
    runInput f := by
  intro rs
  have gen : ∀ (l : List (Nat × Nat)) (acc : List Nat),
      List.foldl f.join (f.reference acc)
        (l.map (fun (v, n) => repeatState f (f.reference [v]) n)) =
      f.reference (acc ++ l.flatMap (fun (v, n) => List.replicate n v)) := by
    intro l
    induction l with
    | nil => intro acc; simp
    | cons r rs ih =>
      obtain ⟨v, n⟩ := r
      intro acc
      simp only [List.map_cons, List.foldl, List.flatMap_cons]
      rw [repeatPower_of_stitch f h]
      rw [h acc (List.replicate n [v]).flatten]
      have hflat : (List.replicate n [v]).flatten = List.replicate n v := by
        induction n with
        | zero => simp
        | succ n ih2 => simp [List.replicate_succ, ih2]
      rw [hflat, ih (acc ++ List.replicate n v)]
      simp [List.append_assoc]
  simpa using gen rs []

/-! ## Elementary list facts -/

private theorem takeWhile_append_true {p : Nat → Bool} (a b : List Nat)
    (h : ∀ x ∈ a, p x = true) : (a ++ b).takeWhile p = a ++ b.takeWhile p := by
  induction a with
  | nil => simp
  | cons x xs ih =>
    simp only [List.cons_append]
    have hx : p x = true := h x (by simp)
    simp [hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_all_true {p : Nat → Bool} (a : List Nat)
    (h : ∀ x ∈ a, p x = true) : a.takeWhile p = a := by
  induction a with
  | nil => simp
  | cons x xs ih =>
    have hx : p x = true := h x (by simp)
    simp [hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_append_false {p : Nat → Bool} (a b : List Nat)
    (h : ∃ x ∈ a, p x = false) : (a ++ b).takeWhile p = a.takeWhile p := by
  induction a with
  | nil => rcases h with ⟨x, hx, _⟩; simp at hx
  | cons x xs ih =>
    simp only [List.cons_append]
    by_cases hx : p x = true
    · simp [hx]
      apply ih
      rcases h with ⟨y, hy, hfy⟩
      simp at hy
      rcases hy with rfl | hy
      · simp [hx] at hfy
      · exact ⟨y, hy, hfy⟩
    · have hx' : p x = false := by cases hp : p x <;> simp_all
      simp [hx']

private theorem mem_takeWhile_prop {p : Nat → Bool} {l : List Nat} {x : Nat}
    (h : x ∈ l.takeWhile p) : p x = true := by
  induction l with
  | nil => simp at h
  | cons a as ih =>
    simp only [List.takeWhile_cons] at h
    by_cases ha : p a = true
    · simp [ha] at h
      rcases h with h | h
      · subst a
        exact ha
      · exact ih h
    · have ha' : p a = false := by cases hp : p a <;> simp_all
      simp [ha'] at h

private theorem exists_false_of_not_all {p : Nat → Bool} {a : List Nat}
    (h : ¬ ∀ x ∈ a, p x = true) : ∃ x ∈ a, p x = false := by
  apply Classical.byContradiction
  intro hc
  apply h
  intro x hx
  apply Classical.byContradiction
  intro hne
  apply hc
  refine ⟨x, hx, ?_⟩
  cases hp : p x <;> simp_all

private theorem takeWhile_len_lt_of_false {p : Nat → Bool} {a : List Nat}
    (h : ∃ x ∈ a, p x = false) : (a.takeWhile p).length < a.length := by
  have hsub := List.takeWhile_sublist (p := p) (l := a)
  refine Nat.lt_of_le_of_ne (List.Sublist.length_le hsub) ?_
  intro heq
  have hall : a.takeWhile p = a := List.Sublist.eq_of_length hsub heq
  rcases h with ⟨x, hx, hfx⟩
  have htp := mem_takeWhile_prop (p := p) (x := x) (by rw [hall]; exact hx)
  simp [hfx] at htp

private theorem takeWhile_take_length_le {p : Nat → Bool} (l : List Nat) (n : Nat) :
    ((l.take n).takeWhile p).length ≤ (l.takeWhile p).length := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
    cases n with
    | zero => simp
    | succ n =>
      by_cases hx : p x = true
      · simp [hx, ih]
      · have hx' : p x = false := by cases hp : p x <;> simp_all
        simp [hx']

private theorem takeWhile_eq_take_length {p : Nat → Bool} (l : List Nat) :
    l.takeWhile p = l.take (l.takeWhile p).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
    rw [List.takeWhile_cons]
    by_cases ha : p a = true
    · simp [ha]
      exact ih
    · have ha' : p a = false := by cases hp : p a <;> simp_all
      simp [ha']

private theorem getD_max_le {l : List Nat} {b : Nat} (h : ∀ x ∈ l, x ≤ b) :
    l.max?.getD 0 ≤ b := by
  cases hm : l.max? with
  | none => simp [Option.getD]
  | some m =>
    simp only [Option.getD]
    exact h m (List.max?_mem hm)

private theorem le_getD_max {l : List Nat} {x : Nat} (hx : x ∈ l) :
    x ≤ l.max?.getD 0 := by
  cases hm : l.max? with
  | none => simp [List.max?_eq_none_iff.mp hm] at hx
  | some m =>
    simp only [Option.getD]
    exact (List.max?_le_iff hm).mp (Nat.le_refl _) x hx

/-! ## The cross-boundary longest-zero lemma -/

private theorem bridge_run (a b : List Nat) :
    longestZero (a ++ b) =
      max (max (longestZero a) (longestZero b))
        ((a.reverse.takeWhile (· == 0)).length + (b.takeWhile (· == 0)).length) := by
  unfold longestZero peak
  apply Nat.le_antisymm
  · apply getD_max_le
    intro x hx
    simp only [List.mem_map, List.mem_range] at hx
    rcases hx with ⟨i, hi, rfl⟩
    by_cases hia : i ≤ a.length
    · have hdrop : (a ++ b).drop i = a.drop i ++ b := by
        rw [List.drop_append]
        simp [Nat.sub_eq_zero_of_le hia]
      rw [hdrop]
      by_cases hall : ∀ y ∈ a.drop i, (y == 0) = true
      · rw [takeWhile_append_true _ _ hall]
        have hs : a.length - i ≤ (a.reverse.takeWhile (· == 0)).length := by
          have hrev : (a.drop i).reverse = a.reverse.take (a.length - i) := by
            rw [List.reverse_drop]
          have hallrev : ∀ y ∈ (a.drop i).reverse, (y == 0) = true := by
            intro y hy
            exact hall y (List.mem_reverse.mp hy)
          have heq : ((a.drop i).reverse.takeWhile (· == 0)).length =
              (a.drop i).reverse.length := by rw [takeWhile_all_true _ hallrev]
          have hle : ((a.drop i).reverse.takeWhile (· == 0)).length ≤
              (a.reverse.takeWhile (· == 0)).length := by
            rw [hrev]
            exact takeWhile_take_length_le a.reverse (a.length - i)
          have hlenrev : (a.drop i).reverse.length = a.length - i := by
            simp [List.length_drop]
          rw [heq, hlenrev] at hle
          exact hle
        rw [List.length_append]
        rw [show (a.drop i).length = a.length - i by simp [List.length_drop]]
        exact Nat.le_trans (Nat.add_le_add_right hs (b.takeWhile (· == 0)).length)
          (Nat.le_max_right _ _)
      · rw [takeWhile_append_false _ _ (exists_false_of_not_all hall)]
        have hmem : ((a.drop i).takeWhile (· == 0)).length ∈
            (List.range (a.length + 1)).map (fun j => ((a.drop j).takeWhile (· == 0)).length) :=
          List.mem_map.mpr ⟨i, by simp [List.mem_range]; omega, rfl⟩
        exact Nat.le_trans (le_getD_max hmem)
          (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
    · have hdrop : (a ++ b).drop i = b.drop (i - a.length) := by
        have hle : a.length ≤ i := by omega
        rw [List.drop_append]
        simp [hle]
      rw [hdrop]
      have hmem : ((b.drop (i - a.length)).takeWhile (· == 0)).length ∈
          (List.range (b.length + 1)).map (fun j => ((b.drop j).takeWhile (· == 0)).length) :=
        List.mem_map.mpr ⟨i - a.length, by simp [List.mem_range] at hi ⊢; omega, rfl⟩
      exact Nat.le_trans (le_getD_max hmem)
        (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
  · apply Nat.max_le.mpr
    constructor
    · apply Nat.max_le.mpr
      constructor
      · apply getD_max_le
        intro x hx
        simp only [List.mem_map, List.mem_range] at hx
        rcases hx with ⟨i, hi, rfl⟩
        have hmem2 : (((a ++ b).drop i).takeWhile (· == 0)).length ∈
            (List.range ((a ++ b).length + 1)).map
              (fun j => (((a ++ b).drop j).takeWhile (· == 0)).length) :=
          List.mem_map.mpr ⟨i, by simp [List.mem_range] at hi ⊢; omega, rfl⟩
        have hle2 := le_getD_max hmem2
        have hdrop : (a ++ b).drop i = a.drop i ++ b := by
          rw [List.drop_append]
          simp [Nat.sub_eq_zero_of_le (by omega : i ≤ a.length)]
        rw [hdrop] at hle2
        by_cases hall : ∀ y ∈ a.drop i, (y == 0) = true
        · rw [takeWhile_append_true _ _ hall] at hle2
          rw [takeWhile_all_true _ hall]
          simp only [List.length_append]
          simp [List.length_drop] at hle2 ⊢
          omega
        · rw [takeWhile_append_false _ _ (exists_false_of_not_all hall)] at hle2
          exact hle2
      · apply getD_max_le
        intro x hx
        simp only [List.mem_map, List.mem_range] at hx
        rcases hx with ⟨j, hj, rfl⟩
        have hdrop : (a ++ b).drop (a.length + j) = b.drop j := by simp [List.drop_append]
        have hmem2 : ((b.drop j).takeWhile (· == 0)).length ∈
            (List.range ((a ++ b).length + 1)).map
              (fun k => (((a ++ b).drop k).takeWhile (· == 0)).length) :=
          List.mem_map.mpr ⟨a.length + j, by
            simp [List.mem_range] at hj ⊢
            omega, by rw [hdrop]⟩
        exact le_getD_max hmem2
    · have hs : (a.reverse.takeWhile (· == 0)).length ≤ a.length := by
        have hle := List.Sublist.length_le
          (List.takeWhile_sublist (p := (· == 0)) (l := a.reverse))
        simpa [List.length_reverse] using hle
      let i := a.length - (a.reverse.takeWhile (· == 0)).length
      have hi_eq : i = a.length - (a.reverse.takeWhile (· == 0)).length := rfl
      have hi_le : i ≤ a.length := by omega
      have hdrop : (a ++ b).drop i = a.drop i ++ b := by
        rw [List.drop_append]
        simp [hi_le]
      have hall : ∀ x ∈ a.drop i, (x == 0) = true := by
        intro x hx
        have hmem_rev : x ∈ a.reverse.take ((a.reverse.takeWhile (· == 0)).length) := by
          have hxrev : x ∈ (a.drop i).reverse := List.mem_reverse.mpr hx
          have hrev_drop : (a.drop i).reverse =
              a.reverse.take (a.length - i) := by
            rw [List.reverse_drop]
          rw [hrev_drop] at hxrev
          rw [List.mem_take_iff_getElem] at hxrev ⊢
          rcases hxrev with ⟨j, hj, rfl⟩
          exact ⟨j, by omega, rfl⟩
        have hsub : a.reverse.take ((a.reverse.takeWhile (· == 0)).length) =
            a.reverse.takeWhile (· == 0) := (takeWhile_eq_take_length a.reverse).symm
        rw [hsub] at hmem_rev
        exact mem_takeWhile_prop (p := (· == 0)) (x := x) hmem_rev
      have htake : ((a ++ b).drop i).takeWhile (· == 0) =
          a.drop i ++ b.takeWhile (· == 0) := by
        rw [hdrop]
        exact takeWhile_append_true _ _ hall
      have hlen_a : (a.drop i).length = (a.reverse.takeWhile (· == 0)).length := by
        have hld := List.length_drop (i := i) (l := a)
        omega
      have hlen : (((a ++ b).drop i).takeWhile (· == 0)).length =
          (a.reverse.takeWhile (· == 0)).length + (b.takeWhile (· == 0)).length := by
        rw [htake, List.length_append, hlen_a]
      have hmem2 : (((a ++ b).drop i).takeWhile (· == 0)).length ∈
          (List.range ((a ++ b).length + 1)).map
            (fun k => (((a ++ b).drop k).takeWhile (· == 0)).length) :=
        List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
      rw [← hlen]
      exact le_getD_max hmem2

/-! ## Stitch law for `agg` -/

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [agg, List.length_append]
  refine Prod.ext rfl (Prod.ext ?_ (Prod.ext ?_ ?_))
  · by_cases ha : ∀ x ∈ a, (x == 0) = true
    · have hta : a.takeWhile (· == 0) = a := takeWhile_all_true a ha
      have ht : (a ++ b).takeWhile (· == 0) = a ++ b.takeWhile (· == 0) :=
        takeWhile_append_true a b ha
      have hcond : ((a.takeWhile (· == 0)).length == a.length) = true := by simp [hta]
      rw [hcond, ht]
      simp [List.length_append]
    · have hf : ∃ x ∈ a, (x == 0) = false := exists_false_of_not_all ha
      have ht : (a ++ b).takeWhile (· == 0) = a.takeWhile (· == 0) :=
        takeWhile_append_false a b hf
      have hne : (a.takeWhile (· == 0)).length ≠ a.length := by
        intro heq
        have hsub := List.takeWhile_sublist (p := (· == 0)) (l := a)
        have hall : a.takeWhile (· == 0) = a := List.Sublist.eq_of_length hsub heq
        rcases hf with ⟨x, hx, hfx⟩
        have htp := mem_takeWhile_prop (p := (· == 0)) (x := x) (by rw [hall]; exact hx)
        simp [hfx] at htp
      have hcond : ((a.takeWhile (· == 0)).length == a.length) = false := by simp [hne]
      rw [hcond, ht]
      simp
  · by_cases hb : ∀ x ∈ b, (x == 0) = true
    · have hrb : b.reverse.takeWhile (· == 0) = b.reverse :=
        takeWhile_all_true b.reverse (by intro y hy; exact hb y (List.mem_reverse.mp hy))
      have htab : (a ++ b).reverse.takeWhile (· == 0) =
          b.reverse ++ a.reverse.takeWhile (· == 0) := by
        rw [List.reverse_append]
        exact takeWhile_append_true b.reverse a.reverse
          (by intro y hy; exact hb y (List.mem_reverse.mp hy))
      have hcond : ((b.reverse.takeWhile (· == 0)).length == b.length) = true := by
        simp [hrb, List.length_reverse]
      rw [hcond, htab]
      simp [List.length_append]
    · have hf : ∃ x ∈ b, (x == 0) = false := exists_false_of_not_all hb
      have hfR : ∃ x ∈ b.reverse, (x == 0) = false := by
        rcases hf with ⟨x, hx, hfx⟩
        exact ⟨x, List.mem_reverse.mpr hx, hfx⟩
      have htab : (a ++ b).reverse.takeWhile (· == 0) = b.reverse.takeWhile (· == 0) := by
        rw [List.reverse_append]
        exact takeWhile_append_false b.reverse a.reverse hfR
      have hne : (b.reverse.takeWhile (· == 0)).length ≠ b.length := by
        intro heq
        have heq' : (b.reverse.takeWhile (· == 0)).length = b.reverse.length := by
          simpa [List.length_reverse] using heq
        have hsub := List.takeWhile_sublist (p := (· == 0)) (l := b.reverse)
        have hall : b.reverse.takeWhile (· == 0) = b.reverse :=
          List.Sublist.eq_of_length hsub heq'
        rcases hfR with ⟨x, hx, hfx⟩
        have htp := mem_takeWhile_prop (p := (· == 0)) (x := x) (by rw [hall]; exact hx)
        simp [hfx] at htp
      have hcond : ((b.reverse.takeWhile (· == 0)).length == b.length) = false := by
        simp [hne]
      rw [hcond, htab]
      simp
  · change max (max (longestZero a) (longestZero b))
        ((a.reverse.takeWhile (· == 0)).length + (b.takeWhile (· == 0)).length) =
      longestZero (a ++ b)
    exact (bridge_run a b).symm

/-! ## The goal -/

theorem sound_RunInput : LongestHole.RunInput.Goal := by
  exact runInput_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_RunInput
