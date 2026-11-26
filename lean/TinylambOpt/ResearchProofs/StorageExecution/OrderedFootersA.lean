import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `LongestHole.BridgeRun` — `∀ (a b : List Nat), let s := (a.reverse.takeWhile (· ==
  0)).length let p := (b.takeWhile (· == 0)).length longestZero (a++b)=max (max
  (longestZero a) (longestZero b)) (s+p)`
- `LongestHole.ZeroFill` — `∀ n, agg.reference (List.replicate n 0) = (n,n,n,n)`
- `PairSubsequence.BatchedMarkers` — `∀ a b, agg.reference (List.replicate a 1 ++
  List.replicate b 2)=(a,b,a*b)`
- `PairSubsequence.ReversalComplement` — `∀ xs, (agg.reference xs).2.2 + (agg.reference
  xs.reverse).2.2 = xs.count 1 * xs.count 2`
-/

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFootersA

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## Generic consequences of a stitching fold -/

section Generic

variable {σ : Type} (f : Fold σ)

private theorem foldl_reference (h : stitch f) (l : List (List Nat)) (acc : List Nat) :
    List.foldl f.join (f.reference acc) (l.map f.reference) =
      f.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem combine_reference (h : stitch f) (ps : List (List Nat)) :
    combine f ps = f.reference ps.flatten := by
  simpa [combine] using foldl_reference f h ps []

private theorem treeReduce_of_stitch (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      change f.join (rootState f a) (rootState f b) =
        f.reference (plain a ++ plain b)
      rw [iha, ihb, h]

private theorem flatten_replicate_add (n m : Nat) (xs : List Nat) :
    (List.replicate (n + m) xs).flatten =
      (List.replicate n xs).flatten ++ (List.replicate m xs).flatten := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show n + 1 + m = (n + m) + 1 by omega, List.replicate_succ,
        List.replicate_succ, List.flatten_cons, ih, List.flatten_cons]
      exact (List.append_assoc _ _ _).symm

private theorem flatten_replicate_double (n : Nat) (xs : List Nat) :
    (List.replicate (n * 2) xs).flatten =
      (List.replicate n xs).flatten ++ (List.replicate n xs).flatten := by
  rw [← flatten_replicate_add]
  apply congrArg List.flatten
  apply congrArg (fun n => List.replicate n xs)
  omega

private theorem flatten_replicate_rotate (n : Nat) (xs : List Nat) :
    (List.replicate n xs).flatten ++ xs = xs ++ (List.replicate n xs).flatten := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp only [List.replicate_succ, List.flatten_cons]
      calc
        (xs ++ (List.replicate n xs).flatten) ++ xs =
            xs ++ ((List.replicate n xs).flatten ++ xs) := List.append_assoc _ _ _
        _ = xs ++ (xs ++ (List.replicate n xs).flatten) := by rw [ih]

private theorem flatten_replicate_double_succ (n : Nat) (xs : List Nat) :
    ((List.replicate n xs).flatten ++ (List.replicate n xs).flatten) ++ xs =
      (List.replicate (n * 2 + 1) xs).flatten := by
  rw [show n * 2 + 1 = (n + n) + 1 by omega, List.replicate_succ,
    List.flatten_cons, flatten_replicate_add]
  let a := (List.replicate n xs).flatten
  calc
    (a ++ a) ++ xs = a ++ (a ++ xs) := List.append_assoc _ _ _
    _ = a ++ (xs ++ a) := by rw [flatten_replicate_rotate]
    _ = xs ++ (a ++ a) := by
      rw [← List.append_assoc, flatten_replicate_rotate, List.append_assoc]

private theorem repeatPower_of_stitch (h : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
      by_cases hk : k = 0
      · subst k
        simp [repeatState]
      · rw [repeatState.eq_1, dite_eq_right hk]
        have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
        have ih2 := ih (k / 2) hdiv
        have hsq : f.join (repeatState f (f.reference xs) (k / 2))
              (repeatState f (f.reference xs) (k / 2)) =
            f.reference ((List.replicate (k / 2) xs).flatten ++
              (List.replicate (k / 2) xs).flatten) := by
          simp only [ih2]
          exact h _ _
        have hdecomp : k / 2 * 2 + k % 2 = k := by
          rw [Nat.mul_comm]
          exact Nat.div_add_mod k 2
        by_cases he : k % 2 = 0
        · simp only [he, beq_self_eq_true, ite_true]
          rw [hsq]
          apply congrArg f.reference
          calc
            (List.replicate (k / 2) xs).flatten ++
                (List.replicate (k / 2) xs).flatten =
                (List.replicate (k / 2 * 2) xs).flatten :=
                  (flatten_replicate_double (k / 2) xs).symm
            _ = (List.replicate k xs).flatten :=
                  congrArg (fun n => (List.replicate n xs).flatten)
                    (by simpa [he] using hdecomp)
        · have ho : k % 2 = 1 := by omega
          simp only [ho, show (1 == 0) = false by decide, Bool.false_eq_true, ite_false]
          rw [hsq, h]
          apply congrArg f.reference
          calc
            ((List.replicate (k / 2) xs).flatten ++
                (List.replicate (k / 2) xs).flatten) ++ xs =
                (List.replicate (k / 2 * 2 + 1) xs).flatten :=
                  flatten_replicate_double_succ (k / 2) xs
            _ = (List.replicate k xs).flatten :=
                  congrArg (fun n => (List.replicate n xs).flatten)
                    (by simpa [ho] using hdecomp)

private theorem slice_concat (a p z : List Nat) (lo hi : Nat) (hlo : lo ≤ a.length) :
    a.drop lo ++ p ++ z.take hi =
      ((a ++ p ++ z).drop lo).take (a.length - lo + p.length + hi) := by
  rw [show a ++ p ++ z = a ++ (p ++ z) by simp only [List.append_assoc],
    List.drop_append_of_le_length hlo, List.take_append]
  have hd : (a.drop lo).length = a.length - lo := by simp
  rw [hd]
  have hN : a.length - lo + p.length + hi - (a.length - lo) = p.length + hi := by omega
  rw [hN, List.take_append]
  have hp : p.length + hi - p.length = hi := by omega
  rw [hp]
  have htakeA : List.take (a.length - lo + p.length + hi) (a.drop lo) = a.drop lo :=
    List.take_of_length_le (by simp [hd]; omega)
  have htakeP : List.take (p.length + hi) p = p :=
    List.take_of_length_le (by omega)
  rw [htakeA, htakeP]
  exact List.append_assoc _ _ _

private theorem frameSlice_of_stitch (h : stitch f) : frameSlice f := by
  intro a ps z lo hi hlo
  rw [combine_reference f h, h, h]
  refine congrArg f.reference ?_
  simpa only [List.append_assoc] using slice_concat a ps.flatten z lo hi hlo

private theorem prefixCheckpoint_of_stitch (h : stitch f) : prefixCheckpoint f := by
  intro init ps
  apply List.ext_getElem
  · simp
  · intro i hi₁ hi₂
    rw [List.getElem_map (fun i => f.reference (init ++ (ps.take i).flatten)),
      List.getElem_range, List.getElem_scanl]
    rw [← List.map_take]
    exact foldl_reference f h (ps.take i) init

private theorem flatten_set (ps : List (List Nat)) (i : Nat) (xs : List Nat)
    (hi : i < ps.length) :
    (ps.set i xs).flatten =
      (ps.take i).flatten ++ xs ++ (ps.drop (i + 1)).flatten := by
  induction ps generalizing i with
  | nil => simp at hi
  | cons p ps ih =>
      cases i with
      | zero => simp
      | succ j =>
          have hj : j < ps.length := by simpa using hi
          simp only [List.set_cons_succ, List.take_succ_cons, List.drop_succ_cons,
            List.flatten_cons, ih j hj, List.append_assoc]

private theorem amendLeaf_of_stitch (h : stitch f) : amendLeaf f := by
  intro ps i xs hi
  simp only [combine_reference f h]
  rw [flatten_set ps i xs hi, h, h]
  exact congrArg f.reference (by simp only [List.append_assoc])

private theorem runInput_of_stitch (h : stitch f) : runInput f := by
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
        have hflat : (List.replicate n [v]).flatten = List.replicate n v := by
          induction n with
          | zero => simp
          | succ n ihn => simp [List.replicate_succ, ihn]
        rw [hflat, h, ih]
        simp only [List.append_assoc]
  simpa using gen rs []

private theorem reference_append (h : stitch f) (a b : List Nat) :
    f.reference (a ++ b) = f.join (f.reference a) (f.reference b) := (h a b).symm

private theorem reference_join_assoc (h : stitch f) (a b c : List Nat) :
    f.join (f.join (f.reference a) (f.reference b)) (f.reference c) =
      f.join (f.reference a) (f.join (f.reference b) (f.reference c)) := by
  rw [← reference_append f h a b, ← reference_append f h b c,
    ← reference_append f h (a ++ b) c, ← reference_append f h a (b ++ c)]
  exact congrArg f.reference (List.append_assoc _ _ _)

private theorem reorderNeighbors_of_stitch (h : stitch f) : reorderNeighbors f := by
  intro pre a b suf hcomm
  have hab : f.reference (a ++ b) = f.join (f.reference a) (f.reference b) :=
    reference_append f h a b
  have hba : f.reference (b ++ a) = f.join (f.reference b) (f.reference a) :=
    reference_append f h b a
  calc
    f.reference (pre ++ a ++ b ++ suf) =
        f.join (f.reference (pre ++ (a ++ b))) (f.reference suf) := by
          rw [show pre ++ a ++ b ++ suf = (pre ++ (a ++ b)) ++ suf by
            simp only [List.append_assoc]]
          rw [reference_append f h (pre ++ (a ++ b)) suf]
    _ = f.join (f.reference pre)
          (f.join (f.reference (a ++ b)) (f.reference suf)) := by
            rw [reference_append f h pre (a ++ b)]
            exact reference_join_assoc f h pre (a ++ b) suf
    _ = f.join (f.reference pre)
          (f.join (f.join (f.reference a) (f.reference b)) (f.reference suf)) := by
            rw [hab]
    _ = f.join (f.reference pre)
          (f.join (f.join (f.reference b) (f.reference a)) (f.reference suf)) := by
            rw [hcomm]
    _ = f.join (f.reference pre)
          (f.join (f.reference (b ++ a)) (f.reference suf)) := by rw [← hba]
    _ = f.join (f.reference (pre ++ (b ++ a))) (f.reference suf) := by
            rw [reference_append f h pre (b ++ a)]
            exact (reference_join_assoc f h pre (b ++ a) suf).symm
    _ = f.reference (pre ++ b ++ a ++ suf) := by
          rw [show pre ++ b ++ a ++ suf = (pre ++ (b ++ a)) ++ suf by
            simp only [List.append_assoc]]
          rw [reference_append f h (pre ++ (b ++ a)) suf]

end Generic

/-! ## Small list facts available in Lean core -/

private theorem acc_le_foldl_max (xs : List Nat) (a : Nat) :
    a ≤ xs.foldl max a := by
  induction xs generalizing a with
  | nil => simp
  | cons x xs ih =>
      exact le_trans (Nat.le_Nat.max_left a x) (ih (max a x))

private theorem mem_le_foldl_max {xs : List Nat} {a x : Nat} (hx : x ∈ xs) :
    x ≤ xs.foldl max a := by
  induction xs generalizing a with
  | nil => simp at hx
  | cons y ys ih =>
      rcases List.mem_cons.mp hx with rfl | hx
      · exact le_trans (Nat.le_max_right a y) (acc_le_foldl_max ys (max a y))
      · exact ih hx

private theorem foldl_Nat.max_le (xs : List Nat) (a n : Nat) :
    xs.foldl max a ≤ n ↔ a ≤ n ∧ ∀ x ∈ xs, x ≤ n := by
  induction xs generalizing a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl, ih, Nat.Nat.max_le, List.mem_cons]
      constructor
      · rintro ⟨⟨ha, hx⟩, hs⟩
        exact ⟨ha, hx, hs⟩
      · rintro ⟨ha, hx, hs⟩
        exact ⟨⟨ha, hx⟩, hs⟩

private theorem max_getD_le (xs : List Nat) (n : Nat) :
    xs.max?.getD 0 ≤ n ↔ ∀ x ∈ xs, x ≤ n := by
  cases xs with
  | nil => simp [List.max?]
  | cons x xs => simpa [List.max?] using foldl_Nat.max_le xs x n

private theorem le_max_getD_of_mem {xs : List Nat} {x : Nat} (hx : x ∈ xs) :
    x ≤ xs.max?.getD 0 := by
  cases xs with
  | nil => simp at hx
  | cons y ys =>
      rcases List.mem_cons.mp hx with rfl | hx
      · simpa [List.max?] using acc_le_foldl_max ys y
      · simpa [List.max?] using mem_le_foldl_max hx

private theorem length_takeWhile_le (p : Nat → Bool) (xs : List Nat) :
    (xs.takeWhile p).length ≤ xs.length :=
  List.Sublist.length_le List.takeWhile_sublist

private theorem mem_takeWhile_imp {p : Nat → Bool} {xs : List Nat} {x : Nat}
    (hx : x ∈ xs.takeWhile p) : p x = true := by
  induction xs with
  | nil => simp at hx
  | cons y ys ih =>
      simp only [List.takeWhile_cons] at hx
      by_cases hy : p y = true
      · simp only [hy, if_true, List.mem_cons] at hx
        rcases hx with rfl | hx
        · exact hy
        · exact ih hx
      · simp [hy] at hx

private theorem takeWhile_all_true {p : Nat → Bool} (xs : List Nat)
    (h : ∀ x ∈ xs, p x = true) : xs.takeWhile p = xs := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = true := h x (by simp)
      simp [List.takeWhile_cons, hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_eq_self_iff (p : Nat → Bool) (xs : List Nat) :
    xs.takeWhile p = xs ↔ ∀ x ∈ xs, p x = true := by
  constructor
  · intro h x hx
    rw [← h] at hx
    exact mem_takeWhile_imp hx
  · exact takeWhile_all_true xs

private theorem take_takeWhile (p : Nat → Bool) (xs : List Nat) :
    xs.take (xs.takeWhile p).length = xs.takeWhile p := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : p x = true
      · simp [List.takeWhile_cons, hx, ih]
      · simp [List.takeWhile_cons, hx]

/-! ## LongestHole -/

namespace LongestHole

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole

private theorem takeWhile_append_true {p : Nat → Bool} (a b : List Nat)
    (h : ∀ x ∈ a, p x = true) : (a ++ b).takeWhile p = a ++ b.takeWhile p := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.cons_append, List.takeWhile_cons]
      have hx : p x = true := h x (by simp)
      simp [hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_all_true {p : Nat → Bool} (a : List Nat)
    (h : ∀ x ∈ a, p x = true) : a.takeWhile p = a := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = true := h x (by simp)
      simp [List.takeWhile_cons, hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_append_false {p : Nat → Bool} (a b : List Nat)
    (h : ∃ x ∈ a, p x = false) : (a ++ b).takeWhile p = a.takeWhile p := by
  induction a with
  | nil => rcases h with ⟨x, hx, _⟩; simp at hx
  | cons x xs ih =>
      simp only [List.cons_append, List.takeWhile_cons]
      by_cases hx : p x = true
      · simp [hx]
        have h' : ∃ y ∈ xs, p y = false := by
          rcases h with ⟨y, hy, hfy⟩
          simp at hy
          rcases hy with rfl | hy
          · simp [hx] at hfy
          · exact ⟨y, hy, hfy⟩
        exact ih h'
      · have hx' : p x = false := by cases hp : p x <;> simp_all
        simp [hx']

private theorem bridgeRun : BridgeRun.Goal := by
  intro a b
  unfold longestZero peak
  apply Nat.le_antisymm
  · apply max_getD_le.mpr
    intro x hx
    simp only [List.mem_map, List.mem_range] at hx
    rcases hx with ⟨i, hi, rfl⟩
    have hi' : i < (a ++ b).length + 1 := by simpa [List.length_range] using hi
    by_cases hia : i ≤ a.length
    · by_cases hsplit : a.length ≤ i + (((a ++ b).drop i).takeWhile (· == 0)).length
      · have h1 : (((a ++ b).drop i).takeWhile (· == 0)).length ≤
            (a.length - i) + (b.takeWhile (· == 0)).length := by
          have hdrop : (a ++ b).drop i = a.drop i ++ b := by
            rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le hia]
          rw [hdrop]
          by_cases hall : ∀ x ∈ a.drop i, (x == 0) = true
          · rw [takeWhile_append_true _ _ hall, takeWhile_all_true _ hall]
            simp [List.length_append, List.length_drop]; omega
          · simp only [Classical.not_forall] at hall
            rcases hall with ⟨y, hy, hfy⟩
            have hfy' : (y == 0) = false := by revert hfy; cases (y == 0) <;> simp
            rw [takeWhile_append_false _ _ ⟨y, hy, hfy'⟩]
            have := length_takeWhile_le (p := (· == 0)) (l := a.drop i)
            simp [List.length_drop] at this ⊢; omega
        have h2 : (a.length - i) ≤ (a.reverse.takeWhile (· == 0)).length := by
          have hall : ∀ x ∈ a.drop i, (x == 0) = true := by
            intro x hx
            by_cases hc : (x == 0) = true
            · exact hc
            · have hfx' : (x == 0) = false := by cases hh : (x == 0) <;> simp_all
              have hdrop : (a ++ b).drop i = a.drop i ++ b := by
                rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le hia]
              rw [hdrop, takeWhile_append_false _ _ ⟨x, hx, hfx'⟩] at hsplit
              have := length_takeWhile_le (p := (· == 0)) (l := a.drop i)
              simp [List.length_drop] at this hsplit; omega
          have hrev : (a.drop i).reverse = a.reverse.take (a.length - i) := by
            rw [List.reverse_drop]; congr 1; omega
          have hallrev : ∀ x ∈ (a.drop i).reverse, (x == 0) = true := by
            intro x hx; exact hall x (List.mem_reverse.mp hx)
          have hsub : (a.drop i).reverse.length ≤ (a.reverse.takeWhile (· == 0)).length := by
            calc (a.drop i).reverse.length
                = ((a.drop i).reverse.takeWhile (· == 0)).length := by rw [takeWhile_all_true _ hallrev]
              _ ≤ (a.reverse.takeWhile (· == 0)).length := by
                  apply List.Sublist.length_le
                  rw [hrev]
                  exact List.takeWhile_sublist.trans (List.take_sublist _ _)
          simpa [List.length_reverse, List.length_drop] using hsub
        apply le_trans (by omega : (((a ++ b).drop i).takeWhile (· == 0)).length ≤
            (a.reverse.takeWhile (· == 0)).length + (b.takeWhile (· == 0)).length)
        exact le_max_of_le_right le_rfl
      · have hsplit' : i + (((a ++ b).drop i).takeWhile (· == 0)).length < a.length := by
          omega
        have hdrop : (a ++ b).drop i = a.drop i ++ b := by
          rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le hia]
        rw [hdrop]
        have hall_or : (((a.drop i) ++ b).takeWhile (· == 0)).length = ((a.drop i).takeWhile (· == 0)).length := by
          by_cases hall : ∀ x ∈ a.drop i, (x == 0) = true
          · rw [takeWhile_append_true _ _ hall, takeWhile_all_true _ hall]
            simp [List.length_append, List.length_drop] at hsplit' ⊢; omega
          · simp only [Classical.not_forall] at hall
            rcases hall with ⟨y, hy, hfy⟩
            have hfy' : (y == 0) = false := by revert hfy; cases (y == 0) <;> simp
            rw [takeWhile_append_false _ _ ⟨y, hy, hfy'⟩]
        rw [hall_or]
        have hmem : ((a.drop i).takeWhile (· == 0)).length ∈
            (List.range (a.length + 1)).map (fun j => ((a.drop j).takeWhile (· == 0)).length) := by
          exact List.mem_map.mpr ⟨i, by simp [List.mem_range]; omega, rfl⟩
        apply le_trans (le_max_getD_of_mem hmem)
        simp only [Option.getD_some]
        exact le_Nat.max_left (le_Nat.max_left le_rfl) le_rfl
    · have hia' : a.length < i := by omega
      have hdrop : (a ++ b).drop i = b.drop (i - a.length) := by
        rw [List.drop_append]; simp [hia']
      rw [hdrop]
      have hmem : ((b.drop (i - a.length)).takeWhile (· == 0)).length ∈
          (List.range (b.length + 1)).map (fun j => ((b.drop j).takeWhile (· == 0)).length) := by
        exact List.mem_map.mpr ⟨i - a.length, by simp [List.mem_range, List.length_append] at hi' ⊢; omega, rfl⟩
      apply le_trans (le_max_getD_of_mem hmem)
      simp only [Option.getD_some]
      exact le_Nat.max_left (le_max_right le_rfl) le_rfl
  · apply Nat.max_le
    · apply Nat.max_le
      · unfold longestZero peak
        apply max_getD_le.mpr
        intro x hx
        simp only [List.mem_map, List.mem_range] at hx
        rcases hx with ⟨i, hi, rfl⟩
        have hi' : i < a.length + 1 := by simpa [List.length_range] using hi
        by_cases hcross : a.length ≤ i + ((a.drop i).takeWhile (· == 0)).length
        · have hall : ∀ x ∈ a.drop i, (x == 0) = true := by
            intro x hx
            by_cases hc : (x == 0) = true
            · exact hc
            · have hfx' : (x == 0) = false := by cases hh : (x == 0) <;> simp_all
              have hne : (a.drop i).takeWhile (· == 0) ≠ a.drop i := by
                intro heq; rw [heq] at hfx'; exact absurd (mem_takeWhile_imp hx) (by simp [hfx'])
              have hle : ((a.drop i).takeWhile (· == 0)).length < (a.drop i).length := by
                have := length_takeWhile_le (p := (· == 0)) (l := a.drop i)
                exact lt_of_le_of_ne this (fun h => hne (List.Sublist.eq_of_length List.takeWhile_sublist (by rw [h])))
              simp [List.length_drop] at hle; omega
          have hdrop : (a ++ b).drop i = a.drop i ++ b := by
            rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (by omega : i ≤ a.length)]
          have htake : ((a ++ b).drop i).takeWhile (· == 0) = a.drop i ++ b.takeWhile (· == 0) := by
            rw [hdrop]; exact takeWhile_append_true _ _ hall
          have hmem : (((a++b).drop i).takeWhile (· == 0)).length ∈
              (List.range ((a++b).length+1)).map (fun j => (((a++b).drop j).takeWhile (· == 0)).length) := by
            exact List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
          apply le_trans _ (le_max_getD_of_mem hmem)
          simp only [Option.getD_some, htake, List.length_append, takeWhile_all_true _ hall, List.length_drop]
          omega
        · have hcross' : i + ((a.drop i).takeWhile (· == 0)).length < a.length := by omega
          have hdrop : (a ++ b).drop i = a.drop i ++ b := by
            rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (by omega : i ≤ a.length)]
          have hall_or : (((a.drop i) ++ b).takeWhile (· == 0)).length = ((a.drop i).takeWhile (· == 0)).length := by
            by_cases hall : ∀ x ∈ a.drop i, (x == 0) = true
            · rw [takeWhile_append_true _ _ hall, takeWhile_all_true _ hall]
              simp [List.length_append, List.length_drop] at hcross' ⊢; omega
            · simp only [Classical.not_forall] at hall
              rcases hall with ⟨y, hy, hfy⟩
              have hfy' : (y == 0) = false := by revert hfy; cases (y == 0) <;> simp
              rw [takeWhile_append_false _ _ ⟨y, hy, hfy'⟩]
          have hmem : (((a++b).drop i).takeWhile (· == 0)).length ∈
              (List.range ((a++b).length+1)).map (fun j => (((a++b).drop j).takeWhile (· == 0)).length) := by
            exact List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
          apply le_trans _ (le_max_getD_of_mem hmem)
          simp only [Option.getD_some, hdrop, hall_or]
      · unfold longestZero peak
        apply max_getD_le.mpr
        intro x hx
        simp only [List.mem_map, List.mem_range] at hx
        rcases hx with ⟨j, hj, rfl⟩
        have hj' : j < b.length + 1 := by simpa [List.length_range] using hj
        have hdrop : (a ++ b).drop (a.length + j) = b.drop j := by simp [List.drop_append]
        have hmem : (((a++b).drop (a.length+j)).takeWhile (· == 0)).length ∈
            (List.range ((a++b).length+1)).map (fun k => (((a++b).drop k).takeWhile (· == 0)).length) := by
          exact List.mem_map.mpr ⟨a.length + j, by simp [List.mem_range, List.length_append]; omega, rfl⟩
        apply le_trans _ (le_max_getD_of_mem hmem)
        simp [hdrop]
    · let s := (a.reverse.takeWhile (· == 0)).length
      let p := (b.takeWhile (· == 0)).length
      have hs : s ≤ a.length := by
        have : (a.reverse.takeWhile (· == 0)).length ≤ a.reverse.length := length_takeWhile_le ..
        simpa [List.length_reverse] using this
      let i := a.length - s
      have hdrop : (a ++ b).drop i = a.drop i ++ b := by
        rw [List.drop_append]; simp [Nat.sub_eq_zero_of_le (by omega : i ≤ a.length)]
      have hall : ∀ x ∈ a.drop i, (x == 0) = true := by
        intro x hx
        have hmem_rev : x ∈ (a.reverse.take s) := by
          rw [List.mem_take]
          constructor
          · exact List.mem_reverse.mpr hx
          · have hrev_drop : (a.drop i).reverse = a.reverse.take s := by
              rw [List.reverse_drop]; congr 1; omega
            have : x ∈ (a.drop i).reverse := List.mem_reverse.mpr hx
            rw [hrev_drop] at this
            exact (List.mem_take.mp this).2
        have hsub : a.reverse.take s = a.reverse.takeWhile (· == 0) := by
          exact take_takeWhile _ _
        rw [hsub] at hmem_rev
        exact mem_takeWhile_imp hmem_rev
      have htake : ((a ++ b).drop i).takeWhile (· == 0) = a.drop i ++ b.takeWhile (· == 0) := by
        rw [hdrop]; exact takeWhile_append_true _ _ hall
      have hmem : (((a++b).drop i).takeWhile (· == 0)).length ∈
          (List.range ((a++b).length+1)).map (fun k => (((a++b).drop k).takeWhile (· == 0)).length) := by
        exact List.mem_map.mpr ⟨i, by simp [List.mem_range, List.length_append]; omega, rfl⟩
      apply le_trans _ (le_max_getD_of_mem hmem)
      simp only [Option.getD_some, htake, List.length_append, List.length_drop, takeWhile_all_true _ hall]
      omega

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [stitch, agg, List.length_append]
  refine Prod.ext rfl (Prod.ext ?_ (Prod.ext ?_ ?_))
  · by_cases ha : ∀ x ∈ a, (x == 0) = true
    · have ht : (a ++ b).takeWhile (· == 0) = a ++ b.takeWhile (· == 0) := takeWhile_append_true a b ha
      have hta : a.takeWhile (· == 0) = a := takeWhile_all_true a ha
      simp [ht, hta, List.length_append]
    · simp only [Classical.not_forall] at ha
      rcases ha with ⟨x, hx, hfx⟩
      have hfx' : (x == 0) = false := by revert hfx; cases (x == 0) <;> simp
      have ht : (a ++ b).takeWhile (· == 0) = a.takeWhile (· == 0) := takeWhile_append_false a b ⟨x, hx, hfx'⟩
      simp [ht]
      have hne : (a.takeWhile (· == 0)).length ≠ a.length := by
        intro heq
        have hall : a.takeWhile (· == 0) = a := (takeWhile_eq_self_iff _ _).mpr (by
          intro y hy
          by_cases hc : (y == 0) = true
          · exact hc
          · have hc' : (y == 0) = false := by cases hh : (y == 0) <;> simp_all
            have := mem_takeWhile_imp hy
            simp [hc'] at this)
        rw [hall] at hx
        have := mem_takeWhile_imp hx
        simp [hfx'] at this
      simp [hne]
  · by_cases hb : ∀ x ∈ b, (x == 0) = true
    · have hrb : b.reverse.takeWhile (· == 0) = b.reverse := takeWhile_all_true b.reverse (by intro y hy; exact hb y (List.mem_reverse.mp hy))
      have htab : (a ++ b).reverse.takeWhile (· == 0) = b.reverse ++ a.reverse.takeWhile (· == 0) := by
        rw [List.reverse_append]; exact takeWhile_append_true b.reverse a.reverse (by intro y hy; exact hb y (List.mem_reverse.mp hy))
      simp [htab, hrb]
      by_cases ht : (b.reverse.takeWhile (· == 0)).length == b.length <;> simp [ht, hrb]
    · simp only [Classical.not_forall] at hb
      rcases hb with ⟨x, hx, hfx⟩
      have hfx' : (x == 0) = false := by revert hfx; cases (x == 0) <;> simp
      have hxr : x ∈ b.reverse := List.mem_reverse.mpr hx
      have htab : (a ++ b).reverse.takeWhile (· == 0) = b.reverse.takeWhile (· == 0) := by
        rw [List.reverse_append]; exact takeWhile_append_false b.reverse a.reverse ⟨x, hxr, hfx'⟩
      rw [htab]
      have hne : (b.reverse.takeWhile (· == 0)).length ≠ b.length := by
        intro heq
        have hall : b.reverse.takeWhile (· == 0) = b.reverse := (takeWhile_eq_self_iff _ _).mpr (by
          intro y hy
          by_cases hc : (y == 0) = true
          · exact hc
          · have hc' : (y == 0) = false := by cases hh : (y == 0) <;> simp_all
            exact absurd (mem_takeWhile_imp hy) (by simp [hc']))
        rw [hall] at hxr
        exact absurd (mem_takeWhile_imp hxr) (by simp [hfx'])
      simp [hne]
  · simpa using bridgeRun a b

theorem sound_LongestHole_Stitch :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.Stitch.Goal :=
  stitch_agg

theorem sound_LongestHole_TreeReduce :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.TreeReduce.Goal :=
  treeReduce_of_stitch _ stitch_agg

theorem sound_LongestHole_RepeatPower :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.RepeatPower.Goal :=
  repeatPower_of_stitch _ stitch_agg

theorem sound_LongestHole_FrameSlice :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.FrameSlice.Goal :=
  frameSlice_of_stitch _ stitch_agg

theorem sound_LongestHole_PrefixCheckpoint :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

theorem sound_LongestHole_BridgeRun :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.BridgeRun.Goal :=
  bridgeRun

theorem sound_LongestHole_AmendLeaf :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.AmendLeaf.Goal :=
  amendLeaf_of_stitch _ stitch_agg

theorem sound_LongestHole_RunInput :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.RunInput.Goal :=
  runInput_of_stitch _ stitch_agg

theorem sound_LongestHole_ReorderNeighbors :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch _ stitch_agg

theorem sound_LongestHole_ZeroFill :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole.ZeroFill.Goal := by
  intro n
  simp only [agg]
  have hall : ∀ x ∈ List.replicate n 0, (x == 0) = true := by
    intro x hx; rw [List.mem_replicate] at hx; simp [hx.2]
  have htw : (List.replicate n 0).takeWhile (· == 0) = List.replicate n 0 := takeWhile_all_true _ hall
  have hrev : (List.replicate n 0).reverse = List.replicate n 0 := by simp
  have htzrev : (List.replicate n 0).reverse.takeWhile (· == 0) = List.replicate n 0 := by rw [hrev]; exact htw
  have hlz : longestZero (List.replicate n 0) = n := by
    unfold longestZero peak
    apply Nat.le_antisymm
    · apply max_getD_le.mpr
      intro x hx
      simp only [List.mem_map, List.mem_range] at hx
      rcases hx with ⟨i, hi, rfl⟩
      have hle : (((List.replicate n 0).drop i).takeWhile (· == 0)).length ≤ n := by
        calc (((List.replicate n 0).drop i).takeWhile (· == 0)).length
            ≤ ((List.replicate n 0).drop i).length := length_takeWhile_le ..
          _ ≤ n := by simp [List.length_drop]
      exact le_trans hle le_rfl
    · have hmem : n ∈ (List.range (n + 1)).map (fun i => (((List.replicate n 0).drop i).takeWhile (· == 0)).length) := by
        exact List.mem_map.mpr ⟨0, by simp [List.mem_range], by simp [htw]⟩
      exact le_max_getD_of_mem hmem
  simp [htw, htzrev, hlz]

#guard agg.reference [0, 0, 3, 0] == (4, 2, 1, 2)

end LongestHole

/-! ## PairSubsequence -/

namespace PairSubsequence

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

private def cross (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => if v == 1 then (xs.drop (i + 1)).count 2 else 0)).sum

private theorem zipIdx_shift (xs : List Nat) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem cross_nil : cross [] = 0 := by simp [cross]

private theorem cross_cons (x : Nat) (xs : List Nat) :
    cross (x :: xs) = (if x == 1 then xs.count 2 else 0) + cross xs := by
  unfold cross
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, List.drop_zero, Nat.zero_add]
  rw [show xs.zipIdx 1 = (xs.zipIdx).map (fun (v, i) => (v, i + 1)) by
    simpa using zipIdx_shift xs 0 1]
  simp [List.map_map, Function.comp_def]

private theorem cross_append (a b : List Nat) :
    cross (a ++ b) = cross a + cross b + a.count 1 * b.count 2 := by
  induction a with
  | nil => simp [cross_nil]
  | cons x xs ih =>
      simp only [List.cons_append, cross_cons, List.count_append, ih, List.count_cons]
      by_cases hx : x == 1
      · simp [hx, Nat.mul_add, Nat.add_mul, Nat.add_assoc, Nat.add_left_comm,
          Nat.add_comm]
      · simp [hx, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.count 1 + b.count 1, a.count 2 + b.count 2,
    cross a + cross b + a.count 1 * b.count 2) =
    ((a ++ b).count 1, (a ++ b).count 2, cross (a ++ b))
  rw [List.count_append, List.count_append, cross_append]

theorem sound_PairSubsequence_Stitch :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.Stitch.Goal :=
  stitch_agg

theorem sound_PairSubsequence_TreeReduce :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.TreeReduce.Goal :=
  treeReduce_of_stitch _ stitch_agg

theorem sound_PairSubsequence_RepeatPower :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.RepeatPower.Goal :=
  repeatPower_of_stitch _ stitch_agg

theorem sound_PairSubsequence_FrameSlice :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.FrameSlice.Goal :=
  frameSlice_of_stitch _ stitch_agg

theorem sound_PairSubsequence_PrefixCheckpoint :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch _ stitch_agg

theorem sound_PairSubsequence_AmendLeaf :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.AmendLeaf.Goal :=
  amendLeaf_of_stitch _ stitch_agg

theorem sound_PairSubsequence_RunInput :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.RunInput.Goal :=
  runInput_of_stitch _ stitch_agg

theorem sound_PairSubsequence_ReorderNeighbors :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch _ stitch_agg

theorem sound_PairSubsequence_ReversalComplement :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.ReversalComplement.Goal := by
  intro xs
  change cross xs + cross xs.reverse = xs.count 1 * xs.count 2
  induction xs with
  | nil => simp [cross_nil]
  | cons x xs ih =>
      rw [cross_cons, List.reverse_cons, cross_append, cross_cons]
      have hcx : cross [x] = 0 := by simp [cross]
      simp only [cross_nil, List.count_nil, Nat.add_zero]
      simp only [List.count_reverse, List.count_cons, List.count_nil, Nat.add_zero,
        Nat.mul_zero]
      by_cases h1 : x == 1 <;> by_cases h2 : x == 2 <;>
        simp_all [Nat.add_mul, Nat.mul_add] <;> omega

private theorem cross_replicate_one (n : Nat) : cross (List.replicate n 1) = 0 := by
  induction n with
  | zero => simp [cross_nil]
  | succ n ih =>
      rw [List.replicate_succ, cross_cons]
      simp [List.count_replicate, ih]

private theorem cross_replicate_two (n : Nat) : cross (List.replicate n 2) = 0 := by
  induction n with
  | zero => simp [cross_nil]
  | succ n ih =>
      rw [List.replicate_succ, cross_cons]
      simp [ih]

theorem sound_PairSubsequence_BatchedMarkers :
    TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence.BatchedMarkers.Goal := by
  intro a b
  change ((List.replicate a 1 ++ List.replicate b 2).count 1,
    (List.replicate a 1 ++ List.replicate b 2).count 2,
    cross (List.replicate a 1 ++ List.replicate b 2)) = (a, b, a * b)
  rw [List.count_append, List.count_append, cross_append,
    cross_replicate_one, cross_replicate_two]
  simp [List.count_replicate]

end PairSubsequence

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFootersA

-- C++ references to this model or its proof.
-- executor/columnar_tlcs_fast_path_test.cpp
-- executor/columnar_tlcs_scan_executor.cpp
