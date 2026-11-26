import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `LongestHole.BridgeRun` — `∀ (a b : List Nat), let s := (a.reverse.takeWhile (· ==
  0)).length let p := (b.takeWhile (· == 0)).length longestZero (a++b)=max (max
  (longestZero a) (longestZero b)) (s+p)`
- `LongestHole.RepeatPower` — Evaluate grammar repetition by binary powers of the
  summary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_RepeatPower

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.LongestHole

/-! ## Small-input truth checks -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n k : Nat) : Bool :=
  (gen n).all (fun xs =>
    repeatState agg (agg.reference xs) k == agg.reference (List.replicate k xs).flatten)

#eval probe 3 5
#eval probe 4 7

/-! ## Generic binary-power derivation from a stitching fold -/

private theorem repeatPower_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : repeatPower f := by
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
      have hjoin : f.join a a = f.reference ((List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten) := by
        rw [ha']; exact h _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append]
        congr 1
        rw [show k / 2 * 2 = k / 2 + k / 2 by omega, ← List.replicate_append_replicate]
      by_cases he : k % 2 = 0
      · have hk2 : k / 2 * 2 = k := by omega
        have hcond : (k % 2 == 0) = true := by simp [he]
        simp only [hcond, ite_true]
        change f.join a a = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, hk2]
      · have hodd : k % 2 = 1 := by omega
        have hk1 : k = k / 2 * 2 + 1 := by omega
        have hcond : (k % 2 == 0) = false := by simp [hodd]
        simp only [hcond]
        change f.join (f.join a a) (f.reference xs) = f.reference (List.replicate k xs).flatten
        rw [hjoin, hrep2, h]
        congr 1
        rw [hk1, show k / 2 * 2 + 1 = (k / 2 + k / 2) + 1 by omega, ← List.replicate_append_replicate,
            List.flatten_append, List.replicate_one, List.flatten_singleton]
        have heq : (k / 2 + k / 2 + 1) / 2 * 2 = k / 2 + k / 2 := by omega
        simp [heq]

/-! ## Elementary `takeWhile` facts -/

private theorem takeWhile_append_of_all {p : Nat → Bool} (a b : List Nat)
    (h : ∀ x ∈ a, p x = true) : (a ++ b).takeWhile p = a ++ b.takeWhile p := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = true := h x (by simp)
      have hrest : ∀ y ∈ xs, p y = true := fun y hy => h y (by simp [hy])
      simp only [List.cons_append, List.takeWhile_cons]
      simp [hx, ih hrest]

private theorem takeWhile_self_of_all {p : Nat → Bool} (a : List Nat)
    (h : ∀ x ∈ a, p x = true) : a.takeWhile p = a := by
  induction a with
  | nil => simp
  | cons x xs ih =>
      have hx : p x = true := h x (by simp)
      simp [hx, ih (fun y hy => h y (by simp [hy]))]

private theorem takeWhile_append_of_bad {p : Nat → Bool} (a b : List Nat)
    (h : ∃ x ∈ a, p x = false) : (a ++ b).takeWhile p = a.takeWhile p := by
  induction a with
  | nil => rcases h with ⟨x, hx, _⟩; simp at hx
  | cons x xs ih =>
      by_cases hx : p x = true
      · have h' : ∃ y ∈ xs, p y = false := by
          rcases h with ⟨y, hy, hpy⟩
          simp at hy
          rcases hy with rfl | hy
          · simp [hx] at hpy
          · exact ⟨y, hy, hpy⟩
        simp only [List.cons_append, List.takeWhile_cons]
        simp [hx, ih h']
      · have hx' : p x = false := by cases hp : p x <;> simp_all
        simp only [List.cons_append, List.takeWhile_cons]
        simp [hx']

private theorem mem_of_mem_takeWhile {p : Nat → Bool} {l : List Nat} {x : Nat}
    (h : x ∈ l.takeWhile p) : p x = true := by
  induction l generalizing x with
  | nil => simp at h
  | cons a as ih =>
      simp only [List.takeWhile_cons] at h
      by_cases hp : p a = true
      · simp [hp] at h
        rcases h with rfl | h
        · exact hp
        · exact ih h
      · simp [hp] at h

private theorem takeWhile_eq_take_length {p : Nat → Bool} (l : List Nat) :
    l.takeWhile p = l.take (l.takeWhile p).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hp : p a = true
      · simpa [List.takeWhile_cons, hp] using congrArg (fun t => a :: t) ih
      · simp [hp]

private theorem takeWhile_length_lt_of_bad {p : Nat → Bool} {l : List Nat} {x : Nat}
    (hx : x ∈ l) (hbad : p x = false) : (l.takeWhile p).length < l.length := by
  induction l with
  | nil => simp at hx
  | cons a as ih =>
      by_cases ha : p a = true
      · simp only [List.mem_cons] at hx
        rcases hx with rfl | hxas
        · simp [ha] at hbad
        · simpa [List.takeWhile_cons, ha] using Nat.succ_lt_succ (ih hxas)
      · simp [ha]

private theorem takeWhile_length_ge_of_all_take {p : Nat → Bool} {l : List Nat} {s : Nat}
    (hs : s ≤ l.length) (h : ∀ x ∈ l.take s, p x = true) :
    s ≤ (l.takeWhile p).length := by
  induction s generalizing l with
  | zero => simp
  | succ s ih =>
      cases l with
      | nil => simp at hs
      | cons a as =>
          have hla : s ≤ as.length := by simpa using hs
          have ha : p a = true := h a (by simp)
          have has : ∀ x ∈ as.take s, p x = true := by
            intro x hx
            exact h x (by simp [hx])
          simpa [List.takeWhile_cons, ha] using Nat.succ_le_succ (ih hla has)

private theorem exists_bad_of_not_all {p : Nat → Bool} {l : List Nat}
    (h : ¬ ∀ x ∈ l, p x = true) : ∃ x ∈ l, p x = false := by
  apply Classical.byContradiction
  intro hc
  apply h
  intro x hx
  by_cases hpx : p x = true
  · exact hpx
  · exfalso
    exact hc ⟨x, hx, by cases hp : p x <;> simp_all⟩

/-! ## Max of an optional list -/

private theorem getD_max_le {l : List Nat} {b : Nat} (h : ∀ x ∈ l, x ≤ b) : l.max?.getD 0 ≤ b := by
  cases hm : l.max? with
  | none => simp [Option.getD]
  | some m =>
      simp only [Option.getD]
      exact h m (List.max?_mem hm)

private theorem le_getD_max {l : List Nat} {x : Nat} (hx : x ∈ l) : x ≤ l.max?.getD 0 := by
  cases hm : l.max? with
  | none =>
      have : l = [] := List.max?_eq_none_iff.mp hm
      simp [this] at hx
  | some m =>
      simp only [Option.getD]
      exact (List.max?_le_iff hm).mp (Nat.le_refl m) x hx

private theorem run_le_longest (xs : List Nat) (i : Nat) (hi : i ≤ xs.length) :
    ((xs.drop i).takeWhile (· == 0)).length ≤ longestZero xs := by
  unfold longestZero peak
  apply le_getD_max
  exact List.mem_map.mpr ⟨i, by simp [List.mem_range]; omega, rfl⟩

private theorem longestZero_le_of_runs_le (xs : List Nat) (M : Nat)
    (h : ∀ i, i ≤ xs.length → ((xs.drop i).takeWhile (· == 0)).length ≤ M) :
    longestZero xs ≤ M := by
  unfold longestZero peak
  apply getD_max_le
  intro v hv
  simp only [List.mem_map, List.mem_range] at hv
  rcases hv with ⟨i, hi, rfl⟩
  exact h i (by omega)

/-! ## Cross-boundary zero run -/

private theorem bridgeRun : BridgeRun.Goal := by
  intro a b
  dsimp only
  apply Nat.le_antisymm
  · apply longestZero_le_of_runs_le
    intro i hi
    by_cases hia : i ≤ a.length
    · rw [List.drop_append, Nat.sub_eq_zero_of_le hia]
      simp
      by_cases hall : ∀ x ∈ a.drop i, (x == 0) = true
      · have htw : ((a.drop i ++ b).takeWhile (· == 0)).length =
            (a.length - i) + (b.takeWhile (· == 0)).length := by
          rw [takeWhile_append_of_all _ _ hall]
          simp [List.length_append, List.length_drop]
        have hs_bound : a.length - i ≤ (a.reverse.takeWhile (· == 0)).length := by
          have hrev : (a.drop i).reverse = a.reverse.take (a.length - i) := by rw [List.reverse_drop]
          apply takeWhile_length_ge_of_all_take
          · simp
          · intro x hx
            rw [← hrev] at hx
            exact hall x (List.mem_reverse.mp hx)
        rw [htw]
        exact Nat.le_trans (by omega) (Nat.le_max_right _ _)
      · obtain ⟨x, hx, hbadx⟩ := exists_bad_of_not_all hall
        have htw : ((a.drop i ++ b).takeWhile (· == 0)).length =
            ((a.drop i).takeWhile (· == 0)).length := by
          rw [takeWhile_append_of_bad _ _ ⟨x, hx, hbadx⟩]
        rw [htw]
        exact Nat.le_trans (run_le_longest a i hia)
          (Nat.le_max_left (longestZero a)
            (max (longestZero b)
              ((a.reverse.takeWhile (· == 0)).length +
                (b.takeWhile (· == 0)).length)))
    · have hdropnil : List.drop i a = [] := List.drop_eq_nil_of_le (by omega)
      rw [List.drop_append, hdropnil, List.nil_append]
      have hib : i - a.length ≤ b.length := by
        simp at hi
        omega
      exact Nat.le_trans (run_le_longest b (i - a.length) hib)
        (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
  · apply Nat.max_le.mpr
    constructor
    · apply Nat.max_le.mpr
      constructor
      · apply longestZero_le_of_runs_le
        intro i hi
        have hle := run_le_longest (a ++ b) i (by
          simp [List.length_append]
          omega)
        by_cases hall : ∀ x ∈ a.drop i, (x == 0) = true
        · have htw_a : ((a.drop i).takeWhile (· == 0)).length = a.length - i := by
            rw [takeWhile_self_of_all _ hall]
            simp [List.length_drop]
          have htw_ab : (((a ++ b).drop i).takeWhile (· == 0)).length =
              (a.length - i) + (b.takeWhile (· == 0)).length := by
            rw [List.drop_append, Nat.sub_eq_zero_of_le hi]
            simp
            rw [takeWhile_append_of_all _ _ hall]
            simp [List.length_append, List.length_drop]
          rw [htw_ab] at hle
          omega
        · obtain ⟨x, hx, hbadx⟩ := exists_bad_of_not_all hall
          have htw : (((a ++ b).drop i).takeWhile (· == 0)).length =
              ((a.drop i).takeWhile (· == 0)).length := by
            rw [List.drop_append, Nat.sub_eq_zero_of_le hi]
            simp
            rw [takeWhile_append_of_bad _ _ ⟨x, hx, hbadx⟩]
          rw [htw] at hle
          exact hle
      · apply longestZero_le_of_runs_le
        intro j hj
        have hle := run_le_longest (a ++ b) (a.length + j) (by
          simp [List.length_append]
          omega)
        have hdrop : (a ++ b).drop (a.length + j) = b.drop j := by simp [List.drop_append]
        rw [hdrop] at hle
        exact hle
    · let i := a.length - (a.reverse.takeWhile (· == 0)).length
      have hs_le : (a.reverse.takeWhile (· == 0)).length ≤ a.length := by
        have := List.Sublist.length_le (List.takeWhile_sublist (p := (· == 0)) (l := a.reverse))
        simpa using this
      have hi_le : i ≤ a.length := by dsimp [i]; omega
      have hrev : (a.drop i).reverse = a.reverse.take (a.length - i) := by rw [List.reverse_drop]
      have hall : ∀ x ∈ a.drop i, (x == 0) = true := by
        intro x hx
        have hxrev : x ∈ (a.drop i).reverse := List.mem_reverse.mpr hx
        rw [hrev] at hxrev
        have htake : a.reverse.take (a.length - i) = a.reverse.takeWhile (· == 0) := by
          rw [show a.length - i = (a.reverse.takeWhile (· == 0)).length by dsimp [i]; omega]
          exact (takeWhile_eq_take_length a.reverse).symm
        rw [htake] at hxrev
        exact mem_of_mem_takeWhile (p := fun x => x == 0) (l := a.reverse) (x := x) hxrev
      have htw : (((a ++ b).drop i).takeWhile (· == 0)).length =
          (a.length - i) + (b.takeWhile (· == 0)).length := by
        rw [List.drop_append, Nat.sub_eq_zero_of_le hi_le]
        simp
        rw [takeWhile_append_of_all _ _ hall]
        simp [List.length_append, List.length_drop]
      have hle := run_le_longest (a ++ b) i (by
        simp [List.length_append]
        omega)
      rw [htw] at hle
      dsimp [i] at hle ⊢
      omega

/-! ## The fold stitches -/

private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [agg, List.length_append]
  refine Prod.ext rfl (Prod.ext ?_ (Prod.ext ?_ ?_))
  · by_cases ha : ∀ x ∈ a, (x == 0) = true
    · rw [takeWhile_append_of_all _ _ ha, takeWhile_self_of_all _ ha]
      simp
    · obtain ⟨x, hx, hbadx⟩ := exists_bad_of_not_all ha
      rw [takeWhile_append_of_bad _ _ ⟨x, hx, hbadx⟩]
      have hne : (a.takeWhile (· == 0)).length ≠ a.length := by
        intro heq
        have hlt := takeWhile_length_lt_of_bad (p := fun x => x == 0) (l := a) hx hbadx
        omega
      simp [hne]
  · by_cases hb : ∀ x ∈ b, (x == 0) = true
    · have hb_rev : ∀ x ∈ b.reverse, (x == 0) = true := fun x hx => hb x (List.mem_reverse.mp hx)
      rw [List.reverse_append, takeWhile_append_of_all _ _ hb_rev, takeWhile_self_of_all _ hb_rev]
      simp
    · obtain ⟨x, hx, hbadx⟩ := exists_bad_of_not_all hb
      have hxr : x ∈ b.reverse := List.mem_reverse.mpr hx
      rw [List.reverse_append, takeWhile_append_of_bad _ _ ⟨x, hxr, hbadx⟩]
      have hne : (b.reverse.takeWhile (· == 0)).length ≠ b.length := by
        intro heq
        have hlt := takeWhile_length_lt_of_bad (p := fun x => x == 0) (l := b.reverse) hxr hbadx
        have hlt' : (b.reverse.takeWhile (· == 0)).length < b.length := by simpa using hlt
        omega
      simp [hne]
  · dsimp
    exact (bridgeRun a b).symm

/-! ## Goal -/

theorem sound_RepeatPower : LongestHole.RepeatPower.Goal :=
  repeatPower_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_LongestHole_RepeatPower
