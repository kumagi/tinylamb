import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common
set_option maxHeartbeats 0

/-! Balance-floor fold: signed steps, running sum and minimum prefix sum.
All contracts reduce to `stitch` via `SE_ORF`; the minimum-prefix machinery
is handled by the auxiliary `minFrom`. -/
namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.BalanceFloor
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open SE_ORF

/-- Running minimum of prefix sums of an `Int` list, started at `s`. -/
def minFrom : Int → List Int → Int
  | s, [] => s
  | s, x :: xs => min s (minFrom (s + x) xs)

theorem scanl_min?_some (s : Int) (l : List Int) :
    ∃ m, (l.scanl (· + ·) s).min? = some m := by
  cases l with
  | nil => exact ⟨s, by simp⟩
  | cons x xs => simp [List.scanl_cons]

theorem minFrom_eq (s : Int) (l : List Int) :
    minFrom s l = (l.scanl (· + ·) s).min?.getD s := by
  induction l generalizing s with
  | nil => simp [minFrom]
  | cons x xs ih =>
    obtain ⟨m, hm⟩ := scanl_min?_some (s + x) xs
    have him : minFrom (s + x) xs = m := by
      rw [ih (s + x), hm]; simp
    simp [minFrom, List.scanl_cons, List.min?_cons, hm, him]

theorem minFrom_le (s : Int) (l : List Int) : minFrom s l ≤ s := by
  cases l with
  | nil => simp [minFrom]
  | cons x xs =>
    simp only [minFrom]
    omega

theorem minFrom_add (s : Int) (l : List Int) : minFrom s l = s + minFrom 0 l := by
  induction l generalizing s with
  | nil => simp [minFrom]
  | cons x xs ih =>
    simp only [minFrom]
    rw [ih (s + x), ih (0 + x)]
    omega

theorem minFrom_append (s : Int) (l r : List Int) :
    minFrom s (l ++ r) = min (minFrom s l) (minFrom (s + l.sum) r) := by
  induction l generalizing s with
  | nil =>
    simp only [List.nil_append, List.sum_nil, Int.add_zero, minFrom]
    have hle := minFrom_le s r
    omega
  | cons x xs ih =>
    simp only [List.cons_append, List.sum_cons, minFrom]
    rw [ih (s + x)]
    have hsum : s + x + xs.sum = s + (x + xs.sum) := by omega
    rw [hsum, Int.min_assoc]

theorem minFrom_le_iff (s : Int) (l : List Int) (m : Int) :
    minFrom s l ≤ m ↔ ∃ i ≤ l.length, s + (l.take i).sum ≤ m := by
  induction l generalizing s with
  | nil => simp [minFrom]
  | cons x xs ih =>
    constructor
    · intro h
      simp only [minFrom] at h
      by_cases hs : s ≤ m
      · refine ⟨0, by omega, ?_⟩
        simpa using hs
      · have hmin : minFrom (s + x) xs ≤ m := by omega
        obtain ⟨i, hi, hsum⟩ := (ih (s + x)).mp hmin
        refine ⟨i + 1, by simpa using Nat.succ_le_succ hi, ?_⟩
        have htake : (x :: xs).take (i + 1) = x :: xs.take i := by simp [List.take]
        rw [htake]
        simp
        omega
    · rintro ⟨i, hi, hsum⟩
      simp only [minFrom]
      cases i with
      | zero =>
        simp at hsum
        omega
      | succ j =>
        have hj : j ≤ xs.length := by simpa using hi
        have htake : (x :: xs).take (j + 1) = x :: xs.take j := by simp [List.take]
        rw [htake, List.sum_cons] at hsum
        have hsum' : s + x + (xs.take j).sum ≤ m := by omega
        have hmin := (ih (s + x)).mpr ⟨j, hj, hsum'⟩
        omega

/-- The signed step function used by `BalanceFloor.agg.reference`. -/
def steps (xs : List Nat) : List Int :=
  xs.map (fun x => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int))

theorem ref_eq (xs : List Nat) :
    BalanceFloor.agg.reference xs = ((steps xs).sum, minFrom 0 (steps xs)) := by
  simp [BalanceFloor.agg, steps, ← minFrom_eq]

theorem join_spec (s m t n : Int) :
    BalanceFloor.agg.join (s, m) (t, n) = (s + t, min m (s + n)) := rfl

theorem stitch_agg : stitch BalanceFloor.agg := by
  intro a b
  rw [ref_eq a, ref_eq b, ref_eq (a ++ b)]
  change ((steps a).sum + (steps b).sum,
      min (minFrom 0 (steps a)) ((steps a).sum + minFrom 0 (steps b))) =
    ((steps (a ++ b)).sum, minFrom 0 (steps (a ++ b)))
  rw [show steps (a ++ b) = steps a ++ steps b by simp [steps],
    List.sum_append, minFrom_append, minFrom_add (0 + (steps a).sum) (steps b)]
  apply Prod.ext <;> omega

theorem sound_Stitch : BalanceFloor.Stitch.Goal := stitch_agg

theorem sound_TreeReduce : BalanceFloor.TreeReduce.Goal :=
  treeReduce_of_stitch stitch_agg

theorem sound_RepeatPower : BalanceFloor.RepeatPower.Goal :=
  repeatPower_of_stitch stitch_agg

theorem sound_FrameSlice : BalanceFloor.FrameSlice.Goal :=
  frameSlice_of_stitch stitch_agg

theorem sound_PrefixCheckpoint : BalanceFloor.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg

theorem sound_AmendLeaf : BalanceFloor.AmendLeaf.Goal :=
  amendLeaf_of_stitch stitch_agg

theorem sound_RunInput : BalanceFloor.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)

theorem sound_ReorderNeighbors : BalanceFloor.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

theorem sound_ShardAdmission : BalanceFloor.ShardAdmission.Goal := by
  intro a b credit
  rw [ref_eq a, ref_eq b, ref_eq (a ++ b)]
  dsimp only [Prod.fst, Prod.snd]
  rw [show steps (a ++ b) = steps a ++ steps b by simp [steps],
    minFrom_append, minFrom_add (0 + (steps a).sum) (steps b)]
  omega

theorem sound_CreditLimit : BalanceFloor.CreditLimit.Goal := by
  intro xs credit
  have hmin : (BalanceFloor.agg.reference xs).2 = minFrom 0 (steps xs) := by
    rw [ref_eq]
  have hsum : ∀ i, (BalanceFloor.agg.reference (xs.take i)).1 = ((steps xs).take i).sum := by
    intro i
    have ht : steps (xs.take i) = (steps xs).take i := by
      simp [steps, List.map_take]
    rw [← ht, ref_eq]
  have hlen : (steps xs).length = xs.length := by simp [steps]
  constructor
  · intro h i hi
    rw [hmin] at h
    have hle : minFrom 0 (steps xs) ≤ ((steps xs).take i).sum :=
      (minFrom_le_iff 0 (steps xs) _).mpr ⟨i, by omega, by simp⟩
    rw [hsum i]
    omega
  · intro h
    by_cases ht : 0 ≤ credit + (BalanceFloor.agg.reference xs).2
    · exact ht
    · rw [hmin] at ht ⊢
      obtain ⟨i, hi, hsumi⟩ :=
        (minFrom_le_iff 0 (steps xs) (-credit - 1)).mp (by omega)
      have hok := h i (by omega)
      rw [hsum i] at hok
      omega

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.BalanceFloor
