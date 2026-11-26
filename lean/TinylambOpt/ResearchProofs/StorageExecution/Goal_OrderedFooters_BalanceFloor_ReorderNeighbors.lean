import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BalanceFloor.ReorderNeighbors` — Two neighboring states may be scheduled in reverse
  if they commute on their reachable values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_ReorderNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs, 3 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun pre => ls.all (fun a => ls.all (fun b => ls.all (fun suf =>
    let hyp := agg.join (agg.reference a) (agg.reference b) == agg.join (agg.reference b) (agg.reference a)
    let concl := agg.reference (pre ++ a ++ b ++ suf) == agg.reference (pre ++ b ++ a ++ suf)
    !hyp || concl))))

#eval probe 2

private theorem scanl_min_some (start : Int) (l : List Int) :
    ∃ m, (l.scanl (· + ·) start).min? = some m := by
  cases l with
  | nil => exact ⟨start, by simp⟩
  | cons x xs => simp [List.scanl_cons]

private def mf (start : Int) : List Int → Int
  | [] => start
  | x :: xs => min start (mf (start + x) xs)

private theorem mf_eq (start : Int) (l : List Int) :
    mf start l = (l.scanl (· + ·) start).min?.getD start := by
  induction l generalizing start with
  | nil => simp [mf]
  | cons x xs ih =>
      obtain ⟨m, hm⟩ := scanl_min_some (start + x) xs
      have him : mf (start + x) xs = m := by
        rw [ih (start + x), hm]
        simp
      simp [mf, List.scanl_cons, hm, him]

private theorem mf_le_start (start : Int) (l : List Int) : mf start l ≤ start := by
  cases l with
  | nil => simp [mf]
  | cons x xs =>
      simp only [mf]
      omega

private theorem mf_append (start : Int) (l₁ l₂ : List Int) :
    mf start (l₁ ++ l₂) = min (mf start l₁) (mf (start + l₁.sum) l₂) := by
  induction l₁ generalizing start with
  | nil =>
      simp only [List.nil_append, List.sum_nil]
      rw [show start + 0 = start by omega]
      change mf start l₂ = min start (mf start l₂)
      have hle := mf_le_start start l₂
      omega
  | cons x xs ih =>
      simp only [List.cons_append, List.sum_cons, mf]
      rw [ih (start + x)]
      have hsum : start + x + xs.sum = start + (x + xs.sum) := by omega
      rw [hsum, Int.min_assoc]

private theorem mf_shift (start t : Int) (l : List Int) :
    mf (start + t) l = start + mf t l := by
  induction l generalizing start t with
  | nil => simp [mf]
  | cons x xs ih =>
      simp only [mf]
      rw [ih (start + t) x, ih t x]
      omega

private theorem mf_add (start : Int) (l : List Int) : mf start l = start + mf 0 l := by
  simpa using mf_shift start 0 l

private theorem mf_three (l₁ l₂ l₃ : List Int) (start : Int) :
    mf start (l₁ ++ l₂ ++ l₃) =
      min (min (mf start l₁) (start + l₁.sum + mf 0 l₂)) (mf (start + (l₁.sum + l₂.sum)) l₃) := by
  rw [mf_append start (l₁ ++ l₂) l₃]
  rw [mf_append start l₁ l₂]
  rw [mf_add (start + l₁.sum) l₂]
  rw [List.sum_append]

private def sts (xs : List Nat) : List Int :=
  xs.map (fun x : Nat => if x % 2 == 0 then ((x : Int) / 2) else -(((x : Int) / 2) + 1))

private theorem reference_eq (xs : List Nat) :
    agg.reference xs = ((sts xs).sum, mf 0 (sts xs)) := by
  simp [agg, sts, ← mf_eq]

private theorem mf_append_pair (a b : List Nat) :
    mf 0 (sts (a ++ b)) = min (mf 0 (sts a)) ((sts a).sum + mf 0 (sts b)) := by
  rw [show sts (a ++ b) = sts a ++ sts b by simp [sts]]
  rw [mf_append 0 (sts a) (sts b)]
  simp only [Int.zero_add]
  rw [mf_add (sts a).sum (sts b)]

theorem sound_ReorderNeighbors : BalanceFloor.ReorderNeighbors.Goal := by
  intro pre a b suf h
  have hsnd : min (mf 0 (sts a)) ((sts a).sum + mf 0 (sts b)) =
              min (mf 0 (sts b)) ((sts b).sum + mf 0 (sts a)) := by
    have hh := congrArg Prod.snd h
    rw [reference_eq a, reference_eq b] at hh
    simpa [agg] using hh
  have hab : mf 0 (sts (a ++ b)) = mf 0 (sts (b ++ a)) := by
    rw [mf_append_pair, mf_append_pair]
    exact hsnd
  rw [reference_eq (pre ++ a ++ b ++ suf), reference_eq (pre ++ b ++ a ++ suf)]
  apply Prod.ext
  · change (sts (pre ++ a ++ b ++ suf)).sum = (sts (pre ++ b ++ a ++ suf)).sum
    simp [sts]
    omega
  · change mf 0 (sts (pre ++ a ++ b ++ suf)) = mf 0 (sts (pre ++ b ++ a ++ suf))
    rw [List.append_assoc pre a b, List.append_assoc pre b a]
    rw [show sts ((pre ++ (a ++ b)) ++ suf) = sts pre ++ sts (a ++ b) ++ sts suf by simp [sts]]
    rw [show sts ((pre ++ (b ++ a)) ++ suf) = sts pre ++ sts (b ++ a) ++ sts suf by simp [sts]]
    rw [mf_three (sts pre) (sts (a ++ b)) (sts suf) 0]
    rw [mf_three (sts pre) (sts (b ++ a)) (sts suf) 0]
    simp only [Int.zero_add]
    have hsum : (sts (a ++ b)).sum = (sts (b ++ a)).sum := by
      simp [sts]
      omega
    rw [hab, hsum]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_ReorderNeighbors
