import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0

/-! ## Generic derivations from stitch -/

section Generic

variable {σ : Type} (f : Fold σ)

theorem stitch_nil_left (h : stitch f) (a : List Nat) :
    f.join (f.reference []) (f.reference a) = f.reference a := by
  simpa using h [] a

theorem stitch_nil_right (h : stitch f) (a : List Nat) :
    f.join (f.reference a) (f.reference []) = f.reference a := by
  simpa using h a []

theorem stitch_assoc (h : stitch f) (a b c : List Nat) :
    f.join (f.join (f.reference a) (f.reference b)) (f.reference c) =
      f.join (f.reference a) (f.join (f.reference b) (f.reference c)) := by
  rw [h a b, h (a ++ b) c, h b c, h a (b ++ c), List.append_assoc]

theorem combine_eq_reference_flatten (h : stitch f) (ps : List (List Nat)) :
    combine f ps = f.reference ps.flatten := by
  unfold combine
  -- generalize: foldl f.join (f.reference acc) (ps.map f.reference) = f.reference (acc ++ ps.flatten)
  have gen : ∀ (l : List (List Nat)) (acc : List Nat),
      List.foldl f.join (f.reference acc) (l.map f.reference) = f.reference (acc ++ l.flatten) := by
    intro l
    induction l with
    | nil => intro acc; simp
    | cons p ps ih =>
      intro acc
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p]
      rw [ih (acc ++ p)]
      simp [List.append_assoc]
  simpa using gen ps []

theorem treeReduce_of_stitch (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => simp [rootState, plain]
  | node a b iha ihb =>
    simp [rootState, plain, iha, ihb, h]

theorem repeatPower_of_stitch (h : stitch f) : repeatPower f := by
  intro xs n
  induction n using Nat.strongRecOn with
  | _ k ih =>
    unfold repeatState
    by_cases hk : k = 0
    · simp [hk]
    · simp only [hk, dite_false]
      have hdiv : k / 2 < k := Nat.div_lt_self (by omega) (by decide)
      have ih2 := ih (k / 2) hdiv
      set a := repeatState f (f.reference xs) (k / 2) with ha
      have ha' : a = f.reference (List.replicate (k / 2) xs).flatten := by simpa [ha] using ih2
      have hjoin : f.join a a = f.reference ((List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten) := by
        rw [ha']; exact h _ _
      have hrep2 : (List.replicate (k / 2) xs).flatten ++ (List.replicate (k / 2) xs).flatten =
          (List.replicate (k / 2 * 2) xs).flatten := by
        rw [← List.flatten_append, ← List.replicate_add]
        congr 1; omega
      by_cases he : k % 2 = 0
      · simp only [he, ite_true]
        rw [hjoin, hrep2]
        congr 1
        have : k / 2 * 2 = k := by omega
        simp [this]
      · have hodd : k % 2 = 1 := by omega
        simp only [hodd, show (1 == 0) = false by decide, ite_false]
        rw [hjoin, hrep2, h]
        congr 1
        have hk1 : k = k / 2 * 2 + 1 := by omega
        conv_rhs => rw [hk1]
        rw [List.replicate_add, List.flatten_append]
        simp [List.replicate_one]
