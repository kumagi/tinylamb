import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Lcp.RangeMinimum` — `∀ (ws : List Word) i j, ws.Pairwise (· ≤ ·) → i < j → j <
  ws.length → ((lcpEdges ws).drop i |>.take (j-i)).min? = some (common (ws[i]?.getD
  []) (ws[j]?.getD []))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_RangeMinimum

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp

/-- The head of a lexicographically smaller nonempty cons is no larger. -/
private theorem head_le_of_cons_le {x y : Nat} {xs ys : Word}
    (h : x :: xs ≤ y :: ys) : x ≤ y := by
  by_cases hxy : x ≤ y
  · exact hxy
  · have hyx : y < x := by omega
    exact False.elim (h (List.Lex.rel hyx))

/-- Equal heads can be stripped from a lexicographic inequality. -/
private theorem tail_le_of_cons_le_eq {x : Nat} {xs ys : Word}
    (h : x :: xs ≤ x :: ys) : xs ≤ ys := by
  intro hlt
  exact h (List.Lex.cons hlt)

/-- For three lexicographically ordered words, the LCP of the endpoints is the
    smaller of the two consecutive LCPs. -/
private theorem common_middle (a b c : Word) (hab : a ≤ b) (hbc : b ≤ c) :
    common a c = min (common a b) (common b c) := by
  induction a generalizing b c with
  | nil => simp [common]
  | cons x xs ih =>
      cases b with
      | nil => exact False.elim (hab List.Lex.nil)
      | cons y ys =>
          cases c with
          | nil => exact False.elim (hbc List.Lex.nil)
          | cons z zs =>
              by_cases hxy : x = y
              · by_cases hyz : y = z
                · subst y
                  subst z
                  have hxs : xs ≤ ys := tail_le_of_cons_le_eq hab
                  have hys : ys ≤ zs := tail_le_of_cons_le_eq hbc
                  have h := ih ys zs hxs hys
                  simp [common, h]
                · subst x
                  simp [common, hyz]
              · have hxylt : x < y := Nat.lt_of_le_of_ne (head_le_of_cons_le hab) hxy
                have hy_le_z : y ≤ z := head_le_of_cons_le hbc
                have hxz : x < z := by omega
                have hxz_ne : ¬ x = z := by omega
                simp [common, hxy, hxz_ne]

/-- Dropping the first `i` words drops exactly the first `i` LCP edges. -/
private theorem lcpEdges_drop : ∀ (i : Nat) (ws : List Word), i ≤ ws.length →
    (lcpEdges ws).drop i = lcpEdges (ws.drop i)
  | 0, ws, _ => by simp
  | i + 1, [], h => by simp at h
  | i + 1, [w], h => by
      cases i with
      | zero => simp [lcpEdges]
      | succ i => simp at h
  | i + 1, w :: v :: rest, h => by
      change (lcpEdges (v :: rest)).drop i = lcpEdges (List.drop i (v :: rest))
      apply lcpEdges_drop i (v :: rest)
      simp at h ⊢
      omega

/-- Minimum of the LCP edges starting at the head of a sorted nonempty list. -/
private theorem prefix_min : ∀ (ws : List Word), ws.Pairwise (· ≤ ·) →
    ∀ j, 0 < j → j < ws.length →
      ((lcpEdges ws).take j).min? = some (common (ws.head?.getD []) (ws[j]?.getD []))
  | [], _, j, _, hjl => by simp at hjl
  | [w], _, j, hj, hjl => by simp at hjl; omega
  | w :: v :: rest, hp, j, hj, hjl => by
      have hp_tail : (v :: rest).Pairwise (· ≤ ·) := hp.tail
      have hwv : w ≤ v := (List.pairwise_cons.mp hp).1 v (by simp)
      cases j with
      | zero => omega
      | succ k =>
          cases k with
          | zero => simp [lcpEdges]
          | succ k =>
              have hklt : k < rest.length := by
                simpa using hjl
              have ih : ((lcpEdges (v :: rest)).take (k + 1)).min? =
                  some (common v (rest[k]?.getD [])) := by
                simpa using prefix_min (v :: rest) hp_tail (k + 1) (Nat.succ_pos _) (by simpa using hklt)
              have hxmem : (rest[k]?.getD []) ∈ rest := by
                rw [List.getElem?_eq_getElem hklt, Option.getD_some]
                exact List.getElem_mem hklt
              have hvx : v ≤ (rest[k]?.getD []) := (List.pairwise_cons.mp hp_tail).1 _ hxmem
              change (common w v :: (lcpEdges (v :: rest)).take (k + 1)).min? =
                some (common w (rest[k]?.getD []))
              rw [List.min?_cons, ih]
              simp only [Option.elim_some]
              rw [← common_middle w v (rest[k]?.getD []) hwv hvx]

/-- Brute-force check over sorted word lists of length ≤ 4 with symbols 0..2. -/
private def genWords (n : Nat) : List Word :=
  match n with
  | 0 => [[]]
  | k + 1 => List.flatMap (fun w => (List.range 3).map (fun x => w ++ [x])) (genWords k)

private def bruteRangeMinimum : Bool :=
  let words : List Word := (List.range 3).flatMap genWords
  let lists := [[]] ++ words.map (fun w => [w]) ++
    words.flatMap (fun w => words.map (fun v => [w, v])) ++
    words.flatMap (fun w => words.flatMap (fun v => words.map (fun u => [w, v, u]))) ++
    words.flatMap (fun w => words.flatMap (fun v => words.flatMap (fun u => words.map (fun t => [w, v, u, t]))))
  lists.foldl (fun acc ws => acc &&
    if ws.Pairwise (· ≤ ·) then
      (List.range ws.length).all (fun i =>
        (List.range ws.length).all (fun j =>
          i < j →
          ((lcpEdges ws).drop i |>.take (j - i)).min? ==
            some (common (ws[i]?.getD []) (ws[j]?.getD []))))
    else true) true

#guard bruteRangeMinimum

/-- Sorted dictionaries enjoy the LCP interval-minimum property. -/
theorem sound_Lcp_RangeMinimum : RangeMinimum.Goal := by
  intro ws i j hp hij hjl
  have hil : i < ws.length := by omega
  have hkpos : 0 < j - i := by omega
  have hklt : j - i < (ws.drop i).length := by
    simp [List.length_drop]
    omega
  have hm := prefix_min (ws.drop i) (List.Pairwise.drop hp) (j - i) hkpos hklt
  rw [lcpEdges_drop i ws (Nat.le_of_lt hil), hm]
  simp [List.head?_drop, List.getElem?_drop, Nat.add_sub_of_le (Nat.le_of_lt hij)]

end TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_RangeMinimum
