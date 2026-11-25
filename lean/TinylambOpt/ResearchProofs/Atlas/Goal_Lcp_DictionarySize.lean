import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_DictionarySize

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp

/-- `common a b` is the length of a prefix of `b`, hence bounded by `b.length`. -/
private theorem common_le_right (a b : Word) : common a b ≤ b.length := by
  cases a with
  | nil => simp [common]
  | cons x xs =>
      cases b with
      | nil => simp [common]
      | cons y ys =>
          by_cases h : x == y
          · simp [common, h]
            have ih := common_le_right xs ys
            omega
          · simp [common, h]

/-- The front-coded length plus the dropped prefix length recovers the word length. -/
private theorem drop_common (prev w : Word) :
    w.length - common prev w + common prev w = w.length := by
  exact Nat.sub_add_cancel (common_le_right prev w)

/-- Front-coding bytes with an explicitly supplied previous word. -/
private def fb (prev : Word) : List Word → Nat
  | [] => 0
  | w :: ws => (w.drop (common prev w)).length + fb w ws

private theorem frontBytes_fb (prev : Word) (ws : List Word) :
    ((ws.zip (prev :: ws)).map (fun (w, p) => (w.drop (common p w)).length)).sum = fb prev ws := by
  induction ws generalizing prev with
  | nil => simp [fb]
  | cons w ws ih =>
      simpa [fb] using ih w

private theorem frontBytes_eq_fb (ws : List Word) : frontBytes ws = fb [] ws := by
  cases ws with
  | nil => simp [frontBytes, fb]
  | cons w ws =>
      simpa [frontBytes, fb] using frontBytes_fb w ws

/-- Each front-coded word contributes `length - common prev`, and the sum of
    those `common prev` values is exactly the adjacent-LCP total. -/
private theorem fb_main (prev : Word) (ws : List Word) :
    fb prev ws + (lcpEdges ws).sum + common prev (ws.head?.getD []) = (ws.map List.length).sum := by
  induction ws generalizing prev with
  | nil => simp [fb, lcpEdges, common]
  | cons w ws ih =>
      cases ws with
      | nil =>
          simp [fb, lcpEdges]
          exact drop_common prev w
      | cons r rs =>
          have h1 := drop_common prev w
          have h2 := ih w
          simp [fb, lcpEdges] at h1 h2 ⊢
          omega

/-- Brute-force check over all word lists of length ≤ 3 with symbols 0..3. -/
private def genWords (n : Nat) : List Word :=
  match n with
  | 0 => [[]]
  | k + 1 => List.flatMap (fun w => (List.range 4).map (fun x => w ++ [x])) (genWords k)

private def bruteDictionarySize : Bool :=
  let words : List Word := (List.range 4).flatMap genWords
  let lists := [[]] ++ words.map (fun w => [w]) ++
    words.flatMap (fun w => words.map (fun v => [w, v])) ++
    words.flatMap (fun w => words.flatMap (fun v => words.map (fun u => [w, v, u])))
  lists.foldl (fun acc ws => acc &&
    (frontBytes ws + (lcpEdges ws).sum == (ws.map List.length).sum)) true

#guard bruteDictionarySize

/-- `frontBytes` drops the shared prefix with the previous word, so summing it
    with the adjacent LCP edges recovers the total number of stored symbols. -/
theorem sound_Lcp_DictionarySize : DictionarySize.Goal := by
  intro ws
  rw [frontBytes_eq_fb]
  have h := fb_main [] ws
  simpa [common] using h

end TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_DictionarySize
