import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_ShardBridge

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp

/-- `common a b` never exceeds the length of `b`. -/
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

private theorem drop_common (prev w : Word) :
    w.length - common prev w + common prev w = w.length := by
  exact Nat.sub_add_cancel (common_le_right prev w)

/-- Front-coding with an explicitly supplied previous word. -/
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

/-- For a nonempty list the `getD` default is irrelevant. -/
private theorem getLastD_same (x : Word) : ∀ (xs : List Word) (p q : Word),
    (x :: xs).getLast?.getD p = (x :: xs).getLast?.getD q := by
  intro xs
  induction xs with
  | nil => simp
  | cons y ys ih =>
      intro p q
      cases ys with
      | nil => simp
      | cons z zs =>
          simpa using ih p q

private theorem getLastD_cons (w : Word) (ws : List Word) (prev : Word) :
    (w :: ws).getLast?.getD prev = ws.getLast?.getD w := by
  cases ws with
  | nil => simp
  | cons x xs => exact getLastD_same x xs prev w

/-- Splitting the front-coded stream at a concatenation: the previous word of
    the first element of the right part is the last word of the left part. -/
private theorem fb_append (prev : Word) (a b : List Word) :
    fb prev (a ++ b) = fb prev a + fb (a.getLast?.getD prev) b := by
  induction a generalizing prev with
  | nil => simp [fb]
  | cons w ws ih =>
      simp [fb, ih, getLastD_cons]
      omega

/-- Adding the shared prefix to the front-coded length of the first word
    recovers its full length, so the choice of previous word does not matter
    beyond that first prefix. -/
private theorem fb_head (p : Word) (b : List Word) :
    fb p b + common p (b.head?.getD []) = fb [] b := by
  cases b with
  | nil => simp [fb, common]
  | cons w ws =>
      simp [fb, common]
      have h := drop_common p w
      omega

/-- Brute-force check over word lists of length ≤ 3 with symbols 0..1. -/
private def genWords (n : Nat) : List Word :=
  match n with
  | 0 => [[]]
  | k + 1 => List.flatMap (fun w => (List.range 2).map (fun x => w ++ [x])) (genWords k)

private def bruteShardBridge : Bool :=
  let words : List Word := (List.range 3).flatMap genWords
  let lists := [[]] ++ words.map (fun w => [w]) ++
    words.flatMap (fun w => words.map (fun v => [w, v])) ++
    words.flatMap (fun w => words.flatMap (fun v => words.map (fun u => [w, v, u])))
  lists.foldl (fun acc a => acc &&
    lists.foldl (fun acc' b => acc' &&
      (frontBytes (a ++ b) + common (a.getLast?.getD []) (b.head?.getD []) ==
        frontBytes a + frontBytes b)) true) true

#guard bruteShardBridge

/-- Concatenating two dictionaries only changes the front-coding of the first
    word of the right shard: it is encoded against the last word of the left
    shard instead of the empty previous word. -/
theorem sound_Lcp_ShardBridge : ShardBridge.Goal := by
  intro a b
  simp only [frontBytes_eq_fb]
  rw [fb_append]
  have h := fb_head (a.getLast?.getD []) b
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_ShardBridge
