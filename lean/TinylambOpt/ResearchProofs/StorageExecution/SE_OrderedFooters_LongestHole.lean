import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved (10 claims, including the following)

- `LongestHole.AmendLeaf` — Update one cached block using the left and right partial
  states.
- `LongestHole.BridgeRun` — `∀ (a b : List Nat), let s := (a.reverse.takeWhile (· ==
  0)).length let p := (b.takeWhile (· == 0)).length longestZero (a++b)=max (max
  (longestZero a) (longestZero b)) (s+p)`
- `LongestHole.FrameSlice` — Use only the two ragged boundaries and whole-page
  summaries for a window.
-/
set_option maxHeartbeats 0
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.LongestHole
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open SE_ORF

theorem ref_eq (xs : List Nat) :
    LongestHole.agg.reference xs = (xs.length, (List.length (List.takeWhile (fun x => x == 0) (xs))), (List.length (List.takeWhile (fun x => x == 0) (List.reverse (xs)))), longestZero xs) :=
  rfl

private theorem hj :
    LongestHole.agg.join = fun (n, p, s, a) (m, q, t, b) =>
      (n + m, if p == n then n + q else p, if t == m then m + s else t, max (max a b) (s + q)) :=
  rfl

private theorem drop_cons_succ (x : Nat) (l : List Nat) (n : Nat) :
    ((x :: l).drop (n + 1) : List Nat) = l.drop n := by
  rw [List.drop_cons (i := n + 1) (a := x) (l := l) (by omega), Nat.add_sub_cancel]

private theorem tw_len_le (xs : List Nat) : (List.length (List.takeWhile (fun x => x == 0) (xs))) ≤ xs.length := by
  induction xs with
  | nil => exact Nat.zero_le _
  | cons x r ih =>
      rw [List.takeWhile_cons, List.length_cons]
      split
      · rw [List.length_cons]
        exact Nat.succ_le_succ ih
      · rw [List.length_nil]
        exact Nat.zero_le _

private theorem tlen_le (xs : List Nat) : (List.length (List.takeWhile (fun x => x == 0) (List.reverse (xs)))) ≤ xs.length := by
  have h := tw_len_le xs.reverse
  rw [List.length_reverse] at h
  exact h

private theorem drop_app_le (b : List Nat) :
    ∀ (a : List Nat) i, i ≤ a.length → (a ++ b).drop i = a.drop i ++ b := by
  intro a
  induction a with
  | nil =>
      intro i hi
      have h0 : i = 0 := by simpa using hi
      subst h0
      rfl
  | cons x r ih =>
      intro i hi
      by_cases hi0 : i = 0
      · subst hi0
        rfl
      · have hex : ∃ j, i = j + 1 := ⟨i - 1, by omega⟩
        obtain ⟨j, rfl⟩ := hex
        rw [show ((x :: r) ++ b : List Nat) = x :: (r ++ b) from rfl]
        rw [drop_cons_succ]
        rw [show ((x :: r : List Nat)).drop (j + 1) = r.drop j from drop_cons_succ x r j]
        rw [ih j (by rw [List.length_cons] at hi; omega)]

private theorem drop_app_len (a b : List Nat) :
    ∀ j, (a ++ b).drop (a.length + j) = b.drop j := by
  induction a with
  | nil => intro j; simp
  | cons x r ih =>
      intro j
      have hlen : (x :: r).length = r.length + 1 := by rw [List.length_cons]
      rw [show (x :: r ++ b : List Nat) = x :: (r ++ b) from rfl, hlen,
        Nat.add_right_comm r.length 1 j, drop_cons_succ, ih j]

/-- The leading zero run of an append. -/
theorem tw_app (a b : List Nat) :
    (List.length (List.takeWhile (fun x => x == 0) (a ++ b))) = (List.length (List.takeWhile (fun x => x == 0) (a))) + if (List.length (List.takeWhile (fun x => x == 0) (a))) = a.length then (List.length (List.takeWhile (fun x => x == 0) (b))) else 0 := by
  induction a generalizing b with
  | nil =>
      rw [List.nil_append]
      rw [show (List.length (List.takeWhile (fun x => x == 0) ([] : List Nat))) = 0 from rfl, show ([] : List Nat).length = 0 from rfl]
      simp
  | cons x r ih =>
      rw [show (x :: r ++ b : List Nat) = x :: (r ++ b) from rfl]
      by_cases hx : x = 0
      · subst hx
        have hc : List.takeWhile (fun x => x == 0) (0 :: (r ++ b))
            = 0 :: List.takeWhile (fun x => x == 0) (r ++ b) := by
          rw [List.takeWhile_cons]
          rfl
        have hd : List.takeWhile (fun x => x == 0) (0 :: r)
            = 0 :: List.takeWhile (fun x => x == 0) r := by
          rw [List.takeWhile_cons]
          rfl
        rw [hc, hd, List.length_cons, List.length_cons, List.length_cons, ih]
        by_cases h : (List.length (List.takeWhile (fun x => x == 0) (r))) = r.length
        · rw [if_pos h]
          rw [if_pos (by omega : (List.length (List.takeWhile (fun x => x == 0) (r))) + 1 = r.length + 1)]
          omega
        · rw [if_neg h]
          rw [if_neg (by omega : ¬ ((List.length (List.takeWhile (fun x => x == 0) (r))) + 1 = r.length + 1))]
          try omega
      · have hc : List.takeWhile (fun x => x == 0) (x :: (r ++ b)) = [] := by
          rw [List.takeWhile_cons]
          simp [hx]
        have hd : List.takeWhile (fun x => x == 0) (x :: r) = [] := by
          rw [List.takeWhile_cons]
          simp [hx]
        rw [hc, hd, List.length_nil, List.length_cons]
        rw [if_neg (by omega : ¬ ((0 : Nat) = r.length + 1))]
        try rfl

/-- Snoc of a nonzero does not change the leading zero run. -/
private theorem tw_snoc_nonzero (u : List Nat) (x : Nat) (hx : ¬ (x == 0)) :
    List.takeWhile (fun x => x == 0) (u ++ [x]) = List.takeWhile (fun x => x == 0) u := by
  induction u with
  | nil =>
      have h : List.takeWhile (fun x => x == 0) (([] ++ [x] : List Nat)) = [] := by
        show List.takeWhile (fun x => x == 0) ([x] : List Nat) = []
        rw [List.takeWhile_cons]
        simp [hx]
      simpa using h
  | cons y u ih =>
      rw [show ((y :: u) ++ [x] : List Nat) = y :: (u ++ [x]) from rfl]
      have hc : List.takeWhile (fun x => x == 0) (y :: (u ++ [x]))
          = if (y == 0) = true then y :: List.takeWhile (fun x => x == 0) (u ++ [x]) else [] :=
        List.takeWhile_cons
      have hd : List.takeWhile (fun x => x == 0) (y :: u)
          = if (y == 0) = true then y :: List.takeWhile (fun x => x == 0) u else [] :=
        List.takeWhile_cons
      rw [hc, hd, ih]

/-- A list whose trailing zero run covers it entirely is all zeros. -/
private theorem full_of_sz_full (u : List Nat) :
    (List.length (List.takeWhile (fun x => x == 0) (List.reverse (u)))) = u.length → List.takeWhile (fun x => x == 0) u = u := by
  induction u with
  | nil => intro _; rfl
  | cons x r ih =>
      intro h
      rw [List.reverse_cons, List.length_cons] at h
      by_cases hx : x = 0
      · subst hx
        have htw := tw_app r.reverse [0]
        have h1 : (List.length (List.takeWhile (fun x => x == 0) ([0] : List Nat))) = 1 := rfl
        rw [htw] at h
        by_cases hc : (List.length (List.takeWhile (fun x => x == 0) (r.reverse))) = r.reverse.length
        · rw [if_pos hc, h1] at h
          have hpos : List.takeWhile (fun x => x == 0) (0 :: r)
              = 0 :: List.takeWhile (fun x => x == 0) r := by
            rw [List.takeWhile_cons]
            rfl
          rw [hpos, ih (hc.trans List.length_reverse)]
        · rw [if_neg hc] at h
          have hle := tw_len_le r.reverse
          rw [List.length_reverse] at hle
          omega
      · have hx' : ¬ (x == 0) := by simpa using hx
        have hle := tlen_le r
        rw [tw_snoc_nonzero r.reverse x hx'] at h
        omega

/-- A list whose leading zero run covers it entirely has a full trailing run too. -/
private theorem suff_of_full (u : List Nat) :
    (List.length (List.takeWhile (fun x => x == 0) (u))) = u.length → (List.length (List.takeWhile (fun x => x == 0) (List.reverse (u)))) = u.length := by
  induction u with
  | nil => intro _; rfl
  | cons x r ih =>
      intro h
      rw [List.reverse_cons]
      by_cases hx : x = 0
      · subst hx
        have hpos : List.takeWhile (fun x => x == 0) (0 :: r)
            = 0 :: List.takeWhile (fun x => x == 0) r := by
          rw [List.takeWhile_cons]
          rfl
        have hlen : (0 :: r : List Nat).length = r.length + 1 := by rw [List.length_cons]
        rw [hpos, List.length_cons, hlen] at h
        have hr : (List.length (List.takeWhile (fun x => x == 0) (r))) = r.length := by omega
        have hih := ih hr
        have h1 : (List.length (List.takeWhile (fun x => x == 0) ([0] : List Nat))) = 1 := rfl
        rw [hlen]
        rw [tw_app]
        rw [List.length_reverse]
        rw [if_pos hih, h1]
        omega
      · have hnil : List.takeWhile (fun x => x == 0) (x :: r) = [] := by
          rw [List.takeWhile_cons]
          simp [hx]
        rw [hnil, List.length_nil, List.length_cons] at h
        omega

/-- Dropping the trailing zero run leaves a list covered by its leading zero run. -/
private theorem drop_sz_full (a : List Nat) :
    List.takeWhile (fun x => x == 0) (a.drop (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a))))))
      = a.drop (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a))))) := by
  induction a with
  | nil => rfl
  | cons x r ih =>
      by_cases hx : x = 0
      · subst hx
        have hrev : ((0 :: r : List Nat).reverse) = r.reverse ++ [0] := List.reverse_cons
        by_cases hfull : (List.length (List.takeWhile (fun x => x == 0) (List.reverse (0 :: r)))) = (0 :: r).length
        · have h0 : (0 :: r).length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (0 :: r)))) = 0 := by omega
          rw [h0, List.drop_zero]
          exact full_of_sz_full (0 :: r) hfull
        · have hszx : (List.length (List.takeWhile (fun x => x == 0) (r.reverse ++ [0]))) = (List.length (List.takeWhile (fun x => x == 0) (r.reverse))) := by
            rw [tw_app]
            by_cases hc : (List.length (List.takeWhile (fun x => x == 0) (r.reverse))) = r.reverse.length
            · rw [if_pos hc]
              have h1 : (List.length (List.takeWhile (fun x => x == 0) ([0] : List Nat))) = 1 := rfl
              rw [h1]
              have hcontra : (List.length (List.takeWhile (fun x => x == 0) (List.reverse (0 :: r)))) = (0 :: r).length := by
                rw [hrev]
                rw [tw_app, if_pos hc, h1]
                rw [List.length_cons]
                have h2 := hc
                rw [List.length_reverse] at h2
                omega
              exact absurd hcontra hfull
            · rw [if_neg hc]
              omega
          have hlen : (0 :: r : List Nat).length = r.length + 1 := by rw [List.length_cons]
          have hdrop : (0 :: r : List Nat).drop ((0 :: r).length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (0 :: r)))))
              = r.drop (r.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (r))))) := by
            rw [hlen, hrev, hszx]
            rw [show r.length + 1 - (List.length (List.takeWhile (fun x => x == 0) (r.reverse))) = r.length - (List.length (List.takeWhile (fun x => x == 0) (r.reverse))) + 1
              from by have hlb := tlen_le r; omega]
            exact drop_cons_succ 0 r (r.length - (List.length (List.takeWhile (fun x => x == 0) (r.reverse))))
          rw [hdrop]
          exact ih
      · have hrev : ((x :: r : List Nat).reverse) = r.reverse ++ [x] := List.reverse_cons
        have hx' : ¬ (x == 0) := by simpa using hx
        have hszx : List.takeWhile (fun x => x == 0) (r.reverse ++ [x])
            = List.takeWhile (fun x => x == 0) r.reverse := tw_snoc_nonzero r.reverse x hx'
        have hdrop : (x :: r : List Nat).drop ((x :: r).length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (x :: r)))))
            = r.drop (r.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (r))))) := by
          rw [List.reverse_cons, hszx]
          rw [List.length_cons]
          rw [show r.length + 1 - (List.length (List.takeWhile (fun x => x == 0) (r.reverse))) = r.length - (List.length (List.takeWhile (fun x => x == 0) (r.reverse))) + 1
            from by have hlb := tlen_le r; omega]
          exact drop_cons_succ x r (r.length - (List.length (List.takeWhile (fun x => x == 0) (r.reverse))))
        rw [hdrop]
        exact ih

/-- The bridge identity: the longest zero run of an append. -/
theorem bridge (a b : List Nat) :
    longestZero (a ++ b) = max (max (longestZero a) (longestZero b)) ((List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))) + (List.length (List.takeWhile (fun x => x == 0) (b)))) := by
  have hlen : (a ++ b).length = a.length + b.length := List.length_append
  unfold longestZero
  apply Nat.le_antisymm
  · apply peak_le_of_le
    intro y hy
    obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hy
    simp only [List.mem_range] at hi
    show (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop i))) ≤ _
    by_cases hle : i ≤ a.length
    · rw [drop_app_le b a i hle, tw_app]
      by_cases hfull : (List.length (List.takeWhile (fun x => x == 0) (a.drop i))) = (a.drop i).length
      · rw [if_pos hfull]
        have hpzmon : (List.length (List.takeWhile (fun x => x == 0) ((a.drop i).reverse))) ≤ (List.length (List.takeWhile (fun x => x == 0) (a.reverse))) := by
          have hrevapp : (a.reverse : List Nat)
              = (a.drop i).reverse ++ (a.take i).reverse := by
            rw [← List.reverse_append, List.take_append_drop]
          rw [hrevapp, tw_app]
          exact Nat.le_add_right _ _
        have h1 : (List.length (List.takeWhile (fun x => x == 0) (a.drop i))) ≤ (List.length (List.takeWhile (fun x => x == 0) (a.reverse))) := by
          have h := suff_of_full (a.drop i) hfull
          omega
        exact Nat.le_trans (Nat.add_le_add_right h1 ((List.length (List.takeWhile (fun x => x == 0) (b))))) (Nat.le_max_right _ _)
      · rw [if_neg hfull, Nat.add_zero]
        have hmem : i ∈ List.range (a.length + 1) := by
          rw [List.mem_range]; omega
        have hle2 : (List.length (List.takeWhile (fun x => x == 0) (a.drop i)))
            ≤ TinylambOpt.ResearchGoals.Compression.Wire.peak ((List.range (a.length + 1)).map (fun k => (List.length (List.takeWhile (fun x => x == 0) (a.drop k))))) := by
          show (fun k => (List.length (List.takeWhile (fun x => x == 0) (a.drop k)))) i ≤ _
          exact le_peak_of_mem (List.mem_map_of_mem
            (f := fun k => (List.length (List.takeWhile (fun x => x == 0) (a.drop k)))) hmem)
        exact Nat.le_trans hle2
          (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
    · have hexj : ∃ j, i = a.length + j + 1 := ⟨i - a.length - 1, by omega⟩
      obtain ⟨j, hjl⟩ := hexj
      have hjb : j + 1 ≤ b.length := by
        rw [hlen] at hi
        rw [hjl] at hi
        omega
      have hdrop : ((a ++ b).drop i : List Nat) = b.drop (j + 1) := by
        rw [hjl]
        rw [show a.length + j + 1 = a.length + (j + 1) from Nat.add_assoc a.length j 1]
        exact drop_app_len a b (j + 1)
      rw [hdrop]
      have hmem : j + 1 ∈ List.range (b.length + 1) := by
        rw [List.mem_range]; omega
      have hle2 : (List.length (List.takeWhile (fun x => x == 0) (b.drop (j + 1))))
          ≤ TinylambOpt.ResearchGoals.Compression.Wire.peak ((List.range (b.length + 1)).map (fun k => (List.length (List.takeWhile (fun x => x == 0) (b.drop k))))) := by
        show (fun k => (List.length (List.takeWhile (fun x => x == 0) (b.drop k)))) (j + 1) ≤ _
        exact le_peak_of_mem (List.mem_map_of_mem
          (f := fun k => (List.length (List.takeWhile (fun x => x == 0) (b.drop k)))) hmem)
      exact Nat.le_trans hle2
        (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
  · refine Nat.max_le.mpr ⟨Nat.max_le.mpr ⟨?_, ?_⟩, ?_⟩
    · -- longestZero a ≤ longestZero (a ++ b)
      apply peak_le_of_le
      intro y hy
      obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hy
      simp only [List.mem_range] at hi
      have hmem : i ∈ List.range ((a ++ b).length + 1) := by
        rw [List.mem_range]
        rw [hlen]
        omega
      have hstep : (List.length (List.takeWhile (fun x => x == 0) (a.drop i))) ≤ (List.length (List.takeWhile (fun x => x == 0) (a.drop i ++ b))) := by
        rw [tw_app]
        omega
      have hle2 : (fun k => (List.length (List.takeWhile (fun x => x == 0) (a.drop k)))) i
          ≤ TinylambOpt.ResearchGoals.Compression.Wire.peak ((List.range ((a ++ b).length + 1)).map (fun k => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop k))))) := by
        refine Nat.le_trans hstep (le_peak_of_mem ?_)
        have hmem' : (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop i))) ∈ (List.range ((a ++ b).length + 1)).map (fun k => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop k)))) :=
          List.mem_map_of_mem (f := fun k => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop k)))) hmem
        rw [show ((a ++ b).drop i : List Nat) = a.drop i ++ b from
          drop_app_le b a i (by omega)] at hmem'
        exact hmem'
      exact hle2
    · -- longestZero b ≤ longestZero (a ++ b)
      apply peak_le_of_le
      intro y hy
      obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hy
      simp only [List.mem_range] at hi
      have hmem : a.length + i ∈ List.range ((a ++ b).length + 1) := by
        rw [List.mem_range]
        rw [hlen]
        omega
      have hle2 : (fun k => (List.length (List.takeWhile (fun x => x == 0) (b.drop k)))) i
          ≤ TinylambOpt.ResearchGoals.Compression.Wire.peak ((List.range ((a ++ b).length + 1)).map (fun k => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop k))))) := by
        have hmem' : (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop (a.length + i)))) ∈ (List.range ((a ++ b).length + 1)).map (fun k => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop k)))) :=
          List.mem_map_of_mem (f := fun k => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop k)))) hmem
        rw [show ((a ++ b).drop (a.length + i) : List Nat) = b.drop i from
          drop_app_len a b i] at hmem'
        exact le_peak_of_mem hmem'
      exact hle2
    · -- the bridging run
      have hle : a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))) ≤ a.length := Nat.sub_le _ _
      have hmem : a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))) ∈ List.range ((a ++ b).length + 1) := by
        rw [List.mem_range]
        rw [hlen]
        omega
      have hdrop : ((a ++ b).drop (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a))))) : List Nat)
          = a.drop (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a))))) ++ b := drop_app_le b a _ hle
      have hcond : (List.length (List.takeWhile (fun x => x == 0) (a.drop (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a))))))))
          = (a.drop (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))))).length := by
        rw [drop_sz_full]
      have hplen : (a.drop (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))))).length = (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))) := by
        rw [List.length_drop]
        have := tlen_le a
        omega
      have hkey : (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))))))) = (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))) + (List.length (List.takeWhile (fun x => x == 0) (b))) := by
        rw [hdrop, tw_app, if_pos hcond]
        rw [hcond, hplen]
      have hle3 : (fun k => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop k)))) (a.length - (List.length (List.takeWhile (fun x => x == 0) (List.reverse (a)))))
          ≤ TinylambOpt.ResearchGoals.Compression.Wire.peak ((List.range ((a ++ b).length + 1)).map (fun i => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop i))))) :=
        le_peak_of_mem (List.mem_map_of_mem
          (f := fun k => (List.length (List.takeWhile (fun x => x == 0) ((a ++ b).drop k)))) hmem)
      rw [← hkey]
      exact hle3

theorem stitch_agg : stitch LongestHole.agg := by
  intro a b
  rw [ref_eq, ref_eq, ref_eq, hj]
  dsimp only
  rw [List.reverse_append]
  apply Prod.ext
  · rw [List.length_append]
  · apply Prod.ext
    · -- leading run
      by_cases hfull : (List.length (List.takeWhile (fun x => x == 0) (a))) = a.length
      · have hb : ((List.length (List.takeWhile (fun x => x == 0) (a))) == a.length) = true := by simp [hfull]
        rw [hb]
        simp
        rw [tw_app, if_pos hfull]
        omega
      · have hb : ((List.length (List.takeWhile (fun x => x == 0) (a))) == a.length) = false := by simp [hfull]
        rw [hb]
        simp
        rw [tw_app, if_neg hfull]
        omega
    · apply Prod.ext
      · -- trailing run
        by_cases hfull : (List.length (List.takeWhile (fun x => x == 0) (b.reverse))) = b.reverse.length
        · have hb : ((List.length (List.takeWhile (fun x => x == 0) (b.reverse))) == b.length) = true := by
            simp [hfull, List.length_reverse]
          have hz : (List.length (List.takeWhile (fun x => x == 0) (b.reverse))) = b.length := hfull.trans List.length_reverse
          rw [hb]
          simp
          rw [tw_app, if_pos hfull]
          omega
        · have hlb : b.reverse.length = b.length := List.length_reverse
          have hb : ((List.length (List.takeWhile (fun x => x == 0) (b.reverse))) == b.length) = false := by
            rw [← hlb]
            simpa using hfull
          rw [hb]
          simp
          rw [tw_app, if_neg hfull]
          omega
      · -- longest run
        exact (bridge a b).symm

theorem sound_Stitch : LongestHole.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : LongestHole.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : LongestHole.RepeatPower.Goal := repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : LongestHole.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : LongestHole.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : LongestHole.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : LongestHole.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : LongestHole.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

theorem sound_BridgeRun : LongestHole.BridgeRun.Goal := by
  intro a b
  exact bridge a b

private theorem tw_replicate_zero : ∀ n : Nat,
    List.takeWhile (fun x => x == 0) (List.replicate n 0) = List.replicate n 0 := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.replicate_succ]
      have hc : List.takeWhile (fun x => x == 0) (0 :: List.replicate n 0)
          = 0 :: List.takeWhile (fun x => x == 0) (List.replicate n 0) := by
        rw [List.takeWhile_cons]
        rfl
      rw [hc, ih]

theorem sound_ZeroFill : LongestHole.ZeroFill.Goal := by
  intro n
  rw [ref_eq]
  have hlen : (List.replicate n 0).length = n := List.length_replicate
  have hpz : (List.length (List.takeWhile (fun x => x == 0) (List.replicate n 0))) = n := by
    rw [tw_replicate_zero, hlen]
  have hsz : (List.length (List.takeWhile (fun x => x == 0) (List.reverse (List.replicate n 0)))) = n := by
    rw [List.reverse_replicate, tw_replicate_zero, hlen]
  have hlz : longestZero (List.replicate n 0) = n := by
    unfold longestZero
    rw [hlen]
    have hmap : (List.range (n + 1)).map (fun i => (List.length (List.takeWhile (fun x => x == 0) ((List.replicate n 0).drop i))))
        = (List.range (n + 1)).map (fun i => n - i) := by
      congr 1
      funext i
      have h1 : (List.replicate n 0).drop i = List.replicate (n - i) 0 :=
        List.drop_replicate
      rw [h1, tw_replicate_zero, List.length_replicate]
    rw [hmap]
    refine Nat.le_antisymm (peak_le_of_le ?_) ?_
    · intro y hy
      obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hy
      exact Nat.sub_le _ _
    · refine le_peak_of_mem ?_
      have : (fun i => n - i) 0 ∈ (List.range (n + 1)).map (fun i => n - i) :=
        List.mem_map_of_mem (f := fun i => n - i) (List.mem_range.mpr (by omega))
      simpa using this
  rw [hlen, hpz, hsz, hlz]

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.LongestHole
