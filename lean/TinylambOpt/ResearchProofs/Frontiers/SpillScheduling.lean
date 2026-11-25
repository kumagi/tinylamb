import TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.SpillScheduling

open TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

/-! ### ExcessFirst -/

private theorem bestPeak_ge_width : ∀ t : Tree, width t ≤ bestPeak t
  | .leaf n => by simp [width, bestPeak]
  | .node n l r => by
      simp only [width, bestPeak]
      have hl := bestPeak_ge_width l
      have hr := bestPeak_ge_width r
      omega

theorem ExcessFirst_sound : TinylambOpt.ResearchGoals.Frontiers.SpillScheduling.ExcessFirst.Goal := by
  intro l r n h
  simp only [bestPeak]
  have hl := bestPeak_ge_width l
  have hr := bestPeak_ge_width r
  have hAB : max (bestPeak l) (width l + bestPeak r) ≤
      max (bestPeak r) (width r + bestPeak l) := by
    omega
  rw [Nat.min_eq_left hAB]

/-! ### TreePeak -/

private theorem width_of_order {a b : Tree} (h : Order a b) : width a = width b := by
  induction h with
  | leaf n => rfl
  | same hac hbd ihac ihbd => rfl
  | swap hac hbd ihac ihbd => rfl

private theorem bestPeak_le_of_order : ∀ {t s : Tree}, Order t s → bestPeak t ≤ peak s := by
  intro t s h
  induction h with
  | leaf n => simp [bestPeak, peak]
  | same hac hbd ihac ihbd =>
      have hc : bestPeak _ ≤ peak _ := ihac
      have hd : bestPeak _ ≤ peak _ := ihbd
      have hwa : width _ = width _ := width_of_order hac
      have hwb : width _ = width _ := width_of_order hbd
      simp only [bestPeak, peak] at hc hd ⊢
      omega
  | swap hac hbd ihac ihbd =>
      have hc : bestPeak _ ≤ peak _ := ihac
      have hd : bestPeak _ ≤ peak _ := ihbd
      have hwa : width _ = width _ := width_of_order hac
      have hwb : width _ = width _ := width_of_order hbd
      simp only [bestPeak, peak] at hc hd ⊢
      omega

private theorem exists_order_bestPeak : ∀ t : Tree, ∃ s, Order t s ∧ peak s = bestPeak t := by
  intro t
  induction t with
  | leaf n => exact ⟨.leaf n, Order.leaf n, by simp [bestPeak, peak]⟩
  | node n l r ihl ihr =>
      obtain ⟨sl, hsl, hpl⟩ := ihl
      obtain ⟨sr, hsr, hpr⟩ := ihr
      have hwl : width sl = width l := (width_of_order hsl).symm
      have hwr : width sr = width r := (width_of_order hsr).symm
      let A := max (bestPeak l) (width l + bestPeak r)
      let B := max (bestPeak r) (width r + bestPeak l)
      let W := width l + width r + n
      have hp1 : peak (.node n sl sr) = max W A := by
        dsimp [W, A]
        simp only [peak]
        rw [hpl, hpr, hwl, hwr]
        omega
      have hp2 : peak (.node n sr sl) = max W B := by
        dsimp [W, B]
        simp only [peak]
        rw [hpl, hpr, hwl, hwr]
        omega
      have hbp : bestPeak (.node n l r) = max W (min A B) := by
        rfl
      rw [hbp]
      by_cases h : A ≤ B
      · have hm : min A B = A := Nat.min_eq_left h
        rw [hm, ← hp1]
        exact ⟨.node n sl sr, Order.same hsl hsr, rfl⟩
      · have h' : B ≤ A := by omega
        have hm : min A B = B := Nat.min_eq_right h'
        rw [hm, ← hp2]
        exact ⟨.node n sr sl, Order.swap hsl hsr, rfl⟩

theorem TreePeak_sound : TinylambOpt.ResearchGoals.Frontiers.SpillScheduling.TreePeak.Goal := by
  intro t
  exact ⟨exists_order_bestPeak t, fun s hs => bestPeak_le_of_order hs⟩

/-! ### AdjacentRuns (false) -/

private theorem leaves_length_pos : ∀ t : Runs, 0 < (leaves t).length
  | .run n => by simp [leaves]
  | .merge a b => by
      simp [leaves]
      have ha := leaves_length_pos a
      have hb := leaves_length_pos b
      omega

private theorem runs_of_leaves_len1 : ∀ t : Runs, (leaves t).length = 1 → ∃ n, t = .run n
  | .run n, _ => ⟨n, rfl⟩
  | .merge a b, h => by
      have ha := leaves_length_pos a
      have hb := leaves_length_pos b
      simp [leaves] at h
      omega

private theorem writes_sum_of_leaves_len2 (t : Runs) (x y : Nat) (h : leaves t = [x, y]) :
    writes t = x + y := by
  cases t with
  | run m => simp [leaves] at h
  | merge a b =>
      have hlen := congrArg List.length h
      simp [leaves] at hlen
      have ha := leaves_length_pos a
      have hb := leaves_length_pos b
      have hla : (leaves a).length = 1 := by omega
      have hlb : (leaves b).length = 1 := by omega
      obtain ⟨na, rfl⟩ := runs_of_leaves_len1 a hla
      obtain ⟨nb, rfl⟩ := runs_of_leaves_len1 b hlb
      simp [leaves] at h
      rcases h with ⟨hx, hy⟩
      subst x; subst y
      simp [writes, leaves]

private theorem leaves_a_of_append_singleton (xs : List Nat) (n x y z : Nat)
    (h : xs ++ [n] = [x, y, z]) (hlen : xs.length = 2) : xs = [x, y] ∧ n = z := by
  cases xs with
  | nil => simp at hlen
  | cons a xs' =>
      cases xs' with
      | nil => simp at hlen
      | cons b xs'' =>
          cases xs'' with
          | nil =>
              simp at h
              exact ⟨by simp [h.1, h.2.1], h.2.2⟩
          | cons c xs''' =>
              simp at hlen

private theorem writes_of_leaves_1_10_1 : ∀ t : Runs, leaves t = [1,10,1] → writes t = 23 := by
  intro t
  cases t with
  | run n => simp [leaves]
  | merge a b =>
      intro h
      have hlen := congrArg List.length h
      simp [leaves] at hlen
      have ha := leaves_length_pos a
      have hb := leaves_length_pos b
      have hleaves : leaves a ++ leaves b = [1,10,1] := by simpa [leaves] using h
      have hcases : ((leaves a).length = 1 ∧ (leaves b).length = 2) ∨
          ((leaves a).length = 2 ∧ (leaves b).length = 1) := by omega
      rcases hcases with ⟨hla, hlb⟩ | ⟨hla, hlb⟩
      · obtain ⟨na, rfl⟩ := runs_of_leaves_len1 a hla
        have hpair : na = 1 ∧ leaves b = [10,1] := by simpa [leaves] using hleaves
        rcases hpair with ⟨rfl, hb'⟩
        have hw := writes_sum_of_leaves_len2 b 10 1 hb'
        simp [hb', leaves, writes, hw]
      · obtain ⟨nb, rfl⟩ := runs_of_leaves_len1 b hlb
        have hleaves' : leaves a ++ [nb] = [1,10,1] := by simpa [leaves] using hleaves
        have hpair := leaves_a_of_append_singleton (leaves a) nb 1 10 1 hleaves' hla
        rcases hpair with ⟨ha', rfl⟩
        have hw := writes_sum_of_leaves_len2 a 1 10 ha'
        simp [ha', leaves, writes, hw]

theorem AdjacentRuns_disproof : ¬ TinylambOpt.ResearchGoals.Frontiers.SpillScheduling.AdjacentRuns.Goal := by
  intro h
  obtain ⟨t, ht, hw⟩ := h [1,10,1] (by decide)
  have h23 := writes_of_leaves_1_10_1 t ht
  rw [h23] at hw
  have hh : huffman [1,10,1].length [1,10,1] = 14 := by native_decide
  rw [hh] at hw
  omega

/-! ### AdjacentPenalty -/

private theorem le_bool_trans (a b c : Nat) (h1 : decide (a ≤ b) = true) (h2 : decide (b ≤ c) = true) :
    decide (a ≤ c) = true := by
  apply decide_eq_true
  exact Nat.le_trans (of_decide_eq_true h1) (of_decide_eq_true h2)

private theorem le_bool_total (a b : Nat) : (decide (a ≤ b) || decide (b ≤ a)) = true := by
  by_cases h : a ≤ b
  · simp [h]
  · have hba : b ≤ a := by omega
    simp [h, hba]

private theorem mergeSort_perm_eq {l1 l2 : List Nat} (h : List.Perm l1 l2) :
    l1.mergeSort = l2.mergeSort := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Nat => decide (a ≤ b) = true)
  · intro a b ha hb hab hba
    exact Nat.le_antisymm (of_decide_eq_true hab) (of_decide_eq_true hba)
  · exact List.pairwise_mergeSort le_bool_trans le_bool_total l1
  · exact List.pairwise_mergeSort le_bool_trans le_bool_total l2
  · exact (List.mergeSort_perm l1 _).trans (h.trans (List.mergeSort_perm l2 _).symm)

private theorem huffman_perm (k : Nat) {l1 l2 : List Nat} (h : List.Perm l1 l2) :
    huffman k l1 = huffman k l2 := by
  cases k with
  | zero => rfl
  | succ k =>
      unfold huffman
      rw [mergeSort_perm_eq h]

private theorem huffman_two (x y : Nat) : huffman 2 [x,y] = x + y := by
  by_cases h : x ≤ y <;>
    simp [huffman, h, List.mergeSort_pair, List.mergeSort_singleton] <;> omega

private theorem huffman_three_of_sorted (x y z : Nat) (hxy : x ≤ y) (hyz : y ≤ z) :
    huffman 3 [x,y,z] = 2*(x+y+z) - z := by
  unfold huffman
  have hsorted : [x,y,z].Pairwise (fun a b : Nat => decide (a ≤ b) = true) := by
    simp [hxy, hyz, Nat.le_trans hxy hyz]
  have hm : [x,y,z].mergeSort = [x,y,z] := List.mergeSort_of_pairwise hsorted
  rw [hm]
  simp only
  rw [huffman_two]
  omega

private theorem permSwapHead (a b : Nat) (l : List Nat) : List.Perm (a::b::l) (b::a::l) :=
  List.Perm.swap' b a (List.Perm.refl l)

private theorem perm_acb (a b c : Nat) : List.Perm [a,b,c] [a,c,b] :=
  List.Perm.cons a (permSwapHead b c [])

private theorem perm_cab (a b c : Nat) : List.Perm [a,b,c] [c,a,b] :=
  (perm_acb a b c).trans (permSwapHead a c [b])

private theorem perm_bac (a b c : Nat) : List.Perm [a,b,c] [b,a,c] :=
  permSwapHead a b [c]

private theorem perm_bca (a b c : Nat) : List.Perm [a,b,c] [b,c,a] :=
  (perm_bac a b c).trans (List.Perm.cons b (permSwapHead a c []))

private theorem perm_cba (a b c : Nat) : List.Perm [a,b,c] [c,b,a] :=
  (perm_cab a b c).trans (List.Perm.cons c (permSwapHead a b []))

private theorem huffman_three_formula (a b c : Nat) :
    huffman 3 [a,b,c] = 2*(a+b+c) - max a (max b c) := by
  by_cases hab : a ≤ b
  · by_cases hbc : b ≤ c
    · rw [huffman_three_of_sorted a b c hab hbc]
      omega
    · have hcb : c ≤ b := by omega
      by_cases hac : a ≤ c
      · rw [huffman_perm 3 (perm_acb a b c)]
        rw [huffman_three_of_sorted a c b hac hcb]
        omega
      · have hca : c ≤ a := by omega
        rw [huffman_perm 3 (perm_cab a b c)]
        rw [huffman_three_of_sorted c a b hca hab]
        omega
  · have hba : b ≤ a := by omega
    by_cases hac : a ≤ c
    · rw [huffman_perm 3 (perm_bac a b c)]
      rw [huffman_three_of_sorted b a c hba hac]
      omega
    · have hca : c ≤ a := by omega
      by_cases hbc : b ≤ c
      · rw [huffman_perm 3 (perm_bca a b c)]
        rw [huffman_three_of_sorted b c a hbc hca]
        omega
      · have hcb : c ≤ b := by omega
        rw [huffman_perm 3 (perm_cba a b c)]
        rw [huffman_three_of_sorted c b a hcb hba]
        omega

theorem AdjacentPenalty_sound : TinylambOpt.ResearchGoals.Frontiers.SpillScheduling.AdjacentPenalty.Goal := by
  intro a b c
  unfold adjacentThree
  rw [huffman_three_formula]
  by_cases hac : a ≤ c
  · have hmin : min (a + b + (a + b + c)) (b + c + (a + b + c)) = a + b + (a + b + c) := by omega
    rw [hmin]
    omega
  · have hca : c ≤ a := by omega
    have hmin : min (a + b + (a + b + c)) (b + c + (a + b + c)) = b + c + (a + b + c) := by omega
    rw [hmin]
    omega

/-! ### TileImbalance -/

private theorem pairwise_sort_prop (l : List Nat) : l.mergeSort.Pairwise (fun a b : Nat => a ≤ b) := by
  apply (List.pairwise_mergeSort le_bool_trans le_bool_total l).imp
  intro a b h
  exact of_decide_eq_true h

private theorem max?_getD_le (l : List Nat) (a : Nat) (h : ∀ b ∈ l, b ≤ a) : l.max?.getD 0 ≤ a := by
  cases l with
  | nil => simp
  | cons x xs =>
      have hs : (x::xs).max? ≠ none := by
        intro hnone
        have : (x::xs) = [] := List.max?_eq_none_iff.mp hnone
        simp at this
      rcases hmx : (x::xs).max? with _ | M
      · simp [hmx] at hs
      · exact (List.max?_le_iff hmx).2 h

private theorem le_min?_getD (l : List Nat) (a : Nat) (hne : l ≠ []) (h : ∀ b ∈ l, a ≤ b) :
    a ≤ l.min?.getD 0 := by
  cases l with
  | nil => exact (hne rfl).elim
  | cons x xs =>
      have hs : (x::xs).min? ≠ none := by
        intro hnone
        have : (x::xs) = [] := List.min?_eq_none_iff.mp hnone
        simp at this
      rcases hmn : (x::xs).min? with _ | M
      · simp [hmn] at hs
      · exact (List.le_min?_iff hmn).2 h

private theorem min?_getD_le_mem (l : List Nat) (x : Nat) (hx : x ∈ l) : l.min?.getD 0 ≤ x := by
  cases l with
  | nil => simp at hx
  | cons y ys =>
      have hs : (y::ys).min? ≠ none := by
        intro hnone
        have : (y::ys) = [] := List.min?_eq_none_iff.mp hnone
        simp at this
      rcases hmn : (y::ys).min? with _ | M
      · simp [hmn] at hs
      · exact ((List.le_min?_iff hmn).1 (Nat.le_refl _)) x hx

private theorem mem_le_max?_getD (l : List Nat) (x : Nat) (hx : x ∈ l) : x ≤ l.max?.getD 0 := by
  cases l with
  | nil => simp at hx
  | cons y ys =>
      have hs : (y::ys).max? ≠ none := by
        intro hnone
        have : (y::ys) = [] := List.max?_eq_none_iff.mp hnone
        simp at this
      rcases hmx : (y::ys).max? with _ | M
      · simp [hmx] at hs
      · exact ((List.max?_le_iff hmx).1 (Nat.le_refl _)) x hx

private theorem head_le_of_pairwise {a : Nat} {s : List Nat}
    (h : (a::s).Pairwise (fun x y : Nat => x ≤ y)) : ∀ b ∈ a::s, a ≤ b := by
  intro b hb
  simp only [List.mem_cons] at hb
  rcases hb with rfl | hb
  · exact Nat.le_refl _
  · exact List.rel_of_pairwise_cons h hb

private theorem head_mergeSort_eq_min (l : List Nat) (hne : l ≠ []) :
    (l.mergeSort).head?.getD 0 = l.min?.getD 0 := by
  have hsorted := pairwise_sort_prop l
  have hlen : l.mergeSort ≠ [] := by
    intro h
    have hp := List.mergeSort_perm l (fun a b : Nat => decide (a ≤ b))
    have hle := hp.length_eq
    simp [h] at hle
    apply hne
    exact List.length_eq_zero_iff.mp hle.symm
  rcases hm : l.mergeSort with _ | ⟨x, rest⟩
  · exact (hlen hm).elim
  · have hxmem : x ∈ l.mergeSort := by simp [hm]
    have hxmem_l : x ∈ l := (List.mem_mergeSort).1 hxmem
    have hmin_le_x : l.min?.getD 0 ≤ x := min?_getD_le_mem l x hxmem_l
    have hx_le_min : x ≤ l.min?.getD 0 := by
      apply le_min?_getD l x hne
      intro b hb
      have hb_s : b ∈ l.mergeSort := (List.mem_mergeSort).2 hb
      rw [hm] at hsorted hb_s
      have hxle : ∀ y ∈ x::rest, x ≤ y := head_le_of_pairwise hsorted
      exact hxle b hb_s
    simp [hm]
    omega

private def dstep (loads : List Nat) (j : Nat) : List Nat :=
  match loads.mergeSort with | [] => [] | l::rest => (l+j)::rest

private theorem dispatch_eq_foldl (loads jobs : List Nat) : dispatch loads jobs = jobs.foldl dstep loads := rfl

private theorem dstep_length (loads : List Nat) (j : Nat) : (dstep loads j).length = loads.length := by
  unfold dstep
  split
  next h =>
    have hp := List.mergeSort_perm loads (fun a b : Nat => decide (a ≤ b))
    have hle := hp.length_eq
    simp [h] at hle
    omega
  next l rest hs =>
    have hp := List.mergeSort_perm loads (fun a b : Nat => decide (a ≤ b))
    have hle := hp.length_eq
    simp [hs] at hle
    simp
    omega

private theorem dstep_inv (loads : List Nat) (j cap : Nat) (hpos : 0 < loads.length)
    (hinv : makespan loads - loads.min?.getD 0 ≤ cap) (hj : j ≤ cap) :
    makespan (dstep loads j) - (dstep loads j).min?.getD 0 ≤ cap := by
  have hne : loads ≠ [] := by
    intro h; subst loads; simp at hpos
  have hsortedne : loads.mergeSort ≠ [] := by
    intro h
    have hp := List.mergeSort_perm loads (fun a b : Nat => decide (a ≤ b))
    have hle := hp.length_eq
    simp [h] at hle
    apply hne
    exact List.length_eq_zero_iff.mp hle.symm
  rcases hs : loads.mergeSort with _ | ⟨l, rest⟩
  · exact (hsortedne hs).elim
  · have hl : l = loads.min?.getD 0 := by
      have := head_mergeSort_eq_min loads hne
      simpa [hs] using this
    have hmax_le : makespan loads ≤ l + cap := by
      have hm : loads.max?.getD 0 ≤ l + cap := by
        apply max?_getD_le
        intro b hb
        have hble := mem_le_max?_getD loads b hb
        simp [makespan] at hinv
        omega
      simpa [makespan] using hm
    have hnewmax : makespan (dstep loads j) ≤ l + cap := by
      unfold makespan
      unfold dstep
      rw [hs]
      apply max?_getD_le
      intro b hb
      cases hb with
      | head => omega
      | tail _ hb =>
          have hb_s : b ∈ l :: rest := by exact List.mem_cons_of_mem _ hb
          have hb_ms : b ∈ loads.mergeSort := by simpa [hs] using hb_s
          have hb_l : b ∈ loads := (List.mem_mergeSort).1 hb_ms
          have hble := mem_le_max?_getD loads b hb_l
          simp [makespan] at hmax_le
          omega
    have hnewmin : l ≤ (dstep loads j).min?.getD 0 := by
      unfold dstep
      rw [hs]
      apply le_min?_getD
      · simp
      · intro b hb
        cases hb with
        | head => omega
        | tail _ hb =>
            have hb_s : b ∈ l :: rest := by exact List.mem_cons_of_mem _ hb
            have hb_ms : b ∈ loads.mergeSort := by simpa [hs] using hb_s
            have hb_l : b ∈ loads := (List.mem_mergeSort).1 hb_ms
            have hminle := min?_getD_le_mem loads b hb_l
            omega
    omega

private theorem dispatch_inv (jobs loads : List Nat) (cap : Nat) (hpos : 0 < loads.length)
    (hinv : makespan loads - loads.min?.getD 0 ≤ cap) (hjob : ∀ j ∈ jobs, j ≤ cap) :
    makespan (dispatch loads jobs) - (dispatch loads jobs).min?.getD 0 ≤ cap := by
  induction jobs generalizing loads with
  | nil =>
      change makespan loads - loads.min?.getD 0 ≤ cap
      exact hinv
  | cons j js ih =>
      have hj : j ≤ cap := hjob j (by simp)
      have hs := dstep_inv loads j cap hpos hinv hj
      have hlen : 0 < (dstep loads j).length := by
        rw [dstep_length]
        exact hpos
      have hjob' : ∀ x ∈ js, x ≤ cap := by
        intro x hx
        exact hjob x (by simp [hx])
      exact ih (dstep loads j) hlen hs hjob'

theorem TileImbalance_sound : TinylambOpt.ResearchGoals.Frontiers.SpillScheduling.TileImbalance.Goal := by
  intro jobs m cap hm hjob
  dsimp only
  apply dispatch_inv jobs (List.replicate m 0) cap
  · simp [hm]
  · change makespan (List.replicate m 0) - (List.replicate m 0).min?.getD 0 ≤ cap
    simp [makespan, List.max?_replicate_of_pos hm, List.min?_replicate_of_pos hm]
  · exact hjob

end TinylambOpt.ResearchProofs.Frontiers.SpillScheduling
