import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_Rearrangement

open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Couplings

/-! ### Exhaustive sanity check on small inputs -/

private def rowsOf : Nat → List (Int × Int) → List (List (Int × Int))
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun r => (rowsOf n pool).map (r :: ·))

private def smallGrid : List Int := [-2, -1, 0, 1, 2]

private def smallPool : List (Int × Int) :=
  smallGrid.flatMap (fun a => smallGrid.map (fun b => (a, b)))

private def bruteCheck : Bool :=
  (rowsOf 3 smallPool).all fun rs =>
    let d := dotRows rs
    let lo := dotRows (counter rs)
    let hi := dotRows (monotone rs)
    lo ≤ d && d ≤ hi

#eval bruteCheck

/-! ### Dot product of two columns -/

private def dot (xs ys : List Int) : Int := (xs.zipWith (fun a b : Int => a * b) ys).sum

private theorem dotRows_eq_dot (rs : Rows) :
    dotRows rs = dot (rs.map Prod.fst) (rs.map Prod.snd) := by
  induction rs with
  | nil => rfl
  | cons p rs ih =>
      rcases p with ⟨x, y⟩
      change x * y + dotRows rs = x * y + dot (rs.map Prod.fst) (rs.map Prod.snd)
      rw [ih]

private theorem dotRows_zip (xs ys : List Int) : dotRows (xs.zip ys) = dot xs ys := by
  induction xs generalizing ys with
  | nil => simp [dotRows, dot]
  | cons x xs ih =>
      cases ys with
      | nil => simp [dotRows, dot]
      | cons y ys =>
          change x * y + dotRows (xs.zip ys) = x * y + dot xs ys
          rw [ih]

private theorem dotRows_perm_of_perm {rs rs' : Rows} (h : List.Perm rs rs') :
    dotRows rs = dotRows rs' := by
  induction h with
  | nil => rfl
  | cons a p ih =>
      unfold dotRows at ih ⊢
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
  | swap a b l =>
      unfold dotRows
      simp only [List.map_cons, List.sum_cons]
      rcases a with ⟨x1, y1⟩
      rcases b with ⟨x2, y2⟩
      grind
  | trans p1 p2 ih1 ih2 => exact Eq.trans ih1 ih2

private theorem dot_map_neg (xs ys : List Int) :
    dot (xs.map (fun x : Int => -x)) ys = - dot xs ys := by
  induction xs generalizing ys with
  | nil => simp [dot]
  | cons x xs ih =>
      cases ys with
      | nil => simp [dot]
      | cons y ys =>
          change -x * y + dot (xs.map fun x : Int => -x) ys = -(x * y + dot xs ys)
          rw [ih]
          rw [Int.neg_mul]
          grind

private theorem map_negpair_fst : ∀ rs : Rows,
    (rs.map (fun p : Int × Int => (-p.1, p.2))).map Prod.fst =
      (rs.map Prod.fst).map (fun x : Int => -x)
  | [] => rfl
  | (x, y) :: rs => by
      simp only [List.map_cons]
      rw [map_negpair_fst rs]

private theorem map_negpair_snd : ∀ rs : Rows,
    (rs.map (fun p : Int × Int => (-p.1, p.2))).map Prod.snd = rs.map Prod.snd
  | [] => rfl
  | (x, y) :: rs => by
      simp only [List.map_cons]
      rw [map_negpair_snd rs]

private theorem dotRows_map_neg_fst (rs : Rows) :
    dotRows (rs.map (fun p : Int × Int => (-p.1, p.2))) = - dotRows rs := by
  rw [dotRows_eq_dot, dotRows_eq_dot]
  rw [map_negpair_fst rs, map_negpair_snd rs, dot_map_neg]

private theorem dot_append_singleton (p q : List Int) (a b : Int)
    (hlen : p.length = q.length) :
    dot (p ++ [a]) (q ++ [b]) = dot p q + a * b := by
  induction p generalizing q with
  | nil =>
      have hq : q = [] := by simpa using hlen.symm
      subst q
      simp [dot]
  | cons x xs ih =>
      cases q with
      | nil => simp at hlen
      | cons y ys =>
          change x * y + dot (xs ++ [a]) (ys ++ [b]) =
            (x * y + dot xs ys) + a * b
          rw [ih ys (by simpa using hlen)]
          grind

private theorem dot_rev_rev (xs ys : List Int) (hlen : xs.length = ys.length) :
    dot xs.reverse ys.reverse = dot xs ys := by
  induction xs generalizing ys with
  | nil =>
      have hys : ys = [] := by simpa using hlen.symm
      subst ys
      simp [dot]
  | cons x xs ih =>
      cases ys with
      | nil => simp at hlen
      | cons y ys =>
          rw [List.reverse_cons, List.reverse_cons,
            dot_append_singleton _ _ x y (by simpa using hlen)]
          rw [ih ys (by simpa using hlen)]
          change dot xs ys + x * y = x * y + dot xs ys
          omega

/-! ### Sorted-list utilities -/

private abbrev intLe (a b : Int) : Bool := a ≤ b

private theorem intLe_trans (a b c : Int) :
    intLe a b → intLe b c → intLe a c := by
  simp [intLe]
  omega

private theorem intLe_total (a b : Int) : intLe a b || intLe b a := by
  unfold intLe
  by_cases h : a ≤ b
  · simp [h]
  · have h' : b ≤ a := by omega
    simp [h, h']

private theorem sorted_mergeSort (l : List Int) :
    (l.mergeSort intLe).Pairwise (fun a b : Int => a ≤ b) := by
  have hp := List.pairwise_mergeSort intLe_trans intLe_total l
  exact hp.imp (by intro a b h; simpa [intLe] using h)

private theorem boolPairwise_of_le {l : List Int}
    (h : l.Pairwise (fun a b : Int => a ≤ b)) :
    l.Pairwise (fun a b : Int => intLe a b = true) := by
  exact h.imp (by intro a b hab; simp [intLe, hab])

private theorem mergeSort_eq_of_perm {l₁ l₂ : List Int} (h : List.Perm l₁ l₂) :
    l₁.mergeSort = l₂.mergeSort := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    (fun a b _ _ hab hba => by omega)
  · exact sorted_mergeSort l₁
  · exact sorted_mergeSort l₂
  · exact (List.mergeSort_perm l₁ intLe).trans
      (h.trans (List.mergeSort_perm l₂ intLe).symm)

private theorem pairwise_reverse_neg : ∀ {l : List Int},
    l.Pairwise (fun a b : Int => a ≤ b) →
    (l.reverse.map (fun x : Int => -x)).Pairwise (fun a b : Int => a ≤ b)
  | [], h => by simp
  | a :: l, h => by
      have hpair := List.pairwise_cons.mp h
      have ht := pairwise_reverse_neg hpair.2
      rw [List.reverse_cons, List.map_append, List.pairwise_append]
      refine ⟨ht, ?_, ?_⟩
      · simp
      · intro u hu v hv
        simp at hv
        subst v
        simp only [List.mem_map] at hu
        rcases hu with ⟨u', hu', rfl⟩
        have hul : u' ∈ l := List.mem_reverse.mp hu'
        have hau : a ≤ u' := hpair.1 u' (by simp [hul])
        omega

private theorem mergeSort_map_neg (l : List Int) :
    (l.map (fun x : Int => -x)).mergeSort =
      l.mergeSort.reverse.map (fun x : Int => -x) := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    (fun a b _ _ hab hba => by omega)
  · exact sorted_mergeSort _
  · exact pairwise_reverse_neg (sorted_mergeSort l)
  · have p1 := List.mergeSort_perm (l.map (fun x : Int => -x)) intLe
    have p2 := (List.mergeSort_perm l intLe).map (fun x : Int => -x)
    have p3 : List.Perm (l.mergeSort.map (fun x : Int => -x))
        (l.mergeSort.reverse.map (fun x : Int => -x)) :=
      ((List.reverse_perm l.mergeSort).map (fun x : Int => -x)).symm
    exact p1.trans (p2.symm.trans p3)

/-! ### Bubble-sort argument for the rearrangement inequality -/

private def invCount : List Int → Nat
  | [] => 0
  | a :: l => l.countP (fun b : Int => b < a) + invCount l

private theorem invCount_pairwise : ∀ {l : List Int}, invCount l = 0 →
    l.Pairwise (fun a b : Int => a ≤ b)
  | [], h => by simp
  | a :: l, h => by
      simp only [invCount] at h
      have hcnt : l.countP (fun b : Int => b < a) = 0 := by omega
      have htail : invCount l = 0 := by omega
      rw [List.pairwise_cons]
      refine ⟨?_, invCount_pairwise htail⟩
      intro b hb
      have hnot' := (List.countP_eq_zero.mp hcnt) b hb
      have hnot : ¬ b < a := by simpa using hnot'
      omega

private def bubbleStep : List Int → Option (List Int)
  | [] => none
  | [_] => none
  | a :: b :: t =>
      if b < a then some (b :: a :: t)
      else (bubbleStep (b :: t)).map (fun l => a :: l)

private theorem bubbleStep_some_of_pos : ∀ {ys : List Int},
    0 < invCount ys → ∃ zs, bubbleStep ys = some zs
  | [], h => by simp [invCount] at h
  | [a], h => by simp [invCount] at h
  | a :: b :: t, h => by
      by_cases hlt : b < a
      · exact ⟨b :: a :: t, by simp [bubbleStep, hlt]⟩
      · by_cases hcnt : 0 < (b :: t).countP (fun z : Int => z < a)
        · obtain ⟨c, hct, hcaBool⟩ := List.countP_pos_iff.mp hcnt
          have hca : c < a := by simpa using hcaBool
          have hct' : c ∈ t := by
            simp only [List.mem_cons] at hct
            rcases hct with rfl | hct
            · exact False.elim (hlt hca)
            · exact hct
          have hcb : c < b := by omega
          have hcntb : 0 < t.countP (fun z : Int => z < b) :=
            List.countP_pos_iff.mpr ⟨c, hct', by simpa using hcb⟩
          have htail : 0 < invCount (b :: t) := by
            simp only [invCount]
            omega
          obtain ⟨zs, hz⟩ := bubbleStep_some_of_pos htail
          exact ⟨a :: zs, by simp [bubbleStep, hlt, hz]⟩
        · simp only [invCount] at h
          have hz : (b :: t).countP (fun z : Int => z < a) = 0 := by omega
          have htail : 0 < invCount (b :: t) := by
            simp only [invCount]
            omega
          obtain ⟨zs, hz'⟩ := bubbleStep_some_of_pos htail
          exact ⟨a :: zs, by simp [bubbleStep, hlt, hz']⟩

private theorem bubbleStep_spec : ∀ (ys zs : List Int),
    bubbleStep ys = some zs →
    List.Perm zs ys ∧ invCount zs < invCount ys ∧
      ∀ (xs : List Int), xs.length = ys.length →
        xs.Pairwise (fun a b : Int => a ≤ b) → dot xs ys ≤ dot xs zs
  | [], zs, h => by simp [bubbleStep] at h
  | [a], zs, h => by simp [bubbleStep] at h
  | a :: b :: t, zs, h => by
      by_cases hlt : b < a
      · simp [bubbleStep, hlt] at h
        subst zs
        refine ⟨List.Perm.swap a b t, ?_, ?_⟩
        · have hba : ¬ a < b := by omega
          simp only [invCount, List.countP_cons]
          simp [hlt, hba]
          omega
        · intro xs hlen hxsp
          cases xs with
          | nil => simp at hlen
          | cons x1 xs1 =>
              cases xs1 with
              | nil => simp at hlen
              | cons x2 xs2 =>
                  have hx12 : x1 ≤ x2 := (List.pairwise_cons.mp hxsp).1 x2 (by simp)
                  have hprod : 0 ≤ (x2 - x1) * (a - b) :=
                    Int.mul_nonneg (by omega) (by omega)
                  unfold dot
                  simp only [List.zipWith_cons_cons, List.sum_cons]
                  grind
      · cases hb : bubbleStep (b :: t) with
        | none => simp [bubbleStep, hlt, hb] at h
        | some zs' =>
            simp [bubbleStep, hlt, hb] at h
            subst zs
            have sp := bubbleStep_spec (b :: t) zs' hb
            rcases sp with ⟨hp, hi, hd⟩
            refine ⟨hp.cons a, ?_, ?_⟩
            · have hcp : zs'.countP (fun z : Int => z < a) =
                  (b :: t).countP (fun z : Int => z < a) := hp.countP_eq _
              change zs'.countP (fun z : Int => z < a) + invCount zs' <
                (b :: t).countP (fun z : Int => z < a) + invCount (b :: t)
              rw [hcp]
              omega
            · intro xs hlen hxsp
              cases xs with
              | nil => simp at hlen
              | cons x xs' =>
                  have hlen' : xs'.length = (b :: t).length := by simpa using hlen
                  have hxsp' : xs'.Pairwise (fun a b : Int => a ≤ b) :=
                    (List.pairwise_cons.mp hxsp).2
                  have hd' := hd xs' hlen' hxsp'
                  change x * a + dot xs' (b :: t) ≤ x * a + dot xs' zs'
                  omega

private theorem dot_le_sorted (xs ys : List Int) (hlen : xs.length = ys.length)
    (hxsp : xs.Pairwise (fun a b : Int => a ≤ b)) :
    dot xs ys ≤ dot xs ys.mergeSort := by
  have hmain : ∀ n : Nat, ∀ (xs ys : List Int), invCount ys = n →
      xs.length = ys.length → xs.Pairwise (fun a b : Int => a ≤ b) →
      dot xs ys ≤ dot xs ys.mergeSort := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro xs ys hn hlen hxsp
        by_cases hz : invCount ys = 0
        · rw [List.mergeSort_of_pairwise (le := intLe)
            (boolPairwise_of_le (invCount_pairwise hz))]
          omega
        · have hpos : 0 < invCount ys := Nat.pos_of_ne_zero hz
          obtain ⟨zs, hstep⟩ := bubbleStep_some_of_pos hpos
          have sp := bubbleStep_spec ys zs hstep
          rcases sp with ⟨hp, hdec, hd⟩
          have hlenZ : zs.length = ys.length := hp.length_eq
          have hd' : dot xs ys ≤ dot xs zs := hd xs (by omega) hxsp
          have hdecn : invCount zs < n := by omega
          have ih' := ih (invCount zs) hdecn xs zs rfl (by omega) hxsp
          have hs := mergeSort_eq_of_perm hp
          rw [← hs]
          omega
  exact hmain (invCount ys) xs ys rfl hlen hxsp

/-! ### Sorting the rows by their first column -/

private def rowLe (p q : Int × Int) : Bool := decide (p.1 ≤ q.1)

private theorem rowLe_trans (p q r : Int × Int) :
    rowLe p q → rowLe q r → rowLe p r := by
  simp [rowLe]
  omega

private theorem rowLe_total (p q : Int × Int) : rowLe p q || rowLe q p := by
  unfold rowLe
  by_cases h : p.1 ≤ q.1
  · simp [h]
  · have h' : q.1 ≤ p.1 := by omega
    simp [h, h']

private theorem sortedRows_fst_pairwise (rs : Rows) :
    ((rs.mergeSort rowLe).map Prod.fst).Pairwise (fun a b : Int => a ≤ b) := by
  have hp := List.pairwise_mergeSort rowLe_trans rowLe_total rs
  have hp' : (rs.mergeSort rowLe).Pairwise (fun p q : Int × Int => p.1 ≤ q.1) :=
    hp.imp (by intro p q h; simpa [rowLe] using h)
  exact hp'.map Prod.fst (by intro p q h; exact h)

private theorem sortedRows_fst_eq (rs : Rows) :
    (rs.mergeSort rowLe).map Prod.fst = (rs.map Prod.fst).mergeSort := by
  apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
    (fun a b _ _ hab hba => by omega)
  · exact sortedRows_fst_pairwise rs
  · exact sorted_mergeSort _
  · exact ((List.mergeSort_perm rs rowLe).map Prod.fst).trans
      (List.mergeSort_perm (rs.map Prod.fst) intLe).symm

private theorem sortedRows_snd_mergeSort_eq (rs : Rows) :
    ((rs.mergeSort rowLe).map Prod.snd).mergeSort = (rs.map Prod.snd).mergeSort := by
  exact mergeSort_eq_of_perm ((List.mergeSort_perm rs rowLe).map Prod.snd)

/-! ### Main inequalities -/

private theorem upper_bound (rs : Rows) : dotRows rs ≤ dotRows (monotone rs) := by
  let sr := rs.mergeSort rowLe
  let X := (rs.map Prod.fst).mergeSort
  let Y := (rs.map Prod.snd).mergeSort
  have hperm : List.Perm sr rs := by
    dsimp [sr]
    exact List.mergeSort_perm rs rowLe
  have hp1 : sr.map Prod.fst = X := by
    dsimp [sr, X]
    exact sortedRows_fst_eq rs
  have hp2 : (sr.map Prod.snd).mergeSort = Y := by
    dsimp [sr, Y]
    exact sortedRows_snd_mergeSort_eq rs
  have hstep := dot_le_sorted (sr.map Prod.fst) (sr.map Prod.snd) (by simp)
    (by simpa [sr] using sortedRows_fst_pairwise rs)
  calc
    dotRows rs = dotRows sr := (dotRows_perm_of_perm hperm).symm
    _ = dot X (sr.map Prod.snd) := by rw [dotRows_eq_dot, hp1]
    _ ≤ dot X Y := by
      rw [← hp1, ← hp2]
      exact hstep
    _ = dotRows (monotone rs) := by
      dsimp [X, Y, monotone]
      rw [dotRows_zip]

private theorem lower_bound (rs : Rows) : dotRows (counter rs) ≤ dotRows rs := by
  let rs' : Rows := rs.map (fun p : Int × Int => (-p.1, p.2))
  have hu := upper_bound rs'
  have hneg1 : dotRows rs' = - dotRows rs := by
    dsimp [rs']
    exact dotRows_map_neg_fst rs
  have hxs : (rs'.map Prod.fst).mergeSort =
      ((rs.map Prod.fst).mergeSort).reverse.map (fun x : Int => -x) := by
    dsimp [rs']
    simpa [Function.comp_def] using mergeSort_map_neg (rs.map Prod.fst)
  have hys : (rs'.map Prod.snd).mergeSort = (rs.map Prod.snd).mergeSort := by
    dsimp [rs']
    rw [map_negpair_snd rs]
  have hneg2 : dotRows (monotone rs') = - dotRows (counter rs) := by
    dsimp [monotone, counter]
    rw [hxs, hys, dotRows_zip, dotRows_zip, dot_map_neg]
    congr 1
    simpa using (dot_rev_rev ((rs.map Prod.fst).mergeSort)
      ((rs.map Prod.snd).mergeSort).reverse (by simp))
  rw [hneg1, hneg2] at hu
  omega

theorem sound_Couplings_Rearrangement : Couplings.Rearrangement.Goal := by
  intro rs
  exact ⟨lower_bound rs, upper_bound rs⟩

end TinylambOpt.ResearchProofs.Atlas.Goal_Couplings_Rearrangement
