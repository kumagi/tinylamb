import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_TwoStageOrder

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline Pipeline

private def sumF (xs : List Job) : Nat := (xs.map (fun j : Job => j.1)).sum
private def sumS (xs : List Job) : Nat := (xs.map (fun j : Job => j.2)).sum

/-- Johnson の隣接入替え条件: p を q の前に置いても完了時刻が増えない。 -/
private def beforeJob (p q : Job) : Prop := min p.1 q.2 ≤ min p.2 q.1

private theorem permNatSum {l₁ l₂ : List Nat} : l₁.Perm l₂ → l₁.sum = l₂.sum := by
  intro h
  induction h with
  | nil => rfl
  | cons a h ih => simp [ih]
  | swap a b l => simp [Nat.add_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => rw [ih₁, ih₂]

private theorem sumFPerm {xs ys : List Job} (h : xs.Perm ys) : sumF xs = sumF ys := by
  exact permNatSum (h.map (fun j : Job => j.1))

private theorem sumSPerm {xs ys : List Job} (h : xs.Perm ys) : sumS xs = sumS ys := by
  exact permNatSum (h.map (fun j : Job => j.2))

private theorem foldFst (s t : Nat) (xs : List Job) :
    (xs.foldl step (s, t)).1 = s + sumF xs := by
  induction xs generalizing s t with
  | nil => simp [sumF]
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp [step]
      rw [ih]
      simp [sumF]
      omega

private theorem foldSndGe (s t : Nat) (xs : List Job) :
    t + sumS xs ≤ (xs.foldl step (s, t)).2 := by
  induction xs generalizing s t with
  | nil => simp [sumS]
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp [step]
      have hi := ih (s + a) (max (s + a) t + b)
      simp [sumS] at hi ⊢
      omega

private theorem foldFstLeSnd (s t : Nat) (xs : List Job) (hst : s ≤ t) :
    (xs.foldl step (s, t)).1 ≤ (xs.foldl step (s, t)).2 := by
  induction xs generalizing s t with
  | nil => simpa using hst
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons]
      dsimp [step]
      apply ih
      omega

private theorem stepTMono (s t₁ t₂ : Nat) (h : t₁ ≤ t₂) (xs : List Job) :
    (xs.foldl step (s, t₁)).2 ≤ (xs.foldl step (s, t₂)).2 := by
  induction xs generalizing s t₁ t₂ with
  | nil => simpa using h
  | cons j rest ih =>
      rcases j with ⟨a, b⟩
      rw [List.foldl_cons, List.foldl_cons]
      dsimp only [step]
      apply ih
      have hm : max (s + a) t₁ ≤ max (s + a) t₂ := (Nat.max_le).mpr
        ⟨Nat.le_max_left _ _, Nat.le_trans h (Nat.le_max_right _ _)⟩
      omega

private theorem twoStepLe (a b c d s t : Nat) (h : min a d ≤ min b c) :
    max (s + a + c) (max (s + a) t + b) + d ≤
      max (s + c + a) (max (s + c) t + d) + b := by
  by_cases had : a ≤ d
  · have hmin : min a d = a := Nat.min_eq_left had
    have habc : a ≤ min b c := by simpa [hmin] using h
    have hab : a ≤ b := Nat.le_trans habc (Nat.min_le_left _ _)
    have hac : a ≤ c := Nat.le_trans habc (Nat.min_le_right _ _)
    have hma : s + a ≤ max (s + a) t := Nat.le_max_left _ _
    have hmc : s + c ≤ max (s + c) t := Nat.le_max_left _ _
    have hmt : t ≤ max (s + c) t := Nat.le_max_right _ _
    omega
  · have hda : d ≤ a := by omega
    have hmin : min a d = d := Nat.min_eq_right hda
    have hdbc : d ≤ min b c := by simpa [hmin] using h
    have hdb : d ≤ b := Nat.le_trans hdbc (Nat.min_le_left _ _)
    have hdc : d ≤ c := Nat.le_trans hdbc (Nat.min_le_right _ _)
    have hma : s + a ≤ max (s + a) t := Nat.le_max_left _ _
    have hmc : s + c ≤ max (s + c) t := Nat.le_max_left _ _
    have hmt : t ≤ max (s + c) t := Nat.le_max_right _ _
    omega

private theorem swapStateLe (s t : Nat) (p q : Job) (rest : List Job)
    (hcond : beforeJob p q) :
    ((p :: q :: rest).foldl step (s, t)).2 ≤ ((q :: p :: rest).foldl step (s, t)).2 := by
  have htwo := twoStepLe p.1 p.2 q.1 q.2 s t hcond
  have hstate : (step (step (s, t) p) q).2 ≤ (step (step (s, t) q) p).2 := by
    simpa [step] using htwo
  have hfst : (step (step (s, t) p) q).1 = (step (step (s, t) q) p).1 := by
    simp [step]
    omega
  have hmono := stepTMono (step (step (s, t) q) p).1
    (step (step (s, t) p) q).2 (step (step (s, t) q) p).2 hstate rest
  calc
    ((p :: q :: rest).foldl step (s, t)).2 =
        (rest.foldl step (step (step (s, t) p) q)).2 := by rfl
    _ = (rest.foldl step ((step (step (s, t) q) p).1, (step (step (s, t) p) q).2)).2 := by
      have hpairs : step (step (s, t) p) q =
          ((step (step (s, t) q) p).1, (step (step (s, t) p) q).2) := Prod.ext hfst rfl
      rw [hpairs]
    _ ≤ (rest.foldl step ((step (step (s, t) q) p).1, (step (step (s, t) q) p).2)).2 := hmono
    _ = ((q :: p :: rest).foldl step (s, t)).2 := by rfl

private theorem bubbleFrontState (a : Job) (s t : Nat) (pre post : List Job)
    (hbefore : ∀ q ∈ pre, beforeJob a q) :
    ((a :: (pre ++ post)).foldl step (s, t)).2 ≤
      ((pre ++ a :: post).foldl step (s, t)).2 := by
  induction pre generalizing s t post with
  | nil => simp
  | cons q qs ih =>
      have hq : beforeJob a q := hbefore q (by simp)
      have hrest : ∀ z ∈ qs, beforeJob a z := by
        intro z hz
        exact hbefore z (by simp [hz])
      have hswap := swapStateLe s t a q (qs ++ post) hq
      have hih := ih (step (s, t) q).1 (step (s, t) q).2 post hrest
      calc
        ((a :: ((q :: qs) ++ post)).foldl step (s, t)).2 =
            ((a :: q :: (qs ++ post)).foldl step (s, t)).2 := by rfl
        _ ≤ ((q :: a :: (qs ++ post)).foldl step (s, t)).2 := hswap
        _ = ((a :: (qs ++ post)).foldl step (step (s, t) q)).2 := by
          rfl
        _ ≤ ((qs ++ a :: post).foldl step (step (s, t) q)).2 := hih
        _ = (((q :: qs) ++ a :: post).foldl step (s, t)).2 := by
          rfl

private theorem sortedPermStateLe (l ys : List Job) (hperm : l.Perm ys)
    (hsorted : l.Pairwise beforeJob) (s t : Nat) :
    (l.foldl step (s, t)).2 ≤ (ys.foldl step (s, t)).2 := by
  induction l generalizing ys s t with
  | nil =>
      have hylen : ys.length = 0 := by simpa using hperm.length_eq.symm
      cases ys with
      | nil => simp
      | cons y ys' => simp at hylen
  | cons a rest ih =>
      have ha_ys : a ∈ ys := hperm.mem_iff.mp (by simp)
      rcases List.eq_append_cons_of_mem ha_ys with ⟨pre, post, hys_eq, hnotpre⟩
      have hpw := List.pairwise_cons.mp hsorted
      have hperm' : (a :: rest).Perm (pre ++ a :: post) := by
        simpa [hys_eq] using hperm
      have htailperm : rest.Perm (pre ++ post) := by
        have h := hperm'.erase a
        simpa [List.erase_append, hnotpre] using h
      have hbefore_pre : ∀ q ∈ pre, beforeJob a q := by
        intro q hq
        have hqtail : q ∈ pre ++ post := by simp [hq]
        have hqrest : q ∈ rest := htailperm.symm.mem_iff.mp hqtail
        exact hpw.1 q hqrest
      have hih := ih (pre ++ post) htailperm hpw.2 (step (s, t) a).1 (step (s, t) a).2
      have hbub := bubbleFrontState a s t pre post hbefore_pre
      calc
        ((a :: rest).foldl step (s, t)).2 =
            (rest.foldl step (step (s, t) a)).2 := by rw [List.foldl_cons]
        _ ≤ ((pre ++ post).foldl step (step (s, t) a)).2 := hih
        _ = ((a :: (pre ++ post)).foldl step (s, t)).2 := by rw [List.foldl_cons]
        _ ≤ ((pre ++ a :: post).foldl step (s, t)).2 := hbub
        _ = (ys.foldl step (s, t)).2 := by rw [hys_eq]

private theorem pairwiseBeforeA {l : List Job} (leA : Job → Job → Bool)
    (hleA : ∀ a b, leA a b = true → a.1 ≤ b.1)
    (hall : ∀ j ∈ l, j.1 ≤ j.2)
    (hpw : l.Pairwise (fun a b => leA a b = true)) : l.Pairwise beforeJob := by
  induction hpw with
  | nil => simp
  | cons hforall hpair ih =>
      rename_i a t
      rw [List.pairwise_cons]
      constructor
      · intro b hb
        have hab : a.1 ≤ b.1 := hleA a b (hforall b hb)
        have haA : a.1 ≤ a.2 := hall a (by simp)
        have hbA : b.1 ≤ b.2 := hall b (List.mem_cons_of_mem a hb)
        unfold beforeJob
        omega
      · apply ih
        intro j hj
        exact hall j (List.mem_cons_of_mem a hj)

private theorem pairwiseBeforeB {l : List Job} (leB : Job → Job → Bool)
    (hleB : ∀ a b, leB a b = true → b.2 ≤ a.2)
    (hall : ∀ j ∈ l, j.2 ≤ j.1)
    (hpw : l.Pairwise (fun a b => leB a b = true)) : l.Pairwise beforeJob := by
  induction hpw with
  | nil => simp
  | cons hforall hpair ih =>
      rename_i a t
      rw [List.pairwise_cons]
      constructor
      · intro b hb
        have hba : b.2 ≤ a.2 := hleB a b (hforall b hb)
        have haB : a.2 ≤ a.1 := hall a (by simp)
        have hbB : b.2 ≤ b.1 := hall b (List.mem_cons_of_mem a hb)
        unfold beforeJob
        omega
      · apply ih
        intro j hj
        exact hall j (List.mem_cons_of_mem a hj)

private theorem filterPermAppend (p : Job → Bool) : ∀ xs : List Job,
    (xs.filter p ++ xs.filter (not ∘ p)).Perm xs := by
  intro xs
  induction xs with
  | nil => simp
  | cons j rest ih =>
      cases hp : p j with
      | true =>
          simp [hp, ih]
      | false =>
          have hmove : (rest.filter p ++ j :: rest.filter (not ∘ p)).Perm
              (j :: (rest.filter p ++ rest.filter (not ∘ p))) := by
            induction rest.filter p with
            | nil => simp
            | cons x xs' ih2 =>
                have h1 : (x :: (xs' ++ j :: rest.filter (not ∘ p))).Perm
                    (x :: (j :: (xs' ++ rest.filter (not ∘ p)))) := ih2.cons x
                have h2 : (x :: (j :: (xs' ++ rest.filter (not ∘ p)))).Perm
                    (j :: (x :: (xs' ++ rest.filter (not ∘ p)))) :=
                  List.Perm.swap j x (xs' ++ rest.filter (not ∘ p))
                exact h1.trans h2
          simpa [hp] using hmove.trans (ih.cons j)

private theorem filterPartitionSum (p : Job → Bool) (f : Job → Nat) : ∀ xs : List Job,
    ((xs.filter p).map f).sum + ((xs.filter (not ∘ p)).map f).sum = (xs.map f).sum := by
  intro xs
  induction xs with
  | nil => simp
  | cons j rest ih =>
      cases hp : p j <;> simp [hp, ih, Nat.add_assoc, Nat.add_left_comm]

private theorem partitionSumF (p : Job → Bool) (xs : List Job) :
    sumF (xs.partition p).1 + sumF (xs.partition p).2 = sumF xs := by
  simpa [List.partition_eq_filter_filter, sumF] using filterPartitionSum p (fun j : Job => j.1) xs

private theorem partitionSumS (p : Job → Bool) (xs : List Job) :
    sumS (xs.partition p).1 + sumS (xs.partition p).2 = sumS xs := by
  simpa [List.partition_eq_filter_filter, sumS] using filterPartitionSum p (fun j : Job => j.2) xs

theorem sound_Pipeline_TwoStageOrder : TwoStageOrder.Goal := by
  intro xs ys hperm
  let p : Job → Bool := fun j => j.1 ≤ j.2
  let front := (xs.partition p).1
  let back := (xs.partition p).2
  let leA : Job → Job → Bool := fun a b => a.1 ≤ b.1
  let leB : Job → Job → Bool := fun a b => a.2 ≥ b.2
  let jf := front.mergeSort leA
  let jb := back.mergeSort leB
  have hfrontProp : ∀ j ∈ front, j.1 ≤ j.2 := by
    intro j hj
    have hjf : j ∈ xs.filter p := by
      simpa [front, List.partition_eq_filter_filter] using hj
    simpa [p] using (List.mem_filter.mp hjf).2
  have hbackProp : ∀ j ∈ back, j.2 ≤ j.1 := by
    intro j hj
    have hjb : j ∈ xs.filter (not ∘ p) := by
      simpa [back, List.partition_eq_filter_filter] using hj
    have hnot : (not ∘ p) j = true := (List.mem_filter.mp hjb).2
    have hpj : p j = false := by
      cases hpj : p j <;> simp [hpj] at hnot ⊢
    have hnle : ¬ j.1 ≤ j.2 := by
      intro hle
      have hptrue : p j = true := by simp [p, hle]
      simp [hpj] at hptrue
    omega
  have hpwA := List.pairwise_mergeSort
    (le := leA)
    (by intro a b c hab hbc; simp [leA] at hab hbc ⊢; omega)
    (by intro a b; simp [leA]; omega) front
  have hpwB := List.pairwise_mergeSort
    (le := leB)
    (by intro a b c hab hbc; simp [leB] at hab hbc ⊢; omega)
    (by intro a b; simp [leB]; omega) back
  have hjfSorted : jf.Pairwise beforeJob := by
    apply pairwiseBeforeA leA (by intro a b h; simpa [leA] using h)
    · intro j hj
      apply hfrontProp j
      simpa [jf] using (List.mem_mergeSort.mp hj)
    · simpa [jf] using hpwA
  have hjbSorted : jb.Pairwise beforeJob := by
    apply pairwiseBeforeB leB (by intro a b h; simpa [leB] using h)
    · intro j hj
      apply hbackProp j
      simpa [jb] using (List.mem_mergeSort.mp hj)
    · simpa [jb] using hpwB
  have hcross : ∀ a ∈ jf, ∀ b ∈ jb, beforeJob a b := by
    intro a ha b hb
    have haA : a.1 ≤ a.2 := hfrontProp a (by simpa [jf] using (List.mem_mergeSort.mp ha))
    have hbB : b.2 ≤ b.1 := hbackProp b (by simpa [jb] using (List.mem_mergeSort.mp hb))
    unfold beforeJob
    omega
  have hsorted : (jf ++ jb).Pairwise beforeJob := by
    rw [List.pairwise_append]
    exact ⟨hjfSorted, hjbSorted, hcross⟩
  have hfperm : jf.Perm front := List.mergeSort_perm front leA
  have hbperm : jb.Perm back := List.mergeSort_perm back leB
  have hpartperm : (front ++ back).Perm xs := by
    simpa [front, back, List.partition_eq_filter_filter] using filterPermAppend p xs
  have hjperm : (jf ++ jb).Perm xs := (hfperm.append hbperm).trans hpartperm
  have hmain := sortedPermStateLe (jf ++ jb) ys (hjperm.trans hperm) hsorted 0 0
  have hjdef : johnson xs = jf ++ jb := by
    rfl
  rw [hjdef]
  exact hmain

end TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_TwoStageOrder
