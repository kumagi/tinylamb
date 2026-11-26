import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_H2
open TinylambOpt.ResearchGoals.November26

abbrev Row := Nat × Nat
abbrev ZRow := Row × Nat

def le (a b : Row) : Bool := decide (b.2 ≤ a.2)
def zle (a b : ZRow) : Bool := List.zipIdxLE le a b

def bucket (n : Nat) (xs : List Row) : List Row := xs.filter (fun r => r.2 == n)
def buckets (m : Nat) (xs : List Row) : List Row :=
  (List.range (m + 1)).reverse.flatMap (fun n => bucket n xs)

def bucketZ (n : Nat) (qs : List ZRow) : List ZRow := qs.filter (fun q => q.1.2 == n)
def bucketsZ : Nat → List ZRow → List ZRow
  | 0, qs => bucketZ 0 qs
  | m + 1, qs => bucketZ (m + 1) qs ++ bucketsZ m qs

theorem le_trans : ∀ a b c : Row, le a b = true → le b c = true → le a c = true := by
  intro a b c hab hbc
  simp [le] at hab hbc ⊢
  omega

theorem le_total : ∀ a b : Row, (le a b || le b a) = true := by
  intro a b
  simp [le]
  omega

theorem zle_trans (a b c : ZRow) : zle a b = true → zle b c = true → zle a c = true := by
  exact List.zipIdxLE_trans le_trans a b c

theorem zle_total (a b : ZRow) : (zle a b || zle b a) = true := by
  exact List.zipIdxLE_total le_total a b

theorem pairwise_zipIdx_index (off : Nat) : ∀ xs : List Row,
    (xs.zipIdx off).Pairwise (fun a b : ZRow => a.2 ≤ b.2)
  | [] => by simp
  | x :: xs => by
      rw [List.zipIdx_cons, List.pairwise_cons]
      constructor
      · intro q hq
        obtain ⟨_, hlt, _⟩ := List.mem_zipIdx hq
        omega
      · exact pairwise_zipIdx_index (off + 1) xs

theorem zle_of_count_eq_index_le {a b : ZRow} {n : Nat}
    (ha : a.1.2 = n) (hb : b.1.2 = n) (hi : a.2 ≤ b.2) : zle a b = true := by
  simp [zle, List.zipIdxLE, le, ha, hb, hi]

theorem zle_of_high_low {a b : ZRow} {m : Nat}
    (ha : a.1.2 = m + 1) (hb : b.1.2 ≤ m) : zle a b = true := by
  simp [zle, List.zipIdxLE, le, ha]
  omega

theorem bucketZ_pairwise (n : Nat) : ∀ (qs : List ZRow),
    qs.Pairwise (fun a b : ZRow => a.2 ≤ b.2) →
    (bucketZ n qs).Pairwise (fun a b : ZRow => zle a b = true)
  | [], hp => by simp [bucketZ]
  | q :: qs, hp => by
      rw [List.pairwise_cons] at hp
      obtain ⟨hhead, htail⟩ := hp
      unfold bucketZ
      rw [List.filter_cons]
      by_cases h : q.1.2 == n
      · simp [h]
        constructor
        · intro a c i hpqs hc
          have hi : q.2 ≤ i := hhead ((⟨a, c⟩, i) : ZRow) hpqs
          exact zle_of_count_eq_index_le (by simpa [beq_iff_eq] using h) hc hi
        · exact bucketZ_pairwise n qs htail
      · simp [h]
        exact bucketZ_pairwise n qs htail

theorem bucketsZ_mem_le (m : Nat) : ∀ (qs : List ZRow) (q : ZRow),
    q ∈ bucketsZ m qs → q.1.2 ≤ m
  | qs, q, hq => by
      induction m with
      | zero =>
          unfold bucketsZ bucketZ at hq
          have hc : q.1.2 = 0 := by simpa [beq_iff_eq] using (List.mem_filter.mp hq).2
          omega
      | succ m ih =>
          unfold bucketsZ at hq
          rcases List.mem_append.mp hq with hhigh | hlow
          · unfold bucketZ at hhigh
            have hc : q.1.2 = m + 1 := by simpa [beq_iff_eq] using (List.mem_filter.mp hhigh).2
            omega
          · exact Nat.le.step (ih hlow)

theorem bucketsZ_pairwise (m : Nat) : ∀ (qs : List ZRow),
    qs.Pairwise (fun a b : ZRow => a.2 ≤ b.2) →
    (bucketsZ m qs).Pairwise (fun a b : ZRow => zle a b = true)
  | qs, hp => by
      induction m with
      | zero =>
          unfold bucketsZ
          exact bucketZ_pairwise 0 qs hp
      | succ m ih =>
          unfold bucketsZ
          rw [List.pairwise_append]
          refine ⟨bucketZ_pairwise (m + 1) qs hp, ih, ?_⟩
          intro a ha b hb
          unfold bucketZ at ha
          have haC : a.1.2 = m + 1 := by simpa [beq_iff_eq] using (List.mem_filter.mp ha).2
          have hbC : b.1.2 ≤ m := bucketsZ_mem_le m qs b hb
          exact zle_of_high_low haC hbC

theorem bucketZ_succ_append_le_perm (m : Nat) : ∀ (qs : List ZRow),
    (bucketZ (m + 1) qs ++ qs.filter (fun q : ZRow => decide (q.1.2 ≤ m))).Perm
      (qs.filter (fun q : ZRow => decide (q.1.2 ≤ m + 1)))
  | [] => List.Perm.nil
  | q :: qs => by
      unfold bucketZ
      rw [List.filter_cons, List.filter_cons, List.filter_cons]
      by_cases hhi : q.1.2 == m + 1
      · have hc : q.1.2 = m + 1 := by simpa [beq_iff_eq] using hhi
        have hle : ¬ q.1.2 ≤ m := by omega
        have hle' : q.1.2 ≤ m + 1 := by omega
        simp [hhi, hle, hle']
        exact bucketZ_succ_append_le_perm m qs
      · have hc : q.1.2 ≠ m + 1 := by simpa [beq_iff_eq] using hhi
        by_cases hle : q.1.2 ≤ m
        · have hle' : q.1.2 ≤ m + 1 := by omega
          simp [hhi, hle, hle']
          exact List.perm_middle.trans (List.Perm.cons q (bucketZ_succ_append_le_perm m qs))
        · have hle' : ¬ q.1.2 ≤ m + 1 := by omega
          simp [hhi, hle, hle']
          exact bucketZ_succ_append_le_perm m qs

theorem bucketsZ_perm (m : Nat) : ∀ (qs : List ZRow),
    (bucketsZ m qs).Perm (qs.filter (fun q : ZRow => decide (q.1.2 ≤ m))) := by
  intro qs
  induction m with
      | zero =>
          unfold bucketsZ bucketZ
          apply List.Perm.of_eq
          congr 1
          funext q
          by_cases hz : q.1.2 = 0
          · simp [hz]
          · simp [hz]
      | succ m ih =>
          unfold bucketsZ
          exact (List.Perm.append (List.Perm.refl _) ih).trans
            (bucketZ_succ_append_le_perm m qs)

theorem bucketsZ_of_bound (m : Nat) : ∀ (qs : List ZRow),
    (∀ q ∈ qs, q.1.2 ≤ m) → (bucketsZ m qs).Perm qs
  | qs, h => by
      have hf : qs.filter (fun q : ZRow => decide (q.1.2 ≤ m)) = qs := by
        apply List.filter_eq_self.mpr
        intro q hq
        simp [h q hq]
      exact (bucketsZ_perm m qs).trans (List.Perm.of_eq hf)

theorem map_fst_filter_count (n : Nat) : ∀ (xs : List Row) (off : Nat),
    ((xs.zipIdx off).filter (fun q : ZRow => q.1.2 == n)).map Prod.fst =
      xs.filter (fun r : Row => r.2 == n)
  | [], off => rfl
  | x :: xs, off => by
      rw [List.zipIdx_cons]
      by_cases h : x.2 == n
      · simp [h, map_fst_filter_count n xs (off + 1)]
      · simp [h, map_fst_filter_count n xs (off + 1)]

theorem buckets_succ (m : Nat) (xs : List Row) :
    buckets (m + 1) xs = bucket (m + 1) xs ++ buckets m xs := by
  unfold buckets
  rw [show m + 1 + 1 = (m + 1).succ by omega]
  rw [List.range_succ, List.reverse_append, List.reverse_singleton]
  simp [bucket]

theorem bucketsZ_fst (m : Nat) : ∀ (xs : List Row) (off : Nat),
    (bucketsZ m (xs.zipIdx off)).map Prod.fst = buckets m xs
  | xs, off => by
      induction m with
      | zero =>
          unfold bucketsZ bucketZ buckets bucket
          rw [map_fst_filter_count]
          simp
      | succ m ih =>
          unfold bucketsZ
          rw [List.map_append, ih, buckets_succ]
          unfold bucketZ bucket
          rw [map_fst_filter_count]

theorem main : ∀ (xs : List Row) (m : Nat), (∀ r ∈ xs, r.2 ≤ m) →
    buckets m xs = xs.mergeSort le
  | xs, m, hbound => by
      let qs := xs.zipIdx 0
      have hqsBound : ∀ q ∈ qs, q.1.2 ≤ m := by
        intro q hq
        obtain ⟨hge, hlt, hrow⟩ := List.mem_zipIdx hq
        have hlt' : q.2 < xs.length := by simpa using hlt
        have hrow' : q.1 = xs[q.2] := by simpa using hrow
        have hx := hbound xs[q.2] (List.getElem_mem hlt')
        rw [← hrow'] at hx
        exact hx
      have hperm1 := bucketsZ_of_bound m qs hqsBound
      have hperm2 := List.mergeSort_perm qs zle
      have hsorted1 := bucketsZ_pairwise m qs (pairwise_zipIdx_index 0 xs)
      have hsorted2 := List.pairwise_mergeSort zle_trans zle_total qs
      have hEq : bucketsZ m qs = qs.mergeSort zle := by
        apply List.Perm.eq_of_pairwise (le := fun a b : ZRow => zle a b = true)
        · intro p q hp hq hpq hqp
          have hpZ : p ∈ qs := (hperm1.mem_iff).mp hp
          have hqZ : q ∈ qs := (List.mem_mergeSort.mp hq)
          have hle1 : le p.1 q.1 = true := by
            by_cases hh : le p.1 q.1 = true
            · exact hh
            · have hfalse : le p.1 q.1 = false := by cases hb : le p.1 q.1 <;> simp_all
              simp [zle, List.zipIdxLE, hfalse] at hpq
          have hle2 : le q.1 p.1 = true := by
            by_cases hh : le q.1 p.1 = true
            · exact hh
            · have hfalse : le q.1 p.1 = false := by cases hb : le q.1 p.1 <;> simp_all
              simp [zle, List.zipIdxLE, hfalse] at hqp
          have hi1 : p.2 ≤ q.2 := by
            rw [zle, List.zipIdxLE] at hpq
            rw [hle1, hle2] at hpq
            simpa using hpq
          have hi2 : q.2 ≤ p.2 := by
            rw [zle, List.zipIdxLE] at hqp
            rw [hle2, hle1] at hqp
            simpa using hqp
          have hi : p.2 = q.2 := Nat.le_antisymm hi1 hi2
          obtain ⟨_, _, hprow⟩ := List.mem_zipIdx hpZ
          obtain ⟨_, _, hqrow⟩ := List.mem_zipIdx hqZ
          obtain ⟨pr, pi⟩ := p
          obtain ⟨qr, qi⟩ := q
          simp at hi
          subst qi
          have hpRow : pr = xs[pi] := by simpa using hprow
          have hqRow : qr = xs[pi] := by simpa using hqrow
          exact Prod.ext (hpRow.trans hqRow.symm) rfl
        · exact hsorted1
        · exact hsorted2
        · exact hperm1.trans hperm2.symm
      rw [← bucketsZ_fst m xs 0, hEq]
      exact List.mergeSort_zipIdx

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.H2.Goal := by
  intro groups maxCount hbound
  exact main groups maxCount hbound


#print axioms TinylambOpt.ResearchProofs.November26.Analytics.Goal_H2.sound
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_H2
