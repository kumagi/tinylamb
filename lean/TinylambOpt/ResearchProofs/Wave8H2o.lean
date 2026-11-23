import TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o

namespace TinylambOpt.ResearchProofs.Wave8H2o

-- TODO(SkewHybrid): goal appears true (verified by exhaustive small-input probes),
-- but the hash-map fold invariants for the hot/cold partition are not yet
-- formalized here.

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o
open TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown

/-! ## Shared algebra for `Totals` -/

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a
  cases b
  cases c
  simp [plus]
  omega

private theorem plus_comm (a b : Totals) : plus a b = plus b a := by
  cases a
  cases b
  simp [plus]
  omega

private theorem plus_left_comm (a b c : Totals) : plus a (plus b c) = plus b (plus a c) := by
  cases a
  cases b
  cases c
  simp [plus]
  omega

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  cases a
  simp [plus, zero]

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  cases a
  simp [plus, zero]

private theorem foldl_add_left {α : Type} (xs : List α) (g : α → Totals)
    (acc a : Totals) :
    xs.foldl (fun s x => plus s (g x)) (plus a acc) =
      plus a (xs.foldl (fun s x => plus s (g x)) acc) := by
  induction xs generalizing acc with
  | nil => simp [plus_comm]
  | cons x xs ih =>
      change xs.foldl (fun s y => plus s (g y)) (plus (plus a acc) (g x)) =
        plus a (xs.foldl (fun s y => plus s (g y)) (plus acc (g x)))
      rw [show plus (plus a acc) (g x) = plus a (plus acc (g x)) by
        simp [plus_assoc]]
      exact ih (acc := plus acc (g x))

private theorem foldl_add_sum {α : Type} (xs : List α) (g : α → Totals) (acc : Totals) :
    xs.foldl (fun s x => plus s (g x)) acc =
      plus acc (xs.foldl (fun s x => plus s (g x)) zero) := by
  simpa [plus_zero_right] using foldl_add_left xs g zero acc

private theorem summarize_append (xs ys : List (Option Int)) :
    summarize (xs ++ ys) = plus (summarize xs) (summarize ys) := by
  unfold summarize
  rw [List.foldl_append]
  exact foldl_add_sum ys (fun v : Option Int => weighted 1 v) (xs.foldl (fun s v => plus s (weighted 1 v)) zero)

private theorem summarize_cons (v : Option Int) (vs : List (Option Int)) :
    summarize (v :: vs) = plus (weighted 1 v) (summarize vs) := by
  change vs.foldl (fun s x => plus s (weighted 1 x)) (plus zero (weighted 1 v)) =
    plus (weighted 1 v) (vs.foldl (fun s x => plus s (weighted 1 x)) zero)
  simpa [plus_zero_left, plus_zero_right] using
    foldl_add_left vs (fun x : Option Int => weighted 1 x) zero (weighted 1 v)

/-! ## H1: HotRegister -/

private def hotStep (hot : Nat) (s : Nat × Std.HashMap Nat Nat × Nat) (x : Nat) :=
  if x == hot then (s.1 + 1, s.2.1, s.2.2)
  else (s.1, s.2.1.insert x (s.2.1.getD x 0 + 1), s.2.2 + 1)

private theorem hotCount_eq_fold (xs : List Nat) (hot : Nat) :
    hotCount xs hot = xs.foldl (hotStep hot) (0, ({} : Std.HashMap Nat Nat), 0) := by
  rfl

private theorem hotStep_count_inv (hot : Nat) :
    ∀ (xs ys : List Nat) (n0 : Nat) (h0 : Std.HashMap Nat Nat) (u0 : Nat),
      (∀ k : Nat, k ≠ hot → h0.getD k 0 = ys.count k) →
      n0 = ys.count hot →
      u0 + ys.count hot = ys.length →
      let s := xs.foldl (hotStep hot) (n0, h0, u0)
      (∀ k : Nat, k ≠ hot → s.2.1.getD k 0 = (ys ++ xs).count k) ∧
      s.1 = (ys ++ xs).count hot ∧
      s.2.2 + (ys ++ xs).count hot = (ys ++ xs).length := by
  intro xs
  induction xs with
  | nil =>
      intro ys n0 h0 u0 hh hn hu
      simpa [List.count_append] using And.intro hh (And.intro hn hu)
  | cons x xs ih =>
      intro ys n0 h0 u0 hh hn hu
      by_cases hx : x == hot
      · have hxeq : x = hot := by simpa using hx
        have hh' : ∀ k : Nat, k ≠ hot → h0.getD k 0 = (ys ++ [x]).count k := by
          intro k hk
          have hkx : x ≠ k := by omega
          calc
            h0.getD k 0 = ys.count k := hh k hk
            _ = (ys ++ [x]).count k := by simp [List.count_append, List.count_cons, hkx]
        have hn' : n0 + 1 = (ys ++ [x]).count hot := by
          rw [List.count_append, hn]
          simp [List.count_cons, hx]
        have hu' : u0 + (ys ++ [x]).count hot = (ys ++ [x]).length := by
          simp [List.count_append, List.count_cons, hx]
          omega
        have ih' := ih (ys ++ [x]) (n0 + 1) h0 u0 hh' hn' hu'
        simpa [hotStep, hx, List.count_append, Nat.add_assoc] using ih'
      · have hxne : x ≠ hot := by simpa using hx
        have hh' : ∀ k : Nat, k ≠ hot → (h0.insert x (h0.getD x 0 + 1)).getD k 0 = (ys ++ [x]).count k := by
          intro k hk
          by_cases hkx : x == k
          · have hkeq : x = k := by simpa using hkx
            calc
              (h0.insert x (h0.getD x 0 + 1)).getD k 0 = h0.getD x 0 + 1 := by
                rw [Std.HashMap.getD_insert]
                simp [hkeq]
              _ = ys.count x + 1 := by rw [hh x hxne]
              _ = (ys ++ [x]).count k := by
                rw [hkeq]
                simp [List.count_append]
          · calc
              (h0.insert x (h0.getD x 0 + 1)).getD k 0 = h0.getD k 0 := by
                simp [Std.HashMap.getD_insert, hkx]
              _ = ys.count k := hh k hk
              _ = (ys ++ [x]).count k := by simp [List.count_append, List.count_cons, hkx]
        have hn' : n0 = (ys ++ [x]).count hot := by
          rw [List.count_append, hn]
          simp [List.count_cons, hx]
        have hu' : u0 + 1 + (ys ++ [x]).count hot = (ys ++ [x]).length := by
          simp [List.count_append, List.count_cons, hx]
          omega
        have ih' := ih (ys ++ [x]) n0 (h0.insert x (h0.getD x 0 + 1)) (u0 + 1) hh' hn' hu'
        simpa [hotStep, hx, List.count_append, Nat.add_assoc] using ih'

theorem HotRegister_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o.HotRegister.Goal := by
  intro xs hot k
  rw [hotCount_eq_fold]
  dsimp
  have hi := hotStep_count_inv hot xs [] 0 ({} : Std.HashMap Nat Nat) 0
    (by intro k hk; simp)
    (by simp)
    (by simp)
  dsimp at hi
  rcases hi with ⟨hget, hn, hu⟩
  constructor
  · by_cases hk : k == hot
    · have hkeq : k = hot := by simpa using hk
      rw [hkeq]
      simpa using hn
    · have hkne : k ≠ hot := by simpa using hk
      simpa [hkne] using hget k hkne
  · simpa using hu

/-! ## H2: CountTail -/

private def leCnt {β : Type} (cnt : β → Nat) (a b : β) : Bool := decide (cnt b ≤ cnt a)

private def smallMap {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) : Std.HashMap Nat (List β) :=
  gs.foldl (fun h g => if cnt g ≤ t then h.insert (cnt g) (g :: h.getD (cnt g) []) else h) {}

private theorem smallMap_inv {β : Type} (cnt : β → Nat) (t : Nat) :
    ∀ (xs ys : List β) (h : Std.HashMap Nat (List β)),
      (∀ n, n ≤ t → h.getD n [] = (ys.filter fun g => cnt g = n).reverse) →
      ∀ n, n ≤ t →
        (xs.foldl (fun h g => if cnt g ≤ t then h.insert (cnt g) (g :: h.getD (cnt g) []) else h) h).getD n [] =
          ((ys ++ xs).filter fun g => cnt g = n).reverse := by
  intro xs
  induction xs with
  | nil =>
      intro ys h hh n hn
      simpa using hh n hn
  | cons g xs ih =>
      intro ys h hh n hn
      simp only [List.foldl_cons]
      by_cases hgt : cnt g ≤ t
      · have hh' : ∀ m, m ≤ t →
            (h.insert (cnt g) (g :: h.getD (cnt g) [])).getD m [] =
              ((ys ++ [g]).filter fun x => cnt x = m).reverse := by
          intro m hm
          by_cases hmg : cnt g = m
          · subst m
            simp [Std.HashMap.getD_insert, hh (cnt g) hgt, List.filter_append]
          · have hmg' : m ≠ cnt g := by omega
            simp [Std.HashMap.getD_insert, hmg', hh m hm, List.filter_append, hmg]
        simpa [List.append_assoc, hgt] using
          ih (ys ++ [g]) (h.insert (cnt g) (g :: h.getD (cnt g) [])) hh' n hn
      · have hh' : ∀ m, m ≤ t → h.getD m [] = ((ys ++ [g]).filter fun x => cnt x = m).reverse := by
          intro m hm
          rw [hh m hm]
          have hne : cnt g ≠ m := by omega
          simp [List.filter_append, hne]
        simpa [List.append_assoc, hgt] using ih (ys ++ [g]) h hh' n hn

private theorem smallMap_final {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) :
    ∀ n, n ≤ t → (smallMap cnt t gs).getD n [] = (gs.filter fun g => cnt g = n).reverse := by
  unfold smallMap
  simpa using smallMap_inv cnt t gs [] {} (by intro n hn; simp)

private def bucketTail {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) : List β :=
  (List.range (t + 1)).reverse.flatMap (fun n => gs.filter fun g => cnt g = n)

private def sortedByBuckets {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) : List β :=
  (gs.filter fun g => t < cnt g).mergeSort (leCnt cnt) ++ bucketTail cnt t gs

private theorem flatMap_congr_self {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.flatMap_cons]
      rw [h x (by simp), ih (by intro y hy; exact h y (by simp [hy]))]

private theorem hybridOrder_eq (gs : List GroupCount) (t : Nat) :
    hybridOrder gs t = sortedByBuckets (fun g : GroupCount => g.2) t gs := by
  unfold hybridOrder sortedByBuckets bucketTail
  dsimp
  congr 1
  apply flatMap_congr_self
  intro n hn
  have hnle : n ≤ t := by
    simp only [List.mem_reverse, List.mem_range] at hn
    omega
  change ((smallMap (fun g : GroupCount => g.2) t gs).getD n []).reverse =
    (gs.filter fun g => (fun g : GroupCount => g.2) g = n)
  rw [smallMap_final (fun g : GroupCount => g.2) t gs n hnle]
  simp

private theorem leCnt_trans {β : Type} (cnt : β → Nat) :
    ∀ a b c, leCnt cnt a b = true → leCnt cnt b c = true → leCnt cnt a c = true := by
  intro a b c
  simp [leCnt]
  omega

private theorem leCnt_total {β : Type} (cnt : β → Nat) :
    ∀ a b, (leCnt cnt a b || leCnt cnt b a) = true := by
  intro a b
  simp [leCnt]
  omega

private theorem mem_bucketTail_le {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β)
    {p : β} (hp : p ∈ bucketTail cnt t gs) : cnt p ≤ t := by
  unfold bucketTail at hp
  rw [List.mem_flatMap] at hp
  rcases hp with ⟨n, hn, hp⟩
  simp only [List.mem_reverse, List.mem_range] at hn
  have hpn : cnt p = n := by simpa using (List.mem_filter.mp hp).2
  omega

private theorem cons_append_perm_h2 {α : Type} (a : α) : ∀ (l₁ l₂ : List α),
    (a :: (l₁ ++ l₂)).Perm (l₁ ++ (a :: l₂))
  | [], l₂ => by simp
  | b :: l₁, l₂ =>
      (List.Perm.swap a b (l₁ ++ l₂)).symm.trans ((cons_append_perm_h2 a l₁ l₂).cons b)

private theorem split_filter_perm_h2 {β : Type} (p q : β → Bool) (rows : List β)
    (hd : ∀ r ∈ rows, ¬(p r = true ∧ q r = true)) :
    (rows.filter fun r => p r || q r).Perm ((rows.filter p) ++ (rows.filter q)) := by
  induction rows with
  | nil => simp
  | cons r rows ih =>
      have ih' := ih (by intro z hz; exact hd z (by simp [hz]))
      by_cases hp : p r
      · by_cases hq : q r
        · exact False.elim (hd r (by simp) ⟨hp, hq⟩)
        · simpa [hp, hq] using ih'.cons r
      · by_cases hq : q r
        · simpa [hp, hq] using (ih'.cons r).trans (cons_append_perm_h2 r (rows.filter p) (rows.filter q))
        · simpa [hp, hq] using ih'

private theorem bucketTail_perm {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) :
    (bucketTail cnt t gs).Perm (gs.filter fun g => cnt g ≤ t) := by
  induction t with
  | zero =>
      unfold bucketTail
      have hrange : (List.range (0 + 1)).reverse = [0] := by decide
      rw [hrange]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      apply List.Perm.of_eq
      apply List.filter_congr
      intro a ha
      simp
  | succ t ih =>
      unfold bucketTail at ih ⊢
      rw [show List.range (t + 1 + 1) = List.range (t + 1) ++ [t + 1] by
        rw [show t + 1 + 1 = (t + 1).succ by omega, List.range_succ]]
      rw [List.reverse_append, List.reverse_singleton, List.flatMap_append]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      refine (List.Perm.append (List.Perm.refl _) ih).trans ?_
      have hd : ∀ r ∈ gs, ¬(decide (cnt r = t + 1) = true ∧ decide (cnt r ≤ t) = true) := by
        intro r hr h
        have h1 : cnt r = t + 1 := by simpa using h.1
        have h2 : cnt r ≤ t := by simpa using h.2
        omega
      have hp := split_filter_perm_h2 (fun r : β => decide (cnt r = t + 1))
        (fun r => decide (cnt r ≤ t)) gs hd
      refine hp.symm.trans ?_
      apply List.Perm.of_eq
      apply List.filter_congr
      intro a ha
      by_cases hlet : cnt a ≤ t
      · have hle : cnt a ≤ t + 1 := by omega
        simp [hlet, hle]
      · by_cases hle : cnt a ≤ t + 1
        · have heq : cnt a = t + 1 := by omega
          simp [heq, hle]
        · have hne : cnt a ≠ t + 1 := by omega
          simp [hne, hlet, hle]

private theorem filter_cnt_pairwise {β : Type} (cnt : β → Nat) (gs : List β) (n : Nat) :
    (gs.filter fun g => cnt g = n).Pairwise (fun a b => cnt b ≤ cnt a) := by
  induction gs with
  | nil => simp
  | cons g gs ih =>
      by_cases hg : cnt g = n
      · have hf : List.filter (fun x : β => decide (cnt x = n)) (g :: gs) =
            g :: List.filter (fun x : β => decide (cnt x = n)) gs := by simp [hg]
        rw [hf]
        apply List.Pairwise.cons
        · intro a ha
          have hca : cnt a = n := by simpa using (List.mem_filter.mp ha).2
          omega
        · exact ih
      · have hf : List.filter (fun x : β => decide (cnt x = n)) (g :: gs) =
            List.filter (fun x : β => decide (cnt x = n)) gs := by simp [hg]
        rw [hf]
        exact ih

private theorem bucketTail_pairwise_cnt {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) :
    (bucketTail cnt t gs).Pairwise (fun a b => cnt b ≤ cnt a) := by
  induction t with
  | zero =>
      unfold bucketTail
      have hrange : (List.range (0 + 1)).reverse = [0] := by decide
      rw [hrange]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      exact filter_cnt_pairwise cnt gs 0
  | succ t ih =>
      unfold bucketTail at ih ⊢
      rw [show List.range (t + 1 + 1) = List.range (t + 1) ++ [t + 1] by
        rw [show t + 1 + 1 = (t + 1).succ by omega, List.range_succ]]
      rw [List.reverse_append, List.reverse_singleton, List.flatMap_append]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      rw [List.pairwise_append]
      constructor
      · exact filter_cnt_pairwise cnt gs (t + 1)
      constructor
      · exact ih
      · intro a ha b hb
        have hca : cnt a = t + 1 := by simpa using (List.mem_filter.mp ha).2
        have hcb : cnt b ≤ t := mem_bucketTail_le cnt t gs hb
        omega

private theorem filter_or_true_eq {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) :
    (gs.filter fun r => decide (t < cnt r) || decide (cnt r ≤ t)) = gs := by
  induction gs with
  | nil => rfl
  | cons g gs ih =>
      by_cases hle : cnt g ≤ t
      · have hnot : ¬t < cnt g := by omega
        have hp : (decide (t < cnt g) || decide (cnt g ≤ t)) = true := by
          simp [hnot, hle]
        simp [hp, ih]
      · have hlt : t < cnt g := by omega
        have hp : (decide (t < cnt g) || decide (cnt g ≤ t)) = true := by
          simp [hlt, hle]
        simp [hp, ih]

private theorem sortedByBuckets_perm {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) :
    (sortedByBuckets cnt t gs).Perm gs := by
  unfold sortedByBuckets
  have hms : ((gs.filter fun g => t < cnt g).mergeSort (leCnt cnt)).Perm
      (gs.filter fun g => t < cnt g) := List.mergeSort_perm _ _
  have happ := List.Perm.append hms (bucketTail_perm cnt t gs)
  have hd : ∀ r ∈ gs, ¬(decide (t < cnt r) = true ∧ decide (cnt r ≤ t) = true) := by
    intro r hr h
    have h1 : t < cnt r := by simpa using h.1
    have h2 : cnt r ≤ t := by simpa using h.2
    omega
  have hp := split_filter_perm_h2 (fun r : β => decide (t < cnt r))
    (fun r => decide (cnt r ≤ t)) gs hd
  refine (List.Perm.trans happ hp.symm).trans ?_
  exact List.Perm.of_eq (filter_or_true_eq cnt t gs)

private theorem sortedByBuckets_pairwise {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β) :
    (sortedByBuckets cnt t gs).Pairwise (fun a b => cnt b ≤ cnt a) := by
  unfold sortedByBuckets
  rw [List.pairwise_append]
  constructor
  · have h := List.pairwise_mergeSort (leCnt_trans cnt) (leCnt_total cnt)
      (gs.filter fun g => t < cnt g)
    exact h.imp (by intro a b hab; simpa [leCnt] using hab)
  constructor
  · exact bucketTail_pairwise_cnt cnt t gs
  · intro a ha b hb
    have hmsmem : a ∈ gs.filter (fun g => t < cnt g) := List.mem_mergeSort.mp ha
    have hca : t < cnt a := by simpa using (List.mem_filter.mp hmsmem).2
    have hcb : cnt b ≤ t := mem_bucketTail_le cnt t gs hb
    omega

private theorem filter_mergeSort_eq {β : Type} (cnt : β → Nat) (gs : List β) (n : Nat) :
    ((gs.mergeSort (leCnt cnt)).filter fun g => cnt g = n) = (gs.filter fun g => cnt g = n) := by
  let p : β → Bool := fun g => decide (cnt g = n)
  have hpair : (gs.filter p).Pairwise (fun a b => leCnt cnt a b = true) := by
    apply (filter_cnt_pairwise cnt gs n).imp
    intro a b hab
    simp [leCnt, hab]
  have hsub : List.Sublist (gs.filter p) (gs.mergeSort (leCnt cnt)) :=
    List.sublist_mergeSort (leCnt_trans cnt) (leCnt_total cnt) hpair List.filter_sublist
  have hsub' := hsub.filter p
  have hidem : (gs.filter p).filter p = gs.filter p := by
    induction gs with
    | nil => rfl
    | cons g gs ih => by_cases hg : p g <;> simp [hg, ih]
  rw [hidem] at hsub'
  have hlen : ((gs.mergeSort (leCnt cnt)).filter p).length = (gs.filter p).length :=
    ((List.mergeSort_perm gs (leCnt cnt)).filter p).length_eq
  exact (hsub'.eq_of_length hlen.symm).symm

private theorem filter_bucketTail_eq {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β)
    (n : Nat) (hn : n ≤ t) :
    (bucketTail cnt t gs).filter (fun g => cnt g = n) = gs.filter (fun g => cnt g = n) := by
  induction t with
  | zero =>
      have hn0 : n = 0 := by omega
      subst n
      unfold bucketTail
      have hrange : (List.range (0 + 1)).reverse = [0] := by decide
      rw [hrange]
      simp
  | succ t ih =>
      unfold bucketTail at ih ⊢
      rw [show List.range (t + 1 + 1) = List.range (t + 1) ++ [t + 1] by
        rw [show t + 1 + 1 = (t + 1).succ by omega, List.range_succ]]
      rw [List.reverse_append, List.reverse_singleton, List.flatMap_append, List.filter_append]
      simp only [List.flatMap_cons, List.flatMap_nil, List.append_nil]
      by_cases hnEq : n = t + 1
      · subst n
        have hempty : (bucketTail cnt t gs).filter (fun g => cnt g = t + 1) = [] := by
          apply (List.eq_nil_iff_forall_not_mem).2
          intro a ha
          have hm := List.mem_filter.mp ha
          have hca : cnt a = t + 1 := by simpa using hm.2
          have hle := mem_bucketTail_le cnt t gs hm.1
          omega
        have hempty' : (List.flatMap (fun n => gs.filter fun g => cnt g = n)
            (List.range (t + 1)).reverse).filter (fun g => cnt g = t + 1) = [] := by
          simpa [bucketTail] using hempty
        rw [hempty', List.append_nil]
        simp
      · have hnlt : n ≤ t := by omega
        have hempty : (gs.filter fun g : β => cnt g = t + 1).filter (fun g => cnt g = n) = [] := by
          apply (List.eq_nil_iff_forall_not_mem).2
          intro a ha
          have hm := List.mem_filter.mp ha
          have hm2 : cnt a = t + 1 := by simpa using (List.mem_filter.mp hm.1).2
          have hm3 : cnt a = n := by simpa using hm.2
          omega
        rw [hempty, List.nil_append]
        exact ih hnlt

private theorem filter_sortedByBuckets_eq {β : Type} (cnt : β → Nat) (t : Nat) (gs : List β)
    (n : Nat) :
    (sortedByBuckets cnt t gs).filter (fun g => cnt g = n) = gs.filter (fun g => cnt g = n) := by
  unfold sortedByBuckets
  rw [List.filter_append]
  by_cases hn : n ≤ t
  · have hhead : ((gs.filter fun g => t < cnt g).mergeSort (leCnt cnt)).filter
        (fun g => cnt g = n) = [] := by
      apply (List.eq_nil_iff_forall_not_mem).2
      intro a ha
      have hm := List.mem_filter.mp ha
      have hmemorig : a ∈ gs.filter (fun g => t < cnt g) := List.mem_mergeSort.mp hm.1
      have hca : t < cnt a := by simpa using (List.mem_filter.mp hmemorig).2
      have hcan : cnt a = n := by simpa using hm.2
      omega
    rw [hhead, List.nil_append]
    exact filter_bucketTail_eq cnt t gs n hn
  · have htail : (bucketTail cnt t gs).filter (fun g => cnt g = n) = [] := by
      apply (List.eq_nil_iff_forall_not_mem).2
      intro a ha
      have hm := List.mem_filter.mp ha
      have hca : cnt a = n := by simpa using hm.2
      have hle := mem_bucketTail_le cnt t gs hm.1
      omega
    rw [htail, List.append_nil]
    have hbig : (gs.filter fun g => t < cnt g).filter (fun g => cnt g = n) =
        gs.filter (fun g => cnt g = n) := by
      have hpEq : (fun g : β => decide (cnt g = n) && decide (t < cnt g)) =
          (fun g : β => decide (cnt g = n)) := by
        funext g
        by_cases hgn : cnt g = n
        · have hgt : t < cnt g := by omega
          have hgt' : t < n := by omega
          simp [hgn, hgt, hgt']
        · simp [hgn]
      rw [List.filter_filter]
      exact congrArg (fun f : β → Bool => gs.filter f) hpEq
    rw [filter_mergeSort_eq cnt (gs.filter fun g => t < cnt g) n, hbig]

private theorem sorted_perm_eq_of_filters {β : Type} (cnt : β → Nat) {l₁ l₂ : List β}
    (hperm : l₁.Perm l₂)
    (hs1 : l₁.Pairwise (fun a b => cnt b ≤ cnt a))
    (hs2 : l₂.Pairwise (fun a b => cnt b ≤ cnt a))
    (hf : ∀ n, l₁.filter (fun a => cnt a = n) = l₂.filter (fun a => cnt a = n)) :
    l₁ = l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      exact (List.perm_nil.mp hperm.symm).symm
  | cons a rest ih =>
      cases l₂ with
      | nil => simp at hperm
      | cons b bs =>
          have haMem : a ∈ b :: bs := hperm.mem_iff.mp (by simp)
          have hbMem : b ∈ a :: rest := hperm.mem_iff.mpr (by simp)
          have hcnt_le : cnt b ≤ cnt a := by
            by_cases hba : b = a
            · have hc : cnt b = cnt a := congrArg cnt hba
              omega
            · have hbRest : b ∈ rest := by simpa [hba] using hbMem
              exact List.rel_of_pairwise_cons hs1 hbRest
          have hcnt_ge : cnt a ≤ cnt b := by
            by_cases hab : a = b
            · have hc : cnt a = cnt b := congrArg cnt hab
              omega
            · have haBs : a ∈ bs := by simpa [hab] using haMem
              exact List.rel_of_pairwise_cons hs2 haBs
          have hcnt_eq : cnt b = cnt a := by omega
          have hhead1 : ((a :: rest).filter fun x => cnt x = cnt a).head? = some a := by simp
          have hhead2 : ((b :: bs).filter fun x => cnt x = cnt a).head? = some b := by
            simp [hcnt_eq]
          rw [← hf (cnt a)] at hhead2
          rw [hhead1] at hhead2
          have habEq : a = b := Option.some.inj hhead2
          cases habEq
          have hperm' : rest.Perm bs := List.Perm.cons_inv hperm
          have hf' : ∀ n, rest.filter (fun x => cnt x = n) = bs.filter (fun x => cnt x = n) := by
            intro n
            by_cases hn : cnt a = n
            · subst n
              simpa using hf (cnt a)
            · have hh := hf n
              simpa [hn] using hh
          exact congrArg (fun l => a :: l)
            (ih hperm' hs1.tail hs2.tail hf')

private theorem sum_filter_le {β : Type} (f : β → Nat) (p : β → Bool) (gs : List β) :
    ((gs.filter p).map f).sum ≤ (gs.map f).sum := by
  induction gs with
  | nil => simp
  | cons g gs ih =>
      by_cases hg : p g
      · simp [hg]
        omega
      · simp [hg]
        omega

private theorem sum_ge_length_mul {β : Type} (f : β → Nat) (t : Nat) (l : List β)
    (hall : ∀ a ∈ l, t < f a) : l.length * (t + 1) ≤ (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have ha := hall a (by simp)
      have hrest := ih (by intro b hb; exact hall b (by simp [hb]))
      simp only [List.length_cons, List.map_cons, List.sum_cons]
      change (l.length + 1) * (t + 1) ≤ f a + (l.map f).sum
      rw [Nat.add_mul, Nat.one_mul]
      omega

/-! ## H3: ThreeBaseSums -/

private theorem summarize_map_add_base (vs : List (Option Int)) (base : Int) :
    summarize (vs.map fun v => v.map (base + ·)) =
      let st := summarize vs
      { st with sum := st.sum + base * (st.nn : Int) } := by
  induction vs with
  | nil => simp [summarize, zero, weighted]
  | cons v vs ih =>
      cases v with
      | none =>
          rw [List.map_cons, Option.map_none, summarize_cons, ih]
          simp [summarize_cons, weighted, plus, zero]
      | some c =>
          rw [List.map_cons, Option.map_some, summarize_cons, ih]
          simp only [summarize_cons, weighted, plus, zero]
          dsimp
          rw [TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown.PreAgg.mk.injEq]
          constructor
          · omega
          constructor
          · omega
          · have hmul : base * ((1 : Int) + (summarize vs).nn) =
                base + base * (summarize vs).nn := by
              rw [Int.mul_add, Int.mul_one]
            rw [hmul]
            omega

private theorem foldl_map_plus {α : Type} (xs : List α) (f : α → Totals) (acc : Totals) :
    (xs.map f).foldl plus acc = xs.foldl (fun a x => plus a (f x)) acc := by
  induction xs generalizing acc with
  | nil => rfl
  | cons x xs ih =>
      change (xs.map f).foldl plus (plus acc (f x)) =
        xs.foldl (fun a y => plus a (f y)) (plus acc (f x))
      exact ih (acc := plus acc (f x))

theorem ThreeBaseSums_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o.ThreeBaseSums.Goal := by
  intro blocks g j
  let rowsOf (b : Block) : List (Option Int) :=
    (b.2.filter fun r => r.1 == g).map fun r => r.2 j
  let decoded (b : Block) : List (Option Int) :=
    let (base, rows) := b
    (rows.filter fun r => r.1 == g).map fun r => (r.2 j).map (base j + ·)
  have hdec_eq (b : Block) : decoded b = (rowsOf b).map (fun v => v.map (b.1 j + ·)) := by
    dsimp [decoded, rowsOf]
    cases b
    simp [List.map_map, Function.comp_apply]
  have hdecode (b : Block) : decodeTotal b g j = summarize (decoded b) := by
    rw [hdec_eq]
    unfold decodeTotal
    dsimp [rowsOf]
    exact (summarize_map_add_base (rowsOf b) (b.1 j)).symm
  have hfold : ∀ (bs : List Block) (acc : Totals),
      bs.foldl (fun a b => plus a (decodeTotal b g j)) acc =
        bs.foldl (fun a b => plus a (summarize (decoded b))) acc := by
    intro bs acc
    induction bs generalizing acc with
    | nil => rfl
    | cons b bs ih =>
        simp only [List.foldl_cons]
        rw [hdecode]
        exact ih (acc := plus acc (summarize (decoded b)))
  have hflat : ∀ (bs : List Block),
      bs.foldl (fun a b => plus a (summarize (decoded b))) zero =
        summarize (bs.flatMap decoded) := by
    intro bs
    induction bs with
    | nil => simp [summarize, zero]
    | cons b bs ih =>
        simp only [List.foldl_cons, List.flatMap_cons]
        rw [summarize_append, plus_zero_left]
        rw [show bs.foldl (fun a b' => plus a (summarize (decoded b'))) (summarize (decoded b)) =
            plus (summarize (decoded b)) (bs.foldl (fun a b' => plus a (summarize (decoded b'))) zero) by
          exact foldl_add_sum bs (fun b' => summarize (decoded b')) (summarize (decoded b))]
        rw [ih]
  rw [foldl_map_plus, hfold, hflat]

/-! ## H4: EpochAverage -/

private def epochVals (rs : List Input) (k : Nat × Nat) : List (Option Int) :=
  (rs.filter fun r => r.1 == k && r.2.1 < 4).map fun r => r.2.2

private theorem epochVals_append (ys : List Input) (r : Input) (k : Nat × Nat) :
    epochVals (ys ++ [r]) k =
      if (r.1 == k && r.2.1 < 4) = true then epochVals ys k ++ [r.2.2] else epochVals ys k := by
  unfold epochVals
  rw [List.filter_append]
  by_cases hk : r.1 == k
  · by_cases hlt : r.2.1 < 4
    · simp [hk, hlt]
    · simp [hk, hlt]
  · simp [hk]

private theorem summarize_singleton (v : Option Int) : summarize [v] = weighted 1 v := by
  simp [summarize, zero, weighted, plus]

private def accumStep (epoch : Nat) (s : Slots × List (Nat × Nat)) (r : Input) :=
  match r with
  | (k, id3, v) =>
    if id3 ≥ 4 then s else
      match s.1[k]? with
      | some (stamp, t) =>
          if stamp == epoch then (s.1.insert k (epoch, plus t (weighted 1 v)), s.2)
          else (s.1.insert k (epoch, weighted 1 v), k :: s.2)
      | none => (s.1.insert k (epoch, weighted 1 v), k :: s.2)

private theorem accumulate_eq_fold (old : Slots) (epoch : Nat) (rs : List Input) :
    accumulate old epoch rs = rs.foldl (accumStep epoch) (old, []) := by
  rfl

private theorem accumulate_inv (old : Slots) (epoch : Nat)
    (hold : ∀ entry ∈ old.toList, entry.2.1 ≠ epoch) :
    ∀ (rs ys : List Input) (h : Slots) (keys : List (Nat × Nat)),
      keys.Nodup →
      (∀ q, q ∈ keys ↔ epochVals ys q ≠ []) →
      (∀ q, q ∈ keys → h[q]? = some (epoch, summarize (epochVals ys q))) →
      (∀ q stamp tot, h[q]? = some (stamp, tot) → stamp == epoch → q ∈ keys) →
      let s := rs.foldl (accumStep epoch) (h, keys)
      s.2.Nodup ∧
      (∀ q, q ∈ s.2 ↔ epochVals (ys ++ rs) q ≠ []) ∧
      (∀ q, q ∈ s.2 → s.1[q]? = some (epoch, summarize (epochVals (ys ++ rs) q))) ∧
      (∀ q stamp tot, s.1[q]? = some (stamp, tot) → stamp == epoch → q ∈ s.2) := by
  intro rs
  induction rs with
  | nil =>
      intro ys h keys hA hB hC hD
      simpa using And.intro hA (And.intro hB (And.intro hC hD))
  | cons r rs ih =>
      intro ys h keys hA hB hC hD
      simp only [List.foldl_cons]
      rcases r with ⟨kr, id3, v⟩
      by_cases hge : id3 ≥ 4
      · have hvals : ∀ q, epochVals (ys ++ [(kr, id3, v)]) q = epochVals ys q := by
          intro q
          rw [epochVals_append]
          simp [hge]
        simpa [accumStep, hge, List.append_assoc] using
          ih (ys ++ [(kr, id3, v)]) h keys hA
            (by intro q; simpa [hvals q] using hB q)
            (by intro q hq; simpa [hvals q] using hC q hq)
            (by intro q st t hq he; exact hD q st t hq he)
      · have hlt : id3 < 4 := by omega
        by_cases hnone : h[kr]? = none
        · have hknot : kr ∉ keys := by
            intro hk
            have hc := hC kr hk
            rw [hnone] at hc
            cases hc
          have hvempty : epochVals ys kr = [] := by
            cases hve : epochVals ys kr with
            | nil => rfl
            | cons x xs =>
                exact False.elim (hknot ((hB kr).mpr (by simp [hve])))
          have hA' : (kr :: keys).Nodup := by simp [hknot, hA]
          have hB' : ∀ q, q ∈ kr :: keys ↔ epochVals (ys ++ [(kr, id3, v)]) q ≠ [] := by
            intro q
            by_cases hq : kr == q
            · have hqeq : kr = q := by simpa using hq
              subst q
              simp [epochVals_append, hlt]
            · have hqne : kr ≠ q := by
                intro heq
                apply hq
                simp [heq]
              constructor
              · intro hqin
                have hqin' : q ∈ keys := by
                  simp only [List.mem_cons] at hqin
                  rcases hqin with hqeq | hqin'
                  · exact False.elim (hqne hqeq.symm)
                  · exact hqin'
                have hne := (hB q).mp hqin'
                simpa [epochVals_append, hlt, hq] using hne
              · intro hne
                have hne' : epochVals ys q ≠ [] := by
                  simpa [epochVals_append, hlt, hq] using hne
                exact List.mem_cons_of_mem kr ((hB q).mpr hne')
          have hC' : ∀ q, q ∈ kr :: keys →
              (h.insert kr (epoch, weighted 1 v))[q]? =
                some (epoch, summarize (epochVals (ys ++ [(kr, id3, v)]) q)) := by
            intro q hq
            simp only [List.mem_cons] at hq
            rcases hq with hq | hq
            · subst q
              have hins : (h.insert kr (epoch, weighted 1 v))[kr]? = some (epoch, weighted 1 v) := by
                simp [Std.HashMap.getElem?_insert]
              rw [hins]
              rw [epochVals_append]
              simp [hlt, hvempty, summarize_singleton]
            · have hqne : kr ≠ q := by
                intro heq
                exact hknot (by simpa [heq] using hq)
              have hins : (h.insert kr (epoch, weighted 1 v))[q]? = h[q]? := by
                simp [Std.HashMap.getElem?_insert, hqne]
              rw [hins]
              rw [epochVals_append]
              simp [hlt, hqne]
              exact hC q hq
          have hD' : ∀ q stamp tot, (h.insert kr (epoch, weighted 1 v))[q]? = some (stamp, tot) →
              stamp == epoch → q ∈ kr :: keys := by
            intro q stamp tot hq hs
            by_cases hkeq : kr == q
            · have hqeq : kr = q := by simpa using hkeq
              subst q
              simp
            · have hins : (h.insert kr (epoch, weighted 1 v))[q]? = h[q]? := by
                simp [Std.HashMap.getElem?_insert, hkeq]
              rw [hins] at hq
              exact List.mem_cons_of_mem kr (hD q stamp tot hq hs)
          simpa [accumStep, hge, hnone] using
            ih (ys ++ [(kr, id3, v)]) (h.insert kr (epoch, weighted 1 v)) (kr :: keys)
              hA' hB' hC' hD'
        · have hsome : ∃ stamp tot, h[kr]? = some (stamp, tot) := by
            cases hh : h[kr]? with
            | none =>
                simp [hh] at hnone
            | some v => exact ⟨v.1, v.2, by simpa using hh⟩
          rcases hsome with ⟨stamp, t, hget⟩
          by_cases hstamp : stamp == epoch
          · have hs_eq : stamp = epoch := by simpa using hstamp
            have hk_in : kr ∈ keys := hD kr stamp t hget hstamp
            have hc_eq : t = summarize (epochVals ys kr) := by
              have hc := hC kr hk_in
              rw [hget] at hc
              simp [hs_eq] at hc
              exact hc
            have hA' : keys.Nodup := hA
            have hB' : ∀ q, q ∈ keys ↔ epochVals (ys ++ [(kr, id3, v)]) q ≠ [] := by
              intro q
              by_cases hq : kr == q
              · have hqeq : kr = q := by simpa using hq
                subst q
                simp [hk_in, epochVals_append, hlt]
              · simp [epochVals_append, hlt, hq, hB]
            have hC' : ∀ q, q ∈ keys →
                (h.insert kr (epoch, plus t (weighted 1 v)))[q]? =
                  some (epoch, summarize (epochVals (ys ++ [(kr, id3, v)]) q)) := by
              intro q hq
              by_cases hkeq : kr == q
              · have hqeq : kr = q := by simpa using hkeq
                subst q
                have hins : (h.insert kr (epoch, plus t (weighted 1 v)))[kr]? =
                    some (epoch, plus t (weighted 1 v)) := by
                  simp [Std.HashMap.getElem?_insert]
                rw [hins, hc_eq, epochVals_append]
                simp [hlt, summarize_append, summarize_singleton]
              · have hins : (h.insert kr (epoch, plus t (weighted 1 v)))[q]? = h[q]? := by
                  simp [Std.HashMap.getElem?_insert, hkeq]
                rw [hins]
                rw [epochVals_append]
                simp [hlt, hkeq]
                exact hC q hq
            have hD' : ∀ q stamp' tot, (h.insert kr (epoch, plus t (weighted 1 v)))[q]? = some (stamp', tot) →
                stamp' == epoch → q ∈ keys := by
              intro q stamp' tot hq hs
              by_cases hkeq : kr == q
              · have hqeq : kr = q := by simpa using hkeq
                subst q
                exact hk_in
              · have hins : (h.insert kr (epoch, plus t (weighted 1 v)))[q]? = h[q]? := by
                  simp [Std.HashMap.getElem?_insert, hkeq]
                rw [hins] at hq
                exact hD q stamp' tot hq hs
            simpa [accumStep, hge, hget, hstamp] using
              ih (ys ++ [(kr, id3, v)]) (h.insert kr (epoch, plus t (weighted 1 v))) keys
                hA' hB' hC' hD'
          · have hknot : kr ∉ keys := by
              intro hk
              have hc := hC kr hk
              rw [hget] at hc
              have heq : some (stamp, t) = some (epoch, summarize (epochVals ys kr)) := hc
              simp at heq
              have hseq : stamp = epoch := heq.1
              exact hstamp (by simp [hseq])
            have hvempty : epochVals ys kr = [] := by
              cases hve : epochVals ys kr with
              | nil => rfl
              | cons x xs =>
                  exact False.elim (hknot ((hB kr).mpr (by simp [hve])))
            have hA' : (kr :: keys).Nodup := by simp [hknot, hA]
            have hB' : ∀ q, q ∈ kr :: keys ↔ epochVals (ys ++ [(kr, id3, v)]) q ≠ [] := by
              intro q
              if hq : kr == q then
                have hqeq : kr = q := by simpa using hq
                subst q
                simp [epochVals_append, hlt]
              else
                have hqne : kr ≠ q := by
                  intro heq
                  apply hq
                  simp [heq]
                constructor
                · intro hqin
                  have hqin' : q ∈ keys := by
                    simp only [List.mem_cons] at hqin
                    rcases hqin with hqeq | hqin'
                    · exact False.elim (hqne hqeq.symm)
                    · exact hqin'
                  have hne := (hB q).mp hqin'
                  simpa [epochVals_append, hlt, hq] using hne
                · intro hne
                  have hne' : epochVals ys q ≠ [] := by
                    simpa [epochVals_append, hlt, hq] using hne
                  exact List.mem_cons_of_mem kr ((hB q).mpr hne')
            have hC' : ∀ q, q ∈ kr :: keys →
                (h.insert kr (epoch, weighted 1 v))[q]? =
                  some (epoch, summarize (epochVals (ys ++ [(kr, id3, v)]) q)) := by
              intro q hq
              simp only [List.mem_cons] at hq
              rcases hq with hq | hq
              · subst q
                have hins : (h.insert kr (epoch, weighted 1 v))[kr]? = some (epoch, weighted 1 v) := by
                  simp [Std.HashMap.getElem?_insert]
                rw [hins]
                rw [epochVals_append]
                simp [hlt, hvempty, summarize_singleton]
              · have hqne : kr ≠ q := by
                  intro heq
                  exact hknot (by simpa [heq] using hq)
                have hins : (h.insert kr (epoch, weighted 1 v))[q]? = h[q]? := by
                  simp [Std.HashMap.getElem?_insert, hqne]
                rw [hins]
                rw [epochVals_append]
                simp [hlt, hqne]
                exact hC q hq
            have hD' : ∀ q stamp' tot, (h.insert kr (epoch, weighted 1 v))[q]? = some (stamp', tot) →
                stamp' == epoch → q ∈ kr :: keys := by
              intro q stamp' tot hq hs
              by_cases hkeq : kr == q
              · have hqeq : kr = q := by simpa using hkeq
                subst q
                simp
              · have hins : (h.insert kr (epoch, weighted 1 v))[q]? = h[q]? := by
                  simp [Std.HashMap.getElem?_insert, hkeq]
                rw [hins] at hq
                exact List.mem_cons_of_mem kr (hD q stamp' tot hq hs)
            simpa [accumStep, hge, hget, hstamp] using
              ih (ys ++ [(kr, id3, v)]) (h.insert kr (epoch, weighted 1 v)) (kr :: keys)
                hA' hB' hC' hD'

theorem EpochAverage_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o.EpochAverage.Goal := by
  intro old epoch rs k hold
  rw [accumulate_eq_fold]
  have hi := accumulate_inv old epoch hold rs [] old []
    (by simp)
    (by intro q; simp [epochVals])
    (by intro q hq; simp at hq)
    (by
      intro q stamp tot hq hs
      have hent : (q, (stamp, tot)) ∈ old.toList :=
        Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hq
      have hne := hold (q, (stamp, tot)) hent
      have hseq : stamp = epoch := by simpa using hs
      exact (hne hseq).elim)
  dsimp at hi
  rcases hi with ⟨hnodup, hmem, hget, hstamps⟩
  constructor
  · exact hnodup
  constructor
  · have hv := hmem k
    simpa [epochVals] using hv
  · let s := List.foldl (accumStep epoch) (old, []) rs
    by_cases hkin : k ∈ s.2
    · have hcontain : s.2.contains k = true := List.contains_iff_mem.mpr hkin
      have hgs : s.1[k]? = some (epoch, summarize (epochVals rs k)) := by
        simpa [s] using hget k hkin
      have hgetD : (s.1.getD k (epoch, zero)).2 = summarize (epochVals rs k) := by
        rw [Std.HashMap.getD_eq_getD_getElem?]
        rw [hgs]
        simp
      simpa [s, hcontain, hkin, epochVals] using hgetD
    · have hcontain : s.2.contains k = false := by
        apply Bool.eq_false_of_ne_true
        intro hc
        exact hkin (List.contains_iff_mem.mp hc)
      have hv : epochVals rs k = [] := by
        cases hve : epochVals rs k with
        | nil => rfl
        | cons x xs =>
            exact False.elim (hkin ((hmem k).mpr (by simp [hve])))
      change (if s.2.contains k then (s.1.getD k (epoch, zero)).2 else zero) = summarize (epochVals rs k)
      simp [hkin, hv, summarize, zero]
/-! ## H6: InRangeSynopsis -/

private theorem summarize_char (vs : List (Option Int)) :
    summarize vs =
      ⟨vs.length, (vs.filterMap id).length, (vs.map fun v => v.getD 0).sum⟩ := by
  induction vs with
  | nil => simp [summarize, zero]
  | cons v vs ih =>
      rw [summarize_cons, ih]
      cases v <;> simp [weighted, plus] <;> omega

private def inRangeVals (rows : List Sample) (a : Nat) (lo hi : Int) : List (Option Int) :=
  (rows.filter fun (x, y, _) => x == a && lo ≤ y && y ≤ hi).map fun r => r.2.2

private theorem tally_split (rows : List Sample) (a : Nat) (lo hi : Int) (hlohi : lo ≤ hi) :
    summarize ((rows.filter fun (x, y, _) => x == a && y < hi + 1).map fun r => r.2.2) =
      plus (summarize ((rows.filter fun (x, y, _) => x == a && y < lo).map fun r => r.2.2))
        (summarize (inRangeVals rows a lo hi)) := by
  induction rows with
  | nil => simp [inRangeVals, summarize, zero, plus]
  | cons r rows ih =>
      rcases r with ⟨x, y, v⟩
      by_cases hxa : x == a
      · by_cases hlt : y < lo
        · have hhi : y < hi + 1 := by omega
          have hloNot : ¬lo ≤ y := by omega
          simp [hxa, hlt, hhi, hloNot, inRangeVals, summarize_cons] at ih ⊢
          rw [ih]
          simp [plus_assoc]
        · by_cases hle : y ≤ hi
          · have hhi : y < hi + 1 := by omega
            have hlo' : lo ≤ y := by omega
            simp [hxa, hlt, hhi, hle, hlo', inRangeVals, summarize_cons] at ih ⊢
            rw [ih]
            simp [plus_assoc, plus_comm, plus_left_comm]
          · have hhi : ¬y < hi + 1 := by omega
            simp [hxa, hlt, hhi, hle, inRangeVals, ih]
      · simp [hxa, inRangeVals, ih]

private theorem prefixDiff (rows : List Sample) (a : Nat) (lo hi : Int) (hlohi : lo ≤ hi) :
    let h := prefixAt rows a (hi + 1)
    let l := prefixAt rows a lo
    ⟨h.cnt - l.cnt, h.nn - l.nn, h.sum - l.sum⟩ = summarize (inRangeVals rows a lo hi) := by
  unfold prefixAt
  have hs := tally_split rows a lo hi hlohi
  have hsumHi := summarize_char ((rows.filter fun (x, y, _) => x == a && y < hi + 1).map fun r => r.2.2)
  have hsumLo := summarize_char ((rows.filter fun (x, y, _) => x == a && y < lo).map fun r => r.2.2)
  have hsumRange := summarize_char (inRangeVals rows a lo hi)
  rw [hsumHi, hsumLo, hsumRange] at hs
  simp [plus] at hs
  have hc := hs.1
  have hn := hs.2.1
  have hsu := hs.2.2
  rw [summarize_char, summarize_char, summarize_char]
  dsimp
  simp only [List.length_map, List.map_map, Function.comp_apply, Function.comp_def, List.filterMap_map, id_eq] at hc hn hsu ⊢
  rw [TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown.PreAgg.mk.injEq]
  constructor
  · omega
  constructor
  · omega
  · omega

private theorem eraseDups_nodup : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: l => by
      rw [List.eraseDups_cons]
      exact List.nodup_cons.mpr ⟨by
        intro hm
        have hm' : a ∈ l.filter (fun b => !(b == a)) := List.mem_eraseDups.mp hm
        simp at hm', eraseDups_nodup (l.filter fun b => !(b == a))⟩
termination_by l => l.length
decreasing_by exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem summarize_perm {l₁ l₂ : List (Option Int)} (h : l₁.Perm l₂) :
    summarize l₁ = summarize l₂ := by
  induction h with
  | nil => rfl
  | cons a h ih =>
      simp [summarize_cons, ih]
  | swap a b l =>
      simp [summarize_cons, plus_assoc, plus_comm, plus_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem cons_append_perm {α : Type} (a : α) : ∀ (l₁ l₂ : List α),
    (a :: (l₁ ++ l₂)).Perm (l₁ ++ (a :: l₂))
  | [], l₂ => by simp
  | b :: l₁, l₂ =>
      (List.Perm.swap a b (l₁ ++ l₂)).symm.trans ((cons_append_perm a l₁ l₂).cons b)

private theorem filter_or_perm (p q : Sample → Bool) (rows : List Sample)
    (hd : ∀ r ∈ rows, ¬(p r = true ∧ q r = true)) :
    (rows.filter fun r => p r || q r).Perm ((rows.filter p) ++ (rows.filter q)) := by
  induction rows with
  | nil => simp
  | cons r rows ih =>
      have ih' := ih (by intro z hz; exact hd z (by simp [hz]))
      by_cases hp : p r
      · by_cases hq : q r
        · exact False.elim (hd r (by simp) ⟨hp, hq⟩)
        · simpa [hp, hq] using ih'.cons r
      · by_cases hq : q r
        · simpa [hp, hq] using (ih'.cons r).trans (cons_append_perm r (rows.filter p) (rows.filter q))
        · simpa [hp, hq] using ih'

private theorem summarize_contains_cons_range (rows : List Sample) (d : Nat) (ds : List Nat)
    (hdnot : d ∉ ds) (lo hi : Int) :
    summarize ((rows.filter fun (x, y, _) => (d :: ds).contains x && lo ≤ y && y ≤ hi).map fun r => r.2.2) =
      plus
        (summarize ((rows.filter fun (x, y, _) => x == d && lo ≤ y && y ≤ hi).map fun r => r.2.2))
        (summarize ((rows.filter fun (x, y, _) => ds.contains x && lo ≤ y && y ≤ hi).map fun r => r.2.2)) := by
  let p : Sample → Bool := fun r => r.1 == d && lo ≤ r.2.1 && r.2.1 ≤ hi
  let q : Sample → Bool := fun r => ds.contains r.1 && lo ≤ r.2.1 && r.2.1 ≤ hi
  have hpred : (fun r : Sample => (d :: ds).contains r.1 && lo ≤ r.2.1 && r.2.1 ≤ hi) =
      (fun r : Sample => p r || q r) := by
    funext r
    by_cases hr : r.1 == d
    · have hrEq : r.1 = d := by simpa using hr
      have hmem : r.1 ∈ d :: ds := by
        simp only [List.mem_cons]
        exact Or.inl hrEq
      have hc : (d :: ds).contains r.1 = true := List.contains_iff_mem.mpr hmem
      have hdc : ds.contains r.1 = false := by
        by_cases hx : ds.contains r.1
        · exact False.elim (hdnot (by simpa [hrEq] using (List.contains_iff_mem.mp hx)))
        · exact Bool.eq_false_of_ne_true hx
      simp [p, q, hrEq, hdnot]
    · have hrNe : r.1 ≠ d := by
        intro heq
        exact hr (by simp [heq])
      have hc : (d :: ds).contains r.1 = ds.contains r.1 := by
        by_cases hx : ds.contains r.1 <;> simp [hrNe, hx]
      simp [p, q, hrNe, hc]
  have hdisj : ∀ r ∈ rows, ¬(p r = true ∧ q r = true) := by
    intro r hr h
    have hp1 := h.1
    have hq1 := h.2
    simp [p] at hp1
    simp [q] at hq1
    have hrd : r.1 = d := hp1.1.1
    have hrds : r.1 ∈ ds := hq1.1.1
    exact hdnot (by simpa [hrd] using hrds)
  have hperm := filter_or_perm p q rows hdisj
  have hfeq : (rows.filter fun (x, y, _) => (d :: ds).contains x && lo ≤ y && y ≤ hi) =
      rows.filter (fun r => p r || q r) := by
    apply List.filter_congr
    intro r hr
    exact congrFun hpred r
  rw [hfeq]
  have hmap : (rows.filter fun r => p r || q r).map (fun r => r.2.2) |>.Perm
      (((rows.filter p).map fun r => r.2.2) ++ ((rows.filter q).map fun r => r.2.2)) := by
    simpa [List.map_append] using hperm.map (fun r : Sample => r.2.2)
  rw [summarize_perm hmap, summarize_append]

private def inRangeValsAny (rows : List Sample) (ds : List Nat) (lo hi : Int) : List (Option Int) :=
  (rows.filter fun (x, y, _) => ds.contains x && lo ≤ y && y ≤ hi).map fun r => r.2.2

private theorem fold_ids_aux (rows : List Sample) (ds : List Nat)
    (hds : ds.Nodup) (lo hi : Int) (hlohi : lo ≤ hi) :
    ds.foldl (fun total a =>
      let l := prefixAt rows a lo
      let h := prefixAt rows a (hi + 1)
      plus total ⟨h.cnt - l.cnt, h.nn - l.nn, h.sum - l.sum⟩) zero =
      summarize (inRangeValsAny rows ds lo hi) := by
  revert hds
  induction ds with
  | nil =>
      intro hds
      change zero = summarize (List.map (fun r => r.2.2) (List.filter (fun x => false) rows))
      have hf : rows.filter (fun _ : Sample => false) = [] := by
        induction rows with
        | nil => simp
        | cons a as i => simp [i]
      simp [hf, summarize, zero]
  | cons d ds ih =>
      intro hds
      have hdnot : d ∉ ds := (List.nodup_cons.mp hds).1
      have hds' : ds.Nodup := (List.nodup_cons.mp hds).2
      simp only [List.foldl_cons]
      rw [show ds.foldl (fun total a =>
          let l := prefixAt rows a lo
          let h := prefixAt rows a (hi + 1)
          plus total ⟨h.cnt - l.cnt, h.nn - l.nn, h.sum - l.sum⟩)
          (let l := prefixAt rows d lo
           let h := prefixAt rows d (hi + 1)
           plus zero ⟨h.cnt - l.cnt, h.nn - l.nn, h.sum - l.sum⟩) =
        plus (let l := prefixAt rows d lo
              let h := prefixAt rows d (hi + 1)
              ⟨h.cnt - l.cnt, h.nn - l.nn, h.sum - l.sum⟩)
          (ds.foldl (fun total a =>
            let l := prefixAt rows a lo
            let h := prefixAt rows a (hi + 1)
            plus total ⟨h.cnt - l.cnt, h.nn - l.nn, h.sum - l.sum⟩) zero) by
        simpa [plus_zero_left] using
          foldl_add_sum ds (fun a =>
            let l := prefixAt rows a lo
            let h := prefixAt rows a (hi + 1)
            ⟨h.cnt - l.cnt, h.nn - l.nn, h.sum - l.sum⟩)
            (let l := prefixAt rows d lo
             let h := prefixAt rows d (hi + 1)
             ⟨h.cnt - l.cnt, h.nn - l.nn, h.sum - l.sum⟩)]
      rw [ih hds']
      rw [prefixDiff rows d lo hi hlohi]
      unfold inRangeVals inRangeValsAny
      exact (summarize_contains_cons_range rows d ds hdnot lo hi).symm

theorem InRangeSynopsis_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o.InRangeSynopsis.Goal := by
  intro rows ids lo hi hlohi
  unfold prefixSlices
  rw [fold_ids_aux rows ids.eraseDups (eraseDups_nodup ids) lo hi hlohi]
  unfold inRangeValsAny
  have hf : (rows.filter fun (x, y, _) => ids.eraseDups.contains x && lo ≤ y && y ≤ hi) =
      (rows.filter fun (x, y, _) => ids.contains x && lo ≤ y && y ≤ hi) := by
    apply List.filter_congr
    intro r hr
    have hmem : (r.1 ∈ ids.eraseDups) ↔ r.1 ∈ ids := List.mem_eraseDups
    have hc : ids.eraseDups.contains r.1 ↔ r.1 ∈ ids.eraseDups := List.contains_iff_mem
    simp [hc, hmem]
  rw [hf]

theorem CountTail_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o.CountTail.Goal := by
  intro gs t
  let cnt : GroupCount → Nat := fun g => g.2
  constructor
  · have hperm : (sortedByBuckets cnt t gs).Perm (gs.mergeSort (leCnt cnt)) := by
      exact (sortedByBuckets_perm cnt t gs).trans (List.mergeSort_perm gs (leCnt cnt)).symm
    have hs1 : (sortedByBuckets cnt t gs).Pairwise (fun a b => cnt b ≤ cnt a) :=
      sortedByBuckets_pairwise cnt t gs
    have hs2 : (gs.mergeSort (leCnt cnt)).Pairwise (fun a b => cnt b ≤ cnt a) := by
      have h := List.pairwise_mergeSort (leCnt_trans cnt) (leCnt_total cnt) gs
      exact h.imp (by intro a b hab; simpa [leCnt] using hab)
    have hf : ∀ n, (sortedByBuckets cnt t gs).filter (fun g => cnt g = n) =
        (gs.mergeSort (leCnt cnt)).filter (fun g => cnt g = n) := by
      intro n
      rw [filter_sortedByBuckets_eq cnt t gs n, filter_mergeSort_eq cnt gs n]
    have heq := sorted_perm_eq_of_filters cnt hperm hs1 hs2 hf
    rw [hybridOrder_eq]
    change sortedByBuckets (fun g : GroupCount => g.2) t gs =
      gs.mergeSort (leCnt (fun g : GroupCount => g.2))
    exact heq
  · let big := gs.filter (fun g => t < g.2)
    have hle : big.length * (t + 1) ≤ (big.map Prod.snd).sum := by
      apply sum_ge_length_mul (fun g : GroupCount => g.2) t big
      intro a ha
      simpa using (List.mem_filter.mp ha).2
    have hsum : (big.map Prod.snd).sum ≤ (gs.map (fun g : GroupCount => g.2)).sum := by
      exact sum_filter_le (fun g : GroupCount => g.2) (fun g => t < g.2) gs
    exact Nat.le_trans hle hsum

/-! ## H5: SkewHybrid -/

private def pplus (a b : Totals × Totals) : Totals × Totals :=
  (plus a.1 b.1, plus a.2 b.2)

private def h2c (l r : Totals) : Totals × Totals :=
  (⟨l.cnt * r.cnt, l.nn * r.cnt, l.sum * (r.cnt : Int)⟩,
   ⟨r.cnt * l.cnt, r.nn * l.cnt, r.sum * (l.cnt : Int)⟩)

private theorem pplus_assoc (a b c : Totals × Totals) : pplus (pplus a b) c = pplus a (pplus b c) := by
  cases a
  cases b
  cases c
  simp [pplus, plus]
  omega

private theorem pplus_comm (a b : Totals × Totals) : pplus a b = pplus b a := by
  cases a
  cases b
  simp [pplus, plus]
  omega

private theorem pplus_left_comm (a b c : Totals × Totals) : pplus a (pplus b c) = pplus b (pplus a c) := by
  cases a
  cases b
  cases c
  simp [pplus, plus]
  omega

private theorem pplus_zero_left (a : Totals × Totals) : pplus (zero, zero) a = a := by
  cases a
  simp [pplus, plus_zero_left]

private theorem pplus_zero_right (a : Totals × Totals) : pplus a (zero, zero) = a := by
  cases a
  simp [pplus, plus_zero_right]

private theorem h2c_add_left (l₁ l₂ r : Totals) : h2c (plus l₁ l₂) r = pplus (h2c l₁ r) (h2c l₂ r) := by
  cases l₁
  cases l₂
  cases r
  simp [h2c, pplus, plus, Nat.add_mul, Nat.mul_add, Int.add_mul, Int.mul_add,
    Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]

private theorem h2c_zero_left (r : Totals) : h2c zero r = (zero, zero) := by
  cases r
  simp [h2c, zero]

private theorem h2c_zero_right (l : Totals) : h2c l zero = (zero, zero) := by
  cases l
  simp [h2c, zero]

private theorem h2c_eq_zero_of_cnt (l r : Totals) (h : l.cnt = 0 ∨ r.cnt = 0) :
    h2c l r = (zero, zero) := by
  rcases h with h | h
  · cases l with
    | mk lc ln ls =>
      cases r with
      | mk rc rn rs =>
        simp at h
        subst lc
        simp [h2c, zero]
  · cases l with
    | mk lc ln ls =>
      cases r with
      | mk rc rn rs =>
        subst rc
        simp [h2c, zero]

private theorem contribute_getD (h : Groups) (g q : Int) (l r : Totals) :
    (contribute h g l r).getD q (zero, zero) =
      if q == g then pplus (h.getD q (zero, zero)) (h2c l r)
      else h.getD q (zero, zero) := by
  unfold contribute
  by_cases hz : l.cnt == 0 || r.cnt == 0
  · rw [if_pos hz]
    by_cases hq : q == g
    · have hc : l.cnt = 0 ∨ r.cnt = 0 := by
        simpa [Bool.or_eq_true] using hz
      rw [if_pos hq]
      rw [h2c_eq_zero_of_cnt l r hc, pplus_zero_right]
    · rw [if_neg hq]
  · rw [if_neg hz]
    by_cases hq : q == g
    · have hqeq : q = g := by simpa using hq
      subst q
      rw [if_pos (by simp : (g == g) = true)]
      simp [Std.HashMap.getD_insert, pplus, h2c, plus, zero,
        Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc, Int.mul_comm]
    · rw [if_neg hq]
      have hne : g ≠ q := by
        intro heq
        exact hq (by simpa [heq])
      simp [Std.HashMap.getD_insert, hne]

end TinylambOpt.ResearchProofs.Wave8H2o
