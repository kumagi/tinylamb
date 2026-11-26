import TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.

Proved

- `CountTail` — `∀ gs t, hybridOrder gs t = gs.mergeSort (fun a b => b.2 ≤ a.2) ∧
  (gs.filter (fun g => t < g.2)).length * (t+1) ≤ (gs.map Prod.snd).sum`
- `EpochAverage` — `∀ old epoch rs k, (∀ entry ∈ old.toList, entry.2.1 ≠ epoch) → let
  out := accumulate old epoch rs let keys := out.2 let vals := (rs.filter (fun r =>
  r.1 == k && r.2.1 < 4)).map (fun r => r.2.2) keys.Nodup ∧ (k ∈ keys ↔ vals ≠ []) ∧
  (if keys.contains k then (out.1.getD k (epoch,zero)).2 else zero) = summarize vals`
- `HotRegister` — `∀ (xs : List Nat) hot k, let (n,h,updates) := hotCount xs hot (if k
  == hot then n else h.getD k 0) = xs.count k ∧ updates + xs.count hot = xs.length`
- `InRangeSynopsis` — `∀ rows ids lo hi, lo ≤ hi → prefixSlices (prefixAt rows) ids lo
  hi = summarize ((rows.filter (fun (a,b,_) => ids.contains a && lo ≤ b && b ≤
  hi)).map (fun r => r.2.2))`
- `SkewHybrid` — `∀ ls rs g hot, let out := hybridGroups ls rs hot let expected :=
  refGroup ls rs g finish out g = expected ∧ out.contains g = decide (0 <
  expected.1)`
- `ThreeBaseSums` — `∀ (blocks : List Block) g j, (blocks.map (fun b => decodeTotal b g
  j)).foldl plus zero = summarize (blocks.flatMap fun (base,rows) => (rows.filter
  (fun r => r.1 == g)).map fun r => (r.2 j).map (base j + ·))`
-/

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.HotKeyAndEpochAggregates

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates
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

theorem HotRegister_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates.HotRegister.Goal := by
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

theorem ThreeBaseSums_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates.ThreeBaseSums.Goal := by
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

theorem EpochAverage_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates.EpochAverage.Goal := by
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

theorem InRangeSynopsis_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates.InRangeSynopsis.Goal := by
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

theorem CountTail_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates.CountTail.Goal := by
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

private theorem pfold_start (l : List (Totals × Totals)) (st a : Totals × Totals) :
    l.foldl pplus (pplus a st) = pplus a (l.foldl pplus st) := by
  induction l generalizing st a with
  | nil => simp
  | cons x xs ih =>
      change xs.foldl pplus (pplus (pplus a st) x) = pplus a (xs.foldl pplus (pplus st x))
      rw [show pplus (pplus a st) x = pplus a (pplus st x) by rw [pplus_assoc]]
      exact ih (pplus st x) a

private theorem pfold_map_start {α : Type} (l : List α) (f : α → Totals × Totals)
    (st a : Totals × Totals) :
    l.foldl (fun t x => pplus t (f x)) (pplus a st) =
      pplus a (l.foldl (fun t x => pplus t (f x)) st) := by
  induction l generalizing st a with
  | nil => simp
  | cons x xs ih =>
      change xs.foldl (fun t y => pplus t (f y)) (pplus (pplus a st) (f x)) =
        pplus a (xs.foldl (fun t y => pplus t (f y)) (pplus st (f x)))
      rw [show pplus (pplus a st) (f x) = pplus a (pplus st (f x)) by rw [pplus_assoc]]
      exact ih (pplus st (f x)) a

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

private def Valid (t : Totals) : Prop := t.nn ≤ t.cnt ∧ (t.nn = 0 → t.sum = 0)

private theorem valid_zero : Valid zero := by
  simp [Valid, zero]

private theorem valid_weighted (n : Nat) (v : Option Int) : Valid (weighted n v) := by
  cases v with
  | none => simp [Valid, weighted]
  | some x =>
      constructor
      · simp [weighted]
      · intro h
        simp [weighted] at h
        subst n
        simp [weighted]

private theorem valid_plus {a b : Totals} (ha : Valid a) (hb : Valid b) : Valid (plus a b) := by
  rcases a with ⟨ac, an, as⟩
  rcases b with ⟨bc, bn, bs⟩
  simp [Valid, plus] at ha hb ⊢
  constructor
  · omega
  · intro h1
    intro h2
    rw [ha.2 h1, hb.2 h2]
    simp

private theorem valid_summarize (vs : List (Option Int)) : Valid (summarize vs) := by
  induction vs with
  | nil => exact valid_zero
  | cons v vs ih =>
      rw [summarize_cons]
      exact valid_plus (valid_weighted 1 v) ih

private theorem valid_h2c_fst {l r : Totals} (hl : Valid l) (hr : Valid r) :
    Valid (h2c l r).1 := by
  rcases l with ⟨lc, ln, ls⟩
  rcases r with ⟨rc, rn, rs⟩
  simp [Valid, h2c] at hl hr ⊢
  constructor
  · exact Nat.mul_le_mul_right rc hl.1
  · intro h
    have hz : ln = 0 ∨ rc = 0 := Nat.mul_eq_zero.mp h
    rcases hz with hz | hz
    · rw [hl.2 hz]
      simp
    · simp [hz]

private theorem valid_h2c_snd {l r : Totals} (hl : Valid l) (hr : Valid r) :
    Valid (h2c l r).2 := by
  rcases l with ⟨lc, ln, ls⟩
  rcases r with ⟨rc, rn, rs⟩
  simp [Valid, h2c] at hl hr ⊢
  constructor
  · exact Nat.mul_le_mul_right lc hr.1
  · intro h
    have hz : rn = 0 ∨ lc = 0 := Nat.mul_eq_zero.mp h
    rcases hz with hz | hz
    · rw [hr.2 hz]
      simp
    · simp [hz]

private theorem h2c_eq_zero_of_cnt_valid {l r : Totals} (hl : Valid l) (hr : Valid r)
    (h : l.cnt = 0 ∨ r.cnt = 0) : h2c l r = (zero, zero) := by
  rcases l with ⟨lc, ln, ls⟩
  rcases r with ⟨rc, rn, rs⟩
  simp [Valid] at hl hr
  simp only [h2c, zero, Prod.mk.injEq, PreAgg.mk.injEq]
  rcases h with h | h
  · simp at h
    subst lc
    have hln : ln = 0 := by omega
    subst ln
    have hls : ls = 0 := hl.2 rfl
    subst ls
    simp
  · simp at h
    subst rc
    have hrn : rn = 0 := by omega
    subst rn
    have hrs : rs = 0 := hr.2 rfl
    subst rs
    simp

private theorem contribute_getD (h : Groups) (g q : Int) (l r : Totals) :
    (contribute h g l r).getD q (zero, zero) =
      if (q == g) = true && !(l.cnt == 0 || r.cnt == 0) then
        pplus (h.getD q (zero, zero)) (h2c l r)
      else h.getD q (zero, zero) := by
  unfold contribute
  by_cases hz : l.cnt == 0 || r.cnt == 0
  · rw [if_pos hz]
    simp [hz]
  · rw [if_neg hz]
    by_cases hq : q == g
    · have hqeq : q = g := by simpa using hq
      subst q
      simp [Std.HashMap.getD_insert, pplus, h2c, plus, zero, hz,
        Nat.mul_comm, Nat.mul_left_comm, Nat.mul_assoc, Int.mul_comm]
    · have hne : g ≠ q := by
        intro heq
        exact hq (by simpa [heq])
      simp [Std.HashMap.getD_insert, hq, hne, hz]

private theorem contribute_getD_v (h : Groups) (g q : Int) (l r : Totals)
    (hl : Valid l) (hr : Valid r) :
    (contribute h g l r).getD q (zero, zero) =
      if (q == g) = true then pplus (h.getD q (zero, zero)) (h2c l r)
      else h.getD q (zero, zero) := by
  rw [contribute_getD]
  by_cases hq : (q == g) = true
  · by_cases hz : l.cnt == 0 || r.cnt == 0
    · have hc : l.cnt = 0 ∨ r.cnt = 0 := by simpa [Bool.or_eq_true] using hz
      simp [hq, hz, h2c_eq_zero_of_cnt_valid hl hr hc, pplus_zero_right]
    · simp [hq, hz]
  · simp [hq]

private def rTotals (rs : List RRow) (k : Int) : Totals :=
  summarize ((rs.filter fun r => r.k == some k).map fun r => r.v)

private theorem rTotals_append (rs : List RRow) (k : Int) (r : RRow) :
    rTotals (rs ++ [r]) k =
      if r.k == some k then plus (rTotals rs k) (weighted 1 r.v) else rTotals rs k := by
  unfold rTotals
  rw [List.filter_append, List.map_append, summarize_append]
  cases hr : r.k with
  | none => simp [hr, summarize, plus_zero_right]
  | some rk =>
      by_cases h : rk == k
      · have hrk : some rk == some k := by simp [h]
        simp [hr, hrk, summarize_singleton, plus_zero_right]
      · have hrk : ¬(some rk == some k) = true := by simp [h]
        simp [hr, hrk, summarize, plus_zero_right]

private theorem build_inv (rs : List RRow) :
    ∀ (xs ys : List RRow) (h : Std.HashMap Int Totals),
      (∀ k, h.getD k zero = rTotals ys k) →
      ∀ k, (xs.foldl (fun h r => match r.k with
        | none => h
        | some k => h.insert k (plus (h.getD k zero) (weighted 1 r.v))) h).getD k zero =
          rTotals (ys ++ xs) k := by
  intro xs
  induction xs with
  | nil =>
      intro ys h hh k
      simpa using hh k
  | cons r xs ih =>
      intro ys h hh k
      simp only [List.foldl_cons]
      cases hr : r.k with
      | none =>
          have hh' : ∀ k, h.getD k zero = rTotals (ys ++ [r]) k := by
            intro k
            rw [hh k]
            rw [rTotals_append]
            simp [hr]
          simpa [hr, List.append_assoc] using ih (ys ++ [r]) h hh' k
      | some rk =>
          have hh' : ∀ k', (h.insert rk (plus (h.getD rk zero) (weighted 1 r.v))).getD k' zero =
              rTotals (ys ++ [r]) k' := by
            intro k'
            by_cases hk' : rk == k'
            · have heq : rk = k' := by simpa using hk'
              subst k'
              rw [Std.HashMap.getD_insert]
              simp
              rw [hh rk]
              rw [rTotals_append]
              simp [hr, summarize_singleton]
            · rw [Std.HashMap.getD_insert]
              simp [hk']
              rw [hh k']
              rw [rTotals_append]
              simp [hr, hk']
          simpa [hr, List.append_assoc] using
            ih (ys ++ [r]) (h.insert rk (plus (h.getD rk zero) (weighted 1 r.v))) hh' k

private theorem build_final (rs : List RRow) :
    ∀ k, (rs.foldl (fun (h : Std.HashMap Int Totals) (r : RRow) => match r.k with
      | none => h
      | some k => h.insert k (plus (h.getD k zero) (weighted 1 r.v))) {}).getD k zero = rTotals rs k := by
  simpa [rTotals, summarize] using build_inv rs rs [] {} (by intro k; simp [rTotals, summarize])

private def lContrib (rs : List RRow) (l : LRow) : Totals × Totals :=
  match l.k with
  | none => (zero, zero)
  | some k => h2c (weighted 1 l.v) (rTotals rs k)

private def lActive (rs : List RRow) (g : Int) (l : LRow) : Bool :=
  l.g == g && (match l.k with
    | some k => !(rTotals rs k).cnt == 0
    | none => false)

private def naiveFold (rs : List RRow) (h : Groups) (xs : List LRow) : Groups :=
  xs.foldl (fun h l => match l.k with
    | none => h
    | some k => contribute h l.g (weighted 1 l.v) (rTotals rs k)) h

private theorem naiveFold_getD (rs : List RRow) (xs : List LRow) (h : Groups) (g : Int) :
    (naiveFold rs h xs).getD g (zero, zero) =
      pplus (h.getD g (zero, zero))
        ((xs.filter (lActive rs g)).foldl (fun t l => pplus t (lContrib rs l)) (zero, zero)) := by
  induction xs generalizing h with
  | nil => simp [naiveFold, lActive, pplus_zero_right]
  | cons l xs ih =>
      simp only [naiveFold, List.foldl_cons]
      unfold naiveFold at ih
      cases hk : l.k with
      | none =>
          have hact : lActive rs g l = false := by simp [lActive, hk]
          simp [hk, hact] at ih ⊢
          exact ih h
      | some k =>
          simp [hk]
          by_cases hg : l.g == g
          · have hgeq : l.g = g := by simpa using hg
            by_cases hcnt : (rTotals rs k).cnt == 0
            · have hact : lActive rs g l = false := by simp [lActive, hk, hg, hcnt]
              have hskip : contribute h l.g (weighted 1 l.v) (rTotals rs k) = h := by
                unfold contribute
                simp [hk, hcnt]
              rw [hskip]
              simp [hact] at ih ⊢
              exact ih h
            · have hact : lActive rs g l = true := by simp [lActive, hk, hg, hcnt]
              have hcont : (contribute h l.g (weighted 1 l.v) (rTotals rs k)).getD g (zero, zero) =
                  pplus (h.getD g (zero, zero)) (lContrib rs l) := by
                rw [contribute_getD]
                simp [lContrib, hk, hgeq, hg, hcnt, weighted]
              have hfold := ih (h := contribute h l.g (weighted 1 l.v) (rTotals rs k))
              rw [hcont] at hfold
              rw [hfold]
              simp [hact]
              rw [pplus_zero_left]
              rw [← pplus_zero_right (lContrib rs l)]
              rw [pfold_map_start (List.filter (lActive rs g) xs) (lContrib rs) (zero, zero) (lContrib rs l)]
              rw [pplus_zero_right (lContrib rs l)]
              rw [pplus_assoc]
          · have hact : lActive rs g l = false := by simp [lActive, hk, hg]
            have hgne : g ≠ l.g := by
              intro heq
              exact hg (by simpa [heq])
            have hskip : (contribute h l.g (weighted 1 l.v) (rTotals rs k)).getD g (zero, zero) =
                h.getD g (zero, zero) := by
              rw [contribute_getD]
              simp [hg, hgne]
            have hfold := ih (h := contribute h l.g (weighted 1 l.v) (rTotals rs k))
            rw [hskip] at hfold
            rw [hfold]
            simp [hact]

private theorem valid_rTotals (rs : List RRow) (k : Int) : Valid (rTotals rs k) :=
  valid_summarize _

private def eContrib (rs : List RRow) (e : (Int × Int) × Totals) : Totals × Totals :=
  h2c e.2 (rTotals rs e.1.2)

private def entryStep (rs : List RRow) (h : Groups) (e : (Int × Int) × Totals) : Groups :=
  contribute h e.1.1 e.2 (rTotals rs e.1.2)

private def entryFold (rs : List RRow) (es : List ((Int × Int) × Totals)) (h : Groups) : Groups :=
  es.foldl (entryStep rs) h

private theorem entryFold_getD (rs : List RRow) (es : List ((Int × Int) × Totals))
    (h : Groups) (g : Int) (hv : ∀ e ∈ es, Valid e.2) :
    (entryFold rs es h).getD g (zero, zero) =
      pplus (h.getD g (zero, zero))
        ((es.filter fun e => e.1.1 == g).foldl (fun t e => pplus t (eContrib rs e)) (zero, zero)) := by
  induction es generalizing h with
  | nil => simp [entryFold, pplus_zero_right]
  | cons e es ih =>
      simp only [entryFold, List.foldl_cons]
      unfold entryFold at ih
      have hev : Valid e.2 := hv e (by simp)
      have htailv : ∀ e' ∈ es, Valid e'.2 := by
        intro e' h'
        exact hv e' (by simp [h'])
      by_cases hg : e.1.1 == g
      · have hgeq : e.1.1 = g := by simpa using hg
        have hcont : (entryStep rs h e).getD g (zero, zero) =
            pplus (h.getD g (zero, zero)) (eContrib rs e) := by
          unfold entryStep eContrib
          rw [contribute_getD_v _ _ _ _ _ hev (valid_rTotals rs e.1.2)]
          simp [hgeq]
        have hfold := ih (h := entryStep rs h e) htailv
        rw [hcont] at hfold
        rw [hfold]
        simp [hg]
        rw [pplus_zero_left]
        rw [← pplus_zero_right (eContrib rs e)]
        rw [pfold_map_start (List.filter (fun e : (Int × Int) × Totals => e.1.1 == g) es)
          (eContrib rs) (zero, zero) (eContrib rs e)]
        rw [pplus_zero_right (eContrib rs e)]
        rw [pplus_assoc]
      · have hgne : e.1.1 ≠ g := by
          intro heq
          exact hg (by simpa [heq])
        have hgne2 : g ≠ e.1.1 := by
          intro heq
          exact hgne heq.symm
        have hskip : (entryStep rs h e).getD g (zero, zero) = h.getD g (zero, zero) := by
          unfold entryStep
          rw [contribute_getD_v _ _ _ _ _ hev (valid_rTotals rs e.1.2)]
          simp [hgne2]
        have hfold := ih (h := entryStep rs h e) htailv
        rw [hskip] at hfold
        rw [hfold]
        simp [hg]

private theorem pplus_foldl_zero {α : Type} (l : List α) (f : α → Totals × Totals)
    (acc : Totals × Totals) :
    pplus acc (l.foldl (fun t x => pplus t (f x)) (zero, zero)) =
      l.foldl (fun t x => pplus t (f x)) acc := by
  rw [← pfold_map_start l f (zero, zero) acc]
  rw [pplus_zero_right acc]

private theorem pfold_entries_perm {α : Type} {l₁ l₂ : List α} (hp : l₁.Perm l₂)
    (f : α → Totals × Totals) (acc : Totals × Totals) :
    l₁.foldl (fun t x => pplus t (f x)) acc = l₂.foldl (fun t x => pplus t (f x)) acc := by
  induction hp generalizing acc with
  | nil => rfl
  | cons x h ih =>
      simp only [List.foldl_cons]
      exact ih (acc := pplus acc (f x))
  | swap x y l => simp [pplus_assoc, pplus_comm, pplus_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => exact (ih₁ acc).trans (ih₂ acc)

private theorem entryFold_perm (rs : List RRow) {es₁ es₂ : List ((Int × Int) × Totals)}
    (hp : es₁.Perm es₂) (hv : ∀ e ∈ es₁, Valid e.2) (h : Groups) (g : Int) :
    (entryFold rs es₁ h).getD g (zero, zero) = (entryFold rs es₂ h).getD g (zero, zero) := by
  have hv2 : ∀ e ∈ es₂, Valid e.2 := by
    intro e he
    exact hv e (hp.mem_iff.mpr he)
  rw [entryFold_getD rs es₁ h g hv, entryFold_getD rs es₂ h g hv2]
  rw [pplus_foldl_zero, pplus_foldl_zero]
  rw [pfold_entries_perm (hp.filter _) (eContrib rs) (h.getD g (zero, zero))]

instance : Std.Associative pplus := ⟨pplus_assoc⟩
instance : Std.Commutative pplus := ⟨pplus_comm⟩

private def gsum (rs : List RRow) (es : List ((Int × Int) × Totals)) (g : Int) : Totals × Totals :=
  (es.filter fun e => e.1.1 == g).foldl (fun t e => pplus t (eContrib rs e)) (zero, zero)

private def finalize (rs : List RRow) (p : Std.HashMap (Int × Int) Totals) (cold : Groups) : Groups :=
  p.fold (fun groups (g, k) left => contribute groups g left (rTotals rs k)) cold

private theorem finalize_eq_entryFold (rs : List RRow) (p : Std.HashMap (Int × Int) Totals)
    (cold : Groups) :
    finalize rs p cold = entryFold rs p.toList cold := by
  unfold finalize entryFold entryStep
  rw [Std.HashMap.fold_eq_foldl_toList]

private theorem valid_getD (p : Std.HashMap (Int × Int) Totals) (q : Int × Int)
    (hv : ∀ e ∈ p.toList, Valid e.2) : Valid (p.getD q zero) := by
  by_cases hq : q ∈ p
  · have hsome : p[q]? = some (p.getD q zero) := Std.HashMap.getElem?_eq_some_getD hq
    have hmem : (q, p.getD q zero) ∈ p.toList :=
      Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hsome
    exact hv (q, p.getD q zero) hmem
  · have hnone : p[q]? = none := Std.HashMap.getElem?_eq_none hq
    rw [Std.HashMap.getD_eq_getD_getElem?, hnone]
    simp
    exact valid_zero

private theorem pairwise_key_eq {l : List ((Int × Int) × Totals)}
    (h : l.Pairwise (fun a b => (a.1 == b.1) = false))
    {u v : (Int × Int) × Totals} (hu : u ∈ l) (hv : v ∈ l) (hk : u.1 == v.1) : u = v := by
  induction l generalizing u v with
  | nil => cases hu
  | cons z zs ih =>
      simp only [List.mem_cons] at hu hv
      rcases hu with huz | hu
      · have huz' : u = z := by simpa using huz
        rcases hv with hvz | hv
        · have hvz' : v = z := by simpa using hvz
          rw [huz', hvz']
        · have hne : (z.1 == v.1) = false := List.rel_of_pairwise_cons h hv
          simp [huz', hne] at hk
      · rcases hv with hvz | hv
        · have hvz' : v = z := by simpa using hvz
          have hne : (z.1 == u.1) = false := List.rel_of_pairwise_cons h hu
          have huk : u.1 = z.1 := by simpa [hvz'] using hk
          exfalso
          have htrue : (z.1 == u.1) = true := by simpa [huk]
          rw [htrue] at hne
          cases hne
        · exact ih h.tail hu hv hk

private theorem toList_filter_key_eq (p : Std.HashMap (Int × Int) Totals) (q : Int × Int)
    {e : (Int × Int) × Totals} (he : e ∈ p.toList.filter fun z => z.1 == q) :
    e = (q, p.getD q zero) := by
  have hm := List.mem_filter.mp he
  have hkey : e.1 = q := by simpa using hm.2
  have hcong : p[e.1]? = p[q]? := by
    simpa [hkey]
  have hsome := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hm.1
  have hget : p.getD q zero = e.2 := by
    rw [Std.HashMap.getD_eq_getD_getElem?]
    rw [← hcong, hsome]
    simp
  have heq : e = (q, e.2) := by
    cases e
    simp at hkey ⊢
    exact hkey
  rw [heq]
  simp at hget ⊢
  rw [hget]

private theorem pfold_append {α : Type} (l₁ l₂ : List α) (f : α → Totals × Totals)
    (acc : Totals × Totals) :
    (l₁ ++ l₂).foldl (fun t x => pplus t (f x)) acc =
      l₂.foldl (fun t x => pplus t (f x)) (l₁.foldl (fun t x => pplus t (f x)) acc) := by
  simp [List.foldl_append]

private theorem filter_key_nil_or_singleton (p : Std.HashMap (Int × Int) Totals) (q : Int × Int) :
    (p.toList.filter fun e => e.1 == q) = [] ∨
      (p.toList.filter fun e => e.1 == q) = [(q, p.getD q zero)] := by
  let l := p.toList.filter fun e : (Int × Int) × Totals => e.1 == q
  have hpair : l.Pairwise (fun a b => (a.1 == b.1) = false) := by
    dsimp [l]
    exact (Std.HashMap.distinct_keys_toList (m := p)).filter _
  have hall : ∀ e ∈ l, e = (q, p.getD q zero) := by
    intro e he
    dsimp [l] at he
    exact toList_filter_key_eq p q he
  change l = [] ∨ l = [(q, p.getD q zero)]
  cases hl : l with
  | nil => exact Or.inl rfl
  | cons e es =>
      right
      rw [hl] at hpair
      have he := hall e (by rw [hl]; simp)
      have hes : es = [] := by
        cases es with
        | nil => rfl
        | cons e2 es2 =>
            have hkey : e2.1 == e.1 := by
              have he2 := hall e2 (by rw [hl]; simp)
              rw [he2, he]
              simp
            have hne : (e.1 == e2.1) = false := List.rel_of_pairwise_cons hpair (by simp)
            exfalso
            have heqkeys : e2.1 = e.1 := by simpa using hkey
            have htrue : (e.1 == e2.1) = true := by simp [heqkeys]
            rw [htrue] at hne
            cases hne
      simp [hl, he, hes]

private theorem filter_key_partition (p : Std.HashMap (Int × Int) Totals) (q : Int × Int)
    (g : Int) (hqg : (q.1 == g) = true) :
    (p.toList.filter fun e => e.1.1 == g).Perm
      ((p.toList.filter fun e => e.1 == q) ++
        (p.toList.filter fun e => e.1.1 == g && !(q == e.1))) := by
  have hd : ∀ e ∈ p.toList, ¬((e.1 == q) = true ∧ (e.1.1 == g && !(q == e.1)) = true) := by
    intro e he h
    rcases h with ⟨h1, h2⟩
    have heq : e.1 = q := by simpa using h1
    have hqeq : (q == e.1) = true := by simp [heq]
    simpa [hqeq] using h2
  have hp := split_filter_perm_h2 (fun e : (Int × Int) × Totals => e.1 == q)
    (fun e => e.1.1 == g && !(q == e.1)) p.toList hd
  refine (List.Perm.of_eq ?_).trans hp
  apply List.filter_congr
  intro e he
  by_cases hq : e.1 == q
  · have heq : e.1 = q := by simpa using hq
    have hk : e.1.1 == g := by simpa [heq] using hqg
    simp [hq, hk]
  · by_cases hk : e.1.1 == g
    · have hq' : (q == e.1) = false := by
        apply Bool.eq_false_of_ne_true
        intro ht
        have heq : q = e.1 := by simpa using ht
        exact hq (by simpa [heq])
      simp [hq, hk, hq']
    · simp [hq, hk]

private theorem finalize_insert (rs : List RRow) (p : Std.HashMap (Int × Int) Totals)
    (q : Int × Int) (v : Totals) (cold : Groups)
    (hv : Valid v) (hvmap : ∀ e ∈ p.toList, Valid e.2) :
    ∀ g : Int, (finalize rs (p.insert q (plus (p.getD q zero) v)) cold).getD g (zero, zero) =
      (contribute (finalize rs p cold) q.1 v (rTotals rs q.2)).getD g (zero, zero) := by
  intro g
  let oldv := p.getD q zero
  let newv := plus oldv v
  let r := rTotals rs q.2
  have holdv : Valid oldv := by dsimp [oldv]; exact valid_getD p q hvmap
  have hnewv : Valid newv := by dsimp [newv]; exact valid_plus holdv hv
  have hvmapNew : ∀ e ∈ (p.insert q newv).toList, Valid e.2 := by
    intro e he
    have hsome : (p.insert q newv)[e.1]? = some e.2 :=
      Std.HashMap.mem_toList_iff_getElem?_eq_some.mp he
    have heqval : e.2 = (p.insert q newv).getD e.1 zero := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hsome]
      simp
    rw [heqval, Std.HashMap.getD_insert]
    by_cases hq : q == e.1
    · simp [hq, hnewv]
    · simp [hq]
      exact valid_getD p e.1 hvmap
  have hL : (finalize rs (p.insert q newv) cold).getD g (zero, zero) =
      pplus (cold.getD g (zero, zero)) (gsum rs (p.insert q newv).toList g) := by
    rw [finalize_eq_entryFold]
    unfold gsum
    exact entryFold_getD rs (p.insert q newv).toList cold g hvmapNew
  have hP : (finalize rs p cold).getD g (zero, zero) =
      pplus (cold.getD g (zero, zero)) (gsum rs p.toList g) := by
    rw [finalize_eq_entryFold]
    unfold gsum
    exact entryFold_getD rs p.toList cold g hvmap
  let esNew := (q, newv) :: p.toList.filter (fun e => !(q == e.1))
  have hpPerm : (p.insert q newv).toList.Perm esNew := by
    dsimp [esNew]
    exact Std.HashMap.toList_insert_perm
  have hgsumNew : gsum rs (p.insert q newv).toList g = gsum rs esNew g := by
    unfold gsum
    exact pfold_entries_perm (hpPerm.filter _) (eContrib rs) (zero, zero)
  rw [hL, hgsumNew]
  rw [contribute_getD_v _ _ _ _ _ hv (valid_rTotals rs q.2)]
  rw [hP]
  by_cases hqg : (q.1 == g) = true
  · have hqgeq : q.1 = g := by simpa using hqg
    rw [if_pos (by simpa [hqgeq] : (g == q.1) = true)]
    -- describe the g-filter of esNew
    have hesNew : esNew.filter (fun e : (Int × Int) × Totals => e.1.1 == g) =
        (q, newv) :: (p.toList.filter (fun e => !(q == e.1))).filter
          (fun e => e.1.1 == g) := by
      dsimp [esNew]
      simp [hqgeq]
    have hrest : (p.toList.filter fun e => !(q == e.1)).filter (fun e => e.1.1 == g) =
        p.toList.filter (fun e => e.1.1 == g && !(q == e.1)) := by
      ext e
      simp [Bool.and_comm, Bool.and_left_comm, Bool.and_assoc]
    have hQ : p.toList.filter (fun e => e.1 == q) = [] ∨
        p.toList.filter (fun e => e.1 == q) = [(q, oldv)] := by
      dsimp [oldv]
      exact filter_key_nil_or_singleton p q
    have hpart := filter_key_partition p q g hqg
    let Q := p.toList.filter fun e => e.1 == q
    let R := p.toList.filter fun e => e.1.1 == g && !(q == e.1)
    let f := fun t e => pplus t (eContrib rs e)
    have hsumP : gsum rs p.toList g = pplus (Q.foldl f (zero, zero)) (R.foldl f (zero, zero)) := by
      unfold gsum
      have hpf : (Q ++ R).foldl f (zero, zero) =
          (p.toList.filter fun e => e.1.1 == g).foldl f (zero, zero) := by
        exact (pfold_entries_perm (by simpa [Q, R] using hpart) (eContrib rs) (zero, zero)).symm
      rw [← hpf]
      rw [pfold_append]
      exact (pplus_foldl_zero R (eContrib rs) (Q.foldl f (zero, zero))).symm
    have hLsum : gsum rs esNew g =
        pplus (eContrib rs (q, newv)) (R.foldl f (zero, zero)) := by
      unfold gsum
      rw [hesNew, hrest]
      simp only [List.foldl_cons]
      rw [pplus_zero_left]
      change R.foldl (fun t e => pplus t (eContrib rs e)) (eContrib rs (q, newv)) =
        pplus (eContrib rs (q, newv))
          (R.foldl (fun t e => pplus t (eContrib rs e)) (zero, zero))
      have htmp := pfold_map_start R (eContrib rs) (zero, zero) (eContrib rs (q, newv))
      rw [pplus_zero_right] at htmp
      exact htmp
    rw [hLsum, hsumP]
    rcases hQ with hnil | hsingle
    · have hQnil : Q = [] := by simpa [Q] using hnil
      have hnotmem : q ∉ p := by
        intro hqmem
        have hs : p[q]? = some (p.getD q zero) := Std.HashMap.getElem?_eq_some_getD hqmem
        have hm : (q, p.getD q zero) ∈ p.toList := Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hs
        have : (q, p.getD q zero) ∈ Q := by simp [Q, hm]
        simp [hQnil] at this
      have holdzero : oldv = zero := by
        dsimp [oldv]
        rw [Std.HashMap.getD_eq_getD_getElem?, Std.HashMap.getElem?_eq_none hnotmem]
        simp
      have hnewEq : newv = v := by
        dsimp [newv]
        rw [holdzero]
        cases v
        simp [plus, zero]
      rw [show eContrib rs (q, newv) = h2c v r by simp [eContrib, hnewEq, r]]
      rw [show Q.foldl f (zero, zero) = (zero, zero) by simp [hQnil, f]]
      dsimp [r]
      rw [pplus_zero_left]
      ac_rfl
    · have hQsingle : Q = [(q, oldv)] := by simpa [Q] using hsingle
      rw [show Q.foldl f (zero, zero) = eContrib rs (q, oldv) by
        simp [hQsingle, f, eContrib, pplus_zero_left]]
      rw [show eContrib rs (q, newv) = pplus (eContrib rs (q, oldv)) (h2c v r) by
        dsimp [eContrib, newv, r]
        exact h2c_add_left oldv v (rTotals rs q.2)]
      dsimp [r]
      ac_rfl
  · have hqgne : q.1 ≠ g := by
      intro heq
      exact hqg (by simpa [heq])
    rw [if_neg (by
      intro ht
      have heq : g = q.1 := by simpa using ht
      exact hqgne heq.symm)]
    unfold gsum
    have hesNew : esNew.filter (fun e : (Int × Int) × Totals => e.1.1 == g) =
        (p.toList.filter (fun e => !(q == e.1))).filter (fun e => e.1.1 == g) := by
      dsimp [esNew]
      simp [hqgne]
    rw [hesNew]
    have hrest : (p.toList.filter fun e => !(q == e.1)).filter (fun e => e.1.1 == g) =
        p.toList.filter (fun e => e.1.1 == g) := by
      rw [List.filter_filter]
      apply List.filter_congr
      intro e he
      by_cases hg' : e.1.1 == g
      · have hnq : (q == e.1) = false := by
          apply Bool.eq_false_of_ne_true
          intro hqe
          have heq : q = e.1 := by simpa using hqe
          have : q.1 == g := by simpa [heq] using hg'
          exact hqg this
        simp [hg', hnq]
      · simp [hg']
    rw [hrest]

private def hStep (rs : List RRow) (hot : Int → Bool)
    (st : Std.HashMap (Int × Int) Totals × Groups) (l : LRow) :=
  match l.k with
  | none => st
  | some k =>
      let left := weighted 1 l.v
      if hot k then
        (st.1.insert (l.g, k) (plus (st.1.getD (l.g, k) zero) left), st.2)
      else (st.1, contribute st.2 l.g left (rTotals rs k))

private def hProcess (rs : List RRow) (hot : Int → Bool) (ls : List LRow) :=
  ls.foldl (hStep rs hot) ({}, {})

private theorem process_partial_valid (rs : List RRow) (hot : Int → Bool) :
    ∀ (xs : List LRow) (p : Std.HashMap (Int × Int) Totals) (c : Groups),
      (∀ e ∈ p.toList, Valid e.2) →
      ∀ e ∈ (xs.foldl (hStep rs hot) (p, c)).1.toList, Valid e.2 := by
  intro xs
  induction xs with
  | nil =>
      intro p c hv e he
      exact hv e he
  | cons l xs ih =>
      intro p c hv
      cases hk : l.k with
      | none =>
          simpa [hStep, hk] using ih p c hv
      | some k =>
          by_cases hhot : hot k
          · let p' := p.insert (l.g, k) (plus (p.getD (l.g, k) zero) (weighted 1 l.v))
            have hpvalid : ∀ e ∈ p'.toList, Valid e.2 := by
              intro e he
              have hs : p'[e.1]? = some e.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp he
              have heqval : e.2 = p'.getD e.1 zero := by
                rw [Std.HashMap.getD_eq_getD_getElem?, hs]
                simp
              rw [heqval, Std.HashMap.getD_insert]
              by_cases hq : (l.g, k) == e.1
              · simp [hq]
                apply valid_plus
                · exact valid_getD p (l.g, k) hv
                · exact valid_weighted 1 l.v
              · simp [hq]
                exact valid_getD p e.1 hv
            simpa [hStep, hk, hhot, p'] using ih p' c hpvalid
          · simpa [hStep, hk, hhot] using ih p (contribute c l.g (weighted 1 l.v) (rTotals rs k)) hv

private theorem finalize_contribute (rs : List RRow) (p : Std.HashMap (Int × Int) Totals)
    (cold : Groups) (g : Int) (l r : Totals) (hl : Valid l) (hr : Valid r)
    (hv : ∀ e ∈ p.toList, Valid e.2) :
    ∀ q, (finalize rs p (contribute cold g l r)).getD q (zero, zero) =
      (contribute (finalize rs p cold) g l r).getD q (zero, zero) := by
  intro q
  rw [finalize_eq_entryFold, finalize_eq_entryFold]
  rw [contribute_getD_v _ _ _ _ _ hl hr]
  rw [entryFold_getD rs p.toList (contribute cold g l r) q hv]
  rw [contribute_getD_v _ _ _ _ _ hl hr]
  rw [entryFold_getD rs p.toList cold q hv]
  by_cases hq : (q == g) = true <;> simp [hq] <;> ac_rfl

private theorem naiveFold_append_singleton (rs : List RRow) (h : Groups) (ys : List LRow)
    (l : LRow) (k : Int) (hk : l.k = some k) :
    naiveFold rs h (ys ++ [l]) =
      contribute (naiveFold rs h ys) l.g (weighted 1 l.v) (rTotals rs k) := by
  unfold naiveFold
  rw [List.foldl_append]
  simp [hk]

private theorem process_final_aux (rs : List RRow) (hot : Int → Bool) :
    ∀ (xs ys : List LRow) (p : Std.HashMap (Int × Int) Totals) (c : Groups),
      (∀ e ∈ p.toList, Valid e.2) →
      (∀ q, (finalize rs p c).getD q (zero, zero) =
        (naiveFold rs {} ys).getD q (zero, zero)) →
      ∀ q, (finalize rs (xs.foldl (hStep rs hot) (p, c)).1
          (xs.foldl (hStep rs hot) (p, c)).2).getD q (zero, zero) =
        (naiveFold rs {} (ys ++ xs)).getD q (zero, zero) := by
  intro xs
  induction xs with
  | nil =>
      intro ys p c hv h q
      simpa using h q
  | cons l xs ih =>
      intro ys p c hv h q
      cases hk : l.k with
      | none =>
          simp only [List.foldl_cons]
          have hstep : hStep rs hot (p, c) l = (p, c) := by simp [hStep, hk]
          rw [hstep]
          have happ : ys ++ l :: xs = (ys ++ [l]) ++ xs := by simp [List.append_assoc]
          rw [happ]
          have hnaive : ∀ q', (naiveFold rs {} (ys ++ [l])).getD q' (zero, zero) =
              (naiveFold rs {} ys).getD q' (zero, zero) := by
            intro q'
            simp [naiveFold, hk, List.foldl_append]
          exact ih (ys ++ [l]) p c hv (by intro q'; rw [hnaive q']; exact h q') q
      | some k =>
          by_cases hhot : hot k
          · let p' := p.insert (l.g, k) (plus (p.getD (l.g, k) zero) (weighted 1 l.v))
            have hv' : ∀ e ∈ p'.toList, Valid e.2 := by
              intro e he
              have hs : p'[e.1]? = some e.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp he
              have heqval : e.2 = p'.getD e.1 zero := by
                rw [Std.HashMap.getD_eq_getD_getElem?, hs]
                simp
              rw [heqval, Std.HashMap.getD_insert]
              by_cases hke : (l.g, k) == e.1
              · simp [hke]
                exact valid_plus (valid_getD p (l.g, k) hv) (valid_weighted 1 l.v)
              · simp [hke]
                exact valid_getD p e.1 hv
            have hfinal' : ∀ q', (finalize rs p' c).getD q' (zero, zero) =
                (naiveFold rs {} (ys ++ [l])).getD q' (zero, zero) := by
              intro q'
              dsimp [p']
              rw [finalize_insert rs p (l.g, k) (weighted 1 l.v) c
                (valid_weighted 1 l.v) hv q']
              rw [contribute_getD_v _ _ _ _ _ (valid_weighted 1 l.v)
                (valid_rTotals rs (l.g, k).2)]
              rw [h q']
              rw [naiveFold_append_singleton rs {} ys l k hk]
              rw [contribute_getD_v _ _ _ _ _ (valid_weighted 1 l.v)
                (valid_rTotals rs (l.g, k).2)]
            simp only [List.foldl_cons]
            have hstep : hStep rs hot (p, c) l = (p', c) := by
              simp [hStep, hk, hhot, p']
            rw [hstep]
            have happ : ys ++ l :: xs = (ys ++ [l]) ++ xs := by simp [List.append_assoc]
            rw [happ]
            exact ih (ys ++ [l]) p' c hv' hfinal' q
          · have hfinal' : ∀ q', (finalize rs p
                (contribute c l.g (weighted 1 l.v) (rTotals rs k))).getD q' (zero, zero) =
                (naiveFold rs {} (ys ++ [l])).getD q' (zero, zero) := by
              intro q'
              rw [finalize_contribute rs p c l.g (weighted 1 l.v) (rTotals rs k)
                (valid_weighted 1 l.v) (valid_rTotals rs k) hv q']
              rw [contribute_getD_v _ _ _ _ _ (valid_weighted 1 l.v)
                (valid_rTotals rs k)]
              rw [h q']
              rw [naiveFold_append_singleton rs {} ys l k hk]
              rw [contribute_getD_v _ _ _ _ _ (valid_weighted 1 l.v)
                (valid_rTotals rs k)]
            simp only [List.foldl_cons]
            have hstep : hStep rs hot (p, c) l =
                (p, contribute c l.g (weighted 1 l.v) (rTotals rs k)) := by
              simp [hStep, hk, hhot]
            rw [hstep]
            have happ : ys ++ l :: xs = (ys ++ [l]) ++ xs := by simp [List.append_assoc]
            rw [happ]
            exact ih (ys ++ [l]) p (contribute c l.g (weighted 1 l.v) (rTotals rs k)) hv hfinal' q

private def buildOf (rs : List RRow) : Std.HashMap Int Totals :=
  rs.foldl (fun (h : Std.HashMap Int Totals) r => match r.k with
    | none => h
    | some k => h.insert k (plus (h.getD k zero) (weighted 1 r.v))) {}

private theorem buildOf_getD (rs : List RRow) (k : Int) : (buildOf rs).getD k zero = rTotals rs k := by
  unfold buildOf
  exact build_final rs k

private theorem buildOf_valid (rs : List RRow) (k : Int) : Valid ((buildOf rs).getD k zero) := by
  rw [buildOf_getD]
  exact valid_rTotals rs k

private def buildContribStep (rs : List RRow) (h : Groups) (l : LRow) : Groups :=
  match l.k with
  | none => h
  | some k => contribute h l.g (weighted 1 l.v) ((buildOf rs).getD k zero)

private theorem naiveFold_build_eq (rs : List RRow) (ys : List LRow) (h : Groups) :
    ∀ g, (ys.foldl (buildContribStep rs) h).getD g (zero, zero) =
      (naiveFold rs h ys).getD g (zero, zero) := by
  induction ys generalizing h with
  | nil => intro g; rfl
  | cons l ys ih =>
      intro g
      simp only [List.foldl_cons]
      cases hk : l.k with
      | none =>
          simp [buildContribStep, naiveFold, hk]
          exact ih h g
      | some k =>
          have hget : (buildOf rs).getD k zero = rTotals rs k := buildOf_getD rs k
          simpa [buildContribStep, naiveFold, hk, hget] using
            ih (contribute h l.g (weighted 1 l.v) (rTotals rs k)) g

private def hProcessBuild (rs : List RRow) (hot : Int → Bool) (ls : List LRow)
    : Std.HashMap (Int × Int) Totals × Groups :=
  ls.foldl (fun (st : Std.HashMap (Int × Int) Totals × Groups) l => match l.k with
    | none => st
    | some k =>
        let left := weighted 1 l.v
        if hot k then
          (st.1.insert (l.g, k) (plus (st.1.getD (l.g, k) zero) left), st.2)
        else (st.1, contribute st.2 l.g left ((buildOf rs).getD k zero))) ({}, {})

private theorem hProcess_fst_eq_aux (rs : List RRow) (hot : Int → Bool) :
    ∀ (ls : List LRow) (p : Std.HashMap (Int × Int) Totals) (cB cR : Groups),
      (ls.foldl (fun (st : Std.HashMap (Int × Int) Totals × Groups) l => match l.k with
        | none => st
        | some k =>
            let left := weighted 1 l.v
            if hot k then
              (st.1.insert (l.g, k) (plus (st.1.getD (l.g, k) zero) left), st.2)
            else (st.1, contribute st.2 l.g left ((buildOf rs).getD k zero))) (p, cB)).1 =
      (ls.foldl (hStep rs hot) (p, cR)).1 := by
  intro ls
  induction ls with
  | nil => intro p cB cR; rfl
  | cons l ls ih =>
      intro p cB cR
      cases hk : l.k with
      | none => simpa [hStep, hk] using ih p cB cR
      | some k =>
          by_cases hhot : hot k
          · simpa [hStep, hk, hhot] using
              ih (p.insert (l.g, k) (plus (p.getD (l.g, k) zero) (weighted 1 l.v))) cB cR
          · simpa [hStep, hk, hhot] using
              ih p (contribute cB l.g (weighted 1 l.v) ((buildOf rs).getD k zero))
                (contribute cR l.g (weighted 1 l.v) (rTotals rs k))

private theorem hProcess_fst_eq (rs : List RRow) (hot : Int → Bool) (ls : List LRow) :
    (hProcessBuild rs hot ls).1 = (hProcess rs hot ls).1 := by
  unfold hProcessBuild hProcess
  exact hProcess_fst_eq_aux rs hot ls {} {} {}

private theorem hProcessBuild_snd_getD_aux (rs : List RRow) (hot : Int → Bool) :
    ∀ (ls : List LRow) (p : Std.HashMap (Int × Int) Totals) (cB cR : Groups),
      (∀ q, cB.getD q (zero, zero) = cR.getD q (zero, zero)) →
      ∀ q, (ls.foldl (fun (st : Std.HashMap (Int × Int) Totals × Groups) l => match l.k with
        | none => st
        | some k =>
            let left := weighted 1 l.v
            if hot k then
              (st.1.insert (l.g, k) (plus (st.1.getD (l.g, k) zero) left), st.2)
            else (st.1, contribute st.2 l.g left ((buildOf rs).getD k zero))) (p, cB)).2.getD q (zero, zero) =
        (ls.foldl (hStep rs hot) (p, cR)).2.getD q (zero, zero) := by
  intro ls
  induction ls with
  | nil => intro p cB cR hc q; exact hc q
  | cons l ls ih =>
      intro p cB cR hc q
      cases hk : l.k with
      | none => simpa [hStep, hk] using ih p cB cR hc q
      | some k =>
          by_cases hhot : hot k
          · simpa [hStep, hk, hhot] using
              ih (p.insert (l.g, k) (plus (p.getD (l.g, k) zero) (weighted 1 l.v))) cB cR hc q
          · have hget : (buildOf rs).getD k zero = rTotals rs k := buildOf_getD rs k
            have hnext : ∀ q', (contribute cB l.g (weighted 1 l.v) ((buildOf rs).getD k zero)).getD q' (zero, zero) =
                (contribute cR l.g (weighted 1 l.v) (rTotals rs k)).getD q' (zero, zero) := by
              intro q'
              rw [contribute_getD_v _ _ _ _ _ (valid_weighted 1 l.v) (buildOf_valid rs k)]
              rw [contribute_getD_v _ _ _ _ _ (valid_weighted 1 l.v) (valid_rTotals rs k)]
              rw [hget]
              by_cases hq : (q' == l.g) = true
              · simp [hq]
                rw [hc q']
              · simp [hq]
                exact hc q'
            simpa [hStep, hk, hhot] using
              ih p (contribute cB l.g (weighted 1 l.v) ((buildOf rs).getD k zero))
                (contribute cR l.g (weighted 1 l.v) (rTotals rs k)) hnext q

private theorem hProcessBuild_snd_getD (rs : List RRow) (hot : Int → Bool) (ls : List LRow) :
    ∀ g, (hProcessBuild rs hot ls).2.getD g (zero, zero) =
      (hProcess rs hot ls).2.getD g (zero, zero) := by
  unfold hProcessBuild hProcess
  exact hProcessBuild_snd_getD_aux rs hot ls {} {} {} (by intro q; rfl)

private def finalizeBuild (rs : List RRow) (p : Std.HashMap (Int × Int) Totals) (c : Groups) : Groups :=
  p.fold (fun groups (g, k) left => contribute groups g left ((buildOf rs).getD k zero)) c

private theorem entryFold_build_eq_aux (rs : List RRow) :
    ∀ (es : List ((Int × Int) × Totals)) (cB cR : Groups),
      (∀ q, cB.getD q (zero, zero) = cR.getD q (zero, zero)) →
      (∀ e ∈ es, Valid e.2) →
      ∀ g, (es.foldl (fun h e => contribute h e.1.1 e.2 ((buildOf rs).getD e.1.2 zero)) cB).getD g (zero, zero) =
        (es.foldl (entryStep rs) cR).getD g (zero, zero) := by
  intro es
  induction es with
  | nil =>
      intro cB cR hc hv g
      exact hc g
  | cons e es ih =>
      intro cB cR hc hv g
      have hev : Valid e.2 := hv e (by simp)
      have htail : ∀ e' ∈ es, Valid e'.2 := by
        intro e' h'
        exact hv e' (by simp [h'])
      have hnext : ∀ q, (contribute cB e.1.1 e.2 ((buildOf rs).getD e.1.2 zero)).getD q (zero, zero) =
          (entryStep rs cR e).getD q (zero, zero) := by
        intro q
        rw [contribute_getD_v _ _ _ _ _ hev (buildOf_valid rs e.1.2)]
        unfold entryStep
        rw [contribute_getD_v _ _ _ _ _ hev (valid_rTotals rs e.1.2)]
        rw [buildOf_getD]
        by_cases hq : e.1.1 == q
        · have hqeq : e.1.1 = q := by simpa using hq
          simp [hqeq]
          rw [hc q]
        · have hqne : e.1.1 ≠ q := by
            intro heq
            exact hq (by simpa [heq])
          have hqne2 : q ≠ e.1.1 := by intro heq; exact hqne heq.symm
          simp [hqne2]
          exact hc q
      exact ih (contribute cB e.1.1 e.2 ((buildOf rs).getD e.1.2 zero))
        (entryStep rs cR e) hnext htail g

private theorem entryFold_build_eq (rs : List RRow) (es : List ((Int × Int) × Totals))
    (c : Groups) (hv : ∀ e ∈ es, Valid e.2) :
    ∀ g, (es.foldl (fun h e => contribute h e.1.1 e.2 ((buildOf rs).getD e.1.2 zero)) c).getD g (zero, zero) =
      (es.foldl (entryStep rs) c).getD g (zero, zero) := by
  exact entryFold_build_eq_aux rs es c c (by intro q; rfl) hv

private theorem finalizeBuild_eq_aux (rs : List RRow) (p : Std.HashMap (Int × Int) Totals)
    (cB cR : Groups) (hc : ∀ q, cB.getD q (zero, zero) = cR.getD q (zero, zero))
    (hv : ∀ e ∈ p.toList, Valid e.2) :
    ∀ g, (finalizeBuild rs p cB).getD g (zero, zero) =
      (finalize rs p cR).getD g (zero, zero) := by
  intro g
  unfold finalizeBuild finalize
  rw [Std.HashMap.fold_eq_foldl_toList, Std.HashMap.fold_eq_foldl_toList]
  exact entryFold_build_eq_aux rs p.toList cB cR hc hv g

private theorem process_partial_valid_final (rs : List RRow) (hot : Int → Bool) (ls : List LRow) :
    ∀ e ∈ (hProcess rs hot ls).1.toList, Valid e.2 := by
  unfold hProcess
  exact process_partial_valid rs hot ls {} {} (by
    intro e he
    have : False := by simpa using he
    exact False.elim this)

private theorem hybridGroups_getD (ls : List LRow) (rs : List RRow)
    (hot : Int → Bool) (g : Int) :
    (hybridGroups ls rs hot).getD g (zero, zero) = (naiveFold rs {} ls).getD g (zero, zero) := by
  unfold hybridGroups
  dsimp
  change (finalizeBuild rs (hProcessBuild rs hot ls).1 (hProcessBuild rs hot ls).2).getD g (zero, zero) =
    (naiveFold rs {} ls).getD g (zero, zero)
  rw [hProcess_fst_eq]
  rw [finalizeBuild_eq_aux rs (hProcess rs hot ls).1 (hProcessBuild rs hot ls).2
    (hProcess rs hot ls).2 (hProcessBuild_snd_getD rs hot ls)
    (process_partial_valid_final rs hot ls) g]
  have hinit : ∀ q, (finalize rs {} {}).getD q (zero, zero) =
      (naiveFold rs {} []).getD q (zero, zero) := by
    intro q
    rw [finalize_eq_entryFold]
    simp [entryFold, naiveFold]
  exact process_final_aux rs hot ls [] {} {}
    (by
      intro e he
      have : False := by simpa using he
      exact False.elim this) hinit g

private theorem h2c_add_right (l r₁ r₂ : Totals) :
    h2c l (plus r₁ r₂) = pplus (h2c l r₁) (h2c l r₂) := by
  cases l
  cases r₁
  cases r₂
  simp [h2c, pplus, plus, Nat.add_mul, Nat.mul_add, Int.add_mul, Int.mul_add,
    Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]

private theorem h2c_weighted_weighted (x y : Option Int) :
    h2c (weighted 1 x) (weighted 1 y) = (weighted 1 x, weighted 1 y) := by
  cases x <;> cases y <;> simp [h2c, weighted, zero]

private theorem h2c_weighted_pairs (rs : List RRow) (k : Int) (l : LRow) :
    h2c (weighted 1 l.v) (rTotals rs k) =
      let rsK := rs.filter fun r => r.k == some k
      (summarize ((rsK.map fun r => (l, r)).map fun p => p.1.v),
       summarize ((rsK.map fun r => (l, r)).map fun p => p.2.v)) := by
  induction rs with
  | nil =>
      simp [rTotals, summarize, h2c, weighted, zero]
  | cons r rs ih =>
      by_cases hrk : r.k == some k
      · have hrt : rTotals (r :: rs) k =
            plus (weighted 1 r.v) (rTotals rs k) := by
          unfold rTotals
          simp [hrk, summarize_cons]
        rw [hrt, h2c_add_right, h2c_weighted_weighted]
        dsimp
        simp [hrk, ih, summarize_cons, pplus, List.map_map, plus]
      · have hrt : rTotals (r :: rs) k = rTotals rs k := by
          unfold rTotals
          simp [hrk]
        rw [hrt]
        simp [hrk] at ih ⊢
        exact ih

private theorem beq_int_comm_h5 (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have : a = b := by simpa using h
    simp [this]
  · intro h
    have : b = a := by simpa using h
    simp [this]

private theorem rTotals_cnt (rs : List RRow) (k : Int) :
    (rTotals rs k).cnt = (rs.filter fun r => r.k == some k).length := by
  unfold rTotals
  rw [summarize_char]
  simp

private theorem h2c_weighted_pairsFM (rs : List RRow) (k : Int) (l : LRow) :
    h2c (weighted 1 l.v) (rTotals rs k) =
      let ps := rs.filterMap fun r =>
        match some k, r.k with
        | some lk, some rk => if lk == rk then some (l, r) else none
        | _, _ => none
      (summarize (ps.map fun p => p.1.v), summarize (ps.map fun p => p.2.v)) := by
  induction rs with
  | nil => simp [rTotals, summarize, h2c, weighted, zero]
  | cons r rs ih =>
      cases hr : r.k with
      | none =>
          have hrt : rTotals (r :: rs) k = rTotals rs k := by
            unfold rTotals
            simp [hr]
          rw [hrt]
          simp [hr] at ih ⊢
          exact ih
      | some rk =>
          by_cases h1 : k == rk
          · have hEq : k = rk := by simpa using h1
            have hrt : rTotals (r :: rs) k = plus (weighted 1 r.v) (rTotals rs k) := by
              unfold rTotals
              simp [hr, hEq, summarize_cons]
            rw [hrt, h2c_add_right, h2c_weighted_weighted, ih]
            dsimp
            simp [hr, hEq, summarize_cons, pplus, plus]
          · have hEq : k ≠ rk := by
              intro heq
              exact h1 (by simp [heq])
            have hrt : rTotals (r :: rs) k = rTotals rs k := by
              unfold rTotals
              have hEq' : rk ≠ k := by
                intro heq
                exact hEq heq.symm
              have hne : (r.k == some k) = false := by simp [hr, hEq']
              simp [hne]
            rw [hrt]
            simp [hr, hEq] at ih ⊢
            exact ih


private theorem pairsFM_nil_of_cnt_zero (rs : List RRow) (k : Int) (l : LRow)
    (h : (rTotals rs k).cnt = 0) :
    (rs.filterMap fun r =>
      match some k, r.k with
      | some lk, some rk => if lk == rk then some (l, r) else none
      | _, _ => none) = [] := by
  let ps := rs.filterMap fun r =>
    match some k, r.k with
    | some lk, some rk => if lk == rk then some (l, r) else none
    | _, _ => none
  have hw := h2c_weighted_pairsFM rs k l
  have hz : h2c (weighted 1 l.v) (rTotals rs k) = (zero, zero) :=
    h2c_eq_zero_of_cnt_valid (valid_weighted 1 l.v) (valid_rTotals rs k) (Or.inr h)
  change h2c (weighted 1 l.v) (rTotals rs k) =
    (summarize (ps.map fun p => p.1.v), summarize (ps.map fun p => p.2.v)) at hw
  rw [hz] at hw
  have hfst := congrArg Prod.fst hw
  rw [summarize_char] at hfst
  simp [zero] at hfst
  exact List.eq_nil_of_length_eq_zero hfst.1.symm

private theorem filterMap_none_nil {α β : Type} : ∀ xs : List α,
    xs.filterMap (fun _ => (none : Option β)) = []
  | [] => rfl
  | a :: xs => by simp [filterMap_none_nil xs]

private theorem refJoinedPairs_cons (l : LRow) (ls : List LRow) (rs : List RRow) (g : Int) :
    refJoinedPairs (l :: ls) rs g =
      (if l.g == g then rs.filterMap (fun r =>
        match l.k, r.k with
        | some lk, some rk => if lk == rk then some (l, r) else none
        | _, _ => none) else []) ++ refJoinedPairs ls rs g := by
  unfold refJoinedPairs
  by_cases h : l.g == g <;> simp [h] <;> rfl

private theorem pfold_start_eq {α : Type} (l : List α) (f : α → Totals × Totals)
    (acc : Totals × Totals) :
    l.foldl (fun t x => pplus t (f x)) acc =
      pplus acc (l.foldl (fun t x => pplus t (f x)) (zero, zero)) := by
  have h := pfold_map_start l f (zero, zero) acc
  rw [pplus_zero_right acc] at h
  exact h

private theorem activeVals_eq_refTotals (ls : List LRow) (rs : List RRow) (g : Int) :
    ((ls.filter (lActive rs g)).foldl (fun t l => pplus t (lContrib rs l)) (zero, zero)) =
      (summarize ((refJoinedPairs ls rs g).map fun p => p.1.v),
       summarize ((refJoinedPairs ls rs g).map fun p => p.2.v)) := by
  induction ls with
  | nil => simp [refJoinedPairs, lActive, summarize, zero]
  | cons l ls ih =>
      cases hk : l.k with
      | none =>
          have hact : lActive rs g l = false := by simp [lActive, hk]
          rw [List.filter_cons_of_neg (by simpa using hact)]
          rw [refJoinedPairs_cons]
          by_cases hlg : l.g == g
          · simp [hlg, hk]
            rw [show rs.filterMap (fun _ : RRow => (none : Option (LRow × RRow))) = [] from
              filterMap_none_nil rs]
            simp
            exact ih
          · simp [hlg, hk]
            exact ih
      | some k =>
          by_cases hlg : l.g == g
          · by_cases hcnt : (rTotals rs k).cnt == 0
            · have hact : lActive rs g l = false := by simp [lActive, hk, hlg, hcnt]
              have hnil := pairsFM_nil_of_cnt_zero rs k l (by simpa using hcnt)
              rw [List.filter_cons_of_neg (by simpa using hact)]
              rw [refJoinedPairs_cons]
              simp only [if_pos hlg]
              rw [hk]
              rw [hnil]
              simp
              exact ih
            · have hact : lActive rs g l = true := by simp [lActive, hk, hlg, hcnt]
              have hp := h2c_weighted_pairsFM rs k l
              dsimp at hp
              rw [List.filter_cons_of_pos (by simpa using hact)]
              simp only [List.foldl_cons, pplus_zero_left]
              have hlc : lContrib rs l = h2c (weighted 1 l.v) (rTotals rs k) := by
                simp [lContrib, hk]
              rw [hlc]
              rw [hp]
              rw [refJoinedPairs_cons]
              simp [hlg, hk]
              rw [summarize_append, summarize_append]
              rw [pfold_start_eq]
              rw [ih]
              simp [pplus]
          · have hact : lActive rs g l = false := by simp [lActive, hk, hlg]
            rw [List.filter_cons_of_neg (by simpa using hact)]
            rw [refJoinedPairs_cons]
            simp [hlg, hk]
            exact ih


private theorem naiveFold_eq_refTotals (ls : List LRow) (rs : List RRow) (g : Int) :
    (naiveFold rs {} ls).getD g (zero, zero) =
      let ps := refJoinedPairs ls rs g
      (summarize (ps.map fun p => p.1.v), summarize (ps.map fun p => p.2.v)) := by
  have hzero : ({} : Groups).getD g (zero, zero) = (zero, zero) := by simp
  rw [naiveFold_getD, hzero, pplus_zero_left]

  exact activeVals_eq_refTotals ls rs g

private def Good (h : Groups) : Prop :=
  ∀ g, h.contains g = decide (0 < (h.getD g (zero, zero)).1.cnt)

private theorem good_empty : Good ({} : Groups) := by
  intro g
  simp [zero]

private theorem good_contribute (h : Groups) (g : Int) (l r : Totals)
    (hh : Good h) (hl : Valid l) (hr : Valid r) : Good (contribute h g l r) := by
  intro q
  unfold contribute
  by_cases hz : l.cnt == 0 || r.cnt == 0
  · rw [if_pos hz]
    exact hh q
  · rw [if_neg hz]
    by_cases hq : g == q
    · have hqeq : g = q := by simpa using hq
      subst q
      rw [Std.HashMap.contains_insert, Std.HashMap.getD_insert]
      simp
      have hl0 : l.cnt ≠ 0 := by
        intro h0
        apply hz
        simp [h0]
      have hr0 : r.cnt ≠ 0 := by
        intro h0
        apply hz
        simp [h0]
      have hp : 0 < l.cnt * r.cnt := Nat.mul_pos (Nat.pos_of_ne_zero hl0) (Nat.pos_of_ne_zero hr0)
      change 0 < (Std.HashMap.getD h g (zero, zero)).1.cnt + l.cnt * r.cnt
      omega
    · have hq' : (q == g) = false := by
        apply Bool.eq_false_of_ne_true
        intro ht
        have heq : q = g := by simpa using ht
        exact hq (by simpa [heq])
      rw [Std.HashMap.contains_insert, Std.HashMap.getD_insert]
      simp [hq, hq']
      exact hh q

private theorem naiveFold_good_aux (rs : List RRow) :
    ∀ (ys : List LRow) (h : Groups), Good h → Good (naiveFold rs h ys) := by
  intro ys
  induction ys with
  | nil => intro h hh; simpa [naiveFold] using hh
  | cons l ys ih =>
      intro h hh
      cases hk : l.k with
      | none => simpa [naiveFold, hk] using ih h hh
      | some k =>
          simpa [naiveFold, hk] using
            ih (contribute h l.g (weighted 1 l.v) (rTotals rs k))
              (good_contribute h l.g (weighted 1 l.v) (rTotals rs k) hh
                (valid_weighted 1 l.v) (valid_rTotals rs k))

private theorem naiveFold_good (rs : List RRow) (ls : List LRow) : Good (naiveFold rs {} ls) := by
  exact naiveFold_good_aux rs ls {} good_empty

private theorem fold_entries_good (rs : List RRow) (es : List ((Int × Int) × Totals))
    (c : Groups) (hc : Good c) (hv : ∀ e ∈ es, Valid e.2) :
    Good (es.foldl (fun h e => contribute h e.1.1 e.2 ((buildOf rs).getD e.1.2 zero)) c) := by
  induction es generalizing c with
  | nil => exact hc
  | cons e es ih =>
      apply ih
      · exact good_contribute c e.1.1 e.2 ((buildOf rs).getD e.1.2 zero) hc
          (hv e (by simp)) (buildOf_valid rs e.1.2)
      · intro e' h'
        exact hv e' (by simp [h'])

private theorem hProcess_good_aux (rs : List RRow) (hot : Int → Bool) :
    ∀ (ls : List LRow) (p : Std.HashMap (Int × Int) Totals) (c : Groups), Good c →
      Good (ls.foldl (hStep rs hot) (p, c)).2 := by
  intro ls
  induction ls with
  | nil => intro p c hc; simpa using hc
  | cons l ls ih =>
      intro p c hc
      cases hk : l.k with
      | none => simpa [hStep, hk] using ih p c hc
      | some k =>
          by_cases hhot : hot k
          · simpa [hStep, hk, hhot] using
              ih (p.insert (l.g, k) (plus (p.getD (l.g, k) zero) (weighted 1 l.v))) c hc
          · simpa [hStep, hk, hhot] using
              ih p (contribute c l.g (weighted 1 l.v) (rTotals rs k))
                (good_contribute c l.g (weighted 1 l.v) (rTotals rs k) hc
                  (valid_weighted 1 l.v) (valid_rTotals rs k))

private theorem hProcess_good (rs : List RRow) (hot : Int → Bool) :
    ∀ ls : List LRow, Good (hProcess rs hot ls).2 := by
  intro ls
  unfold hProcess
  exact hProcess_good_aux rs hot ls {} {} good_empty

private theorem hProcessBuild_good_aux (rs : List RRow) (hot : Int → Bool) :
    ∀ (ls : List LRow) (p : Std.HashMap (Int × Int) Totals) (c : Groups), Good c →
      Good (ls.foldl (fun (st : Std.HashMap (Int × Int) Totals × Groups) l => match l.k with
        | none => st
        | some k =>
            let left := weighted 1 l.v
            if hot k then
              (st.1.insert (l.g, k) (plus (st.1.getD (l.g, k) zero) left), st.2)
            else (st.1, contribute st.2 l.g left ((buildOf rs).getD k zero))) (p, c)).2 := by
  intro ls
  induction ls with
  | nil => intro p c hc; simpa using hc
  | cons l ls ih =>
      intro p c hc
      cases hk : l.k with
      | none => simpa [hk] using ih p c hc
      | some k =>
          by_cases hhot : hot k
          · simpa [hk, hhot] using
              ih (p.insert (l.g, k) (plus (p.getD (l.g, k) zero) (weighted 1 l.v))) c hc
          · simpa [hk, hhot] using
              ih p (contribute c l.g (weighted 1 l.v) ((buildOf rs).getD k zero))
                (good_contribute c l.g (weighted 1 l.v) ((buildOf rs).getD k zero) hc
                  (valid_weighted 1 l.v) (buildOf_valid rs k))

private theorem hProcessBuild_good (rs : List RRow) (hot : Int → Bool) :
    ∀ ls : List LRow, Good (hProcessBuild rs hot ls).2 := by
  intro ls
  unfold hProcessBuild
  exact hProcessBuild_good_aux rs hot ls {} {} good_empty

private theorem hybridGroups_good (ls : List LRow) (rs : List RRow) (hot : Int → Bool) :
    Good (hybridGroups ls rs hot) := by
  unfold hybridGroups
  dsimp
  change Good (finalizeBuild rs (hProcessBuild rs hot ls).1 (hProcessBuild rs hot ls).2)
  unfold finalizeBuild
  rw [Std.HashMap.fold_eq_foldl_toList]
  apply fold_entries_good
  · exact hProcessBuild_good rs hot ls
  · intro e he
    have hv := process_partial_valid_final rs hot ls
    rw [hProcess_fst_eq rs hot ls] at he
    exact hv e he


private def nnCount : List (Option Int) → Nat
  | [] => 0
  | none :: xs => nnCount xs
  | some _ :: xs => nnCount xs + 1

private def nnSum : List (Option Int) → Int
  | [] => 0
  | none :: xs => nnSum xs
  | some v :: xs => v + nnSum xs

private theorem nnSum_eq_zero_of_nnCount_eq_zero {xs : List (Option Int)}
    (h : nnCount xs = 0) : nnSum xs = 0 := by
  induction xs with
  | nil => simp [nnSum]
  | cons x xs ih =>
      cases x with
      | none => simpa [nnCount, nnSum] using ih h
      | some v => simp [nnCount] at h

private def sumStep (a v : Option Int) : Option Int :=
  match a, v with
  | none, y => y
  | x, none => x
  | some a, some b => some (a + b)

private theorem sumOptListAux (acc : Option Int) (xs : List (Option Int)) :
    xs.foldl sumStep acc =
      match acc with
      | none => if nnCount xs = 0 then none else some (nnSum xs)
      | some a => if nnCount xs = 0 then some a else some (a + nnSum xs) := by
  induction xs generalizing acc with
  | nil => cases acc <;> simp [nnCount, nnSum]
  | cons x xs ih =>
      cases acc with
      | none =>
          cases x with
          | none =>
              change xs.foldl sumStep none = (if nnCount xs = 0 then none else some (nnSum xs))
              exact ih (acc := none)
          | some v =>
              simp [sumStep, nnCount, nnSum]
              rw [ih (acc := some v)]
              by_cases h : nnCount xs = 0
              · rw [nnSum_eq_zero_of_nnCount_eq_zero h]
                simp [h]
              · simp [h]
      | some a =>
          cases x with
          | none =>
              change xs.foldl sumStep (some a) =
                (if nnCount xs = 0 then some a else some (a + nnSum xs))
              exact ih (acc := some a)
          | some v =>
              simp [sumStep, nnCount, nnSum]
              rw [ih (acc := some (a + v))]
              by_cases h : nnCount xs = 0
              · rw [nnSum_eq_zero_of_nnCount_eq_zero h]
                simp [h]
              · simp [h]
                omega

private theorem sumOptList_char (xs : List (Option Int)) :
    sumOptList xs = if nnCount xs = 0 then none else some (nnSum xs) := by
  change xs.foldl sumStep none = if nnCount xs = 0 then none else some (nnSum xs)
  exact sumOptListAux none xs

private theorem nnCount_eq_length (xs : List (Option Int)) :
    nnCount xs = (xs.filterMap id).length := by
  induction xs with
  | nil => simp [nnCount]
  | cons x xs ih => cases x <;> simp [nnCount, ih]

private theorem nnSum_eq_sum (xs : List (Option Int)) :
    nnSum xs = (xs.map fun v => v.getD 0).sum := by
  induction xs with
  | nil => simp [nnSum]
  | cons x xs ih => cases x <;> simp [nnSum, ih]

private theorem finish_eq_of_getD (h₁ h₂ : Groups) (g : Int)
    (heq : h₁.getD g (zero, zero) = h₂.getD g (zero, zero)) :
    finish h₁ g = finish h₂ g := by
  unfold finish
  rw [heq]

private theorem if_beq_zero_eq (n : Nat) (x : Int) :
    (if (n == 0) = true then (none : Option Int) else some x) = (if n = 0 then none else some x) := by
  by_cases hn : n = 0 <;> simp [hn]

private theorem finish_naive_ref (ls : List LRow) (rs : List RRow) (g : Int) :
    finish (naiveFold rs {} ls) g = refGroup ls rs g := by
  have hget := naiveFold_eq_refTotals ls rs g
  unfold finish
  rw [hget]
  unfold refGroup
  dsimp
  let ps := refJoinedPairs ls rs g
  let lv := ps.map fun p => p.1.v
  let rv := ps.map fun p => p.2.v
  change (let lT := summarize lv
          let rT := summarize rv
          (lT.cnt, (if lT.nn == 0 then none else some lT.sum),
            (if rT.nn == 0 then none else some rT.sum))) =
         (ps.length, sumOptList lv, sumOptList rv)
  rw [summarize_char, summarize_char]
  rw [sumOptList_char, sumOptList_char]
  rw [nnCount_eq_length, nnSum_eq_sum, nnCount_eq_length, nnSum_eq_sum]
  dsimp [lv, rv]
  let n1 := (List.filterMap id (List.map (fun p : LRow × RRow => p.1.v) ps)).length
  let n2 := (List.filterMap id (List.map (fun p : LRow × RRow => p.2.v) ps)).length
  let s1 := (List.map ((fun v => v.getD 0) ∘ fun p : LRow × RRow => p.1.v) ps).sum
  let s2 := (List.map ((fun v => v.getD 0) ∘ fun p : LRow × RRow => p.2.v) ps).sum
  rw [Prod.mk.injEq]
  rw [Prod.mk.injEq]
  constructor
  · simp
  constructor
  · simpa [List.map_map, Function.comp_apply] using if_beq_zero_eq
      (List.filterMap id (List.map (fun p : LRow × RRow => p.1.v) ps)).length
      (List.map ((fun v => v.getD 0) ∘ fun p : LRow × RRow => p.1.v) ps).sum
  · simpa [List.map_map, Function.comp_apply] using if_beq_zero_eq
      (List.filterMap id (List.map (fun p : LRow × RRow => p.2.v) ps)).length
      (List.map ((fun v => v.getD 0) ∘ fun p : LRow × RRow => p.2.v) ps).sum

theorem SkewHybrid_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates.SkewHybrid.Goal := by
  intro ls rs g hot
  constructor
  · rw [finish_eq_of_getD (hybridGroups ls rs hot) (naiveFold rs {} ls) g
      (hybridGroups_getD ls rs hot g)]
    exact finish_naive_ref ls rs g
  · have hgood := hybridGroups_good ls rs hot g
    rw [hgood]
    have hcnt : (hybridGroups ls rs hot).getD g (zero, zero) =
        (naiveFold rs {} ls).getD g (zero, zero) := hybridGroups_getD ls rs hot g
    rw [hcnt]
    have hnf := naiveFold_eq_refTotals ls rs g
    have hcntNat : ((naiveFold rs {} ls).getD g (zero, zero)).1.cnt =
        (refJoinedPairs ls rs g).length := by
      rw [hnf]
      dsimp
      rw [summarize_char]
      simp
    have href : (refGroup ls rs g).1 = (refJoinedPairs ls rs g).length := by rfl
    by_cases hp : 0 < ((naiveFold rs {} ls).getD g (zero, zero)).1.cnt
    · have hq : 0 < (refGroup ls rs g).1 := by
        rw [href, ← hcntNat]
        exact hp
      simp [hp, hq]
    · have hq : ¬0 < (refGroup ls rs g).1 := by
        intro hq
        apply hp
        have hlen : 0 < (refJoinedPairs ls rs g).length := by
          simpa [href] using hq
        simpa [hcntNat] using hlen
      simp [hp, hq]
end TinylambOpt.ResearchProofs.AnalyticsWorkloads.HotKeyAndEpochAggregates

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join_test.cpp
-- executor/detail/columnar_tlcs_block.cpp
-- executor/detail/typed_stream_aggregate.cpp
-- executor/detail/typed_stream_aggregate_direct.cpp
-- executor/detail/typed_stream_aggregate_partitioned.cpp
-- executor/detail/typed_stream_aggregate_test.cpp
