import TinylambOpt.ResearchGoals.AnalyticsWorkloads.SocialGraphJoinWeights

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.

Proved

- `ActiveFriends` — `∀ es active country, let (groups,reads) := pull (index es) active
  groups.getD country 0 = (es.flatMap (fun (person,c) => (active.filter (· ==
  person)).map fun _ => c)).count country ∧ reads = (es.countP fun e =>
  active.contains e.1) ∧ reads ≤ es.length`
- `DirectoryDegree` — `∀ es width ps label, (∀ e ∈ es, e.1 < width) → degreeAvg
  (offsets es width) ps label = summarize (ps.flatMap fun (id,c,age) => if c == label
  then (es.filter (fun e => e.1 == id)).map (fun _ => age) else [])`
- `ForumRuns` — `∀ members rows key, let (groups,reads) := carried members rows
  groups.getD key zero = summarize (rows.flatMap fun (f,y,v) => if (f,y) == key then
  List.replicate (members f) v else []) ∧ reads = (rows.zipIdx.countP fun (r,i) => i
  == 0 || (rows[i-1]?.map Prod.fst) != some r.1)`
- `ForwardReplies` — `∀ rs year, (rs.map Reply.id).Nodup → (rs.foldl arrive
  {}).groups.getD year zero = replyRef rs year`
- `LateForumWeight` — `∀ fs ms k, afterGrouping fs ms k = summarize (fs.flatMap fun f
  => if f != k then [] else (ms.filter (fun m => m.forum == some f && m.year == 2012
  && m.parent.isSome)).map ForumMessage.score)`
-/

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.ForumRunsAndGraphDegrees

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.SocialGraphJoinWeights
open TinylambOpt.ResearchGoals.RecursiveDelta (index)

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  rcases a with ⟨ac, an, as⟩
  rcases b with ⟨bc, bn, bs⟩
  rcases c with ⟨cc, cn, cs⟩
  simp [plus]
  <;> omega

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  rcases a with ⟨ac, an, as⟩
  simp [plus, zero]

private theorem foldl_summarize (acc : Totals) (xs : List (Option Int)) :
    xs.foldl (fun a v => plus a (weighted 1 v)) acc = plus acc (summarize xs) := by
  induction xs generalizing acc with
  | nil =>
      rcases acc with ⟨ac, an, as⟩
      simp [summarize, plus, zero]
  | cons x xs ih =>
      rw [List.foldl_cons, ih]
      change plus (plus acc (weighted 1 x)) (summarize xs) =
        plus acc (List.foldl (fun a v => plus a (weighted 1 v)) (plus zero (weighted 1 x)) xs)
      rw [plus_zero_left, ih, plus_assoc]

private theorem summarize_append (xs ys : List (Option Int)) :
    summarize (xs ++ ys) = plus (summarize xs) (summarize ys) := by
  change (xs ++ ys).foldl (fun a v => plus a (weighted 1 v)) zero = _
  rw [List.foldl_append, foldl_summarize]
  rfl

private theorem summarize_replicate (n : Nat) (v : Option Int) :
    summarize (List.replicate n v) = weighted n v := by
  induction n with
  | zero => simp [summarize, weighted, zero]
  | succ n ih =>
      rw [List.replicate_succ]
      change summarize (v :: List.replicate n v) = weighted (n + 1) v
      rw [summarize, List.foldl_cons, plus_zero_left, foldl_summarize, ih]
      cases v with
      | none =>
          simp [weighted, plus]
          omega
      | some z =>
          simp [weighted, plus]
          rw [Int.add_mul, Int.one_mul, Int.add_comm]
          constructor <;> omega

private theorem weighted_fold_getD (m : Std.HashMap Nat Nat) (cs : List Nat) (k w : Nat) :
    (cs.foldl (fun h c => h.insert c (h.getD c 0 + w)) m).getD k 0 =
      m.getD k 0 + cs.count k * w := by
  induction cs generalizing m with
  | nil => simp
  | cons c cs ih =>
      simp only [List.foldl_cons]
      rw [ih, Std.HashMap.getD_insert]
      by_cases h : c == k
      · have hc : c = k := by simpa using h
        subst c
        simp [Nat.succ_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · rw [ite_eq_right h]
        simp [h, List.count_cons]

private def feed (incoming : Std.HashMap Nat (List Nat))
    (s : Std.HashMap Nat Nat × Nat) (p : Nat × Nat) : Std.HashMap Nat Nat × Nat :=
  let (person, w) := p
  let block := incoming.getD person []
  (block.foldl (fun h c => h.insert c (h.getD c 0 + w)) s.1, s.2 + block.length)

private theorem feed_getD (incoming) (s) (x w k : Nat) :
    (feed incoming s (x, w)).1.getD k 0 = s.1.getD k 0 + w * (incoming.getD x []).count k := by
  simp [feed, weighted_fold_getD, Nat.mul_comm]

private def wcount (incoming : Std.HashMap Nat (List Nat)) (k : Nat) (l : List (Nat × Nat)) : Nat :=
  (l.flatMap (fun p => (incoming.getD p.1 []).filter (fun c => c == k) |>.map (fun _ => p.2))).sum

private theorem map_const_sum (l : List Nat) (w : Nat) : (l.map (fun _ => w)).sum = l.length * w := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.succ_mul, Nat.add_comm, Nat.add_left_comm]

private theorem wcount_cons (incoming) (k x w : Nat) (l : List (Nat × Nat)) :
    wcount incoming k ((x, w) :: l) = w * (incoming.getD x []).count k + wcount incoming k l := by
  simp [wcount, map_const_sum, ← List.count_eq_length_filter, Nat.mul_comm]

private theorem fold_getD_eq (incoming) (l : List (Nat × Nat)) (m : Std.HashMap Nat Nat) (N k : Nat) :
    (l.foldl (feed incoming) (m, N)).1.getD k 0 = m.getD k 0 + wcount incoming k l := by
  induction l generalizing m N with
  | nil => simp [wcount]
  | cons p l ih =>
      rcases p with ⟨x, w⟩
      rw [List.foldl_cons, ih, feed_getD]
      change m.getD k 0 + w * (incoming.getD x []).count k + wcount incoming k l =
        m.getD k 0 + wcount incoming k ((x, w) :: l)
      rw [wcount_cons]
      omega

private theorem fold_snd_eq (incoming) (l : List (Nat × Nat)) (m : Std.HashMap Nat Nat) (N : Nat) :
    (l.foldl (feed incoming) (m, N)).2 = N + (l.flatMap (fun p => incoming.getD p.1 [])).length := by
  induction l generalizing m N with
  | nil => simp
  | cons p l ih =>
      rw [List.foldl_cons, ih]
      rcases p with ⟨x, w⟩
      simp [feed, Nat.add_assoc]

private theorem wcount_perm (incoming) (k) {l₁ l₂ : List (Nat × Nat)} (h : List.Perm l₁ l₂) :
    wcount incoming k l₁ = wcount incoming k l₂ := by
  exact List.Perm.sum_nat (h.flatMap_right (fun p => (incoming.getD p.1 []).filter (fun c => c == k) |>.map (fun _ => p.2)))

private theorem flatlen_perm (incoming : Std.HashMap Nat (List Nat)) {l₁ l₂ : List (Nat × Nat)} (h : List.Perm l₁ l₂) :
    (l₁.flatMap (fun p => incoming.getD p.1 [])).length = (l₂.flatMap (fun p => incoming.getD p.1 [])).length := by
  exact List.Perm.length_eq (h.flatMap_right (fun p => incoming.getD p.1 []))

private theorem filter_key_eq_single {l : List (Nat × Nat)}
    (hkeys : l.Pairwise (fun a b => (a.1 == b.1) = false)) (k v : Nat)
    (hmem : (k, v) ∈ l) : l.filter (fun p => p.1 == k) = [(k, v)] := by
  induction l with
  | nil => simp at hmem
  | cons p ps ih =>
      simp only [List.pairwise_cons] at hkeys
      rcases hkeys with ⟨hnot, hps⟩
      cases hmem with
      | head =>
          simp
          intro a b hab haeq
          have hbad := hnot (a, b) hab
          simp [haeq] at hbad
      | tail _ htail =>
          by_cases hp : p.1 == k
          · have hbad := hnot (k, v) htail
            simp [hp] at hbad
          · simpa [hp] using ih hps htail

private theorem filter_key_eq (H : Std.HashMap Nat Nat) (x : Nat) :
    H.toList.filter (fun p => p.1 == x) =
      if h : x ∈ H then [(x, H.getD x 0)] else [] := by
  by_cases hx : x ∈ H
  · simp only [dif_pos hx]
    have hmem : (x, H.getD x 0) ∈ H.toList := by
      rw [Std.HashMap.mem_toList_iff_getElem?_eq_some]
      exact Std.HashMap.getElem?_eq_some_getD hx
    exact filter_key_eq_single (Std.HashMap.distinct_keys_toList (m := H)) x (H.getD x 0) hmem
  · simp only [dif_neg hx]
    rw [List.filter_eq_nil_iff]
    intro p hp hpkey
    have hpx : p.1 = x := by simpa using hpkey
    have hxp : x == p.1 := by simp [hpx]
    have hentry : (x, p.2) ∈ H.toList := by
      rw [Std.HashMap.mem_toList_iff_getElem?_eq_some]
      rw [Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList]
      exact ⟨p.1, hxp, hp⟩
    have hs : (H[x]?).isSome := by
      rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hentry
      simp [hentry]
    exact hx (Std.HashMap.isSome_getElem?_iff_mem.mp hs)

private theorem histogram_getD_nat2 (xs : List Nat) (k : Nat) :
    (histogram xs).getD k 0 = xs.count k := by
  simpa [histogram] using weighted_fold_getD ({} : Std.HashMap Nat Nat) xs k 1

private theorem beq_comm_nat (a b : Nat) : (a == b) = (b == a) := by
  by_cases h : a == b
  · have hab : a = b := by simpa using h
    simp [hab]
  · have hab : a ≠ b := by intro heq; exact h (by simp [heq])
    have hba : b ≠ a := by intro heq; exact hab heq.symm
    simp [h, hab, hba]

private theorem toList_nodup (m : Std.HashMap Nat Nat) : m.toList.Nodup := by
  rw [List.nodup_iff_pairwise_ne]
  exact (Std.HashMap.distinct_keys_toList (m := m)).imp (by
    intro a b h
    intro heq
    subst b
    simp at h)

private theorem filter_key_eq_left (H : Std.HashMap Nat Nat) (x : Nat) :
    H.toList.filter (fun p => x == p.1) =
      if h : x ∈ H then [(x, H.getD x 0)] else [] := by
  have h0 := filter_key_eq H x
  by_cases hx : x ∈ H
  · rw [dif_pos hx] at h0 ⊢
    rw [← h0]
    apply List.filter_congr
    intro p hp
    exact beq_comm_nat x p.1
  · rw [dif_neg hx] at h0 ⊢
    rw [← h0]
    apply List.filter_congr
    intro p hp
    exact beq_comm_nat x p.1

private theorem nodup_eraseDups : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      simp only [List.nodup_cons]
      constructor
      · simp
      · exact nodup_eraseDups (as.filter fun b => !b == a)
termination_by l => l.length
decreasing_by
  have hle : (List.filter (fun b => !b == a) as).length ≤ as.length := List.length_filter_le _ _
  simp only [List.length_cons]
  omega

private theorem count_of_mem_nodup {α : Type} [DecidableEq α] {a : α} {l : List α}
    (hnd : l.Nodup) (h : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => simp at h
  | cons b bs ih =>
      rw [List.nodup_cons] at hnd
      rcases hnd with ⟨hb, hbs⟩
      by_cases hba : b == a
      · have heq : b = a := by simpa using hba
        subst b
        simpa [List.count_cons, List.count_eq_zero_of_not_mem hb]
      · cases h with
        | head => exact (hba (by simp)).elim
        | tail _ ht =>
            simpa [List.count_cons, hba] using ih hbs ht

private theorem perm_eraseDups_cons_of_mem (x : Nat) {as : List Nat} (h : x ∈ as) :
    List.Perm (x :: as).eraseDups as.eraseDups := by
  rw [List.perm_iff_count]
  intro a
  by_cases ha : a ∈ as
  · have h1 : a ∈ (x :: as).eraseDups := by simp [ha]
    have h2 : a ∈ as.eraseDups := by simp [ha]
    rw [count_of_mem_nodup (nodup_eraseDups (x :: as)) h1,
      count_of_mem_nodup (nodup_eraseDups as) h2]
  · have hax : a ≠ x := by
      intro heq
      exact ha (by simpa [heq] using h)
    have h1 : a ∉ (x :: as).eraseDups := by simp [hax, ha]
    have h2 : a ∉ as.eraseDups := by simp [ha]
    rw [List.count_eq_zero_of_not_mem h1, List.count_eq_zero_of_not_mem h2]

private theorem toList_value (m : Std.HashMap Nat Nat) {p : Nat × Nat} (hp : p ∈ m.toList) :
    p.2 = m.getD p.1 0 := by
  rw [Std.HashMap.mem_toList_iff_getElem?_eq_some] at hp
  rw [Std.HashMap.getD_eq_getD_getElem?, hp]
  simp

private theorem mem_foldl_hist (m0 : Std.HashMap Nat Nat) (xs : List Nat) {x : Nat}
    (h : x ∈ m0 ∨ 0 < xs.count x) :
    x ∈ xs.foldl (fun m y => m.insert y (m.getD y 0 + 1)) m0 := by
  induction xs generalizing m0 with
  | nil => simpa using h
  | cons y ys ih =>
      simp only [List.foldl_cons]
      apply ih (m0 := m0.insert y (m0.getD y 0 + 1))
      rcases h with hmem | hc
      · exact Or.inl (Std.HashMap.mem_insert.mpr (Or.inr hmem))
      · by_cases hy : y == x
        · exact Or.inl (Std.HashMap.mem_insert.mpr (Or.inl hy))
        · exact Or.inr (by simpa [List.count_cons, hy] using hc)

private theorem mem_histogram_of_count_pos (xs : List Nat) {x : Nat} (h : 0 < xs.count x) :
    x ∈ histogram xs := by
  simpa [histogram] using mem_foldl_hist ({} : Std.HashMap Nat Nat) xs (Or.inr h)

private theorem histogram_toList_mem (xs : List Nat) {x : Nat} (h : 0 < xs.count x) :
    (x, xs.count x) ∈ (histogram xs).toList := by
  rw [Std.HashMap.mem_toList_iff_getElem?_eq_some]
  have hmem := mem_histogram_of_count_pos xs h
  have hg := Std.HashMap.getElem?_eq_some_getD (m := histogram xs) (fallback := 0) hmem
  simpa [histogram_getD_nat2] using hg

private theorem allPos_foldl_hist (m0 : Std.HashMap Nat Nat) (xs : List Nat)
    (h0 : ∀ p ∈ m0.toList, 0 < p.2) :
    ∀ p ∈ (xs.foldl (fun m y => m.insert y (m.getD y 0 + 1)) m0).toList, 0 < p.2 := by
  induction xs generalizing m0 with
  | nil => exact h0
  | cons y ys ih =>
      apply ih
      intro p hp
      have hperm := Std.HashMap.toList_insert_perm (m := m0) (k := y) (v := m0.getD y 0 + 1)
      have hp' : p ∈ (y, m0.getD y 0 + 1) :: m0.toList.filter (fun q => !(y == q.1)) :=
        hperm.mem_iff.mp hp
      rw [List.mem_cons] at hp'
      rcases hp' with hphead | hptail
      · have hpeq : p = (y, m0.getD y 0 + 1) := by simpa using hphead
        subst p
        simp
      · exact h0 p (List.mem_filter.mp hptail).1

private theorem histogram_allPos (xs : List Nat) :
    ∀ p ∈ (histogram xs).toList, 0 < p.2 := by
  unfold histogram
  apply allPos_foldl_hist
  simp [Std.HashMap.toList_empty]

private theorem nodup_filter (p : Nat × Nat → Bool) {l : List (Nat × Nat)} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at h
      rcases h with ⟨ha, has⟩
      by_cases hp : p a
      · rw [List.filter_cons_of_pos hp]
        rw [List.nodup_cons]
        constructor
        · intro hm
          exact ha (List.mem_filter.mp hm).1
        · exact ih has
      · rw [List.filter_cons_of_neg hp]
        exact ih has

private theorem histogram_toList_cons_perm (x : Nat) (xs : List Nat) :
    List.Perm (histogram (x :: xs)).toList
      ((x, xs.count x + 1) :: (histogram xs).toList.filter (fun p => !(x == p.1))) := by
  rw [List.perm_iff_count]
  intro p
  rcases p with ⟨k, v⟩
  have hnod1 : ((histogram (x :: xs)).toList).Nodup := toList_nodup _
  have hnod2 : ((x, xs.count x + 1) :: (histogram xs).toList.filter (fun p => !(x == p.1))).Nodup := by
    rw [List.nodup_cons]
    constructor
    · simp
    · exact nodup_filter _ (toList_nodup _)
  have hmiff : (k, v) ∈ (histogram (x :: xs)).toList ↔
      (k, v) ∈ (x, xs.count x + 1) :: (histogram xs).toList.filter (fun p => !(x == p.1)) := by
    constructor
    · intro hp
      have hv0 := toList_value (histogram (x :: xs)) hp
      change v = (histogram (x :: xs)).getD k 0 at hv0
      rw [histogram_getD_nat2] at hv0
      by_cases hkx : k == x
      · have hkeq : k = x := by simpa using hkx
        subst k
        simp [List.count_cons, hv0]
      · have hxk : !(x == k) = true := by simpa [beq_comm_nat] using hkx
        have hxk_ne : x ≠ k := by
          intro heq
          have htrue : (x == k) = true := by simp [heq]
          simp [htrue] at hxk
        have hv : v = xs.count k := by simpa [List.count_cons, hxk_ne] using hv0
        have hvpos : 0 < v := histogram_allPos (x :: xs) (k, v) hp
        have hvxspos : 0 < xs.count k := by simpa [hv] using hvpos
        have hmemxs : (k, xs.count k) ∈ (histogram xs).toList := histogram_toList_mem xs hvxspos
        rw [List.mem_cons]
        right
        rw [hv]
        exact List.mem_filter.mpr ⟨hmemxs, hxk⟩
    · intro hp
      rw [List.mem_cons] at hp
      rcases hp with hphead | hptail
      · have hkv : (k, v) = (x, xs.count x + 1) := by simpa using hphead
        rcases hkv with ⟨rfl, rfl⟩
        have hpos : 0 < (x :: xs).count x := by simp [List.count_cons]
        simpa [List.count_cons] using histogram_toList_mem (x :: xs) (x := x) hpos
      · have hpair := List.mem_filter.mp hptail
        rcases hpair with ⟨hpmem, hpfil⟩
        have hv0 := toList_value (histogram xs) hpmem
        change v = (histogram xs).getD k 0 at hv0
        rw [histogram_getD_nat2] at hv0
        have hv : v = xs.count k := hv0
        have hxk_ne : x ≠ k := by
          intro heq
          have htrue : (x == k) = true := by simp [heq]
          simp [htrue] at hpfil
        have hvpos : 0 < v := histogram_allPos xs (k, v) hpmem
        have hvxspos : 0 < (x :: xs).count k := by
          simpa [List.count_cons, hxk_ne, hv] using hvpos
        have hmem := histogram_toList_mem (x :: xs) hvxspos
        simpa [List.count_cons, hxk_ne, hv] using hmem
  by_cases hm : (k, v) ∈ (histogram (x :: xs)).toList
  · rw [count_of_mem_nodup (l := (histogram (x :: xs)).toList) hnod1 hm,
      count_of_mem_nodup (l := (x, xs.count x + 1) :: (histogram xs).toList.filter (fun p => !(x == p.1))) hnod2 (hmiff.mp hm)]
  · rw [List.count_eq_zero_of_not_mem hm,
      List.count_eq_zero_of_not_mem (by simpa [hmiff] using hm)]

private theorem flatlen_eraseDups_perm (incoming : Std.HashMap Nat (List Nat))
    {a b : List Nat} (h : List.Perm a b) :
    (a.flatMap (fun x => incoming.getD x [])).length =
      (b.flatMap (fun x => incoming.getD x [])).length := by
  exact List.Perm.length_eq (h.flatMap_right (fun x => incoming.getD x []))

private theorem count_pos_of_mem (x : Nat) {xs : List Nat} (h : x ∈ xs) : 0 < xs.count x := by
  induction xs with
  | nil => simp at h
  | cons a as ih =>
      rw [List.mem_cons] at h
      rcases h with hx | hx
      · subst a
        simp [List.count_cons]
      · by_cases hax : a == x
        · have heq : a = x := by simpa using hax
          subst a
          simp [List.count_cons]
        · simpa [List.count_cons, hax] using ih hx

private theorem not_mem_histogram_of_not_count_pos (xs : List Nat) {x : Nat}
    (h : xs.count x = 0) : x ∉ histogram xs := by
  intro hxH
  have hg := Std.HashMap.getElem?_eq_some_getD (m := histogram xs) (fallback := 0) hxH
  have hp : (x, (histogram xs).getD x 0) ∈ (histogram xs).toList := by
    rw [Std.HashMap.mem_toList_iff_getElem?_eq_some]
    exact hg
  have hvpos := histogram_allPos xs (x, (histogram xs).getD x 0) hp
  rw [histogram_getD_nat2, h] at hvpos
  omega

private theorem pull_fold_getD (incoming : Std.HashMap Nat (List Nat)) (xs : List Nat)
    (m : Std.HashMap Nat Nat) (N k : Nat) :
    ((histogram xs).toList.foldl (feed incoming) (m, N)).1.getD k 0 =
      m.getD k 0 + (xs.flatMap (fun x => incoming.getD x [])).count k := by
  induction xs generalizing m N with
  | nil => simp [histogram]
  | cons x xs ih =>
      let c := xs.count x
      let B := incoming.getD x []
      let L := (histogram xs).toList
      let Lx := L.filter (fun p => !(x == p.1))
      have hperm := histogram_toList_cons_perm x xs
      have hperm' : List.Perm (histogram (x :: xs)).toList ((x, c + 1) :: Lx) := by
        simpa [c, L, Lx] using hperm
      rw [fold_getD_eq, wcount_perm incoming k hperm', wcount_cons]
      change m.getD k 0 + ((c + 1) * B.count k + wcount incoming k Lx) =
        m.getD k 0 + ((x :: xs).flatMap (fun y => incoming.getD y [])).count k
      have hih := ih m N
      rw [fold_getD_eq] at hih
      change m.getD k 0 + wcount incoming k L = m.getD k 0 +
        (xs.flatMap (fun y => incoming.getD y [])).count k at hih
      have hwL : wcount incoming k L = (xs.flatMap (fun y => incoming.getD y [])).count k := by omega
      have hflat : ((x :: xs).flatMap (fun y => incoming.getD y [])).count k =
          B.count k + (xs.flatMap (fun y => incoming.getD y [])).count k := by
        simp [B]
      rw [hflat, ← hwL]
      by_cases hx : x ∈ xs
      · have hpos : 0 < c := by simpa [c] using count_pos_of_mem x hx
        have hmem : (x, c) ∈ L := by
          simpa [c, L] using histogram_toList_mem xs (x := x) hpos
        have hxH : x ∈ histogram xs := mem_histogram_of_count_pos xs hpos
        have hfilt := filter_key_eq_left (histogram xs) x
        rw [dif_pos hxH] at hfilt
        have hfilt' : L.filter (fun p => x == p.1) = [(x, c)] := by simpa [L, c, histogram_getD_nat2] using hfilt
        have hpermL : List.Perm L ((x, c) :: Lx) := by
          have hp1 := List.perm_cons_erase (a := (x, c)) hmem
          have hLnod : L.Nodup := by simpa [L] using toList_nodup (histogram xs)
          have herase : L.erase (x, c) = Lx := by
            rw [List.Nodup.erase_eq_filter hLnod]
            apply congrArg (fun t => t)
            apply List.filter_congr
            intro p hp
            by_cases hpk : x == p.1
            · have hpin : p ∈ L.filter (fun q => x == q.1) := List.mem_filter.mpr ⟨hp, hpk⟩
              rw [hfilt'] at hpin
              have peq : p = (x, c) := by simpa using hpin
              simp [peq, hpk]
            · have hpneq : p != (x, c) := by
                by_cases hpeq : p == (x, c)
                · have heq : p = (x, c) := by simpa using hpeq
                  have htrue : (x == p.1) = true := by simp [heq]
                  exact (hpk htrue).elim
                · simpa using hpeq
              simp [hpneq, hpk]
          simpa [herase] using hp1
        have hwperm := wcount_perm incoming k hpermL
        rw [wcount_cons] at hwperm
        rw [Nat.succ_mul]
        rw [hwperm]
        simp [B, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · have hc0 : c = 0 := by simpa [c, List.count_eq_zero_of_not_mem hx]
        have hxH : x ∉ histogram xs := not_mem_histogram_of_not_count_pos xs (by simpa [c] using hc0)
        have hfilt := filter_key_eq_left (histogram xs) x
        rw [dif_neg hxH] at hfilt
        have hLx_eq : Lx = L := by
          apply List.filter_eq_self.mpr
          intro p hp
          have hpnot : p ∉ L.filter (fun q => x == q.1) := by
            rw [show L.filter (fun q => x == q.1) = [] by simpa [L] using hfilt]
            simp
          by_cases ht : (x == p.1) = true
          · exact (hpnot (List.mem_filter.mpr ⟨hp, ht⟩)).elim
          · simpa using ht
        rw [hLx_eq, hc0]
        simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem histogram_toList_erase_perm (x : Nat) (xs : List Nat) (hx : x ∈ xs) :
    List.Perm (histogram xs).toList
      ((x, xs.count x) :: (histogram xs).toList.filter (fun p => !(x == p.1))) := by
  let c := xs.count x
  let L := (histogram xs).toList
  let Lx := L.filter (fun p => !(x == p.1))
  have hpos : 0 < c := by simpa [c] using count_pos_of_mem x hx
  have hmem : (x, c) ∈ L := by simpa [c, L] using histogram_toList_mem xs (x := x) hpos
  have hxH : x ∈ histogram xs := mem_histogram_of_count_pos xs hpos
  have hfilt := filter_key_eq_left (histogram xs) x
  rw [dif_pos hxH] at hfilt
  have hfilt' : L.filter (fun p => x == p.1) = [(x, c)] := by
    simpa [L, c, histogram_getD_nat2] using hfilt
  have hp1 := List.perm_cons_erase (a := (x, c)) hmem
  have hLnod : L.Nodup := by simpa [L] using toList_nodup (histogram xs)
  have herase : L.erase (x, c) = Lx := by
    rw [List.Nodup.erase_eq_filter hLnod]
    apply List.filter_congr
    intro p hp
    by_cases hpk : x == p.1
    · have hpin : p ∈ L.filter (fun q => x == q.1) := List.mem_filter.mpr ⟨hp, hpk⟩
      rw [hfilt'] at hpin
      have peq : p = (x, c) := by simpa using hpin
      simp [peq, hpk]
    · have hpneq : p != (x, c) := by
        by_cases hpeq : p == (x, c)
        · have heq : p = (x, c) := by simpa using hpeq
          have htrue : (x == p.1) = true := by simp [heq]
          exact (hpk htrue).elim
        · simpa using hpeq
      simp [hpneq, hpk]
  simpa [L, Lx, c, herase] using hp1

private theorem histogram_filter_eq_self_of_not_mem (x : Nat) (xs : List Nat) (hx : x ∉ xs) :
    (histogram xs).toList.filter (fun p => !(x == p.1)) = (histogram xs).toList := by
  let L := (histogram xs).toList
  have hxH : x ∉ histogram xs := not_mem_histogram_of_not_count_pos xs (by simpa using List.count_eq_zero_of_not_mem hx)
  have hfilt := filter_key_eq_left (histogram xs) x
  rw [dif_neg hxH] at hfilt
  have hfilt' : L.filter (fun q => x == q.1) = [] := by simpa [L] using hfilt
  apply List.filter_eq_self.mpr
  intro p hp
  have hpnot : p ∉ L.filter (fun q => x == q.1) := by rw [hfilt']; simp
  by_cases ht : (x == p.1) = true
  · exact (hpnot (List.mem_filter.mpr ⟨hp, ht⟩)).elim
  · simpa using ht

private theorem pull_fold_snd (incoming : Std.HashMap Nat (List Nat)) (xs : List Nat)
    (m : Std.HashMap Nat Nat) (N : Nat) :
    ((histogram xs).toList.foldl (feed incoming) (m, N)).2 =
      N + (xs.eraseDups.flatMap (fun x => incoming.getD x [])).length := by
  induction xs generalizing m N with
  | nil => simp [histogram]
  | cons x xs ih =>
      let c := xs.count x
      let B := incoming.getD x []
      let L := (histogram xs).toList
      let Lx := L.filter (fun p => !(x == p.1))
      have hperm := histogram_toList_cons_perm x xs
      have hperm' : List.Perm (histogram (x :: xs)).toList ((x, c + 1) :: Lx) := by
        simpa [c, L, Lx] using hperm
      rw [fold_snd_eq, flatlen_perm incoming hperm']
      change N + (((x, c + 1) :: Lx).flatMap (fun p => incoming.getD p.1 [])).length =
        N + ((x :: xs).eraseDups.flatMap (fun y => incoming.getD y [])).length
      simp only [List.flatMap_cons, List.length_cons]
      have hih := ih m N
      rw [fold_snd_eq] at hih
      change N + (L.flatMap (fun p => incoming.getD p.1 [])).length =
        N + (xs.eraseDups.flatMap (fun y => incoming.getD y [])).length at hih
      have hlenL : (L.flatMap (fun p => incoming.getD p.1 [])).length =
          (xs.eraseDups.flatMap (fun y => incoming.getD y [])).length := by omega
      by_cases hx : x ∈ xs
      · have hpermL := histogram_toList_erase_perm x xs hx
        have hpermL' : List.Perm L ((x, c) :: Lx) := by simpa [L, Lx, c] using hpermL
        have hlenperm := flatlen_perm incoming hpermL'
        change (L.flatMap (fun p => incoming.getD p.1 [])).length =
          (((x, c) :: Lx).flatMap (fun p => incoming.getD p.1 [])).length at hlenperm
        simp only [List.flatMap_cons, List.length_cons] at hlenperm
        have hlenLx : (Lx.flatMap (fun p => incoming.getD p.1 [])).length + B.length =
            (L.flatMap (fun p => incoming.getD p.1 [])).length := by
          rw [hlenperm]
          simp [B, Nat.add_comm, Nat.add_assoc]
        have hdp := perm_eraseDups_cons_of_mem x hx
        have hlendp := flatlen_eraseDups_perm incoming hdp
        change ((x :: xs).eraseDups.flatMap (fun y => incoming.getD y [])).length =
          (xs.eraseDups.flatMap (fun y => incoming.getD y [])).length at hlendp
        rw [hlendp]
        rw [← hlenL, ← hlenLx]
        simp [B, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · have hfilter := histogram_filter_eq_self_of_not_mem x xs hx
        have hLx_eq : Lx = L := by simpa [L, Lx] using hfilter
        have hfilterxs : (xs.filter fun b => !b == x) = xs := by
          apply List.filter_eq_self.mpr
          intro b hb
          by_cases hbxe : b == x
          · have heq : b = x := by simpa using hbxe
            exact (hx (by simpa [heq] using hb)).elim
          · simpa using hbxe
        have herase : (x :: xs).eraseDups = x :: xs.eraseDups := by
          rw [List.eraseDups_cons, hfilterxs]
        rw [herase, List.flatMap_cons]
        simp only [List.length_append]
        rw [hLx_eq, hlenL]

private theorem pull_eq_feed (incoming : Std.HashMap Nat (List Nat)) (active : List Nat) :
    pull incoming active = (histogram active).toList.foldl (feed incoming) (({} : Std.HashMap Nat Nat), 0) := by
  rfl

private theorem index_getD_general_count (es : List Edge) (m : Std.HashMap Nat (List Nat)) (x k : Nat) :
    ((es.foldl (fun m e => m.insert e.1 (e.2 :: m.getD e.1 [])) m).getD x []).count k =
      (m.getD x []).count k + ((es.filter fun e => e.1 == x).map Prod.snd).count k := by
  induction es generalizing m with
  | nil => simp
  | cons e es ih =>
      rcases e with ⟨u, v⟩
      simp only [List.foldl_cons]
      rw [ih (m := m.insert u (v :: m.getD u []))]
      rw [Std.HashMap.getD_insert]
      by_cases hux : u == x
      · have heq : u = x := by simpa using hux
        subst u
        simp [hux, List.filter_cons, List.map_cons, List.count_cons, Nat.add_assoc, Nat.add_comm]
        omega
      · simp [hux, List.filter_cons, List.map_cons, Nat.add_assoc]

private theorem index_getD_count (es : List Edge) (x k : Nat) :
    ((index es).getD x []).count k = ((es.filter fun e => e.1 == x).map Prod.snd).count k := by
  simpa [index] using index_getD_general_count es ({} : Std.HashMap Nat (List Nat)) x k

private theorem index_getD_general_length (es : List Edge) (m : Std.HashMap Nat (List Nat)) (x : Nat) :
    ((es.foldl (fun m e => m.insert e.1 (e.2 :: m.getD e.1 [])) m).getD x []).length =
      (m.getD x []).length + ((es.filter fun e => e.1 == x).map Prod.snd).length := by
  induction es generalizing m with
  | nil => simp
  | cons e es ih =>
      rcases e with ⟨u, v⟩
      simp only [List.foldl_cons]
      rw [ih (m := m.insert u (v :: m.getD u []))]
      rw [Std.HashMap.getD_insert]
      by_cases hux : u == x
      · have heq : u = x := by simpa using hux
        subst u
        simp [hux, List.filter_cons, List.map_cons, Nat.add_assoc, Nat.add_comm]
        omega
      · simp [hux, List.filter_cons, List.map_cons, Nat.add_assoc]

private theorem index_getD_length (es : List Edge) (x : Nat) :
    ((index es).getD x []).length = ((es.filter fun e => e.1 == x).map Prod.snd).length := by
  simpa [index] using index_getD_general_length es ({} : Std.HashMap Nat (List Nat)) x

private theorem countP_or_disjoint {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ a ∈ l, p a = true → q a = false) :
    l.countP (fun a => p a || q a) = l.countP p + l.countP q := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have hpq := h a (by simp)
      have ht := ih (by intro b hb hpb; exact h b (by simp [hb]) hpb)
      by_cases hpa : p a = true
      · have hqa : q a = false := hpq hpa
        simp [List.countP_cons, hpa, hqa, ht, Nat.add_comm, Nat.add_assoc]
        omega
      · have hqa : q a = true ∨ q a = false := by cases q a <;> simp
        cases hqa with
        | inl hqat => simp [hpa, hqat, ht, Nat.add_comm, Nat.add_assoc]
        | inr hqaf => simp [hpa, hqaf, ht]

private theorem nodup_flatMap_filter_length (d : List Nat) (hd : d.Nodup) (es : List Edge) :
    (d.flatMap (fun x => es.filter (fun e => e.1 == x))).length =
      es.countP (fun e => d.contains e.1) := by
  induction d with
  | nil => simp
  | cons x d ih =>
      rw [List.nodup_cons] at hd
      rcases hd with ⟨hxnot, hdnd⟩
      simp only [List.flatMap_cons, List.length_append]
      rw [ih hdnd]
      have hdis : ∀ e ∈ es, (e.1 == x) = true → d.contains e.1 = false := by
        intro e he hx
        have heq : e.1 = x := by simpa using hx
        simpa [heq, hxnot]
      have hcnt := countP_or_disjoint es (fun e => e.1 == x) (fun e => d.contains e.1) hdis
      have hcont : es.countP (fun e => (x :: d).contains e.1) =
          es.countP (fun e => e.1 == x || d.contains e.1) := by
        apply List.countP_congr
        intro e he
        simp [List.contains_cons]
      rw [hcont, hcnt]
      simp [List.countP_eq_length_filter]

private theorem count_flatMap_add_head (es : List Edge) (x : Nat) (xs : List Nat) (country : Nat) :
    (es.flatMap (fun e => ((x :: xs).filter (fun y => y == e.1)).map fun _ => e.2)).count country =
      ((es.filter fun e => e.1 == x).map Prod.snd).count country +
        (es.flatMap (fun e => (xs.filter (fun y => y == e.1)).map fun _ => e.2)).count country := by
  induction es with
  | nil => simp
  | cons e es ih =>
      rcases e with ⟨p, c⟩
      simp only [List.filter_cons] at ih ⊢
      by_cases hpx : p == x
      · have heq : p = x := by simpa using hpx
        subst p
        simp only [List.flatMap_cons, List.map_cons, hpx, ↓reduceIte, List.count_cons, List.count_append]
        rw [ih]
        omega
      · have hpbeq : (p == x) = false := by simpa using hpx
        have hxbeq : (x == p) = false := by
          rw [beq_comm_nat x p]
          exact hpbeq
        simp only [List.flatMap_cons, List.map_cons, hpbeq, hxbeq, Bool.false_eq_true,
          ↓reduceIte, List.count_cons, List.count_append]
        rw [ih]
        omega

private theorem count_flatMap_flip (es : List Edge) (active : List Nat) (country : Nat) :
    (active.flatMap (fun x => (es.filter fun e => e.1 == x).map Prod.snd)).count country =
      (es.flatMap (fun (person, c) => (active.filter (fun x => x == person)).map fun _ => c)).count country := by
  induction active with
  | nil =>
      have hempty : es.flatMap (fun _ : Edge => ([] : List Nat)) = [] := by
        induction es with
        | nil => rfl
        | cons e es ihe => simp [List.flatMap_cons, ihe]
      simp [hempty]
  | cons x xs ih =>
      rw [count_flatMap_add_head]
      simp only [List.flatMap_cons, List.count_append]
      rw [ih]

private theorem flatMap_length_congr {α β γ : Type} (l : List α) (f : α → List β) (g : α → List γ)
    (h : ∀ a ∈ l, (f a).length = (g a).length) :
    (l.flatMap f).length = (l.flatMap g).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp [List.flatMap_cons, List.length_append, h a (by simp),
        ih (by intro b hb; exact h b (by simp [hb]))]

private theorem flatMap_count_congr {α : Type} (l : List α) (f g : α → List Nat) (k : Nat)
    (h : ∀ a ∈ l, (f a).count k = (g a).count k) :
    (l.flatMap f).count k = (l.flatMap g).count k := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp [List.flatMap_cons, List.count_append, h a (by simp),
        ih (by intro b hb; exact h b (by simp [hb]))]

private theorem reads_eq_countP (es : List Edge) (active : List Nat) :
    (active.eraseDups.flatMap (fun x => (index es).getD x [])).length =
      es.countP (fun e => active.contains e.1) := by
  have hflat : (active.eraseDups.flatMap (fun x => (index es).getD x [])).length =
      (active.eraseDups.flatMap (fun x => es.filter (fun e => e.1 == x))).length := by
    exact flatMap_length_congr (active.eraseDups)
      (fun x => (index es).getD x []) (fun x => es.filter (fun e => e.1 == x))
      (by intro x hx; rw [index_getD_length es x]; simp)
  rw [hflat]
  have hnodes : active.eraseDups.Nodup := nodup_eraseDups active
  rw [nodup_flatMap_filter_length active.eraseDups hnodes es]
  apply List.countP_congr
  intro e he
  simp

theorem ActiveFriends_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.SocialGraphJoinWeights.ActiveFriends.Goal := by
  intro es active country
  rw [pull_eq_feed]
  constructor
  · rw [pull_fold_getD]
    simp only [Std.HashMap.getD_empty, Nat.zero_add]
    rw [show (active.flatMap (fun x => (index es).getD x [])).count country =
        (es.flatMap (fun (person, c) => (active.filter (fun x => x == person)).map fun _ => c)).count country by
      have hflat : (active.flatMap (fun x => (index es).getD x [])).count country =
          (active.flatMap (fun x => (es.filter fun e => e.1 == x).map Prod.snd)).count country := by
        exact flatMap_count_congr active
          (fun x => (index es).getD x []) (fun x => (es.filter fun e => e.1 == x).map Prod.snd) country
          (by intro x hx; exact index_getD_count es x country)
      rw [hflat]
      exact count_flatMap_flip es active country]
  · constructor
    · rw [pull_fold_snd]
      simp only [Nat.zero_add]
      exact reads_eq_countP es active
    · rw [pull_fold_snd]
      simp only [Nat.zero_add]
      rw [reads_eq_countP]
      exact List.countP_le_length

-- L2 -----------------------------------------------------------------------

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  rcases a with ⟨ac, an, as⟩
  simp [plus, zero]

private theorem weighted_zero (v : Option Int) : weighted 0 v = zero := by
  rcases v with _ | z <;> simp [weighted, zero]

private def carryStep (members : Nat → Nat)
    (s : Option Nat × Nat × Std.HashMap (Nat × Int) Totals × Nat) (r : Message) :=
  let (prev, w, st, n) := s
  let (f, y, v) := r
  let same := prev == some f
  let weight := if same then w else members f
  let updated := if weight == 0 then st else
    st.insert (f, y) (plus (st.getD (f, y) zero) (weighted weight v))
  (some f, weight, updated, n + (!same).toNat)

private theorem carried_eq (members : Nat → Nat) (rows : List Message) :
    carried members rows =
      (let (_,_,out,n) := rows.foldl (carryStep members) (none, 0, ({} : Std.HashMap (Nat × Int) Totals), 0)
       (out, n)) := by
  rfl

private def wOf (members : Nat → Nat) (prev : Option Nat) : Nat :=
  match prev with
  | none => 0
  | some f => members f

private def refRuns (members : Nat → Nat) (rows : List Message) (key : Nat × Int) : Totals :=
  summarize (rows.flatMap fun (f, y, v) =>
    if (f, y) == key then List.replicate (members f) v else [])

private theorem refRuns_nil (members : Nat → Nat) (key : Nat × Int) :
    refRuns members [] key = zero := by
  simp [refRuns, summarize, zero]

private theorem refRuns_cons (members : Nat → Nat) (f : Nat) (y : Int) (v : Option Int)
    (rows : List Message) (key : Nat × Int) :
    refRuns members ((f, y, v) :: rows) key =
      plus (if (f, y) == key then weighted (members f) v else zero)
        (refRuns members rows key) := by
  unfold refRuns
  rw [List.flatMap_cons]
  by_cases h : (f, y) == key
  · simp [h, summarize_append, summarize_replicate]
  · simp [h]
    rw [plus_zero_left]

private theorem step_getD (key : Nat × Int) (st : Std.HashMap (Nat × Int) Totals)
    (f : Nat) (y : Int) (v : Option Int) (weight : Nat) :
  (let updated := if weight == 0 then st else
      st.insert (f, y) (plus (st.getD (f, y) zero) (weighted weight v))
   updated.getD key zero) =
    plus (st.getD key zero) (if (f, y) == key then weighted weight v else zero) := by
  by_cases hw0 : weight == 0
  · have hw0' : weight = 0 := by simpa using hw0
    subst weight
    simp [hw0, weighted_zero, plus_zero_right]
  · by_cases hkey : (f, y) == key
    · have heq : (f, y) = key := by simpa using hkey
      subst key
      simp [hw0, hkey, Std.HashMap.getD_insert]
    · simp [hw0, hkey, Std.HashMap.getD_insert, plus_zero_right]

private theorem carried_groups_aux (members : Nat → Nat) (rows : List Message)
    (key : Nat × Int) (st : Std.HashMap (Nat × Int) Totals) (prev : Option Nat) (w n : Nat)
    (hw : w = wOf members prev) :
  ((rows.foldl (carryStep members) (prev, w, st, n)).2.2.1).getD key zero =
    plus (st.getD key zero) (refRuns members rows key) := by
  induction rows generalizing st prev w n with
  | nil => simp [refRuns_nil, plus_zero_right]
  | cons row rows ih =>
      rcases row with ⟨f, y, v⟩
      simp only [carryStep, List.foldl_cons]
      let same := prev == some f
      let weight := if same then w else members f
      let updated := if weight == 0 then st else
        st.insert (f, y) (plus (st.getD (f, y) zero) (weighted weight v))
      have hweight : weight = members f := by
        by_cases hs : same
        · have hp : prev = some f := by simpa [same] using hs
          simp [weight, hs, hw, wOf, hp]
        · simp [weight, hs]
      have hw' : weight = wOf members (some f) := by simp [wOf, hweight]
      rw [ih (st := updated) (prev := some f) (w := weight) (n := n + (!same).toNat) hw']
      have hstep := step_getD key st f y v weight
      rw [hstep]
      rw [refRuns_cons]
      rw [hweight]
      by_cases hkey : (f, y) == key
      · simp [hkey, plus_assoc]
      · simp [hkey, plus_assoc, plus_zero_left]

private theorem zipIdx_add (rows : List Message) (start : Nat) :
    rows.zipIdx (start + 1) = (rows.zipIdx start).map (fun p => (p.1, p.2 + 1)) := by
  induction rows generalizing start with
  | nil => rfl
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, ih]
      simp only [List.map_cons]

private theorem countP_map {α β : Type} (f : α → β) (p : β → Bool) (l : List α) :
    (l.map f).countP p = l.countP (fun x => p (f x)) := by
  induction l with
  | nil => rfl
  | cons x xs ih => rw [List.map_cons, List.countP_cons, List.countP_cons, ih]

private theorem countP_congr {α : Type} {p q : α → Bool} {l : List α}
    (h : ∀ x ∈ l, p x = q x) : l.countP p = l.countP q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons, h a (by simp),
        ih (by intro x hx; exact h x (by simp [hx]))]

private theorem cons_getElem_map_fst (row : Message) (rows : List Message) (i : Nat) :
    ((row :: rows)[i]?.map Prod.fst) =
      (if i == 0 then some row.1 else rows[i - 1]?.map Prod.fst) := by
  cases i <;> simp

private def probeAux (prev : Option Nat) (rows : List Message) : Nat :=
  rows.zipIdx.countP fun (r, i) =>
    (if i == 0 then prev else rows[i - 1]?.map Prod.fst) != some r.1

private theorem probeAux_cons (prev : Option Nat) (row : Message) (rows : List Message) :
    probeAux prev (row :: rows) =
      (if prev != some row.1 then 1 else 0) + probeAux (some row.1) rows := by
  unfold probeAux
  rw [List.zipIdx_cons, List.countP_cons]
  simp only [Nat.zero_eq, Nat.zero_sub, if_pos rfl, List.getElem?_cons_zero,
    Option.map_some]
  rw [zipIdx_add rows 0, countP_map, Nat.add_comm]
  apply congrArg (fun t => (if prev != some row.1 then 1 else 0) + t)
  apply countP_congr
  intro x hx
  rcases x with ⟨r, i⟩
  simp [cons_getElem_map_fst]

private theorem probeFold_aux (prev : Option Nat) (rows : List Message) (n : Nat) :
    (rows.foldl (fun (p, m) (f, _, _) => (some f, m + (!(p == some f)).toNat)) (prev, n)).2 =
      n + probeAux prev rows := by
  induction rows generalizing prev n with
  | nil => simp [probeAux]
  | cons row rows ih =>
      rcases row with ⟨f, y, v⟩
      simp only [List.foldl_cons]
      change (rows.foldl (fun (p, m) (f, _, _) => (some f, m + (!(p == some f)).toNat))
        (some f, n + (!(prev == some f)).toNat)).2 = _
      rw [ih (prev := some f) (n := n + (!(prev == some f)).toNat), probeAux_cons]
      simp only [Prod.fst]
      have h : (!(prev == some f)).toNat = (if prev != some f then 1 else 0) := by
        by_cases hpf : prev == some f
        · have hp : prev = some f := by simpa using hpf
          simp [hpf, hp]
        · have hp : prev ≠ some f := by intro heq; exact hpf (by simp [heq])
          simp [hpf, hp]
      rw [h]
      omega

private theorem carried_probes_fold (members : Nat → Nat) (rows : List Message)
    (st : Std.HashMap (Nat × Int) Totals) (prev : Option Nat) (w n : Nat) :
  (rows.foldl (carryStep members) (prev, w, st, n)).2.2.2 =
    (rows.foldl (fun (p, m) (f, _, _) => (some f, m + (!(p == some f)).toNat)) (prev, n)).2 := by
  induction rows generalizing st prev w n with
  | nil => rfl
  | cons row rows ih =>
      rcases row with ⟨f, y, v⟩
      simp only [carryStep, List.foldl_cons]
      apply ih

private theorem probeAux_none (rows : List Message) :
    probeAux none rows =
      rows.zipIdx.countP (fun (r, i) => i == 0 || rows[i - 1]?.map Prod.fst != some r.1) := by
  unfold probeAux
  apply countP_congr
  intro x hx
  rcases x with ⟨r, i⟩
  by_cases hi : i == 0 <;> simp [hi]

theorem ForumRuns_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.SocialGraphJoinWeights.ForumRuns.Goal := by
  intro members rows key
  rw [carried_eq]
  constructor
  · dsimp
    rw [carried_groups_aux members rows key ({} : Std.HashMap (Nat × Int) Totals) none 0 0]
    · simp [Std.HashMap.getD_empty, plus_zero_left, refRuns]
    · simp [wOf]
  · dsimp
    rw [carried_probes_fold]
    rw [probeFold_aux none rows 0, probeAux_none]
    simp

private def passes (m : ForumMessage) (f : Nat) : Bool :=
  m.forum == some f && m.year == 2012 && m.parent.isSome

private def block (ms : List ForumMessage) (f : Nat) : List (Option Int) :=
  (ms.filter (fun m => passes m f)).map ForumMessage.score

private theorem lateForumWeight_aux (fs : List Nat) (ms : List ForumMessage) (k : Nat) :
    summarize (fs.flatMap fun f => if f != k then [] else block ms f) =
      let a := summarize (block ms k)
      (⟨a.cnt * fs.count k, a.nn * fs.count k, a.sum * fs.count k⟩ : Totals) := by
  induction fs with
  | nil => simp [summarize, zero]
  | cons f fs ih =>
      by_cases hf : f == k
      · have hfeq : f = k := by simpa using hf
        subst f
        have hne : (k != k) = false := by simp
        simp only [List.flatMap_cons, List.count_cons, if_pos rfl, hne, Bool.false_eq_true,
          ↓reduceIte]
        rw [summarize_append, ih]
        simp only [hf, ↓reduceIte]
        rcases summarize (block ms k) with ⟨ac, an, as⟩
        change (⟨ac + ac * List.count k fs, an + an * List.count k fs,
          as + as * (List.count k fs : Int)⟩ : Totals) =
          ⟨ac * (List.count k fs + 1), an * (List.count k fs + 1),
            as * ((List.count k fs + 1 : Nat) : Int)⟩
        have hcast : ((List.count k fs + 1 : Nat) : Int) = (List.count k fs : Int) + 1 := by simp
        rw [hcast, Int.mul_add, Int.mul_one]
        simp [Nat.mul_add, Nat.mul_one, Int.add_comm] <;> omega
      · have hne : (f != k) = true := by simpa using hf
        rw [List.flatMap_cons, List.count_cons, if_neg hf, Nat.add_zero, if_pos hne,
          List.nil_append]
        exact ih

theorem LateForumWeight_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.SocialGraphJoinWeights.LateForumWeight.Goal := by
  intro fs ms k
  unfold afterGrouping
  change (let a := summarize (block ms k)
      (⟨a.cnt * fs.count k, a.nn * fs.count k, a.sum * fs.count k⟩ : Totals)) = _
  rw [← lateForumWeight_aux]
  rfl

-- L5 -----------------------------------------------------------------------

private theorem histogram_getD_general_nat (m : Std.HashMap Nat Nat) (xs : List Nat) (k : Nat) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD k 0 =
      m.getD k 0 + xs.count k := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih, Std.HashMap.getD_insert]
      by_cases h : x == k
      · have hx : x = k := by simpa using h
        subst x
        rw [if_pos h]
        simp [List.count_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · rw [if_neg h]
        simp [h, List.count_cons]

private theorem histogram_getD_nat (xs : List Nat) (k : Nat) :
    (histogram xs).getD k 0 = xs.count k := by
  simpa [histogram] using histogram_getD_general_nat ({} : Std.HashMap Nat Nat) xs k

private theorem count_map_fst (es : List Edge) (i : Nat) :
    (es.map Prod.fst).count i = (es.filter fun e => e.1 == i).length := by
  induction es with
  | nil => simp
  | cons e es ih =>
      rcases e with ⟨u, v⟩
      simp only [List.map_cons, List.count_cons, List.filter_cons]
      by_cases h : u == i <;> simp [h, ih, Nat.add_comm]

private def deg (es : List Edge) := histogram (es.map Prod.fst)

private def pref (es : List Edge) (i : Nat) : Nat :=
  (List.range i).foldl (fun n j => n + (deg es).getD j 0) 0

private theorem offsets_get (es : List Edge) (width i : Nat) (h : i ≤ width) :
    (offsets es width)[i]?.getD 0 = pref es i := by
  unfold offsets
  rw [List.getElem?_toArray, List.getElem?_scanl]
  simp only [List.length_range]
  rw [if_pos h, Option.getD_some]
  congr 1
  rw [List.take_range, Nat.min_eq_left h]

private theorem pref_succ (es : List Edge) (i : Nat) :
    pref es (i + 1) = pref es i + (es.filter fun e => e.1 == i).length := by
  unfold pref
  rw [List.range_succ]
  rw [List.foldl_append]
  simp only [List.foldl_cons, List.foldl_nil]
  rw [show (deg es).getD i 0 = (es.filter fun e => e.1 == i).length by
    simp [deg, histogram_getD_nat, count_map_fst]]

private theorem offsets_list_get (es : List Edge) (width i : Nat) :
    (offsets es width)[i]? =
      (List.scanl (fun n j => n + (deg es).getD j 0) 0 (List.range width))[i]? := by
  simp [offsets, deg]

private theorem offsets_diff (es : List Edge) (width id : Nat)
    (hall : ∀ e ∈ es, e.1 < width) :
    (offsets es width)[id + 1]?.getD 0 - (offsets es width)[id]?.getD 0 =
      (es.filter fun e => e.1 == id).length := by
  by_cases hid : id < width
  · rw [offsets_get es width (id + 1) (by omega), offsets_get es width id (by omega),
      pref_succ]
    rw [← List.countP_eq_length_filter]
    omega
  · have hfilt : es.filter (fun e => e.1 == id) = [] := by
      induction es with
      | nil => rfl
      | cons e es ih =>
          have he : (e.1 == id) = false := by
            by_cases h : e.1 == id
            · have hw := hall e (by simp)
              have heq : e.fst = id := by simpa using h
              omega
            · simpa using h
          simp [he, ih (by intro z hz; exact hall z (by simp [hz]))]
    rw [hfilt]
    simp
    have hnext : (offsets es width)[id + 1]? = none := by
      rw [offsets_list_get]
      apply List.getElem?_eq_none_iff.mpr
      simp
      omega
    rw [hnext]
    simp

private theorem filter_map_const_eq_replicate (es : List Edge) (id : Nat) (age : Option Int) :
    (es.filter (fun e => e.1 == id)).map (fun _ => age) =
      List.replicate ((es.filter fun e => e.1 == id).length) age := by
  induction es with
  | nil => simp
  | cons e es ih =>
      by_cases h : e.1 == id
      · simp [h]
        rw [ih]
        rw [show (List.filter (fun e => e.fst == id) es).length + 1 =
            Nat.succ (List.filter (fun e => e.fst == id) es).length by omega,
          List.replicate_succ]
      · simp [h, ih]

private theorem degreeAvg_fold (dir : Array Nat) (label : String) (acc : Totals) :
    ∀ ps : List Resident,
    ps.foldl (fun st (i, c, age) => if c != label then st else
      plus st (weighted (dir[i + 1]?.getD 0 - dir[i]?.getD 0) age)) acc =
      plus acc (summarize (ps.flatMap fun (id, c, age) =>
        if c == label then List.replicate (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age else [])) := by
  intro ps
  induction ps generalizing acc with
  | nil => simp [summarize, plus_zero_right]
  | cons p ps ih =>
      rcases p with ⟨id, c, age⟩
      simp only [List.foldl_cons, List.flatMap_cons]
      by_cases hcl : c == label
      · have hcl' : c = label := by simpa using hcl
        rw [show (if c != label then acc else
              plus acc (weighted (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age)) =
              plus acc (weighted (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age) by simp [hcl']]
        rw [ih (acc := plus acc (weighted (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age))]
        simp [hcl, summarize_append, summarize_replicate, plus_assoc]
      · have hcl' : c ≠ label := by intro heq; exact hcl (by simp [heq])
        rw [show (if c != label then acc else
              plus acc (weighted (dir[id + 1]?.getD 0 - dir[id]?.getD 0) age)) = acc by simp [hcl']]
        rw [ih (acc := acc)]
        simp [hcl, plus_assoc, plus_zero_left]

theorem DirectoryDegree_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.SocialGraphJoinWeights.DirectoryDegree.Goal := by
  intro es width ps label hall
  unfold degreeAvg
  rw [degreeAvg_fold]
  simp only [plus_zero_left]
  congr 1
  induction ps with
  | nil => simp
  | cons p ps ih =>
      rcases p with ⟨id, c, age⟩
      simp only [List.flatMap_cons]
      by_cases hcl : c == label
      · rw [if_pos hcl, if_pos hcl, ih, offsets_diff es width id hall,
          filter_map_const_eq_replicate]
      · rw [if_neg hcl, if_neg hcl, ih]

-- L3 -----------------------------------------------------------------------

private def pos (v : Option Int) : Bool := v.any fun x => 0 < x
private def rng (y : Int) : Bool := decide (2010 ≤ y) && decide (y ≤ 2014)

private theorem rng_true_iff (y : Int) : rng y = true ↔ 2010 ≤ y ∧ y ≤ 2014 := by
  simp [rng]

private def refP (pre : List Reply) (y : Int) : Totals :=
  summarize (pre.flatMap fun a =>
    if a.year == y && rng y then
      (pre.filter fun b => b.parent == some a.id && pos b.score).map Reply.score
    else [])

private def pendP (pre : List Reply) (id : Nat) : Totals :=
  if pre.any (fun a => a.id == id) then zero else
    summarize ((pre.filter fun b => b.parent == some id && pos b.score).map Reply.score)

private def Inv (pre : List Reply) (s : State) : Prop :=
  (∀ id, s.years[id]? = (pre.find? fun a => a.id == id).map Reply.year) ∧
  (∀ id, s.pending.getD id zero = pendP pre id) ∧
  (∀ y, s.groups.getD y zero = refP pre y)

private theorem arrive_years (s : State) (r : Reply) :
    (arrive s r).years = s.years.insert r.id r.year := by
  simp only [arrive, Id.run]
  repeat' (first | split | simp [credit] | rfl)

private theorem find_append_single (pre : List Reply) (r : Reply) (id : Nat) :
    (pre ++ [r]).find? (fun a => a.id == id) =
      match pre.find? (fun a => a.id == id) with
      | some a => some a
      | none => if r.id == id then some r else none := by
  induction pre with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x.id == id
      · simp [h]
      · simp [h, ih]

private theorem find_none_of_not_mem {pre : List Reply} {id : Nat}
    (h : id ∉ pre.map Reply.id) : (pre.find? fun a => a.id == id) = none := by
  induction pre with
  | nil => simp
  | cons x xs ih =>
      have hx : x.id ≠ id := by
        intro heq
        exact h (by simp [heq])
      have ht : id ∉ xs.map Reply.id := by
        intro hm
        exact h (by simp [hm])
      by_cases hbe : x.id == id
      · have : x.id = id := by simpa using hbe
        exact (hx this).elim
      · simp [hbe, ih ht]

private theorem find_some_mem {pre : List Reply} {id : Nat} {a : Reply}
    (h : (pre.find? fun x => x.id == id) = some a) : a ∈ pre := by
  induction pre with
  | nil => simp at h
  | cons x xs ih =>
      by_cases hx : x.id == id
      · simp [hx] at h
        cases h
        simp
      · simp [hx] at h
        simpa using (Or.inr (ih h) : a = x ∨ a ∈ xs)

private theorem refP_eq_replyRef (rs : List Reply) (y : Int) : refP rs y = replyRef rs y := by
  simp [refP, replyRef, rng, pos, and_assoc]

private theorem summarize_singleton (v : Option Int) : summarize [v] = weighted 1 v := by
  simp [summarize, plus_zero_left]

private theorem summarize_nil : summarize [] = zero := by
  simp [summarize, zero]

private theorem summarize_cnt (vs : List (Option Int)) : (summarize vs).cnt = vs.length := by
  induction vs with
  | nil => simp [summarize, zero]
  | cons v vs ih =>
      rw [show summarize (v :: vs) = plus (weighted 1 v) (summarize vs) by
        change (v :: vs).foldl (fun a v => plus a (weighted 1 v)) zero = _
        rw [List.foldl_cons, plus_zero_left, foldl_summarize]]
      simp [plus, weighted, ih]
      omega

private theorem summarize_nil_of_pos_zero {vs : List (Option Int)}
    (hpos : ∀ v ∈ vs, pos v = true) (hzero : summarize vs = zero) : vs = [] := by
  have hc : (summarize vs).cnt = 0 := by rw [hzero]; rfl
  rw [summarize_cnt] at hc
  cases vs with
  | nil => rfl
  | cons v vs => simp at hc

private theorem credit_years (s : State) (y : Int) (v : Totals) :
    (credit s y v).years = s.years := by
  by_cases h : 2010 ≤ y && y ≤ 2014 <;> simp [credit, h]

private theorem credit_pending (s : State) (y : Int) (v : Totals) :
    (credit s y v).pending = s.pending := by
  by_cases h : 2010 ≤ y && y ≤ 2014 <;> simp [credit, h]

private theorem credit_groups_getD (s : State) (y z : Int) (v : Totals) :
    (credit s y v).groups.getD z zero =
      if y == z && (2010 ≤ y && y ≤ 2014) then plus (s.groups.getD z zero) v
      else s.groups.getD z zero := by
  by_cases hyz : y == z
  · have heq : y = z := by simpa using hyz
    subst z
    by_cases hyr : 2010 ≤ y && y ≤ 2014 <;> simp [credit, hyr]
  · have hyzf : (y == z) = false := by cases hb : y == z <;> simp_all
    by_cases hyr : 2010 ≤ y && y ≤ 2014 <;> simp [credit, hyr, hyzf, Std.HashMap.getD_insert]

private theorem any_id_eq_iff_mem_map {pre : List Reply} {id : Nat} :
    pre.any (fun a => a.id == id) = true ↔ id ∈ pre.map Reply.id := by
  induction pre with
  | nil => simp
  | cons a as ih =>
      by_cases h : a.id == id
      · have h' : a.id = id := by simpa using h
        simp [h, h']
      · have hf : (a.id == id) = false := by cases hb : a.id == id <;> simp_all
        have h' : (id == a.id) = false := by rw [beq_comm_nat]; exact hf
        have hne' : a.id ≠ id := by intro heq; exact h (by simp [heq])
        have hne : id ≠ a.id := by intro heq; exact hne' heq.symm
        simp [hf, h', hne, ih]

private theorem any_id_false_of_not_mem {pre : List Reply} {id : Nat}
    (h : id ∉ pre.map Reply.id) : pre.any (fun a => a.id == id) = false := by
  by_cases ht : pre.any (fun a => a.id == id) = true
  · exact (h (any_id_eq_iff_mem_map.mp ht)).elim
  · cases hb : pre.any (fun a => a.id == id) <;> simp_all

private theorem any_id_eq_false_of_not_mem (c : Reply → Bool) {as : List Reply} {pid : Nat}
    (hpid : pid ∉ as.map Reply.id) : as.any (fun a => c a && a.id == pid) = false := by
  induction as with
  | nil => simp
  | cons a as ih =>
      have ha : a.id ≠ pid := by intro heq; exact hpid (by simp [heq])
      have ht : pid ∉ as.map Reply.id := by intro hm; exact hpid (by simp [hm])
      simp [ha, ih ht]

private theorem arrive_pending_of_none (s : State) (r : Reply) (hp : r.parent = none) :
    (arrive s r).pending = s.pending.erase r.id := by
  rcases h : s.pending[r.id]? with _ | w <;>
    simp [arrive, hp, h, pos, credit_pending]

private theorem arrive_pending_of_notpos (s : State) (r : Reply) (hps : pos r.score = false) :
    (arrive s r).pending = s.pending.erase r.id := by
  have hps' : r.score.any (fun v => 0 < v) = false := by simpa [pos] using hps
  rcases hp : r.parent with _ | p <;> rcases h : s.pending[r.id]? with _ | w <;>
    simp [arrive, hp, h, hps', pos, credit_pending]

private theorem arrive_pending_known (s : State) (r : Reply) (p : Nat) (y : Int)
    (hp : r.parent = some p) (hps : pos r.score = true)
    (hy : (s.years.insert r.id r.year)[p]? = some y) :
    (arrive s r).pending = s.pending.erase r.id := by
  have hps' : r.score.any (fun v => 0 < v) = true := by simpa [pos] using hps
  rcases h : s.pending[r.id]? with _ | w <;>
    simp [arrive, hp, h, hps', hy, pos, credit_years, credit_pending]

private theorem arrive_pending_unknown (s : State) (r : Reply) (p : Nat)
    (hp : r.parent = some p) (hps : pos r.score = true)
    (hy : (s.years.insert r.id r.year)[p]? = none) :
    (arrive s r).pending =
      (s.pending.erase r.id).insert p
        (plus ((s.pending.erase r.id).getD p zero) (weighted 1 r.score)) := by
  have hps' : r.score.any (fun v => 0 < v) = true := by simpa [pos] using hps
  rcases h : s.pending[r.id]? with _ | w <;>
    simp [arrive, hp, h, hps', hy, pos, credit_years, credit_pending]

private def pendList (pre : List Reply) (id : Nat) : List (Option Int) :=
  (pre.filter fun b => b.parent == some id && pos b.score).map Reply.score

private theorem pendList_append (pre : List Reply) (r : Reply) (id : Nat) :
    pendList (pre ++ [r]) id = pendList pre id ++
      (if r.parent == some id && pos r.score then [r.score] else []) := by
  unfold pendList
  rw [List.filter_append, List.map_append]
  cases hpar : r.parent == some id <;> cases hps : pos r.score <;> simp [hpar, hps]

private theorem pendP_append_mem (pre : List Reply) (r : Reply) (id : Nat)
    (hid : id ∈ pre.map Reply.id) : pendP (pre ++ [r]) id = zero := by
  have hmem : id ∈ (pre ++ [r]).map Reply.id := by simp [hid]
  have hany : (pre ++ [r]).any (fun a => a.id == id) = true := any_id_eq_iff_mem_map.mpr hmem
  simp [pendP, hany]

private theorem pendP_append_self (pre : List Reply) (r : Reply)
    (hrnot : r.id ∉ pre.map Reply.id) : pendP (pre ++ [r]) r.id = zero := by
  have hmem : r.id ∈ (pre ++ [r]).map Reply.id := by simp
  have hany : (pre ++ [r]).any (fun a => a.id == r.id) = true := any_id_eq_iff_mem_map.mpr hmem
  simp [pendP, hany]

private theorem pendP_append_fresh (pre : List Reply) (r : Reply) (id : Nat)
    (hrnot : r.id ∉ pre.map Reply.id) (hid : id ∉ pre.map Reply.id) (hidr : id ≠ r.id) :
    pendP (pre ++ [r]) id =
      plus (pendP pre id)
        (if r.parent == some id && pos r.score then weighted 1 r.score else zero) := by
  have hmem : id ∉ (pre ++ [r]).map Reply.id := by
    simpa [List.map_append, hid, hidr]
  have hanyPre : pre.any (fun a => a.id == id) = false := any_id_false_of_not_mem hid
  have hanyApp : (pre ++ [r]).any (fun a => a.id == id) = false := any_id_false_of_not_mem hmem
  change (if (pre ++ [r]).any (fun a => a.id == id) then zero else summarize (pendList (pre ++ [r]) id)) =
    plus (if pre.any (fun a => a.id == id) then zero else summarize (pendList pre id))
      (if r.parent == some id && pos r.score then weighted 1 r.score else zero)
  rw [hanyPre, hanyApp, pendList_append, summarize_append]
  cases h : r.parent == some id && pos r.score <;>
    simp [h, summarize_singleton, summarize_nil, plus_zero_left]

private theorem find_some_of_mem {pre : List Reply} {id : Nat}
    (h : id ∈ pre.map Reply.id) : ∃ a, (pre.find? fun x => x.id == id) = some a := by
  induction pre with
  | nil => simp at h
  | cons a as ih =>
      by_cases hbe : a.id == id
      · exact ⟨a, by simp [hbe]⟩
      · have hne : id ≠ a.id := by
          intro heq
          apply hbe
          simpa [heq]
        have ht : id ∈ as.map Reply.id := by
          have h' : id ∈ a.id :: as.map Reply.id := by simpa using h
          rcases List.mem_cons.mp h' with hia | hias
          · exact (hne hia).elim
          · exact hias
        rcases ih ht with ⟨x, hx⟩
        exact ⟨x, by simp [hbe, hx]⟩

private theorem arrive_getD_erase_ne (m : Std.HashMap Nat Totals) {a b : Nat}
    (h : (a == b) = false) : (m.erase a).getD b zero = m.getD b zero := by
  simp [Std.HashMap.getD_erase, h]

private theorem beq_false_symm {a b : Nat} (h : (a == b) = false) : (b == a) = false := by
  rw [beq_comm_nat]
  exact h

private theorem years_insert_get_ne (s : State) (r : Reply) {id : Nat}
    (h : (r.id == id) = false) : (s.years.insert r.id r.year)[id]? = s.years[id]? := by
  simp [Std.HashMap.getElem?_insert, h]

private theorem insert_getD_ne (m : Std.HashMap Nat Totals) {a b : Nat} (v : Totals)
    (h : (a == b) = false) : (m.insert a v).getD b zero = m.getD b zero := by
  simp [Std.HashMap.getD_insert, h, beq_false_symm h]

private theorem arrive_inv_pending (pre : List Reply) (s : State) (r : Reply)
    (hrnot : r.id ∉ pre.map Reply.id)
    (hYears : ∀ id, s.years[id]? = (pre.find? fun a => a.id == id).map Reply.year)
    (hPend : ∀ id, s.pending.getD id zero = pendP pre id) :
    ∀ id, (arrive s r).pending.getD id zero = pendP (pre ++ [r]) id := by
  intro id
  by_cases hrid : r.id == id
  · have heq : r.id = id := by simpa using hrid
    subst id
    rw [pendP_append_self pre r hrnot]
    rcases hp : r.parent with _ | p
    · rw [arrive_pending_of_none s r hp]
      simp
    · by_cases hps : pos r.score = true
      · rcases hy : (s.years.insert r.id r.year)[p]? with _ | y
        · rw [arrive_pending_unknown s r p hp hps hy]
          have hpn : p ≠ r.id := by
            intro hpr
            subst p
            simp at hy
          have hpnB : (p == r.id) = false := by simpa using hpn
          simp [hpnB, beq_false_symm hpnB, Std.HashMap.getD_insert]
        · rw [arrive_pending_known s r p y hp hps hy]
          simp
      · have hpsf : pos r.score = false := by cases hb : pos r.score <;> simp_all
        rw [arrive_pending_of_notpos s r hpsf]
        simp
  · have hridf : (r.id == id) = false := by cases hb : r.id == id <;> simp_all
    have hErase : (s.pending.erase r.id).getD id zero = s.pending.getD id zero :=
      arrive_getD_erase_ne s.pending hridf
    by_cases hid : id ∈ pre.map Reply.id
    · rw [pendP_append_mem pre r id hid]
      have hany : pre.any (fun a => a.id == id) = true := any_id_eq_iff_mem_map.mpr hid
      have hPid : s.pending.getD id zero = zero := by rw [hPend id]; simp [pendP, hany]
      rcases hp : r.parent with _ | p
      · rw [arrive_pending_of_none s r hp, hErase, hPid]
      · by_cases hps : pos r.score = true
        · rcases hy : (s.years.insert r.id r.year)[p]? with _ | y
          · rw [arrive_pending_unknown s r p hp hps hy]
            have hpn : p ≠ id := by
              intro hpi
              subst p
              have hyS : s.years[id]? = none := by simpa [hridf] using hy
              rcases find_some_of_mem hid with ⟨a, hfa⟩
              have hysome : s.years[id]? = some a.year := by rw [hYears id, hfa]; rfl
              rw [hysome] at hyS
              contradiction
            have hpnB : (p == id) = false := by simpa using hpn
            simp [hpnB, beq_false_symm hpnB, Std.HashMap.getD_insert, hErase, hPid]
          · rw [arrive_pending_known s r p y hp hps hy, hErase, hPid]
        · have hpsf : pos r.score = false := by cases hb : pos r.score <;> simp_all
          rw [arrive_pending_of_notpos s r hpsf, hErase, hPid]
    · have hPid : s.pending.getD id zero = summarize (pendList pre id) := by
        rw [hPend id]
        simp [pendP, any_id_false_of_not_mem hid, pendList]
      have hPendP : pendP pre id = summarize (pendList pre id) := by
        simp [pendP, any_id_false_of_not_mem hid, pendList]
      have hidr : id ≠ r.id := by intro heq; exact hrid (by simp [heq])
      rw [pendP_append_fresh pre r id hrnot hid hidr]
      rcases hp : r.parent with _ | p
      · rw [arrive_pending_of_none s r hp, hErase, hPid, hPendP]
        simp [plus_zero_right]
      · by_cases hps : pos r.score = true
        · rcases hy : (s.years.insert r.id r.year)[p]? with _ | y
          · rw [arrive_pending_unknown s r p hp hps hy]
            by_cases hpid : p == id
            · have hpeq : p = id := by simpa using hpid
              subst p
              simp [hp, hps, hErase, hPid, hPendP]
            · have hpidf : (p == id) = false := by cases hb : p == id <;> simp_all
              simp [hpidf, beq_false_symm hpidf, Std.HashMap.getD_insert,
                hErase, hPid, hPendP, plus_zero_right]
          · rw [arrive_pending_known s r p y hp hps hy, hErase, hPid]
            have hpn : p ≠ id := by
              intro hpi
              subst p
              have hysome : s.years[id]? = some y := by
                rw [years_insert_get_ne s r hridf] at hy
                exact hy
              have hynone : s.years[id]? = none := by
                rw [hYears id, find_none_of_not_mem hid]
                rfl
              rw [hynone] at hysome
              contradiction
            simp [hpn, hPendP, plus_zero_right]
        · have hpsf : pos r.score = false := by cases hb : pos r.score <;> simp_all
          rw [arrive_pending_of_notpos s r hpsf, hErase, hPid, hPendP]
          simp [hpsf, plus_zero_right]

private theorem arrive_inv_years (pre : List Reply) (s : State) (r : Reply)
    (hrnot : r.id ∉ pre.map Reply.id)
    (hYears : ∀ id, s.years[id]? = (pre.find? fun a => a.id == id).map Reply.year) :
    ∀ id, (arrive s r).years[id]? = ((pre ++ [r]).find? fun a => a.id == id).map Reply.year := by
  intro id
  rw [arrive_years, find_append_single]
  by_cases hrid : r.id == id
  · have heq : r.id = id := by simpa using hrid
    subst id
    have hfnone : (pre.find? fun a => a.id == r.id) = none := find_none_of_not_mem hrnot
    simp [hfnone, Std.HashMap.getElem?_insert]
  · have hridf : (r.id == id) = false := by cases hb : r.id == id <;> simp_all
    rw [show (s.years.insert r.id r.year)[id]? = s.years[id]? by
      simp [Std.HashMap.getElem?_insert, hridf], hYears id]
    cases hf : pre.find? (fun a => a.id == id) with
    | none => simp [hf, hridf]
    | some a => simp [hf]

private def kids (r : Reply) (a : Reply) : List (Option Int) :=
  if r.parent == some a.id && pos r.score then [r.score] else []

private theorem plus_swap_middle (a b c d : Totals) :
    plus (plus a b) (plus c d) = plus (plus a c) (plus b d) := by
  rcases a with ⟨ac, an, as⟩
  rcases b with ⟨bc, bn, bs⟩
  rcases c with ⟨cc, cn, cs⟩
  rcases d with ⟨dc, dn, ds⟩
  simp [plus]
  constructor <;> omega

private theorem summarize_flatMap_concat {α : Type} (l : List α) (f g : α → List (Option Int)) :
    summarize (l.flatMap fun a => f a ++ g a) =
      plus (summarize (l.flatMap f)) (summarize (l.flatMap g)) := by
  induction l with
  | nil => simp [summarize_nil, plus_zero_left]
  | cons a l ih =>
      simp only [List.flatMap_cons]
      rw [summarize_append, summarize_append, ih, summarize_append, summarize_append]
      exact plus_swap_middle _ _ _ _

private theorem flatMap_nil_eq_nil {α β : Type} (l : List α) :
    l.flatMap (fun _ => ([] : List β)) = [] := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [List.flatMap_cons, ih]

private theorem flatMap_congr {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [List.flatMap_cons, h a (by simp),
        ih (by intro b hb; exact h b (by simp [hb]))]

private theorem kids_append (pre : List Reply) (r : Reply) (a : Reply) :
    ((pre ++ [r]).filter fun b => b.parent == some a.id && pos b.score).map Reply.score =
      (pre.filter fun b => b.parent == some a.id && pos b.score).map Reply.score ++ kids r a := by
  unfold kids
  rw [List.filter_append, List.map_append]
  cases hpar : r.parent == some a.id <;> cases hps : pos r.score <;> simp [hpar, hps]

private theorem ref_terms_append (pre : List Reply) (r : Reply) (a : Reply) (y : Int) :
    (if a.year == y && rng y then
       ((pre ++ [r]).filter fun b => b.parent == some a.id && pos b.score).map Reply.score
     else []) =
    (if a.year == y && rng y then
       (pre.filter fun b => b.parent == some a.id && pos b.score).map Reply.score
     else []) ++
    (if a.year == y && rng y then kids r a else []) := by
  by_cases hc : a.year == y && rng y
  · simpa [hc] using kids_append pre r a
  · have hcf : (a.year == y && rng y) = false := by cases hb : a.year == y && rng y <;> simp_all
    simp [hcf]

private theorem refP_append_aux (pre : List Reply) (r : Reply) (y : Int) :
    refP (pre ++ [r]) y =
      plus (plus (refP pre y)
        (summarize (pre.flatMap fun a =>
          if a.year == y && rng y then kids r a else [])))
        (if r.year == y && rng y then
          summarize (pendList pre r.id ++ kids r r) else zero) := by
  unfold refP
  rw [List.flatMap_append, summarize_append]
  congr
  · have hflat :
        (pre.flatMap fun a =>
          if a.year == y && rng y then
            ((pre ++ [r]).filter fun b => b.parent == some a.id && pos b.score).map Reply.score
          else []) =
        pre.flatMap (fun a =>
          (if a.year == y && rng y then
            (pre.filter fun b => b.parent == some a.id && pos b.score).map Reply.score
           else []) ++
          (if a.year == y && rng y then kids r a else [])) := by
      apply flatMap_congr
      intro a ha
      exact ref_terms_append pre r a y
    rw [hflat, summarize_flatMap_concat]
  · have hsingleton :
        ([r].filter fun b => b.parent == some r.id && pos b.score).map Reply.score = kids r r := by
      simpa [List.filter_append, List.map_append] using kids_append pre r r
    cases hyq : r.year == y
    · have hneq : r.year ≠ y := by
        intro h
        subst y
        simp at hyq
      cases hrg : rng y
      · simp [summarize_nil, hneq]
      · simp [summarize_nil, hneq]
    · have heq : r.year = y := by simpa using hyq
      cases hrg : rng y
      · simp [summarize_nil, heq]
      · simp [heq, hrg, pendList, hsingleton]

private theorem summarize_flatMap_single_find (pre : List Reply) (c : Reply → Prop) [DecidablePred c]
    (pid : Nat) (v : Option Int) (hnd : (pre.map Reply.id).Nodup) :
    summarize (pre.flatMap fun a => if c a ∧ a.id = pid then [v] else []) =
      match pre.find? (fun a => a.id == pid) with
      | none => zero
      | some a => if c a then weighted 1 v else zero := by
  induction pre with
  | nil => simp [summarize_nil]
  | cons a as ih =>
      simp only [List.map_cons] at hnd
      rw [List.nodup_cons] at hnd
      rcases hnd with ⟨ha_not, has_nd⟩
      by_cases hbe : a.id == pid
      · have haeq : a.id = pid := by simpa using hbe
        have hpidnot : pid ∉ as.map Reply.id := by simpa [haeq] using ha_not
        have hfindAs : (as.find? fun x => x.id == pid) = none := find_none_of_not_mem hpidnot
        have hsumAs : summarize (as.flatMap fun x => if c x ∧ x.id = pid then [v] else []) = zero := by
          rw [ih has_nd, hfindAs]
        rw [List.flatMap_cons]
        by_cases hc : c a
        · simp [haeq, hc]
          change summarize ([v] ++ as.flatMap (fun x => if c x ∧ x.id = pid then [v] else [])) = weighted 1 v
          rw [summarize_append, summarize_singleton, hsumAs, plus_zero_right]
        · simp [haeq, hc, hsumAs]
      · have hane : a.id ≠ pid := by
          intro h
          apply hbe
          simp [h]
        have hfind : (a :: as).find? (fun x => x.id == pid) = as.find? (fun x => x.id == pid) := by
          simp [hbe]
        rw [List.flatMap_cons]
        simp [hane, hfind, ih has_nd]

private theorem kids_sum (pre : List Reply) (r : Reply) (y : Int)
    (hnd : (pre.map Reply.id).Nodup) :
    summarize (pre.flatMap fun a => if a.year == y && rng y then kids r a else []) =
      match r.parent with
      | none => zero
      | some pid =>
          match pre.find? (fun a => a.id == pid) with
          | none => zero
          | some a => if pos r.score && a.year == y && rng y then weighted 1 r.score else zero := by
  rcases hp : r.parent with _ | pid
  · rw [show (pre.flatMap fun a => if a.year == y && rng y then kids r a else []) =
        pre.flatMap (fun _ : Reply => ([] : List (Option Int))) by
      apply flatMap_congr
      intro a ha
      simp [kids, hp]]
    rw [flatMap_nil_eq_nil, summarize_nil]
  · by_cases hpos : pos r.score = true
    · by_cases hrng : rng y = true
      · have hkids : ∀ a : Reply, kids r a = if a.id == pid then [r.score] else [] := by
          intro a
          simp [kids, hp, hpos, beq_comm_nat]
        have hpoint : (pre.flatMap fun a => if a.year == y && rng y then kids r a else []) =
            (pre.flatMap fun a => if a.year = y ∧ a.id = pid then [r.score] else []) := by
          apply flatMap_congr
          intro a ha
          rw [hkids a]
          by_cases hay : a.year == y
          · have hayP : a.year = y := by simpa using hay
            by_cases haid : a.id == pid
            · have haidP : a.id = pid := by simpa using haid
              simp [hayP, haidP, hrng]
            · have haidF : a.id ≠ pid := by
                intro h
                apply haid
                simp [h]
              simp [hayP, haidF]
          · have hayF : a.year ≠ y := by
              intro h
              apply hay
              simp [h]
            simp [hayF]
        rw [hpoint]
        rw [summarize_flatMap_single_find pre (fun a => a.year = y) pid r.score hnd]
        simp [hp, hpos, hrng]
      · rw [show (pre.flatMap fun a => if a.year == y && rng y then kids r a else []) =
            pre.flatMap (fun _ : Reply => ([] : List (Option Int))) by
          apply flatMap_congr
          intro a ha
          simp [kids, hp, hpos, hrng]]
        rw [flatMap_nil_eq_nil, summarize_nil]
        cases hfind : pre.find? (fun a => a.id == pid) <;> simp [hp, hrng, hfind]
    · rw [show (pre.flatMap fun a => if a.year == y && rng y then kids r a else []) =
          pre.flatMap (fun _ : Reply => ([] : List (Option Int))) by
        apply flatMap_congr
        intro a ha
        simp [kids, hp, hpos]]
      rw [flatMap_nil_eq_nil, summarize_nil]
      cases hfind : pre.find? (fun a => a.id == pid) <;> simp [hp, hpos, hfind]

private theorem plus_comm (a b : Totals) : plus a b = plus b a := by
  rcases a with ⟨ac, an, as⟩
  rcases b with ⟨bc, bn, bs⟩
  simp [plus]
  constructor <;> omega

private theorem plus_right_comm (a b c : Totals) : plus (plus a b) c = plus (plus a c) b := by
  rw [plus_assoc, plus_comm b c, ← plus_assoc]

private theorem range_eq_rng (y : Int) : (2010 ≤ y && y ≤ 2014) = rng y := by
  simp [rng]

private theorem arrive_groups_notpos_none (s : State) (r : Reply) (y : Int) (p : Nat)
    (hw : s.pending[r.id]? = none) (hp : r.parent = some p) (hpsf : pos r.score = false) :
    (arrive s r).groups.getD y zero = s.groups.getD y zero := by
  have hps' : r.score.any (fun v => 0 < v) = false := by simpa [pos] using hpsf
  simp [arrive, hw, hp, hps', credit_years, credit_groups_getD]

private theorem arrive_groups_notpos_wait (s : State) (r : Reply) (y : Int) (p : Nat) (w : Totals)
    (hw : s.pending[r.id]? = some w) (hp : r.parent = some p) (hpsf : pos r.score = false) :
    (arrive s r).groups.getD y zero =
      if r.year == y && (2010 ≤ r.year && r.year ≤ 2014) then
        plus (s.groups.getD y zero) w else s.groups.getD y zero := by
  have hps' : r.score.any (fun v => 0 < v) = false := by simpa [pos] using hpsf
  simp [arrive, hw, hp, hps', credit_years, credit_groups_getD, Std.HashMap.getD_insert]

private theorem arrive_groups_none_none (s : State) (r : Reply) (y : Int)
    (hw : s.pending[r.id]? = none) (hp : r.parent = none) :
    (arrive s r).groups.getD y zero = s.groups.getD y zero := by
  simp [arrive, hw, hp, credit_groups_getD]

private theorem arrive_groups_wait_none (s : State) (r : Reply) (y : Int) (w : Totals)
    (hw : s.pending[r.id]? = some w) (hp : r.parent = none) :
    (arrive s r).groups.getD y zero =
      if r.year == y && (2010 ≤ r.year && r.year ≤ 2014) then
        plus (s.groups.getD y zero) w else s.groups.getD y zero := by
  simp [arrive, hw, hp, credit_groups_getD, Std.HashMap.getD_insert]

private theorem arrive_groups_none_known (s : State) (r : Reply) (y z : Int) (p : Nat)
    (hw : s.pending[r.id]? = none) (hp : r.parent = some p) (hps : pos r.score = true)
    (hy : (s.years.insert r.id r.year)[p]? = some z) :
    (arrive s r).groups.getD y zero =
      if z == y && (2010 ≤ z && z ≤ 2014) then
        plus (s.groups.getD y zero) (weighted 1 r.score) else s.groups.getD y zero := by
  have hps' : r.score.any (fun v => 0 < v) = true := by simpa [pos] using hps
  simp [arrive, hw, hp, hps', hy, credit_years, credit_groups_getD, Std.HashMap.getD_insert]

private theorem arrive_groups_wait_known (s : State) (r : Reply) (y z : Int) (p : Nat) (w : Totals)
    (hw : s.pending[r.id]? = some w) (hp : r.parent = some p) (hps : pos r.score = true)
    (hy : (s.years.insert r.id r.year)[p]? = some z) :
    (arrive s r).groups.getD y zero =
      if z == y && (2010 ≤ z && z ≤ 2014) then
        plus (if r.year == y && (2010 ≤ r.year && r.year ≤ 2014) then
          plus (s.groups.getD y zero) w else s.groups.getD y zero) (weighted 1 r.score)
      else (if r.year == y && (2010 ≤ r.year && r.year ≤ 2014) then
        plus (s.groups.getD y zero) w else s.groups.getD y zero) := by
  have hps' : r.score.any (fun v => 0 < v) = true := by simpa [pos] using hps
  simp [arrive, hw, hp, hps', hy, credit_years, credit_groups_getD, Std.HashMap.getD_insert]

private theorem wait_sum_some (pre : List Reply) (s : State) (r : Reply) (w : Totals)
    (hrnot : r.id ∉ pre.map Reply.id)
    (hPend : ∀ id, s.pending.getD id zero = pendP pre id)
    (hw : s.pending[r.id]? = some w) : w = summarize (pendList pre r.id) := by
  have hget : s.pending.getD r.id zero = w := by
    rw [Std.HashMap.getD_eq_getD_getElem?, hw]
    simp
  rw [hPend r.id] at hget
  symm
  simpa [pendP, any_id_false_of_not_mem hrnot, pendList] using hget

private theorem wait_sum_none (pre : List Reply) (s : State) (r : Reply)
    (hrnot : r.id ∉ pre.map Reply.id)
    (hPend : ∀ id, s.pending.getD id zero = pendP pre id)
    (hw : s.pending[r.id]? = none) : summarize (pendList pre r.id) = zero := by
  have hget : s.pending.getD r.id zero = zero := by
    rw [Std.HashMap.getD_eq_getD_getElem?, hw]
    simp
  have hp0 : pendP pre r.id = zero := by
    rw [← hPend r.id]
    exact hget
  simpa [pendP, any_id_false_of_not_mem hrnot, pendList] using hp0

private theorem arrive_inv_groups (pre : List Reply) (s : State) (r : Reply) (y : Int)
    (hnd : (pre.map Reply.id).Nodup) (hrnot : r.id ∉ pre.map Reply.id)
    (hGroups : ∀ y, s.groups.getD y zero = refP pre y)
    (hYears : ∀ id, s.years[id]? = (pre.find? fun a => a.id == id).map Reply.year)
    (hPend : ∀ id, s.pending.getD id zero = pendP pre id) :
    (arrive s r).groups.getD y zero = refP (pre ++ [r]) y := by
  rw [refP_append_aux, kids_sum pre r y hnd, ← hGroups y]
  rcases hp : r.parent with _ | p
  · -- parent is null: only the waiting credit applies
    simp [hp, kids, plus_zero_right]
    rcases hw : s.pending[r.id]? with _ | w
    · have hwait0 := wait_sum_none pre s r hrnot hPend hw
      rw [arrive_groups_none_none s r y hw hp]
      by_cases hry : r.year = y
      · subst y
        by_cases hrng : rng r.year = true
        · simp [hrng, hwait0, range_eq_rng, plus_zero_right]
        · simp [hrng, hwait0, range_eq_rng, plus_zero_right]
      · simp [hry, hwait0, range_eq_rng, plus_zero_right]
    · have hwv := wait_sum_some pre s r w hrnot hPend hw
      rw [arrive_groups_wait_none s r y w hw hp, hwv]
      by_cases hry : r.year = y
      · subst y
        by_cases hrng : rng r.year = true
        · simp [hrng, range_eq_rng, plus_assoc]
        · simp [hrng, range_eq_rng, plus_zero_right]
      · simp [hry, range_eq_rng, plus_zero_right]
  · by_cases hpos : pos r.score = true
    · have hps' : r.score.any (fun v => 0 < v) = true := by simpa [pos] using hpos
      rcases hy : (s.years.insert r.id r.year)[p]? with _ | z
      · -- parent year unknown: the child is buffered, no group credit
        have hpidf : (r.id == p) = false := by
          cases hb : r.id == p
          · rfl
          · have heq : r.id = p := by simpa using hb
            subst p
            simp [Std.HashMap.getElem?_insert] at hy
        have hpne : p ≠ r.id := by
          intro h
          have hb : (r.id == p) = true := by simp [h]
          rw [hpidf] at hb
          contradiction
        have hsp : s.years[p]? = none := by
          rw [years_insert_get_ne s r hpidf] at hy
          exact hy
        have hfindp : (pre.find? fun a => a.id == p) = none := by
          have ht : (pre.find? fun a => a.id == p).map Reply.year = none := by
            rw [← hYears p]
            exact hsp
          cases hf : pre.find? (fun a => a.id == p) <;> simp [hf] at ht ⊢
        simp [hp, hpos, hfindp, kids, plus_zero_right, hpne]
        rcases hw : s.pending[r.id]? with _ | w
        · have hwait0 := wait_sum_none pre s r hrnot hPend hw
          rw [show (arrive s r).groups.getD y zero = s.groups.getD y zero by
            simp [arrive, hw, hp, hps', hy, credit_years, credit_groups_getD], hwait0]
          by_cases hry : r.year = y
          · subst y
            by_cases hrng : rng r.year = true
            · simp [hrng, range_eq_rng, plus_zero_right]
            · simp [hrng, range_eq_rng, plus_zero_right]
          · simp [hry, range_eq_rng, plus_zero_right]
        · have hwv := wait_sum_some pre s r w hrnot hPend hw
          rw [show (arrive s r).groups.getD y zero =
              (if r.year == y && (2010 ≤ r.year && r.year ≤ 2014) then
                plus (s.groups.getD y zero) w else s.groups.getD y zero) by
              simp [arrive, hw, hp, hps', hy, credit_years, credit_groups_getD], hwv]
          by_cases hry : r.year = y
          · subst y
            by_cases hrng : rng r.year = true
            · simp [hrng, range_eq_rng, plus_assoc]
            · simp [hrng, range_eq_rng, plus_zero_right]
          · simp [hry, range_eq_rng, plus_zero_right]
      · -- parent year known
        by_cases hpid : p == r.id
        · have hpeq : p = r.id := by simpa using hpid
          subst p
          have hfindp : (pre.find? fun a => a.id == r.id) = none := find_none_of_not_mem hrnot
          have hz : r.year = z := by simpa [Std.HashMap.getElem?_insert] using hy
          subst z
          simp [hp, hpos, hfindp, kids, plus_zero_right, summarize_append, summarize_singleton]
          rcases hw : s.pending[r.id]? with _ | w
          · have hwait0 := wait_sum_none pre s r hrnot hPend hw
            rw [arrive_groups_none_known s r y r.year r.id hw hp hps' hy]
            by_cases hry : r.year = y
            · subst y
              by_cases hrng : rng r.year = true
              · simp [hrng, hwait0, range_eq_rng, plus_zero_right, plus_zero_left]
              · simp [hrng, hwait0, range_eq_rng, plus_zero_right, plus_zero_left]
            · simp [hry, hwait0, range_eq_rng, plus_zero_right]
          · have hwv := wait_sum_some pre s r w hrnot hPend hw
            rw [arrive_groups_wait_known s r y r.year r.id w hw hp hps' hy, hwv]
            by_cases hry : r.year = y
            · subst y
              by_cases hrng : rng r.year = true
              · simp [hrng, range_eq_rng, plus_assoc]
              · simp [hrng, range_eq_rng, plus_zero_right]
            · simp [hry, range_eq_rng, plus_zero_right]
        · have hpidf : (r.id == p) = false := by
            have hpr : (p == r.id) = false := by cases hb : p == r.id <;> simp_all
            rw [beq_comm_nat]
            exact hpr
          have hpne : p ≠ r.id := by
            intro h
            have hb : (r.id == p) = true := by simp [h]
            rw [hpidf] at hb
            contradiction
          have hsp : s.years[p]? = some z := by
            rw [years_insert_get_ne s r hpidf] at hy
            exact hy
          rcases hf : pre.find? (fun a => a.id == p) with _ | a
          · rw [hYears p, hf] at hsp
            simp at hsp
          · have haz : a.year = z := by
              have h1 : s.years[p]? = some a.year := by rw [hYears p, hf]; rfl
              rw [hsp] at h1
              exact (Option.some.inj h1).symm
            simp [hp, hf, hpos, haz, kids, plus_zero_right, hpne]
            rcases hw : s.pending[r.id]? with _ | w
            · have hwait0 := wait_sum_none pre s r hrnot hPend hw
              rw [arrive_groups_none_known s r y z p hw hp hps' hy]
              by_cases hzy : z = y
              · subst y
                by_cases hrng : rng z = true
                · simp [hrng, hwait0, range_eq_rng, plus_zero_right, plus_zero_left]
                · simp [hrng, hwait0, range_eq_rng, plus_zero_right, plus_zero_left]
              · simp [hzy, hwait0, range_eq_rng, plus_zero_right, plus_zero_left]
            · have hwv := wait_sum_some pre s r w hrnot hPend hw
              rw [arrive_groups_wait_known s r y z p w hw hp hps' hy, hwv]
              by_cases hzy : z = y
              · subst y
                by_cases hrng : rng z = true
                · by_cases hry : r.year = z
                  · simp [hrng, hry, range_eq_rng]
                    exact plus_right_comm _ _ _
                  · simp [hrng, hry, range_eq_rng, plus_zero_right]
                · by_cases hry : r.year = z
                  · simp [hrng, hry, range_eq_rng, plus_zero_right]
                  · simp [hrng, hry, range_eq_rng, plus_zero_right]
              · by_cases hry : r.year = y
                · by_cases hrng : rng y = true
                  · simp [hzy, hry, hrng, range_eq_rng, plus_assoc, plus_zero_left]
                  · simp [hzy, hry, hrng, range_eq_rng, plus_zero_right]
                · simp [hzy, hry, range_eq_rng, plus_zero_right]
    · have hpsf : pos r.score = false := by cases hb : pos r.score <;> simp_all
      have hkids0 :
          (match pre.find? (fun a => a.id == p) with
           | none => zero
           | some a => zero) = zero := by
        cases hf : pre.find? (fun a => a.id == p) <;> simp [hf]
      simp [hp, hpsf, kids, plus_zero_right, hkids0]
      rcases hw : s.pending[r.id]? with _ | w
      · have hwait0 := wait_sum_none pre s r hrnot hPend hw
        rw [arrive_groups_notpos_none s r y p hw hp hpsf, hwait0]
        by_cases hry : r.year = y
        · subst y
          by_cases hrng : rng r.year = true
          · simp [hrng, range_eq_rng, plus_zero_right]
          · simp [hrng, range_eq_rng, plus_zero_right]
        · simp [hry, range_eq_rng, plus_zero_right]
      · have hwv := wait_sum_some pre s r w hrnot hPend hw
        rw [arrive_groups_notpos_wait s r y p w hw hp hpsf, hwv]
        by_cases hry : r.year = y
        · subst y
          by_cases hrng : rng r.year = true
          · simp [hrng, range_eq_rng, plus_assoc]
          · simp [hrng, range_eq_rng, plus_zero_right]
        · simp [hry, range_eq_rng, plus_zero_right]

private theorem arrive_inv (pre : List Reply) (s : State) (r : Reply)
    (hnd : (pre.map Reply.id).Nodup) (hrnot : r.id ∉ pre.map Reply.id) (hInv : Inv pre s) :
    Inv (pre ++ [r]) (arrive s r) := by
  rcases hInv with ⟨hY, hP, hG⟩
  constructor
  · exact arrive_inv_years pre s r hrnot hY
  constructor
  · exact arrive_inv_pending pre s r hrnot hY hP
  · intro y
    exact arrive_inv_groups pre s r y hnd hrnot hG hY hP

private theorem inv_foldl_aux (done : List Reply) (todo : List Reply) (s : State)
    (hnd : ((done ++ todo).map Reply.id).Nodup) (hInv : Inv done s) :
    Inv (done ++ todo) (todo.foldl arrive s) := by
  induction todo generalizing done s with
  | nil => simpa using hInv
  | cons a as ih =>
      have hnd'' : (((done.map Reply.id) ++ [a.id]) ++ as.map Reply.id).Nodup := by
        simpa [List.map_append, List.append_assoc] using hnd
      have happ : ((done.map Reply.id) ++ [a.id]).Nodup := (List.nodup_append.mp hnd'').1
      have hparts := List.nodup_append.mp happ
      have hdoneNd : (done.map Reply.id).Nodup := hparts.1
      have haNotDone : a.id ∉ done.map Reply.id := by
        intro hm
        exact hparts.2.2 a.id hm a.id (by simp) rfl
      have hstep : Inv (done ++ [a]) (arrive s a) :=
        arrive_inv done s a hdoneNd haNotDone hInv
      simpa [List.foldl_cons, List.append_assoc] using
        ih (done := done ++ [a]) (s := arrive s a)
          (by simpa [List.map_append, List.append_assoc] using hnd) hstep

private theorem inv_empty : Inv [] ({} : State) := by
  constructor
  · intro id
    simp [Std.HashMap.getElem?_empty]
  constructor
  · intro id
    simp [pendP, summarize_nil]
  · intro y
    simp [refP, summarize_nil]

theorem ForwardReplies_sound :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.SocialGraphJoinWeights.ForwardReplies.Goal := by
  intro rs year hnd
  have hi : Inv rs (rs.foldl arrive {}) := by
    simpa using inv_foldl_aux [] rs ({} : State) (by simpa using hnd) inv_empty
  rcases hi with ⟨_, _, hGroups⟩
  rw [hGroups year, refP_eq_replyRef]

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.ForumRunsAndGraphDegrees
