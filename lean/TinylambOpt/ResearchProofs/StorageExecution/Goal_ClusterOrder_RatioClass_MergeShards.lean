import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_MergeShards

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.RatioClass

private abbrev Rw := Nat × Nat

/-! The RatioClass key `(x/(y+1), x%(y+1))` is **not** injective: distinct rows
may share a key.  The `mergeShards` comparator is therefore only a total
preorder on rows, not a total order.  The proof decorates every row with its
original ordinal: `perm` sorts lexicographically by `(key, ordinal)`, a genuine
total order on `zipIdx` entries.  Two sorting steps (mergeSort of each shard and
merge of the shards) agree because for an entry of `a` the ordinal is `< |a|`
and for a shifted entry of `b` it is `>= |a|`, so the two comparators coincide
across shards.  Finally, sorted + same permutation + distinct ordinals forces
the decorated lists to be equal. -/

private abbrev K := Nat × Nat
private abbrev T := K × Nat

private def leK (a b : K) : Bool := a.1 < b.1 || (a.1 == b.1 && a.2 ≤ b.2)
private def leT (a b : T) : Bool := leK a.1 b.1
private def cmpT (a b : T) : Bool :=
  a.1.1 < b.1.1 || (a.1.1 == b.1.1 && (a.1.2 < b.1.2 || (a.1.2 == b.1.2 && a.2 ≤ b.2)))

private theorem leK_trans : ∀ a b c : K, leK a b = true → leK b c = true → leK a c = true := by
  intro a b c
  simp [leK]
  omega

private theorem cmpT_trans : ∀ a b c : T, cmpT a b = true → cmpT b c = true → cmpT a c = true := by
  intro a b c
  simp [cmpT]
  omega

private theorem cmpT_total : ∀ a b : T, (cmpT a b || cmpT b a) = true := by
  intro a b
  simp [cmpT]
  omega

private theorem cmpT_shift (n : Nat) (p q : T) :
    cmpT (p.1, p.2 + n) (q.1, q.2 + n) = cmpT p q := by
  by_cases h1 : p.1.1 < q.1.1
  · simp [cmpT, h1]
  · by_cases h2 : p.1.1 = q.1.1
    · by_cases h3 : p.1.2 < q.1.2
      · simp [cmpT, h1, h2, h3]
      · by_cases h4 : p.1.2 = q.1.2
        · have hi : (p.2 + n ≤ q.2 + n) = (p.2 ≤ q.2) := by
            simp [Nat.add_le_add_iff_right]
          simp [cmpT, h1, h2, h3, h4, hi]
        · simp [cmpT, h1, h2, h3, h4]
    · simp [cmpT, h1, h2]

private theorem cmpT_imp_leT {a b : T} (h : cmpT a b = true) : leT a b = true := by
  simp [cmpT, leT, leK] at h ⊢
  omega

private theorem leT_eq_cmpT_of_idx {p q : T} {n : Nat} (hp : p.2 < n) (hq : n ≤ q.2) :
    leT p q = cmpT p q := by
  by_cases hle : leT p q = true
  · rw [hle]
    have hc : cmpT p q = true := by
      simp [leT, leK, cmpT] at hle ⊢
      by_cases h1 : p.1.1 = q.1.1
      · simp [h1] at hle ⊢
        by_cases h2 : p.1.2 = q.1.2
        · simp [h2] at hle ⊢
          omega
        · have hlt : p.1.2 < q.1.2 := by omega
          simp [hlt, h2]
      · have hlt : p.1.1 < q.1.1 := by omega
        simp [hlt, h1]
    rw [hc]
  · have hpf : leT p q = false := by cases hb : leT p q <;> simp_all
    rw [hpf]
    have hc : cmpT p q = false := by
      simp [leT, leK, cmpT] at hpf ⊢
      by_cases h1 : p.1.1 = q.1.1
      · simp [h1] at hpf ⊢
        by_cases h2 : p.1.2 = q.1.2
        · simp [h2] at hpf ⊢
        · have hgt : q.1.2 < p.1.2 := by omega
          simp [h2]
          exact Nat.le_of_lt hgt
      · have hgt : q.1.1 < p.1.1 := by omega
        simp [h1]
        exact Nat.le_of_lt hgt
    rw [hc]

-- Specializations to the RatioClass key.
private def leKey (x y : Rw) : Bool :=
  let a := key x
  let b := key y
  a.1 < b.1 || (a.1 == b.1 && a.2 ≤ b.2)

private def cmpPair (p q : Rw × Nat) : Bool :=
  let a := key p.1
  let b := key q.1
  a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && p.2 ≤ q.2)))

private def lePair (p q : Rw × Nat) : Bool := leKey p.1 q.1

private def shift (n : Nat) (p : Rw × Nat) : Rw × Nat := (p.1, p.2 + n)

private theorem cmpPair_trans : ∀ p q r : Rw × Nat, cmpPair p q = true → cmpPair q r = true → cmpPair p r = true := by
  intro p q r hpq hqr
  change cmpT (key p.1, p.2) (key q.1, q.2) = true at hpq
  change cmpT (key q.1, q.2) (key r.1, r.2) = true at hqr
  change cmpT (key p.1, p.2) (key r.1, r.2) = true
  exact cmpT_trans _ _ _ hpq hqr

private theorem cmpPair_total : ∀ p q : Rw × Nat, (cmpPair p q || cmpPair q p) = true := by
  intro p q
  change cmpT (key p.1, p.2) (key q.1, q.2) || cmpT (key q.1, q.2) (key p.1, p.2) = true
  exact cmpT_total _ _

private theorem cmpPair_shift (n : Nat) (p q : Rw × Nat) :
    cmpPair (shift n p) (shift n q) = cmpPair p q := by
  change cmpT (key p.1, p.2 + n) (key q.1, q.2 + n) =
    cmpT (key p.1, p.2) (key q.1, q.2)
  exact cmpT_shift n (key p.1, p.2) (key q.1, q.2)

-- ---------------------------------------------------------------------------
-- Decorated physical layout.

private theorem getD_zipIdx (rs : List Rw) (ri : Rw × Nat) (h : ri ∈ rs.zipIdx) :
    rs[ri.2]?.getD (0, 0) = ri.1 := by
  rcases ri with ⟨r, i⟩
  have hm := List.mem_zipIdx h
  simp at hm
  have hg : rs[i]? = some rs[i] := List.getElem?_eq_getElem hm.1
  rw [hg]
  simpa using hm.2.symm

private theorem physical_eq_map_fst (rs : List Rw) :
    physical key rs = (rs.zipIdx.mergeSort cmpPair).map Prod.fst := by
  unfold physical perm
  change ((rs.zipIdx.mergeSort cmpPair).map Prod.snd).map (fun i => rs[i]?.getD (0, 0)) =
    (rs.zipIdx.mergeSort cmpPair).map Prod.fst
  simp only [List.map_map]
  apply List.map_congr_left
  intro ri hri
  exact getD_zipIdx rs ri ((List.mem_mergeSort (a := ri) (l := rs.zipIdx) (le := cmpPair)).1 hri)

private theorem physical_perm (rs : List Rw) : (physical key rs).Perm rs := by
  rw [physical_eq_map_fst]
  have h := (List.mergeSort_perm (rs.zipIdx) cmpPair).map Prod.fst
  simpa [List.zipIdx_map_snd, ← List.range_eq_range'] using h

private theorem zipIdx_start (l : List Rw) (k : Nat) :
    l.zipIdx k = (l.zipIdx).map (fun p : Rw × Nat => (p.1, p.2 + k)) := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    rw [List.getElem_zipIdx h1]
    have h2' : i < l.zipIdx.length := by simpa using h2
    rw [List.getElem_map, List.getElem_zipIdx h2']
    simp [shift]
    omega

private theorem zipIdx_append_shift (a b : List Rw) :
    (a ++ b).zipIdx = a.zipIdx ++ (b.zipIdx.map (shift a.length)) := by
  rw [List.zipIdx_append]
  simp only [Nat.zero_add]
  rw [zipIdx_start b a.length]
  rfl

private theorem zipIdx_snd_inj (rs : List Rw) {p q : Rw × Nat}
    (hp : p ∈ rs.zipIdx) (hq : q ∈ rs.zipIdx) (hi : p.2 = q.2) : p = q := by
  rcases p with ⟨r, i⟩
  rcases q with ⟨s, j⟩
  simp at hi
  have hpm := List.mem_zipIdx hp
  have hqm := List.mem_zipIdx hq
  simp only [Nat.zero_le, true_and, Nat.sub_zero] at hpm hqm
  have hrs : r = s := by
    rw [hpm.2, hqm.2]
    subst j
    rfl
  subst s
  subst j
  rfl

-- ---------------------------------------------------------------------------
-- Merge lemmas (proved by induction on the sum of list lengths, since the
-- merge recursion may shrink either argument).

private theorem map_merge {α β : Type} (f : α → β) (le : β → β → Bool) (l1 l2 : List α) :
    (l1.map f).merge (l2.map f) le = ((l1.merge l2 (fun x y => le (f x) (f y))).map f) := by
  let P : Nat → Prop := fun n => ∀ (a b : List α), a.length + b.length = n →
    (a.map f).merge (b.map f) le = ((a.merge b (fun x y => le (f x) (f y))).map f)
  have hP : ∀ n, P n := by
    intro n
    induction n with
    | zero =>
        intro a b hlen
        have ha : a.length = 0 := by omega
        have hb : b.length = 0 := by omega
        have ha' : a = [] := List.length_eq_zero_iff.mp ha
        have hb' : b = [] := List.length_eq_zero_iff.mp hb
        subst a; subst b; simp
    | succ n ih =>
        intro a b hlen
        cases a with
        | nil => simp
        | cons x xs =>
            cases b with
            | nil => simp
            | cons y ys =>
                by_cases h : le (f x) (f y) = true
                · have hn : xs.length + (y :: ys).length = n := by
                    simp only [List.length_cons] at hlen ⊢
                    omega
                  simpa [h] using ih xs (y :: ys) hn
                · have hf : le (f x) (f y) = false := by cases hb' : le (f x) (f y) <;> simp_all
                  have hn : (x :: xs).length + ys.length = n := by
                    simp only [List.length_cons] at hlen ⊢
                    omega
                  simpa [hf] using ih (x :: xs) ys hn
  exact hP (l1.length + l2.length) l1 l2 rfl

private theorem merge_eq_of_eq_on_sides {α : Type} (le cmp : α → α → Bool)
    (l1 l2 : List α) (h : ∀ p ∈ l1, ∀ q ∈ l2, le p q = cmp p q) :
    l1.merge l2 le = l1.merge l2 cmp := by
  let P : Nat → Prop := fun n => ∀ (a b : List α), a.length + b.length = n →
    (∀ p ∈ a, ∀ q ∈ b, le p q = cmp p q) → a.merge b le = a.merge b cmp
  have hP : ∀ n, P n := by
    intro n
    induction n with
    | zero =>
        intro a b hlen h'
        have ha : a.length = 0 := by omega
        have hb : b.length = 0 := by omega
        have ha' : a = [] := List.length_eq_zero_iff.mp ha
        have hb' : b = [] := List.length_eq_zero_iff.mp hb
        subst a; subst b; simp
    | succ n ih =>
        intro a b hlen h'
        cases a with
        | nil => simp
        | cons x xs =>
            cases b with
            | nil => simp
            | cons y ys =>
                have hxy : le x y = cmp x y := h' x (by simp) y (by simp)
                by_cases hp : le x y = true
                · have hc : cmp x y = true := by rwa [← hxy]
                  have hn : xs.length + (y :: ys).length = n := by
                    simp only [List.length_cons] at hlen ⊢
                    omega
                  have ih' := ih xs (y :: ys) hn (by
                    intro p hp' q hq'
                    exact h' p (List.mem_cons_of_mem x hp') q hq')
                  simpa [hp, hc] using ih'
                · have hpf : le x y = false := by cases hb'' : le x y <;> simp_all
                  have hc : cmp x y = false := by rwa [← hxy]
                  have hn : (x :: xs).length + ys.length = n := by
                    simp only [List.length_cons] at hlen ⊢
                    omega
                  have ih' := ih (x :: xs) ys hn (by
                    intro p hp' q hq'
                    exact h' p hp' q (List.mem_cons_of_mem y hq'))
                  simpa [hpf, hc] using ih'
  exact hP (l1.length + l2.length) l1 l2 rfl h

private theorem sorted_eq_of_perm_inj : ∀ (l1 l2 : List (Rw × Nat)),
    (∀ p ∈ l1, ∀ q ∈ l1, p.2 = q.2 → p = q) →
    l1.Pairwise (fun p q => cmpPair p q = true) →
    l2.Pairwise (fun p q => cmpPair p q = true) →
    l1.Perm l2 → l1 = l2
  | [], [], _, _, _, _ => rfl
  | [], _ :: _, _, _, _, hp => by simp at hp
  | _ :: _, [], _, _, _, hp => by simp at hp
  | x :: xs, z :: zs, hinj, h1, h2, hp => by
      have hpair1 : (∀ y ∈ xs, cmpPair x y = true) ∧ xs.Pairwise (fun p q => cmpPair p q = true) := by
        simpa using h1
      have hpair2 : (∀ y ∈ zs, cmpPair z y = true) ∧ zs.Pairwise (fun p q => cmpPair p q = true) := by
        simpa using h2
      have hxmem : x ∈ z :: zs := hp.mem_iff.mp (by simp)
      have hzmem : z ∈ x :: xs := hp.symm.mem_iff.mp (by simp)
      have hzx : cmpPair z x = true := by
        by_cases hz : z = x
        · simp [hz, cmpPair, key]
        · exact hpair2.1 x (by simpa [hz, Ne.symm hz] using hxmem)
      have hxz : cmpPair x z = true := by
        by_cases hz : z = x
        · simp [hz, cmpPair, key]
        · exact hpair1.1 z (by simpa [hz, Ne.symm hz] using hzmem)
      have hidx : x.2 = z.2 := by
        change cmpT (key x.1, x.2) (key z.1, z.2) = true at hxz
        change cmpT (key z.1, z.2) (key x.1, x.2) = true at hzx
        simp [cmpT] at hxz hzx
        omega
      have hxz' : x = z := hinj x List.mem_cons_self z hzmem hidx
      subst z
      exact congrArg (List.cons x)
        (sorted_eq_of_perm_inj xs zs
          (by intro p hp' q hq' hi; exact hinj p (by simp [hp']) q (by simp [hq']) hi)
          hpair1.2 hpair2.2 (List.Perm.cons_inv hp))

-- ---------------------------------------------------------------------------
-- Truth check on small inputs (all `true`).
private def checkOne (a b : List Rw) : Bool :=
  List.merge (physical key a) (physical key b) leKey == physical key (a ++ b)

#eval checkOne [(0, 5), (0, 1)] [(0, 1), (0, 5)]
#eval checkOne [(2, 1), (1, 1), (2, 0)] [(1, 1), (0, 5), (2, 0)]
#eval checkOne [] [(0, 0), (0, 0)]

theorem sound_MergeShards : RatioClass.MergeShards.Goal := by
  intro a b
  change List.merge (physical key a) (physical key b) leKey = physical key (a ++ b)
  let da := a.zipIdx.mergeSort cmpPair
  let db := b.zipIdx.mergeSort cmpPair
  let dbs := db.map (shift a.length)
  have hda_perm : da.Perm a.zipIdx := by simpa [da] using List.mergeSort_perm a.zipIdx cmpPair
  have hdb_perm : db.Perm b.zipIdx := by simpa [db] using List.mergeSort_perm b.zipIdx cmpPair
  have hdbs_perm : dbs.Perm (b.zipIdx.map (shift a.length)) := by
    simpa [dbs] using hdb_perm.map (shift a.length)
  have hmerge_perm : (List.merge da dbs lePair).Perm ((a ++ b).zipIdx) := by
    have h := (List.merge_perm_append lePair (xs := da) (ys := dbs)).trans (hda_perm.append hdbs_perm)
    simpa [zipIdx_append_shift] using h
  have hda_sorted : da.Pairwise (fun p q => cmpPair p q = true) := by
    simpa [da] using List.pairwise_mergeSort cmpPair_trans cmpPair_total a.zipIdx
  have hdb_sorted : db.Pairwise (fun p q => cmpPair p q = true) := by
    simpa [db] using List.pairwise_mergeSort cmpPair_trans cmpPair_total b.zipIdx
  have hdbs_sorted : dbs.Pairwise (fun p q => cmpPair p q = true) := by
    dsimp [dbs]
    exact hdb_sorted.map (shift a.length) (by
      intro p q h
      rw [cmpPair_shift]
      exact h)
  have hside : ∀ p ∈ da, ∀ q ∈ dbs, lePair p q = cmpPair p q := by
    intro p hp q hq
    have hpidx : p.2 < a.length := by
      have hm := List.mem_zipIdx (hda_perm.mem_iff.mp hp)
      simpa using hm.2.1
    have hq' : q ∈ db.map (shift a.length) := by simpa [dbs] using hq
    rcases List.mem_map.mp hq' with ⟨q0, hq0, rfl⟩
    have hqidx : a.length ≤ (shift a.length q0).2 := by simp [shift]
    change leT (key p.1, p.2) (key (shift a.length q0).1, (shift a.length q0).2) =
      cmpT (key p.1, p.2) (key (shift a.length q0).1, (shift a.length q0).2)
    exact leT_eq_cmpT_of_idx hpidx hqidx
  have hmerge_eq : List.merge da dbs lePair = List.merge da dbs cmpPair :=
    merge_eq_of_eq_on_sides lePair cmpPair da dbs hside
  have hmerge_perm_cmp : (List.merge da dbs cmpPair).Perm ((a ++ b).zipIdx) := by
    rw [← hmerge_eq]
    exact hmerge_perm
  have hmerged_sorted : (List.merge da dbs cmpPair).Pairwise (fun p q => cmpPair p q = true) :=
    List.pairwise_merge cmpPair_trans cmpPair_total da dbs hda_sorted hdbs_sorted
  have hdec_sorted : ((a ++ b).zipIdx.mergeSort cmpPair).Pairwise (fun p q => cmpPair p q = true) :=
    List.pairwise_mergeSort cmpPair_trans cmpPair_total (a ++ b).zipIdx
  have hdec_eq : List.merge da dbs cmpPair = (a ++ b).zipIdx.mergeSort cmpPair := by
    apply sorted_eq_of_perm_inj
    · intro p hp q hq hi
      exact zipIdx_snd_inj (a ++ b) (hmerge_perm_cmp.mem_iff.mp hp) (hmerge_perm_cmp.mem_iff.mp hq) hi
    · exact hmerged_sorted
    · exact hdec_sorted
    · exact hmerge_perm_cmp.trans (List.mergeSort_perm (a ++ b).zipIdx cmpPair).symm
  have hmap : List.merge (da.map Prod.fst) (dbs.map Prod.fst) leKey =
      (List.merge da dbs lePair).map Prod.fst := by
    change (da.map Prod.fst).merge (dbs.map Prod.fst) leKey = _
    rw [map_merge]
    rfl
  rw [physical_eq_map_fst a, physical_eq_map_fst b, physical_eq_map_fst (a ++ b)]
  change List.merge (da.map Prod.fst) (db.map Prod.fst) leKey =
    ((a ++ b).zipIdx.mergeSort cmpPair).map Prod.fst
  have hdbs_fst : dbs.map Prod.fst = db.map Prod.fst := by
    simp [dbs, shift, List.map_map]
  rw [← hdbs_fst, hmap, hmerge_eq, hdec_eq]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_RatioClass_MergeShards
