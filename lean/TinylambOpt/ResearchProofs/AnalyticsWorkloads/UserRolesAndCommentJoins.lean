import TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.

Proved

- `DemandBadges` — `∀ (ps us bs : List Nat) k, k ∈ ps → (Std.HashMap.ofList
  (badgeDemand ps us bs)).getD k 0 = (us.filter (· == k)).length * (bs.filter (· ==
  k)).length`
- `EndpointProfiles` — `∀ ps es k, linkState ps es k = summarize (es.flatMap fun
  (u,v,t) => if t != k then [] else (ps.filter (fun p => p.1 == u)).flatMap fun a =>
  (ps.filter (fun p => p.1 == v && p.2.1)).map fun _ => a.2.2)`
- `RepeatedScore` — `∀ acc v n, inI64 acc → inI64 v → repeatChecked acc v n =
  repeatedReference acc v n`
- `RoleRouting` — `∀ users ps cs, (users.map User.id).Nodup → (ps.map Prod.fst).Nodup →
  (routed users ps cs).Perm (cs.flatMap fun (post,who) => (ps.filter (fun p => p.1 ==
  post)).flatMap fun p => (users.filter (fun u => u.id == p.2 && 2012 ≤
  u.year)).flatMap fun owner => (users.filter (fun u => u.id == who && 1000 <
  u.rep)).map fun _ => owner.id)`
- `SparseOuterCorrection` — `∀ hits tags, corrected hits tags = summarize (hits.flatMap
  fun (t,v) => List.replicate (max 1 (tags.countP fun s => s.isSome && s == t)) v)`
- `YearOnlyOrder` — `∀ rows, yearBuckets rows = rows.mergeSort (fun a b => a.1 ≤ b.1)`
-/

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.UserRolesAndCommentJoins

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins
open TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum (i64Min i64Max inI64)

-- RepeatedScore -------------------------------------------------------------

private theorem nat_mul_le_iff_le_ediv_toNat (D w : Int) (hD : 0 ≤ D) (hw : 0 < w) (k : Nat) :
    (k : Int) * w ≤ D ↔ k ≤ (D / w).toNat := by
  have hw0 : 0 ≤ w := by omega
  have hq_nonneg : 0 ≤ D / w := Int.ediv_nonneg hD hw0
  have hq_cast : ((D / w).toNat : Int) = D / w := Int.toNat_of_nonneg hq_nonneg
  constructor
  · intro h
    by_cases hk : k ≤ (D / w).toNat
    · exact hk
    · have hkq : (D / w : Int) + 1 ≤ (k : Int) := by omega
      have hmul : ((D / w : Int) + 1) * w ≤ (k : Int) * w :=
        Int.mul_le_mul_of_nonneg_right hkq hw0
      have hlt : D < ((D / w : Int) + 1) * w := Int.lt_ediv_add_one_mul_self D hw
      omega
  · intro h
    have hkq : (k : Int) ≤ D / w := by omega
    have hmul : (k : Int) * w ≤ (D / w) * w :=
      Int.mul_le_mul_of_nonneg_right hkq hw0
    have hdiv : (D / w) * w ≤ D := Int.ediv_mul_le D (by omega)
    omega

private theorem inI64_add_pos (acc v : Int) (hacc : inI64 acc = true) (hv : 0 < v) (k : Nat) :
    inI64 (acc + (k : Int) * v) = true ↔ k ≤ ((i64Max - acc) / v).toNat := by
  have hmin : i64Min ≤ acc := of_decide_eq_true (Bool.and_eq_true_iff.mp hacc).1
  have hmax : acc ≤ i64Max := of_decide_eq_true (Bool.and_eq_true_iff.mp hacc).2
  have hD : 0 ≤ i64Max - acc := by omega
  have hkv_nonneg : 0 ≤ (k : Int) * v := by
    have hk : 0 ≤ (k : Int) := by omega
    exact Int.mul_nonneg hk (by omega)
  constructor
  · intro h
    have hupper : acc + (k : Int) * v ≤ i64Max :=
      of_decide_eq_true (Bool.and_eq_true_iff.mp h).2
    have hle : (k : Int) * v ≤ i64Max - acc := by omega
    exact (nat_mul_le_iff_le_ediv_toNat (i64Max - acc) v hD hv k).mp hle
  · intro h
    have hle : (k : Int) * v ≤ i64Max - acc :=
      (nat_mul_le_iff_le_ediv_toNat (i64Max - acc) v hD hv k).mpr h
    apply Bool.and_eq_true_iff.mpr
    constructor
    · exact decide_eq_true (by omega)
    · exact decide_eq_true (by omega)

private theorem inI64_add_neg (acc v : Int) (hacc : inI64 acc = true) (hv : v < 0) (k : Nat) :
    inI64 (acc + (k : Int) * v) = true ↔ k ≤ ((acc - i64Min) / (-v)).toNat := by
  have hmin : i64Min ≤ acc := of_decide_eq_true (Bool.and_eq_true_iff.mp hacc).1
  have hmax : acc ≤ i64Max := of_decide_eq_true (Bool.and_eq_true_iff.mp hacc).2
  have hD : 0 ≤ acc - i64Min := by omega
  have hw : 0 < -v := by omega
  have hkv_nonpos : (k : Int) * v ≤ 0 := by
    have hk : 0 ≤ (k : Int) := by omega
    exact Int.mul_nonpos_of_nonneg_of_nonpos hk (by omega)
  constructor
  · intro h
    have hlower : i64Min ≤ acc + (k : Int) * v :=
      of_decide_eq_true (Bool.and_eq_true_iff.mp h).1
    have hle : (k : Int) * (-v) ≤ acc - i64Min := by
      have hrewrite : (k : Int) * (-v) = -((k : Int) * v) := by rw [Int.mul_neg]
      omega
    exact (nat_mul_le_iff_le_ediv_toNat (acc - i64Min) (-v) hD hw k).mp hle
  · intro h
    have hle : (k : Int) * (-v) ≤ acc - i64Min :=
      (nat_mul_le_iff_le_ediv_toNat (acc - i64Min) (-v) hD hw k).mpr h
    apply Bool.and_eq_true_iff.mpr
    constructor
    · exact decide_eq_true (by
        have hrewrite : (k : Int) * (-v) = -((k : Int) * v) := by rw [Int.mul_neg]
        omega)
    · exact decide_eq_true (by omega)

private def refLoop (v : Int) : List Nat → Int → Option (Except Nat Int) × Int
  | [], s => (none, s)
  | i :: is, s =>
      let s' := s + v
      if inI64 s' = false then (some (.error (i + 1)), s') else refLoop v is s'

private def loopStep (v : Int) (i : Nat) (st : Option (Except Nat Int) × Int) :
    Id (ForInStep (Option (Except Nat Int) × Int)) :=
  if inI64 (st.2 + v) = false then pure (ForInStep.done (some (Except.error (i + 1)), st.2 + v))
  else pure (ForInStep.yield (none, st.2 + v))

private theorem refLoop_append (v : Int) (l₁ l₂ : List Nat) (s : Int) :
    refLoop v (l₁ ++ l₂) s =
      match refLoop v l₁ s with
      | (none, s') => refLoop v l₂ s'
      | (some e, s') => (some e, s') := by
  induction l₁ generalizing s with
  | nil => rfl
  | cons i is ih =>
      simp only [List.cons_append, refLoop]
      by_cases h : inI64 (s + v) = false
      · simp [h]
      · simp [h, ih]

private theorem idRun_forIn_eq_refLoop (v : Int) (l : List Nat) (s : Int) :
    Id.run (forIn l (none, s) (loopStep v)) = refLoop v l s := by
  induction l generalizing s with
  | nil => rfl
  | cons i is ih =>
      rw [List.forIn_cons]
      simp only [Id.run_bind, loopStep]
      by_cases h : inI64 (s + v) = false
      · simp [h, refLoop]
      · simp [h, refLoop, ih]

private theorem loopFun_eq (v : Int) :
    (fun i (st : Option (Except Nat Int) × Int) =>
      let s' := st.2 + v
      if (!inI64 s') = true then pure (ForInStep.done (some (Except.error (i + 1)), s'))
      else pure (ForInStep.yield (none, s'))) = loopStep v := by
  funext i st
  cases h : inI64 (st.2 + v) <;> simp [loopStep, h]

private theorem repeatedReference_eq_refLoop (acc v : Int) (n : Nat) :
    repeatedReference acc v n =
      match refLoop v (List.range n) acc with
      | (some e, _) => e
      | (none, s) => .ok s := by
  unfold repeatedReference
  dsimp only
  rw [loopFun_eq]
  simp only [Id.run_bind]
  cases hres : (Id.run (forIn (List.range n) (none, acc) (loopStep v))) with
  | mk r s =>
      rw [idRun_forIn_eq_refLoop] at hres
      cases r <;> simp_all

private theorem mul_succ_cast (v : Int) (n : Nat) :
    v * ((n + 1 : Nat) : Int) = v * (n : Int) + v := by
  have hcast : ((n + 1 : Nat) : Int) = (n : Int) + 1 := by omega
  rw [hcast, Int.mul_add, Int.mul_one]

private theorem sum_step (acc v : Int) (n : Nat) :
    acc + v * (n : Int) + v = acc + ((n + 1 : Nat) : Int) * v := by
  rw [Int.mul_comm ((n + 1 : Nat) : Int) v, mul_succ_cast]
  omega

private theorem refLoop_pos (acc v : Int) (hacc : inI64 acc = true) (hv : 0 < v) (n : Nat) :
    refLoop v (List.range n) acc =
      if ((i64Max - acc) / v).toNat < n then
        (some (.error (((i64Max - acc) / v).toNat + 1)),
         acc + v * (((((i64Max - acc) / v).toNat + 1 : Nat) : Int)))
      else (none, acc + v * (n : Int)) := by
  let safe := ((i64Max - acc) / v).toNat
  change refLoop v (List.range n) acc =
      if safe < n then (some (.error (safe + 1)), acc + v * (((safe + 1 : Nat) : Int)))
      else (none, acc + v * (n : Int))
  induction n with
  | zero => simp [refLoop]
  | succ n ih =>
      rw [List.range_succ, refLoop_append]
      by_cases hlt : safe < n
      · have hlt' : safe < n + 1 := by omega
        rw [ite_eq_left hlt] at ih
        rw [ih]
        simp [hlt']
      · have hnle : n ≤ safe := by omega
        rw [ite_eq_right hlt] at ih
        rw [ih]
        by_cases hs : safe = n
        · have hlt' : safe < n + 1 := by omega
          rw [ite_eq_left hlt', hs]
          have hnot : inI64 (acc + ((n + 1 : Nat) : Int) * v) = false := by
            apply Bool.eq_false_iff.mpr
            intro htrue
            have hle : n + 1 ≤ safe := by
              simpa [safe] using (inI64_add_pos acc v hacc hv (n + 1)).mp htrue
            omega
          simp [refLoop]
          rw [sum_step]
          rw [Int.mul_comm] at hnot
          rw [Int.mul_comm]
          simpa [show ((n + 1 : Nat) : Int) = (n : Int) + 1 by omega] using hnot
        · have hlt' : ¬ safe < n + 1 := by omega
          rw [ite_eq_right hlt']
          have hyes : inI64 (acc + ((n + 1 : Nat) : Int) * v) = true := by
            apply (inI64_add_pos acc v hacc hv (n + 1)).mpr
            simpa [safe] using (show n + 1 ≤ safe by omega)
          simp [refLoop]
          rw [sum_step]
          rw [Int.mul_comm] at hyes
          rw [Int.mul_comm]
          simpa [show ((n + 1 : Nat) : Int) = (n : Int) + 1 by omega] using hyes

private theorem refLoop_neg (acc v : Int) (hacc : inI64 acc = true) (hv : v < 0) (n : Nat) :
    refLoop v (List.range n) acc =
      if ((acc - i64Min) / (-v)).toNat < n then
        (some (.error (((acc - i64Min) / (-v)).toNat + 1)),
         acc + v * (((((acc - i64Min) / (-v)).toNat + 1 : Nat) : Int)))
      else (none, acc + v * (n : Int)) := by
  let safe := ((acc - i64Min) / (-v)).toNat
  change refLoop v (List.range n) acc =
      if safe < n then (some (.error (safe + 1)), acc + v * (((safe + 1 : Nat) : Int)))
      else (none, acc + v * (n : Int))
  induction n with
  | zero => simp [refLoop]
  | succ n ih =>
      rw [List.range_succ, refLoop_append]
      by_cases hlt : safe < n
      · have hlt' : safe < n + 1 := by omega
        rw [ite_eq_left hlt] at ih
        rw [ih]
        simp [hlt']
      · rw [ite_eq_right hlt] at ih
        rw [ih]
        by_cases hs : safe = n
        · have hlt' : safe < n + 1 := by omega
          rw [ite_eq_left hlt', hs]
          have hnot : inI64 (acc + ((n + 1 : Nat) : Int) * v) = false := by
            apply Bool.eq_false_iff.mpr
            intro htrue
            have hle : n + 1 ≤ safe := by
              simpa [safe] using (inI64_add_neg acc v hacc hv (n + 1)).mp htrue
            omega
          simp [refLoop]
          rw [sum_step]
          rw [Int.mul_comm] at hnot
          rw [Int.mul_comm]
          simpa [show ((n + 1 : Nat) : Int) = (n : Int) + 1 by omega] using hnot
        · have hlt' : ¬ safe < n + 1 := by omega
          rw [ite_eq_right hlt']
          have hyes : inI64 (acc + ((n + 1 : Nat) : Int) * v) = true := by
            apply (inI64_add_neg acc v hacc hv (n + 1)).mpr
            simpa [safe] using (show n + 1 ≤ safe by omega)
          simp [refLoop]
          rw [sum_step]
          rw [Int.mul_comm] at hyes
          rw [Int.mul_comm]
          simpa [show ((n + 1 : Nat) : Int) = (n : Int) + 1 by omega] using hyes

private theorem refLoop_zero (acc : Int) (hacc : inI64 acc = true) (n : Nat) :
    refLoop 0 (List.range n) acc = (none, acc) := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [List.range_succ, refLoop_append, ih]
      simp [refLoop, hacc]

theorem RepeatedScore_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins.RepeatedScore.Goal := by
  intro acc v n hacc hvIn
  rw [repeatedReference_eq_refLoop]
  by_cases hvpos : 0 < v
  · rw [refLoop_pos acc v hacc hvpos]
    simp only [repeatChecked, hvpos, ↓reduceIte]
    by_cases hsafe : ((i64Max - acc) / v).toNat < n <;> simp [hsafe]
  · by_cases hvneg : v < 0
    · rw [refLoop_neg acc v hacc hvneg]
      simp only [repeatChecked, hvpos, hvneg, ↓reduceIte]
      by_cases hsafe : (-((acc - i64Min) / v)).toNat < n <;> simp [hsafe]
    · have hvzero : v = 0 := by omega
      subst v
      rw [refLoop_zero acc hacc]
      simp [repeatChecked]

-- YearOnlyOrder -------------------------------------------------------------

private abbrev IGroup := Group × Nat

private def bucketFold {α β : Type} [BEq α] [Hashable α] (key : β → α) (l : List β) :
    Std.HashMap α (List β) :=
  l.foldl (fun h x => h.insert (key x) (x :: h.getD (key x) [])) {}

private theorem bucketFold_fold_getD {α β : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (key : β → α) (l : List β) (m : Std.HashMap α (List β)) (k : α) :
    (l.foldl (fun h x => h.insert (key x) (x :: h.getD (key x) [])) m).getD k [] =
      (l.filter fun x => key x == k).reverse ++ m.getD k [] := by
  induction l generalizing m with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih]
      by_cases hx : key x == k
      · have hxeq : key x = k := beq_iff_eq.mp hx
        rw [Std.HashMap.getD_insert]
        simp only [hx, ↓reduceIte]
        simp [hxeq, List.append_assoc]
      · have hget : (m.insert (key x) (x :: m.getD (key x) [])).getD k [] = m.getD k [] := by
          rw [Std.HashMap.getD_insert]
          simp [hx]
        rw [hget]
        simp [hx]

private theorem bucketFold_getD {α β : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (key : β → α) (l : List β) (k : α) :
    (bucketFold key l).getD k [] = (l.filter fun x => key x == k).reverse := by
  simpa [bucketFold] using bucketFold_fold_getD key l ({} : Std.HashMap α (List β)) k

private theorem bucketFold_fold_mem {α β : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (key : β → α) (l : List β) (m : Std.HashMap α (List β)) (k : α) :
    k ∈ (l.foldl (fun h x => h.insert (key x) (x :: h.getD (key x) [])) m) ↔
      k ∈ m ∨ ∃ x ∈ l, (key x == k) = true := by
  induction l generalizing m with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashMap.mem_insert]
      by_cases hx : key x == k
      · have hxeq : key x = k := beq_iff_eq.mp hx
        simp [hxeq]
      · have hxne : key x ≠ k := by
          intro he
          exact hx (beq_iff_eq.mpr he)
        simp [hxne]

private theorem bucketFold_mem {α β : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (key : β → α) (l : List β) (k : α) :
    k ∈ bucketFold key l ↔ ∃ x ∈ l, (key x == k) = true := by
  simpa [bucketFold] using bucketFold_fold_mem key l ({} : Std.HashMap α (List β)) k

private def yearBucketsIdx (l : List IGroup) : List IGroup :=
  (bucketFold (fun r : IGroup => r.1.1) l).keys.mergeSort.flatMap
    (fun y => ((bucketFold (fun r : IGroup => r.1.1) l).getD y []).reverse)

private theorem yearBucketsIdx_filters (l : List IGroup) :
    yearBucketsIdx l =
      (bucketFold (fun r : IGroup => r.1.1) l).keys.mergeSort.flatMap
        (fun y => l.filter fun r => r.1.1 == y) := by
  unfold yearBucketsIdx
  congr 1
  funext y
  rw [bucketFold_getD]
  simp

private theorem yearBuckets_filters (rows : List Group) :
    yearBuckets rows =
      (bucketFold (fun g : Group => g.1) rows).keys.mergeSort.flatMap
        (fun y => rows.filter fun g => g.1 == y) := by
  unfold yearBuckets
  dsimp
  change (bucketFold (fun g : Group => g.1) rows).keys.mergeSort.flatMap
    (fun y => ((bucketFold (fun g : Group => g.1) rows).getD y []).reverse) = _
  congr 1
  funext y
  rw [bucketFold_getD]
  simp

private theorem zipIdx_filter_map_fst_aux (rows : List Group) (k : Nat) (y : Int) :
    ((rows.zipIdx k).filter (fun r : IGroup => r.1.1 == y)).map Prod.fst =
      rows.filter (fun g : Group => g.1 == y) := by
  induction rows generalizing k with
  | nil => rfl
  | cons g gs ih =>
      rw [List.zipIdx_cons]
      by_cases hy : g.1 == y
      · simp [hy, ih (k + 1)]
      · simp [hy, ih (k + 1)]

private theorem zipIdx_filter_map_fst (rows : List Group) (y : Int) :
    ((rows.zipIdx).filter (fun r : IGroup => r.1.1 == y)).map Prod.fst =
      rows.filter (fun g : Group => g.1 == y) := by
  exact zipIdx_filter_map_fst_aux rows 0 y

private theorem fst_mem_of_mem_zipIdx {α : Type} {l : List α} {k : Nat} {r : α × Nat}
    (h : r ∈ l.zipIdx k) : r.1 ∈ l := by
  induction l generalizing k with
  | nil => simp at h
  | cons a as ih =>
      rw [List.zipIdx_cons] at h
      simp only [List.mem_cons] at h
      rcases h with h | h
      · have hr : r.1 = a := congrArg Prod.fst h
        rw [hr]
        simp
      · exact List.mem_cons_of_mem a (ih h)

private theorem exists_zipIdx_of_mem {α : Type} {l : List α} {k : Nat} {x : α} (h : x ∈ l) :
    ∃ i, (x, i) ∈ l.zipIdx k := by
  induction l generalizing k with
  | nil => simp at h
  | cons a as ih =>
      simp only [List.mem_cons] at h
      rcases h with h | h
      · subst x
        exact ⟨k, by rw [List.zipIdx_cons]; simp⟩
      · rcases ih (k := k + 1) h with ⟨i, hi⟩
        exact ⟨i, by rw [List.zipIdx_cons]; simp [hi]⟩

private theorem mergeSort_keys_nodup (m : Std.HashMap Int (List IGroup)) :
    (m.keys.mergeSort).Nodup := by
  exact (List.mergeSort_perm m.keys (fun a b : Int => decide (a ≤ b))).symm.nodup Std.HashMap.nodup_keys

private theorem mergeSort_keys_sorted {β : Type} (m : Std.HashMap Int (List β)) :
    (m.keys.mergeSort).Pairwise (fun a b : Int => a ≤ b) := by
  have h : (m.keys.mergeSort (fun a b : Int => decide (a ≤ b))).Pairwise
      (fun a b : Int => (decide (a ≤ b)) = true) := by
    apply List.pairwise_mergeSort (le := fun a b : Int => decide (a ≤ b))
    · intro a b c hab hbc
      simp only [decide_eq_true_eq] at hab hbc ⊢
      omega
    · intro a b
      by_cases hab : a ≤ b
      · simp [hab]
      · have hba : b ≤ a := by omega
        simp [hab, hba]
  simpa using h

private theorem bucketKeys_mergeSort_eq (rows : List Group) :
    (bucketFold (fun r : IGroup => r.1.1) rows.zipIdx).keys.mergeSort =
    (bucketFold (fun g : Group => g.1) rows).keys.mergeSort := by
  let kI := bucketFold (fun r : IGroup => r.1.1) rows.zipIdx
  let kR := bucketFold (fun g : Group => g.1) rows
  have hmem : ∀ y, y ∈ kI.keys ↔ y ∈ kR.keys := by
    intro y
    rw [Std.HashMap.mem_keys, Std.HashMap.mem_keys, bucketFold_mem, bucketFold_mem]
    constructor
    · rintro ⟨r, hr, hry⟩
      exact ⟨r.1, fst_mem_of_mem_zipIdx hr, hry⟩
    · rintro ⟨g, hg, hgy⟩
      rcases exists_zipIdx_of_mem hg with ⟨i, hi⟩
      exact ⟨(g, i), hi, hgy⟩
  have hperm : kI.keys.Perm kR.keys :=
    (List.perm_ext_iff_of_nodup (Std.HashMap.nodup_keys) (Std.HashMap.nodup_keys)).mpr hmem
  have hpermS : kI.keys.mergeSort.Perm kR.keys.mergeSort := by
    exact (List.mergeSort_perm kI.keys (fun a b : Int => decide (a ≤ b))).trans
      (hperm.trans (List.mergeSort_perm kR.keys (fun a b : Int => decide (a ≤ b))).symm)
  apply List.Perm.eq_of_pairwise (le := fun a b : Int => a ≤ b)
  · intro a b ha hb hab hba
    omega
  · exact mergeSort_keys_sorted kI
  · exact mergeSort_keys_sorted kR
  · exact hpermS

private theorem yearBucketsIdx_map_fst (rows : List Group) :
    (yearBucketsIdx rows.zipIdx).map Prod.fst = yearBuckets rows := by
  rw [yearBucketsIdx_filters, yearBuckets_filters]
  rw [bucketKeys_mergeSort_eq]
  rw [List.map_flatMap]
  congr 1
  funext y
  exact zipIdx_filter_map_fst rows y

private theorem flatMap_congr {α β : Type} (f g : α → List β) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.flatMap_cons, List.flatMap_cons, h a (by simp), ih (by
        intro x hx
        exact h x (by simp [hx]))]

private theorem filter_filter_ne {α β : Type} [BEq α] [LawfulBEq α]
    (key : β → α) (l : List β) (y z : α) (hzy : z ≠ y) :
    l.filter (fun r => key r == z) =
      (l.filter (fun r => !(key r == y))).filter (fun r => key r == z) := by
  induction l with
  | nil => rfl
  | cons r rs ih =>
      by_cases hrz : key r == z
      · have hry : (key r == y) = false := by
          apply Bool.eq_false_iff.mpr
          intro hry
          have hkzy : z = y := (beq_iff_eq.mp hrz).symm.trans (beq_iff_eq.mp hry)
          exact hzy hkzy
        simp [hrz, hry, ih]
      · simp [hrz, ih]

private theorem perm_flatMap_filter_nodup {α β : Type} [BEq α] [LawfulBEq α]
    (key : β → α) (l : List β) (ks : List α)
    (hks : ks.Nodup) (hall : ∀ r ∈ l, key r ∈ ks) :
    (ks.flatMap fun y => l.filter (fun r => key r == y)).Perm l := by
  induction ks generalizing l with
  | nil =>
      have hlnil : l = [] := by
        cases l with
        | nil => rfl
        | cons r rs =>
            exact False.elim (by simpa using hall r (by simp))
      subst l
      simp
  | cons y ys ih =>
      have hy_notin : y ∉ ys := (List.nodup_cons.mp hks).1
      have hys_nodup : ys.Nodup := (List.nodup_cons.mp hks).2
      let ly := l.filter (fun r => key r == y)
      let ln := l.filter (fun r => !(key r == y))
      have hp : (ly ++ ln).Perm l := by
        simpa [ly, ln] using List.filter_append_perm (fun r => key r == y) l
      have hall_ln : ∀ r ∈ ln, key r ∈ ys := by
        intro r hr
        have hr_l : r ∈ l := (List.mem_filter.mp hr).1
        have hnot := (List.mem_filter.mp hr).2
        have hnot' : (key r == y) = false := by simpa using hnot
        have hk := hall r hr_l
        simp only [List.mem_cons] at hk
        rcases hk with hk | hk
        · have hky : key r = y := beq_iff_eq.mp (by simpa using hk)
          have hbeq : (key r == y) = true := by simp [hky]
          simp [hbeq] at hnot'
        · exact hk
      have hforall : ∀ z ∈ ys, l.filter (fun r => key r == z) =
          ln.filter (fun r => key r == z) := by
        intro z hz
        have hzy : z ≠ y := by
          intro heq
          exact hy_notin (heq ▸ hz)
        exact filter_filter_ne key l y z hzy
      have hfg : ys.flatMap (fun z => l.filter (fun r => key r == z)) =
          ys.flatMap (fun z => ln.filter (fun r => key r == z)) :=
        flatMap_congr _ _ ys hforall
      have hflat : (List.flatMap (fun z => l.filter (fun r => key r == z)) (y :: ys)) =
          ly ++ (ys.flatMap fun z => ln.filter (fun r => key r == z)) := by
        rw [List.flatMap_cons]
        dsimp [ly]
        rw [hfg]
      rw [hflat]
      exact ((ih (l := ln) hys_nodup hall_ln).append_left ly).trans hp

private def yearLe (a b : Group) : Bool := decide (a.1 ≤ b.1)
private def idxLe (a b : IGroup) : Bool := List.zipIdxLE yearLe a b

private theorem yearLe_trans (a b c : Group) : yearLe a b = true → yearLe b c = true →
    yearLe a c = true := by
  simp only [yearLe, decide_eq_true_eq]
  intro hab hbc
  omega

private theorem yearLe_total (a b : Group) : (yearLe a b || yearLe b a) = true := by
  by_cases hab : a.1 ≤ b.1
  · simp [yearLe, hab]
  · have hba : b.1 ≤ a.1 := by omega
    simp [yearLe, hab, hba]

private theorem idxLe_trans (a b c : IGroup) : idxLe a b = true → idxLe b c = true →
    idxLe a c = true := by
  exact List.zipIdxLE_trans yearLe_trans a b c

private theorem idxLe_total (a b : IGroup) : (idxLe a b || idxLe b a) = true := by
  exact List.zipIdxLE_total yearLe_total a b

private theorem zipIdx_snd_pairwise (rows : List Group) (k : Nat) :
    (rows.zipIdx k).Pairwise (fun a b : IGroup => a.2 < b.2) := by
  induction rows generalizing k with
  | nil => simp
  | cons g gs ih =>
      rw [List.zipIdx_cons]
      constructor
      · intro r hr
        have hr' := List.mem_zipIdx hr
        omega
      · exact ih (k + 1)

private theorem within_year_pairwise (rows : List Group) (y : Int) :
    (rows.zipIdx.filter (fun r : IGroup => r.1.1 == y)).Pairwise
      (fun a b => idxLe a b = true) := by
  rw [List.pairwise_filter]
  exact (zipIdx_snd_pairwise rows 0).imp (by
    intro a b hlt pa pb
    have hay' : a.1.1 = y := beq_iff_eq.mp pa
    have hby' : b.1.1 = y := beq_iff_eq.mp pb
    simp [idxLe, List.zipIdxLE, yearLe, hay', hby']
    omega)

private theorem cross_year_idxLe {a b : IGroup} {y₁ y₂ : Int}
    (hay : a.1.1 == y₁) (hby : b.1.1 == y₂) (hlt : y₁ < y₂) :
    idxLe a b = true := by
  have hay' : a.1.1 = y₁ := beq_iff_eq.mp hay
  have hby' : b.1.1 = y₂ := beq_iff_eq.mp hby
  simp [idxLe, List.zipIdxLE, yearLe, hay', hby']
  omega

private theorem pairwise_flatMap_year (rows : List Group) (ks : List Int)
    (hks_nodup : ks.Nodup) (hks_sorted : ks.Pairwise (fun a b : Int => a ≤ b)) :
    (ks.flatMap (fun y => rows.zipIdx.filter (fun r : IGroup => r.1.1 == y))).Pairwise
      (fun a b => idxLe a b = true) := by
  induction ks with
  | nil => simp
  | cons y ys ih =>
      have hnodup := List.nodup_cons.mp hks_nodup
      have hsorted := List.pairwise_cons.mp hks_sorted
      rw [List.flatMap_cons, List.pairwise_append]
      refine ⟨within_year_pairwise rows y, ih hnodup.2 hsorted.2, ?_⟩
      intro a ha b hb
      rcases List.mem_flatMap.mp hb with ⟨z, hz, hbz⟩
      have hyz : y < z := by
        have hle : y ≤ z := hsorted.1 z hz
        have hne : y ≠ z := by
          intro heq
          exact hnodup.1 (heq ▸ hz)
        omega
      exact cross_year_idxLe (List.mem_filter.mp ha).2 (List.mem_filter.mp hbz).2 hyz

private theorem yearBucketsIdx_perm (rows : List Group) :
    (yearBucketsIdx rows.zipIdx).Perm rows.zipIdx := by
  rw [yearBucketsIdx_filters]
  let buckets := bucketFold (fun r : IGroup => r.1.1) rows.zipIdx
  let ks := buckets.keys.mergeSort
  have hnodup : ks.Nodup := mergeSort_keys_nodup buckets
  have hall : ∀ r ∈ rows.zipIdx, (fun r : IGroup => r.1.1) r ∈ ks := by
    intro r hr
    have hm : r.1.1 ∈ buckets := (bucketFold_mem (fun r : IGroup => r.1.1) rows.zipIdx r.1.1).mpr
      ⟨r, hr, by simp⟩
    have hmk : r.1.1 ∈ buckets.keys := Std.HashMap.mem_keys.mpr hm
    exact (List.mergeSort_perm buckets.keys (fun a b : Int => decide (a ≤ b))).symm.subset hmk
  exact perm_flatMap_filter_nodup (fun r : IGroup => r.1.1) rows.zipIdx ks hnodup hall

private theorem yearBucketsIdx_sorted (rows : List Group) :
    (yearBucketsIdx rows.zipIdx).Pairwise (fun a b => idxLe a b = true) := by
  rw [yearBucketsIdx_filters]
  let buckets := bucketFold (fun r : IGroup => r.1.1) rows.zipIdx
  let ks := buckets.keys.mergeSort
  exact pairwise_flatMap_year rows ks (mergeSort_keys_nodup buckets) (mergeSort_keys_sorted buckets)

private theorem idxLe_antisymm (rows : List Group) {a b : IGroup}
    (ha : a ∈ rows.zipIdx) (hb : b ∈ rows.zipIdx)
    (hab : idxLe a b = true) (hba : idxLe b a = true) : a = b := by
  have hle1 : a.1.1 ≤ b.1.1 := by
    by_cases h : a.1.1 ≤ b.1.1
    · exact h
    · have hlt : b.1.1 < a.1.1 := by omega
      simp [idxLe, List.zipIdxLE, yearLe] at hab
      exact False.elim (h hab.1)
  have hle2 : b.1.1 ≤ a.1.1 := by
    by_cases h : b.1.1 ≤ a.1.1
    · exact h
    · have hlt : a.1.1 < b.1.1 := by omega
      simp [idxLe, List.zipIdxLE, yearLe] at hba
      exact False.elim (h hba.1)
  have hy : a.1.1 = b.1.1 := by omega
  have hai : a.2 ≤ b.2 := by
    simp [idxLe, List.zipIdxLE, yearLe, hy] at hab
    omega
  have hbi : b.2 ≤ a.2 := by
    simp [idxLe, List.zipIdxLE, yearLe, hy] at hba
    omega
  have hij : a.2 = b.2 := by omega
  have ha' := List.mem_zipIdx ha
  have hb' := List.mem_zipIdx hb
  cases a with
  | mk ag ai =>
  cases b with
  | mk bg bi =>
      simp only at hij
      subst bi
      have hga : ag = rows[ai] := ha'.2.2
      have hgb : bg = rows[ai] := by simpa using hb'.2.2
      rw [hga, hgb]

theorem YearOnlyOrder_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins.YearOnlyOrder.Goal := by
  intro rows
  let I := rows.zipIdx
  have h1 : (yearBucketsIdx I).Pairwise (fun a b => idxLe a b = true) := yearBucketsIdx_sorted rows
  have h2 : (I.mergeSort idxLe).Pairwise (fun a b => idxLe a b = true) :=
    List.pairwise_mergeSort idxLe_trans idxLe_total I
  have hp1 : (yearBucketsIdx I).Perm I := yearBucketsIdx_perm rows
  have hp2 : (I.mergeSort idxLe).Perm I := List.mergeSort_perm I idxLe
  have heq : yearBucketsIdx I = I.mergeSort idxLe := by
    apply List.Perm.eq_of_pairwise (le := fun a b => idxLe a b = true)
    · intro a b ha hb hab hba
      exact idxLe_antisymm rows (hp1.symm.mem_iff.mpr ha) (hp2.symm.mem_iff.mpr hb) hab hba
    · exact h1
    · exact h2
    · exact hp1.trans hp2.symm
  calc
    yearBuckets rows = (yearBucketsIdx rows.zipIdx).map Prod.fst := (yearBucketsIdx_map_fst rows).symm
    _ = (I.mergeSort idxLe).map Prod.fst := by rw [heq]
    _ = rows.mergeSort yearLe := by
      exact List.mergeSort_zipIdx (l := rows) (le := yearLe)

private theorem histogram_fold_getD (xs : List Nat) (m : Std.HashMap Nat Nat) (k : Nat) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD k 0 =
      m.getD k 0 + (xs.filter (fun x => x == k)).length := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hx' : x = k := beq_iff_eq.mp hx
        subst k
        simp [ih]
        omega
      · simp [Std.HashMap.getD_insert, hx, ih]

private theorem histogram_getD_count (xs : List Nat) (k : Nat) :
    (histogram xs).getD k 0 = (xs.filter (fun x => x == k)).length := by
  simp [histogram, histogram_fold_getD]

private theorem histogram_getD_count_filter (xs : List Nat) (k : Nat) (p : Nat → Bool)
    (hk : p k = true) :
    (histogram (xs.filter p)).getD k 0 = (xs.filter (fun x => x == k)).length := by
  rw [histogram_getD_count]
  congr 1
  rw [List.filter_filter]
  apply List.filter_congr
  intro x hxmem
  by_cases hxk : x == k
  · have hxeq : x = k := beq_iff_eq.mp hxk
    subst k
    simp [hk]
  · simp [hxk]

private theorem histogram_fold_contains (xs : List Nat) (m : Std.HashMap Nat Nat) (k : Nat) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).contains k =
      (m.contains k || ((xs.filter (fun x => x == k)).length > 0)) := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashMap.contains_insert]
      by_cases hx : x == k <;> by_cases hm : m.contains k <;> simp [hx, hm]

private theorem histogram_contains (xs : List Nat) (k : Nat) :
    (histogram xs).contains k = ((xs.filter (fun x => x == k)).length > 0) := by
  simp [histogram, histogram_fold_contains]

private theorem keys_not_contains_of_not_mem {α : Type} [BEq α] [Hashable α] [LawfulBEq α]
    [LawfulHashable α] (m : Std.HashMap α Nat) (k : α) (h : ¬ k ∈ m) :
    (m.keys.contains k) = false := by
  apply Bool.eq_false_iff.mpr
  intro hc
  have hd : decide (k ∈ m.keys) = true := by simpa [List.elem_eq_mem] using hc
  exact h ((Std.HashMap.mem_keys).mp (of_decide_eq_true hd))

-- DemandBadges

theorem DemandBadges_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins.DemandBadges.Goal := by
  intro ps us bs k hkp
  let demand := Std.HashSet.ofList ps
  let users := histogram (us.filter demand.contains)
  let hb := histogram (bs.filter users.contains)
  let lst := hb.toList.map (fun p => (p.1, p.2 * users.getD p.1 0))
  have hdemand : demand.contains k = true := by
    simp [demand, Std.HashSet.contains_ofList, hkp]
  have husers : users.getD k 0 = (us.filter (fun x => x == k)).length := by
    dsimp [users]
    exact histogram_getD_count_filter us k demand.contains hdemand
  have hdistinct : lst.Pairwise (fun a b => (a.1 == b.1) = false) := by
    dsimp [lst]
    rw [List.pairwise_map]
    exact Std.HashMap.distinct_keys_toList
  have hlst_keys : lst.map Prod.fst = hb.keys := by
    dsimp [lst]
    rw [List.map_map]
    change hb.toList.map Prod.fst = hb.keys
    exact Std.HashMap.map_fst_toList_eq_keys
  by_cases huc : users.contains k = true
  · have hbs : hb.getD k 0 = (bs.filter (fun x => x == k)).length := by
      dsimp [hb]
      exact histogram_getD_count_filter bs k users.contains huc
    by_cases hbc : hb.contains k = true
    · have hmem0 : (k, hb.getD k 0) ∈ hb.toList := by
        rw [Std.HashMap.mem_toList_iff_getElem?_eq_some]
        exact Std.HashMap.getElem?_eq_some_getD_of_contains hbc
      have hmem : (k, hb.getD k 0 * users.getD k 0) ∈ lst := by
        dsimp [lst]
        exact List.mem_map.mpr ⟨(k, hb.getD k 0), hmem0, rfl⟩
      have hget : (Std.HashMap.ofList lst).getD k 0 = hb.getD k 0 * users.getD k 0 := by
        exact Std.HashMap.getD_ofList_of_mem (k_beq := by simp) hdistinct hmem
      change (Std.HashMap.ofList lst).getD k 0 =
        (us.filter (fun x => x == k)).length * (bs.filter (fun x => x == k)).length
      rw [hget, husers, hbs, Nat.mul_comm]
    · have hnotmem : ¬ k ∈ hb := by
        intro hm
        exact hbc (Std.HashMap.mem_iff_contains.mp hm)
      have hnokey : (lst.map Prod.fst).contains k = false := by
        rw [hlst_keys]
        exact keys_not_contains_of_not_mem hb k hnotmem
      have hget : (Std.HashMap.ofList lst).getD k 0 = 0 := by
        rw [Std.HashMap.getD_eq_fallback_of_contains_eq_false]
        rw [Std.HashMap.contains_ofList]
        exact hnokey
      have hb0 : hb.getD k 0 = 0 := Std.HashMap.getD_eq_fallback_of_contains_eq_false (Bool.eq_false_iff.mpr hbc)
      have hcB : (bs.filter (fun x => x == k)).length = 0 := by omega
      change (Std.HashMap.ofList lst).getD k 0 =
        (us.filter (fun x => x == k)).length * (bs.filter (fun x => x == k)).length
      rw [hget, hcB]
      simp
  · have huc0 : users.contains k = false := Bool.eq_false_iff.mpr huc
    have husers0 : users.getD k 0 = 0 := Std.HashMap.getD_eq_fallback_of_contains_eq_false huc0
    have hnotmem_hb : ¬ k ∈ hb := by
      intro hm
      have hbc : hb.contains k = true := Std.HashMap.mem_iff_contains.mp hm
      have hd : ((bs.filter users.contains).filter (fun x => x == k)).length > 0 := by
        rw [← histogram_contains]
        exact hbc
      obtain ⟨x, hxmem⟩ := List.exists_mem_of_ne_nil _ (List.ne_nil_of_length_pos hd)
      simp only [List.mem_filter] at hxmem
      have hxeq : x = k := beq_iff_eq.mp hxmem.2
      subst x
      exact huc hxmem.1.2
    have hnokey : (lst.map Prod.fst).contains k = false := by
      rw [hlst_keys]
      exact keys_not_contains_of_not_mem hb k hnotmem_hb
    have hget : (Std.HashMap.ofList lst).getD k 0 = 0 := by
      rw [Std.HashMap.getD_eq_fallback_of_contains_eq_false]
      rw [Std.HashMap.contains_ofList]
      exact hnokey
    change (Std.HashMap.ofList lst).getD k 0 =
      (us.filter (fun x => x == k)).length * (bs.filter (fun x => x == k)).length
    rw [hget, ← husers, husers0]
    simp

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a; cases b; cases c; simp [plus, Nat.add_assoc, Int.add_assoc]
private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  cases a; simp [plus, zero]
private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  cases a; simp [plus, zero]
private theorem foldl_summarize (xs : List (Option Int)) (a : Totals) :
    xs.foldl (fun a v => plus a (weighted 1 v)) a = plus a (summarize xs) := by
  induction xs generalizing a with
  | nil => simp [summarize, plus_zero_right]
  | cons x xs ih =>
      have hs : summarize (x :: xs) = plus (weighted 1 x) (summarize xs) := by
        simpa [summarize, plus_zero_left] using ih (weighted 1 x)
      rw [show (x :: xs).foldl (fun a v => plus a (weighted 1 v)) a =
            xs.foldl (fun a v => plus a (weighted 1 v)) (plus a (weighted 1 x)) by rfl]
      rw [ih, hs, plus_assoc]
private theorem summarize_cons (x : Option Int) (xs : List (Option Int)) :
    summarize (x :: xs) = plus (weighted 1 x) (summarize xs) := by
  simpa [summarize, plus_zero_left] using foldl_summarize xs (weighted 1 x)
private theorem summarize_append (l₁ l₂ : List (Option Int)) :
    summarize (l₁ ++ l₂) = plus (summarize l₁) (summarize l₂) := by
  rw [show summarize (l₁ ++ l₂) = l₂.foldl (fun a v => plus a (weighted 1 v)) (summarize l₁) by
    simp [summarize, List.foldl_append]]
  exact foldl_summarize l₂ (summarize l₁)
private theorem summarize_replicate (n : Nat) (v : Option Int) :
    summarize (List.replicate n v) = ⟨n, if v.isSome then n else 0, (n:Int) * v.getD 0⟩ := by
  induction n with
  | zero => simp [summarize, zero, weighted]
  | succ n ih =>
      rw [List.replicate_succ, summarize_cons, ih]
      cases v with
      | none => simp [weighted, plus]; omega
      | some x =>
          simp [weighted, plus]
          constructor
          · omega
          · rw [Int.add_mul]
            simp [Int.add_comm]
private def scale (n : Nat) (t : Totals) : Totals := ⟨n*t.cnt, n*t.nn, (n:Int)*t.sum⟩
private theorem scale_plus (n : Nat) (a b : Totals) : scale n (plus a b) = plus (scale n a) (scale n b) := by
  cases a; cases b; simp [scale, plus, Nat.mul_add, Int.mul_add]
private theorem sum_replicate_scale (n : Nat) (x : Option Int) :
    summarize (List.replicate n x) = scale n (weighted 1 x) := by
  cases x with
  | none => simp [summarize_replicate, scale, weighted]
  | some v => simp [summarize_replicate, scale, weighted]
private theorem sum_flatMap_replicate (xs : List (Option Int)) (n : Nat) :
    summarize (xs.flatMap fun v => List.replicate n v) = scale n (summarize xs) := by
  induction xs with
  | nil => simp [summarize, zero, scale]
  | cons x xs ih =>
      rw [List.flatMap_cons, summarize_append, sum_replicate_scale, ih]
      rw [summarize_cons, scale_plus]

private theorem src_fold_getD (ps : List PostValue) (h : Std.HashMap Nat Totals) (u : Nat) :
    (ps.foldl (fun m p => m.insert p.1 (plus (m.getD p.1 zero) (weighted 1 p.2.2))) h).getD u zero =
      plus (h.getD u zero) (summarize ((ps.filter fun p => p.1 == u).map fun p => p.2.2)) := by
  induction ps generalizing h with
  | nil => simp [summarize, plus_zero_right]
  | cons p ps ih =>
      by_cases hp : p.1 == u
      · have hpT : (p.1 == u) = true := hp
        have hpeq : p.1 = u := beq_iff_eq.mp hpT
        subst u
        simp only [List.foldl_cons, Std.HashMap.getD_insert, hp, ↓reduceIte]
        rw [ih]
        simp [List.map_cons, summarize_cons, plus_assoc]
      · have hpF : (p.1 == u) = false := Bool.eq_false_iff.mpr hp
        rw [List.foldl_cons, ih]
        have hget : (h.insert p.1 (plus (h.getD p.1 zero) (weighted 1 p.2.2))).getD u zero =
            h.getD u zero := by
          rw [Std.HashMap.getD_insert]
          simp [hpF]
        rw [hget]
        simp [hp]

private def foldSrc (ps : List PostValue) : Std.HashMap Nat Totals :=
  ps.foldl (fun (h : Std.HashMap Nat Totals) (k,_,v) =>
    h.insert k (plus (h.getD k zero) (weighted 1 v))) {}

private def foldDst (ps : List PostValue) : Std.HashMap Nat Nat :=
  histogram ((ps.filter (fun p => p.2.1)).map Prod.fst)

private theorem src_getD (ps : List PostValue) (u : Nat) :
    (foldSrc ps).getD u zero =
      summarize ((ps.filter fun p => p.1 == u).map fun p => p.2.2) := by
  dsimp [foldSrc]
  simpa [plus_zero_left] using src_fold_getD ps {} u

private theorem dst_getD (ps : List PostValue) (v : Nat) :
    (foldDst ps).getD v 0 =
      (ps.filter (fun p => p.1 == v && p.2.1)).length := by
  dsimp [foldDst]
  rw [histogram_getD_count]
  have hf : ((ps.filter (fun p : PostValue => p.2.1)).map Prod.fst).filter (fun x => x == v) =
      (ps.filter (fun p : PostValue => p.1 == v && p.2.1)).map Prod.fst := by
    rw [List.filter_map, List.filter_filter]
    rfl
  simpa using congrArg List.length hf

private theorem map_const_replicate {α β : Type} (xs : List α) (y : β) :
    xs.map (fun _ => y) = List.replicate xs.length y := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih, List.replicate_succ]

private theorem flatMap_ext {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [List.flatMap_cons, h a (by simp), ih (by intro x hx; exact h x (by simp [hx]))]

private theorem map_flatMap_vals (xs : List PostValue) (n : Nat) :
    xs.flatMap (fun a => List.replicate n a.2.2) =
      (xs.map fun a => a.2.2).flatMap (fun v => List.replicate n v) := by
  induction xs with
  | nil => rfl
  | cons a xs ih => simp [List.flatMap_cons, ih]

private theorem edge_eq (ps : List PostValue) (u v : Nat) :
    summarize ((ps.filter (fun p => p.1 == u)).flatMap fun a =>
      (ps.filter (fun p => p.1 == v && p.2.1)).map fun _ => a.2.2) =
    scale ((ps.filter (fun p => p.1 == v && p.2.1)).length)
      (summarize ((ps.filter fun p => p.1 == u).map fun p => p.2.2)) := by
  let m := ps.filter (fun p => p.1 == v && p.2.1)
  have hf : (ps.filter (fun p => p.1 == u)).flatMap (fun a => m.map (fun _ => a.2.2)) =
      (ps.filter (fun p => p.1 == u)).flatMap (fun a => List.replicate m.length a.2.2) := by
    apply flatMap_ext
    intro a ha
    exact map_const_replicate m a.2.2
  rw [hf, map_flatMap_vals]
  exact sum_flatMap_replicate ((ps.filter fun p => p.1 == u).map fun p => p.2.2) m.length

private theorem edge_record_eq_scale (n : Nat) (a : Totals) :
    ⟨a.cnt * n, a.nn * n, a.sum * (n:Int)⟩ = scale n a := by
  cases a
  simp [scale, Nat.mul_comm, Int.mul_comm]

private theorem link_fold (ps : List PostValue) (es : List Link) (k : Nat) (st : Totals) :
    es.foldl (fun st (u,v,t) =>
      if t != k then st else
        let a := (foldSrc ps).getD u zero
        let n := (foldDst ps).getD v 0
        plus st ⟨a.cnt*n, a.nn*n, a.sum*n⟩) st =
    plus st (summarize (es.flatMap fun (u,v,t) =>
      if t != k then [] else (ps.filter (fun p => p.1 == u)).flatMap fun a =>
        (ps.filter (fun p => p.1 == v && p.2.1)).map fun _ => a.2.2)) := by
  induction es generalizing st with
  | nil => simp [summarize, plus_zero_right]
  | cons e es ih =>
      rcases e with ⟨u,v,t⟩
      by_cases ht : (t == k) = true
      · have hteq : t = k := beq_iff_eq.mp ht
        subst k
        rw [List.foldl_cons, List.flatMap_cons]
        have hhead : (t != t) = false := by simp
        simp only [hhead, Bool.false_eq_true, ↓reduceIte]
        rw [ih]
        rw [plus_assoc, summarize_append]
        congr 1
        have hedge : summarize ((ps.filter (fun p => p.1 == u)).flatMap fun a =>
              (ps.filter (fun p => p.1 == v && p.2.1)).map fun _ => a.2.2) =
            ⟨(((foldSrc ps).getD u zero).cnt) * ((foldDst ps).getD v 0),
             (((foldSrc ps).getD u zero).nn) * ((foldDst ps).getD v 0),
             (((foldSrc ps).getD u zero).sum) * (((foldDst ps).getD v 0 : Nat) : Int)⟩ := by
          rw [edge_eq, src_getD, dst_getD, edge_record_eq_scale]
        rw [hedge]
      · have htF : (t == k) = false := Bool.eq_false_iff.mpr ht
        have hhead : (t != k) = true := by simpa using htF
        rw [List.foldl_cons, List.flatMap_cons]
        simp only [hhead, ↓reduceIte, List.nil_append]
        exact ih st

theorem EndpointProfiles_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins.EndpointProfiles.Goal := by
  intro ps es k
  change (es.foldl (fun st (u,v,t) =>
      if t != k then st else
        let a := (foldSrc ps).getD u zero
        let n := (foldDst ps).getD v 0
        plus st ⟨a.cnt*n, a.nn*n, a.sum*n⟩) zero) =
    summarize (es.flatMap fun (u,v,t) =>
      if t != k then [] else (ps.filter (fun p => p.1 == u)).flatMap fun a =>
        (ps.filter (fun p => p.1 == v && p.2.1)).map fun _ => a.2.2)
  simpa [plus_zero_left] using (link_fold ps es k zero)

private theorem histogram_fold_getD_gen {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (m : Std.HashMap α Nat) (k : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD k 0 =
      m.getD k 0 + (xs.filter (fun x => x == k)).length := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hx' : x = k := beq_iff_eq.mp hx
        subst k
        simp [ih]
        omega
      · simp [Std.HashMap.getD_insert, hx, ih]

private theorem histogram_getD_count_gen {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (k : α) :
    (histogram xs).getD k 0 = (xs.filter (fun x => x == k)).length := by
  simp [histogram, histogram_fold_getD_gen]

private theorem extras_mem (tags : List (Option String)) (s : String) (n : Nat) :
    (s, n) ∈ extras tags ↔ n > 0 ∧ (tags.filterMap id).countP (fun x => x == s) = n + 1 := by
  unfold extras
  rw [List.mem_filterMap]
  constructor
  · rintro ⟨⟨s', c⟩, hmem, hsome⟩
    by_cases hc : c > 1
    · have hsv : s' = s ∧ c - 1 = n := by
        simpa [hc] using hsome
      have hs' : s' = s := hsv.1
      subst s'
      have hget : (histogram (tags.filterMap id))[s]? = some c :=
        Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hmem
      have hcont : (histogram (tags.filterMap id)).contains s = true := by
        rw [Std.HashMap.contains_eq_isSome_getElem?]
        simp [hget]
      have hget2 := Std.HashMap.getElem?_eq_some_getD_of_contains (m := histogram (tags.filterMap id))
        (a := s) (fallback := 0) hcont
      have hcg : c = (histogram (tags.filterMap id)).getD s 0 := by
        apply Option.some.inj
        rw [← hget, hget2]
      have hcget : (histogram (tags.filterMap id)).getD s 0 = c := hcg.symm
      rw [histogram_getD_count_gen] at hcget
      rw [List.countP_eq_length_filter]
      constructor <;> omega
    · simp [hc] at hsome
  · rintro ⟨hnpos, hcount⟩
    have hcnt : (histogram (tags.filterMap id)).getD s 0 = n + 1 := by
      rw [histogram_getD_count_gen]
      rw [List.countP_eq_length_filter] at hcount
      exact hcount
    have hcont : (histogram (tags.filterMap id)).contains s = true := by
      by_cases hc : (histogram (tags.filterMap id)).contains s = true
      · exact hc
      · have hf : (histogram (tags.filterMap id)).contains s = false := Bool.eq_false_iff.mpr hc
        have hzero : (histogram (tags.filterMap id)).getD s 0 = 0 :=
          Std.HashMap.getD_eq_fallback_of_contains_eq_false hf
        omega
    have hget : (histogram (tags.filterMap id))[s]? = some (n + 1) := by
      have hg := Std.HashMap.getElem?_eq_some_getD_of_contains (m := histogram (tags.filterMap id))
        (a := s) (fallback := 0) hcont
      simpa [hcnt] using hg
    have hmem : (s, n + 1) ∈ (histogram (tags.filterMap id)).toList :=
      Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hget
    refine ⟨(s, n + 1), hmem, ?_⟩
    have hn1 : n + 1 > 1 := by omega
    simp [hn1]

private theorem extras_distinct (tags : List (Option String)) :
    (extras tags).Pairwise (fun a b => (a.1 == b.1) = false) := by
  unfold extras
  apply List.Pairwise.filterMap
  intro a b hab va hva vb hvb
  have ha2 : a.2 > 1 := by
    by_cases h : a.2 > 1
    · exact h
    · simp [h] at hva
  have hb2 : b.2 > 1 := by
    by_cases h : b.2 > 1
    · exact h
    · simp [h] at hvb
  have hva1 : va.1 = a.1 := by
    have h := congrArg Prod.fst (show va = (a.1, a.2 - 1) from by simpa [ha2] using hva.symm)
    simpa using h
  have hvb1 : vb.1 = b.1 := by
    have h := congrArg Prod.fst (show vb = (b.1, b.2 - 1) from by simpa [hb2] using hvb.symm)
    simpa using h
  rw [hva1, hvb1]
  exact hab
  exact Std.HashMap.distinct_keys_toList

private theorem extras_getD (tags : List (Option String)) (s : String) :
    (Std.HashMap.ofList (extras tags)).getD s 0 =
      (tags.filterMap id).countP (fun x => x == s) - 1 := by
  let c := (tags.filterMap id).countP (fun x => x == s)
  by_cases hc : c > 1
  · have hcpos : c - 1 > 0 := by omega
    have hmem : (s, c - 1) ∈ extras tags := (extras_mem tags s (c - 1)).mpr ⟨hcpos, by omega⟩
    have hget : (Std.HashMap.ofList (extras tags)).getD s 0 = c - 1 :=
      Std.HashMap.getD_ofList_of_mem (k_beq := by simp) (extras_distinct tags) hmem
    simpa [c] using hget
  · have hcontF : (Std.HashMap.ofList (extras tags)).contains s = false := by
      rw [Std.HashMap.contains_ofList]
      apply Bool.eq_false_iff.mpr
      intro hb
      have hd : decide (s ∈ (extras tags).map Prod.fst) = true := by
        simpa [List.elem_eq_mem] using hb
      have hdmem : s ∈ (extras tags).map Prod.fst := of_decide_eq_true hd
      rw [List.mem_map] at hdmem
      rcases hdmem with ⟨p, hp_mem, hp_s⟩
      rcases p with ⟨s', n⟩
      have hs' : s' = s := hp_s
      subst s'
      have hm := (extras_mem tags s n).mp hp_mem
      omega
    have hget : (Std.HashMap.ofList (extras tags)).getD s 0 = 0 :=
      Std.HashMap.getD_eq_fallback_of_contains_eq_false hcontF
    rw [hget]
    omega

private theorem countP_some (tags : List (Option String)) (s : String) :
    tags.countP (fun o => o.isSome && o == some s) = (tags.filterMap id).countP (fun x => x == s) := by
  simp only [List.countP_eq_length_filter]
  induction tags with
  | nil => rfl
  | cons o os ih =>
      cases o with
      | none => simpa using ih
      | some x =>
          by_cases hx : x == s
          · have hxe : x = s := beq_iff_eq.mp hx
            subst x
            simp [ih]
          · have hxf : (x == s) = false := Bool.eq_false_iff.mpr hx
            simp [hxf, ih]

private theorem countP_none (tags : List (Option String)) :
    tags.countP (fun o => o.isSome && o == none) = 0 := by
  simp only [List.countP_eq_length_filter]
  have hf : tags.filter (fun o => o.isSome && o == none) = [] := by
    apply List.filter_eq_nil_iff.mpr
    intro a ha
    cases a with
    | none => simp
    | some x => simp
  rw [hf]
  simp

private theorem ofList_singleton_getD_ne {s name : String} (n : Nat) (hsf : (s == name) = false) :
    (Std.HashMap.ofList [(name, n)]).getD s 0 = 0 := by
  rw [Std.HashMap.getD_eq_fallback_of_contains_eq_false]
  rw [Std.HashMap.contains_ofList]
  have hsneq : s ≠ name := by
    intro heq
    subst heq
    simp at hsf
  simp [hsneq]


private theorem weight_some_match (tags : List (Option String)) (s : String) :
    (match extras tags with
     | [] => 1
     | [(name,n)] => if some s == some name then n + 1 else 1
     | more => 1 + (Std.HashMap.ofList more).getD s 0) =
      1 + (Std.HashMap.ofList (extras tags)).getD s 0 := by
  cases hE : extras tags with
  | nil => simp [hE]
  | cons p ps =>
      cases ps with
      | nil =>
          rcases p with ⟨name,n⟩
          by_cases hs : s == name
          · have hseq : s = name := beq_iff_eq.mp hs
            subst name
            simp [hE, Nat.add_comm]
          · have hsf : (s == name) = false := Bool.eq_false_iff.mpr hs
            rw [ofList_singleton_getD_ne n hsf]
            simp [hE, hsf]
      | cons q qs => simp [hE]


private theorem weight_eq (tags : List (Option String)) (k : Option String) :
    (match extras tags with
     | [] => 1
     | [(name,n)] => if k == some name then n + 1 else 1
     | more => 1 + k.elim 0 ((Std.HashMap.ofList more).getD · 0)) =
      max 1 (tags.countP fun s => s.isSome && s == k) := by
  rcases k with _ | s
  · have hzero : max 1 (tags.countP (fun s => s.isSome && s == none)) = 1 := by
      rw [countP_none]
      simp
    rw [hzero]
    cases hE : extras tags with
    | nil => simp [hE]
    | cons p ps =>
        cases ps with
        | nil => simp
        | cons q qs => simp
  · simp only [Option.elim_some]
    rw [weight_some_match]
    rw [countP_some]
    have hget := extras_getD tags s
    omega



private theorem corrected_fold_fun (w : Option String → Nat) (hits : List Hit) (st : Totals) :
    hits.foldl (fun st (tag,v) => plus st (weighted (w tag) v)) st =
      plus st (summarize (hits.flatMap fun (t,v) => List.replicate (w t) v)) := by
  induction hits generalizing st with
  | nil => simp [summarize, plus_zero_right]
  | cons h hs ih =>
      rcases h with ⟨tag,v⟩
      rw [List.foldl_cons, List.flatMap_cons, ih]
      rw [plus_assoc, summarize_append]
      congr 1
      rw [sum_replicate_scale]
      have hscale : scale (w tag) (weighted 1 v) = weighted (w tag) v := by
        cases v with
        | none => simp [scale, weighted]
        | some x => simp [scale, weighted, Nat.mul_comm, Int.mul_comm]
      rw [hscale]

theorem SparseOuterCorrection_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins.SparseOuterCorrection.Goal := by
  intro hits tags
  unfold corrected
  let weight := match extras tags with
     | [] => fun _ => 1
     | [(name,n)] => fun k => if k == some name then n+1 else 1
     | more => let extra := Std.HashMap.ofList more; fun k => 1 + k.elim 0 (extra.getD · 0)
  change hits.foldl (fun st (tag,v) => plus st (weighted (weight tag) v)) zero = _
  have hw : weight = fun k => max 1 (tags.countP fun s => s.isSome && s == k) := by
    dsimp [weight]
    funext k
    cases hE : extras tags with
    | nil => simpa [hE] using weight_eq tags k
    | cons p ps =>
        cases hps : ps with
        | nil => simpa [hE, hps] using weight_eq tags k
        | cons q qs => simpa [hE, hps] using weight_eq tags k
  rw [hw]
  simpa [plus_zero_left] using
    (corrected_fold_fun (fun k => max 1 (tags.countP fun s => s.isSome && s == k)) hits zero)

private theorem filter_singleton {α : Type} (f : α → Nat) (xs : List α)
    (hnd : (xs.map f).Nodup) (k : Nat) :
    xs.filter (fun x => f x == k) = [] ∨ ∃ x, xs.filter (fun x => f x == k) = [x] := by
  induction xs with
  | nil => exact Or.inl rfl
  | cons a xs ih =>
      have hndTail : (xs.map f).Nodup := (List.nodup_cons.mp hnd).2
      have hnotmem : f a ∉ xs.map f := (List.nodup_cons.mp hnd).1
      by_cases hk : f a == k
      · right
        rcases ih hndTail with ht | ⟨x, hx⟩
        · refine ⟨a, ?_⟩
          simp [hk, ht]
        · exfalso
          have hxmem : x ∈ xs.filter (fun y => f y == k) := by simp [hx]
          have hfxk : f x == k := (List.mem_filter.mp hxmem).2
          have hfeq : f a = f x := (beq_iff_eq.mp hk).trans (beq_iff_eq.mp hfxk).symm
          apply hnotmem
          rw [hfeq]
          exact List.mem_map.mpr ⟨x, (List.mem_filter.mp hxmem).1, rfl⟩
      · rcases ih hndTail with ht | ⟨x, hx⟩
        · left; simp [hk, ht]
        · right; refine ⟨x, ?_⟩; simp [hk, hx]

private theorem hashset_contains_iff (xs : List Nat) (k : Nat) :
    (Std.HashSet.ofList xs).contains k = true ↔ k ∈ xs := by
  rw [Std.HashSet.contains_ofList]
  change (List.elem k xs) = true ↔ k ∈ xs
  rw [List.elem_eq_mem]
  constructor
  · exact of_decide_eq_true
  · intro h; exact decide_eq_true h

private theorem hashset_filter_map_iff {α : Type} (f : α → Nat) (p : α → Bool)
    (xs : List α) (k : Nat) :
    (Std.HashSet.ofList ((xs.filter p).map f)).contains k = true ↔
      ∃ x, x ∈ xs ∧ p x = true ∧ f x = k := by
  rw [hashset_contains_iff]
  constructor
  · intro h
    rw [List.mem_map] at h
    rcases h with ⟨x, hx, hfk⟩
    have hxp : p x = true := (List.mem_filter.mp hx).2
    refine ⟨x, (List.mem_filter.mp hx).1, hxp, ?_⟩
    exact (beq_iff_eq.mp (by simpa using hfk))
  · rintro ⟨x, hxs, hpx, hfk⟩
    rw [List.mem_map]
    refine ⟨x, by simp [hxs, hpx], ?_⟩
    rw [hfk]

private theorem fst_nodup_pairwise (l : List (Nat × Nat)) (hnd : (l.map Prod.fst).Nodup) :
    l.Pairwise (fun a b => (a.1 == b.1) = false) := by
  induction l with
  | nil => simp
  | cons p l ih =>
      have h := List.nodup_cons.mp hnd
      constructor
      · intro q hq
        apply Bool.eq_false_iff.mpr
        intro he
        have heq : p.1 = q.1 := beq_iff_eq.mp he
        apply h.1
        rw [heq]
        exact List.mem_map.mpr ⟨q, hq, rfl⟩
      · exact ih h.2

private theorem hashmap_get_ofList_of_mem (l : List (Nat × Nat)) (k v : Nat)
    (hnd : (l.map Prod.fst).Nodup) (hm : (k,v) ∈ l) :
    (Std.HashMap.ofList l)[k]? = some v := by
  apply Std.HashMap.getElem?_ofList_of_mem (k := k) (k' := k) (v := v) (k_beq := by simp)
  · exact fst_nodup_pairwise l hnd
  · exact hm

private theorem hashmap_get_ofList_of_notmem (l : List (Nat × Nat)) (k : Nat)
    (hno : ∀ v, (k,v) ∉ l) :
    (Std.HashMap.ofList l)[k]? = none := by
  apply Std.HashMap.getElem?_ofList_of_contains_eq_false
  apply Bool.eq_false_iff.mpr
  intro hc
  change (List.elem k (l.map Prod.fst)) = true at hc
  rw [List.elem_eq_mem] at hc
  have hcmem : k ∈ l.map Prod.fst := of_decide_eq_true hc
  rw [List.mem_map] at hcmem
  rcases hcmem with ⟨p, hpm, hpk⟩
  have hp_eq : (k, p.2) = p := by cases p; simp_all
  exact hno p.2 (by rw [hp_eq]; exact hpm)

private theorem filter_nodup_map {α : Type} (f : α → Nat) (xs : List α) (p : α → Bool)
    (hnd : (xs.map f).Nodup) : ((xs.filter p).map f).Nodup := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      have h := List.nodup_cons.mp hnd
      by_cases hp : p a = true
      · rw [List.filter_cons_of_pos hp]
        simp only [List.map_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro hxmem
          apply h.1
          rw [List.mem_map] at hxmem ⊢
          rcases hxmem with ⟨x, hxmem, hfx⟩
          exact ⟨x, (List.mem_filter.mp hxmem).1, hfx⟩
        · exact ih h.2
      · have hpF : p a = false := Bool.eq_false_iff.mpr hp
        rw [List.filter_cons_of_neg hp]
        exact ih h.2

private def ownersOf (users : List User) :=
  Std.HashSet.ofList ((users.filter (fun u => 2012 ≤ u.year)).map User.id)
private def eligibleOf (users : List User) :=
  Std.HashSet.ofList ((users.filter (fun u => 1000 < u.rep)).map User.id)
private def routeOf (users : List User) (ps : List Post) :=
  Std.HashMap.ofList (ps.filter (fun p => (ownersOf users).contains p.2))

-- redefine as def (theorems above can't be definitions)

private theorem route_get (users : List User) (ps : List Post)
    (hnp : (ps.map Prod.fst).Nodup) (post : Nat) :
    (routeOf users ps)[post]? =
      ((ps.filter (fun p => p.1 == post)).filterMap (fun p =>
        if (ownersOf users).contains p.2 then some p.2 else none)).head? := by
  let l := ps.filter (fun p => (ownersOf users).contains p.2)
  have hnd : (l.map Prod.fst).Nodup := by
    dsimp [l]
    apply filter_nodup_map Prod.fst
    exact hnp
  change (Std.HashMap.ofList l)[post]? =
      ((ps.filter (fun p => p.1 == post)).filterMap (fun p =>
        if (ownersOf users).contains p.2 then some p.2 else none)).head?
  rcases filter_singleton (fun p : Post => p.1) ps hnp post with hp | ⟨p, hp⟩
  · have hlp : l.filter (fun p => p.1 == post) = [] := by
      dsimp [l]
      rw [List.filter_filter]
      have hnil : ps.filter (fun a => a.1 == post && (ownersOf users).contains a.2) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro a ha hbeq
        have : a ∈ ps.filter (fun x => x.1 == post) := by exact List.mem_filter.mpr ⟨ha, (Bool.and_eq_true_iff.mp hbeq).1⟩
        rw [hp] at this
        simp at this
      rw [hnil]
    rw [show (ps.filter (fun p => p.1 == post)).filterMap (fun p =>
        if (ownersOf users).contains p.2 then some p.2 else none) = [] by
        rw [hp]; simp]
    simp
    intro v hm
    have hmp : (post, v) ∈ ps := (List.mem_filter.mp hm).1
    have hpost : (post, v).1 == post := by simp
    have hfilt : (post, v) ∈ ps.filter (fun p => p.1 == post) := by simp [hmp, hpost]
    rw [hp] at hfilt
    simp at hfilt
  · have hpmem : p ∈ ps.filter (fun x : Post => x.1 == post) := by simp [hp]
    have hp1 : p.1 == post := (List.mem_filter.mp hpmem).2
    have hp1eq : p.1 = post := beq_iff_eq.mp hp1
    subst post
    by_cases hc : (ownersOf users).contains p.2 = true
    · have hpsmem : p ∈ ps := (List.mem_filter.mp hpmem).1
      have hmem : p ∈ l := by
        dsimp [l]
        exact List.mem_filter.mpr ⟨hpsmem, hc⟩
      have hget : (Std.HashMap.ofList l)[p.1]? = some p.2 :=
        hashmap_get_ofList_of_mem l p.1 p.2 hnd hmem
      have hcmem : p.snd ∈ ownersOf users := by
        have hc' := hc
        dsimp [ownersOf] at hc'
        rw [Std.HashSet.contains_ofList] at hc'
        rw [ownersOf, Std.HashSet.mem_ofList]
        exact hc'
      rw [hget]
      rw [hp]
      simp [hcmem]
    · have hno : ∀ v, (p.1, v) ∉ l := by
        intro v hm
        have hmp := (List.mem_filter.mp hm).1
        have hp_eq : (p.1, v) = p := by
          have hfilt : (p.1, v) ∈ ps.filter (fun q => q.1 == p.1) := by
            exact List.mem_filter.mpr ⟨hmp, by simpa using hp1⟩
          rw [hp] at hfilt
          simpa using hfilt
        have hcv : (ownersOf users).contains v = true := (List.mem_filter.mp hm).2
        have hv : v = p.2 := congrArg Prod.snd hp_eq
        rw [hv] at hcv
        exact hc hcv
      have hget := hashmap_get_ofList_of_notmem l p.1 hno
      have hcnotmem : ¬ p.snd ∈ ownersOf users := by
        intro hm
        have hc' := hc
        dsimp [ownersOf] at hc'
        rw [Std.HashSet.contains_ofList] at hc'
        apply hc'
        rw [ownersOf, Std.HashSet.mem_ofList] at hm
        exact hm
      rw [hget]
      rw [hp]
      simp [hcnotmem]

private theorem owners_contains_iff_filter (users : List User) (id : Nat) :
    (ownersOf users).contains id = true ↔
      (users.filter (fun u => u.id == id && 2012 ≤ u.year)).length > 0 := by
  rw [ownersOf, hashset_filter_map_iff]
  constructor
  · rintro ⟨u, hus, hy, hid⟩
    apply List.length_pos_iff.mpr
    intro hnil
    have hm : u ∈ users.filter (fun u => u.id == id && 2012 ≤ u.year) := by
      simp [hus, hid, hy]
    rw [hnil] at hm
    simp at hm
  · intro h
    have hne := List.ne_nil_of_length_pos h
    obtain ⟨u, hum⟩ := List.exists_mem_of_ne_nil _ hne
    have hmem := List.mem_filter.mp hum
    refine ⟨u, hmem.1, ?_, ?_⟩
    · exact (Bool.and_eq_true_iff.mp hmem.2).2
    · rw [beq_iff_eq.mp (Bool.and_eq_true_iff.mp hmem.2).1]

private theorem eligible_contains_iff_filter (users : List User) (id : Nat) :
    (eligibleOf users).contains id = true ↔
      (users.filter (fun u => u.id == id && 1000 < u.rep)).length > 0 := by
  rw [eligibleOf, hashset_filter_map_iff]
  constructor
  · rintro ⟨u, hus, hr, hid⟩
    apply List.length_pos_iff.mpr
    intro hnil
    have hm : u ∈ users.filter (fun u => u.id == id && 1000 < u.rep) := by
      simp [hus, hid, hr]
    rw [hnil] at hm
    simp at hm
  · intro h
    have hne := List.ne_nil_of_length_pos h
    obtain ⟨u, hum⟩ := List.exists_mem_of_ne_nil _ hne
    have hmem := List.mem_filter.mp hum
    refine ⟨u, hmem.1, ?_, ?_⟩
    · exact (Bool.and_eq_true_iff.mp hmem.2).2
    · rw [beq_iff_eq.mp (Bool.and_eq_true_iff.mp hmem.2).1]

private def refOne (users : List User) (ps : List Post) (post who : Nat) : List Nat :=
  (ps.filter (fun p => p.1 == post)).flatMap fun p =>
  (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
  (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id

private def optOne (users : List User) (ps : List Post) (post who : Nat) : Option Nat :=
  if (eligibleOf users).contains who then (routeOf users ps)[post]? else none

private theorem filter_and_eq {α : Type} (q r : α → Bool) (xs : List α) :
    xs.filter (fun a => q a && r a) = (xs.filter q).filter r := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      by_cases hq : q a = true <;> by_cases hr : r a = true <;> simp [hq, hr, ih]

private theorem filter_and_nil {α : Type} (q r : α → Bool) (xs : List α)
    (h : xs.filter q = []) : xs.filter (fun a => q a && r a) = [] := by
  rw [filter_and_eq, h]
  simp

private theorem optOne_eq_refOne (users : List User) (ps : List Post)
    (hnu : (users.map User.id).Nodup) (hnp : (ps.map Prod.fst).Nodup) (post who : Nat) :
    optOne users ps post who = match refOne users ps post who with
      | [] => none | x :: _ => some x := by
  unfold optOne refOne
  rcases filter_singleton (fun p : Post => p.1) ps hnp post with hp | ⟨p, hp⟩
  · have hroute : (routeOf users ps)[post]? = none := by
      rw [route_get users ps hnp post, hp]
      simp
    have href : (ps.filter (fun p : Post => p.1 == post)).flatMap (fun p =>
        (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
        (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id) = [] := by
      rw [hp]; simp
    rw [href, hroute]
    simp
  · rcases filter_singleton (fun u : User => u.id) users hnu p.2 with ho | ⟨owner, ho⟩
    · have hoYear : users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) = [] := by
        apply filter_and_nil (fun u : User => u.id == p.2) (fun u => 2012 ≤ u.year) users ho
      have hroute : (routeOf users ps)[post]? = none := by
        rw [route_get users ps hnp post]
        have hf : (ps.filter (fun p : Post => p.1 == post)).filterMap (fun p =>
            if (ownersOf users).contains p.2 then some p.2 else none) = [] := by
          rw [hp]
          have hcF : (ownersOf users).contains p.2 = false := by
            apply Bool.eq_false_iff.mpr
            intro hc
            have hl := (owners_contains_iff_filter users p.2).mp hc
            rw [hoYear] at hl
            simp at hl
          have hcFnot : ¬ p.2 ∈ ownersOf users := by
            intro hm
            have ht := (Std.HashSet.mem_iff_contains.mp hm)
            rw [hcF] at ht
            contradiction
          simp [hcFnot]
        rw [hf]
        simp
      have href : (ps.filter (fun p : Post => p.1 == post)).flatMap (fun p =>
          (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
          (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id) = [] := by
        rw [hp]
        simp [hoYear]
      rw [href, hroute]
      simp
    · have hoMem : owner ∈ users.filter (fun u : User => u.id == p.2) := by simp [ho]
      have hoid : owner.id == p.2 := (List.mem_filter.mp hoMem).2
      have hoidEq : owner.id = p.2 := beq_iff_eq.mp hoid
      by_cases hy : 2012 ≤ owner.year
      · have hoYear : users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) = [owner] := by
          rw [show users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) =
              (users.filter (fun u : User => u.id == p.2)).filter (fun u => 2012 ≤ u.year) by
                rw [filter_and_eq]]
          rw [ho]
          simp [hy]
        have hcT : (ownersOf users).contains p.2 = true := (owners_contains_iff_filter users p.2).mpr (by
          rw [hoYear]; simp)
        have hroute := route_get users ps hnp post
        rcases filter_singleton (fun u : User => u.id) users hnu who with he | ⟨elig, he⟩
        · have heRep : users.filter (fun u => u.id == who && 1000 < u.rep) = [] := by
            apply filter_and_nil (fun u : User => u.id == who) (fun u => 1000 < u.rep) users he
          have hElFalse : (eligibleOf users).contains who = false := by
            apply Bool.eq_false_iff.mpr
            intro hc
            have hl := (eligible_contains_iff_filter users who).mp hc
            rw [heRep] at hl
            simp at hl
          have href : (ps.filter (fun p : Post => p.1 == post)).flatMap (fun p =>
              (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
              (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id) = [] := by
            simp [hp, hoYear, heRep]

          rw [href]
          simp [hElFalse]
        · have heMem : elig ∈ users.filter (fun u : User => u.id == who) := by simp [he]
          have heid : elig.id == who := (List.mem_filter.mp heMem).2
          by_cases hr : 1000 < elig.rep
          · have heRep : users.filter (fun u => u.id == who && 1000 < u.rep) = [elig] := by
              rw [show users.filter (fun u => u.id == who && 1000 < u.rep) =
                  (users.filter (fun u : User => u.id == who)).filter (fun u => 1000 < u.rep) by
                rw [filter_and_eq]]
              rw [he]
              simp [hr]
            have hElTrue : (eligibleOf users).contains who = true := (eligible_contains_iff_filter users who).mpr (by
              rw [heRep]; simp)
            have hf : (ps.filter (fun p : Post => p.1 == post)).filterMap (fun p =>
                if (ownersOf users).contains p.2 then some p.2 else none) = [p.2] := by
              rw [hp]
              have hcTmem : p.2 ∈ ownersOf users := Std.HashSet.mem_iff_contains.mpr hcT
              simp [hcTmem]
            have hrouteSome : (routeOf users ps)[post]? = some p.2 := by
              rw [hroute, hf]
              rfl
            have href : (ps.filter (fun p : Post => p.1 == post)).flatMap (fun p =>
                (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
                (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id) = [owner.id] := by
              simp [hp, hoYear, heRep]

            rw [hrouteSome, href]
            simp [hElTrue, hoidEq]
          · have heRep : users.filter (fun u => u.id == who && 1000 < u.rep) = [] := by
              rw [show users.filter (fun u => u.id == who && 1000 < u.rep) =
                  (users.filter (fun u : User => u.id == who)).filter (fun u => 1000 < u.rep) by
                rw [filter_and_eq]]
              rw [he]
              simp [hr]
            have hElFalse : (eligibleOf users).contains who = false := by
              apply Bool.eq_false_iff.mpr
              intro hc
              have hl := (eligible_contains_iff_filter users who).mp hc
              rw [heRep] at hl
              simp at hl
            have href : (ps.filter (fun p : Post => p.1 == post)).flatMap (fun p =>
                (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
                (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id) = [] := by
              simp [hp, hoYear, heRep]

            rw [href]
            simp [hElFalse]
      · have hoYear : users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) = [] := by
          rw [show users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) =
              (users.filter (fun u : User => u.id == p.2)).filter (fun u => 2012 ≤ u.year) by
                rw [filter_and_eq]]
          rw [ho]
          simp [hy]
        have hcF : (ownersOf users).contains p.2 = false := by
          apply Bool.eq_false_iff.mpr
          intro hc
          have hl := (owners_contains_iff_filter users p.2).mp hc
          rw [hoYear] at hl
          simp at hl
        have hroute := route_get users ps hnp post
        have hf : (ps.filter (fun p : Post => p.1 == post)).filterMap (fun p =>
            if (ownersOf users).contains p.2 then some p.2 else none) = [] := by
          rw [hp]
          have hcFnot : ¬ p.2 ∈ ownersOf users := by
            intro hm
            have ht := (Std.HashSet.mem_iff_contains.mp hm)
            rw [hcF] at ht
            contradiction
          simp [hcFnot]
        have hrouteNone : (routeOf users ps)[post]? = none := by rw [hroute, hf]; simp
        have href : (ps.filter (fun p : Post => p.1 == post)).flatMap (fun p =>
            (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
            (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id) = [] := by
          simp [hp, hoYear]

        rw [href, hrouteNone]
        simp

private theorem refOne_cases (users : List User) (ps : List Post)
    (hnu : (users.map User.id).Nodup) (hnp : (ps.map Prod.fst).Nodup) (post who : Nat) :
    refOne users ps post who = [] ∨ ∃ x, refOne users ps post who = [x] := by
  unfold refOne
  rcases filter_singleton (fun p : Post => p.1) ps hnp post with hp | ⟨p, hp⟩
  · left; rw [hp]; simp
  · rw [hp]
    rcases filter_singleton (fun u : User => u.id) users hnu p.2 with ho | ⟨owner, ho⟩
    · left
      have hoY : users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) = [] := by
        apply filter_and_nil (fun u : User => u.id == p.2) (fun u => 2012 ≤ u.year) users ho
      simp [hoY]
    · by_cases hy : 2012 ≤ owner.year
      · have hoY : users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) = [owner] := by
          rw [show users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) =
              (users.filter (fun u : User => u.id == p.2)).filter (fun u => 2012 ≤ u.year) by
            rw [filter_and_eq]]
          rw [ho]
          simp [hy]
        rcases filter_singleton (fun u : User => u.id) users hnu who with he | ⟨elig, he⟩
        · left
          have heR : users.filter (fun u => u.id == who && 1000 < u.rep) = [] := by
            apply filter_and_nil (fun u : User => u.id == who) (fun u => 1000 < u.rep) users he
          simp [hoY, heR]
        · by_cases hr : 1000 < elig.rep
          · right
            refine ⟨owner.id, ?_⟩
            have heR : users.filter (fun u => u.id == who && 1000 < u.rep) = [elig] := by
              rw [show users.filter (fun u => u.id == who && 1000 < u.rep) =
                  (users.filter (fun u : User => u.id == who)).filter (fun u => 1000 < u.rep) by
                rw [filter_and_eq]]
              rw [he]
              simp [hr]
            simp [hoY, heR]
          · left
            have heR : users.filter (fun u => u.id == who && 1000 < u.rep) = [] := by
              rw [show users.filter (fun u => u.id == who && 1000 < u.rep) =
                  (users.filter (fun u : User => u.id == who)).filter (fun u => 1000 < u.rep) by
                rw [filter_and_eq]]
              rw [he]
              simp [hr]
            simp [hoY, heR]
      · left
        have hoY : users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) = [] := by
          rw [show users.filter (fun u => u.id == p.2 && 2012 ≤ u.year) =
              (users.filter (fun u : User => u.id == p.2)).filter (fun u => 2012 ≤ u.year) by
            rw [filter_and_eq]]
          rw [ho]
          simp [hy]
        simp [hoY]

private theorem refOne_of_opt (users : List User) (ps : List Post)
    (hnu : (users.map User.id).Nodup) (hnp : (ps.map Prod.fst).Nodup) (post who : Nat) :
    refOne users ps post who = match optOne users ps post who with
      | none => [] | some x => [x] := by
  have h := optOne_eq_refOne users ps hnu hnp post who
  rcases refOne_cases users ps hnu hnp post who with hnil | ⟨x, hx⟩
  · rw [hnil] at h
    simp at h
    rw [hnil, h]

  · rw [hx] at h
    simp at h
    rw [hx, h]


private theorem routed_list_eq (users : List User) (ps : List Post) (cs : List Comment)
    (hnu : (users.map User.id).Nodup) (hnp : (ps.map Prod.fst).Nodup) :
    cs.filterMap (fun (post,who) => optOne users ps post who) =
      cs.flatMap (fun (post,who) => refOne users ps post who) := by
  induction cs with
  | nil => rfl
  | cons c cs ih =>
      rcases c with ⟨post, who⟩
      have h := refOne_of_opt users ps hnu hnp post who
      cases hO : optOne users ps post who with
      | none =>
          have hr : refOne users ps post who = [] := by simpa [hO] using h
          rw [List.filterMap_cons, List.flatMap_cons]
          simp [hO, hr, ih]
      | some x =>
          have hr : refOne users ps post who = [x] := by simpa [hO] using h
          rw [List.filterMap_cons, List.flatMap_cons]
          simp [hO, hr, ih]

theorem RoleRouting_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.UserRolesAndCommentJoins.RoleRouting.Goal := by
  intro users ps cs hnu hnp
  apply List.Perm.of_eq
  change cs.filterMap (fun (post, who) =>
      if (eligibleOf users).contains who then (routeOf users ps)[post]? else none) =
    cs.flatMap (fun (post, who) =>
      (ps.filter (fun p => p.1 == post)).flatMap fun p =>
      (users.filter (fun u => u.id == p.2 && 2012 ≤ u.year)).flatMap fun owner =>
      (users.filter (fun u => u.id == who && 1000 < u.rep)).map fun _ => owner.id)
  change cs.filterMap (fun (post, who) => optOne users ps post who) =
    cs.flatMap (fun (post, who) => refOne users ps post who)
  exact routed_list_eq users ps cs hnu hnp

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.UserRolesAndCommentJoins

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.cpp
-- executor/detail/execution_strategies.hpp
