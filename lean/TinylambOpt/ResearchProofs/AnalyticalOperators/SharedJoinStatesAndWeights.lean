import TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights
import TinylambOpt.ResearchProofs.AnalyticsWorkloads.HotKeyAndEpochAggregates

/-!
These models connect SQL operators to their reference rows, order, or aggregate results.

Proved (7 claims, including the following)

- `CollisionBucket`
- `CommentQuotient` — C2: aggregate repeated comments before routing by post.
  Owner-role and commenter-role multiplicities come from different filtered
  projections of users; neither post ids nor user ids have to be globally unique.
- `CountryProfiles` — L1: intern identical country multisets of incoming edges.
  Aggregate active friend multiplicity per interned profile, then scatter to
  countries once per profile. Canonicalizing by set would lose parallel
  knows/source/place paths.
-/

namespace TinylambOpt.ResearchProofs.AnalyticalOperators.SharedJoinStatesAndWeights

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.HotKeyAndEpochAggregates
open TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown (LRow RRow PreAgg refGroup refJoinedPairs sumOptList)
open TinylambOpt.ResearchGoals.QueryRewrites.JoinMultiplicityAndZoneFilters (PostRec LinkRec refBridgeAgg refDualUserCount UserRow)
open TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights (Bucket classify bucketWeight)

set_option maxHeartbeats 0

private theorem beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    simp [heq]
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    simp [heq]

/-! ### Shared algebra for Totals and histograms -/

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  rcases a with ⟨ac,an,as⟩
  rcases b with ⟨bc,bn,bs⟩
  rcases c with ⟨cc,cn,cs⟩
  simp [plus]
  omega

private theorem plus_comm (a b : Totals) : plus a b = plus b a := by
  rcases a with ⟨ac,an,as⟩
  rcases b with ⟨bc,bn,bs⟩
  simp [plus]
  omega

private theorem plus_left_comm (a b c : Totals) : plus a (plus b c) = plus b (plus a c) := by
  rcases a with ⟨ac,an,as⟩
  rcases b with ⟨bc,bn,bs⟩
  rcases c with ⟨cc,cn,cs⟩
  simp [plus]
  omega

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  rcases a with ⟨ac,an,as⟩
  simp [plus, zero]

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  rcases a with ⟨ac,an,as⟩
  simp [plus, zero]

private theorem foldl_plus_left {α : Type} (xs : List α) (g : α → Totals) (acc a : Totals) :
    xs.foldl (fun s x => plus s (g x)) (plus a acc) =
      plus a (xs.foldl (fun s x => plus s (g x)) acc) := by
  induction xs generalizing acc with
  | nil => simp [plus_comm]
  | cons x xs ih =>
      change xs.foldl (fun s y => plus s (g y)) (plus (plus a acc) (g x)) =
        plus a (xs.foldl (fun s y => plus s (g y)) (plus acc (g x)))
      rw [show plus (plus a acc) (g x) = plus a (plus acc (g x)) by simp [plus_assoc]]
      exact ih (acc := plus acc (g x))

private theorem foldl_plus_start {α : Type} (xs : List α) (g : α → Totals) (acc : Totals) :
    xs.foldl (fun s x => plus s (g x)) acc =
      plus acc (xs.foldl (fun s x => plus s (g x)) zero) := by
  simpa [plus_zero_right] using foldl_plus_left xs g zero acc

private theorem summarize_append (xs ys : List (Option Int)) :
    summarize (xs ++ ys) = plus (summarize xs) (summarize ys) := by
  unfold summarize
  rw [List.foldl_append, foldl_plus_start]

private theorem summarize_cons (v : Option Int) (vs : List (Option Int)) :
    summarize (v :: vs) = plus (weighted 1 v) (summarize vs) := by
  change vs.foldl (fun a x => plus a (weighted 1 x)) (plus zero (weighted 1 v)) =
    plus (weighted 1 v) (vs.foldl (fun a x => plus a (weighted 1 x)) zero)
  simpa [plus_zero_left, plus_zero_right] using
    foldl_plus_left vs (fun x : Option Int => weighted 1 x) zero (weighted 1 v)

private theorem summarize_replicate (n : Nat) (v : Option Int) :
    summarize (List.replicate n v) = weighted n v := by
  induction n with
  | zero => simp [summarize, weighted, zero]
  | succ n ih =>
      rw [List.replicate_succ, summarize_cons, ih]
      rcases v with _ | z
      · simp [weighted, plus]
        omega
      · simp [weighted, plus]
        constructor
        · omega
        · rw [Int.add_mul]
          simp [Int.add_comm]

private theorem weighted_eq_summarize_replicate (n : Nat) (v : Option Int) :
    weighted n v = summarize (List.replicate n v) := (summarize_replicate n v).symm

private theorem histogram_getD_fold {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (m : Std.HashMap α Nat) (k : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD k 0 =
      m.getD k 0 + (xs.filter fun x => x == k).length := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih]
      by_cases hx : x == k
      · have hxe : x = k := beq_iff_eq.mp hx
        subst k
        simp [Std.HashMap.getD_insert]
        omega
      · simp [Std.HashMap.getD_insert, hx]

private theorem histogram_getD_count {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (k : α) :
    (histogram xs).getD k 0 = (xs.filter fun x => x == k).length := by
  simp [histogram, histogram_getD_fold]

private theorem perm_sum_nat {α : Type} (g : α → Nat) {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    (l₁.map g).sum = (l₂.map g).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using ih
  | swap a b l => simp [Nat.add_left_comm, Nat.add_comm]
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

private theorem filter_eq_nil_of_all_false {α : Type} (p : α → Bool) {xs : List α}
    (h : ∀ x ∈ xs, p x = false) : xs.filter p = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ z ∈ xs, p z = false := by intro z hz; exact h z (by simp [hz])
      simp [hx, ih ht]

private theorem filter_eq_singleton_of_pairwise {α β : Type} [BEq α] [LawfulBEq α]
    (L : List (α × β)) (k : α) (v : β)
    (hmem : (k, v) ∈ L)
    (hdist : L.Pairwise (fun a b => (a.1 == b.1) = false)) :
    L.filter (fun p => k == p.1) = [(k, v)] := by
  induction L with
  | nil => simp at hmem
  | cons p ps ih =>
      by_cases hpk : p.1 = k
      · have hkp : (k == p.1) = true := by simp [hpk]
        have hno : ∀ q ∈ ps, (k == q.1) = false := by
          have hpair := (List.pairwise_cons.mp hdist).1
          intro q hq
          have hfalse := hpair q hq
          simpa [hpk] using hfalse
        have hfilterps : ps.filter (fun q => k == q.1) = [] := by
          apply filter_eq_nil_of_all_false
          intro q hq
          exact hno q hq
        have hmemhead : p = (k, v) := by
          rw [List.mem_cons] at hmem
          rcases hmem with hp | hq
          · cases hp
            rfl
          · have hn : (k == (k, v).fst) = false := hno (k, v) hq
            simp at hn
        simp [hkp, hfilterps, hmemhead]
      · have hkp : (k == p.1) = false := by simpa [show k ≠ p.1 by intro h; exact hpk h.symm]
        simp [hkp]
        apply ih
        · rw [List.mem_cons] at hmem
          rcases hmem with hp | hq
          · exfalso
            have hp' : p.1 = k := by rw [← hp]
            exact hpk hp'
          · exact hq
        · exact (List.pairwise_cons.mp hdist).2

private theorem sum_filter_partition {α : Type} (L : List α) (p : α → Bool) (g : α → Nat) :
    ((L.filter p).map g).sum + ((L.filter fun x => !p x).map g).sum = (L.map g).sum := by
  induction L with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · have hnp : (!p x) = false := by simp [hp]
        simp [hp, hnp]
        omega
      · have hnp : (!p x) = true := by simp [hp]
        simp [hp, hnp]
        rw [← ih]
        omega

private theorem toList_matching_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (f : α → Nat) :
    ((m.toList.filter (fun p => k == p.1)).map fun p => p.2 * f p.1).sum =
      m.getD k 0 * f k := by
  by_cases hmem : k ∈ m
  · have hget : m.get? k = some (m.getD k 0) := Std.HashMap.getElem?_eq_some_getD hmem
    rcases (Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList.mp hget) with ⟨k', hk', hm'⟩
    have hk'eq : k = k' := by simpa using hk'
    subst k'
    have hfilt : m.toList.filter (fun p => k == p.1) = [(k, m.getD k 0)] :=
      filter_eq_singleton_of_pairwise m.toList k (m.getD k 0) hm' (Std.HashMap.distinct_keys_toList (m := m))
    rw [hfilt]
    simp
  · have hget : m[k]? = none := Std.HashMap.getElem?_eq_none hmem
    have hno : ∀ p ∈ m.toList, (k == p.1) = false := by
      intro p hp
      by_cases hbeq : (k == p.1) = true
      · have hpeq' : k = p.1 := by simpa using hbeq
        have hgetp : m[p.1]? = some p.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hp
        have hcontra : m[k]? = some p.2 := by simpa [hpeq'] using hgetp
        rw [hget] at hcontra
        cases hcontra
      · exact Bool.eq_false_of_ne_true hbeq
    have hfilt : m.toList.filter (fun p => k == p.1) = [] := by
      apply filter_eq_nil_of_all_false
      intro p hp
      exact hno p hp
    rw [hfilt]
    have hzero : m.getD k 0 = 0 := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hget]
      rfl
    rw [hzero]
    simp

private theorem S_insert_add {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (w : Nat) (f : α → Nat) :
    ((m.insert k (m.getD k 0 + w)).toList.map fun p => p.2 * f p.1).sum =
      (m.toList.map fun p => p.2 * f p.1).sum + w * f k := by
  let g : α × Nat → Nat := fun p => p.2 * f p.1
  let v := m.getD k 0 + w
  have hp := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := v)
  have hsum := perm_sum_nat g hp
  have hmatch := toList_matching_sum m k f
  have hfilter_eq : (m.toList.filter (fun x => decide ¬(k == x.fst) = true)) =
      m.toList.filter (fun p : α × Nat => !(k == p.1)) := by
    apply List.filter_congr
    intro p hp
    cases h : k == p.fst <;> simp [h]
  rw [hfilter_eq] at hsum
  simp only [List.map_cons, List.sum_cons] at hsum
  dsimp [g] at hsum
  have hpart := sum_filter_partition m.toList (fun p : α × Nat => k == p.1)
      (fun p => p.2 * f p.1)
  change ((m.insert k v).toList.map fun p => p.2 * f p.1).sum =
    (m.toList.map fun p => p.2 * f p.1).sum + w * f k
  rw [hsum, ← hpart, hmatch]
  dsimp [v]
  rw [Nat.add_mul]
  omega

private theorem S_insert {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (f : α → Nat) :
    ((m.insert k (m.getD k 0 + 1)).toList.map fun p => p.2 * f p.1).sum =
      (m.toList.map fun p => p.2 * f p.1).sum + f k := by
  simpa [Nat.one_mul] using S_insert_add m k 1 f

private theorem histogram_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (f : α → Nat) :
    ((histogram xs).toList.map fun p => p.2 * f p.1).sum = (xs.map f).sum := by
  have h : ∀ (m : Std.HashMap α Nat),
      ((xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).toList.map fun p => p.2 * f p.1).sum =
        (m.toList.map fun p => p.2 * f p.1).sum + (xs.map f).sum := by
    intro m
    induction xs generalizing m with
    | nil => simp
    | cons x xs ih =>
        simp only [List.foldl_cons, List.map_cons, List.sum_cons]
        rw [ih, S_insert]
        omega
  simpa [histogram] using h ({} : Std.HashMap α Nat)

private theorem foldl_pair_sum {α : Type} (l : List (α × Nat)) (f : α → Nat) (acc : Nat) :
    l.foldl (fun n p => n + p.2 * f p.1) acc = acc + ((l.map fun p => p.2 * f p.1).sum) := by
  induction l generalizing acc with
  | nil => simp
  | cons p l ih =>
      simp only [List.foldl_cons]
      rw [ih]
      simp
      omega

private theorem histogram_fold_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (f : α → Nat) :
    (histogram xs).toList.foldl (fun n p => n + p.2 * f p.1) 0 = (xs.map f).sum := by
  rw [foldl_pair_sum]
  simpa using histogram_sum xs f


private theorem weighted_fold_sum {α β : Type} [BEq β] [Hashable β] [LawfulBEq β] [LawfulHashable β]
    (entries : List (α × Nat)) (m0 : Std.HashMap β Nat) (key : α → β) (f : β → Nat) :
    ((entries.foldl (fun m p => let k := key p.1; m.insert k (m.getD k 0 + p.2)) m0).toList.foldl
        (fun n p => n + p.2 * f p.1) 0) =
      (m0.toList.foldl (fun n p => n + p.2 * f p.1) 0) +
        (entries.map fun p => p.2 * f (key p.1)).sum := by
  induction entries generalizing m0 with
  | nil => simp
  | cons p ps ih =>
      simp only [List.foldl_cons]
      rw [ih]
      rw [foldl_pair_sum]
      rw [foldl_pair_sum]
      simp only [List.map_cons, List.sum_cons]
      rw [S_insert_add]
      omega


/-! ### L1: CountryProfiles -/

private theorem adjacency_getD_fold (edges : List (Nat × Nat)) (m : Std.HashMap Nat (List Nat)) (f : Nat) :
    (edges.foldl (fun (m : Std.HashMap Nat (List Nat)) (e : Nat × Nat) =>
      m.insert e.1 (e.2 :: m.getD e.1 [])) m).getD f [] =
      ((edges.filter fun e => e.1 == f).map Prod.snd).reverse ++ m.getD f [] := by
  induction edges generalizing m with
  | nil => simp
  | cons e es ih =>
      rcases e with ⟨x, c⟩
      rw [List.foldl_cons, ih]
      by_cases h : x == f
      · have hx : x = f := beq_iff_eq.mp h
        subst f
        simp [Std.HashMap.getD_insert, h, List.filter_cons_of_pos,
          List.reverse_cons, List.append_assoc]
      · simp [Std.HashMap.getD_insert, h, List.filter_cons_of_neg]

private theorem count_mergeSort (cs : List Nat) (country : Nat) :
    (cs.mergeSort).count country = cs.count country :=
  (List.mergeSort_perm cs _).count country

private theorem edge_count_eq (edges : List (Nat × Nat)) (f country : Nat) :
    ((edges.filter fun e => e.1 == f).map Prod.snd).count country =
      (edges.map fun e => if e.1 == f && e.2 == country then 1 else 0).sum := by
  induction edges with
  | nil => simp
  | cons e es ih =>
      rcases e with ⟨x, c⟩
      by_cases hx : x == f
      · have hxeq : x = f := beq_iff_eq.mp hx
        subst x
        by_cases hc : c == country
        · have hceq : c = country := beq_iff_eq.mp hc
          subst c
          simp [ih, Nat.add_comm]
        · have hcne : c ≠ country := by intro h; exact hc (by simp [h])
          simp [hc, hcne, ih, Nat.add_comm]
      · have hxne : x ≠ f := by intro h; exact hx (by simp [h])
        simp [hx, hxne, ih]

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map fun x => f x + g x).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_zero {α : Type} (xs : List α) : (xs.map fun _ => 0).sum = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simpa using ih

private theorem sum_swap {α β : Type} (xs : List α) (ys : List β) (f : α → β → Nat) :
    (xs.map fun x => (ys.map fun y => f x y).sum).sum =
      (ys.map fun y => (xs.map fun x => f x y).sum).sum := by
  induction xs with
  | nil => simp [sum_map_zero]
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      rw [sum_map_add]

private theorem count_eq_indicator {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (a : α) :
    xs.count a = (xs.map fun x => if x == a then 1 else 0).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.count_cons, ih]
      by_cases h : x == a <;> simp [h, ih, Nat.add_comm]

private theorem count_eq_indicator_rev {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (a : α) :
    xs.count a = (xs.map fun x => if a == x then 1 else 0).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.count_cons, ih]
      by_cases h : a == x
      · have hx : x = a := by
          have := beq_iff_eq.mp h
          exact this.symm
        subst x
        simp [h, Nat.add_comm]
      · have hx : x ≠ a := by intro hx; exact h (by simp [hx])
        simp [h, hx]

private theorem ite_count_indicator (active : List Nat) (e : Nat × Nat) (country : Nat) :
    (if e.2 == country then (active.map fun a => if e.1 == a then 1 else 0).sum else 0) =
      (active.map fun a => if e.1 == a && e.2 == country then 1 else 0).sum := by
  by_cases hc : e.2 == country
  · rw [ite_eq_left hc]
    have hceq : e.2 = country := beq_iff_eq.mp hc
    subst country
    induction active with
    | nil => simp
    | cons a as ih =>
        by_cases ha : e.1 == a
        · have haeq : a = e.1 := by
            have := beq_iff_eq.mp ha
            exact this.symm
          subst a
          simp [ih]
        · simp [ha, ih]
  · rw [ite_eq_right hc]
    have hfalse : ∀ a : Nat, (e.1 == a && e.2 == country) = false := by
      intro a
      simp [hc]
    simp [hfalse, sum_map_zero]

private theorem active_edges_sum (edges : List (Nat × Nat)) (active : List Nat) (country : Nat) :
    (active.map fun f => ((edges.filter fun e => e.1 == f).map Prod.snd).count country).sum =
      (edges.map fun e => if e.2 == country then active.count e.1 else 0).sum := by
  rw [show (edges.map fun e => if e.2 == country then active.count e.1 else 0).sum =
      (edges.map fun e =>
        if e.2 == country then (active.map fun a => if e.1 == a then 1 else 0).sum else 0).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro e he
        rw [count_eq_indicator_rev]]
  rw [show (edges.map fun e =>
        if e.2 == country then (active.map fun a => if e.1 == a then 1 else 0).sum else 0).sum =
      (edges.map fun e => (active.map fun a => if e.1 == a && e.2 == country then 1 else 0).sum).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro e he
        exact ite_count_indicator active e country]
  rw [sum_swap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro f hf
  exact edge_count_eq edges f country

theorem CountryProfiles_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights.CountryProfiles.Goal := by
  intro edges active country
  dsimp only
  let adjacency := edges.foldl (fun (m : Std.HashMap Nat (List Nat)) (p : Nat × Nat) =>
    m.insert p.1 (p.2 :: m.getD p.1 [])) ({} : Std.HashMap Nat (List Nat))
  let profiles := (histogram active).toList.foldl
    (fun (m : Std.HashMap (List Nat) Nat) (p : Nat × Nat) =>
      let cs := (adjacency.getD p.1 []).mergeSort (fun a b : Nat => decide (a ≤ b))
      m.insert cs (m.getD cs 0 + p.2)) ({} : Std.HashMap (List Nat) Nat)
  change ((histogram active).toList.foldl
    (fun (m : Std.HashMap (List Nat) Nat) (p : Nat × Nat) =>
      let cs := (adjacency.getD p.1 []).mergeSort (fun a b : Nat => decide (a ≤ b))
      m.insert cs (m.getD cs 0 + p.2)) ({} : Std.HashMap (List Nat) Nat)).toList.foldl
      (fun n (p : List Nat × Nat) => n + p.2 * p.1.count country) 0 =
    (edges.map (fun (f, c) => if c == country then active.count f else 0)).sum
  rw [weighted_fold_sum (entries := (histogram active).toList)
    (m0 := ({} : Std.HashMap (List Nat) Nat))
    (key := fun f : Nat => (adjacency.getD f []).mergeSort (fun a b : Nat => decide (a ≤ b)))
    (f := fun cs : List Nat => cs.count country)]
  rw [show ({} : Std.HashMap (List Nat) Nat).toList = [] by simp]
  simp only [List.foldl_nil, Nat.zero_add]
  rw [histogram_sum active (fun f => ((adjacency.getD f []).mergeSort (fun a b : Nat => decide (a ≤ b))).count country)]
  have hadj : ∀ f : Nat, adjacency.getD f [] =
      ((edges.filter fun e => e.1 == f).map Prod.snd).reverse := by
    intro f
    rw [show adjacency = edges.foldl (fun (m : Std.HashMap Nat (List Nat)) (e : Nat × Nat) =>
      m.insert e.1 (e.2 :: m.getD e.1 [])) {} by rfl]
    rw [adjacency_getD_fold]
    simp
  have hcnt : ∀ f : Nat, ((adjacency.getD f []).mergeSort (fun a b : Nat => decide (a ≤ b))).count country =
      ((edges.filter fun e => e.1 == f).map Prod.snd).count country := by
    intro f
    rw [hadj]
    have hp := (List.mergeSort_perm ((edges.filter fun e => e.1 == f).map Prod.snd).reverse
      (fun a b : Nat => decide (a ≤ b))).count country
    simpa using hp
  simp only [hcnt]
  exact active_edges_sum edges active country


/-! ### C4: CollisionBucket -/

private theorem fold_weighted_eq_summarize {α : Type} (xs : List α) (w : α → Nat) (v : α → Option Int) :
    xs.foldl (fun a x => plus a (weighted (w x) (v x))) zero =
      summarize (xs.flatMap fun x => List.replicate (w x) (v x)) := by
  induction xs with
  | nil => simp [summarize]
  | cons x xs ih =>
      simp only [List.foldl_cons, List.flatMap_cons]
      rw [plus_zero_left, foldl_plus_start, ih, summarize_append, summarize_replicate]

private theorem bucketWeight_classify_count (labels : List String) (s : String) :
    bucketWeight (classify labels) s = max 1 (labels.count s) := by
  cases labels with
  | nil => simp [classify, bucketWeight]
  | cons h tail =>
      by_cases hall : tail.all (fun x => x == h)
      · have hAllEq : ∀ x ∈ tail, x = h := by
          intro x hx
          exact beq_iff_eq.mp ((List.all_eq_true.mp hall) x hx)
        have hcntH : tail.count h = tail.length := (List.count_eq_length).mpr (by
          intro b hb
          exact (hAllEq b hb).symm)
        simp [classify, bucketWeight, hall]
        by_cases hs : s == h
        · have hseq : s = h := beq_iff_eq.mp hs
          subst s
          simp [hcntH]
        · have hsne : s ≠ h := by intro heq; exact hs (by simp [heq])
          have hcnt0 : tail.count s = 0 := (List.count_eq_zero).mpr (by
            intro hm
            exact hsne (hAllEq s hm))
          have hhead : (h == s) = false := by
            rw [beq_comm h s]
            exact Bool.eq_false_of_ne_true hs
          simp only [if_neg hsne]
          rw [List.count_cons, hhead, hcnt0]
          simp
      · simp [classify, bucketWeight, hall]

private theorem countP_some_eq_filterMap_count (tags : List (Option String)) (s : String) :
    tags.countP (fun t => t == some s) = (tags.filterMap id).count s := by
  induction tags with
  | nil => rfl
  | cons t ts ih =>
      cases t with
      | none => simp [ih]
      | some x =>
          by_cases hx : x == s
          · have hxe : x = s := beq_iff_eq.mp hx
            subst s
            simp [ih]
          · have hxne : x ≠ s := by intro heq; exact hx (by simp [heq])
            simp [hx, hxne, ih]

private theorem tags_count_bucket (tags : List (Option String)) (hashKey : String → Nat) (s : String) :
    ((tags.filterMap id).filter fun x => hashKey x == hashKey s).count s =
      tags.countP (fun t => t == some s) := by
  induction tags with
  | nil => rfl
  | cons t ts ih =>
      cases t with
      | none => simp [ih]
      | some x =>
          by_cases hx : x == s
          · have hxe : x = s := beq_iff_eq.mp hx
            subst x
            simp [ih]
          · have hxne : x ≠ s := by intro heq; exact hx (by simp [heq])
            by_cases hh : hashKey x == hashKey s
            · simp [hx, hxne, hh, ih]
            · simp [hx, hxne, hh, ih]

private theorem bucket_list_getD_fold (tags : List String) (m : Std.HashMap Nat (List String))
    (hashKey : String → Nat) (h : Nat) :
    (tags.foldl (fun (m : Std.HashMap Nat (List String)) (s : String) =>
      m.insert (hashKey s) (s :: m.getD (hashKey s) [])) m).getD h [] =
      (tags.filter fun s => hashKey s == h).reverse ++ m.getD h [] := by
  induction tags generalizing m with
  | nil => simp
  | cons s ss ih =>
      rw [List.foldl_cons, ih]
      by_cases hs : hashKey s == h
      · have hse : hashKey s = h := beq_iff_eq.mp hs
        subst h
        simp [Std.HashMap.getD_insert, hs, List.filter_cons_of_pos,
          List.reverse_cons, List.append_assoc]
      · simp [Std.HashMap.getD_insert, hs, List.filter_cons_of_neg]

private theorem foldl_insert_getD_avoid {α β γ : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (entries : List (α × β)) (m0 : Std.HashMap α γ) (h : α) (fallback : γ) (T : β → γ)
    (havoid : ∀ p ∈ entries, (p.1 == h) = false) :
    (entries.foldl (fun m p => m.insert p.1 (T p.2)) m0).getD h fallback = m0.getD h fallback := by
  induction entries generalizing m0 with
  | nil => rfl
  | cons p ps ih =>
      have hp := havoid p (by simp)
      simp only [List.foldl_cons]
      rw [ih (m0.insert p.1 (T p.2)) (by
        intro q hq
        exact havoid q (by simp [hq]))]
      rw [Std.HashMap.getD_insert, hp]
      simp

private theorem foldl_insert_getD {α β γ : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (entries : List (α × β)) (m0 : Std.HashMap α γ) (h : α) (fallback : γ) (T : β → γ)
    (hd : entries.Pairwise (fun a b => (a.1 == b.1) = false)) :
    (entries.foldl (fun m p => m.insert p.1 (T p.2)) m0).getD h fallback =
      match entries.find? (fun p => p.1 == h) with
      | none => m0.getD h fallback
      | some p => T p.2 := by
  induction entries generalizing m0 with
  | nil => simp
  | cons p ps ih =>
      have hpair := (List.pairwise_cons.mp hd).1
      have htail := (List.pairwise_cons.mp hd).2
      by_cases hpT : p.1 == h
      · have hp : (p.1 == h) = true := hpT
        have hpeq : p.1 = h := beq_iff_eq.mp hp
        have hqeq : ∀ q ∈ ps, (q.1 == h) = false := by
          intro q hq
          have hqpair := hpair q hq
          rw [← hpeq]
          rw [beq_comm q.1 p.1]
          exact hqpair
        have hpfirst : ps.find? (fun q => q.1 == h) = none := by
          rw [List.find?_eq_none]
          intro q hq htrue
          have hf := hqeq q hq
          rw [htrue] at hf
          contradiction
        have hins : (m0.insert p.1 (T p.2)).getD h fallback = T p.2 := by
          rw [Std.HashMap.getD_insert, hp]
          simp
        simp only [List.foldl_cons]
        rw [foldl_insert_getD_avoid ps (m0.insert p.1 (T p.2)) h fallback T hqeq]
        rw [hins]
        simp [hp, hpfirst]
      · have hp : (p.1 == h) = false := Bool.eq_false_of_ne_true hpT
        simp only [List.foldl_cons]
        rw [ih (m0.insert p.1 (T p.2)) htail]
        rw [Std.HashMap.getD_insert, hp]
        simp [hp]

private theorem find_of_mem_nodup_keys (L : List (Nat × List String)) (p : Nat × List String)
    (hmem : p ∈ L) (hd : L.Pairwise (fun a b => (a.1 == b.1) = false)) :
    L.find? (fun q => q.1 == p.1) = some p := by
  induction L with
  | nil => simp at hmem
  | cons q qs ih =>
      by_cases hqT : q.1 == p.1
      · have hq : (q.1 == p.1) = true := hqT
        have hqeq : q = p := by
          rw [List.mem_cons] at hmem
          rcases hmem with rfl | hmemtail
          · rfl
          · have hbad := (List.pairwise_cons.mp hd).1 p hmemtail
            simp [hq] at hbad
        simp [hq, hqeq]
      · have hq : (q.1 == p.1) = false := Bool.eq_false_of_ne_true hqT
        rw [List.find?_cons, hq]
        apply ih
        · rw [List.mem_cons] at hmem
          rcases hmem with rfl | hmemtail
          · simp at hq
          · exact hmemtail
        · exact (List.pairwise_cons.mp hd).2

private theorem compiled_getD (buckets : Std.HashMap Nat (List String)) (h : Nat) :
    (buckets.toList.foldl (fun (c : Std.HashMap Nat Bucket) (p : Nat × List String) =>
      c.insert p.1 (classify p.2)) ({} : Std.HashMap Nat Bucket)).getD h .empty =
      classify (buckets.getD h []) := by
  rw [foldl_insert_getD buckets.toList ({} : Std.HashMap Nat Bucket) h Bucket.empty classify
    (Std.HashMap.distinct_keys_toList (m := buckets))]
  simp only [Std.HashMap.getD_empty]
  by_cases hm : h ∈ buckets
  · have hget := Std.HashMap.getElem?_eq_some_getD (m := buckets) (fallback := []) hm
    rcases (Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList.mp hget) with ⟨k, hkh, hmem⟩
    rw [beq_comm h k] at hkh
    have hkeq : k = h := beq_iff_eq.mp hkh
    subst k
    have hfind := find_of_mem_nodup_keys buckets.toList (h, buckets.getD h []) hmem
      (Std.HashMap.distinct_keys_toList (m := buckets))
    rw [hfind]
  · have hnone : buckets[h]? = none := Std.HashMap.getElem?_eq_none hm
    have hfindnone : buckets.toList.find? (fun p : Nat × List String => p.1 == h) = none := by
      rw [List.find?_eq_none]
      intro p hp
      by_cases hpkhT : p.1 == h
      · have hpkh : (p.1 == h) = true := hpkhT
        have hpkeq : p.1 = h := beq_iff_eq.mp hpkh
        have hgetp := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hp
        have hcontra : buckets[h]? = some p.2 := by simpa [hpkeq] using hgetp
        rw [hnone] at hcontra
        cases hcontra
      · exact hpkhT
    rw [hfindnone]
    have hzero : buckets.getD h [] = [] := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hnone]
      rfl
    rw [hzero]
    rfl

private theorem flatMap_congr {α β : Type} (xs : List α) (f g : α → List β)
    (h : ∀ x ∈ xs, f x = g x) : xs.flatMap f = xs.flatMap g := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.flatMap_cons]
      rw [h x (by simp), ih (by intro y hy; exact h y (by simp [hy]))]

theorem CollisionBucket_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights.CollisionBucket.Goal := by
  intro tags hashKey selected
  dsimp only
  let buckets := (tags.filterMap id).foldl (fun (m : Std.HashMap Nat (List String)) s =>
    m.insert (hashKey s) (s::m.getD (hashKey s) [])) ({} : Std.HashMap Nat (List String))
  let compiled := buckets.toList.foldl (fun (m : Std.HashMap Nat Bucket) (h,labels) =>
    m.insert h (classify labels)) ({} : Std.HashMap Nat Bucket)
  change selected.foldl (fun a (p : Option String × Option Int) =>
    plus a (weighted (p.1.elim 1 (fun s => bucketWeight (compiled.getD (hashKey s) .empty) s)) p.2)) zero =
    summarize (selected.flatMap (fun (k,v) =>
      List.replicate (max 1 (tags.countP (fun t => k.isSome && t == k))) v))
  rw [fold_weighted_eq_summarize]
  apply congrArg summarize
  apply flatMap_congr
  intro p hp
  cases p with
  | mk k v =>
      cases k with
      | none => simp
      | some s =>
          simp only [Option.elim_some, Option.isSome_some, Bool.true_and]
          rw [show compiled.getD (hashKey s) .empty = classify (buckets.getD (hashKey s) []) by
            dsimp [compiled]
            exact compiled_getD buckets (hashKey s)]
          rw [show buckets.getD (hashKey s) [] =
              ((tags.filterMap id).filter fun x => hashKey x == hashKey s).reverse by
            dsimp [buckets]
            rw [bucket_list_getD_fold]
            simp]
          rw [bucketWeight_classify_count, List.count_reverse, tags_count_bucket]


private theorem foldl_plus_zero {α : Type} (xs : List α) :
    xs.foldl (fun a _ => plus a zero) zero = zero := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simpa [plus_zero_right] using ih

private theorem foldl_plus_plus_aux {α : Type} (xs : List α) (f g : α → Totals) (a b : Totals) :
    xs.foldl (fun acc x => plus acc (plus (f x) (g x))) (plus a b) =
      plus (xs.foldl (fun acc x => plus acc (f x)) a) (xs.foldl (fun acc x => plus acc (g x)) b) := by
  induction xs generalizing a b with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, List.foldl_cons, List.foldl_cons]
      have hacc : plus (plus a b) (plus (f x) (g x)) = plus (plus a (f x)) (plus b (g x)) := by
        simp [plus_assoc, plus_left_comm, plus_comm]
      rw [hacc]
      exact ih (plus a (f x)) (plus b (g x))

private theorem foldl_plus_plus_zero {α : Type} (xs : List α) (f g : α → Totals) :
    xs.foldl (fun acc x => plus acc (plus (f x) (g x))) zero =
      plus (xs.foldl (fun acc x => plus acc (f x)) zero) (xs.foldl (fun acc x => plus acc (g x)) zero) := by
  simpa [plus_zero_left] using foldl_plus_plus_aux xs f g zero zero

private theorem foldl_cons_start {α : Type} (xs : List α) (g : α → Totals) (x : α) :
    (x :: xs).foldl (fun a x' => plus a (g x')) zero =
      plus (g x) (xs.foldl (fun a x' => plus a (g x')) zero) := by
  rw [List.foldl_cons, plus_zero_left, foldl_plus_start]

private theorem foldl_plus_swap {α β : Type} (xs : List α) (ys : List β) (f : α → β → Totals) :
    xs.foldl (fun acc x => plus acc (ys.foldl (fun a y => plus a (f x y)) zero)) zero =
      ys.foldl (fun acc y => plus acc (xs.foldl (fun a x => plus a (f x y)) zero)) zero := by
  induction xs with
  | nil => simp [foldl_plus_zero]
  | cons x xs ih =>
      rw [List.foldl_cons]
      rw [show plus zero (ys.foldl (fun a y => plus a (f x y)) zero) =
          ys.foldl (fun a y => plus a (f x y)) zero by rw [plus_zero_left]]
      rw [foldl_plus_start, ih]
      rw [show (fun acc y => plus acc ((x :: xs).foldl (fun a x' => plus a (f x' y)) zero)) =
          (fun acc y => plus acc (plus (f x y) (xs.foldl (fun a x' => plus a (f x' y)) zero))) by
          funext (acc : Totals) (y : β)
          congr 1
          exact foldl_cons_start xs (fun x' => f x' y) x]
      rw [foldl_plus_plus_zero]

private theorem weighted_add (a b : Nat) (v : Option Int) :
    weighted (a + b) v = plus (weighted a v) (weighted b v) := by
  cases v with
  | none => simp [weighted, plus]
  | some z => simp [weighted, plus, Int.natCast_add, Int.add_mul]

private theorem weighted_zero (v : Option Int) : weighted 0 v = zero := by
  cases v <;> simp [weighted, zero]

private theorem fold_filter_weighted_aux {α : Type} (xs : List α) (p : α → Bool)
    (n : Nat) (v : α → Option Int) (acc : Totals) :
    xs.foldl (fun a x => plus a (weighted (if p x then n else 0) (v x))) acc =
      (xs.filter p).foldl (fun a x => plus a (weighted n (v x))) acc := by
  induction xs generalizing acc with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x
      · rw [List.foldl_cons, List.filter_cons_of_pos h, if_pos h]
        exact ih (plus acc (weighted n (v x)))
      · rw [List.foldl_cons, List.filter_cons_of_neg h, if_neg h]
        rw [ih, weighted_zero, plus_zero_right]

private theorem fold_filter_weighted {α : Type} (xs : List α) (p : α → Bool)
    (n : Nat) (v : α → Option Int) :
    xs.foldl (fun acc x => plus acc (weighted (if p x then n else 0) (v x))) zero =
      (xs.filter p).foldl (fun acc x => plus acc (weighted n (v x))) zero := by
  exact fold_filter_weighted_aux xs p n v zero

private theorem summarize_flatMap_fold {α : Type} (xs : List α) (f : α → List (Option Int)) :
    summarize (xs.flatMap f) = xs.foldl (fun acc x => plus acc (summarize (f x))) zero := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [List.flatMap_cons, summarize_append, ih, List.foldl_cons]
      rw [show plus zero (summarize (f x)) = summarize (f x) by rw [plus_zero_left]]
      exact (foldl_plus_start xs (fun z => summarize (f z)) (summarize (f x))).symm

private theorem replicate_add {α : Type} (m n : Nat) (a : α) :
    List.replicate (m + n) a = List.replicate m a ++ List.replicate n a := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [Nat.succ_add, List.replicate_succ, ih, List.replicate_succ]
      simp only [List.cons_append]

private theorem replicate_sum {α : Type} (xs : List α) (f : α → Nat) (v : Option Int) :
    List.replicate ((xs.map f).sum) v = xs.flatMap (fun x => List.replicate (f x) v) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.flatMap_cons]
      rw [replicate_add, ih]

private theorem fold_weight_getD {α β : Type} [BEq β] [Hashable β] [LawfulBEq β] [LawfulHashable β]
    (entries : List α) (m0 : Std.HashMap β Nat) (u : β) (key : α → β) (w : α → Nat) :
    (entries.foldl (fun m p => m.insert (key p) (m.getD (key p) 0 + w p)) m0).getD u 0 =
      m0.getD u 0 + ((entries.filter fun p => key p == u).map w).sum := by
  induction entries generalizing m0 with
  | nil => simp
  | cons p ps ih =>
      simp only [List.foldl_cons]
      rw [ih (m0.insert (key p) (m0.getD (key p) 0 + w p))]
      by_cases hpT : key p == u
      · have hpeq : key p = u := beq_iff_eq.mp hpT
        have hget : (m0.insert (key p) (m0.getD (key p) 0 + w p)).getD u 0 = m0.getD u 0 + w p := by
          rw [Std.HashMap.getD_insert, hpT]
          simp [hpeq]
        have hpfirst : (p :: ps).filter (fun q => key q == u) = p :: (ps.filter fun q => key q == u) :=
          List.filter_cons_of_pos hpT
        rw [hget, hpfirst]
        simp only [List.map_cons, List.sum_cons]
        omega
      · have hget : (m0.insert (key p) (m0.getD (key p) 0 + w p)).getD u 0 = m0.getD u 0 := by
          rw [Std.HashMap.getD_insert]
          simp [hpT]
        have hpfirst : (p :: ps).filter (fun q => key q == u) = ps.filter (fun q => key q == u) :=
          List.filter_cons_of_neg hpT
        rw [hget, hpfirst]

private theorem sum_filter_mul_eq_map_ite {γ β : Type} (L : List (γ × Nat)) (key : γ → β)
    (p : β → Bool) (g : γ × Nat → Nat) :
    ((L.filter fun x => p (key x.1)).map fun x => x.2 * g x).sum =
      (L.map fun x => x.2 * (if p (key x.1) then g x else 0)).sum := by
  induction L with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p (key x.1)
      · simp [h, ih]
      · simp [h, ih]

private theorem sum_map_ite_eq_filter {α : Type} (L : List α) (p : α → Bool) (g : α → Nat) :
    (L.map fun x => if p x then g x else 0).sum = ((L.filter p).map g).sum := by
  induction L with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih]
      · simp [h, ih]

private theorem target_count_getD_fold (posts : List PostRec) (v : Nat) :
    (((posts.filter fun p => p.2.1).map Prod.fst).filter fun x => x == v).length =
      (posts.filter fun p => p.1 == v && p.2.1).length := by
  induction posts with
  | nil => rfl
  | cons p ps ih =>
      rcases p with ⟨id, ok, sc⟩
      by_cases hok : ok
      · by_cases hid : id == v
        · simp [hok, hid, ih]
        · simp [hok, hid, ih]
      · simp [hok, ih]

private theorem target_count_getD (posts : List PostRec) (v : Nat) :
    (histogram ((posts.filter fun p => p.2.1).map Prod.fst)).getD v 0 =
      (posts.filter fun p => p.1 == v && p.2.1).length := by
  rw [histogram_getD_count, target_count_getD_fold]

private theorem flatMap_filter_prop {α β : Type} (xs : List α) (p : α → Prop) [DecidablePred p]
    (f : α → List β) :
    xs.flatMap (fun x => if p x then f x else []) = (xs.filter p).flatMap f := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih]
      · simp [h, ih]

private theorem link_filter_sum (sel : List LinkRec) (u : Nat) (f : Nat → Nat) :
    (((sel.map fun e => (e.1, e.2.1)).filter fun p => p.1 == u).map fun p => f p.2).sum =
      ((sel.filter fun e => e.1 == u).map fun e => f e.2.1).sum := by
  induction sel with
  | nil => rfl
  | cons e es ih =>
      rcases e with ⟨a, b, ty⟩
      by_cases h : a == u
      · have heq : a = u := beq_iff_eq.mp h
        subst a
        simp [h, ih]
      · have hne : a ≠ u := by intro heq; exact h (by simp [heq])
        simp [h, hne, ih]

private theorem weight_getD (posts : List PostRec) (sel : List LinkRec) (u : Nat) :
    let targets := histogram ((posts.filter fun p => p.2.1).map Prod.fst)
    let links := sel.map fun e => (e.1, e.2.1)
    let weight := (histogram links).toList.foldl (fun (m : Std.HashMap Nat Nat) p =>
      m.insert p.1.1 (m.getD p.1.1 0 + p.2 * targets.getD p.1.2 0)) ({} : Std.HashMap Nat Nat)
    weight.getD u 0 =
      ((sel.filter fun e => e.1 == u).map fun e => targets.getD e.2.1 0).sum := by
  dsimp only
  rw [@fold_weight_getD ((Nat × Nat) × Nat) Nat _ _ _ _
    ((histogram (sel.map fun e => (e.1, e.2.1))).toList)
    ({} : Std.HashMap Nat Nat) u (fun p : (Nat × Nat) × Nat => p.1.1)
    (fun p : (Nat × Nat) × Nat => p.2 * (histogram ((posts.filter fun p => p.2.1).map Prod.fst)).getD p.1.2 0)]
  simp only [Std.HashMap.getD_empty, Nat.zero_add]
  let links := sel.map fun e => (e.1, e.2.1)
  let targets := histogram ((posts.filter fun p => p.2.1).map Prod.fst)
  change (((histogram links).toList.filter (fun p : (Nat × Nat) × Nat => p.1.1 == u)).map
      (fun p => p.2 * targets.getD p.1.2 0)).sum =
    (((sel.filter fun e => e.1 == u).map fun e => targets.getD e.2.1 0)).sum
  rw [sum_filter_mul_eq_map_ite _ (fun p : Nat × Nat => p.1) (fun x => x == u)
    (fun p : (Nat × Nat) × Nat => targets.getD p.1.2 0)]
  rw [histogram_sum links (fun p : Nat × Nat => if p.1 == u then targets.getD p.2 0 else 0)]
  rw [sum_map_ite_eq_filter]
  exact link_filter_sum sel u (fun v => targets.getD v 0)

private theorem flatMap_filter {α β : Type} (xs : List α) (p : α → Bool) (f : α → List β) :
    xs.flatMap (fun x => if p x then f x else []) = (xs.filter p).flatMap f := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih]
      · simp [h, ih]

private theorem bool_eq_not_ne (a b : Nat) : (a == b) = (!(b != a)) := by
  by_cases h : a == b
  · have heq : a = b := beq_iff_eq.mp h
    subst b
    simp
  · have hbne : b ≠ a := by
      intro hb
      have hba : a = b := hb.symm
      exact h (by simp [hba])
    simp [h, hbne]

private theorem filter_decide_eq_beq (xs : List LinkRec) (t : Nat) :
    xs.filter (fun e => decide (e.2.2 = t)) = xs.filter (fun e => e.2.2 == t) := by
  induction xs with
  | nil => rfl
  | cons e es ih =>
      by_cases h : e.2.2 == t
      · have heq : e.2.2 = t := beq_iff_eq.mp h
        simp [h, heq, ih]
      · have hne : e.2.2 ≠ t := by intro heq; exact h (by simp [heq])
        simp [h, hne, ih]

private theorem refBridgeAgg_eq_filter (posts : List PostRec) (es : List LinkRec) (t : Nat) :
    refBridgeAgg posts es t =
      summarize ((es.filter fun e => e.2.2 == t).flatMap fun (u, v, _) =>
        (posts.filter fun a => a.1 == u).flatMap fun a =>
          (posts.filter fun p => p.1 == v && p.2.1).map fun _ => a.2.2) := by
  induction es with
  | nil => rfl
  | cons e es ih =>
      rcases e with ⟨u, v, ty⟩
      by_cases h : ty == t
      · have heq : ty = t := beq_iff_eq.mp h
        subst ty
        simp [refBridgeAgg, ih, summarize_append, flatMap_filter, flatMap_filter_prop, filter_decide_eq_beq]
      · have hneq : (t != ty) = true := by
          have hb := bool_eq_not_ne ty t
          have hF : (ty == t) = false := Bool.eq_false_of_ne_true h
          rw [hF] at hb
          simpa using hb.symm
        have htne : ty ≠ t := by intro heq; exact h (by simp [heq])
        simp [refBridgeAgg, h, hneq, htne, ih, summarize_append, flatMap_filter, flatMap_filter_prop, filter_decide_eq_beq]

private theorem foldl_congr {α β : Type} (xs : List α) (f g : β → α → β) (b : β)
    (h : ∀ b' x, f b' x = g b' x) : xs.foldl f b = xs.foldl g b := by
  induction xs generalizing b with
  | nil => rfl
  | cons x xs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      calc
        xs.foldl f (f b x) = xs.foldl g (f b x) := ih (f b x)
        _ = xs.foldl g (g b x) := by rw [h b x]

private theorem weighted_sum_eq_fold {α : Type} (xs : List α) (p : α → Bool)
    (n : α → Nat) (v : Option Int) :
    weighted (((xs.filter p).map n).sum) v =
      xs.foldl (fun acc x => plus acc (weighted (if p x then n x else 0) v)) zero := by
  induction xs with
  | nil => simp [weighted_zero]
  | cons x xs ih =>
      by_cases h : p x
      · rw [List.filter_cons_of_pos h, List.map_cons, List.sum_cons, List.foldl_cons]
        rw [if_pos h]
        rw [weighted_add, ih]
        rw [show plus zero (weighted (n x) v) = weighted (n x) v by rw [plus_zero_left]]
        exact (foldl_plus_start xs (fun z => weighted (if p z then n z else 0) v)
          (weighted (n x) v)).symm
      · rw [List.filter_cons_of_neg h, List.foldl_cons, if_neg h]
        rw [weighted_zero, plus_zero_left]
        exact ih

private theorem fold_filter_weighted_comm (xs : List PostRec) (u : Nat) (n : Nat) :
    xs.foldl (fun acc a => plus acc (weighted (if u == a.1 then n else 0) a.2.2)) zero =
      (xs.filter fun a => a.1 == u).foldl (fun acc a => plus acc (weighted n a.2.2)) zero := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : u == x.1
      · have hx : (x.1 == u) = true := by rw [beq_comm]; exact h
        have hfilt : (x :: xs).filter (fun a => a.1 == u) = x :: xs.filter (fun a => a.1 == u) :=
          List.filter_cons_of_pos hx
        rw [List.foldl_cons, hfilt, if_pos h]
        rw [show plus zero (weighted n x.2.2) = weighted n x.2.2 by rw [plus_zero_left]]
        rw [List.foldl_cons]
        rw [show plus zero (weighted n x.2.2) = weighted n x.2.2 by rw [plus_zero_left]]
        rw [foldl_plus_start, ih]
        exact (foldl_plus_start (xs.filter (fun a : PostRec => a.1 == u))
          (fun a : PostRec => weighted n a.2.2) (weighted n x.2.2)).symm
      · have hxneg : ¬ ((x.1 == u) = true) := by
          intro hxtrue
          have hxtrue' : (x.1 == u) = true := hxtrue
          rw [beq_comm] at hxtrue'
          exact h hxtrue'
        have hfilt : (x :: xs).filter (fun a => a.1 == u) = xs.filter (fun a => a.1 == u) :=
          List.filter_cons_of_neg hxneg
        rw [List.foldl_cons, hfilt, if_neg h, weighted_zero, plus_zero_left]
        exact ih

private theorem map_const_replicate {α β : Type} (xs : List α) (y : β) :
    xs.map (fun _ => y) = List.replicate xs.length y := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.replicate_succ, ih]

theorem SourceWeights_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights.SourceWeights.Goal := by
  intro posts es t
  dsimp only
  let sel := es.filter fun e => e.2.2 == t
  let targets := histogram ((posts.filter fun p => p.2.1).map Prod.fst)
  let edges := histogram (sel.map fun e => (e.1, e.2.1))
  let weight := edges.toList.foldl (fun (m : Std.HashMap Nat Nat) p =>
    m.insert p.1.1 (m.getD p.1.1 0 + p.2 * targets.getD p.1.2 0)) ({} : Std.HashMap Nat Nat)
  change posts.foldl (fun a (u, _, score) => plus a (weighted (weight.getD u 0) score)) zero =
    refBridgeAgg posts es t
  rw [refBridgeAgg_eq_filter]
  have hw : ∀ u : Nat, weight.getD u 0 =
      ((sel.filter fun e => e.1 == u).map fun e => targets.getD e.2.1 0).sum := by
    intro u
    exact weight_getD posts sel u
  have htgt : ∀ e : LinkRec, targets.getD e.2.1 0 =
      (posts.filter fun p => p.1 == e.2.1 && p.2.1).length := by
    intro e
    exact target_count_getD posts e.2.1
  rw [show (es.filter fun e => e.2.2 == t) = sel by rfl]
  simp only [htgt]
  calc
    posts.foldl (fun a (u, _, score) => plus a (weighted (weight.getD u 0) score)) zero
        = posts.foldl (fun acc a => plus acc (sel.foldl (fun acc' e => plus acc'
            (weighted (if e.1 == a.1 then
              (posts.filter fun p => p.1 == e.2.1 && p.2.1).length else 0) a.2.2)) zero)) zero := by
          apply foldl_congr
          intro acc a
          rcases a with ⟨u, ok, score⟩
          simp only
          rw [hw u, weighted_sum_eq_fold]
          simp only [htgt]
    _ = sel.foldl (fun acc e => plus acc (posts.foldl (fun acc' a => plus acc'
          (weighted (if e.1 == a.1 then
            (posts.filter fun p => p.1 == e.2.1 && p.2.1).length else 0) a.2.2)) zero)) zero := by
          rw [foldl_plus_swap]
    _ = sel.foldl (fun acc e => plus acc ((posts.filter fun a => a.1 == e.1).foldl
          (fun acc' a => plus acc' (weighted
            (posts.filter fun p => p.1 == e.2.1 && p.2.1).length a.2.2)) zero)) zero := by
          apply foldl_congr
          intro acc e
          rcases e with ⟨u, v, ty⟩
          exact congrArg (fun t => plus acc t) (fold_filter_weighted_comm posts u
            (posts.filter fun p => p.1 == v && p.2.1).length)
    _ = sel.foldl (fun acc e => plus acc (summarize
          ((posts.filter fun a => a.1 == e.1).flatMap fun a => List.replicate
            (posts.filter fun p => p.1 == e.2.1 && p.2.1).length a.2.2))) zero := by
          apply foldl_congr
          intro acc e
          rw [fold_weighted_eq_summarize]
    _ = summarize (sel.flatMap fun e =>
          (posts.filter fun a => a.1 == e.1).flatMap fun a =>
            List.replicate (posts.filter fun p => p.1 == e.2.1 && p.2.1).length a.2.2) := by
          rw [summarize_flatMap_fold]
    _ = summarize (sel.flatMap fun (u, v, _) =>
          (posts.filter fun a => a.1 == u).flatMap fun a =>
            (posts.filter fun p => p.1 == v && p.2.1).map fun _ => a.2.2) := by
          apply congrArg summarize
          apply flatMap_congr
          intro e he
          rcases e with ⟨u, v, ty⟩
          simp only
          congr 1
          funext a
          rw [map_const_replicate]

/-! ### C3: EndpointDemand -/

private theorem src_filter_eq (edges : List LinkRec) (posts : List PostRec) (u : Nat)
    (hu : u ∈ (edges.map Prod.fst).eraseDups) :
    (posts.filter fun p => ((edges.map Prod.fst).eraseDups).contains p.1).filter
        (fun p => p.1 == u) =
      posts.filter (fun p => p.1 == u) := by
  let S : List Nat := (edges.map Prod.fst).eraseDups
  change (posts.filter fun p : PostRec => S.contains p.1).filter (fun p => p.1 == u) =
    posts.filter (fun p => p.1 == u)
  induction posts with
  | nil => rfl
  | cons p ps ih =>
      by_cases h : p.1 == u
      · have hpeq : p.1 = u := beq_iff_eq.mp h
        have hc : S.contains p.1 = true := by
          rw [List.contains_iff_mem]
          simpa [S, hpeq] using hu
        have hfS : (p :: ps).filter (fun q : PostRec => S.contains q.1) =
            p :: (ps.filter fun q : PostRec => S.contains q.1) := List.filter_cons_of_pos hc
        have hf1 : (p :: (ps.filter fun q : PostRec => S.contains q.1)).filter
            (fun q : PostRec => q.1 == u) =
            p :: ((ps.filter fun q : PostRec => S.contains q.1).filter fun q : PostRec => q.1 == u) :=
          List.filter_cons_of_pos h
        have hf2 : (p :: ps).filter (fun q : PostRec => q.1 == u) =
            p :: (ps.filter fun q : PostRec => q.1 == u) := List.filter_cons_of_pos h
        rw [hfS, hf1, hf2, ih]
      · by_cases hcT : S.contains p.1 = true
        · have hfS : (p :: ps).filter (fun q : PostRec => S.contains q.1) =
              p :: (ps.filter fun q : PostRec => S.contains q.1) := List.filter_cons_of_pos hcT
          have hf1 : (p :: (ps.filter fun q : PostRec => S.contains q.1)).filter
              (fun q : PostRec => q.1 == u) =
              ((ps.filter fun q : PostRec => S.contains q.1).filter fun q : PostRec => q.1 == u) :=
            List.filter_cons_of_neg h
          have hf2 : (p :: ps).filter (fun q : PostRec => q.1 == u) =
              ps.filter fun q : PostRec => q.1 == u := List.filter_cons_of_neg h
          rw [hfS, hf1, hf2, ih]
        · have hcF : S.contains p.1 = false := Bool.eq_false_of_ne_true hcT
          have hfS : (p :: ps).filter (fun q : PostRec => S.contains q.1) =
              ps.filter fun q : PostRec => S.contains q.1 := List.filter_cons_of_neg hcT
          have hf2 : (p :: ps).filter (fun q : PostRec => q.1 == u) =
              ps.filter fun q : PostRec => q.1 == u := List.filter_cons_of_neg h
          rw [hfS, hf2, ih]

private theorem dst_count_eq (edges : List LinkRec) (posts : List PostRec) (v : Nat)
    (hv : v ∈ (edges.map fun e => e.2.1).eraseDups) :
    (posts.filter fun p => p.2.1 && ((edges.map fun e => e.2.1).eraseDups).contains p.1).countP
        (fun p => p.1 == v) =
      (posts.filter fun p => p.1 == v && p.2.1).length := by
  let S : List Nat := (edges.map fun e => e.2.1).eraseDups
  change (posts.filter fun p : PostRec => p.2.1 && S.contains p.1).countP (fun p => p.1 == v) =
    (posts.filter fun p : PostRec => p.1 == v && p.2.1).length
  induction posts with
  | nil => rfl
  | cons p ps ih =>
      rcases p with ⟨id, ok, sc⟩
      by_cases h : id == v
      · have hieq : id = v := beq_iff_eq.mp h
        have hc : S.contains id = true := by
          rw [List.contains_iff_mem]
          simpa [S, hieq] using hv
        by_cases hok : ok
        · have hf : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.2.1 && S.contains q.1) =
              (id, ok, sc) :: ps.filter (fun q : PostRec => q.2.1 && S.contains q.1) :=
            List.filter_cons_of_pos (by
              have hmem : id ∈ S := List.contains_iff_mem.mp hc
              simp [hok, hmem])
          have hf2 : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.1 == v && q.2.1) =
              (id, ok, sc) :: ps.filter (fun q : PostRec => q.1 == v && q.2.1) :=
            List.filter_cons_of_pos (by simp [h, hok])
          rw [hf, hf2, List.countP_cons, List.length_cons, if_pos h, ih]
        · have hfD : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.2.1 && S.contains q.1) =
              ps.filter (fun q : PostRec => q.2.1 && S.contains q.1) :=
            List.filter_cons_of_neg (by simp [hok])
          have hf2 : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.1 == v && q.2.1) =
              ps.filter (fun q : PostRec => q.1 == v && q.2.1) :=
            List.filter_cons_of_neg (by simp [hok])
          rw [hfD, hf2, ih]
      · by_cases hcT : S.contains id = true
        · by_cases hok : ok
          · have hf : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.2.1 && S.contains q.1) =
                (id, ok, sc) :: ps.filter (fun q : PostRec => q.2.1 && S.contains q.1) :=
              List.filter_cons_of_pos (by
                have hmem : id ∈ S := List.contains_iff_mem.mp hcT
                simp [hok, hmem])
            have hf2 : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.1 == v && q.2.1) =
                ps.filter (fun q : PostRec => q.1 == v && q.2.1) :=
              List.filter_cons_of_neg (by simp [h])
            rw [hf, hf2, List.countP_cons, if_neg h, ih]
            omega
          · have hfD : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.2.1 && S.contains q.1) =
                ps.filter (fun q : PostRec => q.2.1 && S.contains q.1) :=
              List.filter_cons_of_neg (by simp [hok])
            have hf2 : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.1 == v && q.2.1) =
                ps.filter (fun q : PostRec => q.1 == v && q.2.1) :=
              List.filter_cons_of_neg (by simp [h])
            rw [hfD, hf2, ih]
        · have hcF : S.contains id = false := Bool.eq_false_of_ne_true hcT
          have hfD : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.2.1 && S.contains q.1) =
              ps.filter (fun q : PostRec => q.2.1 && S.contains q.1) :=
            List.filter_cons_of_neg (by
              have hnot : ¬ id ∈ S := by
                intro hm
                have hcT' : S.contains id = true := by
                  rw [List.contains_eq_mem]
                  exact decide_eq_true hm
                exact hcT hcT' 
              simp [hnot])
          have hf2 : ((id, ok, sc) :: ps).filter (fun q : PostRec => q.1 == v && q.2.1) =
              ps.filter (fun q : PostRec => q.1 == v && q.2.1) :=
            List.filter_cons_of_neg (by simp [h])
          rw [hfD, hf2, ih]

theorem EndpointDemand_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights.EndpointDemand.Goal := by
  intro posts es t
  dsimp only
  let edges := es.filter fun e => e.2.2 == t
  let sources := (edges.map Prod.fst).eraseDups
  let targets := (edges.map fun e => e.2.1).eraseDups
  let src := posts.filter fun p => sources.contains p.1
  let dst := posts.filter fun p => p.2.1 && targets.contains p.1
  change summarize (edges.flatMap fun (u, v, _) =>
    (src.filter (fun p => p.1 == u)).flatMap fun p =>
      List.replicate (dst.countP (fun p => p.1 == v)) p.2.2) = refBridgeAgg posts es t
  rw [refBridgeAgg_eq_filter]
  rw [show (es.filter fun e => e.2.2 == t) = edges by rfl]
  apply congrArg summarize
  apply flatMap_congr
  intro e he
  rcases e with ⟨u, v, ty⟩
  have hu : u ∈ (edges.map Prod.fst).eraseDups := by
    rw [List.mem_eraseDups]
    exact List.mem_map.mpr ⟨(u, v, ty), he, rfl⟩
  have hv : v ∈ (edges.map fun e => e.2.1).eraseDups := by
    rw [List.mem_eraseDups]
    exact List.mem_map.mpr ⟨(u, v, ty), he, rfl⟩
  simp only
  rw [show src = posts.filter fun p => ((edges.map Prod.fst).eraseDups).contains p.1 by rfl]
  rw [show dst = posts.filter fun p => p.2.1 && ((edges.map fun e => e.2.1).eraseDups).contains p.1 by rfl]
  rw [src_filter_eq edges posts u hu]
  rw [dst_count_eq edges posts v hv]
  apply flatMap_congr
  intro a ha
  rw [map_const_replicate]

/-! ### C2: CommentQuotient -/

private theorem repCnt_getD (users : List UserRow) (u : Nat) :
    (histogram ((users.filter UserRow.repOk).map UserRow.id)).getD u 0 =
      (users.filter fun x => x.id == u && x.repOk).length := by
  rw [histogram_getD_count]
  induction users with
  | nil => rfl
  | cons x xs ih =>
      by_cases hrep : x.repOk
      · by_cases hid : x.id == u
        · simp [hrep, hid, ih]
        · simp [hrep, hid, ih]
      · simp [hrep, ih]

private theorem mass_getD (users : List UserRow) (comments : List (Nat × Nat)) (p : Nat) :
    let commenters := histogram ((users.filter UserRow.repOk).map UserRow.id)
    let mass := (histogram comments).toList.foldl (fun (m : Std.HashMap Nat Nat) (e : (Nat × Nat) × Nat) =>
      m.insert e.1.1 (m.getD e.1.1 0 + e.2 * commenters.getD e.1.2 0)) ({} : Std.HashMap Nat Nat)
    mass.getD p 0 =
      ((comments.filter fun c => c.1 == p).map fun c =>
        (histogram ((users.filter UserRow.repOk).map UserRow.id)).getD c.2 0).sum := by
  dsimp only
  rw [@fold_weight_getD ((Nat × Nat) × Nat) Nat _ _ _ _ ((histogram comments).toList)
    ({} : Std.HashMap Nat Nat) p (fun e : (Nat × Nat) × Nat => e.1.1)
    (fun e : (Nat × Nat) × Nat => e.2 *
      (histogram ((users.filter UserRow.repOk).map UserRow.id)).getD e.1.2 0)]
  simp only [Std.HashMap.getD_empty, Nat.zero_add]
  rw [sum_filter_mul_eq_map_ite _ (fun c : Nat × Nat => c.1) (fun x => x == p)
    (fun e : (Nat × Nat) × Nat =>
      (histogram ((users.filter UserRow.repOk).map UserRow.id)).getD e.1.2 0)]
  rw [histogram_sum comments (fun c : Nat × Nat =>
    if c.1 == p then (histogram ((users.filter UserRow.repOk).map UserRow.id)).getD c.2 0 else 0)]
  rw [sum_map_ite_eq_filter]

private theorem owner_filter_same (users : List UserRow) (owner : Nat) :
 (users.filter fun u => u.id == owner && u.yrOk && u.id == owner).length =
 users.countP (fun u => u.yrOk && u.id == owner) := by
  induction users with
  | nil => rfl
  | cons u us ih =>
      by_cases h : u.id == owner && u.yrOk && u.id == owner
      · have hf : (u :: us).filter (fun x => x.id == owner && x.yrOk && x.id == owner) =
            u :: (us.filter fun x => x.id == owner && x.yrOk && x.id == owner) := List.filter_cons_of_pos h
        rw [hf, List.length_cons, ih, List.countP_cons]
        have h1 := Bool.and_eq_true_iff.mp h
        have h3 := Bool.and_eq_true_iff.mp h1.1
        have h2 : ((u.yrOk && u.id == owner) = true) := Bool.and_eq_true_iff.mpr ⟨h3.2, h3.1⟩
        simp [h2]
      · have hf : (u :: us).filter (fun x => x.id == owner && x.yrOk && x.id == owner) =
            us.filter fun x => x.id == owner && x.yrOk && x.id == owner := List.filter_cons_of_neg h
        rw [hf, ih, List.countP_cons]
        have h2 : ¬ ((u.yrOk && u.id == owner) = true) := by
          intro hb
          have hb1 := Bool.and_eq_true_iff.mp hb
          apply h
          exact Bool.and_eq_true_iff.mpr ⟨Bool.and_eq_true_iff.mpr ⟨hb1.2, hb1.1⟩, hb1.2⟩
        simp [h2]

private theorem owner_filter_count (users : List UserRow) (oid owner : Nat) :
    (users.filter fun u => u.id == oid && u.yrOk && u.id == owner).length =
      if oid == owner then users.countP (fun u => u.yrOk && u.id == owner) else 0 := by
  by_cases hoid : oid == owner
  · have ho : oid = owner := beq_iff_eq.mp hoid
    subst oid
    rw [if_pos (by simp)]
    exact owner_filter_same users owner
  · rw [if_neg hoid]
    induction users with
    | nil => rfl
    | cons u us ih =>
        have hf : (u :: us).filter (fun x => x.id == oid && x.yrOk && x.id == owner) =
            us.filter fun x => x.id == oid && x.yrOk && x.id == owner := by
          apply List.filter_cons_of_neg
          intro ht
          have h1 := Bool.and_eq_true_iff.mp ht
          have hidoid : u.id = oid := beq_iff_eq.mp (Bool.and_eq_true_iff.mp h1.1).1
          have hidowner : u.id = owner := beq_iff_eq.mp h1.2
          have eo : oid = owner := hidoid.symm.trans hidowner
          exact hoid (by simp [eo])
        rw [hf, ih]
private theorem length_flatMap_const {α β : Type} (xs : List α) (ys : List β) :
    (xs.flatMap fun _ => ys).length = xs.length * ys.length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.succ_mul]
      omega

private theorem comment_contrib (users : List UserRow) (posts : List (Nat × Nat))
    (pid uid owner : Nat) :
    ((posts.filter fun p => p.1 == pid).flatMap fun p =>
      (users.filter fun u => u.id == p.2 && u.yrOk && u.id == owner).flatMap fun _ =>
        users.filter fun u2 => u2.id == uid && u2.repOk).length =
      (posts.filter fun p => p.1 == pid && p.2 == owner).length *
        users.countP (fun u => u.yrOk && u.id == owner) *
        (users.filter fun u2 => u2.id == uid && u2.repOk).length := by
  induction posts with
  | nil => simp
  | cons p ps ih =>
      rcases p with ⟨id, oid⟩
      by_cases hid : id == pid
      · have hieq : id = pid := beq_iff_eq.mp hid
        subst id
        have hf : ((pid, oid) :: ps).filter (fun q : Nat × Nat => q.1 == pid) =
            (pid, oid) :: (ps.filter fun q : Nat × Nat => q.1 == pid) :=
          List.filter_cons_of_pos (by simp)
        by_cases ho : oid == owner
        · have hoeq : oid = owner := beq_iff_eq.mp ho
          subst oid
          have hf2 : ((pid, owner) :: ps).filter (fun q : Nat × Nat => q.1 == pid && q.2 == owner) =
              (pid, owner) :: (ps.filter fun q : Nat × Nat => q.1 == pid && q.2 == owner) :=
            List.filter_cons_of_pos (by simp)
          rw [hf, List.flatMap_cons, List.length_append, ih, hf2, List.length_cons]
          simp only [Prod.snd, Prod.fst]
          rw [length_flatMap_const]
          rw [owner_filter_same]
          rw [Nat.succ_mul, Nat.add_mul]
          ac_rfl
        · have hf2 : ((pid, oid) :: ps).filter (fun q : Nat × Nat => q.1 == pid && q.2 == owner) =
              ps.filter fun q : Nat × Nat => q.1 == pid && q.2 == owner := by
            apply List.filter_cons_of_neg
            intro ht
            exact ho (Bool.and_eq_true_iff.mp ht).2
          have hnil : (users.filter fun u => u.id == oid && u.yrOk && u.id == owner) = [] := by
            apply filter_eq_nil_of_all_false
            intro u hu0
            apply Bool.eq_false_of_ne_true
            intro ht
            have h1 := Bool.and_eq_true_iff.mp ht
            have hi1 : u.id = oid := beq_iff_eq.mp (Bool.and_eq_true_iff.mp h1.1).1
            have hi2 : u.id = owner := beq_iff_eq.mp h1.2
            exact ho (by simpa [hi1.symm.trans hi2])
          rw [hf, List.flatMap_cons, List.length_append, ih, hf2]
          simp [hnil]
      · have hf : ((id, oid) :: ps).filter (fun q : Nat × Nat => q.1 == pid) =
            ps.filter fun q : Nat × Nat => q.1 == pid :=
          List.filter_cons_of_neg hid
        rw [hf, ih]
        by_cases ho : oid == owner
        · have hf2 : ((id, oid) :: ps).filter (fun q : Nat × Nat => q.1 == pid && q.2 == owner) =
              ps.filter fun q : Nat × Nat => q.1 == pid && q.2 == owner := by
            apply List.filter_cons_of_neg
            intro ht
            exact hid (Bool.and_eq_true_iff.mp ht).1
          rw [hf2]
        · have hf2 : ((id, oid) :: ps).filter (fun q : Nat × Nat => q.1 == pid && q.2 == owner) =
              ps.filter fun q : Nat × Nat => q.1 == pid && q.2 == owner := by
            apply List.filter_cons_of_neg
            intro ht
            exact ho (Bool.and_eq_true_iff.mp ht).2
          rw [hf2]

private theorem length_flatMap {α β : Type} (xs : List α) (f : α → List β) :
    (xs.flatMap f).length = (xs.map fun x => (f x).length).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem refDualUserCount_eq_sum (users : List UserRow) (posts : List (Nat × Nat))
    (comments : List (Nat × Nat)) (owner : Nat) :
    refDualUserCount users posts comments owner =
      (comments.map fun (pid, uid) =>
        (posts.filter fun p => p.1 == pid && p.2 == owner).length *
          users.countP (fun u => u.yrOk && u.id == owner) *
          (users.filter fun u2 => u2.id == uid && u2.repOk).length).sum := by
  unfold refDualUserCount
  rw [length_flatMap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro c hc
  rcases c with ⟨pid, uid⟩
  exact comment_contrib users posts pid uid owner

private theorem sum_indicator_count {α : Type} (xs : List α) (p : α → Bool)
    (c : Nat) :
    (xs.map fun x => if p x then c else 0).sum =
      c * (xs.filter p).length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x
      · simp [h, ih, Nat.mul_succ, Nat.add_comm]
      · simp [h, ih]

private theorem filter_filter_length {α : Type} [BEq α] [LawfulBEq α]
    (xs : List α) (p q : α → Bool) :
    ((xs.filter p).filter q).length = (xs.filter fun x => q x && p x).length := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases hp : p x
      · by_cases hq : q x
        · simp [hp, hq, ih]
        · simp [hp, hq, ih]
      · simp [hp, ih]

private theorem mul_sum {α : Type} (c : Nat) (xs : List α) (f : α → Nat) :
    c * (xs.map f).sum = (xs.map fun x => c * f x).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [Nat.mul_add, ih]

private theorem sum_posts_comments (posts comments : List (Nat × Nat)) (owner : Nat)
    (f : Nat → Nat) :
    ((posts.filter fun p => p.2 == owner).map fun p =>
      ((comments.filter fun c => c.1 == p.1).map fun c => f c.2).sum).sum =
      (comments.map fun c => f c.2 *
        (posts.filter fun p => p.1 == c.1 && p.2 == owner).length).sum := by
  calc
    ((posts.filter fun p => p.2 == owner).map fun p =>
      ((comments.filter fun c => c.1 == p.1).map fun c => f c.2).sum).sum =
        ((posts.filter fun p => p.2 == owner).map fun p =>
          (comments.map fun c => if c.1 == p.1 then f c.2 else 0).sum).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro p hp
          rw [sum_map_ite_eq_filter]
    _ = (comments.map fun c =>
          ((posts.filter fun p => p.2 == owner).map fun p =>
            if c.1 == p.1 then f c.2 else 0).sum).sum := by
          rw [sum_swap]
    _ = (comments.map fun c => f c.2 *
          (posts.filter fun p => p.1 == c.1 && p.2 == owner).length).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro c hc
          rw [sum_indicator_count]
          congr 1
          have hlen : ((posts.filter fun p : Nat × Nat => p.2 == owner).filter fun p => c.1 == p.1).length =
              (posts.filter fun p : Nat × Nat => p.1 == c.1 && p.2 == owner).length := by
            induction posts with
            | nil => rfl
            | cons q qs ihq =>
                by_cases hq1 : q.1 == c.1
                · by_cases hq2 : q.2 == owner
                  · simp [hq1, hq2, ihq, beq_comm]
                  · simp [hq2, ihq, beq_comm]
                · simp [hq1, ihq, beq_comm]
          rw [hlen]

theorem CommentQuotient_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights.CommentQuotient.Goal := by
  intro users posts comments owner
  dsimp only
  let commenters := histogram ((users.filter UserRow.repOk).map UserRow.id)
  let owners := users.countP (fun u => u.yrOk && u.id == owner)
  let mass := (histogram comments).toList.foldl (fun (m : Std.HashMap Nat Nat) ((p, u), w) =>
    m.insert p (m.getD p 0 + w * commenters.getD u 0)) ({} : Std.HashMap Nat Nat)
  change owners * ((posts.filter (·.2 == owner)).map (fun p => mass.getD p.1 0)).sum =
    refDualUserCount users posts comments owner
  rw [refDualUserCount_eq_sum]
  have hmass : ∀ p : Nat, mass.getD p 0 =
      ((comments.filter fun c => c.1 == p).map fun c =>
        (users.filter fun u2 => u2.id == c.2 && u2.repOk).length).sum := by
    intro p
    rw [show mass = (histogram comments).toList.foldl (fun (m : Std.HashMap Nat Nat) (e : (Nat × Nat) × Nat) =>
        m.insert e.1.1 (m.getD e.1.1 0 + e.2 * commenters.getD e.1.2 0)) ({} : Std.HashMap Nat Nat) by rfl]
    rw [mass_getD]
    apply congrArg List.sum
    apply List.map_congr_left
    intro c hc
    rw [repCnt_getD]
  simp only [hmass]
  rw [sum_posts_comments posts comments owner
    (fun u => (users.filter fun u2 => u2.id == u && u2.repOk).length)]
  rw [mul_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro c hc
  rcases c with ⟨pid, uid⟩
  simp [owners, Bool.and_comm, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm]



/-! ### H5 infrastructure (adapted from AnalyticsWorkloads.HotKeyAndEpochAggregates) for the two bridge goals -/

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
private theorem summarize_singleton (v : Option Int) : summarize [v] = weighted 1 v := by
  simp [summarize, zero, weighted, plus]
private theorem summarize_char (vs : List (Option Int)) :
    summarize vs =
      ⟨vs.length, (vs.filterMap id).length, (vs.map fun v => v.getD 0).sum⟩ := by
  induction vs with
  | nil => simp [summarize, zero]
  | cons v vs ih =>
      rw [summarize_cons, ih]
      cases v <;> simp [weighted, plus] <;> omega
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



/-! ### RightStateClasses: regrouping a HashMap of per-signature Totals -/

private def sigOf (r : Totals) : Nat × Nat × Int := (r.cnt, r.nn, r.sum)
private def totalsOfSig (s : Nat × Nat × Int) : Totals := ⟨s.1, s.2.1, s.2.2⟩
private theorem totalsOfSig_sigOf (r : Totals) : totalsOfSig (sigOf r) = r := by
  cases r
  rfl

private theorem eq_getD_of_getElem?_eq_some {β : Type} [BEq β] [Hashable β]
    [LawfulBEq β] [LawfulHashable β] (m : Std.HashMap β Totals) (k : β) (v : Totals)
    (h : m[k]? = some v) : v = m.getD k zero := by
  rw [Std.HashMap.getD_eq_getD_getElem?, h]
  simp

private theorem valid_getD_gen {β : Type} [BEq β] [Hashable β] [LawfulBEq β] [LawfulHashable β]
    (m : Std.HashMap β Totals) (k : β) (hv : ∀ e ∈ m.toList, Valid e.2) :
    Valid (m.getD k zero) := by
  by_cases hk : k ∈ m
  · have hs : m[k]? = some (m.getD k zero) := Std.HashMap.getElem?_eq_some_getD hk
    have hm : (k, m.getD k zero) ∈ m.toList := Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hs
    exact hv (k, m.getD k zero) hm
  · rw [Std.HashMap.getD_eq_getD_getElem?, Std.HashMap.getElem?_eq_none hk]
    simp
    exact valid_zero

private theorem valid_getD_insert {β : Type} [BEq β] [Hashable β] [LawfulBEq β] [LawfulHashable β]
    (m : Std.HashMap β Totals) (k : β) (w : Totals) (hw : Valid w)
    (hv : ∀ z, Valid (m.getD z zero)) : ∀ z, Valid ((m.insert k w).getD z zero) := by
  intro z
  rw [Std.HashMap.getD_insert]
  by_cases h : k == z
  · rw [if_pos h]
    exact hw
  · rw [if_neg h]
    exact hv z

private theorem build_valid_inv : ∀ (xs : List RRow) (h : Std.HashMap Int Totals),
    (∀ k, Valid (h.getD k zero)) →
    ∀ k, Valid ((xs.foldl (fun (m : Std.HashMap Int Totals) r => match r.k with
      | none => m
      | some k => m.insert k (plus (m.getD k zero) (weighted 1 r.v))) h).getD k zero) := by
  intro xs
  induction xs with
  | nil => intro h hv k; exact hv k
  | cons r xs ih =>
      intro h hv k
      cases hr : r.k with
      | none => simpa [hr] using ih h hv k
      | some rk =>
          let h' := h.insert rk (plus (h.getD rk zero) (weighted 1 r.v))
          have hv' : ∀ q, Valid (h'.getD q zero) := by
            intro q
            dsimp [h']
            exact valid_getD_insert h rk (plus (h.getD rk zero) (weighted 1 r.v))
              (valid_plus (hv rk) (valid_weighted 1 r.v)) hv q
          simpa [hr] using ih h' hv' k

private theorem build_valid_getD (rs : List RRow) (k : Int) : Valid ((buildOf rs).getD k zero) := by
  unfold buildOf
  exact build_valid_inv rs {} (by intro k; simp; exact valid_zero) k

private theorem build_mem_inv : ∀ (xs : List RRow) (h : Std.HashMap Int Totals) (k : Int),
    k ∈ (xs.foldl (fun (m : Std.HashMap Int Totals) r => match r.k with
      | none => m
      | some k => m.insert k (plus (m.getD k zero) (weighted 1 r.v))) h) ↔
      k ∈ h ∨ ∃ r ∈ xs, r.k = some k := by
  intro xs
  induction xs with
  | nil => intro h k; simp
  | cons r xs ih =>
      intro h k
      cases hr : r.k with
      | none => simpa [hr] using ih h k
      | some rk =>
          let h' := h.insert rk (plus (h.getD rk zero) (weighted 1 r.v))
          rw [show (r :: xs).foldl (fun (m : Std.HashMap Int Totals) r => match r.k with
            | none => m | some k => m.insert k (plus (m.getD k zero) (weighted 1 r.v))) h =
            xs.foldl (fun (m : Std.HashMap Int Totals) r => match r.k with
            | none => m | some k => m.insert k (plus (m.getD k zero) (weighted 1 r.v))) h' by
              simp [hr, h']]
          rw [ih h' k]
          constructor
          · intro hk
            rcases hk with hk | ⟨a, ha, hak⟩
            · rw [Std.HashMap.mem_insert] at hk
              rcases hk with heq | hk
              · have heq' : rk = k := beq_iff_eq.mp heq
                subst k
                exact Or.inr ⟨r, by simp, by simpa [hr]⟩
              · exact Or.inl hk
            · exact Or.inr ⟨a, by simp [ha], hak⟩
          · intro hk
            rcases hk with hk | ⟨a, ha, hak⟩
            · left
              rw [Std.HashMap.mem_insert]
              exact Or.inr hk
            · rcases List.mem_cons.mp ha with rfl | ha
              · have hEq : rk = k := by simpa [hr] using hak
                left
                rw [Std.HashMap.mem_insert]
                exact Or.inl (by simp [hEq])
              · exact Or.inr ⟨a, ha, hak⟩

private theorem mem_buildOf_iff {rs : List RRow} {k : Int} :
    k ∈ buildOf rs ↔ ∃ r ∈ rs, r.k = some k := by
  dsimp [buildOf]
  rw [build_mem_inv]
  simp

private theorem cnt_pos_of_mem_buildOf (rs : List RRow) (k : Int) (h : k ∈ buildOf rs) :
    0 < (rTotals rs k).cnt := by
  rw [rTotals_cnt]
  rcases (mem_buildOf_iff.mp h) with ⟨r, hr, hrk⟩
  apply List.length_pos_of_mem
  exact List.mem_filter.mpr ⟨hr, by simp [hrk]⟩

private theorem cnt_zero_of_not_mem_buildOf (rs : List RRow) (k : Int) (h : k ∉ buildOf rs) :
    (rTotals rs k).cnt = 0 := by
  have hnil : (rs.filter fun r : RRow => r.k == some k) = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro r hr
    have hk : r.k = some k := by simpa using (List.mem_filter.mp hr).2
    exact h (mem_buildOf_iff.mpr ⟨r, (List.mem_filter.mp hr).1, hk⟩)
  rw [rTotals_cnt, hnil]
  rfl

private theorem filter_key_nil_or_singleton_totals {β : Type} [BEq β] [Hashable β]
    [LawfulBEq β] [LawfulHashable β] (m : Std.HashMap β Totals) (k : β) :
    m.toList.filter (fun e : β × Totals => k == e.1) = [] ∨
      m.toList.filter (fun e : β × Totals => k == e.1) = [(k, m.getD k zero)] := by
  let l := m.toList.filter fun e : β × Totals => k == e.1
  have hpair : l.Pairwise (fun a b => (a.1 == b.1) = false) := by
    dsimp [l]
    exact (Std.HashMap.distinct_keys_toList (m := m)).filter _
  have hall : ∀ e ∈ l, e = (k, m.getD k zero) := by
    intro e he
    dsimp [l] at he
    have hm := List.mem_filter.mp he
    have hkey : e.1 = k := by
      exact (beq_iff_eq.mp hm.2).symm
    have hsome : m[e.1]? = some e.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hm.1
    have hsome' : m[k]? = some e.2 := by simpa [hkey] using hsome
    have hget : m.getD k zero = e.2 := by
      rw [Std.HashMap.getD_eq_getD_getElem?, hsome']
      simp
    cases e with
    | mk kk vv =>
        simp at hkey ⊢
        subst kk
        simp at hget ⊢
        rw [hget]
  change l = [] ∨ l = [(k, m.getD k zero)]
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
            have heq : e2.1 = e.1 := by simpa using hkey
            have htrue : (e.1 == e2.1) = true := by simp [heq]
            rw [htrue] at hne
            cases hne
      simp [hl, he, hes]

private theorem pfold_insert_add {β : Type} [BEq β] [Hashable β] [LawfulBEq β] [LawfulHashable β]
    (m : Std.HashMap β Totals) (k : β) (w : Totals) (rOf : β → Totals) (acc : Totals × Totals) :
    ((m.insert k (plus (m.getD k zero) w)).toList.foldl
        (fun (a : Totals × Totals) (e : β × Totals) => pplus a (h2c e.2 (rOf e.1))) acc) =
      pplus (m.toList.foldl
        (fun (a : Totals × Totals) (e : β × Totals) => pplus a (h2c e.2 (rOf e.1))) acc)
        (h2c w (rOf k)) := by
  let v := plus (m.getD k zero) w
  let A := m.toList.filter fun e : β × Totals => !(k == e.1)
  let fe := fun (e : β × Totals) => h2c e.2 (rOf e.1)
  have hpPerm : (m.insert k v).toList.Perm ((k, v) :: A) := by
    dsimp [A, v]
    exact Std.HashMap.toList_insert_perm
  rw [pfold_entries_perm hpPerm fe acc]
  dsimp [A, fe]
  rw [pfold_start_eq A fe (pplus acc (h2c v (rOf k)))]
  rw [pfold_start_eq m.toList fe acc]
  rcases filter_key_nil_or_singleton_totals m k with hnil | hsingle
  · have hnone : ∀ e ∈ m.toList, (k == e.1) = false := by
      intro e he
      apply Bool.eq_false_of_ne_true
      intro hktrue
      have hm : e ∈ m.toList.filter (fun x : β × Totals => k == x.1) := by simp [he, hktrue]
      rw [hnil] at hm
      simp at hm
    have hA : A = m.toList := by
      dsimp [A]
      apply List.filter_eq_self.mpr
      intro e he
      simp [hnone e he]
    rw [hA]
    have hnotmem : k ∉ m := by
      intro hk
      have hs : m[k]? = some (m.getD k zero) := Std.HashMap.getElem?_eq_some_getD hk
      have hm : (k, m.getD k zero) ∈ m.toList := Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hs
      have : (k, m.getD k zero) ∈ m.toList.filter (fun x : β × Totals => k == x.1) := by simp [hm]
      rw [hnil] at this
      simp at this
    have holdzero : m.getD k zero = zero := by
      rw [Std.HashMap.getD_eq_getD_getElem?, Std.HashMap.getElem?_eq_none hnotmem]
      simp
    have hv : v = w := by dsimp [v]; rw [holdzero]; simp [plus, zero]
    rw [hv]
    ac_rfl
  · have hd : ∀ e ∈ m.toList,
        ¬((k == e.1) = true ∧ (!(k == e.1)) = true) := by
      intro e he h
      have hf := h.2
      simp [h.1] at hf
    have hpall := split_filter_perm_h2 (fun e : β × Totals => k == e.1)
      (fun e : β × Totals => !(k == e.1)) m.toList hd
    have hall : (m.toList.filter fun e : β × Totals => (k == e.1) || !(k == e.1)) = m.toList := by
      apply List.filter_eq_self.mpr
      intro e he
      cases h : k == e.1 <;> simp [h]
    rw [hall, hsingle] at hpall
    have hfoldm : m.toList.foldl (fun a e => pplus a (fe e)) (zero, zero) =
        A.foldl (fun a e => pplus a (fe e)) (h2c (m.getD k zero) (rOf k)) := by
      rw [pfold_entries_perm hpall fe (zero, zero)]
      dsimp [A]
      simp [fe, pplus_zero_left]
    have hvEq : h2c v (rOf k) = pplus (h2c (m.getD k zero) (rOf k)) (h2c w (rOf k)) := by
      dsimp [v]
      exact h2c_add_left (m.getD k zero) w (rOf k)
    rw [hvEq, hfoldm]
    rw [pfold_start_eq A fe (h2c (m.getD k zero) (rOf k))]
    ac_rfl

private theorem regroup_cells {β : Type} [BEq β] [Hashable β] [LawfulBEq β] [LawfulHashable β]
    (entries : List (β × Option Int)) (rOf : β → Totals) :
    (entries.foldl (fun (m : Std.HashMap β Totals) (e : β × Option Int) =>
      m.insert e.1 (plus (m.getD e.1 zero) (weighted 1 e.2))) ({} : Std.HashMap β Totals)).toList.foldl
        (fun (a : Totals × Totals) (e : β × Totals) => pplus a (h2c e.2 (rOf e.1))) (zero, zero) =
      entries.foldl (fun (a : Totals × Totals) (e : β × Option Int) =>
        pplus a (h2c (weighted 1 e.2) (rOf e.1))) (zero, zero) := by
  let ins := fun (m : Std.HashMap β Totals) (e : β × Option Int) =>
    m.insert e.1 (plus (m.getD e.1 zero) (weighted 1 e.2))
  have h : ∀ (es : List (β × Option Int)) (m : Std.HashMap β Totals) (acc : Totals × Totals),
      (es.foldl ins m).toList.foldl
          (fun (a : Totals × Totals) (e : β × Totals) => pplus a (h2c e.2 (rOf e.1))) acc =
        pplus (m.toList.foldl
          (fun (a : Totals × Totals) (e : β × Totals) => pplus a (h2c e.2 (rOf e.1))) acc)
          (es.foldl (fun (a : Totals × Totals) (e : β × Option Int) =>
            pplus a (h2c (weighted 1 e.2) (rOf e.1))) (zero, zero)) := by
    intro es
    induction es with
    | nil =>
        intro m acc
        simp [ins, pplus_zero_right]
    | cons e es ih =>
        intro m acc
        simp only [List.foldl_cons]
        rw [ih (ins m e) acc]
        dsimp [ins]
        rw [pfold_insert_add m e.1 (weighted 1 e.2) rOf acc]
        rw [pfold_start_eq es (fun (z : β × Option Int) =>
          h2c (weighted 1 z.2) (rOf z.1)) (pplus (zero, zero) (h2c (weighted 1 e.2) (rOf e.1)))]
        rw [pplus_zero_left]
        ac_rfl
  have hh := h entries {} (zero, zero)
  simpa [ins, pplus_zero_left] using hh

private def cellStep (build : Std.HashMap Int Totals) (m : Std.HashMap (Nat × Nat × Int) Totals)
    (l : LRow) : Std.HashMap (Nat × Nat × Int) Totals :=
  Id.run do
    let some k := l.k | return m
    let some r := build[k]? | return m
    let sig := sigOf r
    return m.insert sig (plus (m.getD sig zero) (weighted 1 l.v))

private def activeEntry (build : Std.HashMap Int Totals) (l : LRow) : Option (Int × Option Int × Totals) :=
  match l.k with
  | none => none
  | some k => match build[k]? with
      | none => none
      | some r => some (k, l.v, r)

private theorem cellStep_eq_active (build : Std.HashMap Int Totals)
    (m : Std.HashMap (Nat × Nat × Int) Totals) (l : LRow) :
    cellStep build m l = match activeEntry build l with
      | none => m
      | some (k, v, r) => m.insert (sigOf r) (plus (m.getD (sigOf r) zero) (weighted 1 v)) := by
  cases hk : l.k with
  | none => simp [cellStep, activeEntry, hk]
  | some k =>
      cases hb : build[k]? with
      | none => simp [cellStep, activeEntry, hk, hb]
      | some r => simp [cellStep, activeEntry, hk, hb, sigOf]

private theorem cellStep_fold_eq (build : Std.HashMap Int Totals) (ls : List LRow)
    (m : Std.HashMap (Nat × Nat × Int) Totals) :
    ls.foldl (cellStep build) m =
      ((ls.filterMap (activeEntry build)).map (fun e => (sigOf e.2.2, e.2.1))).foldl
        (fun (m : Std.HashMap (Nat × Nat × Int) Totals) (e : (Nat × Nat × Int) × Option Int) =>
          m.insert e.1 (plus (m.getD e.1 zero) (weighted 1 e.2))) m := by
  induction ls generalizing m with
  | nil => rfl
  | cons l ls ih =>
      rw [List.foldl_cons, cellStep_eq_active]
      cases ha : activeEntry build l with
      | none => simp [ha]; exact ih m
      | some e =>
          simp [ha]
          exact ih (m.insert (sigOf e.2.2) (plus (m.getD (sigOf e.2.2) zero) (weighted 1 e.2.1)))

private theorem cellInsert_valid_kv {β : Type} [BEq β] [Hashable β] [LawfulBEq β] [LawfulHashable β]
    (K : β → Prop) (m : Std.HashMap β Totals) (k : β) (w : Totals)
    (hk : K k) (hw : Valid w) (hv : ∀ e ∈ m.toList, K e.1 ∧ Valid e.2) :
    ∀ e ∈ (m.insert k w).toList, K e.1 ∧ Valid e.2 := by
  have hvall : ∀ z, Valid (m.getD z zero) := by
    intro z
    exact valid_getD_gen m z (by intro q hq; exact (hv q hq).2)
  intro e he
  have heqval : e.2 = (m.insert k w).getD e.1 zero := by
    have hs : (m.insert k w)[e.1]? = some e.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp he
    rw [Std.HashMap.getD_eq_getD_getElem?, hs]
    simp
  have heval : Valid e.2 := by
    rw [heqval]
    exact valid_getD_insert m k w hw hvall e.1
  have hekey : K e.1 := by
    by_cases hke : k == e.1
    · have hkeq : k = e.1 := by simpa using hke
      simpa [hkeq] using hk
    · have hs : (m.insert k w)[e.1]? = some e.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp he
      have hs' : m[e.1]? = some e.2 := by simpa [Std.HashMap.getElem?_insert, hke] using hs
      have hm : e ∈ m.toList := Std.HashMap.mem_toList_iff_getElem?_eq_some.mpr hs'
      exact (hv e hm).1
  exact ⟨hekey, heval⟩

private theorem cellFold_valid {β : Type} [BEq β] [Hashable β] [LawfulBEq β] [LawfulHashable β]
    (K : β → Prop) (entries : List (β × Option Int)) (m0 : Std.HashMap β Totals)
    (hv0 : ∀ e ∈ m0.toList, K e.1 ∧ Valid e.2)
    (hkvalid : ∀ e ∈ entries, K e.1)
    (hwvalid : ∀ e ∈ entries, Valid (weighted 1 e.2)) :
    ∀ e ∈ (entries.foldl (fun (m : Std.HashMap β Totals) (p : β × Option Int) =>
        m.insert p.1 (plus (m.getD p.1 zero) (weighted 1 p.2))) m0).toList, K e.1 ∧ Valid e.2 := by
  induction entries generalizing m0 with
  | nil => exact hv0
  | cons e es ih =>
      exact ih (m0.insert e.1 (plus (m0.getD e.1 zero) (weighted 1 e.2)))
        (cellInsert_valid_kv K m0 e.1 (plus (m0.getD e.1 zero) (weighted 1 e.2))
          (hkvalid e (by simp))
          (valid_plus (valid_getD_gen m0 e.1 (by intro z hz; exact (hv0 z hz).2)) (hwvalid e (by simp)))
          hv0)
        (by intro z hz; exact hkvalid z (by simp [hz]))
        (by intro z hz; exact hwvalid z (by simp [hz]))

private theorem active_entries_pfold_eq (ls : List LRow) (rs : List RRow) (g : Int) :
    ((((ls.filter (fun l : LRow => l.g == g)).filterMap (activeEntry (buildOf rs))).map
        (fun e => (sigOf e.2.2, e.2.1))).foldl
      (fun (a : Totals × Totals) (e : (Nat × Nat × Int) × Option Int) =>
        pplus a (h2c (weighted 1 e.2) (totalsOfSig e.1))) (zero, zero)) =
      ((ls.filter (lActive rs g)).foldl (fun t l => pplus t (lContrib rs l)) (zero, zero)) := by
  induction ls with
  | nil => rfl
  | cons l ls ih =>
      by_cases hg : l.g == g
      · have hfilt : (l :: ls).filter (fun x : LRow => x.g == g) =
            l :: ls.filter (fun x => x.g == g) := List.filter_cons_of_pos hg
        rw [hfilt]
        cases hk : l.k with
        | none =>
            have hact : lActive rs g l = false := by simp [lActive, hk]
            simp [activeEntry, hk, hact]
            exact ih
        | some k =>
            by_cases hbeq : (buildOf rs)[k]? = none
            · have hnot : k ∉ buildOf rs := by
                intro hm
                have hs : (buildOf rs)[k]? = some ((buildOf rs).getD k zero) :=
                  Std.HashMap.getElem?_eq_some_getD hm
                rw [hbeq] at hs
                cases hs
              have hc := cnt_zero_of_not_mem_buildOf rs k hnot
              have hact : lActive rs g l = false := by simp [lActive, hk, hc]
              simp [activeEntry, hk, hbeq, hact]
              exact ih
            · rcases Option.ne_none_iff_exists.mp hbeq with ⟨r, hrbr⟩
              have hbr : (buildOf rs)[k]? = some r := hrbr.symm
              have hr_eq : r = rTotals rs k := by
                rw [eq_getD_of_getElem?_eq_some _ _ _ hbr]
                simpa [buildOf] using build_final rs k
              have hmem : k ∈ buildOf rs := by
                by_cases hm : k ∈ buildOf rs
                · exact hm
                · have hnone2 : (buildOf rs)[k]? = none := Std.HashMap.getElem?_eq_none hm
                  rw [hnone2] at hbr
                  cases hbr
              have hpos : 0 < (rTotals rs k).cnt := cnt_pos_of_mem_buildOf rs k hmem
              have hcntnz : (rTotals rs k).cnt ≠ 0 := by omega
              have hcntne : ((rTotals rs k).cnt == 0) = false := by simpa using hcntnz
              have hact : lActive rs g l = true := by simp [lActive, hk, hg, hcntne]
              simp [activeEntry, hk, hbr, hact, lContrib, hk, hr_eq, totalsOfSig_sigOf, pplus_zero_left]
              rw [pfold_start_eq]
              rw [ih]
              rw [pfold_start_eq (List.filter (lActive rs g) ls)
                (fun (x : LRow) => match x.k with
                  | none => (zero, zero)
                  | some kk => h2c (weighted 1 x.v) (rTotals rs kk))
                (h2c (weighted 1 l.v) (rTotals rs k))]
              ac_rfl
      · have hfilt : (l :: ls).filter (fun x : LRow => x.g == g) =
            ls.filter (fun x => x.g == g) := List.filter_cons_of_neg hg
        have hact : lActive rs g l = false := by simp [lActive, hg]
        rw [hfilt]
        simp [hact]
        exact ih

theorem RightStateClasses_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights.RightStateClasses.Goal := by
  intro ls rs g
  dsimp only
  let build : Std.HashMap Int Totals := rs.foldl
    (fun (m : Std.HashMap Int Totals) r => match r.k with
      | none => m
      | some k => m.insert k (plus (m.getD k zero) (weighted 1 r.v))) {}
  let cells := (ls.filter (fun l : LRow => l.g == g)).foldl (cellStep build) ({} : Std.HashMap (Nat × Nat × Int) Totals)
  let entries := (((ls.filter (fun l : LRow => l.g == g)).filterMap (activeEntry build)).map
    (fun e => (sigOf e.2.2, e.2.1)))
  let grouped := cells.toList.foldl
    (fun (groups : Groups) (p : (Nat × Nat × Int) × Totals) => contribute groups g p.2 (totalsOfSig p.1))
    ({} : Groups)
  change finish grouped g = refGroup ls rs g
  have hcells : cells = entries.foldl
      (fun (m : Std.HashMap (Nat × Nat × Int) Totals) (e : (Nat × Nat × Int) × Option Int) =>
        m.insert e.1 (plus (m.getD e.1 zero) (weighted 1 e.2))) ({} : Std.HashMap (Nat × Nat × Int) Totals) := by
    dsimp [cells, entries]
    exact cellStep_fold_eq build (ls.filter fun l : LRow => l.g == g) {}
  have hentryK : ∀ e ∈ entries, Valid (totalsOfSig e.1) := by
    dsimp [entries]
    intro e he
    rw [List.mem_map] at he
    rcases he with ⟨a, ha, rfl⟩
    rcases List.mem_filterMap.mp ha with ⟨l0, hl0, hfa⟩
    cases hk0 : l0.k with
    | none => simp [activeEntry, hk0] at hfa
    | some k0 =>
        cases hb0 : build[k0]? with
        | none => simp [activeEntry, hk0, hb0] at hfa
        | some r0 =>
            have haeq : a = (k0, l0.v, r0) := by simpa [activeEntry, hk0, hb0] using hfa.symm
            subst a
            rw [totalsOfSig_sigOf, eq_getD_of_getElem?_eq_some _ _ _ hb0]
            simpa [build, buildOf] using build_valid_getD rs k0
  have hentryW : ∀ e ∈ entries, Valid (weighted 1 e.2) := by
    intro e he
    exact valid_weighted 1 e.2
  have hcellsValid : ∀ e ∈ cells.toList, Valid (totalsOfSig e.1) ∧ Valid e.2 := by
    rw [hcells]
    let K := fun s : Nat × Nat × Int => Valid (totalsOfSig s)
    exact cellFold_valid K entries {}
      (by intro e he; simp at he) hentryK hentryW
  have hgrouped_aux : ∀ (es : List ((Nat × Nat × Int) × Totals)) (h0 : Groups),
      (∀ q, Valid ((h0.getD q (zero, zero)).1) ∧ Valid ((h0.getD q (zero, zero)).2)) →
      (∀ e ∈ es, Valid (totalsOfSig e.1) ∧ Valid e.2) →
      (es.foldl (fun (groups : Groups) (p : (Nat × Nat × Int) × Totals) =>
        contribute groups g p.2 (totalsOfSig p.1)) h0).getD g (zero, zero) =
        pplus (h0.getD g (zero, zero))
          (es.foldl (fun a (e : (Nat × Nat × Int) × Totals) => pplus a (h2c e.2 (totalsOfSig e.1))) (zero, zero)) := by
    intro es
    induction es with
    | nil =>
        intro h0 hkv hv
        simp [pplus_zero_right]
    | cons e es ih =>
        intro h0 hkv hv
        rw [List.foldl_cons, List.foldl_cons]
        have he := hv e (by simp)
        have htail : ∀ z ∈ es, Valid (totalsOfSig z.1) ∧ Valid z.2 := by intro z hz; exact hv z (by simp [hz])
        have hk' : ∀ q, Valid (((contribute h0 g e.2 (totalsOfSig e.1)).getD q (zero, zero)).1) ∧
            Valid (((contribute h0 g e.2 (totalsOfSig e.1)).getD q (zero, zero)).2) := by
          intro q
          rw [contribute_getD_v h0 g q e.2 (totalsOfSig e.1) he.2 he.1]
          by_cases hq : (q == g) = true
          · simp [hq]
            exact ⟨valid_plus (hkv q).1 (valid_h2c_fst he.2 he.1),
              valid_plus (hkv q).2 (valid_h2c_snd he.2 he.1)⟩
          · simp [hq]
            exact hkv q
        rw [ih (contribute h0 g e.2 (totalsOfSig e.1)) hk' htail]
        rw [contribute_getD_v h0 g g e.2 (totalsOfSig e.1) he.2 he.1]
        rw [pfold_start_eq es (fun (z : (Nat × Nat × Int) × Totals) =>
          h2c z.2 (totalsOfSig z.1)) (pplus (zero, zero) (h2c e.2 (totalsOfSig e.1)))]
        simp [pplus_zero_left]
        ac_rfl
  have hgrouped : grouped.getD g (zero, zero) =
      cells.toList.foldl (fun a (e : (Nat × Nat × Int) × Totals) =>
        pplus a (h2c e.2 (totalsOfSig e.1))) (zero, zero) := by
    dsimp [grouped]
    rw [hgrouped_aux cells.toList ({} : Groups)
      (by intro q; simp [zero, Valid]) hcellsValid]
    simp [pplus_zero_left]
  have hreg := regroup_cells (entries := entries) (rOf := totalsOfSig)
  have hpfoldCells : cells.toList.foldl (fun a (e : (Nat × Nat × Int) × Totals) =>
        pplus a (h2c e.2 (totalsOfSig e.1))) (zero, zero) =
      entries.foldl (fun a (e : (Nat × Nat × Int) × Option Int) =>
        pplus a (h2c (weighted 1 e.2) (totalsOfSig e.1))) (zero, zero) := by
    rw [hcells]
    exact hreg
  have hactive := active_entries_pfold_eq ls rs g
  have hnaive : (naiveFold rs ({} : Groups) ls).getD g (zero, zero) =
      ((ls.filter (lActive rs g)).foldl (fun t l => pplus t (lContrib rs l)) (zero, zero)) := by
    rw [naiveFold_getD]
    simp [pplus_zero_left]
  have hstate : grouped.getD g (zero, zero) = (naiveFold rs ({} : Groups) ls).getD g (zero, zero) := by
    rw [hgrouped, hpfoldCells]
    rw [show entries.foldl (fun a (e : (Nat × Nat × Int) × Option Int) =>
        pplus a (h2c (weighted 1 e.2) (totalsOfSig e.1))) (zero, zero) =
        ((ls.filter (lActive rs g)).foldl (fun t l => pplus t (lContrib rs l)) (zero, zero)) by
      simpa [entries, build, buildOf] using hactive]
    exact hnaive.symm
  rw [finish_eq_of_getD grouped (naiveFold rs ({} : Groups) ls) g hstate]
  exact finish_naive_ref ls rs g

/-! ### ShardedHybrid: shard partition of the accumulator state -/

private def shardL (ls : List LRow) (route : Int → Nat) (j : Nat) : List LRow :=
  ls.filter fun l => l.k.any (fun k => route k == j)
private def shardR (rs : List RRow) (route : Int → Nat) (j : Nat) : List RRow :=
  rs.filter fun r => r.k.any (fun k => route k == j)

private theorem pfold_zero {α : Type} (l : List α) :
    l.foldl (fun a _ => pplus a (zero, zero)) (zero, zero) = (zero, zero) := by
  induction l with
  | nil => rfl
  | cons x xs ih => simpa [pplus_zero_right] using ih

private theorem pfold_map_zero (workers : Nat) :
    ((List.range workers).map fun _ : Nat => (zero, zero)).foldl pplus (zero, zero) = (zero, zero) := by
  induction workers with
  | zero => rfl
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.foldl_append, ih]
      simp [pplus_zero_right]

private theorem pfold_range_update (workers j0 : Nat) (hj0 : j0 < workers) :
    ∀ (old : Nat → Totals × Totals) (v : Totals × Totals),
    ((List.range workers).map fun j => pplus (old j) (if j == j0 then v else (zero, zero))).foldl
        pplus (zero, zero) =
      pplus (((List.range workers).map old).foldl pplus (zero, zero)) v := by
  induction workers with
  | zero => intro old v; omega
  | succ workers ih =>
      intro old v
      rw [List.range_succ, List.map_append, List.foldl_append]
      by_cases hj : j0 < workers
      · rw [ih hj old v]
        have hneq : workers ≠ j0 := by omega
        have hne : (workers == j0) = false := by simp [hneq]
        simp [hneq, hne, pplus_zero_right]
        ac_rfl
      · have hjEq : j0 = workers := by omega
        subst j0
        have hprev : (List.range workers).map (fun j => pplus (old j) (if j == workers then v else (zero, zero))) =
            (List.range workers).map old := by
          apply List.map_congr_left
          intro j hjmem
          have hjlt : j < workers := List.mem_range.mp hjmem
          have hneq : j ≠ workers := by omega
          have hne : (j == workers) = false := by simp [hneq]
          simp [hne, pplus_zero_right]
        rw [hprev]
        have hlast : (workers == workers) = true := by simp
        simp [hlast]
        ac_rfl

private theorem rTotals_shard (rs : List RRow) (route : Int → Nat) (j : Nat) (k : Int)
    (hk : route k = j) : rTotals (shardR rs route j) k = rTotals rs k := by
  unfold rTotals shardR
  have hfilt : (rs.filter fun r : RRow => r.k.any (fun kk => route kk == j)).filter
        (fun r : RRow => r.k == some k) =
      rs.filter (fun r : RRow => r.k == some k) := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro r hr
    by_cases hrk : r.k == some k
    · have hreq : r.k = some k := by simpa using hrk
      have hany : r.k.any (fun kk => route kk == j) = true := by simp [hreq, hk]
      simp [hrk, hany]
    · simp [hrk]
  rw [hfilt]

private theorem lActive_shard (rs : List RRow) (route : Int → Nat) (j : Nat) (l : LRow) (k : Int)
    (hk : l.k = some k) (hkj : route k = j) (g : Int) :
    lActive (shardR rs route j) g l = lActive rs g l ∧
      lContrib (shardR rs route j) l = lContrib rs l := by
  have hrt := rTotals_shard rs route j k hkj
  constructor
  · unfold lActive
    simp [hk, hrt]
  · unfold lContrib
    simp [hk, hrt]

private theorem naiveFold_cons_getD (rs : List RRow) (ls : List LRow) (g : Int) (l : LRow) :
    (naiveFold rs ({} : Groups) (l :: ls)).getD g (zero, zero) =
      if lActive rs g l then
        pplus ((naiveFold rs ({} : Groups) ls).getD g (zero, zero)) (lContrib rs l)
      else (naiveFold rs ({} : Groups) ls).getD g (zero, zero) := by
  rw [naiveFold_getD, naiveFold_getD]
  simp [pplus_zero_left]
  by_cases hact : lActive rs g l
  · rw [List.filter_cons_of_pos hact, List.foldl_cons]
    rw [if_pos hact]
    rw [pplus_zero_left]
    rw [pfold_start_eq (List.filter (lActive rs g) ls) (lContrib rs) (lContrib rs l)]
    ac_rfl
  · rw [List.filter_cons_of_neg hact]
    rw [if_neg hact]

private theorem shardState_step (ls : List LRow) (rs : List RRow) (route : Int → Nat)
    (hot : Nat → Int → Bool) (g : Int) (j : Nat) (l : LRow) (k : Int)
    (hk : l.k = some k) (hkj : route k = j) :
    (HotKeyAndEpochAggregates.hybridGroups (shardL (l :: ls) route j) (shardR rs route j) (hot j)).getD g (zero, zero) =
      pplus ((HotKeyAndEpochAggregates.hybridGroups (shardL ls route j) (shardR rs route j) (hot j)).getD g (zero, zero))
        (if lActive rs g l then lContrib rs l else (zero, zero)) := by
  have hsL : shardL (l :: ls) route j = l :: shardL ls route j := by
    unfold shardL
    simp [hk, hkj]
  rw [hsL]
  rw [hybridGroups_getD, hybridGroups_getD]
  rw [naiveFold_cons_getD]
  have hsame := lActive_shard rs route j l k hk hkj g
  rw [hsame.2]
  by_cases hact : lActive rs g l
  · rw [if_pos hact]
    rw [if_pos (by simpa [hsame.1] using hact)]
  · rw [if_neg hact]
    rw [hsame.1]
    rw [if_neg hact]
    rw [pplus_zero_right]

private theorem shardState_same (ls : List LRow) (rs : List RRow) (route : Int → Nat)
    (hot : Nat → Int → Bool) (g : Int) (j : Nat) (l : LRow) (k : Int)
    (hk : l.k = some k) (hkj : route k ≠ j) :
    (HotKeyAndEpochAggregates.hybridGroups (shardL (l :: ls) route j) (shardR rs route j) (hot j)).getD g (zero, zero) =
      (HotKeyAndEpochAggregates.hybridGroups (shardL ls route j) (shardR rs route j) (hot j)).getD g (zero, zero) := by
  have hsL : shardL (l :: ls) route j = shardL ls route j := by
    unfold shardL
    simp [hk, hkj]
  rw [hsL]

theorem ShardedHybrid_sound :
    TinylambOpt.ResearchGoals.AnalyticalOperators.SharedJoinStatesAndWeights.ShardedHybrid.Goal := by
  intro ls rs g route hot workers hbound
  dsimp only
  have hpart : ((List.range workers).map (fun j =>
      (HotKeyAndEpochAggregates.hybridGroups (shardL ls route j) (shardR rs route j) (hot j)).getD g (zero, zero))).foldl
        pplus (zero, zero) =
      (naiveFold rs ({} : Groups) ls).getD g (zero, zero) := by
    induction ls with
    | nil =>
        have hz : ∀ j, (HotKeyAndEpochAggregates.hybridGroups ([] : List LRow) (shardR rs route j) (hot j)).getD g (zero, zero) =
            (zero, zero) := by
          intro j
          rw [hybridGroups_getD]
          rw [naiveFold_getD]
          simp [pplus_zero_left]
        have hsl0 : ∀ j, shardL ([] : List LRow) route j = [] := by
          intro j
          simp [shardL]
        simp [hsl0, hz]
        rw [pfold_map_zero]
        rw [naiveFold_getD]
        simp [pplus_zero_left]
    | cons l ls ih =>
        cases hk : l.k with
        | none =>
            have hsl : ∀ j, shardL (l :: ls) route j = shardL ls route j := by
              intro j
              unfold shardL
              simp [hk]
            simp [hsl]
            rw [ih]
            rw [naiveFold_cons_getD]
            have hact : lActive rs g l = false := by simp [lActive, hk]
            have hactNeg : ¬ lActive rs g l = true := by simpa using hact
            rw [if_neg hactNeg]
        | some k =>
            let j0 := route k
            have hj0 : j0 < workers := hbound k
            let old := fun j : Nat =>
              (HotKeyAndEpochAggregates.hybridGroups (shardL ls route j) (shardR rs route j) (hot j)).getD g (zero, zero)
            let v := if lActive rs g l then lContrib rs l else (zero, zero)
            have hstates : (List.range workers).map (fun j =>
                (HotKeyAndEpochAggregates.hybridGroups (shardL (l :: ls) route j) (shardR rs route j) (hot j)).getD g (zero, zero)) =
                (List.range workers).map (fun j => pplus (old j) (if j == j0 then v else (zero, zero))) := by
              apply List.map_congr_left
              intro j hjmem
              by_cases hEq : j == j0
              · have heq : j = j0 := by simpa using hEq
                subst j
                dsimp [old, v, j0]
                rw [shardState_step ls rs route hot g (route k) l k hk rfl]
                simp
              · have hkj : route k ≠ j := by
                  intro h
                  apply hEq
                  simp [h, j0]
                rw [shardState_same ls rs route hot g j l k hk hkj]
                dsimp [old, v, j0]
                have hkj' : j ≠ route k := by intro h; exact hkj h.symm
                simp [hEq, hkj, hkj', pplus_zero_right]
            rw [hstates]
            rw [pfold_range_update workers j0 hj0 old v]
            change pplus (((List.range workers).map old).foldl pplus (zero, zero)) v =
              (naiveFold rs ({} : Groups) (l :: ls)).getD g (zero, zero)
            rw [ih]
            rw [naiveFold_cons_getD]
            dsimp [v, j0]
            by_cases hact : lActive rs g l
            · rw [if_pos hact, if_pos hact]
            · have hactNeg : ¬ lActive rs g l = true := by simpa using hact
              rw [if_neg hactNeg, if_neg hactNeg]
              rw [pplus_zero_right]
  let states := (List.range workers).map (fun j =>
    (HotKeyAndEpochAggregates.hybridGroups (shardL ls route j) (shardR rs route j) (hot j)).getD g (zero, zero))
  let total := states.foldl (fun (u, v) (s, t) => (plus u s, plus v t)) (zero, zero)
  change HotKeyAndEpochAggregates.finish (({} : Groups).insert g total) g = refGroup ls rs g
  have hstate : total = (naiveFold rs ({} : Groups) ls).getD g (zero, zero) := by
    dsimp [total, states]
    change ((List.range workers).map (fun j =>
      (HotKeyAndEpochAggregates.hybridGroups (shardL ls route j) (shardR rs route j) (hot j)).getD g (zero, zero))).foldl
        pplus (zero, zero) = _
    exact hpart
  rw [show HotKeyAndEpochAggregates.finish (({} : Groups).insert g total) g =
      HotKeyAndEpochAggregates.finish (naiveFold rs ({} : Groups) ls) g by
    apply finish_eq_of_getD
    simp [hstate]]
  exact finish_naive_ref ls rs g

end TinylambOpt.ResearchProofs.AnalyticalOperators.SharedJoinStatesAndWeights
