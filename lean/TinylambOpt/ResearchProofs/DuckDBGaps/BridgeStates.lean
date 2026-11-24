import TinylambOpt.ResearchGoals.DuckDBGaps.BridgeStates
import TinylambOpt.ResearchProofs.Wave8H2o

namespace TinylambOpt.ResearchProofs.DuckDBGaps.BridgeStates

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o
open TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown (LRow RRow refGroup)
open TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining (PostRec LinkRec refBridgeAgg refDualUserCount UserRow)
open TinylambOpt.ResearchGoals.DuckDBGaps.BridgeStates (Bucket)

set_option maxHeartbeats 0

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
    TinylambOpt.ResearchGoals.DuckDBGaps.BridgeStates.CountryProfiles.Goal := by
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

end TinylambOpt.ResearchProofs.DuckDBGaps.BridgeStates
