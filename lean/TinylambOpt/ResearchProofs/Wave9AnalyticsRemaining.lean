import TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining

namespace TinylambOpt.ResearchProofs.Wave9AnalyticsRemaining

open TinylambOpt.ResearchGoals.AnalyticsWorkloads (Totals zero weighted plus summarize histogram)
open TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining (PostRec LinkRec UserRow)

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  rcases a with ⟨ac, an, as⟩
  rcases b with ⟨bc, bn, bs⟩
  rcases c with ⟨cc, cn, cs⟩
  simp [plus]
  omega

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  rcases a with ⟨ac, an, as⟩
  simp [plus, zero]

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  rcases a with ⟨ac, an, as⟩
  simp [plus, zero]

private theorem foldl_summarize (xs : List (Option Int)) (acc : Totals) :
    xs.foldl (fun a v => plus a (weighted 1 v)) acc = plus acc (summarize xs) := by
  induction xs generalizing acc with
  | nil => simp [summarize, plus_zero_right]
  | cons x xs ih =>
      rw [List.foldl_cons, ih]
      change plus (plus acc (weighted 1 x)) (summarize xs) =
        plus acc (List.foldl (fun a v => plus a (weighted 1 v)) (plus zero (weighted 1 x)) xs)
      rw [plus_zero_left, ih, plus_assoc]

private theorem summarize_cons (v : Option Int) (vs : List (Option Int)) :
    summarize (v :: vs) = plus (weighted 1 v) (summarize vs) := by
  simpa [summarize, plus_zero_left] using foldl_summarize vs (weighted 1 v)

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
      rw [List.replicate_succ, summarize_cons, ih]
      rcases v with _ | z
      · simp [weighted, plus]
        omega
      · simp [weighted, plus]
        constructor
        · omega
        · rw [Int.add_mul]
          simp [Int.add_comm]

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

/-! ## FilteredSemiJoinCountBeforeGroup -/

private theorem edge_degree_getD (es : List (Nat × Nat)) (id : Nat) :
    (histogram (es.map Prod.fst)).getD id 0 = (es.filter fun e => e.1 == id).length := by
  rw [histogram_getD_count, List.filter_map, List.length_map]
  congr 1

private theorem map_const_replicate {α β : Type} (xs : List α) (y : β) :
    xs.map (fun _ => y) = List.replicate xs.length y := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih, List.replicate_succ]

private theorem weighted_eq_summarize_filter (es : List (Nat × Nat)) (id : Nat) (v : Option Int) :
    weighted ((histogram (es.map Prod.fst)).getD id 0) v =
      summarize ((es.filter fun e => e.1 == id).map fun _ => v) := by
  rw [edge_degree_getD, map_const_replicate, summarize_replicate]

private theorem degree_fold (entities : List (Nat × Nat × Option Int))
    (edges : List (Nat × Nat)) (groupKey : Nat) (acc : Totals) :
    entities.foldl (fun acc (id, g, v) =>
      if g == groupKey then plus acc
        (weighted ((histogram (edges.map Prod.fst)).getD id 0) v) else acc) acc =
      plus acc (summarize (entities.flatMap fun (id, g, v) =>
        if g != groupKey then [] else (edges.filter fun e => e.1 == id).map fun _ => v)) := by
  induction entities generalizing acc with
  | nil => simp [summarize, plus_zero_right]
  | cons e entities ih =>
      rcases e with ⟨id, g, v⟩
      simp only [List.foldl_cons]
      by_cases hg : (g == groupKey) = true
      · rw [ite_eq_left hg, ih, List.flatMap_cons]
        have hne : (g != groupKey) = false := by simpa using hg
        simp [hne, summarize_append, weighted_eq_summarize_filter, plus_assoc]
      · rw [ite_eq_right hg, ih, List.flatMap_cons]
        have hgF : (g == groupKey) = false := Bool.eq_false_of_ne_true hg
        have hne : (g != groupKey) = true := by simpa using hgF
        simp [hne, plus_assoc]

theorem FilteredSemiJoinCountBeforeGroup_sound :
    TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.FilteredSemiJoinCountBeforeGroup.Goal := by
  intro entities edges groupKey
  unfold TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.degreeWeightedAgg
  unfold TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.refEdgeJoinAgg
  dsimp only
  simpa [plus_zero_left] using (degree_fold entities edges groupKey zero)

/-! ## MultiKeyInRangeZoneBlockSkip -/

theorem MultiKeyInRangeZoneBlockSkip_sound :
    TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.MultiKeyInRangeZoneBlockSkip.Goal := by
  intro rows min1 max1 min2 max2 lo hi s hrows hblock
  have hparts : (s.any (fun x : Int => min1 ≤ x && x ≤ max1) = false) ∨
      ((!(max2 < lo || hi < min2)) = false) := Bool.and_eq_false_iff.mp hblock
  have hfiltered : (rows.filter fun r => s.contains r.1 && lo ≤ r.2.1 && r.2.1 ≤ hi) = [] := by
    apply (List.filter_eq_nil_iff).mpr
    intro r hr hptrue
    have hb := hrows r hr
    simp only [Bool.and_eq_true_iff] at hptrue
    rcases hparts with hAny | hRange
    · have hc : s.contains r.1 = true := hptrue.1.1
      have hmem : r.1 ∈ s := List.contains_iff_mem.mp hc
      have hbad := (List.any_eq_false.mp hAny) r.1 hmem
      have hgood : (min1 ≤ r.1 && r.1 ≤ max1) = true := by
        simp [hb.1, hb.2.1]
      exact hbad hgood
    · have hdisj : (max2 < lo || hi < min2) = true := by
        by_cases hd : (max2 < lo || hi < min2) = true
        · exact hd
        · have hdF : (max2 < lo || hi < min2) = false := Bool.eq_false_of_ne_true hd
          simp [hdF] at hRange
      rcases Bool.or_eq_true_iff.mp hdisj with hlo | hhi
      · have hlt : max2 < lo := of_decide_eq_true hlo
        have hloLE : lo ≤ r.2.1 := of_decide_eq_true hptrue.1.2
        omega
      · have hlt : hi < min2 := of_decide_eq_true hhi
        have hhiGE : r.2.1 ≤ hi := of_decide_eq_true hptrue.2
        omega
  rw [hfiltered]
  simp [summarize, zero]

/-! ## SelfJoinBridgeBipartiteWeights -/

private def scale (n : Nat) (t : Totals) : Totals := ⟨n * t.cnt, n * t.nn, (n : Int) * t.sum⟩

private theorem scale_plus (n : Nat) (a b : Totals) : scale n (plus a b) = plus (scale n a) (scale n b) := by
  rcases a with ⟨ac, an, as⟩
  rcases b with ⟨bc, bn, bs⟩
  simp [scale, plus, Nat.mul_add, Int.mul_add]

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

private def bridgeStep (st : Std.HashMap Nat Totals × Std.HashMap Nat Nat) (p : PostRec) :
    Std.HashMap Nat Totals × Std.HashMap Nat Nat :=
  let (s, d) := st
  let (id, ok, sc) := p
  let s' := s.insert id (plus (s.getD id zero) (weighted 1 sc))
  let d' := if ok then d.insert id (d.getD id 0 + 1) else d
  (s', d')

private def postFold (posts : List PostRec) :=
  posts.foldl bridgeStep (({} : Std.HashMap Nat Totals), ({} : Std.HashMap Nat Nat))

private theorem bridge_fold_src_getD (posts : List PostRec)
    (s : Std.HashMap Nat Totals) (d : Std.HashMap Nat Nat) (u : Nat) :
    (posts.foldl bridgeStep (s, d)).1.getD u zero =
      plus (s.getD u zero) (summarize ((posts.filter fun p => p.1 == u).map fun p => p.2.2)) := by
  induction posts generalizing s d with
  | nil => simp [summarize, plus_zero_right]
  | cons p ps ih =>
      rcases p with ⟨id, ok, sc⟩
      simp only [List.foldl_cons, bridgeStep]
      by_cases hu : id == u
      · have hueq : id = u := beq_iff_eq.mp hu
        subst u
        rw [ih]
        simp [Std.HashMap.getD_insert, hu, summarize_cons, plus_assoc]
      · rw [ih]
        have hget : (s.insert id (plus (s.getD id zero) (weighted 1 sc))).getD u zero = s.getD u zero := by
          rw [Std.HashMap.getD_insert]
          simp [hu]
        rw [hget]
        simp [hu]

private theorem bridge_fold_dst_getD (posts : List PostRec)
    (s : Std.HashMap Nat Totals) (d : Std.HashMap Nat Nat) (v : Nat) :
    (posts.foldl bridgeStep (s, d)).2.getD v 0 =
      d.getD v 0 + (posts.filter fun p => p.1 == v && p.2.1).length := by
  induction posts generalizing s d with
  | nil => simp
  | cons p ps ih =>
      rcases p with ⟨id, ok, sc⟩
      simp only [List.foldl_cons, bridgeStep]
      by_cases hok : (ok : Bool) = true
      · rw [ite_eq_left hok]
        by_cases hv : id == v
        · have hveq : id = v := beq_iff_eq.mp hv
          subst v
          rw [ih]
          simp [Std.HashMap.getD_insert, hv, hok]
          omega
        · rw [ih]
          have hget : (d.insert id (d.getD id 0 + 1)).getD v 0 = d.getD v 0 := by
            rw [Std.HashMap.getD_insert]
            simp [hv]
          rw [hget]
          simp [hv]
      · have hokF : ok = false := Bool.eq_false_of_ne_true hok
        rw [ite_eq_right hok]
        rw [ih]
        simp [hokF]

private theorem src_getD (posts : List PostRec) (u : Nat) :
    (postFold posts).1.getD u zero =
      summarize ((posts.filter fun p => p.1 == u).map fun p => p.2.2) := by
  dsimp [postFold]
  simpa [plus_zero_left] using
    (bridge_fold_src_getD posts ({} : Std.HashMap Nat Totals) ({} : Std.HashMap Nat Nat) u)

private theorem dst_getD (posts : List PostRec) (v : Nat) :
    (postFold posts).2.getD v 0 =
      (posts.filter fun p => p.1 == v && p.2.1).length := by
  dsimp [postFold]
  simpa using
    (bridge_fold_dst_getD posts ({} : Std.HashMap Nat Totals) ({} : Std.HashMap Nat Nat) v)

private theorem flatMap_ext {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp [List.flatMap_cons, h a (by simp), ih (by intro x hx; exact h x (by simp [hx]))]

private theorem map_flatMap_vals (xs : List PostRec) (n : Nat) :
    xs.flatMap (fun a => List.replicate n a.2.2) =
      (xs.map fun a => a.2.2).flatMap (fun v => List.replicate n v) := by
  induction xs with
  | nil => rfl
  | cons a xs ih => simp [List.flatMap_cons, ih]

private theorem bridge_edge (posts : List PostRec) (u v : Nat) :
    summarize ((posts.filter fun p => p.1 == u).flatMap fun a =>
      (posts.filter fun p => p.1 == v && p.2.1).map fun _ => a.2.2) =
      ⟨((postFold posts).1.getD u zero).cnt * ((postFold posts).2.getD v 0),
       ((postFold posts).1.getD u zero).nn * ((postFold posts).2.getD v 0),
       ((postFold posts).1.getD u zero).sum * ((postFold posts).2.getD v 0)⟩ := by
  let m := posts.filter fun p => p.1 == v && p.2.1
  have hflat : (posts.filter fun p => p.1 == u).flatMap (fun a => m.map (fun _ => a.2.2)) =
      (posts.filter fun p => p.1 == u).flatMap (fun a => List.replicate m.length a.2.2) := by
    apply flatMap_ext
    intro a ha
    exact map_const_replicate m a.2.2
  rw [hflat, map_flatMap_vals, sum_flatMap_replicate]
  rw [src_getD]
  have hm : m.length = (postFold posts).2.getD v 0 := by
    rw [dst_getD]
  rw [← hm]
  rcases summarize ((posts.filter fun p => p.1 == u).map fun p => p.2.2) with ⟨c, n, sm⟩
  simp [scale, Nat.mul_comm, Int.mul_comm]

private theorem bridge_link_fold (posts : List PostRec) (links : List LinkRec) (lt : Nat)
    (st : Totals) :
    links.foldl (fun acc (u, v, t) =>
      if t != lt then acc else
        let a := (postFold posts).1.getD u zero
        let m := (postFold posts).2.getD v 0
        plus acc ⟨a.cnt * m, a.nn * m, a.sum * m⟩) st =
      plus st (summarize (links.flatMap fun (u, v, t) =>
        if t != lt then [] else (posts.filter fun p => p.1 == u).flatMap fun a =>
          (posts.filter fun p => p.1 == v && p.2.1).map fun _ => a.2.2)) := by
  induction links generalizing st with
  | nil => simp [summarize, plus_zero_right]
  | cons e es ih =>
      rcases e with ⟨u, v, t⟩
      simp only [List.foldl_cons, List.flatMap_cons]
      by_cases ht : (t == lt) = true
      · have hne : (t != lt) = false := by simpa using ht
        have hneNot : ¬ (t != lt) = true := by intro h; rw [h] at hne; contradiction
        rw [ite_eq_right hneNot, ih]
        rw [summarize_append, plus_assoc]
        congr 1
        simp [hne, bridge_edge]
      · have htF : (t == lt) = false := Bool.eq_false_of_ne_true ht
        have hne : (t != lt) = true := by simpa using htF
        have hneProp : (t != lt) = true := hne
        rw [ite_eq_left hneProp, ih]
        simp [hne]

theorem SelfJoinBridgeBipartiteWeights_sound :
    TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.SelfJoinBridgeBipartiteWeights.Goal := by
  intro posts links lt
  unfold TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.singleScanBridgeAgg
  unfold TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.refBridgeAgg
  change (links.foldl (fun acc (u, v, t) =>
      if t != lt then acc else
        let a := (postFold posts).1.getD u zero
        let m := (postFold posts).2.getD v 0
        plus acc ⟨a.cnt * m, a.nn * m, a.sum * m⟩) zero) =
    summarize (links.flatMap fun (u, v, t) =>
      if t != lt then [] else (posts.filter fun p => p.1 == u).flatMap fun a =>
        (posts.filter fun p => p.1 == v && p.2.1).map fun _ => a.2.2)
  simpa [plus_zero_left] using bridge_link_fold posts links lt zero

/-! ## DualRoleUniqueIdBitsetRoute -/

private theorem filter_eq_singleton_of_nodup {α : Type}
    (key : α → Nat) (xs : List α) (hnd : (xs.map key).Nodup) (k : Nat) (P : α → Bool) :
    xs.filter (fun x => key x == k && P x) =
      match xs.find? (fun x => key x == k) with
      | some x => if P x then [x] else []
      | none => [] := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hxnot : key x ∉ xs.map key := (List.nodup_cons.mp hnd).1
      have hxsnd : (xs.map key).Nodup := (List.nodup_cons.mp hnd).2
      rw [List.filter_cons, List.find?_cons]
      by_cases hk : key x == k
      · have hkx : key x = k := beq_iff_eq.mp hk
        have hfind : xs.find? (fun y => key y == k) = none := by
          rw [List.find?_eq_none]
          intro y hy hp
          have hkey : key y = k := beq_iff_eq.mp hp
          have hymem : key y ∈ xs.map key := by
            rw [List.mem_map]
            exact ⟨y, hy, rfl⟩
          apply hxnot
          simpa [hkey, hkx] using hymem
        have hfil : xs.filter (fun y => key y == k && P y) = [] := by
          rw [List.filter_eq_nil_iff]
          intro y hy hp
          have hk_y : (key y == k) = true := (Bool.and_eq_true_iff.mp hp).1
          have hkey : key y = k := beq_iff_eq.mp hk_y
          have hymem : key y ∈ xs.map key := by
            rw [List.mem_map]
            exact ⟨y, hy, rfl⟩
          apply hxnot
          simpa [hkey, hkx] using hymem
        rw [hk]
        simp [hk, hfind, hfil]
      · have hfind : xs.find? (fun y => key y == k) = xs.find? (fun y => key y == k) := rfl
        rw [ih hxsnd]
        simp [hk]

private def userBits (users : List UserRow) : Std.HashSet Nat × Std.HashSet Nat :=
  users.foldl (fun (a, b) u =>
    (if u.yrOk then a.insert u.id else a, if u.repOk then b.insert u.id else b)) (∅, ∅)

private theorem userBits_fst_aux (users : List UserRow) (a b : Std.HashSet Nat) (k : Nat) :
    (users.foldl (fun (a, b) u =>
      (if u.yrOk then a.insert u.id else a, if u.repOk then b.insert u.id else b)) (a, b)).1.contains k = true ↔
      a.contains k = true ∨ ∃ u ∈ users, u.id = k ∧ u.yrOk = true := by
  induction users generalizing a b with
  | nil => simp
  | cons u us ih =>
      rw [List.foldl_cons]
      dsimp
      by_cases hy : u.yrOk = true
      · rw [ite_eq_left hy, ih]
        have hc : (a.insert u.id).contains k = true ↔ a.contains k = true ∨ u.id = k := by
          rw [Std.HashSet.contains_insert, Bool.or_eq_true_iff, beq_iff_eq]
          grind
        simp [hc, hy]
        grind
      · rw [ite_eq_right hy, ih]
        have hyF : u.yrOk = false := Bool.eq_false_of_ne_true hy
        simp [hyF]

private theorem userBits_snd_aux (users : List UserRow) (a b : Std.HashSet Nat) (k : Nat) :
    (users.foldl (fun (a, b) u =>
      (if u.yrOk then a.insert u.id else a, if u.repOk then b.insert u.id else b)) (a, b)).2.contains k = true ↔
      b.contains k = true ∨ ∃ u ∈ users, u.id = k ∧ u.repOk = true := by
  induction users generalizing a b with
  | nil => simp
  | cons u us ih =>
      rw [List.foldl_cons]
      dsimp
      by_cases hr : u.repOk = true
      · rw [ite_eq_left hr, ih]
        have hc : (b.insert u.id).contains k = true ↔ b.contains k = true ∨ u.id = k := by
          rw [Std.HashSet.contains_insert, Bool.or_eq_true_iff, beq_iff_eq]
          grind
        simp [hc, hr]
        grind
      · rw [ite_eq_right hr, ih]
        have hrF : u.repOk = false := Bool.eq_false_of_ne_true hr
        simp [hrF]

private theorem userBits_fst_mem (users : List UserRow) (k : Nat) :
    (userBits users).1.contains k = true ↔ ∃ u ∈ users, u.id = k ∧ u.yrOk = true := by
  simpa [userBits] using userBits_fst_aux users ∅ ∅ k

private theorem userBits_snd_mem (users : List UserRow) (k : Nat) :
    (userBits users).2.contains k = true ↔ ∃ u ∈ users, u.id = k ∧ u.repOk = true := by
  simpa [userBits] using userBits_snd_aux users ∅ ∅ k

private def postStep (uOk : Std.HashSet Nat) (m : Std.HashMap Nat Nat) (p : Nat × Nat) : Std.HashMap Nat Nat :=
  let (pid, oid) := p
  if uOk.contains oid then m.insert pid oid else m

private theorem post_fold_preserve (uOk : Std.HashSet Nat) (posts : List (Nat × Nat))
    (m : Std.HashMap Nat Nat) (pid : Nat)
    (hnot : ∀ p ∈ posts, p.1 ≠ pid) :
    (posts.foldl (postStep uOk) m)[pid]? = m[pid]? := by
  induction posts generalizing m with
  | nil => rfl
  | cons p ps ih =>
      rcases p with ⟨pid0, oid0⟩
      have hpid0ne : pid0 ≠ pid := hnot (pid0, oid0) (by simp)
      have hrest : ∀ q ∈ ps, q.1 ≠ pid := by
        intro q hq
        exact hnot q (by simp [hq])
      simp only [List.foldl_cons, postStep]
      by_cases hc0 : uOk.contains oid0 = true
      · rw [ite_eq_left hc0]
        have hm' : (m.insert pid0 oid0)[pid]? = m[pid]? := by
          rw [Std.HashMap.getElem?_insert]
          have hbeq : (pid0 == pid) = false := by simpa using hpid0ne
          simp [hbeq]
        rw [ih (m := m.insert pid0 oid0) hrest, hm']
      · rw [ite_eq_right hc0, ih (m := m) hrest]

private theorem post_fold_get_aux (uOk : Std.HashSet Nat) (posts : List (Nat × Nat))
    (m : Std.HashMap Nat Nat) (pid : Nat) (hm : m[pid]? = none)
    (hnd : (posts.map Prod.fst).Nodup) :
    (posts.foldl (postStep uOk) m)[pid]? =
      (posts.find? fun p => p.1 == pid).bind fun p =>
        if uOk.contains p.2 then some p.2 else none := by
  induction posts generalizing m with
  | nil => simp [hm]
  | cons p ps ih =>
      rcases p with ⟨pid0, oid0⟩
      have hxnot : pid0 ∉ ps.map Prod.fst := (List.nodup_cons.mp hnd).1
      have hpsnd : (ps.map Prod.fst).Nodup := (List.nodup_cons.mp hnd).2
      simp only [List.foldl_cons, postStep, List.find?_cons]
      by_cases hc0 : uOk.contains oid0 = true
      · rw [ite_eq_left hc0]
        by_cases hpid : pid0 == pid
        · have hpideq : pid0 = pid := beq_iff_eq.mp hpid
          have hnotps : ∀ p ∈ ps, p.1 ≠ pid := by
            intro p hp hpeq
            have hpMem : p.1 ∈ ps.map Prod.fst := by
              rw [List.mem_map]
              exact ⟨p, hp, rfl⟩
            apply hxnot
            simpa [hpeq, hpideq] using hpMem
          rw [post_fold_preserve uOk ps (m.insert pid0 oid0) pid hnotps]
          have hmem0 : oid0 ∈ uOk := by simpa using hc0
          simp [Std.HashMap.getElem?_insert, hpideq, hpid, hmem0]
        · have hm' : (m.insert pid0 oid0)[pid]? = none := by
            rw [Std.HashMap.getElem?_insert]
            simp [hpid, hm]
          rw [ih (m := m.insert pid0 oid0) hm' hpsnd]
          simp [hpid, hc0]
      · rw [ite_eq_right hc0]
        by_cases hpid : pid0 == pid
        · have hpideq : pid0 = pid := beq_iff_eq.mp hpid
          have hfindps : ps.find? (fun p => p.1 == pid) = none := by
            rw [List.find?_eq_none]
            intro p hp hpp
            have hpeq : p.1 = pid := beq_iff_eq.mp hpp
            have hpMem : p.1 ∈ ps.map Prod.fst := by
              rw [List.mem_map]
              exact ⟨p, hp, rfl⟩
            apply hxnot
            simpa [hpeq, hpideq] using hpMem
          have hc0F : uOk.contains oid0 = false := Bool.eq_false_of_ne_true hc0
          rw [ih (m := m) hm hpsnd]
          have hnotmem0 : ¬ oid0 ∈ uOk := by simpa using hc0F
          simp [hpid, hfindps, hnotmem0]
        · rw [ih (m := m) hm hpsnd]
          simp [hpid, hc0]

private theorem post_fold_get (uOk : Std.HashSet Nat) (posts : List (Nat × Nat))
    (pid : Nat) (hnd : (posts.map Prod.fst).Nodup) :
    (posts.foldl (postStep uOk) {})[pid]? =
      (posts.find? fun p => p.1 == pid).bind fun p =>
        if uOk.contains p.2 then some p.2 else none := by
  exact post_fold_get_aux uOk posts {} pid (by simp) hnd


private theorem eq_of_pairwise_id (users : List UserRow) :
    users.Pairwise (fun a b => a.id ≠ b.id) → ∀ {a b : UserRow},
      a ∈ users → b ∈ users → a.id = b.id → a = b := by
  induction users with
  | nil =>
      intro hpw a b ha hb hid
      simp at ha
  | cons x xs ih =>
      intro hpw a b ha hb hid
      rw [List.pairwise_cons] at hpw
      simp only [List.mem_cons] at ha hb
      rcases ha with ha | ha
      · subst a
        rcases hb with hb | hb
        · subst b
          rfl
        · exact False.elim (hpw.1 b hb hid)
      · rcases hb with hb | hb
        · subst b
          exact False.elim (hpw.1 a ha hid.symm)
        · exact ih hpw.2 ha hb hid

private theorem find_none_not_exists {α : Type}
    (key : α → Nat) (xs : List α) (k : Nat)
    (h : xs.find? (fun x => key x == k) = none) :
    ¬ ∃ x ∈ xs, key x = k := by
  rw [List.find?_eq_none] at h
  rintro ⟨x, hx, hxk⟩
  exact h x hx (by simpa [hxk])

private theorem rep_len_eq (users : List UserRow) (hndU : (users.map UserRow.id).Nodup) (uid : Nat) :
    (users.filter fun u => u.id == uid && u.repOk).length =
      if (userBits users).2.contains uid then 1 else 0 := by
  have hpw : users.Pairwise (fun a b => a.id ≠ b.id) := by
    exact (List.pairwise_map (f := UserRow.id) (R := fun a b : Nat => a ≠ b)).mp hndU
  rw [filter_eq_singleton_of_nodup UserRow.id users hndU uid UserRow.repOk]
  by_cases hf : users.find? (fun u => u.id == uid) = none
  · rw [hf]
    have hcF : (userBits users).2.contains uid = false := by
      apply Bool.eq_false_of_ne_true
      intro hcT
      rcases (userBits_snd_mem users uid).mp hcT with ⟨v, hv, hvid, hvr⟩
      exact (find_none_not_exists UserRow.id users uid hf) ⟨v, hv, hvid⟩
    simp [hcF]
  · cases hfs : users.find? (fun u => u.id == uid) with
    | none => exact (hf hfs).elim
    | some u =>
        have hfindSome : (fun u : UserRow => u.id == uid) u = true :=
          List.find?_some (p := fun u : UserRow => u.id == uid) hfs
        have huid : u.id = uid := beq_iff_eq.mp hfindSome
        have huMem : u ∈ users := List.mem_of_find?_eq_some hfs
        have hciff : (userBits users).2.contains uid = true ↔ u.repOk = true := by
          constructor
          · intro hcT
            rcases (userBits_snd_mem users uid).mp hcT with ⟨v, hv, hvid, hvr⟩
            have hvu : v = u := eq_of_pairwise_id users hpw hv huMem (by omega)
            simpa [hvu] using hvr
          · intro hur
            exact (userBits_snd_mem users uid).mpr ⟨u, huMem, huid, hur⟩
        by_cases hr : u.repOk = true
        · simp [hfs, hr, hciff.mpr hr]
        · have hrF : u.repOk = false := Bool.eq_false_of_ne_true hr
          have hcF : (userBits users).2.contains uid = false := by
            apply Bool.eq_false_of_ne_true
            intro hcT
            exact hr (hciff.mp hcT)
          simp [hfs, hrF, hcF]


private theorem comment_contrib (users : List UserRow) (posts : List (Nat × Nat))
    (hndU : (users.map UserRow.id).Nodup) (hndP : (posts.map Prod.fst).Nodup)
    (pid uid ownerId : Nat) :
    let uOk := (userBits users).1
    let u2Ok := (userBits users).2
    let postOwner := posts.foldl (postStep uOk) {}
    ((posts.filter fun p => p.1 == pid).flatMap fun p =>
        (users.filter fun u => u.id == p.2 && u.yrOk && u.id == ownerId).flatMap fun _ =>
          users.filter fun u => u.id == uid && u.repOk).length =
      if u2Ok.contains uid && postOwner[pid]? == some ownerId then 1 else 0 := by
  dsimp
  let uOk := (userBits users).1
  let u2Ok := (userBits users).2
  let postOwner := posts.foldl (postStep uOk) {}
  have hpostF := filter_eq_singleton_of_nodup Prod.fst posts hndP pid (fun _ : Nat × Nat => true)
  have hpostsPred : (fun p : Nat × Nat => p.1 == pid) =
      (fun p => p.1 == pid && true) := by
    funext p
    cases (p.1 == pid) <;> rfl
  rw [hpostsPred, hpostF]
  by_cases hpf : posts.find? (fun p : Nat × Nat => p.1 == pid) = none
  · rw [hpf]
    simp
    have hget : postOwner[pid]? =
        (posts.find? fun p => p.1 == pid).bind (fun p => if uOk.contains p.2 then some p.2 else none) := by
      dsimp [postOwner]
      exact post_fold_get uOk posts pid hndP
    rw [hpf] at hget
    simp only [Option.bind_none] at hget
    rw [hget]
    simp
  · cases hpfS : posts.find? (fun p : Nat × Nat => p.1 == pid) with
    | none => exact (hpf hpfS).elim
    | some p =>
        simp only [ite_true, List.flatMap_cons, List.flatMap_nil, List.append_nil, List.nil_append]
        have hpFst : p.1 = pid := beq_iff_eq.mp
          (List.find?_some (p := fun p : Nat × Nat => p.1 == pid) hpfS)
        have hpred : (fun u : UserRow => u.id == p.2 && u.yrOk && u.id == ownerId) =
            (fun u => u.id == p.2 && (u.yrOk && u.id == ownerId)) := by
          funext u
          cases u.yrOk <;> cases (u.id == p.2) <;> cases (u.id == ownerId) <;> rfl
        have hownF := filter_eq_singleton_of_nodup UserRow.id users hndU p.2
          (fun u => u.yrOk && u.id == ownerId)
        rw [hpred, hownF]
        have hget : postOwner[pid]? =
            (posts.find? fun p => p.1 == pid).bind (fun p => if uOk.contains p.2 then some p.2 else none) := by
          dsimp [postOwner]
          exact post_fold_get uOk posts pid hndP
        rw [hpfS] at hget
        by_cases hof : users.find? (fun u : UserRow => u.id == p.2) = none
        · have hcF : uOk.contains p.2 = false := by
            apply Bool.eq_false_of_ne_true
            intro hcT
            rcases (userBits_fst_mem users p.2).mp hcT with ⟨v, hv, hvid, hvr⟩
            exact (find_none_not_exists UserRow.id users p.2 hof) ⟨v, hv, hvid⟩
          have hcFmem : ¬ p.2 ∈ uOk := by simpa using hcF
          have hgetNone : postOwner[pid]? = none := by simpa [hcFmem] using hget
          rw [hgetNone]
          simp [hof]
        · cases hofS : users.find? (fun u : UserRow => u.id == p.2) with
          | none => exact (hof hofS).elim
          | some u =>
              have hfindSome : ((fun u : UserRow => u.id == p.2) u) = true :=
                List.find?_some (p := fun u : UserRow => u.id == p.2) hofS
              have huidp : u.id = p.2 := beq_iff_eq.mp hfindSome
              have huMem : u ∈ users := List.mem_of_find?_eq_some hofS
              have hpw : users.Pairwise (fun a b => a.id ≠ b.id) := by
                exact (List.pairwise_map (f := UserRow.id) (R := fun a b : Nat => a ≠ b)).mp hndU
              have hciff : uOk.contains p.2 = true ↔ u.yrOk = true := by
                dsimp [uOk]
                constructor
                · intro hcT
                  rcases (userBits_fst_mem users p.2).mp hcT with ⟨v, hv, hvid, hvr⟩
                  have hvu : v = u := eq_of_pairwise_id users hpw hv huMem (by omega)
                  simpa [hvu] using hvr
                · intro hur
                  exact (userBits_fst_mem users p.2).mpr ⟨u, huMem, huidp, hur⟩
              by_cases hyr : u.yrOk = true
              · by_cases hko : (u.id == ownerId) = true
                · have hcT : uOk.contains p.2 = true := hciff.mpr hyr
                  have hcTmem : p.2 ∈ uOk := by simpa using hcT
                  have hval : postOwner[pid]? = some p.2 := by simpa [hcTmem] using hget
                  have hrepEq : (users.filter fun u => u.id == uid && u.repOk).length =
                      if u2Ok.contains uid then 1 else 0 := by
                    change (users.filter fun u => u.id == uid && u.repOk).length =
                      if (userBits users).2.contains uid then 1 else 0
                    exact rep_len_eq users hndU uid
                  rw [hval]
                  have hpkeq : p.2 = ownerId := by
                    have hueq : u.id = ownerId := beq_iff_eq.mp hko
                    omega
                  simp [hofS, hyr, huidp, hpkeq, hrepEq, u2Ok]
                · have hkoF : (u.id == ownerId) = false := Bool.eq_false_of_ne_true hko
                  have hcT : uOk.contains p.2 = true := hciff.mpr hyr
                  have hcTmem : p.2 ∈ uOk := by simpa using hcT
                  have hval : postOwner[pid]? = some p.2 := by simpa [hcTmem] using hget
                  rw [hval]
                  have hpkoF : (p.2 == ownerId) = false := by simpa [huidp] using hkoF
                  have huidNe : u.id ≠ ownerId := by
                    intro h
                    have ht : (u.id == ownerId) = true := by simp [h]
                    rw [ht] at hkoF
                    contradiction
                  simp [hyr, hpkoF, huidNe, u2Ok]
              · have hyrF : u.yrOk = false := Bool.eq_false_of_ne_true hyr
                have hcF : uOk.contains p.2 = false := by
                  apply Bool.eq_false_of_ne_true
                  intro hcT
                  exact hyr (hciff.mp hcT)
                have hcFmem : ¬ p.2 ∈ uOk := by simpa using hcF
                have hgetNone : postOwner[pid]? = none := by simpa [hcFmem] using hget
                rw [hgetNone]
                simp [hyrF, u2Ok]


theorem DualRoleUniqueIdBitsetRoute_sound :
    TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.DualRoleUniqueIdBitsetRoute.Goal := by
  intro users posts comments ownerId hndU hndP
  unfold TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.dualBitsetCount
  unfold TinylambOpt.ResearchGoals.Wave9.AnalyticsRemaining.refDualUserCount
  let uOk : Std.HashSet Nat := (userBits users).1
  let u2Ok : Std.HashSet Nat := (userBits users).2
  let postOwner : Std.HashMap Nat Nat := posts.foldl (postStep uOk) {}
  change comments.countP (fun (pid, uid) => u2Ok.contains uid && postOwner[pid]? == some ownerId) =
    (comments.flatMap fun (pid, uid) =>
      (posts.filter fun p => p.1 == pid).flatMap fun p =>
        (users.filter fun u => u.id == p.2 && u.yrOk && u.id == ownerId).flatMap fun _ =>
          users.filter fun u2 => u2.id == uid && u2.repOk).length
  have hfold :
      (comments.flatMap fun (pid, uid) =>
        (posts.filter fun p => p.1 == pid).flatMap fun p =>
          (users.filter fun u => u.id == p.2 && u.yrOk && u.id == ownerId).flatMap fun _ =>
            users.filter fun u2 => u2.id == uid && u2.repOk).length =
      comments.countP (fun (pid, uid) => u2Ok.contains uid && postOwner[pid]? == some ownerId) := by
    induction comments with
    | nil => simp
    | cons c cs ih =>
        rcases c with ⟨pid, uid⟩
        have hc := comment_contrib users posts hndU hndP pid uid ownerId
        dsimp [uOk, u2Ok, postOwner] at hc ih ⊢
        simp [List.flatMap_cons, List.countP_cons, hc, ih, Nat.add_comm]
  exact hfold.symm
