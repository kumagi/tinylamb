import TinylambOpt.ResearchGoals.DuckDBGaps.Orders
import TinylambOpt.ResearchProofs.CloudBI.Common

set_option maxHeartbeats 0
set_option maxRecDepth 100000

namespace TinylambOpt.ResearchProofs.DuckDBGaps.Orders

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.DuckDBGaps.Orders
open TinylambOpt.ResearchProofs.CloudBI.Helpers

/-! ==== DirectionBuckets ==== -/

private theorem db_filterMap_ite {α β : Type} (rows : List α) (p : α → Bool) (v : α → β) :
    rows.filterMap (fun r => if p r then some (v r) else none) = (rows.filter p).map v := by
  induction rows with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : p r = true
      · simp [h, ih]
      · have hf : p r = false := Bool.eq_false_of_ne_true h
        simp [hf, ih]

private theorem db_row_eq (a b : String) (hab : a ≠ b) (s c : Option String) (y : Int) (v : Option Int)
    (flag : Bool) (year : Int) :
    (do
      let src ← side a b s
      let dst ← side a b c
      if src != dst && src == flag && y == year then some v else none)
    = if s == some (if flag then b else a) && c == some (if flag then a else b) && y == year
      then some v else none := by
  have hbane : b ≠ a := fun h => hab h.symm
  cases flag <;> simp only [Bool.false_eq_true, ↓reduceIte]
  all_goals
    rcases s with _ | s
    · simp [side]
    rcases c with _ | c
    · simp [side, hab, hbane]
    by_cases hsa : s = a
    · subst s
      by_cases hca : c = a
      · subst c
        simp [side, hab, hbane]
      · by_cases hcb : c = b
        · subst c
          simp [side, hab, hbane]
        · have hca_beq : (c == a) = false := beq_eq_false_iff_ne.mpr hca
          have hcb_beq : (c == b) = false := beq_eq_false_iff_ne.mpr hcb
          simp [side, hca_beq, hcb_beq]
    · by_cases hsb : s = b
      · subst s
        by_cases hca : c = a
        · subst c
          simp [side, hab, hbane]
        · by_cases hcb : c = b
          · subst c
            simp [side, hab, hbane]
          · have hca_beq : (c == a) = false := beq_eq_false_iff_ne.mpr hca
            have hcb_beq : (c == b) = false := beq_eq_false_iff_ne.mpr hcb
            simp [side, hca_beq, hcb_beq]
      · have hsa_beq : (s == a) = false := beq_eq_false_iff_ne.mpr hsa
        have hsb_beq : (s == b) = false := beq_eq_false_iff_ne.mpr hsb
        simp [side, hsa_beq, hsb_beq]


theorem DirectionBuckets_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Orders.DirectionBuckets.Goal := by
  intro a b rows flag year hab
  dsimp only
  let p : Shipment → Bool := fun r =>
    r.1 == some (if flag then b else a) && r.2.1 == some (if flag then a else b) && r.2.2.1 == year
  have hpoint : ∀ r : Shipment,
      (do
        let src ← side a b r.1
        let dst ← side a b r.2.1
        if src != dst && src == flag && r.2.2.1 == year then some r.2.2.2 else none)
      = if p r then some r.2.2.2 else none := by
    intro r
    rcases r with ⟨s,c,y,v⟩
    exact db_row_eq a b hab s c y v flag year
  have hf : rows.filterMap (fun r : Shipment =>
        let (s,c,y,v) := r
        do
          let src ← side a b s
          let dst ← side a b c
          if src != dst && src == flag && y == year then some v else none)
      = rows.filterMap (fun r => if p r then some r.2.2.2 else none) := by
    induction rows with
    | nil => rfl
    | cons r rs ih =>
        simp only [List.filterMap_cons]
        rw [hpoint r, ih]
  rw [hf, db_filterMap_ite]
  simp only [p, Prod.fst, Prod.snd]
  congr 2
  apply List.filter_congr
  intro r hr
  rcases r with ⟨s,c,y,v⟩
  cases flag <;> rfl



/-! ==== MarketShareWeights ==== -/

private theorem msw_summarize_uniform (v : Option Int) : ∀ (l : List α),
    summarize (l.map (fun _ => v)) = weighted l.length v
  | [] => by simp [summarize_nil, weighted, zero]
  | x :: xs => by
      rw [List.map_cons, summarize_cons, msw_summarize_uniform v xs, List.length_cons, weighted_add]

private theorem msw_countP_map_eq {α β : Type} [BEq α] [LawfulBEq α] (f : β → α) (p : α → Bool) :
    ∀ (l : List β), (l.map f).countP p = l.countP (fun x => p (f x))
  | [] => rfl
  | x :: xs => by
      rw [List.map_cons, List.countP_cons, List.countP_cons, msw_countP_map_eq f p xs]

private theorem msw_hist_getD_countP {α β : Type} [BEq α] [LawfulBEq α] [Hashable α] [LawfulHashable α]
    (f : β → α) (l : List β) (k : α) :
    (histogram (l.map f)).getD k 0 = l.countP (fun x => f x == k) := by
  rw [histogram_getD, List.count_eq_countP]
  exact msw_countP_map_eq f (fun x => x == k) l

private theorem msw_foldl_congr' {α β : Type} (f g : β → α → β) (xs : List α) (init : β)
    (h : ∀ (acc : β) x, f acc x = g acc x) : xs.foldl f init = xs.foldl g init := by
  induction xs generalizing init with
  | nil => rfl
  | cons x xs ih =>
      change xs.foldl f (f init x) = xs.foldl g (g init x)
      rw [h init x]
      exact ih (g init x)

private theorem msw_flatMap_map {α β γ : Type} (f : α → List β) (g : β → γ) :
    ∀ (xs : List α), (xs.flatMap f).map g = xs.flatMap (fun x => (f x).map g)
  | [] => rfl
  | x :: xs => by rw [List.flatMap_cons, List.map_append, msw_flatMap_map f g xs, List.flatMap_cons]

private theorem msw_row_fst (supplier : List (Nat × Option String)) (country : String)
    (k : Nat) (v : Option Int) :
    summarize ((supplier.filter (fun s => s.1 == k)).map (fun s => if s.2 == some country then v else some 0))
      = plus (weighted ((histogram ((supplier.filter (fun s => s.2 == some country)).map Prod.fst)).getD k 0) v)
             (weighted ((histogram (supplier.map Prod.fst)).getD k 0 -
                         (histogram ((supplier.filter (fun s => s.2 == some country)).map Prod.fst)).getD k 0) (some 0)) := by
  let A := supplier.filter (fun s => s.1 == k)
  let Q : Nat × Option String → Bool := fun s => s.2 == some country
  let fv : Nat × Option String → Option Int := fun s => if Q s then v else some 0
  change summarize (A.map fv) =
      plus (weighted ((histogram ((supplier.filter Q).map Prod.fst)).getD k 0) v)
           (weighted ((histogram (supplier.map Prod.fst)).getD k 0 -
                       (histogram ((supplier.filter Q).map Prod.fst)).getD k 0) (some 0))
  have hw : (histogram (supplier.map Prod.fst)).getD k 0 = A.length := by
    rw [msw_hist_getD_countP]
    exact List.countP_eq_length_filter
  have hp : (histogram ((supplier.filter Q).map Prod.fst)).getD k 0 = (A.filter Q).length := by
    rw [msw_hist_getD_countP]
    rw [List.countP_eq_length_filter, List.filter_filter, List.filter_filter]
    have hflt : List.filter (fun a => a.fst == k && Q a) supplier =
        List.filter (fun a => Q a && a.fst == k) supplier := by
      apply List.filter_congr
      intro a ha
      cases h1 : (a.fst == k) <;> cases h2 : Q a <;> rfl
    exact congrArg List.length hflt
  have hpart := filter_not_partition Q A
  have hmap := hpart.map fv
  have hcombine : summarize (A.map fv) =
      plus (summarize ((A.filter Q).map fv)) (summarize ((A.filter (fun x => !Q x)).map fv)) := by
    rw [← summarize_append, ← List.map_append]
    exact summarize_perm hmap.symm
  have hqmap : (A.filter Q).map fv = (A.filter Q).map (fun _ => v) := by
    apply List.map_congr_left
    intro s hs
    have hq : Q s = true := (List.mem_filter.mp hs).2
    simp [fv, hq]
  have hnqmap : (A.filter (fun x => !Q x)).map fv = (A.filter (fun x => !Q x)).map (fun _ => some 0) := by
    apply List.map_congr_left
    intro s hs
    have hqf : Q s = false := by
      have hnq : (!Q s) = true := (List.mem_filter.mp hs).2
      cases hq : Q s with
      | true => simp [hq] at hnq
      | false => rfl
    simp [fv, hqf]
  have hlen : (A.filter Q).length + (A.filter (fun x => !Q x)).length = A.length := by
    have := hpart.length_eq
    simpa using this
  rw [hcombine, hqmap, hnqmap, msw_summarize_uniform, msw_summarize_uniform, hp, hw]
  have hsub : A.length - (A.filter Q).length = (A.filter (fun x => !Q x)).length := by omega
  rw [hsub]

private theorem msw_row_snd (supplier : List (Nat × Option String)) (k : Nat) (v : Option Int) :
    summarize ((supplier.filter (fun s => s.1 == k)).map (fun _ => v))
      = weighted ((histogram (supplier.map Prod.fst)).getD k 0) v := by
  have hw : (histogram (supplier.map Prod.fst)).getD k 0 = (supplier.filter (fun s => s.1 == k)).length := by
    rw [msw_hist_getD_countP]
    exact List.countP_eq_length_filter
  rw [msw_summarize_uniform, hw]

private theorem msw_foldl_pair {α : Type} (f g : α → List (Option Int)) :
    ∀ (xs : List α) (n d : Totals),
    xs.foldl (fun (acc : Totals × Totals) x => (plus acc.1 (summarize (f x)), plus acc.2 (summarize (g x)))) (n, d)
      = (plus n (summarize (xs.flatMap f)), plus d (summarize (xs.flatMap g))) := by
  intro xs
  induction xs with
  | nil => intro n d; simp [summarize_nil, plus_zero, plus_zero']
  | cons x xs ih =>
      intro n d
      rw [List.foldl_cons, ih]
      simp only [List.flatMap_cons, summarize_append]
      simp [plus_assoc]


theorem MarketShareWeights_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Orders.MarketShareWeights.Goal := by
  intro sales supplier country
  dsimp only
  let allRows := histogram (supplier.map Prod.fst)
  let chosen := histogram ((supplier.filter (fun s => s.2 == some country)).map Prod.fst)
  let fstVals : Nat × Option Int → List (Option Int) := fun (k,v) =>
    (supplier.filter (fun s => s.1 == k)).map (fun s => if s.2 == some country then v else some 0)
  let sndVals : Nat × Option Int → List (Option Int) := fun (k,v) =>
    (supplier.filter (fun s => s.1 == k)).map (fun _ => v)
  let perRow : Nat × Option Int → List (Option Int × Option Int) := fun (k,v) =>
    (supplier.filter (fun s => s.1 == k)).map (fun s => (if s.2 == some country then v else some 0, v))
  have hf : ∀ (kr : Nat × Option Int), summarize (fstVals kr) =
      plus (weighted (chosen.getD kr.1 0) kr.2) (weighted (allRows.getD kr.1 0 - chosen.getD kr.1 0) (some 0)) := by
    intro kr
    rcases kr with ⟨k,v⟩
    exact msw_row_fst supplier country k v
  have hg : ∀ (kr : Nat × Option Int), summarize (sndVals kr) = weighted (allRows.getD kr.1 0) kr.2 := by
    intro kr
    rcases kr with ⟨k,v⟩
    exact msw_row_snd supplier k v
  have hrowf : ∀ kr, (perRow kr).map Prod.fst = fstVals kr := by
    intro kr; rcases kr with ⟨k,v⟩; rw [List.map_map]; rfl
  have hrows : ∀ kr, (perRow kr).map Prod.snd = sndVals kr := by
    intro kr; rcases kr with ⟨k,v⟩; rw [List.map_map]; rfl
  have hfold := msw_foldl_pair fstVals sndVals sales zero zero
  rw [show sales.foldl (fun (num,den) (k,v) =>
        let w := allRows.getD k 0
        let p := chosen.getD k 0
        (plus num (plus (weighted p v) (weighted (w-p) (some 0))),plus den (weighted w v))) (zero,zero)
      = sales.foldl (fun (acc : Totals × Totals) kr =>
          (plus acc.1 (summarize (fstVals kr)), plus acc.2 (summarize (sndVals kr)))) (zero, zero) by
        apply msw_foldl_congr'
        intro acc kr
        rcases kr with ⟨k,v⟩
        simp [hf, hg]]
  rw [hfold]
  change (plus zero (summarize (sales.flatMap fstVals)), plus zero (summarize (sales.flatMap sndVals))) =
    (summarize ((sales.flatMap perRow).map Prod.fst), summarize ((sales.flatMap perRow).map Prod.snd))
  apply Prod.ext
  · simp only [plus_zero']
    rw [msw_flatMap_map]
    simp [hrowf]
  · simp only [plus_zero']
    rw [msw_flatMap_map]
    simp [hrows]



/-! ==== YearRoutes ==== -/

private theorem yr_countP_map {α β : Type} [BEq α] [LawfulBEq α] (f : β → α) (p : α → Bool) :
    ∀ (l : List β), (l.map f).countP p = l.countP (fun x => p (f x))
  | [] => rfl
  | x :: xs => by rw [List.map_cons, List.countP_cons, List.countP_cons, yr_countP_map f p xs]

private theorem yr_hist_getD_countP {α β : Type} [BEq α] [LawfulBEq α] [Hashable α] [LawfulHashable α]
    (f : β → α) (l : List β) (k : α) :
    (histogram (l.map f)).getD k 0 = l.countP (fun x => f x == k) := by
  rw [histogram_getD, List.count_eq_countP]
  exact yr_countP_map f (fun x => x == k) l

private theorem yr_summarize_all_eq (v : Option Int) : ∀ (l : List (Option Int)),
    (∀ x ∈ l, x = v) → summarize l = weighted l.length v
  | [], _ => by cases v <;> simp [summarize_nil, weighted, zero]
  | x :: xs, h => by
      have hx : x = v := h x (by simp)
      have hxs : ∀ y ∈ xs, y = v := by intro y hy; exact h y (by simp [hy])
      rw [summarize_cons, yr_summarize_all_eq v xs hxs, hx, List.length_cons, ← weighted_add]

private theorem yr_foldl_total {α : Type} (f : α → List (Option Int)) :
    ∀ (xs : List α) (z : Totals),
    xs.foldl (fun acc x => plus acc (summarize (f x))) z = plus z (summarize (xs.flatMap f))
  | [], z => by simp [summarize_nil, plus_zero]
  | x :: xs, z => by
      rw [List.foldl_cons, yr_foldl_total f xs, List.flatMap_cons, summarize_append, plus_assoc]

private theorem yr_foldl_congr' {α β : Type} (f g : β → α → β) (xs : List α) (init : β)
    (h : ∀ (acc : β) x, f acc x = g acc x) : xs.foldl f init = xs.foldl g init := by
  induction xs generalizing init with
  | nil => rfl
  | cons x xs ih =>
      change xs.foldl f (f init x) = xs.foldl g (g init x)
      rw [h init x]
      exact ih (g init x)

private theorem yr_foldl_add {α : Type} (g : α → Nat) : ∀ (l : List α) (z : Nat),
    l.foldl (fun a x => a + g x) z = z + (l.map g).sum
  | [], z => by simp
  | x :: xs, z => by
      rw [List.foldl_cons, List.map_cons, List.sum_cons, yr_foldl_add g xs (z + g x)]
      omega

private theorem yr_routes_fold_general (orders : List (Nat × Nat × Int)) (customers : Std.HashMap Nat Nat)
    (m0 : Std.HashMap (Nat × Int) Nat) (id : Nat × Int) :
    (orders.foldl (fun m (o,c,y) => m.insert (o,y) (m.getD (o,y) 0 + customers.getD c 0)) m0).getD id 0
      = m0.getD id 0 +
        (orders.filter (fun r => (r.1, r.2.2) == id)).foldl (fun a r => a + customers.getD r.2.1 0) 0 := by
  induction orders generalizing m0 with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨o,c,y⟩
      simp only [List.foldl_cons]
      rw [ih]
      by_cases hk : ((o,y) == id) = true
      · have hkid : (o,y) = id := beq_iff_eq.mp hk
        rw [Std.HashMap.getD_insert]
        simp only [hk, ↓reduceIte]
        have hfil : List.filter (fun r : Nat × Nat × Int => (r.1, r.2.2) == id) ((o,c,y)::rs)
            = (o,c,y) :: List.filter (fun r => (r.1, r.2.2) == id) rs := by
          rw [List.filter_cons_of_pos (by simpa using hk)]
        rw [hfil, List.foldl_cons, hkid]
        repeat rw [yr_foldl_add]
        simp
        omega
      · rw [Std.HashMap.getD_insert]
        have hfil : List.filter (fun r : Nat × Nat × Int => (r.1, r.2.2) == id) ((o,c,y)::rs)
            = List.filter (fun r => (r.1, r.2.2) == id) rs := by
          rw [List.filter_cons_of_neg (by simpa using hk)]
        rw [hfil]
        simp [hk]


theorem YearRoutes_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Orders.YearRoutes.Goal := by
  intro orders qc lines year
  dsimp only
  let customers := histogram qc
  let routes := orders.foldl (fun (m : Std.HashMap (Nat × Int) Nat) (o,c,y) =>
    m.insert (o,y) (m.getD (o,y) 0 + customers.getD c 0)) {}
  let perLine : Nat × Option Int → List (Option Int) := fun (o,v) =>
    (orders.filter (fun (k,_,y) => o == k && y == year)).flatMap (fun (_,c,_) =>
      (qc.filter (· == c)).map (fun _ => v))
  have hroute : ∀ (o : Nat), routes.getD (o,year) 0 =
      (orders.filter (fun r => (r.1, r.2.2) == (o,year))).foldl (fun a r => a + customers.getD r.2.1 0) 0 := by
    intro o
    dsimp [routes]
    rw [yr_routes_fold_general]
    rw [Std.HashMap.getD_empty, Nat.zero_add]
  have hqc : ∀ c, (qc.filter (· == c)).length = customers.getD c 0 := by
    intro c
    dsimp [customers]
    have hh : (histogram qc).getD c 0 = qc.countP (fun x => x == c) := by
      simpa using yr_hist_getD_countP (fun x : Nat => x) qc c
    rw [hh]
    exact (List.countP_eq_length_filter (p := fun x => x == c) (l := qc)).symm
  have hlen : ∀ (o : Nat) (v : Option Int), (perLine (o,v)).length = routes.getD (o,year) 0 := by
    intro o v
    dsimp [perLine]
    rw [List.length_flatMap]
    rw [hroute]
    rw [yr_foldl_add]
    have hflt : List.filter (fun r : Nat × Nat × Int => (r.1, r.2.2) == (o,year)) orders =
        List.filter (fun r => r.1 == o && r.2.2 == year) orders := by
      apply List.filter_congr
      intro r hr
      rcases r with ⟨k,c,y⟩
      rfl
    rw [hflt]
    have hflt2 : List.filter (fun r : Nat × Nat × Int => o == r.1 && r.2.2 == year) orders =
        List.filter (fun r : Nat × Nat × Int => r.1 == o && r.2.2 == year) orders := by
      apply List.filter_congr
      intro r hr
      rcases r with ⟨k,c,y⟩
      have hc : (o == k) = (k == o) := by
        by_cases heq : o = k
        · subst k; rfl
        · have h1 : (o == k) = false := beq_eq_false_iff_ne.mpr heq
          have h2 : (k == o) = false := beq_eq_false_iff_ne.mpr (fun hk => heq hk.symm)
          rw [h1, h2]
      rw [hc]
    rw [hflt2]
    simp [hqc]
  have hmem : ∀ (o : Nat) (v : Option Int), ∀ x ∈ perLine (o,v), x = v := by
    intro o v x hx
    dsimp [perLine] at hx
    obtain ⟨r, hr, hxin⟩ := List.mem_flatMap.mp hx
    rcases r with ⟨k,c,y⟩
    obtain ⟨z, hz, hzv⟩ := List.mem_map.mp hxin
    exact hzv.symm
  have hsum : ∀ (o : Nat) (v : Option Int), summarize (perLine (o,v)) = weighted (routes.getD (o,year) 0) v := by
    intro o v
    rw [yr_summarize_all_eq v (perLine (o,v)) (hmem o v), hlen o v]
  have hfold := yr_foldl_total perLine lines zero
  rw [show lines.foldl (fun st (o,v) => plus st (weighted (routes.getD (o,year) 0) v)) zero
      = lines.foldl (fun st kr => plus st (summarize (perLine kr))) zero by
        apply yr_foldl_congr'
        intro acc kr
        rcases kr with ⟨o,v⟩
        rw [hsum o v]]
  rw [hfold]
  simp [perLine, plus_zero']



/-! ==== LateSlack ==== -/

private theorem ls_slack_pos (r : LineDates) :
    ((slack r).any (0 < ·)) = (r.2.1.any (fun cm => r.2.2.any (fun rcpt => cm < rcpt))) := by
  rcases r with ⟨o, c, rc⟩
  cases c <;> cases rc <;> simp [slack] <;> omega

private theorem ls_max_pos_bool (a b : Int) :
    (decide (0 < max a b) : Bool) = (decide (0 < a) || decide (0 < b)) := by
  by_cases ha : 0 < a <;> by_cases hb : 0 < b <;> simp [ha, hb] <;> omega

private theorem ls_latest_pos_general (lines : List LineDates) (m0 : Std.HashMap Nat Int) (k : Nat) :
    (((lines.foldl (fun (m : Std.HashMap Nat Int) r =>
      match slack r with
      | none => m
      | some d => m.insert r.1 (max (m.getD r.1 d) d)) m0)[k]?).any (0 < ·)) =
      (((m0[k]?).any (0 < ·)) || (lines.any (fun r => r.1 == k && (slack r).any (0 < ·)))) := by
  induction lines generalizing m0 with
  | nil => simp
  | cons r rs ih =>
      rw [List.foldl_cons]
      rcases r with ⟨o, c, rc⟩
      cases hsl : slack (o, c, rc) with
      | none =>
          simp [hsl, ih]
      | some d =>
          simp only [hsl]
          rw [ih]
          by_cases hok : o == k
          · have hokeq : o = k := beq_iff_eq.mp hok
            subst o
            rw [Std.HashMap.getElem?_insert]
            simp only [hok, ↓reduceIte]
            rw [Std.HashMap.getD_eq_getD_getElem?]
            cases hk : m0[k]? with
            | none =>
                simp [Option.getD, hk, hsl]
            | some v =>
                simp only [Option.getD, Option.any_some, Bool.true_or]
                rw [ls_max_pos_bool]
                simp [hsl, hok, Bool.or_assoc]
          · have hok' : (o == k) = false := Bool.eq_false_of_ne_true hok
            rw [Std.HashMap.getElem?_insert]
            simp only [hok', ↓reduceIte]
            simp [hsl, hok']


theorem LateSlack_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Orders.LateSlack.Goal := by
  intro lines orders
  let latest := lines.foldl (fun (m : Std.HashMap Nat Int) r =>
    match slack r with
    | none => m
    | some d => m.insert r.1 (max (m.getD r.1 d) d)) {}
  change (orders.filter (fun k => (latest[k]?).any (0 < ·)) =
    orders.filter (fun k => lines.any (fun (o,c,r) => o == k && c.any (fun x => r.any (x < ·)))))
  have hpos : ∀ k : Nat,
      ((latest[k]?).any (0 < ·)) = (lines.any (fun r => r.1 == k && (slack r).any (0 < ·))) := by
    intro k
    simpa [latest] using (ls_latest_pos_general lines ({} : Std.HashMap Nat Int) k)
  apply List.filter_congr
  intro k hk
  rw [hpos k]
  congr 1
  funext r
  rcases r with ⟨o, c, rc⟩
  rw [ls_slack_pos]



/-! ==== RetireWitness ==== -/

private theorem rw_hashset_nodup_toList (s : Std.HashSet Nat) : s.toList.Nodup := by
  unfold Std.HashSet.toList
  exact Std.HashMap.nodup_keys

private theorem rw_nodup_filter' {α : Type} (p : α → Bool) : ∀ (l : List α), l.Nodup → (l.filter p).Nodup := by
  intro l
  induction l with
  | nil => intro _; exact List.Pairwise.nil
  | cons x xs ih =>
      intro h
      have hn : x ∉ xs := (List.nodup_cons.mp h).1
      have hxs : xs.Nodup := (List.nodup_cons.mp h).2
      cases hp : p x with
      | true =>
          rw [List.filter_cons_of_pos hp]
          refine List.Pairwise.cons (fun b hb hxb => ?_) (ih hxs)
          exact hn (hxb.symm ▸ (List.mem_filter.mp hb).1)
      | false =>
          rw [List.filter_cons_of_neg (by simpa using hp)]
          exact ih hxs

private theorem rw_hashset_fold_contains (xs : List Nat) (s : Std.HashSet Nat) (k : Nat) :
    (xs.foldl (fun s j => s.insert j) s).contains k = (xs.contains k || s.contains k) := by
  induction xs generalizing s with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashSet.contains_insert]
      by_cases h : k = x
      · subst x; simp
      · have h1 : (x == k) = false := beq_eq_false_iff_ne.mpr (fun hx => h hx.symm)
        simp [h1, Bool.or_assoc]
        intro heq
        exfalso
        exact h heq

private theorem rw_pending_contains (orders : List (Nat × Nat)) (k : Nat) :
    ((orders.map Prod.fst).foldl (fun (s : Std.HashSet Nat) j => s.insert j) {}).contains k =
      (orders.map Prod.fst).contains k := by
  rw [rw_hashset_fold_contains]
  simp

private theorem rw_countP_map {α β : Type} [BEq α] [LawfulBEq α] (f : β → α) (p : α → Bool) :
    ∀ (l : List β), (l.map f).countP p = l.countP (fun x => p (f x))
  | [] => rfl
  | x :: xs => by rw [List.map_cons, List.countP_cons, List.countP_cons, rw_countP_map f p xs]

private theorem rw_hist_getD_countP {α β : Type} [BEq α] [LawfulBEq α] [Hashable α] [LawfulHashable α]
    (f : β → α) (l : List β) (k : α) :
    (histogram (l.map f)).getD k 0 = l.countP (fun x => f x == k) := by
  rw [histogram_getD, List.count_eq_countP, rw_countP_map]

private theorem rw_weight_eq (orders : List (Nat × Nat)) (priority k : Nat) :
    (histogram ((orders.filter (fun r => r.2 == priority)).map Prod.fst)).getD k 0 =
      orders.countP (fun r => r.1 == k && r.2 == priority) := by
  rw [rw_hist_getD_countP (f := Prod.fst)]
  rw [List.countP_eq_length_filter, List.filter_filter, List.countP_eq_length_filter]

private theorem rw_countP_cons_contains (orders : List (Nat × Nat)) (priority x : Nat) (ps : List Nat) :
    ¬ ps.contains x = true →
    orders.countP (fun r => r.2 == priority && (x :: ps).contains r.1) =
      orders.countP (fun r => r.2 == priority && ps.contains r.1) +
        orders.countP (fun r => r.1 == x && r.2 == priority) := by
  intro hnps
  have hpsxf : (ps.contains x) = false := Bool.eq_false_of_ne_true hnps
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter, List.countP_eq_length_filter]
  let p1 : Nat × Nat → Bool := fun r => r.2 == priority && ps.contains r.1
  let p2 : Nat × Nat → Bool := fun r => r.1 == x && r.2 == priority
  have hdis : ∀ r ∈ orders, (p1 r && p2 r) = false := by
    intro r hr
    rcases r with ⟨j,pj⟩
    dsimp [p1, p2]
    by_cases h1 : j == x
    · have he : j = x := beq_iff_eq.mp h1
      subst j
      rw [hpsxf]
      simp
    · have h1f : (j == x) = false := Bool.eq_false_of_ne_true h1
      simp [h1f]
  have hp := filter_disjoint_perm p1 p2 orders hdis
  have hlen := hp.length_eq
  have htarget : orders.filter (fun r => p1 r || p2 r) =
      orders.filter (fun r => r.2 == priority && (x :: ps).contains r.1) := by
    apply List.filter_congr
    intro r hr
    rcases r with ⟨j,pj⟩
    dsimp [p1, p2]
    by_cases h1 : j == x
    · have he : j = x := beq_iff_eq.mp h1
      subst j
      rw [hpsxf]
      simp
    · have h1f : (j == x) = false := Bool.eq_false_of_ne_true h1
      have hxf : (x == j) = false := by
        have hne : j ≠ x := beq_eq_false_iff_ne.mp h1f
        exact beq_eq_false_iff_ne.mpr (fun he => hne he.symm)
      have hne : j ≠ x := beq_eq_false_iff_ne.mp h1f
      simp [hne]
  rw [← htarget]
  simpa [p1, p2] using hlen.symm

private theorem rw_countP_cons_noop (orders : List (Nat × Nat)) (priority x : Nat) (ps : List Nat)
    (h : (orders.map Prod.fst).contains x = true → ps.contains x = true) :
    orders.countP (fun r => r.2 == priority && (x :: ps).contains r.1) =
      orders.countP (fun r => r.2 == priority && ps.contains r.1) := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_congr
  intro r hr
  rcases r with ⟨j,pj⟩
  by_cases hjx : j == x
  · have hjxe : j = x := beq_iff_eq.mp hjx
    subst j
    have hmem : ((orders.map Prod.fst).contains x) = true := by
      rw [List.contains_iff_mem]
      exact List.mem_map.mpr ⟨(x, pj), hr, rfl⟩
    have hpsx : ps.contains x = true := h hmem
    have hcons : (x :: ps).contains x = true := by simp
    rw [hcons, hpsx]
  · have hjxf : (j == x) = false := Bool.eq_false_of_ne_true hjx
    have hne : j ≠ x := beq_eq_false_iff_ne.mp hjxf
    simp [hne]

private theorem rw_reverse_contains (l : List Nat) (k : Nat) : l.reverse.contains k = l.contains k := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      rw [List.reverse_cons]
      simp [ih, Bool.or_comm]


theorem RetireWitness_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Orders.RetireWitness.Goal := by
  intro orders lateKeys priority
  dsimp only
  let pending := (orders.map Prod.fst).foldl (fun (s : Std.HashSet Nat) k => s.insert k) {}
  let weights := histogram ((orders.filter (fun r => r.2 == priority)).map Prod.fst)
  let step : Std.HashSet Nat × Nat → Nat → Std.HashSet Nat × Nat := fun (todo,n) k =>
    if todo.contains k then (todo.erase k, n + weights.getD k 0) else (todo,n)
  have hw : ∀ k, weights.getD k 0 = orders.countP (fun r => r.1 == k && r.2 == priority) := by
    intro k; exact rw_weight_eq orders priority k
  have hpcon : ∀ k, pending.contains k = (orders.map Prod.fst).contains k := by
    intro k; exact rw_pending_contains orders k
  have inv : ∀ (ls : List Nat) (ps : List Nat) (todo : Std.HashSet Nat) (n : Nat),
      (∀ k, todo.contains k = (pending.contains k && !ps.contains k)) →
      n = orders.countP (fun r => r.2 == priority && ps.contains r.1) →
      let (left,n') := ls.foldl step (todo,n)
      (∀ k, left.contains k = (pending.contains k && !(ps ++ ls).contains k)) ∧
      n' = orders.countP (fun r => r.2 == priority && (ps ++ ls).contains r.1) := by
    intro ls
    induction ls with
    | nil =>
        intro ps todo n hm hn
        simp [hm, hn]
    | cons x xs ih =>
        intro ps todo n hm hn
        simp only [List.foldl_cons]
        by_cases htx : todo.contains x = true
        · have hmx := hm x
          rw [htx] at hmx
          have hpx : pending.contains x = true := by
            cases hp : pending.contains x with
            | true => rfl
            | false =>
                rw [hp] at hmx
                simp at hmx
          have hnps : ¬ ps.contains x = true := by
            intro hps
            rw [hps] at hmx
            simp at hmx
          dsimp [step]
          rw [ite_eq_left htx]
          have hm1 : ∀ k, (todo.erase x).contains k = (pending.contains k && !(x :: ps).contains k) := by
            intro k
            rw [Std.HashSet.contains_erase]
            have hmk := hm k
            by_cases hkx : x == k
            · have hxkeq : x = k := beq_iff_eq.mp hkx
              subst k
              simp [hpx]
            · have hkxf : (x == k) = false := Bool.eq_false_of_ne_true hkx
              have hkxf' : (k == x) = false := by
                have hne : x ≠ k := beq_eq_false_iff_ne.mp hkxf
                exact beq_eq_false_iff_ne.mpr (fun he => hne he.symm)
              rw [hmk]
              have hne : k ≠ x := by
                intro he
                exact hkx (by rw [he]; exact beq_self_eq_true x)
              simp [hkx, hne, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm]
          have hn1 : n + weights.getD x 0 = orders.countP (fun r => r.2 == priority && (x :: ps).contains r.1) := by
            rw [hw x, hn]
            symm
            exact rw_countP_cons_contains orders priority x ps hnps
          have ih' := ih (x::ps) (todo.erase x) (n + weights.getD x 0) hm1 hn1
          simpa [step, List.cons_append, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm,
                 Bool.or_assoc, Bool.or_left_comm, Bool.or_comm] using ih'
        · have htxf : todo.contains x = false := Bool.eq_false_of_ne_true htx
          dsimp [step]
          rw [ite_eq_right htx]
          have hm1 : ∀ k, todo.contains k = (pending.contains k && !(x :: ps).contains k) := by
            intro k
            have hmk := hm k
            by_cases hkx : x == k
            · have hxkeq : x = k := beq_iff_eq.mp hkx
              subst k
              simpa using htxf
            · rw [hmk]
              have hne : k ≠ x := by
                intro he
                exact hkx (by rw [he]; exact beq_self_eq_true x)
              simp [hkx, hne, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm]
          have hn1 : n = orders.countP (fun r => r.2 == priority && (x :: ps).contains r.1) := by
            rw [hn]
            symm
            apply rw_countP_cons_noop
            intro hxmem
            have hpx : (orders.map Prod.fst).contains x = true := hxmem
            have hmx := hm x
            rw [hpcon x, hpx, htxf] at hmx
            cases hs : ps.contains x with
            | true => rfl
            | false =>
                rw [hs] at hmx
                simp at hmx
          have ih' := ih (x::ps) todo n hm1 hn1
          simpa [step, List.cons_append, Bool.and_assoc, Bool.and_left_comm, Bool.and_comm,
                 Bool.or_assoc, Bool.or_left_comm, Bool.or_comm] using ih'
  have hfin := inv lateKeys [] pending 0 (by intro k; simp) (by simp)
  dsimp at hfin
  let left := (lateKeys.foldl step (pending,0)).1
  let n' := (lateKeys.foldl step (pending,0)).2
  have hmemfinal : ∀ k, left.contains k = (pending.contains k && !lateKeys.contains k) := by
    intro k
    simpa [left] using hfin.1 k
  have hnfinal : n' = orders.countP (fun r => r.2 == priority && lateKeys.contains r.1) := by
    dsimp [n']
    simpa using hfin.2
  constructor
  · exact hnfinal
  · rw [← Std.HashSet.length_toList (m := left)]
    have hperm : left.toList.Perm ((orders.map Prod.fst).eraseDups.filter (fun k => !lateKeys.contains k)) := by
      apply perm_of_nodup_mem (rw_hashset_nodup_toList left)
      · exact rw_nodup_filter' (fun k => !lateKeys.contains k) _ (nodup_eraseDups _)
      · intro k
        rw [Std.HashSet.mem_toList, Std.HashSet.mem_iff_contains, hmemfinal k, hpcon k]
        simp [List.mem_eraseDups]
    exact hperm.length_eq



/-! ==== RebucketOrders ==== -/

private theorem rb_hist_getD_countP {α β : Type} [BEq α] [LawfulBEq α] [Hashable α] [LawfulHashable α]
    (f : β → α) (l : List β) (k : α) :
    (histogram (l.map f)).getD k 0 = l.countP (fun x => f x == k) := by
  rw [histogram_getD, List.count_eq_countP]
  induction l with
  | nil => rfl
  | cons x xs ih => rw [List.map_cons, List.countP_cons, List.countP_cons, ih]

private theorem rb_countP_partition (l : List (Nat × Bool)) (c : Nat) :
    l.countP (fun r => r.1 == c) =
      l.countP (fun r => r.1 == c && r.2) + l.countP (fun r => r.1 == c && !r.2) := by
  induction l with
  | nil => simp
  | cons r rs ih =>
      rw [List.countP_cons, List.countP_cons, List.countP_cons]
      rcases r with ⟨j,b⟩
      by_cases hj : j == c <;> by_cases hb : b <;> simp [hj, hb] <;> rw [ih] <;> omega

private theorem rb_retained_eq (allOrders : List (Nat × Bool)) (c : Nat) :
    allOrders.countP (fun r => r.1 == c && !r.2) =
      (histogram (allOrders.map Prod.fst)).getD c 0 -
        (histogram ((allOrders.filter Prod.snd).map Prod.fst)).getD c 0 := by
  rw [rb_hist_getD_countP (f := Prod.fst) allOrders c]
  rw [rb_hist_getD_countP (f := Prod.fst) (allOrders.filter Prod.snd) c]
  have hpart := rb_countP_partition allOrders c
  have hbad : (allOrders.filter Prod.snd).countP (fun r => r.1 == c) =
      allOrders.countP (fun r => r.1 == c && r.2) := by
    simp [List.countP_filter]
  rw [hbad] at *
  omega

private def rb_stepR (cw full : Std.HashMap Nat Nat) (bucket : Nat) (n : Int) (p : Nat × Nat) : Int :=
  let c := p.1
  let d := p.2
  let cnt := full.getD c 0
  let w : Int := cw.getD c 0
  n - (if cnt == bucket then w else 0) + (if cnt - d == bucket then w else 0)

private theorem rb_fold_shift (bad : List (Nat × Nat)) (cw1 cw2 : Std.HashMap Nat Nat) (full : Std.HashMap Nat Nat)
    (bucket : Nat) (h : ∀ p ∈ bad, cw1.getD p.1 0 = cw2.getD p.1 0) :
    ∀ (n delta : Int), bad.foldl (rb_stepR cw1 full bucket) (n + delta) =
      bad.foldl (rb_stepR cw2 full bucket) n + delta := by
  induction bad with
  | nil => intro n delta; rfl
  | cons p tl ih =>
      intro n delta
      rw [List.foldl_cons, List.foldl_cons]
      have hp := h p (by simp)
      have hstep : rb_stepR cw1 full bucket (n + delta) p = rb_stepR cw2 full bucket n p + delta := by
        unfold rb_stepR; dsimp only; rw [hp]; omega
      rw [hstep]
      have ht : ∀ q ∈ tl, cw1.getD q.1 0 = cw2.getD q.1 0 := by
        intro q hq; exact h q (by simp [hq])
      exact ih ht (rb_stepR cw2 full bucket n p) delta

private theorem rb_fold_cw_update (bad : List (Nat × Nat))
    (hnd : bad.Pairwise (fun p q => (p.1 == q.1) = false))
    (cw : Std.HashMap Nat Nat) (c : Nat) (old : Int)
    (full : Std.HashMap Nat Nat) (bucket : Nat) :
    let cw' := cw.insert c (cw.getD c 0 + 1)
    bad.foldl (rb_stepR cw' full bucket) old =
      bad.foldl (rb_stepR cw full bucket) old
      + (match bad.find? (fun p => p.1 == c) with
         | none => 0
         | some (_, d) => -(if full.getD c 0 == bucket then (1:Int) else 0) +
                          (if full.getD c 0 - d == bucket then (1:Int) else 0)) := by
  induction bad generalizing old with
  | nil => simp
  | cons hd tl ih =>
      rcases hd with ⟨k,d⟩
      have hp := List.pairwise_cons.mp hnd
      have hno : ∀ p ∈ tl, (k == p.1) = false := hp.1
      have hndtl : tl.Pairwise (fun p q => (p.1 == q.1) = false) := hp.2
      dsimp only
      by_cases hkc : k == c
      · have hkeq : k = c := beq_iff_eq.mp hkc
        subst k
        have hnotc : ∀ p ∈ tl, (c == p.1) = false := by intro p hp1; exact hno p hp1
        have ih' := ih hndtl old
        simp only [List.find?_cons, hkc, ↓reduceIte]
        rw [List.foldl_cons, List.foldl_cons]
        have htail : ∀ p ∈ tl,
            (cw.insert c (cw.getD c 0 + 1)).getD p.1 0 = cw.getD p.1 0 := by
          intro p hp1; rw [Std.HashMap.getD_insert]; simp [hnotc p hp1]
        have hstep : rb_stepR (cw.insert c (cw.getD c 0 + 1)) full bucket old (c,d) =
            rb_stepR cw full bucket old (c,d) +
              (-(if full.getD c 0 == bucket then (1:Int) else 0) +
               (if full.getD c 0 - d == bucket then (1:Int) else 0)) := by
          unfold rb_stepR; dsimp only; rw [Std.HashMap.getD_insert]; simp; omega
        rw [hstep]
        rw [rb_fold_shift tl (cw.insert c (cw.getD c 0 + 1)) cw full bucket htail]
      · have hkcf : (k == c) = false := Bool.eq_false_of_ne_true hkc
        have hkc' : (c == k) = false := by
          have hne : k ≠ c := beq_eq_false_iff_ne.mp hkcf
          exact beq_eq_false_iff_ne.mpr (fun hh => hne hh.symm)
        have ih' := ih hndtl (rb_stepR cw full bucket old (k,d))
        simp only [List.find?_cons, hkc, ↓reduceIte]
        rw [List.foldl_cons, List.foldl_cons]
        have hhead : rb_stepR (cw.insert c (cw.getD c 0 + 1)) full bucket old (k,d) =
            rb_stepR cw full bucket old (k,d) := by
          unfold rb_stepR; dsimp only; rw [Std.HashMap.getD_insert]; simp [hkc']
        rw [hhead]
        rw [ih']


theorem RebucketOrders_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Orders.RebucketOrders.Goal := by
  intro customers allOrders bucket
  dsimp only
  let full := histogram (allOrders.map Prod.fst)
  let bad := histogram ((allOrders.filter Prod.snd).map Prod.fst)
  let cw := histogram customers
  have hndbad : bad.toList.Pairwise (fun p q => (p.1 == q.1) = false) := Std.HashMap.distinct_keys_toList
  have hmain : ∀ (cs : List Nat),
      (let cw0 := histogram cs
       let old0 := cs.countP (fun c => full.getD c 0 == bucket)
       bad.toList.foldl (rb_stepR cw0 full bucket) (old0 : Int) =
         (cs.countP (fun c => allOrders.countP (fun r => r.1 == c && !r.2) == bucket) : Int)) := by
    intro cs
    induction cs with
    | nil =>
        dsimp
        have hstep0 : ∀ (n : Int) (p : Nat × Nat), rb_stepR (histogram ([] : List Nat)) full bucket n p = n := by
          intro n p
          unfold rb_stepR
          dsimp [histogram]
          simp [Std.HashMap.getD_empty]
        induction bad.toList with
        | nil => rfl
        | cons p ps ih => rw [List.foldl_cons, hstep0, ih]
    | cons c cs ih =>
        dsimp only
        let cw0 := histogram cs
        let old0 := cs.countP (fun c => full.getD c 0 == bucket)
        have hold : (c :: cs).countP (fun x => full.getD x 0 == bucket) =
            old0 + (if full.getD c 0 == bucket then 1 else 0) := by
          dsimp [old0]
          simp [List.countP_cons]
        have hret : (c :: cs).countP (fun x => allOrders.countP (fun r => r.1 == x && !r.2) == bucket) =
            cs.countP (fun x => allOrders.countP (fun r => r.1 == x && !r.2) == bucket) +
              (if allOrders.countP (fun r => r.1 == c && !r.2) == bucket then 1 else 0) := by
          simp [List.countP_cons]
        have hshift := rb_fold_shift bad.toList (cw0.insert c (cw0.getD c 0 + 1))
            (cw0.insert c (cw0.getD c 0 + 1)) full bucket (by intro p hp; rfl)
            (old0 : Int) (if full.getD c 0 == bucket then (1:Int) else 0)
        rw [show ((c :: cs).countP (fun x => full.getD x 0 == bucket) : Int) =
            (old0 : Int) + (if full.getD c 0 == bucket then (1:Int) else 0) by
              rw [hold]; split <;> simp]
        have hget_eq : ∀ k, (histogram (c :: cs)).getD k 0 =
            (cw0.insert c (cw0.getD c 0 + 1)).getD k 0 := by
          intro k
          rw [Std.HashMap.getD_insert]
          rw [histogram_getD, histogram_getD]
          dsimp [cw0]
          rw [List.count_cons]
          cases hck : c == k with
          | true =>
              have he : c = k := beq_iff_eq.mp hck
              subst k
              simp
          | false =>
              rw [← histogram_getD]
              simp [hck]
        have hchange : bad.toList.foldl (rb_stepR (histogram (c :: cs)) full bucket)
              ((old0 : Int) + (if full.getD c 0 == bucket then (1:Int) else 0)) =
            bad.toList.foldl (rb_stepR (cw0.insert c (cw0.getD c 0 + 1)) full bucket)
              ((old0 : Int) + (if full.getD c 0 == bucket then (1:Int) else 0)) := by
          simpa using (rb_fold_shift bad.toList (histogram (c :: cs))
            (cw0.insert c (cw0.getD c 0 + 1)) full bucket
            (by intro p hp; exact hget_eq p.1)
            ((old0 : Int) + (if full.getD c 0 == bucket then (1:Int) else 0)) 0)
        rw [hchange]
        rw [hshift]
        have hupd := rb_fold_cw_update bad.toList hndbad cw0 c (old0 : Int) full bucket
        dsimp only at hupd
        rw [hupd]
        rw [hret]
        dsimp only at ih
        rw [ih]
        cases hfind : bad.toList.find? (fun p => p.1 == c) with
        | none =>
            have hno : ∀ p ∈ bad.toList, (p.1 == c) = false := by
              intro p hp
              have hf := (List.find?_eq_none.mp hfind) p hp
              exact Bool.eq_false_of_ne_true hf
            have hnotmem : ¬ c ∈ bad := by
              intro hc
              have hg : bad[c]? = some (bad.getD c 0) := Std.HashMap.getElem?_eq_some_getD hc
              have hp : (c, bad.getD c 0) ∈ bad.toList :=
                (Std.HashMap.mem_toList_iff_getElem?_eq_some (m := bad)).mpr hg
              have hbe : ((c, bad.getD c 0).1 == c) = false := hno (c, bad.getD c 0) hp
              exact (beq_eq_false_iff_ne.mp hbe) rfl
            have hzero : bad.getD c 0 = 0 := by
              rw [Std.HashMap.getD_eq_getD_getElem?, Std.HashMap.getElem?_eq_none hnotmem]
              rfl
            have hre : allOrders.countP (fun r => r.1 == c && !r.2) =
                (histogram (allOrders.map Prod.fst)).getD c 0 := by
              rw [rb_retained_eq, hzero, Nat.sub_zero]
            dsimp [full]
            by_cases hb : (histogram (allOrders.map Prod.fst)).getD c 0 == bucket <;> simp [hfind, hre, hb]
        | some p =>
            have hp : p ∈ bad.toList := List.mem_of_find?_eq_some hfind
            have hpeq : (p.1 == c) = true := by
              simpa using (List.find?_some (a := p) (l := bad.toList)
                (p := fun q : Nat × Nat => q.1 == c) hfind)
            have heq : p.1 = c := beq_iff_eq.mp hpeq
            have hval : bad.getD c 0 = p.2 := by
              subst heq
              have hget : bad[p.1]? = some p.2 :=
                (Std.HashMap.mem_toList_iff_getElem?_eq_some (m := bad)).mp hp
              rw [Std.HashMap.getD_eq_getD_getElem?, hget]
              rfl
            have hre : allOrders.countP (fun r => r.1 == c && !r.2) =
                (histogram (allOrders.map Prod.fst)).getD c 0 - p.2 := by
              rw [rb_retained_eq, hval]
            dsimp [full]
            by_cases h1 : (histogram (allOrders.map Prod.fst)).getD c 0 == bucket <;>
              by_cases h2 : (histogram (allOrders.map Prod.fst)).getD c 0 - p.snd == bucket <;>
                simp [hfind, hre, h1, h2] <;> omega

  change bad.toList.foldl (rb_stepR cw full bucket)
      ((customers.countP (fun c => full.getD c 0 == bucket) : Nat) : Int) =
    (customers.countP (fun c => allOrders.countP (fun r => r.1 == c && !r.2) == bucket) : Int)
  exact hmain customers



/-! ==== RevenueRuns ==== -/

private theorem rr_beq_sym {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  by_cases h : a = b
  · subst b; rfl
  · have h1 : (a == b) = false := beq_eq_false_iff_ne.mpr h
    have h2 : (b == a) = false := beq_eq_false_iff_ne.mpr (fun hb => h hb.symm)
    rw [h1, h2]

private def rr_srStep (acc : List (Nat × Totals)) (x : Nat × Option Int) : List (Nat × Totals) :=
  match acc with
  | [] => [(x.1, weighted 1 x.2)]
  | (g,t)::done_ => if x.1 == g then (g, plus t (weighted 1 x.2))::done_ else (x.1, weighted 1 x.2)::acc

private def rr_extend (runs : List (Nat × Totals)) (k : Nat) (v : Option Int) : List (Nat × Totals) :=
  match runs with
  | [] => [(k, weighted 1 v)]
  | (g,t)::ds => if k == g then (g, plus t (weighted 1 v))::ds else (k, weighted 1 v)::runs

private theorem rr_streamRevenue_eq : ∀ (rows : List (Nat × Option Int)), streamRevenue rows = rows.foldl rr_srStep [] := by intro rows; rfl

private theorem rr_streamRevenue_snoc (rows : List (Nat × Option Int)) (k : Nat) (v : Option Int) :
    streamRevenue (rows ++ [(k,v)]) = rr_extend (streamRevenue rows) k v := by
  rw [rr_streamRevenue_eq, rr_streamRevenue_eq, List.foldl_append]
  simp only [List.foldl_cons, List.foldl_nil]
  cases h : rows.foldl rr_srStep [] with
  | nil => rfl
  | cons g ds => rfl

private theorem rr_sr_head_key (rows : List (Nat × Option Int)) (x : Nat × Option Int) :
    ((streamRevenue (rows ++ [x])).head?.map Prod.fst = some x.1) := by
  rw [rr_streamRevenue_snoc]
  cases h : streamRevenue rows with
  | nil => simp [rr_extend]
  | cons g ds =>
      by_cases hk : x.1 == g.1
      · have he : x.1 = g.1 := beq_iff_eq.mp hk
        rw [he]
        simp [rr_extend]
      · simp [rr_extend, hk]

private theorem rr_exists_init_last {α : Type} : ∀ (l : List α), l ≠ [] → ∃ init last, l = init ++ [last]
  | [], h => by contradiction
  | [x], h => ⟨[], x, rfl⟩
  | x :: y :: ys, h => by
      obtain ⟨init, last, hdec⟩ := rr_exists_init_last (y :: ys) (by simp)
      exact ⟨x :: init, last, by rw [hdec]; rfl⟩

private theorem rr_sr_fold_keys : ∀ (rest src : List (Nat × Option Int)) (acc : List (Nat × Totals)),
    (∀ p ∈ acc, ∃ x ∈ src, x.1 = p.1) →
    ∀ p ∈ rest.foldl rr_srStep acc, ∃ x ∈ src ++ rest, x.1 = p.1 := by
  intro rest
  induction rest with
  | nil => intro src acc h p hp; simpa using h p hp
  | cons r rs ih =>
      intro src acc h p hp
      rw [List.foldl_cons] at hp
      have hacc : ∀ q ∈ rr_srStep acc r, ∃ x ∈ src ++ [r], x.1 = q.1 := by
        intro q hq
        unfold rr_srStep at hq
        cases ha : acc with
        | nil =>
            simp [ha] at hq
            subst q
            exact ⟨r, by simp, rfl⟩
        | cons g ds =>
            by_cases hk : r.1 == g.1
            · simp [ha, hk] at hq
              rcases hq with hq | hq
              · subst q
                exact ⟨r, by simp, beq_iff_eq.mp hk⟩
              · obtain ⟨x, hx, hxk⟩ := h q (by simp [ha, hq])
                exact ⟨x, by simp [hx], hxk⟩
            · simp [ha, hk] at hq
              rcases hq with hq | hq
              · subst q
                exact ⟨r, by simp, rfl⟩
              · obtain ⟨x, hx, hxk⟩ := h q (by simp [ha, hq])
                exact ⟨x, by simp [hx], hxk⟩
      have hres := ih (src ++ [r]) (rr_srStep acc r) hacc p hp
      simpa [List.append_assoc] using hres

private theorem rr_sr_keys_mem (rows : List (Nat × Option Int)) (j : Nat) (t : Totals) :
    (j,t) ∈ streamRevenue rows → ∃ x ∈ rows, x.1 = j := by
  intro h
  rw [rr_streamRevenue_eq] at h
  simpa using (rr_sr_fold_keys rows [] [] (by intro p hp; cases hp) (j,t) h)

private theorem rr_pairwise_le_last (l : List (Nat × Option Int))
    (h : l.Pairwise (fun a b => a.1 ≤ b.1)) :
    ∀ x ∈ l, x.1 ≤ (l.getLast?.map Prod.fst).getD x.1 := by
  intro x hx
  by_cases hem : l = []
  · subst hem; cases hx
  · obtain ⟨init, last, rfl⟩ := rr_exists_init_last l hem
    have hc := (List.pairwise_append.mp h)
    have hx' : x ∈ init ++ [last] := hx
    simp only [List.getLast?_append, List.getLast?_singleton, Option.or_some]
    rw [List.mem_append] at hx'
    cases hx' with
    | inl hi => exact hc.2.2 x hi last (by simp)
    | inr hl => simp at hl; subst x; exact Nat.le_refl _

private theorem rr_sr_all_keys_le (rows : List (Nat × Option Int))
    (h : rows.Pairwise (fun a b => a.1 ≤ b.1)) (hem : rows ≠ []) :
    ∀ p ∈ streamRevenue rows, p.1 ≤ (rows.getLast?.map Prod.fst).getD p.1 := by
  intro p hp
  obtain ⟨x, hx, hxk⟩ := rr_sr_keys_mem rows p.1 p.2 hp
  rw [← hxk]
  exact rr_pairwise_le_last rows h x hx

private theorem rr_merge_extend (A B : List (Nat × Option Int)) (k : Nat) (v : Option Int)
    (hAB : (A ++ B ++ [(k,v)]).Pairwise (fun a b => a.1 ≤ b.1)) :
    mergeBoundary (streamRevenue A) (rr_extend (streamRevenue B) k v) =
      rr_extend (mergeBoundary (streamRevenue A) (streamRevenue B)) k v := by
  cases hA : A with
  | nil => subst A; simp [mergeBoundary, streamRevenue]
  | cons a atail =>
    subst A
    cases hB : B with
    | nil =>
      subst B
      cases hRA : streamRevenue (a :: atail) with
      | nil => simp [mergeBoundary, streamRevenue]
      | cons g ds =>
        by_cases hkg : k == g.1
        · simp [mergeBoundary, rr_extend, streamRevenue, hkg, rr_beq_sym, plus_assoc]
        · simp [mergeBoundary, rr_extend, streamRevenue, hkg, rr_beq_sym]
    | cons b btail =>
      subst B
      have hABpair : ((a :: atail) ++ (b :: btail)).Pairwise (fun x y => x.1 ≤ y.1) :=
        (List.pairwise_append.mp hAB).1
      have hBsort : (b :: btail).Pairwise (fun x y => x.1 ≤ y.1) :=
        (List.pairwise_append.mp hABpair).2.1
      obtain ⟨init, last, hdec⟩ := rr_exists_init_last (b :: btail) (by simp)
      have hlastkey : (streamRevenue (b :: btail)).head?.map Prod.fst = some last.1 := by
        rw [hdec]
        exact rr_sr_head_key init last
      cases hRB : streamRevenue (b :: btail) with
      | nil =>
          have hf : False := by
            have hh := hlastkey
            rw [hRB] at hh
            simp at hh
          exact False.elim hf
      | cons g ds =>
        have hg : g.1 = last.1 := by simpa [hRB] using hlastkey
        cases hRA : streamRevenue (a :: atail) with
        | nil => simp [mergeBoundary, rr_extend]
        | cons p ps =>
          by_cases hkg : k == g.1
          · cases hds : ds.reverse with
            | nil =>
                by_cases hgp : g.1 == p.1
                · have hkp : k = p.1 := (beq_iff_eq.mp hkg).trans (beq_iff_eq.mp hgp)
                  simp [mergeBoundary, rr_extend, hds, hkg, hgp, hkp, rr_beq_sym, plus_assoc, streamRevenue]
                · simp [mergeBoundary, rr_extend, hds, hkg, hgp, rr_beq_sym, plus_assoc, streamRevenue]
            | cons j jt =>
                by_cases hb : p.1 == j.1
                · dsimp [mergeBoundary]
                  simp [hds, hkg, hb, rr_beq_sym, plus_assoc, rr_extend]
                · dsimp [mergeBoundary]
                  simp [hds, hkg, hb, rr_beq_sym, plus_assoc, rr_extend]
          · cases hds : ds.reverse with
            | nil =>
                by_cases hgp : g.1 == p.1
                · have hkgf : (k == g.1) = false := Bool.eq_false_of_ne_true hkg
                  have hkneg : k ≠ g.1 := beq_eq_false_iff_ne.mp hkgf
                  have hgp' : g.1 = p.1 := beq_iff_eq.mp hgp
                  have hknp : k ≠ p.1 := by intro hkp; exact hkneg (hkp.trans hgp'.symm)
                  simp [mergeBoundary, rr_extend, hds, hkg, hgp, hknp, rr_beq_sym, plus_assoc, streamRevenue]
                · simp [mergeBoundary, rr_extend, hds, hkg, hgp, rr_beq_sym, plus_assoc, streamRevenue]
            | cons j jt =>
                by_cases hb : p.1 == j.1
                · dsimp [mergeBoundary]
                  simp [hds, hkg, hb, rr_beq_sym, plus_assoc, rr_extend]
                · dsimp [mergeBoundary]
                  simp [hds, hkg, hb, rr_beq_sym, plus_assoc, rr_extend]

private theorem rr_merge_sr (A B : List (Nat × Option Int))
    (h : (A ++ B).Pairwise (fun a b => a.1 ≤ b.1)) :
    mergeBoundary (streamRevenue A) (streamRevenue B) = streamRevenue (A ++ B) := by
  by_cases hB : B = []
  · subst B
    rw [show streamRevenue ([] : List (Nat × Option Int)) = [] by rfl]
    cases hsr : streamRevenue A <;> simp [mergeBoundary, hsr]
  · obtain ⟨init, last, hdec⟩ := rr_exists_init_last B hB
    subst B
    have hABinit : (A ++ init).Pairwise (fun a b => a.1 ≤ b.1) := by
      have ht := h.take (i := (A ++ init).length)
      rw [← List.append_assoc] at ht
      have htake : (A ++ init ++ [last]).take (A ++ init).length = A ++ init := by
        rw [List.take_append_of_le_length (Nat.le_refl _)]
        rw [List.take_length]
      rw [htake] at ht
      exact ht
    have hABinitlast : (A ++ init ++ [last]).Pairwise (fun a b => a.1 ≤ b.1) := by
      simpa [List.append_assoc] using h
    have ih := rr_merge_sr A init hABinit
    rw [rr_streamRevenue_snoc]
    rw [rr_merge_extend A init last.1 last.2 hABinitlast]
    rw [ih]
    rw [← List.append_assoc]
    rw [rr_streamRevenue_snoc]
termination_by B.length
decreasing_by rw [hdec]; simp

private theorem rr_sr_nodup (rows : List (Nat × Option Int))
    (h : rows.Pairwise (fun a b => a.1 ≤ b.1)) :
    (streamRevenue rows).Pairwise (fun p q => p.1 ≠ q.1) := by
  by_cases hem : rows = []
  · subst hem; simp [streamRevenue]
  · obtain ⟨init, last, hdec⟩ := rr_exists_init_last rows hem
    subst rows
    have hinit : init.Pairwise (fun a b => a.1 ≤ b.1) := (List.pairwise_append.mp h).1
    have ih := rr_sr_nodup init hinit
    rw [rr_streamRevenue_snoc]
    cases hsr : streamRevenue init with
    | nil => simp [rr_extend]
    | cons g gs =>
        have ihgs : gs.Pairwise (fun p q => p.1 ≠ q.1) := (List.pairwise_cons.mp (by simpa [hsr] using ih)).2
        by_cases hgl : g.1 == last.1
        · have hge : g.1 = last.1 := beq_iff_eq.mp hgl
          rw [← hge]
          simp [rr_extend, beq_self_eq_true]
          refine ⟨?_, ihgs⟩
          intro a b hmem hge
          have ihpair : (g :: gs).Pairwise (fun p q => p.1 ≠ q.1) := by simpa [hsr] using ih
          have hneq : g.1 ≠ a := (List.pairwise_cons.mp ihpair).1 (a,b) hmem
          exact hneq hge
        · have hg_le : g.1 ≤ last.1 := by
            obtain ⟨x, hx, hxg⟩ := rr_sr_keys_mem init g.1 g.2 (by simp [hsr])
            have hxle : x.1 ≤ last.1 := (List.pairwise_append.mp h).2.2 x hx last (by simp)
            rwa [hxg] at hxle
          have hg_lt : g.1 < last.1 := by
            have hgne : g.1 ≠ last.1 := by intro he; exact hgl (by rw [he]; exact beq_self_eq_true last.1)
            omega
          have hall : ∀ p ∈ gs, p.1 < last.1 := by
            intro p hp
            have ihpair : (g :: gs).Pairwise (fun p q => p.1 ≠ q.1) := by simpa [hsr] using ih
            have hpg : g.1 ≠ p.1 := (List.pairwise_cons.mp ihpair).1 p hp
            have hpg' : p.1 ≠ g.1 := fun hh => hpg hh.symm
            have hg_last : g.1 = (init.getLast?.map Prod.fst).getD g.1 := by
              by_cases hi : init = []
              · rw [hi] at hsr; simp [streamRevenue] at hsr
              · obtain ⟨i2, l2, hd2⟩ := rr_exists_init_last init hi
                have hs := rr_sr_head_key i2 l2
                rw [← hd2] at hs
                have hs' : (streamRevenue init).head?.map Prod.fst = some l2.1 := by simpa [hd2] using hs
                have hg1 : g.1 = l2.1 := by rw [hsr] at hs'; simpa using hs'
                simp [hd2, hg1]
            have hp_le_g : p.1 ≤ g.1 := by
              obtain ⟨x, hx, hxp⟩ := rr_sr_keys_mem init p.1 p.2 (by simp [hsr, hp])
              have hget : (init.getLast?.map Prod.fst).getD p.1 = g.1 := by
                cases hl : init.getLast? with
                | none =>
                    have hi0 : init = [] := by
                      cases init with
                      | nil => rfl
                      | cons y ys => simp at hl
                    subst init
                    simp [streamRevenue] at hsr
                | some y =>
                    simp [hl] at hg_last ⊢
                    exact hg_last.symm
              rw [← hxp] at hget ⊢
              rw [← hget]
              exact rr_pairwise_le_last init hinit x hx
            omega
          have hgl' : (last.1 == g.1) = false := by
            rw [rr_beq_sym]
            exact Bool.eq_false_of_ne_true hgl
          simp [rr_extend, hgl']
          refine ⟨⟨?_, ?_⟩, ?_⟩
          · intro hEq
            exact hgl (by rw [hEq]; exact beq_self_eq_true g.1)
          · intro a b hmem hEq
            have hlt := hall (a,b) hmem
            omega
          · refine ⟨?_, ihgs⟩
            intro a b hmem hEq
            have ihpair : (g :: gs).Pairwise (fun p q => p.1 ≠ q.1) := by simpa [hsr] using ih
            have hneq : g.1 ≠ a := (List.pairwise_cons.mp ihpair).1 (a,b) hmem
            exact hneq hEq
termination_by rows.length
decreasing_by rw [hdec]; simp

private theorem rr_lookup_extend_eq_head (runs : List (Nat × Totals)) (k : Nat) (v : Option Int) (t : Totals)
    (hhead : runs.head?.map Prod.fst = some k) (h : runs.lookup k = some t) :
    (rr_extend runs k v).lookup k = some (plus t (weighted 1 v)) := by
  cases runs with
  | nil => simp at hhead
  | cons g ds =>
      have hgp : g.1 = k := by simpa using hhead
      have hval : t = g.2 := by
        rw [← hgp] at h
        rw [List.lookup_cons] at h
        simp at h
        exact h.symm
      rw [hval, ← hgp]
      simp [rr_extend]

private theorem rr_lookup_extend_ne_head (runs : List (Nat × Totals)) (k : Nat) (v : Option Int)
    (hhead : runs.head?.map Prod.fst ≠ some k) :
    (rr_extend runs k v).lookup k = some (weighted 1 v) := by
  cases runs with
  | nil => simp [rr_extend]
  | cons g ds =>
      have hgp : g.1 ≠ k := by intro h; exact hhead (by simp [h])
      have hkp : k ≠ g.1 := fun h => hgp h.symm
      simp [rr_extend, hkp]

private theorem rr_lookup_extend_none (runs : List (Nat × Totals)) (k : Nat) (v : Option Int)
    (h : runs.lookup k = none) : (rr_extend runs k v).lookup k = some (weighted 1 v) := by
  cases runs with
  | nil => simp [rr_extend]
  | cons g ds =>
      by_cases hkp : k = g.1
      · rw [hkp] at h
        simp at h
      · simpa [rr_extend, hkp] using h

private theorem rr_lookup_extend_other_key (runs : List (Nat × Totals)) (k j : Nat) (v : Option Int)
    (hk : k ≠ j) : (rr_extend runs j v).lookup k = runs.lookup k := by
  cases runs with
  | nil => simp [rr_extend, hk]
  | cons g ds =>
      by_cases hj : j == g.1
      · have hjeq : j = g.1 := beq_iff_eq.mp hj
        have hkne : k ≠ g.1 := by intro he; exact hk (he.trans hjeq.symm)
        have hkb : (k == g.1) = false := beq_eq_false_iff_ne.mpr hkne
        simp only [rr_extend, hj, ↓reduceIte]
        rw [List.lookup_cons, List.lookup_cons]
        simp [hkb]
      · have hkbeq : (k == j) = false := beq_eq_false_iff_ne.mpr hk
        have hjf : (j == g.1) = false := Bool.eq_false_of_ne_true hj
        simp [rr_extend, hjf]
        rw [List.lookup_cons, List.lookup_cons]
        simp [hkbeq]

private theorem rr_sr_lookup_sum (rows : List (Nat × Option Int))
    (h : rows.Pairwise (fun a b => a.1 ≤ b.1)) (k : Nat) :
    (streamRevenue rows).lookup k =
      (if (rows.filter (fun r => r.1 == k)).isEmpty then none
       else some (summarize ((rows.filter (fun r => r.1 == k)).map Prod.snd))) := by
  by_cases hem : rows = []
  · subst hem; simp [streamRevenue]
  · obtain ⟨init, last, hdec⟩ := rr_exists_init_last rows hem
    subst rows
    have hinit : init.Pairwise (fun a b => a.1 ≤ b.1) := (List.pairwise_append.mp h).1
    have ih := rr_sr_lookup_sum init hinit k
    rw [rr_streamRevenue_snoc]
    by_cases hkl : k == last.1
    · have heq : k = last.1 := beq_iff_eq.mp hkl
      rw [← heq]
      cases hsr : streamRevenue init with
      | nil =>
          rw [rr_lookup_extend_none]
          · have hno : ∀ x : Option Int, ¬(k, x) ∈ init := by
              rw [hsr] at ih
              simpa using ih
            have hfilt : (init.filter (fun r => r.1 == k)).isEmpty = true := by
              rw [List.isEmpty_iff, List.eq_nil_iff_forall_not_mem]
              intro r hr
              have hrm := List.mem_filter.mp hr
              rcases r with ⟨kk, vv⟩
              have hkeq : kk = k := beq_iff_eq.mp hrm.2
              have hmem : (k, vv) ∈ init := by
                rw [← hkeq]
                exact hrm.1
              exact hno vv hmem
            have hmemlast : last ∈ List.filter (fun r => r.1 == k) (init ++ [last]) := by
              rw [List.mem_filter]
              constructor
              · simp
              · show (last.1 == k) = true
                rw [heq]
                exact beq_self_eq_true last.1
            have happ : (List.filter (fun r => r.1 == k) (init ++ [last])).isEmpty = false := by
              apply Bool.eq_false_of_ne_true
              intro hh
              have hnil : List.filter (fun r => r.1 == k) (init ++ [last]) = [] := List.isEmpty_iff.mp hh
              have : last ∈ ([] : List (Nat × Option Int)) := by
                rw [← hnil]
                exact hmemlast
              simpa using this
            simp [happ, List.filter_append, hfilt, hkl, summarize_nil, weighted]
            constructor
            · intro _; exact heq.symm
            · have hf1 : (List.filter (fun r => r.1 == k) [last]).map Prod.snd = [last.2] := by
                simp [heq]
              have hf0 : (List.filter (fun r => r.1 == k) init).map Prod.snd = [] := by
                rw [← List.map_nil]
                congr
                exact List.isEmpty_iff.mp hfilt
              rw [hf1, hf0]
              simp [summarize_cons, summarize_nil, weighted, plus_zero', zero, plus]
          · rfl
      | cons g gs =>
          have hhead : (streamRevenue init).head?.map Prod.fst = some g.1 := by simp [hsr]
          by_cases hgk : g.1 == k
          · have hdh : (streamRevenue init).head?.map Prod.fst = some k := by simpa [beq_iff_eq.mp hgk] using hhead
            cases hlk : (streamRevenue init).lookup k with
            | none =>
                have hfilt : (init.filter (fun r => r.1 == k)).isEmpty = true := by
                  rw [hlk] at ih
                  by_cases hfe : (init.filter (fun r => r.1 == k)).isEmpty = true
                  · exact hfe
                  · have hfef : (init.filter (fun r => r.1 == k)).isEmpty = false := Bool.eq_false_of_ne_true hfe
                    rw [hfef] at ih
                    simp at ih
                rw [rr_lookup_extend_none (g :: gs) k last.2 (by simpa [hsr] using hlk)]
                have hmemlast : last ∈ List.filter (fun r => r.1 == k) (init ++ [last]) := by
                  rw [List.mem_filter]
                  constructor
                  · simp
                  · show (last.1 == k) = true
                    rw [heq]
                    exact beq_self_eq_true last.1
                have happ : (List.filter (fun r => r.1 == k) (init ++ [last])).isEmpty = false := by
                  apply Bool.eq_false_of_ne_true
                  intro hh
                  have hnil := List.isEmpty_iff.mp hh
                  have : last ∈ ([] : List (Nat × Option Int)) := by rw [← hnil]; exact hmemlast
                  simpa using this
                have hf0 : (List.filter (fun r => r.1 == k) init).map Prod.snd = [] := by
                  rw [← List.map_nil]; congr; exact List.isEmpty_iff.mp hfilt
                have hf1 : (List.filter (fun r => r.1 == k) [last]).map Prod.snd = [last.2] := by
                  simp [heq]
                rw [List.filter_append, List.map_append, hf0, hf1]
                simp [happ, summarize_cons, summarize_nil, weighted, plus]
                constructor
                · intro _; exact heq.symm
                · simp [zero, plus]
            | some t =>
                rw [rr_lookup_extend_eq_head (g :: gs) k last.2 t (by simpa [hsr] using hdh) (by simpa [hsr] using hlk)]
                have hsum : t = summarize ((init.filter (fun r => r.1 == k)).map Prod.snd) := by
                  have hh := ih
                  rw [hlk] at hh
                  by_cases hfe : (init.filter (fun r => r.1 == k)).isEmpty = true
                  · simp [hfe] at hh
                  · simp [hfe] at hh
                    exact hh
                have hfilt : (init.filter (fun r => r.1 == k)).isEmpty = false := by
                  by_cases hf : (init.filter (fun r => r.1 == k)).isEmpty = false
                  · exact hf
                  · have hfe : (init.filter (fun r => r.1 == k)).isEmpty = true := Bool.eq_true_of_ne_false hf
                    rw [hlk, hfe] at ih
                    simp at ih
                have hmemlast : last ∈ List.filter (fun r => r.1 == k) (init ++ [last]) := by
                  rw [List.mem_filter]
                  constructor
                  · simp
                  · show (last.1 == k) = true
                    rw [heq]
                    exact beq_self_eq_true last.1
                have happ : (List.filter (fun r => r.1 == k) (init ++ [last])).isEmpty = false := by
                  apply Bool.eq_false_of_ne_true
                  intro hh
                  have hnil := List.isEmpty_iff.mp hh
                  have : last ∈ ([] : List (Nat × Option Int)) := by rw [← hnil]; exact hmemlast
                  simpa using this
                rw [hsum]
                have hf1 : (List.filter (fun r => r.1 == k) [last]).map Prod.snd = [last.2] := by
                  simp [heq]
                simp [List.filter_append, hf1, summarize_append, summarize_cons, summarize_nil, weighted, plus]
                constructor
                · intro _; exact heq.symm
                · simp [zero, plus]
          · have hdhne : (streamRevenue init).head?.map Prod.fst ≠ some k := by
              intro hh
              have hk0 : g.1 = k := by
                have : some g.1 = some k := hhead.symm.trans hh
                exact Option.some.inj this
              exact hgk (by rw [hk0]; exact beq_self_eq_true k)
            rw [rr_lookup_extend_ne_head (g :: gs) k last.2 (by simpa [hsr] using hdhne)]
            have hg_le_k : g.1 ≤ k := by
              obtain ⟨x, hx, hxg⟩ := rr_sr_keys_mem init g.1 g.2 (by simp [hsr])
              have hxle : x.1 ≤ last.1 := (List.pairwise_append.mp h).2.2 x hx last (by simp)
              rw [hxg] at hxle
              rw [heq]
              exact hxle
            have hg_lt_k : g.1 < k := by
              have hgne : g.1 ≠ k := by intro he; exact hgk (by rw [he]; exact beq_self_eq_true k)
              omega
            have hfilt0 : (init.filter (fun r => r.1 == k)).isEmpty = true := by
              have hnok : ∀ x ∈ init, x.1 ≠ k := by
                intro x hx hxk
                have hlastk : g.1 = (init.getLast?.map Prod.fst).getD g.1 := by
                  by_cases hi : init = []
                  · rw [hi] at hsr; simp [streamRevenue] at hsr
                  · obtain ⟨i2, l2, hd2⟩ := rr_exists_init_last init hi
                    have hs := rr_sr_head_key i2 l2
                    rw [← hd2] at hs
                    have hs' : (streamRevenue init).head?.map Prod.fst = some l2.1 := by
                      simpa [hd2] using hs
                    have hg1 : g.1 = l2.1 := by
                      rw [hsr] at hs'
                      simpa using hs'
                    simp [hd2, hg1]
                have hxle : x.1 ≤ g.1 := by
                  have hget : (init.getLast?.map Prod.fst).getD x.1 = g.1 := by
                    cases hl : init.getLast? with
                    | none =>
                        have hi0 : init = [] := by
                          cases init with
                          | nil => rfl
                          | cons y ys => simp at hl
                        subst init
                        simp [streamRevenue] at hsr
                    | some y =>
                        simp [hl] at hlastk ⊢
                        exact hlastk.symm
                  rw [← hget]
                  exact rr_pairwise_le_last init hinit x hx
                rw [hxk] at hxle
                omega
              rw [List.isEmpty_iff, List.eq_nil_iff_forall_not_mem]
              intro r hr
              have hrm := List.mem_filter.mp hr
              rcases r with ⟨kk, vv⟩
              exact hnok (kk, vv) hrm.1 (beq_iff_eq.mp hrm.2)
            have hf0 : (List.filter (fun r => r.1 == k) init).map Prod.snd = [] := by
              rw [← List.map_nil]; congr; exact List.isEmpty_iff.mp hfilt0
            have hf1 : (List.filter (fun r => r.1 == k) [last]).map Prod.snd = [last.2] := by
              simp [heq]
            rw [List.filter_append, List.map_append, hf0, hf1]
            simp [summarize_cons, summarize_nil, weighted, plus]
            constructor
            · intro _; exact heq.symm
            · simp [zero, plus]
    · have hkne : k ≠ last.1 := by
        intro hh
        exact hkl (by rw [hh]; exact beq_self_eq_true last.1)
      rw [rr_lookup_extend_other_key]
      · rw [ih]
        have hlne : last.1 ≠ k := by intro h; exact hkne h.symm
        have hllast : (List.filter (fun r : Nat × Option Int => r.1 == k) [last]) = [] := by
          simp [hlne]
        rw [List.filter_append, hllast, List.append_nil]
      · exact hkne
termination_by rows.length
decreasing_by rw [hdec]; simp

private theorem rr_mem_of_lookup_some (l : List (Nat × Totals)) (k : Nat) (v : Totals) (h : l.lookup k = some v) :
    ∃ t, (k,t) ∈ l := by
  induction l with
  | nil => simp at h
  | cons p ps ih =>
      by_cases hk : p.1 == k
      · have hpe : p.1 = k := beq_iff_eq.mp hk
        have hp_eq : (k, p.2) = p := by
          cases p
          simp at hpe ⊢
          exact hpe.symm
        exact ⟨p.2, by rw [hp_eq]; simp⟩
      · have hkf : (p.1 == k) = false := Bool.eq_false_of_ne_true hk
        have hkf' : (k == p.1) = false := by
          rw [rr_beq_sym]
          exact hkf
        rw [List.lookup_cons, hkf'] at h
        simp at h

        obtain ⟨t, ht⟩ := ih h
        exact ⟨t, by simp [ht]⟩

private theorem rr_sr_length (rows : List (Nat × Option Int))
    (h : rows.Pairwise (fun a b => a.1 ≤ b.1)) :
    (streamRevenue rows).length = (rows.map Prod.fst).eraseDups.length := by
  have hnd1 : ((streamRevenue rows).map Prod.fst).Nodup := by
    show ((streamRevenue rows).map Prod.fst).Pairwise (fun a b => a ≠ b)
    rw [List.pairwise_map]
    exact rr_sr_nodup rows h
  have hnd2 : ((rows.map Prod.fst).eraseDups).Nodup := nodup_eraseDups _
  have hp : (streamRevenue rows).map Prod.fst |>.Perm (rows.map Prod.fst).eraseDups := by
    apply perm_of_nodup_mem hnd1
    · exact hnd2
    · intro k
      constructor
      · intro hk
        obtain ⟨p, hp, hpk⟩ := List.mem_map.mp hk
        obtain ⟨x, hx, hxk⟩ := rr_sr_keys_mem rows p.1 p.2 hp
        rw [List.mem_eraseDups]
        exact List.mem_map.mpr ⟨x, hx, hxk.trans hpk⟩
      · intro hk
        have hk' : k ∈ rows.map Prod.fst := List.mem_eraseDups.mp hk
        have hnon : (rows.filter (fun r => r.1 == k)).isEmpty = false := by
          by_cases hh : (rows.filter (fun r => r.1 == k)).isEmpty = true
          · have hnil := List.isEmpty_iff.mp hh
            obtain ⟨x, hx, hxk⟩ := List.mem_map.mp hk'
            have hmem : x ∈ rows.filter (fun r => r.1 == k) := by
              rw [List.mem_filter]
              exact ⟨hx, by rw [hxk]; exact beq_self_eq_true k⟩
            rw [hnil] at hmem
            simpa using hmem
          · exact Bool.eq_false_of_ne_true hh
        have hlk := rr_sr_lookup_sum rows h k
        rw [hnon] at hlk
        obtain ⟨t, ht⟩ := rr_mem_of_lookup_some (streamRevenue rows) k _ hlk
        exact List.mem_map.mpr ⟨(k,t), ht, rfl⟩
  simpa using hp.length_eq


private theorem rr_isEmpty_map {α β : Type} (f : α → β) (l : List α) : (l.map f).isEmpty = l.isEmpty := by
  cases l <;> simp


theorem RevenueRuns_sound : TinylambOpt.ResearchGoals.DuckDBGaps.Orders.RevenueRuns.Goal := by
  intro chunks k hsort
  have hfold : ∀ (cs : List (List (Nat × Option Int))) (P : List (Nat × Option Int)),
      (P ++ cs.flatten).Pairwise (fun a b => a.1 ≤ b.1) →
      (cs.map streamRevenue).foldl mergeBoundary (streamRevenue P) =
        streamRevenue (P ++ cs.flatten) := by
    intro cs
    induction cs with
    | nil => intro P h; simp
    | cons c cs ih =>
        intro P h
        simp only [List.map_cons, List.flatten_cons]
        simp only [List.flatten_cons] at h
        rw [← List.append_assoc] at h
        rw [List.foldl_cons]
        have hPc : (P ++ c).Pairwise (fun a b => a.1 ≤ b.1) := by
          have ht := h.take (i := (P ++ c).length)
          have htake : (P ++ c ++ cs.flatten).take (P ++ c).length = P ++ c := by
            rw [List.take_append_of_le_length (Nat.le_refl _), List.take_length]
          rw [htake] at ht
          exact ht
        rw [rr_merge_sr P c hPc]
        simpa [List.append_assoc] using ih (P ++ c) h
  have hout := hfold chunks [] (by simpa using hsort)
  have hsre : streamRevenue ([] : List (Nat × Option Int)) = [] := rfl
  rw [hsre] at hout
  have hout' : (chunks.map streamRevenue).foldl mergeBoundary [] = streamRevenue chunks.flatten := by
    simpa using hout
  refine ⟨hout', ?_, ?_⟩
  · rw [hout']
    rw [rr_isEmpty_map]
    simpa using (rr_sr_lookup_sum chunks.flatten hsort k)
  · rw [hout']
    exact rr_sr_length chunks.flatten hsort





end TinylambOpt.ResearchProofs.DuckDBGaps.Orders
