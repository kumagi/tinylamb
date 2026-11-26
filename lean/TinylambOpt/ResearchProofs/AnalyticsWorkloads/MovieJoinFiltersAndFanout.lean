import TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.

Proved

- `CompanyPair` — `∀ rows companies types, let cache := pairCache rows companies types
  rows.foldl (fun acc (a,b,y) => plus acc (weighted (cache.getD (a,b) 0) (some y)))
  zero = summarize (rows.flatMap fun (a,b,y) => (companies.filter (· == a)).flatMap
  fun _ => (types.filter (· == b)).map fun _ => some y)`
- `CorrelatedFanout` — `∀ (cells : List (List Mass)), let a := fun rs => mass rs (fun x
  _ => x) let b := fun rs => mass rs (fun _ y => y) let both := mass cells.flatten (·
  && ·) (cells.map fun rs => a rs + b rs - mass rs (fun _ _ => true)).sum ≤ both ∧
  both ≤ (cells.map fun rs => min (a rs) (b rs)).sum`
- `DecimalPrefix` — `∀ s code, certified s code → aka s code = s.startsWith "aka-1"`
- `FixedGender` — `∀ (rows : List (String × Option Int)), let kept := rows.filter fun r
  => r.1 == "F" singletonGroup (kept.map Prod.snd) = ((kept.map
  Prod.fst).eraseDups.map fun g => (g,summarize ((kept.filter (fun r => r.1 ==
  g)).map Prod.snd)))`
- `LabelQuotient` — `∀ bridge dict info label, labelCount (labelPairs bridge dict) info
  label = (bridge.flatMap fun (movie,k) => (info.filter (· == movie)).flatMap fun _
  => (dict.filter (fun e => e.1 == k)).map Prod.snd).count label`
- `RatingOrder` — `∀ (ratings : List (Option String)) bound, ratings.filter (fun v =>
  v.any (atLeast bound)) = ratings.filter (fun v => v.any (fun s => decide (bound ≤
  s)))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.MovieJoinFiltersAndFanout

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  cases a
  simp [plus, zero]

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  cases a
  simp [plus, zero]

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a; cases b; cases c
  simp [plus]
  omega

private theorem weighted_add (n m : Nat) (v : Option Int) :
    plus (weighted n v) (weighted m v) = weighted (n+m) v := by
  cases v <;> simp [plus, weighted, Int.natCast_add, Int.add_mul] <;> omega

private theorem summarize_foldl (xs : List (Option Int)) (acc : Totals) :
    xs.foldl (fun a v => plus a (weighted 1 v)) acc = plus acc (summarize xs) := by
  induction xs generalizing acc with
  | nil => simp [summarize, plus_zero_right]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih]
      change plus (plus acc (weighted 1 x)) (summarize xs) =
        plus acc (xs.foldl (fun a v => plus a (weighted 1 v)) (plus zero (weighted 1 x)))
      rw [plus_zero_left]
      rw [ih]
      rw [plus_assoc]

private theorem summarize_append (xs ys : List (Option Int)) :
    summarize (xs ++ ys) = plus (summarize xs) (summarize ys) := by
  change (xs ++ ys).foldl (fun a v => plus a (weighted 1 v)) zero = _
  rw [List.foldl_append]
  rw [summarize_foldl xs zero]
  rw [plus_zero_left]
  rw [summarize_foldl ys (summarize xs)]

private theorem summarize_map_const (xs : List Nat) (v : Option Int) :
    summarize (xs.map fun _ => v) = weighted xs.length v := by
  induction xs with
  | nil => simp [summarize, weighted, zero]
  | cons x xs ih =>
      simp only [List.map_cons]
      change summarize (v :: xs.map fun _ => v) = _
      rw [← List.singleton_append]
      rw [summarize_append]
      rw [ih]
      change plus (summarize [v]) (weighted xs.length v) = weighted (x::xs).length v
      have hs : summarize [v] = weighted 1 v := by simp [summarize, weighted, zero, plus]
      rw [hs]
      change plus (weighted 1 v) (weighted xs.length v) = weighted (xs.length+1) v
      rw [weighted_add]
      rw [Nat.add_comm]

private theorem summarize_flat_const (cs ts : List Nat) (y : Int) :
    summarize (cs.flatMap fun _ => ts.map fun _ => some y) =
      weighted (cs.length * ts.length) (some y) := by
  induction cs with
  | nil => simp [summarize, weighted, zero]
  | cons c cs ih =>
      simp only [List.flatMap_cons]
      rw [summarize_append]
      rw [summarize_map_const]
      rw [ih]
      rw [weighted_add]
      simp [Nat.succ_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private theorem histogram_foldl_getD (xs : List Nat) (m : Std.HashMap Nat Nat) (a : Nat) :
  (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD a 0 =
    m.getD a 0 + (xs.filter (fun x => x == a)).length := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih]
      rw [Std.HashMap.getD_insert]
      by_cases hxa : (x == a) = true
      · have hxeq : x = a := by simpa using hxa
        simp [hxeq]
        omega
      · simp [hxa]

private theorem histogram_getD (xs : List Nat) (a : Nat) :
  (histogram xs).getD a 0 = (xs.filter (fun x => x == a)).length := by
  unfold histogram
  have h := histogram_foldl_getD xs ({} : Std.HashMap Nat Nat) a
  simpa using h

private theorem foldl_insert_getD_preserve {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (ks : List α) (v : α → Nat) (m : Std.HashMap α Nat) (k : α) (hm : m.getD k 0 = v k) :
    (ks.foldl (fun (m : Std.HashMap α Nat) x => m.insert x (v x)) m).getD k 0 = v k := by
  induction ks generalizing m with
  | nil => exact hm
  | cons x xs ih =>
      simp only [List.foldl_cons]
      apply ih
      rw [Std.HashMap.getD_insert]
      by_cases hxk : (x == k) = true
      · have hxeq : x = k := by simpa using hxk
        subst x
        simp
      · simp [hxk, hm]

private theorem foldl_insert_getD_of_mem {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (ks : List α) (v : α → Nat) (m : Std.HashMap α Nat) (k : α) (hmem : k ∈ ks) :
    (ks.foldl (fun (m : Std.HashMap α Nat) x => m.insert x (v x)) m).getD k 0 = v k := by
  induction ks generalizing m with
  | nil => simp at hmem
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [List.mem_cons] at hmem
      rcases hmem with hkx | hmem
      · subst k
        exact foldl_insert_getD_preserve xs v (m.insert x (v x)) x (by simp)
      · exact ih (m.insert x (v x)) hmem

private theorem cache_correct (rows : List Bridge) (companies types : List Nat)
    (a b : Nat) (y : Int) (hy : (a,b,y) ∈ rows) :
    (pairCache rows companies types).getD (a,b) 0 =
      (histogram companies).getD a 0 * (histogram types).getD b 0 := by
  unfold pairCache
  dsimp
  apply foldl_insert_getD_of_mem
  apply List.mem_eraseDups.mpr
  apply List.mem_map.mpr
  exact ⟨(a,b,y), hy, rfl⟩

private theorem summarize_bridge_row (companies types : List Nat) (a b : Nat) (y : Int) :
    summarize ((companies.filter (fun x => x == a)).flatMap fun _ =>
      (types.filter (fun x => x == b)).map fun _ => some y) =
      weighted ((histogram companies).getD a 0 * (histogram types).getD b 0) (some y) := by
  rw [summarize_flat_const]
  rw [histogram_getD, histogram_getD]

private def stepW (f : Nat → Nat → Nat) (acc : Totals) (r : Bridge) : Totals :=
  match r with
  | (a,b,y) => plus acc (weighted (f a b) (some y))

private theorem foldl_weights_acc (rows : List Bridge) (f : Nat → Nat → Nat) (acc : Totals) :
    rows.foldl (stepW f) acc = plus acc (rows.foldl (stepW f) zero) := by
  induction rows generalizing acc with
  | nil => simp [plus_zero_right]
  | cons r rows ih =>
      simp only [List.foldl_cons]
      rw [show stepW f acc r = plus acc (weighted (f r.1 r.2.1) (some r.2.2)) by rfl]
      rw [show stepW f zero r = weighted (f r.1 r.2.1) (some r.2.2) by simp [stepW, plus_zero_left]]
      rw [ih (weighted (f r.1 r.2.1) (some r.2.2))]
      have hfinal := ih (plus acc (weighted (f r.1 r.2.1) (some r.2.2)))
      rw [plus_assoc] at hfinal
      exact hfinal

private theorem company_pair_general (rows : List Bridge) (companies types : List Nat)
    (f : Nat → Nat → Nat)
    (hf : ∀ (a b : Nat) (y : Int), (a,b,y) ∈ rows →
      f a b = (histogram companies).getD a 0 * (histogram types).getD b 0) :
    rows.foldl (stepW f) zero =
      summarize (rows.flatMap fun (a,b,y) => (companies.filter (fun x => x == a)).flatMap fun _ =>
        (types.filter (fun x => x == b)).map fun _ => some y) := by
  induction rows with
  | nil => simp [summarize]
  | cons r rows ih =>
      rcases r with ⟨a,b,y⟩
      have hfhead := hf a b y (by simp)
      have hftail : ∀ (a' b' : Nat) (y' : Int), (a',b',y') ∈ rows →
          f a' b' = (histogram companies).getD a' 0 * (histogram types).getD b' 0 := by
        intro a' b' y' h
        exact hf a' b' y' (by simp [h])
      simp only [List.foldl_cons]
      rw [show stepW f zero (a,b,y) = weighted (f a b) (some y) by simp [stepW, plus_zero_left]]
      rw [foldl_weights_acc]
      rw [hfhead]
      rw [List.flatMap_cons]
      rw [summarize_append]
      rw [summarize_bridge_row]
      rw [ih hftail]

theorem CompanyPair_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.CompanyPair.Goal := by
  intro rows companies types
  let f := fun a b => (pairCache rows companies types).getD (a,b) 0
  change rows.foldl (stepW f) zero =
    summarize (rows.flatMap fun (a,b,y) => (companies.filter (fun x => x == a)).flatMap fun _ =>
      (types.filter (fun x => x == b)).map fun _ => some y)
  apply company_pair_general
  intro a b y hy
  dsimp [f]
  exact cache_correct rows companies types a b y hy

private theorem summarize_cnt (ys : List (Option Int)) : (summarize ys).cnt = ys.length := by
  change (ys.foldl (fun a v => plus a (weighted 1 v)) zero).cnt = ys.length
  induction ys with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [summarize_foldl xs (plus zero (weighted 1 x))]
      rw [plus_zero_left]
      change 1 + (summarize xs).cnt = (x :: xs).length
      have ih' : (summarize xs).cnt = xs.length := by simpa [summarize] using ih
      rw [ih']
      simp only [List.length_cons]
      omega

private theorem filter_not_eq_nil {α : Type} [BEq α] (a : α) (l : List α)
    (hall : ∀ x, x ∈ l → (x == a) = true) :
    l.filter (fun x => ! x == a) = [] := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : (x == a) = true := hall x (by simp)
      have ht : ∀ z, z ∈ xs → (z == a) = true := by intro z hz; exact hall z (by simp [hz])
      simp [hx, ih ht]

private theorem eraseDups_const {α : Type} [BEq α] (a : α) (l : List α)
    (hall : ∀ x, x ∈ l → (x == a) = true) : (a :: l).eraseDups = [a] := by
  rw [List.eraseDups_cons]
  rw [filter_not_eq_nil a l hall]
  simp

theorem FixedGender_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.FixedGender.Goal := by
  intro rows
  let kept := rows.filter fun r => r.1 == "F"
  change singletonGroup (kept.map Prod.snd) =
    ((kept.map Prod.fst).eraseDups.map fun g =>
      (g, summarize ((kept.filter (fun r => r.1 == g)).map Prod.snd)))
  have hall : ∀ r ∈ kept, (r.1 == "F") = true := by
    intro r hr
    exact (List.mem_filter.mp hr).2
  have hallEq : ∀ r ∈ kept, r.1 = "F" := by
    intro r hr
    have := hall r hr
    simpa using this
  by_cases hnil : kept = []
  · rw [hnil]
    simp [singletonGroup, summarize_cnt]
  · have hfst : (kept.map Prod.fst).eraseDups = ["F"] := by
      obtain ⟨r, rs, hkr⟩ := List.exists_cons_of_ne_nil hnil
      rw [hkr]
      have hrF : r.1 = "F" := hallEq r (by rw [hkr]; simp)
      have htail : ∀ z ∈ rs, z.1 = "F" := by
        intro z hz
        exact hallEq z (by rw [hkr]; simp [hz])
      have htailBE : ∀ z ∈ rs.map Prod.fst, (z == "F") = true := by
        intro z hz
        rcases List.mem_map.mp hz with ⟨w, hw, rfl⟩
        have := htail w hw
        simpa [this]
      simpa [hrF] using eraseDups_const "F" (rs.map Prod.fst) htailBE
    have hfilter : kept.filter (fun r => r.1 == "F") = kept := by
      apply List.filter_eq_self.mpr
      intro r hr
      exact hall r hr
    have hcnt : (summarize (kept.map Prod.snd)).cnt ≠ 0 := by
      rw [summarize_cnt]
      simp only [List.length_map]
      have hpos : 0 < kept.length := List.length_pos_iff.mpr hnil
      omega
    simp [singletonGroup, hfst, hfilter, hcnt]

private theorem digit_cases (c : Char) (h : c.isDigit = true) :
    c = '0' ∨ c = '1' ∨ c = '2' ∨ c = '3' ∨ c = '4' ∨ c = '5' ∨ c = '6' ∨ c = '7' ∨ c = '8' ∨ c = '9' := by
  have hv32 : (48 : UInt32) ≤ c.val ∧ c.val ≤ 57 := by
    simpa [Char.isDigit, decide_eq_true_eq] using h
  have hv : 48 ≤ c.val.toNat ∧ c.val.toNat ≤ 57 := by
    constructor
    · have hle : (48 : UInt32).toBitVec ≤ c.val.toBitVec := by
        change (48 : UInt32).toBitVec ≤ c.val.toBitVec
        exact hv32.1
      exact BitVec.le_def.mp hle
    · have hle : c.val.toBitVec ≤ (57 : UInt32).toBitVec := by
        change c.val.toBitVec ≤ (57 : UInt32).toBitVec
        exact hv32.2
      exact BitVec.le_def.mp hle
  rcases (show c.val.toNat = 48 ∨ c.val.toNat = 49 ∨ c.val.toNat = 50 ∨ c.val.toNat = 51 ∨
      c.val.toNat = 52 ∨ c.val.toNat = 53 ∨ c.val.toNat = 54 ∨ c.val.toNat = 55 ∨
      c.val.toNat = 56 ∨ c.val.toNat = 57 by omega) with h48 | h49 | h50 | h51 | h52 | h53 | h54 | h55 | h56 | h57
  · exact Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h48))
  · exact Or.inr (Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h49)))
  · exact Or.inr (Or.inr (Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h50))))
  · exact Or.inr (Or.inr (Or.inr (Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h51)))))
  · exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h52))))))
  · exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h53)))))))
  · exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h54))))))))
  · exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h55)))))))))
  · exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h56))))))))))
  · exact Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr (by apply Char.ext; exact UInt32.toNat_inj.mp (by simpa using h57))))))))))

private theorem rating_shape {s : String} {r : Nat} (h : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating s = some r) :
    ∃ a b : Char, a.isDigit = true ∧ b.isDigit = true ∧ s = String.ofList [a, '.', b] ∧
      r = (a.toNat - 48) * 10 + b.toNat - 48 := by
  unfold TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating at h
  cases hs : s.toList with
  | nil => simp [hs] at h
  | cons a tl =>
      cases tl with
      | nil => simp [hs] at h
      | cons c tl =>
          cases tl with
          | nil => simp [hs] at h
          | cons b tl =>
              cases tl with
              | nil =>
                  by_cases hc : c = '.'
                  · subst c
                    simp [hs] at h
                    rcases h with ⟨⟨ha, hb⟩, hr⟩
                    have hs' : s = String.ofList [a, '.', b] := by
                      have ht := String.ofList_toList (s := s)
                      rw [hs] at ht
                      exact ht.symm
                    exact ⟨a, b, ha, hb, hs', hr.symm⟩
                  · simp [hs, hc] at h
              | cons d tl => simp [hs] at h

private theorem cmp_ok (s t : String) (rs rt : Nat)
    (hs : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating s = some rs)
    (ht : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating t = some rt) :
    (decide (rs ≤ rt) : Bool) = decide (s ≤ t) := by
  rcases rating_shape hs with ⟨a, b, ha, hb, rfl, hrs⟩
  rcases rating_shape ht with ⟨c, d, hc, hd, rfl, hrt⟩
  subst rs
  subst rt
  rcases digit_cases a ha with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
  rcases digit_cases b hb with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
  rcases digit_cases c hc with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
  rcases digit_cases d hd with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
  native_decide

private theorem atLeast_ok (bound s : String) :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.atLeast bound s = decide (bound ≤ s) := by
  unfold TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.atLeast
  by_cases hb : ∃ r, TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating bound = some r
  · rcases hb with ⟨rb, hb⟩
    by_cases hs : ∃ r, TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating s = some r
    · rcases hs with ⟨rs, hs⟩
      have hcmp := cmp_ok bound s rb rs hb hs
      simp only [hb, hs]
      by_cases hle : rb ≤ rs
      · have h1 : decide (bound ≤ s) = true := by
          have h0 : decide (rb ≤ rs) = true := decide_eq_true hle
          rwa [hcmp] at h0
        simp [hle, h1]
      · have h0 : decide (rb ≤ rs) = false := decide_eq_false hle
        have h1 : decide (bound ≤ s) = false := by
          rwa [← hcmp]
        simp [hle, h1]
    · have hn : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating s = none := by
        cases h : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating s with
        | none => rfl
        | some v => exact False.elim (hs ⟨v, h⟩)
      simp [hb, hn]
  · have hn : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating bound = none := by
      cases h : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.rating bound with
      | none => rfl
      | some v => exact False.elim (hb ⟨v, h⟩)
    simp [hn]

theorem RatingOrder_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.RatingOrder.Goal := by
  intro ratings bound
  apply List.filter_congr
  intro v hv
  cases v with
  | none => simp
  | some s => simp [atLeast_ok]


private theorem akaPrefix_head (t : String) : ("aka-" ++ t).startsWith "aka-1" = (t.toList.head? == some '1') := by
  apply Bool.eq_iff_iff.mpr
  simp only [String.startsWith_string_iff]
  cases ht : t.toList with
  | nil =>
      have ht' : t = "" := by
        apply String.ext
        simpa using ht.symm
      subst t
      native_decide
  | cons c cs =>
      have ht' : t = String.ofList (c :: cs) := by
        apply String.ext
        simpa [ht]
      subst t
      simp only [String.toList_append, String.toList_ofList, List.head?_cons]
      change ['a', 'k', 'a', '-', '1'] <+: (['a', 'k', 'a', '-'] ++ (c :: cs)) ↔ (c == '1') = true
      constructor
      · intro h
        rcases h with ⟨u, hu⟩
        simp only [List.cons_append, List.nil_append] at hu
        have hu' := congrArg (List.drop 4) hu
        have hc : c = '1' := by simpa [eq_comm] using congrArg List.head? hu'
        subst c
        rfl
      · intro h
        have hc : c = '1' := by
          have hd : decide (c = '1') = true := by simpa using h
          exact of_decide_eq_true hd
        subst c
        exact ⟨cs, by simp⟩

private theorem leading_one_iff (n : Nat) (hn : 0 < n) :
    let L := (Nat.toDigits 10 n).length
    (10 ^ (L - 1) ≤ n ∧ n < 2 * 10 ^ (L - 1)) ↔ (Nat.toDigits 10 n).head? == some '1' := by
  revert hn
  refine Nat.strongRecOn n ?_
  intro n ih hn
  dsimp
  by_cases hlt : n < 10
  · rw [Nat.toDigits_of_lt_base hlt]
    simp [Nat.digitChar_eq_one]
    omega
  · have hn10 : 10 ≤ n := by omega
    let q := n / 10
    let r := n % 10
    have hqpos : 0 < q := Nat.div_pos hn10 (by omega)
    have hqlt : q < n := by
      dsimp [q]
      have hle : n / 10 ≤ n := Nat.div_le_self n 10
      omega
    have hdigits : Nat.toDigits 10 n = Nat.toDigits 10 q ++ [Nat.digitChar r] := by
      dsimp [q, r]
      exact Nat.toDigits_of_base_le (by omega) hn10
    have hLn : (Nat.toDigits 10 n).length = (Nat.toDigits 10 q).length + 1 := by
      rw [hdigits]
      simp
    have hhead : (Nat.toDigits 10 n).head? = (Nat.toDigits 10 q).head? := by
      rw [hdigits]
      cases hdq : Nat.toDigits 10 q with
      | nil => simp at hdq
      | cons c cs => simp
    have hnr : 10 * q + r = n := by
      dsimp [q, r]
      exact Nat.div_add_mod n 10
    have hrlt : r < 10 := by dsimp [r]; exact Nat.mod_lt n (by omega)
    have hwq : 10 ^ ((Nat.toDigits 10 q).length + 1 - 1) =
        10 * 10 ^ ((Nat.toDigits 10 q).length - 1) := by
      have hpos : 0 < (Nat.toDigits 10 q).length := Nat.length_toDigits_pos
      have heq : (Nat.toDigits 10 q).length + 1 - 1 = (Nat.toDigits 10 q).length := by omega
      rw [heq]
      have hL : (Nat.toDigits 10 q).length = ((Nat.toDigits 10 q).length - 1) + 1 := by omega
      calc
        10 ^ (Nat.toDigits 10 q).length = 10 ^ ((Nat.toDigits 10 q).length - 1 + 1) := by
          exact congrArg (fun e : Nat => 10 ^ e) hL
        _ = 10 ^ ((Nat.toDigits 10 q).length - 1) * 10 := by rw [Nat.pow_succ]
        _ = 10 * 10 ^ ((Nat.toDigits 10 q).length - 1) := by rw [Nat.mul_comm]
    have ihq := ih q hqlt hqpos
    dsimp at ihq
    constructor
    · intro h
      rw [hLn, hwq] at h
      have hqcond : (10 ^ ((Nat.toDigits 10 q).length - 1) ≤ q ∧
          q < 2 * 10 ^ ((Nat.toDigits 10 q).length - 1)) := by
        constructor <;> omega
      have hheadq := ihq.mp hqcond
      rw [hhead]
      exact hheadq
    · intro h
      have hheadq : (Nat.toDigits 10 q).head? == some '1' := by simpa [hhead] using h
      have hqcond := ihq.mpr hheadq
      rw [hLn, hwq]
      constructor <;> omega

theorem DecimalPrefix_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.DecimalPrefix.Goal := by
  intro s code hcert
  cases code with
  | none => rfl
  | some c =>
      rcases c with ⟨n, w⟩
      simp only [TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.certified] at hcert
      rcases hcert with ⟨rfl, rfl⟩
      simp only [TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.aka]
      by_cases hn : n = 0
      · subst n
        native_decide
      · have hnpos : 0 < n := Nat.pos_of_ne_zero hn
        have hlead := leading_one_iff n hnpos
        dsimp at hlead
        have hlen : (Nat.toDigits 10 n).length = (toString n).length := by
          rw [Nat.toString_eq_ofList_toDigits]
          exact (String.length_ofList (l := Nat.toDigits 10 n)).symm
        have htolist : (toString n).toList = Nat.toDigits 10 n := by
          simp
        apply Bool.eq_iff_iff.mpr
        simp only [Bool.and_eq_true, decide_eq_true_eq]
        rw [akaPrefix_head]
        rw [← hlen, htolist]
        exact hlead

private theorem perm_sum {α : Type} (g : α → Nat) {l₁ l₂ : List α} (p : l₁.Perm l₂) :
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

private theorem sum_filter_partition {α : Type} (L : List α) (p : α → Bool) (g : α → Nat) :
    ((L.filter p).map g).sum + ((L.filter fun x => !p x).map g).sum = (L.map g).sum := by
  induction L with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · have hnp : !p x = false := by simp [hp]
        simp [hp, hnp]
        omega
      · have hnp : !p x = true := by simp [hp]
        simp [hp, hnp]
        omega

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
        have hgetp : m.get? p.1 = some p.2 := Std.HashMap.mem_toList_iff_getElem?_eq_some.mp hp
        have hcontra : m.get? k = some p.2 := by simpa [hpeq'] using hgetp
        have hg2 : m[k]? = some p.2 := hcontra
        rw [hget] at hg2
        cases hg2
      · exact Bool.eq_false_of_ne_true hbeq
    have hfilt : m.toList.filter (fun p => k == p.1) = [] := by
      apply filter_eq_nil_of_all_false
      intro p hp
      exact hno p hp
    rw [hfilt]
    have hzero : m.getD k 0 = 0 := by
      rw [Std.HashMap.getD_eq_getD_getElem?]
      rw [hget]
      rfl
    rw [hzero]
    simp

private theorem S_insert {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (f : α → Nat) :
    ((m.insert k (m.getD k 0 + 1)).toList.map fun p => p.2 * f p.1).sum =
      (m.toList.map fun p => p.2 * f p.1).sum + f k := by
  let g : α × Nat → Nat := fun p => p.2 * f p.1
  let v := m.getD k 0 + 1
  have hp := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := v)
  have hsum := perm_sum g hp
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
    (m.toList.map fun p => p.2 * f p.1).sum + f k
  rw [hsum]
  rw [← hpart]
  rw [hmatch]
  dsimp [v]
  rw [Nat.succ_mul]
  omega

private theorem histogram_foldl_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (m : Std.HashMap α Nat) (f : α → Nat) :
    ((xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).toList.map fun p => p.2 * f p.1).sum =
      (m.toList.map fun p => p.2 * f p.1).sum + (xs.map f).sum := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih]
      rw [S_insert]
      simp
      omega

private theorem histogram_sum {α : Type} [BEq α] [Hashable α] [LawfulBEq α] [LawfulHashable α]
    (xs : List α) (f : α → Nat) :
    ((histogram xs).toList.map fun p => p.2 * f p.1).sum = (xs.map f).sum := by
  unfold histogram
  have h := histogram_foldl_sum xs ({} : Std.HashMap α Nat) f
  simpa using h


private theorem count_eq_indicator {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (a : α) :
  xs.count a = (xs.map fun x => if x == a then 1 else 0).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.count_cons, List.map_cons, List.sum_cons]
      rw [ih]
      split <;> omega


private theorem label_row (movie k : Nat) (dict : List (Nat × String)) (info : List Nat)
    (label : String) :
    ((dict.filter (fun e => e.1 == k)).map fun e =>
      if e.2 = label then (histogram info).getD movie 0 else 0).sum =
    (histogram info).getD movie 0 * ((dict.filter (fun e => e.1 == k)).map Prod.snd).count label := by
  induction dict with
  | nil => simp
  | cons e dict ih =>
      by_cases hek : e.1 = k
      · by_cases hel : e.2 = label
        · simp [hek, hel, ih, Nat.mul_add, Nat.add_comm, Nat.add_assoc]
        · simp [hek, hel, ih]
      · simp [hek, ih]


private theorem count_replicate_rows (xs : List Nat) (ds : List String) (label : String) :
    (xs.flatMap fun _ => ds).count label = xs.length * ds.count label := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.succ_mul, Nat.add_comm]

private def wcount (dict : List (Nat × String)) (info : List Nat) (label : String)
    (r : Nat × Nat) : Nat :=
  (histogram info).getD r.1 0 * ((dict.filter (fun e => e.1 == r.2)).map Prod.snd).count label

private def lf (info : List Nat) (label : String) (p : Nat × String) : Nat :=
  if p.2 == label then (histogram info).getD p.1 0 else 0

private theorem label_lhs (bridge : List (Nat × Nat)) (dict : List (Nat × String)) (info : List Nat)
    (label : String) :
    ((labelPairs bridge dict).map (lf info label)).sum =
    (bridge.map (wcount dict info label)).sum := by
  induction bridge with
  | nil => simp [labelPairs]
  | cons r bridge ih =>
      rcases r with ⟨movie,k⟩
      simp only [labelPairs, List.flatMap_cons]
      rw [List.map_append, List.sum_append]
      have hrow : ((dict.filter (fun e => e.1 == k)).map fun e =>
          if e.2 == label then (histogram info).getD movie 0 else 0).sum =
          (histogram info).getD movie 0 *
            ((dict.filter (fun e => e.1 == k)).map Prod.snd).count label := by
        simpa using label_row movie k dict info label
      change (List.map (lf info label)
          (List.map (fun e : Nat × String => (movie, e.2)) (dict.filter (fun e => e.1 == k)))).sum +
          (List.map (lf info label) (labelPairs bridge dict)).sum =
        wcount dict info label (movie,k) + (List.map (wcount dict info label) bridge).sum
      simp only [List.map_map, Function.comp_apply]
      change (List.map (fun e : Nat × String =>
          if e.2 == label then (histogram info).getD movie 0 else 0)
          (dict.filter (fun e => e.1 == k))).sum +
          (List.map (lf info label) (labelPairs bridge dict)).sum =
        wcount dict info label (movie,k) + (List.map (wcount dict info label) bridge).sum
      rw [hrow]
      rw [ih]
      rfl

private theorem label_rhs (bridge : List (Nat × Nat)) (dict : List (Nat × String)) (info : List Nat)
    (label : String) :
    (bridge.flatMap fun (movie,k) => (info.filter (fun x => x == movie)).flatMap fun _ =>
      (dict.filter (fun e => e.1 == k)).map Prod.snd).count label =
    (bridge.map (wcount dict info label)).sum := by
  induction bridge with
  | nil => simp
  | cons r bridge ih =>
      rcases r with ⟨movie,k⟩
      simp only [List.flatMap_cons, List.map_cons, List.sum_cons]
      rw [List.count_append]
      have h1 : (List.count label
          ((info.filter (fun x => x == movie)).flatMap fun _ =>
            (dict.filter (fun e => e.1 == k)).map Prod.snd)) =
          (histogram info).getD movie 0 *
            ((dict.filter (fun e => e.1 == k)).map Prod.snd).count label := by
        rw [count_replicate_rows]
        rw [← histogram_getD]
      rw [h1]
      change (histogram info).getD movie 0 *
          ((dict.filter (fun e => e.1 == k)).map Prod.snd).count label +
          List.count label (List.flatMap (fun x : Nat × Nat =>
            (info.filter (fun y => y == x.1)).flatMap fun _ =>
              (dict.filter (fun e => e.1 == x.2)).map Prod.snd) bridge) =
        wcount dict info label (movie,k) + (List.map (wcount dict info label) bridge).sum
      rw [ih]
      rfl

theorem LabelQuotient_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.LabelQuotient.Goal := by
  intro bridge dict info label
  unfold labelCount
  dsimp
  change ((histogram (labelPairs bridge dict)).toList.map fun p =>
      if p.1.2 == label then p.2 * (histogram info).getD p.1.1 0 else 0).sum =
    (bridge.flatMap fun (movie,k) => (info.filter (fun x => x == movie)).flatMap fun _ =>
      (dict.filter (fun e => e.1 == k)).map Prod.snd).count label
  have hconv : ∀ p : (Nat × String) × Nat,
      (if p.1.2 == label then p.2 * (histogram info).getD p.1.1 0 else 0) =
      p.2 * (if p.1.2 == label then (histogram info).getD p.1.1 0 else 0) := by
    intro p
    by_cases h : (p.1.2 == label) = true <;> simp [h]
  simp only [hconv]
  have hsum := histogram_sum (labelPairs bridge dict)
    (fun p : Nat × String => if p.2 == label then (histogram info).getD p.1 0 else 0)
  rw [hsum]
  change (List.map (lf info label) (labelPairs bridge dict)).sum =
    List.count label (List.flatMap (fun x : Nat × Nat =>
      (info.filter (fun y => y == x.1)).flatMap fun _ =>
        (dict.filter (fun e => e.1 == x.2)).map Prod.snd) bridge)
  rw [label_lhs]
  rw [label_rhs]


private theorem cell_bounds (rs : List Mass) :
    let a := mass rs (fun x _ => x)
    let b := mass rs (fun _ y => y)
    let both := mass rs (fun x y => x && y)
    let tot := mass rs (fun _ _ => true)
    a + b ≤ both + tot ∧ both ≤ a ∧ both ≤ b := by
  induction rs with
  | nil => simp [mass]
  | cons r rs ih =>
      rcases r with ⟨x, y, w⟩
      dsimp at ih ⊢
      simp [mass, List.map_cons, List.sum_cons] at ih ⊢
      cases x <;> cases y <;> simp at ih ⊢ <;> omega

private theorem flatten_map_sum {α : Type} (cells : List (List α)) (f : α → Nat) :
    (cells.map fun rs => rs.map f).flatten.sum = (cells.map fun rs => (rs.map f).sum).sum := by
  induction cells with
  | nil => simp
  | cons rs cells ih => simp [List.flatten_cons, List.map_append, List.sum_append, ih]

private theorem mass_flatten (cells : List (List Mass)) (p : Bool → Bool → Bool) :
    mass cells.flatten p = (cells.map fun rs => mass rs p).sum := by
  unfold mass
  rw [List.map_flatten]
  exact flatten_map_sum cells (fun r : Mass => if p r.1 r.2.1 then r.2.2 else 0)

private theorem sum_le_of_pointwise {α : Type} (f g : α → Nat) (xs : List α)
    (h : ∀ x ∈ xs, f x ≤ g x) : (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ z ∈ xs, f z ≤ g z := by intro z hz; exact h z (by simp [hz])
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add hx (ih ht)

theorem CorrelatedFanout_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.MovieJoinFiltersAndFanout.CorrelatedFanout.Goal := by
  intro cells
  let a := fun rs => mass rs (fun x _ => x)
  let b := fun rs => mass rs (fun _ y => y)
  let both := fun rs => mass rs (fun x y => x && y)
  let tot := fun rs => mass rs (fun _ _ => true)
  have hmass : mass cells.flatten (fun x y => x && y) = (cells.map both).sum := by
    exact mass_flatten cells (fun x y => x && y)
  constructor
  · rw [hmass]
    apply sum_le_of_pointwise
    intro rs hrs
    have hc := cell_bounds rs
    dsimp [a, b, both, tot] at hc ⊢
    exact Nat.sub_le_iff_le_add.mpr hc.1
  · rw [hmass]
    apply sum_le_of_pointwise
    intro rs hrs
    have hc := cell_bounds rs
    dsimp [a, b, both, tot] at hc ⊢
    exact Nat.le_min.mpr ⟨hc.2.1, hc.2.2⟩


end TinylambOpt.ResearchProofs.AnalyticsWorkloads.MovieJoinFiltersAndFanout

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.cpp
-- executor/detail/analytics_factorized_join_test.cpp
