import TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.

Proved

- `CodeFanout` — `∀ dict ranks vs country, let (gs,n) := translate dict ranks vs let
  selected := vs.filter fun r => r.2.1 == country gs.getD country zero = summarize
  (selected.flatMap fun (code,_,v) => let key := dict[code]?.join (ranks.filter (fun
  (s,rank) => s.isSome && s == key && rank > 40)).map fun _ => v) ∧ n = ((vs.map
  Prod.fst).eraseDups.countP fun i => dict[i]?.join.isSome)`
- `LateLabels`
- `PriceSynopsis` — `∀ xs cut, let (n,scanned) := priceCount (prices xs) xs cut n =
  xs.countP (fun x => x.any (cut < ·)) ∧ (scanned = true ↔ (∃ x ∈ xs, x.any (· ≤ cut)
  = true) ∧ (∃ x ∈ xs, x.any (cut < ·) = true))`
- `ResidualWords` — `∀ rows, let kept := rows.filter fun (c,w,_) => c == some "US" ||
  w.any (·.contains "data") searchAgg rows = (summarize (kept.map (fun r => r.2.2)),
  (((rows.filter (fun r => r.1 != some "US")).filterMap (fun r =>
  r.2.1)).eraseDups).length)`
- `UrlGrouping` — `∀ (visits : List (Option String × Option Int)) key, let h :=
  visits.foldl (fun (m : Std.HashMap Url Totals) (u,v) => let k := encode u m.insert
  k (plus (m.getD k zero) (weighted 1 v))) {} decode (encode key) = key ∧ h.getD
  (encode key) zero = summarize ((visits.filter (fun r => r.1 == key)).map Prod.snd)`
-/

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.DictionaryJoinsAndLateLabels

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a; cases b; cases c
  simp [plus]
  omega

private theorem plus_zero_left (a : Totals) : plus zero a = a := by
  cases a
  simp [plus, zero]

private theorem plus_zero_right (a : Totals) : plus a zero = a := by
  cases a
  simp [plus, zero]

private theorem foldl_plus_acc (ts : List Totals) (acc : Totals) :
    ts.foldl plus acc = plus acc (ts.foldl plus zero) := by
  induction ts generalizing acc with
  | nil => simpa [plus_zero_right]
  | cons t ts ih =>
      have h1 := ih (plus acc t)
      have h2 := ih t
      have hz : plus zero t = t := plus_zero_left t
      simp only [List.foldl_cons]
      rw [h1, hz, h2, plus_assoc]

private theorem sumTotals_cons (t : Totals) (ts : List Totals) :
    (t :: ts).foldl plus zero = plus t (ts.foldl plus zero) := by
  simp only [List.foldl_cons]
  rw [plus_zero_left]
  exact foldl_plus_acc ts t

private theorem foldl_plus_eq (xs : List (Option Int)) (acc : Totals) :
    xs.foldl (fun a v => plus a (weighted 1 v)) acc =
      plus acc (xs.foldl (fun a v => plus a (weighted 1 v)) zero) := by
  induction xs generalizing acc with
  | nil => simpa [plus_zero_right]
  | cons x xs ih =>
      have h1 := ih (plus acc (weighted 1 x))
      have h2 := ih (weighted 1 x)
      have hz : plus zero (weighted 1 x) = weighted 1 x := plus_zero_left _
      simp only [List.foldl_cons]
      rw [h1, hz, h2, plus_assoc]

private theorem summarize_cons (x : Option Int) (xs : List (Option Int)) :
    summarize (x :: xs) = plus (weighted 1 x) (summarize xs) := by
  rw [summarize]
  simpa [summarize, plus_zero_left] using foldl_plus_eq xs (weighted 1 x)

private theorem foldl_min_spec (a : Int) (xs : List Int) :
    let m := xs.foldl min a
    m ∈ a :: xs ∧ ∀ b ∈ a :: xs, m ≤ b := by
  induction xs generalizing a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      have hs := ih (min a x)
      rcases hs with ⟨hm_mem, hm_le⟩
      have hm_le_a : (xs.foldl min (min a x)) ≤ a := by
        by_cases h : a ≤ x
        · have hm : min a x = a := Int.min_eq_left h
          exact hm_le a (by simp [hm])
        · have hxa : x ≤ a := by omega
          have hm : min a x = x := Int.min_eq_right hxa
          have hmx : (xs.foldl min (min a x)) ≤ x := hm_le x (by simp [hm])
          omega
      have hm_le_x : (xs.foldl min (min a x)) ≤ x := by
        by_cases h : a ≤ x
        · have hm : min a x = a := Int.min_eq_left h
          have hma : (xs.foldl min (min a x)) ≤ a := hm_le a (by simp [hm])
          omega
        · have hxa : x ≤ a := by omega
          have hm : min a x = x := Int.min_eq_right hxa
          exact hm_le x (by simp [hm])
      constructor
      · simp only [List.mem_cons] at hm_mem ⊢
        rcases hm_mem with hm_eq | hm_tail
        · by_cases h : a ≤ x
          · have hm : min a x = a := Int.min_eq_left h
            exact Or.inl (by simpa [hm] using hm_eq)
          · have hxa : x ≤ a := by omega
            have hm : min a x = x := Int.min_eq_right hxa
            exact Or.inr (Or.inl (by simpa [hm] using hm_eq))
        · exact Or.inr (Or.inr hm_tail)
      · intro b hb
        simp only [List.mem_cons] at hb
        rcases hb with hba | hbx | hbtail
        · simpa [hba] using hm_le_a
        · simpa [hbx] using hm_le_x
        · exact hm_le b (by simp [hbtail])

private theorem foldl_max_spec (a : Int) (xs : List Int) :
    let m := xs.foldl max a
    m ∈ a :: xs ∧ ∀ b ∈ a :: xs, b ≤ m := by
  induction xs generalizing a with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      have hs := ih (max a x)
      rcases hs with ⟨hm_mem, hm_le⟩
      have ha_le_m : a ≤ (xs.foldl max (max a x)) := by
        by_cases h : x ≤ a
        · have hm : max a x = a := Int.max_eq_left h
          exact hm_le a (by simp [hm])
        · have hax : a ≤ x := by omega
          have hm : max a x = x := Int.max_eq_right hax
          have hxm : x ≤ (xs.foldl max (max a x)) := hm_le x (by simp [hm])
          omega
      have hx_le_m : x ≤ (xs.foldl max (max a x)) := by
        by_cases h : x ≤ a
        · have hm : max a x = a := Int.max_eq_left h
          have ham : a ≤ (xs.foldl max (max a x)) := hm_le a (by simp [hm])
          omega
        · have hax : a ≤ x := by omega
          have hm : max a x = x := Int.max_eq_right hax
          exact hm_le x (by simp [hm])
      constructor
      · simp only [List.mem_cons] at hm_mem ⊢
        rcases hm_mem with hm_eq | hm_tail
        · by_cases h : x ≤ a
          · have hm : max a x = a := Int.max_eq_left h
            exact Or.inl (by simpa [hm] using hm_eq)
          · have hax : a ≤ x := by omega
            have hm : max a x = x := Int.max_eq_right hax
            exact Or.inr (Or.inl (by simpa [hm] using hm_eq))
        · exact Or.inr (Or.inr hm_tail)
      · intro b hb
        simp only [List.mem_cons] at hb
        rcases hb with hba | hbx | hbtail
        · simpa [hba] using ha_le_m
        · simpa [hbx] using hx_le_m
        · exact hm_le b (by simp [hbtail])

private theorem min?_none_iff (l : List Int) : l.min? = none ↔ l = [] := by
  cases l <;> simp [List.min?]

private theorem max?_none_iff (l : List Int) : l.max? = none ↔ l = [] := by
  cases l <;> simp [List.max?]

private theorem min?_spec_of_some {l : List Int} {m : Int} (h : l.min? = some m) :
    m ∈ l ∧ ∀ b ∈ l, m ≤ b := by
  cases l with
  | nil => simp at h
  | cons a as =>
      simp [List.min?] at h
      subst m
      simpa using foldl_min_spec a as

private theorem max?_spec_of_some {l : List Int} {m : Int} (h : l.max? = some m) :
    m ∈ l ∧ ∀ b ∈ l, b ≤ m := by
  cases l with
  | nil => simp at h
  | cons a as =>
      simp [List.max?] at h
      subst m
      simpa using foldl_max_spec a as

private theorem min?_exists {l : List Int} (hne : l ≠ []) :
    ∃ m, l.min? = some m ∧ m ∈ l ∧ ∀ b ∈ l, m ≤ b := by
  cases hm : l.min? with
  | none => exact False.elim (hne ((min?_none_iff l).mp hm))
  | some m =>
      refine ⟨m, rfl, ?_⟩
      exact min?_spec_of_some hm

private theorem max?_exists {l : List Int} (hne : l ≠ []) :
    ∃ m, l.max? = some m ∧ m ∈ l ∧ ∀ b ∈ l, b ≤ m := by
  cases hm : l.max? with
  | none => exact False.elim (hne ((max?_none_iff l).mp hm))
  | some m =>
      refine ⟨m, rfl, ?_⟩
      exact max?_spec_of_some hm

private theorem max_all_le_iff (vs : List Int) (cut : Int) :
    Option.all (fun v : Int => v ≤ cut) vs.max? = true ↔ ∀ v ∈ vs, v ≤ cut := by
  by_cases he : vs = []
  · subst vs
    simp
  · rcases max?_exists he with ⟨m, hm, hmem, hmax⟩
    rw [hm]
    simp
    constructor
    · intro h v hv
      have hvle := hmax v hv
      omega
    · intro h
      exact h m hmem

private theorem min_any_gt_iff (vs : List Int) (cut : Int) :
    Option.any (fun v : Int => cut < v) vs.min? = true ↔
      vs ≠ [] ∧ ∀ v ∈ vs, cut < v := by
  by_cases he : vs = []
  · subst vs
    simp
  · rcases min?_exists he with ⟨m, hm, hmem, hmin⟩
    rw [hm]
    simp
    constructor
    · intro h
      exact ⟨he, fun v hv => by
        have hminv := hmin v hv
        omega⟩
    · intro h
      exact h.2 m hmem

private theorem countP_option_any (xs : List (Option Int)) (p : Int → Bool) :
    xs.countP (fun x => x.any p) = (xs.filterMap id).countP p := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simp [ih]
      | some v =>
          by_cases hp : p v = true <;> simp [hp, ih]

private theorem exists_any_iff (xs : List (Option Int)) (p : Int → Bool) :
    (∃ x ∈ xs, x.any p = true) ↔ ∃ v ∈ xs.filterMap id, p v = true := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      cases x <;> simp [ih]

private theorem countP_eq_zero_of_all_false {α : Type} (p : α → Bool) {l : List α}
    (h : ∀ a ∈ l, p a = false) : l.countP p = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      have ha : p a = false := h a (by simp)
      have ht : ∀ b ∈ l, p b = false := by intro b hb; exact h b (by simp [hb])
      simp [ha, ih ht]

private theorem countP_eq_length_of_all_true {α : Type} (p : α → Bool) {l : List α}
    (h : ∀ a ∈ l, p a = true) : l.countP p = l.length := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      have ha : p a = true := h a (by simp)
      have ht : ∀ b ∈ l, p b = true := by intro b hb; exact h b (by simp [hb])
      simp [ha, ih ht]

private theorem url_beq_eq (a b : Url) : (a == b) = true ↔ a = b := by
  cases a <;> cases b <;>
    dsimp [BEq.beq, instBEqUrl, instBEqUrl.beq] <;> simp

local instance : ReflBEq Url := ⟨by
  intro a
  cases a <;> dsimp [BEq.beq, instBEqUrl, instBEqUrl.beq] <;> simp⟩

local instance : LawfulBEq Url := ⟨by
  intro a b h
  exact (url_beq_eq a b).mp h⟩

local instance : EquivBEq Url where
  rfl := ReflBEq.rfl
  symm := by
    intro a b h
    have hab : a = b := LawfulBEq.eq_of_beq h
    subst b
    exact ReflBEq.rfl
  trans := by
    intro a b c hab hbc
    have h1 : a = b := LawfulBEq.eq_of_beq hab
    have h2 : b = c := LawfulBEq.eq_of_beq hbc
    subst b
    subst c
    exact ReflBEq.rfl

local instance : LawfulHashable Url := ⟨by
  intro a b h
  have hab : a = b := LawfulBEq.eq_of_beq h
  subst b
  rfl⟩

private theorem url_decode_encode (k : Option String) : decode (encode k) = k := by
  cases k with
  | none => rfl
  | some s =>
      cases hn : (s.drop 5).toNat? with
      | none => simp [encode, decode, hn]
      | some n =>
          by_cases h : "page-" ++ toString n == s
          · have h' : "page-" ++ n.repr = s := by simpa using beq_iff_eq.mp h
            simp [encode, decode, hn]
            rw [h']
            simpa using h'
          · have hneq : "page-" ++ n.repr ≠ s := by
              intro heq
              apply h
              apply beq_iff_eq.mpr
              simpa using heq
            simp [encode, decode, hn, hneq]

private theorem url_filter_eq (visits : List (Option String × Option Int)) (key : Option String) :
    visits.filter (fun r => encode r.1 == encode key) =
      visits.filter (fun r => r.1 == key) := by
  induction visits with
  | nil => rfl
  | cons r rs ih =>
      by_cases h : encode r.1 == encode key
      · have heq : encode r.1 = encode key := beq_iff_eq.mp h
        have hr : r.1 = key := by
          have := congrArg decode heq
          simpa [url_decode_encode] using this
        have hbeq : r.1 == key := beq_iff_eq.mpr hr
        simp [h, hbeq, ih]
      · have hr : r.1 ≠ key := by
          intro hreq
          apply h
          rw [hreq]
          exact ReflBEq.rfl
        have hbeq : (r.1 == key) = false := by
          by_cases hk : (r.1 == key) = true
          · exact False.elim (hr (beq_iff_eq.mp hk))
          · cases hb' : r.1 == key
            · rfl
            · exact False.elim (hk hb')
        simp [h, hbeq, ih]

private theorem hm_foldl_getD {α β : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    [LawfulBEq α] (key : β → α) (w : β → Totals) :
    ∀ (xs : List β) (m : Std.HashMap α Totals) (k : α),
      (xs.foldl (fun m b => m.insert (key b) (plus (m.getD (key b) zero) (w b))) m).getD k zero =
      plus (m.getD k zero) (((xs.filter fun b => key b == k).map w).foldl plus zero) := by
  intro xs
  induction xs with
  | nil =>
      intro m k
      simp [plus_zero_left, plus_zero_right]
  | cons b xs ih =>
      intro m k
      simp only [List.foldl_cons]
      rw [ih (m.insert (key b) (plus (m.getD (key b) zero) (w b))) k]
      rw [Std.HashMap.getD_insert]
      by_cases h : key b == k
      · have hk : key b = k := beq_iff_eq.mp h
        subst k
        rw [if_pos h]
        simp only [List.filter_cons, List.map_cons, ReflBEq.rfl, decide_true, ↓reduceIte]
        simp only [List.foldl_cons]
        rw [show plus zero (w b) = w b from plus_zero_left _]
        rw [show List.foldl plus (w b)
              (List.map w (List.filter (fun b_1 => key b_1 == key b) xs)) =
            plus (w b) (List.foldl plus zero
              (List.map w (List.filter (fun b_1 => key b_1 == key b) xs))) from
          foldl_plus_acc _ _]
        simp [plus_zero_left, plus_assoc]
      · simp [h]

theorem UrlGrouping_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels.UrlGrouping.Goal := by
  intro visits key
  constructor
  · exact url_decode_encode key
  · let h := visits.foldl (fun (m : Std.HashMap Url Totals) (u, v) =>
      let k := encode u
      m.insert k (plus (m.getD k zero) (weighted 1 v))) {}
    have hfold := hm_foldl_getD (fun r : Option String × Option Int => encode r.1)
      (fun r => weighted 1 r.2) visits ({} : Std.HashMap Url Totals) (encode key)
    have hfilter := url_filter_eq visits key
    have hsum : ((visits.filter fun r => encode r.1 == encode key).map
        (fun r => weighted 1 r.2)).foldl plus zero =
        summarize ((visits.filter fun r => r.1 == key).map Prod.snd) := by
      have hmap : (visits.filter fun r => encode r.1 == encode key).map
          (fun r => weighted 1 r.2) =
          (visits.filter fun r => r.1 == key).map (fun r => weighted 1 r.2) := by
        rw [hfilter]
      rw [hmap]
      induction (visits.filter fun r => r.1 == key) with
      | nil => simp [summarize]
      | cons r rs ih =>
          simp only [List.map_cons, List.foldl_cons]
          rw [plus_zero_left, foldl_plus_acc, ih, summarize_cons]
    rw [hfold]
    simpa [plus_zero_left, hsum]

private theorem plus_comm (a b : Totals) : plus a b = plus b a := by cases a; cases b; simp [plus]; omega
private theorem weighted_zero (v : Option Int) : weighted 0 v = zero := by cases v <;> simp [weighted, zero]
private theorem foldl_contrib_acc {α : Type} (xs : List α) (g : α → Totals) (acc : Totals) :
    xs.foldl (fun a x => plus a (g x)) acc = plus acc (xs.foldl (fun a x => plus a (g x)) zero) := by
  induction xs generalizing acc with
  | nil => simp [plus_zero_right]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih, ih (plus zero (g x))]
      rw [plus_zero_left]
      exact plus_assoc _ _ _
private theorem summarize_append (xs ys : List (Option Int)) :
    summarize (xs ++ ys) = plus (summarize xs) (summarize ys) := by
  dsimp [summarize]
  rw [List.foldl_append]
  exact foldl_plus_eq ys (xs.foldl (fun a v => plus a (weighted 1 v)) zero)
private theorem histogram_foldl_getD {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    [LawfulBEq α] (xs : List α) (m : Std.HashMap α Nat) (k : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) m).getD k 0 = m.getD k 0 + (xs.filter (fun x => x == k)).length := by
  induction xs generalizing m with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih (m.insert x (m.getD x 0 + 1))]
      rw [Std.HashMap.getD_insert]
      by_cases h : (x == k) = true
      · rw [if_pos h]
        have hxk : x = k := LawfulBEq.eq_of_beq h
        subst k
        simp [ih, ReflBEq.rfl]
        omega
      · rw [if_neg h]
        simp [h]
private theorem hist_getD_count {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    [LawfulBEq α] (xs : List α) (k : α) :
    (histogram xs).getD k 0 = (xs.filter (fun x => x == k)).length := by
  simpa [histogram] using histogram_foldl_getD xs ({} : Std.HashMap α Nat) k
private theorem countP_eraseDups_append_singleton (p : Nat → Bool) (l : List Nat) (a : Nat) (ha : a ∉ l) :
    (l ++ [a]).eraseDups.countP p = l.eraseDups.countP p + (if p a then 1 else 0) := by
  rw [List.eraseDups_append]
  have hnot : a ∉ l.eraseDups := by intro h; exact ha (List.mem_eraseDups.mp h)
  simp [ha, List.removeAll, List.countP_append, List.eraseDups_cons]
private theorem countP_eraseDups_append_of_mem (p : Nat → Bool) (l : List Nat) (a : Nat) (ha : a ∈ l) :
    (l ++ [a]).eraseDups.countP p = l.eraseDups.countP p := by
  rw [List.eraseDups_append]
  simp [ha, List.removeAll]
private theorem countP_eraseDups_cons_of_mem (p : Nat → Bool) (l : List Nat) (a : Nat) (ha : a ∈ l) :
    (a :: l).eraseDups.countP p = l.eraseDups.countP p := by
  have main : ∀ n, ∀ (l : List Nat), l.length = n → ∀ (a : Nat), a ∈ l →
      (a :: l).eraseDups.countP p = l.eraseDups.countP p := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro l hlen a ha
        cases l with
        | nil => simp at ha
        | cons b bs =>
            by_cases hba : (b == a) = true
            · have hbae : b = a := LawfulBEq.eq_of_beq hba
              subst b
              simp [List.eraseDups_cons]
            · have ha_bs : a ∈ bs := by
                rcases List.mem_cons.mp ha with haeq | htail
                · exact False.elim (hba (beq_iff_eq.mpr haeq.symm))
                · exact htail
              have hab_false : (a == b) = false := by cases h : a == b <;> simp_all
              have ha_filter : a ∈ bs.filter (fun x => !(x == b)) := by
                simp [ha_bs, hab_false]
              have hlen' : (bs.filter (fun x => !(x == b))).length < n := by
                simp only [List.length_cons] at hlen
                have : (bs.filter (fun x => !(x == b))).length ≤ bs.length := List.length_filter_le _ _
                omega
              have ih' := ih _ hlen' (bs.filter (fun x => !(x == b))) rfl a ha_filter
              simp [List.eraseDups_cons, hba] at ih' ⊢
              by_cases hpa : p a = true <;> by_cases hpb : p b = true <;>
                simp [hpa, hpb, Bool.and_comm, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] at ih' ⊢
              all_goals omega
  exact main l.length l rfl a ha
private theorem hm_foldl_getD_if {α K : Type} [BEq K] [Hashable K] [EquivBEq K] [LawfulHashable K] [LawfulBEq K]
    (xs : List α) (m0 : Std.HashMap K Totals) (key : α → K) (contrib : α → Totals) (cond : α → Bool) (k : K) :
    (xs.foldl (fun m x => if cond x then m.insert (key x) (plus (m.getD (key x) zero) (contrib x)) else m) m0).getD k zero =
      plus (m0.getD k zero) (((xs.filter fun x => cond x && (key x == k)).map contrib).foldl plus zero) := by
  induction xs generalizing m0 with
  | nil => simp [plus_zero_left, plus_zero_right]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih]
      by_cases hc : (cond x) = true
      · simp only [hc, ↓reduceIte]
        rw [Std.HashMap.getD_insert]
        by_cases hk : (key x == k) = true
        · rw [if_pos hk]
          have hkeq : key x = k := LawfulBEq.eq_of_beq hk
          subst k
          have hfacc := foldl_contrib_acc ((xs.filter fun x_1 => cond x_1 && (key x_1 == key x)).map contrib)
            (fun z : Totals => z) (plus zero (contrib x))
          simp [hc, hk, plus_assoc]
          rw [hfacc]
          simp [plus_zero_left]
        · rw [if_neg hk]
          simp [hk]
      · have hcf : cond x = false := by cases hb : cond x <;> simp_all
        simp [hcf]
private theorem fold_cf_skip_zero (xs : List Visit) (cf : Nat → Nat) :
    xs.foldl (fun acc r => plus acc (weighted (cf r.1) r.2.2)) zero =
      (xs.filter (fun r => !(cf r.1 == 0))).foldl (fun acc r => plus acc (weighted (cf r.1) r.2.2)) zero := by
  induction xs with
  | nil => rfl
  | cons r rs ih =>
      simp only [List.foldl_cons]
      by_cases h : (cf r.1 == 0) = true
      · have hz : cf r.1 = 0 := by cases hb : cf r.1 <;> simp_all
        simp [h, hz, weighted_zero, plus_zero_left, ih]
      · have hf : (cf r.1 == 0) = false := by cases hb : cf r.1 == 0 <;> simp_all
        have hfilt : (r :: rs).filter (fun z => !(cf z.1 == 0)) =
            r :: rs.filter (fun z => !(cf z.1 == 0)) := by simp [hf]
        rw [hfilt]
        simp only [List.foldl_cons]
        have hfacc := foldl_contrib_acc (rs.filter (fun z => !(cf z.1 == 0)))
          (fun r => weighted (cf r.1) r.2.2) (plus zero (weighted (cf r.1) r.2.2))
        rw [hfacc]
        have hacc := foldl_contrib_acc rs (fun r => weighted (cf r.1) r.2.2)
          (plus zero (weighted (cf r.1) r.2.2))
        rw [hacc]
        rw [ih]

private def cfWeight (dict : Array (Option String)) (ranks : List (Option String × Int)) (code : Nat) : Nat :=
  let build := histogram ((ranks.filter (fun r => 40 < r.2)).filterMap Prod.fst)
  let url := dict[code]?.join
  url.elim 0 (build.getD · 0)

private theorem cf_filter_count (ranks : List (Option String × Int)) (url : String) :
    (((ranks.filter (fun r => 40 < r.2)).filterMap Prod.fst).filter (fun s => s == url)).length =
    (ranks.filter (fun r => r.1 == some url && 40 < r.2)).length := by
  induction ranks with
  | nil => simp
  | cons r rs ih =>
      by_cases hk : (r.1 == some url) = true
      · have hkeq : r.1 = some url := LawfulBEq.eq_of_beq hk
        rcases r with ⟨s, rank⟩
        simp only at hkeq
        subst s
        by_cases hr : 40 < rank <;> simp [hr, hk, ih]
      · have hkf : (r.1 == some url) = false := by cases hb : r.1 == some url <;> simp_all
        rcases r with ⟨s, rank⟩
        cases s with
        | none => by_cases hr : 40 < rank <;> simp [hr, hkf, ih]
        | some s' =>
            have hne : s' ≠ url := by
              intro hsu
              have hf : False := by simpa [hsu] using hkf
              exact hf
            have hbf : (s' == url) = false := by cases hb : s' == url <;> simp_all
            by_cases hr : 40 < rank <;> simp [hr, hkf, hbf, ih]

private theorem cfWeight_eq_count (dict : Array (Option String)) (ranks : List (Option String × Int)) (code : Nat) :
    cfWeight dict ranks code =
      (ranks.filter (fun r => r.1.isSome && r.1 == dict[code]?.join && 40 < r.2)).length := by
  dsimp [cfWeight]
  cases hopt : dict[code]?.join with
  | none =>
      have hzero : ∀ r : Option String × Int, (r.1.isSome && r.1.isNone && 40 < r.2) = false := by
        intro r
        cases r.1 <;> simp
      simp [hopt, hzero]
      induction ranks with
      | nil => simp
      | cons r rs ih => simp [hzero, ih]
  | some url =>
      simp [hopt]
      rw [hist_getD_count]
      rw [cf_filter_count]
      congr 1
      apply List.filter_congr
      intro r hr
      by_cases h : (r.1 == some url) = true
      · have heq : r.1 = some url := LawfulBEq.eq_of_beq h
        rcases r with ⟨s, rank⟩
        simp only at heq
        subst s
        simp
      · have hf : (r.1 == some url) = false := by cases hb : r.1 == some url <;> simp_all
        simp [hf]

private def cfStep (dict : Array (Option String)) (ranks : List (Option String × Int))
    (st : Std.HashMap Nat Nat × Std.HashMap String Totals × Nat) (visit : Visit) :=
  let (cache,gs,n) := st
  let (code,c,v) := visit
  let (next,w,calls) := match cache[code]? with
    | some w => (cache,w,0)
    | none =>
      let url := dict[code]?.join
      let w := url.elim 0 ((histogram ((ranks.filter (fun q : Option String × Int => 40 < q.2)).filterMap Prod.fst)).getD · 0)
      (cache.insert code w,w,url.isSome.toNat)
  let gs := if w == 0 then gs else gs.insert c (plus (gs.getD c zero) (weighted w v))
  (next,gs,n+calls)


private def cfOk (dict : Array (Option String)) (ranks : List (Option String × Int))
    (processed : List Visit) (st : Std.HashMap Nat Nat × Std.HashMap String Totals × Nat) : Prop :=
  let (cache,gs,n) := st
  (∀ code : Nat, cache[code]? =
      if code ∈ processed.map Prod.fst then some (cfWeight dict ranks code) else none) ∧
  (∀ country : String, gs.getD country zero =
      (processed.filter (fun r => r.2.1 == country)).foldl
        (fun acc r => plus acc (weighted (cfWeight dict ranks r.1) r.2.2)) zero) ∧
  n = (processed.map Prod.fst).eraseDups.countP (fun i => dict[i]?.join.isSome)

private theorem cfStep_preserves (dict : Array (Option String)) (ranks : List (Option String × Int))
    (processed : List Visit) (r : Visit) (st : Std.HashMap Nat Nat × Std.HashMap String Totals × Nat)
    (h : cfOk dict ranks processed st) :
    cfOk dict ranks (processed ++ [r]) (cfStep dict ranks st r) := by
  rcases st with ⟨cache, gs, n⟩
  rcases r with ⟨code,c,v⟩
  dsimp [cfOk] at h ⊢
  dsimp [cfStep]
  rcases h with ⟨hcache, hgs, hn⟩
  simp only [List.map_append, List.map_singleton]
  by_cases hmem : code ∈ processed.map Prod.fst
  · have hcval : cache[code]? = some (cfWeight dict ranks code) := by simpa [hmem] using hcache code
    simp only [hcval, ↓reduceIte]
    constructor
    · intro code'
      by_cases hc' : (code' == code) = true
      · have heq : code' = code := beq_iff_eq.mp hc'
        subst code'
        simpa [hmem] using hcache code
      · have hmem' : code' ∈ (code :: processed.map Prod.fst) ↔ code' ∈ processed.map Prod.fst := by
          constructor
          · intro hh
            rcases List.mem_cons.mp hh with hhead | htail
            · exact False.elim (hc' (beq_iff_eq.mpr hhead))
            · exact htail
          · intro htail
            exact List.mem_cons.mpr (Or.inr htail)
        by_cases hm' : code' ∈ processed.map Prod.fst
        · simp [hmem', hm', hcache code']
        · have hcval' : cache[code']? = none := by simpa [hm'] using hcache code'
          have hne : code' ≠ code := by intro h; exact hc' (beq_iff_eq.mpr h)
          simp [hmem', hm', hcval', hne]
    · constructor
      · intro country
        by_cases hc : (c == country) = true
        · have hce : c = country := beq_iff_eq.mp hc
          subst country
          by_cases hw : (cfWeight dict ranks code == 0) = true
          · have hw0 : cfWeight dict ranks code = 0 := by
              cases hb : cfWeight dict ranks code <;> simp_all
            simp [hw, hw0, weighted_zero, plus_zero_right]
            simpa using hgs c
          · simp [hw]
            rw [hgs]
        · have hcf : (c == country) = false := by cases hb : c == country <;> simp_all
          by_cases hw : (cfWeight dict ranks code == 0) = true
          · simp [hw, hcf, List.filter_append, hgs]
          · simp [hw, hcf, List.filter_append]
            rw [Std.HashMap.getD_insert]
            rw [if_neg hc]
            rw [hgs]
      · simpa [List.append_assoc, countP_eraseDups_append_of_mem
          (fun i => dict[i]?.join.isSome) (processed.map Prod.fst) code hmem] using hn
  · have hcval : cache[code]? = none := by simpa [hmem] using hcache code
    have hwdef : (dict[code]?.join.elim 0 ((histogram
        ((ranks.filter (fun q : Option String × Int => 40 < q.2)).filterMap Prod.fst)).getD · 0)) =
        cfWeight dict ranks code := rfl
    simp [hcval]
    constructor
    · intro code'
      by_cases hc' : (code == code') = true
      · have heq : code = code' := beq_iff_eq.mp hc'
        subst code'
        simp [cfWeight]
      · simp only [Std.HashMap.getElem?_insert, hc', ↓reduceIte]
        have hmem' : code' ∈ (code :: processed.map Prod.fst) ↔ code' ∈ processed.map Prod.fst := by
          constructor
          · intro hh
            rcases List.mem_cons.mp hh with hhead | htail
            · have heq : code = code' := hhead.symm
              exact False.elim (hc' (beq_iff_eq.mpr heq))
            · exact htail
          · intro htail
            exact List.mem_cons.mpr (Or.inr htail)
        by_cases hm' : code' ∈ processed.map Prod.fst
        · simp [hmem', hm', hcache code']
        · have hcval' : cache[code']? = none := by simpa [hm'] using hcache code'
          have hne : code' ≠ code := by intro h; exact hc' (beq_iff_eq.mpr h.symm)
          simp [hmem', hm', hcval', hne]
    · constructor
      · intro country
        by_cases hc : (c == country) = true
        · have hce : c = country := beq_iff_eq.mp hc
          subst country
          by_cases hw : (cfWeight dict ranks code == 0) = true
          · have hw0 : cfWeight dict ranks code = 0 := by
              cases hb : cfWeight dict ranks code <;> simp_all
            simp [hwdef, hw, hw0, weighted_zero, plus_zero_right]
            simpa using hgs c
          · have hwne : cfWeight dict ranks code ≠ 0 := by
              intro hz
              apply hw
              simp [hz]
            simp [hwdef, hwne]
            rw [hgs]
        · have hcf : (c == country) = false := by cases hb : c == country <;> simp_all
          by_cases hw : (cfWeight dict ranks code == 0) = true
          · have hw0 : cfWeight dict ranks code = 0 := by
              cases hb : cfWeight dict ranks code <;> simp_all
            simp [hwdef, hw0, hcf, List.filter_append, hgs]
          · have hwne : cfWeight dict ranks code ≠ 0 := by
              intro hz
              apply hw
              simp [hz]
            simp [hwdef, hwne, hcf, List.filter_append]
            rw [Std.HashMap.getD_insert]
            rw [if_neg hc]
            rw [hgs]
      · rw [countP_eraseDups_append_singleton (fun i => dict[i]?.join.isSome)
          (processed.map Prod.fst) code hmem]
        rw [hn]
        cases hb : dict[code]?.join.isSome <;> simp [hb]


private theorem cf_fold_preserves (dict : Array (Option String)) (ranks : List (Option String × Int)) :
    ∀ (vs : List Visit) (st : Std.HashMap Nat Nat × Std.HashMap String Totals × Nat)
      (processed : List Visit),
      cfOk dict ranks processed st →
      cfOk dict ranks (processed ++ vs) (vs.foldl (cfStep dict ranks) st) := by
  intro vs
  induction vs with
  | nil =>
      intro st processed h
      simpa using h
  | cons r rs ih =>
      intro st processed h
      simp only [List.foldl_cons]
      have hstep := cfStep_preserves dict ranks processed r st h
      have htail := ih (cfStep dict ranks st r) (processed ++ [r]) hstep
      simpa [List.append_assoc] using htail

private theorem translate_fold_invariants (dict : Array (Option String)) (ranks : List (Option String × Int))
    (vs : List Visit) :
    cfOk dict ranks vs (vs.foldl (cfStep dict ranks)
      (({} : Std.HashMap Nat Nat), ({} : Std.HashMap String Totals), 0)) := by
  have h0 : cfOk dict ranks [] (({} : Std.HashMap Nat Nat), ({} : Std.HashMap String Totals), 0) := by
    simp [cfOk, cfWeight]
  have h := cf_fold_preserves dict ranks vs
    (({} : Std.HashMap Nat Nat), ({} : Std.HashMap String Totals), 0) [] h0
  simpa using h

private theorem map_const_eq_replicate {α β : Type} (N : List α) (v : β) :
    N.map (fun _ => v) = List.replicate N.length v := by
  induction N with
  | nil => rfl
  | cons n Ns ih => simp [List.replicate_succ, ih]
private theorem weighted_add (a b : Nat) (v : Option Int) :
    plus (weighted a v) (weighted b v) = weighted (a + b) v := by
  cases v <;> simp [plus, weighted, Int.add_mul, Int.add_comm, Int.add_left_comm, Int.add_assoc]
private theorem summarize_replicate (n : Nat) (v : Option Int) :
    summarize (List.replicate n v) = weighted n v := by
  induction n with
  | zero => simp [summarize, weighted, zero]
  | succ n ih =>
      simp only [List.replicate_succ]
      rw [summarize_cons, ih]
      simpa [Nat.add_comm] using weighted_add 1 n v
private theorem summarize_map_const {α : Type} (l : List α) (v : Option Int) :
    summarize (l.map fun _ => v) = weighted l.length v := by
  rw [map_const_eq_replicate, summarize_replicate]
private theorem summarize_flatMap {α : Type} (xs : List α) (f : α → List (Option Int)) :
    summarize (xs.flatMap f) =
      xs.foldl (fun acc x => plus acc (summarize (f x))) zero := by
  induction xs with
  | nil => simp [summarize]
  | cons x xs ih =>
      simp only [List.flatMap_cons, List.foldl_cons]
      rw [summarize_append, ih]
      have hz : plus zero (summarize (f x)) = summarize (f x) := plus_zero_left _
      rw [hz]
      have hacc := foldl_contrib_acc xs (fun x => summarize (f x)) (summarize (f x))
      rw [← hacc]
private theorem foldl_congr_contrib {α : Type} (xs : List α) (g h : α → Totals) (acc : Totals)
    (hgh : ∀ x ∈ xs, g x = h x) :
    xs.foldl (fun a x => plus a (g x)) acc = xs.foldl (fun a x => plus a (h x)) acc := by
  induction xs generalizing acc with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [hgh x (by simp)]
      rw [ih (plus acc (h x)) (fun y hy => hgh y (by simp [hy]))]
private theorem code_summarize (dict : Array (Option String)) (ranks : List (Option String × Int)) (r : Visit) :
    summarize ((ranks.filter (fun (s,rank) => s.isSome && s == dict[r.1]?.join && rank > 40)).map
      (fun _ => r.2.2)) = weighted (cfWeight dict ranks r.1) r.2.2 := by
  rw [summarize_map_const, cfWeight_eq_count]

private theorem translate_eq_cfStep (dict : Array (Option String)) (ranks : List (Option String × Int))
    (vs : List Visit) :
    translate dict ranks vs =
      (let st := vs.foldl (cfStep dict ranks) (({} : Std.HashMap Nat Nat),
        ({} : Std.HashMap String Totals), 0); (st.2.1, st.2.2)) := by
  rfl

theorem CodeFanout_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels.CodeFanout.Goal := by
  intro dict ranks vs country
  dsimp only
  have hok := translate_fold_invariants dict ranks vs
  dsimp [cfOk] at hok
  rcases hok with ⟨hcache, hgs, hn⟩
  constructor
  · rw [translate_eq_cfStep]
    dsimp
    rw [hgs country]
    rw [summarize_flatMap]
    apply foldl_congr_contrib
    intro r hr
    exact (code_summarize dict ranks r).symm
  · rw [translate_eq_cfStep]
    dsimp
    exact hn

private def wordsOf (rows : List Search) : List String :=
  (rows.filter (fun r => r.1 != some "US")).filterMap (fun r => r.2.1)
private def keptOf (rows : List Search) : List Search :=
  rows.filter (fun (c,w,_) => c == some "US" || w.any (·.contains "data"))

private def rwOk (rows : List Search) (st : Std.HashMap String Bool × Totals × Nat) : Prop :=
  let (cache,total,n) := st
  (∀ w : String, cache[w]? = if w ∈ wordsOf rows then some (w.contains "data") else none) ∧
  total = (keptOf rows).foldl (fun acc r => plus acc (weighted 1 r.2.2)) zero ∧
  n = (wordsOf rows).eraseDups.length

private def rwStep (st : Std.HashMap String Bool × Totals × Nat) (r : Search) :=
  let (cache,total_,n) := st
  let (country,word,v) := r
  if country == some "US" then (cache,plus total_ (weighted 1 v),n) else
    match word with
    | none => (cache,total_,n)
    | some w => match cache[w]? with
      | some ok => (cache,if ok then plus total_ (weighted 1 v) else total_,n)
      | none =>
        let ok := w.contains "data"
        (cache.insert w ok,if ok then plus total_ (weighted 1 v) else total_,n+1)

private theorem eraseDups_append_of_mem_len [BEq String] [LawfulBEq String]
    (l : List String) (a : String) (ha : a ∈ l) :
    (l ++ [a]).eraseDups.length = l.eraseDups.length := by
  rw [List.eraseDups_append]
  simp [ha, List.removeAll]

private theorem eraseDups_append_singleton_len [BEq String] [LawfulBEq String]
    (l : List String) (a : String) (ha : a ∉ l) :
    (l ++ [a]).eraseDups.length = l.eraseDups.length + 1 := by
  rw [List.eraseDups_append]
  have hnot : a ∉ l.eraseDups := by intro h; exact ha (List.mem_eraseDups.mp h)
  simp [ha, List.removeAll, List.eraseDups_cons]

private theorem rwStep_preserves (rows : List Search) (r : Search)
    (st : Std.HashMap String Bool × Totals × Nat) (h : rwOk rows st) :
    rwOk (rows ++ [r]) (rwStep st r) := by
  rcases st with ⟨cache,total,n⟩
  rcases r with ⟨country,word,v⟩
  dsimp [rwOk] at h ⊢
  dsimp [rwStep]
  rcases h with ⟨hcache, htotal, hn⟩
  by_cases hus : (country == some "US") = true
  · simp [hus]
    have hke : keptOf (rows ++ [(country,word,v)]) =
        keptOf rows ++ [(country,word,v)] := by
      dsimp [keptOf]
      simp [List.filter_append, hus]
    have hwo : wordsOf (rows ++ [(country,word,v)]) = wordsOf rows := by
      dsimp [wordsOf]
      have hne : (country != some "US") = false := by
        cases hb : country == some "US" <;> simp_all
      simp [List.filter_append, hne]
    constructor
    · simpa [hwo] using hcache
    · constructor
      · rw [hke]
        simp only [List.foldl_append, List.foldl_cons]
        rw [htotal]
        rw [foldl_contrib_acc]
        simp [plus_zero_left]
      · simpa [hwo] using hn
  · have hneus : (country == some "US") = false := by cases hb : country == some "US" <;> simp_all
    have hne_bool : (country != some "US") = true := by cases hb : country == some "US" <;> simp_all
    cases word with
    | none =>
        simp [hus]
        have hke : keptOf (rows ++ [(country,none,v)]) = keptOf rows := by
          dsimp [keptOf]
          simp [List.filter_append, hus]
        have hwo : wordsOf (rows ++ [(country,none,v)]) = wordsOf rows := by
          dsimp [wordsOf]
          simp [List.filter_append, hne_bool]
        constructor
        · simpa [hwo] using hcache
        · constructor
          · simpa [hke] using htotal
          · simpa [hwo] using hn
    | some w =>
        by_cases hm : w ∈ wordsOf rows
        · have hcval : cache[w]? = some (w.contains "data") := by simpa [hm] using hcache w
          simp [hus, hcval]
          have hwo : wordsOf (rows ++ [(country,some w,v)]) = wordsOf rows ++ [w] := by
            dsimp [wordsOf]
            simp [List.filter_append, hne_bool]
          constructor
          · intro w'
            by_cases hw' : (w' == w) = true
            · have heq : w' = w := beq_iff_eq.mp hw'
              subst w'
              simpa [hwo, hm] using hcval
            · have hmem' : w' ∈ w :: wordsOf rows ↔ w' ∈ wordsOf rows := by
                constructor
                · intro hh
                  rcases List.mem_cons.mp hh with hh' | ht
                  · exact False.elim (hw' (beq_iff_eq.mpr hh'))
                  · exact ht
                · intro ht
                  exact List.mem_cons.mpr (Or.inr ht)
              by_cases hm' : w' ∈ wordsOf rows
              · simp [hwo, hm', hmem', hcache w']
              · have hcval' : cache[w']? = none := by simpa [hm'] using hcache w'
                have hne : w' ≠ w := by intro h; exact hm' (by simpa [h] using hm)
                simp [hwo, hm', hmem', hcval', hne]
          · constructor
            · by_cases hok : w.contains "data" = true
              · have hokc : ['d','a','t','a'] <:+: w.toList := by
                  simpa [String.contains] using hok
                have hs : (some w).any (fun s : String => s.contains "data") = true := by simpa using hok
                have hke : keptOf (rows ++ [(country,some w,v)]) =
                    keptOf rows ++ [(country,some w,v)] := by
                  dsimp [keptOf]
                  simp [List.filter_append, hus, hs]
                rw [hke]
                simp only [List.foldl_append, List.foldl_cons]
                rw [htotal]
                rw [foldl_contrib_acc]
                simp [hok, hokc, plus_zero_left]
              · have hokf : (w.contains "data") = false := by cases hb : w.contains "data" <;> simp_all
                have hokcf : ¬ ['d','a','t','a'] <:+: w.toList := by
                  simpa [String.contains] using hokf
                have hke : keptOf (rows ++ [(country,some w,v)]) = keptOf rows := by
                  dsimp [keptOf]
                  simp [List.filter_append, hus, hokf]
                simp [hokf, hokcf, hke, htotal]
            · rw [hwo, eraseDups_append_of_mem_len (wordsOf rows) w hm]
              exact hn
        · have hcval : cache[w]? = none := by simpa [hm] using hcache w
          simp [hus, hcval]
          have hwo : wordsOf (rows ++ [(country,some w,v)]) = wordsOf rows ++ [w] := by
            dsimp [wordsOf]
            simp [List.filter_append, hne_bool, hm]
          constructor
          · intro w'
            by_cases hw' : (w == w') = true
            · have heq : w = w' := beq_iff_eq.mp hw'
              subst w'
              simp [hwo]
            · by_cases hw' : (w == w') = true
              · have heq : w = w' := beq_iff_eq.mp hw'
                subst w'
                simp [hwo]
              · have hcf' : (w == w') = false := by cases hb : w == w' <;> simp_all
                simp only [Std.HashMap.getElem?_insert, hcf', ↓reduceIte]
                by_cases hm' : w' ∈ wordsOf rows
                · simp [hwo, hm', hcache w']
                · have hcval' : cache[w']? = none := by simpa [hm'] using hcache w'
                  have hne : w' ≠ w := by intro h; exact hw' (beq_iff_eq.mpr h.symm)
                  simp [hwo, hm', hcval', List.mem_append, hne]
          · constructor
            · by_cases hok : w.contains "data" = true
              · have hokc : ['d','a','t','a'] <:+: w.toList := by
                  simpa [String.contains] using hok
                have hs : (some w).any (fun s : String => s.contains "data") = true := by simpa using hok
                have hke : keptOf (rows ++ [(country,some w,v)]) =
                    keptOf rows ++ [(country,some w,v)] := by
                  dsimp [keptOf]
                  simp [List.filter_append, hus, hs]
                rw [hke]
                simp only [List.foldl_append, List.foldl_cons]
                rw [htotal]
                rw [foldl_contrib_acc]
                simp [hok, hokc, plus_zero_left]
              · have hokf : (w.contains "data") = false := by cases hb : w.contains "data" <;> simp_all
                have hokcf : ¬ ['d','a','t','a'] <:+: w.toList := by
                  simpa [String.contains] using hokf
                have hke : keptOf (rows ++ [(country,some w,v)]) = keptOf rows := by
                  dsimp [keptOf]
                  simp [List.filter_append, hus, hokf]
                simp [hokf, hokcf, hke, htotal]
            · rw [hwo]
              rw [eraseDups_append_singleton_len]
              · rw [hn]
              · exact hm

private theorem rw_fold_preserves :
    ∀ (rows : List Search) (st : Std.HashMap String Bool × Totals × Nat)
      (processed : List Search),
      rwOk processed st → rwOk (processed ++ rows) (rows.foldl rwStep st) := by
  intro rows
  induction rows with
  | nil =>
      intro st processed h
      simpa using h
  | cons r rs ih =>
      intro st processed h
      simp only [List.foldl_cons]
      have hstep := rwStep_preserves processed r st h
      have htail := ih (rwStep st r) (processed ++ [r]) hstep
      simpa [List.append_assoc] using htail

private theorem searchAgg_eq_rwFold (rows : List Search) :
    searchAgg rows = (let st := rows.foldl rwStep (({} : Std.HashMap String Bool), zero, 0); (st.2.1, st.2.2)) := by
  rfl

private theorem fold_snd_eq_summarize (rows : List Search) :
    rows.foldl (fun acc r => plus acc (weighted 1 r.2.2)) zero =
      summarize (rows.map fun r => r.2.2) := by
  induction rows with
  | nil => simp [summarize]
  | cons r rs ih =>
      simp only [List.foldl_cons, List.map_cons]
      rw [foldl_contrib_acc, ih]
      simp [summarize_cons, plus_zero_left]

theorem ResidualWords_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels.ResidualWords.Goal := by
  intro rows
  dsimp only
  have hok := rw_fold_preserves rows (({} : Std.HashMap String Bool), zero, 0) [] (by
    simp [rwOk, wordsOf, keptOf, cfWeight])
  dsimp [rwOk] at hok
  rcases hok with ⟨hcache, htotal, hn⟩
  rw [searchAgg_eq_rwFold]
  dsimp
  apply Prod.ext
  · rw [htotal]
    exact fold_snd_eq_summarize _
  · simpa [wordsOf] using hn
private theorem plus_left_comm (a b c : Totals) : plus a (plus b c) = plus b (plus a c) := by cases a; cases b; cases c; simp [plus]; omega
private def scaleT (w : Nat) (a : Totals) : Totals := ⟨a.cnt * w, a.nn * w, a.sum * w⟩
private theorem scale_zero (w : Nat) : scaleT w zero = zero := by simp [scaleT, zero]
private theorem scale_add (w : Nat) (a b : Totals) : scaleT w (plus a b) = plus (scaleT w a) (scaleT w b) := by cases a; cases b; simp [scaleT, plus, Nat.add_mul, Int.add_mul]

private theorem foldl_scaled_acc {K : Type} (l : List (K × Totals)) (p : K → Bool) (f : K → Nat) (acc : Totals) :
    l.foldl (fun a q => if p q.1 then plus a (scaleT (f q.1) q.2) else a) acc =
      plus acc (l.foldl (fun a q => if p q.1 then plus a (scaleT (f q.1) q.2) else a) zero) := by
  induction l generalizing acc with
  | nil => simp [plus_zero_right]
  | cons q qs ih =>
      simp only [List.foldl_cons]
      by_cases hp : p q.1 = true
      · simp only [hp, ↓reduceIte]
        rw [ih (plus acc (scaleT (f q.1) q.2)), ih (plus zero (scaleT (f q.1) q.2))]
        rw [plus_zero_left]
        exact plus_assoc _ _ _
      · simp [hp]
        rw [ih]

private theorem foldl_scaled_decomp {K : Type} [BEq K] [LawfulBEq K]
    (l : List (K × Totals)) (k : K) (p : K → Bool) (f : K → Nat)
    (hu : l.Pairwise (fun a b => (a.1 == b.1) = false)) :
    l.foldl (fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc) zero =
      plus (match l.find? (fun q => q.1 == k) with
              | some q => if p q.1 then scaleT (f q.1) q.2 else zero
              | none => zero)
        ((l.filter (fun q => !(q.1 == k))).foldl
          (fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc) zero) := by
  induction l with
  | nil => simp [plus_zero_left]
  | cons q qs ih =>
      rcases q with ⟨k', v⟩
      rw [List.pairwise_cons] at hu
      have hu_head : ∀ z ∈ qs, (k' == z.1) = false := hu.1
      have hu_tail : qs.Pairwise (fun a b => (a.1 == b.1) = false) := hu.2
      have ih' := ih hu_tail
      by_cases h : (k' == k) = true
      · have hkn : ∀ z ∈ qs, (z.1 == k) = false := by
          intro z hz
          cases hzk : z.1 == k
          · rfl
          · exfalso
            have hkz_eq : k' = z.1 := by rw [LawfulBEq.eq_of_beq h, LawfulBEq.eq_of_beq hzk]
            have hkz_true : (k' == z.1) = true := beq_iff_eq.mpr hkz_eq
            have hkz : (k' == z.1) = false := hu_head z hz
            simp [hkz_true] at hkz
        have hfilt : qs.filter (fun z => !(z.1 == k)) = qs := by
          apply List.filter_eq_self.mpr
          intro z hz
          simp [hkn z hz]
        have hfind : qs.find? (fun z => z.1 == k) = none := by
          cases hfq : qs.find? (fun z => z.1 == k) with
          | none => rfl
          | some z =>
              have hzmem : z ∈ qs := List.mem_of_find?_eq_some hfq
              have hzfalse := hkn z hzmem
              have hzpred : (z.1 == k) = true := (List.find?_eq_some_iff_append.mp hfq).1
              exact False.elim (by simpa [hzpred] using hzfalse)
        simp only [List.foldl_cons]
        simp [h, hfilt]
        rw [foldl_scaled_acc]
        rw [ih']
        simp [plus_zero_left]
      · have hf : (k' == k) = false := by cases hb : k' == k <;> simp_all
        simp only [List.foldl_cons]
        simp [hf]
        rw [foldl_scaled_acc]
        rw [ih']
        by_cases hp' : p k' = true
        · simp only [hp', ↓reduceIte, plus_zero_left]
          have hfacc := foldl_scaled_acc (qs.filter (fun z => !(z.1 == k))) p f (scaleT (f k') v)
          rw [hfacc]
          by_cases hp : p k = true <;> simp [hp', hp, plus_zero_left, plus_assoc, plus_comm, plus_left_comm]
        · by_cases hp : p k = true <;> simp [hp', hp, plus_zero_left, plus_assoc, plus_comm, plus_left_comm]

private theorem beq_comm_bool {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  by_cases h : (a == b) = true
  · have hab : a = b := LawfulBEq.eq_of_beq h
    have hba : (b == a) = true := beq_iff_eq.mpr hab.symm
    simp [h, hba]
  · have hf : (a == b) = false := by cases hb : a == b <;> simp_all
    have hba : (b == a) = false := by
      cases hb : b == a
      · rfl
      · have : a = b := (LawfulBEq.eq_of_beq hb).symm
        exact False.elim (h (beq_iff_eq.mpr this))
    simp [hf, hba]

private theorem hm_foldl_scaled_insert {K : Type} [BEq K] [Hashable K] [EquivBEq K] [LawfulHashable K] [LawfulBEq K]
    (m : Std.HashMap K Totals) (k : K) (d : Totals) (p : K → Bool) (f : K → Nat) :
    let m' := m.insert k (plus (m.getD k zero) d)
    m'.toList.foldl (fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc) zero =
      plus (m.toList.foldl (fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc) zero)
        (if p k then scaleT (f k) d else zero) := by
  dsimp only
  let new := plus (m.getD k zero) d
  have hpperm := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := new)
  have hcomm : ∀ x ∈ (m.insert k new).toList, ∀ y ∈ (m.insert k new).toList, ∀ z,
      (fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc)
        ((fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc) z x) y =
      (fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc)
        ((fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc) z y) x := by
    intro x hx y hy z
    by_cases hx' : p x.1 = true <;> by_cases hy' : p y.1 = true <;>
      simp [hx', hy', plus_assoc, plus_comm, plus_left_comm]
  rw [hpperm.foldl_eq' hcomm zero]
  have hdecomp := foldl_scaled_decomp m.toList k p f (Std.HashMap.distinct_keys_toList (m := m))
  rw [hdecomp]
  have hfindc : (match m.toList.find? (fun q => q.1 == k) with
              | some q => if p q.1 then scaleT (f q.1) q.2 else zero
              | none => zero) =
      (if p k then scaleT (f k) (m.getD k zero) else zero) := by
    cases hg : m[k]? with
    | none =>
        have hnotmem : ¬ k ∈ m := by
          intro hmem
          have his : (m[k]?).isSome := (Std.HashMap.mem_iff_isSome_getElem?).mp hmem
          simp [hg] at his
        have hfind : m.toList.find? (fun q => q.1 == k) = none :=
          (Std.HashMap.find?_toList_eq_none_iff_not_mem (m := m) (k := k)).mpr hnotmem
        have hgd : m.getD k zero = zero := by rw [Std.HashMap.getD_eq_getD_getElem?, hg]; rfl
        simp [hfind, hgd, scale_zero]
    | some v =>
        have hmem : k ∈ m := (Std.HashMap.mem_iff_isSome_getElem?).mpr (by rw [hg]; simp)
        have hgetkey : m.getKey? k = some k := Std.HashMap.getKey?_eq_some hmem
        have hfind : m.toList.find? (fun q => q.1 == k) = some (k, v) := by
          rw [Std.HashMap.find?_toList_eq_some_iff_getKey?_eq_some_and_getElem?_eq_some]
          exact ⟨hgetkey, hg⟩
        have hgd : m.getD k zero = v := by rw [Std.HashMap.getD_eq_getD_getElem?, hg]; rfl
        simp [hfind, hgd]
  rw [hfindc]
  dsimp [new]
  rw [foldl_scaled_acc]
  by_cases hp : p k = true
  · simp only [hp, ↓reduceIte]
    rw [scale_add]
    simp [plus_zero_left, beq_comm_bool, plus_assoc, plus_comm, plus_left_comm]
  · simp [hp, plus_zero_left, beq_comm_bool, plus_assoc, plus_comm, plus_left_comm]

private theorem foldl_G_acc {α K : Type} [BEq K] [Hashable K]
    (xs : List α) (m0 : Std.HashMap K Totals) (key : α → K) (contrib : α → Totals)
    (cond : α → Bool) (p : K → Bool) (f : K → Nat) (acc : Totals) :
    xs.foldl (fun a x =>
      if cond x then (if p (key x) then plus a (scaleT (f (key x)) (contrib x)) else a) else a) acc =
      plus acc (xs.foldl (fun a x =>
      if cond x then (if p (key x) then plus a (scaleT (f (key x)) (contrib x)) else a) else a) zero) := by
  induction xs generalizing acc with
  | nil => simp [plus_zero_right]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      by_cases hc : cond x = true
      · simp only [hc, ↓reduceIte]
        by_cases hp : p (key x) = true
        · simp only [hp, ↓reduceIte]
          rw [ih (plus acc (scaleT (f (key x)) (contrib x))), ih (plus zero (scaleT (f (key x)) (contrib x)))]
          rw [plus_zero_left]
          exact plus_assoc _ _ _
        · simp [hp]
          rw [ih]
      · have hcf : cond x = false := by cases hb : cond x <;> simp_all
        simp [hcf]
        rw [ih]

private theorem hm_foldl_scaled_of_fold {α K : Type} [BEq K] [Hashable K] [EquivBEq K]
    [LawfulHashable K] [LawfulBEq K]
    (xs : List α) (m0 : Std.HashMap K Totals) (key : α → K) (contrib : α → Totals) (cond : α → Bool)
    (p : K → Bool) (f : K → Nat) :
    let m := xs.foldl (fun m x =>
      if cond x then m.insert (key x) (plus (m.getD (key x) zero) (contrib x)) else m) m0
    m.toList.foldl (fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc) zero =
      plus (m0.toList.foldl (fun acc q => if p q.1 then plus acc (scaleT (f q.1) q.2) else acc) zero)
        (xs.foldl (fun acc x =>
          if cond x then (if p (key x) then plus acc (scaleT (f (key x)) (contrib x)) else acc) else acc) zero) := by
  induction xs generalizing m0 with
  | nil => simp [plus_zero_right]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      by_cases hc : cond x = true
      · simp only [hc, ↓reduceIte]
        rw [ih]
        rw [hm_foldl_scaled_insert]
        by_cases hp : p (key x) = true
        · simp only [hp, ↓reduceIte, plus_zero_left]
          have hgacc := foldl_G_acc xs m0 key contrib cond p f (scaleT (f (key x)) (contrib x))
          rw [hgacc]
          simp [hp, plus_zero_left, plus_assoc]
        · simp [hp, plus_zero_right]
      · have hcf : cond x = false := by cases hb : cond x <;> simp_all
        simp [hcf]
        exact ih m0

private theorem scale_weighted (w n : Nat) (v : Option Int) :
    scaleT w (weighted n v) = weighted (n * w) v := by
  cases v <;> simp [scaleT, weighted, Nat.mul_comm, Int.mul_comm, Int.mul_left_comm, Int.mul_assoc]

private theorem filter_pair_count (d : List (Nat × String)) (s : Nat) (label : String) :
    (((d.filter (fun e => e.2 == label)).map Prod.fst).filter (fun x => x == s)).length =
      (d.filter (fun e => e == (s, label))).length := by
  induction d with
  | nil => simp
  | cons e es ih =>
      by_cases h2 : e.2 == label
      · by_cases h1 : e.1 == s
        · have heq : e = (s, label) := by
            rcases e with ⟨a,b⟩
            have ha : a = s := LawfulBEq.eq_of_beq h1
            have hb : b = label := LawfulBEq.eq_of_beq h2
            simp [ha, hb]
          simp [h2, h1, heq, ih]
        · have hne : e ≠ (s, label) := by
            intro heq
            apply h1
            simpa using congrArg Prod.fst heq
          simp [h2, h1, hne, ih]
      · have hne : e ≠ (s, label) := by
          intro heq
          apply h2
          simpa using congrArg Prod.snd heq
        simp [h2, hne, ih]
private theorem summarize_flat2 {α β : Type} (M : List α) (N : List β) (v : Option Int) :
    summarize (M.flatMap fun _ => N.map fun _ => v) = weighted (M.length * N.length) v := by
  induction M with
  | nil => simp [summarize, weighted, zero]
  | cons m Ms ih =>
      simp only [List.flatMap_cons, List.length_cons]
      rw [summarize_append, ih, map_const_eq_replicate, summarize_replicate]
      rw [Nat.succ_mul]
      simpa [Nat.add_comm] using (weighted_add (N.length) (Ms.length * N.length) v)
private theorem summarize_flat3 {α β γ : Type} (L : List α) (M : List β) (N : List γ) (v : Option Int) :
    summarize (L.flatMap fun _ => M.flatMap fun _ => N.map fun _ => v) =
      weighted (L.length * (M.length * N.length)) v := by
  induction L with
  | nil => simp [summarize, weighted, zero]
  | cons l Ls ih =>
      simp only [List.flatMap_cons, List.length_cons]
      rw [summarize_append, ih, summarize_flat2]
      rw [Nat.succ_mul]
      simpa [Nat.add_comm] using (weighted_add (M.length * N.length) (Ls.length * (M.length * N.length)) v)

theorem LateLabels_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels.LateLabels.Goal := by
  intro sales eligible stores items region category year
  dsimp only
  let sw := lookupLabel stores region
  let iw := lookupLabel items category
  let cc : Nat → Nat := fun c => (histogram eligible).getD c 0
  let cond : Sale → Bool := fun (s,i,c,y,v) => 2011 ≤ y && y ≤ 2016 && (cc c > 0)
  let key : Sale → Nat × Nat × Int := fun (s,i,c,y,v) => (s,i,y)
  let contrib : Sale → Totals := fun (s,i,c,y,v) => weighted (cc c) v
  let p : Nat × Nat × Int → Bool := fun (s,i,y) => y == year
  let f : Nat × Nat × Int → Nat := fun (s,i,y) => sw.getD s 0 * iw.getD i 0
  have hcells : cells sales eligible =
      sales.foldl (fun m x => if cond x then m.insert (key x) (plus (m.getD (key x) zero) (contrib x)) else m)
        ({} : Std.HashMap (Nat × Nat × Int) Totals) := by
    dsimp [cells, cond, key, contrib, cc]
  have hfold := hm_foldl_scaled_of_fold sales
      ({} : Std.HashMap (Nat × Nat × Int) Totals) key contrib cond p f
  rw [← hcells] at hfold
  dsimp [p, f, scaleT, sw, iw] at hfold
  simp only [Int.ofNat_mul_ofNat] at hfold
  rw [hfold]
  simp only [Std.HashMap.toList_empty, List.foldl_nil, plus_zero_left]
  -- now prove the fold over sales equals the reference summarize
  let rowsOf : Sale → List (Option Int) := fun (s,i,c,y,v) =>
    if y == year && 2011 ≤ y && y ≤ 2016 then
      (eligible.filter (· == c)).flatMap fun _ =>
      (stores.filter (· == (s,region))).flatMap fun _ =>
      (items.filter (· == (i,category))).map fun _ => v
    else []
  have hper : ∀ x : Sale,
      (if cond x then (if p (key x) then plus zero (scaleT (f (key x)) (contrib x)) else zero) else zero) =
      summarize (rowsOf x) := by
    intro x
    rcases x with ⟨s,i,c,y,v⟩
    dsimp [cond, p, key, f, contrib, cc, rowsOf, sw, iw]
    by_cases hy : (y == year) = true
    · by_cases hy1 : 2011 ≤ y
      · by_cases hy2 : y ≤ 2016
        · have hd1 : decide (2011 ≤ y) = true := decide_eq_true hy1
          have hd2 : decide (y ≤ 2016) = true := decide_eq_true hy2
          have hcc : (histogram eligible).getD c 0 = (eligible.filter (fun z => z == c)).length :=
            hist_getD_count eligible c
          have hsw0 : (lookupLabel stores region).getD s 0 = (((stores.filter (fun e => e.2 == region)).map Prod.fst).filter (fun z => z == s)).length := by
            dsimp [lookupLabel]
            exact hist_getD_count ((stores.filter (fun e => e.2 == region)).map Prod.fst) s
          have hsw : (lookupLabel stores region).getD s 0 = (stores.filter (fun e => e == (s, region))).length := by
            rw [hsw0, filter_pair_count]
          have hiw0 : (lookupLabel items category).getD i 0 = (((items.filter (fun e => e.2 == category)).map Prod.fst).filter (fun z => z == i)).length := by
            dsimp [lookupLabel]
            exact hist_getD_count ((items.filter (fun e => e.2 == category)).map Prod.fst) i
          have hiw : (lookupLabel items category).getD i 0 = (items.filter (fun e => e == (i, category))).length := by
            rw [hiw0, filter_pair_count]
          by_cases hpos : (histogram eligible).getD c 0 > 0
          · have hdpos : decide ((histogram eligible).getD c 0 > 0) = true := decide_eq_true hpos
            simp only [hy, hd1, hd2, hdpos, Bool.and_true, ↓reduceIte, plus_zero_left]
            rw [scale_weighted]
            have hflat := summarize_flat3 (L := eligible.filter (fun z => z == c))
              (M := stores.filter (fun e => e == (s, region)))
              (N := items.filter (fun e => e == (i, category))) v
            rw [hflat]
            rw [hcc, hsw, hiw]
          · have hzero : (histogram eligible).getD c 0 = 0 := by omega
            have hdpos : decide ((histogram eligible).getD c 0 > 0) = false := decide_eq_false hpos
            simp only [hy, hd1, hd2, hdpos, Bool.and_true, Bool.and_false, ↓reduceIte]
            rw [hcc] at hzero
            have hnil : eligible.filter (fun z => z == c) = [] := List.eq_nil_of_length_eq_zero hzero
            simp [hnil, summarize, weighted, zero]
        · simp [hy, hy1, hy2, summarize]
      · simp [hy, hy1, summarize]
    · simp [hy, summarize]
  clear hcells hfold
  have hseq : sales.foldl (fun acc x =>
        if cond x then (if p (key x) then plus acc (scaleT (f (key x)) (contrib x)) else acc) else acc) zero =
      summarize (sales.flatMap rowsOf) := by
    induction sales with
    | nil => simp [summarize]
    | cons x xs ih =>
        simp only [List.foldl_cons, List.flatMap_cons]
        have hg := foldl_G_acc xs ({} : Std.HashMap (Nat × Nat × Int) Totals) key contrib cond p f
            (if cond x then (if p (key x) then plus zero (scaleT (f (key x)) (contrib x)) else zero) else zero)
        rw [hg]
        rw [ih]
        rw [hper x]
        rw [← summarize_append]
  -- final target
  dsimp [p, f, scaleT, sw, iw] at hseq
  simp only [Int.ofNat_mul_ofNat] at hseq
  simpa [rowsOf] using hseq


theorem PriceSynopsis_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels.PriceSynopsis.Goal := by
  intro xs cut
  dsimp only
  let vs := xs.filterMap id
  have hhigh : Option.all (fun v : Int => v ≤ cut) (prices xs).high = true ↔
      ∀ v ∈ vs, v ≤ cut := by
    dsimp [prices, vs]
    exact max_all_le_iff (xs.filterMap id) cut
  have hlow : Option.any (fun v : Int => cut < v) (prices xs).low = true ↔
      vs ≠ [] ∧ ∀ v ∈ vs, cut < v := by
    dsimp [prices, vs]
    exact min_any_gt_iff (xs.filterMap id) cut
  have hle_lift : (∃ x ∈ xs, x.any (fun v : Int => v ≤ cut) = true) ↔
      ∃ v ∈ vs, v ≤ cut := by
    constructor
    · intro h
      rcases (exists_any_iff xs (fun v : Int => v ≤ cut)).mp h with ⟨v, hv, hvbool⟩
      exact ⟨v, by simpa [vs] using hv, of_decide_eq_true hvbool⟩
    · intro h
      rcases h with ⟨v, hv, hvprop⟩
      apply (exists_any_iff xs (fun v : Int => v ≤ cut)).mpr
      exact ⟨v, by simpa [vs] using hv, decide_eq_true hvprop⟩
  have hgt_lift : (∃ x ∈ xs, x.any (fun v : Int => cut < v) = true) ↔
      ∃ v ∈ vs, cut < v := by
    constructor
    · intro h
      rcases (exists_any_iff xs (fun v : Int => cut < v)).mp h with ⟨v, hv, hvbool⟩
      exact ⟨v, by simpa [vs] using hv, of_decide_eq_true hvbool⟩
    · intro h
      rcases h with ⟨v, hv, hvprop⟩
      apply (exists_any_iff xs (fun v : Int => cut < v)).mpr
      exact ⟨v, by simpa [vs] using hv, decide_eq_true hvprop⟩
  by_cases hall : Option.all (fun v : Int => v ≤ cut) (prices xs).high = true
  · have hallvs : ∀ v ∈ vs, v ≤ cut := hhigh.mp hall
    have hc0 : xs.countP (fun x => x.any (cut < ·)) = 0 := by
      rw [countP_option_any]
      apply countP_eq_zero_of_all_false (fun v : Int => cut < v)
      intro v hv
      exact decide_eq_false (by have := hallvs v hv; omega)
    have hpc : priceCount (prices xs) xs cut = (0, false) := by
      simp [priceCount, hall]
    constructor
    · rw [hpc]
      exact hc0.symm
    · rw [hpc]
      constructor
      · intro h
        cases h
      · intro h
        exfalso
        rcases h with ⟨_, hB⟩
        rcases (hgt_lift.mp hB) with ⟨v, hv, _⟩
        have hvle := hallvs v hv
        omega
  · by_cases hlowt : Option.any (fun v : Int => cut < v) (prices xs).low = true
    · have hallgt : ∀ v ∈ vs, cut < v := (hlow.mp hlowt).2
      have hcnt : xs.countP (fun x => x.any (cut < ·)) = (prices xs).nonNull := by
        rw [countP_option_any]
        dsimp [prices, vs]
        apply countP_eq_length_of_all_true (fun v : Int => cut < v)
        intro v hv
        exact decide_eq_true (hallgt v hv)
      have hpc : priceCount (prices xs) xs cut = ((prices xs).nonNull, false) := by
        simp [priceCount, hall, hlowt]
      constructor
      · rw [hpc]
        exact hcnt.symm
      · rw [hpc]
        constructor
        · intro h
          cases h
        · intro h
          exfalso
          rcases h with ⟨hA, _⟩
          rcases (hle_lift.mp hA) with ⟨v, hv, _⟩
          have hgt := hallgt v hv
          omega
    · have hvsne : vs ≠ [] := by
        intro he
        have hhighnone : (prices xs).high = none := by simp [prices, vs, he]
        exact hall (by simp [hhighnone])
      rcases max?_exists hvsne with ⟨mx, hmx, hmxmem, _⟩
      have hhighmx : (prices xs).high = some mx := by simpa [prices, vs] using hmx
      have hmxgt : cut < mx := by
        by_cases hmxle : mx ≤ cut
        · exfalso
          apply hall
          rw [hhighmx]
          simp [hmxle]
        · omega
      rcases min?_exists hvsne with ⟨mn, hmn, hmnmMem, _⟩
      have hlowmn : (prices xs).low = some mn := by simpa [prices, vs] using hmn
      have hmnle : mn ≤ cut := by
        by_cases hmngt : cut < mn
        · exfalso
          apply hlowt
          rw [hlowmn]
          simp [hmngt]
        · omega
      have hexgt : ∃ x ∈ xs, x.any (fun v : Int => cut < v) = true := by
        apply hgt_lift.mpr
        exact ⟨mx, hmxmem, hmxgt⟩
      have hexle : ∃ x ∈ xs, x.any (fun v : Int => v ≤ cut) = true := by
        apply hle_lift.mpr
        exact ⟨mn, hmnmMem, hmnle⟩
      have hpc : priceCount (prices xs) xs cut =
          (xs.countP (fun x => x.any (cut < ·)), true) := by
        simp [priceCount, hall, hlowt]
      constructor
      · rw [hpc]
      · rw [hpc]
        constructor
        · intro _
          exact ⟨hexle, hexgt⟩
        · intro _
          rfl

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.DictionaryJoinsAndLateLabels

-- C++ references to this model or its proof.
-- executor/detail/analytics_factorized_join.cpp
-- executor/detail/analytics_factorized_join.hpp
-- executor/detail/analytics_factorized_join_test.cpp
