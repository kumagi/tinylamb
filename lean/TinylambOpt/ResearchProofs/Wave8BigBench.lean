import TinylambOpt.ResearchGoals.AnalyticsWorkloads.BigBench

namespace TinylambOpt.ResearchProofs.Wave8BigBench

open TinylambOpt.ResearchGoals.AnalyticsWorkloads
open TinylambOpt.ResearchGoals.AnalyticsWorkloads.BigBench

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

theorem UrlGrouping_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.BigBench.UrlGrouping.Goal := by
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

-- TODO(CodeFanout): caching/histogram invariant not yet closed.
-- TODO(LateLabels): HashMap.toList fold multiplicity invariant not yet closed.
-- TODO(ResidualWords): fold/caching invariant not yet closed.

theorem PriceSynopsis_sound : TinylambOpt.ResearchGoals.AnalyticsWorkloads.BigBench.PriceSynopsis.Goal := by
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

end TinylambOpt.ResearchProofs.Wave8BigBench
