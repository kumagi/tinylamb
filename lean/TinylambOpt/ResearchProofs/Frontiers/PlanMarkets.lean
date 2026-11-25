import TinylambOpt.ResearchGoals.Frontiers.PlanMarkets

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.Frontiers.PlanMarkets

open TinylambOpt.ResearchGoals.Frontiers.PlanMarkets

/-! ### `min?` / `optimum` basics -/

private theorem min_attained : ∀ l : List Nat, l ≠ [] → ∃ v ∈ l, l.min? = some v
  | [], h => by simp at h
  | a :: l, h => by
      by_cases hl : l = []
      · subst l
        exact ⟨a, by simp, by simp⟩
      · obtain ⟨v, hv, hvmin⟩ := min_attained l hl
        have hcons : (a :: l).min? = some (min a v) := by
          rw [List.min?_cons, hvmin]
          rfl
        by_cases hav : a ≤ v
        · have hm : min a v = a := Nat.min_eq_left hav
          exact ⟨a, by simp, by simpa [hm] using hcons⟩
        · have hva : v ≤ a := by omega
          have hm : min a v = v := Nat.min_eq_right hva
          exact ⟨v, by simp [hv], by simpa [hm] using hcons⟩

private theorem optimum_attained (ps : List Line) (n : Nat) (hps : ps ≠ []) :
    ∃ a ∈ ps, optimum ps n = some (cost a n) := by
  unfold optimum
  have hmap : ps.map (cost · n) ≠ [] := by
    intro h
    have : ps = [] := by simpa using congrArg List.length h
    exact hps this
  obtain ⟨v, hv, hvmin⟩ := min_attained (ps.map (cost · n)) hmap
  obtain ⟨a, ha, rfl⟩ := List.mem_map.mp hv
  exact ⟨a, ha, hvmin⟩

private theorem optimum_le_of_eq {ps n a q} (h : optimum ps n = some (cost a n)) (hq : q ∈ ps) :
    cost a n ≤ cost q n := by
  unfold optimum at h
  exact (List.min?_eq_some_iff.mp h).2 (cost q n) (List.mem_map.mpr ⟨q, hq, rfl⟩)
private theorem min_drop {x y : Nat} {l : List Nat} (hxy : x ≤ y) :
    (x :: y :: l).min? = (x :: l).min? := by
  have hne : x :: l ≠ [] := by simp
  obtain ⟨v, hv, hvmin⟩ := min_attained (x :: l) hne
  have hvx : v ≤ x := (List.min?_eq_some_iff.mp hvmin).2 x (by simp)
  have hvy : v ≤ y := Nat.le_trans hvx hxy
  have hmem : v ∈ x :: y :: l := by
    rcases List.mem_cons.mp hv with rfl | hv
    · simp
    · simp [hv]
  have hs1 : (x :: y :: l).min? = some v := by
    apply (List.min?_eq_some_iff).mpr
    constructor
    · exact hmem
    · intro z hz
      rcases List.mem_cons.mp hz with rfl | hz
      · exact hvx
      · rcases List.mem_cons.mp hz with rfl | hz
        · exact hvy
        · exact (List.min?_eq_some_iff.mp hvmin).2 z (by simp [hz])
  simp [hvmin, hs1]
/-! ### EndpointEnvelope -/

private theorem endpoint_dom (a b : Line) {lo hi n : Nat} (hlo : lo ≤ n) (hhi : n ≤ hi)
    (h1 : cost a lo ≤ cost b lo) (h2 : cost a hi ≤ cost b hi) : cost a n ≤ cost b n := by
  by_cases hp : a.perRow ≤ b.perRow
  · let k := n - lo
    have hn : n = lo + k := by dsimp [k]; omega
    calc
      cost a n = cost a lo + a.perRow * k := by rw [hn]; simp [cost, Nat.mul_add, Nat.add_assoc]
      _ ≤ cost b lo + a.perRow * k := Nat.add_le_add_right h1 _
      _ ≤ cost b lo + b.perRow * k := Nat.add_le_add_left (Nat.mul_le_mul_right k hp) _
      _ = cost b n := by rw [hn]; simp [cost, Nat.mul_add, Nat.add_assoc]
  · have hqp : b.perRow ≤ a.perRow := by omega
    let k := hi - n
    have hh : hi = n + k := by dsimp [k]; omega
    have hhi' : cost a n + a.perRow * k ≤ cost b n + b.perRow * k := by
      calc
        cost a n + a.perRow * k = cost a hi := by rw [hh]; simp [cost, Nat.mul_add, Nat.add_assoc]
        _ ≤ cost b hi := h2
        _ = cost b n + b.perRow * k := by rw [hh]; simp [cost, Nat.mul_add, Nat.add_assoc]
    have hle : cost a n + a.perRow * k ≤ cost b n + a.perRow * k :=
      Nat.le_trans hhi' (Nat.add_le_add_left (Nat.mul_le_mul_right k hqp) _)
    omega

theorem sound_EndpointEnvelope : TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.EndpointEnvelope.Goal := by
  intro a b rest lo hi h1 h2 n hlo hhi
  have hdom := endpoint_dom a b hlo hhi h1 h2
  unfold optimum
  exact min_drop hdom

/-! ### RobustRegret -/

private theorem optimum_get_le {ps n} {q : Line} (hq : q ∈ ps) : (optimum ps n).getD 0 ≤ cost q n := by
  have hps : ps ≠ [] := by
    intro h
    subst ps
    cases hq
  obtain ⟨a, ha, haopt⟩ := optimum_attained ps n hps
  rw [haopt]
  simp only [Option.getD_some]
  exact optimum_le_of_eq haopt hq

private theorem regret_ge_diff (p q : Line) (ps : List Line) (n : Nat) (hq : q ∈ ps) :
    cost p n - cost q n ≤ regret p ps n := by
  have hps : ps ≠ [] := by
    intro h
    subst ps
    cases hq
  obtain ⟨a, ha, haopt⟩ := optimum_attained ps n hps
  unfold regret
  rw [haopt]
  simp only [Option.getD_some]
  exact Nat.sub_le_sub_left (optimum_le_of_eq haopt hq) (cost p n)

private theorem cost_add (p : Line) (n k : Nat) : cost p (n + k) = cost p n + p.perRow * k := by
  simp [cost, Nat.mul_add, Nat.add_assoc]

private theorem sub_add_add_le {A B u v : Nat} (h : u ≤ v) : (A + u) - (B + v) ≤ A - B := by omega
private theorem add_sub_le_add_sub_add {A B u v : Nat} (h : v ≤ u) : A - B ≤ (A + u) - (B + v) := by omega

private theorem diff_le_left (p q : Line) {lo n : Nat} (hpq : p.perRow ≤ q.perRow) (hlo : lo ≤ n) :
    cost p n - cost q n ≤ cost p lo - cost q lo := by
  let k := n - lo
  have hn : n = lo + k := by dsimp [k]; omega
  rw [hn, cost_add, cost_add]
  exact sub_add_add_le (Nat.mul_le_mul_right k hpq)

private theorem diff_le_right (p q : Line) {n hi : Nat} (hqp : q.perRow ≤ p.perRow) (hhi : n ≤ hi) :
    cost p n - cost q n ≤ cost p hi - cost q hi := by
  let k := hi - n
  have hh : hi = n + k := by dsimp [k]; omega
  rw [hh, cost_add, cost_add]
  exact add_sub_le_add_sub_add (Nat.mul_le_mul_right k hqp)
theorem sound_RobustRegret : TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.RobustRegret.Goal := by
  intro p ps lo hi n hlo hhi
  by_cases hempty : ps = []
  · subst ps
    simp [regret, optimum]
  · obtain ⟨q, hq, hqopt⟩ := optimum_attained ps n hempty
    unfold regret
    rw [hqopt]
    simp only [Option.getD_some]
    have hlo' := regret_ge_diff p q ps lo hq
    have hhi' := regret_ge_diff p q ps hi hq
    by_cases hpq : p.perRow ≤ q.perRow
    · have hd := diff_le_left p q hpq hlo
      exact Nat.le_trans hd (Nat.le_trans hlo' (Nat.le_max_left _ _))
    · have hqp : q.perRow ≤ p.perRow := by omega
      have hd := diff_le_right p q hqp hhi
      exact Nat.le_trans hd (Nat.le_trans hhi' (Nat.le_max_right _ _))
/-- Counterexample: `a = ⟨0,10⟩`, `b = ⟨30,0⟩`,
    `ps = [⟨0,10⟩, ⟨10,2⟩, ⟨30,0⟩]`, `hi = 10`, `n = 3`.
    The endpoint winners are `cost a 0 = 0` and `cost b 10 = 30`, but
    `optimum [a,b] 3 = 30` while `optimum ps 3 = 16`. -/
theorem refutation_TwoWinners : ¬ TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.TwoWinners.Goal := by
  intro h
  have h0 : optimum [⟨0,10⟩,⟨10,2⟩,⟨30,0⟩] 0 = some (cost ⟨0,10⟩ 0) := by native_decide
  have h10 : optimum [⟨0,10⟩,⟨10,2⟩,⟨30,0⟩] 10 = some (cost ⟨30,0⟩ 10) := by native_decide
  have hres := h ⟨0,10⟩ ⟨30,0⟩ [⟨0,10⟩,⟨10,2⟩,⟨30,0⟩] 10 h0 h10
    (by simp) (by simp) 3 (by native_decide)
  exact (by native_decide : ¬ (optimum [⟨0,10⟩,⟨30,0⟩] 3 = optimum [⟨0,10⟩,⟨10,2⟩,⟨30,0⟩] 3)) hres

/-- Counterexample: `qs = [(1,[0,1])]`, `small = []`, `large = [0]`, `v = 1`.
    The second view's marginal benefit grows from
    `bundles qs [1] - bundles qs [] = 0` to
    `bundles qs [1,0] - bundles qs [0] = 1`. -/
theorem refutation_ComplementaryViews : ¬ TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.ComplementaryViews.Goal := by
  intro h
  have hres := h ([(1,[0,1])] : Workload) ([] : List View) [0] 1 (by native_decide)
  exact (by native_decide : ¬ (bundles [(1,[0,1])] [1,0] - bundles [(1,[0,1])] [0] ≤
    bundles [(1,[0,1])] [1] - bundles [(1,[0,1])] [])) hres

theorem sound_SlopeSwitch : TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.SlopeSwitch.Goal := by
  intro ps a b x y hxy ha hb hxa hyb
  have h1 : cost a x ≤ cost b x := by
    unfold optimum at hxa
    exact (List.min?_eq_some_iff.mp hxa).2 (cost b x) (List.mem_map.mpr ⟨b, hb, rfl⟩)
  have h2 : cost b y ≤ cost a y := by
    unfold optimum at hyb
    exact (List.min?_eq_some_iff.mp hyb).2 (cost a y) (List.mem_map.mpr ⟨a, ha, rfl⟩)
  unfold cost at h1 h2
  by_cases hle : b.perRow ≤ a.perRow
  · exact hle
  exfalso
  have hlt : a.perRow < b.perRow := by omega
  let d := b.perRow - a.perRow
  have hdpos : 0 < d := by dsimp [d]; omega
  have hpb : b.perRow = a.perRow + d := by dsimp [d]; omega
  have h1' : a.startup ≤ b.startup + d*x := by
    rw [hpb, Nat.add_mul] at h1
    omega
  have h2' : b.startup + d*y ≤ a.startup := by
    rw [hpb, Nat.add_mul] at h2
    omega
  have hdx : d*y ≤ d*x := by omega
  have hyx : y ≤ x := Nat.le_of_mul_le_mul_left hdx hdpos
  omega

theorem sound_OnlineSpool : TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.OnlineSpool.Goal := by
  intro n read build
  by_cases h : n * read ≤ build
  · simp [rentThenBuy, h, Nat.min_eq_left h]
    omega
  · have hb : build ≤ n * read := by omega
    simp [rentThenBuy, h, Nat.min_eq_right hb]
    have hd := Nat.div_mul_le_self build read
    omega

theorem sound_BoxWorst : TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.BoxWorst.Goal := by
  intro dims hdims
  induction dims with
  | nil => simp
  | cons d ds ih =>
      have hd := hdims d (by simp)
      have hds : ∀ e ∈ ds, e.2.1 ≤ e.2.2.2 ∧ e.2.2.2 ≤ e.2.2.1 := by
        intro e he
        exact hdims e (by simp [he])
      have hi := ih hds
      simp only [List.map_cons, List.sum_cons]
      have hdim : d.1 * d.2.2.2 ≤ d.1 * (if 0 ≤ d.1 then d.2.2.1 else d.2.1) := by
        by_cases hw : 0 ≤ d.1
        · rw [ite_eq_left hw]
          exact Int.mul_le_mul_of_nonneg_left hd.2 hw
        · rw [ite_eq_right hw]
          have hwle : d.1 ≤ 0 := by omega
          exact Int.mul_le_mul_of_nonpos_left hwle hd.1
      exact Int.add_le_add hdim hi
/-! ### DiminishingReuse -/

private theorem any_or_cons (xs : List View) (f g : View → Bool) :
    xs.any (fun x => f x || g x) = (xs.any f || xs.any g) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [List.any_cons, ih, Bool.or_assoc, Bool.or_left_comm]

private theorem any_cons_contains (xs : List View) (v : View) (chosen : List View) :
    xs.any (v :: chosen).contains = (xs.any (fun x => x == v) || xs.any chosen.contains) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.any_cons, List.any_cons, List.any_cons, ih, List.contains_cons]
      cases hx : x == v <;> cases hc : chosen.contains x <;> simp only [hx, hc, Bool.true_or, Bool.false_or, Bool.or_true, Bool.or_false, Bool.or_assoc]

private theorem any_mono {f g : View → Bool} (h : ∀ x, f x = true → g x = true) :
    ∀ xs : List View, xs.any f = true → xs.any g = true := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.any_cons, Bool.or_eq_true]
      intro hx
      rcases hx with hx | hx
      · exact Or.inl (h x hx)
      · exact Or.inr (ih hx)

private theorem sum_filter_le {α : Type} (l : List (Nat × α)) (p q : Nat × α → Bool)
    (h : ∀ z, p z = true → q z = true) :
    ((l.filter p).map Prod.fst).sum ≤ ((l.filter q).map Prod.fst).sum := by
  induction l with
  | nil => simp
  | cons z zs ih =>
      by_cases hp : p z = true
      · have hq : q z = true := h z hp
        simp [hp, hq]
        exact ih
      · by_cases hq : q z = true
        · simp [hp, hq]
          omega
        · simp [hp, hq]
          exact ih

private def bonus (qs : Workload) (chosen : List View) (v : View) : Nat :=
  (qs.filter (fun q => (q.2.any (fun x => x == v)) && !(q.2.any chosen.contains))).map Prod.fst |>.sum

private theorem saved_cons_eq (qs : Workload) (chosen : List View) (v : View) :
    saved qs (v :: chosen) = saved qs chosen + bonus qs chosen v := by
  induction qs with
  | nil => simp [saved, bonus]
  | cons q qs ih =>
      rcases q with ⟨val, xs⟩
      simp only [saved, bonus, List.filter_cons]
      rw [any_cons_contains xs v chosen]
      by_cases hC : xs.any chosen.contains = true
      · simp [hC]
        change val + saved qs (v :: chosen) = val + saved qs chosen + bonus qs chosen v
        rw [ih]
        omega
      · have hCf : xs.any chosen.contains = false := Bool.eq_false_of_ne_true hC
        by_cases hV : xs.any (fun x => x == v) = true
        · simp [hC, hCf, hV]
          change val + saved qs (v :: chosen) = saved qs chosen + (val + bonus qs chosen v)
          rw [ih]
          omega
        · have hVf : xs.any (fun x => x == v) = false := Bool.eq_false_of_ne_true hV
          simp [hC, hCf, hV, hVf]
          exact ih

private theorem gain_eq_bonus (qs : Workload) (chosen : List View) (v : View) :
    gain qs chosen v = bonus qs chosen v := by
  rw [gain, saved_cons_eq]
  omega

private theorem bonus_reverse (qs : Workload) {small large : List View} (v : View)
    (hsub : ∀ x ∈ small, x ∈ large) : bonus qs large v ≤ bonus qs small v := by
  unfold bonus
  apply sum_filter_le
  intro q hq
  cases hV : q.2.any (fun x => x == v) <;> cases hL : q.2.any large.contains <;> simp [hV, hL] at hq ⊢
  cases hS : q.2.any small.contains
  · intro x hx hs
    have ht : q.2.any small.contains = true := by
      rw [List.any_eq_true]
      exact ⟨x, hx, by simpa using hs⟩
    rw [ht] at hS
    cases hS
  · have hl : q.2.any large.contains = true := any_mono (by
      intro x hx
      have hxs : x ∈ small := by simpa using hx
      simpa using hsub x hxs) q.2 hS
    rw [hl] at hL
    cases hL

private theorem any_nil_contains (xs : List View) : xs.any ([] : List View).contains = false := by
  rw [List.any_eq_false]
  intro x hx
  simp

private theorem filter_nil_contains (qs : Workload) :
    qs.filter (fun q => q.2.any ([] : List View).contains) = [] := by
  induction qs with
  | nil => rfl
  | cons q qs ih => simp [List.filter_cons, any_nil_contains, ih]

private theorem saved_nil (qs : Workload) : saved qs [] = 0 := by
  unfold saved
  rw [filter_nil_contains]
  rfl

private theorem saved_mono (qs : Workload) {S T : List View} (hsub : ∀ x ∈ S, x ∈ T) :
    saved qs S ≤ saved qs T := by
  unfold saved
  apply sum_filter_le
  intro q hq
  exact any_mono (by
    intro x hx
    have hxs : x ∈ S := by simpa using hx
    simpa using hsub x hxs) q.2 hq
theorem sound_DiminishingReuse : TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.DiminishingReuse.Goal := by
  intro qs small large v hsub
  rw [gain_eq_bonus, gain_eq_bonus]
  exact bonus_reverse qs v hsub

/-! ### GreedyViews -/

private theorem saved_append_le (qs : Workload) : ∀ O G : List View,
    saved qs (O ++ G) ≤ saved qs G + (O.map (fun v => bonus qs G v)).sum
  | [], G => by simp [saved_nil]
  | v :: O, G => by
      rw [show (v :: O) ++ G = v :: (O ++ G) by rfl]
      rw [saved_cons_eq]
      have hb : bonus qs (O ++ G) v ≤ bonus qs G v := bonus_reverse qs v (by
        intro x hx
        simp [hx])
      have hi := saved_append_le qs O G
      simp only [List.map_cons, List.sum_cons]
      omega

private theorem sum_map_le_length_mul (l : List View) (f : View → Nat) (c : Nat)
    (h : ∀ v ∈ l, f v ≤ c) : (l.map f).sum ≤ l.length * c := by
  induction l with
  | nil => simp
  | cons v vs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.succ_mul]
      have hv := h v (by simp)
      have hvs : ∀ u ∈ vs, f u ≤ c := by intro u hu; exact h u (by simp [hu])
      have hi := ih hvs
      omega

private def greedyLe (qs : Workload) (selected : List View) : View → View → Bool :=
  fun a b => decide (gain qs selected b ≤ gain qs selected a)

private theorem greedyLe_trans (qs : Workload) (selected : List View) :
    ∀ a b c : View, greedyLe qs selected a b = true → greedyLe qs selected b c = true →
      greedyLe qs selected a c = true := by
  intro a b c hab hbc
  simp [greedyLe] at hab hbc ⊢
  omega

private theorem greedyLe_total (qs : Workload) (selected : List View) :
    ∀ a b : View, greedyLe qs selected a b || greedyLe qs selected b a = true := by
  intro a b
  simp [greedyLe]
  by_cases h : gain qs selected a ≥ gain qs selected b
  · simp [h]
  · simp [h]
    omega

private theorem mergeSort_head_max {α : Type} (le : α → α → Bool)
    (trans : ∀ a b c, le a b = true → le b c = true → le a c = true)
    (total : ∀ a b, le a b || le b a = true)
    (xs : List α) {c : α} {t : List α} (h : xs.mergeSort le = c :: t) : ∀ x ∈ xs, le c x = true := by
  have hp := List.pairwise_mergeSort trans total xs
  rw [h] at hp
  cases hp with
  | cons hhead htail =>
      intro x hx
      have hxsort0 : x ∈ xs.mergeSort le := (List.mem_mergeSort (a := x) (l := xs) (le := le)).mpr hx
      have hxsort : x ∈ c :: t := by simpa [h] using hxsort0
      rcases List.mem_cons.mp hxsort with rfl | hxt
      · have hh := total x x
        simp at hh
        exact hh
      · exact hhead x hxt

private theorem greedy_choice (qs : Workload) (pool selected : List View) :
    let ch := (pool.filter (fun v => !selected.contains v)).mergeSort (greedyLe qs selected)
    match ch with
    | [] => ∀ v ∈ pool, v ∈ selected
    | c :: _ => c ∈ pool ∧ c ∉ selected ∧
        ∀ v ∈ pool, v ∉ selected → gain qs selected v ≤ gain qs selected c := by
  dsimp
  let le := greedyLe qs selected
  let fl := pool.filter (fun v => !selected.contains v)
  by_cases hnil : fl.mergeSort le = []
  · rw [hnil]
    intro v hv
    by_cases hvsel : v ∈ selected
    · exact hvsel
    · have hvf : v ∈ fl := by simp [fl, hv, hvsel]
      have : v ∈ fl.mergeSort le := (List.mem_mergeSort).mpr hvf
      rw [hnil] at this
      cases this
  · cases hsort : fl.mergeSort le with
    | nil => contradiction
    | cons c t =>
      have hcf : c ∈ fl := by
        rw [← List.mem_mergeSort]
        rw [hsort]
        simp
      have hc : c ∈ pool ∧ c ∉ selected := by
        simpa [fl] using (List.mem_filter.mp hcf)
      refine ⟨hc.1, hc.2, ?_⟩
      intro v hv hvsel
      have hvf : v ∈ fl := by simp [fl, hv, hvsel]
      have hle := mergeSort_head_max le (greedyLe_trans qs selected) (greedyLe_total qs selected)
        fl (c := c) (t := t) hsort v hvf
      simp [le, greedyLe] at hle
      exact hle

private theorem comparator_eq (qs : Workload) (selected : List View) :
    (fun a b : View => decide (gain qs selected b ≤ gain qs selected a)) = greedyLe qs selected := by
  funext a b
  rfl

private theorem greedyLe_eq_raw (qs : Workload) (selected : List View) :
    greedyLe qs selected = (fun a b => decide (gain qs selected a >= gain qs selected b)) := rfl

private theorem filter_contains_eq_decide (pool selected : List View) :
    pool.filter (fun v => !decide (v ∈ selected)) = pool.filter (fun v => !selected.contains v) := by
  apply List.filter_congr
  intro v hv
  simp

private theorem greedy_succ_eq (qs : Workload) (pool : List View) :
    ∀ n selected,
      greedy qs pool (n + 1) selected =
        let G := greedy qs pool n selected
        match (pool.filter (fun v => !G.contains v)).mergeSort (greedyLe qs G) with
        | [] => G
        | c :: _ => c :: G := by
  intro n
  induction n with
  | zero =>
      intro selected
      simp only [greedy]
      rw [greedyLe_eq_raw]
      rfl
  | succ n ih =>
      intro selected
      let ch := (pool.filter (fun v => !selected.contains v)).mergeSort
        (fun a b => decide (gain qs selected a ≥ gain qs selected b))
      cases hch : ch with
      | nil =>
          have hs0 : greedy qs pool (n + 1) selected = selected := by
            simp only [greedy]
            rw [show (pool.filter (fun v => !selected.contains v)).mergeSort
              (fun a b => decide (gain qs selected a >= gain qs selected b)) = ch from rfl]
            simp [hch]
          have hs1 : greedy qs pool (n + 2) selected = selected := by
            simp only [greedy]
            rw [show (pool.filter (fun v => !selected.contains v)).mergeSort
              (fun a b => decide (gain qs selected a >= gain qs selected b)) = ch from rfl]
            simp [hch]
          rw [hs0, hs1]
          dsimp
          have hch_eq : (pool.filter (fun v => !selected.contains v)).mergeSort
              (greedyLe qs selected) = ch := by
            dsimp [ch]
            rw [greedyLe_eq_raw]
          rw [hch_eq, hch]
      | cons v t =>
          have hs0 : greedy qs pool (n + 1) selected = greedy qs pool n (v :: selected) := by
            simp only [greedy]
            rw [show (pool.filter (fun v => !selected.contains v)).mergeSort
              (fun a b => decide (gain qs selected a >= gain qs selected b)) = ch from rfl]
            simp [hch]
          have hs1 : greedy qs pool (n + 2) selected = greedy qs pool (n + 1) (v :: selected) := by
            simp only [greedy]
            rw [show (pool.filter (fun v => !selected.contains v)).mergeSort
              (fun a b => decide (gain qs selected a >= gain qs selected b)) = ch from rfl]
            simp [hch]
          rw [hs1, ih (v :: selected), ← hs0]

private theorem bonus_mem_zero (qs : Workload) {G : List View} {v : View} (hv : v ∈ G) :
    bonus qs G v = 0 := by
  unfold bonus
  have hf : ∀ qs : Workload, qs.filter (fun q => (q.2.any (fun x => x == v)) && !(q.2.any G.contains)) = [] := by
    intro qs
    induction qs with
    | nil => rfl
    | cons q qs ih =>
        simp only [List.filter_cons]
        by_cases hP : ((q.2.any (fun x => x == v)) && !(q.2.any G.contains)) = true
        · have hV : q.2.any (fun x => x == v) = true := by
            cases hV' : q.2.any (fun x => x == v) <;> simp [hV'] at hP ⊢
          have hc : q.2.any G.contains = true := any_mono (by
            intro x hx
            have hxv : x = v := (beq_iff_eq (α := View)).mp (by simpa using hx)
            simpa [hxv] using hv) q.2 hV
          rw [hc] at hP
          simp at hP
        · rw [ite_eq_right hP]
          exact ih
  rw [hf qs]
  rfl

private theorem greedy_opt_step (qs : Workload) (pool opt G : List View) (k : Nat) (hk : 0 < k)
    (hopt_len : opt.length ≤ k) (hopt_pool : ∀ x ∈ opt, x ∈ pool) :
    saved qs opt ≤ saved qs G + k * (saved qs (greedy qs pool 1 G) - saved qs G) := by
  let ch := (pool.filter (fun v => !G.contains v)).mergeSort (greedyLe qs G)
  have hzero : greedy qs pool 0 G = G := rfl
  have hstep := greedy_succ_eq qs pool 0 G
  rw [hzero] at hstep
  cases hch : ch with
  | nil =>
      have hg1 : greedy qs pool 1 G = G := by
        rw [hstep]
        dsimp
        have hch_eq : (pool.filter (fun v => !G.contains v)).mergeSort (greedyLe qs G) = ch := rfl
        rw [hch_eq, hch]
      rw [hg1]
      simp only [Nat.sub_self, Nat.mul_zero, Nat.add_zero]
      apply saved_mono
      intro x hx
      by_cases hxg : x ∈ G
      · exact hxg
      · have hxf : x ∈ pool.filter (fun v => !G.contains v) := by
          simp [hopt_pool x hx, hxg]
        have hxch : x ∈ ch := (List.mem_mergeSort).mpr hxf
        rw [hch] at hxch
        cases hxch
  | cons c t =>
      have hg1 : greedy qs pool 1 G = c :: G := by
        rw [hstep]
        dsimp
        have hch_eq : (pool.filter (fun v => !G.contains v)).mergeSort (greedyLe qs G) = ch := rfl
        rw [hch_eq, hch]
      have hchoice := greedy_choice qs pool G
      rw [show (pool.filter (fun v => !G.contains v)).mergeSort (greedyLe qs G) = ch from rfl] at hchoice
      rw [hch] at hchoice
      have hcmax : ∀ v ∈ opt, bonus qs G v ≤ gain qs G c := by
        intro v hv
        by_cases hvg : v ∈ G
        · rw [bonus_mem_zero qs hvg]
          omega
        · have hvpool := hopt_pool v hv
          have hle := hchoice.2.2 v hvpool hvg
          rw [gain_eq_bonus] at hle
          exact hle
      have hsum := sum_map_le_length_mul opt (fun v => bonus qs G v) (gain qs G c) hcmax
      have happ := saved_append_le qs opt G
      have hmono := saved_mono qs (show ∀ x ∈ opt, x ∈ opt ++ G by intro x hx; simp [hx])
      have hbound : saved qs opt ≤ saved qs G + k * gain qs G c := by
        have h1 : (opt.map (fun v => bonus qs G v)).sum ≤ k * gain qs G c := by
          exact Nat.le_trans hsum (Nat.mul_le_mul_right (gain qs G c) hopt_len)
        omega
      have hdiff : saved qs (greedy qs pool 1 G) - saved qs G = gain qs G c := by
        rw [hg1, gain_eq_bonus, saved_cons_eq]
        omega
      rw [hdiff]
      exact hbound

private theorem step_factor (k a d : Nat) (hk : 0 < k) (h : a ≤ k * d) :
    k * (a - d) ≤ (k - 1) * a := by
  have hk1 : k - 1 + 1 = k := Nat.sub_add_cancel (by omega : 1 ≤ k)
  have hleft : k * (a - d) = k * a - k * d := by
    rw [Nat.mul_sub_left_distrib]
  have hr : (k - 1) * a + a = k * a := by
    calc
      (k - 1) * a + a = (k - 1) * a + 1 * a := by simp
      _ = ((k - 1) + 1) * a := (Nat.add_mul (k - 1) 1 a).symm
      _ = k * a := by rw [hk1]
  have hright : (k - 1) * a = k * a - a := by omega
  rw [hleft, hright]
  exact Nat.sub_le_sub_left h (k * a)

private theorem greedy_factor (qs : Workload) (pool opt : List View) (k : Nat) (hk : 0 < k)
    (hopt_len : opt.length ≤ k) (hopt_pool : ∀ x ∈ opt, x ∈ pool) :
    ∀ i ≤ k, k ^ i * (saved qs opt - saved qs (greedy qs pool i [])) ≤ (k - 1) ^ i * saved qs opt := by
  intro i
  induction i with
  | zero =>
      intro _
      simp [greedy, saved_nil]
  | succ i ih =>
      intro hi
      have hi_le : i ≤ k := by omega
      let G := greedy qs pool i []
      let G1 := greedy qs pool (i + 1) []
      have hstep_i := greedy_succ_eq qs pool i []
      have hstep_0 := greedy_succ_eq qs pool 0 G
      rw [show greedy qs pool 0 G = G by rfl] at hstep_0
      have hG1 : G1 = greedy qs pool 1 G := by
        dsimp [G1, G]
        rw [hstep_i, hstep_0]
      have hg_le : saved qs G ≤ saved qs G1 := by
        dsimp [G1]
        rw [hstep_i]
        dsimp
        split
        · exact Nat.le_refl _
        · rw [saved_cons_eq]
          exact Nat.le_add_right _ _
      have hineq : saved qs opt ≤ saved qs G + k * (saved qs G1 - saved qs G) := by
        have h := greedy_opt_step qs pool opt G k hk hopt_len hopt_pool
        rw [← hG1] at h
        exact h
      let a := saved qs opt - saved qs G
      let d := saved qs G1 - saved qs G
      have ha_le : a ≤ k * d := by
        dsimp [a, d]
        omega
      have hfac := step_factor k a d hk ha_le
      have hG1_eq : saved qs G1 = saved qs G + d := by dsimp [d]; omega
      have hsub : saved qs opt - saved qs G1 = a - d := by
        dsimp [a]
        rw [hG1_eq]
        omega
      have hmul1 := Nat.mul_le_mul_left (k ^ i) hfac
      have hmul2 := Nat.mul_le_mul_left (k - 1) (ih hi_le)
      dsimp [G1] at hsub
      calc
        k ^ (i + 1) * (saved qs opt - saved qs (greedy qs pool (i + 1) []))
            = k ^ i * (k * (a - d)) := by
              rw [Nat.pow_succ, hsub]
              ac_rfl
        _ ≤ k ^ i * ((k - 1) * a) := hmul1
        _ = (k - 1) * (k ^ i * a) := by ac_rfl
        _ ≤ (k - 1) * ((k - 1) ^ i * saved qs opt) := hmul2
        _ = (k - 1) ^ (i + 1) * saved qs opt := by
              rw [Nat.pow_succ]
              ac_rfl
theorem sound_GreedyViews : TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.GreedyViews.Goal := by
  intro qs pool k opt hk hlen hpool
  let f := saved qs opt
  let g := saved qs (greedy qs pool k [])
  by_cases hle : f ≤ g
  · have hcoef : k ^ k - (k - 1) ^ k ≤ k ^ k := Nat.sub_le _ _
    have h1 : (k ^ k - (k - 1) ^ k) * f ≤ k ^ k * f := Nat.mul_le_mul_right f hcoef
    have h2 : k ^ k * f ≤ k ^ k * g := Nat.mul_le_mul_left (k ^ k) hle
    change (k ^ k - (k - 1) ^ k) * f ≤ k ^ k * g
    exact Nat.le_trans h1 h2
  · have hfg : g < f := by omega
    let a := f - g
    have hf := greedy_factor qs pool opt k hk hlen hpool k (Nat.le_refl k)
    have hf' : k ^ k * a ≤ (k - 1) ^ k * f := by
      dsimp [a, f, g]
      exact hf
    have hfa : f = g + a := by dsimp [a]; omega
    have hkpow : (k - 1) ^ k ≤ k ^ k := Nat.pow_le_pow_left (Nat.sub_le k 1) k
    have hmul : (k ^ k - (k - 1) ^ k) * f = k ^ k * f - (k - 1) ^ k * f := by
      rw [Nat.sub_mul]
    change (k ^ k - (k - 1) ^ k) * f ≤ k ^ k * g
    rw [hmul, hfa]
    have hC := Nat.mul_add (k ^ k) g a
    have hB := Nat.mul_add ((k - 1) ^ k) g a
    rw [hfa] at hf'
    rw [hC, hB]
    omega
/-! ### ApproxMemo -/

theorem sound_ApproxMemo : TinylambOpt.ResearchGoals.Frontiers.PlanMarkets.ApproxMemo.Goal := by
  intro ps cache e n hps happrox
  obtain ⟨a, ha, haopt⟩ := optimum_attained ps n hps
  obtain ⟨q, hqc, hqb⟩ := happrox a ha
  have hqopt := optimum_get_le (ps := cache) (n := n) hqc
  have hval : cost q n ≤ cost a n + e * (n + 1) := by
    have hmul : q.perRow * n ≤ (a.perRow + e) * n := Nat.mul_le_mul_right n hqb.2
    rw [Nat.add_mul] at hmul
    unfold cost
    rw [Nat.mul_add]
    omega
  change (optimum cache n).getD 0 ≤ (optimum ps n).getD 0 + e * (n + 1)
  rw [haopt]
  simp only [Option.getD_some]
  exact Nat.le_trans hqopt hval

end TinylambOpt.ResearchProofs.Frontiers.PlanMarkets
