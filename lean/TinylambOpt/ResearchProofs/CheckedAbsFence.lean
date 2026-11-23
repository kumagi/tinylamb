import TinylambOpt.ResearchGoals.CheckedAbsFence

namespace TinylambOpt.ResearchProofs.CheckedAbsFence

open TinylambOpt.ResearchGoals.CheckedAbsFence

/-!
The checked-abs fence: the JIT skips a page when `canSkip` fires. A hit test
`x.toInt.natAbs < limit` is only equivalent to the unfenced arithmetic if no
cell equals `INT64_MIN`. We prove that a skipped page can never contain
`INT64_MIN` (the fence keeps its min/extremum above the boundary), so pruning
only drops pages that would have collected no hits and no failure.
-/

private theorem dtrue {b : Bool} (h : ¬ b = false) : b = true := by
  cases b <;> simp_all

private theorem dfalse {b : Bool} (h : ¬ b = true) : b = false := by
  cases b <;> simp_all

private theorem toInt_ge (i : Int64) : (-(2 ^ 63) : Int) ≤ i.toInt :=
  BitVec.le_toInt i.toBitVec

/-! ## Extremum fold -/

private def stepE (old : Option (Int × Int)) (x : Int) : Option (Int × Int) :=
  some (old.elim (x, x) fun (a, b) => (min a x, max b x))

private theorem extrema_eq (xs : Cells) :
    extrema xs = (xs.filterMap (fun c => c.map Int64.toInt)).foldl stepE none := rfl

private theorem stepE_some (s : Option (Int × Int)) (x : Int) :
    ∃ p : Int × Int, stepE s x = some p := by
  cases s <;> exact ⟨_, rfl⟩

private theorem foldl_ne_none : ∀ (l : List Int) (s : Option (Int × Int)),
    s ≠ none → l.foldl stepE s ≠ none := by
  intro l
  induction l with
  | nil => intro s h; exact h
  | cons a as ih =>
      intro s h
      rw [List.foldl_cons]
      obtain ⟨p, hp⟩ := stepE_some s a
      exact ih (stepE s a) (by rw [hp]; simp)

private theorem foldl_eq_none : ∀ (l : List Int), l.foldl stepE none = none → l = [] := by
  intro l
  cases l with
  | nil => intro _; rfl
  | cons a as =>
      intro h
      rw [List.foldl_cons] at h
      exact absurd h (foldl_ne_none as (stepE none a) (by
        obtain ⟨p, hp⟩ := stepE_some none a
        rw [hp]; simp))

private theorem stepE_dom (s : Option (Int × Int)) (x : Int) :
    ∃ m M, stepE s x = some (m, M) ∧ m ≤ x ∧ x ≤ M := by
  cases s with
  | none => exact ⟨x, x, rfl, by omega, by omega⟩
  | some ab =>
      cases ab with
      | mk a b => exact ⟨min a x, max b x, rfl, by omega, by omega⟩

private theorem foldl_bounds : ∀ (l : List Int) (s r : Option (Int × Int)),
    l.foldl stepE s = r →
    (∀ z ∈ l, ∀ a b, r = some (a, b) → a ≤ z ∧ z ≤ b) ∧
    (∀ a₀ b₀ a b, s = some (a₀, b₀) → r = some (a, b) → a ≤ a₀ ∧ b₀ ≤ b) := by
  intro l
  induction l with
  | nil =>
      intro s r h
      rw [List.foldl_nil] at h
      subst h
      exact ⟨fun _ hz _ _ => absurd hz List.not_mem_nil, fun a₀ b₀ a b hs hr => by
        obtain ⟨rfla, rflb⟩ := Option.some.inj (hs.symm.trans hr)
        exact ⟨by omega, by omega⟩⟩
  | cons x xs ih =>
      intro s r h
      rw [List.foldl_cons] at h
      obtain ⟨h1, h2⟩ := ih (stepE s x) r h
      refine ⟨fun z hz a b hr => ?_, fun a₀ b₀ a b hs hr => ?_⟩
      · obtain ⟨m, M, hmM, hmx, hxM⟩ := stepE_dom s x
        have hb := h2 m M a b hmM hr
        rcases List.mem_cons.mp hz with rfl | hmem
        · exact ⟨by omega, by omega⟩
        · exact h1 z hmem a b hr
      · have hst : stepE s x = some (min a₀ x, max b₀ x) := by rw [hs]; rfl
        obtain ⟨ham, hMb⟩ := h2 (min a₀ x) (max b₀ x) a b hst hr
        exact ⟨by omega, by omega⟩

private theorem foldl_reach_inner : ∀ (l : List Int) (a₀ b₀ p₁ p₂ : Int),
    (∃ r, l.foldl stepE (some (a₀, b₀)) = r ∧ r = some (p₁, p₂)) →
      (∃ z ∈ l, p₁ = z) ∨ (p₁ = a₀ ∧ ∀ z ∈ l, a₀ ≤ z) := by
  intro l
  induction l with
  | nil =>
      intro a₀ b₀ p₁ p₂ ⟨r, h, hr⟩
      rw [List.foldl_nil] at h
      obtain ⟨rfl1, _⟩ := Option.some.inj (h.trans hr)
      right
      exact ⟨rfl, fun z hz => absurd hz List.not_mem_nil⟩
  | cons x xs ih =>
      intro a₀ b₀ p₁ p₂ ⟨r, h, hr⟩
      rw [List.foldl_cons] at h
      have hst : stepE (some (a₀, b₀)) x = some (min a₀ x, max b₀ x) := rfl
      rw [hst] at h
      rcases ih (min a₀ x) (max b₀ x) p₁ p₂ ⟨r, h, hr⟩ with ⟨z, hz, rflz⟩ | ⟨hp1, hall⟩
      · exact Or.inl ⟨z, List.mem_cons_of_mem x hz, rflz⟩
      · by_cases hx : x < a₀
        · exact Or.inl ⟨x, List.mem_cons_self, by rw [hp1]; omega⟩
        · right
          refine ⟨by rw [hp1]; omega, fun z hz => ?_⟩
          rcases List.mem_cons.mp hz with rfl | hmem
          · omega
          · have h1 := hall z hmem
            omega

private theorem foldl_reach : ∀ (l : List Int) (p₁ p₂ : Int),
    (∃ r, l.foldl stepE none = r ∧ r = some (p₁, p₂)) → ∃ z ∈ l, p₁ = z := by
  intro l
  cases l with
  | nil =>
      intro p₁ p₂ ⟨r, h, hr⟩
      rw [List.foldl_nil] at h
      exact absurd (h.trans hr) (by simp)
  | cons x xs =>
      intro p₁ p₂ ⟨r, h, hr⟩
      rw [List.foldl_cons] at h
      have hst : stepE none x = some (x, x) := rfl
      rw [hst] at h
      rcases foldl_reach_inner xs x x p₁ p₂ ⟨r, h, hr⟩ with ⟨z, hz, rflz⟩ | ⟨hp1, _⟩
      · exact ⟨z, List.mem_cons_of_mem x hz, rflz⟩
      · exact ⟨x, List.mem_cons_self, hp1⟩

private theorem ext_some_bounds : ∀ (cs : Cells) (a b : Int), extrema cs = some (a, b) →
    ∀ v ∈ cs, match v with
      | none => True
      | some x => a ≤ x.toInt ∧ x.toInt ≤ b := by
  intro cs a b h v hv
  have h' : (cs.filterMap (fun c => c.map Int64.toInt)).foldl stepE none = some (a, b) := by
    rw [← extrema_eq, h]
  obtain ⟨h1, _⟩ := foldl_bounds _ _ _ h'
  cases v with
  | none => trivial
  | some x =>
      have hm : x.toInt ∈ cs.filterMap (fun c => c.map Int64.toInt) := by
        rw [List.mem_filterMap]
        exact ⟨some x, hv, rfl⟩
      exact h1 _ hm a b rfl

private theorem ext_some_reach : ∀ (cs : Cells) (a b : Int), extrema cs = some (a, b) →
    ∃ x : Int64, some x ∈ cs ∧ a = x.toInt := by
  intro cs a b h
  have h' : (cs.filterMap (fun c => c.map Int64.toInt)).foldl stepE none = some (a, b) := by
    rw [← extrema_eq, h]
  obtain ⟨z, hz, hza⟩ := foldl_reach _ a b ⟨_, h', rfl⟩
  rw [List.mem_filterMap] at hz
  obtain ⟨c, hc, hfl⟩ := hz
  cases c with
  | none => simp at hfl
  | some x =>
      simp at hfl
      exact ⟨x, hc, hza.trans hfl.symm⟩

private theorem ext_none : ∀ (cs : Cells), extrema cs = none → ∀ v ∈ cs, v = none := by
  intro cs h v hv
  have h' : cs.filterMap (fun c => c.map Int64.toInt) = [] :=
    foldl_eq_none _ (by rw [← extrema_eq, h])
  cases v with
  | none => rfl
  | some x =>
      exfalso
      have hm : x.toInt ∈ cs.filterMap (fun c => c.map Int64.toInt) := by
        rw [List.mem_filterMap]
        exact ⟨some x, hv, rfl⟩
      rw [h'] at hm
      exact absurd hm List.not_mem_nil

private theorem canSkip_true_cases (p : Page) (limit : Int) (h : canSkip p limit = true) :
    (∃ a b, p.minVal = some a ∧ p.maxVal = some b ∧
      a ≠ -(2 ^ 63) ∧ (limit ≤ 0 ∨ a ≥ limit ∨ b ≤ -limit)) ∨
    (p.minVal = none ∧ p.maxVal = none) := by
  rcases p with ⟨mn, mx, cs⟩
  unfold canSkip at h
  dsimp only at h
  rcases mn with _|a <;> rcases mx with _|b
  · exact Or.inr ⟨rfl, rfl⟩
  · exact absurd h (by simp)
  · exact absurd h (by simp)
  · rw [Bool.and_eq_true_iff] at h
    obtain ⟨hne, hor⟩ := h
    have hfence : a ≠ -(2 ^ 63) := by simpa [ne_eq] using hne
    have hsplit : (decide (limit ≤ 0) || decide (a ≥ limit) = true) ∨
        decide (b ≤ -limit) = true := Bool.or_eq_true_iff.mp hor
    rcases hsplit with h12 | h3
    · rw [Bool.or_eq_true_iff] at h12
      rcases h12 with h1 | h2
      · exact Or.inl ⟨a, b, rfl, rfl, hfence, Or.inl (of_decide_eq_true h1)⟩
      · exact Or.inl ⟨a, b, rfl, rfl, hfence, Or.inr (Or.inl (of_decide_eq_true h2))⟩
    · exact Or.inl ⟨a, b, rfl, rfl, hfence, Or.inr (Or.inr (of_decide_eq_true h3))⟩

/-! ## Cell-level collectors -/

private def ovfIn : Nat → Cells → Nat → Option (Nat × Nat)
  | _i, [], _n => none
  | i, some x :: cs, n => if x.toInt = -(2 ^ 63) then some (i, n) else ovfIn i cs (n + 1)
  | i, none :: cs, n => ovfIn i cs (n + 1)

private def hitsC (limit : Int) : Nat → Cells → Nat → List (Nat × Nat)
  | _i, [], _n => []
  | i, some x :: cs, n =>
      if x.toInt = -(2 ^ 63) then []
      else if (x.toInt.natAbs : Int) < limit then (i, n) :: hitsC limit i cs (n + 1)
      else hitsC limit i cs (n + 1)
  | i, none :: cs, n => hitsC limit i cs (n + 1)

private theorem ovfIn_none_of_no_ovf : ∀ (i : Nat) (cs : Cells) (n : Nat),
    (∀ v ∈ cs, match v with | none => True | some x => x.toInt ≠ -(2 ^ 63)) →
      ovfIn i cs n = none := by
  intro i cs
  induction cs with
  | nil => intro n _; rfl
  | cons a as ih =>
      intro n h
      cases a with
      | none => exact ih (n + 1) fun v hv => h v (List.mem_cons_of_mem _ hv)
      | some x =>
          rw [ovfIn, if_neg (h (some x) List.mem_cons_self)]
          exact ih (n + 1) fun v hv => h v (List.mem_cons_of_mem _ hv)

private theorem hitsC_nil : ∀ (limit : Int) (i : Nat) (cs : Cells) (n : Nat),
    (∀ v ∈ cs, match v with
      | none => True
      | some x => x.toInt ≠ -(2 ^ 63) ∧ ¬((x.toInt.natAbs : Int) < limit)) →
      hitsC limit i cs n = [] := by
  intro limit i cs
  induction cs with
  | nil => intro n _; rfl
  | cons a as ih =>
      intro n h
      cases a with
      | none => exact ih (n + 1) fun v hv => h v (List.mem_cons_of_mem _ hv)
      | some x =>
          obtain ⟨h1, h2⟩ := h (some x) List.mem_cons_self
          rw [hitsC, if_neg h1, if_neg h2]
          exact ih (n + 1) fun v hv => h v (List.mem_cons_of_mem _ hv)

private theorem skip_silent (p : Page) (limit : Int) (hp : valid p) (hsk : canSkip p limit = true) :
    (∀ v ∈ p.cells, match v with
      | none => True
      | some x => x.toInt ≠ -(2 ^ 63) ∧ ¬((x.toInt.natAbs : Int) < limit)) := by
  obtain ⟨hmin, hmax⟩ := hp
  rcases canSkip_true_cases p limit hsk with ⟨a, b, hva, hvb, hfence, hdisj⟩ | ⟨hva, hvb⟩
  · have hnone : extrema p.cells ≠ none := by
      intro he
      rw [hmin] at hva
      rw [he] at hva
      exact absurd hva (by simp)
    obtain ⟨c, hc⟩ : ∃ c, extrema p.cells = some (a, c) := by
      cases h : extrema p.cells with
      | none => exact absurd h hnone
      | some mm =>
          cases mm with
          | mk m1 m2 =>
              have he1 : p.minVal = some m1 := by rw [hmin, h]; rfl
              rw [hva] at he1
              obtain ⟨rflma⟩ := Option.some.inj he1
              exact ⟨m2, rfl⟩
    have hc' : c = b := by
      have he2 : p.maxVal = some c := by rw [hmax, hc]; rfl
      rw [hvb] at he2
      exact (Option.some.inj he2).symm
    subst hc'
    obtain ⟨x0, hx0, hax0⟩ := ext_some_reach p.cells a c hc
    have hbnds := ext_some_bounds p.cells a c hc
    have hage : (-(2 ^ 63) : Int) ≤ a := by rw [hax0]; exact toInt_ge x0
    intro v hv
    cases v with
    | none => trivial
    | some x =>
        obtain ⟨hax, hxb⟩ := hbnds _ hv
        refine ⟨by omega, ?_⟩
        rcases hdisj with hl0 | ha | hb
        · intro hh; omega
        · intro hh; omega
        · intro hh; omega
  · have hext : extrema p.cells = none := by
      cases h : extrema p.cells with
      | none => rfl
      | some mm =>
          cases mm with
          | mk m1 m2 =>
              have he1 : p.minVal = some m1 := by rw [hmin, h]; rfl
              rw [hva] at he1
              exact absurd he1 (by simp)
    intro v hv
    cases v with
    | none => trivial
    | some x => exact absurd (ext_none p.cells hext _ hv) (by simp)

private theorem skip_page_silent (limit : Int) (p : Page) (i : Nat)
    (hp : valid p) (hsk : canSkip p limit = true) :
    hitsC limit i p.cells 0 = [] ∧ ovfIn i p.cells 0 = none := by
  have hall := skip_silent p limit hp hsk
  refine ⟨hitsC_nil limit i p.cells 0 hall, ovfIn_none_of_no_ovf i p.cells 0 ?_⟩
  intro v hv
  cases v with
  | none => trivial
  | some x => exact (hall _ hv).1

/-! ## List-level collectors -/

private def np (limit : Int) (pi : Page × Nat) : Bool := !canSkip pi.1 limit

private def pre : Bool → Int → List (Page × Nat) → List (Page × Nat)
  | true, limit, l => l.filter (np limit)
  | false, _, l => l

private def clipC : List (Page × Nat) → List (Page × Nat)
  | [] => []
  | (p, i) :: t => if ovfIn i p.cells 0 = none then (p, i) :: clipC t else [(p, i)]

private def collectHits (limit : Int) : List (Page × Nat) → List (Nat × Nat)
  | [] => []
  | (p, i) :: t => hitsC limit i p.cells 0 ++ collectHits limit t

private def collectOpened : List (Page × Nat) → List Nat
  | [] => []
  | (p, i) :: t => i :: collectOpened t

private def firstOvf : List (Page × Nat) → Option (Nat × Nat)
  | [] => none
  | (p, i) :: t => match ovfIn i p.cells 0 with
    | some q => some q
    | none => firstOvf t

private theorem clipC_subset : ∀ (l : List (Page × Nat)) (x : Page × Nat),
    x ∈ clipC l → x ∈ l := by
  intro l
  induction l with
  | nil => intro x h; exact absurd h List.not_mem_nil
  | cons a as ih =>
      intro x h
      rw [clipC] at h
      by_cases ho : ovfIn a.snd a.fst.cells 0 = none
      · rw [if_pos ho] at h
        rcases List.mem_cons.mp h with rfl | hmem
        · exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (ih x hmem)
      · rw [if_neg ho] at h
        obtain ⟨rflx⟩ := List.mem_singleton.mp h
        exact List.mem_cons_self

private theorem clipC_filter (limit : Int) :
    ∀ (l : List (Page × Nat)),
      (∀ pi ∈ l, ¬np limit pi = true → ovfIn pi.2 pi.1.cells 0 = none) →
      clipC (l.filter (np limit)) = (clipC l).filter (np limit) := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a as ih =>
      intro h
      have hrest : ∀ pi ∈ as, ¬np limit pi = true → ovfIn pi.2 pi.1.cells 0 = none :=
        fun pi hpi hnp => h pi (List.mem_cons_of_mem _ hpi) hnp
      by_cases ha : np limit a = true
      · by_cases ho : ovfIn a.snd a.fst.cells 0 = none
        · rw [List.filter_cons_of_pos ha, clipC, if_pos ho, ih hrest, clipC, if_pos ho,
            List.filter_cons_of_pos ha]
        · rw [List.filter_cons_of_pos ha, clipC, if_neg ho, clipC, if_neg ho,
            List.filter_cons_of_pos ha, List.filter_nil]
      · rw [List.filter_cons_of_neg ha, ih hrest, clipC,
            if_pos (h a List.mem_cons_self ha), List.filter_cons_of_neg ha]

private theorem collectHits_filter (limit : Int) :
    ∀ (M : List (Page × Nat)),
      (∀ pi ∈ M, ¬np limit pi = true →
        hitsC limit pi.2 pi.1.cells 0 = [] ∧ ovfIn pi.2 pi.1.cells 0 = none) →
      collectHits limit (M.filter (np limit)) = collectHits limit M := by
  intro M
  induction M with
  | nil => intro _; rfl
  | cons a as ih =>
      intro h
      have hrest : ∀ pi ∈ as, ¬np limit pi = true →
          hitsC limit pi.2 pi.1.cells 0 = [] ∧ ovfIn pi.2 pi.1.cells 0 = none :=
        fun pi hpi hnp => h pi (List.mem_cons_of_mem _ hpi) hnp
      by_cases ha : np limit a = true
      · rw [List.filter_cons_of_pos ha, collectHits, collectHits, ih hrest]
      · rw [List.filter_cons_of_neg ha, collectHits, (h a List.mem_cons_self ha).1,
            List.nil_append, ih hrest]

private theorem firstOvf_filter (limit : Int) :
    ∀ (M : List (Page × Nat)),
      (∀ pi ∈ M, ¬np limit pi = true → ovfIn pi.2 pi.1.cells 0 = none) →
      firstOvf (M.filter (np limit)) = firstOvf M := by
  intro M
  induction M with
  | nil => intro _; rfl
  | cons a as ih =>
      intro h
      have hrest : ∀ pi ∈ as, ¬np limit pi = true → ovfIn pi.2 pi.1.cells 0 = none :=
        fun pi hpi hnp => h pi (List.mem_cons_of_mem _ hpi) hnp
      by_cases ha : np limit a = true
      · rw [List.filter_cons_of_pos ha, firstOvf, firstOvf, ih hrest]
      · rw [List.filter_cons_of_neg ha, ih hrest, firstOvf]
        have hove : ovfIn a.snd a.fst.cells 0 = none := h a List.mem_cons_self ha
        rw [hove]

private theorem collectOpened_map (l : List (Page × Nat)) :
    collectOpened l = l.map Prod.snd := by
  induction l with
  | nil => rfl
  | cons a as ih => rw [collectOpened, List.map_cons, ih]

private theorem map_fst_zipIdx (n : Nat) (ps : List Page) :
    (List.zipIdx ps n).map Prod.fst = ps := by
  induction ps generalizing n with
  | nil => rfl
  | cons a as ih =>
      have hz : List.zipIdx (a :: as) n = (a, n) :: List.zipIdx as (n + 1) := rfl
      rw [hz, List.map_cons, ih]

/-! ## Per-page loop bridge -/

/-- Body of the desugared per-cell loop (pair state, `Id` monad). -/
private def ib2 : Int → Nat → (Option Int64 × Nat) → Option Obs × Obs → ForInStep (Option Obs × Obs)
  | limit, i, (v, j), st =>
    match v with
    | none => ForInStep.yield (none, st.snd)
    | some x =>
      if x.toInt = -(2 ^ 63) then
        ForInStep.done (some { st.snd with failure := some (i, j) }, st.snd)
      else if (x.toInt.natAbs : Int) < limit then
        ForInStep.yield (none, { st.snd with hits := (i, j) :: st.snd.hits })
      else
        ForInStep.yield (none, st.snd)

private theorem ib2_some (limit : Int) (i n : Nat) (x : Int64) (r : Option Obs) (s : Obs) :
    ib2 limit i (some x, n) (r, s) =
      if x.toInt = -(2 ^ 63) then
        ForInStep.done (some { s with failure := some (i, n) }, s)
      else if (x.toInt.natAbs : Int) < limit then
        ForInStep.yield (none, { s with hits := (i, n) :: s.hits })
      else
        ForInStep.yield (none, s) := rfl

private theorem reverse_cons_append (a : Nat × Nat) (t s : List (Nat × Nat)) :
    (a :: t).reverse ++ s = t.reverse ++ (a :: s) := by
  rw [List.reverse_cons, List.append_assoc]
  rfl

private theorem page_spec (limit : Int) (i : Nat) :
    ∀ (cs : Cells) (n : Nat) (s : Obs),
      match ovfIn i cs n with
      | none =>
          (forIn (List.zipIdx cs n) (none, s) (ib2 limit i) : Id (Option Obs × Obs))
            = (none, { s with hits := (hitsC limit i cs n).reverse ++ s.hits })
      | some q =>
          (forIn (List.zipIdx cs n) (none, s) (ib2 limit i) : Id (Option Obs × Obs))
            = (some { s with
                  hits := (hitsC limit i cs n).reverse ++ s.hits, failure := some q },
                { s with hits := (hitsC limit i cs n).reverse ++ s.hits }) := by
  intro cs
  induction cs with
  | nil => intro n s; rfl
  | cons a as ih =>
    intro n s
    cases a with
    | none =>
      have hz : List.zipIdx (none :: as) n = (none, n) :: List.zipIdx as (n + 1) := rfl
      have hnone : ib2 limit i (none, n) (none, s) = ForInStep.yield (none, s) := rfl
      have hred : (forIn (List.zipIdx (none :: as) n) (none, s) (ib2 limit i) : Id (Option Obs × Obs))
          = (forIn (List.zipIdx as (n + 1)) (none, s) (ib2 limit i) : Id (Option Obs × Obs)) := by
        rw [hz, List.forIn_cons, hnone]
        rfl
      have hov : ovfIn i (none :: as) n = ovfIn i as (n + 1) := rfl
      have hh : hitsC limit i (none :: as) n = hitsC limit i as (n + 1) := rfl
      rw [hred, hov, hh]
      exact ih (n + 1) s
    | some x =>
      have hred : (forIn (List.zipIdx (some x :: as) n) (none, s) (ib2 limit i) : Id (Option Obs × Obs))
          = match ib2 limit i (some x, n) (none, s) with
            | ForInStep.done b => b
            | ForInStep.yield b => (forIn (List.zipIdx as (n + 1)) b (ib2 limit i) : Id (Option Obs × Obs)) := by
        rw [show List.zipIdx (some x :: as) n = (some x, n) :: List.zipIdx as (n + 1) from rfl,
          List.forIn_cons]
        generalize ib2 limit i (some x, n) (none, s) = m
        cases m <;> rfl
      have hov : ovfIn i (some x :: as) n =
          if x.toInt = -(2 ^ 63) then some (i, n) else ovfIn i as (n + 1) := rfl
      have hh : hitsC limit i (some x :: as) n =
          if x.toInt = -(2 ^ 63) then []
          else if (x.toInt.natAbs : Int) < limit then (i, n) :: hitsC limit i as (n + 1)
          else hitsC limit i as (n + 1) := rfl
      rw [hred, hov, hh]
      by_cases hfence : x.toInt = -(2 ^ 63)
      · rw [if_pos hfence, if_pos hfence, List.reverse_nil, List.nil_append]
        have hstep : ib2 limit i (some x, n) (none, s)
            = ForInStep.done (some { s with failure := some (i, n) }, s) := by
          rw [ib2_some, if_pos hfence]
        rw [hstep]
      · rw [if_neg hfence]
        by_cases hhits : (x.toInt.natAbs : Int) < limit
        · rw [if_neg hfence, if_pos hhits]
          have hstep : ib2 limit i (some x, n) (none, s)
              = ForInStep.yield (none, { s with hits := (i, n) :: s.hits }) := by
            rw [ib2_some, if_neg hfence, if_pos hhits]
          have h1 := ih (n + 1) { s with hits := (i, n) :: s.hits }
          cases h2 : ovfIn i as (n + 1) with
          | none =>
              rw [h2] at h1
              rw [hstep]
              show (forIn (List.zipIdx as (n + 1)) (none, { s with hits := (i, n) :: s.hits })
                  (ib2 limit i) : Id (Option Obs × Obs)) = _
              rw [h1, reverse_cons_append]
          | some q =>
              rw [h2] at h1
              rw [hstep]
              show (forIn (List.zipIdx as (n + 1)) (none, { s with hits := (i, n) :: s.hits })
                  (ib2 limit i) : Id (Option Obs × Obs)) = _
              rw [h1, reverse_cons_append]
        · rw [if_neg hfence, if_neg hhits]
          have hstep : ib2 limit i (some x, n) (none, s) = ForInStep.yield (none, s) := by
            rw [ib2_some, if_neg hfence, if_neg hhits]
          have h1 := ih (n + 1) s
          cases h2 : ovfIn i as (n + 1) with
          | none =>
              rw [h2] at h1
              rw [hstep]
              show (forIn (List.zipIdx as (n + 1)) (none, s) (ib2 limit i) : Id (Option Obs × Obs)) = _
              exact h1
          | some q =>
              rw [h2] at h1
              rw [hstep]
              show (forIn (List.zipIdx as (n + 1)) (none, s) (ib2 limit i) : Id (Option Obs × Obs)) = _
              exact h1

/-! ## Page-level loop -/

private def ib1 (prune : Bool) (limit : Int)
    : (Page × Nat) → Option Obs × Obs → ForInStep (Option Obs × Obs)
  | (p, i), (r, s) =>
    if !(prune && canSkip p limit) then
      let res : Option Obs × Obs :=
        (forIn (List.zipIdx p.cells) (none, { s with opened := i :: s.opened }) (ib2 limit i)
          : Id (Option Obs × Obs))
      match res with
      | (some f, s') => ForInStep.done (some f, s')
      | (none, s') => ForInStep.yield (none, s')
    else ForInStep.yield (none, s)

private def go (prune : Bool) (limit : Int) (l : List (Page × Nat)) (s₀ : Obs) : Obs :=
  match (forIn l (none, s₀) (ib1 prune limit) : Id (Option Obs × Obs)) with
  | (some f, _) => f
  | (none, s) => s

private def obsWith (limit : Int) (M : List (Page × Nat)) (s₀ : Obs) : Obs :=
  { s₀ with
    opened := collectOpened (clipC M).reverse ++ s₀.opened,
    hits := (collectHits limit (clipC M)).reverse ++ s₀.hits,
    failure := firstOvf M }

private theorem go_spec (prune : Bool) (limit : Int) :
    ∀ (l : List (Page × Nat)) (s₀ : Obs), s₀.failure = none → (∀ pi ∈ l, valid pi.1) →
      go prune limit l s₀ = obsWith limit (pre prune limit l) s₀ := by
  intro l
  induction l with
  | nil =>
      intro s₀ hs₀ _
      cases s₀ with
      | mk f o h =>
          have ho : o = none := hs₀
          subst ho
          cases prune <;> rfl
  | cons a ms ih =>
      obtain ⟨p, i⟩ := a
      intro s₀ hs₀ hv
      have hrest : ∀ pi ∈ ms, valid pi.1 := fun pi hpi => hv _ (List.mem_cons_of_mem _ hpi)
      have hgo : go prune limit ((p, i) :: ms) s₀
          = match ib1 prune limit (p, i) (none, s₀) with
            | ForInStep.done b => match b with | (some f, _) => f | (none, s) => s
            | ForInStep.yield b =>
                match (forIn ms b (ib1 prune limit) : Id (Option Obs × Obs)) with
                | (some f, _) => f
                | (none, s) => s := by
        unfold go
        rw [List.forIn_cons]
        cases ib1 prune limit (p, i) (none, s₀) with
        | done b =>
            cases b with
            | mk f' s' => cases f' <;> rfl
        | yield b => rfl
      by_cases hC : (prune && canSkip p limit) = true
      · obtain ⟨hpr, hsk⟩ := Bool.and_eq_true_iff.mp hC
        subst hpr
        have hnp : ¬np limit (p, i) = true := by
          show ¬(Bool.not (canSkip p limit) = true)
          rw [hsk]
          exact Bool.false_ne_true
        have hpre : pre true limit ((p, i) :: ms) = pre true limit ms := by
          show ((p, i) :: ms).filter (np limit) = ms.filter (np limit)
          rw [List.filter_cons_of_neg hnp]
        have hx : ¬(Bool.not (true && canSkip p limit) = true) := by
          rw [hsk]
          exact Bool.false_ne_true
        have hibs : ib1 true limit (p, i) (none, s₀) = ForInStep.yield (none, s₀) := by
          show (if Bool.not (true && canSkip p limit) then _ else ForInStep.yield (none, s₀)) = _
          rw [if_neg hx]
        rw [hgo, hibs]
        show go true limit ms s₀ = obsWith limit (pre true limit ((p, i) :: ms)) s₀
        rw [ih s₀ hs₀ hrest, hpre]
      · have hnot : Bool.not (prune && canSkip p limit) = true := by
          rw [dfalse hC]
          rfl
        have hpre : pre prune limit ((p, i) :: ms)
            = (p, i) :: pre prune limit ms := by
          cases hpr : prune with
          | false => rfl
          | true =>
              rw [hpr] at hC
              show ((p, i) :: ms).filter (np limit) = (p, i) :: ms.filter (np limit)
              rw [List.filter_cons_of_pos]
              cases hc : canSkip p limit with
              | true => exact (hC (by rw [hc]; rfl)).elim
              | false => dsimp only [np]; rw [hc]; rfl
        have hib1o : ib1 prune limit (p, i) (none, s₀)
            = match (forIn (List.zipIdx p.cells)
                (none, { s₀ with opened := i :: s₀.opened }) (ib2 limit i)
                : Id (Option Obs × Obs)) with
              | (some f, s') => ForInStep.done (some f, s')
              | (none, s') => ForInStep.yield (none, s') := by
          show (if Bool.not (prune && canSkip p limit) then _ else _) = _
          rw [if_pos hnot]
          rfl
        rw [hgo, hib1o]
        cases hov : ovfIn i p.cells 0 with
        | none =>
            have hpg := page_spec limit i p.cells 0 { s₀ with opened := i :: s₀.opened }
            rw [hov] at hpg
            rw [hpg]
            show (go prune limit ms ({ s₀ with opened := i :: s₀.opened, hits := (hitsC limit i p.cells 0).reverse ++ s₀.hits } : Obs) = obsWith limit (pre prune limit ((p, i) :: ms)) s₀)
            have hs' : ({ s₀ with opened := i :: s₀.opened, hits := (hitsC limit i p.cells 0).reverse ++ s₀.hits } : Obs).failure = none := hs₀
            rw [ih _ hs' hrest, hpre]
            have hcl : clipC ((p, i) :: pre prune limit ms)
                = (p, i) :: clipC (pre prune limit ms) := by
              show (if ovfIn i p.cells 0 = none then (p, i) :: clipC (pre prune limit ms)
                else [(p, i)]) = _
              rw [if_pos hov]
            have hfo : firstOvf ((p, i) :: pre prune limit ms)
                = firstOvf (pre prune limit ms) := by
              show (match ovfIn i p.cells 0 with | some q' => some q' | none => firstOvf (pre prune limit ms)) = _
              rw [hov]
            have hch : collectHits limit ((p, i) :: clipC (pre prune limit ms))
                = hitsC limit i p.cells 0
                    ++ collectHits limit (clipC (pre prune limit ms)) := rfl
            have hco : collectOpened (((p, i) :: clipC (pre prune limit ms)).reverse)
                = collectOpened ((clipC (pre prune limit ms)).reverse) ++ [i] := by
              rw [List.reverse_cons, collectOpened_map, collectOpened_map, List.map_append]
              rfl
            rw [obsWith, obsWith, hcl, hfo, hch, hco]
            simp only [List.reverse_nil, List.nil_append, List.reverse_cons,
              List.cons_append, List.reverse_append, List.append_assoc, List.append_nil]
        | some q =>
            have hpg := page_spec limit i p.cells 0 { s₀ with opened := i :: s₀.opened }
            rw [hov] at hpg
            rw [hpg]
            show ({ s₀ with opened := i :: s₀.opened, hits := (hitsC limit i p.cells 0).reverse ++ s₀.hits, failure := some q } : Obs) = obsWith limit (pre prune limit ((p, i) :: ms)) s₀
            rw [hpre]
            have hcl : clipC ((p, i) :: pre prune limit ms) = [(p, i)] := by
              show (if ovfIn i p.cells 0 = none then (p, i) :: clipC (pre prune limit ms)
                else [(p, i)]) = _
              have hnn : ¬(ovfIn i p.cells 0 = none) := by
                rw [hov]
                exact fun he => nomatch he
              rw [if_neg hnn]
            have hfo : firstOvf ((p, i) :: pre prune limit ms) = some q := by
              show (match ovfIn i p.cells 0 with | some q' => some q' | none => firstOvf (pre prune limit ms)) = _
              rw [hov]
            have hch : collectHits limit [(p, i)] = hitsC limit i p.cells 0 ++ [] := rfl
            have hco : collectOpened ([(p, i)].reverse) = [i] := by
              rw [List.reverse_singleton]
              rfl
            rw [obsWith, hcl, hfo, hch, hco]
            simp only [List.reverse_nil, List.nil_append, List.reverse_cons,
              List.cons_append, List.reverse_append, List.append_assoc, List.append_nil]

/-! ## Bridging the real `run` -/

private theorem getSomeMem {α : Type} :
    ∀ (l : List α) (i : Nat) (a : α), l[i]? = some a → a ∈ l := by
  intro l i a h
  induction l generalizing i with
  | nil => cases i <;> simp at h
  | cons x xs ih =>
      cases i with
      | zero =>
          rw [List.getElem?_cons_zero] at h
          have hxa : x = a := Option.some.inj h
          subst hxa
          exact List.mem_cons_self
      | succ i' =>
          rw [List.getElem?_cons_succ] at h
          exact List.mem_cons_of_mem x (ih i' h)

/-- The per-cell function `run`'s compiled loop uses (named via its matcher constant). -/
private theorem hinner (limit : Int) (i : Nat) :
    ((fun x_1 __s =>
            run.match_1 (fun _ => Id (ForInStep (Option Obs × Obs))) x_1.fst
              (fun x_2 =>
                if x_2.toInt = -(2 ^ 63) then
                  pure (ForInStep.done
                    (some { hits := __s.snd.hits, failure := some (i, x_1.snd), opened := __s.snd.opened }, __s.snd))
                else
                  if (x_2.toInt.natAbs : Int) < limit then
                    pure (ForInStep.yield (none,
                      { hits := (i, x_1.snd) :: __s.snd.hits, failure := __s.snd.failure, opened := __s.snd.opened }))
                  else
                    pure (ForInStep.yield (none, __s.snd)))
              (fun x => pure (ForInStep.yield (none, __s.snd))))
        : (Option Int64 × Nat) → Option Obs × Obs → Id (ForInStep (Option Obs × Obs)))
      = ib2 limit i := by
  funext y τ
  cases y with
  | mk v j =>
      cases τ with
      | mk rr ss =>
          cases v <;> rfl

private theorem run_go (prune : Bool) (limit : Int) (ps : List Page) :
    run prune limit ps = go prune limit (List.zipIdx ps) {} := by
  have hΛ : ((fun x __s =>
            if !(prune && canSkip x.fst limit) then
              (@Bind.bind Id _ (Option Obs × Obs) (ForInStep (Option Obs × Obs))
                ((forIn (List.zipIdx x.fst.cells)
                  (none, { hits := __s.snd.hits, failure := __s.snd.failure, opened := x.snd :: __s.snd.opened })
                  (fun x_1 __s =>
                    run.match_1 (fun _ => Id (ForInStep (Option Obs × Obs))) x_1.fst
                      (fun x_2 =>
                        if x_2.toInt = -(2 ^ 63) then
                          pure (ForInStep.done
                            (some { hits := __s.snd.hits, failure := some (x.snd, x_1.snd), opened := __s.snd.opened }, __s.snd))
                        else
                          if (x_2.toInt.natAbs : Int) < limit then
                            pure (ForInStep.yield (none,
                              { hits := (x.snd, x_1.snd) :: __s.snd.hits, failure := __s.snd.failure, opened := __s.snd.opened }))
                          else
                            pure (ForInStep.yield (none, __s.snd)))
                      (fun x => pure (ForInStep.yield (none, __s.snd))))) : Id (Option Obs × Obs))
                (fun __s =>
                  Break.runK.match_1 (fun _ => Id (ForInStep (Option Obs × Obs))) __s.fst
                    (fun r => pure (ForInStep.done (some r, __s.snd)))
                    (fun _ => pure (ForInStep.yield (none, __s.snd)))))
            else
              pure (ForInStep.yield (none, __s.snd)))
          : (Page × Nat) → Option Obs × Obs → Id (ForInStep (Option Obs × Obs)))
      = ib1 prune limit := by
    funext a b
    obtain ⟨p, i⟩ := a
    obtain ⟨r, s⟩ := b
    dsimp only
    rw [hinner limit i]
    have hb : ∀ (t : Id (Option Obs × Obs)),
        ((@Bind.bind Id _ (Option Obs × Obs) (ForInStep (Option Obs × Obs)) t
              (fun __s =>
                Break.runK.match_1 (fun _ => Id (ForInStep (Option Obs × Obs))) __s.fst
                  (fun r => pure (ForInStep.done (some r, __s.snd)))
                  (fun _ => pure (ForInStep.yield (none, __s.snd)))))
          : Id (ForInStep (Option Obs × Obs)))
        = (match t with
          | (some f, s') => ForInStep.done (some f, s')
          | (none, s') => ForInStep.yield (none, s')) := by
      intro t
      cases t with
      | mk a b => cases a <;> rfl
    rw [hb]
    rfl
  unfold TinylambOpt.ResearchGoals.CheckedAbsFence.run
  dsimp only [Id.run]
  rw [hΛ]
  cases h : (forIn (List.zipIdx ps) (none, ({ } : Obs)) (ib1 prune limit)
      : Id (Option Obs × Obs)) with
  | mk v p =>
      unfold go
      rw [h]
      cases v <;> rfl

/-! ## Opened-list bookkeeping -/

private theorem map_snd_filter_np (ps : List Page) (limit : Int) :
    ∀ (l : List (Page × Nat)), (∀ x ∈ l, x ∈ List.zipIdx ps) →
      (l.filter (np limit)).map Prod.snd
        = (l.map Prod.snd).filter (fun i => (ps[i]?.any fun p => !canSkip p limit)) := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons a as ih =>
      obtain ⟨p, i⟩ := a
      intro hsub
      have hmem : (p, i) ∈ List.zipIdx ps := hsub _ List.mem_cons_self
      have hgi : (ps[i]?.any fun p => !canSkip p limit) = np limit (p, i) := by
        dsimp only [np]
        rw [List.mem_zipIdx_iff_getElem?] at hmem
        rw [hmem]
        rfl
      by_cases hnpa : np limit (p, i) = true
      · rw [List.filter_cons_of_pos hnpa, List.map_cons, List.map_cons]
        have hg :
            ((fun i => (ps[i]?.any fun p => !canSkip p limit)) ((p, i).snd)) = true := by
          show (ps[i]?.any fun p => !canSkip p limit) = true
          rw [hgi]
          exact hnpa
        rw [List.filter_cons_of_pos
          (p := fun i => (ps[i]?.any fun p => !canSkip p limit)) (a := (p, i).snd) hg]
        rw [ih (fun x hx => hsub x (List.mem_cons_of_mem _ hx))]
      · have hg :
            ¬((fun i => (ps[i]?.any fun p => !canSkip p limit)) ((p, i).snd) = true) := by
          intro h
          apply hnpa
          rw [← hgi]
          exact h
        rw [List.filter_cons_of_neg hnpa, List.map_cons]
        rw [List.filter_cons_of_neg
          (p := fun i => (ps[i]?.any fun p => !canSkip p limit)) (a := (p, i).snd) hg]
        exact ih (fun x hx => hsub x (List.mem_cons_of_mem _ hx))

/-! ## Main theorem -/

theorem sound : TinylambOpt.ResearchGoals.CheckedAbsFence.Goal := by
  intro ps limit hvalid
  show (run true limit ps).hits = (run false limit ps).hits ∧
    (run true limit ps).failure = (run false limit ps).failure ∧
    (run true limit ps).opened
      = (run false limit ps).opened.filter (fun i => (ps[i]?.any fun p => !canSkip p limit))
  have hmem : ∀ pi ∈ List.zipIdx ps, pi.1 ∈ ps := by
    intro pi hpi
    obtain ⟨p, i⟩ := pi
    rw [List.mem_zipIdx_iff_getElem?] at hpi
    exact getSomeMem ps i p hpi
  have hskipovf : ∀ pi ∈ List.zipIdx ps, ¬np limit pi = true → ovfIn pi.2 pi.1.cells 0 = none := by
    intro pi hpi hnp
    obtain ⟨p, i⟩ := pi
    have hsk : canSkip p limit = true := by
      dsimp only [np] at hnp
      cases h : canSkip p limit with
      | true => rfl
      | false => exact (hnp (by rw [h]; exact Bool.not_false)).elim
    exact (skip_page_silent limit p i (hvalid p (hmem (p, i) hpi)) hsk).2
  have hboth : ∀ pi ∈ clipC (List.zipIdx ps), ¬np limit pi = true →
      hitsC limit pi.2 pi.1.cells 0 = [] ∧ ovfIn pi.2 pi.1.cells 0 = none := by
    intro pi hpi hnp
    obtain ⟨p, i⟩ := pi
    have hsk : canSkip p limit = true := by
      dsimp only [np] at hnp
      cases h : canSkip p limit with
      | true => rfl
      | false => exact (hnp (by rw [h]; exact Bool.not_false)).elim
    exact skip_page_silent limit p i (hvalid _ (hmem _ (clipC_subset _ _ hpi))) hsk
  have hzsub : ∀ x ∈ clipC (List.zipIdx ps), x ∈ List.zipIdx ps :=
    fun x hx => clipC_subset (List.zipIdx ps) x hx
  have hopt := go_spec true limit (List.zipIdx ps) {} rfl (fun pi hpi => hvalid _ (hmem _ hpi))
  have href := go_spec false limit (List.zipIdx ps) {} rfl (fun pi hpi => hvalid _ (hmem _ hpi))
  rw [run_go true, run_go false, hopt, href]
  simp only [obsWith, pre]
  rw [clipC_filter limit (List.zipIdx ps) hskipovf,
    collectHits_filter limit (clipC (List.zipIdx ps)) hboth,
    firstOvf_filter limit (List.zipIdx ps) hskipovf]
  rw [collectOpened_map (List.reverse (List.filter (np limit) (clipC (List.zipIdx ps)))),
    List.map_reverse,
    map_snd_filter_np ps limit (clipC (List.zipIdx ps)) hzsub,
    ← List.filter_reverse,
    collectOpened_map (List.reverse (clipC (List.zipIdx ps))),
    List.map_reverse, List.append_nil]
  rw [List.append_nil, List.append_nil]
  exact ⟨rfl, rfl, rfl⟩

end TinylambOpt.ResearchProofs.CheckedAbsFence
