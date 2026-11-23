import TinylambOpt.ResearchGoals.AnalyticsWorkloads.BigBench

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.BigBench

open TinylambOpt.ResearchGoals.AnalyticsWorkloads.BigBench
open TinylambOpt.ResearchGoals.AnalyticsWorkloads

/-! ### Url lawful instances -/

private instance lawfulBEqUrl : LawfulBEq Url where
  eq_of_beq := by
    intro a b h
    cases a with
    | nullKey =>
      cases b with
      | nullKey => rfl
      | page n => exact absurd h Bool.false_ne_true
      | literal s => exact absurd h Bool.false_ne_true
    | page n =>
      cases b with
      | nullKey => exact absurd h Bool.false_ne_true
      | page m =>
        have hnm : (n == m) = true := h
        have hn : n = m := eq_of_beq hnm
        rw [hn]
      | literal s => exact absurd h Bool.false_ne_true
    | literal s =>
      cases b with
      | nullKey => exact absurd h Bool.false_ne_true
      | page n => exact absurd h Bool.false_ne_true
      | literal t =>
        have hst : (s == t) = true := h
        have hs : s = t := eq_of_beq hst
        rw [hs]
  rfl := by
    intro a
    cases a with
    | nullKey => rfl
    | page n => show (n == n) = true; exact BEq.rfl
    | literal s => show (s == s) = true; exact BEq.rfl

/-! ### Url helpers -/

private theorem encode_none : encode none = Url.nullKey := rfl

private theorem encode_some_spec (s : String) :
    (∃ n : Nat, encode (some s) = Url.page n ∧ s = "page-" ++ toString n) ∨
    encode (some s) = Url.literal s := by
  rcases hnat : (s.drop 5).toString.toNat? with _ | n
  · right
    simp only [encode, hnat]
  · by_cases hc : "page-" ++ toString n == s
    · left
      refine ⟨n, ?_, (eq_of_beq hc).symm⟩
      simp only [encode, hnat]
      rw [ite_eq_left hc]
    · right
      simp only [encode, hnat]
      rw [ite_eq_right hc]

private theorem encode_inj (a b : Option String) : encode a = encode b → a = b := by
  intro h
  cases a with
  | none =>
    cases b with
    | none => rfl
    | some t =>
      rcases encode_some_spec t with ⟨n, hpg, -⟩ | hlit
      · rw [encode_none, hpg] at h
        simp at h
      · rw [encode_none, hlit] at h
        simp at h
  | some s =>
    cases b with
    | none =>
      rcases encode_some_spec s with ⟨n, hpg, -⟩ | hlit
      · rw [hpg, encode_none] at h
        simp at h
      · rw [hlit, encode_none] at h
        simp at h
    | some t =>
      rcases encode_some_spec s with ⟨ns, hps, hs_eq⟩ | hls
      · rcases encode_some_spec t with ⟨nt, hpt, ht_eq⟩ | hlt
        · rw [hps, hpt] at h
          have hnm : ns = nt := by injection h
          rw [hs_eq, ht_eq, hnm]
        · rw [hps, hlt] at h
          simp at h
      · rcases encode_some_spec t with ⟨nt, hpt, ht_eq⟩ | hlt
        · rw [hls, hpt] at h
          simp at h
        · rw [hls, hlt] at h
          have hst : s = t := by injection h
          rw [hst]

private theorem decode_encode (key : Option String) : decode (encode key) = key := by
  cases key with
  | none => rfl
  | some s =>
    rcases encode_some_spec s with ⟨n, hpg, hs⟩ | hlit
    · rw [hpg]
      show some ("page-" ++ toString n) = some s
      rw [← hs]
    · simp only [hlit, decode]

private theorem encode_beq (a b : Option String) : (encode a == encode b) = (a == b) := by
  apply Bool.eq_iff_iff.mpr
  simp only [beq_iff_eq]
  exact ⟨encode_inj a b, fun h => by rw [h]⟩

/-! ### Totals helpers -/

private theorem plus_zero (a : Totals) : plus a zero = a := by
  cases a <;> simp [plus, zero]

private theorem zero_plus (a : Totals) : plus zero a = a := by
  cases a <;> simp [plus, zero]

private theorem plus_assoc (a b c : Totals) : plus (plus a b) c = plus a (plus b c) := by
  cases a <;> cases b <;> cases c <;> simp [plus] <;> omega

private theorem plus_comm (a b : Totals) : plus a b = plus b a := by
  cases a <;> cases b <;> simp [plus] <;> omega

private theorem summarize_foldl_init (l : List (Option Int)) (init : Totals) :
    l.foldl (fun a v => plus a (weighted 1 v)) init =
      plus init (l.foldl (fun a v => plus a (weighted 1 v)) zero) := by
  induction l generalizing init with
  | nil => exact (plus_zero init).symm
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih (plus init (weighted 1 x)), plus_assoc, zero_plus, ih (weighted 1 x)]

private theorem summarize_eq (l : List (Option Int)) :
    summarize l = l.foldl (fun a v => plus a (weighted 1 v)) zero := rfl

private theorem summarize_nil : summarize ([] : List (Option Int)) = zero := rfl

private theorem summarize_append (l₁ l₂ : List (Option Int)) :
    summarize (l₁ ++ l₂) = plus (summarize l₁) (summarize l₂) := by
  simp only [summarize_eq, List.foldl_append]
  exact summarize_foldl_init l₂ _

private theorem summarize_singleton (v : Option Int) : summarize [v] = weighted 1 v := by
  simp only [summarize_eq, List.foldl_cons, List.foldl_nil, zero_plus]

private theorem summarize_cons (v : Option Int) (vs : List (Option Int)) :
    summarize (v :: vs) = plus (summarize vs) (weighted 1 v) := by
  have h := summarize_append [v] vs
  rw [List.singleton_append] at h
  rw [h, summarize_singleton, plus_comm]

/-! ### UrlGrouping map fold -/

abbrev UVisit := Option String × Option Int

private def foldStep (m : Std.HashMap Url Totals) (p : UVisit) : Std.HashMap Url Totals :=
  m.insert (encode p.1) (plus (m.getD (encode p.1) zero) (weighted 1 p.2))

private def contribs (visits : List UVisit) (k : Url) : Totals :=
  summarize ((visits.filter (fun r => encode r.1 == k)).map Prod.snd)

private theorem contribs_nil (k : Url) : contribs ([] : List UVisit) k = zero := by
  simp [contribs, summarize_nil]

private theorem contribs_cons (r : UVisit) (rest : List UVisit) (k : Url) :
    contribs (r :: rest) k =
      if encode r.1 == k then plus (contribs rest k) (weighted 1 r.2)
      else contribs rest k := by
  by_cases h : encode r.1 == k
  · rw [ite_eq_left h]
    unfold contribs
    have hf : (r :: rest).filter (fun r => encode r.1 == k) =
        r :: rest.filter (fun r => encode r.1 == k) :=
      List.filter_cons_of_pos h
    rw [hf, List.map_cons]
    exact summarize_cons _ _
  · rw [ite_eq_right h]
    unfold contribs
    have hf : (r :: rest).filter (fun r => encode r.1 == k) =
        rest.filter (fun r => encode r.1 == k) :=
      List.filter_cons_of_neg h
    rw [hf]

private theorem foldMap_getD :
    ∀ (visits : List UVisit) (m : Std.HashMap Url Totals) (k : Url),
      (visits.foldl foldStep m).getD k zero = plus (m.getD k zero) (contribs visits k) := by
  intro visits
  induction visits with
  | nil =>
      intro m k
      simp [contribs_nil, plus_zero]
  | cons r rest ih =>
      intro m k
      rw [List.foldl_cons]
      rw [ih (foldStep m r) k]
      rw [contribs_cons]
      by_cases h : encode r.1 == k
      · rw [ite_eq_left h]
        have hget : (foldStep m r).getD k zero =
            plus (m.getD k zero) (weighted 1 r.2) := by
          show (m.insert (encode r.1) (plus (m.getD (encode r.1) zero) (weighted 1 r.2))).getD k zero = _
          rw [Std.HashMap.getD_insert, ite_eq_left h]
          have he : encode r.1 = k := eq_of_beq h
          rw [he]
        rw [hget, plus_assoc, plus_comm (weighted 1 r.2) (contribs rest k)]
      · rw [ite_eq_right h]
        have hget : (foldStep m r).getD k zero = m.getD k zero := by
          show (m.insert (encode r.1) (plus (m.getD (encode r.1) zero) (weighted 1 r.2))).getD k zero = _
          rw [Std.HashMap.getD_insert, ite_eq_right h]
        rw [hget]

theorem sound_UrlGrouping :
    TinylambOpt.ResearchGoals.AnalyticsWorkloads.BigBench.UrlGrouping.Goal := by
  intro visits key
  show decode (encode key) = key ∧
    (visits.foldl foldStep {}).getD (encode key) zero =
      summarize ((visits.filter (fun r => r.1 == key)).map Prod.snd)
  constructor
  · exact decode_encode key
  · rw [foldMap_getD visits {} (encode key)]
    simp only [Std.HashMap.getD_empty, zero_plus]
    unfold contribs
    have hfil : visits.filter (fun r => encode r.1 == encode key) =
        visits.filter (fun r => r.1 == key) := by
      apply List.filter_congr
      intro x hx
      exact encode_beq x.1 key
    rw [hfil]

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.BigBench
