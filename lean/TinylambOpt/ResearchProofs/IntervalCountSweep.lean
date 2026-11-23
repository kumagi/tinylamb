import TinylambOpt.ResearchGoals.IntervalCountSweep

namespace TinylambOpt.ResearchProofs.IntervalCountSweep

open TinylambOpt.ResearchGoals.IntervalCountSweep

/-!
# Soundness of the interval-count sweep

We sort the endpoint/probe events with their source indices attached, so that
every item is unique and the sorted order is a total order.  A structural
simulation of the sweep loop then shows that the value written at a query item
is `#starts before it - #stops before it`.  Sortedness identifies those counts
with `#spans with lo <= t` and `#spans with hi <= t`, and the elementary set
inclusion `hi <= t -> lo < t` (valid spans have `lo < hi`) gives both
non-negativity of the `Nat` subtraction and the final count
`#lo <= t - #hi <= t = #(lo <= t && t < hi)`.
-/

private abbrev Item := Event × Nat

private def Cmp (a b : Event) : Bool :=
  if a.time == b.time then phase a.op ≤ phase b.op else a.time < b.time

private def key (e : Event) : Int := 4 * e.time + (phase e.op : Int)

private def endsOf (spans : List Span) : List Event :=
  spans.flatMap fun s => match s with
    | (some lo, some hi) => if lo < hi then [Event.mk lo Op.start, Event.mk hi Op.stop] else []
    | _ => []

private def probesOf (points : List Cell) : List Event :=
  (points.zipIdx).filterMap fun pi => match pi with
    | (p, i) => p.map fun t => Event.mk t (Op.query i)

private def base (spans : List Span) (points : List Cell) : List Event :=
  endsOf spans ++ probesOf points

private def evs (spans : List Span) (points : List Cell) : List Item :=
  ((base spans points).zipIdx).mergeSort (List.zipIdxLE Cmp)

private theorem events_merge (spans : List Span) (points : List Cell) :
    events spans points = (base spans points).mergeSort Cmp :=
  rfl

private theorem events_eq_map (spans : List Span) (points : List Cell) :
    events spans points = (evs spans points).map Prod.fst := by
  rw [events_merge, ← List.mergeSort_zipIdx]
  rfl

/-! ### The comparison is a linear order on events -/

private theorem decide_congr_Iff {a b : Prop} [Decidable a] [Decidable b] (h : a ↔ b) :
    decide a = decide b := by
  by_cases ha : a
  · have hb : b := h.mp ha
    rw [show decide a = true by simp [ha], show decide b = true by simp [hb]]
  · have hb : ¬b := fun hb => ha (h.mpr hb)
    rw [show decide a = false by simp [ha], show decide b = false by simp [hb]]

private theorem phase_le_two (o : Op) : phase o ≤ 2 := by
  cases o <;> simp only [phase] <;> omega

private theorem Cmp_key_aux (ta tb : Int) (ia ib : Nat) (ha : ia ≤ 2) (hb : ib ≤ 2) :
    (if (ta == tb) then decide (ia ≤ ib) else decide (ta < tb)) =
      decide (4 * ta + (ia : Int) ≤ 4 * tb + (ib : Int)) := by
  by_cases hc : (ta == tb)
  · have h : ta = tb := by simpa using hc
    subst h
    rw [ite_eq_left hc]
    refine decide_congr_Iff ?_
    omega
  · rw [ite_eq_right hc]
    refine decide_congr_Iff ?_
    have hne : ¬(ta = tb) := by simpa using hc
    omega

private theorem Cmp_key (a b : Event) :
    Cmp a b = decide (key a ≤ key b) :=
  Cmp_key_aux a.time b.time (phase a.op) (phase b.op) (phase_le_two a.op) (phase_le_two b.op)

private theorem Cmp_trans {x y z : Event} (h₁ : Cmp x y = true) (h₂ : Cmp y z = true) :
    Cmp x z = true := by
  rw [Cmp_key x y] at h₁
  rw [Cmp_key y z] at h₂
  rw [Cmp_key x z]
  simp only [decide_eq_true_eq] at h₁ h₂ ⊢
  omega

private theorem Cmp_total (x y : Event) : Cmp x y = true || Cmp y x = true := by
  rw [Cmp_key x y, Cmp_key y x]
  simp only [Bool.or_eq_true, decide_eq_true_eq]
  omega

private theorem Cmp_of_key_lt {a b : Event} (h : key a < key b) : Cmp a b = true := by
  rw [Cmp_key]
  exact decide_eq_true (by omega)

private theorem Cmp_of_key_gt {a b : Event} (h : key b < key a) : Cmp a b = false := by
  rw [Cmp_key]
  exact decide_eq_false (by omega)

private theorem leItem_of_key_lt {a b : Item} (h : key a.1 < key b.1) :
    List.zipIdxLE Cmp a b = true := by
  have hab : Cmp a.1 b.1 = true := Cmp_of_key_lt h
  have hba : Cmp b.1 a.1 = false := Cmp_of_key_gt h
  simp [List.zipIdxLE, hab, hba]

private theorem leItem_key {a b : Item} (h : List.zipIdxLE Cmp a b = true) :
    key a.1 ≤ key b.1 := by
  unfold List.zipIdxLE at h
  by_cases hab : Cmp a.1 b.1 = true
  · have hk : decide (key a.1 ≤ key b.1) = true := by simpa [Cmp_key] using hab
    simpa using hk
  · simp [hab] at h

private theorem evs_pw (spans : List Span) (points : List Cell) :
    List.Pairwise (fun a b : Item => List.zipIdxLE Cmp a b = true) (evs spans points) := by
  apply List.pairwise_mergeSort
  · intro a b c hab hbc
    exact List.zipIdxLE_trans
      (fun (x y z : Event) (h₁ : Cmp x y = true) (h₂ : Cmp y z = true) => Cmp_trans h₁ h₂) a b c hab hbc
  · intro a b
    exact List.zipIdxLE_total Cmp_total a b

/-! ### Sorted-list position facts -/

private theorem pairwise_getElem {l : List Item} (hp : List.Pairwise (fun a b : Item => List.zipIdxLE Cmp a b = true) l) :
    ∀ {i j : Nat} (hi : i < l.length) (hj : j < l.length) (hij : i < j),
      List.zipIdxLE Cmp l[i] l[j] = true := by
  revert hp
  induction l with
  | nil => intro hp i j hi hj hij; simp at hi
  | cons a tl ih =>
      intro hp
      cases hp with
      | cons hfirst hrest =>
          intro i j hi hj hij
          simp only [List.length_cons] at hi hj
          cases i with
          | zero =>
              cases j with
              | zero => omega
              | succ j' =>
                  rw [List.getElem_cons_succ]
                  exact hfirst tl[j'] (List.getElem_mem (by omega))
          | succ i' =>
              cases j with
              | zero => omega
              | succ j' =>
                  rw [List.getElem_cons_succ, List.getElem_cons_succ]
                  exact ih hrest (by omega) (by omega) (by omega)

private theorem zipIdx_nodup (l : List α) : ∀ n : Nat, (l.zipIdx n).Nodup := by
  induction l with
  | nil => intro n; simp
  | cons a tl ih =>
      intro n
      rw [List.zipIdx_cons, List.nodup_cons]
      constructor
      · intro h
        obtain ⟨hge, _⟩ := List.mem_zipIdx_iff_le_and_getElem?_sub.mp h
        omega
      · exact ih (n + 1)

private theorem evs_nodup (spans : List Span) (points : List Cell) : (evs spans points).Nodup :=
  (List.mergeSort_perm ((base spans points).zipIdx) (List.zipIdxLE Cmp)).nodup_iff.mpr
    (zipIdx_nodup _ 0)

private theorem evs_snduniq (spans : List Span) (points : List Cell) :
    ∀ I J : Item, I ∈ evs spans points → J ∈ evs spans points → I.2 = J.2 → I = J := by
  intro I J hI hJ hj
  obtain ⟨_, hbI⟩ := List.mem_zipIdx_iff_le_and_getElem?_sub.mp (List.mem_mergeSort.mp hI)
  obtain ⟨_, hbJ⟩ := List.mem_zipIdx_iff_le_and_getElem?_sub.mp (List.mem_mergeSort.mp hJ)
  have hs : some I.1 = some J.1 := by rw [← hbI, ← hbJ, hj]
  exact Prod.ext (by simpa using hs) hj

/-! ### The sweep fold -/

private abbrev St := Nat × Array Nat

private def step : St → Event → St
| (a, out), e => match e.op with
  | .stop => (a - 1, out)
  | .start => (a + 1, out)
  | .query i => (a, out.setIfInBounds i a)

private theorem loop_eq_foldl (l : List Event) (a : Nat) (out : Array Nat) :
    (Id.run do
      let mut active := a
      let mut out := out
      for e in l do
        match e.op with
        | .stop => active := active - 1
        | .start => active := active + 1
        | .query i => out := out.setIfInBounds i active
      return (active, out)) = List.foldl step (a, out) l := by
  induction l generalizing a out with
  | nil => simp [List.forIn_eq_foldlM]
  | cons e l ih =>
      cases he : e.op with
      | stop => simpa [step, he] using ih (a - 1) out
      | start => simpa [step, he] using ih (a + 1) out
      | query i => simpa [step, he] using ih a (out.setIfInBounds i a)

private theorem sweep_foldl (spans : List Span) (points : List Cell) :
    sweep spans points = (List.foldl step (0, Array.replicate points.length 0) (events spans points)).2.toList := by
  have h := loop_eq_foldl (events spans points) 0 (Array.replicate points.length 0)
  rw [← h]
  rfl

private def go : List Item → St → St
| [], s => s
| (e, _) :: l, s => go l (step s e)

private theorem foldl_map_fst (l : List Item) (s : St) :
    List.foldl step s (l.map Prod.fst) = go l s := by
  induction l generalizing s with
  | nil => rfl
  | cons x l ih =>
      obtain ⟨e, k⟩ := x
      simp only [List.map_cons, List.foldl_cons, go]
      exact ih (step s e)

private theorem sweep_go (spans : List Span) (points : List Cell) :
    sweep spans points = (go (evs spans points) (0, Array.replicate points.length 0)).2.toList := by
  rw [sweep_foldl, events_eq_map, foldl_map_fst]

/-! ### Counting endpoint events -/

private def isStartE (e : Event) : Bool :=
  match e.op with | .start => true | _ => false

private def isStopE (e : Event) : Bool :=
  match e.op with | .stop => true | _ => false

private def startLeE (t : Int) (e : Event) : Bool := isStartE e && decide (e.time ≤ t)
private def startLtE (t : Int) (e : Event) : Bool := isStartE e && decide (e.time < t)
private def stopLeE (t : Int) (e : Event) : Bool := isStopE e && decide (e.time ≤ t)

private def loLeSpan (t : Int) (s : Span) : Bool :=
  match s with | (some lo, some hi) => decide (lo < hi && lo ≤ t) | _ => false

private def loLtSpan (t : Int) (s : Span) : Bool :=
  match s with | (some lo, some hi) => decide (lo < hi && lo < t) | _ => false

private def hiLeSpan (t : Int) (s : Span) : Bool :=
  match s with | (some lo, some hi) => decide (lo < hi && hi ≤ t) | _ => false

private theorem countP_zipIdx (p : Event → Bool) :
    ∀ (l : List Event) (n : Nat),
      List.countP (fun I : Event × Nat => p I.1) (l.zipIdx n) = List.countP p l := by
  intro l
  induction l with
  | nil => intro n; rfl
  | cons a tl ih =>
      intro n
      rw [List.zipIdx_cons]
      simp [List.countP_cons, ih]

private theorem countP_eq_zero_of_not {α : Type} {p : α → Bool} {l : List α}
    (h : ∀ a ∈ l, p a = false) : List.countP p l = 0 := by
  induction l with
  | nil => rfl
  | cons a tl ih =>
      have ha : p a = false := h a (by simp)
      simpa [List.countP_cons, ha] using ih (fun b hb => h b (by simp [hb]))

private theorem mem_probesOf_query (points : List Cell) (e : Event) :
    e ∈ probesOf points → ∃ i : Nat, e.op = Op.query i := by
  intro h
  rw [probesOf, List.mem_filterMap] at h
  obtain ⟨pi, hmem, hsome⟩ := h
  obtain ⟨p, i⟩ := pi
  cases p with
  | none => simp at hsome
  | some t =>
      simp at hsome
      rw [← hsome]
      exact ⟨i, rfl⟩

private theorem countP_probes_start (points : List Cell) (t : Int) :
    List.countP (startLeE t) (probesOf points) = 0 := by
  apply countP_eq_zero_of_not
  intro e he
  obtain ⟨i, hi⟩ := mem_probesOf_query points e he
  simp [startLeE, isStartE, hi]

private theorem countP_probes_stop (points : List Cell) (t : Int) :
    List.countP (stopLeE t) (probesOf points) = 0 := by
  apply countP_eq_zero_of_not
  intro e he
  obtain ⟨i, hi⟩ := mem_probesOf_query points e he
  simp [stopLeE, isStopE, hi]

private theorem countP_probes_startLt (points : List Cell) (t : Int) :
    List.countP (startLtE t) (probesOf points) = 0 := by
  apply countP_eq_zero_of_not
  intro e he
  obtain ⟨i, hi⟩ := mem_probesOf_query points e he
  simp [startLtE, isStartE, hi]

private theorem count_ends_startLe (spans : List Span) (t : Int) :
    List.countP (startLeE t) (endsOf spans) = List.countP (loLeSpan t) spans := by
  induction spans with
  | nil => rfl
  | cons s tl ih =>
      rcases s with ⟨c, d⟩
      cases c with
      | none => simpa [endsOf, loLeSpan] using ih
      | some lo =>
          cases d with
          | none => simpa [endsOf, loLeSpan] using ih
          | some hi =>
              by_cases hv : lo < hi
              · simpa [endsOf, hv, loLeSpan, startLeE, isStartE, List.countP_cons] using ih
              · simpa [endsOf, hv, loLeSpan] using ih

private theorem count_ends_startLt (spans : List Span) (t : Int) :
    List.countP (startLtE t) (endsOf spans) = List.countP (loLtSpan t) spans := by
  induction spans with
  | nil => rfl
  | cons s tl ih =>
      rcases s with ⟨c, d⟩
      cases c with
      | none => simpa [endsOf, loLtSpan] using ih
      | some lo =>
          cases d with
          | none => simpa [endsOf, loLtSpan] using ih
          | some hi =>
              by_cases hv : lo < hi
              · simpa [endsOf, hv, loLtSpan, startLtE, isStartE, List.countP_cons] using ih
              · simpa [endsOf, hv, loLtSpan] using ih

private theorem count_ends_stopLe (spans : List Span) (t : Int) :
    List.countP (stopLeE t) (endsOf spans) = List.countP (hiLeSpan t) spans := by
  induction spans with
  | nil => rfl
  | cons s tl ih =>
      rcases s with ⟨c, d⟩
      cases c with
      | none => simpa [endsOf, hiLeSpan] using ih
      | some lo =>
          cases d with
          | none => simpa [endsOf, hiLeSpan] using ih
          | some hi =>
              by_cases hv : lo < hi
              · simpa [endsOf, hv, hiLeSpan, stopLeE, isStopE, List.countP_cons] using ih
              · simpa [endsOf, hv, hiLeSpan] using ih

private theorem count_evs_startLe (spans : List Span) (points : List Cell) (t : Int) :
    List.countP (fun I : Item => startLeE t I.1) (evs spans points) =
      List.countP (loLeSpan t) spans := by
  rw [show List.countP (fun I : Item => startLeE t I.1) (evs spans points) =
      List.countP (fun I : Item => startLeE t I.1) ((base spans points).zipIdx) by
    apply List.Perm.countP_eq
    exact List.mergeSort_perm _ _]
  rw [countP_zipIdx]
  simp [base, List.countP_append, countP_probes_start, count_ends_startLe]

private theorem count_evs_startLt (spans : List Span) (points : List Cell) (t : Int) :
    List.countP (fun I : Item => startLtE t I.1) (evs spans points) =
      List.countP (loLtSpan t) spans := by
  rw [show List.countP (fun I : Item => startLtE t I.1) (evs spans points) =
      List.countP (fun I : Item => startLtE t I.1) ((base spans points).zipIdx) by
    apply List.Perm.countP_eq
    exact List.mergeSort_perm _ _]
  rw [countP_zipIdx]
  simp [base, List.countP_append, countP_probes_startLt, count_ends_startLt]

private theorem count_evs_stopLe (spans : List Span) (points : List Cell) (t : Int) :
    List.countP (fun I : Item => stopLeE t I.1) (evs spans points) =
      List.countP (hiLeSpan t) spans := by
  rw [show List.countP (fun I : Item => stopLeE t I.1) (evs spans points) =
      List.countP (fun I : Item => stopLeE t I.1) ((base spans points).zipIdx) by
    apply List.Perm.countP_eq
    exact List.mergeSort_perm _ _]
  rw [countP_zipIdx]
  simp [base, List.countP_append, countP_probes_stop, count_ends_stopLe]

/-! ### Elementary span-count identities -/

private theorem countP_congr_eq {α : Type} {p q : α → Bool} {l : List α}
    (h : ∀ a ∈ l, p a = q a) : List.countP p l = List.countP q l := by
  induction l with
  | nil => rfl
  | cons a tl ih =>
      simpa [List.countP_cons, h a (by simp)] using
        ih (fun b hb => h b (by simp [hb]))

private theorem hi_le_implies_lo_lt (t : Int) (s : Span) :
    hiLeSpan t s = true → loLtSpan t s = true := by
  rcases s with ⟨c, d⟩
  cases c with
  | none => simp [hiLeSpan]
  | some lo =>
      cases d with
      | none => simp [hiLeSpan]
      | some hi =>
          simp only [hiLeSpan, loLtSpan]
          intro h
          have h' : lo < hi ∧ hi ≤ t := by simpa using h
          have hlt : lo < t := by omega
          simp [h'.1, hlt]

private theorem hi_le_implies_lo_le (t : Int) (s : Span) :
    (hiLeSpan t s && loLeSpan t s) = hiLeSpan t s := by
  rcases s with ⟨c, d⟩
  cases c with
  | none => simp [hiLeSpan, loLeSpan]
  | some lo =>
      cases d with
      | none => simp [hiLeSpan, loLeSpan]
      | some hi =>
          by_cases hv : lo < hi <;> by_cases hh : hi ≤ t <;>
            simp [hiLeSpan, loLeSpan, hv, hh] <;> omega

private theorem hi_le_count_le_lo_lt (spans : List Span) (t : Int) :
    List.countP (hiLeSpan t) spans ≤ List.countP (loLtSpan t) spans := by
  induction spans with
  | nil => simp
  | cons s tl ih =>
      by_cases hh : hiLeSpan t s = true
      · have hl : loLtSpan t s = true := hi_le_implies_lo_lt t s hh
        simp [List.countP_cons, hh, hl]
        omega
      · simp [List.countP_cons, hh]
        by_cases hl : loLtSpan t s = true
        · simp [hl]
          omega
        · simp [hl]
          exact ih

private theorem contains_some_eq (t : Int) (s : Span) :
    contains s (some t) = (loLeSpan t s && ! hiLeSpan t s) := by
  rcases s with ⟨c, d⟩
  cases c with
  | none => simp [contains, loLeSpan, hiLeSpan]
  | some lo =>
      cases d with
      | none => simp [contains, loLeSpan, hiLeSpan]
      | some hi =>
          by_cases hv : lo < hi <;> by_cases hl : lo ≤ t <;> by_cases hh : hi ≤ t <;>
            simp [contains, loLeSpan, hiLeSpan, hv, hl, hh] <;> omega

private theorem ref_count_split (spans : List Span) (t : Int) :
    List.countP (loLeSpan t) spans =
      List.countP (fun s => loLeSpan t s && hiLeSpan t s) spans +
        List.countP (fun s => loLeSpan t s && ! hiLeSpan t s) spans := by
  induction spans with
  | nil => simp
  | cons s tl ih =>
      by_cases hlo : loLeSpan t s = true <;> by_cases hhi : hiLeSpan t s = true <;>
        simp [List.countP_cons, hlo, hhi, ih] <;> omega

private theorem ref_count_both (spans : List Span) (t : Int) :
    List.countP (fun s => loLeSpan t s && hiLeSpan t s) spans =
      List.countP (hiLeSpan t) spans := by
  apply countP_congr_eq
  intro s hs
  rw [Bool.and_comm]
  exact hi_le_implies_lo_le t s

private theorem reference_count (spans : List Span) (t : Int) :
    (spans.filter (contains · (some t))).length =
      List.countP (loLeSpan t) spans - List.countP (hiLeSpan t) spans := by
  have hf : spans.filter (contains · (some t)) =
      spans.filter (fun s => loLeSpan t s && ! hiLeSpan t s) :=
    List.filter_congr (fun s _ => contains_some_eq t s)
  rw [hf, ← List.countP_eq_length_filter]
  have hs := ref_count_split spans t
  have hh := ref_count_both spans t
  omega

/-! ### Reading the comparison key off an event -/

private theorem key_start (v : Int) : key (Event.mk v Op.start) = 4 * v + 1 := by
  simp [key, phase]

private theorem key_stop (v : Int) : key (Event.mk v Op.stop) = 4 * v := by
  simp [key, phase]

private theorem key_query (v : Int) (i : Nat) : key (Event.mk v (Op.query i)) = 4 * v + 2 := by
  simp [key, phase]

private theorem startLe_of_key_le (t : Int) (I : Item) (h : key I.1 ≤ 4 * t + 2) :
    startLeE t I.1 = isStartE I.1 := by
  rcases I with ⟨⟨v, op⟩, k⟩
  cases op with
  | start => simp [startLeE, isStartE, key, phase] at h ⊢; omega
  | stop => simp [startLeE, isStartE]
  | query i => simp [startLeE, isStartE]

private theorem stopLe_of_key_le (t : Int) (I : Item) (h : key I.1 ≤ 4 * t + 2) :
    stopLeE t I.1 = isStopE I.1 := by
  rcases I with ⟨⟨v, op⟩, k⟩
  cases op with
  | stop => simp [stopLeE, isStopE, key, phase] at h ⊢; omega
  | start => simp [stopLeE, isStopE]
  | query i => simp [stopLeE, isStopE]

private theorem startLt_of_key_le_stop (t : Int) (I : Item) (h : key I.1 ≤ 4 * t) :
    startLtE t I.1 = isStartE I.1 := by
  rcases I with ⟨⟨v, op⟩, k⟩
  cases op with
  | start => simp [startLtE, isStartE, key, phase] at h ⊢; omega
  | stop => simp [startLtE, isStartE]
  | query i => simp [startLtE, isStartE]

private theorem stopLe_of_key_le_stop (t : Int) (I : Item) (h : key I.1 ≤ 4 * t) :
    stopLeE t I.1 = isStopE I.1 := by
  rcases I with ⟨⟨v, op⟩, k⟩
  cases op with
  | stop => simp [stopLeE, isStopE, key, phase] at h ⊢; omega
  | start => simp [stopLeE, isStopE]
  | query i => simp [stopLeE, isStopE]

private theorem not_startLe_of_key_ge (t : Int) (I : Item) (h : 4 * t + 2 ≤ key I.1) :
    startLeE t I.1 = false := by
  rcases I with ⟨⟨v, op⟩, k⟩
  cases op with
  | start => simp [startLeE, isStartE, key, phase] at h ⊢; omega
  | stop => simp [startLeE, isStartE]
  | query i => simp [startLeE, isStartE]

private theorem not_stopLe_of_key_ge (t : Int) (I : Item) (h : 4 * t + 2 ≤ key I.1) :
    stopLeE t I.1 = false := by
  rcases I with ⟨⟨v, op⟩, k⟩
  cases op with
  | stop => simp [stopLeE, isStopE, key, phase] at h ⊢; omega
  | start => simp [stopLeE, isStopE]
  | query i => simp [stopLeE, isStopE]

private theorem not_startLt_of_key_ge_stop (t : Int) (I : Item) (h : 4 * t ≤ key I.1) :
    startLtE t I.1 = false := by
  rcases I with ⟨⟨v, op⟩, k⟩
  cases op with
  | start => simp [startLtE, isStartE, key, phase] at h ⊢; omega
  | stop => simp [startLtE, isStartE]
  | query i => simp [startLtE, isStartE]

private theorem key_le_of_mem_take (E : List Item)
    (hpw : List.Pairwise (fun a b : Item => List.zipIdxLE Cmp a b = true) E)
    (j : Nat) (hj : j < E.length) (q : Item) (hEj : E[j] = q) :
    ∀ I ∈ E.take j, key I.1 ≤ key q.1 := by
  intro I hI
  obtain ⟨m, hm⟩ := List.mem_iff_getElem?.mp hI
  rw [List.getElem?_take] at hm
  by_cases hmj : m < j
  · rw [ite_eq_left hmj] at hm
    obtain ⟨hpos, hget⟩ := List.getElem?_eq_some_iff.mp hm
    have hle := pairwise_getElem hpw hpos hj hmj
    have hk := leItem_key hle
    simpa [hget, hEj] using hk
  · rw [ite_eq_right hmj] at hm
    simp at hm

private theorem key_ge_of_mem_drop (E : List Item)
    (hpw : List.Pairwise (fun a b : Item => List.zipIdxLE Cmp a b = true) E)
    (j : Nat) (hj : j < E.length) (q : Item) (hEj : E[j] = q) :
    ∀ I ∈ E.drop j, key q.1 ≤ key I.1 := by
  intro I hI
  obtain ⟨m, hm⟩ := List.mem_iff_getElem?.mp hI
  rw [List.getElem?_drop] at hm
  obtain ⟨hpos, hget⟩ := List.getElem?_eq_some_iff.mp hm
  by_cases hm0 : m = 0
  · subst m
    have hq : q = E[j] := hEj.symm
    have hi : I = E[j] := by simpa using hget.symm
    rw [hq, hi]
    omega
  · have hjm : j < j + m := by omega
    have hle := pairwise_getElem hpw hj hpos hjm
    have hk := leItem_key hle
    simpa [hEj, hget] using hk

private theorem countP_pos_of_mem {α : Type} {p : α → Bool} {l : List α}
    (a : α) (ha : a ∈ l) (hp : p a = true) : 0 < List.countP p l := by
  induction l with
  | nil => simp at ha
  | cons b tl ih =>
      by_cases hmem : a ∈ tl
      · have hi := ih hmem
        by_cases hpb : p b = true
        · simp [List.countP_cons, hpb]
        · simpa [List.countP_cons, hpb] using hi
      · have haeq : a = b := by simpa [hmem] using ha
        subst b
        simp [hp]

private theorem countP_le_of_imp {α : Type} {p q : α → Bool} {l : List α}
    (h : ∀ a ∈ l, p a = true → q a = true) : List.countP p l ≤ List.countP q l := by
  induction l with
  | nil => simp
  | cons a tl ih =>
      have hi := ih (fun b hb hpb => h b (by simp [hb]) hpb)
      by_cases hpa : p a = true
      · have hqa : q a = true := h a (by simp) hpa
        simp [List.countP_cons, hpa, hqa]
        omega
      · simp [List.countP_cons, hpa]
        omega

/-! ### Query items are unique -/

private theorem mem_endsOf_op (spans : List Span) (e : Event) :
    e ∈ endsOf spans → e.op = Op.start ∨ e.op = Op.stop := by
  induction spans with
  | nil => simp [endsOf]
  | cons s tl ih =>
      intro h
      rcases s with ⟨c, d⟩
      cases c with
      | none =>
          have htail : e ∈ endsOf tl := by simpa [endsOf] using h
          exact ih htail
      | some lo =>
          cases d with
          | none =>
              have htail : e ∈ endsOf tl := by simpa [endsOf] using h
              exact ih htail
          | some hi =>
              by_cases hv : lo < hi
              · have hc : endsOf ((some lo, some hi) :: tl) =
                    [Event.mk lo Op.start, Event.mk hi Op.stop] ++ endsOf tl := by
                  simp [endsOf, hv]
                rw [hc, List.mem_append, List.mem_cons, List.mem_singleton] at h
                obtain hhead | htail := h
                · rcases hhead with rfl | rfl
                  · exact Or.inl rfl
                  · exact Or.inr rfl
                · exact ih htail
              · have htail : e ∈ endsOf tl := by simpa [endsOf, hv] using h
                exact ih htail

private theorem mem_probesOf_full (points : List Cell) (e : Event) :
    e ∈ probesOf points ↔ ∃ i : Nat, ∃ t : Int,
      points[i]? = some (some t) ∧ e = Event.mk t (Op.query i) := by
  constructor
  · intro h
    rw [probesOf, List.mem_filterMap] at h
    obtain ⟨pi, hmem, hsome⟩ := h
    rcases pi with ⟨p, k⟩
    obtain ⟨_, hget⟩ := List.mem_zipIdx_iff_le_and_getElem?_sub.mp hmem
    have hget' : points[k]? = some p := by simpa using hget
    cases p with
    | none => simp at hsome
    | some t =>
        simp at hsome
        exact ⟨k, t, hget', hsome.symm⟩
  · rintro ⟨i, t, hi, rfl⟩
    rw [probesOf, List.mem_filterMap]
    refine ⟨(some t, i), ?_, ?_⟩
    · exact List.mem_zipIdx_iff_le_and_getElem?_sub.mpr ⟨by omega, by simpa using hi⟩
    · simp

private theorem filterMap_probes_nodup :
    ∀ (l : List (Cell × Nat)), l.Nodup →
      (l.filterMap fun pi => match pi with
        | (p, i) => p.map fun t => Event.mk t (Op.query i)).Nodup := by
  intro l
  induction l with
  | nil => intro h; simp
  | cons x tl ih =>
      intro h
      rw [List.filterMap_cons]
      rcases x with ⟨p, i⟩
      cases p with
      | none => exact ih (List.nodup_cons.mp h).2
      | some t =>
          simp
          constructor
          · intro x k hxmem t' hxeq hteq hki
            rw [hxeq, hteq, hki] at hxmem
            exact (List.nodup_cons.mp h).1 hxmem
          · exact ih (List.nodup_cons.mp h).2

private theorem probesOf_nodup (points : List Cell) : (probesOf points).Nodup := by
  unfold probesOf
  exact filterMap_probes_nodup (points.zipIdx) (zipIdx_nodup points 0)

private theorem zipIdx_index_unique_of_nodup {α : Type} {l : List α} (hn : l.Nodup) :
    ∀ {a : α} {i j : Nat}, (a, i) ∈ l.zipIdx → (a, j) ∈ l.zipIdx → i = j := by
  intro a i j hi hj
  obtain ⟨_, hgi0⟩ := List.mem_zipIdx_iff_le_and_getElem?_sub.mp hi
  obtain ⟨_, hgj0⟩ := List.mem_zipIdx_iff_le_and_getElem?_sub.mp hj
  have hgi : l[i]? = some a := by simpa using hgi0
  have hgj : l[j]? = some a := by simpa using hgj0
  obtain ⟨hposi, _⟩ := List.getElem?_eq_some_iff.mp hgi
  exact (List.Nodup.getElem?_inj hposi hn).mp (by rw [hgi, hgj])

private theorem mem_of_mem_zipIdx {α : Type} {l : List α} {a : α} {k : Nat} :
    (a, k) ∈ l.zipIdx → a ∈ l := by
  intro h
  obtain ⟨_, hget⟩ := List.mem_zipIdx_iff_le_and_getElem?_sub.mp h
  exact List.mem_iff_getElem?.mpr ⟨k, hget⟩

private theorem base_query_index (spans : List Span) (points : List Cell) :
    ∀ (e : Event) (k : Nat), (e, k) ∈ (base spans points).zipIdx →
      (e.op = Op.start ∨ e.op = Op.stop) = false →
      ∃ k' : Nat, (e, k') ∈ (probesOf points).zipIdx ∧
        k = (endsOf spans).length + k' := by
  intro e k hz hno
  obtain ⟨_, hget⟩ := List.mem_zipIdx_iff_le_and_getElem?_sub.mp hz
  have hget' : (endsOf spans ++ probesOf points)[k]? = some e := by
    simpa [base] using hget
  rw [List.getElem?_append] at hget'
  by_cases hk : k < (endsOf spans).length
  · rw [ite_eq_left hk] at hget'
    have hemem : e ∈ endsOf spans := List.mem_iff_getElem?.mpr ⟨k, hget'⟩
    have hop := mem_endsOf_op spans e hemem
    simp [hop] at hno
  · rw [ite_eq_right hk] at hget'
    refine ⟨k - (endsOf spans).length, ?_, by omega⟩
    exact List.mem_zipIdx_iff_le_and_getElem?_sub.mpr ⟨by omega, hget'⟩

private theorem query_item_unique (spans : List Span) (points : List Cell) (i : Nat) :
    ∀ I J : Item, I ∈ evs spans points → J ∈ evs spans points →
      I.1.op = Op.query i → J.1.op = Op.query i → I = J := by
  intro I J hI hJ hIo hJo
  have hIz : I ∈ (base spans points).zipIdx := List.mem_mergeSort.mp hI
  have hJz : J ∈ (base spans points).zipIdx := List.mem_mergeSort.mp hJ
  have hnoI : (I.1.op = Op.start ∨ I.1.op = Op.stop) = false := by simp [hIo]
  have hnoJ : (J.1.op = Op.start ∨ J.1.op = Op.stop) = false := by simp [hJo]
  obtain ⟨kI, hkI, hkIeq⟩ := base_query_index spans points I.1 I.2 hIz hnoI
  obtain ⟨kJ, hkJ, hkEq⟩ := base_query_index spans points J.1 J.2 hJz hnoJ
  have hIprobe : I.1 ∈ probesOf points := mem_of_mem_zipIdx hkI
  have hJprobe : J.1 ∈ probesOf points := mem_of_mem_zipIdx hkJ
  have hEv : I.1 = J.1 := by
    obtain ⟨i1, t1, hpt1, he1⟩ := (mem_probesOf_full points I.1).mp hIprobe
    obtain ⟨i2, t2, hpt2, he2⟩ := (mem_probesOf_full points J.1).mp hJprobe
    have hi1 : i1 = i := by rw [he1] at hIo; simpa using hIo
    have hi2 : i2 = i := by rw [he2] at hJo; simpa using hJo
    have hpt1' : points[i]? = some (some t1) := by simpa [hi1] using hpt1
    have hpt2' : points[i]? = some (some t2) := by simpa [hi2] using hpt2
    have h12 : some (some t1) = some (some t2) := by rw [← hpt1', hpt2']
    have ht : t1 = t2 := by simpa using h12
    rw [he1, he2, ht, hi1, hi2]
  rw [← hEv] at hkJ
  have hk : kI = kJ := zipIdx_index_unique_of_nodup (probesOf_nodup points) hkI hkJ
  have hsnd : I.2 = J.2 := by omega
  exact evs_snduniq spans points I J hI hJ hsnd

/-! ### What the prefix before an event looks like -/

private theorem countP_take_eq_of_drop_zero {p : Item → Bool} {E : List Item} {j : Nat}
    (hdrop : List.countP p (E.drop j) = 0) :
    List.countP p (E.take j) = List.countP p E := by
  have h := List.countP_append (p := p) (l₁ := E.take j) (l₂ := E.drop j)
  rw [hdrop, Nat.add_zero, List.take_append_drop] at h
  exact h.symm

private theorem prefix_query_counts (spans : List Span) (points : List Cell) (t : Int) (i : Nat)
    (j : Nat) (hj : j < (evs spans points).length) (q : Item)
    (hEj : (evs spans points)[j] = q) (hq : q.1 = Event.mk t (Op.query i)) :
    List.countP (fun I : Item => startLeE t I.1) ((evs spans points).take j) =
        List.countP (loLeSpan t) spans ∧
    List.countP (fun I : Item => stopLeE t I.1) ((evs spans points).take j) =
        List.countP (hiLeSpan t) spans := by
  let E := evs spans points
  have hpw : List.Pairwise (fun a b : Item => List.zipIdxLE Cmp a b = true) E := evs_pw spans points
  have hqkey : key q.1 = 4 * t + 2 := by simp [hq, key_query]
  have hdropS : List.countP (fun I : Item => startLeE t I.1) (E.drop j) = 0 := by
    apply countP_eq_zero_of_not
    intro I hI
    have hk := key_ge_of_mem_drop E hpw j hj q hEj I hI
    rw [hqkey] at hk
    exact not_startLe_of_key_ge t I hk
  have hdropO : List.countP (fun I : Item => stopLeE t I.1) (E.drop j) = 0 := by
    apply countP_eq_zero_of_not
    intro I hI
    have hk := key_ge_of_mem_drop E hpw j hj q hEj I hI
    rw [hqkey] at hk
    exact not_stopLe_of_key_ge t I hk
  have htS := countP_take_eq_of_drop_zero hdropS
  have htO := countP_take_eq_of_drop_zero hdropO
  constructor
  · change List.countP (fun I : Item => startLeE t I.1) (E.take j) = List.countP (loLeSpan t) spans
    rw [htS, count_evs_startLe]
  · change List.countP (fun I : Item => stopLeE t I.1) (E.take j) = List.countP (hiLeSpan t) spans
    rw [htO, count_evs_stopLe]

private theorem startLt_implies_start (t : Int) (I : Item) :
    startLtE t I.1 = true → isStartE I.1 = true := by
  intro hp
  unfold startLtE at hp
  exact (Bool.and_eq_true _ _).mp hp |>.1

private theorem prefix_stop_bound (spans : List Span) (points : List Cell) (t : Int)
    (j : Nat) (hj : j < (evs spans points).length) (q : Item)
    (hEj : (evs spans points)[j] = q) (hq : q.1 = Event.mk t Op.stop) :
    List.countP (fun I : Item => isStopE I.1) ((evs spans points).take j) + 1 ≤
      List.countP (fun I : Item => isStartE I.1) ((evs spans points).take j) := by
  let E := evs spans points
  have hpw : List.Pairwise (fun a b : Item => List.zipIdxLE Cmp a b = true) E := evs_pw spans points
  have hqkey : key q.1 = 4 * t := by simp [hq, key_stop]
  have hstopEq : List.countP (fun I : Item => isStopE I.1) (E.take j) =
      List.countP (fun I : Item => stopLeE t I.1) (E.take j) := by
    apply countP_congr_eq
    intro I hI
    have hk := key_le_of_mem_take E hpw j hj q hEj I hI
    rw [hqkey] at hk
    exact (stopLe_of_key_le_stop t I hk).symm
  have hqmem : q ∈ E.drop j := by
    apply List.mem_iff_getElem?.mpr
    refine ⟨0, ?_⟩
    simpa [List.getElem?_drop, hEj] using (List.getElem?_eq_getElem hj)
  have hdropPos : 0 < List.countP (fun I : Item => stopLeE t I.1) (E.drop j) := by
    apply countP_pos_of_mem q hqmem
    simp [stopLeE, isStopE, hq]
  have happ := List.countP_append (p := fun I : Item => stopLeE t I.1) (l₁ := E.take j) (l₂ := E.drop j)
  rw [List.take_append_drop] at happ
  have htotalStop := count_evs_stopLe spans points t
  have htotalStopE : List.countP (fun I : Item => stopLeE t I.1) E =
      List.countP (hiLeSpan t) spans := by simpa [E] using htotalStop
  have hstop_le : List.countP (fun I : Item => isStopE I.1) (E.take j) + 1 ≤
      List.countP (hiLeSpan t) spans := by omega
  have hdropStart : List.countP (fun I : Item => startLtE t I.1) (E.drop j) = 0 := by
    apply countP_eq_zero_of_not
    intro I hI
    have hk := key_ge_of_mem_drop E hpw j hj q hEj I hI
    have hk4 : 4 * t ≤ key I.1 := by rw [hqkey] at hk; exact hk
    exact not_startLt_of_key_ge_stop t I hk4
  have htStart := countP_take_eq_of_drop_zero hdropStart
  have happStart := List.countP_append (p := fun I : Item => startLtE t I.1) (l₁ := E.take j) (l₂ := E.drop j)
  rw [List.take_append_drop] at happStart
  have htotalStart := count_evs_startLt spans points t
  have htotalStartE : List.countP (fun I : Item => startLtE t I.1) E =
      List.countP (loLtSpan t) spans := by simpa [E] using htotalStart
  have hstart_total : List.countP (fun I : Item => startLtE t I.1) (E.take j) =
      List.countP (loLtSpan t) spans := by
    have ht := countP_take_eq_of_drop_zero hdropStart
    rw [ht, htotalStartE]
  have hineq := hi_le_count_le_lo_lt spans t
  have hleStart : List.countP (fun I : Item => startLtE t I.1) (E.take j) ≤
      List.countP (fun I : Item => isStartE I.1) (E.take j) := by
    apply countP_le_of_imp
    intro I hI hp
    exact startLt_implies_start t I hp
  change List.countP (fun I : Item => isStopE I.1) (E.take j) + 1 ≤
      List.countP (fun I : Item => isStartE I.1) (E.take j)
  omega

private theorem prefix_general_bound (spans : List Span) (points : List Cell)
    (j : Nat) (hj : j < (evs spans points).length) :
    List.countP (fun I : Item => isStopE I.1) ((evs spans points).take j) ≤
      List.countP (fun I : Item => isStartE I.1) ((evs spans points).take j) := by
  let E := evs spans points
  have hpw : List.Pairwise (fun a b : Item => List.zipIdxLE Cmp a b = true) E := evs_pw spans points
  let q := E[j]
  let t := q.1.time
  have hEj : E[j] = q := rfl
  have hkge : 4 * t ≤ key q.1 := by
    simp [t, key]
    have hp := phase_le_two q.1.op
    omega
  have hkle : key q.1 ≤ 4 * t + 2 := by
    simp [t, key]
    have hp := phase_le_two q.1.op
    omega
  have hstopEq : List.countP (fun I : Item => isStopE I.1) (E.take j) =
      List.countP (fun I : Item => stopLeE t I.1) (E.take j) := by
    apply countP_congr_eq
    intro I hI
    have hk := key_le_of_mem_take E hpw j hj q hEj I hI
    have hk' : key I.1 ≤ 4 * t + 2 := by omega
    exact (stopLe_of_key_le t I hk').symm
  have happ := List.countP_append (p := fun I : Item => stopLeE t I.1) (l₁ := E.take j) (l₂ := E.drop j)
  rw [List.take_append_drop] at happ
  have htotalStop := count_evs_stopLe spans points t
  have htotalStopE : List.countP (fun I : Item => stopLeE t I.1) E =
      List.countP (hiLeSpan t) spans := by simpa [E] using htotalStop
  have hstop_le : List.countP (fun I : Item => isStopE I.1) (E.take j) ≤
      List.countP (hiLeSpan t) spans := by omega
  have hdropStart : List.countP (fun I : Item => startLtE t I.1) (E.drop j) = 0 := by
    apply countP_eq_zero_of_not
    intro I hI
    have hk := key_ge_of_mem_drop E hpw j hj q hEj I hI
    exact not_startLt_of_key_ge_stop t I (by omega)
  have happStart := List.countP_append (p := fun I : Item => startLtE t I.1) (l₁ := E.take j) (l₂ := E.drop j)
  rw [List.take_append_drop] at happStart
  have htotalStart := count_evs_startLt spans points t
  have htotalStartE : List.countP (fun I : Item => startLtE t I.1) E =
      List.countP (loLtSpan t) spans := by simpa [E] using htotalStart
  have hstart_total : List.countP (fun I : Item => startLtE t I.1) (E.take j) =
      List.countP (loLtSpan t) spans := by
    have ht := countP_take_eq_of_drop_zero hdropStart
    rw [ht, htotalStartE]
  have hineq := hi_le_count_le_lo_lt spans t
  have hleStart : List.countP (fun I : Item => startLtE t I.1) (E.take j) ≤
      List.countP (fun I : Item => isStartE I.1) (E.take j) := by
    apply countP_le_of_imp
    intro I hI hp
    exact startLt_implies_start t I hp
  change List.countP (fun I : Item => isStopE I.1) (E.take j) ≤
      List.countP (fun I : Item => isStartE I.1) (E.take j)
  omega

/-! ### Simulating the sweep counter -/

private def cnt (l : List Item) : Nat :=
  List.countP (fun I : Item => isStartE I.1) l - List.countP (fun I : Item => isStopE I.1) l

private theorem cnt_succ (spans : List Span) (points : List Cell)
    (j : Nat) (hj : j < (evs spans points).length) :
    cnt ((evs spans points).take (j + 1)) =
      match (evs spans points)[j].1.op with
      | Op.stop => cnt ((evs spans points).take j) - 1
      | Op.start => cnt ((evs spans points).take j) + 1
      | Op.query _ => cnt ((evs spans points).take j) := by
  let E := evs spans points
  change cnt (E.take (j + 1)) = match E[j].1.op with
    | Op.stop => cnt (E.take j) - 1
    | Op.start => cnt (E.take j) + 1
    | Op.query _ => cnt (E.take j)
  have hts0 : E.take (j + 1) = E.take j ++ E[j]?.toList := List.take_add_one
  have ho : E[j]?.toList = [E[j]] := by
    simp [List.getElem?_eq_getElem (by simpa [E] using hj)]
  have hts : E.take (j + 1) = E.take j ++ [E[j]] := by rw [hts0, ho]
  have hgen0 := prefix_general_bound spans points j hj
  have hgen : List.countP (fun I : Item => isStopE I.1) (E.take j) ≤
      List.countP (fun I : Item => isStartE I.1) (E.take j) := by simpa [E] using hgen0
  rcases hq : E[j] with ⟨⟨time, op⟩, k⟩
  rw [hts, hq]
  cases op with
  | stop =>
      have hpos0 := prefix_stop_bound spans points time j hj (Event.mk time Op.stop, k) (by simpa [E] using hq) rfl
      have hpos : List.countP (fun I : Item => isStopE I.1) (E.take j) + 1 ≤
          List.countP (fun I : Item => isStartE I.1) (E.take j) := by simpa [E] using hpos0
      simp [cnt, List.countP_append, isStartE, isStopE] at hgen hpos ⊢
      omega
  | start =>
      simp [cnt, List.countP_append, isStartE, isStopE] at hgen ⊢
      omega
  | query i =>
      simp [cnt, List.countP_append, isStartE, isStopE]

private theorem go_size (l : List Item) (s : St) : (go l s).2.size = s.2.size := by
  induction l generalizing s with
  | nil => rfl
  | cons I l ih =>
      rcases I with ⟨⟨time, op⟩, k⟩
      cases op with
      | stop => exact ih (s.1 - 1, s.2)
      | start => exact ih (s.1 + 1, s.2)
      | query i =>
          change (go l (s.1, s.2.setIfInBounds i s.1)).2.size = s.2.size
          simpa [Array.size_setIfInBounds] using ih (s.1, s.2.setIfInBounds i s.1)

private theorem go_append (l₁ l₂ : List Item) (s : St) :
    go (l₁ ++ l₂) s = go l₂ (go l₁ s) := by
  induction l₁ generalizing s with
  | nil => rfl
  | cons I l ih =>
      rcases I with ⟨⟨time, op⟩, k⟩
      cases op with
      | stop => simp [go]; exact ih (s.1 - 1, s.2)
      | start => simp [go]; exact ih (s.1 + 1, s.2)
      | query i => simp [go]; exact ih (s.1, s.2.setIfInBounds i s.1)

private theorem go_prefix_counter (spans : List Span) (points : List Cell) (out : Array Nat) :
    ∀ j : Nat, j ≤ (evs spans points).length →
      (go ((evs spans points).take j) (0, out)).1 = cnt ((evs spans points).take j) := by
  let E := evs spans points
  intro j
  induction j with
  | zero => intro hj; simp [cnt, go]
  | succ j ih =>
      intro hj
      have hjlt : j < (evs spans points).length := by omega
      have hjltE : j < E.length := by simpa [E] using hjlt
      have hprev := ih (by omega : j ≤ (evs spans points).length)
      have hts0 : E.take (j + 1) = E.take j ++ E[j]?.toList := List.take_add_one
      have ho : E[j]?.toList = [E[j]] := by simp
      have hts : E.take (j + 1) = E.take j ++ [E[j]] := by rw [hts0, ho]
      have hc0 := cnt_succ spans points j hjlt
      have hc : cnt (E.take j ++ [E[j]]) = match E[j].1.op with
          | Op.stop => cnt (E.take j) - 1
          | Op.start => cnt (E.take j) + 1
          | Op.query _ => cnt (E.take j) := by simpa [E] using hc0
      rw [hts, go_append]
      change (go [E[j]] (go (E.take j) (0, out))).1 = cnt (E.take j ++ [E[j]])
      have hstate : go (E.take j) (0, out) = (cnt (E.take j), (go (E.take j) (0, out)).2) := by
        apply Prod.ext
        · simpa [E] using hprev
        · rfl
      rw [hstate]
      rcases hq : E[j] with ⟨⟨time, op⟩, k⟩
      have hq' : (evs spans points)[j] = (Event.mk time op, k) := by simpa [E] using hq
      rw [hq'] at hc
      simp at hc
      cases op with
      | stop => simpa [go, step] using hc.symm
      | start => simpa [go, step] using hc.symm
      | query i => simpa [go, step] using hc.symm

private theorem go_no_query (i : Nat) :
    ∀ (l : List Item) (a : Nat) (out : Array Nat),
      (∀ J ∈ l, J.1.op ≠ Op.query i) → (go l (a, out)).2[i]? = out[i]? := by
  intro l
  induction l with
  | nil => intro a out h; simp [go]
  | cons I l ih =>
      intro a out h
      rcases I with ⟨⟨time, op⟩, k⟩
      cases op with
      | stop =>
          change (go l (a - 1, out)).2[i]? = out[i]?
          exact ih (a - 1) out (fun J hj => h J (by simp [hj]))
      | start =>
          change (go l (a + 1, out)).2[i]? = out[i]?
          exact ih (a + 1) out (fun J hj => h J (by simp [hj]))
      | query j =>
          have hne : j ≠ i := by
            intro hji
            subst j
            exact h (Event.mk time (Op.query i), k) (by simp) rfl
          have hih := ih a (out.setIfInBounds j a) (fun J hj => h J (by simp [hj]))
          change (go l (a, out.setIfInBounds j a)).2[i]? = out[i]?
          rw [hih]
          rw [Array.getElem?_setIfInBounds]
          simp [hne]

private theorem go_query_write (i : Nat) (q : Item) (hq : q.1.op = Op.query i)
    (l : List Item) (a : Nat) (out : Array Nat) (hi : i < out.size)
    (hrest : ∀ J ∈ l, J.1.op ≠ Op.query i) :
    (go (q :: l) (a, out)).2[i]? = some a := by
  change (go l (step (a, out) q.1)).2[i]? = some a
  rcases q with ⟨⟨time, op⟩, k⟩
  cases op with
  | stop => simp at hq
  | start => simp at hq
  | query j =>
      have hji : j = i := by simpa using hq
      subst j
      have hih := go_no_query i l a (out.setIfInBounds i a) hrest
      simp [step]
      rw [hih]
      rw [Array.getElem?_setIfInBounds]
      simp [hi]

private theorem event_mem_evs (spans : List Span) (points : List Cell) {e : Event} :
    e ∈ base spans points → ∃ k : Nat, (e, k) ∈ evs spans points := by
  intro h
  obtain ⟨k, hk⟩ := List.mem_iff_getElem?.mp h
  refine ⟨k, List.mem_mergeSort.mpr ?_⟩
  exact List.mem_zipIdx_iff_le_and_getElem?_sub.mpr ⟨by omega, by simpa using hk⟩

private theorem no_query_item_of_not_some_some (spans : List Span) (points : List Cell) (i : Nat)
    (hpi : ∀ t : Int, points[i]? ≠ some (some t)) :
    ∀ J ∈ evs spans points, J.1.op ≠ Op.query i := by
  intro J hJ hJop
  have hJmap : J.1 ∈ (evs spans points).map Prod.fst := List.mem_map.mpr ⟨J, hJ, rfl⟩
  have hJevents : J.1 ∈ events spans points := by simpa [events_eq_map] using hJmap
  rw [events_merge] at hJevents
  have hJbase : J.1 ∈ base spans points := by
    obtain ⟨k, hk⟩ := List.mem_iff_getElem?.mp (List.mem_mergeSort.mp hJevents)
    exact List.mem_iff_getElem?.mpr ⟨k, by simpa [base] using hk⟩
  have hJprobe : J.1 ∈ probesOf points := by
    have h : J.1 ∈ endsOf spans ++ probesOf points := by simpa [base] using hJbase
    rcases List.mem_append.mp h with hends | hprobe
    · have hop := mem_endsOf_op spans J.1 hends
      simp [hJop] at hop
    · exact hprobe
  obtain ⟨i', t, hpt, he⟩ := (mem_probesOf_full points J.1).mp hJprobe
  have hi' : i' = i := by rw [he] at hJop; simpa using hJop
  rw [hi'] at hpt
  exact hpi t hpt

theorem sound : TinylambOpt.ResearchGoals.IntervalCountSweep.Goal := by
  intro spans points
  let E := evs spans points
  let out := Array.replicate points.length 0
  rw [sweep_go]
  change (go E (0, out)).2.toList = reference spans points
  apply List.ext_getElem?
  intro i
  by_cases hi : i < points.length
  · have hpi0 : points[i]? = some points[i] := List.getElem?_eq_getElem hi
    cases hcell : points[i] with
    | none =>
        have hpi : points[i]? = some none := by simpa [hcell] using hpi0
        have hnoq : ∀ J ∈ E, J.1.op ≠ Op.query i := by
          have h' : ∀ t : Int, points[i]? ≠ some (some t) := by intro t; simp [hpi]
          simpa [E] using no_query_item_of_not_some_some spans points i h' 
        have hgo := go_no_query i E 0 out hnoq
        rw [Array.getElem?_toList, hgo]
        have hf : (spans.filter (fun x => contains x none)).length = 0 := by simp [contains]
        simp [out, reference, hi, hpi, hf, hcell]
    | some t =>
        have hpi : points[i]? = some (some t) := by simpa [hcell] using hpi0
        have hprobe : Event.mk t (Op.query i) ∈ probesOf points :=
          (mem_probesOf_full points (Event.mk t (Op.query i))).mpr ⟨i, t, hpi, rfl⟩
        have hbase : Event.mk t (Op.query i) ∈ base spans points := by
          simp [base, hprobe]
        obtain ⟨qk, hqk⟩ := event_mem_evs spans points hbase
        let q : Item := (Event.mk t (Op.query i), qk)
        have hqmem : q ∈ E := by simpa [q, E] using hqk
        obtain ⟨j, hjq⟩ := List.mem_iff_getElem?.mp hqmem
        obtain ⟨hjlen, hEj'⟩ := List.getElem?_eq_some_iff.mp hjq
        have hEj : E[j] = q := hEj'
        have hrest : ∀ J ∈ E.drop (j + 1), J.1.op ≠ Op.query i := by
          intro J hJdrop hJop
          have hJmem : J ∈ E := by
            obtain ⟨m, hm⟩ := List.mem_iff_getElem?.mp hJdrop
            rw [List.getElem?_drop] at hm
            exact List.mem_iff_getElem?.mpr ⟨j + 1 + m, hm⟩
          have hJeq : J = q := query_item_unique spans points i J q hJmem hqmem hJop (by simp [q])
          obtain ⟨m, hm⟩ := List.mem_iff_getElem?.mp hJdrop
          rw [List.getElem?_drop] at hm
          rw [hJeq] at hm
          have heqidx : j + 1 + m = j :=
            ((List.Nodup.getElem?_inj (xs := E) hjlen
              (by simpa [E] using evs_nodup spans points)).mp
              (show E[j]? = E[j + 1 + m]? from by rw [hjq, hm])).symm
          omega
        have hc1 := prefix_query_counts spans points t i j (by simpa [E] using hjlen) q
          (by simpa [E] using hEj) (by simp [q])
        have hc1a : List.countP (fun I : Item => startLeE t I.1) (E.take j) =
            List.countP (loLeSpan t) spans := by simpa [E] using hc1.1
        have hc1b : List.countP (fun I : Item => stopLeE t I.1) (E.take j) =
            List.countP (hiLeSpan t) spans := by simpa [E] using hc1.2
        have hqkey : key q.1 = 4 * t + 2 := by simp [q, key_query]
        have hsEq : List.countP (fun I : Item => isStartE I.1) (E.take j) =
            List.countP (fun I : Item => startLeE t I.1) (E.take j) := by
          apply countP_congr_eq
          intro I hI
          have hk := key_le_of_mem_take E (evs_pw spans points) j hjlen q hEj I hI
          rw [hqkey] at hk
          exact (startLe_of_key_le t I hk).symm
        have hoEq : List.countP (fun I : Item => isStopE I.1) (E.take j) =
            List.countP (fun I : Item => stopLeE t I.1) (E.take j) := by
          apply countP_congr_eq
          intro I hI
          have hk := key_le_of_mem_take E (evs_pw spans points) j hjlen q hEj I hI
          rw [hqkey] at hk
          exact (stopLe_of_key_le t I hk).symm
        have hc : cnt (E.take j) = (spans.filter (contains · (some t))).length := by
          unfold cnt
          rw [hsEq, hoEq, hc1a, hc1b, ← reference_count spans t]
        have hjle : j ≤ (evs spans points).length := by simpa [E] using (Nat.le_of_lt hjlen)
        have hcnt := go_prefix_counter spans points out j hjle
        have hstate : go (E.take j) (0, out) =
            (cnt (E.take j), (go (E.take j) (0, out)).2) := by
          apply Prod.ext
          · simpa [E] using hcnt
          · rfl
        have hsplit : go E (0, out) = go (E.drop j) (go (E.take j) (0, out)) := by
          calc
            go E (0, out) = go (E.take j ++ E.drop j) (0, out) := by rw [List.take_append_drop]
            _ = go (E.drop j) (go (E.take j) (0, out)) := go_append _ _ _
        rw [hsplit, hstate, List.drop_eq_getElem_cons hjlen, hEj]
        have hsz0 := go_size (E.take j) (0, out)
        have hsz : (go (E.take j) (0, out)).2.size = out.size := by simpa using hsz0
        have houtsize : out.size = points.length := by simp [out]
        have hiarr : i < (go (E.take j) (0, out)).2.size := by omega
        have hwrite := go_query_write i q (by simp [q]) (E.drop (j + 1)) (cnt (E.take j))
          (go (E.take j) (0, out)).2 hiarr hrest
        rw [Array.getElem?_toList, hwrite, hc]
        simp [reference, List.getElem?_map, hi, hpi, hcell]
  · have hpi : points[i]? = none := List.getElem?_eq_none_iff.mpr (by omega)
    have hnoq : ∀ J ∈ E, J.1.op ≠ Op.query i := by
      have h' : ∀ t : Int, points[i]? ≠ some (some t) := by intro t; simp [hpi]
      simpa [E] using no_query_item_of_not_some_some spans points i h' 
    have hgo := go_no_query i E 0 out hnoq
    rw [Array.getElem?_toList, hgo]
    simp [out, reference, List.getElem?_map, hi]

end TinylambOpt.ResearchProofs.IntervalCountSweep
