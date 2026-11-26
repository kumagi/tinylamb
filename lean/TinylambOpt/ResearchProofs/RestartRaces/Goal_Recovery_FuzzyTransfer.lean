import TinylambOpt.ResearchGoals.RestartRaces.Recovery

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.

Proved

- `FuzzyTransfer` — Restart can crash between the two page installations. From
  arbitrary per-page cuts, interrupt redo at any record and replay from the start;
  both balances converge even for self-transfers and signed deltas.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Recovery_FuzzyTransfer

open TinylambOpt.ResearchGoals.RestartRaces.Recovery.FuzzyTransfer

private def redo1 (st : Nat × Int) (t : Transfer) (k : Nat) : Nat × Int :=
  let (seen, v) := st
  if (t.src == k || t.dst == k) && seen < t.stamp then (t.stamp, v + contribution t k) else (seen, v)

private theorem foldl_redo1 (l : List Transfer) (s : Nat → Nat × Int) (k : Nat) :
    (l.foldl redo s) k = l.foldl (fun st t => redo1 st t k) (s k) := by
  induction l generalizing s with
  | nil => rfl
  | cons t l ih =>
      simp only [List.foldl_cons]
      rw [ih (redo s t)]
      have hred : redo s t k = redo1 (s k) t k := rfl
      rw [hred]

private theorem contribution_notouch (t : Transfer) (k : Nat)
    (h : ¬((t.src == k || t.dst == k) = true)) : contribution t k = 0 := by
  have hs : (t.src != k) = true := by
    by_cases h' : (t.src == k) = true
    · exact (h (by simp [h'])).elim
    · cases hsrc : t.src == k <;> simp_all
  have hd : (t.dst != k) = true := by
    by_cases h' : (t.dst == k) = true
    · exact (h (by simp [h'])).elim
    · cases hdst : t.dst == k <;> simp_all
  have hs' : (k == t.src) = false := by cases hks : (k == t.src) <;> simp_all
  have hd' : (k == t.dst) = false := by cases hkd : (k == t.dst) <;> simp_all
  simp [contribution, hs', hd']

private theorem sum_filter_eq_zero {α : Type} (l : List α) (f : α → Int)
    (p : α → Bool) (h : ∀ x ∈ l, ¬p x = true) : ((l.filter p).map f).sum = 0 := by
  have hnil : l.filter p = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro x hx
    exact h x (List.mem_filter.mp hx).1 (List.mem_filter.mp hx).2
  simp [hnil]

private theorem sum_filter_split {α : Type} (l : List α) (f : α → Int) (p : α → Bool) :
    (l.map f).sum = ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      by_cases hp : p a = true
      · simp [hp, ih]
        omega
      · have hpf : p a = false := by
          cases hpv : p a <;> simp_all
        simp [hpf, ih]
        omega

private theorem filter_eq_of_pred {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.filter_cons]
      rw [h a (by simp), ih (fun x hx => h x (by simp [hx]))]

private theorem seen_mono (l : List Transfer) (st : Nat × Int) (k : Nat) :
    st.1 ≤ (l.foldl (fun st t => redo1 st t k) st).1 := by
  induction l generalizing st with
  | nil => simp
  | cons t l ih =>
      simp only [List.foldl_cons]
      have hstep : st.1 ≤ (redo1 st t k).1 := by
        rcases st with ⟨seen, v⟩
        by_cases hc : (t.src == k || t.dst == k) && seen < t.stamp
        · have hlt : seen < t.stamp := by simpa using (Bool.and_eq_true_iff.mp hc).2
          simp [redo1, hc]
          omega
        · simp [redo1, hc]
      exact Nat.le_trans hstep (ih (redo1 st t k))

private theorem redo1_seen_ge_of_touch (st : Nat × Int) (t : Transfer) (k : Nat)
    (h : (t.src == k || t.dst == k) = true) : t.stamp ≤ (redo1 st t k).1 := by
  by_cases hle : st.1 < t.stamp
  · simp [redo1, h, hle]
  · simp [redo1, h, hle]
    omega

private theorem redo_skip (st : Nat × Int) (t : Transfer) (k : Nat) (h : st.1 ≥ t.stamp) :
    redo1 st t k = st := by
  rcases st with ⟨seen, v⟩
  dsimp [redo1]
  by_cases hc : (t.src == k || t.dst == k) && seen < t.stamp
  · have hlt : seen < t.stamp := by simpa using (Bool.and_eq_true_iff.mp hc).2
    simp [redo1, hc]
    omega
  · simp [redo1, hc]

private theorem redo_skip_notouch (st : Nat × Int) (t : Transfer) (k : Nat)
    (h : ¬((t.src == k || t.dst == k) = true)) : redo1 st t k = st := by
  rcases st with ⟨seen, v⟩
  dsimp [redo1]
  by_cases hc : (t.src == k || t.dst == k) && seen < t.stamp
  · simp [redo1, hc]
    exact (h (Bool.and_eq_true_iff.mp hc).1).elim
  · simp [redo1, hc]

private theorem prefix_idem (l : List Transfer) (hpair : l.Pairwise (fun a b => a.stamp < b.stamp))
    (st : Nat × Int) (k : Nat) :
    l.foldl (fun st t => redo1 st t k) (l.foldl (fun st t => redo1 st t k) st) =
      l.foldl (fun st t => redo1 st t k) st := by
  induction l generalizing st with
  | nil => simp
  | cons t l ih =>
      simp only [List.foldl_cons]
      rw [List.pairwise_cons] at hpair
      have hskip : redo1 (l.foldl (fun st t => redo1 st t k) (redo1 st t k)) t k =
          l.foldl (fun st t => redo1 st t k) (redo1 st t k) := by
        by_cases htouch : (t.src == k || t.dst == k) = true
        · apply redo_skip
          have hge : t.stamp ≤ (redo1 st t k).1 := redo1_seen_ge_of_touch st t k htouch
          have hmono : (redo1 st t k).1 ≤
              (l.foldl (fun st t => redo1 st t k) (redo1 st t k)).1 :=
            seen_mono l (redo1 st t k) k
          exact Nat.le_trans hge hmono
        · exact redo_skip_notouch _ t k htouch
      rw [hskip]
      exact ih hpair.2 (redo1 st t k)

private theorem value_formula (l : List Transfer) (hpair : l.Pairwise (fun a b => a.stamp < b.stamp))
    (k : Nat) (st0 : Nat × Int) :
    (l.foldl (fun st t => redo1 st t k) st0).2 =
      st0.2 + (((l.filter (fun t => (t.src == k || t.dst == k) && st0.1 < t.stamp)).map
        (fun t => contribution t k)).sum) := by
  induction l generalizing st0 with
  | nil => simp
  | cons t l ih =>
      rw [List.pairwise_cons] at hpair
      simp only [List.foldl_cons]
      by_cases htouch : (t.src == k || t.dst == k) = true
      · by_cases hle : st0.1 < t.stamp
        · have happ : redo1 st0 t k = (t.stamp, st0.2 + contribution t k) := by
            dsimp [redo1]
            simp [htouch, hle]
          rw [happ]
          rw [ih hpair.2 (t.stamp, st0.2 + contribution t k)]
          have hfilt : (List.filter (fun u : Transfer => (u.src == k || u.dst == k) && t.stamp < u.stamp) l) =
              (List.filter (fun u : Transfer => (u.src == k || u.dst == k) && st0.1 < u.stamp) l) := by
            apply filter_eq_of_pred
            intro u hu
            have hgt := hpair.1 u hu
            simp [hgt, (show t.stamp < u.stamp from hgt), (show st0.1 < u.stamp by omega)]
          rw [hfilt]
          simp [htouch, hle]
          omega
        · have hskip : redo1 st0 t k = st0 := by
            dsimp [redo1]
            simp [htouch, hle]
          rw [hskip]
          rw [ih hpair.2 st0]
          have hfilt : (List.filter (fun u : Transfer => (u.src == k || u.dst == k) && st0.1 < u.stamp) (t :: l)) =
              (List.filter (fun u : Transfer => (u.src == k || u.dst == k) && st0.1 < u.stamp) l) := by
            simp [htouch, hle]
          rw [hfilt]
      · have hskip : redo1 st0 t k = st0 := by
          dsimp [redo1]
          simp [htouch]
        rw [hskip]
        rw [ih hpair.2 st0]
        have hfilt : (List.filter (fun u : Transfer => (u.src == k || u.dst == k) && st0.1 < u.stamp) (t :: l)) =
            (List.filter (fun u : Transfer => (u.src == k || u.dst == k) && st0.1 < u.stamp) l) := by
          simp [htouch]
        rw [hfilt]

private theorem sum_notouch_zero (l : List Transfer) (k : Nat)
    (h : ∀ t ∈ l, ¬((t.src == k || t.dst == k) = true)) :
    (l.map (fun t => contribution t k)).sum = 0 := by
  induction l with
  | nil => simp
  | cons t l ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [contribution_notouch t k (h t (by simp))]
      simp [ih (fun x hx => h x (by simp [hx]))]

private theorem filter_touch_gt (ts : List Transfer) (k cut : Nat) :
    (ts.filter (fun t => !(t.stamp ≤ cut))).filter (fun t => (t.src == k || t.dst == k)) =
      ts.filter (fun t => (t.src == k || t.dst == k) && cut < t.stamp) := by
  induction ts with
  | nil => rfl
  | cons t ts ih =>
      simp only [List.filter_cons]
      by_cases hle : t.stamp ≤ cut
      · have hnotgt : ¬cut < t.stamp := by omega
        simp [hle, hnotgt, ih]
      · have hgt : cut < t.stamp := by omega
        by_cases ht : (t.src == k || t.dst == k) = true
        · simp [hle, hgt, ht, ih]
        · have htf : (t.src == k || t.dst == k) = false := by
            cases hb : (t.src == k || t.dst == k) <;> simp_all
          simp [hle, hgt, htf, ih]

theorem sound : Goal := by
  intro ts base cut progress hpair
  dsimp
  intro k
  let f : Nat × Int → Transfer → Nat × Int := fun st t => redo1 st t k
  let st0 : Nat × Int := (cut k, base k + ((ts.filter (fun t => t.stamp ≤ cut k)).map (fun t => contribution t k)).sum)
  let pre := ts.take (progress k)
  let st1 := pre.foldl f st0
  have hpairpre : pre.Pairwise (fun a b => a.stamp < b.stamp) := by
    dsimp [pre]
    exact hpair.take
  have hid := prefix_idem pre hpairpre st0 k
  have hfinal : ts.foldl f st1 = ts.foldl f st0 := by
    rw [show ts = pre ++ ts.drop (progress k) by simp [pre, List.take_append_drop]]
    rw [List.foldl_append, List.foldl_append]
    rw [hid]
  have hpart : ((ts.filter (fun t => t.stamp ≤ cut k)).map (fun t => contribution t k)).sum +
      (((ts.filter (fun t => (t.src == k || t.dst == k) && cut k < t.stamp)).map
        (fun t => contribution t k)).sum) =
      (ts.map (fun t => contribution t k)).sum := by
    have h1 := sum_filter_split ts (fun t => contribution t k) (fun t => t.stamp ≤ cut k)
    have hgt : ((ts.filter (fun t => !(t.stamp ≤ cut k))).map (fun t => contribution t k)).sum =
        (((ts.filter (fun t => !(t.stamp ≤ cut k))).filter (fun t => (t.src == k || t.dst == k))).map
          (fun t => contribution t k)).sum := by
      have h2 := sum_filter_split (ts.filter (fun t => !(t.stamp ≤ cut k)))
        (fun t => contribution t k) (fun t => (t.src == k || t.dst == k))
      have hzero : (((ts.filter (fun t => !(t.stamp ≤ cut k))).filter
          (fun t => !(t.src == k || t.dst == k))).map (fun t => contribution t k)).sum = 0 := by
        apply sum_notouch_zero
        intro t ht
        have hn := (List.mem_filter.mp ht).2
        intro htouch
        cases hb : (t.src == k || t.dst == k) <;> simp_all
      omega
    have hfilt := filter_touch_gt ts k (cut k)
    rw [hfilt] at hgt
    omega
  let startF : Nat → Nat × Int := fun kk =>
    (cut kk, applyAll (ts.filter (fun t => t.stamp ≤ cut kk)) base kk)
  let partialF : Nat → Nat × Int := fun kk => ((ts.take (progress kk)).foldl redo startF) kk
  change (ts.foldl redo partialF k).2 = applyAll ts base k
  rw [foldl_redo1]
  have hpartial : partialF k = st1 := by
    dsimp [partialF, st1, pre, f]
    rw [foldl_redo1]
    rfl
  rw [hpartial]
  change (ts.foldl f st1).2 = applyAll ts base k
  rw [hfinal]
  rw [value_formula ts hpair k st0]
  dsimp [st0, applyAll]
  omega

end TinylambOpt.ResearchProofs.RestartRaces.Recovery_FuzzyTransfer
