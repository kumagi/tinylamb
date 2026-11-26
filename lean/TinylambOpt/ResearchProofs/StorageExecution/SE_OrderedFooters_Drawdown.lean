import TinylambOpt.ResearchProofs.StorageExecution.SE_OrderedFooters_Common

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved (10 claims, including the following)

- `Drawdown.AmendLeaf` — Update one cached block using the left and right partial
  states.
- `Drawdown.AscendingPage` — `∀ xs, xs.Pairwise (· ≤ ·) → (agg.reference xs).all (fun e
  => e.2.2==0)`
- `Drawdown.FrameSlice` — Use only the two ragged boundaries and whole-page summaries
  for a window.
-/
set_option maxHeartbeats 0
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.Drawdown
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.Compression.Wire
open SE_ORF

theorem ref_eq (xs : List Nat) :
    Drawdown.agg.reference xs =
      xs.head?.map (fun _ =>
        (xs.min?.getD 0, peak xs,
          peak ((xs.zipIdx 0).map (fun (x, i) => x - ((xs.drop i).min?.getD x))))) := rfl

theorem join_eq :
    Drawdown.agg.join =
      optionalJoin (fun (lo, hi, d) (l, h, e) =>
        (min lo l, max hi h, max (max d e) (hi - l))) := rfl

private theorem optjoin_ss (f : α → α → α) (a b : α) :
    optionalJoin f (some a) (some b) = some (f a b) := rfl

private theorem drop_cons_succ (x : Nat) (l : List Nat) (n : Nat) :
    ((x :: l).drop (n + 1) : List Nat) = l.drop n := by
  rw [List.drop_cons (i := n + 1) (a := x) (l := l) (by omega), Nat.add_sub_cancel]

private theorem drop_app_le (b : List Nat) :
    ∀ (a : List Nat) i, i ≤ a.length → (a ++ b).drop i = a.drop i ++ b := by
  intro a
  induction a with
  | nil =>
      intro i hi
      have h0 : i = 0 := by simpa using hi
      subst h0
      rfl
  | cons x r ih =>
      intro i hi
      by_cases hi0 : i = 0
      · subst hi0
        rfl
      · have hex : ∃ j, i = j + 1 := ⟨i - 1, by omega⟩
        obtain ⟨j, rfl⟩ := hex
        rw [show ((x :: r) ++ b : List Nat) = x :: (r ++ b) from rfl]
        rw [drop_cons_succ]
        rw [show ((x :: r : List Nat)).drop (j + 1) = r.drop j from drop_cons_succ x r j]
        rw [ih j (by rw [List.length_cons] at hi; omega)]

private theorem drop_app_len (a b : List Nat) :
    ∀ j, (a ++ b).drop (a.length + j) = b.drop j := by
  induction a with
  | nil => intro j; simp
  | cons x r ih =>
      intro j
      have hlen : (x :: r).length = r.length + 1 := by rw [List.length_cons]
      rw [show (x :: r ++ b : List Nat) = x :: (r ++ b) from rfl, hlen,
        Nat.add_right_comm r.length 1 j, drop_cons_succ, ih j]

/-! ## Indexing helpers (this environment lacks `List.get?`) -/

private def gget (l : List Nat) (i : Nat) : Option Nat :=
  match l, i with
  | [], _ => none
  | z :: _, 0 => some z
  | _ :: r, i + 1 => gget r i

private theorem gget_cons_zero (z : Nat) (r : List Nat) : gget (z :: r) 0 = some z := rfl

private theorem gget_cons_succ (z : Nat) (r : List Nat) (i : Nat) :
    gget (z :: r) (i + 1) = gget r i := rfl

private theorem gget_mem_zipIdx :
    ∀ (l : List Nat) (i k x : Nat), gget l i = some x → (x, k + i) ∈ l.zipIdx k := by
  intro l
  induction l with
  | nil => intro i k x h; exact absurd h (by simp [gget])
  | cons z r ih =>
      intro i k x h
      cases i with
      | zero =>
          rw [gget_cons_zero] at h
          have hz : x = z := (Option.some.inj h).symm
          subst hz
          rw [List.zipIdx_cons]
          exact List.mem_cons_self
      | succ i =>
          rw [gget_cons_succ] at h
          have hi := ih i (k + 1) x h
          rw [List.zipIdx_cons]
          refine List.mem_cons_of_mem _ ?_
          rw [show ((x, k + (i + 1)) : Nat × Nat) = (x, k + 1 + i) from
            congrArg (Prod.mk x) (by omega)]
          exact hi

private theorem zipIdx_mem_get :
    ∀ {l : List Nat} {k : Nat} {p : Nat × Nat}, p ∈ l.zipIdx k →
      ∃ i x, p = (x, k + i) ∧ gget l i = some x ∧ i < l.length := by
  intro l
  induction l with
  | nil => intro k p h; exact absurd h (by simp)
  | cons z r ih =>
      intro k p h
      rw [List.zipIdx_cons] at h
      rcases List.mem_cons.mp h with hp | hp
      · rw [hp]
        exact ⟨0, z, rfl, gget_cons_zero z r, Nat.zero_lt_succ _⟩
      · obtain ⟨i, x, hpe, hget, hlt⟩ := ih hp
        refine ⟨i + 1, x, ?_, by rw [gget_cons_succ]; exact hget, Nat.succ_lt_succ hlt⟩
        rw [hpe]
        exact congrArg (Prod.mk x) (by omega)

private theorem gget_lt_length : ∀ (l : List Nat) (i x : Nat), gget l i = some x → i < l.length := by
  intro l i x h
  induction l generalizing i x with
  | nil => exact absurd h (by simp [gget])
  | cons z r ih =>
      cases i with
      | zero => exact Nat.zero_lt_succ _
      | succ i =>
          rw [gget_cons_succ] at h
          exact Nat.succ_lt_succ (ih i x h)

private theorem drop_gget : ∀ (l : List Nat) (i x : Nat), gget l i = some x →
    (l.drop i : List Nat) = x :: l.drop (i + 1) := by
  intro l i x h
  induction l generalizing i x with
  | nil => exact absurd h (by simp [gget])
  | cons z r ih =>
      cases i with
      | zero =>
          rw [gget_cons_zero] at h
          have hz : x = z := (Option.some.inj h).symm
          subst hz
          rfl
      | succ i =>
          rw [gget_cons_succ] at h
          exact ih i x h

private theorem mem_of_gget : ∀ (l : List Nat) (i x : Nat), gget l i = some x → x ∈ l := by
  intro l i x h
  induction l generalizing i x with
  | nil => exact absurd h (by simp [gget])
  | cons z r ih =>
      cases i with
      | zero =>
          rw [gget_cons_zero] at h
          have hz : x = z := (Option.some.inj h).symm
          subst hz
          exact List.mem_cons_self
      | succ i =>
          rw [gget_cons_succ] at h
          exact List.mem_cons_of_mem z (ih i x h)

private theorem gget_of_mem : ∀ (l : List Nat) (x : Nat), x ∈ l → ∃ i, gget l i = some x := by
  intro l
  induction l with
  | nil => intro x h; exact absurd h (by simp)
  | cons z r ih =>
      intro x h
      rcases List.mem_cons.mp h with rfl | h'
      · exact ⟨0, gget_cons_zero x r⟩
      · obtain ⟨i, hi⟩ := ih x h'
        exact ⟨i + 1, by rw [gget_cons_succ]; exact hi⟩

/-! ## min? / max? helpers -/

private theorem min?_some (l : List Nat) (hl : l ≠ []) : ∃ v, l.min? = some v := by
  cases l with
  | nil => exact absurd hl (by simp)
  | cons z r =>
      cases h : r.min? with
      | none => exact ⟨z, by rw [List.min?_cons, h]; simp⟩
      | some m => exact ⟨Nat.min z m, by rw [List.min?_cons, h]; simp⟩

private theorem max?_some (l : List Nat) (hl : l ≠ []) : ∃ v, l.max? = some v := by
  cases l with
  | nil => exact absurd hl (by simp)
  | cons z r =>
      cases h : r.max? with
      | none => exact ⟨z, by rw [List.max?_cons, h]; simp⟩
      | some m => exact ⟨Nat.max z m, by rw [List.max?_cons, h]; simp⟩

private theorem eq_nil_of_min?_none : ∀ (l : List Nat), l.min? = none → l = [] := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons z r ih =>
      intro h
      cases hr : r.min? with
      | none => rw [List.min?_cons, hr, Option.elim_none] at h; cases h
      | some m => rw [List.min?_cons, hr, Option.elim_some _ _ m] at h; cases h

private theorem min?_getD_noneq (l : List Nat) (x : Nat) (hl : l ≠ []) :
    l.min?.getD x = l.min?.getD 0 := by
  obtain ⟨v, hv⟩ := min?_some l hl
  rw [hv]
  simp only [Option.getD_some]

private theorem min?_getD_le : ∀ (l : List Nat) (d x : Nat), x ∈ l → l.min?.getD d ≤ x := by
  intro l
  induction l with
  | nil => intro d x h; exact absurd h (by simp)
  | cons z r ih =>
      intro d x hx
      cases hr : r.min? with
      | none =>
          have hnil : r = [] := eq_nil_of_min?_none r hr
          subst hnil
          rw [List.min?_cons, List.min?_nil, Option.elim_none, Option.getD_some]
          rcases List.mem_cons.mp hx with rfl | hx'
          · exact Nat.le_refl _
          · exact absurd hx' (by simp)
      | some m =>
          rw [List.min?_cons, hr, Option.elim_some, Option.getD_some]
          rcases List.mem_cons.mp hx with rfl | hx'
          · exact Nat.min_le_left _ _
          · have hi := ih d x hx'
            rw [hr] at hi
            simp only [Option.getD_some] at hi
            exact Nat.le_trans (Nat.min_le_right _ _) hi

private theorem min?_getD_mem : ∀ (l : List Nat) (x : Nat), l ≠ [] → l.min?.getD x ∈ l := by
  intro l
  induction l with
  | nil => intro x h; exact absurd h (by simp)
  | cons z r ih =>
      intro x hl
      cases hr : r.min? with
      | none =>
          have hnil : r = [] := eq_nil_of_min?_none r hr
          subst hnil
          rw [List.min?_cons, List.min?_nil, Option.elim_none, Option.getD_some]
          exact List.mem_cons_self
      | some m =>
          rw [List.min?_cons, hr, Option.elim_some, Option.getD_some]
          by_cases hzm : z ≤ m
          · rw [Nat.min_eq_left hzm]
            exact List.mem_cons_self
          · rw [Nat.min_eq_right (Nat.le_of_not_le hzm)]
            exact List.mem_cons_of_mem z (List.min?_mem hr)

private theorem min?_append_getD (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) (x : Nat) :
    ((a ++ b).min?.getD x) = min (a.min?.getD x) (b.min?.getD x) := by
  have hne : (a ++ b : List Nat) ≠ [] := by
    cases a with
    | nil => exact absurd ha (by simp)
    | cons w ws =>
        intro hc
        rw [show ((w :: ws) ++ b : List Nat) = w :: (ws ++ b) from rfl] at hc
        cases hc
  refine Nat.le_antisymm (Nat.le_min.mpr ⟨?_, ?_⟩) ?_
  · exact min?_getD_le (a ++ b) x (a.min?.getD x)
      (List.mem_append.mpr (Or.inl (min?_getD_mem a x ha)))
  · exact min?_getD_le (a ++ b) x (b.min?.getD x)
      (List.mem_append.mpr (Or.inr (min?_getD_mem b x hb)))
  · have hLM : ∀ y ∈ (a ++ b : List Nat), min (a.min?.getD x) (b.min?.getD x) ≤ y := by
      intro y hy
      rcases List.mem_append.mp hy with hy | hy
      · exact Nat.le_trans (Nat.min_le_left _ _) (min?_getD_le a x y hy)
      · exact Nat.le_trans (Nat.min_le_right _ _) (min?_getD_le b x y hy)
    exact hLM _ (min?_getD_mem (a ++ b) x hne)

private theorem max?_getD_mem : ∀ (l : List Nat) (x : Nat), l ≠ [] → l.max?.getD x ∈ l := by
  intro l
  induction l with
  | nil => intro x h; exact absurd h (by simp)
  | cons z r ih =>
      intro x hl
      cases hr : r.max? with
      | none =>
          have hnil : r = [] := by
            cases r with
            | nil => rfl
            | cons w ws =>
                obtain ⟨v, hv⟩ := max?_some (w :: ws) (by simp)
                rw [hr] at hv
                cases hv
          rw [hnil, List.max?_cons, List.max?_nil, Option.elim_none, Option.getD_some]
          exact List.mem_cons_self
      | some m =>
          rw [List.max?_cons, hr, Option.elim_some, Option.getD_some]
          by_cases hzm : z ≤ m
          · rw [Nat.max_eq_right hzm]
            exact List.mem_cons_of_mem z (List.max?_mem hr)
          · rw [Nat.max_eq_left (Nat.le_of_not_le hzm)]
            exact List.mem_cons_self

private theorem peak_mem (a : List Nat) (ha : a ≠ []) : peak a ∈ a := max?_getD_mem a 0 ha

/-! ## Peak / drawdown component lemmas -/

theorem peak_append (a b : List Nat) : peak (a ++ b) = max (peak a) (peak b) := by
  refine Nat.le_antisymm (peak_le_of_le ?_) (Nat.max_le.mpr ⟨?_, ?_⟩)
  · intro y hy
    rcases List.mem_append.mp hy with hy | hy
    · exact Nat.le_trans (le_peak_of_mem hy) (Nat.le_max_left _ _)
    · exact Nat.le_trans (le_peak_of_mem hy) (Nat.le_max_right _ _)
  · refine peak_le_of_le (n := peak (a ++ b)) ?_
    intro x hx
    exact le_peak_of_mem (List.mem_append.mpr (Or.inl hx))
  · refine peak_le_of_le (n := peak (a ++ b)) ?_
    intro x hx
    exact le_peak_of_mem (List.mem_append.mpr (Or.inr hx))

/-- The drawdown statistic of an append. -/
theorem dd_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    peak (((a ++ b).zipIdx 0).map (fun (x, i) => x - (((a ++ b).drop i).min?.getD x)))
      = max (max (peak ((a.zipIdx 0).map (fun (x, i) => x - ((a.drop i).min?.getD x))))
               (peak ((b.zipIdx 0).map (fun (x, i) => x - ((b.drop i).min?.getD x)))))
          (peak a - (b.min?.getD 0)) := by
  have gab : List (Nat × Nat) → Nat :=
    fun qs => peak (qs.map (fun (x, i) => x - (((a ++ b).drop i).min?.getD x)))
  rw [List.zipIdx_append]
  simp only [Nat.zero_add]
  rw [List.map_append]
  refine Nat.le_antisymm (peak_le_of_le ?_) (Nat.max_le.mpr ⟨Nat.max_le.mpr ⟨?_, ?_⟩, ?_⟩)
  · -- upper bound
    intro yv hy
    rcases List.mem_append.mp hy with hpa | hpb
    · obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hpa
      obtain ⟨i, x, hpe, hxi, hlt⟩ := zipIdx_mem_get hp
      simp only [Nat.zero_add] at hpe
      subst hpe
      -- goal: x - ((a ++ b).drop i).min?.getD x ≤ max (max da db) (peak a - bget)
      have hne : (a.drop i : List Nat) ≠ [] := by
        intro hc
        rw [drop_gget a i x hxi] at hc
        cases hc
      have hd : ((a ++ b).drop i : List Nat) = a.drop i ++ b :=
        drop_app_le b a i (Nat.le_of_lt hlt)
      have hb0 : (b.min?.getD x) = b.min?.getD 0 := min?_getD_noneq b x hb
      have hle2 : x - (a.drop i).min?.getD x
          ≤ peak ((a.zipIdx 0).map (fun (x, i) => x - (a.drop i).min?.getD x)) :=
        le_peak_of_mem (List.mem_map_of_mem
          (f := fun (x, i) => x - (a.drop i).min?.getD x)
          (by have h0 := gget_mem_zipIdx a i 0 x hxi; simpa using h0))
      by_cases hmin : (a.drop i).min?.getD x ≤ b.min?.getD x
      · rw [show ((a ++ b).drop i).min?.getD x = (a.drop i).min?.getD x from by
          rw [hd, min?_append_getD (a.drop i) b hne hb x, Nat.min_eq_left hmin]]
        exact Nat.le_trans hle2 (Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_left _ _))
      · rw [show ((a ++ b).drop i).min?.getD x = b.min?.getD x from by
          rw [hd, min?_append_getD (a.drop i) b hne hb x,
            Nat.min_eq_right (Nat.le_of_not_le hmin)]]
        rw [hb0]
        have hxp : x ≤ peak a := le_peak_of_mem (mem_of_gget a i x hxi)
        exact Nat.le_trans (Nat.sub_le_sub_right hxp (b.min?.getD 0)) (Nat.le_max_right _ _)
    · obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hpb
      obtain ⟨j, x, hpe, hji, hlt⟩ := zipIdx_mem_get hp
      subst hpe
      -- goal: x - ((a ++ b).drop (a.length + j)).min?.getD x ≤ max (max da db) (...)
      have hle2 : x - (b.drop j).min?.getD x
          ≤ peak ((b.zipIdx 0).map (fun (x, i) => x - (b.drop i).min?.getD x)) :=
        le_peak_of_mem (List.mem_map_of_mem
          (f := fun (x, i) => x - (b.drop i).min?.getD x)
          (by have h0 := gget_mem_zipIdx b j 0 x hji; simpa using h0))
      refine Nat.le_trans ?_ (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
      show x - ((a ++ b).drop (a.length + j)).min?.getD x ≤ _
      rw [show ((a ++ b).drop (a.length + j) : List Nat) = b.drop j from drop_app_len a b j]
      exact hle2
  · -- da ≤ ...
    apply peak_le_of_le
    intro y hy
    obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hy
    obtain ⟨i, x, hpe, hxi, hlt⟩ := zipIdx_mem_get hp
    simp only [Nat.zero_add] at hpe
    subst hpe
    have hne : (a.drop i : List Nat) ≠ [] := by
      intro hc
      rw [drop_gget a i x hxi] at hc
      cases hc
    refine Nat.le_trans ?_ (le_peak_of_mem
      (List.mem_append.mpr (Or.inl (List.mem_map_of_mem
        (f := fun (x, i) => x - ((a ++ b).drop i).min?.getD x) hp))))
    -- goal: x - (a.drop i).min?.getD x ≤ x - ((a ++ b).drop i).min?.getD x
    have hle : ((a ++ b).drop i).min?.getD x ≤ (a.drop i).min?.getD x := by
      rw [show ((a ++ b).drop i : List Nat) = a.drop i ++ b from
        drop_app_le b a i (Nat.le_of_lt hlt), min?_append_getD (a.drop i) b hne hb x]
      exact Nat.min_le_left _ _
    exact Nat.sub_le_sub_left hle x
  · -- db ≤ ...
    apply peak_le_of_le
    intro y hy
    obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hy
    obtain ⟨j, x, hpe, hji, hlt⟩ := zipIdx_mem_get hp
    subst hpe
    have hmid : (x, a.length + j) ∈ (b.zipIdx a.length : List (Nat × Nat)) :=
      gget_mem_zipIdx b j a.length x hji
    refine Nat.le_trans ?_ (le_peak_of_mem
      (List.mem_append.mpr (Or.inr (List.mem_map_of_mem
        (f := fun (x, i) => x - ((a ++ b).drop i).min?.getD x) hmid))))
    simp only [Nat.zero_add, Prod.fst, Prod.snd]
    show x - (b.drop j).min?.getD x ≤ x - ((a ++ b).drop (a.length + j)).min?.getD x
    rw [show ((a ++ b).drop (a.length + j) : List Nat) = b.drop j from drop_app_len a b j]
    exact Nat.le_refl _
  · -- peak a - bget ≤ ...
    obtain ⟨j, haj⟩ := gget_of_mem a (peak a) (peak_mem a ha)
    have hjl : j < a.length := gget_lt_length a j (peak a) haj
    have hjb : ((a ++ b).drop j : List Nat) = a.drop j ++ b :=
      drop_app_le b a j (Nat.le_of_lt hjl)
    have hne : (a.drop j : List Nat) ≠ [] := by
      intro hc
      rw [drop_gget a j (peak a) haj] at hc
      cases hc
    have hb0 : (b.min?.getD (peak a)) = b.min?.getD 0 := min?_getD_noneq b (peak a) hb
    have hle' : ((a ++ b).drop j).min?.getD (peak a) ≤ b.min?.getD 0 := by
      rw [hjb, min?_append_getD (a.drop j) b hne hb (peak a), hb0]
      exact Nat.min_le_right _ _
    refine Nat.le_trans (Nat.sub_le_sub_left hle' (peak a))
      (le_peak_of_mem (List.mem_append.mpr (Or.inl (List.mem_map_of_mem
        (f := fun (x, i) => x - ((a ++ b).drop i).min?.getD x)
        (by have h0 := gget_mem_zipIdx a j 0 (peak a) haj; simpa using h0)))))

theorem stitch_agg : stitch Drawdown.agg := by
  intro a b
  cases a with
  | nil =>
      cases b with
      | nil => rfl
      | cons z zb => rfl
  | cons y ya =>
      cases b with
      | nil =>
          rw [List.append_nil]
          rfl
      | cons z zb =>
          rw [ref_eq, ref_eq, ref_eq, join_eq]
          rw [min?_append_getD (y :: ya) (z :: zb) (by simp) (by simp) _,
            peak_append, dd_append (y :: ya) (z :: zb) (by simp) (by simp)]
          exact (optjoin_ss _ _ _).trans rfl

theorem sound_Stitch : Drawdown.Stitch.Goal := stitch_agg
theorem sound_TreeReduce : Drawdown.TreeReduce.Goal := treeReduce_of_stitch stitch_agg
theorem sound_RepeatPower : Drawdown.RepeatPower.Goal := repeatPower_of_stitch stitch_agg
theorem sound_FrameSlice : Drawdown.FrameSlice.Goal := frameSlice_of_stitch stitch_agg
theorem sound_PrefixCheckpoint : Drawdown.PrefixCheckpoint.Goal :=
  prefixCheckpoint_of_stitch stitch_agg
theorem sound_AmendLeaf : Drawdown.AmendLeaf.Goal := amendLeaf_of_stitch stitch_agg
theorem sound_RunInput : Drawdown.RunInput.Goal :=
  runInput_of_stitch stitch_agg (repeatPower_of_stitch stitch_agg)
theorem sound_ReorderNeighbors : Drawdown.ReorderNeighbors.Goal :=
  reorderNeighbors_of_stitch stitch_agg

theorem sound_PeakRecovery : Drawdown.PeakRecovery.Goal := by
  intro a b ha hb
  rcases a with _ | ⟨y, ya⟩
  · exact absurd ha (by simp)
  rw [ref_eq]
  have hhead : (((y :: ya) ++ (b : List Nat)).head? : Option Nat) = some y := rfl
  have hgoal : ((((y :: ya) ++ b).head?.map
        (fun _ => (((y :: ya) ++ b).min?.getD 0, peak ((y :: ya) ++ b),
          peak ((((y :: ya) ++ b).zipIdx 0).map
            (fun (x, i) => x - (((y :: ya) ++ b).drop i).min?.getD x))))).map
        (fun r => r.2.2)).getD 0
      = peak ((((y :: ya) ++ b).zipIdx 0).map
          (fun (x, i) => x - (((y :: ya) ++ b).drop i).min?.getD x)) := by
    rw [hhead]
    rfl
  have hd := dd_append (y :: ya) b (by simp) hb
  rw [hgoal, hd]
  exact Nat.le_max_right _ _

private theorem min?_getD_head_le :
    ∀ (l : List Nat) (x : Nat), (∀ y ∈ l, x ≤ y) → x ≤ l.min?.getD x := by
  intro l x h
  cases l with
  | nil => simp
  | cons z r =>
      cases hr : r.min? with
      | none =>
          have hnil : r = [] := eq_nil_of_min?_none r hr
          subst hnil
          rw [List.min?_cons, List.min?_nil, Option.elim_none, Option.getD_some]
          exact h z List.mem_cons_self
      | some m =>
          have hi := min?_getD_head_le r x (fun y hy => h y (List.mem_cons_of_mem z hy))
          rw [hr] at hi
          simp only [Option.getD_some] at hi
          have h1 := h z List.mem_cons_self
          rw [List.min?_cons, hr, Option.elim_some, Option.getD_some]
          omega

private theorem drop_head_le :
    ∀ (l : List Nat) (i x : Nat), l.Pairwise (· ≤ ·) → gget l i = some x → ∀ y ∈ l.drop i, x ≤ y := by
  intro l
  induction l with
  | nil => intro i x _ hx; exact absurd hx (by simp [gget])
  | cons z r ih =>
      intro i x hp hx y hy
      cases i with
      | zero =>
          rw [gget_cons_zero] at hx
          have hz : x = z := (Option.some.inj hx).symm
          subst hz
          obtain ⟨h1, _⟩ := List.pairwise_cons.mp hp
          rcases List.mem_cons.mp hy with rfl | hy'
          · exact Nat.le_refl _
          · exact h1 _ hy'
      | succ i =>
          obtain ⟨_, h2⟩ := List.pairwise_cons.mp hp
          rw [List.drop_cons (i := i + 1) (a := z) (l := r) (by omega)] at hy
          rw [gget_cons_succ] at hx
          exact ih i x h2 hx y hy

theorem sound_AscendingPage : Drawdown.AscendingPage.Goal := by
  intro xs hps
  have hgen : ∀ (l : List Nat), l.Pairwise (· ≤ ·) →
      peak ((l.zipIdx 0).map (fun (x, i) => x - ((l.drop i).min?.getD x))) = 0 := by
    intro l hl
    refine Nat.le_antisymm (peak_le_of_le (n := 0) ?_) (Nat.zero_le _)
    intro v hv
    obtain ⟨p, hp, rfl⟩ := List.mem_map.mp hv
    obtain ⟨i, x, hpe, hxi, hlt⟩ := zipIdx_mem_get hp
    simp only [Nat.zero_add] at hpe
    subst hpe
    have hge : ∀ w ∈ (l.drop i : List Nat), x ≤ w := drop_head_le l i x hl hxi
    show x - ((l.drop i).min?.getD x) ≤ 0
    rw [Nat.sub_eq_zero_of_le (min?_getD_head_le (l.drop i) x hge)]
    exact Nat.le_refl _
  rcases xs with _ | ⟨y, ys⟩
  · rw [ref_eq]
    rfl
  have hd0 := hgen (y :: ys) hps
  rw [ref_eq, hd0]
  rfl

end TinylambOpt.ResearchProofs.StorageExecution.OrderedFooters.Drawdown
