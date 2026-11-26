import TinylambOpt.ResearchGoals.CteSinglePassSplitFilter

/-!
Proved

- `Goal` — Single-pass split-filter CTE scan (`query/tpch_select_rewrites.cpp`
  `__merged_scalar_agg`, TPC-DS Q90/Q9/Q28/Q88): pre-filtering by Kleene 3VL `base
  AND (p_1 OR ... OR p_k)` and conditionally accumulating all branches in one pass
  equals `k` separate scans (even when `ps` overlap or contain NULL predicates).

The reference model is defined in `TinylambOpt.ResearchGoals.CteSinglePassSplitFilter`.
-/

namespace TinylambOpt.ResearchProofs.CteSinglePassSplitFilter

open TinylambOpt.ResearchGoals.CteSinglePassSplitFilter

variable {α : Type}

/-! ## Kleene (three-valued) helpers -/

private theorem isTrue3_sqlAnd (a b : Option Bool) :
    isTrue3 (sqlAnd a b) = (isTrue3 a && isTrue3 b) := by
  rcases a with _ | ⟨aa⟩
  · rcases b with _ | ⟨bb⟩
    · rfl
    · cases bb <;> rfl
  · rcases b with _ | ⟨bb⟩
    · cases aa <;> rfl
    · cases aa <;> cases bb <;> rfl

private theorem isTrue3_sqlOr (a b : Option Bool) :
    isTrue3 (sqlOr a b) = (isTrue3 a || isTrue3 b) := by
  rcases a with _ | ⟨aa⟩
  · rcases b with _ | ⟨bb⟩
    · rfl
    · cases bb <;> rfl
  · rcases b with _ | ⟨bb⟩
    · cases aa <;> rfl
    · cases aa <;> cases bb <;> rfl

/-- A row survives the split filter iff `base` is true and *some* branch
    predicate is true on it. -/
private theorem isTrue3_foldl_sqlOr (r : α) :
    ∀ (acc : Option Bool) (ps : List (α → Option Bool)),
      isTrue3 (List.foldl (fun a p => sqlOr a (p r)) acc ps)
        = (isTrue3 acc || ps.any (fun p => isTrue3 (p r))) := by
  intro acc ps
  induction ps generalizing acc with
  | nil => simp
  | cons p ps ih =>
      rw [List.foldl_cons, ih (sqlOr acc (p r)), isTrue3_sqlOr, List.any_cons]
      cases isTrue3 acc <;> cases isTrue3 (p r) <;>
        cases ps.any (fun p => isTrue3 (p r)) <;> rfl

private theorem isTrue3_disjPred (r : α) (ps : List (α → Option Bool)) :
    isTrue3 (disjPred ps r) = ps.any (fun p => isTrue3 (p r)) := by
  show isTrue3 (List.foldl (fun a p => sqlOr a (p r)) (some false) ps) = _
  rw [isTrue3_foldl_sqlOr]
  simp [isTrue3]

private theorem any_true_of_mem {β : Type} :
    ∀ (l : List β) (a : β), a ∈ l → ∀ (p : β → Bool), p a = true → l.any p = true := by
  intro l
  induction l with
  | nil => intro a ha; exact absurd ha List.not_mem_nil
  | cons b bs ih =>
      intro a ha p hpa
      rcases List.mem_cons.mp ha with hab | hab
      · subst hab
        rw [List.any_cons, hpa]
        exact Bool.true_or _
      · rw [List.any_cons, ih a hab p hpa]
        exact Bool.or_true _

/-! ## Accumulator steps and per-branch folds -/

private def bump (m : α → Option Int) (r : α) (acc : Nat × Option Int) : Nat × Option Int :=
  (acc.1 + 1, combineSumOpt acc.2 (m r))

private def branchStep (q : α → Bool) (m : α → Option Int)
    (acc : Nat × Option Int) (r : α) : Nat × Option Int :=
  if q r then bump m r acc else acc

/-- Fold of one branch's `(COUNT, SUM)` accumulator with predicate `q`. -/
private def aggFold (q : α → Bool) (m : α → Option Int) (ws : List α)
    (acc : Nat × Option Int) : Nat × Option Int :=
  List.foldl (branchStep q m) acc ws

/-- The single-pass per-branch update, seen as a function of one `zip`ped cell. -/
private def sstep (m : α → Option Int) (r : α)
    (x : (α → Option Bool) × (Nat × Option Int)) : Nat × Option Int :=
  match x with
  | (p, acc) => branchStep (fun s => isTrue3 (p s)) m acc r

private theorem updateBranches_eq (m : α → Option Int) (r : α)
    (ps : List (α → Option Bool)) (accs : List (Nat × Option Int)) :
    updateBranches r m ps accs = List.map (sstep m r) (List.zip ps accs) := by
  unfold updateBranches
  congr 1

private theorem foldl_step_congr {β γ : Type} (ws : List β) (acc : γ)
    (f g : γ → β → γ) (h : ∀ b r, f b r = g b r) :
    List.foldl f acc ws = List.foldl g acc ws := by
  induction ws generalizing acc with
  | nil => rfl
  | cons x ws ih =>
      show List.foldl f (f acc x) ws = List.foldl g (g acc x) ws
      rw [← h]
      exact ih (f acc x)

private theorem aggFold_cons (q : α → Bool) (m : α → Option Int) (w : α) (ws : List α)
    (acc : Nat × Option Int) :
    aggFold q m (w :: ws) acc = aggFold q m ws (branchStep q m acc w) := by
  simp only [aggFold, List.foldl_cons]

private theorem foldl_step_congr_mem {β γ : Type} (ws : List β) (acc : γ)
    (f g : γ → β → γ) (h : ∀ b, ∀ r ∈ ws, f b r = g b r) :
    List.foldl f acc ws = List.foldl g acc ws := by
  induction ws generalizing acc with
  | nil => rfl
  | cons x ws ih =>
      rw [List.foldl_cons, List.foldl_cons, ← h acc x List.mem_cons_self]
      exact ih (f acc x) (fun b r hr => h b r (List.mem_cons_of_mem x hr))

private theorem multiPassBranch_eq (base : α → Option Bool) (measure : α → Option Int)
    (rows : List α) (p : α → Option Bool) :
    multiPassBranch rows base p measure =
      aggFold (fun r => isTrue3 (sqlAnd (base r) (p r))) measure rows (0, none) := by
  refine foldl_step_congr rows (0, none) ?_ ?_ ?_
  intro b r
  cases b with
  | mk cnt sumAcc => simp only [bump, branchStep]

/-! ## Zip plumbing -/

private theorem length_zip {β : Type} :
    ∀ (ps : List α) (accs : List β), (List.zip ps accs).length = min ps.length accs.length := by
  intro ps
  induction ps with
  | nil =>
      intro accs
      cases accs with
      | nil => rfl
      | cons b bs => simp
  | cons p pt ih =>
      intro accs
      cases accs with
      | nil => simp
      | cons b bs => simp [ih bs]

private theorem zip_map_snd {β : Type} :
    ∀ (ps : List α) (accs : List β),
      accs.length = ps.length → List.map (fun x => x.2) (List.zip ps accs) = accs := by
  intro ps
  induction ps with
  | nil =>
      intro accs h
      cases accs with
      | nil => rfl
      | cons b bs => exact absurd h (by simp)
  | cons p pt ih =>
      intro accs h
      cases accs with
      | nil => exact absurd h (by simp)
      | cons b bs =>
          rw [List.zip_cons_cons, List.map_cons]
          rw [ih bs (by simpa using h)]

private theorem zip_map_sstep (m : α → Option Int) (w : α) :
    ∀ (ps : List (α → Option Bool)) (accs : List (Nat × Option Int)),
      accs.length = ps.length →
        List.zip ps (List.map (sstep m w) (List.zip ps accs))
          = List.map (fun x => (x.1, sstep m w x)) (List.zip ps accs) := by
  intro ps
  induction ps with
  | nil =>
      intro accs _
      cases accs with
      | nil => rfl
      | cons b bs => rfl
  | cons p pt ih =>
      intro accs h
      cases accs with
      | nil => rfl
      | cons b bs =>
          simp [ih bs (by simpa using h)]

private theorem zip_replicate {β : Type} (a : β) :
    ∀ ps : List α, List.zip ps (List.replicate ps.length a) = List.map (fun p => (p, a)) ps := by
  intro ps
  induction ps with
  | nil => rfl
  | cons p pt ih =>
      rw [List.length_cons, List.replicate_succ, List.zip_cons_cons, List.map_cons, ih]

/-! ## Componentwise view of the single-pass fold -/

private theorem foldl_sstep_comp (m : α → Option Int) (ps : List (α → Option Bool)) :
    ∀ (ws : List α) (accs : List (Nat × Option Int)),
      accs.length = ps.length →
        List.foldl (fun accs r => List.map (sstep m r) (List.zip ps accs)) accs ws
          = List.map (fun x => aggFold (fun r => isTrue3 (x.1 r)) m ws x.2) (List.zip ps accs) := by
  intro ws
  induction ws with
  | nil =>
      intro accs h
      simp only [List.foldl_nil, aggFold, List.foldl_nil]
      exact (zip_map_snd ps accs h).symm
  | cons w ws ih =>
      intro accs h
      rw [List.foldl_cons]
      have hlen : (List.map (sstep m w) (List.zip ps accs)).length = ps.length := by
        rw [List.length_map, length_zip ps accs]
        omega
      rw [ih (List.map (sstep m w) (List.zip ps accs)) hlen,
          zip_map_sstep m w ps accs h, List.map_map]
      refine List.map_congr_left (fun x _ => ?_)
      cases x with
      | mk p acc =>
          rw [aggFold_cons]
          congr 1

/-! ## Dropping filtered rows from the multi-pass folds -/

private theorem bool_lemma (A B D : Bool) (hBD : B = true → D = true) (hK : ¬ (A && D)) :
    (A && B) = false := by
  cases A <;> cases B
  · rfl
  · rfl
  · rfl
  · exact absurd (hBD rfl) hK

private theorem keep_and (base : α → Option Bool) (ps : List (α → Option Bool))
    (p : α → Option Bool) (hp : p ∈ ps) (r : α) :
    (isTrue3 (sqlAnd (base r) (p r)) && isTrue3 (sqlAnd (base r) (disjPred ps r)))
      = isTrue3 (sqlAnd (base r) (p r)) := by
  rw [isTrue3_sqlAnd (base r) (p r), isTrue3_sqlAnd (base r) (disjPred ps r),
      isTrue3_disjPred r ps]
  have hB2 : isTrue3 (p r) = true → ps.any (fun q => isTrue3 (q r)) = true :=
    any_true_of_mem ps p hp (fun q => isTrue3 (q r))
  cases hA : isTrue3 (base r) <;> cases hB : isTrue3 (p r) <;>
    cases hD : ps.any (fun q => isTrue3 (q r)) <;>
    (try rfl) <;> (try exact absurd (hB2 hB) (by simp [hD]))

private theorem keep_imp (base : α → Option Bool) (ps : List (α → Option Bool))
    (p : α → Option Bool) (r : α)
    (hK : isTrue3 (sqlAnd (base r) (disjPred ps r)) = true) :
    isTrue3 (sqlAnd (base r) (p r)) = isTrue3 (p r) := by
  rw [isTrue3_sqlAnd (base r) (disjPred ps r), isTrue3_disjPred r ps] at hK
  obtain ⟨hA, _⟩ := Bool.and_eq_true_iff.mp hK
  rw [isTrue3_sqlAnd (base r) (p r), hA]
  simp

private theorem foldl_filter_congr {β γ : Type} (ws : List β) (acc : γ) (g : β → Bool)
    (f : γ → β → γ) (h : ∀ r ∈ ws, ¬ g r → ∀ b, f b r = b) :
    List.foldl f acc ws = List.foldl f acc (List.filter g ws) := by
  induction ws generalizing acc with
  | nil => rfl
  | cons x ws ih =>
      by_cases hx : g x = true
      · rw [List.filter_cons_of_pos hx, List.foldl_cons]
        exact ih (f acc x) (fun r hr hg => h r (List.mem_cons_of_mem x hr) hg)
      · rw [List.filter_cons_of_neg hx, List.foldl_cons]
        rw [h x List.mem_cons_self hx]
        exact ih acc (fun r hr hg => h r (List.mem_cons_of_mem x hr) hg)

private theorem branch_eq (rows : List α) (base : α → Option Bool)
    (ps : List (α → Option Bool)) (measure : α → Option Int)
    (p : α → Option Bool) (hp : p ∈ ps) :
    aggFold (fun r => isTrue3 (p r)) measure
        (List.filter (fun r => isTrue3 (sqlAnd (base r) (disjPred ps r))) rows) (0, none)
      = multiPassBranch rows base p measure := by
  rw [multiPassBranch_eq base measure]
  simp only [aggFold]
  have hdrop : ∀ r ∈ rows, ¬ isTrue3 (sqlAnd (base r) (disjPred ps r)) →
      ∀ b, branchStep (fun s => isTrue3 (sqlAnd (base s) (p s))) measure b r = b := by
    intro r hr hK b
    have hBD : isTrue3 (p r) = true → isTrue3 (disjPred ps r) = true := by
      intro hB
      rw [isTrue3_disjPred]
      exact any_true_of_mem ps p hp (fun q => isTrue3 (q r)) hB
    have hq : isTrue3 (sqlAnd (base r) (p r)) = false := by
      rw [isTrue3_sqlAnd (base r) (p r)]
      have hK' : ¬ (isTrue3 (base r) && isTrue3 (disjPred ps r)) := by
        rwa [← isTrue3_sqlAnd (base r) (disjPred ps r)]
      exact bool_lemma _ _ _ hBD hK'
    simp [branchStep, hq]
  rw [foldl_filter_congr rows (0, none) (fun r => isTrue3 (sqlAnd (base r) (disjPred ps r)))
        (branchStep (fun r => isTrue3 (sqlAnd (base r) (p r))) measure) hdrop]
  refine foldl_step_congr_mem
      (List.filter (fun r => isTrue3 (sqlAnd (base r) (disjPred ps r))) rows) (0, none)
    (branchStep (fun r => isTrue3 (p r)) measure)
    (branchStep (fun r => isTrue3 (sqlAnd (base r) (p r))) measure)
    (fun b r hr => ?_)
  have hK := (List.mem_filter.mp hr).2
  have hq := keep_imp base ps p r hK
  simp [branchStep, hq]

/-! ## Main theorem -/

private theorem sps_expand (rows : List α) (base : α → Option Bool)
    (ps : List (α → Option Bool)) (measure : α → Option Int) :
    singlePassSplit rows base ps measure
      = List.foldl (fun accs r => List.map (sstep measure r) (List.zip ps accs))
          (List.replicate ps.length (0, none))
          (List.filter (fun r => isTrue3 (sqlAnd (base r) (disjPred ps r))) rows) := by
  simp only [singlePassSplit, updateBranches_eq]

private theorem mpr_expand (rows : List α) (base : α → Option Bool)
    (ps : List (α → Option Bool)) (measure : α → Option Int) :
    multiPassRef rows base ps measure
      = List.map (fun p => multiPassBranch rows base p measure) ps := rfl

theorem sound : Goal := by
  intro α rows base ps measure
  rw [sps_expand, mpr_expand,
      foldl_sstep_comp measure ps
        (List.filter (fun r => isTrue3 (sqlAnd (base r) (disjPred ps r))) rows)
        (List.replicate ps.length (0, none)) (by simp),
      zip_replicate ((0, none) : Nat × Option Int) ps, List.map_map]
  refine List.map_congr_left (fun p hp => ?_)
  simpa using branch_eq rows base ps measure p hp

end TinylambOpt.ResearchProofs.CteSinglePassSplitFilter
