import Std.Tactic

/-!
Walk children of `r` on lane `l`, collecting the pairs it actually evaluates (in source
order). `cells` is the list of cells of those already evaluated children; it is the
argument to `needNext`.

Reference: sequential evaluation of one lane.

Named results include `childWalk_subset`, `childWalk_congr`, `rootEvalW_congr`.
-/

namespace TinylambOpt.Research.RuntimePredicateDag

/-
# Batched short-circuit DAG with lane-wise demand (issue `runtime-predicate-dag`)

A batch has lanes (rows).  A small deterministic DAG has *atoms* (leaf scalar
nodes with an intrinsic result per lane) and *parents* (roots).  Each parent
owns an ordered child list (its short-circuit order).  A cell is one of
unevaluated / NULL / value / error.  Determinism is structural: `atom` and
`result` are plain functions, so evaluating a `(node, lane)` twice gives the
same cell.

**Reference** (`seqLane`): walk lanes in order; inside a lane walk roots in
order, and inside a root walk children left to right using `needNext` on the
cells evaluated so far.  The first error in short-circuit order is reported
per lane (`firstErrIn`); evaluation continues to the remaining roots so every
output column is produced.  This mirrors `expression/bytecode.cpp`.

**Optimized** (`batchedEvents`): first compute the *union of demanded lanes*
per atom (`atomLanes`); then execute each `(atom, lane)` pair of that union
once (a batched / SIMD kernel).  `optLane` re-runs the same short-circuit walk
but reads cells from the event table (`lookupCell`).

Explicit sufficient hypotheses: `rows.Nodup`, `atoms.Nodup`, no duplicate
children/roots (`WellFormed`), and every child belongs to `atoms` (`Covered`).

`runtime_predicate_dag_sound`: optimized reconstruction equals the sequential
result for every lane in `rows`, and no `(atom, lane)` pair is executed twice.
`batchedSteps` is an explicit step-count evaluator bounded by
`atoms.length * rows.length`.

Regression (`naive_all_lanes_is_unsound`): parent 0 avoids the shared division
via a leading FALSE; parent 1 demands it only on lane 1.  Division errors on
lanes 0 and 1.  The correct first error is lane 1; evaluating the shared atom
on every lane reports lane 0.
-/

abbrev Node := Nat
abbrev Lane := Nat

inductive Err where
  | divZero
  | overflow
  deriving DecidableEq, Repr

inductive Cell where
  | unevaluated
  | nullv
  | value : Int → Cell
  | error : Err → Cell
  deriving DecidableEq, Repr

structure Spec where
  atom : Node → Lane → Cell
  children : Node → List Node
  needNext : Node → Lane → List Cell → Bool
  result : Node → Lane → List Cell → Cell
  roots : List Node

def firstErrIn : List (Node × Cell) → Option (Node × Err)
  | [] => none
  | (a, .error e) :: _ => some (a, e)
  | _ :: xs => firstErrIn xs

/-! ### Generic short-circuit walk -/

/-- Walk children of `r` on lane `l`, collecting the pairs it actually
    evaluates (in source order).  `cells` is the list of cells of those
    already evaluated children; it is the argument to `needNext`. -/
def childWalk (cellOf : Node → Cell) (needNext : Node → Lane → List Cell → Bool)
    (r l : Nat) : List Node → List Cell → List (Node × Cell)
  | [], _ => []
  | c :: cs, cells =>
      if needNext r l cells then
        (c, cellOf c) :: childWalk cellOf needNext r l cs (cells ++ [cellOf c])
      else []

def rootEvalW (cellOf : Node → Cell) (s : Spec) (l : Nat) : List Node →
    List (Node × Cell) × List (Option Cell) × Option (Node × Err)
  | [] => ([], [], none)
  | r :: rest =>
      let wev := childWalk cellOf s.needNext r l (s.children r) []
      let tail := rootEvalW cellOf s l rest
      (wev ++ tail.1,
       some (s.result r l (wev.map Prod.snd)) :: tail.2.1,
       firstErrIn wev <|> tail.2.2)

/-- Reference: sequential evaluation of one lane. -/
def seqLane (s : Spec) (l : Lane) := rootEvalW (fun a => s.atom a l) s l s.roots
def seqEvents (s : Spec) (l : Lane) : List (Node × Cell) := (seqLane s l).1
def seqOutputs (s : Spec) (l : Lane) : List (Option Cell) := (seqLane s l).2.1
def seqError (s : Spec) (l : Lane) : Option (Node × Err) := (seqLane s l).2.2

def demandedAtomsLane (s : Spec) (l : Lane) : List Node := (seqEvents s l).map Prod.fst

/-! ### Batched evaluator and event-count evaluator -/

def atomLanes (s : Spec) (rows : List Lane) (a : Node) : List Lane :=
  rows.filter (fun l => decide (a ∈ demandedAtomsLane s l))

def batchedEvents (s : Spec) (rows : List Lane) (atoms : List Node) : List (Node × Lane × Cell) :=
  atoms.flatMap (fun a => (atomLanes s rows a).map (fun l => (a, l, s.atom a l)))

/-- Explicit step counter: one event per executed `(atom, lane)` pair. -/
def batchedSteps (s : Spec) (rows : List Lane) (atoms : List Node) : Nat :=
  (batchedEvents s rows atoms).length

def executedPairs (s : Spec) (rows : List Lane) (atoms : List Node) : List (Node × Lane) :=
  atoms.flatMap (fun a => (atomLanes s rows a).map (fun l => (a, l)))

def eventsForLane (s : Spec) (rows : List Lane) (atoms : List Node) (l : Lane) : List (Node × Cell) :=
  ((batchedEvents s rows atoms).filter (fun e => e.2.1 == l)).map (fun e => (e.1, e.2.2))

def lookupCell : List (Node × Cell) → Node → Cell
  | [], _ => Cell.unevaluated
  | (a, c) :: xs, b => if a == b then c else lookupCell xs b

/-- Optimized lane reconstruction: same walk, cell from the event table. -/
def optLane (s : Spec) (rows : List Lane) (atoms : List Node) (l : Lane) :=
  rootEvalW (lookupCell (eventsForLane s rows atoms l)) s l s.roots

def Covered (s : Spec) (atoms : List Node) : Prop :=
  ∀ r, r ∈ s.roots → ∀ c, c ∈ s.children r → c ∈ atoms

def WellFormed (s : Spec) : Prop := s.roots.Nodup ∧ ∀ r, (s.children r).Nodup

/-! ### Walk lemmas -/

theorem childWalk_subset (cellOf : Node → Cell)
    (needNext : Node → Lane → List Cell → Bool) (r l : Nat) :
    ∀ (cs : List Node) (cells : List Cell) (e : Node × Cell),
      e ∈ childWalk cellOf needNext r l cs cells → e.1 ∈ cs := by
  intro cs
  induction cs with
  | nil => intro cells e he; simp [childWalk] at he
  | cons c cs ih =>
      intro cells e he
      simp only [childWalk] at he
      by_cases hn : needNext r l cells
      · simp [hn] at he
        rcases he with heq | he
        · have : e.1 = c := by simpa using congrArg Prod.fst heq
          simp only [List.mem_cons]
          exact Or.inl this
        · simp only [List.mem_cons]
          exact Or.inr (ih (cells ++ [cellOf c]) e he)
      · simp [hn] at he

/-- Two cell lookups that agree on the atoms actually evaluated by the first
    walk produce identical walks. -/
theorem childWalk_congr (cell₁ cell₂ : Node → Cell)
    (needNext : Node → Lane → List Cell → Bool) (r l : Nat) :
    ∀ (cs : List Node) (cells : List Cell),
      (∀ a ∈ (childWalk cell₁ needNext r l cs cells).map Prod.fst, cell₁ a = cell₂ a) →
      childWalk cell₁ needNext r l cs cells = childWalk cell₂ needNext r l cs cells := by
  intro cs
  induction cs with
  | nil => intro cells h; rfl
  | cons c cs ih =>
      intro cells h
      simp only [childWalk]
      by_cases hn : needNext r l cells = true
      · have hc : cell₁ c = cell₂ c := by
          apply h
          exact List.mem_map.mpr ⟨(c, cell₁ c), by simp [childWalk, hn], rfl⟩
        rw [hc]
        simp [hn]
        apply ih
        intro a ha
        apply h
        rcases List.mem_map.mp ha with ⟨x, hx, hxa⟩
        refine List.mem_map.mpr ⟨x, ?_, hxa⟩
        simp only [childWalk, hn, ↓reduceIte, List.mem_cons]
        exact Or.inr (by simpa [hc] using hx)
      · simp [hn]

theorem rootEvalW_congr (cell₁ cell₂ : Node → Cell) (s : Spec) (l : Nat) :
    ∀ (rs : List Node),
      (∀ a ∈ (rootEvalW cell₁ s l rs).1.map Prod.fst, cell₁ a = cell₂ a) →
      rootEvalW cell₁ s l rs = rootEvalW cell₂ s l rs := by
  intro rs
  induction rs with
  | nil => intro h; rfl
  | cons r rs ih =>
      intro h
      have hw : childWalk cell₁ s.needNext r l (s.children r) [] =
          childWalk cell₂ s.needNext r l (s.children r) [] := by
        apply childWalk_congr
        intro a ha
        apply h
        simp [rootEvalW, ha]
      have htail : ∀ a ∈ (rootEvalW cell₁ s l rs).1.map Prod.fst, cell₁ a = cell₂ a := by
        intro a ha
        apply h
        simp [rootEvalW, ha]
      have hi := ih htail
      simp only [rootEvalW]
      rw [hw, hi]

theorem rootEvalW_event_parent (s : Spec) (l : Nat) :
    ∀ (rs : List Node) (e : Node × Cell),
      e ∈ (rootEvalW (fun a => s.atom a l) s l rs).1 →
        ∃ r ∈ rs, e.1 ∈ s.children r := by
  intro rs
  induction rs with
  | nil =>
      intro e he
      simp [rootEvalW] at he
  | cons r rs ih =>
      intro e he
      have heApp : e ∈ (childWalk (fun a => s.atom a l) s.needNext r l (s.children r) [] ++
          (rootEvalW (fun a => s.atom a l) s l rs).1) := by
        simpa only [rootEvalW] using he
      rcases List.mem_append.mp heApp with hew | hetail
      · have hc := childWalk_subset (fun a => s.atom a l) s.needNext r l (s.children r) [] e hew
        exact ⟨r, by simp, hc⟩
      · rcases ih e hetail with ⟨r', hr', hc⟩
        exact ⟨r', by simp [hr'], hc⟩

/-! ### Event-table lookup -/

theorem lookupCell_eq_of_mem (pairs : List (Node × Cell)) (a : Node) (c : Cell)
    (hall : ∀ p ∈ pairs, p.1 = a → p.2 = c) (hex : (a, c) ∈ pairs) :
    lookupCell pairs a = c := by
  induction pairs with
  | nil => simpa using hex
  | cons p ps ih =>
      simp only [lookupCell]
      by_cases hp : p.1 == a
      · have hpa : p.1 = a := by simpa using hp
        have hpc : p.2 = c := hall p (by simp) hpa
        simp [hp, hpc]
      · have hpna : p.1 ≠ a := by simpa using hp
        have hps : (a, c) ∈ ps := by
          rcases List.mem_cons.mp hex with hhead | htail
          · exfalso
            apply hpna
            exact (congrArg Prod.fst hhead).symm
          · exact htail
        have hall' : ∀ q ∈ ps, q.1 = a → q.2 = c := by
          intro q hq hqa
          exact hall q (by simp [hq]) hqa
        simp [hp, ih hall' hps]

theorem mem_batchedEvents {s : Spec} {rows : List Lane} {atoms : List Node}
    {a : Node} {l : Lane} (ha : a ∈ atoms) (hl : l ∈ rows)
    (hd : a ∈ demandedAtomsLane s l) :
    (a, l, s.atom a l) ∈ batchedEvents s rows atoms := by
  simp only [batchedEvents, List.mem_flatMap]
  refine ⟨a, ha, ?_⟩
  simp only [List.mem_map]
  refine ⟨l, ?_, rfl⟩
  simp only [atomLanes, List.mem_filter]
  exact ⟨hl, decide_eq_true hd⟩

theorem eventsForLane_lookup (s : Spec) (rows : List Lane) (atoms : List Node)
    (hc : Covered s atoms) {a : Node} {l : Lane} (hl : l ∈ rows)
    (hd : a ∈ demandedAtomsLane s l) :
    lookupCell (eventsForLane s rows atoms l) a = s.atom a l := by
  have hparent : ∃ r ∈ s.roots, a ∈ s.children r := by
    unfold demandedAtomsLane seqEvents seqLane at hd
    rcases List.mem_map.mp hd with ⟨e, he, heq⟩
    have heq' : a = e.1 := heq.symm
    subst a
    exact rootEvalW_event_parent s l s.roots e he
  rcases hparent with ⟨r, hr, hcild⟩
  have ha : a ∈ atoms := hc r hr a hcild
  apply lookupCell_eq_of_mem
  · intro p hp hp1
    have hp' : p ∈ ((batchedEvents s rows atoms).filter (fun e => e.2.1 == l)).map
        (fun e => (e.1, e.2.2)) := hp
    rcases List.mem_map.mp hp' with ⟨e, hefilt, heq⟩
    have hebat : e ∈ batchedEvents s rows atoms := (List.mem_filter.mp hefilt).1
    have hcell : e.2.2 = s.atom a l := by
      simp only [batchedEvents, List.mem_flatMap] at hebat
      rcases hebat with ⟨a', ha', hem⟩
      simp only [List.mem_map] at hem
      rcases hem with ⟨l', hl', rfl⟩
      have hpa' : p.1 = a' := by
        have := congrArg Prod.fst heq.symm
        simpa using this
      have haa : a' = a := hpa'.symm.trans hp1
      subst a'
      have hl'eq : l' = l := by
        have hb := (List.mem_filter.mp hefilt).2
        exact beq_iff_eq.mp hb
      subst l'
      rw [haa]
    have hpeq : p = (e.1, e.2.2) := heq.symm
    rw [hpeq]
    have hpa : e.1 = a := (congrArg Prod.fst heq).trans hp1
    rw [hpa, hcell]
  · have hbat := mem_batchedEvents ha hl hd
    apply List.mem_map.mpr
    refine ⟨(a, l, s.atom a l), ?_, rfl⟩
    apply List.mem_filter.mpr
    exact ⟨hbat, by simp⟩

/-! ### Main theorem -/

theorem runtime_predicate_dag_sound (s : Spec) (rows : List Lane) (atoms : List Node)
    (_hrows : rows.Nodup) (_hatoms : atoms.Nodup) (_hwf : WellFormed s)
    (hc : Covered s atoms) (l : Lane) (hl : l ∈ rows) :
    optLane s rows atoms l = seqLane s l ∧
    (∀ e₁ ∈ batchedEvents s rows atoms, ∀ e₂ ∈ batchedEvents s rows atoms,
       e₁.1 = e₂.1 → e₁.2.1 = e₂.2.1 → e₁ = e₂) := by
  constructor
  · unfold optLane seqLane
    symm
    apply rootEvalW_congr
    intro a ha
    have hd : a ∈ demandedAtomsLane s l := by
      simpa [demandedAtomsLane, seqEvents, seqLane] using ha
    exact (eventsForLane_lookup s rows atoms hc hl hd).symm
  · intro e₁ he₁ e₂ he₂ hnode hlane
    simp only [batchedEvents, List.mem_flatMap] at he₁ he₂
    rcases he₁ with ⟨a₁, ha₁, he₁m⟩
    rcases he₂ with ⟨a₂, ha₂, he₂m⟩
    rcases List.mem_map.mp he₁m with ⟨l₁, hl₁, rfl⟩
    rcases List.mem_map.mp he₂m with ⟨l₂, hl₂, rfl⟩
    simp only at hnode hlane
    subst a₂
    subst l₂
    rfl

/-! ### Step-count bound and at-most-once counting -/

theorem filter_nodup {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) : (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : x ∉ xs := (List.nodup_cons.mp h).1
      have hxs : xs.Nodup := (List.nodup_cons.mp h).2
      simp only [List.filter_cons]
      by_cases hp : p x
      · simp [hp, List.nodup_cons]
        exact ⟨fun hmem => hx hmem, ih hxs⟩
      · simp [hp]
        exact ih hxs

theorem atomLanes_nodup (s : Spec) (rows : List Lane) (a : Node) (hrows : rows.Nodup) :
    (atomLanes s rows a).Nodup := by
  unfold atomLanes
  exact filter_nodup (fun l => decide (a ∈ demandedAtomsLane s l)) hrows

theorem executedPairs_nodup (s : Spec) (rows : List Lane) (atoms : List Node)
    (hrows : rows.Nodup) (hatoms : atoms.Nodup) :
    (executedPairs s rows atoms).Nodup := by
  induction atoms with
  | nil => simp [executedPairs]
  | cons a as ih =>
      have ha_not : a ∉ as := (List.nodup_cons.mp hatoms).1
      have has : as.Nodup := (List.nodup_cons.mp hatoms).2
      simp only [executedPairs, List.flatMap_cons]
      apply List.nodup_append.mpr
      refine ⟨?_, ?_, ?_⟩
      · exact List.Pairwise.map (fun l : Lane => (a, l)) (by
          intro x y hne heq
          exact hne (congrArg Prod.snd heq)) (atomLanes_nodup s rows a hrows)
      · simpa [executedPairs] using ih has
      · intro e he₁ e' he₂
        rcases List.mem_map.mp he₁ with ⟨l₁, hl₁, rfl⟩
        rcases List.mem_flatMap.mp he₂ with ⟨a₂, ha₂, he₂m⟩
        rcases List.mem_map.mp he₂m with ⟨l₂, hl₂, rfl⟩
        have hne : a₂ ≠ a := by
          intro h
          subst a₂
          exact ha_not ha₂
        intro heq
        exact hne (congrArg Prod.fst heq).symm

theorem batchedSteps_le (s : Spec) (rows : List Lane) (atoms : List Node) :
    batchedSteps s rows atoms ≤ atoms.length * rows.length := by
  change (atoms.flatMap (fun a => (atomLanes s rows a).map (fun l => (a, l, s.atom a l)))).length ≤
    atoms.length * rows.length
  rw [List.length_flatMap]
  induction atoms with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have hfl : ((atomLanes s rows a).map (fun l => (a, l, s.atom a l))).length ≤ rows.length := by
        rw [List.length_map]
        unfold atomLanes
        exact List.length_filter_le (fun l => decide (a ∈ demandedAtomsLane s l)) rows
      have hi : (List.map (fun a => ((atomLanes s rows a).map (fun l => (a, l, s.atom a l))).length) as).sum ≤
          as.length * rows.length := ih
      simp only [List.length_cons, Nat.succ_mul]
      omega

theorem nodup_count_le_one {α : Type} [DecidableEq α] (l : List α) (c : α)
    (hnd : l.Nodup) (hall : ∀ x ∈ l, x = c) : l.length ≤ 1 := by
  cases l with
  | nil => simp
  | cons x xs =>
      have hx : x = c := hall x (by simp)
      have hxsnd : xs.Nodup := (List.nodup_cons.mp hnd).2
      have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
      have hxsNil : xs = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro y hy
        have hyc : y = c := hall y (by simp [hy])
        apply hxnot
        rw [hx, ← hyc]
        exact hy
      simp [hxsNil]

theorem pair_executed_at_most_once (s : Spec) (rows : List Lane) (atoms : List Node)
    (hrows : rows.Nodup) (hatoms : atoms.Nodup) (a : Node) (l : Lane) :
    (executedPairs s rows atoms).countP (fun p => decide (p.1 = a ∧ p.2 = l)) ≤ 1 := by
  rw [List.countP_eq_length_filter]
  apply nodup_count_le_one _ (a, l)
  · exact (executedPairs_nodup s rows atoms hrows hatoms).filter _
  · intro p hp
    have hprop : p.1 = a ∧ p.2 = l := by
      have hb := (List.mem_filter.mp hp).2
      simpa using hb
    exact Prod.ext hprop.1 hprop.2

/-! ### Regression: the "all lanes" lowering changes the first error -/

def divNode : Node := 0
def falseNode : Node := 1
def root0 : Node := 2
def root1 : Node := 3

def regressionSpec : Spec where
  atom a l :=
    if a = divNode then if l = 0 ∨ l = 1 then .error .divZero else .value 1
    else if a = falseNode then .value 0
    else .value 1
  children r :=
    if r = root0 then [falseNode, divNode]
    else if r = root1 then [divNode]
    else []
  needNext r l cells :=
    if r = root0 then
      match cells with
      | [] => true
      | .value 0 :: _ => false
      | _ => true
    else l == 1
  result _ _ cells := match cells with | [] => .nullv | c :: _ => c
  roots := [root0, root1]

/-- Naive lowering: evaluate every atom on EVERY row, ignoring demand. -/
def naiveEvents (s : Spec) (rows : List Lane) (atoms : List Node) : List (Node × Lane × Cell) :=
  atoms.flatMap (fun a => rows.map (fun l => (a, l, s.atom a l)))

/-- First error by lane order (the C++ ballot reports the smallest lane). -/
def firstErrorLane : List (Node × Lane × Cell) → Option (Lane × Node × Err)
  | [] => none
  | (a, l, .error e) :: xs =>
      match firstErrorLane xs with
      | none => some (l, a, e)
      | some (l', a', e') => if l ≤ l' then some (l, a, e) else some (l', a', e')
  | _ :: xs => firstErrorLane xs

def seqAllEvents (s : Spec) (rows : List Lane) : List (Node × Lane × Cell) :=
  rows.flatMap (fun l => (seqEvents s l).map (fun p => (p.1, l, p.2)))

theorem regression_demand_excludes_lane0 :
    atomLanes regressionSpec [0, 1] divNode = [1] := by
  decide

theorem naive_all_lanes_is_unsound :
    firstErrorLane (naiveEvents regressionSpec [0, 1] [divNode]) =
        some (0, divNode, Err.divZero) ∧
    firstErrorLane (seqAllEvents regressionSpec [0, 1]) =
        some (1, divNode, Err.divZero) ∧
    atomLanes regressionSpec [0, 1] divNode ≠ [0, 1] := by
  decide

/-- The shared node is evaluated once per demanded lane, not once per row. -/
theorem regression_batched_steps :
    batchedSteps regressionSpec [0, 1] [divNode, falseNode] = 3 := by
  decide

end TinylambOpt.Research.RuntimePredicateDag
