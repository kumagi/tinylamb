import TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

/-! Reusable facts about the three-valued evaluation model.  Every expression
in this domain reads finitely many slots, so all contracts can be decided by
splitting the finitely many slot values and simplifying.  The fault site is a
Nat carried unchanged through every operator; splitting on a slot gives it as
an arbitrary variable, so the analysis covers all sites. -/

@[simp] theorem beq_yes_yes : (V.yes == V.yes) = true := rfl
@[simp] theorem beq_no_no : (V.no == V.no) = true := rfl
@[simp] theorem beq_null_null : (V.null == V.null) = true := rfl
@[simp] theorem beq_yes_no : (V.yes == V.no) = false := rfl
@[simp] theorem beq_yes_null : (V.yes == V.null) = false := rfl
@[simp] theorem beq_yes_fault (s : Nat) : (V.yes == V.fault s) = false := rfl
@[simp] theorem beq_no_yes : (V.no == V.yes) = false := rfl
@[simp] theorem beq_no_null : (V.no == V.null) = false := rfl
@[simp] theorem beq_no_fault (s : Nat) : (V.no == V.fault s) = false := rfl
@[simp] theorem beq_null_yes : (V.null == V.yes) = false := rfl
@[simp] theorem beq_null_no : (V.null == V.no) = false := rfl
@[simp] theorem beq_null_fault (s : Nat) : (V.null == V.fault s) = false := rfl
@[simp] theorem beq_fault_yes (s : Nat) : (V.fault s == V.yes) = false := rfl
@[simp] theorem beq_fault_no (s : Nat) : (V.fault s == V.no) = false := rfl
@[simp] theorem beq_fault_null (s : Nat) : (V.fault s == V.null) = false := rfl
@[simp] theorem beq_fault_fault (s t : Nat) : (V.fault s == V.fault t) = (s == t) := rfl

@[simp] theorem flip_yes : TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA.flip V.yes = V.no := rfl
@[simp] theorem flip_no : TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA.flip V.no = V.yes := rfl
@[simp] theorem flip_null : TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA.flip V.null = V.null := rfl
@[simp] theorem flip_fault (s : Nat) : TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA.flip (V.fault s) = V.fault s := rfl

@[simp] theorem known_yes : known .yes = .yes := rfl
@[simp] theorem known_no : known .no = .no := rfl
@[simp] theorem known_null : known .null = .no := rfl
@[simp] theorem known_fault (s : Nat) : known (.fault s) = .fault s := rfl

@[simp] theorem accepted_yes : accepted .yes = .yes := rfl
@[simp] theorem accepted_no : accepted .no = .no := rfl
@[simp] theorem accepted_null : accepted .null = .no := rfl
@[simp] theorem accepted_fault (s : Nat) : accepted (.fault s) = .fault s := rfl

@[simp] theorem isNull_yes : isNull .yes = .no := rfl
@[simp] theorem isNull_no : isNull .no = .no := rfl
@[simp] theorem isNull_null : isNull .null = .yes := rfl
@[simp] theorem isNull_fault (s : Nat) : isNull (.fault s) = .fault s := rfl

@[simp] theorem nonfault_yes : nonfault .yes = true := rfl
@[simp] theorem nonfault_no : nonfault .no = true := rfl
@[simp] theorem nonfault_null : nonfault .null = true := rfl
@[simp] theorem nonfault_fault (s : Nat) : nonfault (.fault s) = false := rfl

/-! Determinism: the result of `run` is fixed by the values of the slots it
reads.  This is the engine behind `readSetGuard`. -/

theorem run_eq_of_trace_slots (env f : List V) (expr : E) :
    (∀ i ∈ (run env expr).2, slot env i = slot f i) → run f expr = run env expr := by
  induction expr generalizing env f with
  | arg i =>
      intro hs
      simpa [run, slot] using (hs i (by simp [run])).symm
  | lit v => simp [run]
  | neg e ih =>
      intro hs
      rcases he : run env e with ⟨v, r⟩
      have h' := ih env f (by simpa [run, he] using hs)
      rw [he] at h'
      simp [run, he, h']
  | isTrue e ih =>
      intro hs
      rcases he : run env e with ⟨v, r⟩
      have h' := ih env f (by simpa [run, he] using hs)
      rw [he] at h'
      simp [run, he, h']
  | missing e ih =>
      intro hs
      rcases he : run env e with ⟨v, r⟩
      have h' := ih env f (by simpa [run, he] using hs)
      rw [he] at h'
      simp [run, he, h']
  | branch p a b ihp iha ihb =>
      intro hs
      rcases hp : run env p with ⟨vp, rp⟩
      cases vp with
      | fault s =>
          have hsp : ∀ i ∈ rp, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, hp, hi])
          have hp' : run f p = (V.fault s, rp) := by
            simpa [hp] using ihp env f (by simpa [hp] using hsp)
          simp [run, after, hp, hp']
      | yes =>
          rcases ha : run env a with ⟨va, ra⟩
          have hsp : ∀ i ∈ rp, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, hp, hi, List.mem_append])
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, hp, ha, hi])
          have hp' : run f p = (V.yes, rp) := by
            simpa [hp] using ihp env f (by simpa [hp] using hsp)
          have ha' : run f a = (va, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          simp [run, after, hp, ha, hp', ha']
      | no =>
          rcases hb : run env b with ⟨vb, rb⟩
          have hsp : ∀ i ∈ rp, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, hp, hi, List.mem_append])
          have hsb : ∀ i ∈ rb, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, hp, hb, hi])
          have hp' : run f p = (V.no, rp) := by
            simpa [hp] using ihp env f (by simpa [hp] using hsp)
          have hb' : run f b = (vb, rb) := by
            simpa [hb] using ihb env f (by simpa [hb] using hsb)
          simp [run, after, hp, hb, hp', hb']
      | null =>
          rcases hb : run env b with ⟨vb, rb⟩
          have hsp : ∀ i ∈ rp, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, hp, hi, List.mem_append])
          have hsb : ∀ i ∈ rb, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, hp, hb, hi])
          have hp' : run f p = (V.null, rp) := by
            simpa [hp] using ihp env f (by simpa [hp] using hsp)
          have hb' : run f b = (vb, rb) := by
            simpa [hb] using ihb env f (by simpa [hb] using hsb)
          simp [run, after, hp, hb, hp', hb']
  | co a b iha ihb =>
      intro hs
      rcases ha : run env a with ⟨va, ra⟩
      cases va with
      | fault s =>
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi])
          have ha' : run f a = (V.fault s, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          simp [run, after, ha, ha']
      | null =>
          rcases hb : run env b with ⟨vb, rb⟩
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi, List.mem_append])
          have hsb : ∀ i ∈ rb, slot env i = slot f i := by
            intro i hi
            cases vb <;> exact hs i (by simp [run, after, ha, hb, hi])
          have ha' : run f a = (V.null, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          have hb' : run f b = (vb, rb) := by
            simpa [hb] using ihb env f (by simpa [hb] using hsb)
          simp [run, after, ha, hb, ha', hb']
      | yes =>
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi])
          have ha' : run f a = (V.yes, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          simp [run, after, ha, ha']
      | no =>
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi])
          have ha' : run f a = (V.no, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          simp [run, after, ha, ha']
  | conj a b iha ihb =>
      intro hs
      rcases ha : run env a with ⟨va, ra⟩
      cases va with
      | fault s =>
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi])
          have ha' : run f a = (V.fault s, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          simp [run, after, ha, ha']
      | no =>
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi])
          have ha' : run f a = (V.no, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          simp [run, after, ha, ha']
      | yes =>
          rcases hb : run env b with ⟨vb, rb⟩
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi, List.mem_append])
          have hsb : ∀ i ∈ rb, slot env i = slot f i := by
            intro i hi
            cases vb <;> exact hs i (by simp [run, after, ha, hb, hi])
          have ha' : run f a = (V.yes, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          have hb' : run f b = (vb, rb) := by
            simpa [hb] using ihb env f (by simpa [hb] using hsb)
          simp [run, after, ha, hb, ha', hb']
      | null =>
          rcases hb : run env b with ⟨vb, rb⟩
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi, List.mem_append])
          have hsb : ∀ i ∈ rb, slot env i = slot f i := by
            intro i hi
            cases vb <;> exact hs i (by simp [run, after, ha, hb, hi])
          have ha' : run f a = (V.null, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          have hb' : run f b = (vb, rb) := by
            simpa [hb] using ihb env f (by simpa [hb] using hsb)
          simp [run, after, ha, hb, ha', hb']
  | disj a b iha ihb =>
      intro hs
      rcases ha : run env a with ⟨va, ra⟩
      cases va with
      | fault s =>
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi])
          have ha' : run f a = (V.fault s, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          simp [run, after, ha, ha']
      | yes =>
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi])
          have ha' : run f a = (V.yes, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          simp [run, after, ha, ha']
      | no =>
          rcases hb : run env b with ⟨vb, rb⟩
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi, List.mem_append])
          have hsb : ∀ i ∈ rb, slot env i = slot f i := by
            intro i hi
            cases vb <;> exact hs i (by simp [run, after, ha, hb, hi])
          have ha' : run f a = (V.no, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          have hb' : run f b = (vb, rb) := by
            simpa [hb] using ihb env f (by simpa [hb] using hsb)
          simp [run, after, ha, hb, ha', hb']
      | null =>
          rcases hb : run env b with ⟨vb, rb⟩
          have hsa : ∀ i ∈ ra, slot env i = slot f i := by
            intro i hi
            exact hs i (by simp [run, after, ha, hi, List.mem_append])
          have hsb : ∀ i ∈ rb, slot env i = slot f i := by
            intro i hi
            cases vb <;> exact hs i (by simp [run, after, ha, hb, hi])
          have ha' : run f a = (V.null, ra) := by
            simpa [ha] using iha env f (by simpa [ha] using hsa)
          have hb' : run f b = (vb, rb) := by
            simpa [hb] using ihb env f (by simpa [hb] using hsb)
          simp [run, after, ha, hb, ha', hb']

/-! General contract implications. -/

theorem projection_imp_whereMask (r : Rewrite) (hp : projection r) : whereMask r := by
  intro e hg
  rw [hp e hg]

theorem readSetGuard_of_projection (r : Rewrite) (hp : projection r) : readSetGuard r := by
  intro e f hg hslots
  have hsrc := run_eq_of_trace_slots e f r.src (by
    intro i hi
    exact hslots i (by simp [hi]))
  have hdst := run_eq_of_trace_slots e f r.dst (by
    intro i hi
    exact hslots i (by simp [hi]))
  rw [hdst, hsrc]
  exact hp e hg

theorem truthDominates_of_source_yes (r : Rewrite)
    (hyes : ∀ e, (run e r.src).1 = .yes → (run e r.dst).1 = .yes) :
    truthDominates r := by
  intro e he
  have hd := hyes e he
  simp [run, after, hd]

def findFailFrom (k : List V → Trace) (es : List (List V)) (n : Nat) : Option (Nat × Nat) :=
  (es.zipIdx n).findSome? (fun (e, i) => match (k e).1 with | .fault s => some (i, s) | _ => none)

theorem findFailFrom_eq_of (r : Rewrite)
    (h : ∀ e s, (run e r.src).1 = .fault s ↔ (dispatch r e).1 = .fault s) :
    ∀ es n, findFailFrom (dispatch r) es n = findFailFrom (fun e => run e r.src) es n := by
  intro es
  induction es with
  | nil => intro n; simp [findFailFrom]
  | cons e es ih =>
      intro n
      have hpoint : ∀ s, (run e r.src).1 = .fault s ↔ (dispatch r e).1 = .fault s := h e
      rcases hv : (run e r.src).1 with _ | _ | _ | s
      · have hno : ∀ t, (dispatch r e).1 ≠ .fault t := by
          intro t ht
          have : (run e r.src).1 = .fault t := (hpoint t).mpr ht
          simp [hv] at this
        rcases hdv : (dispatch r e).1 with _ | _ | _ | t
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · exact (hno t hdv).elim
      · have hno : ∀ t, (dispatch r e).1 ≠ .fault t := by
          intro t ht
          have : (run e r.src).1 = .fault t := (hpoint t).mpr ht
          simp [hv] at this
        rcases hdv : (dispatch r e).1 with _ | _ | _ | t
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · exact (hno t hdv).elim
      · have hno : ∀ t, (dispatch r e).1 ≠ .fault t := by
          intro t ht
          have : (run e r.src).1 = .fault t := (hpoint t).mpr ht
          simp [hv] at this
        rcases hdv : (dispatch r e).1 with _ | _ | _ | t
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · simpa [findFailFrom, hv, hdv] using ih (n + 1)
        · exact (hno t hdv).elim
      · have hvd : (dispatch r e).1 = .fault s := (hpoint s).mp hv
        simp [findFailFrom, hv, hvd]

theorem faultOrder_of_pointwise (r : Rewrite)
    (h : ∀ e s, (run e r.src).1 = .fault s ↔ (dispatch r e).1 = .fault s) :
    faultOrder r := by
  intro es
  change findFailFrom (dispatch r) es 0 = findFailFrom (fun e => run e r.src) es 0
  exact findFailFrom_eq_of r h es 0

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common
