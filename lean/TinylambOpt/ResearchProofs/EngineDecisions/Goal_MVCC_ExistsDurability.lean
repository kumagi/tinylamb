import TinylambOpt.ResearchGoals.EngineDecisions.MVCC

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Named results include `sound`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.MVCC_ExistsDurability

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.ExistsDurability

private theorem obs_cons (live : List Nat) (b : Batch) (bs : List Batch) :
    observation live (b :: bs) = observation (applyBatch live b) bs := by
  unfold observation
  simp [List.foldl_cons]

private theorem obs_nil (live : List Nat) : observation live [] = !live.isEmpty := by
  unfold observation
  simp

private theorem filter_head : ∀ (b : Batch) (bs : List Batch) (d : Nat),
    (∀ c ∈ bs, b.walEnd < c.walEnd) →
    ((b :: bs).filter fun x => x.walEnd ≤ d) =
      if b.walEnd ≤ d then b :: bs.filter (fun x => x.walEnd ≤ d) else [] := by
  intro b bs d h
  by_cases hb : b.walEnd ≤ d
  · rw [List.filter_cons_of_pos (p := (fun x : Batch => x.walEnd ≤ d)) (by simpa using hb),
      ite_eq_left hb]
  · have hnil : bs.filter (fun x => x.walEnd ≤ d) = [] := by
      refine List.filter_eq_nil_iff.mpr ?_
      intro c hc hcd
      have h2 := h c hc
      have hc2 : c.walEnd ≤ d := by simpa using hcd
      omega
    rw [List.filter_cons_of_neg (p := (fun x : Batch => x.walEnd ≤ d)) (by simpa using hb),
      hnil, ite_eq_right hb]

private theorem lastFlip_ge : ∀ (log : List Batch),
    log.Pairwise (fun a b => a.walEnd < b.walEnd) →
    ∀ (live : List Nat) (f : Nat), (∀ b ∈ log, f ≤ b.walEnd) → f ≤ lastFlip live f log := by
  intro log
  induction log with
  | nil => intro _ live f _; simp [lastFlip]
  | cons b bs ih =>
      intro hpos live f hf
      have hfor : ∀ c ∈ bs, b.walEnd < c.walEnd := by
        cases hpos with
        | cons h _ => exact h
      have hbs : bs.Pairwise (fun a c => a.walEnd < c.walEnd) := List.Pairwise.tail hpos
      have hfb : ∀ c ∈ bs, f ≤ c.walEnd := by
        intro c hc
        exact hf c (by simp [hc])
      by_cases hflip : (applyBatch live b).isEmpty == live.isEmpty
      · have this := ih hbs (applyBatch live b) f hfb
        have hgoal : lastFlip live f (b :: bs) = lastFlip (applyBatch live b) f bs := by
          simp [lastFlip, hflip]
        rw [hgoal]
        exact this
      · have hf' : f ≤ b.walEnd := hf b (by simp)
        have hbs' := ih hbs (applyBatch live b) b.walEnd
          (by intro c hc; have := hfor c hc; omega)
        have hgoal : lastFlip live f (b :: bs) = lastFlip (applyBatch live b) b.walEnd bs := by
          simp [lastFlip, hflip]
        rw [hgoal]
        exact Nat.le_trans hf' hbs'

private theorem bool_eq_of_beq {b c : Bool} (h : (b == c)) : b = c := by
  cases b <;> cases c <;> simp_all

private theorem bool_ne_of_not_beq {b c : Bool} (h : ¬ (b == c)) : b ≠ c := by
  intro hc
  cases b <;> cases c <;> simp_all

/-- Parametric form: with an arbitrary starting fence `f ≤ X`, the running
last-flip fence stays `≤ X` exactly when every WAL prefix taken at durability
`≥ X` reproduces the final observation. -/
private theorem main : ∀ (log : List Batch),
    (∀ b ∈ log, 0 < b.walEnd) →
    log.Pairwise (fun a b => a.walEnd < b.walEnd) →
    ∀ (live : List Nat) (f X : Nat), f ≤ X →
      (lastFlip live f log ≤ X ↔
        ∀ durable, X ≤ durable →
          observation live (log.filter fun b => b.walEnd ≤ durable)
            = observation live log) := by
  intro log
  induction log with
  | nil =>
      intro _ _ live f X hf
      constructor
      · intro _ _ _; rfl
      · intro _; simp [lastFlip]; assumption
  | cons b bs ih =>
      intro hp hpos live f X hf
      have hbs : bs.Pairwise (fun a c => a.walEnd < c.walEnd) := List.Pairwise.tail hpos
      cases hpos with
      | cons hfor htail =>
          have hpbs : ∀ c ∈ bs, 0 < c.walEnd := by
            intro c hc
            exact hp c (by simp [hc])
          have hge : b.walEnd ≤ lastFlip (applyBatch live b) b.walEnd bs :=
            lastFlip_ge bs hbs (applyBatch live b) b.walEnd
              (by intro c hc; have := hfor c hc; omega)
          have hstep : lastFlip live f (b :: bs)
              = lastFlip (applyBatch live b)
                  (if (applyBatch live b).isEmpty == live.isEmpty then f else b.walEnd) bs := by
            simp [lastFlip]
          have hnil_of_lt : ∀ (y : Nat), y < b.walEnd →
              bs.filter (fun x => x.walEnd ≤ y) = [] := by
            intro y hy
            rw [List.filter_eq_nil_iff]
            intro c hc hc'
            have := hfor c hc
            have hc2 : c.walEnd ≤ y := by simpa using hc'
            omega
          constructor
          · -- forward: fence below X forces every prefix taken at ≥ X to agree
            intro hle durable hd
            rw [filter_head b bs durable hfor]
            by_cases hbd : b.walEnd ≤ durable
            · rw [ite_eq_left hbd, obs_cons]
              by_cases hflip : (applyBatch live b).isEmpty == live.isEmpty
              · have h1 : lastFlip (applyBatch live b) f bs ≤ X := by
                  rw [hstep] at hle
                  simp [hflip] at hle
                  exact hle
                exact (ih hpbs hbs (applyBatch live b) f X hf).mp h1 durable hd
              · have h1 : lastFlip (applyBatch live b) b.walEnd bs ≤ X := by
                  rw [hstep] at hle
                  simp [hflip] at hle
                  exact hle
                have hbx : b.walEnd ≤ X := Nat.le_trans hge h1
                exact (ih hpbs hbs (applyBatch live b) b.walEnd X hbx).mp h1 durable hd
            · -- durable < b.walEnd: the filter is empty
              rw [ite_eq_right hbd, obs_nil, obs_cons]
              by_cases hflip : (applyBatch live b).isEmpty == live.isEmpty
              · -- no flip: the empty tail prefix reproduces the same observation
                have h1 : lastFlip (applyBatch live b) f bs ≤ X := by
                  rw [hstep] at hle
                  simp [hflip] at hle
                  exact hle
                have hres := (ih hpbs hbs (applyBatch live b) f X hf).mp h1 X (Nat.le_refl X)
                rw [← hres, hnil_of_lt X (by omega), obs_nil, bool_eq_of_beq hflip]
              · -- flip at b.walEnd, but durable < b.walEnd ≤ X ≤ durable: absurd
                have h1 : lastFlip (applyBatch live b) b.walEnd bs ≤ X := by
                  rw [hstep] at hle
                  simp [hflip] at hle
                  exact hle
                have hbx : b.walEnd ≤ X := Nat.le_trans hge h1
                exfalso
                have hbdt : ¬ (b.walEnd ≤ durable) := hbd
                omega
          · -- backward
            intro hlen
            by_cases hflip : (applyBatch live b).isEmpty == live.isEmpty
            · -- no flip: recurse with the unchanged fence
              have heq : (applyBatch live b).isEmpty = live.isEmpty := bool_eq_of_beq hflip
              rw [hstep]
              simp [hflip]
              refine (ih hpbs hbs (applyBatch live b) f X hf).mpr ?_
              intro durable hd
              by_cases hbd : b.walEnd ≤ durable
              · have := hlen durable hd
                rw [filter_head b bs durable hfor, ite_eq_left hbd, obs_cons, obs_cons] at this
                exact this
              · have h1 := hlen durable hd
                rw [filter_head b bs durable hfor, ite_eq_right hbd, obs_nil, obs_cons] at h1
                rw [hnil_of_lt durable (by omega), obs_nil, heq]
                exact h1
            · -- flip at b.walEnd
              have hne : (applyBatch live b).isEmpty ≠ live.isEmpty := bool_ne_of_not_beq hflip
              rw [hstep]
              simp [hflip]
              by_cases hbx : b.walEnd ≤ X
              · refine (ih hpbs hbs (applyBatch live b) b.walEnd X hbx).mpr ?_
                intro durable hd
                have hbd : b.walEnd ≤ durable := by omega
                have := hlen durable hd
                rw [filter_head b bs durable hfor, ite_eq_left hbd, obs_cons, obs_cons] at this
                exact this
              · -- X < b.walEnd: agreement at X and at b.walEnd contradicts the flip
                exfalso
                have h1 := hlen X (Nat.le_refl X)
                rw [filter_head b bs X hfor, ite_eq_right hbx, obs_nil, obs_cons] at h1
                have hbnil : bs.filter (fun x => x.walEnd ≤ b.walEnd) = [] := by
                  rw [List.filter_eq_nil_iff]
                  intro c hc hc'
                  have := hfor c hc
                  have hc2 : c.walEnd ≤ b.walEnd := by simpa using hc'
                  omega
                have h2 := hlen b.walEnd (by omega)
                rw [filter_head b bs b.walEnd hfor, ite_eq_left (Nat.le_refl (b.walEnd)),
                  hbnil, obs_cons, obs_nil, obs_cons] at h2
                have h12 : (!live.isEmpty) = (!(applyBatch live b).isEmpty) := by rw [h1, ← h2]
                cases hA : live.isEmpty <;> cases hB : (applyBatch live b).isEmpty <;> simp_all

theorem sound : Goal := by
  intro initial log fence hpos hp
  exact main log hpos hp initial 0 fence (Nat.zero_le fence)

end TinylambOpt.ResearchProofs.EngineDecisions.MVCC_ExistsDurability

-- C++ references to this model or its proof.
-- transaction/mvcc_decisions.hpp
