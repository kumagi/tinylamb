import TinylambOpt.ResearchGoals.PlanCacheToggleInvalidation

namespace TinylambOpt.ResearchProofs.PlanCacheToggleInvalidation

open TinylambOpt.ResearchGoals.PlanCacheToggleInvalidation

private abbrev CKey := Nat × OptCfg

-- ---------------------------------------------------------------------------
-- Basic lookup facts
-- ---------------------------------------------------------------------------

private theorem lookupPlan_eq_some_mem (k : CKey) (cache : List (CKey × Nat)) (p : Nat)
    (h : lookupPlan k cache = some p) : (k, p) ∈ cache := by
  induction cache with
  | nil => simp [lookupPlan] at h
  | cons e rest ih =>
      rcases e with ⟨k', p'⟩
      simp [lookupPlan] at h
      by_cases hEq : k' = k
      · subst k'
        simp at h
        subst p
        simp
      · simp [hEq] at h
        have hm := ih h
        simp [hm]

private theorem lookupPlan_some_iff (cache : List (CKey × Nat)) (k : CKey) :
    (∃ p, lookupPlan k cache = some p) ↔ k ∈ cache.map Prod.fst := by
  induction cache with
  | nil => simp [lookupPlan]
  | cons e rest ih =>
      rcases e with ⟨k', p'⟩
      constructor
      · rintro ⟨p, hp⟩
        simp [lookupPlan] at hp
        by_cases hEq : k' = k
        · subst k'
          simp at hp
          subst p
          simp
        · simp [hEq] at hp
          have hm : k ∈ rest.map Prod.fst := ih.mp ⟨p, hp⟩
          simp [hm]
      · intro hm
        simp only [List.map_cons, List.mem_cons] at hm
        rcases hm with hEq | hm
        · subst k'
          exact ⟨p', by simp [lookupPlan]⟩
        · by_cases hEq : k' = k
          · subst k'
            exact ⟨p', by simp [lookupPlan]⟩
          · rcases ih.mpr hm with ⟨p, hp⟩
            exact ⟨p, by simp [lookupPlan, hEq, hp]⟩

-- ---------------------------------------------------------------------------
-- Configuration before a prefix
-- ---------------------------------------------------------------------------

private def cfgBefore (initCfg : OptCfg) (pre : List Op) : OptCfg :=
  pre.foldl stepCfg initCfg

private theorem cfgBeforeIdx_append_prefix (initCfg : OptCfg) (pre ops : List Op) (j : Nat)
    (hj : j ≤ pre.length) :
    cfgBeforeIdx initCfg (pre ++ ops) j = cfgBeforeIdx initCfg pre j := by
  unfold cfgBeforeIdx
  rw [List.take_append_of_le_length hj]

private theorem cfgBeforeIdx_append_at (initCfg : OptCfg) (pre ops : List Op) :
    cfgBeforeIdx initCfg (pre ++ ops) pre.length = cfgBefore initCfg pre := by
  unfold cfgBeforeIdx cfgBefore
  rw [List.take_left]

-- ---------------------------------------------------------------------------
-- "seen an earlier query with this key" predicate (Bool-valued)
-- ---------------------------------------------------------------------------

private def seenAt (initCfg : OptCfg) (pre : List Op) (sqlId : Nat) (cfg : OptCfg) : Bool :=
  ((List.range pre.length).any fun j =>
    match pre[j]? with
    | some (.runQuery s') => s' == sqlId && cfgBeforeIdx initCfg pre j == cfg
    | _ => false)

private theorem seenAt_iff_any_prefix (initCfg : OptCfg) (pre ops : List Op)
    (sqlId : Nat) (cfg : OptCfg) :
    ((List.range pre.length).any fun j =>
      match (pre ++ ops)[j]? with
      | some (.runQuery s') => s' == sqlId && cfgBeforeIdx initCfg (pre ++ ops) j == cfg
      | _ => false) = seenAt initCfg pre sqlId cfg := by
  unfold seenAt
  rw [Bool.eq_iff_iff]
  constructor
  · intro hv
    rw [List.any_eq_true] at hv ⊢
    rcases hv with ⟨j, hj, hval⟩
    have hjlt : j < pre.length := by simpa using hj
    refine ⟨j, hj, ?_⟩
    have hget : (pre ++ ops)[j]? = pre[j]? := by
      rw [List.getElem?_append]
      simp [hjlt]
    have hcfg : cfgBeforeIdx initCfg (pre ++ ops) j = cfgBeforeIdx initCfg pre j :=
      cfgBeforeIdx_append_prefix initCfg pre ops j (Nat.le_of_lt hjlt)
    simpa [hget, hcfg] using hval
  · intro hv
    rw [List.any_eq_true] at hv ⊢
    rcases hv with ⟨j, hj, hval⟩
    have hjlt : j < pre.length := by simpa using hj
    refine ⟨j, hj, ?_⟩
    have hget : (pre ++ ops)[j]? = pre[j]? := by
      rw [List.getElem?_append]
      simp [hjlt]
    have hcfg : cfgBeforeIdx initCfg (pre ++ ops) j = cfgBeforeIdx initCfg pre j :=
      cfgBeforeIdx_append_prefix initCfg pre ops j (Nat.le_of_lt hjlt)
    simpa [hget, hcfg] using hval

private theorem seenAt_append (initCfg : OptCfg) (pre : List Op) (op : Op)
    (sqlId : Nat) (cfg : OptCfg) :
    seenAt initCfg (pre ++ [op]) sqlId cfg =
      (seenAt initCfg pre sqlId cfg ||
       (match op with
        | .runQuery s => (s == sqlId) && (cfgBefore initCfg pre == cfg)
        | _ => false)) := by
  unfold seenAt
  have hlen : (pre ++ [op]).length = pre.length + 1 := by simp
  rw [hlen, List.range_succ, List.any_append]
  congr 1
  · exact seenAt_iff_any_prefix initCfg pre [op] sqlId cfg
  · have hget : (pre ++ [op])[pre.length]? = some op := by simp
    have hcfg : cfgBeforeIdx initCfg (pre ++ [op]) pre.length = cfgBefore initCfg pre :=
      cfgBeforeIdx_append_at initCfg pre [op]
    cases op with
    | runQuery s => simp [hcfg]
    | bumpSchema => simp
    | setOptBits b => simp

-- ---------------------------------------------------------------------------
-- The abstract recompile-flag sequence produced by the cache
-- ---------------------------------------------------------------------------

private def flagsFor (initCfg : OptCfg) (pre : List Op) : List Op → List Bool
  | [] => []
  | .runQuery s :: rest =>
      (!seenAt initCfg pre s (cfgBefore initCfg pre)) ::
        flagsFor initCfg (pre ++ [.runQuery s]) rest
  | .bumpSchema :: rest => flagsFor initCfg (pre ++ [.bumpSchema]) rest
  | .setOptBits b :: rest => flagsFor initCfg (pre ++ [.setOptBits b]) rest

-- ---------------------------------------------------------------------------
-- Soundness of the cached execution relative to flagsFor
-- ---------------------------------------------------------------------------

private theorem cachedAux (compile : Nat → OptCfg → Nat) (exec : Nat → Nat) (initCfg : OptCfg) :
    ∀ (pre ops : List Op) (cfg : OptCfg) (cache : List (CKey × Nat)),
      cfg = cfgBefore initCfg pre →
      (∀ k p, (k, p) ∈ cache → p = compile k.1 k.2) →
      (∀ sqlId c, (sqlId, c) ∈ cache.map Prod.fst → seenAt initCfg pre sqlId c = true) →
      (∀ sqlId c, seenAt initCfg pre sqlId c = true → (sqlId, c) ∈ cache.map Prod.fst) →
      let out := cachedRun compile exec cfg cache ops
      out.map Prod.snd = uncachedRun compile exec cfg ops ∧
      out.map Prod.fst = flagsFor initCfg pre ops := by
  intro pre ops
  induction ops generalizing pre with
  | nil =>
      intro cfg cache hcfg hp hk1 hk2
      simp [cachedRun, uncachedRun, flagsFor]
  | cons op rest ih =>
      intro cfg cache hcfg hp hk1 hk2
      cases op with
      | bumpSchema =>
          have hcfg' : { cfg with schemaGen := cfg.schemaGen + 1 } =
              cfgBefore initCfg (pre ++ [.bumpSchema]) := by
            rw [hcfg]
            simp [cfgBefore, List.foldl_append, stepCfg]
          have hk1' : ∀ sqlId c, (sqlId, c) ∈ cache.map Prod.fst →
              seenAt initCfg (pre ++ [.bumpSchema]) sqlId c = true := by
            intro sqlId c hm
            rw [seenAt_append, Bool.or_eq_true]
            exact Or.inl (hk1 sqlId c hm)
          have hk2' : ∀ sqlId c, seenAt initCfg (pre ++ [.bumpSchema]) sqlId c = true →
              (sqlId, c) ∈ cache.map Prod.fst := by
            intro sqlId c hs
            rw [seenAt_append, Bool.or_eq_true] at hs
            rcases hs with hs | hf
            · exact hk2 sqlId c hs
            · simp at hf
          simpa [cachedRun, uncachedRun, flagsFor] using
            ih (pre ++ [.bumpSchema]) { cfg with schemaGen := cfg.schemaGen + 1 } cache hcfg' hp hk1' hk2'
      | setOptBits b =>
          have hcfg' : { cfg with optBits := b } =
              cfgBefore initCfg (pre ++ [.setOptBits b]) := by
            rw [hcfg]
            simp [cfgBefore, List.foldl_append, stepCfg]
          have hk1' : ∀ sqlId c, (sqlId, c) ∈ cache.map Prod.fst →
              seenAt initCfg (pre ++ [.setOptBits b]) sqlId c = true := by
            intro sqlId c hm
            rw [seenAt_append, Bool.or_eq_true]
            exact Or.inl (hk1 sqlId c hm)
          have hk2' : ∀ sqlId c, seenAt initCfg (pre ++ [.setOptBits b]) sqlId c = true →
              (sqlId, c) ∈ cache.map Prod.fst := by
            intro sqlId c hs
            rw [seenAt_append, Bool.or_eq_true] at hs
            rcases hs with hs | hf
            · exact hk2 sqlId c hs
            · simp at hf
          simpa [cachedRun, uncachedRun, flagsFor] using
            ih (pre ++ [.setOptBits b]) { cfg with optBits := b } cache hcfg' hp hk1' hk2'
      | runQuery s =>
          let key : CKey := (s, cfg)
          have hcfg' : cfg = cfgBefore initCfg (pre ++ [.runQuery s]) := by
            rw [hcfg]
            simp [cfgBefore, List.foldl_append, stepCfg]
          by_cases hsome : ∃ p, lookupPlan key cache = some p
          · rcases hsome with ⟨p, hlookup⟩
            have hmemk : key ∈ cache.map Prod.fst :=
              (lookupPlan_some_iff cache key).mp ⟨p, hlookup⟩
            have hplan : p = compile s cfg := by
              have hm : (key, p) ∈ cache := lookupPlan_eq_some_mem key cache p hlookup
              have hh := hp key p hm
              simpa [key] using hh
            have hseen : seenAt initCfg pre s cfg = true := by
              apply hk1 s cfg
              simpa [key] using hmemk
            have hk1' : ∀ sqlId c, (sqlId, c) ∈ cache.map Prod.fst →
                seenAt initCfg (pre ++ [.runQuery s]) sqlId c = true := by
              intro sqlId c hm
              rw [seenAt_append, Bool.or_eq_true]
              exact Or.inl (hk1 sqlId c hm)
            have hk2' : ∀ sqlId c, seenAt initCfg (pre ++ [.runQuery s]) sqlId c = true →
                (sqlId, c) ∈ cache.map Prod.fst := by
              intro sqlId c hs
              rw [seenAt_append, Bool.or_eq_true] at hs
              rcases hs with hs | hf
              · exact hk2 sqlId c hs
              · rw [Bool.and_eq_true] at hf
                rcases hf with ⟨hsql, hceq⟩
                have hsq : s = sqlId := beq_iff_eq.mp hsql
                have hce : cfgBefore initCfg pre = c := beq_iff_eq.mp hceq
                subst sqlId
                subst c
                simpa [key, hcfg] using hmemk
            have ihres := ih (pre ++ [.runQuery s]) cfg cache hcfg' hp hk1' hk2'
            constructor
            · simpa [cachedRun, uncachedRun, key, hlookup, hplan] using ihres.1
            · simp [cachedRun, flagsFor, key, hlookup]
              exact ⟨by simpa [hcfg] using hseen, ihres.2⟩
          · have hnone : lookupPlan key cache = none := by
              cases h : lookupPlan key cache with
              | none => rfl
              | some p => exact False.elim (hsome ⟨p, h⟩)
            have hnotseen : seenAt initCfg pre s cfg = false := by
              cases h : seenAt initCfg pre s cfg with
              | false => rfl
              | true => exact False.elim (by
                  have hm : (s, cfg) ∈ cache.map Prod.fst := hk2 s cfg h
                  exact hsome ((lookupPlan_some_iff cache key).mpr (by simpa [key] using hm)))
            let p := compile s cfg
            have hp' : ∀ k p', (k, p') ∈ (key, p) :: cache → p' = compile k.1 k.2 := by
              intro k p' hm
              simp at hm
              rcases hm with hEq | hm
              · rcases hEq with ⟨hkEq, hpEq⟩
                subst k
                subst p'
                simp [key, p]
              · exact hp k p' hm
            have hk1' : ∀ sqlId c, (sqlId, c) ∈ ((key, p) :: cache).map Prod.fst →
                seenAt initCfg (pre ++ [.runQuery s]) sqlId c = true := by
              intro sqlId c hm
              simp only [List.map_cons, List.mem_cons] at hm
              rcases hm with hEq | hm
              · have hsq : sqlId = s := by simpa [key] using congrArg Prod.fst hEq
                have hce : c = cfg := by simpa [key] using congrArg Prod.snd hEq
                subst sqlId
                subst c
                rw [seenAt_append, Bool.or_eq_true]
                exact Or.inr (by simp [hcfg])
              · rw [seenAt_append, Bool.or_eq_true]
                exact Or.inl (hk1 sqlId c hm)
            have hk2' : ∀ sqlId c, seenAt initCfg (pre ++ [.runQuery s]) sqlId c = true →
                (sqlId, c) ∈ ((key, p) :: cache).map Prod.fst := by
              intro sqlId c hs
              rw [seenAt_append, Bool.or_eq_true] at hs
              rcases hs with hs | hf
              · have hm : (sqlId, c) ∈ cache.map Prod.fst := hk2 sqlId c hs
                simp [hm]
              · rw [Bool.and_eq_true] at hf
                rcases hf with ⟨hsql, hceq⟩
                have hsq : s = sqlId := beq_iff_eq.mp hsql
                have hce : cfgBefore initCfg pre = c := beq_iff_eq.mp hceq
                subst sqlId
                subst c
                simp [key, hcfg]
            have ihres := ih (pre ++ [.runQuery s]) cfg ((key, p) :: cache) hcfg' hp' hk1' hk2'
            constructor
            · simpa [cachedRun, uncachedRun, key, hnone, p] using ihres.1
            · simp [cachedRun, flagsFor, key, hnone]
              exact ⟨by simpa [hcfg] using hnotseen, ihres.2⟩

-- ---------------------------------------------------------------------------
-- The abstract flag sequence agrees with expectedRecompiles
-- ---------------------------------------------------------------------------

private def erFrom (initCfg : OptCfg) (allOps : List Op) (idx : Nat) : List Op → List Bool
  | [] => []
  | op :: rest =>
      match op with
      | .runQuery s =>
          let curCfg := cfgBeforeIdx initCfg allOps idx
          let seenEarlier := (List.range idx).any fun j =>
            match allOps[j]? with
            | some (.runQuery s') => s' == s && cfgBeforeIdx initCfg allOps j == curCfg
            | _ => false
          (!seenEarlier) :: erFrom initCfg allOps (idx + 1) rest
      | _ => erFrom initCfg allOps (idx + 1) rest

private theorem flagsFor_eq_erFrom (initCfg : OptCfg) :
    ∀ (pre ops : List Op), flagsFor initCfg pre ops = erFrom initCfg (pre ++ ops) pre.length ops := by
  intro pre ops
  induction ops generalizing pre with
  | nil => simp [flagsFor, erFrom]
  | cons op rest ih =>
      cases op with
      | bumpSchema =>
          have hlen : (pre ++ [Op.bumpSchema]).length = pre.length + 1 := by simp
          simpa [flagsFor, erFrom, hlen, List.append_assoc] using ih (pre ++ [Op.bumpSchema])
      | setOptBits b =>
          have hlen : (pre ++ [Op.setOptBits b]).length = pre.length + 1 := by simp
          simpa [flagsFor, erFrom, hlen, List.append_assoc] using ih (pre ++ [Op.setOptBits b])
      | runQuery s =>
          have hcur : cfgBeforeIdx initCfg (pre ++ (Op.runQuery s :: rest)) pre.length =
              cfgBefore initCfg pre := by
            simpa [List.append_assoc] using
              (cfgBeforeIdx_append_at initCfg pre (Op.runQuery s :: rest))
          have hseen :
              ((List.range pre.length).any fun j =>
                match (pre ++ (Op.runQuery s :: rest))[j]? with
                | some (.runQuery s') => s' == s && cfgBeforeIdx initCfg (pre ++ (Op.runQuery s :: rest)) j == cfgBefore initCfg pre
                | _ => false) = seenAt initCfg pre s (cfgBefore initCfg pre) := by
            exact seenAt_iff_any_prefix initCfg pre (Op.runQuery s :: rest) s (cfgBefore initCfg pre)
          have hlen : (pre ++ [Op.runQuery s]).length = pre.length + 1 := by simp
          have htail := ih (pre ++ [Op.runQuery s])
          simpa [flagsFor, erFrom, hcur, hseen, hlen, List.append_assoc] using htail

private def expectedFrom (initCfg : OptCfg) (allOps : List Op) (_idx : Nat) :
    List (Op × Nat) → List Bool :=
  List.filterMap fun (op, i) =>
    match op with
    | .runQuery s =>
        let curCfg := cfgBeforeIdx initCfg allOps i
        let seenEarlier := (List.range i).any fun j =>
          match allOps[j]? with
          | some (.runQuery s') => s' == s && cfgBeforeIdx initCfg allOps j == curCfg
          | _ => false
        some (!seenEarlier)
    | _ => none

private theorem erFrom_eq_expectedFrom (initCfg : OptCfg) (allOps : List Op) :
    ∀ (idx : Nat) (rest : List Op),
      erFrom initCfg allOps idx rest = expectedFrom initCfg allOps idx (rest.zipIdx idx) := by
  intro idx rest
  induction rest generalizing idx with
  | nil => simp [erFrom, expectedFrom]
  | cons op rest ih =>
      cases op with
      | runQuery s => simp [erFrom, expectedFrom, ih, List.filterMap_cons]
      | bumpSchema => simp [erFrom, expectedFrom, ih]
      | setOptBits b => simp [erFrom, expectedFrom, ih]

-- ---------------------------------------------------------------------------
-- Final theorem
-- ---------------------------------------------------------------------------

theorem sound : TinylambOpt.ResearchGoals.PlanCacheToggleInvalidation.Goal := by
  intro compile exec initCfg ops
  dsimp
  have hres := cachedAux compile exec initCfg [] ops initCfg [] rfl
    (by intro k p hm; simp at hm)
    (by intro sqlId c hm; simp at hm)
    (by intro sqlId c hs; simp [seenAt] at hs)
  constructor
  · exact hres.1
  · rw [hres.2]
    rw [flagsFor_eq_erFrom initCfg [] ops]
    change erFrom initCfg ops 0 ops = expectedRecompiles initCfg ops
    rw [erFrom_eq_expectedFrom initCfg ops 0 ops]
    rfl

end TinylambOpt.ResearchProofs.PlanCacheToggleInvalidation
