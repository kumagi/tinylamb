import Std

namespace TinylambOpt.ResearchGoals.LazyBlockCacheReuse

abbrev Key := Nat × Nat

inductive Op where
  | read (key : Key)
  | evict (key : Key)
  | mutate (nextStorage : Key → Nat)

structure CacheState where
  curGen : Nat
  storage : Key → Nat
  cache : List (Key × Nat × Nat)

def lookupCache (key : Key) : List (Key × Nat × Nat) → Option (Nat × Nat)
  | [] => none
  | (k, gv) :: rest => if k == key then some gv else lookupCache key rest

def eraseKey (key : Key) (entries : List (Key × Nat × Nat)) : List (Key × Nat × Nat) :=
  entries.filter (fun p => p.1 != key)

def refRun (storage : Key → Nat) : List Op → List Nat
  | [] => []
  | .read k :: rest => storage k :: refRun storage rest
  | .evict _ :: rest => refRun storage rest
  | .mutate next :: rest => refRun next rest

def optRun (st : CacheState) : List Op → List (Nat × Bool)
  | [] => []
  | .read k :: rest =>
      match lookupCache k st.cache with
      | some (g, v) =>
          if g == st.curGen then
            (v, false) :: optRun st rest
          else
            let vNew := st.storage k
            let cache' := (k, (st.curGen, vNew)) :: st.cache
            (vNew, true) :: optRun { st with cache := cache' } rest
      | none =>
          let vNew := st.storage k
          let cache' := (k, (st.curGen, vNew)) :: st.cache
          (vNew, true) :: optRun { st with cache := cache' } rest
  | .evict k :: rest =>
      optRun { st with cache := eraseKey k st.cache } rest
  | .mutate next :: rest =>
      optRun { st with curGen := st.curGen + 1, storage := next } rest

def noResetBetween (ops : List Op) (j idx : Nat) (k : Key) : Bool :=
  (List.range idx).all fun m =>
    if j < m then
      match ops[m]? with
      | some (.mutate _) => false
      | some (.evict k') => k' != k
      | _ => true
    else true

def shouldHitAt (ops : List Op) (idx : Nat) (k : Key) : Bool :=
  (List.range idx).any fun j =>
    match ops[j]? with
    | some (.read k') => k' == k && noResetBetween ops j idx k
    | _ => false

def expectedDecodeFlags (ops : List Op) : List Bool :=
  (ops.zipIdx).filterMap fun (op, idx) =>
    match op with
    | .read k => some (!shouldHitAt ops idx k)
    | _ => none

/-- C++ `ReuseLazyDecodedBlock` (`table/tlcs_file.cpp`): bumping
    `LayoutGeneration` invalidates stale cache entries lazily without eagerly
    flushing the per-block cache, while per-key `evict` drops only that key. -/
def Goal : Prop :=
  ∀ (initStorage : Key → Nat) (ops : List Op),
    let res := optRun ⟨1, initStorage, []⟩ ops
    res.map Prod.fst = refRun initStorage ops ∧
    res.map Prod.snd = expectedDecodeFlags ops

/-- Naive cache lookup without checking `gen == curGen` serves stale values
    after `mutate`. -/
def badOptRun (st : CacheState) : List Op → List Nat
  | [] => []
  | .read k :: rest =>
      match lookupCache k st.cache with
      | some (_, v) => v :: badOptRun st rest
      | none =>
          let vNew := st.storage k
          vNew :: badOptRun { st with cache := (k, (st.curGen, vNew)) :: st.cache } rest
  | .evict k :: rest =>
      badOptRun { st with cache := eraseKey k st.cache } rest
  | .mutate next :: rest =>
      badOptRun { st with curGen := st.curGen + 1, storage := next } rest

private def sampleOps : List Op :=
  [ .read (0, 0)
  , .read (0, 0)
  , .mutate (fun _ => 20)
  , .read (0, 0)
  , .read (1, 0)
  , .evict (0, 0)
  , .read (0, 0)
  , .read (1, 0)
  ]

example :
    optRun ⟨1, fun _ => 10, []⟩ sampleOps =
      [(10, true), (10, false), (20, true), (20, true), (20, true), (20, false)] := by
  decide

example :
    (optRun ⟨1, fun _ => 10, []⟩ sampleOps).map Prod.snd =
      expectedDecodeFlags sampleOps := by
  decide

example :
    badOptRun ⟨1, fun _ => 10, []⟩ [.read (0, 0), .mutate (fun _ => 20), .read (0, 0)] =
      [10, 10] := by
  decide

end TinylambOpt.ResearchGoals.LazyBlockCacheReuse
