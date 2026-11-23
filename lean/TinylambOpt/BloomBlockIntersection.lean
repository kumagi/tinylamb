/-
  TinylambOpt/BloomBlockIntersection.lean

  Bloom-filter based block pruning for an inner equi-join.

  Abstract semantics (stated explicitly):
    * a key is either NULL (`nul`) or a normalized natural number (`val k`);
      ordinary `=` equi-join never matches a NULL key;
    * a Bloom configuration fixes `width`, `seed`, a key `normalize` function
      and a hash function `hash seed normalizedKey -> bit positions`;
    * a signature is the list of set bit positions (duplicates harmless);
      `sigOfKeys` ORs the bits of every non-NULL key into one signature;
    * the probe block advertises a signature `probeSig` that must cover the
      bits of every non-NULL key physically present in the block.

  Soundness hypotheses (sufficient):
    (H1) build and probe use the SAME configuration (width, seed, normalize,
         hash) -- a seed mismatch invalidates the check (boundary theorem);
    (H2) every non-NULL key sets at least one bit;
    (H3) the probe summary covers every non-NULL key it contains;
    (H4) build and probe signatures have an empty bit product.

  Under H1-H4 no normalized key can be present on both sides, so the inner
  join emits no row from this block.  `bloomSkip_sound` additionally models
  the implementation decision: when the compatibility flag is false the
  skip decision is false and the block is read in full.

  C++ realization: common/runtime_filter.hpp.  The C++ code must compare
  bit width, seed, hash/normalization identity (or a version tag) before
  applying the filter, and must treat NULL keys separately; a NULL-safe
  equality join also needs an independent "contains NULL" flag, because
  an empty signature does NOT exclude NULL = NULL (boundary theorem).
-/

namespace TinylambOpt.BloomBlockIntersection

/-- Configuration shared by build and probe sides (functions are parameters,
    not computed here). -/
structure BloomCfg where
  width : Nat
  seed : Nat
  normalize : Nat → Nat
  hash : Nat → Nat → List Nat

/-- A join key: NULL, or a non-NULL integer key. -/
inductive JoinKey where
  | nul : JoinKey
  | val : Nat → JoinKey
  deriving DecidableEq, Repr

/-- A Bloom signature: the set of set bit positions. -/
abbrev Sig := List Nat

/-- The bits set by one normalized key under a configuration. -/
def keyBits (cfg : BloomCfg) (k : Nat) : Sig :=
  cfg.hash cfg.seed (cfg.normalize k)

/-- Build (or probe) signature: OR of the bits of all non-NULL keys. -/
def sigOfKeys (cfg : BloomCfg) (keys : List JoinKey) : Sig :=
  keys.flatMap (fun key => match key with | JoinKey.nul => [] | JoinKey.val k => keyBits cfg k)

/-- A key listed in `keys` contributes all of its bits to `sigOfKeys`. -/
theorem keyBits_subset_sigOfKeys (cfg : BloomCfg) (keys : List JoinKey) (k : Nat)
    (h : JoinKey.val k ∈ keys) : keyBits cfg k ⊆ sigOfKeys cfg keys := by
  induction keys with
  | nil => simp at h
  | cons key keys ih =>
      simp only [sigOfKeys, List.flatMap_cons] at h ⊢
      cases key with
      | nul =>
          simp at h
          exact ih h
      | val k' =>
          simp at h
          rcases h with hk | hmem
          · subst k'
            intro i hi
            exact List.mem_append.mpr (Or.inl hi)
          · intro i hi
            exact List.mem_append.mpr (Or.inr (ih hmem hi))

/-- MAIN: with identical configurations, a bit-product-zero probe signature
    rules out every normalized-key match between build and probe keys. -/
theorem bloom_block_intersection_sound (cfgB cfgP : BloomCfg)
    (buildKeys probeKeys : List JoinKey) (probeSig : Sig)
    (cfgEq : cfgB = cfgP)
    (nonemptyBits : ∀ k : Nat, ∃ i, i ∈ keyBits cfgP k)
    (probeCovers : ∀ k : Nat, JoinKey.val k ∈ probeKeys → keyBits cfgP k ⊆ probeSig)
    (hdis : ∀ i, i ∈ sigOfKeys cfgB buildKeys → i ∈ probeSig → False) :
    ∀ (x : JoinKey), x ∈ buildKeys →
      ∀ (y : JoinKey), y ∈ probeKeys →
        (match x, y with
         | JoinKey.val a, JoinKey.val b => cfgP.normalize a = cfgP.normalize b
         | _, _ => False) → False := by
  subst cfgB
  intro x hx y hy hmatch
  cases x with
  | nul => simp at hmatch
  | val a =>
      cases y with
      | nul => simp at hmatch
      | val b =>
          simp only at hmatch
          rcases nonemptyBits b with ⟨i, hi⟩
          have hcovB : keyBits cfgP a ⊆ sigOfKeys cfgP buildKeys :=
            keyBits_subset_sigOfKeys cfgP buildKeys a hx
          have hcovP : keyBits cfgP b ⊆ probeSig := probeCovers b hy
          have hbitsEq : keyBits cfgP a = keyBits cfgP b := by
            simp [keyBits, hmatch]
          exact hdis i (hcovB (by rwa [hbitsEq])) (hcovP hi)

/-- Implementation decision procedure: bit product zero AND a compatibility
    flag.  The flag stands for the C++ width/seed/normalize/hash check; when
    it is false we fall back to a full scan. -/
def bloomSkip (buildSig probeSig : Sig) (compat : Bool) : Bool :=
  compat && probeSig.all (fun i => decide (i ∉ buildSig))

/-- Corollary for the decision procedure: compatibility = true must imply
    equal configurations (the C++ guard), hence the same conclusion. -/
theorem bloomSkip_sound (cfgB cfgP : BloomCfg)
    (buildKeys probeKeys : List JoinKey) (probeSig : Sig) (compat : Bool)
    (compatOk : compat = true → cfgB = cfgP)
    (nonemptyBits : ∀ k : Nat, ∃ i, i ∈ keyBits cfgP k)
    (probeCovers : ∀ k : Nat, JoinKey.val k ∈ probeKeys → keyBits cfgP k ⊆ probeSig)
    (hskip : bloomSkip (sigOfKeys cfgB buildKeys) probeSig compat = true) :
    ∀ (x : JoinKey), x ∈ buildKeys →
      ∀ (y : JoinKey), y ∈ probeKeys →
        (match x, y with
         | JoinKey.val a, JoinKey.val b => cfgP.normalize a = cfgP.normalize b
         | _, _ => False) → False := by
  simp [bloomSkip, List.all_eq_true] at hskip
  apply bloom_block_intersection_sound cfgB cfgP buildKeys probeKeys probeSig
    (compatOk hskip.1) nonemptyBits probeCovers
  intro i hib hprobe
  have hn : i ∉ sigOfKeys cfgB buildKeys := by simpa using hskip.2 i hprobe
  exact hn hib

/-- A concrete configuration used for boundary examples: the hash ignores the
    key and only depends on the seed. -/
def tinyCfg (seed : Nat) : BloomCfg where
  width := 4
  seed := seed
  normalize := id
  hash := fun s _ => [s % 4]

/-- Boundary example 1: different seeds give an empty bit product even for
    the very same key, so configuration compatibility is indispensable. -/
theorem different_seed_can_hide_same_key :
    let cb := tinyCfg 0
    let cp := tinyCfg 1
    let bs := sigOfKeys cb [JoinKey.val 7]
    let ps := sigOfKeys cp [JoinKey.val 7]
    bs.any (fun i => i ∈ ps) = false ∧ cb.seed ≠ cp.seed ∧
    (match JoinKey.val 7, JoinKey.val 7 with
     | JoinKey.val a, JoinKey.val b => cp.normalize a = cp.normalize b
     | _, _ => False) := by
  decide

/-- NULL-safe equality needs an extra NULL-presence flag: an empty
    intersection does not rule out NULL = NULL. -/
def nullSafeEq (a b : JoinKey) : Bool :=
  match a, b with
  | JoinKey.nul, JoinKey.nul => true
  | JoinKey.val x, JoinKey.val y => x == y
  | _, _ => false

theorem bloom_disjoint_does_not_exclude_null_null_match :
    let bs : Sig := sigOfKeys (tinyCfg 0) [JoinKey.nul]
    let ps : Sig := sigOfKeys (tinyCfg 0) [JoinKey.nul]
    bs.any (fun i => i ∈ ps) = false ∧ nullSafeEq JoinKey.nul JoinKey.nul = true := by
  decide

def nullSafeSkip (cfg : BloomCfg) (bs ps : List JoinKey) : Bool :=
  bloomSkip (sigOfKeys cfg bs) (sigOfKeys cfg ps) true &&
    !(bs.contains .nul && ps.contains .nul)

theorem nullSafeSkip_sound (cfg : BloomCfg) (bs ps : List JoinKey)
    (bits : ∀ k, ∃ i, i ∈ keyBits cfg k) (hs : nullSafeSkip cfg bs ps = true)
    (x y : JoinKey) (hx : x ∈ bs) (hy : y ∈ ps) :
    (match x, y with
     | .nul, .nul => True
     | .val a, .val b => cfg.normalize a = cfg.normalize b
     | _, _ => False) → False := by
  simp only [nullSafeSkip, Bool.and_eq_true] at hs
  have h := bloomSkip_sound cfg cfg bs ps _ true (fun _ => rfl) bits
    (fun k hk => keyBits_subset_sigOfKeys cfg ps k hk) hs.1 x hx y hy
  cases x <;> cases y <;> simp_all

end TinylambOpt.BloomBlockIntersection
