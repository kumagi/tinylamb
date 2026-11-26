import TinylambOpt.ResearchGoals.TwoProbePackedBloom

/-!
Proved

- `Goal` — Two-probe biased-2x32 packed Bloom filter (`executor/detail/build_input.cpp`
  `packed_bloom`): biasing non-negative offsets within `[0, 2^32)` avoids signed
  sign-extension bit corruption and yields exact SQL equi-join parity for any 64-bit
  hash function and bit width `1 ≤ m ≤ 20`.

The reference model is defined in `TinylambOpt.ResearchGoals.TwoProbePackedBloom`.
-/

open TinylambOpt.ResearchGoals.TwoProbePackedBloom

namespace TinylambOpt.ResearchProofs.TwoProbePackedBloom

/-- Record type of the build/probe lists. -/
private abbrev Rec : Type := (Option Int × Option Int) × Nat

/-- Probe bit position `b0` for packed key `pk`. -/
private def pos0 (m : Nat) (h : BitVec 64 → BitVec 64) (pk : BitVec 64) : Nat :=
  (h pk &&& ((1#64 <<< m) - 1#64)).toNat

/-- Probe bit position `b1` for packed key `pk`. -/
private def pos1 (m : Nat) (h : BitVec 64 → BitVec 64) (pk : BitVec 64) : Nat :=
  ((h pk >>> 24) &&& ((1#64 <<< m) - 1#64)).toNat

/-- One `foldl` step of `buildBloom`'s accumulator function. -/
private def bloomStep (lo1 lo2 : Int) (m : Nat) (h : BitVec 64 → BitVec 64)
    (bits : Nat) (rec : Rec) : Nat :=
  let mask : BitVec 64 := (1#64 <<< m) - 1#64
  match rec.1 with
  | (some k1, some k2) =>
      match packKey lo1 lo2 k1 k2 with
      | some pk =>
          let kh := h pk
          let b0 := (kh &&& mask).toNat
          let b1 := ((kh >>> 24) &&& mask).toNat
          bits ||| (1 <<< b0) ||| (1 <<< b1)
      | none => bits
  | _ => bits

/-- `buildBloom`'s fold started at an arbitrary accumulator value. -/
private def bloomFrom (lo1 lo2 : Int) (m : Nat) (h : BitVec 64 → BitVec 64)
    (n : Nat) (build : List Rec) : Nat :=
  build.foldl (bloomStep lo1 lo2 m h) n

private theorem buildBloom_eq (lo1 lo2 : Int) (m : Nat) (h : BitVec 64 → BitVec 64)
    (build : List Rec) :
    buildBloom lo1 lo2 m h build = bloomFrom lo1 lo2 m h 0 build :=
  rfl

/-- For a bit `x ≤ 1`, being unequal to `0` is being equal to `1`. -/
private theorem le_one_iff {x : Nat} (hx : x ≤ 1) : (x == 1) = (x != 0) := by
  rcases Nat.eq_zero_or_pos x with rfl | h
  · rfl
  · have hx1 : x = 1 := by omega
    subst hx1
    rfl

/-- Bridge between the `(x >>> i) &&& 1 == 1` spelling and `Nat.testBit`. -/
private theorem and_one_eq_testBit (n i : Nat) :
    ((n >>> i) &&& 1 == 1) = n.testBit i := by
  show ((n >>> i) &&& 1 == 1) = (1 &&& (n >>> i) != 0)
  rw [Nat.and_comm, le_one_iff (Nat.and_le_left)]

private theorem testBit_one_shiftLeft (i : Nat) : (1 <<< i).testBit i = true := by
  rw [Nat.one_shiftLeft, Nat.testBit_two_pow_self]

/-- Setting the low probe bit survives any further OR-ing. -/
private theorem testBit_two_or_left (x i j : Nat) :
    (x ||| (1 <<< i) ||| (1 <<< j)).testBit i = true := by
  rw [Nat.testBit_or, Nat.testBit_or, testBit_one_shiftLeft]
  simp

/-- Setting the high probe bit survives any further OR-ing. -/
private theorem testBit_two_or_right (x i j : Nat) :
    (x ||| (1 <<< i) ||| (1 <<< j)).testBit j = true := by
  rw [Nat.testBit_or, testBit_one_shiftLeft]
  simp

/-- OR-folding never clears a bit that the accumulator already carries. -/
private theorem testBit_bloomFrom {lo1 lo2 : Int} {m : Nat} {h : BitVec 64 → BitVec 64}
    {i : Nat} : ∀ (build : List Rec) (n : Nat),
      n.testBit i = true → (bloomFrom lo1 lo2 m h n build).testBit i = true := by
  intro build
  induction build with
  | nil => exact fun _ hn => hn
  | cons y ys ih =>
      intro n hn
      rcases y with ⟨⟨o1, o2⟩, iy⟩
      have hstep : (bloomStep lo1 lo2 m h n (⟨⟨o1, o2⟩, iy⟩ : Rec)).testBit i = true := by
        cases o1 with
        | none => simp only [bloomStep]; exact hn
        | some a1 =>
            cases o2 with
            | none => simp only [bloomStep]; exact hn
            | some a2 =>
                cases hpk : packKey lo1 lo2 a1 a2 with
                | none => simp only [bloomStep, hpk]; exact hn
                | some pk =>
                    simp only [bloomStep, hpk]
                    rw [Nat.testBit_or, Nat.testBit_or]
                    simp [hn]
      exact ih _ hstep

/-- `bloomMayContain` in terms of `Nat.testBit`. -/
private theorem bloomMayContain_iff_bits {m : Nat} {h : BitVec 64 → BitVec 64}
    {bits : Nat} {pk : BitVec 64} :
    bloomMayContain m h bits pk =
      (bits.testBit (pos0 m h pk) && bits.testBit (pos1 m h pk)) := by
  unfold bloomMayContain
  simp only [pos0, pos1, and_one_eq_testBit]

/-- If the accumulator already carries both probe bits of `pk`, every later
    fold result reports `bloomMayContain m h · pk = true`. -/
private theorem mayContain_bloomFrom {lo1 lo2 : Int} {m : Nat} {h : BitVec 64 → BitVec 64}
    {pk : BitVec 64} :
    ∀ (build : List Rec) (n : Nat),
      n.testBit (pos0 m h pk) = true → n.testBit (pos1 m h pk) = true →
      bloomMayContain m h (bloomFrom lo1 lo2 m h n build) pk = true := by
  intro build n hn0 hn1
  have h0 : (bloomFrom lo1 lo2 m h n build).testBit (pos0 m h pk) = true :=
    testBit_bloomFrom build n hn0
  have h1 : (bloomFrom lo1 lo2 m h n build).testBit (pos1 m h pk) = true :=
    testBit_bloomFrom build n hn1
  rw [bloomMayContain_iff_bits, h0, h1]
  rfl

/-- No false negatives: a build record with packable key always passes. -/
private theorem mayContain_of_mem {lo1 lo2 : Int} {m : Nat} {h : BitVec 64 → BitVec 64}
    {k1 k2 : Int} {pk : BitVec 64} (hpk : packKey lo1 lo2 k1 k2 = some pk) :
    ∀ (build : List Rec) (n : Nat),
      (∃ b ∈ build, b.1 = (some k1, some k2)) →
      bloomMayContain m h (bloomFrom lo1 lo2 m h n build) pk = true := by
  intro build
  induction build with
  | nil => intro n hmem; exact absurd hmem (by simp)
  | cons y ys ih =>
      intro n hmem
      obtain ⟨b, hb, hb1⟩ := hmem
      rw [show bloomFrom lo1 lo2 m h n (y :: ys) =
          bloomFrom lo1 lo2 m h (bloomStep lo1 lo2 m h n y) ys from rfl]
      cases List.mem_cons.mp hb with
      | inr hb' => exact ih _ ⟨b, hb', hb1⟩
      | inl hy =>
          subst hy
          rcases b with ⟨⟨o1, o2⟩, ib⟩
          simp at hb1
          obtain ⟨rfl, rfl⟩ := hb1
          refine mayContain_bloomFrom ys (bloomStep lo1 lo2 m h n (⟨⟨some k1, some k2⟩, ib⟩ : Rec))
            ?_ ?_
          · simp only [bloomStep, pos0, hpk]
            exact testBit_two_or_left n _ _
          · simp only [bloomStep, pos1, hpk]
            exact testBit_two_or_right n _ _

/-- `bloomJoin` as a `flatMap` over per-probe branches. -/
private def bloomProbe (lo1 lo2 : Int) (m : Nat) (h : BitVec 64 → BitVec 64)
    (bits : Nat) (build : List Rec) (p : Rec) : List (Nat × Nat) :=
  match p.1 with
  | (some k1, some k2) =>
      match packKey lo1 lo2 k1 k2 with
      | none => []
      | some pk =>
          if !bloomMayContain m h bits pk then []
          else build.filterMap fun b =>
            if sqlKeyEq b.1 p.1 then some (b.2, p.2) else none
  | _ => []

/-- `refJoin` as a `flatMap` over per-probe branches. -/
private def refProbe (build : List Rec) (p : Rec) : List (Nat × Nat) :=
  build.filterMap fun b =>
    if sqlKeyEq b.1 p.1 then some (b.2, p.2) else none

private theorem bloomJoin_eq (lo1 lo2 : Int) (m : Nat) (h : BitVec 64 → BitVec 64)
    (build probes : List Rec) :
    bloomJoin lo1 lo2 m h build probes =
      probes.flatMap (bloomProbe lo1 lo2 m h (buildBloom lo1 lo2 m h build) build) :=
  rfl

private theorem refJoin_eq (build probes : List Rec) :
    refJoin build probes = probes.flatMap (refProbe build) :=
  rfl

private theorem filterMap_all_none (build : List Rec) (f : Rec → Option (Nat × Nat))
    (h : ∀ b ∈ build, f b = none) : build.filterMap f = [] := by
  induction build with
  | nil => rfl
  | cons b bs ih =>
      rw [List.filterMap_cons, h b List.mem_cons_self]
      exact ih (fun x hx => h x (List.mem_cons_of_mem _ hx))

/-- A matched build record forces the probe key to be packable and equal. -/
private theorem key_eq_of_match {lo1 lo2 : Int} {build : List Rec}
    (hbuild : ∀ b ∈ build, ∃ k1 k2 : Int,
      b.1 = (some k1, some k2) ∧ (packKey lo1 lo2 k1 k2).isSome = true)
    {a1 a2 : Int} {b : Rec} (hmem : b ∈ build)
    (h : sqlKeyEq b.1 (some a1, some a2) = true) :
    b.1 = (some a1, some a2) ∧ (packKey lo1 lo2 a1 a2).isSome = true := by
  obtain ⟨k1, k2, heq, hsome⟩ := hbuild b hmem
  rw [heq] at h
  simp only [sqlKeyEq] at h
  rw [Bool.and_eq_true] at h
  obtain ⟨hk1, hk2⟩ := h
  have e1 : k1 = a1 := of_decide_eq_true hk1
  have e2 : k2 = a2 := of_decide_eq_true hk2
  subst e1; subst e2
  exact ⟨heq, hsome⟩

private theorem probe_eq_of_valid {lo1 lo2 : Int} {m : Nat} {h : BitVec 64 → BitVec 64}
    {build : List Rec}
    (hbuild : ∀ b ∈ build, ∃ k1 k2 : Int,
      b.1 = (some k1, some k2) ∧ (packKey lo1 lo2 k1 k2).isSome = true) :
    ∀ p : Rec,
      bloomProbe lo1 lo2 m h (buildBloom lo1 lo2 m h build) build p = refProbe build p := by
  intro p
  rcases p with ⟨⟨o1, o2⟩, ip⟩
  cases o1 with
  | none =>
      simp only [bloomProbe, refProbe]
      refine (filterMap_all_none build _ ?_).symm
      intro b hb
      obtain ⟨k1, k2, heq, _⟩ := hbuild b hb
      show (if sqlKeyEq b.1 (none, o2) = true then some (b.2, ip) else none) = none
      rw [heq, ite_eq_right]
      simp [sqlKeyEq]
  | some a1 =>
      cases o2 with
      | none =>
          simp only [bloomProbe, refProbe]
          refine (filterMap_all_none build _ ?_).symm
          intro b hb
          obtain ⟨k1, k2, heq, _⟩ := hbuild b hb
          show (if sqlKeyEq b.1 (some a1, none) = true then some (b.2, ip) else none) = none
          rw [heq, ite_eq_right]
          simp [sqlKeyEq]
      | some a2 =>
          cases hpk : packKey lo1 lo2 a1 a2 with
          | none =>
              simp only [bloomProbe, refProbe, hpk]
              refine (filterMap_all_none build _ ?_).symm
              intro b hb
              obtain ⟨k1, k2, heq, _⟩ := hbuild b hb
              show
                (if sqlKeyEq b.1 (some a1, some a2) = true then some (b.2, ip) else none) = none
              by_cases h : sqlKeyEq b.1 (some a1, some a2) = true
              · rw [ite_eq_left h]
                obtain ⟨_, hsome'⟩ := key_eq_of_match hbuild hb h
                rw [hpk] at hsome'
                simp at hsome'
              · rw [ite_eq_right h]
          | some pk =>
              simp only [bloomProbe, refProbe, hpk]
              by_cases
                  hcond : (!bloomMayContain m h (buildBloom lo1 lo2 m h build) pk) = true
              · rw [ite_eq_left hcond]
                refine (filterMap_all_none build _ ?_).symm
                intro b hmem
                show
                  (if sqlKeyEq b.1 (some a1, some a2) = true then some (b.2, ip) else none) = none
                by_cases hk : sqlKeyEq b.1 (some a1, some a2) = true
                · rw [ite_eq_left hk]
                  obtain ⟨hsq, _⟩ := key_eq_of_match hbuild hmem hk
                  have hmay :
                      bloomMayContain m h (buildBloom lo1 lo2 m h build) pk = true := by
                    rw [buildBloom_eq]
                    exact mayContain_of_mem hpk build 0 ⟨b, hmem, hsq⟩
                  rw [hmay] at hcond
                  simp at hcond
                · rw [ite_eq_right hk]
              · rw [ite_eq_right hcond]

theorem sound : TinylambOpt.ResearchGoals.TwoProbePackedBloom.Goal := by
  intro lo1 lo2 m h build probes _ _ hbuild
  rw [bloomJoin_eq, refJoin_eq]
  induction probes with
  | nil => rfl
  | cons p ps ih =>
      rw [List.flatMap_cons, List.flatMap_cons, ih]
      rw [probe_eq_of_valid hbuild p]

end TinylambOpt.ResearchProofs.TwoProbePackedBloom

-- C++ references to this model or its proof.
-- executor/detail/ordered_join_input.cpp
