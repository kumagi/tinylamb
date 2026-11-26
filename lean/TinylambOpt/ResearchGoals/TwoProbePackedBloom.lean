import Std

/-!
Naive bitwise pack without bias subtraction corrupts the upper 32 bits whenever `k2` is
negative (`-1` has all high bits `1`).

Modeled claims

- `Goal` — Two-probe biased-2x32 packed Bloom filter (`executor/detail/build_input.cpp`
  `packed_bloom`): biasing non-negative offsets within `[0, 2^32)` avoids signed
  sign-extension bit corruption and yields exact SQL equi-join parity for any 64-bit
  hash function and bit width `1 ≤ m ≤ 20`.
-/

namespace TinylambOpt.ResearchGoals.TwoProbePackedBloom

def twoPow32 : Int := 4294967296

def packKey (lo1 lo2 k1 k2 : Int) : Option (BitVec 64) :=
  if decide (0 ≤ k1 - lo1 ∧ k1 - lo1 < twoPow32 ∧ 0 ≤ k2 - lo2 ∧ k2 - lo2 < twoPow32) then
    let d1 : BitVec 64 := BitVec.ofNat 64 (k1 - lo1).toNat
    let d2 : BitVec 64 := BitVec.ofNat 64 (k2 - lo2).toNat
    some ((d1 <<< 32) ||| d2)
  else
    none

def buildBloom (lo1 lo2 : Int) (m : Nat) (h : BitVec 64 → BitVec 64)
    (build : List ((Option Int × Option Int) × Nat)) : Nat :=
  let mask : BitVec 64 := (1#64 <<< m) - 1#64
  build.foldl
    (fun bits rec =>
      match rec.1 with
      | (some k1, some k2) =>
          match packKey lo1 lo2 k1 k2 with
          | some pk =>
              let kh := h pk
              let b0 := (kh &&& mask).toNat
              let b1 := ((kh >>> 24) &&& mask).toNat
              bits ||| (1 <<< b0) ||| (1 <<< b1)
          | none => bits
      | _ => bits)
    0

def bloomMayContain (m : Nat) (h : BitVec 64 → BitVec 64) (bits : Nat)
    (pk : BitVec 64) : Bool :=
  let mask : BitVec 64 := (1#64 <<< m) - 1#64
  let kh := h pk
  let b0 := (kh &&& mask).toNat
  let b1 := ((kh >>> 24) &&& mask).toNat
  ((bits >>> b0) &&& 1) == 1 && ((bits >>> b1) &&& 1) == 1

def sqlKeyEq (a b : Option Int × Option Int) : Bool :=
  match a, b with
  | (some a1, some a2), (some b1, some b2) => a1 == b1 && a2 == b2
  | _, _ => false

def refJoin (build probes : List ((Option Int × Option Int) × Nat)) : List (Nat × Nat) :=
  probes.flatMap fun p =>
    build.filterMap fun b =>
      if sqlKeyEq b.1 p.1 then some (b.2, p.2) else none

def bloomJoin (lo1 lo2 : Int) (m : Nat) (h : BitVec 64 → BitVec 64)
    (build probes : List ((Option Int × Option Int) × Nat)) : List (Nat × Nat) :=
  let bits := buildBloom lo1 lo2 m h build
  probes.flatMap fun p =>
    match p.1 with
    | (some k1, some k2) =>
        match packKey lo1 lo2 k1 k2 with
        | none => []
        | some pk =>
            if !bloomMayContain m h bits pk then
              []
            else
              build.filterMap fun b =>
                if sqlKeyEq b.1 p.1 then some (b.2, p.2) else none
    | _ => []

/-- Two-probe biased-2x32 packed Bloom filter (`executor/detail/build_input.cpp`
    `packed_bloom`): biasing non-negative offsets within `[0, 2^32)` avoids
    signed sign-extension bit corruption and yields exact SQL equi-join parity
    for any 64-bit hash function and bit width `1 ≤ m ≤ 20`. -/
def Goal : Prop :=
  ∀ (lo1 lo2 : Int) (m : Nat) (h : BitVec 64 → BitVec 64)
    (build probes : List ((Option Int × Option Int) × Nat)),
    1 ≤ m → m ≤ 20 →
    (∀ b ∈ build, ∃ k1 k2 : Int,
      b.1 = (some k1, some k2) ∧ (packKey lo1 lo2 k1 k2).isSome = true) →
    bloomJoin lo1 lo2 m h build probes = refJoin build probes

/-- Naive bitwise pack without bias subtraction corrupts the upper 32 bits
    whenever `k2` is negative (`-1` has all high bits `1`). -/
def badPack (k1 k2 : Int) : BitVec 64 :=
  (BitVec.ofInt 64 k1 <<< 32) ||| BitVec.ofInt 64 k2

private def toyHash (x : BitVec 64) : BitVec 64 :=
  x * 0x9E3779B97F4A7C15#64

private def bEx : List ((Option Int × Option Int) × Nat) :=
  [((some (-5), some (-1)), 1), ((some 0, some 7), 2)]

private def pEx : List ((Option Int × Option Int) × Nat) :=
  [((some (-5), some (-1)), 10), ((some (-5), none), 11), ((some 9, some 9), 12)]

example :
    bloomJoin (-10) (-10) 4 toyHash bEx pEx = refJoin bEx pEx := by
  decide

example :
    badPack 1 (-1) = badPack 0 (-1) ∧ packKey 0 (-5) 1 (-1) ≠ packKey 0 (-5) 0 (-1) := by
  decide

end TinylambOpt.ResearchGoals.TwoProbePackedBloom
