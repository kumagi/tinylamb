import Std.Tactic

namespace TinylambOpt.SimdGroupHash

def mix64 (key : BitVec 64) : BitVec 64 :=
  let a := key + 0x9e3779b97f4a7c15
  let b := (a ^^^ (a >>> 30)) * 0xbf58476d1ce4e5b9
  let c := (b ^^^ (b >>> 27)) * 0x94d049bb133111eb
  c ^^^ (c >>> 31)

def intHash (seed key : BitVec 64) : BitVec 64 :=
  mix64 (seed ^^^ mix64 key)

theorem int_hash_lane (seed : BitVec 64) (keys : Vector (BitVec 64) n)
    (lane : Nat) (valid : lane < n) :
    (keys.map (intHash seed))[lane] = intHash seed keys[lane] := by
  simp

theorem full_vector_fits (index full total lane : Nat)
    (within : index + 8 ≤ full) (source : full ≤ total) (valid : lane < 8) :
    index + lane < total := by
  omega

theorem decimal_pair_little_endian (a b c d : BitVec 8) :
    let word := a.zeroExtend 32 ||| (b.zeroExtend 32 <<< 8) |||
      (c.zeroExtend 32 <<< 16) ||| (d.zeroExtend 32 <<< 24)
    (word &&& 255).truncate 8 = a ∧
      ((word >>> 8) &&& 255).truncate 8 = b := by
  bv_decide

theorem civil_component (day hour minute second : Nat)
    (validHour : hour < 24) (validMinute : minute < 60) (validSecond : second < 60) :
    let packed := day * 1000000 + hour * 10000 + minute * 100 + second
    (packed / 100) % 100 = minute ∧ (packed / 10000) % 100 = hour := by
  dsimp
  omega

theorem fixed_component_load_fits (start rowEnd bufferEnd offset delta : Nat)
    (canonical : start + 19 ≤ rowEnd) (rowInBuffer : rowEnd ≤ bufferEnd)
    (component : offset = 11 ∨ offset = 14) (byte : delta < 4) :
    start + offset + delta < bufferEnd := by
  omega

theorem sample_window_fits (start finish size byte : Nat)
    (wide : start + 4 ≤ finish) (finishInBuffer : finish ≤ size) (lane : byte < 4) :
    start + byte < size ∧
      (start + (finish - start - 4) / 2) + byte < size ∧
      (finish - 4) + byte < size := by
  omega

theorem short_sample_window (start finish size : Nat)
    (nonempty : start < finish) (finishInBuffer : finish ≤ size) (wide : 4 ≤ size) :
    let base := min start (size - 4)
    base + 4 ≤ size ∧ start - base < 4 ∧ finish ≤ size := by
  dsimp
  omega

theorem short_sample_mask (word : BitVec 32) (length : Nat)
    (short : length = 1 ∨ length = 2 ∨ length = 3) :
    (word &&& ((~~~(0 : BitVec 32)) >>> (((4 - length) % 4) * 8))) =
      word &&& (BitVec.ofNat 32 (256 ^ length - 1)) := by
  rcases short with h | h | h <;> subst length <;> bv_decide

def byteSamples (text : List UInt8) : List UInt8 × List UInt8 × List UInt8 :=
  let extent := text.length - 4
  (text.take 4, (text.drop (extent / 2)).take 4, (text.drop extent).take 4)

theorem fingerprint_congr (a b : List UInt8) (equal : a = b) :
    (a.length, byteSamples a) = (b.length, byteSamples b) :=
  congrArg (fun text => (text.length, byteSamples text)) equal

/-- The partition routing can reorder sightings in other words. Every
    sighting of this word still reaches its single reduction owner. -/
theorem word_partition_sound (rows : List α) (word : α → Nat)
    (capacity target : Nat) :
    ((rows.filter (fun row => word row / capacity == target / capacity)).filter
      (fun row => word row == target)) =
    rows.filter (fun row => word row == target) := by
  rw [List.filter_filter]
  apply List.filter_congr
  intro row _
  by_cases same : word row = target <;> simp [same]

theorem packet_replay_roundtrip (ordinal : BitVec 29) (word : BitVec 25)
    (lanes : BitVec 10) :
    let packet := (ordinal.zeroExtend 64 <<< 35) |||
      (word.zeroExtend 64 <<< 10) ||| lanes.zeroExtend 64
    (packet >>> 35).truncate 29 = ordinal ∧
      (packet >>> 10).truncate 25 = word ∧ packet.truncate 10 = lanes := by
  bv_decide

theorem ordinal_in_block (start count ordinal : Nat)
    (startsHere : start ≤ ordinal) (endsHere : ordinal < start + count) :
    ordinal - start < count ∧ start + (ordinal - start) = ordinal := by
  omega

end TinylambOpt.SimdGroupHash
