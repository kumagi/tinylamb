import TinylambOpt.ResearchGoals.SubstrAsciiByteSliceEquiv

namespace TinylambOpt.ResearchProofs.SubstrAsciiByteSliceEquiv

open TinylambOpt.ResearchGoals.SubstrAsciiByteSliceEquiv

/-- An ASCII byte is always a code-point start byte: `(b &&& 0xC0) != 0x80`. -/
private theorem ascii_start (b : UInt8) (h : b.toNat < 128) : ((b &&& 0xC0) != 0x80) = true := by
  have hne : (b &&& 0xC0) ≠ 0x80 := by
    intro hc
    have h1 : (b &&& 0xC0).toNat = (0x80 : UInt8).toNat := congrArg UInt8.toNat hc
    rw [UInt8.toNat_and] at h1
    have c0 : (0xC0 : UInt8).toNat = 192 := rfl
    have c1 : (0x80 : UInt8).toNat = 128 := rfl
    rw [c0, c1] at h1
    have hle : b.toNat &&& 192 ≤ b.toNat := Nat.and_le_left
    omega
  rw [bne_eq, Bool.not_eq_true']
  exact beq_eq_false_iff_ne.mpr hne

/-- On a list of start bytes, the code-point splitter emits one singleton per byte,
    flushing the current code point into the accumulator at the end. -/
private theorem go_ascii {bs : List UInt8} (h : ∀ b ∈ bs, ((b &&& 0xC0) != 0x80) = true) :
    ∀ (cur : List UInt8) (acc : List (List UInt8)),
      splitCodePoints.go bs cur acc =
        splitCodePoints.finishCur cur acc ++ bs.map (fun b => [b]) := by
  induction bs with
  | nil =>
      intro cur acc
      rw [List.map_nil, List.append_nil]
      rfl
  | cons b rest ih =>
      intro cur acc
      have hb : ((b &&& 0xC0) != 0x80) = true := h b (List.Mem.head rest)
      have hrest : ∀ x ∈ rest, ((x &&& 0xC0) != 0x80) = true :=
        fun x hx => h x (List.mem_cons_of_mem b hx)
      have hstep : splitCodePoints.go (b :: rest) cur acc =
          (if (b &&& 0xC0) != 0x80 then
              splitCodePoints.go rest [b] (splitCodePoints.finishCur cur acc)
            else
              splitCodePoints.go rest (cur ++ [b]) acc) := rfl
      rw [hstep, ite_eq_left hb, ih hrest [b] (splitCodePoints.finishCur cur acc)]
      have h1 : splitCodePoints.finishCur [b] (splitCodePoints.finishCur cur acc)
          = splitCodePoints.finishCur cur acc ++ [[b]] := rfl
      rw [h1, List.map_cons, List.append_assoc, List.singleton_append]

/-- On an all-ASCII input every byte is its own code point, so splitting yields
    exactly the singleton-per-byte list. -/
private theorem splitCodePoints_ascii {bs : List UInt8} (h : isAsciiStr bs = true) :
    splitCodePoints bs = bs.map (fun b => [b]) := by
  have hc : ∀ b ∈ bs, ((b &&& 0xC0) != 0x80) = true := fun b hb =>
    ascii_start b (of_decide_eq_true (List.all_eq_true.mp h b hb))
  show splitCodePoints.go bs [] [] = _
  rw [go_ascii hc]
  have h0 : splitCodePoints.finishCur ([] : List UInt8) ([] : List (List UInt8)) = [] := rfl
  rw [h0, List.nil_append]

private theorem flatten_map_single (l : List UInt8) : (l.map (fun b => [b])).flatten = l := by
  induction l with
  | nil => rfl
  | cons b t ih =>
      simp only [List.map_cons, List.flatten_cons, List.singleton_append, ih]

theorem sound : TinylambOpt.ResearchGoals.SubstrAsciiByteSliceEquiv.Goal := by
  intro bs start optLen h
  cases optLen with
  | none =>
      simp only [fastAsciiSubstr, refSubstr]
      rw [splitCodePoints_ascii h, List.length_map, ← List.map_drop, flatten_map_single]
  | some l =>
      simp only [fastAsciiSubstr, refSubstr]
      rw [splitCodePoints_ascii h, List.length_map, ← List.map_drop, ← List.map_take,
        flatten_map_single]

end TinylambOpt.ResearchProofs.SubstrAsciiByteSliceEquiv
