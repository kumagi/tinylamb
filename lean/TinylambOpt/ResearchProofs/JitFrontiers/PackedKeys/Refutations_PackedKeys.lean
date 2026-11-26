import TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.BackFields_PackedKeys

/-!
SQL grouping or ordering can use a packed key when the representation preserves the
relevant comparisons and validity bits.

Refuted by counterexample

- `DeltaMagnitude.ReuseForSort` — Strong proposal: the unsigned packed comparison also
  implements SQL lexicographic order.
- `MixedDirection.ReuseForSort` — Strong proposal: the unsigned packed comparison also
  implements SQL lexicographic order.
- `SignedHalves.ReuseForSort` — Strong proposal: the unsigned packed comparison also
  implements SQL lexicographic order.
- `XorSide.ReuseForSort` — Strong proposal: the unsigned packed comparison also
  implements SQL lexicographic order.
- `ZigzagPair.ReuseForSort` — Strong proposal: the unsigned packed comparison also
  implements SQL lexicographic order.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
open TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
open TinylambOpt.ResearchProofs.JitFrontiers.PackedKeys.Common

theorem refuteSignedHalvesSort : ¬ SignedHalves.ReuseForSort.Goal := by
  intro h
  let u := (encode SignedHalves.codec (pair (-1) 0)).get!
  let v := (encode SignedHalves.codec (pair 0 0)).get!
  have hu : encode SignedHalves.codec (pair (-1) 0) = some u := by native_decide
  have hv : encode SignedHalves.codec (pair 0 0) = some v := by native_decide
  have hh := h (pair (-1) 0) (pair 0 0) u v hu hv
  have hbad : ¬ (decide (u < v) = lex (pair (-1) 0) (pair 0 0)) := by native_decide
  exact hbad hh

theorem refuteMixedDirectionSort : ¬ MixedDirection.ReuseForSort.Goal := by
  intro h
  let u := (encode MixedDirection.codec (pair 0 0)).get!
  let v := (encode MixedDirection.codec (pair 0 1)).get!
  have hu : encode MixedDirection.codec (pair 0 0) = some u := by native_decide
  have hv : encode MixedDirection.codec (pair 0 1) = some v := by native_decide
  have hh := h (pair 0 0) (pair 0 1) u v hu hv
  have hbad : ¬ (decide (u < v) = lex (pair 0 0) (pair 0 1)) := by native_decide
  exact hbad hh

theorem refuteZigzagPairSort : ¬ ZigzagPair.ReuseForSort.Goal := by
  intro h
  let u := (encode ZigzagPair.codec (pair 0 0)).get!
  let v := (encode ZigzagPair.codec (pair (-1) 0)).get!
  have hu : encode ZigzagPair.codec (pair 0 0) = some u := by native_decide
  have hv : encode ZigzagPair.codec (pair (-1) 0) = some v := by native_decide
  have hh := h (pair 0 0) (pair (-1) 0) u v hu hv
  have hbad : ¬ (decide (u < v) = lex (pair 0 0) (pair (-1) 0)) := by native_decide
  exact hbad hh

theorem refuteXorSideSort : ¬ XorSide.ReuseForSort.Goal := by
  intro h
  let u := (encode XorSide.codec (pair 1 1)).get!
  let v := (encode XorSide.codec (pair 1 0)).get!
  have hu : encode XorSide.codec (pair 1 1) = some u := by native_decide
  have hv : encode XorSide.codec (pair 1 0) = some v := by native_decide
  have hh := h (pair 1 1) (pair 1 0) u v hu hv
  have hbad : ¬ (decide (u < v) = lex (pair 1 1) (pair 1 0)) := by native_decide
  exact hbad hh

theorem refuteDeltaMagnitudeSort : ¬ DeltaMagnitude.ReuseForSort.Goal := by
  intro h
  let u := (encode DeltaMagnitude.codec (pair 0 0)).get!
  let v := (encode DeltaMagnitude.codec (pair 0 (-1))).get!
  have hu : encode DeltaMagnitude.codec (pair 0 0) = some u := by native_decide
  have hv : encode DeltaMagnitude.codec (pair 0 (-1)) = some v := by native_decide
  have hh := h (pair 0 0) (pair 0 (-1)) u v hu hv
  have hbad : ¬ (decide (u < v) = lex (pair 0 0) (pair 0 (-1))) := by native_decide
  exact hbad hh

