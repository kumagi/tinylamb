import Std


-- test hypotheses
example : Float.Model.Format.binary64 = Float.Model.Format.binary64 := rfl

example (f : Float.Model) : f.toBits.toBitVec = Float.Model.UnpackedFloat.pack Float.Model.Format.binary64 f.unpack := by sorry
