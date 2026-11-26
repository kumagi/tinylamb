import Std

/-!
These claims describe when a compact key preserves the modeled ordering. A SQL sort can
compare packed keys once the component ranges and ordering assumptions hold.

Modeled claims

- `Goal` — `∀ a b, before a b = decide ((encode a).toNat < (encode b).toNat) ∧ (encode
  a = encode b ↔ a.major = b.major ∧ a.minor = b.minor)`
-/

namespace TinylambOpt.ResearchGoals.PackedOrderKey

structure Row where
  major : Option (BitVec 16)
  minor : BitVec 16

-- Signed major DESC NULLS LAST, unsigned minor ASC (also within NULL group).
def before (a b : Row) : Bool :=
  match a.major, b.major with
  | none, none => a.minor.toNat < b.minor.toNat
  | none, some _ => false
  | some _, none => true
  | some x, some y => if x = y then a.minor.toNat < b.minor.toNat else x.toInt > y.toInt

def encode (r : Row) : BitVec 33 :=
  let tag : BitVec 1 := BitVec.ofNat 1 r.major.isNone.toNat
  let normalized : BitVec 16 := r.major.elim 0 (fun x => ~~~(x ^^^ 0x8000))
  tag ++ normalized ++ r.minor

def Goal : Prop := ∀ a b, before a b = decide ((encode a).toNat < (encode b).toNat) ∧
  (encode a = encode b ↔ a.major = b.major ∧ a.minor = b.minor)

example : before ⟨some 0x8000,65535⟩ ⟨none,0⟩ = true := by decide
example : (encode ⟨some 0,65535⟩).toNat < (encode ⟨some 0xffff,0⟩).toNat := by decide

end TinylambOpt.ResearchGoals.PackedOrderKey
