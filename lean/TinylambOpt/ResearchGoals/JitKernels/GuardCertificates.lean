import Std

/-! A range/congruence domain for lowering INT64 arithmetic guards. Residues describe correlated row sets only when explicitly supplied; endpoint constraints apply to exact mathematical integers. -/
namespace TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

structure Domain where
  lo : Int
  hi : Int
  modulus : Nat
  residue : Int
def inside (d : Domain) (x : Int) :=
  d.lo ≤ x ∧ x ≤ d.hi ∧ (x-d.residue) % (d.modulus:Int) = 0
def first (d : Domain) := d.lo + (d.residue-d.lo) % (d.modulus:Int)
def last (d : Domain) := d.hi - (d.hi-d.residue) % (d.modulus:Int)
def fits (w : Nat) (x : Int) := -(2^w:Int) ≤ x ∧ x < (2^w:Int)

/-- Use a rounded first representative to detect impossible branches at compile time. -/
def StridedRangeNonempty.Goal : Prop := ∀ d, 0 < d.modulus → ((∃ x, inside d x) ↔ first d ≤ d.hi)

/-- Guard a multiply-add from the first and last realizable values, eliminating impossible interval endpoints. -/
def CongruenceTightAffineGuard.Goal : Prop := ∀ d m c w x, 0 < d.modulus → inside d x → fits w (m*first d+c) → fits w (m*last d+c) → fits w (m*x+c)

/-- Corner evaluations suffice for a quadratic polynomial created by fusing two dependent multiplies. -/
def EndpointsCertifyCheckedSquareDifference.Goal : Prop := ∀ d c w x, 0 < d.modulus → inside d x → fits w (first d*(c-first d)) →
  fits w (last d*(c-last d)) → fits w (x*(c-x))

/-- Hoist the divide-by-zero guard when the denominator class cannot include zero even though its bounds cross zero. -/
def CongruenceRulesOutZeroDivisor.Goal : Prop := ∀ d x, 0 < d.modulus → d.residue % (d.modulus:Int) ≠ 0 → inside d x → x ≠ 0

/-- For compatible domains, choosing the larger stride as their intersection is exact. -/
def IntersectionByLargerStride.Goal : Prop := ∀ a b x, 0 < a.modulus → a.modulus ≤ b.modulus → (a.residue-b.residue) % (a.modulus:Int) = 0 →
  inside b x → a.lo ≤ x → x ≤ a.hi → inside a x

/-- Replace signed remainder checks by a class test before specializing exact division across a batch. -/
def SharedResidueExactDivision.Goal : Prop := ∀ d k x, 0 < k → (k:Nat) ∣ d.modulus → d.residue % (k:Int) = 0 →
  inside d x → (x.tdiv k)*(k:Int) = x

/-- A fused affine expression inherits a multiplied modulus, so downstream divisibility can be tested before evaluating it. -/
def AffineCongruenceTransfer.Goal : Prop := ∀ d (m c x : Int), m ≠ 0 → 0 < d.modulus → inside d x →
  (m*x+c-(m*d.residue+c)) % ((m.natAbs*d.modulus:Nat):Int) = 0

/-- Lower truncating division to an arithmetic right shift throughout any class with stride equal to a power of two. -/
def SignedDivisionAcrossZero.Goal : Prop := ∀ d x k, d.modulus = 2^k → inside d x → x.tdiv (2^k) = x / (2^k)

end TinylambOpt.ResearchGoals.JitKernels.GuardCertificates
