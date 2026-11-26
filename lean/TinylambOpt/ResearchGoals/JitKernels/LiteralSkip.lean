import Std

/-! Specialized literal search kernels enumerate candidate start offsets and verify anchors. Empty needles have a boundary at every position; a chunk owns start offsets, not wholly contained matches. -/
namespace TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

abbrev B := List UInt8
def starts (p s : B) := (List.range (s.length+1)).filter (fun i => p.isPrefixOf (s.drop i))
def anchor (p s : B) (j i : Nat) := p[j]? == s[i+j]?
def cand (p s : B) (j : Nat) := (List.range (s.length+1-p.length)).filter (anchor p s j)
def shift (p : B) (c : UInt8) := ((p.dropLast.reverse.idxOf c)+1)

/-- Verify only starts nominated by a single compiled literal byte and the remaining-length bound. -/
def AnchorVerifyAllHits.Goal : Prop := ∀ p s j, j < p.length → ((cand p s j).filter (fun i => p.isPrefixOf (s.drop i))) = starts p s

/-- Checking both endpoints suffices for a literal whose bytes are pairwise distinct. -/
def EndAnchorsReplaceInterior.Goal : Prop := ∀ p s, p.Nodup → 2 ≤ p.length → ((cand p s 0).filter (anchor p s (p.length-1))) = starts p s

/-- After reading the trailing haystack byte, skip intermediate starts by the distance to its last earlier needle occurrence. -/
def BadCharacterSkipWindow.Goal : Prop := ∀ p s i c, p ≠ [] → s[i+p.length-1]? = some c → i ∉ starts p s →
  ∀ d, 0 < d → d < shift p c → i+d ∉ starts p s

/-- Advance by the whole needle after a hit while enumerating all matching positions. -/
def SuccessfulMatchLengthAdvance.Goal : Prop := ∀ p s i j, p ≠ [] → i ∈ starts p s → j ∈ starts p s → i < j → i+p.length ≤ j

/-- The length jump is safe if the compiled needle has no proper nonempty prefix that is also a suffix. -/
def UnborderedHitAdvance.Goal : Prop := ∀ p s i j, (∀ k, 0 < k → k < p.length → p.take k ≠ p.drop (p.length-k)) →
  i ∈ starts p s → j ∈ starts p s → i < j → i+p.length ≤ j

/-- Return the first candidate without verifying it when the chosen anchor occurs just once in the haystack. -/
def FirstHitFromRareAnchor.Goal : Prop := ∀ p s j, j < p.length → (cand p s j).length = 1 → (cand p s j).head? = (starts p s).head?

/-- A chunk owning starts before the next boundary needs at most needle length minus one successor bytes. -/
def LiteralChunkHaloOwnership.Goal : Prop := ∀ p a b, p ≠ [] → ((starts p (a++b)).filter (· < a.length)) =
  (starts p (a++b.take (p.length-1))).filter (· < a.length)

/-- The least frequent needle byte in the whole buffer always yields the fewest position-valid candidates. -/
def SparseAnchorCostChoice.Goal : Prop := ∀ p s j k x y, p[j]? = some x → p[k]? = some y → s.count x ≤ s.count y →
  (cand p s j).length ≤ (cand p s k).length

/-- Enumerate starts of a longer literal using the hit positions already generated for its short prefix. -/
def SharedNeedlePrefixFilter.Goal : Prop := ∀ (a b s : B), ((starts a s).filter (fun i => b.isPrefixOf (s.drop (i+a.length)))) = starts (a++b) s

/-- Two per-chunk misses allow bypassing the literal search over the concatenated input. -/
def CombinedChunkFirstMiss.Goal : Prop := ∀ p a b, starts p a = [] → starts p b = [] → starts p (a++b) = []

end TinylambOpt.ResearchGoals.JitKernels.LiteralSkip
