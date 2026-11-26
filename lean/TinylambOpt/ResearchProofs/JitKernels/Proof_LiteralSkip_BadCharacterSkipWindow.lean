import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

/-! Independent proof record for the LiteralSkip.BadCharacterSkipWindow research goal: the proposed rewrite is sound and preserves the modeled result. -/

/-!
After a bad-character miss, the distance to the last earlier occurrence of the
observed byte in the needle bounds a safe skip: any closer start would force that
byte to match a later needle position, contradicting the compiled distance. This
justifies skipping intermediate start offsets without changing matches.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_LiteralSkip_BadCharacterSkipWindow
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip
open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.BadCharacterSkipWindow
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem prefix_iff (p t : B) : p.isPrefixOf t = true ↔ ∃ u, p ++ u = t := by
  exact List.isPrefixOf_iff_prefix

theorem anchor_of_prefix (p s : B) (i j : Nat) (hj : j < p.length)
    (h : p.isPrefixOf (s.drop i) = true) : anchor p s j i = true := by
  rcases (prefix_iff p (s.drop i)).mp h with ⟨u, hu⟩
  have hget : s[i + j]? = p[j]? := by
    calc
      s[i + j]? = (s.drop i)[j]? := (List.getElem?_drop).symm
      _ = (p ++ u)[j]? := by rw [hu]
      _ = p[j]? := by
        rw [List.getElem?_append]
        simp [hj]
  unfold anchor
  simp [hget]

theorem idxOf_first (l : B) (c : UInt8) (k : Nat) (hk : k < l.idxOf c) :
    l[k]? ≠ some c := by
  induction l generalizing k with
  | nil => simp at hk
  | cons x xs ih =>
      by_cases hxc : x == c
      · have hidx : (x :: xs).idxOf c = 0 := by simp [List.idxOf_cons, hxc]
        omega
      · have hidx : (x :: xs).idxOf c = xs.idxOf c + 1 := by simp [List.idxOf_cons, hxc]
        cases k with
        | zero =>
            intro heq
            simp at heq
            subst c
            simp at hxc
        | succ k' =>
            simp only [List.getElem?_cons_succ]
            apply ih k'
            rw [hidx] at hk
            omega

theorem sound : Goal := by
  intro p s i c hp hc hmiss d hdpos hdlt hj
  let L := p.length
  let R := L - 1
  let rev := p.dropLast.reverse
  let r := rev.idxOf c
  have hLpos : 0 < L := by
    dsimp [L]
    cases p with
    | nil => contradiction
    | cons a as => simp
  have hR : R = p.dropLast.length := by dsimp [R, L]; rw [List.length_dropLast]
  have hrevlen : rev.length = R := by dsimp [rev]; rw [List.length_reverse, ← hR]
  have hrle : r ≤ R := by
    rw [← hrevlen]
    dsimp [r]
    exact List.idxOf_le_length
  have hshift : shift p c = r + 1 := by rfl
  have hdleR : d ≤ R := by omega
  have hdleL1 : d ≤ L - 1 := by simpa [R] using hdleR
  have hkltL : L - 1 - d < L := by omega
  have hjfilter : i + d ∈ (List.range (s.length + 1)).filter
      (fun j => p.isPrefixOf (s.drop j)) := by
    simpa [starts] using hj
  have hjprop := List.mem_filter.mp hjfilter
  have hprefix : p.isPrefixOf (s.drop (i + d)) = true := hjprop.2
  rcases (prefix_iff p (s.drop (i + d))).mp hprefix with ⟨u, hu⟩
  let k := L - 1 - d
  have hkltL : k < L := by dsimp [k]; omega
  have hanch := anchor_of_prefix p s (i + d) k hkltL hprefix
  have hpos_eq : i + d + k = i + p.length - 1 := by dsimp [k, L]; omega
  have hanch' : (p[k]? == s[i + p.length - 1]?) = true := by
    simpa [anchor, hpos_eq] using hanch
  rw [hc] at hanch'
  have hpk : (p[k]?) = some c := by
    simp at hanch'
    exact hanch'
  have hdminus_lt_R : d - 1 < p.dropLast.length := by
    rw [← hR]
    omega
  have hrevget : rev[d - 1]? = some c := by
    have hrevidx : rev[d - 1]? = p.dropLast[p.dropLast.length - 1 - (d - 1)]? := by
      dsimp [rev]
      exact List.getElem?_reverse hdminus_lt_R
    rw [hrevidx]
    have hidxeq : p.dropLast.length - 1 - (d - 1) = k := by
      rw [← hR]
      dsimp [k]
      omega
    rw [hidxeq]
    have hdlget : p.dropLast[k]? = p[k]? := by
      rw [List.getElem?_dropLast]
      simp only [ite_eq_left_iff]
      intro hnot
      have hklt : k < p.length - 1 := by
        dsimp [k, L]
        omega
      exact False.elim (hnot hklt)
    exact hdlget.trans hpk
  have hdminus_lt_r : d - 1 < r := by omega
  exact (idxOf_first rev c (d - 1) hdminus_lt_r) hrevget

end Nov26.Proof_LiteralSkip_BadCharacterSkipWindow
