import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

namespace TinylambOpt.ResearchProofs.StorageExecution.RankedMasks

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! ## Generic helpers -/

private theorem fromLittle_append (xs : List Nat) (b : Nat) :
    fromLittle (xs ++ [b]) = fromLittle xs + b * 256 ^ xs.length := by
  induction xs with
  | nil => simp [fromLittle]
  | cons x xs ih =>
    simp [fromLittle, ih, Nat.pow_succ]
    ring

private theorem fromLittle_little (n k : Nat) : fromLittle (little n k) = n % 256 ^ k := by
  induction k with
  | zero => simp [little, fromLittle]
  | succ k ih =>
    have hmap : (List.range (k + 1)).map (fun i => n / 256 ^ i % 256) =
        (List.range k).map (fun i => n / 256 ^ i % 256) ++ [n / 256 ^ k % 256] := by
      rw [List.range_succ, List.map_append, List.map_singleton]
    simp only [little, hmap, fromLittle_append]
    have hlen : ((List.range k).map (fun i => n / 256 ^ i % 256)).length = k := by simp
    rw [hlen, ih]
    -- goal: n % 256^k + (n / 256^k % 256) * 256^k = n % 256^(k+1)
    rw [Nat.pow_succ, Nat.mul_comm (256 ^ k) 256]
    -- n % (256*256^k) ? better normalize
    have h256 : (0 : Nat) < 256 := by decide
    have hpk : 0 < 256 ^ k := Nat.pow_pos h256
    have hdm : n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k = n % (256 ^ (k + 1)) := by
      rw [Nat.pow_succ, Nat.mul_comm (256 ^ k) 256]
      -- use Nat.mod_mul_right_div_self / decomposition
      have h1 := Nat.div_add_mod n (256 ^ k)
      have h2 := Nat.div_add_mod (n / 256 ^ k) 256
      -- n = (n/256^k/256*256 + n/256^k%256) * 256^k + n%256^k
      --   = n/256^k/256 * 256^(k+1) + (n/256^k%256 * 256^k + n%256^k)
      have hdecomp : n = n / 256 ^ k / 256 * 256 ^ (k + 1) + (n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k) := by
        rw [Nat.pow_succ, Nat.mul_comm (256 ^ k) 256]
        calc n = n / 256 ^ k * 256 ^ k + n % 256 ^ k := by omega
          _ = (n / 256 ^ k / 256 * 256 + n / 256 ^ k % 256) * 256 ^ k + n % 256 ^ k := by
              conv in (n / 256 ^ k * 256 ^ k) => rw [show n / 256 ^ k = n / 256 ^ k / 256 * 256 + n / 256 ^ k % 256 by omega]
          _ = n / 256 ^ k / 256 * (256 * 256 ^ k) + (n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k) := by ring
      have hbound : n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k < 256 ^ (k + 1) := by
        rw [Nat.pow_succ, Nat.mul_comm (256 ^ k) 256]
        have hm1 : n / 256 ^ k % 256 < 256 := Nat.mod_lt _ h256
        have hm2 : n % 256 ^ k < 256 ^ k := Nat.mod_lt _ hpk
        -- (r1)*256^k + r2 < 256*256^k
        nlinarith [Nat.mod_lt (n / 256 ^ k) h256, Nat.mod_lt n hpk]
      have hmod := Nat.div_add_mod n (256 ^ (k + 1))
      -- uniqueness: n % m = r when n = q*m+r and r<m
      have : n % 256 ^ (k + 1) = n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k := by
        have heq : n / 256 ^ (k + 1) * 256 ^ (k + 1) + n % 256 ^ (k + 1) =
                   n / 256 ^ k / 256 * 256 ^ (k + 1) + (n / 256 ^ k % 256 * 256 ^ k + n % 256 ^ k) := by
          rw [← hmod, ← hdecomp]
        -- from heq and bounds get equality of remainders
        have hdiv_eq : n / 256 ^ (k + 1) = n / 256 ^ k / 256 := by
          rw [Nat.pow_succ, Nat.mul_comm (256 ^ k) 256, Nat.div_div_eq_div_mul]
        omega
      omega
    omega

private theorem word_singleton (w r : Nat) : word w [r] = r % 2 ^ w := by
  simp [word]

private theorem get_pack_singleton (w r : Nat) (hw : r < 2 ^ w) :
    get (pack w [r]) w 0 = r := by
  simp [get, pack, word_singleton, Nat.mod_eq_of_lt hw]
  rw [fromLittle_little]
  have hk : w ≤ 8 * byteCount w := by
    simp [byteCount]; omega
  have hpow : 256 ^ byteCount w = 2 ^ (8 * byteCount w) := by
    have : (256 : Nat) = 2 ^ 8 := by decide
    rw [this, ← Nat.pow_mul]
  rw [hpow]
  have hr : r % 2 ^ (8 * byteCount w) = r :=
    Nat.mod_eq_of_lt (Nat.lt_of_lt_of_le hw (Nat.pow_le_pow_right (by decide) hk))
  simp [hr, Nat.mod_eq_of_lt hw]

private theorem width_lt (n : Nat) : n < 2 ^ width n := by
  simp only [width]
  split
  · rename_i h; simp at h; subst h; decide
  · rename_i h
    have hn : 0 < n := Nat.pos_of_ne_zero (by simpa using h)
    have hlog : n < 2 ^ (n.log2 + 1) := Nat.lt_log2_self
    exact hlog

private theorem codeBits_bound (m : Machine) (n q r : Nat) (hr : r < count m n q) :
    r < 2 ^ codeBits m n q := by
  simp only [codeBits]
  have hlt : count m n q - 1 < 2 ^ width (count m n q - 1) := width_lt _
  omega

