import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution._Scratch

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.Compression.Wire

def iterate {α} (f : α → α) : Nat → α → α
  | 0, x => x
  | n+1, x => f (iterate f n x)

private theorem iterate_add {α} (f : α → α) (a b : Nat) (x : α) :
    iterate f (a+b) x = iterate f a (iterate f b x) := by
  induction a with
  | zero => rw [Nat.zero_add]; simp [iterate]
  | succ a ih => rw [show a + 1 + b = (a + b) + 1 by omega]; simp [iterate, ih]

private theorem power_length (S : Nat) :
    ∀ n t, t.length = S → (power n t).length = S := by
  intro n
  induction n using WellFounded.induction Nat.lt_wfRel.wf with
  | h n ih =>
    intro t ht
    rw [power.eq_1]
    by_cases hzero : n = 0
    · simp [hzero, List.length_range, ht]
    · have ihalf : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
      have hlen_half : (power (n / 2) t).length = S := ih (n/2) ihalf t ht
      by_cases hpar : n % 2 = 0
      · have hparb : (n % 2 == 0) = true := by
          have : n % 2 = 0 := hpar
          simpa [beq_iff_eq] using this
        simp [hzero, hparb, chain, List.length_map, hlen_half]
      · have hparb : (n % 2 == 0) = false := by
          cases h : n % 2 with
          | zero => exfalso; exact hpar h
          | succ m => rfl
        simp [hzero, hparb, chain, List.length_map, hlen_half, ht]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [chain, applyT]
  have hq' : q < a.length := hq
  rw [List.getElem?_eq_getElem hq']
  simp [applyT]

private theorem power_correct (t : Table) (hmap : ∀ y (hy : y < t.length), t[y]'hy < t.length) :
    ∀ n q, q < t.length → applyT (power n t) q = iterate (applyT t) n q := by
  intro n
  induction n using WellFounded.induction Nat.lt_wfRel.wf with
  | h n ih =>
    intro q hq
    rw [power.eq_1]
    by_cases hzero : n = 0
    · simp [hzero, applyT]; rw [List.getElem?_range hq]; simp [iterate]
    · have ihalf : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
      have hlen_half : (power (n / 2) t).length = t.length := power_length t.length (n/2) t (by rfl)
      by_cases hpar : n % 2 = 0
      · have hparb : (n % 2 == 0) = true := by
          have : n % 2 = 0 := hpar
          simpa [beq_iff_eq] using this
        simp [hzero, hparb]
        -- goal: applyT (chain (power (n/2)t)(power (n/2)t)) q = iterate (applyT t) n q
        have hq2 : q < (power (n/2) t).length := by rw [hlen_half]; exact hq
        rw [applyT_chain (power (n/2) t) (power (n/2) t) q hq2]
        -- first half
        have h1 : applyT (power (n/2) t) q = iterate (applyT t) (n/2) q :=
          ih (n/2) ihalf q hq
        rw [h1]
        -- preservation of first half
        have hpres0 : iterate (applyT t) (n/2) q < t.length := by
          induction (n/2) with
          | zero => simpa [iterate] using hq
          | succ m ihp =>
              simp [iterate, applyT]
              rw [List.getElem?_eq_getElem ihp]
              simp
              exact hmap (iterate (applyT t) m q) ihp
        have hq3 : iterate (applyT t) (n/2) q < (power (n/2) t).length := by
          rw [hlen_half]; exact hpres0
        have h2 : applyT (power (n/2) t) (iterate (applyT t) (n/2) q) =
                  iterate (applyT t) (n/2) (iterate (applyT t) (n/2) q) :=
          ih (n/2) ihalf (iterate (applyT t) (n/2) q) hpres0
        rw [h2]
        -- combine halves
        have hn : n/2 + n/2 = n := by omega
        rw [← iterate_add (applyT t) (n/2) (n/2) q, hn]
      · have hparb : (n % 2 == 0) = false := by
          cases h : n % 2 with
          | zero => exfalso; exact hpar h
          | succ m => rfl
        simp [hzero, hparb]
        -- goal: applyT (chain (chain (power (n/2)t)(power (n/2)t)) t) q = iterate (applyT t) n q
        have hq2 : q < (chain (power (n/2) t) (power (n/2) t)).length := by
          simp [chain, List.length_map, hlen_half]
          exact hq
        rw [applyT_chain (chain (power (n/2) t) (power (n/2) t)) t q hq2]
        -- applyT (chain (power...)(power...)) q = iterate (n/2)(iterate (n/2) q)
        have hpres : q < (power (n/2) t).length := by rw [hlen_half]; exact hq
        rw [applyT_chain (power (n/2) t) (power (n/2) t) q hpres]
        have h1 : applyT (power (n/2) t) q = iterate (applyT t) (n/2) q :=
          ih (n/2) ihalf q hq
        rw [h1]
        have hpres0 : iterate (applyT t) (n/2) q < t.length := by
          induction (n/2) with
          | zero => simpa [iterate] using hq
          | succ m ihp =>
              simp [iterate, applyT]
              rw [List.getElem?_eq_getElem ihp]
              simp
              exact hmap (iterate (applyT t) m q) ihp
        have hq3 : iterate (applyT t) (n/2) q < (power (n/2) t).length := by
          rw [hlen_half]; exact hpres0
        have h2 : applyT (power (n/2) t) (iterate (applyT t) (n/2) q) =
                  iterate (applyT t) (n/2) (iterate (applyT t) (n/2) q) :=
          ih (n/2) ihalf (iterate (applyT t) (n/2) q) hpres0
        rw [h2]
        rw [show n = 2*(n/2)+1 by omega]
        rw [iterate_add (applyT t) (n/2) (n/2) q]
        rfl

end TinylambOpt.ResearchProofs.StorageExecution._Scratch
