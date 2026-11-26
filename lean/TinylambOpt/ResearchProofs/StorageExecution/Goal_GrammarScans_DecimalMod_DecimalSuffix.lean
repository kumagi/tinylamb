import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_DecimalMod_DecimalSuffix

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.DecimalMod

/-! ## Truth check on small inputs.

The hypothesis `q < 7` restricts us to non-sink states; inputs with `x ≥ 10`
or `y ≥ 10` are outside the goal's hypothesis and must not be counted as
counterexamples. -/

private def dms_sampleOK (a : Rope) (x y : Nat) : Bool :=
  let q := applyT (compile reader a) 0
  decide (x < 10 → y < 10 → q < 7 →
    applyT (compile reader (.cat a (.lit [48 + x, 48 + y]))) 0 =
      (100 * q + 10 * x + y) % 7)

private def dms_samples : List Rope :=
  [ .lit []
  , .lit [49, 50]
  , .rep 2 (.lit [49, 50])
  , .cat (.lit [48, 49]) (.lit [50, 51])
  , .rep 3 (.lit [48])
  ]

private def dms_check : Bool :=
  dms_samples.all (fun a =>
    (List.range 12).all (fun x => (List.range 12).all (fun y => dms_sampleOK a x y)))

#eval dms_check

example : dms_check := by native_decide

/-! ## Shared transition-table facts (closed-step readers). -/

private theorem dms_power_length (n : Nat) (t : Table) :
    (power n t).length = t.length := by
  induction n using Nat.strongRecOn generalizing t with
  | ind n ih =>
      unfold power
      split
      · simp [List.length_range]
      · rename_i hn
        split
        · simp only [chain, List.length_map]
          exact ih (n / 2) (Nat.div_lt_self (by omega) (by decide)) t
        · simp only [chain, List.length_map]
          exact ih (n / 2) (Nat.div_lt_self (by omega) (by decide)) t

private theorem dms_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
  | rep k a ih => simp [compile, dms_power_length, ih]

private theorem dms_applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private def dms_IsClosed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem dms_foldl_closed (r : Reader) (h : dms_IsClosed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem dms_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem dms_power_apply_of (r : Reader) (h : dms_IsClosed r) (x : Rope)
    (ha : ∀ q, q < r.states → applyT (compile r x) q = (expand x).foldl r.step q)
    (n : Nat) (q : Nat) (hq : q < r.states) :
    applyT (power n (compile r x)) q =
      ((List.replicate n (expand x)).flatten).foldl r.step q := by
  induction n using Nat.strongRecOn generalizing q with
  | ind n ih =>
      rw [power]
      split
      · rename_i hn
        subst n
        rw [dms_compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [applyT, List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, dms_power_length, dms_compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod0 : n % 2 = 0 := by simpa using heven
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, dms_flatten_replicate_add, List.foldl_append]
          rw [dms_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (dms_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)
        · rename_i hodd
          change applyT (chain (chain ht ht) (compile r x)) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod1 : n % 2 = 1 := by
            have hne : n % 2 ≠ 0 := by
              intro hz
              apply hodd
              simp [hz]
            have hlt := Nat.mod_lt n (by decide : 0 < 2)
            omega
          have heq : n = m + m + 1 := by
            change n = n / 2 + n / 2 + 1
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, List.replicate_succ', List.flatten_append]
          simp only [List.flatten_cons, List.flatten_nil, List.nil_append]
          rw [List.foldl_append, List.foldl_append]
          rw [dms_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [dms_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (dms_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [dms_flatten_replicate_add, List.foldl_append]
          have hq1 := dms_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := dms_foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem dms_compile_apply (r : Reader) (h : dms_IsClosed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, dms_applyT_chain _ _ q (by simpa [dms_compile_length] using hq),
        iha q hq]
      rw [ihb _ (dms_foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact dms_power_apply_of r h a ih k q hq

/-! ## DecimalMod specifics. -/

private theorem dms_closed : dms_IsClosed reader := by
  intro q b hq
  have hq8 : q < 8 := by simpa [reader] using hq
  change (if q == 7 || b < 48 || 57 < b then 7 else (10 * q + b - 48) % 7) < 8
  split
  · omega
  · have hm := Nat.mod_lt (10 * q + b - 48) (by decide : 0 < 7)
    omega

private theorem dms_step_digit (q x : Nat) (hq : q < 7) (hx : x < 10) :
    reader.step q (48 + x) = (10 * q + x) % 7 := by
  have hb1 : ¬ (48 + x < 48) := by omega
  have hb2 : ¬ (57 < 48 + x) := by omega
  have hqne : q ≠ 7 := by omega
  have hq7 : (q == 7) = false := by simp [hqne]
  have hsub : 10 * q + (48 + x) - 48 = 10 * q + x := by omega
  simp [reader, hq7, hqne, hb1, hb2, hsub]

theorem sound_DecimalSuffix : DecimalMod.DecimalSuffix.Goal := by
  intro a x y hx hy q hq7
  have hq0 : (0 : Nat) < reader.states := by decide
  rw [dms_compile_apply reader dms_closed (.cat a (.lit [48 + x, 48 + y])) 0 hq0]
  simp [expand, List.foldl_append, List.foldl_cons, List.foldl_nil]
  rw [← dms_compile_apply reader dms_closed a 0 hq0]
  change reader.step (reader.step q (48 + x)) (48 + y) =
    (100 * q + 10 * x + y) % 7
  rw [dms_step_digit q x hq7 hx]
  have hq1 : (10 * q + x) % 7 < 7 := Nat.mod_lt _ (by decide)
  rw [dms_step_digit ((10 * q + x) % 7) y hq1 hy]
  have hmod : (10 * ((10 * q + x) % 7) + y) % 7 = (100 * q + 10 * x + y) % 7 := by
    omega
  exact hmod

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_DecimalMod_DecimalSuffix
