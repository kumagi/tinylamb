import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_RejectEarly

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.AsciiOnly

/-! ## Truth check on small inputs (all with q = 0 < reader.states = 2).

We list both ropes whose scan is rejected (hypothesis holds) and ropes whose
scan is accepted (hypothesis fails), together with the value of the claim. -/

private structure Sample where
  a : Rope
  b : Rope

private def samples : List Sample :=
  [ { a := .lit [], b := .lit [65] }
  , { a := .lit [65], b := .lit [200] }
  , { a := .lit [200], b := .lit [65] }
  , { a := .lit [65, 200], b := .lit [66, 255] }
  , { a := .cat (.lit [65]) (.lit [200]), b := .rep 3 (.lit [65]) }
  , { a := .cat (.lit [200]) (.lit [65]), b := .lit [] }
  , { a := .rep 0 (.lit [200]), b := .lit [65] }
  , { a := .rep 2 (.lit [65, 200]), b := .cat (.lit [66]) (.lit [128]) }
  , { a := .rep 4 (.lit []), b := .lit [200] }
  , { a := .cat (.rep 2 (.lit [65])) (.lit [200]), b := .lit [66] }
  ]

private def sampleOK (s : Sample) : Bool :=
  decide (matched reader s.a 0 = false → matched reader (.cat s.a s.b) 0 = false)

private def check : Bool := samples.all sampleOK

#eval samples.map (fun s => (matched reader s.a 0, matched reader (.cat s.a s.b) 0))
#eval check

example : check := by native_decide

/-! ## Shared transition-table and rope facts. -/

private theorem gc_power_length (n : Nat) (t : Table) :
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

private theorem gc_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
  | rep k a ih => simp [compile, gc_power_length, ih]

private theorem gc_applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private def GcClosed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem gc_foldl_closed (r : Reader) (h : GcClosed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem gc_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem gc_power_apply_of (r : Reader) (h : GcClosed r) (x : Rope)
    (ha : ∀ q, q < r.states →
      applyT (compile r x) q = (expand x).foldl r.step q) (n : Nat)
    (q : Nat) (hq : q < r.states) :
    applyT (power n (compile r x)) q =
      ((List.replicate n (expand x)).flatten).foldl r.step q := by
  induction n using Nat.strongRecOn generalizing q with
  | ind n ih =>
      rw [power]
      split
      · rename_i hn
        subst n
        rw [gc_compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [applyT, List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, gc_power_length, gc_compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod0 : n % 2 = 0 := by simpa using heven
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, gc_flatten_replicate_add, List.foldl_append]
          rw [gc_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (gc_foldl_closed r h
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
          rw [gc_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [gc_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (gc_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [gc_flatten_replicate_add, List.foldl_append]
          have hq1 := gc_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := gc_foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem gc_compile_apply (r : Reader) (h : GcClosed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, gc_applyT_chain _ _ q (by simpa [gc_compile_length] using hq),
        iha q hq]
      rw [ihb _ (gc_foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact gc_power_apply_of r h a ih k q hq

private theorem gc_ascii_closed : GcClosed reader := by
  intro q b hq
  have hq' : q < 2 := by simpa [reader] using hq
  change (if b < 128 then q else 1) < 2
  split <;> omega

/-! State 1 is absorbing: no byte can leave it. -/

private theorem step_absorb (b : Nat) : reader.step 1 b = 1 := by
  simp [reader]

private theorem absorb_foldl (s : Bytes) : s.foldl reader.step 1 = 1 := by
  induction s with
  | nil => rfl
  | cons b bs ih =>
      rw [List.foldl_cons, step_absorb, ih]

/-! ## The goal. -/

theorem sound_RejectEarly : RejectEarly.Goal := by
  intro a b h
  have hq0 : 0 < reader.states := by decide
  have hq1 : 1 < reader.states := by decide

  -- A rejection at state 0 means the compiled table sends 0 to 1.
  have hlt : applyT (compile reader a) 0 < 2 := by
    rw [gc_compile_apply reader gc_ascii_closed a 0 hq0]
    simpa [reader] using
      (gc_foldl_closed reader gc_ascii_closed (expand a) 0 hq0)
  have hne : applyT (compile reader a) 0 ≠ 0 := by
    simpa [matched, reader] using h
  have hval : applyT (compile reader a) 0 = 1 := by omega

  -- No suffix can escape the absorbing state 1.
  have hbval : applyT (compile reader b) 1 = 1 := by
    rw [gc_compile_apply reader gc_ascii_closed b 1 hq1]
    exact absorb_foldl (expand b)

  have hchain : applyT (compile reader (.cat a b)) 0 =
      applyT (compile reader b) (applyT (compile reader a) 0) := by
    change applyT (chain (compile reader a) (compile reader b)) 0 =
      applyT (compile reader b) (applyT (compile reader a) 0)
    exact gc_applyT_chain (compile reader a) (compile reader b) 0 (by
      rw [gc_compile_length]
      exact hq0)

  change reader.accept (applyT (compile reader (.cat a b)) 0) = false
  rw [hchain, hval, hbval]
  simp [reader]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_RejectEarly
