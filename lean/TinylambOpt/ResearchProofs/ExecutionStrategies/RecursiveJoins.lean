import TinylambOpt.ResearchGoals.ExecutionStrategies.RecursiveJoins

namespace TinylambOpt.ResearchProofs.ExecutionStrategies.RecursiveJoins

open TinylambOpt.ResearchGoals.RecursiveJoins
open TinylambOpt.ResearchGoals.RecursiveDelta
open List

/-- A single edge is a one-step path. -/
private theorem reach_edge {g : Graph} {u v : Nat} (h : (u,v) ∈ g) : path g u v := by
  exact Reach.step (Reach.seed (List.mem_singleton_self u)) h

/-- Reachability is monotone in the edge set. -/
private theorem reach_mono {g g' : Graph} {seeds : List Nat} {v : Nat}
    (hsub : ∀ e ∈ g, e ∈ g') (h : Reach g seeds v) : Reach g' seeds v := by
  induction h with
  | seed hs => exact Reach.seed hs
  | step hu he ih => exact Reach.step ih (hsub _ he)

/-- Concatenating a path `seeds → u` with a path `u → v` gives `seeds → v`. -/
private theorem reach_trans {edges : List Edge} {seeds : List Nat} {u v : Nat}
    (h1 : Reach edges seeds u) (h2 : Reach edges [u] v) : Reach edges seeds v := by
  induction h2 with
  | seed hs =>
      cases List.mem_singleton.mp hs
      exact h1
  | step h2a hedge ih =>
      exact Reach.step ih hedge

/-- Inserting one edge `(u,v)` closes reachability exactly when the old graph
already reaches `u` and `v` reaches `t`.  The `↔` is proved by induction on the
`Reach` derivation; the converse reuses the two halves of the disjunction. -/
theorem sound_InsertClosure : InsertClosure.Goal := by
  intro g u v s t
  constructor
  · intro h
    induction h with
    | seed hs =>
        cases List.mem_singleton.mp hs
        exact Or.inl (Reach.seed (List.mem_singleton_self s))
    | step h1 hedge ih =>
        rcases List.mem_cons.mp hedge with heq | hge
        · -- the step uses the inserted edge `(u,v)` itself
          injection heq with hxu htv
          subst hxu
          subst htv
          rcases ih with hleft | ⟨hmid, -⟩
          · exact Or.inr ⟨hleft, Reach.seed (List.mem_singleton_self _)⟩
          · exact Or.inr ⟨hmid, Reach.seed (List.mem_singleton_self _)⟩
        · -- the step uses an old edge of `g`
          rcases ih with hleft | ⟨hmid, hright⟩
          · exact Or.inl (Reach.step hleft hge)
          · exact Or.inr ⟨hmid, Reach.step hright hge⟩
  · rintro (h | ⟨h1, h2⟩)
    · exact reach_mono (fun e he => List.mem_cons_of_mem _ he) h
    · have hsu : path ((u,v)::g) s u :=
        reach_mono (fun e he => List.mem_cons_of_mem _ he) h1
      have hstep : Reach ((u,v)::g) [s] v := Reach.step hsu List.mem_cons_self
      have hvt : Reach ((u,v)::g) [v] t :=
        reach_mono (fun e he => List.mem_cons_of_mem _ he) h2
      exact reach_trans hstep hvt

/-- Batch transitive reduction: every removed edge has a witness path inside the
kept graph, so reachability is unchanged.  Forward direction inducts on the
`Reach` derivation and splices witness paths in place of removed edges. -/
theorem sound_RedundantEdges : RedundantEdges.Goal := by
  intro kept removed s t hred
  constructor
  · intro h
    induction h with
    | seed hs => exact Reach.seed hs
    | step h1 hedge ih =>
        rcases List.mem_append.mp hedge with hk | hr
        · exact Reach.step ih hk
        · exact reach_trans ih (hred _ hr)
  · intro h
    exact reach_mono (fun e he => List.mem_append_left _ he) h

/-- The quotient graph of `QuotientClosure`: projected edges, deduplicated,
self-loops filtered out. -/
private abbrev quotientEdges (g : Graph) (label : Nat → Nat) : Graph :=
  (g.map (fun (u,v) => (label u,label v))).eraseDups.filter (fun (u,v) => u != v)

/-- Quotient reachability: a `g`-path maps to a quotient path on labels, and
every quotient edge lifts back through `hlabel` because equal labels are
connected inside `g`. -/
theorem sound_QuotientClosure : QuotientClosure.Goal := by
  intro g label s t hlabel
  show path g s t ↔ path (quotientEdges g label) (label s) (label t)
  constructor
  · -- forward: a g-path maps to an edges-path on labels
    have lift : ∀ {seeds : List Nat} {v : Nat}, Reach g seeds v →
        Reach (quotientEdges g label) (seeds.map label) (label v) := by
      intro seeds v h
      induction h with
      | seed hs =>
          apply Reach.seed
          exact List.mem_map.mpr ⟨_, hs, rfl⟩
      | step h1 hedge ih =>
          rename_i x y
          by_cases heq : label x = label y
          · rw [← heq]
            exact ih
          · have hmap : (label x, label y) ∈
                g.map (fun (u,v) => (label u,label v)) :=
              List.mem_map.mpr ⟨(x,y), hedge, rfl⟩
            have hq : (label x, label y) ∈ quotientEdges g label := by
              show (label x, label y) ∈
                (g.map (fun (u,v) => (label u,label v))).eraseDups.filter
                  (fun (u,v) => u != v)
              rw [List.mem_filter, List.mem_eraseDups]
              exact ⟨hmap, by simp [heq]⟩
            exact Reach.step ih hq
    intro h
    have h' := lift (seeds := [s]) (v := t) h
    simpa only [List.map_cons, List.map_nil] using h'
  · -- backward: lift a quotient path to a g-path
    have wit : ∀ {a b : Nat}, (a,b) ∈ quotientEdges g label →
        ∃ u v, (u,v) ∈ g ∧ label u = a ∧ label v = b := by
      intro a b hab
      change (a,b) ∈ (g.map (fun (u,v) => (label u,label v))).eraseDups.filter
        (fun (u,v) => u != v) at hab
      rw [List.mem_filter, List.mem_eraseDups] at hab
      obtain ⟨hmap, -⟩ := hab
      obtain ⟨p, hp, hpe⟩ := List.mem_map.mp hmap
      obtain ⟨u, v⟩ := p
      have hpe' : (label u, label v) = (a, b) := hpe
      obtain ⟨e1, e2⟩ := Prod.ext_iff.mp hpe'
      exact ⟨u, v, hp, e1, e2⟩
    have back : ∀ {a : Nat}, Reach (quotientEdges g label) [label s] a →
        ∃ w, label w = a ∧ path g s w := by
      intro a h
      induction h with
      | seed hs =>
          cases List.mem_singleton.mp hs
          exact ⟨s, rfl, Reach.seed (List.mem_singleton_self s)⟩
      | step h1 hedge ih =>
          rename_i x y
          obtain ⟨w, hw, hwp⟩ := ih
          obtain ⟨u, v, huv, hlu, hlv⟩ := wit hedge
          have hwu : path g w u := hlabel w u (hw.trans hlu.symm)
          have huv' : path g u v := reach_edge huv
          have hsu : path g s u := reach_trans hwp hwu
          have hsv : path g s v := reach_trans hsu huv'
          exact ⟨v, hlv, hsv⟩
    intro h
    obtain ⟨w, hw, hwp⟩ := back h
    exact reach_trans hwp (hlabel w t hw)

/-- Exact-length walks, one edge appended at the end. -/
private inductive WalkLen (g : Graph) : Nat → Nat → Nat → Prop where
  | nil {s : Nat} : WalkLen g 0 s s
  | snoc {s w x : Nat} {d : Nat} : WalkLen g d s w → (w,x) ∈ g → WalkLen g (d+1) s x

private theorem walkLen_zero {g : Graph} {s x : Nat} :
    WalkLen g 0 s x ↔ s = x := by
  constructor
  · intro h; cases h; rfl
  · intro h; subst h; exact WalkLen.nil

private theorem walkLen_snoc_iff {g : Graph} {d s x : Nat} :
    WalkLen g (d+1) s x ↔ ∃ w, WalkLen g d s w ∧ (w,x) ∈ g := by
  constructor
  · intro h
    cases h with
    | snoc h1 h2 => exact ⟨_, h1, h2⟩
  · rintro ⟨w, h1, h2⟩; exact WalkLen.snoc h1 h2

/-- First-edge form of an exact-length walk. -/
private theorem walkLen_cons_iff {g : Graph} (d s x : Nat) :
    WalkLen g (d+1) s x ↔ ∃ u, (s,u) ∈ g ∧ WalkLen g d u x := by
  induction d generalizing s x with
  | zero =>
      rw [walkLen_snoc_iff]
      constructor
      · rintro ⟨w, hw, hwx⟩
        obtain rfl := walkLen_zero.mp hw
        exact ⟨x, hwx, WalkLen.nil⟩
      · rintro ⟨u, hsu, hu⟩
        obtain rfl := walkLen_zero.mp hu
        exact ⟨s, WalkLen.nil, hsu⟩
  | succ d ih =>
      rw [walkLen_snoc_iff]
      constructor
      · rintro ⟨w, hw, hwx⟩
        rw [ih s w] at hw
        obtain ⟨u, hsu, huw⟩ := hw
        refine ⟨u, hsu, ?_⟩
        rw [walkLen_snoc_iff]
        exact ⟨w, huw, hwx⟩
      · rintro ⟨u, hsu, hux⟩
        rw [walkLen_snoc_iff] at hux
        obtain ⟨w, huw, hwx⟩ := hux
        refine ⟨w, ?_, hwx⟩
        rw [ih s w]
        exact ⟨u, hsu, huw⟩

/-- `endsAt` membership is exactly an exact-length walk. -/
private theorem walkLen_iff_endsAt {g : Graph} (d s x : Nat) :
    x ∈ endsAt g s d ↔ WalkLen g d s x := by
  induction d generalizing s x with
  | zero =>
      rw [endsAt.eq_1]
      rw [walkLen_zero]
      rw [List.mem_singleton]
      exact ⟨Eq.symm, Eq.symm⟩
  | succ d ih =>
      rw [endsAt.eq_2, List.mem_flatMap]
      constructor
      · rintro ⟨u, hu, hx⟩
        rw [List.mem_map] at hx
        obtain ⟨e, he, hxe⟩ := hx
        rw [List.mem_filter] at he
        obtain ⟨heg, heq⟩ := he
        have he1 : e.1 = u := beq_iff_eq.mp heq
        have he2 : e.2 = x := hxe
        obtain ⟨a, b⟩ := e
        have ha : a = u := beq_iff_eq.mp heq
        have hb : b = x := hxe
        have hux : (u,x) ∈ g := by rw [← ha, ← hb]; exact heg
        have hwu : WalkLen g d s u := (ih s u).mp hu
        exact WalkLen.snoc hwu hux
      · intro h
        rw [walkLen_snoc_iff] at h
        obtain ⟨u, hwu, hux⟩ := h
        refine ⟨u, ?_, ?_⟩
        · exact (ih s u).mpr hwu
        · rw [List.mem_map]
          refine ⟨(u,x), ?_, rfl⟩
          rw [List.mem_filter]
          refine ⟨hux, ?_⟩
          exact beq_iff_eq.mpr rfl

/-- Membership in the transposed edge list flips the endpoints. -/
private theorem mem_transpose {g : Graph} {a b : Nat} :
    (a,b) ∈ transpose g ↔ (b,a) ∈ g := by
  unfold transpose
  rw [List.mem_map]
  constructor
  · rintro ⟨e, he, h⟩
    obtain ⟨p, q⟩ := e
    have h2 : (q,p) = (a,b) := h
    injection h2 with h1 h3
    subst h1; subst h3
    exact he
  · intro h
    exact ⟨(b,a), h, rfl⟩

/-- Walks in the transpose graph are walks in the original graph read backwards. -/
private theorem walkLen_transpose {g : Graph} (d a b : Nat) :
    WalkLen (transpose g) d a b ↔ WalkLen g d b a := by
  induction d generalizing a b with
  | zero =>
      rw [walkLen_zero, walkLen_zero]
      exact ⟨Eq.symm, Eq.symm⟩
  | succ d ih =>
      rw [walkLen_cons_iff]
      constructor
      · rintro ⟨u, hu, hub⟩
        rw [mem_transpose] at hu
        rw [ih u b] at hub
        rw [walkLen_snoc_iff]
        exact ⟨u, hub, hu⟩
      · intro h
        rw [walkLen_snoc_iff] at h
        obtain ⟨w, hbw, hwa⟩ := h
        refine ⟨w, ?_, ?_⟩
        · exact (mem_transpose).mpr hwa
        · exact (ih w b).mpr hbw

/-- Splitting an exact-length walk at position `l`. -/
private theorem walkLen_split {g : Graph} (r : Nat) :
    ∀ (l s t : Nat), WalkLen g (l+r) s t ↔
      ∃ v, WalkLen g l s v ∧ WalkLen g r v t := by
  induction r with
  | zero =>
      intro l s t
      constructor
      · intro h
        exact ⟨t, h, WalkLen.nil⟩
      · rintro ⟨v, hv, hvt⟩
        obtain rfl := walkLen_zero.mp hvt
        exact hv
  | succ r ih =>
      intro l s t
      have hlen : l + (r+1) = (l+r)+1 := by omega
      rw [hlen, walkLen_snoc_iff]
      constructor
      · rintro ⟨w, hw, hwt⟩
        rw [ih l s w] at hw
        obtain ⟨v, hv, hvw⟩ := hw
        exact ⟨v, hv, WalkLen.snoc hvw hwt⟩
      · rintro ⟨v, hv, hvt⟩
        rw [walkLen_snoc_iff] at hvt
        obtain ⟨w, hvw, hwt⟩ := hvt
        refine ⟨w, ?_, hwt⟩
        rw [ih l s w]
        exact ⟨v, hv, hvw⟩

theorem sound_MeetFrontiers : MeetFrontiers.Goal := by
  intro g s t l r
  simp only [walkLen_iff_endsAt]
  rw [walkLen_split r l s t]
  constructor
  · rintro ⟨v, hv1, hv2⟩
    exact ⟨v, hv1, (walkLen_transpose r t v).mpr hv2⟩
  · rintro ⟨v, hv1, hv2⟩
    exact ⟨v, hv1, (walkLen_transpose r t v).mp hv2⟩


private theorem option_bind_id {o : Option Nat} : o.bind (fun v => some v) = o := by
  cases o <;> rfl

/-- `steps f s (n+1)` applies `f` first, then `n` more steps. -/
private theorem steps_succ_first {f : Nat → Option Nat} (s n : Nat) :
    steps f s (n+1) = (f s).bind (fun v => steps f v n) := by
  induction n with
  | zero =>
      rw [steps.eq_2, steps.eq_1, Option.bind_some]
      exact (option_bind_id).symm
  | succ n ih =>
      rw [steps.eq_2, ih, Option.bind_assoc]
      have hfun : (fun y => (steps f y n).bind f) = (fun v => steps f v (n+1)) := by
        funext v
        exact (steps.eq_2 f v n).symm
      rw [hfun]

/-- Iterating the pointwise square `v ↦ f (f v)` `m` times equals `2*m` steps of `f`. -/
private theorem steps_sq {f : Nat → Option Nat} (s m : Nat) :
    steps (fun v => (f v).bind f) s m = steps f s (2*m) := by
  induction m with
  | zero =>
      show steps (fun v => (f v).bind f) s 0 = steps f s 0
      rw [steps.eq_1, steps.eq_1]
  | succ m ih =>
      have hmul : 2 * (m+1) = (2*m+1)+1 := by omega
      rw [steps.eq_2, ih, hmul, steps.eq_2, steps.eq_2, ← Option.bind_assoc]

theorem sound_BinaryLifting : BinaryLifting.Goal := by
  intro f k s d
  induction d generalizing f k s with
  | zero =>
      intro hk
      have hk1 : k < 1 := hk
      have hk0 : k = 0 := by omega
      subst hk0
      rw [liftJump.eq_1, steps.eq_1]
  | succ d ih =>
      intro hk
      rw [liftJump.eq_2]
      have hkd : k / 2 < 2^d := by
        rw [Nat.pow_succ] at hk
        rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 2)]
        omega
      have hfun : liftJump (fun v => (f v).bind f) (k/2) d =
          fun x => steps f x (2*(k/2)) :=
        funext fun x => (ih (fun v => (f v).bind f) (k/2) x hkd).trans (steps_sq x (k/2))
      rw [hfun]
      split
      · rename_i hcond
        have hodd : k % 2 = 1 := beq_iff_eq.mp hcond
        have hko : k = 2*(k/2)+1 := by omega
        have key : steps f s (2*(k/2)+1) = steps f s k := by rw [← hko]
        exact ((steps_succ_first s (2*(k/2))).symm).trans key
      · rename_i hcond
        have hodd : ¬ k % 2 = 1 := fun h => hcond (beq_iff_eq.mpr h)
        have hke : k = 2*(k/2) := by omega
        have key : steps f s (2*(k/2)) = steps f s k := by rw [← hke]
        rw [Option.bind_some]
        show steps f s (2*(k/2)) = steps f s k
        exact key


/-- Forward direction of connector suppression, with invariant:
either the vertex is reachable in the contracted graph, or it is `w`
(reached only via the `u → w` detour, so `u` is reachable). -/
private theorem suppress_fwd {g : Graph} {s u v w : Nat}
    (hw : ∀ e ∈ g, e.1 ≠ w ∧ e.2 ≠ w) (hws : w ≠ s) (hwu : w ≠ u) (hwv : w ≠ v) :
    ∀ {x : Nat}, Reach ((u,w)::(w,v)::g) [s] x →
      (x ≠ w → path ((u,v)::g) s x) ∧ (x = w → path ((u,v)::g) s u) := by
  intro x h
  induction h with
  | seed hs =>
      constructor
      · intro _; exact Reach.seed hs
      · intro hvw
        rw [List.mem_singleton.mp hs] at hvw
        exact absurd hvw.symm hws
  | step h1 hedge ih =>
      rename_i a b
      rcases List.mem_cons.mp hedge with heq | hrest
      · -- (a,b) = (u,w): target is w
        injection heq with ha hb
        subst a; subst b
        constructor
        · intro hbw; exact absurd rfl hbw
        · intro _; exact ih.1 (Ne.symm hwu)
      · rcases List.mem_cons.mp hrest with heq2 | hg
        · -- (a,b) = (w,v): target is v
          injection heq2 with ha hb
          subst a; subst b
          constructor
          · intro _; exact Reach.step (ih.2 rfl) List.mem_cons_self
          · intro hvw; exact absurd hvw.symm hwv
        · -- (a,b) ∈ g: ordinary edge, w untouched
          have hab := hw (a,b) hg
          constructor
          · intro _
            exact Reach.step (ih.1 hab.1) (List.mem_cons_of_mem _ hg)
          · intro hbw; exact absurd hbw hab.2

theorem sound_SuppressConnector : SuppressConnector.Goal := by
  intro g s t u v w hws hwt hwu hwv hw
  constructor
  · intro h
    have hf := suppress_fwd hw hws hwu hwv h
    exact hf.1 (Ne.symm hwt)
  · intro h
    clear hws hwt hwu hwv hw
    induction h with
    | seed hs => exact Reach.seed hs
    | step h1 hedge ih =>
        rename_i a b
        rcases List.mem_cons.mp hedge with heq | hg
        · injection heq with ha hb
          subst a; subst b
          have huw : Reach ((u,w)::(w,v)::g) [s] w :=
            Reach.step ih List.mem_cons_self
          exact Reach.step huw (List.mem_cons_of_mem _ List.mem_cons_self)
        · exact Reach.step ih (List.mem_cons_of_mem _ (List.mem_cons_of_mem _ hg))


end TinylambOpt.ResearchProofs.ExecutionStrategies.RecursiveJoins
