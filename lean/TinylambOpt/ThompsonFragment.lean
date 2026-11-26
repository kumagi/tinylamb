import Std.Tactic

/-!
Source semantics, independent of states, transitions and the compiler.

Two independent directions. The forward field propagates a real match through any closed
labeling. The invariant field constructs a labeling separating all spurious accepting
paths from the source language.

Named results include `assembled_state_count`.
-/

/- This file is a component of `TinylambOpt.RegexBitsetNfa`; the umbrella
   module imports it, and its declarations intentionally live in that API
   namespace rather than a namespace matching this fragment's file name. -/
namespace TinylambOpt.RegexBitsetNfa

abbrev Byte := Nat

inductive Regex where
  | empty
  | lit (b : Byte)
  | class (bs : List Byte)
  | concat (a b : Regex)
  | alt (a b : Regex)
  | star (a : Regex)
  deriving DecidableEq, Repr

def rsize : Regex → Nat
  | .empty => 2
  | .lit _ => 2
  | .class _ => 2
  | .concat a b => rsize a + rsize b
  | .alt a b => rsize a + rsize b + 2
  | .star a => rsize a + 2

namespace Thompson

abbrev Word := List Byte
abbrev Lang := Word → Prop

def product (a b : Lang) : Lang := fun w => ∃ u v, w = u ++ v ∧ a u ∧ b v

inductive Repeat (a : Lang) : Word → Prop
  | nil : Repeat a []
  | snoc : Repeat a u → a v → Repeat a (u ++ v)

/-- Source semantics, independent of states, transitions and the compiler. -/
def language : Regex → Lang
  | .empty => (· = [])
  | .lit b => (· = [b])
  | .class bs => fun w => ∃ b ∈ bs, w = [b]
  | .concat a b => product (language a) (language b)
  | .alt a b => fun w => language a w ∨ language b w
  | .star a => Repeat (language a)

structure Graph (Q : Type) where
  entry : Q
  exit : Q
  edge : Q → Option Byte → Q → Bool

def Closed (g : Graph Q) (p : Q → Lang) :=
  ∀ i c j w, g.edge i c j = true → p i w → p j (w ++ c.toList)

/-- Two independent directions. The forward field propagates a real match
    through any closed labeling. The invariant field constructs a labeling
    separating all spurious accepting paths from the source language. -/
structure Correct (g : Graph Q) (l : Lang) : Prop where
  forward : ∀ (p : Q → Lang), Closed g p → ∀ u v, p g.entry u → l v → p g.exit (u ++ v)
  invariant : ∀ (x : Lang), ∃ p : Q → Lang, Closed g p ∧
    (∀ u, x u → p g.entry u) ∧ (∀ v, p g.exit v → product x l v)

def atom (test : Option Byte → Bool) : Graph Bool :=
  ⟨false,true,fun i c j => !i && j && test c⟩

private theorem atom_correct (test : Option Byte → Bool) :
    Correct (atom test) (fun w => ∃ c, test c = true ∧ w = c.toList) := by
  constructor
  · rintro p hp u v hu ⟨c,h,rfl⟩
    exact hp false c true u (by simp [atom,h]) hu
  · intro x
    let p : Bool → Lang := fun i => if i then product x (fun w => ∃ c, test c = true ∧ w = c.toList) else x
    refine ⟨p, ?_, fun _ h => h, fun _ h => h⟩
    intro i c j w h hw
    cases i <;> cases j <;> simp_all [atom,p]
    exact ⟨w,c.toList,rfl,hw,c,h,rfl⟩

def seqG [DecidableEq A] [DecidableEq B] (a : Graph A) (b : Graph B) : Graph (Sum A B) :=
  ⟨.inl a.entry,.inr b.exit,fun i c j => match i,j with
    | .inl s,.inl t => a.edge s c t
    | .inr s,.inr t => b.edge s c t
    | .inl s,.inr t => s == a.exit && c.isNone && t == b.entry
    | _,_ => false⟩

private theorem seq_correct [DecidableEq A] [DecidableEq B] {a : Graph A} {b : Graph B}
    (ha : Correct a l) (hb : Correct b r) : Correct (seqG a b) (product l r) := by
  constructor
  · rintro p hp u v hu ⟨v₁,v₂,rfl,h₁,h₂⟩
    have left := ha.forward (fun q => p (.inl q)) (fun i c j => hp (.inl i) c (.inl j)) u v₁ hu h₁
    have link := hp (.inl a.exit) none (.inr b.entry) _ (by simp [seqG]) left
    simpa [seqG,List.append_assoc] using hb.forward (fun q => p (.inr q))
      (fun i c j => hp (.inr i) c (.inr j)) (u ++ v₁) v₂ (by simpa using link) h₂
  · intro x
    obtain ⟨p,cp,ep,fp⟩ := ha.invariant x
    obtain ⟨q,cq,eq,fq⟩ := hb.invariant (product x l)
    refine ⟨Sum.elim p q, ?_, ep, ?_⟩
    · intro i c j w h hw
      cases i with
      | inl i =>
        cases j with
        | inl j => exact cp i c j w h hw
        | inr j =>
          simp [seqG] at h
          obtain ⟨⟨rfl,rfl⟩,rfl⟩ := h
          simpa using eq _ (fp _ hw)
      | inr i =>
        cases j with
        | inl => contradiction
        | inr j => exact cq i c j w h hw
    · intro v hv
      obtain ⟨uv,w,rfl,⟨u,t,rfl,hx,ht⟩,hw⟩ := fq _ hv
      exact ⟨u,t++w,List.append_assoc ..,hx,t,w,rfl,ht,hw⟩

/-- Both outer ports are fresh and disjoint from either branch. -/
def altG [DecidableEq A] [DecidableEq B] (a : Graph A) (b : Graph B) :
    Graph (Sum Bool (Sum A B)) :=
  ⟨.inl false,.inl true,fun i c j => match i,j with
    | .inr (.inl s),.inr (.inl t) => a.edge s c t
    | .inr (.inr s),.inr (.inr t) => b.edge s c t
    | .inl false,.inr (.inl t) => c.isNone && t == a.entry
    | .inl false,.inr (.inr t) => c.isNone && t == b.entry
    | .inr (.inl s),.inl true => c.isNone && s == a.exit
    | .inr (.inr s),.inl true => c.isNone && s == b.exit
    | _,_ => false⟩

private theorem alt_correct [DecidableEq A] [DecidableEq B] {a : Graph A} {b : Graph B}
    (ha : Correct a l) (hb : Correct b r) : Correct (altG a b) (fun w => l w ∨ r w) := by
  constructor
  · intro p hp u v hu hv
    rcases hv with hl | hr
    · have enter := hp (.inl false) none (.inr (.inl a.entry)) u (by simp [altG]) hu
      have out := ha.forward (fun q => p (.inr (.inl q)))
        (fun i c j => hp (.inr (.inl i)) c (.inr (.inl j))) u v (by simpa using enter) hl
      simpa [altG] using hp (.inr (.inl a.exit)) none (.inl true) _ (by simp [altG]) out
    · have enter := hp (.inl false) none (.inr (.inr b.entry)) u (by simp [altG]) hu
      have out := hb.forward (fun q => p (.inr (.inr q)))
        (fun i c j => hp (.inr (.inr i)) c (.inr (.inr j))) u v (by simpa using enter) hr
      simpa [altG] using hp (.inr (.inr b.exit)) none (.inl true) _ (by simp [altG]) out
  · intro x
    obtain ⟨p,cp,ep,fp⟩ := ha.invariant x
    obtain ⟨q,cq,eq,fq⟩ := hb.invariant x
    let out := product x (fun w => l w ∨ r w)
    have fl (w) (h : p a.exit w) : out w := by
      obtain ⟨s,t,e,hx,ht⟩ := fp _ h
      exact ⟨s,t,e,hx,Or.inl ht⟩
    have fr (w) (h : q b.exit w) : out w := by
      obtain ⟨s,t,e,hx,ht⟩ := fq _ h
      exact ⟨s,t,e,hx,Or.inr ht⟩
    refine ⟨Sum.elim (fun f => if f then out else x) (Sum.elim p q), ?_, fun _ h => h, fun _ h => h⟩
    intro i c j w edge h
    rcases i with (f | (i | i)) <;> rcases j with (t | (j | j)) <;> (try cases f) <;> (try cases t) <;> cases c <;>
      simp_all [altG,Sum.elim] <;> grind [Option.isNone_iff_eq_none,Closed]

def starG [DecidableEq A] (a : Graph A) : Graph (Sum Bool A) :=
  ⟨.inl false,.inl true,fun i c j => match i,j with
    | .inl false,.inl true => c.isNone
    | .inl false,.inr t => c.isNone && t == a.entry
    | .inr s,.inl true => c.isNone && s == a.exit
    | .inr s,.inr t => a.edge s c t || (c.isNone && s == a.exit && t == a.entry)
    | _,_ => false⟩

private theorem star_correct [DecidableEq A] {a : Graph A} (ha : Correct a l) :
    Correct (starG a) (Repeat l) := by
  constructor
  · intro p hp u v hu hv
    have enter := hp (.inl false) none (.inr a.entry) u (by simp [starG]) hu
    have h : p (.inr a.entry) (u ++ v) ∧ p (.inl true) (u ++ v) := by
      induction hv with
      | nil =>
        exact ⟨by simpa using enter, by simpa using hp (.inl false) none (.inl true) u rfl hu⟩
      | snoc prev last ih =>
        have exitState := ha.forward (fun q => p (.inr q))
          (fun i c j w h => hp (.inr i) c (.inr j) w (by simp [starG,h]))
          _ _ ih.1 last
        exact ⟨by simpa [List.append_assoc] using
                    hp (.inr a.exit) none (.inr a.entry) _ (by simp [starG]) exitState,
               by simpa [List.append_assoc] using
                    hp (.inr a.exit) none (.inl true) _ (by simp [starG]) exitState⟩
    exact h.2
  · intro x
    let out := product x (Repeat l)
    have initial (w) (h : x w) : out w := ⟨w,[],by simp,h,.nil⟩
    obtain ⟨p,cp,ep,fp⟩ := ha.invariant out
    have done (w) (h : p a.exit w) : out w := by
      obtain ⟨_,t,rfl,⟨u,v,rfl,hx,hv⟩,ht⟩ := fp _ h
      exact ⟨u,v++t,List.append_assoc ..,hx,.snoc hv ht⟩
    refine ⟨Sum.elim (fun f => if f then out else x) p, ?_, fun _ h => h, fun _ h => h⟩
    intro i c j w edge h
    rcases i with (f | i) <;> rcases j with (t | j) <;> (try cases f) <;> (try cases t) <;> cases c <;>
      simp_all [starG,Sum.elim] <;> grind [Closed]

/-- The finite list gives each tagged node its machine index. -/
structure Program (l : Lang) where
  Q : Type
  eqQ : DecidableEq Q
  nodes : List Q
  cover : ∀ q, q ∈ nodes
  graph : Graph Q
  correct : Correct graph l

private def atomProgram (test : Option Byte → Bool) (l : Lang)
    (h : l = (fun w => ∃ c, test c = true ∧ w = c.toList)) : Program l :=
  ⟨Bool,inferInstance,[false,true],by intro q; cases q <;> simp,atom test,h ▸ atom_correct test⟩

private def sumNodes (as : List A) (bs : List B) : List (Sum A B) :=
  as.map Sum.inl ++ bs.map Sum.inr

private theorem sumCover {as : List A} {bs : List B} (ha : ∀ a, a ∈ as) (hb : ∀ b, b ∈ bs) :
    ∀ q, q ∈ sumNodes as bs := by
  rintro (a | b) <;> simp [sumNodes,ha,hb]

def assemble : (r : Regex) → Program (language r)
  | .empty => by
    have h : language .empty = (fun w => ∃ c : Option Byte, c.isNone = true ∧ w = c.toList) := by
      funext w; simp [language]
    exact atomProgram Option.isNone _ h
  | .lit b => by
    let f := fun c : Option Byte => c == some b
    have h : language (.lit b) = (fun w => ∃ c, f c = true ∧ w = c.toList) := by
      funext w; simp [language,f]
    exact atomProgram f _ h
  | .class bs => by
    let f := fun c : Option Byte => c.any (fun b => bs.contains b)
    have h : language (.class bs) = (fun w => ∃ c, f c = true ∧ w = c.toList) := by
      funext w; apply propext; constructor
      · rintro ⟨b,h,rfl⟩; exact ⟨some b,by simpa [f] using h,rfl⟩
      · rintro ⟨(_ | b),h,rfl⟩
        · contradiction
        · exact ⟨b,by simpa [f] using h,rfl⟩
    exact atomProgram f _ h
  | .concat a b =>
    let x := assemble a
    let y := assemble b
    letI := x.eqQ
    letI := y.eqQ
    ⟨_,inferInstance,sumNodes x.nodes y.nodes,sumCover x.cover y.cover,
      seqG x.graph y.graph,seq_correct x.correct y.correct⟩
  | .alt a b =>
    let x := assemble a
    let y := assemble b
    letI := x.eqQ
    letI := y.eqQ
    ⟨_,inferInstance,sumNodes [false,true] (sumNodes x.nodes y.nodes),
      sumCover (by intro q; cases q <;> simp) (sumCover x.cover y.cover),
      altG x.graph y.graph,alt_correct x.correct y.correct⟩
  | .star a =>
    let x := assemble a
    letI := x.eqQ
    ⟨_,inferInstance,sumNodes [false,true] x.nodes,
      sumCover (by intro q; cases q <;> simp) x.cover,
      starG x.graph,star_correct x.correct⟩

theorem assembled_state_count (r : Regex) : (assemble r).nodes.length = rsize r := by
  induction r <;> simp_all [assemble,rsize,sumNodes,atomProgram] <;> omega

end Thompson
end TinylambOpt.RegexBitsetNfa
