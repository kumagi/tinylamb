import Std

/-!
These models specify the ordering of SQL transaction, recovery, and concurrent
maintenance events.
-/

namespace TinylambOpt.ResearchGoals.RestartRaces.Catalog

namespace OnlineUnique
/-! A row identity includes incarnation. Each RID is backfilled at its own
commit fence, so different rows of a key swap may have different scan times.
Absence or a SQL NULL key makes no entry in this non-NULL uniqueness check. -/
structure Change where
  ts : Nat
  rid : Nat
  key : Option Nat

def last (base : Option Nat) (cs : List Change) :=
  ((cs.getLast?).map Change.key).getD base
def asOf (base : Nat → Option Nat) (cs : List Change) (rid ts : Nat) :=
  last (base rid) (cs.filter (fun c => c.rid == rid && c.ts ≤ ts))
def entries (n : Nat) (state : Nat → Option Nat) :=
  (List.range n).filterMap (fun i => (state i).map (·,i))

def repair (original : List (Nat × Nat)) (cut : Nat → Nat) (endTs : Nat) (cs : List Change) :=
  let changes := cs.filter (fun c => cut c.rid < c.ts && c.ts ≤ endTs)
  let dirty := (changes.map Change.rid).eraseDups
  (original.filter (fun entry => !dirty.contains entry.2)) ++ dirty.filterMap (fun i =>
    (last none (changes.filter (fun c => c.rid == i))).map (·,i))

/-- Publish an online UNIQUE index from fuzzy backfill plus per-RID last
images, deferring duplicate checking to the atomic publication fence. Check
both covering IDs and the same uniqueness verdict as a fresh build. -/
def Goal : Prop := ∀ base cs cut n endTs, cs.Pairwise (fun a b => a.ts ≤ b.ts) →
  (∀ i ∈ List.range n, cut i ≤ endTs) → (∀ c ∈ cs, c.rid < n) →
  let scanned := fun i => asOf base cs i (cut i)
  let built := repair (entries n scanned) cut endTs cs
  let fresh := entries n (fun i => asOf base cs i endTs)
  built.Perm fresh ∧ ((built.map Prod.fst).Nodup ↔ (fresh.map Prod.fst).Nodup)

#guard repair [(2,0),(2,1)] (fun i => if i == 0 then 4 else 0) 4
  [⟨4,0,some 2⟩,⟨4,1,some 1⟩] == [(2,0),(1,1)]
end OnlineUnique

namespace CascadeFence
/-! Graph edges point from an object to its dependents. Object IDs include
incarnation. A proposed DROP CASCADE checks its closure again at commit. -/
abbrev Graph := Nat → Nat → Prop
inductive Reach (g : Graph) (root : Nat) : Nat → Prop where
  | here : Reach g root root
  | follow {a b} : Reach g root a → g a b → Reach g root b
def inside (g : Graph) (cached : Nat → Prop) : Graph := fun a b => cached a ∧ cached b ∧ g a b

/-- Reuse a cached cascade even across FK additions/removals and cycles:
there must be no outgoing boundary edge, and every formerly dependent object
must still have a root path inside the cached set. Both tests are necessary. -/
def Goal : Prop := ∀ (old live : Graph) root,
  let cached := Reach old root
  ((∀ k, cached k ↔ Reach live root k) ↔
    (∀ a b, cached a → live a b → cached b) ∧
    (∀ a, cached a → Reach (inside live cached) root a))
end CascadeFence

namespace RenameMoves
/-! One atomic batch renames distinct objects by permutation. At the catalog
layer, a primitive move requires an empty target. Slot n is a private temporary
name. Dependencies and reader visibility must be handled by the outer DDL txn. -/
abbrev Move := Nat × Nat
def move (state : List (Option Nat)) (m : Move) : Option (List (Option Nat)) := do
  let id ← state[m.1]?.join
  guard (state[m.2]? == some none)
  return (state.set m.1 none).set m.2 (some id)
def orbit (p : List Nat) : Nat → Nat → List Nat
  | 0, _ => []
  | n+1, i => i :: orbit p n (p[i]?.getD i)
def budget (p : List Nat) :=
  let changed := (List.range p.length).filter (fun i => p[i]? != some i)
  changed.length + changed.countP (fun i => (orbit p p.length i).min? == some i)

/-- A batch requires exactly one move per changed name plus one extra per
nontrivial permutation cycle. Can every permutation attain that lower bound
with just one temporary name, including multiple independent rename cycles? -/
def Goal : Prop := ∀ (p : List Nat), p.Perm (List.range p.length) →
  let start := (List.range p.length).map some ++ [none]
  let finish := p.map some ++ [none]
  let works := fun (ms : List Move) => ms.foldlM move start = some finish
  (∃ ms, works ms ∧ ms.length = budget p) ∧ ∀ ms, works ms → budget p ≤ ms.length

#guard budget [1,0,3,2,4] == 6
#guard [(0,2),(1,0),(2,1)].foldlM move [some 0,some 1,none] == some [some 1,some 0,none]
end RenameMoves

namespace InstantDefault
/-! A metadata-only ADD COLUMN fixes the value for already present rows.
Later ALTER DEFAULT affects new inserts. A missing slot carries its provenance
(epoch), whereas a physically present SQL NULL is an explicit value. -/
abbrev Val := Option Int
inductive Cell where
  | stored (v : Val) | missing (epoch : Nat)
def read (defs : Nat → Val) : Cell → Val
  | .stored v => v | .missing e => defs e

def latent (rows : List Cell) (visible : Nat → Nat → Prop) (e s : Nat) :=
  ∃ i, rows[i]? = some (.missing e) ∧ visible i s

/-- Compact retained schema history into at most k slots. Is it enough to
color epochs that require different values at a common visible schema
differently? The decoder can choose one value per (schema,slot), even where
no original epoch has the desired value at *all* schemas. -/
def Goal : Prop := ∀ (rows : List Cell) (visible : Nat → Nat → Prop)
    (defs : Nat → Nat → Val) (k : Nat),
  let used := latent rows visible
  ((∃ slot : Nat → Nat, ∃ decode : Nat → Nat → Val,
    ∀ e s, used e s → slot e < k ∧ decode s (slot e) = defs s e) ↔
   ∃ color : Nat → Nat,
    (∀ e s, used e s → color e < k) ∧
    (∀ e f s, used e s → used f s → defs s e ≠ defs s f → color e ≠ color f))

#guard read (fun _ => some 5) (.stored none) != read (fun _ => some 5) (.missing 1)
end InstantDefault

namespace ShadowBind
/-! A reusable binding has an ordered lookup path and an object descriptor
including incarnation, types and other properties needed by the compiled plan.
Aborted and transaction-local catalog changes are represented by the live view. -/
abbrev Catalog := Nat → Option Nat
def resolve (view : Catalog) (path : List Nat) := path.findSome? view
def prefixThrough (view : Catalog) : List Nat → List Nat
  | [] => []
  | k::ks => if (view k).isSome then [k] else k :: prefixThrough view ks

/-- Optimistic cache reuse from partial revalidation: checking the old search
prefix suffices if it still resolves to the old descriptor. If its winner
vanishes, require the unread suffix to be empty or otherwise certified.
Here quantify over every possible suffix binding on fresh namespace keys. -/
def Goal : Prop := ∀ old live (path : List Nat), path.Nodup →
  let checked := prefixThrough old path
  let oldAnswer := resolve old path
  ((resolve live checked = oldAnswer ∧ (oldAnswer.isSome ∨ checked = path)) ↔
    ∀ completion : Catalog, (∀ k ∈ checked, live k = completion k) →
      resolve completion path = oldAnswer)

#guard resolve (fun k => if k == 1 then none else some 8) [1,2] == some 8
end ShadowBind

namespace UniqueHandoff
/-! Each RID belongs to one publishing transaction; unchanged rows may use
an owner outside the batch. Key transfers are atomic within each transaction.
Absence / SQL NULL contributes no unique key. -/
structure Row where
  owner : Nat
  oldKey : Option Nat
  newKey : Option Nat
def unique (rs : List Row) (done : List Nat) :=
  (rs.filterMap (fun r => if done.contains r.owner then r.newKey else r.oldKey)).Nodup
def prefixes (rs : List Row) (done : List Nat) : List Nat → Prop
  | [] => unique rs done
  | o::os => unique rs done ∧ prefixes rs (o::done) os
def needs (rs : List Row) (release acquire : Nat) :=
  release ≠ acquire ∧ ∃ a ∈ rs, ∃ b ∈ rs, ∃ k,
    a.owner = release ∧ b.owner = acquire ∧ a.oldKey = some k ∧ b.newKey = some k

/-- Initial and final UNIQUE states can both be valid although a key swap
has no legal sequence of separately visible commits. Is acyclicity of the
old-owner → new-owner handoff graph exactly the serial-publication boundary? -/
def Goal : Prop := ∀ rs (batch : List Nat), batch.Nodup →
  (∀ r ∈ rs, r.owner ∉ batch → r.oldKey = r.newKey) →
  unique rs [] → unique rs batch →
  ((∃ order, order.Perm batch ∧ prefixes rs [] order) ↔
    ∃ rank : Nat → Nat, ∀ a ∈ batch, ∀ b ∈ batch, needs rs a b → rank a < rank b)
end UniqueHandoff

end TinylambOpt.ResearchGoals.RestartRaces.Catalog
