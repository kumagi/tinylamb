import Std

namespace TinylambOpt.ResearchGoals.RestartRaces.Recovery

namespace DestroySummary
/-! Pass 1 over validated records. Owner IDs do not wrap or get reused. A
commit can be in a different WAL segment from its owner's destroy records. -/
inductive Op where
  | write | commit | destroy (page : Nat)
structure Rec where
  owner : Nat
  lsn : Nat
  op : Op

def owners (rs : List Rec) := rs.filterMap fun r =>
  match r.op with | .commit => some r.owner | _ => none

def floorAt (rs : List Rec) (owner page : Nat) :=
  ((rs.filterMap fun r => match r.op with
    | .destroy p => if r.owner == owner && p == page then some r.lsn else none
    | _ => none).max?).getD 0

def high (rs : List Rec) := (rs.map Rec.owner).max?.getD 0

/-- Compose independent pass-1 segment summaries to seed the allocator and
install the exact committed destroy fence. Each worker retains maxima per
(owner,page) even when it has not seen that owner's commit. Zero is no fence. -/
def Goal : Prop := ∀ (chunks : List (List Rec)) page,
  let rs := chunks.flatten
  let commits := chunks.flatMap owners
  ((chunks.map high).max?.getD 0,
    ((commits.map fun id => (chunks.map (fun seg => floorAt seg id page)).max?.getD 0).max?).getD 0) =
  (high rs, ((rs.filterMap fun r => match r.op with
    | .destroy p => if p == page && (owners rs).contains r.owner then some r.lsn else none
    | _ => none).max?).getD 0)

#guard floorAt [⟨2,10,.destroy 7⟩] 2 7 == 10
end DestroySummary

namespace DemandRedo
/-! Committed, ordered after-images and copies. Keys include incarnation. All
actions are total and have no effects on other objects. Work backwards from
the objects demanded at first read, then execute selected records forwards. -/
inductive Value where
  | literal (n : Nat) | copy (key : Nat)
structure Edit where
  dst : Nat
  value : Value
def step (s : Nat → Nat) (e : Edit) (k : Nat) :=
  if k != e.dst then s k else match e.value with
    | .literal n => n | .copy src => s src

def slice (needed : List Nat) : List Edit → List Nat × List Edit
  | [] => (needed,[])
  | e::older =>
    if needed.contains e.dst then
      let rest := needed.filter (· != e.dst)
      let before := match e.value with | .literal _ => rest | .copy src => src::rest
      let (keys,es) := slice before older
      (keys,e::es)
    else slice needed older

/-- Instant restart from a sliced checkpoint and WAL: the surviving base
keys are precisely the initial values that can influence the requested output. -/
def Goal : Prop := ∀ (log : List Edit) wanted,
  let (deps,selected) := slice wanted log.reverse
  (∀ (s : Nat → Nat), wanted.map (selected.reverse.foldl step s) = wanted.map (log.foldl step s)) ∧
  ∀ (loaded : List Nat), (∀ k ∈ deps, k ∈ loaded) ↔
    ∀ (s t : Nat → Nat), (∀ k ∈ loaded, s k = t k) →
      wanted.map (log.foldl step s) = wanted.map (log.foldl step t)

#guard (slice [3] [⟨3,.copy 2⟩,⟨2,.literal 8⟩]).1.isEmpty
end DemandRedo

namespace CommitDrain
/-! Proposed one-pass replay buffers absolute images until their commit.
IDs below are positions in the validated WAL, not data values. -/
structure Image where
  key : Nat
  owner : Nat
def serial (xs : List Image) (commits : List Nat) (k : Nat) :=
  ((xs.zipIdx.filter (fun (x,_) => x.key == k && commits.contains x.owner)).getLast?).map Prod.snd
def drain (xs : List Image) (commits : List Nat) (k : Nat) :=
  ((commits.flatMap (fun o => xs.zipIdx.filter (fun (x,_) => x.key == k && x.owner == o))).getLast?).map Prod.snd
def read (base : Nat) (payload : Nat → Nat) (w : Option Nat) := (w.map payload).getD base

/-- Can a single-pass owner-buffered redo replace two-pass WAL-order replay
even for interleaved writers? A cheap boundary to test is whether the owner
of the last eligible image is also the last committer touching this key. -/
def Goal : Prop := ∀ xs commits k, commits.Nodup →
  let lastOwner := ((commits.filter (fun o => xs.any (fun x => x.key == k && x.owner == o))).getLast?)
  (((serial xs commits k).bind (fun i => xs[i]?.map Image.owner)) = lastOwner ↔
    ∀ base payload, read base payload (serial xs commits k) = read base payload (drain xs commits k))

#guard serial [⟨3,1⟩,⟨3,2⟩] [2,1] 3 != drain [⟨3,1⟩,⟨3,2⟩] [2,1] 3
end CommitDrain

namespace FuzzyTransfer
/-! One atomic committed transfer has two page-local delta records with the
same logical stamp. A fuzzy checkpoint may include different prefixes per
page. Redo does not expose a balance until both pages reach the read barrier. -/
structure Transfer where
  stamp : Nat
  src : Nat
  dst : Nat
  amount : Int
def contribution (t : Transfer) (k : Nat) :=
  (if k == t.dst then t.amount else 0) - (if k == t.src then t.amount else 0)
def applyAll (ts : List Transfer) (s : Nat → Int) (k : Nat) := s k + (ts.map (fun t => contribution t k)).sum

def redo (s : Nat → Nat × Int) (t : Transfer) (k : Nat) :=
  let (seen,v) := s k
  if (t.src == k || t.dst == k) && seen < t.stamp then (t.stamp,v+contribution t k) else (seen,v)

/-- Restart can crash between the two page installations. From arbitrary
per-page cuts, interrupt redo at any record and replay from the start; both
balances converge even for self-transfers and signed deltas. -/
def Goal : Prop := ∀ ts base (cut progress : Nat → Nat), ts.Pairwise (fun a b => a.stamp < b.stamp) →
  let start := fun k => (cut k, applyAll (ts.filter (fun t => t.stamp ≤ cut k)) base k)
  let partialRun := fun k => ((ts.take (progress k)).foldl redo start) k
  ∀ k, ((ts.foldl redo partialRun) k).2 = applyAll ts base k
end FuzzyTransfer

namespace RetentionProbe
/-! Before retiring a WAL prefix, compare recovery from the old and proposed
retained inputs. Each candidate contains independent complete after-images
for a single logical key (tombstones also carry a stamp). None owner denotes
an irrevocably committed image, including one supplied by a checkpoint. -/
structure Image where
  stamp : Nat
  owner : Option Nat

def winner (xs : List Image) (finish : List Nat) :=
  ((xs.filter (fun x => x.owner.all finish.contains)).map Image.stamp).max?

/-- Are no pending commits and one pending commit at a time a complete test
for every possible future commit subset? If true, a retention certificate
for after-images needs only linearly many crash outcomes per key. Stamps are
globally unique image identities shared by old and new representations. -/
def Goal : Prop := ∀ before after (openOwners : List Nat),
  (winner before [] = winner after [] ∧
    ∀ o ∈ openOwners, winner before [o] = winner after [o]) ↔
  ∀ finish : List Nat, (∀ o ∈ finish, o ∈ openOwners) → winner before finish = winner after finish

#guard winner [⟨8,none⟩,⟨6,some 1⟩] [1] == winner [⟨8,none⟩] [1]
end RetentionProbe

namespace GenerationGC
/-! A recovery policy may keep multiple immutable, checked checkpoint
generations, newest first. Each legal crash cut supplies the available files
and WAL lower bound. File IDs identify immutable incarnations. -/
structure Gen where
  id : Nat
  anchor : Nat
  files : List Nat
structure Cut where
  durableFiles : List Nat
  walStart : Nat
  walEnd : Nat
  manifests : List Nat

def chosen (gs : List Gen) (s : Cut) :=
  gs.find? fun g => s.manifests.contains g.id && s.walStart ≤ g.anchor && g.anchor ≤ s.walEnd &&
    g.files.all s.durableFiles.contains

/-- Which sidecars can be retired while the durable manifest generation is
in doubt? Retain the union of dependencies of the winners across all modeled
crash cuts, plus any live leases checked separately. -/
def Goal : Prop := ∀ (gs : List Gen) (cuts : List Cut) (remove : List Nat),
  (gs.map Gen.id).Nodup →
  ((∀ s ∈ cuts, ∀ g, chosen gs s = some g → ∀ f ∈ g.files, f ∉ remove) ↔
    ∀ s ∈ cuts, (chosen gs s).map Gen.id =
      (chosen gs {s with durableFiles := s.durableFiles.filter (fun f => !remove.contains f)}).map Gen.id)
end GenerationGC

end TinylambOpt.ResearchGoals.RestartRaces.Recovery
