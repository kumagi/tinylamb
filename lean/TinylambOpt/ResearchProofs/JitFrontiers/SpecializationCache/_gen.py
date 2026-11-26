#!/usr/bin/env python3
"""Generate per-goal proof files for JitFrontiers.SpecializationCache."""
from pathlib import Path

BASE = Path(__file__).parent
ROOT = BASE.parent
ALL = ROOT / "SpecializationCache_All.lean"

HEAD = '''import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_{model}_{prop}

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.{model}
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

'''

# Reusable helper facts
ROW = '''private def pickRow (i j : Nat) (vi vj : Int) : Row :=
  ((List.replicate (max i j + 1) none).set i (some vi)).set j (some vj)

private theorem get_left (i j : Nat) (vi vj : Int) (hij : i ≠ j) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (pickRow i j vi vj) i = Out.val vi := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  have hi : i < max i j + 1 := by omega
  have hji : j ≠ i := by omega
  simp [pickRow, hji, hi]

private theorem get_right (i j : Nat) (vi vj : Int) (hij : i ≠ j) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (pickRow i j vi vj) j = Out.val vj := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  have hj : j < max i j + 1 := by omega
  simp [pickRow, hij, hj]

private theorem get_self (i : Nat) (vi : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get ((List.replicate (i+1) none).set i (some vi)) i = Out.val vi := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  simp

private def pickLeft (i j : Nat) (v : Int) : Row := (List.replicate (max i j + 1) none).set i (some v)

private theorem get_pickLeft_left (i j : Nat) (v : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (pickLeft i j v) i = Out.val v := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  have hi : i < max i j + 1 := by omega
  simp [pickLeft, hi]

private theorem get_pickLeft_right (i j : Nat) (v : Int) (hij : i ≠ j) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (pickLeft i j v) j = Out.null := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  have hj : j < max i j + 1 := by omega
  simp [pickLeft, hij, hj]

'''

# ---------------- OneVersion bodies ----------------
OV = {}
OV['BoundComparison'] = '''intro c r hacc
unfold BoundComparison.shape
dsimp
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV]
rename_i x
by_cases hlt : x < c.literal <;> simp [hlt]
'''
OV['CheckedAffine'] = '''intro c r hacc
unfold CheckedAffine.shape at hacc ⊢
dsimp at hacc ⊢
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV]
rename_i x
rw [h] at hacc
simp only [fits, Bool.and_eq_true, decide_eq_true_eq] at hacc
have hmul : fits 64 (x * c.scale) := by
  unfold fits
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  omega
have hadd : fits 64 (x * c.scale + c.literal) := by
  unfold fits
  simp only [Bool.and_eq_true, decide_eq_true_eq]
  omega
have hw1 := word_of_fits (x * c.scale) hmul
have hw2 := word_of_fits (x * c.scale + c.literal) hadd
simp [hw1, hw2, checked, hmul, hadd]
'''
OV['NullPolicy'] = '''intro c r hacc
unfold NullPolicy.shape
dsimp
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
| null =>
    have hnn : (Out.null == Out.null) = true := by rfl
    by_cases hm : c.mode = true <;> simp [h, hm, hnn]
| fault =>
    have hfn : (Out.fault == Out.null) = false := by rfl
    by_cases hm : c.mode = true <;> simp [h, hm, hfn]
| val n =>
    have hvn : (Out.val n == Out.null) = false := by rfl
    by_cases hm : c.mode = true <;> simp [h, hm, hvn]
'''
OV['SignDirection'] = '''intro c r hacc
unfold SignDirection.shape
dsimp
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV]
rename_i x
by_cases hx : x = c.literal
· subst x
  simp
· by_cases hm : c.mode
  · by_cases hlt : x < c.literal
    · simp [hm, hlt]
      omega
    · have hgt : c.literal < x := by omega
      have hltb : decide (x < c.literal) = false := by simp [hlt]
      simp [hm, hgt, hltb, hx]
  · by_cases hlt : x < c.literal
    · have hltb : decide (x < c.literal) = true := by simp [hlt]
      simp [hm, hltb, hx]
    · have hltb : decide (x < c.literal) = false := by simp [hlt]
      simp [hm, hltb]
'''
OV['PowerRemainder'] = '''intro c r hacc
unfold PowerRemainder.shape at hacc ⊢
dsimp at hacc ⊢
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV]
rename_i x
rw [h] at hacc
simp only [Bool.and_eq_true, decide_eq_true_eq] at hacc
have hx0 : 0 ≤ x := hacc.2
have hmax : max x 0 = x := by omega
rw [hmax]
'''
OV['ProjectPair'] = '''intro c r hacc
unfold ProjectPair.shape
dsimp
by_cases hm : c.mode <;> simp [hm]
'''
OV['DictionaryContents'] = '''intro c r hacc
unfold DictionaryContents.shape
dsimp
unfold dictionaryLoad dictionaryReference
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
| null => simp [h]
| fault => simp [h]
| val x => simp [h]; simpa [List.getElem?_toArray] using load_eq_reference c.table x
'''
OV['DictionaryChecksum'] = '''intro c r hacc
unfold DictionaryChecksum.shape
dsimp
unfold dictionaryLoad dictionaryReference
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
| null => simp [h]
| fault => simp [h]
| val x => simp [h]; simpa [List.getElem?_toArray] using load_eq_reference c.table x
'''
OV['ConstantLane'] = '''intro c r hacc
unfold ConstantLane.shape at hacc ⊢
dsimp at hacc ⊢
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
| null =>
    rw [h] at hacc
    have hf : (Out.null == Out.val c.literal) = false := by rfl
    rw [hf] at hacc
    cases hacc
| fault =>
    rw [h] at hacc
    have hf : (Out.fault == Out.val c.literal) = false := by rfl
    rw [hf] at hacc
    cases hacc
| val n =>
    rw [h] at hacc
    have hb : (Out.val n == Out.val c.literal) = (n == c.literal) := by rfl
    rw [hb] at hacc
    have hnl : n = c.literal := beq_iff_eq.mp hacc
    rw [hnl]
'''
OV['TruncateKey'] = '''intro c r hacc
unfold TruncateKey.shape
dsimp
cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV, Int.add_emod]
'''

# ---------------- hcode bodies ----------------
HC = {}
HC['BoundComparison'] = '''intro a b hk r
cases a with | mk al asc as am ae atab =>
cases b with | mk bl bsc bs bm be btab =>
unfold BoundComparison.shape at hk ⊢
dsimp at hk ⊢
simp at hk
rcases hk with ⟨hs, hl⟩
have hs' : as = bs := by omega
subst bs
subst bl
rfl
'''
HC['CheckedAffine'] = '''intro a b hk r
cases a with | mk al asc as am ae atab =>
cases b with | mk bl bsc bs bm be btab =>
unfold CheckedAffine.shape at hk ⊢
dsimp at hk ⊢
simp at hk
rcases hk with ⟨hs, hsc, hl⟩
have hs' : as = bs := by omega
subst bs
subst bsc
subst bl
rfl
'''
HC['NullPolicy'] = '''intro a b hk r
by_cases ham : a.mode = true
· by_cases hbm : b.mode = true
  · simp [NullPolicy.shape, ham, hbm] at hk ⊢
    rcases hk with ⟨hs, hl⟩
    have hsl : a.slot = b.slot := by omega
    rw [hsl, hl]
  · simp [NullPolicy.shape, ham, hbm] at hk
· by_cases hbm : b.mode = true
  · simp [NullPolicy.shape, ham, hbm] at hk
  · simp [NullPolicy.shape, ham, hbm] at hk ⊢
    have hsl : a.slot = b.slot := by omega
    rw [hsl]
'''
HC['SignDirection'] = '''intro a b hk r
cases a with | mk al asc as am ae atab =>
cases b with | mk bl bsc bs bm be btab =>
unfold SignDirection.shape at hk ⊢
dsimp at hk ⊢
simp at hk
rcases hk with ⟨hs, hl, hm⟩
have hs' : as = bs := by omega
have hm' : am = bm := by
  cases am <;> cases bm <;> simp at hm ⊢
subst bs
subst bl
subst bm
rfl
'''
HC['PowerRemainder'] = '''intro a b hk r
cases a with | mk al asc as am ae atab =>
cases b with | mk bl bsc bs bm be btab =>
unfold PowerRemainder.shape at hk ⊢
dsimp at hk ⊢
simp at hk
rcases hk with ⟨hs, hsc⟩
have hs' : as = bs := by omega
subst bs
subst bsc
rfl
'''
HC['ProjectPair'] = '''intro a b hk r
unfold ProjectPair.shape at hk ⊢
dsimp at hk ⊢
simp at hk
have hnat : a.slot + a.mode.toNat = b.slot + b.mode.toNat := by exact_mod_cast hk
rw [hnat]
'''
HC['DictionaryContents'] = '''intro a b hk r
have hs : (a.slot : Int) = b.slot := by
  simpa [DictionaryContents.shape] using congrArg List.head? hk
have ht : a.table = b.table := by
  simpa [DictionaryContents.shape] using congrArg List.tail hk
have hsl : a.slot = b.slot := by omega
simp [DictionaryContents.shape, dictionaryLoad, hsl, ht]
'''
HC['ConstantLane'] = '''intro a b hk r
unfold ConstantLane.shape at hk ⊢
dsimp at hk ⊢
simp at hk
rw [hk]
'''
HC['TruncateKey'] = '''intro a b hk r
cases a with | mk al asc as am ae atab =>
cases b with | mk bl bsc bs bm be btab =>
unfold TruncateKey.shape at hk ⊢
dsimp at hk ⊢
simp at hk
rcases hk with ⟨hs, hl⟩
have hs' : as = bs := by omega
subst bs
rw [hl]
'''

# Small row lookups used by model-specific goals
GET0 = '''private theorem get0 (v : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [some v] 0 = Out.val v := by rfl

private theorem get0none :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [none] 0 = Out.null := by rfl

'''

# ---------------- DropLastField refutations ----------------
DROP = {}
DROP['BoundComparison'] = '''let a : Cfg := ⟨0, 1, 0, false, 0, []⟩
let b : Cfg := {a with literal := 2}
apply not_dropLastField_of BoundComparison.shape a b [some 0]
· rfl
· rfl
· native_decide
'''
DROP['CheckedAffine'] = '''let a : Cfg := ⟨0, 0, 0, false, 0, []⟩
let b : Cfg := {a with literal := 1}
apply not_dropLastField_of CheckedAffine.shape a b [some 1]
· rfl
· rfl
· native_decide
'''
DROP['NullPolicy'] = '''let a : Cfg := ⟨1, 0, 0, true, 0, []⟩
let b : Cfg := {a with literal := 2}
apply not_dropLastField_of NullPolicy.shape a b [none]
· rfl
· rfl
· native_decide
'''
DROP['SignDirection'] = '''let a : Cfg := ⟨1, 0, 0, false, 0, []⟩
let b : Cfg := {a with mode := true}
apply not_dropLastField_of SignDirection.shape a b [some 0]
· rfl
· rfl
· native_decide
'''
DROP['PowerRemainder'] = '''let a : Cfg := ⟨0, 0, 1, false, 0, []⟩
let b : Cfg := {a with scale := 2}
apply not_dropLastField_of PowerRemainder.shape a b [some 0, some 2]
· rfl
· rfl
· native_decide
'''
DROP['ProjectPair'] = '''let a : Cfg := ⟨0, 0, 0, false, 0, []⟩
let b : Cfg := ⟨0, 0, 1, true, 0, []⟩
apply not_dropLastField_of ProjectPair.shape a b [some 5, some 7]
· rfl
· rfl
· native_decide
'''
DROP['DictionaryContents'] = '''let a : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
let b : Cfg := {a with table := [1,3]}
apply not_dropLastField_of DictionaryContents.shape a b [some 1]
· rfl
· rfl
· native_decide
'''
DROP['DictionaryChecksum'] = DROP['DictionaryContents'].replace('DictionaryContents', 'DictionaryChecksum')
DROP['ConstantLane'] = '''let a : Cfg := ⟨1, 0, 0, false, 0, []⟩
let b : Cfg := ⟨2, 0, 1, false, 0, []⟩
apply not_dropLastField_of ConstantLane.shape a b [some 1, some 2]
· rfl
· rfl
· native_decide
'''
DROP['TruncateKey'] = DROP['BoundComparison'].replace('BoundComparison', 'TruncateKey')

# ---------------- model-specific goals ----------------
SPEC = {}
SPEC[('BoundComparison', 'UnusedScale')] = ('proof', 'intro c p\nrfl\n')
SPEC[('BoundComparison', 'ThresholdEdge')] = ('proof', 'intro c h\nsimp [BoundComparison.shape, h, get0, mapV]\n', GET0)
SPEC[('CheckedAffine', 'ZeroScale')] = ('proof', '''intro c x hs hl hfit
unfold CheckedAffine.shape
dsimp
rw [hs, hl]
simp [mapV, get0]
rw [show word 0 = 0 by rfl]
simpa using word_of_fits c.literal hfit
''', GET0)
SPEC[('CheckedAffine', 'GuardPriority')] = ('proof', '''intro c x hs hbad
unfold CheckedAffine.shape
dsimp
simp [hs, hbad, get0]
''', GET0)
SPEC[('NullPolicy', 'DeadLiteral')] = ('proof', '''intro c k h
unfold NullPolicy.shape
dsimp
simp [h]
''')
SPEC[('NullPolicy', 'NullWitness')] = ('proof', 'intro c hs hm\nsimp [NullPolicy.shape, hs, hm, get0none]\n', GET0)
SPEC[('SignDirection', 'Tie')] = ('proof', 'intro c hs\nsimp [SignDirection.shape, hs, get0, mapV]\n', GET0)
SPEC[('SignDirection', 'Flip')] = ('proof', '''intro c x hs hx
unfold SignDirection.shape
dsimp
by_cases hlt : x < c.literal
· have hgt : ¬ c.literal < x := by omega
  by_cases hm : c.mode <;> simp [mapV, hs, hm, hlt, hgt, get0]
· have hgt : c.literal < x := by omega
  by_cases hm : c.mode <;> simp [mapV, hs, hm, hlt, hgt, get0]
''', GET0)
SPEC[('PowerRemainder', 'ZeroBits')] = ('proof', 'intro c x hs hl\nsimp [PowerRemainder.shape, hs, hl, get0, mapV]\n', GET0)
SPEC[('PowerRemainder', 'NegativeScaleAlias')] = ('proof', '''intro c h r
simp [PowerRemainder.shape, Int.toNat_of_nonpos (by omega : c.scale ≤ 0)]
''')
SPEC[('ProjectPair', 'EquivalentSlots')] = ('proof', 'intro c r\nrfl\n')
SPEC[('ProjectPair', 'MissingSlot')] = ('proof', '''intro c r h
unfold ProjectPair.shape
dsimp
unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
rw [List.getElem?_eq_none (by omega : r.length ≤ c.slot + c.mode.toNat)]
''')
SPEC[('DictionaryContents', 'EpochIndependent')] = ('proof', 'intro c g\nrfl\n')
SPEC[('DictionaryContents', 'NullOrBadCode')] = ('proof', '''intro c x hs hx
unfold DictionaryContents.shape
dsimp
unfold dictionaryLoad
rw [hs]
rw [show TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [some x] 0 = Out.val x by rfl]
simp [hx]
''')
SPEC[('DictionaryChecksum', 'Collision')] = ('proof', '''let a : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
let b : Cfg := {a with table := [2,1]}
refine ⟨a, b, [some 0], ?_, ?_⟩
· native_decide
· native_decide
''')
SPEC[('DictionaryChecksum', 'FreshEpoch')] = ('proof', '''intro a b h
simp [DictionaryChecksum.shape, compatible]
omega
''')
SPEC[('ConstantLane', 'RebindSlot')] = ('proof', 'intro c j\nrfl\n')
SPEC[('ConstantLane', 'ColdMiss')] = ('proof', '''intro c r h
unfold call
simp [lookup]
cases hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
| null =>
    have heq : (Out.null == Out.val c.literal) = false := by rfl
    simp [ConstantLane.shape, hg, heq]
| fault =>
    have heq : (Out.fault == Out.val c.literal) = false := by rfl
    simp [ConstantLane.shape, hg, heq]
| val n =>
    have heq : (Out.val n == Out.val c.literal) = (n == c.literal) := by rfl
    have hne : n ≠ c.literal := by
      intro hnl; apply h; rw [hg, hnl]
    simp [ConstantLane.shape, hg, heq, hne]
''')
SPEC[('TruncateKey', 'Period')] = ('proof', '''intro c n
unfold TruncateKey.shape
dsimp
rw [Int.add_emod]
simp
''')
SPEC[('TruncateKey', 'CanonicalRange')] = ('proof', '''intro c r v hv
unfold TruncateKey.shape at hv
dsimp at hv
cases hget : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot
· simp [hget, mapV] at hv
· simp [hget, mapV] at hv
· simp [hget, mapV] at hv
  subst v
  rw [Int.add_emod]
  constructor
  · apply Int.emod_nonneg
    native_decide
  · apply Int.emod_lt_of_pos
    native_decide
''')

MODELS = ['BoundComparison', 'CheckedAffine', 'NullPolicy', 'SignDirection', 'PowerRemainder',
          'ProjectPair', 'DictionaryContents', 'DictionaryChecksum', 'ConstantLane', 'TruncateKey']
SHARED = ['OneVersion', 'Substitute', 'KeyMinimal', 'ReplayBatch', 'EvictWhileHeld',
          'CompileOnce', 'PublishRace', 'DropLastField']

BODIES = {}

for m in MODELS:
    BODIES[(m, 'CompileOnce')] = ('proof', 'exact compileOnce shape\n')

    if m in OV:
        BODIES[(m, 'OneVersion')] = ('proof', OV[m])

    if m in HC:
        def indented(s, n):
            return '\n'.join(' ' * n + l if l else '' for l in s.rstrip().splitlines()) + '\n'
        body = ('apply substitute_of %s.shape\n' % m + '·\n' + indented(OV[m], 2) +
                '·\n' + indented(HC[m], 2))
        BODIES[(m, 'Substitute')] = ('proof', body)

    if m in HC:
        pre = ('private theorem hone : oneVersion %s.shape := by\n' % m + indented(OV[m], 2) +
               'private theorem hsu : substitute %s.shape := by\n' % m +
               '  apply substitute_of %s.shape\n' % m +
               '  · exact hone\n' +
               '  ·\n' + indented(HC[m], 4) +
               'private theorem hcall : ∀ pool c r, call %s.shape pool c r = %s.shape.reference c r := by\n' % (m, m) +
               '  exact call_eq_reference %s.shape hone hsu\n\n' % m)
        tactic = {
            'ReplayBatch': 'replayBatch_of %s.shape hcall' % m,
            'EvictWhileHeld': 'evictWhileHeld_of %s.shape hcall hsu' % m,
            'PublishRace': 'publishRace_of %s.shape hcall' % m,
        }
        for p in ('ReplayBatch', 'EvictWhileHeld', 'PublishRace'):
            BODIES[(m, p)] = ('proof', 'exact %s\n' % tactic[p], pre)


def write_file(model, prop, kind, body, preamble=''):
    name = 'Goal_%s_%s.lean' % (model, prop)
    kw = 'refute' if kind == 'refutation' else 'sound'
    goal = '%s.%s.Goal' % (model, prop)
    neg = '¬ ' if kind == 'refutation' else ''
    text = HEAD.format(model=model, prop=prop)
    need_row = prop in ('KeyMinimal',)
    if need_row:
        text += ROW
    text += preamble
    text += 'theorem %s : %s%s := by\n' % (kw, neg, goal)
    text += '\n'.join('  ' + l if l else '' for l in body.rstrip().splitlines()) + '\n\n'
    text += 'end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_%s_%s\n' % (model, prop)
    (BASE / name).write_text(text)


imports = ['-- Proof aggregation for JitFrontiers.SpecializationCache (generated)\n']
for m in MODELS:
    for p in SHARED + []:
        pass
# model-specific names
SPECIFIC = {
    'BoundComparison': ['UnusedScale', 'ThresholdEdge'],
    'CheckedAffine': ['ZeroScale', 'GuardPriority'],
    'NullPolicy': ['DeadLiteral', 'NullWitness'],
    'SignDirection': ['Tie', 'Flip'],
    'PowerRemainder': ['ZeroBits', 'NegativeScaleAlias'],
    'ProjectPair': ['EquivalentSlots', 'MissingSlot'],
    'DictionaryContents': ['EpochIndependent', 'NullOrBadCode'],
    'DictionaryChecksum': ['Collision', 'FreshEpoch'],
    'ConstantLane': ['RebindSlot', 'ColdMiss'],
    'TruncateKey': ['Period', 'CanonicalRange'],
}

# ---------------- KeyMinimal ----------------
KM = {}
KM['BoundComparison'] = ('proof', '''intro a b hkey
unfold BoundComparison.shape at hkey ⊢
dsimp at hkey ⊢
simp at hkey
by_cases hs : a.slot = b.slot
· have hl : a.literal ≠ b.literal := hkey (by omega)
  let x := min a.literal b.literal
  let r : Row := (List.replicate (a.slot+1) none).set a.slot (some x)
  refine ⟨r, by simp, by simp, ?_⟩
  rw [get_self a.slot x]
  rw [← hs, get_self a.slot x]
  dsimp [x]
  by_cases hlt : min a.literal b.literal < a.literal
  · have hb : min a.literal b.literal = b.literal := by omega
    simp [mapV, hlt, hb]
    omega
  · have ha : min a.literal b.literal = a.literal := by omega
    have hltb : a.literal < b.literal := by omega
    simp [mapV, hlt, ha, hltb]
· let r : Row := pickRow a.slot b.slot (a.literal - 1) b.literal
  refine ⟨r, by simp, by simp, ?_⟩
  rw [get_left a.slot b.slot (a.literal - 1) b.literal hs]
  rw [get_right a.slot b.slot (a.literal - 1) b.literal hs]
  simp [mapV]
  omega
''')
KM['CheckedAffine'] = ('refutation', '''let A : Cfg := ⟨0, 1, 0, false, 0, []⟩
let B : Cfg := {A with scale := 18446744073709551617}
apply not_keyMinimal_of CheckedAffine.shape A B
· native_decide
· intro r ha hb
  cases hget : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r A.slot with
  | null =>
      have hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.null := by simpa [A] using hget
      simp [CheckedAffine.shape, A, B, hg, mapV]
  | fault =>
      have hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.fault := by simpa [A] using hget
      simp [CheckedAffine.shape, A, B, hg, mapV]
  | val x =>
      have hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.val x := by simpa [A] using hget
      unfold CheckedAffine.shape at hb
      dsimp at hb
      simp [A, B] at hb
      rw [hg] at hb
      simp only [fits, Bool.and_eq_true, decide_eq_true_eq] at hb
      have hlim : limit 64 = 9223372036854775808 := by native_decide
      rw [hlim] at hb
      have hx : x = 0 := by omega
      subst x
      simp [CheckedAffine.shape, A, B, hg, mapV]
''')
KM['DictionaryChecksum'] = ('refutation', '''let L : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
apply not_keyMinimal_of DictionaryChecksum.shape L {L with epoch := 1}
· native_decide
· intro r ha hb
  rfl
''')
KM['PowerRemainder'] = ('refutation', '''let A : Cfg := ⟨0, 0, 0, false, 0, []⟩
let B : Cfg := {A with scale := 64}
apply not_keyMinimal_of PowerRemainder.shape A B
· native_decide
· intro r ha hb
  unfold PowerRemainder.shape at hb
  dsimp at hb
  simp [B] at hb
''')
KM['TruncateKey'] = ('proof', '''intro a b hkey
unfold TruncateKey.shape at hkey ⊢
dsimp at hkey ⊢
simp at hkey
by_cases hs : a.slot = b.slot
· have hl : a.literal % 65536 ≠ b.literal % 65536 := hkey (by omega)
  let r : Row := (List.replicate (a.slot+1) none).set a.slot (some 0)
  refine ⟨r, by simp, by simp, ?_⟩
  rw [get_self a.slot 0]
  rw [← hs, get_self a.slot 0]
  simp [mapV, Int.add_emod, hl]
· by_cases hl : a.literal % 65536 = b.literal % 65536
  · let r : Row := pickRow a.slot b.slot 0 1
    refine ⟨r, by simp, by simp, ?_⟩
    rw [get_left a.slot b.slot 0 1 hs]
    rw [get_right a.slot b.slot 0 1 hs]
    simp [mapV, Int.add_emod, hl]
    have hr0 : 0 ≤ a.literal % (65536:Int) := Int.emod_nonneg _ (by native_decide)
    have hrlt : a.literal % (65536:Int) < 65536 := Int.emod_lt_of_pos _ (by native_decide)
    omega
  · let r : Row := pickRow a.slot b.slot 0 0
    refine ⟨r, by simp, by simp, ?_⟩
    rw [get_left a.slot b.slot 0 0 hs]
    rw [get_right a.slot b.slot 0 0 hs]
    simp [mapV, Int.add_emod]
    exact hl
''')
KM['ProjectPair'] = ('proof', '''intro a b hkey
unfold ProjectPair.shape at hkey ⊢
dsimp at hkey ⊢
simp at hkey
have hidx : a.slot + a.mode.toNat ≠ b.slot + b.mode.toNat := by exact_mod_cast hkey
let r : Row := pickRow (a.slot + a.mode.toNat) (b.slot + b.mode.toNat) 0 1
refine ⟨r, by simp, by simp, ?_⟩
rw [get_left (a.slot+a.mode.toNat) (b.slot+b.mode.toNat) 0 1 hidx]
rw [get_right (a.slot+a.mode.toNat) (b.slot+b.mode.toNat) 0 1 hidx]
native_decide
''')
KM['ConstantLane'] = ('refutation', '''let A : Cfg := ⟨0, 0, 0, false, 0, []⟩
let B : Cfg := {A with literal := 1}
apply not_keyMinimal_of ConstantLane.shape A B
· native_decide
· intro r ha hb
  unfold ConstantLane.shape at ha hb
  dsimp at ha hb
  cases hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r A.slot with
  | null =>
      have hg0 : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.null := by simpa [A] using hg
      simp [A, B] at ha hb
      rw [hg0] at ha hb
      have hf : (Out.null == Out.val (0:Int)) = false := by rfl
      rw [hf] at ha
      cases ha
  | fault =>
      have hg0 : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.fault := by simpa [A] using hg
      simp [A, B] at ha hb
      rw [hg0] at ha hb
      have hf : (Out.fault == Out.val (0:Int)) = false := by rfl
      rw [hf] at ha
      cases ha
  | val n =>
      have hg0 : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r 0 = Out.val n := by simpa [A] using hg
      simp [A, B] at ha hb
      rw [hg0] at ha hb
      have hba : (Out.val n == Out.val (0:Int)) = (n == 0) := by rfl
      have hbb : (Out.val n == Out.val (1:Int)) = (n == 1) := by rfl
      rw [hba] at ha
      rw [hbb] at hb
      have hna : n = 0 := beq_iff_eq.mp ha
      have hnb : n = 1 := beq_iff_eq.mp hb
      omega
''')

KM['NullPolicy'] = ('proof', '''intro a b hkey
unfold NullPolicy.shape at hkey ⊢
dsimp at hkey ⊢
simp at hkey
by_cases hs : a.slot = b.slot
· let r : Row := List.replicate (a.slot + 1) none
  have hg : get r a.slot = Out.null := get_hole a.slot
  have hg2 : get r b.slot = Out.null := by rw [← hs]; exact hg
  refine ⟨r, by simp, by simp, ?_⟩
  rw [hg, hg2]
  cases am : a.mode
  · cases bm : b.mode
    · exfalso
      apply hkey
      · omega
      · rw [am, bm]
      · simp [am, bm]
    · simp [am, bm]
  · cases bm : b.mode
    · simp [am, bm]
    · have hl : a.literal ≠ b.literal := by
        intro he
        apply hkey
        · omega
        · rw [am, bm]
        · simp [am, bm, he]
      simp [am, bm]
      exact hl
· let r : Row := pickRow a.slot b.slot 0 1
  refine ⟨r, by simp, by simp, ?_⟩
  rw [get_left a.slot b.slot 0 1 hs]
  rw [get_right a.slot b.slot 0 1 hs]
  simp only []
  exact fun hh => absurd (Out.val.inj hh) (by omega)
''', '''private theorem get_hole (i : Nat) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (List.replicate (i+1) none) i = Out.null := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  rw [List.getElem?_replicate, if_pos (by omega : i < i + 1)]

''')

KM['SignDirection'] = ('proof', '''intro a b hkey
unfold SignDirection.shape at hkey ⊢
dsimp at hkey ⊢
simp at hkey
by_cases hs : a.slot ≠ b.slot
· let x := if a.mode then a.literal + 1 else a.literal - 1
  let r : Row := pickRow a.slot b.slot x b.literal
  refine ⟨r, by simp, by simp, ?_⟩
  rw [get_left a.slot b.slot x b.literal hs]
  rw [get_right a.slot b.slot x b.literal hs]
  cases am : a.mode
  · have hx : x = a.literal - 1 := by simp [x, am]
    cases bm : b.mode
    · simp only [mapV, am, bm, hx, Bool.false_eq_true, ite_true, ite_false]
      rw [decide_eq_true (by omega : a.literal - 1 < a.literal),
          decide_eq_false (by omega : ¬ (b.literal < b.literal))]
      decide
    · simp only [mapV, am, bm, hx, Bool.false_eq_true, ite_true, ite_false]
      rw [decide_eq_true (by omega : a.literal - 1 < a.literal),
          decide_eq_false (by omega : ¬ (b.literal > b.literal))]
      decide
  · have hx : x = a.literal + 1 := by simp [x, am]
    cases bm : b.mode
    · simp only [mapV, am, bm, hx, Bool.false_eq_true, ite_true, ite_false]
      rw [decide_eq_true (by omega : a.literal + 1 > a.literal),
          decide_eq_false (by omega : ¬ (b.literal < b.literal))]
      decide
    · simp only [mapV, am, bm, hx, Bool.false_eq_true, ite_true, ite_false]
      rw [decide_eq_true (by omega : a.literal + 1 > a.literal),
          decide_eq_false (by omega : ¬ (b.literal > b.literal))]
      decide
· have heq : a.slot = b.slot := by omega
  cases am : a.mode
  · cases bm : b.mode
    · have hl : a.literal ≠ b.literal := by
        intro he
        apply hkey
        · omega
        · exact he
        · simp [am, bm]
      by_cases hlt : a.literal < b.literal
      · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some a.literal)
        refine ⟨r, by simp, by simp, ?_⟩
        rw [get_self a.slot a.literal, ← heq, get_self a.slot a.literal]
        simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_false (by omega : ¬ (a.literal < a.literal)),
            decide_eq_true (by omega : a.literal < b.literal)]
        decide
      · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some b.literal)
        refine ⟨r, by simp, by simp, ?_⟩
        rw [get_self a.slot b.literal, ← heq, get_self a.slot b.literal]
        simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_true (by omega : b.literal < a.literal),
            decide_eq_false (by omega : ¬ (b.literal < b.literal))]
        decide
    · by_cases hle : a.literal ≤ b.literal
      · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some (a.literal - 1))
        refine ⟨r, by simp, by simp, ?_⟩
        rw [get_self a.slot (a.literal - 1), ← heq, get_self a.slot (a.literal - 1)]
        simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_true (by omega : a.literal - 1 < a.literal),
            decide_eq_false (by omega : ¬ (a.literal - 1 > b.literal))]
        decide
      · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some b.literal)
        refine ⟨r, by simp, by simp, ?_⟩
        rw [get_self a.slot b.literal, ← heq, get_self a.slot b.literal]
        simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_true (by omega : b.literal < a.literal),
            decide_eq_false (by omega : ¬ (b.literal > b.literal))]
        decide
  · cases bm : b.mode
    · by_cases hle : b.literal ≤ a.literal
      · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some (b.literal - 1))
        refine ⟨r, by simp, by simp, ?_⟩
        rw [get_self a.slot (b.literal - 1), ← heq, get_self a.slot (b.literal - 1)]
        simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_false (by omega : ¬ (b.literal - 1 > a.literal)),
            decide_eq_true (by omega : b.literal - 1 < b.literal)]
        decide
      · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some a.literal)
        refine ⟨r, by simp, by simp, ?_⟩
        rw [get_self a.slot a.literal, ← heq, get_self a.slot a.literal]
        simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_false (by omega : ¬ (a.literal > a.literal)),
            decide_eq_true (by omega : a.literal < b.literal)]
        decide
    · have hl : a.literal ≠ b.literal := by
        intro he
        apply hkey
        · omega
        · exact he
        · simp [am, bm]
      by_cases hlt : a.literal < b.literal
      · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some b.literal)
        refine ⟨r, by simp, by simp, ?_⟩
        rw [get_self a.slot b.literal, ← heq, get_self a.slot b.literal]
        simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_true (by omega : b.literal > a.literal),
            decide_eq_false (by omega : ¬ (b.literal > b.literal))]
        decide
      · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some a.literal)
        refine ⟨r, by simp, by simp, ?_⟩
        rw [get_self a.slot a.literal, ← heq, get_self a.slot a.literal]
        simp only [mapV, am, bm, Bool.false_eq_true, ite_true, ite_false]
        rw [decide_eq_false (by omega : ¬ (a.literal > a.literal)),
            decide_eq_true (by omega : a.literal > b.literal)]
        decide
''')

KM['DictionaryContents'] = ('proof', '''intro a b hkey
unfold DictionaryContents.shape at hkey ⊢
dsimp at hkey ⊢
simp at hkey
by_cases hs : a.slot ≠ b.slot
· rcases Nat.lt_or_ge a.slot b.slot with hlt | hgt
  · let r : Row := (List.replicate (a.slot+1) none).set a.slot (some (-1))
    refine ⟨r, by simp, by simp, ?_⟩
    have hga : get r a.slot = Out.val (-1) := get_self a.slot (-1)
    have hgb : get r b.slot = Out.fault :=
      get_eq_fault r b.slot (by
        have hlen : List.length r = a.slot + 1 := by simp [r]
        omega)
    unfold dictionaryLoad
    rw [hga, hgb]
    simp
  · have hgt' : b.slot < a.slot := by omega
    let r : Row := (List.replicate (b.slot+1) none).set b.slot (some (-1))
    refine ⟨r, by simp, by simp, ?_⟩
    have hga : get r a.slot = Out.fault :=
      get_eq_fault r a.slot (by
        have hlen : List.length r = b.slot + 1 := by simp [r]
        omega)
    have hgb : get r b.slot = Out.val (-1) := get_self b.slot (-1)
    unfold dictionaryLoad
    rw [hga, hgb]
    simp
· have heq : a.slot = b.slot := by omega
  have ht : a.table ≠ b.table := by
    intro he
    exact hkey (by omega) he
  obtain ⟨i, hi⟩ := exists_getElem a.table b.table ht
  let r : Row := (List.replicate (a.slot+1) none).set a.slot (some (i:Int))
  refine ⟨r, by simp, by simp, ?_⟩
  have hga : get r a.slot = Out.val (i:Int) := get_self a.slot (i:Int)
  have hgb : get r b.slot = Out.val (i:Int) := by
    rw [← heq]
    exact get_self a.slot (i:Int)
  unfold dictionaryLoad
  rw [hga, hgb]
  simp only []
  rw [if_neg (by omega : ¬ ((i:Int) < 0))]
  have tonat : (i:Int).toNat = i := by simp
  rw [tonat]
  simp only [List.getElem?_toArray]
  cases h1 : a.table[i]? with
  | none =>
      cases h2 : b.table[i]? with
      | none => exact absurd (by rw [h1, h2]) hi
      | some v => simp [h1, h2]
  | some v1 =>
      cases h2 : b.table[i]? with
      | none => simp [h1, h2]
      | some v2 =>
          rw [if_neg (by omega : ¬ ((i:Int) < 0))]
          show ¬(Out.val v1 = Out.val v2)
          exact fun hh => hi (by rw [h1, h2]; exact congrArg Option.some (Out.val.inj hh))
''', '''private theorem exists_getElem (t1 t2 : List Int) (h : t1 ≠ t2) :
    ∃ i : Nat, t1[i]? ≠ t2[i]? := by
  induction t1 generalizing t2 with
  | nil =>
      cases t2 with
      | nil => exact absurd rfl h
      | cons b t => exact ⟨0, by simp⟩
  | cons a t ih =>
      cases t2 with
      | nil => exact ⟨0, by simp⟩
      | cons b u =>
          by_cases hab : a = b
          · subst hab
            obtain ⟨i, hi⟩ := ih u (fun he => h (by rw [he]))
            exact ⟨i + 1, fun hh => hi (by simpa using hh)⟩
          · exact ⟨0, fun hh => hab (Option.some.inj (by simpa using hh))⟩

''')


DC = {}
DC['Substitute'] = '''intro h
let L : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
let R : Cfg := {L with table := [2,1]}
have hcomp : compatible DictionaryChecksum.shape L R := by native_decide
have heq := h L R [some 0] hcomp (by rfl)
have hneq : DictionaryChecksum.shape.code L [some 0] ≠ DictionaryChecksum.shape.reference R [some 0] := by native_decide
exact hneq heq
'''
DC['ReplayBatch'] = '''intro h
let L : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
let R : Cfg := {L with table := [2,1]}
have hres := h [L] R [[some 0]]
have hneq : [Out.val 1] ≠ [Out.val 2] := by native_decide
apply hneq
have hc1 : call DictionaryChecksum.shape [L] R [some 0] = Out.val 1 := by native_decide
have hc2 : DictionaryChecksum.shape.reference R [some 0] = Out.val 2 := by native_decide
simpa [hc1, hc2] using hres.1
'''
DC['EvictWhileHeld'] = '''intro h
let L : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
let R : Cfg := {L with table := [2,1]}
have hlookup : lookup DictionaryChecksum.shape [L] R = some L := by
  simp [lookup, compatible, DictionaryChecksum.shape, L, R, signature]
have heq := h [L] R L [some 0] hlookup (by rfl)
have hneq : DictionaryChecksum.shape.code L [some 0] ≠ call DictionaryChecksum.shape [] R [some 0] := by native_decide
exact hneq heq
'''
DC['PublishRace'] = '''intro h
let L : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
let R : Cfg := {L with table := [2,1]}
have hcomp : compatible DictionaryChecksum.shape L R := by native_decide
have heq := h L R [] L [some 0] hcomp
have hneq : call DictionaryChecksum.shape (L::R::[]) L [some 0] ≠
    call DictionaryChecksum.shape (R::L::[]) L [some 0] := by native_decide
exact hneq heq
'''

DICT_HELPERS = '''private theorem get_repl_null (i n : Nat) (h : i < n) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (List.replicate n none) i = Out.null := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  simp [List.getElem?_replicate, h]

private theorem get_repl_fault (i n : Nat) (h : n ≤ i) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get (List.replicate n none) i = Out.fault := by
  unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
  simp [List.getElem?_replicate, h]

private theorem out_ne_of_opt_ne {x y : Option Int} (h : x ≠ y) :
    (x.map Out.val).getD Out.null ≠ (y.map Out.val).getD Out.null := by
  cases x <;> cases y <;> simp_all

'''

KM['DictionaryContents'] = ('proof', '''intro a b hkey
unfold DictionaryContents.shape at hkey ⊢
dsimp at hkey ⊢
simp at hkey
by_cases hs : a.slot = b.slot
· have ht : a.table ≠ b.table := hkey (by omega)
  have hex : ∃ i : Nat, a.table[i]? ≠ b.table[i]? := by
    by_cases hex' : ∃ i : Nat, a.table[i]? ≠ b.table[i]?
    · exact hex'
    · exfalso
      apply ht
      apply List.ext_getElem?
      intro i
      by_cases hi : a.table[i]? = b.table[i]?
      · exact hi
      · exact False.elim (hex' ⟨i, hi⟩)
  obtain ⟨i, hi⟩ := hex
  let r : Row := (List.replicate (a.slot+1) none).set a.slot (some (i : Int))
  have hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r a.slot = Out.val (i:Int) := by
    unfold TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get
    simp [r]
  have hgb : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r b.slot = Out.val (i:Int) := by
    rw [← hs]
    exact hg
  refine ⟨r, by simp, by simp, ?_⟩
  simp [dictionaryLoad, hg, hgb, List.getElem?_toArray]
  exact out_ne_of_opt_ne hi
· let n := min a.slot b.slot + 1
  let r : Row := List.replicate n none
  have hcase : (a.slot < n ∧ n ≤ b.slot) ∨ (b.slot < n ∧ n ≤ a.slot) := by
    dsimp [n]
    omega
  rcases hcase with ⟨ha, hb⟩ | ⟨hb, ha⟩
  · have hga : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r a.slot = Out.null := by
      simpa [r] using get_repl_null a.slot n ha
    have hgb : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r b.slot = Out.fault := by
      simpa [r] using get_repl_fault b.slot n hb
    refine ⟨r, by simp, by simp, ?_⟩
    simp [dictionaryLoad, hga, hgb]
  · have hga : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r a.slot = Out.fault := by
      simpa [r] using get_repl_fault a.slot n ha
    have hgb : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r b.slot = Out.null := by
      simpa [r] using get_repl_null b.slot n hb
    refine ⟨r, by simp, by simp, ?_⟩
    simp [dictionaryLoad, hga, hgb]
''', DICT_HELPERS)

for m in MODELS:
    BODIES[(m, 'DropLastField')] = ('refutation', DROP[m])
    if m in KM:
        BODIES[(m, 'KeyMinimal')] = KM[m]
    if m == 'DictionaryChecksum':
        for p, b in DC.items():
            BODIES[(m, p)] = ('refutation', b)
    for p in SPECIFIC[m]:
        if (m, p) in SPEC:
            BODIES[(m, p)] = SPEC[(m, p)]

missing = []
for m in MODELS:
    for p in SHARED + SPECIFIC[m]:
        if (m, p) not in BODIES:
            missing.append((m, p))
            continue
        entry = BODIES[(m, p)]
        kind, body = entry[0], entry[1]
        pre = entry[2] if len(entry) > 2 else ''
        write_file(m, p, kind, body, pre)
        imports.append('import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_%s_%s\n' % (m, p))

ALL.write_text(''.join(imports))
print('generated', 100 - len(missing), 'files; missing:', missing)
