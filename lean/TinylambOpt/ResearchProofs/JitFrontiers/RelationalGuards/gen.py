#!/usr/bin/env python3
"""Generate self-contained RelationalGuards proof files.

Usage: python3 gen.py <Module> <Prop> <theorem-name> "<theorem type>" "<proof body>" [lemma files...]
The generated file inlines the common lemmas (no cross-module imports, because
per-file verification uses `lake env lean <file>` without oleans).
"""
import sys
from pathlib import Path

D = Path(__file__).resolve().parent
GOALS = 'TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards'

# namespaces (module suffix in file header) inlined from lemma sources
NS = ['Common', 'CorrelatedSubLemmas', 'ScaledDifferenceLemmas', 'PositiveExcessLemmas',
      'AbsLagLemmas', 'DoubleMinusLemmas', 'ClampedDiffLemmas', 'AbsLagCoverage',
      'SquareLagCoverage', 'LeastShiftLemmas', 'WeightedMixLemmas', 'ProductCornersLemmas']

# lemma source file -> extra namespace it additionally opens in the goal tail
TAIL_OPEN = {
    'CorrelatedSub_Lemmas.lean': 'CorrelatedSubLemmas',
    'ScaledDifference_Lemmas.lean': 'ScaledDifferenceLemmas',
    'PositiveExcess_Lemmas.lean': 'PositiveExcessLemmas',
    'AbsLag_Lemmas.lean': 'AbsLagLemmas',
    'DoubleMinus_Lemmas.lean': 'DoubleMinusLemmas',
    'ClampedDiff_Lemmas.lean': 'ClampedDiffLemmas',
    'AbsLag_Coverage.lean': 'AbsLagCoverage',
    'SquareLag_Coverage.lean': 'SquareLagCoverage',
    'LeastShift_Lemmas.lean': 'LeastShiftLemmas',
    'WeightedMix_Lemmas.lean': 'WeightedMixLemmas',
    'ProductCorners_Lemmas.lean': 'ProductCornersLemmas',
}

P = 'TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.'


def body(p):
    return '\n'.join(l for l in Path(p).read_text().splitlines()
                     if not l.startswith('import '))


def make_uniq(suffix):
    def uniq(text):
        out = []
        for line in text.splitlines():
            for ns in NS:
                for kw in ('namespace', 'end', 'open'):
                    if line == f'{kw} {P}{ns}':
                        line = f'{kw} {P}{ns}_{suffix}'
            out.append(line)
        return '\n'.join(out)
    return uniq


def generate(module, prop, theorem, typ, proof, lemmas=()):
    header = f'import {GOALS}\n\nset_option maxHeartbeats 0\nset_option maxRecDepth 10000\n\n'
    suffix = f'{module}_{prop}'
    uniq = make_uniq(suffix)
    common = uniq(body(D / 'Common_RelationalGuards.lean'))
    extra = '\n'.join(uniq(body(D / l)) for l in lemmas)
    ns = f'TinylambOpt.ResearchProofs.JitFrontiers.RelationalGuards.Goal_{module}_{prop}'
    tail = (f'\nnamespace {ns}\n\n'
            f'open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards\n'
            f'open TinylambOpt.ResearchGoals.JitFrontiers.RelationalGuards.{module}\n'
            f'open {P}Common_{suffix}\n'
            f'open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)\n'
            + ''.join(f'open {P}{TAIL_OPEN[l]}_{suffix}\n' for l in lemmas if l in TAIL_OPEN)
            + '\n'
            f'theorem {theorem} : {typ} := by\n{proof}\n\n'
            f'end {ns}\n')
    out = header + common + '\n' + extra + tail
    path = D / f'Goal_{module}_{prop}.lean'
    path.write_text(out)
    print(path.name)


if __name__ == '__main__':
    generate(*sys.argv[1:6], lemmas=tuple(sys.argv[6:]))
