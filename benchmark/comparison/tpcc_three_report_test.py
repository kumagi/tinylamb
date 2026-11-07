"""Reject missing trials, incorrect accounting and contaminated timings."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from tpcc_three_report import ENGINES, TYPES, load


class TpccThreeReportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)/'original'
        self.directory.mkdir()
        self.env = dict(tpcc_args=dict(trials=1,seconds=1,warmup=0,clients=10),
                        args={k: 1 for k in ['server_cpus','read_workers','tiny_query_memory_mib','data_cache_mib','postgres_image','mysql_image']},
                        runtime_source_sha256={'x':hashlib.sha256(b'frozen').hexdigest()},
                        server_sha256='server',client_sha256='client',fixture_sha256={},mysql_c_library_sha256='mysql')
        self.records=[]
        self.fixtures=[]
        for engine in ENGINES:
            metrics={'tps':'5'}
            for t in TYPES:
                metrics.update({t+'.'+k:str(v) for k,v in dict(attempted=1,committed=1,user_rollback=0,engine_aborts=0,
                                                            tps=1,p50_ms=1,p95_ms=2,p99_ms=3).items()})
            self.records.append(dict(engine=engine,trial=1,valid=True,timing_qualified=True,
                                     invariants=dict(valid=True),exit_code=0,seconds=1,metrics=metrics))
            self.fixtures.append(dict(engine=engine,trial=1,valid=True,fingerprints={}))
        self.save(self.directory,self.records,self.fixtures)

    def save(self,directory,records,fixtures):
        directory.mkdir(exist_ok=True)
        (directory/'runtime-source').mkdir(exist_ok=True)
        (directory/'runtime-source/x').write_bytes(b'frozen')
        (directory/'environment.json').write_text(json.dumps(self.env))
        (directory/'finished.json').write_text(json.dumps(dict(engines=3,trials=1)))
        (directory/'results.jsonl').write_text('\n'.join(json.dumps(r) for r in records))
        (directory/'fixture-validation.jsonl').write_text('\n'.join(json.dumps(r) for r in fixtures))
        for r in records:
            (directory/f"activity-{r['engine']}-1.json").write_text(json.dumps([dict(competing={})]))
        for engine in ENGINES:
            (directory/f'transport-{engine}.log').write_text('transport_test=passed\n')

    def test_complete_matrix_accepted(self):
        self.assertEqual(len(load(self.directory)[1]),3)

    def test_missing_duplicate_accounting_and_unqualified_rejected(self):
        cases=[self.records[:-1],self.records+self.records[:1]]
        for key,value in [('valid',False),('timing_qualified',False),('exit_code',1)]:
            rr=copy.deepcopy(self.records);rr[0][key]=value;cases.append(rr)
        rr=copy.deepcopy(self.records);rr[0]['metrics']['new_order.committed']='2';cases.append(rr)
        rr=copy.deepcopy(self.records);rr[0]['metrics']['new_order.p95_ms']='.1';cases.append(rr)
        for i,records in enumerate(cases):
            with self.subTest(case=i):
                self.save(self.directory,records,self.fixtures)
                with self.assertRaises(ValueError):load(self.directory)

    def test_monitor_overlap_cannot_be_hidden_by_qualified_flag(self):
        (self.directory/'activity-tinylamb-1.json').write_text(json.dumps([dict(competing={'cc1plus':1})]))
        with self.assertRaisesRegex(ValueError,'overlaps'):load(self.directory)

    def test_targeted_repeat_requires_same_binary_and_profile(self):
        original=copy.deepcopy(self.records)
        original[0]['timing_qualified']=False
        self.save(self.directory,original,self.fixtures)
        extra=self.directory.parent/'repeat'
        self.save(extra,self.records[:1],self.fixtures[:1])
        entry=dict(engine='tinylamb',trial=1,directory='../repeat',sha256={
            name:hashlib.sha256((extra/name).read_bytes()).hexdigest() for name in ['results.jsonl','environment.json']})
        manifest=dict(original_results_sha256=hashlib.sha256((self.directory/'results.jsonl').read_bytes()).hexdigest(),replacements=[entry])
        (self.directory/'replacement-selection.json').write_text(json.dumps(manifest))
        self.assertTrue(all(r['timing_qualified'] for r in load(self.directory)[1]))
        env=copy.deepcopy(self.env);env['client_sha256']='different client'
        (extra/'environment.json').write_text(json.dumps(env))
        entry['sha256']['environment.json']=hashlib.sha256((extra/'environment.json').read_bytes()).hexdigest()
        (self.directory/'replacement-selection.json').write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError,'binary/fixture'):load(self.directory)


if __name__=='__main__':
    unittest.main()
