import math
import unittest
from gaps import collect, loss_factor, markdown


def olap(engine, samples, correct=True):
    import statistics
    return dict(kind='olap',engine=engine,correct=correct,status='ok',suite='micro',
                rows=10000,workload='count',samples_ms=samples,
                median_ms=statistics.median(samples),detail='count-'+engine+'.json')


def trial(engine, trial_id, tps, latency):
    return dict(kind='oltp',engine=engine,valid=True,workload='update',clients=16,
                trial=trial_id,tps=tps,p50_ms=latency,p95_ms=latency*2,
                p99_ms=latency*3,errors=0)


class GapTest(unittest.TestCase):
    def test_ratio_direction_and_exact_boundary(self):
        self.assertEqual(loss_factor(10,1),10)
        self.assertEqual(loss_factor(1,10,True),10)
        self.assertLess(loss_factor(9.999,1),10)
        self.assertEqual(loss_factor(0,0,True),1)
        self.assertTrue(math.isinf(loss_factor(0,10,True)))
        with self.assertRaises(ValueError):loss_factor(float('nan'),1)

    def test_olap_uses_warm_median_not_mean_or_first(self):
        a=olap('tinylamb',[1,10,10,10,1000])
        b=olap('postgresql',[1,1,1,1,1000])
        a['first_ms']=.001
        row,=collect([a,b],'indexed')
        self.assertTrue(row['qualified'])
        self.assertEqual(row['loss_factor'],10)
        a['median_ms']=1
        with self.assertRaises(ValueError):collect([a,b],'indexed')

    def test_reference_engine_is_selected_without_mislabelling_values(self):
        rows=[olap('tinylamb',[20]),olap('postgresql',[10]),olap('mysql',[1])]
        pg,=collect(rows,'indexed')
        mysql,=collect(rows,'indexed',reference_engine='mysql')
        self.assertEqual(pg['loss_factor'],2)
        self.assertEqual(mysql['loss_factor'],20)
        self.assertEqual(mysql['mysql'],1)
        self.assertNotIn('postgresql',mysql)
        self.assertTrue(mysql['qualified'])
        self.assertEqual(mysql['detail_mysql'],'count-mysql.json')
        self.assertNotIn('detail_postgresql',mysql)
        self.assertIn('MySQL',markdown([mysql],reference_engine='mysql'))
        with self.assertRaises(ValueError):
            collect(rows[:2],'indexed',reference_engine='mysql')

    def test_mysql_throughput_and_error_labels_use_the_reference(self):
        rows=[trial('tinylamb',0,1,10),trial('mysql',0,10,1)]
        rows[1]['errors']=2
        compared=collect(rows,'indexed',reference_engine='mysql')
        metrics={r['metric']:r for r in compared}
        self.assertEqual(metrics['tps']['loss_factor'],10)
        self.assertEqual(metrics['p50_ms']['loss_factor'],10)
        self.assertEqual(metrics['tps']['measured_errors_mysql'],2)
        self.assertNotIn('measured_errors_postgresql',metrics['tps'])

    def test_median_tps_and_latency_are_independent(self):
        rows=[trial('tinylamb',i,t,l) for i,(t,l) in enumerate([(1,10),(1,10),(100,1)])]
        rows += [trial('postgresql',i,t,l) for i,(t,l) in enumerate([(10,1),(10,1),(1,100)])]
        d={r['metric']:r for r in collect(rows,'unindexed')}
        self.assertEqual(d['tps']['loss_factor'],10)
        self.assertEqual(d['p50_ms']['loss_factor'],10)
        self.assertEqual(d['p95_ms']['domain'],'tail')

    def test_incorrect_or_unpaired_results_are_not_ranked(self):
        with self.assertRaises(ValueError):collect([olap('tinylamb',[10],False),olap('postgresql',[1])],'indexed')
        with self.assertRaises(ValueError):collect([olap('tinylamb',[10])],'indexed')
        with self.assertRaises(ValueError):collect([trial('tinylamb',0,1,1),trial('postgresql',1,10,.1)],'unindexed')

    def test_duplicate_trials_are_rejected(self):
        rows=[trial('tinylamb',0,1,1),trial('postgresql',0,10,.1)]
        with self.assertRaises(ValueError):collect(rows+[rows[0]],'unindexed')

    def test_htap_roles_are_medians_not_inverted_latency(self):
        rows=[trial(e,i,10,1) for e in ('tinylamb','postgresql') for i in range(3)]
        for r in rows:
            r['workload']='htap'
            r['role_tps']={'reader':2 if r['engine']=='tinylamb' else 100,'transaction':10}
        d={r['metric']:r for r in collect(rows,'indexed')}
        self.assertEqual(d['reader_tps']['loss_factor'],50)
        self.assertTrue(d['reader_tps']['qualified'])
        self.assertFalse(d['transaction_tps']['qualified'])

    def test_htap_reader_alone_has_no_writer_role(self):
        rows=[trial(e,0,10,1) for e in ('tinylamb','postgresql')]
        for r in rows:
            r.update(workload='htap', clients=1, role_tps={'reader':10})
        metrics={r['metric'] for r in collect(rows,'unindexed')}
        self.assertIn('reader_tps',metrics)
        self.assertNotIn('transaction_tps',metrics)

    def test_htap_reader_latency_is_not_hidden_by_mixed_request_percentiles(self):
        rows=[trial(e,0,10,1) for e in ('tinylamb','postgresql')]
        for r in rows:
            r.update(workload='htap',clients=4,
                     role_tps={'reader':1,'transaction':9},
                     role_latency={'reader':{'p50_ms':20 if r['engine']=='tinylamb' else 1,
                                             'p95_ms':30,'p99_ms':40},
                                   'transaction':{'p50_ms':1,'p95_ms':2,'p99_ms':3}})
        metrics={r['metric']:r for r in collect(rows,'unindexed')}
        self.assertFalse(metrics['p50_ms']['qualified'])
        self.assertTrue(metrics['reader_p50_ms']['qualified'])
        self.assertEqual(metrics['reader_p50_ms']['loss_factor'],20)
        self.assertEqual(metrics['reader_p99_ms']['domain'],'tail')


if __name__=='__main__':unittest.main()
