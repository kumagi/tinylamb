import unittest
from gap_results import compare


class GapResultsTest(unittest.TestCase):
    def test_requires_both_old_and_new_postgres_targets(self):
        b = dict(profile='indexed',domain='olap',suite='micro',rows=10000,
                 workload='count',metric='median_ms',postgresql=1.,tinylamb=100.)
        n = dict(b,postgresql=10.,tinylamb=20.,loss_factor=2.)
        r, = compare([b],[n])
        self.assertFalse(r['passed'])
        n.update(tinylamb=9.,loss_factor=.9)
        r, = compare([b],[n])
        self.assertTrue(r['passed'])
        n.update(postgresql=.1,loss_factor=90.)
        r, = compare([b],[n])
        self.assertFalse(r['passed'])

    def test_missing_or_reversed_throughput_is_not_silently_passed(self):
        b = dict(profile='unindexed',domain='oltp',clients=16,workload='update',
                 metric='tps',postgresql=100.,tinylamb=1.)
        r, = compare([b],[])
        self.assertFalse(r['passed'])
        n = dict(b,tinylamb=20.,loss_factor=5.)
        r, = compare([b],[n])
        self.assertTrue(r['passed'])
        self.assertEqual(r['loss_against_original_pg'],5.)

    def test_mysql_requires_both_references_and_does_not_match_postgres_rows(self):
        b = dict(profile='indexed',domain='olap',suite='micro',rows=10000,
                 workload='exists',metric='median_ms',reference_engine='mysql',
                 mysql=1.,tinylamb=20.)
        n = dict(b,mysql=2.,tinylamb=15.,loss_factor=7.5)
        r,=compare([b],[n])
        self.assertFalse(r['passed'])
        self.assertEqual(r['loss_against_original_reference'],15.)
        self.assertNotIn('loss_against_original_pg',r)
        n.update(tinylamb=9.,loss_factor=4.5)
        self.assertTrue(compare([b],[n])[0]['passed'])
        n.update(reference_engine='postgresql',postgresql=2.)
        self.assertIsNone(compare([b],[n])[0]['after'])

    def test_inclusive_twofold_goal_checks_both_references_and_direction(self):
        before = dict(profile='indexed', domain='olap', suite='micro',
                      rows=10000, workload='count', metric='median_ms',
                      postgresql=1., tinylamb=4.)
        after = dict(before, tinylamb=2., postgresql=1., loss_factor=2.)
        self.assertTrue(compare([before], [after], 2, True)[0]['passed'])
        self.assertFalse(compare([before], [after], 2)[0]['passed'])
        after.update(postgresql=.5, loss_factor=4.)
        self.assertFalse(compare([before], [after], 2, True)[0]['passed'])
        before.update(domain='oltp', clients=16, metric='tps', postgresql=100.)
        after = dict(before, tinylamb=50., postgresql=100., loss_factor=2.)
        self.assertTrue(compare([before], [after], 2, True)[0]['passed'])
        after.update(tinylamb=49., loss_factor=100/49)
        self.assertFalse(compare([before], [after], 2, True)[0]['passed'])
        for bad in (1, float('nan'), float('inf')):
            with self.assertRaises(ValueError): compare([], [], bad)


if __name__ == '__main__': unittest.main()
