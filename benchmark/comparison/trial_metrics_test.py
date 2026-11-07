import unittest
from trial_metrics import window_metrics


class TrialMetricsTest(unittest.TestCase):
    def test_uses_only_interior_counter_deltas(self):
        def sample(t,n):
            return dict(time_ns=int(t*1e9),tasks=n,queue_ns=n*1000000,
                        execution_ns=n*2000000,completion_ns=n*300000,
                        wal_wait_ns=n*4000000,wal_wait_count=n,
                        wal_sync_count=n//2,wal_sync_ns=n*500000,
                        wal_buffered_bytes=n*100, pending_tasks=n%3,
                        version_chains=n,current_commit_ts=n*10,oldest_snapshot_ts=n)
        result = window_metrics([sample(0,0),sample(2,2),sample(4,4),sample(20,20)],1,5)
        self.assertEqual(result['interior_seconds'],2)
        self.assertEqual(result['commands_per_second'],1)
        self.assertEqual(result['queue_ms_per_command'],1)
        self.assertEqual(result['own_wal_wait_ms_per_commit'],4)
        self.assertEqual(result['commits_per_sync'],2)
        self.assertIsNone(window_metrics([sample(0,0)],1,5))

    def test_deferred_durability_is_separate_from_sql_time(self):
        def point(t, n):
            return dict(time_ns=int(t*1e9),tasks=n,queue_ns=0,execution_ns=n*1000,
                wal_wait_ns=0,wal_wait_count=0,wal_sync_count=n//4,wal_sync_ns=n*100,
                durability_commands=n,durability_wait_ns=n*2000000,
                durability_batches=n//2,wal_buffered_bytes=n*100,
                pending_tasks=0,version_chains=0,current_commit_ts=n,oldest_snapshot_ts=0)
        result = window_metrics([point(1,4),point(3,12)],0,4)
        self.assertEqual(result['deferred_wait_ms_per_command'],2)
        self.assertEqual(result['commands_per_durability_batch'],2)
        self.assertEqual(result['commits_per_sync'],0)
        self.assertEqual(result['deferred_replies_per_sync'],4)
        self.assertIsNone(result['own_wal_wait_ms_per_commit'])


if __name__ == '__main__': unittest.main()
