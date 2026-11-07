"""Read-result mismatches must invalidate a trial even with valid final totals."""
import unittest

from run import ResultMismatch, trial_is_valid, validate_read_result


class ValidationTest(unittest.TestCase):
    def test_sql_conflict_can_keep_a_correct_trial_valid(self):
        self.assertTrue(trial_is_valid([{'errors': 1, 'validation_failures': 0}],
                                       1, True, None))

    def test_read_mismatch_invalidates_conserved_final_state(self):
        self.assertFalse(trial_is_valid([{'errors': 1, 'validation_failures': 1}],
                                        1, True, None))

    def test_ordered_nonkey_rows_are_checked_exactly(self):
        sql = ['SELECT id, balance FROM accounts WHERE bucket = 7 ORDER BY id LIMIT 10']
        rows = [(i, 100000) for i in range(7, 1000, 100)]
        validate_read_result('nonkey_read', rows, sql, 1000, 1000, 'transaction')
        with self.assertRaises(ResultMismatch):
            validate_read_result('nonkey_read', rows[::-1], sql, 1000, 1000, 'transaction')

    def test_htap_wrong_sum_and_malformed_rows_are_correctness_failures(self):
        for rows in [[(0, 1)], [(0,)]]:
            with self.subTest(rows=rows), self.assertRaises(ResultMismatch):
                validate_read_result('htap', rows, [], 100, 1000, 'reader')

    def test_noop_and_point_results_are_checked(self):
        for name in ('noop', 'point_read'):
            with self.subTest(name=name), self.assertRaises(ResultMismatch):
                validate_read_result(name, [(0,)], [], 100, 1000, 'transaction')

    def test_range_sum_checks_full_and_truncated_ranges(self):
        for low, high in ((0, 99), (9901, 9999), (9999, 9999)):
            sql = [f'SELECT SUM(balance) FROM accounts WHERE id BETWEEN {low} AND {high}']
            expected = 100000 * (high - low + 1)
            validate_read_result('range_read', [(expected,)], sql, 10000, 1000000, 'transaction')
            for result in ([(expected + 1,)], [(None,)], [], [(expected,), (expected,)]):
                with self.subTest(low=low, result=result), self.assertRaises(ResultMismatch):
                    validate_read_result('range_read', result, sql, 10000, 1000000, 'transaction')

    def test_range_malformed_bounds_invalidate_result(self):
        with self.assertRaises(ResultMismatch):
            validate_read_result('range_read', [(100000,)], ['bad SQL'], 10000, 1000000, 'transaction')


if __name__ == '__main__':
    unittest.main()
