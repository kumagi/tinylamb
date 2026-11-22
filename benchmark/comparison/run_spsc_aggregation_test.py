"""Regression tests for aggregation benchmark provenance."""
import subprocess
import tempfile
import unittest
from pathlib import Path

from run_spsc_aggregation import git_dirty


class GitStatusTest(unittest.TestCase):
    def test_clean_and_dirty_worktrees(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            self.assertFalse(git_dirty(root))
            (root / 'new-file').write_text('data', encoding='utf-8')
            self.assertTrue(git_dirty(root))
            subprocess.run(['git', '-C', str(root), 'add', 'new-file'], check=True)
            self.assertTrue(git_dirty(root))


if __name__ == '__main__':
    unittest.main()
