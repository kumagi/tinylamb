import io
import unittest

from tpch_pg import PipeRows


class PipeRowsTest(unittest.TestCase):
    def test_chunk_boundaries_preserve_all_fields_and_record_boundaries(self):
        original = '1|a"b|\n2||tail|\n3|hello|\n'
        for size in (1, 2, 7, 8192):
            stream = PipeRows(io.StringIO(original))
            pieces = []
            while chunk := stream.read(size):
                pieces.append(chunk)
            self.assertEqual(''.join(pieces), '1|a"b\n2||tail\n3|hello\n')
            self.assertEqual(stream.read(size), '')

    def test_missing_record_terminator_fails_instead_of_dropping_data(self):
        for invalid in ('1|x\n', '1|x|'):
            with self.assertRaises(ValueError):
                PipeRows(io.StringIO(invalid)).read(8192)


if __name__ == '__main__':
    unittest.main()
