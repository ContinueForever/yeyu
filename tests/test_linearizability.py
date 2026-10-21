import unittest

from linearizability import check


class HistoryCheckerTests(unittest.TestCase):
    def test_rejects_completed_write_followed_by_stale_read(self):
        self.assertFalse(check([
            dict(key="k", kind="put", value="new", start=1, end=2),
            dict(key="k", kind="get", result=None, start=3, end=4),
        ]))

    def test_accepts_overlap_and_both_outcomes_of_unknown_write(self):
        unknown = dict(key="k", kind="put", value="maybe", start=1, end=float("inf"), optional=True)
        for result in (None, "maybe"):
            self.assertTrue(check([unknown, dict(key="k", kind="get", result=result, start=3, end=4)]))
        self.assertTrue(check([
            dict(key="k", kind="put", value="new", start=1, end=4),
            dict(key="k", kind="get", result=None, start=2, end=3),
        ]))


if __name__ == "__main__":
    unittest.main()
