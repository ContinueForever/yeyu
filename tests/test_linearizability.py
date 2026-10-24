import unittest

from linearizability import check


class HistoryCheckerTests(unittest.TestCase):
    def test_equal_boundary_preserves_real_time_order_and_delete(self):
        self.assertFalse(check([
            dict(key="k", kind="put", value="new", start=1, end=2),
            dict(key="k", kind="get", result=None, start=2, end=3),
        ]))
        self.assertFalse(check([
            dict(key="k", kind="put", value="old", start=1, end=2),
            dict(key="k", kind="del", start=3, end=4),
            dict(key="k", kind="get", result="old", start=5, end=6),
        ]))

    def test_invalid_histories_fail_instead_of_being_treated_as_deletes(self):
        for operation in (
            dict(key="k", kind="unknown", start=1, end=2),
            dict(key="k", kind="get", result=None, start=2, end=1),
            dict(key="k", kind="put", value="x", start=1, end=2, optional=True),
            dict(key="k", kind="get", result="x", start=1, end=float("inf"), optional=True),
            dict(key="k", kind="put", start=1, end=2),
        ):
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                check([operation])

    def test_independent_keys_do_not_share_values(self):
        self.assertTrue(check([
            dict(key="a", kind="put", value="a", start=1, end=2),
            dict(key="b", kind="get", result=None, start=3, end=4),
        ]))

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
