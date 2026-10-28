import math
import unittest
from revision_history import check


def op(kind, start, end, **values):
    return dict(key="k", kind=kind, start=start, end=end, **values)


class RevisionHistoryTests(unittest.TestCase):
    def test_two_publishers_have_only_one_winner(self):
        initial = op("put", 1, 2, value="old", code=0, revision=10)
        first = op("cas", 3, 6, expected_revision=10, value="a", code=0, revision=12)
        second = op(
            "cas",
            4,
            5,
            expected_revision=10,
            value="b",
            code=7,
            revision=12,
            result="a",
        )
        self.assertTrue(check([initial, first, second]))
        second.update(code=0, revision=13)
        self.assertFalse(check([initial, first, second]))

    def test_unknown_cas_can_apply_after_timeout_or_remain_absent(self):
        unknown = op(
            "cas",
            1,
            math.inf,
            value="maybe",
            expected_revision=0,
            code=6,
            optional=True,
        )
        self.assertTrue(
            check([unknown, op("get", 3, 4, code=0, result="maybe", revision=57)])
        )
        self.assertTrue(
            check([unknown, op("get", 3, 4, code=1, result=None, revision=0)])
        )
        self.assertFalse(
            check([unknown, op("get", 3, 4, code=0, result="other", revision=57)])
        )

    def test_unknown_version_is_constrained_by_reads_and_later_cas(self):
        history = [
            op("put", 1, math.inf, value="x", code=6, optional=True),
            op("get", 2, 3, code=0, result="x", revision=50),
            op("cas", 4, 5, expected_revision=50, value="y", code=0, revision=61),
        ]
        self.assertTrue(check(history))
        history[-1]["revision"] = 49
        self.assertFalse(check(history))

    def test_delete_recreate_rejects_aba_and_version_regression(self):
        history = [
            op("put", 1, 2, value="same", code=0, revision=10),
            op("del", 3, 4, code=0, revision=12),
            op("cas", 5, 6, expected_revision=0, value="same", code=0, revision=15),
            op(
                "cas",
                7,
                8,
                expected_revision=10,
                value="bad",
                code=7,
                revision=15,
                result="same",
            ),
        ]
        self.assertTrue(check(history))
        history[2]["revision"] = 11
        self.assertFalse(check(history))

    def test_conflict_reports_actual_value_and_missing_key(self):
        self.assertTrue(
            check(
                [
                    op(
                        "cas",
                        1,
                        2,
                        expected_revision=7,
                        value="v",
                        code=7,
                        result=None,
                        revision=0,
                    )
                ]
            )
        )
        self.assertFalse(
            check(
                [
                    op(
                        "cas",
                        1,
                        2,
                        expected_revision=0,
                        value="v",
                        code=7,
                        result=None,
                        revision=0,
                    )
                ]
            )
        )
        self.assertFalse(
            check(
                [
                    op("put", 1, 2, value="x", code=0, revision=10),
                    op(
                        "cas",
                        3,
                        4,
                        expected_revision=9,
                        value="v",
                        code=7,
                        result="wrong",
                        revision=10,
                    ),
                ]
            )
        )

    def test_malformed_and_cross_key_duplicate_success_versions(self):
        with self.assertRaises(ValueError):
            check([op("cas", 1, 2, value="v", code=0, revision=1)])
        with self.assertRaises(ValueError):
            check([op("put", 1, 2, value="v", code=6, optional=True)])
        self.assertFalse(
            check(
                [
                    op("put", 1, 2, value="a", code=0, revision=2),
                    dict(
                        key="other",
                        kind="put",
                        start=1,
                        end=2,
                        value="b",
                        code=0,
                        revision=2,
                    ),
                ]
            )
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
