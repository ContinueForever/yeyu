import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "cluster_backup", Path(__file__).resolve().parents[1] / "scripts/cluster_backup.py")
backup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backup)


class BackupTests(unittest.TestCase):
    def test_manifest_detects_damage_extra_files_missing_nodes_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                backup.manifest(root)
            for i in (1, 2, 3):
                (root / f"node-{i}").mkdir()
                (root / f"node-{i}/raft.state").write_bytes(b"test image")
                (root / f"node-{i}/cluster.identity").write_text(str(i))
            backup.manifest(root)
            backup.verify(root)
            with self.assertRaises(ValueError):
                backup.verify_local(root)
            members = "".join(f"{i}=127.0.0.1:{9100 + i}\n" for i in (1, 2, 3))
            for i in (1, 2, 3):
                (root / f"node-{i}/cluster.identity").write_text(
                    f"yewukv-fixed-cluster-v1\nnode={i}\n" + members)
            backup.manifest(root)
            backup.verify_local(root)
            state = root / "node-1/raft.state"
            state.write_bytes(b"damaged")
            with self.assertRaises(ValueError):
                backup.verify(root)
            state.write_bytes(b"test image")
            (root / "extra").write_text("unexpected")
            with self.assertRaises(ValueError):
                backup.verify(root)
            (root / "extra").unlink()
            (root / "link").symlink_to(state)
            with self.assertRaises(ValueError):
                backup.verify(root)


if __name__ == "__main__":
    unittest.main()
