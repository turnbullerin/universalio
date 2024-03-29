import unittest
import pathlib
import subprocess
import shutil
import os
import time
from universalio.descriptors import SFTPDescriptor, LocalDescriptor
from universalio import GlobalLoopContext
from autoinject import injector
from .helpers import recursive_rmdir


class TestSFTPDescriptor(unittest.TestCase):

    @injector.inject
    def setUp(self, loop: GlobalLoopContext):
        self.loop = loop

    @classmethod
    def setUpClass(cls):
        p = pathlib.Path(__file__).parent / "tmp" / "sftp"
        if not p.exists():
            p.mkdir()
        kf = p / "test_rsa.key"
        if not kf.exists():
            openssl = shutil.which("openssl")
            if openssl is not None:
                cmd = [
                    openssl,
                    "req",
                    "-out",
                    "CSR.csr",
                    "-new",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-subj",
                    "/dc=ORG/CN=localhost",
                    "-keyout",
                    str(kf)
                ]
                subprocess.run(cmd)
                content = None
                with open(kf, "r") as h:
                    content = h.read()
                with open(kf, "w") as h:
                    h.write(content.replace("-----BEGIN PRIVATE KEY", "-----BEGIN RSA PRIVATE KEY"))
        os.chdir(str(p))
        cmd = [
            "sftpserver",
            "-l",
            "WARNING",
            "-k",
            r".\test_rsa.key"
        ]
        cls.proc = subprocess.Popen(cmd, cwd=str(p))
        time.sleep(0.1)
        cls.server_root = p

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        r = cls.proc.communicate()

    def tearDown(self):
        recursive_rmdir(TestSFTPDescriptor.server_root, ["test_rsa.key"], True)

    def test_is_dir(self):
        self.assertTrue(SFTPDescriptor("sftp://localhost:3373/", "admin", "admin").is_dir())
        test_dir = TestSFTPDescriptor.server_root / "foo"
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/foo", "admin", "admin").is_dir())
        test_dir.mkdir()
        self.assertTrue(SFTPDescriptor("sftp://localhost:3373/foo", "admin", "admin").is_dir())
        test_file = test_dir / "bar.txt"
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/foo/bar.txt", "admin", "admin").is_dir())
        with open(test_file, "w") as h:
            h.write("I am the very model of a modern major general")
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/foo/bar.txt", "admin", "admin").is_dir())

    def test_is_file(self):
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/", "admin", "admin").is_file())
        test_dir = TestSFTPDescriptor.server_root / "foo"
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/foo", "admin", "admin").is_file())
        test_dir.mkdir()
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/foo", "admin", "admin").is_file())
        test_file = test_dir / "bar.txt"
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/foo/bar.txt", "admin", "admin").is_file())
        with open(test_file, "w") as h:
            h.write("I am the very model of a modern major general")
        self.assertTrue(SFTPDescriptor("sftp://localhost:3373/foo/bar.txt", "admin", "admin").is_file())

    def test_exists(self):
        self.assertTrue(SFTPDescriptor("sftp://localhost:3373/", "admin", "admin").exists())
        test_dir = TestSFTPDescriptor.server_root / "foo"
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/foo", "admin", "admin").exists())
        test_dir.mkdir()
        self.assertTrue(SFTPDescriptor("sftp://localhost:3373/foo", "admin", "admin").exists())
        test_file = test_dir / "bar.txt"
        self.assertFalse(SFTPDescriptor("sftp://localhost:3373/foo/bar.txt", "admin", "admin").exists())
        with open(test_file, "w") as h:
            h.write("I am the very model of a modern major general")
        self.assertTrue(SFTPDescriptor("sftp://localhost:3373/foo/bar.txt", "admin", "admin").exists())

    def test_list(self):
        d = TestSFTPDescriptor.server_root
        d1 = d / "foo"
        d1.mkdir()
        d2 = d / "bar"
        d2.mkdir()
        f1 = d / "foo2.txt"
        with open(f1, "w") as h:
            h.write("I am the very model of a modern major general")
        f2 = d / "bar2.txt"
        with open(f2, "w") as h:
            h.write("I am the very model of a modern major general")
        dsub = d1 / "barsub"
        dsub.mkdir()
        fsub = d2 / "foo3.txt"
        with open(fsub, "w") as h:
            h.write("I am the very model of a modern major general")
        paths = [x.path for x in SFTPDescriptor("sftp://localhost:3373/", "admin", "admin").list()]
        self.assertIn(pathlib.PurePosixPath("/bar"), paths)
        self.assertIn(pathlib.PurePosixPath("/foo"), paths)
        self.assertIn(pathlib.PurePosixPath("/foo2.txt"), paths)
        self.assertIn(pathlib.PurePosixPath("/bar2.txt"), paths)
        self.assertNotIn(pathlib.PurePosixPath("/foo/barsub"), paths)
        self.assertNotIn(pathlib.PurePosixPath("/bar/foo3.txt"), paths)
        foo_paths = [x.path for x in SFTPDescriptor("sftp://localhost:3373/foo", "admin", "admin").list()]
        self.assertNotIn(pathlib.PurePosixPath("/bar"), foo_paths)
        self.assertNotIn(pathlib.PurePosixPath("/foo"), foo_paths)
        self.assertNotIn(pathlib.PurePosixPath("/foo2.txt"), foo_paths)
        self.assertNotIn(pathlib.PurePosixPath("/bar2.txt"), foo_paths)
        self.assertIn(pathlib.PurePosixPath("/foo/barsub"), foo_paths)
        self.assertNotIn(pathlib.PurePosixPath("/bar/foo3.txt"), foo_paths)

    def test_parent(self):
        d = TestSFTPDescriptor.server_root
        d1 = d / "foo"
        d1.mkdir()
        d2 = d1 / "bar"
        d2.mkdir()
        f1 = d2 / "foo2.txt"
        with open(f1, "w") as h:
            h.write("I am the very model of a modern major general")
        fd = SFTPDescriptor("sftp://localhost:3373/foo/bar/foo2.txt", "admin", "admin")
        self.assertTrue(fd.is_file())
        self.assertTrue(fd.exists())
        parent1 = fd.parent()
        self.assertIsInstance(parent1, SFTPDescriptor)
        self.assertTrue(parent1.is_dir())
        self.assertEqual(parent1.path, pathlib.PurePosixPath("/foo/bar"))
        parent2 = parent1.parent()
        self.assertIsInstance(parent2, SFTPDescriptor)
        self.assertTrue(parent2.is_dir())
        self.assertEqual(parent2.path, pathlib.PurePosixPath("/foo"))
        parent3 = parent2.parent()
        self.assertIsInstance(parent3, SFTPDescriptor)
        self.assertTrue(parent3.is_dir())
        self.assertEqual(parent3.path, pathlib.PurePosixPath("/"))
        parent4 = parent3.parent()
        self.assertIsInstance(parent4, SFTPDescriptor)
        self.assertTrue(parent4.is_dir())
        self.assertEqual(parent4.path, pathlib.PurePosixPath("/"))

    def test_child(self):
        d = TestSFTPDescriptor.server_root
        d1 = d / "foo"
        d1.mkdir()
        d2 = d1 / "bar"
        d2.mkdir()
        f1 = d2 / "foo2.txt"
        with open(f1, "w") as h:
            h.write("I am the very model of a modern major general")
        fd = SFTPDescriptor("sftp://localhost:3373/", "admin", "admin")
        self.assertTrue(fd.is_dir())
        self.assertTrue(fd.exists())
        child1 = fd.child("foo")
        self.assertIsInstance(child1, SFTPDescriptor)
        self.assertTrue(child1.is_dir())
        self.assertEqual(child1.path, pathlib.PurePosixPath("/foo"))
        child2 = child1.child("bar")
        self.assertIsInstance(child2, SFTPDescriptor)
        self.assertTrue(child2.is_dir())
        self.assertEqual(child2.path, pathlib.PurePosixPath("/foo/bar"))
        child3 = child2.child("foo2.txt")
        self.assertIsInstance(child3, SFTPDescriptor)
        self.assertTrue(child3.is_file())
        self.assertEqual(child3.path, pathlib.PurePosixPath("/foo/bar/foo2.txt"))
        child4 = child2.child("fake.txt")
        self.assertIsInstance(child4, SFTPDescriptor)
        self.assertEqual(child4.path, pathlib.PurePosixPath("/foo/bar/fake.txt"))
        self.assertFalse(child4.exists())
        # Test using trailing slashes
        child5 = child1.child("bar/")
        self.assertIsInstance(child2, SFTPDescriptor)
        self.assertTrue(child2.is_dir())
        self.assertEqual(child2.path, pathlib.PurePosixPath("/foo/bar"))

    def test_match_location(self):
        self.assertFalse(SFTPDescriptor.match_location(r"C:\test\file.txt"))
        self.assertFalse(SFTPDescriptor.match_location(r"\\server\test\file.txt"))
        self.assertFalse(SFTPDescriptor.match_location(r"/var/test/file"))
        self.assertFalse(SFTPDescriptor.match_location(r"~/.ssh/config"))
        self.assertTrue(SFTPDescriptor.match_location(r"sftp://server/file"))
        self.assertFalse(SFTPDescriptor.match_location(r"http://server/file"))
        self.assertFalse(SFTPDescriptor.match_location(r"https://server/file"))
        self.assertFalse(SFTPDescriptor.match_location(r"ftps://server/file"))
        self.assertFalse(SFTPDescriptor.match_location(r"ftp://server/file"))

    def test_read(self):
        d = TestSFTPDescriptor.server_root
        pirate_king = """I am the very model of a modern major general\nI've information vegetable, animal, and mineral"""
        f = pathlib.Path(d) / "test.txt"
        with open(f, "wb") as h:
            h.write(pirate_king.encode("utf-8"))
        fd = SFTPDescriptor(r"sftp://localhost:3373/test.txt", "admin", "admin")
        content = fd.text("ascii")
        self.assertEqual(pirate_king, content)

    def test_write(self):
        pirate_king = """I am the very model of a modern major general\nI've information vegetable, animal, and mineral"""
        d = TestSFTPDescriptor.server_root
        f = pathlib.Path(d) / "test.txt"
        fd = SFTPDescriptor(r"sftp://localhost:3373/test.txt", "admin", "admin")
        self.assertFalse(f.exists())
        fd.write(pirate_king.encode("utf-8"))
        with open(f, "rb") as h:
            content = h.read().decode("utf-8")
            self.assertEqual(content, pirate_king)

    def test_copy_to_upload(self):
        pirate_king = """I am the very model of a modern major general\nI've information vegetable, animal, and mineral"""
        d = TestSFTPDescriptor.server_root
        f = pathlib.Path(d) / "test.txt"
        with open(f, "wb") as h:
            h.write(pirate_king.encode("utf-8"))
        ld = LocalDescriptor(f)
        fd = SFTPDescriptor(r"sftp://localhost:3373/test2.txt", "admin", "admin")
        self.assertTrue(ld.exists())
        self.assertFalse(fd.exists())
        ld.copy(fd)
        self.assertTrue(fd.exists())
        self.assertEqual(fd.text("utf-8"), pirate_king)

    def test_copy_to_download(self):
        pirate_king = """I am the very model of a modern major general\nI've information vegetable, animal, and mineral"""
        d = TestSFTPDescriptor.server_root
        f = pathlib.Path(d) / "test.txt"
        with open(f, "wb") as h:
            h.write(pirate_king.encode("utf-8"))
        ld = LocalDescriptor(d / "test2.txt")
        fd = SFTPDescriptor(r"sftp://localhost:3373/test.txt", "admin", "admin")
        self.assertTrue(fd.exists())
        self.assertFalse(ld.exists())
        fd.copy(ld)
        self.assertTrue(ld.exists())
        self.assertEqual(ld.text("utf-8"), pirate_king)

    def test_remove(self):
        pirate_king = """I am the very model of a modern major general\nI've information vegetable, animal, and mineral"""
        d = TestSFTPDescriptor.server_root
        sd = pathlib.Path(d) / "foo"
        sd.mkdir()
        f = sd / "test.txt"
        with open(f, "wb") as h:
            h.write(pirate_king.encode("utf-8"))
        fd2 = SFTPDescriptor(r"sftp://localhost:3373/foo/test.txt", "admin")
        self.assertTrue(fd2.exists())
        self.assertTrue(f.exists())
        fd2.remove()
        self.assertFalse(fd2.exists())
        self.assertFalse(f.exists())
