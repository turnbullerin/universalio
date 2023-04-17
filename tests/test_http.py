import sys
import unittest
import pathlib
import subprocess
import shutil
import os
import time
from universalio.descriptors import HttpDescriptor, LocalDescriptor
from universalio import GlobalLoopContext
from autoinject import injector
from .helpers import recursive_rmdir


class TestHttpDescriptor(unittest.TestCase):

    @injector.inject
    def setUp(self, loop: GlobalLoopContext):
        self.loop = loop

    @classmethod
    def setUpClass(cls):
        p = pathlib.Path(__file__).parent / "tmp" / "http"
        os.chdir(str(p))
        cmd = [
            sys.executable,
            "-m"
            "flask",
            "run",
        ]
        # Of note, if you want to see the errors from Flask here, you can change stdout/stderr
        cls.proc = subprocess.Popen(cmd, cwd=str(p), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.1)
        cls.server_root = p / "content"

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        r = cls.proc.communicate()

    def tearDown(self):
        recursive_rmdir(TestHttpDescriptor.server_root, ["README.md"], True)

    def _wrap(self, path):
        return HttpDescriptor("http://localhost:5000/" + str(path).lstrip("/"))

    def test_is_dir(self):
        self.assertTrue(self._wrap("").is_dir())
        self.assertTrue(self._wrap("/").is_dir())
        self.assertTrue(self._wrap("/foo/").is_dir())
        self.assertTrue(self._wrap("/foo/bar/").is_dir())
        self.assertFalse(self._wrap("/foo/bar/test.txt").is_dir())
        self.assertFalse(self._wrap("/foo/bar").is_dir())

    def test_is_file(self):
        self.assertFalse(self._wrap("").is_file())
        self.assertFalse(self._wrap("/").is_file())
        self.assertFalse(self._wrap("/foo/").is_file())
        self.assertFalse(self._wrap("/foo/bar/").is_file())
        self.assertTrue(self._wrap("/foo/bar/test.txt").is_file())
        self.assertTrue(self._wrap("/foo/bar").is_file())

    def test_exists(self):
        self.assertTrue(self._wrap("").exists())
        self.assertTrue(self._wrap("/").exists())
        self.assertFalse(self._wrap("/foo/").exists())
        self.assertFalse(self._wrap("/foo/bar/").exists())
        self.assertFalse(self._wrap("/foo/bar/test.txt").exists())
        root = TestHttpDescriptor.server_root
        f = root / "foo"
        f.mkdir()
        self.assertTrue(self._wrap("/foo/").exists())
        self.assertFalse(self._wrap("/foo/bar/").exists())
        self.assertFalse(self._wrap("/foo/bar/test.txt").exists())
        b = f / "bar"
        b.mkdir()
        self.assertTrue(self._wrap("/foo/bar/").exists())
        self.assertFalse(self._wrap("/foo/bar/test.txt").exists())
        tst = b / "test.txt"
        with open(tst, "w") as h:
            h.write("I am the very model of a modern major general")
        self.assertTrue(self._wrap("/foo/bar/test.txt").exists())

    def test_parent(self):
        http = self._wrap("/foo/bar/Untitled.png")
        self.assertTrue(http.is_file())
        self.assertFalse(http.is_dir())
        parent = http.parent()
        self.assertFalse(parent.is_file())
        self.assertTrue(parent.is_dir())
        self.assertEqual(parent.path, pathlib.PurePosixPath("/foo/bar"))
        parent2 = parent.parent()
        self.assertFalse(parent2.is_file())
        self.assertTrue(parent2.is_dir())
        self.assertEqual(parent2.path, pathlib.PurePosixPath("/foo"))
        parent3 = parent2.parent()
        self.assertFalse(parent3.is_file())
        self.assertTrue(parent3.is_dir())
        self.assertEqual(parent3.path, pathlib.PurePosixPath("/"))
        parent4 = parent3.parent()
        self.assertFalse(parent4.is_file())
        self.assertTrue(parent4.is_dir())
        self.assertEqual(parent4.path, pathlib.PurePosixPath("/"))

    def test_child(self):
        http = self._wrap("/")
        self.assertFalse(http.is_file())
        self.assertTrue(http.is_dir())
        child1 = http.child("foo/")
        self.assertFalse(child1.is_file())
        self.assertTrue(child1.is_dir())
        child2 = child1.child("bar/")
        self.assertFalse(child2.is_file())
        self.assertTrue(child2.is_dir())
        child3 = child2.child("Untitled.png")
        self.assertTrue(child3.is_file())
        self.assertFalse(child3.is_dir())
        child4 = child2.child("zazz")
        self.assertTrue(child4.is_file())
        self.assertFalse(child4.is_dir())

    def test_rw_rem_chain(self):
        dr = self._wrap("/")
        file = dr.child("test_file.txt")
        if file.exists():
            file.remove()
        self.assertFalse(file.exists())
        file.write("I am the very model of a modern major general".encode("utf-8"))
        self.assertTrue(file.exists())
        self.assertEqual("I am the very model of a modern major general", file.text("utf-8"))
