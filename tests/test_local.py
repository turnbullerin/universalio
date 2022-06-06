import unittest
import tempfile
import pathlib
import asyncio
from universalio.descriptors import LocalDescriptor
from universalio import GlobalLoopContext
from autoinject import injector


class TestLocalDescriptor(unittest.TestCase):

    @injector.inject
    def setUp(self, loop: GlobalLoopContext):
        self.loop = loop

    def test_is_dir(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertTrue(LocalDescriptor(d).is_dir())
            d = pathlib.Path(d)
            self.assertTrue(LocalDescriptor(d).is_dir())
            f = d / "test.txt"
            self.assertFalse(LocalDescriptor(f).is_dir())
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertFalse(LocalDescriptor(f).is_dir())
            sd = d / "subdir"
            self.assertFalse(LocalDescriptor(sd).is_dir())
            sd.mkdir()
            self.assertTrue(LocalDescriptor(sd).is_dir())

    def test_is_file(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertFalse(LocalDescriptor(d).is_file())
            d = pathlib.Path(d)
            self.assertFalse(LocalDescriptor(d).is_file())
            f = d / "test.txt"
            self.assertFalse(LocalDescriptor(d).is_file())
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertTrue(LocalDescriptor(f).is_file())
            sd = d / "subdir"
            self.assertFalse(LocalDescriptor(sd).is_file())
            sd.mkdir()
            self.assertFalse(LocalDescriptor(sd).is_file())

    def test_exists(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertTrue(LocalDescriptor(d).exists())
            d = pathlib.Path(d)
            self.assertTrue(LocalDescriptor(d).exists())
            f = d / "test.txt"
            self.assertFalse(LocalDescriptor(f).exists())
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertTrue(LocalDescriptor(f).exists())
            sd = d / "subdir"
            self.assertFalse(LocalDescriptor(sd).exists())
            sd.mkdir()
            self.assertTrue(LocalDescriptor(sd).exists())

    def test_list(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
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
            paths = [x.path for x in LocalDescriptor(d).list()]
            self.assertIn(d1, paths)
            self.assertIn(d2, paths)
            self.assertIn(f1, paths)
            self.assertIn(f2, paths)
            self.assertNotIn(dsub, paths)
            self.assertNotIn(fsub, paths)

    def test_is_dir_async(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertTrue(self.loop.run(LocalDescriptor(d).is_dir_async()))
            d = pathlib.Path(d)
            self.assertTrue(self.loop.run(LocalDescriptor(d).is_dir_async()))
            f = d / "test.txt"
            self.assertFalse(self.loop.run(LocalDescriptor(f).is_dir_async()))
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertFalse(self.loop.run(LocalDescriptor(f).is_dir_async()))
            sd = d / "subdir"
            self.assertFalse(self.loop.run(LocalDescriptor(sd).is_dir_async()))
            sd.mkdir()
            self.assertTrue(self.loop.run(LocalDescriptor(sd).is_dir_async()))

    def test_is_file_async(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertFalse(self.loop.run(LocalDescriptor(d).is_file_async()))
            d = pathlib.Path(d)
            self.assertFalse(self.loop.run(LocalDescriptor(d).is_file_async()))
            f = d / "test.txt"
            self.assertFalse(self.loop.run(LocalDescriptor(d).is_file_async()))
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertTrue(self.loop.run(LocalDescriptor(f).is_file_async()))
            sd = d / "subdir"
            self.assertFalse(self.loop.run(LocalDescriptor(sd).is_file_async()))
            sd.mkdir()
            self.assertFalse(self.loop.run(LocalDescriptor(sd).is_file_async()))

    def test_exists_async(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertTrue(self.loop.run(LocalDescriptor(d).exists_async()))
            d = pathlib.Path(d)
            self.assertTrue(self.loop.run(LocalDescriptor(d).exists_async()))
            f = d / "test.txt"
            self.assertFalse(self.loop.run(LocalDescriptor(f).exists_async()))
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertTrue(self.loop.run(LocalDescriptor(f).exists_async()))
            sd = d / "subdir"
            self.assertFalse(self.loop.run(LocalDescriptor(sd).exists_async()))
            sd.mkdir()
            self.assertTrue(self.loop.run(LocalDescriptor(sd).exists_async()))

    def test_list_async(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
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
            paths = [x.path for x in self.loop.run(LocalDescriptor(d).list_async())]
            self.assertIn(d1, paths)
            self.assertIn(d2, paths)
            self.assertIn(f1, paths)
            self.assertIn(f2, paths)
            self.assertNotIn(dsub, paths)
            self.assertNotIn(fsub, paths)
