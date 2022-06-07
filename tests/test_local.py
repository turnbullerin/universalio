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

    def test_parent(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            sd = d / "foo"
            sd.mkdir()
            sd2 = sd / "bar"
            sd2.mkdir()
            f = sd2 / "test.txt"
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            fd = LocalDescriptor(f)
            self.assertTrue(fd.is_file())
            parent1 = fd.parent()
            self.assertIsInstance(parent1, LocalDescriptor)
            self.assertTrue(parent1.is_dir())
            self.assertEqual(parent1.path, sd2)
            parent2 = parent1.parent()
            self.assertIsInstance(parent2, LocalDescriptor)
            self.assertTrue(parent2.is_dir())
            self.assertEqual(parent2.path, sd)
            last = d
            test = d.parent
            while not str(last) == str(test):
                last = test
                test = last.parent
            fd2 = LocalDescriptor(test)
            parent3 = fd2.parent()
            self.assertIsInstance(parent3, LocalDescriptor)
            self.assertTrue(parent3.is_dir())
            self.assertEqual(parent3.path, test)

    def test_read(self):
        pirate_king = """I am the very model of a modern major general\nI've information vegetable, animal, and mineral"""
        with tempfile.TemporaryDirectory() as d:
            f = pathlib.Path(d) / "test.txt"
            with open(f, "wb") as h:
                h.write(pirate_king.encode("utf-8"))
            fd = LocalDescriptor(f)
            content = fd.text("ascii")
            self.assertEqual(pirate_king, content)

    def test_write(self):
        pirate_king = """I am the very model of a modern major general\nI've information vegetable, animal, and mineral"""
        with tempfile.TemporaryDirectory() as d:
            f = pathlib.Path(d) / "test.txt"
            fd = LocalDescriptor(f)
            self.assertFalse(f.exists())
            fd.write(pirate_king.encode("utf-8"))
            with open(f, "rb") as h:
                content = h.read().decode("utf-8")
                self.assertEqual(content, pirate_king)

    def test_child(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            sd = d / "foo"
            sd.mkdir()
            sd2 = sd / "bar"
            sd2.mkdir()
            f = sd2 / "test.txt"
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            fd = LocalDescriptor(d)
            self.assertTrue(d.is_dir())
            child1 = fd.child("foo")
            self.assertIsInstance(child1, LocalDescriptor)
            self.assertTrue(child1.is_dir())
            self.assertEqual(child1.path, sd)
            child2 = child1.child("bar")
            self.assertIsInstance(child2, LocalDescriptor)
            self.assertTrue(child2.is_dir())
            self.assertEqual(child2.path, sd2)
            child3 = child2.child("test.txt")
            self.assertIsInstance(child3, LocalDescriptor)
            self.assertTrue(child3.is_file())
            self.assertEqual(child3.path, f)
            child4 = child2.child("fake.txt")
            self.assertIsInstance(child4, LocalDescriptor)
            self.assertFalse(child4.exists())
            self.assertEqual(child4.path, sd2 / "fake.txt")

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

    def test_match_location(self):
        self.assertTrue(LocalDescriptor.match_location(r"C:\test\file.txt"))
        self.assertTrue(LocalDescriptor.match_location(r"\\server\test\file.txt"))
        self.assertTrue(LocalDescriptor.match_location(r"/var/test/file"))
        self.assertTrue(LocalDescriptor.match_location(r"~/.ssh/config"))
        self.assertFalse(LocalDescriptor.match_location(r"sftp://server/file"))
        self.assertFalse(LocalDescriptor.match_location(r"http://server/file"))
        self.assertFalse(LocalDescriptor.match_location(r"https://server/file"))
        self.assertFalse(LocalDescriptor.match_location(r"ftps://server/file"))
        self.assertFalse(LocalDescriptor.match_location(r"ftp://server/file"))

