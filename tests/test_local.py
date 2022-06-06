import unittest
import tempfile
import pathlib
import asyncio
from universalio.descriptors import LocalDescriptor


class TestLocalDescriptor(unittest.TestCase):

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

    def test_is_dir_async(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertTrue(asyncio.run(LocalDescriptor(d).is_dir_async()))
            d = pathlib.Path(d)
            self.assertTrue(asyncio.run(LocalDescriptor(d).is_dir_async()))
            f = d / "test.txt"
            self.assertFalse(asyncio.run(LocalDescriptor(f).is_dir_async()))
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertFalse(asyncio.run(LocalDescriptor(f).is_dir_async()))
            sd = d / "subdir"
            self.assertFalse(asyncio.run(LocalDescriptor(sd).is_dir_async()))
            sd.mkdir()
            self.assertTrue(asyncio.run(LocalDescriptor(sd).is_dir_async()))

    def test_is_file_async(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertFalse(asyncio.run(LocalDescriptor(d).is_file_async()))
            d = pathlib.Path(d)
            self.assertFalse(asyncio.run(LocalDescriptor(d).is_file_async()))
            f = d / "test.txt"
            self.assertFalse(asyncio.run(LocalDescriptor(d).is_file_async()))
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertTrue(asyncio.run(LocalDescriptor(f).is_file_async()))
            sd = d / "subdir"
            self.assertFalse(asyncio.run(LocalDescriptor(sd).is_file_async()))
            sd.mkdir()
            self.assertFalse(asyncio.run(LocalDescriptor(sd).is_file_async()))

    def test_exists_async(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertTrue(asyncio.run(LocalDescriptor(d).exists_async()))
            d = pathlib.Path(d)
            self.assertTrue(asyncio.run(LocalDescriptor(d).exists_async()))
            f = d / "test.txt"
            self.assertFalse(asyncio.run(LocalDescriptor(f).exists_async()))
            with open(f, "w") as h:
                h.write("I am the very model of a modern major general")
            self.assertTrue(asyncio.run(LocalDescriptor(f).exists_async()))
            sd = d / "subdir"
            self.assertFalse(asyncio.run(LocalDescriptor(sd).exists_async()))
            sd.mkdir()
            self.assertTrue(asyncio.run(LocalDescriptor(sd).exists_async()))
