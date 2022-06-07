import unittest
import pathlib
import subprocess
import shutil
import os
import time
import toml
from universalio.descriptors import AzureBlobDescriptor, LocalDescriptor
from universalio import GlobalLoopContext


class TestSFTPDescriptor(unittest.TestCase):

    def setUp(self):
        self.credentials = {}
        cred_file = pathlib.Path(__file__).parent / "tmp" / "credentials.toml"
        if os.path.exists(cred_file):
            with open(cred_file, "r") as h:
                self.credentials = toml.load(h)

    def _check_azure_credentials(self):
        if not "azure" in self.credentials:
            return False
        if not "blob" in self.credentials["azure"]:
            return False
        if not "erintest2" in self.credentials["azure"]["blob"]:
            return False
        if not "connect_str" in self.credentials["azure"]["blob"]["erintest2"]:
            return False
        return True

    def _blob(self, path):
        return AzureBlobDescriptor(
            "https://erintest2.blob.core.windows.net/test/" + str(path).lstrip("/"),
            self.credentials["azure"]["blob"]["erintest2"]["connect_str"]
        )

    def test_is_file(self):
        if not self._check_azure_credentials():
            return
        self.assertTrue(self._blob("/Untitled.png").is_file())
        self.assertFalse(self._blob("/foo").is_file())
        self.assertFalse(self._blob("/foo/bar").is_file())
        self.assertTrue(self._blob("/foo/bar/Untitled.png").is_file())

    def test_is_dir(self):
        if not self._check_azure_credentials():
            return
        self.assertFalse(self._blob("/Untitled.png").is_dir())
        self.assertTrue(self._blob("/foo").is_dir())
        self.assertTrue(self._blob("/foo/bar").is_dir())
        self.assertFalse(self._blob("/foo/bar/Untitled.png").is_dir())

    def test_exists(self):
        if not self._check_azure_credentials():
            return
        self.assertTrue(self._blob("/Untitled.png").exists())
        self.assertTrue(self._blob("/foo").exists())
        self.assertTrue(self._blob("/foo/bar").exists())
        self.assertTrue(self._blob("/foo/bar/Untitled.png").exists())

    def test_list(self):
        if not self._check_azure_credentials():
            return
        paths = [str(x) for x in self._blob("/").list()]
        self.assertIn("https://erintest2.blob.core.windows.net/test/foo", paths)
        self.assertIn("https://erintest2.blob.core.windows.net/test/Untitled.png", paths)
        self.assertNotIn("https://erintest2.blob.core.windows.net/test/foo/bar", paths)
        self.assertNotIn("https://erintest2.blob.core.windows.net/test/foo/world+only+sword+lesbians.pdf", paths)
        self.assertNotIn("https://erintest2.blob.core.windows.net/test/foo/bar2/basic+moves.pdf", paths)
        paths = [str(x) for x in self._blob("/foo").list()]
        self.assertIn("https://erintest2.blob.core.windows.net/test/foo/bar", paths)
        self.assertIn("https://erintest2.blob.core.windows.net/test/foo/bar2", paths)
        self.assertIn("https://erintest2.blob.core.windows.net/test/foo/world+only+sword+lesbians.pdf", paths)
        self.assertNotIn("https://erintest2.blob.core.windows.net/test/foo/bar2/basic+moves.pdf", paths)
        self.assertNotIn("https://erintest2.blob.core.windows.net/test/foo/bar/Untitled.png", paths)

    def test_parent(self):
        if not self._check_azure_credentials():
            return
        blob = self._blob("/foo/bar/Untitled.png")
        self.assertTrue(blob.exists())
        self.assertTrue(blob.is_file())
        self.assertFalse(blob.is_dir())
        parent = blob.parent()
        self.assertTrue(parent.exists())
        self.assertFalse(parent.is_file())
        self.assertTrue(parent.is_dir())
        self.assertEqual(parent.path, pathlib.PurePosixPath("/foo/bar"))
        parent2 = parent.parent()
        self.assertTrue(parent2.exists())
        self.assertFalse(parent2.is_file())
        self.assertTrue(parent2.is_dir())
        self.assertEqual(parent2.path, pathlib.PurePosixPath("/foo"))
        parent3 = parent2.parent()
        self.assertTrue(parent3.exists())
        self.assertFalse(parent3.is_file())
        self.assertTrue(parent3.is_dir())
        self.assertEqual(parent3.path, pathlib.PurePosixPath("/"))
        parent4 = parent3.parent()
        self.assertTrue(parent4.exists())
        self.assertFalse(parent4.is_file())
        self.assertTrue(parent4.is_dir())
        self.assertEqual(parent4.path, pathlib.PurePosixPath("/"))

    def test_child(self):
        if not self._check_azure_credentials():
            return
        blob = self._blob("/")
        self.assertTrue(blob.exists())
        self.assertFalse(blob.is_file())
        self.assertTrue(blob.is_dir())
        child1 = blob.child("foo")
        self.assertTrue(child1.exists())
        self.assertFalse(child1.is_file())
        self.assertTrue(child1.is_dir())
        child2 = child1.child("bar")
        self.assertTrue(child2.exists())
        self.assertFalse(child2.is_file())
        self.assertTrue(child2.is_dir())
        child3 = child2.child("Untitled.png")
        self.assertTrue(child3.exists())
        self.assertTrue(child3.is_file())
        self.assertFalse(child3.is_dir())
        child4 = child2.child("zazz")
        self.assertFalse(child4.exists())
        self.assertFalse(child4.is_file())
        self.assertFalse(child4.is_dir())

    def test_rw_rem_chain(self):
        if not self._check_azure_credentials():
            return
        blob = self._blob("/")
        file = blob.child("test_file.txt")
        if file.exists():
            file.remove()
        self.assertFalse(file.exists())
        file.write("I am the very model of a modern major general".encode("utf-8"))
        self.assertTrue(file.exists())
        self.assertEqual("I am the very model of a modern major general", file.text("utf-8"))
