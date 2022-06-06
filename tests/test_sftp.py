import unittest
import asyncio
import pathlib
import subprocess
import shutil
import os
import time
from universalio.descriptors import SFTPDescriptor


class TestSFTPDescriptor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        p = pathlib.Path(__file__).parent / "tmp"
        if not p.exists():
            p.mkdir()
        kf = p / "test_rsa.key"
        if not kf.exists():
            openssl = shutil.which("openssl")
            cmd = [
                openssl,
                "req",
                "-out",
                "CSR.csr",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "-subj",
                "/dc=ORG/CN=localhost",
                str(kf)
            ]
            subprocess.run(cmd)
            content = None
            with open(kf, "r") as h:
                content = h.read()
            with open(kf, "w") as h:
                h.write(content.replace("-----BEGIN PRIVATE KEY", "-----BEGIN RSA PRIVATE KEY"))
        os.chdir(p)
        cmd = [
            "sftpserver",
            "-l",
            "WARNING",
            "-k",
            "./test_rsa.key"
        ]
        cls.proc = subprocess.Popen(cmd)
        time.sleep(0.1)
        cls.server_root = p

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.communicate()

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
        os.remove(test_file)
        os.rmdir(test_dir)

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
        os.remove(test_file)
        os.rmdir(test_dir)
