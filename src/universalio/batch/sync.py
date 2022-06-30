from universalio.fileman import FileManager
from autoinject import injector
import asyncio
import time
import zirconium as zr
import sqlite3
from batch import AsynchronousThread
from universalio import GlobalLoopContext
import os


class DirectorySync:

    files: FileManager = None
    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, in_memory_checker=False):
        self.t = AsynchronousThread()
        self.t.start()
        self.sem = asyncio.Semaphore(5)
        self.in_memory_checker = in_memory_checker

    def sync_dir(self, src, dst, name=None):
        if name is None:
            name = "sync_{}_to_{}".format(src, dst)
        self.t.run_coro(name, self._do_sync, src, dst)
        return name

    def result(self, job_name):
        return self.t.result(job_name)

    def wait_for_job(self, op_name):
        while True:
            if self.t.is_completed(op_name):
                return True
            time.sleep(0.05)

    def wait_for_all(self):
        self.t.join()

    async def _do_sync(self, src, dst):
        checker = None
        if self.in_memory_checker:
            checker = InMemoryChecker()
        else:
            db = self.config.as_path(("universalio", "sync_db"), default=r".\.sync.db")
            checker = SqliteSyncManager(db)
        synchronizer = DirectorySynchronizer(src, dst, checker, self.sem)
        await synchronizer.sync_all()


class DirectorySynchronizer:

    loop: GlobalLoopContext = None
    files: FileManager = None

    @injector.construct
    def __init__(self, src, dst, sync_checker, sem):
        self.source = self.files.get_descriptor(src)
        self.target = self.files.get_descriptor(dst)
        self.checker = sync_checker
        self.sem = sem

    async def sync_all(self):
        work = []
        async for src_file, dst_file in self.source.crawl(self.target):
            src_print = await src_file.fingerprint_async()
            if await self._check_sync(src_file, dst_file, src_print):
                tsk = self.loop.create_task(self._do_file_copy(src_file, dst_file, src_print))
                work.append(tsk)

    async def _do_file_copy(self, src_file, dst_file, src_print):
        async with self.sem:
            await src_file.copy(dst_file, _skip_dir_check=True, allow_overwrite=True, use_partial_file=True)
            await self.checker.save_fingerprint(str(src_file), src_print)

    async def _check_sync(self, src_file, dst_file, src_print):
        # A fingerprint of None means we can't really tell if it changed or not
        if src_print is None:
            return True
        # Destination file doesn't exist, we need to make it
        if not await dst_file.exists_async():
            return True
        # Check if fingerprint has changed since last sync
        last_print = await self.checker.get_last_fingerprint(str(src_file))
        return last_print != src_print


class SqliteSyncManager:

    def __init__(self, db):
        self.db = db
        self.db_lock = asyncio.Lock()

    def get_last_fingerprint(self, file):
        pass

    def save_fingerprint(self, file, fingerprint):
        pass


class InMemoryChecker:

    def __init__(self, db):
        self._lock = asyncio.Lock()
        self._mem = {}

    async def get_last_fingerprint(self, file):
        if file in self._mem:
            return self._mem[file]
        return None

    async def save_fingerprint(self, file, fingerprint):
        async with self._lock:
            self._mem[file] = fingerprint


