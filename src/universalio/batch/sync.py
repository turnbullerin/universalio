from universalio.fileman import FileManager
from autoinject import injector
import asyncio
import time
import zirconium as zr
import sqlite3
from .batch import AsynchronousThread
from universalio import GlobalLoopContext
import logging


class DirectorySync:

    files: FileManager = None
    config: zr.ApplicationConfig = None

    @injector.construct
    def __init__(self, in_memory_checker=False):
        self.t = AsynchronousThread(self._complete)
        self.t.start()
        self.sem = None
        self.in_memory_checker = in_memory_checker
        self.checker = None

    def _complete(self):
        if self.checker:
            self.checker.close()
            self.checker = None

    def sync_dir(self, src, dst, name=None):
        if name is None:
            name = "sync_{}_to_{}".format(src, dst)
        self.t.run_coro(name, self._do_sync, src, dst)
        return name

    def is_completed(self, job_name):
        return self.t.is_completed(job_name)

    def result(self, job_name):
        return self.t.result(job_name)

    def wait_for_job(self, op_name):
        while True:
            if self.t.is_completed(op_name):
                return True
            time.sleep(0.05)

    def wait_for_all(self):
        self.t.wait_join()

    async def _do_sync(self, src, dst):
        if self.sem is None:
            self.sem = asyncio.Semaphore(5)
        if self.checker is None:
            if self.in_memory_checker:
                self.checker = InMemoryChecker()
            else:
                db = self.config.as_path(("universalio", "sync_db"), default=r".\.sync.db")
                self.checker = SqliteSyncManager(db)
        synchronizer = DirectorySynchronizer(src, dst, self.checker, self.sem)
        await synchronizer.sync_all()
        return synchronizer.file_updates


class DirectorySynchronizer:

    loop: GlobalLoopContext = None
    files: FileManager = None

    @injector.construct
    def __init__(self, src, dst, sync_checker, sem):
        self.source = self.files.get_descriptor(src)
        self.target = self.files.get_descriptor(dst)
        self.checker = sync_checker
        self.sem = sem
        self.file_updates = 0

    async def sync_all(self):
        work = []
        logging.getLogger(__name__).info("Synchronizing directory {}".format(self.source))
        async for src_file, dst_file in self.source.crawl(self.target):
            src_print = await src_file.fingerprint_async()
            logging.getLogger(__name__).debug("Checking file {}".format(src_file))
            if await self._check_sync(src_file, dst_file, src_print):
                tsk = self.loop.create_task(self._do_file_copy(src_file, dst_file, src_print))
                work.append(tsk)
                self.file_updates += 1
        await asyncio.gather(*work)

    async def _do_file_copy(self, src_file, dst_file, src_print):
        async with self.sem:
            logging.getLogger(__name__).debug("Copying file {}".format(src_file))
            await src_file.copy_async(dst_file, _skip_dir_check=True, allow_overwrite=True, use_partial_file=True)
            logging.getLogger(__name__).trace("Saving fingerprint for {}".format(src_file))
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
        self.conn = sqlite3.connect(self.db)
        ddl = "CREATE TABLE IF NOT EXISTS fingerprint_records (filepath text UNIQUE, fingerprint text)"
        cursor = self.conn.cursor()
        cursor.execute(ddl)
        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()

    async def get_last_fingerprint(self, file):
        q = "SELECT fingerprint FROM fingerprint_records WHERE filepath = ?"
        cursor = self.conn.cursor()
        fp = cursor.execute(q, [file]).fetchone()
        return fp[0] if fp else None

    async def save_fingerprint(self, file, fingerprint):
        async with self.db_lock:
            q = "REPLACE INTO fingerprint_records (filepath, fingerprint) VALUES (?, ?)"
            cursor = self.conn.cursor()
            cursor.execute(q, [file, fingerprint])
            self.conn.commit()


class InMemoryChecker:

    def __init__(self):
        self._lock = asyncio.Lock()
        self._mem = {}

    def close(self):
        pass

    async def get_last_fingerprint(self, file):
        if file in self._mem:
            return self._mem[file]
        return None

    async def save_fingerprint(self, file, fingerprint):
        async with self._lock:
            self._mem[file] = fingerprint


