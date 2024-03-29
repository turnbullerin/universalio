from threading import Thread
import queue
from universalio.global_loop import GlobalLoopContext
from universalio.fileman import FileManager
from autoinject import injector
import asyncio
import time
import logging


class AsynchronousThread(Thread):

    loop: GlobalLoopContext = None

    @injector.construct
    def __init__(self, on_complete=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_complete = on_complete
        self._q = queue.Queue()
        self._tasks = set()
        self._completed = {}
        self._exit_set = False
        self.daemon = True

    def run_coro(self, job_name, coro_func, *args, **kwargs):
        self._q.put((coro_func, args, kwargs, job_name))

    def wait_join(self):
        self._q.put("halt")
        while self.is_alive():
            self.join(0.1)

    def run(self):
        self._exit_set = False
        self.loop.run(self.process_queue())

    def is_completed(self, job_name):
        return job_name in self._completed

    def result(self, job_name):
        if job_name in self._completed:
            return self._completed[job_name]
        return None

    async def process_queue(self):
        while True:
            if self._q.empty():
                if self._tasks:
                    done, pending = await asyncio.wait(self._tasks, timeout=0.1, return_when=asyncio.FIRST_COMPLETED)
                    if done:
                        logging.getLogger(__name__).debug("{} tasks completed".format(done))
                    for task in done:
                        try:
                            self._completed[str(task.name)] = task.result()
                        except SystemExit as ex:
                            raise ex
                        except KeyboardInterrupt as ex:
                            raise ex
                        except Exception as ex:
                            logging.getLogger(__name__).exception(ex)
                            self._completed[str(task.name)] = False
                    self._tasks = pending
                else:
                    await asyncio.sleep(1)
            else:
                item = self._q.get()
                if isinstance(item, str) and item == "halt":
                    logging.getLogger(__name__).debug("Halt requested, no more queue items will be processed")
                    break
                else:
                    logging.getLogger(__name__).debug("Queuing new task")
                    tsk = asyncio.create_task(item[0](*item[1], **item[2]))
                    tsk.name = item[3]
                    self._tasks.add(tsk)
        await asyncio.gather(*self._tasks)
        logging.getLogger(__name__).debug("All tasks completed")
        if self._on_complete:
            self._on_complete()


class BatchFileCopy:

    files: FileManager = None

    @injector.construct
    def __init__(self):
        self.t = AsynchronousThread()
        self.t.start()
        self.sem = asyncio.Semaphore(5)

    def queue_copy(self, src, dst, name=None, **kwargs):
        if name is None:
            name = "copy_{}_to_{}".format(src, dst)
        self.t.run_coro(name, self._do_copy, src, dst, **kwargs)
        return name

    def result(self, job_name):
        return self.t.result(job_name)

    def wait_for_job(self, op_name):
        while True:
            if self.t.is_completed(op_name):
                return True
            time.sleep(0.05)

    def wait_for_all(self):
        self.t.wait_join()

    async def _do_copy(self, src, dst, **kwargs):
        src = self.files.get_descriptor(src)
        dst = self.files.get_descriptor(dst)
        async with self.sem:
            await src.copy_async(dst, **kwargs)


