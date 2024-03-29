from types import AsyncGeneratorType

from autoinject import injector
import asyncio
import functools
import atexit
import time


@injector.injectable
class GlobalLoopContext:

    def __init__(self):
        self.loop = None
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError as ex:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        atexit.register(GlobalLoopContext.exit, self)

    def recreate_loop(self):
        if self.loop.is_running():
            raise OSError("Loop is still running, cannot recreate")
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def run(self, coro):
        if self.loop.is_running():
            t = self.loop.create_task(coro)
            while not t.done():
                time.sleep(0.1)
            return t.result()
        else:
            if isinstance(coro, AsyncGeneratorType):
                return self.loop.run_until_complete(self.generate(coro))
            return self.loop.run_until_complete(coro)

    def create_task(self, coro):
        return self.loop.create_task(coro)

    async def execute(self, cb, *args, **kwargs):
        return await self.loop.run_in_executor(None, functools.partial(cb, *args, **kwargs))

    async def generate(self, generator: AsyncGeneratorType):
        values = []
        async for x in generator:
            values.append(x)
        return values

    def exit(self):
        if self.loop.is_running:
            self.loop.stop()

    def __del__(self):
        self.exit()
