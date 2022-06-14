from types import AsyncGeneratorType

from autoinject import injector
import asyncio
import functools
import atexit


@injector.injectable
class GlobalLoopContext:

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        atexit.register(GlobalLoopContext.exit, self)

    def recreate_loop(self):
        if self.loop.is_running():
            raise OSError("Loop is still running, cannot recreate")
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def run(self, coro):
        if isinstance(coro, AsyncGeneratorType):
            return self.loop.run_until_complete(self.generate(coro))
        return self.loop.run_until_complete(coro)

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
