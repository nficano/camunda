import asyncio
from pyzeebe import ZeebeWorker, create_insecure_channel
from workers import router

async def main():
    channel = create_insecure_channel()
    worker = ZeebeWorker(channel, 'order-process')
    worker.include_router(router)
    await worker.work()

if __name__ == "__main__":
    asyncio.run(main())