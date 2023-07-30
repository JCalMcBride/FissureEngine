import asyncio

from fissure_engine.FissureEngine import FissureEngine


async def main():
    fissure_engine = FissureEngine()

    await fissure_engine.build_fissure_list()
    print(fissure_engine.get_soonest_expiry())
    print(fissure_engine.fissure_lists)


if __name__ == "__main__":
    asyncio.run(main())