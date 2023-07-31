import asyncio

from fissure_engine.FissureEngine import FissureEngine


async def main():
    fissure_engine = FissureEngine()

    await fissure_engine.build_fissure_list()
    next_reset = fissure_engine.get_reset_string(fissure_engine.FISSURE_TYPE_NORMAL, fissure_engine.DISPLAY_TYPE_DISCORD)

    print(next_reset)


if __name__ == "__main__":
    asyncio.run(main())