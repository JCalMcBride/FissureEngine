import asyncio

from fissure_engine.FissureEngine import FissureEngine


async def main():
    fissure_engine = FissureEngine()

    await fissure_engine.build_fissure_list()

    fissures = fissure_engine.get_fissures(FissureEngine.FISSURE_TYPE_NORMAL,
                                           tier=[1, 2, 3],
                                           era=[FissureEngine.ERA_LITH, FissureEngine.ERA_MESO,
                                                FissureEngine.ERA_NEO, FissureEngine.ERA_AXI])

    print(fissures)

    fields = [('Era', '{era}'),
              ('Mission', '{mission} - {node} ({planet})'),
              ('Ends', 'expiry')]
    result = fissure_engine.get_fields(fissures, fields)

    for name, value in result.items():
        print(name)
        for item in value:
            print(item)



if __name__ == "__main__":
    asyncio.run(main())
