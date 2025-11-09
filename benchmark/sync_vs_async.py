# [terminal-1]
# cd async_version (or sync_version)
# python server.py --secret abcde
# ----------------
# [terminal-2]
# python main.py
import aiohttp
import orjson
import asyncio
import os
import time
path_this = os.path.dirname(os.path.abspath(__file__))

def json_save(path_file,data):
    os.makedirs(os.path.dirname(path_file),exist_ok=True)
    with open(path_file,'wb') as f:
        f.write(orjson.dumps(data))
def json_load(path_file,default=None):
    try:
        with open(path_file,'rb') as f:
            return orjson.loads(f.read())
    except:
        return default

async def db_set(db_table, data):
    payload = orjson.dumps(data)
    for _ in range(5):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://127.0.0.1:12239/db/{db_table}",
                    data=payload,
                    headers={
                        'Content-Type': 'application/json',
                        'Authorization': f"Bearer abcde",
                    }
                ) as response:
                    return orjson.loads(await response.text())
        except Exception as e:
            print(f"db_set failed: {e}")
            await asyncio.sleep(1)
    return {'suc': False, 'msg': f"Beacon down"}

async def db_get(db_table, where, page=1, limit=20):
    for _ in range(5):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://127.0.0.1:12239/db/{db_table}/{where}?page={page}&limit={limit}",
                    headers={
                        'Authorization': f"Bearer abcde",
                    }
                ) as response:
                    return orjson.loads(await response.text())
        except Exception as e:
            print(f"db_get failed: {e}")
            await asyncio.sleep(1)
    return {'suc': False, 'msg': f"Beacon down"}
async def limited(semaphore,coro):
    async with semaphore:
        return await coro
async def test_sqless():
    semaphore = asyncio.Semaphore(60)
    data = json_load(f'{path_this}/test_data.json')
    t0=time.time()
    async for ret in asyncio.as_completed([
        limited(semaphore,db_set('temp-cats',{'key':i,**item}))
        for i,item in enumerate(data['Cats'])
    ]):
        ...
    t1=time.time()
    async for ret in asyncio.as_completed([
        limited(semaphore,db_get('temp-cats',f'key = {i}'))
        for i in range(1000)
    ]):
        ...
    t2=time.time()
    print("sqless")
    print(f"write: {t1-t0}")
    print(f"read:  {t2-t1}")
    #     async_db limit=20
    # write: 2.822279214859009
    # read:  0.9873285293579102
    #     async_db limit=60
    # write: 2.438852310180664
    # read:  0.9670825004577637
    #     sync_db limit=20
    # write: 2.2639663219451904
    # read:  0.8546068668365479
    #     sync_db limit=60
    # write: 2.0788049697875977
    # read:  0.9295873641967773
if __name__=='__main__':
    asyncio.run(test_sqless())