import os
import time
import base64
import asyncio
from aiohttp import web, ClientSession, FormData, ClientTimeout
import orjson
import aiofiles
from .database import DB
import re
path_src = os.path.dirname(os.path.abspath(__file__))
# ---------- Configuration (use env vars in production) ----------
DEFAULT_SECRET = os.environ.get("SQLESS_SECRET", None)

num2time = lambda t=None, f="%Y%m%d-%H%M%S": time.strftime(f, time.localtime(int(t if t else time.time())))
tspToday = lambda: int(time.time() // 86400 * 86400 - 8 * 3600)  # UTC+8 today midnight

identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*[A-Za-z0-9]$")

def valid_identifier(name: str) -> bool:
    return bool(name and identifier_re.fullmatch(name))

def sanitize_table_name(name: str):
    if not valid_identifier(name):
        raise ValueError(f"Invalid identifier: {name}")
    return name

def check_path(path_file, path_base):
    normalized_path = os.path.realpath(path_file)
    try:
        if os.path.commonpath([path_base, normalized_path]) == path_base:
            return True, normalized_path
    except Exception as e:
        pass
    return False, f"unsafe path: {normalized_path}"



async def run_server(
    host='0.0.0.0',
    port=27018,
    secret=DEFAULT_SECRET,
    path_this = os.getcwd(),
    max_filesize = 200, # MB
):
    path_base_db = os.path.realpath(f"{path_this}/db")
    path_base_fs = os.path.realpath(f"{path_this}/fs")
    if not secret:
        print("[ERROR] Please set SQLESS_SECRET environment variable or pass --secret <secret>")
        return
    
    dbs = {}
    async def get_db(db_key='default'):
        if db_key not in dbs:
            suc, path_db = check_path(f"{path_this}/db/{db_key}.sqlite", path_base_db)
            if not suc:
                return False, path_db
            db = DB(path_db)
            await db.connect()
            dbs[db_key] = db
        return dbs[db_key]

    async def auth_middleware(app, handler):
        async def middleware_handler(request):
            try:
                request['client_ip'] = request.headers.get('X-Real-IP',request.transport.get_extra_info('peername')[0])
            except:
                request['client_ip'] = 'unknown'
            route = request.match_info.route
            if route and getattr(route, "handler", None) == handle_static:
                return await handler(request)
            auth = request.headers.get('Authorization', '')
            if not auth.startswith("Bearer "):
                return web.Response(status=401, text='Unauthorized')
            token = auth.split(" ", 1)[1].strip()
            if token != secret:
                return web.Response(status=401, text='Unauthorized')
            return await handler(request)
        return middleware_handler

    async def handle_post_db(request):
        db_table = request.match_info['db_table']
        if request.content_type == 'application/json':
            data = await request.json()
        else:
            post = await request.post()
            data = dict(post)
        db_key, table = os.path.split(db_table.replace('-', '/'))
        db_key = db_key or 'default'
        try:
            table = sanitize_table_name(table)
        except ValueError:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid table name'}), content_type='application/json')
        db = await get_db(db_key)
        if isinstance(db, tuple) and db[0] is False:
            return web.Response(body=orjson.dumps({'suc': False, 'data': db[1]}), content_type='application/json')
        print(f"[{num2time()}]{request['client_ip']}|POST {db_key}|{table}|{data}")
        if not isinstance(data, dict):
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid data type'}), content_type='application/json')
        ret = await db.upsert(table, data, 'key')
        return web.Response(body=orjson.dumps(ret), content_type='application/json')

    async def handle_delete_db(request):
        db_table = request.match_info['db_table']
        db_key, table = os.path.split(db_table.replace('-', '/'))
        db_key = db_key or 'default'
        try:
            table = sanitize_table_name(table)
        except ValueError:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid table name'}), content_type='application/json')
        db = await get_db(db_key)
        where = request.match_info['where']
        print(f"[{num2time()}]{request['client_ip']}|DELETE {db_key}|{table}|{where}")
        ret = await db.delete(table, where)
        return web.Response(body=orjson.dumps(ret), content_type='application/json')

    async def handle_get_db(request):
        db_table = request.match_info['db_table']
        db_key, table = os.path.split(db_table.replace('-', '/'))
        db_key = db_key or 'default'
        try:
            table = sanitize_table_name(table)
        except ValueError:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'invalid table name'}), content_type='application/json')
        db = await get_db(db_key)
        where = request.match_info['where']
        page = max(int(request.query.get('page', 1)), 1)
        limit = min(max(int(request.query.get('per_page', 20)), 0), 100)
        offset = (page - 1) * limit
        print(f"[{num2time()}]{request['client_ip']}|GET {db_key}|{table}|{where}?page={page}&per_page={limit}")
        ret = await db.query(table, where, limit, offset)
        if isinstance(ret, dict) and ret.get('suc') and limit > 1 and not offset:
            cnt = await db.count(table, where)
            ret['count'] = cnt
            ret['max_page'], rest = divmod(ret['count'], limit)
            if rest:
                ret['max_page'] += 1
        return web.Response(body=orjson.dumps(ret), content_type='application/json')

    async def handle_get_fs(request):
        suc, path_file = check_path(f"fs/{request.match_info['path_file']}", path_base_fs)
        if suc and os.path.isfile(path_file):
            if request.query.get('check') is not None:
                print(f"[{num2time()}]{request['client_ip']}|CHECK {path_file}")
                return web.Response(body=orjson.dumps({'suc': True}), content_type='application/json')
            else:
                print(f"[{num2time()}]{request['client_ip']}|DOWNLOAD {path_file}")
                return web.FileResponse(path_file)
        else:
            if request.query.get('check') is not None:
                return web.Response(body=orjson.dumps({'suc': False}), content_type='application/json')
            else:
                return web.Response(status=404, text='File not found')

    async def handle_post_fs(request):
        suc, path_file = check_path(f"fs/{request.match_info['path_file']}", path_base_fs)
        print(f"[{num2time()}]{request['client_ip']}|UPLOAD attempt {suc} {path_file}")
        if not suc:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'Unsafe path'}), content_type='application/json')
        folder = os.path.dirname(path_file)
        if not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        reader = await request.multipart()
        field = await reader.next()
        if not field:
            return web.Response(body=orjson.dumps({'suc': False, 'data': 'No file uploaded'}), content_type='application/json')
        # write file safely
        try:
            async with aiofiles.open(path_file, 'wb') as f:
                while True:
                    chunk = await field.read_chunk()
                    if not chunk:
                        break
                    await f.write(chunk)
            # ensure uploaded file isn't executable
            try:
                os.chmod(path_file, 0o644)
            except Exception:
                pass
            return web.Response(body=orjson.dumps({'suc': True, 'data': 'File Saved'}), content_type='application/json')
        except Exception as e:
            return web.Response(body=orjson.dumps({'suc': False, 'data': str(e)}), content_type='application/json')

    async def handle_static(request):
        file = request.match_info.get('file') or 'index.html'
        return web.FileResponse(f"{path_this}/www/{file}")

    async def handle_xmlhttpRequest(request):
        try:
            data = await request.json()
            method = data.get("method", "POST").upper()
            url = data.get("url")
            if not url:
                return web.Response(body=orjson.dumps({"suc": False, "text": "no url"}), content_type='application/json')
            headers = data.get("headers", {})
            payload = None
            if data.get('type') == 'form':
                payload = FormData()
                for k, v in data.get("data", {}).items():
                    payload.add_field(k, v)
                for f in data.get("files", []):
                    content = base64.b64decode(f["base64"])
                    payload.add_field(
                        name=f["field"],
                        value=content,
                        filename=f["filename"],
                        content_type=f["content_type"]
                    )
            else:
                payload = data.get('data')
            # enclose outgoing request with timeout
            timeout = ClientTimeout(total=15)
            async with ClientSession(timeout=timeout) as session:
                async with session.request(method, url, headers=headers, data=payload, allow_redirects=True) as resp:
                    text = await resp.text()
                    return web.Response(body=orjson.dumps({
                        "suc": True,
                        "status": resp.status,
                        "text": text,
                        "url": str(resp.url)
                    }), content_type='application/json')
        except Exception as e:
            return web.Response(body=orjson.dumps({"suc": False, "text": str(e)}), content_type='application/json')

    app = web.Application(middlewares=[auth_middleware], client_max_size=max_filesize * 1024 ** 2)
    app.router.add_post('/db/{db_table}', handle_post_db)
    app.router.add_get('/db/{db_table}/{where:.*}', handle_get_db)
    app.router.add_delete('/db/{db_table}/{where:.*}', handle_delete_db)
    app.router.add_get('/fs/{path_file:.*}', handle_get_fs)
    app.router.add_post('/fs/{path_file:.*}', handle_post_fs)
    app.router.add_post('/xmlhttpRequest', handle_xmlhttpRequest)
    app.router.add_get('/{file:.*}', handle_static)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"Serving on http://{'127.0.0.1' if host == '0.0.0.0' else host}:{port}")
    print(f"Serving at {path_this}")
    if not os.path.exists(f"{path_this}/www"):
        os.makedirs(f"{path_this}/www")
    if not os.path.exists(f"{path_this}/www/openapi.yaml"):
        with open(f"{path_src}/openapi.yaml",'r',encoding='utf-8') as f:
            txt = f.read()
        with open(f"{path_this}/www/openapi.yaml",'w',encoding='utf-8') as f:
            f.write(txt.replace('127.0.0.1:12239',f"{'127.0.0.1' if host == '0.0.0.0' else host}:{port}"))
    if not os.path.exists(f"{path_this}/www/index.html"):
        with open(f"{path_src}/docs.html",'r',encoding='utf-8') as f:
            txt = f.read()
        with open(f"{path_this}/www/index.html",'w',encoding='utf-8') as f:
            f.write(txt)
    stop_event = asyncio.Event()
    try:
        # simplified loop, exit on Cancelled/Error
        while not stop_event.is_set():
            await asyncio.sleep(86400)
    except asyncio.CancelledError:
        pass
    finally:
        print("Cleaning up...")
        await runner.cleanup()
        for db_key in list(dbs.keys()):
            await dbs[db_key].close()
            del dbs[db_key]

def main():
    import argparse
    import asyncio
    
    parser = argparse.ArgumentParser(description='Run the sqless server')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=12239, help='Port to bind to (default: 12239)')
    parser.add_argument('--secret', default=DEFAULT_SECRET, help='Secret for authentication')
    parser.add_argument('--path', default=os.getcwd(), help=f'Base path for database and file storage (default: {os.getcwd()})')
    parser.add_argument('--fsize', type=int, default=200, help='Max file size (in MB) allowed in POST /fs')
    args = parser.parse_args()
    
    asyncio.run(run_server(
        host=args.host,
        port=args.port,
        secret=args.secret,
        path_this=args.path,
        max_filesize=args.fsize
    ))

if __name__ == "__main__":
    main()