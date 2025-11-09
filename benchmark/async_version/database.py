import os
import re
import asyncio
import aiosqlite
import orjson
import pickle
from collections import defaultdict

type_map = {
    str:'TEXT',
    int:'INTEGER',
    float:'REAL'
}
value_map = {
    str:None,
    int:None,
    float:None
}
if 'np' in globals():
    try:
        _np = globals()['np']
        type_map[_np.integer]   = 'INTEGER'
        type_map[_np.int8]      = 'INTEGER'
        type_map[_np.int16]     = 'INTEGER'
        type_map[_np.int32]     = 'INTEGER'
        type_map[_np.int64]     = 'INTEGER'
        type_map[_np.uint8]     = 'INTEGER'
        type_map[_np.uint16]    = 'INTEGER'
        type_map[_np.uint32]    = 'INTEGER'
        type_map[_np.uint64]    = 'INTEGER'
        type_map[_np.floating]  = 'REAL'
        type_map[_np.float16]   = 'REAL'
        type_map[_np.float32]   = 'REAL'
        type_map[_np.float64]   = 'REAL'
        value_map[_np.integer]  = int
        value_map[_np.int8]     = int
        value_map[_np.int16]    = int
        value_map[_np.int32]    = int
        value_map[_np.int64]    = int
        value_map[_np.uint8]    = int
        value_map[_np.uint16]   = int
        value_map[_np.uint32]   = int
        value_map[_np.uint64]   = int
        value_map[_np.floating] = float
        value_map[_np.float16]  = float
        value_map[_np.float32]  = float
        value_map[_np.float64]  = float
    except:
        pass


def encode(obj):
    if type(obj) == None:
        return None
    if isinstance(obj, bytes):
        return b'B' + obj
    try:
        return b'J' + orjson.dumps(obj,option=orjson.OPT_SERIALIZE_NUMPY)
    except:
        return b'P' + pickle.dumps(obj)

def decode(binary):
    if type(binary) != bytes:
        return binary
    if binary[0] == ord('J'):
        return orjson.loads(binary[1:])
    if binary[0] == ord('P'):
        return pickle.loads(binary[1:])
    return binary[1:]

identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*[A-Za-z0-9]$")

def valid_identifier(name: str) -> bool:
    return bool(name and identifier_re.fullmatch(name))
def sanitize_table_name(name: str):
    if not valid_identifier(name):
        raise ValueError(f"Invalid identifier: {name}")
    return name

def split(s, sep=',', L="{[(\"'", R="}])\"'"):
    stack = []
    temp = ''
    esc = False
    for c in s:
        if c == '\\':
            esc = True
            temp += c
            continue
        if not esc and c in R and stack:
            if c == R[L.index(stack[-1])]:
                stack.pop()
        elif not esc and c in L:
            stack.append(c)
        elif c == sep and not stack:
            if temp:
                yield temp
            temp = ''
            continue
        temp += c
        esc = False
    if temp:
        yield temp

def parse_where(where_str):
    """Parse limited where expressions into (sql, params).
       This function is intentionally conservative: reject suspicious chars
       and require simple pattern: col op value [and|or ...]
    """
    if not where_str:
        return '', []
    s = where_str.strip()
    # reject common SQL injection characters / comments
    if ';' in s or '--' in s or '/*' in s or '*/' in s:
        return {'suc': False, 'msg': 'Invalid where clause: contains forbidden characters'}, []
    # basic tokenization by spaces while respecting quotes
    try:
        tokens = list(split(s, ' ', "\"'"))
        # expect pattern: col op value [and|or col op value ...]
        if len(tokens) < 3 or (len(tokens) - 1) % 4 not in (0,):
            # attempt fallback: allow "order by" presence handled below
            pass
        # detect 'order by' part
        lower = s.lower()
        if ' order by ' in lower:
            where_part, order_part = s.rsplit('order by', 1)
        else:
            where_part, order_part = s, ''
        # simple parser: split by and/or
        # we will only allow operators in this set:
        allowed_ops = {'=', '==', '!=', '<', '>', '<=', '>=', 'like', 'ilike', 'is'}
        parts = re.split(r'\s+(and|or)\s+', where_part, flags=re.IGNORECASE)
        sql_parts = []
        params = []
        # parts example: [cond1, connector, cond2, ...]
        i = 0
        while i < len(parts):
            cond = parts[i].strip()
            connector = ''
            if i + 1 < len(parts):
                connector = parts[i + 1]
            # parse cond -> col op val
            m = re.match(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=|==|!=|<=|>=|<|>|like|ilike|is)\s*(.+)$', cond, flags=re.I)
            if not m:
                return {'suc': False, 'msg': f"Invalid condition: {cond}"}, []
            col, op, val = m.group(1), m.group(2).lower(), m.group(3).strip()
            if op not in allowed_ops:
                return {'suc': False, 'msg': f"Operator not allowed: {op}"}, []
            if not valid_identifier(col):
                return {'suc': False, 'msg': f"Invalid column name: {col}"}, []
            # handle null
            if val.lower() == 'null' and op == 'is':
                sql_parts.append(f"{col} is null")
            else:
                # strip quotes if present
                if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                    raw = val[1:-1]
                else:
                    raw = val
                sql_parts.append(f"{col} {op} ?")
                params.append(raw)
            if connector:
                sql_parts.append(connector.lower())
            i += 2
        sql = "where " + " ".join(sql_parts)
        # order by parsing (very minimal)
        order_clause = ''
        if order_part:
            cols = []
            for part in order_part.split(','):
                y = part.strip().split()
                if not y:
                    continue
                colname = y[0]
                if not valid_identifier(colname):
                    return {'suc': False, 'msg': f"Invalid order column: {colname}"}, []
                if len(y) == 1:
                    cols.append(colname)
                elif len(y) == 2 and y[1].lower() in ('asc', 'desc'):
                    cols.append(f"{colname} {y[1].lower()}")
                else:
                    return {'suc': False, 'msg': f"Invalid order clause: {part}"}, []
            if cols:
                order_clause = " order by " + ", ".join(cols)
                sql += order_clause
        return sql, params
    except Exception as e:
        return {'suc': False, 'msg': f"parse error: {e}"}, []

# ---------- DB class ----------
class DB:
    def __init__(self, path_db):
        self.path_db = path_db
        self.conn = None
        # use per-table locks to reduce contention
        self._locks = defaultdict(asyncio.Lock)
        self._global_lock = asyncio.Lock()

    async def connect(self):
        os.makedirs(os.path.dirname(self.path_db) or '.', exist_ok=True)
        self.conn = await aiosqlite.connect(self.path_db)
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.commit()
        self.conn.row_factory = aiosqlite.Row

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def ensure_table_and_fields(self, table: str, data: dict, pkey='key'):
        # 校验标识符
        for name in [table, pkey] + list(data.keys()):
            if not name.isidentifier():
                return False, f"Illegal identifier: {name}"

        # 查看现有表头
        async with self.conn.execute(f"PRAGMA table_info({table});") as cursor:
            rows = await cursor.fetchall()

        is_pkey_exists = any(row[5] for row in rows)
        existing_fields = {row[1] for row in rows}

        # 新增字段
        add_fields = {
            k: type_map.get(type(v), 'BLOB')
            for k, v in data.items()
            if k not in existing_fields
        }

        if pkey in add_fields:
            add_fields[pkey] += ' PRIMARY KEY'
        elif not is_pkey_exists:
            add_fields = {pkey: 'INTEGER PRIMARY KEY AUTOINCREMENT', **add_fields}

        # 构建 SQL
        if not rows:
            sql_fields = ','.join(f'{k} {v}' for k, v in add_fields.items())
            sql = f"CREATE TABLE {table} ({sql_fields});"
        else:
            sql = '\n'.join(f"ALTER TABLE {table} ADD COLUMN {k} {v};" for k, v in add_fields.items())

        # 执行 SQL
        try:
            await self.conn.executescript(sql)
            await self.conn.commit()
            return True,'ok'
        except Exception as e:
            return False,f"Ensuring fields error: {e}({sql})"

    async def upsert(self, table, data, key='key'):
        if not isinstance(data, dict):
            return {"suc": False, "msg": "data must be a dict"}
        if key not in data:
            return {"suc": False, "msg": f"Missing key field: '{key}'"}
        try:
            table = sanitize_table_name(table)
        except ValueError as e:
            return {"suc": False, "msg": str(e)}
        lock = self._locks[table]
        async with lock:
            ok,msg = await self.ensure_table_and_fields(table, data, key)
            if not ok:
                return {"suc": False, "msg": msg}
            keys = []
            pins = []
            values = []
            for k,v in data.items():
                keys.append(k)
                pins.append('?')
                L=value_map.get(type(v),encode)
                values.append(L(v) if L else v)
            updates = ", ".join([f"{k}=excluded.{k}" for k in keys])
            keys = ','.join(keys)
            pins = ','.join(pins)
            
            sql = f"""
            INSERT INTO {table} ({keys})
            VALUES ({pins})
            ON CONFLICT({key}) DO UPDATE SET {updates}
            RETURNING *;
            """
            try:
                async with self.conn.execute(sql, values) as cursor:
                    row = await cursor.fetchone()
                await self.conn.commit()
                if row:
                    return {'suc': True, 'data': {k: decode(row[k]) for k in row.keys()}}
                return {'suc': True, 'data': {}}
            except aiosqlite.Error as e:
                return {'suc': False, 'msg': str(e), 'debug': sql}

    async def query(self, table, where='', limit=0, offset=0):
        try:
            table = sanitize_table_name(table)
        except ValueError as e:
            return {'suc': False, 'msg': str(e)}
        parsed = parse_where(where)
        if isinstance(parsed, tuple):
            sql, param = parsed
        else:
            sql, param = parsed
        if isinstance(sql, dict) and sql.get('suc') is False:
            return sql
        if limit > 0:
            sql += f" LIMIT {int(limit)}"
            if offset > 0:
                sql += f" OFFSET {int(offset)}"
        try:
            async with self.conn.execute(f"SELECT * from {table} {sql};", param) as cursor:
                rows = await cursor.fetchall()
                return {'suc': True, 'data': [{k: decode(row[k]) for k in row.keys()} for row in rows]}
        except aiosqlite.Error as e:
            return {'suc': False, 'msg': str(e), 'debug': sql}
        except Exception as e:
            return {'suc': False, 'msg': str(e)}

    async def count(self, table, where=''):
        try:
            table = sanitize_table_name(table)
        except ValueError as e:
            return {'suc': False, 'msg': str(e)}
        sql, param = parse_where(where)
        if isinstance(sql, dict) and sql.get('suc') is False:
            return sql
        async with self.conn.execute(f"SELECT count(*) from {table} {sql};", param) as cursor:
            return (await cursor.fetchone())[0]

    async def list(self, table, where='', total=None):
        limit = min(total, 10) if total else 10
        if not total:
            total = await self.count(table, where)
            if isinstance(total, dict) and total.get('suc') is False:
                return
        max_page, rest = divmod(total, limit)
        if rest != 0:
            max_page += 1
        for i in range(max_page):
            ret = await self.query(table, where, limit=limit, offset=i * limit)
            if not ret['suc']:
                break
            for row in ret['data']:
                yield row

    async def delete(self, table, where):
        try:
            table = sanitize_table_name(table)
        except ValueError as e:
            return {'suc': False, 'msg': str(e)}
        sql, param = parse_where(where)
        if isinstance(sql, dict) and sql.get('suc') is False:
            return sql
        try:
            async with self.conn.execute(f"DELETE FROM {table} {sql};", param) as cursor:
                await self.conn.commit()
                return {'suc': True, 'data': cursor.rowcount}
        except aiosqlite.Error as e:
            return {'suc': False, 'msg': str(e), 'debug': sql}

    async def columns(self, table):
        try:
            table = sanitize_table_name(table)
        except ValueError as e:
            return {'suc': False, 'msg': str(e)}
        try:
            async with self.conn.execute(f"PRAGMA table_info({table});") as cursor:
                rows = await cursor.fetchall()
                return {'suc': True, 'data': [row[1] for row in rows]}
        except aiosqlite.Error as e:
            return {'suc': False, 'msg': str(e)}

#if __name__ == '__main__':
#    import asyncio
#    async def main():
#        db = DB(path_db = "your_database.db")
#        await db.connect()
#        r = await db.upsert("users", {"key": 'U0001', "name": "Tom", 'age':12, 'sex':'M', 'hobby':["football", 'basketball'],'meta':{"height": 1.75, "weight": 70}})
#        print(r)
#        r = await db.upsert("users", {"key": 'U0002', "name": "Jerry", 'age':8, 'sex':'M', 'hobby':["football", 'basketball'],'meta':{"height": 1.25, "weight": 30}})
#        print(r)
#        r = await db.count("users", 'meta like %"height":1.25%')
#        print(r)
#        await db.close()
#    asyncio.run(main())