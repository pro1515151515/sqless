import os
import re
import sqlite3
import orjson
import pickle

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




def parse_where(where_str):
    """
    Parse safe WHERE expressions into (sql, params) with AND/OR/NOT, parentheses, and ORDER BY.

    -------------------------------------------------------------------------
    Syntax Guide for Developers:

    1. Basic condition:
       col operator value
       - col: column name (must match [A-Za-z_][A-Za-z0-9_]*)
       - operator: one of =, ==, !=, <, >, <=, >=, like, ilike, is
       - value: string (quoted with single ' or double ") or numeric literal
         - Example: age >= 18
         - Example: name = "Alice Bob"
         - Example: role is null

    2. Logical operators:
       - AND, OR, NOT (case-insensitive)
       - NOT applies to the condition immediately following it
       - Examples:
         - age >= 18 AND role = "Hero"
         - NOT age < 10

    3. Parentheses:
       - Use () to group expressions and control precedence
       - Examples:
         - (age < 10 AND name like "%e%") OR (role = "Antagonist" AND NOT age >= 16)

    4. ORDER BY clause (optional):
       - Use at the end of the expression: ORDER BY col1 [ASC|DESC], col2 [ASC|DESC], ...
       - Column names must be valid identifiers
       - ASC/DESC is optional; default ordering depends on DB
       - Example:
         - ORDER BY id DESC, name ASC

    5. Safety rules:
       - Forbidden characters: ; -- /* */
       - Only valid identifiers allowed as column names
       - String literals must be quoted
       - Function calls or subqueries are NOT allowed

    6. Return:
       - (True, sql_string, params_list) on success
       - (False, error_message, []) on parse error

    -------------------------------------------------------------------------
    Example usage:

    expr = '(age < 10 AND name like "%e%") OR (role = "Antagonist" AND NOT age >= 16) ORDER BY id DESC, name ASC'
    ok, sql, params = parse_where(expr)
    print(ok)     # True
    print(sql)    # where ( age < ? and name like ? ) or ( role = ? and not age >= ? ) order by id desc, name asc
    print(params) # ['10', '%e%', 'Antagonist', '16']
    -------------------------------------------------------------------------
    """
    allowed_ops = {'=', '==', '!=', '<', '>', '<=', '>=', 'like', 'ilike', 'is'}

    def valid_identifier(s):
        return re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', s) is not None

    if not where_str:
        return True, '', []

    s = where_str.strip()

    # reject dangerous chars
    if any(x in s for x in (';', '--', '/*', '*/')):
        return False, 'contains forbidden characters', []

    # separate ORDER BY if present
    m_order = re.search(r'\border\s+by\b', s, re.IGNORECASE)
    if m_order:
        where_part = s[:m_order.start()].strip()
        order_part = s[m_order.end():].strip()
    else:
        where_part = s
        order_part = ''

    # tokenize where_part
    token_pattern = r"""
        (\() |               # open parenthesis
        (\)) |               # close parenthesis
        ("[^"]*") |          # double-quoted string
        ('[^']*') |          # single-quoted string
        (\bAND\b|\bOR\b|\bNOT\b) |   # logical operators
        (<=|>=|!=|==|=|<|>|like|ilike|is) | # comparison operators
        ([^\s()]+)           # identifiers / values
    """
    tokens = [t for t in re.findall(token_pattern, where_part, re.IGNORECASE | re.VERBOSE)]
    tokens = [next(filter(None, t)) for t in tokens]  # flatten

    sql_parts = []
    params = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]

        # parentheses
        if tok in ('(', ')'):
            sql_parts.append(tok)
            i += 1
            continue

        # logical operators
        if tok.upper() in ('AND', 'OR', 'NOT'):
            sql_parts.append(tok.lower())
            i += 1
            continue

        # expect: col op val
        if i + 2 >= len(tokens):
            return False, f"Invalid condition near: {tok}", []

        col, op, val = tokens[i], tokens[i+1].lower(), tokens[i+2]

        if not valid_identifier(col):
            return False, f"Invalid column name: {col}", []

        if op not in allowed_ops:
            return False, f"Operator not allowed: {op}", []

        # handle NULL
        if val.lower() == 'null' and op == 'is':
            sql_parts.append(f"{col} is null")
        else:
            # strip quotes
            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val_raw = val[1:-1]
            else:
                val_raw = val
            sql_parts.append(f"{col} {op} ?")
            params.append(val_raw)

        i += 3

    sql = "where " + " ".join(sql_parts)

    # handle ORDER BY (simple)
    if order_part:
        order_cols = []
        for part in order_part.split(','):
            items = part.strip().split()
            if not items:
                continue
            colname = items[0]
            if not valid_identifier(colname):
                return False, f"Invalid order column: {colname}", []
            direction = ''
            if len(items) == 2 and items[1].lower() in ('asc', 'desc'):
                direction = f" {items[1].lower()}"
            elif len(items) > 2:
                return False, f"Invalid order clause: {part}", []
            order_cols.append(f"{colname}{direction}")
        if order_cols:
            sql += " order by " + ", ".join(order_cols)

    return True, sql, params


class Table:
    def __init__(self,db,name,pkey):
        self.db = db
        self.name = name
        self.pkey = pkey
    def __str__(self):
        return f"Table({self.name},pkey={self.pkey}){self.db.inspect(self.name)}"
    def __dir__(self):
        return list(self.db.inspect(self.name).keys())
    def __iter__(self):
        return self.db.find(self.name)
    def iter_keys(self):
        for x in self.db.find(self.name,'',self.pkey):
            yield x[self.pkey]
    def keys(self):
        return list(self.iter_keys())
    def find(self,where,select='*'):
        return self.db.find(self.name,where,select)
    def query(self,where='',limit=0,offset=0):
        return self.db.query(self.name,where,limit,offset)
    def __len__(self):
        return self.db.count(self.name)
    def __setitem__(self,key,data:dict):
        if self.pkey not in data:
            data = {self.pkey:key,**data}
        return self.db.upsert(self.name,data,self.pkey)
    def __getitem__(self,key):
        return self.db.get_item(self.name,key,self.pkey)
    def __delitem__(self,key):
        return self.db.delete(self.name,f"{self.pkey} like {key}")
    def __del__(self):
        self.db = None

# ---------- DB class ----------
class DB:
    def __init__(self, path_db,wal=True):
        os.makedirs(os.path.dirname(path_db) or '.',exist_ok=True)
        self.conn = sqlite3.connect(path_db)
        self.cursor = self.conn.cursor()
        if wal:
            # Enable WAL (Write-Ahead Logging)
            self.cursor.execute("PRAGMA journal_mode = WAL;")
            self.conn.commit()
        self.tables = {}

    def close(self):
        if self.conn:
            self.conn.close()
    def __str__(self):
        return self.inspect()
    def __dir__(self):
        return list(self.inspect().keys())
    def __getitem__(self,table_name):
        if not valid_identifier(table_name):
            print(f"DB_ERROR| Illegal identifier: {table_name}")
            return None
        if table_name not in self.tables:
            self.tables[table_name] = Table(self,table_name,'key')
        return self.tables[table_name]
    def __delitem__(self,table_name):
        if not valid_identifier(table_name):
            print(f"DB_ERROR| Illegal identifier: {table_name}")
            return False
        sql = f"DROP TABLE IF EXISTS {table_name};"
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            if table_name in self.tables:
                del self.tables[table_name]
        except Exception as e:
            print(f"DB_ERROR|{e}({sql})")
            return False
    def __contains__(self,table_name):
        if not valid_identifier(table_name):
            print(f"DB_ERROR| Illegal identifier: {table_name}")
            return False
        sql = f"SELECT count(*) FROM sqlite_master WHERE type = 'table' and name = ?"
        values = (table_name,)
        try:
            self.cursor.execute(sql,values)
            return bool(self.cursor.fetchone()[0])
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return False
    
    def ensure_table_and_fields(self, table: str, data: dict, pkey='key'):
        # 校验标识符
        for name in [pkey] + list(data.keys()):
            if not name.isidentifier():
                return False, f"Illegal identifier: {name}"

        # 查看现有表头
        self.cursor.execute(f"PRAGMA table_info({table});")
        rows = self.cursor.fetchall()

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
            self.cursor.executescript(sql)
            self.conn.commit()
            return True,'ok'
        except Exception as e:
            return False,f"Ensuring fields error: {e}({sql})"
    
    def set_index(self,table:str,field:str):
        if not valid_identifier(table):
            print(f"DB_ERROR| Illegal identifier: {field}")
            return False
        sql = f'CREATE INDEX idx_{table}_{field} ON {table}({field})';
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"DB_ERROR|{e}({sql})")
            return False
    
    def upsert(self,table:str,data:dict,pkey='key'):
        if not isinstance(data, dict):
            return {"suc": False, "msg": "data must be a dict"}
        if pkey not in data:
            return {"suc": False, "msg": f"Missing primary key field: '{pkey}'"}
        if not valid_identifier(table):
            return {"suc": False, "msg": f"Invalid table name"}
        keys = []
        pins = []
        values = []
        for k,v in data.items():
            keys.append(k)
            pins.append('?')
            L=value_map.get(type(v),encode)
            values.append(L(v) if L else v)
        keys = ','.join(keys)
        pins = ','.join(pins)
        updates = ", ".join([f"{k}=excluded.{k}" for k in data.keys()])
        sql = f"""INSERT INTO {table} ({keys}) VALUES ({pins})
                  ON CONFLICT({pkey}) DO UPDATE SET {updates};
              """
        try:
            self.cursor.execute(sql,values)
            self.conn.commit()
            return {'suc': True}
        except Exception as e:
            try:
                suc,msg = self.ensure_table_and_fields(table,data,pkey)
                if not suc:
                    return {'suc':False,'msg':msg}
                self.cursor.execute(sql,values)
                self.conn.commit()
                return {'suc': True}
            except Exception as e:
                print(f"DB_ERROR|{e}({sql}){values}")
                return {'suc': False, 'msg': str(e), 'debug': sql}
    
    def upsert_mat(self,table:str,headers:list,mat:list,pkey='key'):
        if not mat or not type(mat[0]) is list:
            return {"suc": False, "msg": f"mat must be list[list]"}
        if pkey not in headers:
            return {"suc": False, "msg": f"Missing primary key field: '{pkey}'"}
        if not valid_identifier(table):
            return {"suc": False, "msg": f"Invalid table name"}
        Ls = [value_map.get(type(v),encode) for v in mat[0]]
        values_mat = [arr.copy() for arr in mat]
        for j,L in enumerate(Ls):
            if L:
                for row in values_mat:
                    row[j]=L(row[j])
        keys = ','.join(headers)
        pins = ','.join('?' for _ in headers)
        updates = ", ".join([f"{k}=excluded.{k}" for k in headers])
        sql = f"""INSERT INTO {table} ({keys}) VALUES ({pins})
                  ON CONFLICT({pkey}) DO UPDATE SET {updates};
              """
        try:
            self.cursor.executemany(sql,values_mat)
            self.conn.commit()
            return {'suc': True}
        except Exception as e:
            try:
                suc,msg = self.ensure_table_and_fields(table,{k:v for k,v in zip(headers,mat[0])},pkey)
                if not suc:
                    return {'suc':False,'msg':msg}
                self.cursor.executemany(sql,values_mat)
                self.conn.commit()
                return {'suc': True}
            except Exception as e:
                print(f"DB_ERROR|{e}({sql}){values_mat}")
                return {'suc': False, 'msg': str(e), 'debug': sql}
    def get_item(self,table,key,pkey='key'):
        if not valid_identifier(table):
            print(f"DB_ERROR| Illegal identifier: {table}")
            return None
        try:
            self.cursor.execute(f"SELECT * FROM {table} WHERE {pkey} = ?;",(key,))
            columns = [description[0] for description in self.cursor.description]
            row = self.cursor.fetchone()
            return {k:decode(v) for k,v in zip(columns,row)}
        except:
            return {}
    def find(self,table,where='',select='*'):
        if not valid_identifier(table):
            print(f"DB_ERROR| Illegal identifier: {table}")
            return None
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return None
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT {select} FROM {table} {sql_where};",values)
        columns = [description[0] for description in cursor.description]
        row = cursor.fetchone()
        while row:
            yield {k:decode(v) for k,v in zip(columns,row)}
            row = cursor.fetchone()
    def query(self,table,where='',limit=0,offset=0):
        if not valid_identifier(table):
            print(f"DB_ERROR| Illegal identifier: {table}")
            return {'suc':False, 'msg': f"Invalid table name"}
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return {'suc':False, 'msg': f"Invalid where: {sql_where}"}
        if limit > 0:
            sql_where += f" LIMIT {limit}"
            if offset > 0:
                sql_where += f" OFFSET {offset}"
        sql = f"SELECT * from {table} {sql_where};"
        try:
            self.cursor.execute(sql, values)
            columns = [description[0] for description in self.cursor.description]
            rows = self.cursor.fetchall()
            return {'suc':True,'data':[{k:decode(v) for k,v in zip(columns,row)} for row in rows]}
        except Exception as e:
            return {'suc':False, 'msg':str(e), 'debug':sql,'values':values}

    def inspect(self,table=None):
        if table: # CASE 1: {field_name: field_type}
            self.cursor.execute(f"PRAGMA table_info({table});")
            return {row[1]:row[2] for row in self.cursor.fetchall()}
        # CASE 2: {table_name: primary_key}
        self.cursor.execute("""
            SELECT 
                m.name,
                (SELECT p.name FROM pragma_table_info(m.name) p WHERE p.pk = 1)
            FROM sqlite_master m
            WHERE m.type = 'table';
        """)
        return {name:pkey for name,pkey in self.cursor.fetchall() if name!='sqlite_sequence'}
    
    def count(self,table,where=''):
        if not valid_identifier(table):
            print(f"DB_ERROR| Illegal identifier: {table}")
            return 0
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return 0
        sql = f"SELECT count(*) from {table} {sql_where};"
        try:
            self.cursor.execute(sql,values)
            total = self.cursor.fetchone()[0]
            return total
        except:
            return 0
    def delete(self,table,where=''):
        if not valid_identifier(table):
            print(f"DB_ERROR| Illegal identifier: {table}")
            return {'suc':False,'msg':f"DB_ERROR|Illegal identifier: {table}"}
        suc, sql_where, values = parse_where(where)
        if not suc:
            print(f"DB_ERROR| Illegal where: {sql_where}")
            return {'suc':False,'msg':f"DB_ERROR|Illegal where: {sql_where}"}
        sql = f"DELETE FROM {table} {sql_where};"
        try:
            self.cursor.execute(sql,values)
            self.conn.commit()
            return {'suc':True}
        except Exception as e:
            print(f"DB_ERROR|{e}({sql}){values}")
            return {'suc':False,'msg':f"DB_ERROR|{e}({sql}){values}"}
    
    

#if __name__ == '__main__':
#    db = DB(path_db = "your_database.db")
#    r = db.upsert("users", {"key": 'U0001', "name": "Tom", 'age':12, 'sex':'M', 'hobby':["football", 'basketball'],'meta':{"height": 1.75, "weight": 70}})
#    print(r)
#    r = db.upsert("users", {"key": 'U0002', "name": "Jerry", 'age':8, 'sex':'M', 'hobby':["football", 'basketball'],'meta':{"height": 1.25, "weight": 30}})
#    print(r)
#    r = db.query("users", 'meta like %"height":1.25%')
#    print(r)
#    r = db.query("users", 'age > 9')
#    print(r)