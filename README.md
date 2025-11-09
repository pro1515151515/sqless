# sqless

An async HTTP server for SQLite, FileStorage and WebPage.

It is also an ORM for lazy people, similar to [dataset](https://github.com/pudo/dataset).

## Why sqless is special:
- **Schema free**. Auto adjusts SQLite schema to fit JSON inputs.
- **High performance**. Faster than many ORMs, see [performance test](#performance-test).
- **Minimal setup**. Just `pip install sqless` to run the server.
- **Multi-file sharding**. Easily store data across multiple SQLite files.
- **SQL-safe**. Uses semantic parsing, parameter binding, and identifier validation..



## Installation

```bash
pip install sqless
```

## Quick Start

### Running the server

```bash
sqless --host 127.0.0.1 --port 12239 --secret your-secret-key
```

This will create `www` directory in the current directory, which is used for WebPage.
You can access the `www/index.html` at `http://127.0.0.1:12239/index.html`

It will also creates `db` and `fs` directories in the current directory, when saving data by database API and file storage API.

### Using the database API

```python
import requests

# Set up the base URL and authentication
BASE_URL = "http://127.0.0.1:12239"
SECRET = "your-secret-key"
DB_TABLE = "users"

# Insert or update data
r = requests.post(
    f"{BASE_URL}/db/{DB_TABLE}",
    headers={"Authorization": f"Bearer {SECRET}"},
    json={"key": "U001", "name": "Tom", "age": 14}
)

# Query data
r = requests.get(
    f"{BASE_URL}/db/{DB_TABLE}/key = U001",
    headers={"Authorization": f"Bearer {SECRET}"}
)

# Fuzzy query
r = requests.get(
    f"{BASE_URL}/db/{DB_TABLE}/name like %om%?limit=10&page=1",
    headers={"Authorization": f"Bearer {SECRET}"}
)

# Value query
r = requests.get(
    f"{BASE_URL}/db/{DB_TABLE}/age > 10?limit=10&page=1",
    headers={"Authorization": f"Bearer {SECRET}"}
)

# Delete data
r = requests.delete(
    f"{BASE_URL}/db/{DB_TABLE}/key = U001",
    headers={"Authorization": f"Bearer {SECRET}"}
)
```
sqless does not limit you to one database.

You can access **many SQLite databases** by using a separator in the table name `DB_TABLE`.

Example:
```
/db/users              -> db/default.sqlite (table: users)
/db/mall-users         -> db/mall.sqlite    (table: users)
/db/east-mall-users    -> db/east/mall.sqlite (table: users)
```

### Using the FileStorage API
```python
import requests

# Upload a file to ./fs/example.txt
with open("example.txt", "rb") as f:
    r = requests.post(
        f"{BASE_URL}/fs/example.txt",
        headers={"Authorization": f"Bearer {SECRET}"},
        files={"file": f}
    )

# Check if a file exists
r = requests.get(
    f"{BASE_URL}/fs/example.txt?check=1",
    headers={"Authorization": f"Bearer {SECRET}"}
)

# Download a file
r = requests.get(
    f"{BASE_URL}/fs/example.txt",
    headers={"Authorization": f"Bearer {SECRET}"},
    stream=True
)
with open("downloaded_example.txt", "wb") as f:
    for chunk in r.iter_content(chunk_size=8192):
        f.write(chunk)
```

### Using the Proxy API
```python
import requests
import base64

payload = {
    "method": "POST",
    "url": "https://httpbin.org/post",
    "headers": {
        "User-Agent": "SQLESS-Client/1.0",
        "Authorization": "Bearer mytoken"
    },
    "type": "form",
    "data": {"foo": "bar"},
    "files": [
        {
            "field": "file1",
            "filename": "example.txt",
            "content_type": "text/plain",
            "base64": base64.b64encode(open("example.txt", "rb").read()).decode()
        }
    ]
}

r = requests.post(
    f"{BASE_URL}/xmlhttpRequest",
    headers={"Authorization": f"Bearer {SECRET}"},
    json=payload
)
print(r.json())
```

## Use sqless as an ORM

Example:
```python
import sqless
db = sqless.DB(path_db = "your_database.db")

# create/get the "users" table
users = db['users']
# upsert item
users['U0001'] = {"name": "Tom", 'age':12, 'sex':'M', 'hobby':["football", 'basketball'],'meta':{"height": 1.75, "weight": 70}}
# get item
print(users['U0001'])

# query data from the "users" table
r = db.query("users", 'age > 9')
if r['suc']:
    print(r['data']) # result list
else:
    print(r['msg']) # error message
```

## Performance Test

Run the benchmark script:

```bash
pip install dataset pony sqlalchemy prettytable
python3 ./benchmark/cmp_with_other_orms.py
```

Test machine: **AMD EPYC 7K62 (4 cores) @ 2.595GHz, Ubuntu 22.04.5 LTS x86_64**

Result: 
```
| name       | init (s)        | write (s)         | read (s)         |
| ---------- | --------------- | ----------------- | ---------------- |
| dataset    | 0.006 (↑94.69%) | 2.932 (↑43.84%)   | 21.421 (↑95.89%) |
| pony.orm   | 0.015 (↑97.85%) | 0.040 (↓4013.15%) | 11.617 (↑92.43%) |
| sqlalchemy | 0.009 (↑96.45%) | 5.357 (↑69.26%)   | 27.279 (↑96.78%) |
| sqless     | 0.000 (↑0.00%)  | 1.647 (↑0.00%)    | 0.879 (↑0.00%)   |
```


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
