# sqless

An async HTTP server for SQLite, FileStorage and WebPage.

## Description

sqless is a Python application that provides web service with local database and local file storage.

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

- `{BASE_URL}/db/users` will read/write records in `users` table in `db/default.sqlite`.


- `{BASE_URL}/db/mall-users` will read/write records in `users` table in `db/mall.sqlite`.


- `{BASE_URL}/db/east-mall-users` will read/write records in `users` table in `db/east/mall.sqlite`.


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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
