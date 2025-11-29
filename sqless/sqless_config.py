import os
import asyncio
from aiohttp_mcp import AiohttpMCP
from sqless import DBS

# --- Paths & Configuration ---

path_this :str = os.path.dirname(os.path.abspath(__file__))
path_base_db :str = os.path.realpath(f"{path_this}/db")
path_base_fs :str = os.path.realpath(f"{path_this}/fs")
path_base_www :str = os.path.realpath(f"{path_this}/www")

max_filesize :int = 200  # Max file size (in MB) allowed in POST /fs

# --- Database & MCP Server Initialization ---

dbs = DBS(path_base_db)
mcp = AiohttpMCP("My App", log_level="WARNING")

# --- MCP Tools / API Functions ---

@mcp.tool()
def hello() -> str:
    """A simple hello tool"""
    return "Hello from MCP!"

@mcp.tool()
def add(a: int, b: int) -> int:
    """A simple add tool"""
    return a + b

@mcp.tool()
def get_time(timezone: str) -> str:
    """Return current time in any timezone"""
    import datetime
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(timezone)
    return datetime.datetime.now(tz).isoformat()

# Async demo with semaphore (limit concurrent invocations)
semaphore_sleep = asyncio.Semaphore(3)

@mcp.tool()
async def sleep(seconds: int) -> str:
    """Sleep for given seconds, demonstrating rate limiting"""
    import datetime
    t0 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with semaphore_sleep:
        t1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        await asyncio.sleep(seconds)
        t2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"Slept for {seconds} seconds, arrive at {t0}, start at {t1}, finish at {t2}"

# --- Usage Notes ---
"""
MCP usage:
    URL: http://127.0.0.1:12239/mcp
    Header: Authorization: Bearer <secret>
    
API usage:
    GET example:
        curl -H "Authorization: Bearer <secret>" \
            "http://127.0.0.1:12239/api/add?a=1&b=2"
    POST example:
        curl -H "Authorization: Bearer <secret>" \
             -H "Content-Type: application/json" \
             -d '{"f":"add","a":1,"b":2}' \
             http://127.0.0.1:12239/api

Browser usage:
    http://127.0.0.1:12239/api/add 1 2
        username: (empty)
        password: <secret>
"""
