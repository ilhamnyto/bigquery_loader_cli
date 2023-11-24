import aiopg
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

class Connection:
    def __init__(self, credentials: dict) -> None:
        self.host = credentials['host']
        self.port = int(credentials['port'])
        self.user = credentials['user']
        self.password = credentials['password']
        self.db = credentials['db']

    async def create_pool(self) -> aiopg.Pool:
        dsn = f"dbname={self.db} user={self.user} password={urllib.parse.quote(self.password)} host={self.host} port={self.port}"
        pool = await aiopg.create_pool(dsn)
        return pool
    
    async def get_query(self, pool: aiopg.Pool, query: str) -> (list, list):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchall()

                columns_name = [i[0] for i in cur.description]
                return columns_name, list(result)

def PostgreSQLConnection(credentials: dict) -> Connection:
    return Connection(credentials)
