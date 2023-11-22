import os
import aiomysql
import urllib.parse
from dotenv import load_dotenv

load_dotenv()

class Connection:
    def __init__(self) -> None:
        self.host = os.getenv('MYSQL_HOST', '127.0.0.1')
        self.port = int(os.getenv('MYSQL_PORT', 3306))
        self.user = os.getenv('MYSQL_USER', 'root')
        self.password = os.getenv('MYSQL_PASSWORD', '')
        self.db = os.getenv('MYSQL_DB', '')

    async def create_pool(self) -> aiomysql.Pool:
        conf = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': urllib.parse.quote(self.password),
            'db': self.db,
            'minsize': 5,
            'maxsize': 10
        }
        pool = await aiomysql.create_pool(**conf)
        return pool
    
    async def get_query(self, pool: aiomysql.Pool, query: str) -> (list, list):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchall()

                columns_name = [i[0] for i in cur.description]
                return columns_name, list(result)

def MySQLConnection() -> Connection:
    return Connection()
