import asyncio
from src.usecase.bq_loader import BigQueryLoader

async def main():
    await BigQueryLoader().run()    

if __name__ == '__main__':
    asyncio.run(main())