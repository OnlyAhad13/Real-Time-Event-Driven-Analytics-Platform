import os
import asyncpg
import logging

logger = logging.getLogger("api")

class Database:
    def __init__(self):
        self.pool = None
        self.url = f"postgresql://{os.getenv('POSTGRES_USER', 'analytics_user')}:{os.getenv('POSTGRES_PASSWORD', 'analytics_password_change_me')}@{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'analytics_warehouse')}"

    async def connect(self):
        logger.info(f"Connecting to database at {self.url.split('@')[-1]}...") # Don't log credentials
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.url,
                min_size=1,
                max_size=10
            )
            logger.info("Database connected.")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database disconnected.")

    async def fetch_all(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetch_one(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

db = Database()
