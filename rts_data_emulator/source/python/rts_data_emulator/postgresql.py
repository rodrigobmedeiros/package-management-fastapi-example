import asyncpg
import asyncio
from datetime import datetime
from datetime import timezone
import json
import json
import os

DB_PORT = "5432"
DB_HOST = "postgres.plat-system.svc.cluster.local"
DB_USER = "postgres"
DB_PASSWORD = "uJbrdP5JKvndVXfH"
DB_NAME = "libra_rts_mock"


class PostgresController:
    def __init__(self, dbname, user, password, ip, port, enabled=False):
        self.user = user
        self.dbname = dbname
        self.password = password
        self.valid = enabled
        self.aconn = None
        self.ip = ip
        self.port = port

    async def disconnect_psql(self):
        try:
            if self.aconn != None:
                await self.aconn.close()
            print("postgresql connection closed")
        except Exception as excp:
            print(f"disconnect_psql - {excp}")
        finally:
            self.valid = False
            self.aconn = None

    async def connect_psql(self):
        # Connect to an existing database
        try:
            m_dsn = f"postgres://{self.user}:{self.password}@{self.ip}:{self.port}/{self.dbname}"
            # self.aconn = await asyncpg.create_pool(dsn = m_dsn, min_size = 10, max_size = 50)
            self.aconn = await asyncpg.create_pool(dsn=m_dsn)
            print(f"postgresql connection ok - @{self.ip}:{self.port}/{self.dbname}")
            self.valid = self.aconn != None

            self.well_tags = await self.get_well_tags()
            self.pi_tags = await self.get_pi_tags()
            self.control_tags = await self.get_control_tags()

            return self.valid
        except Exception as excp:
            print(f"connect_psql - {excp}")
            await self.disconnect_psql()
        return False

    async def get_well_tags_cache(self):
        return self.well_tags

    async def get_pi_tags_cache(self):
        return self.pi_tags

    async def get_control_tags_cache(self):
        return self.control_tags

    async def get_well_tags(self):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    data = await connection.fetch("SELECT * FROM well_tags")
                    # print(data)
                    if data is not None:
                        well_db_id = {}
                        for well in data:
                            well = dict(well)
                            # pp.pprint(well)
                            description = json.loads(well["description"])
                            well_db_id[well["well_tag"]] = {
                                "unique_id": well["unique_id"],
                                "type": description["type"],
                            }  # producer / injector
                        # print('===============================================')
                        # print('well_db_id')
                        # print(well_db_id)
                        return dict(well_db_id)
        return None

    async def get_pi_tags(self):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    data = await connection.fetch("SELECT * FROM pi_tags")
                    # print(data)
                    if data is not None:
                        pi_db_id = {}
                        for pi in data:
                            pi = dict(pi)
                            # description = json.loads(pi['description'])
                            pi_db_id[pi["pi_tag"]] = {
                                "unique_id": pi["unique_id"],
                                "well_tag": pi["well_tag"],
                            }
                        # print('===============================================')
                        # print('pi_db_id')
                        # print(pi_db_id)
                        return dict(pi_db_id)
        return None

    async def get_control_tags(self):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    data = await connection.fetch("SELECT * FROM control_tags")
                    # print(data)
                    if data is not None:
                        control_db_id = {}
                        for ctrl in data:
                            ctrl = dict(ctrl)
                            control_db_id[ctrl["control_tag"]] = ctrl["unique_id"]
                        # print('===============================================')
                        # print('control_db_id')
                        # print(control_db_id)
                        return dict(control_db_id)
        return None

    async def get_measurements(
        self, well_tag, pi_tag, start_time, end_time, limit=None
    ):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    if limit is not None:  # order by timestamp desc
                        data = await connection.fetch(
                            "SELECT * FROM measurements WHERE well_tag = $1 AND pi_tag = $2 AND timestamp >= $3 AND timestamp <= $4 ORDER BY timestamp DESC LIMIT $5",
                            well_tag,
                            pi_tag,
                            start_time,
                            end_time,
                            limit,
                        )
                    else:  # order by timestamp desc
                        data = await connection.fetch(
                            "SELECT * FROM measurements WHERE well_tag = $1 AND pi_tag = $2 AND timestamp >= $3 AND timestamp <= $4 ORDER BY timestamp DESC",
                            well_tag,
                            pi_tag,
                            start_time,
                            end_time,
                        )
                    # print(data)
                    if data is not None:
                        dt = []
                        for d in data:
                            d = dict(d)
                            dt.append(d)
                        return dt
        return None

    async def get_measurements_date_limits(self, well_tag, pi_tag):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    data = await connection.fetchrow(
                        "SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date FROM measurements WHERE well_tag = $1 AND pi_tag = $2",
                        well_tag,
                        pi_tag,
                    )
                    # print(data)
                    if data is not None:
                        return data["min_date"], data["max_date"]
        return None, None

    async def get_pi_control_tags(self, well_tag, pi_tag):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    data = await connection.fetch(
                        "SELECT control_tag FROM control_measurements WHERE well_tag = $1 AND pi_tag = $2",
                        well_tag,
                        pi_tag,
                    )

                    if data is not None:
                        pi_control_db_id = list(set(data))
                        new_lst = []
                        for elem in pi_control_db_id:
                            new_lst.append(dict(elem)["control_tag"])
                        return new_lst
        return None

    async def get_control_measurements(
        self, well_tag, pi_tag, control_tag, start_time, end_time, limit=None
    ):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    if limit is not None:
                        data = await connection.fetch(
                            "SELECT * FROM control_measurements WHERE well_tag = $1 AND pi_tag = $2 AND control_tag = $3 AND timestamp >= $4 AND timestamp <= $5 ORDER BY timestamp DESC LIMIT $6",
                            well_tag,
                            pi_tag,
                            control_tag,
                            start_time,
                            end_time,
                            limit,
                        )
                    else:
                        data = await connection.fetch(
                            "SELECT * FROM control_measurements WHERE well_tag = $1 AND pi_tag = $2 AND control_tag = $3 AND timestamp >= $4 AND timestamp <= $5 ORDER BY timestamp DESC",
                            well_tag,
                            pi_tag,
                            control_tag,
                            start_time,
                            end_time,
                        )
                    # print(data)
                    if data is not None:
                        dt = []
                        for d in data:
                            d = dict(d)
                            dt.append(d)
                        return dt

        return None

    async def get_control_measurements_date_limits(
        self, well_tag, pi_tag, control_tag=None
    ):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    if control_tag is None:
                        data = await connection.fetchrow(
                            "SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date FROM control_measurements WHERE well_tag = $1 AND pi_tag = $2",
                            well_tag,
                            pi_tag,
                        )
                    else:
                        data = await connection.fetchrow(
                            "SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date FROM control_measurements WHERE well_tag = $1 AND pi_tag = $2 AND control_tag = $3",
                            well_tag,
                            pi_tag,
                            control_tag,
                        )
                    # print(data)
                    if data is not None:
                        return data["min_date"], data["max_date"]
        return None, None

    async def get_wells(self):
        if self.valid:
            async with self.aconn.acquire() as connection:
                async with connection.transaction():
                    data = await connection.fetch("SELECT well_tag FROM well_tags")
                    # print(data)
                    if data is not None:
                        wells = []
                        for well in data:
                            well = dict(well)
                            wells.append(well)
                        return wells
        return None


async def main():

    _start = datetime.now()
    postgresConn = PostgresController(
        DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, True
    )

    if await postgresConn.connect_psql():
        well_tags = await postgresConn.get_well_tags_cache()
        print(well_tags)
        pi_tags = await postgresConn.get_pi_tags_cache()
        print(pi_tags)
        control_tags = await postgresConn.get_control_tags_cache()
        print(control_tags)

        print("================================================")
        date_min, date_max = await postgresConn.get_measurements_date_limits(1, 1)
        print(f"date_min: {date_min} - date_max: {date_max}")

        print("================================================")
        get_measurements = await postgresConn.get_measurements(
            1, 1, date_min, date_max, 10
        )
        print(get_measurements)

        print("================================================")
        date_min, date_max = await postgresConn.get_control_measurements_date_limits(
            1, 1, 1
        )
        print(f"date_min: {date_min} - date_max: {date_max}")

        print("================================================")
        get_control_measurements = await postgresConn.get_control_measurements(
            1, 1, 1, date_min, date_max, 10
        )
        print(get_control_measurements)
        print("================================================")

        await postgresConn.disconnect_psql()

    else:
        print("Not connected to Postgres")

    print(f"Execution time: { datetime.now() - _start }")


if __name__ == "__main__":
    asyncio.run(main())
