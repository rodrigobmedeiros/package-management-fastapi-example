import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

Base = sa.orm.declarative_base()


class WellTags(Base):
    __tablename__ = "well_tags"
    unique_id = sa.Column(
        sa.BigInteger, primary_key=True, autoincrement=True, nullable=False
    )
    well_tag = sa.Column(TEXT, nullable=False)
    description = sa.Column(JSONB, nullable=False)


class PITags(Base):
    __tablename__ = "pi_tags"
    unique_id = sa.Column(
        sa.BigInteger, primary_key=True, autoincrement=True, nullable=False
    )
    well_tag = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("well_tags.unique_id"),
        autoincrement=False,
        nullable=False,
    )
    pi_tag = sa.Column(TEXT, nullable=False)
    backup = sa.Column(sa.Boolean, nullable=False)
    main_tag = sa.Column(TEXT, nullable=True)
    description = sa.Column(JSONB, nullable=False)


class ControlTags(Base):
    __tablename__ = "control_tags"
    unique_id = sa.Column(
        sa.BigInteger, primary_key=True, autoincrement=True, nullable=False
    )
    control_tag = sa.Column(TEXT, nullable=False)
    description = sa.Column(TEXT, nullable=True)


class Measurements(Base):
    __tablename__ = "measurements"
    unique_id = sa.Column(
        sa.BigInteger, primary_key=True, autoincrement=True, nullable=False
    )
    well_tag = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("well_tags.unique_id"),
        autoincrement=False,
        nullable=False,
    )
    pi_tag = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("pi_tags.unique_id"),
        autoincrement=False,
        nullable=False,
    )
    value = sa.Column(sa.Float, nullable=False)
    timestamp = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)


class ControlMeasurements(Base):
    __tablename__ = "control_measurements"
    unique_id = sa.Column(
        sa.BigInteger, primary_key=True, autoincrement=True, nullable=False
    )
    well_tag = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("well_tags.unique_id"),
        autoincrement=False,
        nullable=False,
    )
    pi_tag = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("pi_tags.unique_id"),
        autoincrement=False,
        nullable=False,
    )
    control_tag = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("control_tags.unique_id"),
        autoincrement=False,
        nullable=False,
    )
    value = sa.Column(sa.Integer, nullable=False)
    timestamp = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)


class DataClass:
    def __init__(self, name: str = ""):
        from sqlalchemy import create_engine

        self.engine = create_engine(
            "postgresql://postgres:libra123@localhost/libradata", echo=True
        )
        self.name = name

    async def close(self):
        with Session(self.engine) as session:
            await session.close()

    async def get_well_tags(self):
        with Session(self.engine) as session:
            return await session.query(WellTags).all()

    async def get_well_dict(self):
        well_db_id = {}
        for well in await self.get_well_tags():
            well_db_id[well.unique_id] = {
                "desc": well.description["desc"],
                "type": well.description["type"],
            }  # producer / injector
        print("================================================")
        print("well_db_id")
        print(well_db_id)
        return well_db_id

    async def get_pi_tags(self):
        with Session(self.engine) as session:
            return await session.query(PITags).all()

    async def get_pi_dict(self):
        pi_db_id = {}
        for pi in await self.get_pi_tags():
            pi_db_id[pi.pi_tag] = {"unique_id": pi.unique_id, "well_tag": pi.well_tag}
        print("================================================")
        print("pi_db_id")
        print(pi_db_id)
        return pi_db_id

    async def get_control_tags(self):
        with Session(self.engine) as session:
            return await session.query(ControlTags).all()

    async def get_control_dict(self):
        control_db_id = {}
        for ctrl in await self.get_control_tags():
            control_db_id[ctrl.control_tag] = ctrl.unique_id
        print("================================================")
        print("control_db_id")
        print(control_db_id)
        return control_db_id


# async def main():
#     print('start data_class main --------------------------')
#     _start = datetime.now()
#     data = DataClass()
#     well_db_id = await data.get_well_dict()
#     print(well_db_id)
#     print('================================================')
#     pi_db_id = await data.get_pi_dict()
#     print(pi_db_id)
#     print('================================================')
#     control_db_id = await data.get_control_dict()
#     print(control_db_id)

#     await data.close()

#     print(f"Execution time: { datetime.now() - _start }")

# if __name__ == "__main__":
#     asyncio.run(main())
