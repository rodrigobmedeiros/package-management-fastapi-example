"""v1 - load data from pickle

Revision ID: 2f17a66d22b6
Revises: 06ac29e1be0f
Create Date: 2024-03-04 18:54:38.498782

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

# revision identifiers, used by Alembic.
revision: str = '2f17a66d22b6'
down_revision: Union[str, None] = '06ac29e1be0f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

class WellTags(Base):
    __tablename__ = 'well_tags'
    unique_id   = sa.Column(sa.BigInteger, primary_key = True, autoincrement=True, nullable=False)
    well_tag    = sa.Column(TEXT, nullable=False)
    description = sa.Column(JSONB, nullable=False)

class PITags(Base):
    __tablename__ = 'pi_tags'
    unique_id   = sa.Column(sa.BigInteger, primary_key = True, autoincrement=True, nullable=False)
    well_tag    = sa.Column(sa.BigInteger, sa.ForeignKey('well_tags.unique_id'), autoincrement=False, nullable=False)
    pi_tag      = sa.Column(TEXT, nullable=False)
    backup      = sa.Column(sa.Boolean, nullable=False)
    main_tag    = sa.Column(TEXT, nullable=True)
    description = sa.Column(JSONB, nullable=False)

class ControlTags(Base):
    __tablename__ = 'control_tags'
    unique_id   = sa.Column(sa.BigInteger, primary_key = True, autoincrement=True, nullable=False)
    control_tag = sa.Column(TEXT, nullable=False)
    description = sa.Column(TEXT, nullable=True)

class Measurements(Base):
    __tablename__ = 'measurements'
    unique_id   = sa.Column(sa.BigInteger, primary_key = True, autoincrement=True, nullable=False)
    well_tag    = sa.Column(sa.BigInteger, sa.ForeignKey('well_tags.unique_id'), autoincrement=False, nullable=False)
    pi_tag      = sa.Column(sa.BigInteger, sa.ForeignKey('pi_tags.unique_id'), autoincrement=False, nullable=False)
    value       = sa.Column(sa.Float, nullable=False)
    timestamp   = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)

class ControlMeasurements(Base):
    __tablename__ = 'control_measurements'
    unique_id   = sa.Column(sa.BigInteger, primary_key = True, autoincrement=True, nullable=False)
    well_tag    = sa.Column(sa.BigInteger, sa.ForeignKey('well_tags.unique_id'), autoincrement=False, nullable=False)
    pi_tag      = sa.Column(sa.BigInteger, sa.ForeignKey('pi_tags.unique_id'), autoincrement=False, nullable=False)
    control_tag = sa.Column(sa.BigInteger, sa.ForeignKey('control_tags.unique_id'), autoincrement=False, nullable=False)
    value       = sa.Column(sa.Integer, nullable=False)
    timestamp   = sa.Column(sa.TIMESTAMP(timezone=True), nullable=False)

def upgrade() -> None:

    bind = op.get_bind()
    session = orm.Session(bind=bind)


    op.alter_column(table_name = "measurements", column_name = "timestamp",
                    new_column_name= "timestamp_old",
                            nullable=False)
    op.add_column('measurements', sa.Column("timestamp", sa.TIMESTAMP(timezone=True), nullable=False))
    op.drop_column('measurements', 'timestamp_old')

    op.alter_column(table_name = "control_measurements", column_name = "control_tags",
                    new_column_name= "control_tag",
                            nullable=False)
    op.alter_column(table_name = "control_measurements", column_name = "timestamp",
                    new_column_name= "timestamp_old",
                            nullable=False)
    op.add_column('control_measurements', sa.Column("timestamp", sa.TIMESTAMP(timezone=True), nullable=False))
    op.drop_column('control_measurements', 'timestamp_old')

    session.commit()

    import sys
    from datetime import timedelta
    from datetime import timezone
    from pathlib import Path
    python_code = Path().resolve() / "src" / "libra_rt_data" / "python"
    data_files = Path().resolve() / "src" / "data"
    sys.path.append(str(python_code))
    from constants import RJS739_DATA_PATH, RJS742_DATA_PATH
    import datamap_reader

    # ============================================================================================
    well_db_id = {}
    for well in session.query(WellTags).all():
        well_db_id[well.unique_id] = {'desc' : well.description['desc'], 'type': well.description['type']} #producer / injector
    print('================================================')
    print('well_db_id')
    print(well_db_id)
    # ============================================================================================
    pi_db_id = {}
    for pi in session.query(PITags).all():
        pi_db_id[pi.pi_tag] = {'unique_id' : pi.unique_id, 'well_tag' : pi.well_tag}
    print('================================================')
    print('pi_db_id')
    print(pi_db_id)
    # ============================================================================================
    control_db_id = {}
    for ctrl in session.query(ControlTags).all():
        control_db_id[ctrl.control_tag] =  ctrl.unique_id
    print('================================================')
    print('control_db_id')
    print(control_db_id)
    # ============================================================================================


    for tag in pi_db_id.keys():
        measur = []
        c_measur = []
        if well_db_id[pi_db_id[tag]['well_tag']]['type'] == 'producer':
            search_path = data_files / RJS739_DATA_PATH
        else:
            search_path = data_files / RJS742_DATA_PATH

        file_name = tag + '.pkl'
        file_path = search_path / file_name
        if file_path.exists():
            print(f"pickle file {file_path} exists, ok...")
            df = datamap_reader.read_df_pickle(file_path)

            measurements = df['valid'].isin([True])
            control_measurements = df['valid'].isin([False])

            measurements = df[measurements]
            control_measurements = df[control_measurements]

            # print(pi_db_id[tag])

            for index, measure in measurements.iterrows():
                measur.append(Measurements(well_tag = pi_db_id[tag]['well_tag'], pi_tag = pi_db_id[tag]['unique_id'], value = measure['measurement'], timestamp = measure['date_time'].tz_localize(timezone.utc)))

            for index, c_measure in control_measurements.iterrows():
                time_stamp_before  = c_measure['date_time'].tz_localize(timezone.utc) - timedelta(microseconds=1)
                time_stamp_actual  = c_measure['date_time'].tz_localize(timezone.utc)
                time_stamp_r_after = c_measure['date_time'].tz_localize(timezone.utc) + timedelta(microseconds=1)
                time_stamp_clear   = c_measure['date_time'].tz_localize(timezone.utc) + timedelta(microseconds=2)
                c_measur.append(ControlMeasurements(well_tag = pi_db_id[tag]['well_tag'], pi_tag = pi_db_id[tag]['unique_id'], control_tag = control_db_id[c_measure['reading']], value = 0, timestamp = time_stamp_before))
                c_measur.append(ControlMeasurements(well_tag = pi_db_id[tag]['well_tag'], pi_tag = pi_db_id[tag]['unique_id'], control_tag = control_db_id[c_measure['reading']], value = 1, timestamp = time_stamp_actual))
                c_measur.append(ControlMeasurements(well_tag = pi_db_id[tag]['well_tag'], pi_tag = pi_db_id[tag]['unique_id'], control_tag = control_db_id[c_measure['reading']], value = 1, timestamp = time_stamp_r_after))
                c_measur.append(ControlMeasurements(well_tag = pi_db_id[tag]['well_tag'], pi_tag = pi_db_id[tag]['unique_id'], control_tag = control_db_id[c_measure['reading']], value = 0, timestamp = time_stamp_clear))
            # print('Measurements ===================================')
            # print(measurements.head())
            # print('Control     ====================================')
            # print(control_measurements.head())
        session.bulk_save_objects(measur)
        session.bulk_save_objects(c_measur)
        session.commit()
        print(f'commit - ok ======= measures:({len(measur)})========= control:({len(c_measur)})==========')
    # sys.exit()

    print('================================================')
    print('upgrade completed ==============================')
    print('================================================')

def downgrade() -> None:
    bind = op.get_bind()
    session = orm.Session(bind=bind)

    op.drop_table('measurements')
    op.drop_table('control_measurements')

    op.create_table('measurements', 
    sa.Column('unique_id', sa.BigInteger, primary_key = True, autoincrement=True , nullable=False),
    sa.Column('well_tag',sa.BigInteger,sa.ForeignKey('well_tags.unique_id'), autoincrement=False, nullable=False),
    sa.Column('pi_tag',sa.BigInteger,sa.ForeignKey('pi_tags.unique_id'), autoincrement=False, nullable=False),
    sa.Column('timestamp',sa.DateTime, nullable=False),
    sa.Column('value',sa.Float, nullable=False)
    )
    op.create_table('control_measurements', 
    sa.Column('unique_id', sa.BigInteger, primary_key = True, autoincrement=True , nullable=False),
    sa.Column('well_tag',sa.BigInteger,sa.ForeignKey('well_tags.unique_id'), autoincrement=False, nullable=False),
    sa.Column('pi_tag',sa.BigInteger,sa.ForeignKey('pi_tags.unique_id'), autoincrement=False, nullable=False),
    sa.Column('timestamp',sa.DateTime, nullable=False),
    sa.Column('control_tags',sa.BigInteger,sa.ForeignKey('control_tags.unique_id'), autoincrement=False, nullable=False),
    sa.Column('value', sa.Integer, nullable=False)
    )

    session.commit()
    print('================================================')
    print('downgrade completed ============================')
    print('================================================')
