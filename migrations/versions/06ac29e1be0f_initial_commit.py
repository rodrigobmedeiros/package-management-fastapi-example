"""initial commit

Revision ID: 06ac29e1be0f
Revises: 
Create Date: 2024-03-04 11:48:20.098745

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
revision: str = '06ac29e1be0f'
down_revision: Union[str, None] = None
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

def upgrade() -> None:
    import sys
    from pathlib import Path
    python_code = Path().resolve() / "src" / "libra_rt_data" / "python"
    pickle_path = Path().resolve() / "src" / "libra_rt_data" / "notebooks"
    print('================================================')
    print('================================================')
    print(python_code)
    print('================================================')
    print('================================================')

    sys.path.append(str(python_code))
    from constants import RJS739_DATA_PATH, RJS742_DATA_PATH, DIRECTORY_LEVELS_ABOVE
    # from utils import get_data_directory
    import datamap_reader

    full_producer, full_injector = datamap_reader.load_data_from_pickle(pickle_path / 'all_tags_mapping.pkl')
    # print('================================================')
    # print(full_producer)
    # print('================================================')
    # print(full_injector)
    # print('================================================')

    bind = op.get_bind()
    session = orm.Session(bind=bind)

    WellTags.__table__.create(bind)
    PITags.__table__.create(bind)
    ControlTags.__table__.create(bind)

    wells = [{'desc' : 'RJS739', 'type': 'producer', 'metadata' : full_producer}, 
             {'desc' : 'RJS742', 'type': 'injector', 'metadata' : full_injector}]

    #RJS739 or RJS742
    for well in wells:
        well_tag = WellTags(well_tag=well['desc'], description=well)
        session.add(well_tag)
    session.commit()

    well_db_id = {}

    for well in session.query(WellTags).all():
        well_db_id[well.description['desc']] = well.unique_id
    
    print('================================================')
    print('well_db_id')
    print(well_db_id)

    producer_tags = list(full_producer.keys())
    injector_tags = list(full_injector.keys())

    all_tags = set()

    for tag in producer_tags:
        backup = full_producer[tag]['description']['backup']
        description = full_producer[tag]
        if 'count' in description:
            del description['count']
        if 'streaming_tags' in description:
            my_tags = set(description['streaming_tags'].keys())
            if 'numeric' in description['streaming_tags']:
                my_tags.remove('numeric')
            all_tags.update(list(my_tags))

        main_tag = None
        if backup:
            main_tag = full_producer[tag]['description']['main']
        pi_tag = PITags(well_tag=well_db_id['RJS739'], pi_tag=tag, backup = backup, main_tag = main_tag, description=description)
        session.add(pi_tag)
    
    for tag in injector_tags:
        backup = full_injector[tag]['description']['backup']
        description = full_injector[tag]
        if 'count' in description:
            del description['count']
        if 'streaming_tags' in description:
            my_tags = set(description['streaming_tags'].keys())
            if 'numeric' in description['streaming_tags']:
                my_tags.remove('numeric')
            all_tags.update(list(my_tags))
        main_tag = None
        if backup:
            main_tag = full_injector[tag]['description']['main']
        pi_tag = PITags(well_tag=well_db_id['RJS742'], pi_tag=tag, backup = backup, main_tag = main_tag, description=description)
        session.add(pi_tag)
    
    session.commit()
    # print('================================================')
    # print('all_tags')
    # print(all_tags)
    # print('================================================')
    for tag in all_tags:
        control_tag = ControlTags(control_tag=tag)
        session.add(control_tag)
    session.commit()
    tag_db_id = {}
    for tag in session.query(ControlTags).all():
        tag_db_id[tag.control_tag] = tag.unique_id

    print('================================================')
    print('tag_db_id')
    print(tag_db_id)
    print('================================================')


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

    print('================================================')
    print('upgrade completed ==============================')
    print('================================================')

def downgrade() -> None:
    op.drop_table("pi_tags")
    op.drop_table("well_tags")
    op.drop_table("control_tags")

    print('================================================')
    print('downgrade completed ============================')
    print('================================================')
    op.drop_table("measurements")
    op.drop_table("control_measurements")
