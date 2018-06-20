from sqlalchemy import Table, MetaData, Column, Integer, String, Float, ForeignKey, Sequence, DateTime

metadata = MetaData()

stationmeta = Table('stationmetacore', metadata,
                    Column('station_id', String(64), primary_key=True),
                    Column('station_name', String(128)),
                    Column('station_latitude', Float()),
                    Column('station_longitude', Float()),
                    Column('station_altitude', Float()),
                    Column('station_country', String(128)),
                    Column('station_state', String(128)),
                    keep_existing=True,
                    )

mes_meta = Table('measurement', metadata,
                 Column('id', Integer, Sequence('mes_id_seq'), primary_key=True),
                 Column('station_id', None, ForeignKey('stationmetacore.station_id')),
                 Column('parameter', String(60)),
                 Column('value', Float()),
                 Column('unit', String(60)),
                 Column('averagingPeriod', String(80)),
                 Column('date', DateTime),
                 keep_existing=True,
                 )
