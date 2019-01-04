import os
from sqlalchemy import create_engine, select, func

from models import metadata, stationmeta


class MyEngine(object):
    def __init__(self, host, dbname, user, password):
        echo = False
        self.dbname = dbname
        if host == "":
            self.connection_string = 'sqlite:///:memory:'
            echo = False
        else:
            self.connection_string = "postgresql+psycopg2://{}:{}@{}/{}".format(user, password, host, dbname)
        self.engine = create_engine(self.connection_string, echo=echo)

    def get_engine(self):
        return self.engine

    def get_dbname(self):
        return self.dbname


def get_engine():
    # host empty is memory storage
    host = os.getenv('DB_HOST', '')
    dbname = os.getenv('DB_DATABASE', 'openaq')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASS', 'mysecretpassword')
    engine = MyEngine(host=host, dbname=dbname, user=user, password=password)

    return engine


class StationMetaCoreDAO(object):
    def __init__(self, engine):
        self.engine = engine.get_engine()

    def create_table(self):
        metadata.create_all(self.engine)

    def drop_table(self):
        metadata.drop_all(self.engine)

    def store(self, station_id, station_name, station_location, station_latitude, station_longitude, station_altitude,
              station_country, station_state):

        r = self.get_for_name(station_name)
        if r is not None:
            return r[0]

        ins = stationmeta.insert().values(
            station_id=station_id[:64],
            station_name=station_name[:128],
            station_location=station_location[:128],
            station_latitude=station_latitude,
            station_longitude=station_longitude,
            station_altitude=station_altitude,
            station_country=station_country[:128],
            station_state=station_state[:128])

        res = self.engine.execute(ins)
        last_id = res.inserted_primary_key[0]
        res.close()
        return last_id

    def get_for_name(self, station_name):
        s = select([stationmeta]).where(station_name == stationmeta.c.station_name)

        res = self.engine.execute(s)
        return res.first()

    def store_from_json(self, dct):
        return self.store(station_id=dct['location'],
                          station_name=dct['location'],
                          station_location=dct['city'],
                          station_latitude=dct['coordinates']['latitude'],
                          station_longitude=dct['coordinates']['longitude'],
                          station_altitude=0,
                          station_country=dct['country'],
                          station_state='')

    def get_all(self):
        s = select([stationmeta])
        return self.engine.execute(s).fetchall()

    def count(self):
        s= select([func.count()]).select_from(stationmeta)
        res = self.engine.execute(s)
        return res.fetchone()[0]

    def get_limited(self, limit, offset):
        s = select([stationmeta]).offset(offset).limit(limit)
        return self.engine.execute(s).fetchall()
