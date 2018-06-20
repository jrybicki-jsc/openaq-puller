from sqlalchemy import create_engine, select

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


class StationMetaCoreDAO(object):
    def __init__(self, engine):
        self.engine = engine.get_engine()

    def create_table(self):
        metadata.create_all(self.engine)

    def drop_table(self):
        metadata.drop_all(self.engine)

    def store(self, station_id, station_name, station_latitude, station_longitude, station_altitude,
              station_country, station_state):

        r = self.get_for_name(station_name)
        if r is not None:
            return r

        ins = stationmeta.insert().values(
            station_id=station_id,
            station_name=station_name,
            station_latitude=station_latitude,
            station_longitude=station_longitude,
            station_altitude=station_altitude,
            station_country=station_country,
            station_state=station_state)

        res = self.engine.execute(ins)
        last_id = res.inserted_primary_key[0]
        res.close()
        return last_id

    def get_for_name(self, station_name):
        s = select([stationmeta]).where(stationmeta.c.station_name==station_name)

        res = self.engine.execute(s)
        return res.first()

    def store_from_json(self, dct):
        return self.store(station_id=dct['sourceName'],
                          station_name=dct['sourceName'],
                          station_latitude=dct['coordinates']['latitude'],
                          station_longitude=dct['coordinates']['longitude'],
                          station_altitude=0,
                          station_country=dct['country'],
                          station_state='')

    def get_all(self):
        s = select([stationmeta])
        return self.engine.execute(s).fetchall()
