from models import timeseries, metadata
from sqlalchemy import select
from dateutil import parser


class SeriesDAO(object):
    def __init__(self, engine):
        self.engine = engine.get_engine()

    def create_table(self):
        metadata.create_all(self.engine)

    def drop_table(self):
        metadata.drop_all(self.engine)

    def store(self, station_id, parameter, unit, averagingPeriod):
        ins = timeseries.insert().values(
            station_id=station_id,
            parameter=parameter,
            unit=unit,
            averagingPeriod=averagingPeriod)

        res = self.engine.execute(ins)
        last_id = res.inserted_primary_key[0]
        res.close()
        return last_id

    def get_all(self):
        s = select([timeseries])
        return self.engine.execute(s).fetchall()
