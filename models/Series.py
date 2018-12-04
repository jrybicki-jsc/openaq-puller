from models import timeseries, metadata
from sqlalchemy import select, and_
from dateutil import parser


class SeriesDAO(object):
    def __init__(self, engine):
        self.engine = engine.get_engine()

    def create_table(self):
        metadata.create_all(self.engine)

    def drop_table(self):
        metadata.drop_all(self.engine)

    def store(self, station_id, parameter, unit, averagingPeriod):
        r = self.get_for_values(station_id=station_id, parameter=parameter, unit=unit, averagingPeriod=averagingPeriod)
        if r is not None:
            return r[0]

        ins = timeseries.insert().values(
            station_id=station_id,
            parameter=parameter,
            unit=unit,
            averagingPeriod=averagingPeriod)

        res = self.engine.execute(ins)
        last_id = res.inserted_primary_key[0]
        res.close()
        return last_id

    def get_for_values(self, station_id, parameter, unit, averagingPeriod):
        s = select([timeseries]).where(and_(
            station_id == timeseries.c.station_id, 
            parameter == timeseries.c.parameter,
            unit == timeseries.c.unit,
            averagingPeriod == timeseries.c.averagingPeriod))

        res = self.engine.execute(s)
        return res.first()

    def get_all(self):
        s = select([timeseries])
        return self.engine.execute(s).fetchall()
