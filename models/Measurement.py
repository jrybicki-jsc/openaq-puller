from models import meseaurement, metadata
from sqlalchemy import select
from dateutil import parser


class MeasurementDAO(object):
    def __init__(self, engine):
        self.engine = engine.get_engine()

    def create_table(self):
        metadata.create_all(self.engine)

    def drop_table(self):
        metadata.drop_all(self.engine)

    def store(self, series_id, value, date):
        ins = meseaurement.insert().values(
            series_id=series_id,
            value=value,
            date=parser.parse(date))

        res = self.engine.execute(ins)
        res.close()
        return True

    def get_all(self):
        s = select([meseaurement])
        return self.engine.execute(s).fetchall()

    def get_all_for_series(self, series_id, limit=100):
        s = select([meseaurement]).where(series_id == meseaurement.c.series_id).order_by(meseaurement.c.date.desc()).limit(limit)
        res = self.engine.execute(s)
        return res.fetchall()
