from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink
from airflow.hooks.postgres_hook import PostgresHook

from models.Measurement import MeasurementDAO
from models.Series import SeriesDAO
from models.StationMeta import StationMetaCoreDAO
import types
import logging

def setup_daos():
    try:
        pg = PostgresHook(postgres_conn_id='openaq-db')    
    except:
        logging.error('remote database not defined. Use [openaq-db] connection')
        return None

    wrapper = types.SimpleNamespace()
    wrapper.get_engine = pg.get_sqlalchemy_engine
    station_dao = StationMetaCoreDAO(engine=wrapper)
    series_dao = SeriesDAO(engine=wrapper)
    mes_dao = MeasurementDAO(engine=wrapper)

    return station_dao, series_dao, mes_dao

class DataView(BaseView):

    def get_stations(self):
        station_dao, series_dao, _ = setup_daos()
        stations = list()
        for station in station_dao.get_all():
            stations.append(
                {
                    'station_id': station[0],
                    'country': station[6],
                    'state': station[1],
                    'series': len(series_dao.get_all_for_station(station_id=station[0])),
                    'lat': station[3],
                    'lon': station[4]
                }
            )
        stations = list(station_dao.get_all())
        return stations

    def get_series(self, station_name):
        _, series_dao, mes_dao = setup_daos()
        series = series_dao.get_all_for_station(station_id=station_name)
        
        ret = list()
        for s in series:
            ret.append({
                'id': s[0],
                'parameter': s[2],
                'unit': s[3], 
                'averagingPeriod': s[4],
                'count': len(mes_dao.get_all_for_series(series_id=s[0]))
            })

        return ret

    def get_data(self, series_id):
        _, series_dao, mes_dao = setup_daos()
        series = series_dao.get_for_id(series_id = series_id)

        meas = mes_dao.get_all_for_series(series_id=series_id, limit=50)
        ret = list()
        for m in meas:
            ret.append({
                'date': m[2].isoformat(),
                'value': m[1]
            })

        return ret[::-1], series

    @expose('/')
    def stations(self):
        stations = self.get_stations()
        return self.render('dataview/main.html', stations=stations)

    @expose('/series/<string:station_name>')
    def series(self, station_name):
        series = self.get_series(station_name=station_name)
        return self.render('dataview/series.html', station_name=station_name, series=series)

    @expose('/measurements/<int:series_id>')
    def measurements(self, series_id):
        measurements, series =self.get_data(series_id=series_id)
        return self.render('dataview/measurements.html', series_id=series[1], parameter=series[2], unit=series[3], measurements=measurements)
    




admin_view = DataView(category='OpenAQ', name='DataView')
blue_print2 = Blueprint('dataview_plugin', __name__, template_folder='templates', 
           static_folder='static', static_url_path='/static/dataview_plugin')

ml = MenuLink(
    category='OpenAQ',
    name='S3 Bucket',
    url='https://openaq-fetches.s3.amazonaws.com/index.html')



class AirflowDataView(AirflowPlugin):
    name = 'dataview_plugin'
    admin_views = [admin_view]
    flask_blueprints = [blue_print2]
    menu_links = [ml]
