from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request
from flask_admin import BaseView, expose
from flask_admin.model import BaseModelView
from flask_admin.base import MenuLink
from airflow.hooks.postgres_hook import PostgresHook

from wtforms import form, StringField, FloatField

from collections import namedtuple
from flask_admin.model.fields import InlineFormField, InlineFieldList
from flask_admin.model.template import EndpointLinkRowAction, LinkRowAction


from models.Measurement import MeasurementDAO
from models.Series import SeriesDAO
from models.StationMeta import StationMetaCoreDAO
import types
import logging

import pandas as pd

Station = namedtuple('Station', 'station_id country state series lat lon')


def setup_daos():
    try:
        pg = PostgresHook(postgres_conn_id='openaq-db')
    except:
        logging.error(
            'Remote database not defined. Use [openaq-db] connection')
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
                    'series': series_dao.count(station_id=station[0]),
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
                'count': mes_dao.count(series_id=s[0])
            })

        return ret

    def get_data(self, series_id):
        _, series_dao, mes_dao = setup_daos()
        series = series_dao.get_for_id(series_id=series_id)

        meas = mes_dao.get_all_for_series(series_id=series_id, limit=50)

        ret = [{'date': m[2].isoformat(), 'value': m[1]} for m in meas]
        
        return ret[::-1], series

    def get_rolling_mean(self, data):
        myf = pd.DataFrame(data)
        myf['value'] = myf.value.rolling(window=10).mean().fillna(0)
        return myf[['date', 'value']].to_dict('records')

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
        measurements, series = self.get_data(series_id=series_id)
        rolling = self.get_rolling_mean(data=measurements)
        return self.render('dataview/measurements.html', series_id=series[1], parameter=series[2], unit=series[3], measurements=measurements, rolling=rolling)


class Tes(BaseModelView):
    can_create = False
    can_edit = True
    can_delete = False
    page_size = 10

    column_list = ['station_id',
                   'country',
                   'state',
                   'series',
                   'lat',
                   'lon']

    column_default_sort = ('station_id', True)
    column_sortable_list = [
        'station_id'
    ]

    column_labels = dict(station_id='Station Name', country='Country', state='State',
                         series='Series', lat='Latitude', lon='Longitude')  # Rename 'title' column in list view
    column_searchable_list = [
        'station_id'
    ]

    can_export = True
    export_max_rows = 1000
    export_types = ['csv', 'xls']

    column_extra_row_actions = [
        #LinkRowAction('icon-edit', 'http://direct.link/?id={row_id}'),
        EndpointLinkRowAction(icon_class='icon-search glyphicon glyphicon-search',
                              endpoint='dataview.series', title="View Details", id_arg='station_name')
    ]

    def create_model(self, **kwargs):
        pass

    def delete_model(self, **kwargs):
        pass

    def get_pk_value(self, model):
        return model.station_id

    def get_list(self, page, sort_field, sort_desc, search, filters, **kwargs):
        station_dao, series_dao, _ = setup_daos()
        count = station_dao.count()
        stations = station_dao.get_limited(
            limit=self.page_size, offset=page*self.page_size)
        ret = list()
        for station in stations:
            ret.append(Station(station_id=station[0], country=station[6],
                               state=station[1], series=series_dao.count(station_id=station[0]), lat=station[3], lon=station[4]))

        return count, ret

    def get_one(self, id, **kwargs):
        station_dao, _, _ = setup_daos()
        station = station_dao.get_for_name(station_name=id)

        return Station(station_id=station[0], country=station[6], state=station[1], series=0, lat=station[3], lon=station[4])

    # def scaffold_list_columns(self):
    #    return ['name', 'field']

    # def scaffold_sortable_columns(self):
    #    return None

    def scaffold_form(self):
        class MyForm(form.Form):
            station_id = StringField('Station Name')
            country = StringField('Country')
            lat = FloatField('Latitude')
            lon = FloatField('Longitude')
        return MyForm

    def update_model(self, form, model):
        print('Currently not supported')
        return True


admin_view = DataView(category='OpenAQ', name='DataView')
blue_print2 = Blueprint('dataview_plugin', __name__, template_folder='templates',
                        static_folder='static', static_url_path='/static/dataview_plugin')

ml = MenuLink(
    category='OpenAQ',
    name='S3 Bucket',
    url='https://openaq-fetches.s3.amazonaws.com/index.html')

altview = Tes(model='Stations', name='Stations', endpoint='test11', category='OpenAQ')


class AirflowDataView(AirflowPlugin):
    name = 'dataview_plugin'
    admin_views = [admin_view, altview]
    flask_blueprints = [blue_print2]
    menu_links = [ml]
