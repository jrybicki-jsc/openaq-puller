from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_admin import BaseView, expose
from airflow.hooks.postgres_hook import PostgresHook

class MainView(BaseView):

    @expose('/')
    def test(self):
        pg = PostgresHook(postgres_conn_id='file_list_db')
        query = '''SELECT date, prefix, count(name) as objects, sum(size) as size
                   FROM prefix_checks, object 
                   WHERE prefix_checks.id = object.prefix_check 
                   GROUP BY prefix_checks.id'''
        data = list()
        for date, prefix, objects, size in pg.get_records(query):
            data.append({'date': date, 'prefix': prefix, 'objects': objects, 'size': size})
    

        return self.render('myplugin/main.html', data=data)

admin_view = MainView(category='My Plugins', name='Prefix Checks')

blue_print = Blueprint('my_plugin', __name__, template_folder='templates', static_folder='static', static_url_path='/static/my_plugin')

class AiriflowMyPlugin(AirflowPlugin):
    name = 'my_plugin'
    admin_views = [admin_view]
    flask_blueprints = [blue_print]
