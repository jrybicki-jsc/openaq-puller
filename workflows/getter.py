import luigi
from datetime import datetime
from models.StationMeta import StationMetaCoreDAO

from mys3utils.tools import get_object_list, FETCHES_BUCKET, filter_objects, serialize_object, read_object_list, \
    get_jsons_from_object, split_record


class GetPrefixes(luigi.Task):
    prefix = luigi.Parameter(default='realtime/')

    def run(self):
        _, prefixes = get_object_list(bucket_name=FETCHES_BUCKET, prefix=self.prefix)
        with self.output().open('w') as f:
            f.write("\n".join(prefixes))

    def output(self):
        n = datetime.now()
        return luigi.LocalTarget(n.strftime('data/{}-prefixes-%Y-%m-%d.dat'.format(self.prefix.replace('/', ''))))


class GetObjectList(luigi.Task):
    prefix = luigi.Parameter(default='realtime/')

    def requires(self):
        return GetPrefixes(self.prefix)

    def run(self):
        all_objects = []
        with self.input().open('r') as f:
            for prefix in f:
                obj, _ = get_object_list(bucket_name=FETCHES_BUCKET, prefix=prefix)
                all_objects += obj

        with self.output().open('w') as f:
            for obj in all_objects:
                f.write(serialize_object(obj))

    def output(self):
        n = datetime.now()
        return luigi.LocalTarget(n.strftime('data/{}-objects-%Y-%m-%d-%H-%M.csv'.format(self.prefix.replace('/', ''))))


class GetObjects(luigi.Task):
    prefix = luigi.Parameter(default='realtime/')
    start_date = luigi.DateSecondParameter(default=datetime(2017, 8, 7))
    end_date = luigi.DateSecondParameter(default=datetime.now())
    run_date = luigi.DateSecondParameter(default=datetime.now())

    def requires(self):
        return GetObjectList(self.prefix)

    def run(self):
        self.run_date = datetime.now()
        with self.input().open('r') as f:
            wl = read_object_list(f)

        filtered = list(filter_objects(all_objects=wl, start_date=self.start_date, end_date=self.end_date))

        station_dao = StationMetaCoreDAO(host='', dbname='mm', user='jj', password='s3cret')
        station_dao.create_table()

        records = 0
        invalid_records = 0

        for obj in filtered:
            for record in get_jsons_from_object(bucket=FETCHES_BUCKET, object_name=obj['Name']):
                station, measurement, ext = split_record(record)

                station_dao.store_from_json(station)
                records += 1

        with self.output().open('w') as f:
            f.write(self.run_date.strftime('%x'))
            f.write('%d stations stored in db\n' % len(station_dao.get_all()))
            f.write('%d valid records processed\n' % records)
            f.write('%d invalid records found\n' % invalid_records)

    def output(self):
        return luigi.LocalTarget(self.run_date.strftime('data/run-%Y-%m-%d-%H-%M.dat'))
