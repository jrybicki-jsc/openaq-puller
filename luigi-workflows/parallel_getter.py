import luigi
from datetime import datetime
import concurrent.futures

from models.Measurement import MeasurementDAO
from models.StationMeta import StationMetaCoreDAO, get_engine

from mys3utils.tools import get_object_list, FETCHES_BUCKET, filter_objects, serialize_object, read_object_list, \
     get_objects, process_file


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





class AlternativeGetObjects(luigi.Task):
    prefix = luigi.Parameter(default='realtime/')

    def requires(self):
        return GetPrefixes(self.prefix)

    def run(self):
        with self.input().open('r') as f:
            prefix_list = list(map(lambda a: a.strip(), f.readlines()))

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            future_to_url = {executor.submit(get_objects, prefix): prefix for prefix in prefix_list}
            with self.output().open('w') as f:
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print('%r generated an exception: %s' % (url, exc))
                    else:
                        for o in data:
                            f.write(serialize_object(o))

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

        engine = get_engine()
        station_dao = StationMetaCoreDAO(engine=engine)
        mes_dao = MeasurementDAO(engine=engine)
        station_dao.create_table()

        records = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            processor_objects = {executor.submit(process_file, obj['Name'], station_dao, mes_dao): obj['Name']
                                 for obj in filtered}

            for future in concurrent.futures.as_completed(processor_objects):
                object_name = processor_objects[future]
                try:
                    rr = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (object_name, exc))
                else:
                    records += rr

        with self.output().open('w') as f:
            f.write(self.run_date.strftime('%x\n'))
            f.write('%d records processed\n' % records)
            f.write('%d stations stored in db\n' % len(station_dao.get_all()))
            f.write('%d measurements stored in db\n' % len(mes_dao.get_all()))

    def output(self):
        return luigi.LocalTarget(self.run_date.strftime('data/run-%Y-%m-%d-%H-%M.dat'))
