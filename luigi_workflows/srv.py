import random
import luigi
import json
from datetime import timedelta, date, datetime
from luigi.contrib.s3 import S3Client


class GetPrefixList(luigi.Task):
    def run(self):
        start_date = date(2013, 11, 26)
        end_date = date(2018, 6, 7)
        current_date = start_date

        with self.output().open('w') as output:
            while current_date < end_date:
                output.write(current_date.strftime("realtime/%Y-%m-%d/\n"))
                current_date += timedelta(days=1)

    def output(self):
        return luigi.LocalTarget('data/prefixes.list')


class GetObjectList(luigi.Task):
    rundate = luigi.DateSecondParameter(default=datetime.now())

    def requires(self):
        return GetPrefixList()

    def run(self):
        # write date

        with self.output().open('w') as output:
            with self.input().open('r') as in_file:
                for prefix in in_file:
                    for fname in range(random.randint(1,20)):
                        output.write("{}{}.dat\n".format(prefix.strip(), 15212+fname))

    def output(self):
        return luigi.LocalTarget('data/object.list')


class GetObjectContent(luigi.Task):
    def requires(self):
        return [GetObjectList()]

    def run(self):
        counter = 0
        for t in self.input():
            with t.open('r') as file_input:
                for obj in file_input:
                    self.set_status_message("Progress: Pretending to retrieve {}".format(obj))
                    counter += 1

        with self.output().open('w') as output:
            output.write('Got total {} files'.format(counter))

    def output(self):
        return luigi.LocalTarget('aggregated.csv'.format(self.obj))


class ListS3Content(luigi.Task):
    def run(self):
        client = S3Client()

        with self.output().open('w') as output:
            output.write('Yo!')
            lst = client.listdir('/openaq/fetches/realtime/', start_time=None, end_time=None, return_key=False)

    def output(self):
        return luigi.LocalTarget('content.dat')


class GetLocalObjectContent(luigi.Task):
    fname = luigi.Parameter()

    def input_it(self):
        with open(self.fname, 'r') as f:
            for line in f:
                yield json.loads(line)

    def run(self):
        for js in self.input_it():
            pass
            
            #{"date": {"utc": "2018-06-06T23:00:00.000Z", "local": "2018-06-07T05:00:00+06:00"},
            # "parameter": "pm25",
            # "value": 27,
            # "unit": "µg/m³",
            # "averagingPeriod": {"value": 1, "unit": "hours"},
            # --> ??

            # "location": "US Diplomatic Post: Dhaka",
            # "city": "Dhaka"
            # "country": "BD",
            # "coordinates": {"latitude": 23.796373, "longitude": 90.424614},
            # "sourceName": "StateAir_Dhaka",
            # "sourceType": "government",
            # "mobile": false
            # --> stationmeta_core(id, station_id, station_name, station_latitude, station_longitude, station_altitude,
                                   #station_coordinate_status, station_coordinate_validation_date,
                                  #  station_coordinate_validator_id)

            # "attribution": [
            #    {"name": "EPA AirNow DOS",
            #     "url": "http://airnow.gov/index.cfm?action=airnow.global_summary"}],

            # --> provider(id, provider_name, provider_longname, provider_type, provider_country, provider_homepage,
            #                provider_city, provider_postcode, provider_street_address)
            # stationmeta_provider (id, provider_metadata, record_date, provider_id, station_id)
