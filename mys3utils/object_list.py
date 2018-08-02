import csv
import os
import logging

from mys3utils.tools import get_object_list, FETCHES_BUCKET, serialize_object
from dateutil import parser


class ObjectList(object):
    def __init__(self, prefix, execution_date, *args, **kwargs):
        self.execution_date = execution_date
        self.prefix = prefix
        self.objects = list()

    def get_date(self):
        return self.execution_date

    def refresh(self):
        pass

    def store(self):
        pass

    def get_list(self):
        # deep copy?frozen?
        return self.objects


def generate_fname(suffix, base_dir, execution_date):
    fname = os.path.join(base_dir, execution_date)
    os.makedirs(fname, exist_ok=True)
    fname = os.path.join(fname, suffix)
    return fname


class FileBasedObjectList(ObjectList):
    def __init__(self, prefix, execution_date, *args, **kwargs):
        super().__init__(prefix, execution_date, *args, **kwargs)
        self.base_dir = kwargs['base_dir']

    def load(self):
        fname = generate_fname(suffix='objects.csv', base_dir=self.base_dir,
                               execution_date=self.execution_date.strftime('%Y-%m-%d'))

        if not os.path.isfile(fname):
            logging.info('Old object list %s does not exist for some reason. Regenerating', fname)
            self.refresh()
        else:
            with open(fname, 'r') as f:
                object_reader = csv.DictReader(f, fieldnames=['Name', 'Size', 'ETag', 'LastModified'])
                for obj in object_reader:
                    obj['LastModified'] = parser.parse(obj['LastModified'])
                    obj['Size'] = int(obj['Size'])
                    self.objects.append(obj)

    def store(self):
        fname = generate_fname(suffix='objects.csv', base_dir=self.base_dir,
                               execution_date=self.execution_date.strftime('%Y-%m-%d'))

        logging.info('Storing %d objects in %s', len(self.objects), fname)

        with open(fname, 'w+') as f:
            for o in self.objects:
                f.write(serialize_object(o))

        return fname

    def refresh(self):
        self.objects, _ = get_object_list(bucket_name=FETCHES_BUCKET, prefix=self.prefix)
