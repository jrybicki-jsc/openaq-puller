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

    def _get_date(self):
        return self.execution_date

    def _retrieve(self):
        objs, _ = get_object_list(bucket_name=FETCHES_BUCKET, prefix=self.prefix)
        return objs

    def update(self):
        obj = self._retrieve().copy()
        ## do some magic (Key==>Name) with deep copy
        self.objects.clear()
        for a in obj:
            b= a.copy()
            b['Name'] = b.pop('Key')
            self.objects.append(b)
            
        ## and safe
        self._store()

    def _store(self):
        pass

    def load(self):
        pass

    def get_list(self):
        # deep copy?frozen?
        return self.objects

    def get_prefix(self):
        return self.prefix


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
            ## to defensive?
            logging.info(f'Old object list { fname } does not exist for some reason. Retrieving')
            self.update()
    
        self.objects.clear()
    
        with open(fname, 'r') as f:
            object_reader = csv.DictReader(f, fieldnames=['Name', 'Size', 'ETag', 'LastModified'])
            for obj in object_reader:
                obj['LastModified'] = parser.parse(obj['LastModified'])
                obj['Size'] = int(obj['Size'])
                self.objects.append(obj)

    def _store(self):
        fname = generate_fname(suffix='objects.csv', base_dir=self.base_dir,
                               execution_date=self.execution_date.strftime('%Y-%m-%d'))

        logging.info(f'Storing { len(self.objects)} objects in { fname}',)

        with open(fname, 'w+') as f:
            for o in self.objects:
                f.write(','.join([o['Name'], str(o['Size']), o['ETag'], o['LastModified'].isoformat()]) + '\n')
                
        return fname

