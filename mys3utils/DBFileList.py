import logging

from .object_list import ObjectList

from sqlalchemy import Table, MetaData, Column, Integer, String, ForeignKey, Sequence, DateTime, select, create_engine
from sqlalchemy.sql import and_

metadata = MetaData()

checks = Table('prefix_checks', metadata,
               Column('id', Integer, Sequence('prefix_id_seq'), primary_key=True),
               Column('prefix', String(128)),
               Column('date', DateTime),
               keep_existing=True,
               )

objects = Table('object', metadata,
                Column('prefix_check', None, ForeignKey('prefix_checks.id')),
                Column('name', String(128)),
                Column('size', Integer),
                Column('checksum', String(32)),
                Column('created', DateTime),
                keep_existing=True,
                )


class DBBasedObjectList(ObjectList):
    def __init__(self, prefix, execution_date, *args, **kwargs):
        super().__init__(prefix, execution_date, *args, **kwargs)
        self.engine = kwargs['engine']
        self.create_db()
        self.check_id = None

    def create_db(self):
        metadata.create_all(self.engine)

    def get_check(self):
        s = select([checks.c.id]).where(and_(self.prefix == checks.c.prefix, self.execution_date == checks.c.date))

        res = self.engine.execute(s)
        if res is not None:
            first = res.first()
            logging.info('Got check! %r' % first)
            return first

        logging.info('No such check')
        return None

    def create_check(self):
        ins = checks.insert().values(
            prefix=self.prefix,
            date=self.execution_date)

        res = self.engine.execute(ins)
        last_id = res.inserted_primary_key[0]
        res.close()
        return last_id

    def load(self):
        if self.check_id is None:
            self.check_id = self.get_check()

        if self.check_id is None:
                logging.info('No such check. Refreshing')
                self.check_id = self.create_check()
                self.objects = self.retrieve()
        else:
            s = select([objects]).where(self.check_id == objects.c.prefix_check)
            all = self.engine.execute(s).fetchall()
            self.objects = []
            for r in all:
                self.objects.append({'Key': r[1], 'Size': r[2], 'ETag': r[3], 'LastModified': r[4]})

    def store(self):
        logging.info('Storing %d objects in %s', len(self.objects), 'dbname')
        ## is it a new check?
        if self.check_id is None:
            logging.info('No such check....')
            self.check_id = self.create_check()
        else:
            logging.info('This is not a new check. Removing already stored objects')
            stmt = objects.delete().where(self.check_id == objects.c.prefix_check)
            res = self.engine.execute(stmt)

        for o in self.objects:
            ins = objects.insert().values(
                prefix_check=self.check_id,
                name=o['Key'],
                size=o['Size'],
                checksum=o['ETag'],
                created=o['LastModified'])

            res = self.engine.execute(ins)


def get_engine():
    return create_engine('sqlite:///:memory:', echo=True)
