import shutil

from mys3utils.object_list import FileBasedObjectList
from datetime import date


def test_load():
    kwargs = dict()
    kwargs['base_dir'] = './tests/'
    pfl = FileBasedObjectList(prefix='test', execution_date=date(2018, 6, 14), **kwargs)
    pfl.load()
    assert len(pfl.get_list()) == 91


def test_fetch():
    kwargs = dict()

    kwargs['base_dir'] = './tests/'
    shutil.rmtree('./tests/2018-07-21/')
    pfl = FileBasedObjectList(prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), **kwargs)
    pfl.load()
    pfl.store()
    assert len(pfl.get_list()) == 139

    pfl2 = FileBasedObjectList(prefix='realtime/2018-07-21/', execution_date=date(2018, 7, 21), **kwargs)
    pfl2.load()
    assert len(pfl.get_list()) == len(pfl2.get_list()) == 139
