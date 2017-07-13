from __future__ import print_function
import os
import luigi
import requests
import h5py as h5
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth

import base
import settings
import example


class Manifest(base.Task):

    def output(self):
        return self._default_target('db_manifest_{dtstart}.yaml')


class CleanDB(base.Task):

    force = luigi.BoolParameter(significant=False, default=False)

    def output(self):
        return self._default_target('db_backup_{dtstart}.yaml')

    def run(self):
        events_api = "{0}/events/".format(settings.MGMT_API.strip('/'))
        api_auth = (settings.MGMT_API_USER, settings.MGMT_API_PASS)
        for _n, doc in enumerate(self._docs()):
            val = doc.value.tobytes().decode('utf-8','ignore')
            soup = BeautifulSoup(val, 'html.parser')
            body = soup.find('body')
            if body:
                data = {
                    'title': doc.attrs['title'],
                    'url': doc.attrs['target_url'],
                    'start_date': '2016-12-25',
                    'end_date': '2016-12-25',
                    'where': 'nowhere',
                    'what': body.text,
                }
                #res = es.index(index=settings.ES_INDEX, doc_type='auction', id=doc.attrs['_id'], body=data)
                rsp = requests.post(events_api, auth=api_auth, data=data)
                if not rsp.ok:
                    print(rsp.text)
                    break


class UpdateDB(luigi.Task):

    force = luigi.BoolParameter(significant=False, default=False)

    def requires(self):
        yield example.LoggingContent()

    def output(self):
        target_file = os.path.join(settings.FS_DATA, 'db_update.yaml')
        return luigi.LocalTarget(target_file)

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        # To force execution, we just remove all outputs before complete() is called
        if self.force is True:
            outputs = luigi.task.flatten(self.output())
            for out in outputs:
                if out.exists():
                    os.remove(self.output().path)

    def _docs(self):
        for input in self.input():
            with h5.File(input.path) as hdf:
                lst = set()
                hdf.visit(lambda x: lst.add(x) if isinstance(hdf.get(x),h5.Dataset) else None)
                for id in lst:
                    ds = hdf.get(id)
                    ds.attrs['_id'] = id
                    yield ds

    def run(self):
        events_api = "{0}/events/".format(settings.MGMT_API.strip('/'))
        api_auth = (settings.MGMT_API_USER, settings.MGMT_API_PASS)
        for _n, doc in enumerate(self._docs()):
            val = doc.value.tobytes().decode('utf-8','ignore')
            soup = BeautifulSoup(val, 'html.parser')
            body = soup.find('body')
            if body:
                data = {
                    'title': doc.attrs['title'],
                    'url': doc.attrs['target_url'],
                    'start_date': '2017-06-01',
                    'end_date': '2017-07-01',
                    'where': 'nowhere',
                    'what': body.text,
                }
                #res = es.index(index=settings.ES_INDEX, doc_type='auction', id=doc.attrs['_id'], body=data)
                rsp = requests.post(events_api, auth=api_auth, data=data)
                if not rsp.ok:
                    print(rsp.text)
                    break
