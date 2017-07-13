from __future__ import print_function
import logging
import os
import re
import luigi
import requests
import json
import yaml
import time
import uuid
import codecs
import h5py as h5
import numpy as np
import datetime as dt
from jsondiff import diff
from random import shuffle
from bs4 import BeautifulSoup
from collections import namedtuple
from elasticsearch import Elasticsearch
#
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
try:
    from urllib.parse import urlparse as URL
except ImportError:
    from urlparse import urlparse as URL
#
import settings
import base

logger = logging.getLogger('luigi-interface')

Event = namedtuple('Event',['name','url','startDate','endDate'])

Event2 = namedtuple('Event2',['name','schedule','source_url','target_url','h5_dataset'])

Namespace = uuid.UUID('3e177116-ba63-4f17-9a73-9346c582d3b9')

class LoggingExample(object):
    _ns = 'logging-example'


class LoggingRaw(LoggingExample, base.Task):

    def output(self):
        return self._default_target('{ns}_raw_{dtstart}.yaml')

    def _test(self):
        with self.output().open('r') as f:
            for doc in yaml.load_all(f):
                yield doc

    def run(self):
        url = 'http://example.com'
        r = requests.get(url)
        if not r.ok:
            raise Exception('Status Code: %s' % r.status_code)
        soup = BeautifulSoup(r.text, 'html.parser')
        with self.output().open('w') as f:
            for event in soup.select('.eventon_list_event'):
                html = event.encode('utf-8')
                f.write('---\n')
                f.write('html: >\n')
                for ln in html.decode().split('\n'):
                    f.write(' '*4 + ln + '\n')
        test = [doc for doc in self._test()]


class LoggingEvents(LoggingExample, base.Task):

    def requires(self):
        yield LoggingRaw(dtstart=self.dtstart, force=self.force)

    def output(self):
        return self._default_target('{ns}_events_{dtstart}.yaml')

    def _events(self):
        for target in self.input():
            with target.open() as f:
                lst = []
                for doc in yaml.load_all(f):
                    data = {}
                    soup = BeautifulSoup(doc['html'], 'html.parser')
                    for i in soup.select('span[itemprop:name]'):
                        data['name'] = i.text.encode('utf8')
                    for i in soup.select('a[itemprop:url]'):
                        data['url'] = i.attrs['href']
                    for i in soup.select('time[itemprop:startDate]'):
                        data['startDate'] = i.attrs['datetime']
                    for i in soup.select('time[itemprop:endDate]'):
                        data['endDate'] = i.attrs['datetime']
                    if len(data) == len(Event._fields):
                        lst.append(data)
                shuffle(lst)
                for data in lst:
                    yield Event(**data)

    def run(self):
        fail = []
        with self.output().open('w') as out:
            for _n, event in enumerate(self._events()):
                logger.info(event.url)
                r = requests.get(event.url)
                if not r.ok:
                    raise Exception('Status Code: %s' % r.status_code)
                soup = BeautifulSoup(r.text, 'html.parser')
                href = None
                for i in soup.findAll('a',text=re.compile('View \& Bid')):
                    href = i.attrs['href']
                try:
                    if href:
                        out.write('---\n')
                        out.write('name: %s\n' % event.name.decode())
                        out.write('schedule: %s/P1D\n' % event.startDate)
                        out.write('source_url: %s\n' % event.url)
                        out.write('target_url: %s\n' % href)
                        url = URL(href)
                        loc = [re.sub('\.','_',url.hostname)]
                        loc.extend([i for i in url.path.split('/') if i])
                        name = '/'.join(loc)
                        out.write('h5_dataset: %s\n' % name)
                    else:
                        logger.error('MIA: %s' % event.url)
                except Exception as e:
                        fail.append((href, e))
        for href, e in fail:
            logger.error((href, e))


class LoggingIndex(LoggingExample, base.Task):

    def requires(self):
        yield LoggingEvents(dtstart=self.dtstart, force=self.force)

    def output(self):
        return self._default_target('{ns}_index_{dtstart}.json')

    def _events(self):
        for target in self.input():
            with target.open() as f:
                for doc in yaml.load_all(f):
                    try:
                        yield Event2(**doc)
                    except TypeError as e:
                        logger.error((doc, e))

    def run(self):
        with self.output().open('w') as f:
            tbl = {}
            for _n, event in enumerate(self._events()):
                _id = uuid.uuid3(Namespace, event.target_url)
                entry = {
                    'url': event.target_url,
                    'title': event.name,
                }
                if _id not in tbl:
                    tbl.update({str(_id): entry})
                else:
                    logger.error("Duplicate ID: {}".format(_id))
                    difference = diff(tbl[_id], entry, syntax='symmetric')
                    if difference:
                        logger.error(">> {}".format(difference))
            json.dump(tbl, f, indent=4)


class LoggingContent(LoggingExample, base.ContentTask):

    def requires(self):
        yield AuctionHqIndex(dtstart=self.dtstart)

    def output(self):
        return self._default_target('{ns}_content_{dtstart}.hdf')


class LoggingDelta(LoggingExample, base.DeltaTask):

    def output(self):
        return self._default_target('{ns}_delta_{dtstart}.json')

    def requires(self):
        prev = self.dtstart - dt.timedelta(days=1)
        yield LoggingIndex(dtstart=prev, force=False)
        yield LoggingIndex(dtstart=self.dtstart, force=settings.FORCE)


class LoggingUpdate(LoggingExample, base.UpdateTask):

    def requires(self):
        yield LoggingContent()

    def output(self):
        return self._default_target('{ns}_update_{dtstart}.yaml')

    def _docs(self):
        for target in self.input():
            with h5.File(target.path) as hdf:
                lst = set()
                hdf.visit(lambda x: lst.add(x) if isinstance(hdf.get(x),h5.Dataset) else None)
                for id in lst:
                    ds = hdf.get(id)
                    ds.attrs['_id'] = id
                    yield ds

    def run(self):
        with self.output().open('w') as f:
            for update in self._docs():
                pass
                # TODO: stuff

