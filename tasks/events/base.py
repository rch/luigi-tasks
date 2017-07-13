from __future__ import print_function
import logging
import os
import re
import luigi
import json
import yaml
import pytz
import snappy
import requests
import h5py as h5
import numpy as np
import datetime as dt
from jsondiff import diff
from hashlib import sha1
from collections import namedtuple
from bs4 import BeautifulSoup
from luigi.contrib.external_program import ExternalProgramTask
#
import settings

logger = logging.getLogger('luigi-interface')

try:
    class folded(unicode): pass
except NameError:
    class folded(str): pass


def folded_unicode_representer(dumper, data):
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', data, style='>')


yaml.add_representer(folded, folded_unicode_representer)


class Base(object):

    _ns = 'base'

    empty = luigi.BoolParameter(significant=False, default=False)

    force = luigi.BoolParameter(significant=False, default=settings.FORCE)

    dtstart = luigi.DateParameter(
        default=dt.datetime.utcnow().replace(tzinfo=pytz.utc)
    )

    def _default_target(self, fmt='{ns}_{dtstart}.yaml'):
        filename = fmt.format(
            ns = self._ns,
            dtstart = self.dtstart.isoformat()
        )
        if self._ns:
            filename = os.path.join(self._ns, filename)
        target_file = os.path.join(settings.FS_DATA, filename)
        return luigi.LocalTarget(target_file)

    def output(self):
        return self._default_target()

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        # To force execution, we just remove all outputs before complete() is called
        if self.force is True:
            outputs = luigi.task.flatten(self.output())
            for out in outputs:
                if out.exists():
                    os.remove(self.output().path)

    def run(self):
        if self.empty:
            with self.output().open('w') as f:
                json.dump({}, f)
        else:
            with self.output().open('w') as f:
                json.dump({'status':'ok'}, f)


class Task(luigi.Task, Base):
    pass


class DeltaTask(Task):

    cold = luigi.BoolParameter(significant=False, default=False)

    def output(self):
        return self._default_target('{ns}_delta_{dtstart}.json')

    def requires(self):
        prev = self.dtstart - dt.timedelta(days=1)
        if not self.cold:
            yield Task(dtstart=prev, force=False)
            yield Task(dtstart=self.dtstart, force=self.force)
        else:
            yield Task(dtstart=prev, force=False, empty=True)
            yield Task(dtstart=self.dtstart, force=self.force)

    def run(self):
        data = {}
        prev, curr = sorted(self.input(), key=lambda i: i.path)
        for target in [prev, curr]:
            logger.info(target.path)
            with target.open() as f:
                data[target] = json.load(f)
        with self.output().open('w') as f:
            if self.cold:
                dx = {
                    "$insert": data[curr]
                }
            else:
                dx = json.loads(
                    diff(data[prev], data[curr], syntax='explicit', dump=True)
                )
            json.dump(dx, f, indent=4)


class ContentTask(Task):

    limit = luigi.TimeDeltaParameter(default=settings.TIME_LIMIT)

    def requires(self):
        yield Task(dtstart=self.dtstart)

    def output(self):
        return self._default_target('{ns}_content_{dtstart}.hdf')

    def _channel(self):
        for channel in self.input():
            with channel.open() as f:
                try:
                    for msg_id, msg in json.load(f).items():
                        yield self._message(msg_id, msg)
                except Exception as e:
                    logging.error(e, msg_id, msg)

    def _message(self, _id, _msg):
        # override to produce typed messages
        Message = namedtuple('Message', _msg.keys())
        return _id, Message(**_msg)

    def _githash(self, data):
        s = sha1()
        lede = "blob {}\0".format(len(data))
        s.update(lede.encode())
        s.update(data)
        return s.hexdigest()

    def _create_dataset(self, hdf, dataset_id, content):
        try:
            ds = hdf.create_dataset(
                dataset_id,
                data=np.void(
                    snappy.compress(content)
                )
            )
            ds.attrs['x_compression'] = 'snappy'
        except RuntimeError as e:
            logger.error("Could not create dataset: %s" % dataset_id)
            ds = None
        return ds

    def run(self):
        start_time = dt.datetime.now()
        with h5.File(self.output().path,'w') as hdf:
            for message_id, message in self._channel():
                logger.info('> %s' % message.url)
                try:
                    r = requests.get(message.url, stream=True)
                except requests.exceptions.TooManyRedirects as e:
                    logger.error('%s: %s' % (self.__class__, e))
                except requests.exceptions.ConnectionError as e:
                    logger.error('%s: %s' % (self.__class__, e))
                if r.ok:
                    content = r.raw.data
                    content_id = self._githash(content)
                    r.raw.decode_content = True
                    dataset_id = "{}/{}".format(content_id[:2], content_id[2:])
                    ds = self._create_dataset(hdf, dataset_id, content)
                    if ds:
                        ds.attrs['content_id'] = content_id
                        ds.attrs['message_id'] = message_id
                        for field, value in zip(message._fields, message):
                            ds.attrs[field] = value
                        hdf.flush()
                tdelt = dt.datetime.now() - start_time
                if self.limit and tdelt > self.limit:
                    logger.warn('Time limit exceeded.')
                    break


class IndexTask(Task):

    api_url = luigi.Parameter(default=settings.MGMT_API)

    def requires(self):
        logger.warn('Running index-task without content.')
        yield ContentTask()

    def output(self):
        return self._default_target('{ns}_index_{dtstart}.hdf')

    def _message(self, _id, _msg):
        # override to produce typed messages
        if isinstance(_msg, h5.Dataset):
            keys = [k for k in _msg.attrs.keys()
                if not k.startswith('x_')] + ['content']
            Message = namedtuple('Message', keys)
            msg = {k:v for k,v in dict(_msg.attrs).items()
                if not k.startswith('x_')
            }
            if _msg.attrs.get('x_compression', '') == 'snappy':
                content = snappy.decompress(_msg.value.tobytes()).decode('utf-8','ignore')
            else:
                content = _msg.value.tobytes().decode('utf-8','ignore')
            msg.update({
                'content': content
            })
            return _id, Message(**msg)
        else:
            Message = namedtuple('Message', _msg.keys())
            return _id, Message(**_msg)

    def _channel(self):
        for channel in self.input():
            with h5.File(channel.path) as hdf:
                ids = set()
                try:
                    hdf.visit(lambda _id: ids.add(_id)
                        if isinstance(hdf.get(_id), h5.Dataset) else None
                    )
                    for _id in sorted(ids):
                        yield self._message(_id, hdf.get(_id))
                except Exception as e:
                    logging.error('%s : %s' % (self.__class__, e))

    def _create_dataset(self, hdf, dataset_id, content):
        try:
            ds = hdf.create_dataset(
                dataset_id,
                data=np.void(content)
            )
            #ds.attrs['compression'] = 'snappy'
        except RuntimeError as e:
            logger.error("Could not create dataset: %s" % dataset_id)
            ds = None
        return ds

    def _visible(self, element):
        if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
            return False
        elif re.match('<!--.*-->', str(element)):
            return False
        return True

    def run(self):
        with h5.File(self.output().path,'w') as hdf:
            for message_id, message in self._channel():
                logger.info("%s -- %s" % (message_id, message.url))
                soup = BeautifulSoup(message.content, 'html.parser')
                body = soup.find('body')
                index_content = []
                if body:
                    results = body.findAll(text=True)
                    if results:
                        _z = len(str(len(results)))
                        for _n, result in enumerate(results):
                            if self._visible(result):
                                content = str(result).encode()
                                text = ' '.join(content.decode().split())
                                if text:
                                    self._create_dataset(hdf, message_id + '-%s' % str(_n).zfill(_z), text.encode())
                                    index_content.append(text)
                if index_content:
                    data = dict(
                        tag = '%s/latest' % self._ns,
                        url = message.url,
                        content = ' '.join(index_content)
                    )
                    resources_url = self.api_url + 'resources/'
                    headers = {'Authorization': 'Token {}'.format(settings.MGMT_API_TOKEN)}
                    result = requests.post(resources_url, data=data, headers=headers)
                    if not result.ok:
                        logger.error("%s -- %s" % (resources_url, result.status_code))
                        raise Exception('Indexing Error')




class RecursiveTask(Task):

    force = luigi.BoolParameter(significant=False, default=settings.FORCE)

    dtstart = luigi.DateParameter(
        default=dt.datetime.utcnow().replace(tzinfo=pytz.utc)
    )

    def requires(self):
        now = dt.date.today()
        delta = dt.timedelta(days=1)
        prev = self.dtstart - delta
        if prev > (now - delta):
            yield self.__class__(dtstart=prev, force=False)

    def output(self):
        return self._default_target()

    def run(self):
        with self.output().open('w') as f:
            f.write('OK\n')


class UpdateTask(Task):

    def requires(self):
        logger.warn('Running update-task without content.')
        yield ContentTask()

    def output(self):
        return self._default_target('{ns}_update_{dtstart}.json')

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
        with self.output().open('w') as f:
            for update in self._docs():
                f.write('{}\n'.format(update))


class Command(ExternalProgramTask, Base):

    def command(self):
        return 'check'

    def _django_management(self):
        return [
            os.path.join(settings.VIRTUAL_ENVIRONMENT, 'bin','python'),
            os.path.join(settings.DJANGO_APPLICATION, 'mgmt','manage.py'),
        ]

    def program_args(self):
        lst = self._django_management()
        lst.append(self._command())
        return lst
