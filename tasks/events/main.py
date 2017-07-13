from __future__ import print_function
import logging
import os
import luigi
import json
import datetime as dt
#
import base
import updatedb
import settings

logger = logging.getLogger('luigi-interface')

class Main(object):
    _ns = 'main'

class Test(Main, base.Task):
    pass

class Backup(Main, base.Task):
    pass

class Clean(Main, base.Task):
    pass

class Content(Main, base.ContentTask):

    def requires(self):
        yield example.Logging(dtstart=self.dtstart)


class Delta(Main, base.DeltaTask):

    def requires(self):
        prev = self.dtstart - dt.timedelta(days=1)
        for ATask in [example.LoggingDelta,]:
            if self.cold:
                yield ATask(dtstart=self.dtstart, cold=self.cold, force=self.force)
            else:
                yield ATask(dtstart=self.dtstart, cold=self.cold, force=settings.FORCE)


class Update(Main, base.UpdateTask):

    def requires(self):
        yield example.LoggingContent(dtstart=self.dtstart)


class Index(Main, base.IndexTask):

    def requires(self):
        yield example.LoggingContent(dtstart=self.dtstart)


class UpdateIndex(Main, base.Command):

    def _command(self):
        return 'update_index'


class Rebuild(Main, base.Task):

    def requires(self):
        yield example.LoggingIndex()
        yield updatedb.UpdateDB()

    def output(self):
        target_file = os.path.join(settings.FS_DATA, 'manifest.yaml')
        return luigi.LocalTarget(target_file)

    def run(self):
        with self.output().open('w') as f:
            f.write('OK\n')
