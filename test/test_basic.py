# rerun --verbose --ignore=./test/.test_basic.py.swp "nosetests --nocapture"
import sys
sys.path.append('..')

import Indexing as IX
from unittest import TestCase

import os
from os.path import join as pjoin

import time
import tempfile
import random
import shutil
import faker

from nose.tools import \
        assert_equal

class FakeFS:

    def make_filler_file(self, basedir):
        with open(tempfile.mktemp(prefix=self.faker.sentence(), dir=basedir), 'w') as ofile:
            ofile.write('\n'.join(str(v) for v in [time.time(), random.random(), ofile.name]))
        return ofile.name

    def populate(self, verbose=False):
        for i in range(5):
            self.file_list.append(self.make_filler_file(self.BASEDIR))
        for i in range(2):
            somedir = tempfile.mkdtemp(dir=self.BASEDIR)
            for j in range(3):
                self.file_list.append(self.make_filler_file(somedir))

    def __init__(self, root):
        self.BASEDIR = tempfile.mkdtemp(dir=root, prefix='ixtest-')
        self.file_list = []
        self.faker = faker.Faker()

    def destroy(self):
        shutil.rmtree(self.BASEDIR)
        self.BASEDIR = None
        self.file_list = []

class TestIndexer(TestCase):

    def setUp(self):
        TMPDIR = '/Volumes/ramdisk'
        self.fs = FakeFS(TMPDIR)
        self.fs.populate()
        self.db_path = tempfile.mktemp(dir=TMPDIR)
        IX.init_db(self.db_path)
        self.ix = IX.Indexer(self.fs.BASEDIR)

    def test_file_total(self):
        assert_equal(len(self.fs.file_list), 5+2*3)

    def test_reindex(self):
        nproc = self.ix.reindex()
        assert_equal(len(self.fs.file_list), nproc)
        assert_equal(0, self.ix.reindex())

    def test_resync_db(self):
        self.ix.reindex()
        # delete a random file
        idx_delete = random.randint(0, len(self.fs.file_list)-1)
        to_delete = self.fs.file_list.pop(idx_delete)
        os.unlink(to_delete)
        status = self.ix.resync_db()
        assert_equal(len(status['del']), 1)
        deleted, _ = status['del'][0]
        assert_equal(deleted, to_delete)

    def tearDown(self):
        self.fs.destroy()
        os.unlink(self.db_path)


