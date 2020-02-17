# TODO
# change to pyfs
import hashlib
import os
import re
from collections import defaultdict
from datetime import datetime
from os.path import join as pjoin, splitext as psplitext, exists as pexists

import sqlalchemy as sqla
import stringcase
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()
db_sessionmaker = sessionmaker()

DEBUG_LEVEL = 0


def init_db(DB_PATH):
    global db_session

    dsn_db = "sqlite:///%s" % DB_PATH
    db_engine = sqla.create_engine(dsn_db, echo=DEBUG_LEVEL > 0)

    Base.metadata.create_all(db_engine)
    db_session = db_sessionmaker(bind=db_engine)

    algo = HashAlgorithm.ensure(name=Sha256Entry.NAME)
    algo.save()


class DefaultMixin(object):
    id = sqla.Column(sqla.Integer, primary_key=True)

    @declared_attr
    def __tablename__(cls):
        return stringcase.snakecase(cls.__name__)

    @classmethod
    def get(cls, **kw):
        return db_session.query(cls).filter_by(**kw).first()

    def save(self):
        db_session.add(self)
        db_session.commit()

    @classmethod
    def ensure(cls, **kw):
        existing = cls.get(**kw)
        if existing:
            return existing
        else:
            ensured = cls(**kw)
            ensured.save()
            return ensured


class HashAlgorithm(Base, DefaultMixin):
    name = sqla.Column(sqla.String, unique=True)


class Sha256Entry(Base, DefaultMixin):
    NAME = 'sha256'
    value = sqla.Column(sqla.String(64), unique=True)

    @classmethod
    def get_hash(cls, content):
        return hashlib.sha1(content).hexdigest()


class BlobEntryHash(Base, DefaultMixin):
    blob_id = sqla.Column(sqla.Integer, sqla.ForeignKey('blob_entry.id'))
    blob = relationship('BlobEntry', backref='hashes')
    hash_algorithm_id = sqla.Column(sqla.Integer, sqla.ForeignKey('hash_algorithm.id'))
    hash_algorithm = relationship('HashAlgorithm')
    time_created = sqla.Column(sqla.DateTime(), default=datetime.utcnow())
    hash_entry_id = sqla.Column(sqla.Integer)

    def get_hash(self):
        if self.hash_algorithm.name == Sha256Entry.NAME:
            return Sha256Entry.get(id=self.hash_entry_id)
        else:
            raise NotImplementedError

    def __repr__(self):
        return '<{}:{}> ({})'.format(
            self.__class__.__name__, self.hash_algorithm.name, self.id)


class LocalFilePathHistoryEntry(Base, DefaultMixin):
    RELATIVE_BASE_DIR = None
    # __tablename__ = 'local_file_path_history_entry'

    blob_id = sqla.Column(sqla.Integer, sqla.ForeignKey('blob_entry.id'), nullable=False)
    blob = relationship('BlobEntry')
    path = sqla.Column(sqla.String, unique=True)
    time_created = sqla.Column(sqla.DateTime(), default=datetime.utcnow())
    time_verified = sqla.Column(sqla.Float)
    # "exists" seems to conflict with reserved word
    file_exists = sqla.Column(sqla.Boolean)

    _is_valid = None

    def __repr__(self):
        return '<{}> ({}) {}'.format(
            self.__class__.__name__,
            self.id,
            self.path,
        )

    @classmethod
    def get_time_verified(cls, filepath):
        stat = os.stat(filepath)
        return stat.st_mtime

    @property
    def is_valid(self):
        if self._is_valid is None:
            self._is_valid = pexists(self.get_realpath())
        return self._is_valid

    def get_realpath(self):
        if self.RELATIVE_BASE_DIR:
            return pjoin(self.RELATIVE_BASE_DIR, self.path)
        else:
            return self.path

    def get_content(self):
        with open(self.path, 'rb') as ifile:
            return ifile.read()

    def is_match(self, fcheck):
        '''
        @type fcheck: LocalFilePathHistoryEntry
        '''
        return all(
            condition
            for condition in [
                self.path == fcheck.path,
                self.time_verified == fcheck.time_verified,
            ]
        )

    @classmethod
    def get_relpath(cls, path):
        return os.path.relpath(path, cls.RELATIVE_BASE_DIR)

    def __init__(self, **kw):
        super(LocalFilePathHistoryEntry, self).__init__(**kw)

        path = kw['path']
        if self.RELATIVE_BASE_DIR:
            self.path = self.__class__.get_relpath(path)
        else:
            self.path = path

        if self.is_valid:
            stat = os.stat(self.get_realpath())
            self.size = stat.st_size
            self.time_verified = stat.st_mtime
        else:
            self.size = -1
            self.time_verified = -1


class PosixFilePermissionEntry(Base, DefaultMixin):
    file_id = sqla.Column(sqla.Integer, sqla.ForeignKey('local_file_path_history_entry.id'))
    file = relationship('LocalFilePathHistoryEntry', backref='posix_file_permission')
    rwx = sqla.Column(sqla.Integer)


Blob__Tag = sqla.Table("blob__tag", Base.metadata,
                       sqla.Column('blob_entry_id', sqla.Integer, sqla.ForeignKey('blob_entry.id')),
                       sqla.Column('tag_id', sqla.Integer, sqla.ForeignKey('tag.id')),
                       sqla.Column('rank', sqla.Float))


class Tag(Base, DefaultMixin):
    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String, unique=True)

    _cache = {}

    @staticmethod
    def guaranteed_get(text):
        """
        return the tag called $text.
        if it doesn't exist, create it and return it
        
        """
        if text in Tag._cache:
            return Tag._cache[text]
        res = db_session.query(Tag).filter_by(text=text).first()
        if res is None:
            ## create it
            t = Tag(text)
            db_session.add(t)
            db_session.commit()
            Tag._cache[text] = t
        else:
            t = res
        return t

    def __init__(self, text):
        self.text = text

    def __repr__(self):
        return self.text


class BlobEntry(Base, DefaultMixin):
    id = sqla.Column(sqla.Integer, primary_key=True)

    size = sqla.Column(sqla.Integer)
    tags = relationship('Tag', backref='blobs', secondary=Blob__Tag, lazy='dynamic')

    def get_local_files(self):
        return db_session.query(LocalFilePathHistoryEntry).filter_by(
            blob_id=self.id,
            file_exists=True,
        ).order_by(LocalFilePathHistoryEntry.id.desc()).all()

    def get_content(self):
        for file in self.get_local_files():
            return file.get_content()

    def get_checksum(self):
        content = open(self.get_realpath(), 'rb').read()
        if not self.sha1:
            self.sha1 = hashlib.sha1(content).hexdigest()
        return (self.sha1)

    def friendly_size(self):
        for divisor, sizechar in (
                (1e9, "G"),
                (1e6, "M"),
                (1e3, "K"),
                (1e0, "B")):
            ## potentially bad?
            ## this is to avert size = 0 files. though, we should really just forbid empty files
            if self.size + 1 >= divisor:
                return "%3d%s" % (self.size / divisor, sizechar)

    def __repr__(self):
        local_files = self.get_local_files()
        first_filepath = local_files and local_files[0]  # type: LocalFilePathHistoryEntry
        return "(%s) @ %s (%s total)" % (
            self.friendly_size().rjust(4),
            first_filepath,
            len(local_files),
        )

    def add_tag(self, *ltag):
        exclude = dict([(tag.id, True) for tag in self.tags])
        self.tags += [tag for tag in ltag if tag.id not in exclude]
        db_session.add(self)
        db_session.commit()

    def del_tag(self, *ltag):
        exclude = dict((tag.id, True) for tag in ltag)
        self.tags = [tagold for tagold in self.tags if tagold.id not in exclude]
        db_session.add(self)
        db_session.commit()

    def open(self):
        print('open %s' % self.get_realpath())

    OP_AND = 1
    OP_OR = 2

    @staticmethod
    def findall(OP, ltoken):
        if isinstance(ltoken, str):
            ltoken = [ltoken]
        likecond = [Tag.text.like('%%%s%%' % token) for token in ltoken]
        if OP is BlobEntry.OP_AND:
            qr = db_session.query(BlobEntry).join((Tag, BlobEntry.tags)) \
                .filter(sqla.or_(*likecond)) \
                .group_by(BlobEntry.id) \
                .having(sqla.func.count(Tag.id) == len(likecond))
        elif OP is BlobEntry.OP_OR:
            qr = db_session.query(BlobEntry).filter(BlobEntry.tags.any(sqla.or_(*likecond)))
        else:
            raise Exception("unsupported operation: [%s]" % OP)
        return qr.all()

    def __init__(self, tags=None, **kw):
        super(BlobEntry, self).__init__(**kw)
        if tags:
            self.tags = [isinstance(item, Tag) and item or Tag(item) for item in tags]


class Indexer:

    def __init__(self, BASE_DIR):
        self._BASE_DIR = os.path.expanduser(BASE_DIR)
        self.default_hash_algo = HashAlgorithm.get(name=Sha256Entry.NAME)
        self.reload_cache()

    def reload_cache(self):
        ## preload the entire index
        self.dfile = dict((f.path, f) for f in db_session.query(LocalFilePathHistoryEntry).all())
        ## preload tags
        self.dtag = dict((t.text, t) for t in db_session.query(Tag).all())

    def add_file(self, filepath, tags=None, verbose=False):
        relpath = LocalFilePathHistoryEntry.get_relpath(filepath)
        f_existing = LocalFilePathHistoryEntry.get(path=relpath)
        if f_existing is not None:
            file = f_existing
        else:
            file = LocalFilePathHistoryEntry(
                path=relpath, file_exists=pexists(relpath))
        # skip already processed files
        if file.path in self.dfile and file.is_match(self.dfile[file.path]):
            return
        else:
            self.dfile[file.path] = file

        content = file.get_content()
        hash = Sha256Entry.get_hash(content)
        chk = Sha256Entry.ensure(value=hash)
        blob = BlobEntry.ensure(size=len(content))
        if tags:
            blob.tags.extend(tags)
        BlobEntryHash(
            blob_id=blob.id,
            hash_algorithm_id=self.default_hash_algo.id,
            hash_entry_id=chk.id,
        ).save()

        file.blob = blob
        if verbose:
            print('ADDING %s ...' % (file))
        file.save()

    def reindex(self, verbose=False):
        """
        walk self._BASE_DIR and build the index into the database

        The "intelligent" reindexing right now just means that if the size,
        time_verified, and filenames match, we assume they are not changed
        
        returns number of files processed from reindexing
        """
        self.reload_cache()

        def cachedTag(text):
            if text not in self.dtag:
                self.dtag[text] = Tag(text)
            return self.dtag[text]

        # input('press to start.')
        total_processed = 0
        BlobEntry.RELATIVE_BASE_DIR = self._BASE_DIR
        for basedir, lsubdir, lsubfile in os.walk(self._BASE_DIR):
            for subfile in lsubfile:
                filepath = pjoin(basedir, subfile)
                ## add pathname tokens as tags
                basefilepath, ext = psplitext(filepath)
                tags = [cachedTag(token.lower())
                        for token in re.split(r'\W+', basefilepath) + [ext[1:]]
                        if len(token) > 1]
                self.add_file(filepath, tags=tags, verbose=verbose)
                total_processed += 1
        db_session.commit()

        return total_processed

    def resync_db(self):
        """
        verify all entries in the database have not changed on the filesystem.

        If attribute changes are detected, overwrite the value in the db

        If the file is no longer found, in order to preserve work, look for the
        first matched file with a matching hash, rebind all tags to that file,
        and delete the entry

        If no matches are found, assume the file is gone and just delete the
        entry
        
        """

        self.reindex()

        sha256model = HashAlgorithm.get(name=Sha256Entry.NAME)
        rtn = defaultdict(list)
        for blob_indb in db_session.query(BlobEntry).all():  # type: BlobEntry
            for f_infs in blob_indb.get_local_files():  # type:LocalFilePathHistoryEntry
                if not f_infs.is_valid:
                    ## look for matching hash
                    hash = Sha256Entry.get_hash(f_infs.get_content())
                    hash_existing = BlobEntryHash.get(
                        hash_algorithm_id=sha256model.id,
                        hash_entry_id=hash.id,
                    )
                    blob_merge = BlobEntry.get(hash_entry_id=hash_existing.id)
                    if blob_merge:
                        blob_merge.tags.extend(blob_indb.taglist)
                        db_session.add(blob_merge)
                        rtn['mov'].append((blob_indb.get_realpath(), blob_merge.get_realpath()))
                    else:
                        db_session.delete(blob_indb)
                        rtn['del'].append((blob_indb.get_realpath(), None))

            ## NOTE:
            ## since we call self.reindex() above, and reindex walks the tree,
            ## this branch is expected to never run, but we kind of like to see
            ## rtn have DEL, MOV, and also ADD
            # else:
            #     if f_infs.is_match(f_indb):
            #         continue
            #     else:
            #         db_session.add(f_infs)
            #         rtn['add'].append((None, f_indb.get_realpath()))
        return rtn


if __name__ == "__main__":

    import sys
    import argparse

    parser = argparse.ArgumentParser(description='blah blah')

    parser.add_argument('--basedir', nargs='?', default=".",
                        help='base directory to index and treat as root path')

    parser.add_argument('--reindex', action='store_true',
                        help='rebuild index using default "intelligent" method')
    parser.add_argument('--reindex_complete', action='store_true',
                        help='rebuild index, forcing revisit of all files in file tree')
    parser.add_argument('--reindex_from_scratch', action='store_true',
                        help='rebuild index from scratch. equivalent to deleting the index file then indexing')

    parser.add_argument('--tagmatchany', nargs="+",
                        help='list all entries matching any of the given tags (COMMA separated)')
    parser.add_argument('--tagmatchall', nargs="+",
                        help='list all entries matching all given tags (COMMA separated)')
    parser.add_argument('--add', nargs="+",
                        help='dump list of all stored data in TSV compatible format to STDOUT')
    parser.add_argument('--dump', nargs="?", const='-',
                        help='dump list of all stored data in TSV compatible format to FILE if given, else STDOUT')

    parser.add_argument('--use_fakedb', action='store_true',
                        help='use in-memory database')

    args = parser.parse_args()

    INDEXFILEPATH = "_index.db"
    LocalFilePathHistoryEntry.RELATIVE_BASE_DIR = args.basedir

    ## database check + initialization
    if args.use_fakedb:
        ## database definition
        DB_PATH = ":memory:"
    else:
        DB_PATH = INDEXFILEPATH
    init_db(DB_PATH)

    indexer = Indexer(args.basedir)

    if not args.use_fakedb:
        if args.reindex_complete:
            raise Exception("not completely implemented!")
        elif args.reindex_from_scratch:
            pexists(INDEXFILEPATH) and os.unlink(INDEXFILEPATH)
            raise Exception("not completely implemented!")
        elif args.reindex:
            print('reindexing')

    if args.use_fakedb or not pexists(INDEXFILEPATH):
        ## TODO
        ## this is redundant
        indexer.reindex()

    if args.add:
        indexer.add_file(args.add[0], tags=args.add[1:])
        db_session.commit()


    def proc_tag_arglist(s):
        return " ".join(s).split(",")


    if args.tagmatchall:
        for f in BlobEntry.findall(BlobEntry.OP_AND, proc_tag_arglist(args.tagmatchall)):
            print(f)

    if args.tagmatchany:
        for f in BlobEntry.findall(BlobEntry.OP_OR, proc_tag_arglist(args.tagmatchany)):
            print(f)

    if args.dump:

        if args.dump == '-':
            ofile = sys.stdout
        else:
            ofile = open(args.dump, 'w')


        def printrow(*ls):
            ofile.write("\t".join(map(str, ls)) + "\n")


        printrow("sha1", "size", "time_verified", "path", "taglist")
        for b in db_session.query(BlobEntry).all():
            printrow(b.sha1, b.size, b.time_verified, b.path, '"%s"' % (",".join(sorted(map(str, b.taglist)))))

        if args.dump != '-':
            ofile.close()
