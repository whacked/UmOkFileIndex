"""


"""

# TODO
# change to pyfs

from sqlalchemy.ext.declarative import declarative_base                                                                                      
from sqlalchemy.orm import relationship, backref, sessionmaker
import sqlalchemy as sqla

from glob import glob
from os.path import join as pjoin, split as psplit, splitext as psplitext, exists as pexists
import os, re
from collections import defaultdict

import hashlib

Base = declarative_base()
File__Tag = sqla.Table("tfile__ttag", Base.metadata,
        sqla.Column('tfile_id', sqla.Integer, sqla.ForeignKey('tfile.id')),
        sqla.Column('ttag_id', sqla.Integer, sqla.ForeignKey('ttag.id')),
        sqla.Column('rank', sqla.Float),
        )
db_sessionmaker = sessionmaker()

def init_db(DB_PATH):
    global db_session

    dsn_db = "sqlite:///%s" % DB_PATH
    db_engine = sqla.create_engine(dsn_db, echo=False)

    Base.metadata.create_all(db_engine)
    db_session = db_sessionmaker(bind = db_engine)

def utf8str(s):
    return unicode(s).encode("utf-8")

class Tag(Base):

    __tablename__ = 'ttag'

    id   = sqla.Column(sqla.Integer, primary_key=True)
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
        res = db_session.query(Tag).filter_by(text = text).first()
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

class File(Base):
    RELATIVE_BASE_DIR = None

    __tablename__ = 'tfile'

    id    = sqla.Column(sqla.Integer, primary_key=True)

    path  = sqla.Column(sqla.String, unique=True)
    sha1  = sqla.Column(sqla.String(40))
    size  = sqla.Column(sqla.Integer)
    mtime = sqla.Column(sqla.Float)

    is_invalid = None

    taglist = relationship('Tag', backref='filelist', secondary=File__Tag, lazy='dynamic')

    def get_realpath(self):
        if self.RELATIVE_BASE_DIR:
            return pjoin(self.RELATIVE_BASE_DIR, self.path)
        else:
            return self.path

    _is_invalid = None
    @property
    def is_invalid(self):
        if self._is_invalid is None:
            self._is_invalid = not pexists(self.get_realpath())
        return self._is_invalid

    def get_checksum(self):
        if self.is_invalid:
            return (None, None)
        content = open(self.get_realpath()).read()
        if not self.sha1:
            self.sha1 = hashlib.sha1(content).hexdigest()
        return (self.sha1)

    def __init__(self, path, derive_checksum = True, taglist = None):
        if self.RELATIVE_BASE_DIR:
            self.path = path.replace(self.RELATIVE_BASE_DIR, "", 1).lstrip(os.path.sep)
        else:
            self.path = path

        self.is_invalid = not pexists(self.get_realpath())
        if self.is_invalid:
            self.size = -1
            self.mtime = -1
        else:
            stat = os.stat(self.get_realpath())
            self.size = stat.st_size
            self.mtime = stat.st_mtime

        if derive_checksum:
            self.get_checksum()
        if taglist:
            self.taglist = [isinstance(item, Tag) and item or Tag(item) for item in taglist]

    def friendly_size(self):
        if self.is_invalid:
            return "N/A"
        for divisor, sizechar in (
                (1e9, "G"),
                (1e6, "M"),
                (1e3, "K"),
                (1e0, "B")):
            ## potentially bad?
            ## this is to avert size = 0 files. though, we should really just forbid empty files
            if self.size+1 >= divisor:
                return "%3d%s" % (self.size / divisor, sizechar)

    def __repr__(self):
        return "(%s) %s" % (self.friendly_size().rjust(4), self.path)

    def add_tag(self, *ltag):
        exclude = dict([(tag.id, True) for tag in self.taglist])
        self.taglist += [tag for tag in ltag if tag.id not in exclude]
        db_session.add(self)
        db_session.commit()

    def del_tag(self, *ltag):
        exclude = dict([(tag.id, True) for tag in ltag])
        self.taglist = [tagold for tagold in self.taglist if tagold.id not in exclude]
        db_session.add(self)
        db_session.commit()

    def is_match(self, fcheck):
        return all([getattr(self, attr) == getattr(fcheck, attr) for attr in ("size", "mtime", "path")])

    def open(self):
        print('open %s' % self.get_realpath())

    @staticmethod
    def get(**kw):
        return db_session.query(File).filter_by(**kw).first()

    OP_AND = 1
    OP_OR  = 2
    @staticmethod
    def findall(OP, ltoken):
        likecond = [Tag.text.like('%%%s%%' % token) for token in ltoken]
        if OP is File.OP_AND:
            qr = db_session.query(File).join((Tag, File.taglist)) \
                    .filter(sqla.or_(*likecond)) \
                    .group_by(File.id) \
                    .having(sqla.func.count(Tag.id) == len(likecond))
        elif OP is File.OP_OR:
            qr = db_session.query(File).filter(File.taglist.any(sqla.or_(*likecond)))
        else:
            raise Exception("unsupported operation: [%s]" % OP)
        return qr.all()

class Indexer:

    def __init__(self, BASE_DIR):
        self._BASE_DIR = os.path.expanduser(BASE_DIR)

    def reindex(self, verbose=True):
        """
        walk self._BASE_DIR and build the index into the database

        The "intelligent" reindexing right now just means that if the size,
        mtime, and filenames match, we assume they are not changed
        
        """

        ## preload the entire index
        dfile = dict([(f.path, f) for f in db_session.query(File).all()])
        ## preload tags
        dtag = dict([(t.text, f) for t in db_session.query(Tag).all()])
        def cachedTag(text):
            if text not in dtag:
                dtag[text] = Tag(text)
            return dtag[text]

        File.RELATIVE_BASE_DIR = self._BASE_DIR
        for basedir, lsubdir, lsubfile in os.walk(self._BASE_DIR):
            for subfile in lsubfile:
                f = File(pjoin(basedir, subfile), derive_checksum = False)
                if f.size == 0:
                    continue
                if f.path in dfile and f.is_match(dfile[f.path]):
                    continue
                f.get_checksum()

                ## add pathname tokens as tags
                basefilepath, ext = psplitext(f.path)
                f.taglist.extend([cachedTag(token.lower()) for token in re.split(r'\W+', basefilepath) + [ext[1:]] if len(token) > 1])
                if verbose:
                    print('ADDING %s ...' % (f))
                db_session.add(f)
        db_session.commit()

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

        rtn = defaultdict(list)
        for f_indb in db_session.query(File).all():
            f_infs = File(f_indb.path)  
            if f_infs.is_invalid:
                ## look for matching hash
                f_merge = db_session.query(File).filter_by(sha1 = f_infs.sha1).first()
                if f_merge:
                    f_merge.taglist.extend(f_indb.taglist)
                    db_session.add(f_merge)
                    rtn['mov'].append((f_indb.get_realpath(), f_merge.get_realpath()))
                else:
                    db_session.delete(f_indb)
                    rtn['del'].append((f_indb.get_realpath(), None))
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
    File.RELATIVE_BASE_DIR = args.basedir

    if args.reindex_complete:
        raise Exception("not completely implemented!")
    elif args.reindex_from_scratch:
        pexists(INDEXFILEPATH) and os.unlink(INDEXFILEPATH)
        raise Exception("not completely implemented!")
    elif args.reindex:
        print('reindexing')
        
    ## database check + initialization
    if args.use_fakedb:
        ## database definition
        DB_PATH = ":memory:"
    else:
        DB_PATH = INDEXFILEPATH
    init_db(DB_PATH)

    indexer = Indexer(args.basedir)

    if args.use_fakedb or not pexists(INDEXFILEPATH):
        ## TODO
        ## this is redundant
        indexer.reindex()

    if args.add:
        f = File(args.add[0], derive_checksum = True, taglist = args.add[1:])
        db_session.add(f)
        db_session.commit()

    def proc_tag_arglist(s):
        return " ".join(s).split(",")
    if args.tagmatchall:
        for f in File.findall(OP_AND, proc_tag_arglist(args.tagmatchall)):
            print(f)

    if args.tagmatchany:
        for f in File.findall(OP_OR, proc_tag_arglist(args.tagmatchany)):
            print(f)

    if args.dump:

        if args.dump == '-':
            ofile = sys.stdout
        else:
            ofile = open(args.dump, 'w')
        def printrow(*ls):
            ofile.write("\t".join(map(utf8str, ls)) + "\n")

        printrow("sha1","size","mtime","path","taglist")
        for f in db_session.query(File).all():
            printrow(f.sha1, f.size, f.mtime, f.path, '"%s"' % (",".join(sorted(map(utf8str, f.taglist)))))

        if args.dump != '-':
            ofile.close()
