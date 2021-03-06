import wgdb
import types
import time
import os
import ptbd_util
import simplejson



__version__="0.1"
__author__="Vladimir Ulogov"

def find_files_in_dir(_dir, patt):
    import fnmatch
    file_names = [fn for fn in os.listdir(_dir)
                  if fnmatch.fnmatch(fn, patt )]
    file_names.sort()
    return file_names


class DBError(Exception):
    def __init__(self, db, *msg, **kw):
        self.db = db
        self.msg = msg
        self.kw = kw
class SchemaError(Exception):
    def __init__(self, db, *msg, **kw):
        self.db = db
        self.msg = msg
        self.kw = kw
class QueryError(Exception):
    def __init__(self, db, *msg, **kw):
        self.db = db
        self.msg = msg
        self.kw = kw

class RECORD:
    def __init__(self, db, rec):
        if not db.ready:
            raise StopIteration()
        self.db = db
        self.rec = rec
    def __getitem__(self, key):
        if key not in self.db.attrs.keys():
            raise KeyError,key
        try:
            self.db.begin()
            res = wgdb.get_field(self.db.db, self.rec, self.db.attrs[key])
            self.db.commit()
            return res
        except KeyboardInterrupt:
            raise DBError(self.db, "Can not read %s[%s]"%(self.db.ID(),key))
    def __setitem__(self, key, value):
        if key not in self.db.attrs.keys():
            raise KeyError,key
        try:
            self.db.begin(True)
            res = wgdb.set_field(self.db.db, self.rec, self.db.attrs[key], value)
            self.db.commit()
            return res
        except:
            raise DBError(self.db, "Can not set %s[%s]" % (self.db.ID(), key))
    def delete(self):
        try:
            print "Boo",self.dv.ID()
            self.db.begin(True)
            wgdb.delete_record(self.db.db, self.rec)
            self.db.commit()
        except:
            self.db.commit()
            raise DBError(self.db, "Can not delete in %s" % self.db.ID())


class CURSOR:
    def __init__(self, db, rec):
        if not db.ready:
            raise StopIteration()
        self.db = db
        self.rec = rec
    def __iter__(self):
        return self
    def next(self):
        if self.rec != None:
            rec = RECORD(self, self.rec)
        else:
            raise StopIteration()
        try:
            self.db.begin()
            self.rec = wgdb.get_next_record(self.db, self.rec)
            self.db.commit()
        except:
            self.db.commit()
            self.rec = None
            raise StopIteration()
        return rec

class QUERY(CURSOR):
    def __init__(self, db, rec, q):
        CURSOR.__init__(self, db, rec)
        self.q = q
    def next(self):
        if self.rec != None:
            rec = RECORD(self, self.rec)
        else:
            wgdb.free_qauery(self.db, self.q)
            raise StopIteration()
        try:
            self.db.begin()
            self.rec = wgdb.fetch(self.db, self.rec)
            self.db.commit()
        except:
            self.rec = None
            wgdb.free_query(self.db.db, self.q)
            raise StopIteration()
        return rec




class DB:
    def __init__(self, *schema, **kw):
        if not kw.has_key("id"):
            raise DBError(self, "Database constructor do not have database ID")
        if kw.has_key("schema"):
            self.schema = kw["schema"]
        else:
            self.schema = schema
        if kw.has_key("name"):
            self.name = kw["name"]
        else:
            self.name = str(kw["id"])
        self.is_restore = False
        if kw.has_key("restore"):
            self.is_restore = kw["restore"]
        self.tmpdir = "/tmp"
        self.storedir = "/tmp"
        if kw.has_key("tmpdir"):
            self.is_restore = kw["tmpdir"]
        if kw.has_key("storedir"):
            self.is_restore = kw["storedir"]
        self.kw = kw
        self.ready = False
        self.db = None
        self.is_read = 0
        self.is_write = 0
        self.journal_stamp = 0
        self.journal_lock = 0
        self.__check_schema()
        self.__open()
        self.__createindexes()
        if self.is_restore == True:
            self.__restore()
    def __check_schema(self):
        self.attrs = {}
        c = 0
        for i in self.schema:
            if len(i) != 3:
                raise SchemaError(self, "Schema element is not valid", element=i)
            if types.StringType != type(i[0]):
                raise SchemaError(self, "Column name isn't string", element=i)
            if i[1] not in [True, False]:
                raise SchemaError(self, "Schema Indexing parameter is not valid", element=i)
            self.attrs[i[0]] = c
            c += 1
    def __open(self):
        if self.ready:
            return True
        try:
            self.db = wgdb.attach_existing_database(self.ID())
            self.ready = True
        except:
            if not self.kw.has_key("size"):
                raise DBError(self, "Request for a new database %s but no size provided"%self.ID())
            if type(self.kw["size"]) == types.IntType:
                self.size = self.kw["size"]
            elif type(self.kw["size"]) == types.StringType:
                try:
                    self.size = int(self.kw["size"])
                except:
                    try:
                        self.size = ptbd_util.human2bytes(self.kw["size"])
                    except:
                        raise DBError(self, "Request for a new database %s but size have incorrect format" % self.ID())
            try:
                self.db = wgdb.attach_database(shmname=self.ID(), size=self.size)
                self.ready = True
            except:
                raise DBError(self, "Error creating memory segment with key: %s size %d"%(self.ID(), self.kw["size"]))
        return True
    def __createindexes(self):
        if not self.ready:
            return False
        c = 0
        for i in self.schema:
            if i[1] == True:
                self.begin()
                idx = wgdb.indexid(self.db, c)
                self.commit()
                if idx != -1:
                    continue
                try:
                    self.begin(True)
                    res = wgdb.createindex(self.db, c)
                    self.commit()
                    if not res:
                        raise DBError(self)
                except:
                    raise DBError(self, "Error creating index for %s[%d]"%(self.ID(),c))
            c += 1
    def __restore(self):
        _d = self.dumps()
        if len(_d) != 0:
            wgdb.load(self.db, "%s/%s"%(self.storedir, _d[-1]))
        _j = self.journals()
        for j in _j:
            wgdb.replay_log(self.db, "%s/%s"%(self.tmpdir, j))
    def ID(self):
        return self.kw["id"]
    def close(self):
        if not self.ready:
            return False
        self.commit()
        try:
            wgdb.detach_database(self.db)
        except:
            raise DBError(self, "Can not close memory segment %s"%self.ID())
        return True
    def drop(self):
        if self.ready:
            if not self.close():
                return False
        try:
            wgdb.delete_database(self.kw["id"])
        except:
            raise DBError(self, "Can not free memory segment %s"%self.ID())
        return True
    def insert(self, **attrs):
        if not self.ready:
            return False
        for k in attrs.keys():
            if k not in self.attrs.keys():
                raise DBError(self, "Attempt to INSERT missed attribute %s"%k)
        try:
            self.begin(True)
            _rec = wgdb.create_record(self.db, len(self.schema))
            self.commit()
        except:
            raise DBError(self, "Can not create record for %s"%self.ID())
        c = 0
        try:
            for i in self.schema:
                self.begin(True)
                wgdb.set_field(self.db, _rec, c, i[2])
                self.commit()
                c += 1
            for k in attrs.keys():
                self.begin(True)
                wgdb.set_field(self.db, _rec, self.attrs[k], attrs[k])
                self.commit()
        except:
            raise DBError(self, "Error setting fields in Record in %s"%self.ID())
    def first(self):
        if not self.ready:
            raise DBError(self, "Database %s not ready"%self.ID())
        try:
            self.begin()
            rec = wgdb.get_first_record(self.db)
            self.commit()
        except:
            self.commit()
            raise DBError(self, "Can not position at the first record in %s"%self.ID())
        return CURSOR(self, rec)
    def query(self, *qargs):
        if not self.ready:
            raise DBError(self, "Database %s not ready" % self.ID())
        q = wgdb.make_query(self.db, arglist=list(qargs))
        try:
            self.begin()
            rec = wgdb.fetch(self.db, q)
            self.commit()
            return QUERY(self, rec, q)
        except:
            self.commit()
            return None
    def mkquery(self, q):
        res = []
        for _q in q:
            _qe = ["", "", ""]
            if _q[0] not in self.attrs.keys():
                raise QueryError(self, "Query attribute not in the database", q=_q, query=q)
            else:
                _qe[0] = self.attrs[_q[0]]
            if _q[1] == "=":
                _qe[1] = wgdb.COND_EQUAL
            elif _q[1] == "!=":
                _qe[1] = wgdb.COND_NOT_EQUAL
            elif _q[1] == "<":
                _qe[1] = wgdb.COND_LESSTHAN
            elif _q[1] == ">":
                _qe[1] = wgdb.COND_GREATER
            elif _q[1] == "<=":
                _qe[1] = wgdb.COND_GTEQUAL
            elif _q[1] == ">=":
                _qe[1] = wgdb.COND_LTEQUAL
            else:
                raise QueryError(self, "Query operator is incorrect", q=_q, query=q)
            _qe[2] = _q[2]
            res.append(tuple(_qe))
        return res
    def select(self, *qargs):
        if not self.ready:
            raise DBError(self, "Database %s not ready" % self.ID())
        query = self.mkquery(qargs)
        q = wgdb.make_query(self.db, arglist=query)
        #print "AAA",q.res_count
        res = []
        for r in range(q.res_count):
            try:
                self.begin()
                rec = wgdb.fetch(self.db, q)
                self.commit()
                res.append(RECORD(self, rec))
            except:
                self.commit()
                break
        wgdb.free_query(self.db, q)
        return res
    def begin(self, _write=False):
        if not self.ready:
            raise DBError(self, "Database %s not ready" % self.ID())
        if self.is_read != 0 or self.is_write != 0:
            raise DBError(self, "Read or Write transaction already started in %s" % self.ID())
        stamp = time.time()
        if _write == True:
            #print "W"
            self.is_write = stamp
            #self.journal_lock = wgdb.start_write(self.db)
        else:
            #print "R"
            self.is_read = stamp
            #self.journal_lock = wgdb.start_read(self.db)
        return stamp
    def commit(self):
        if not self.ready:
            raise DBError(self, "Database %s not ready" % self.ID())
        if self.is_read != 0:
            #print "CR"
            self.is_read = 0
            #wgdb.end_read(self.db, self.journal_lock)
        if self.is_write != 0:
            #print "CW"
            self.is_write = 0
            #wgdb.end_write(self.db, self.journal_lock)
        self.journal_lock = 0
    def is_journal(self):
        if self.journal_stamp == 0:
            return False
        return True
    def journal(self):
        if not self.ready:
            raise DBError(self, "Database %s not ready" % self.ID())
        if self.is_journal() != True:
            #print "JS"
            self.journal_stamp = time.time()
            if not wgdb.start_logging(self.db):
                raise DBError(self, "Can not start logging on %s" % self.ID())
        else:
            #print "JC"
            self.journal_stamp = 0
            if not wgdb.stop_logging(self.db):
                raise DBError(self, "Can not stop logging on %s" % self.ID())
    def journals(self):
        return find_files_in_dir(self.tmpdir, "wgdb.journal.%s*"%self.ID())
    def dumps(self):
        return find_files_in_dir(self.storedir, "%s*.dump" % self.ID())
    def sync(self):
        if not self.ready:
            raise DBError(self, "Database %s not ready" % self.ID())
        s = time.strftime("%Y.%m.%d.%H.%M.%S.%s")
        fname = "%s/%s.%s.dump"%(self.storedir,self.name,s)
        is_j = self.is_journal()
        if is_j:
            self.journal()
        wgdb.dump(self.db, fname)
        self.begin(True)
        j = self.journals()
        for f in j:
            os.unlink("%s/%s"%(self.tmpdir,f))
        self.commit()
        if is_j:
            self.journal()
    def __len__(self):
        if not self.ready:
            raise DBError(self, "Database %s not ready" % self.ID())
        self.begin()
        _len = wgdb.size(self.db) - wgdb.free(self.db)
        self.commit()
        return int(_len)



class CATALOG:
    def __init__(self, id, size):
        self.db = DB(("id", True, ""),("name", True, ""), ("schema", False, "[]"), id=id, size=size, restore=True)
        print self.has_key("test")
    def has_key(self, name):
        res = self.has_name(name)
        if len(res) == 0:
            return False
        else:
            return True
    def has_name(self, name):
        res = self.db.select(("name","=",name))
        return res
    def __getitem__(self, key):
        res = self.has_name(key)
        if len(res) != 1:
            raise KeyError, key
        _db = res[0]
        print _db["id"]
        db = DB(id=_db["id"], schema=simplejson.loads(_db["schema"]), name=_db["name"])
        return db
    def search_for_id(self):
        try:
            c = self.db.first()
        except:
            return str(int(self.db.ID())+1)
        for r in c.next():
            print r
    def add(self, name, size, *schema):
        print "BBB",name
        res = self.has_key(name)
        print "CCC",res
        if res:
            db = self[name]
        else:
            print "ADD",schema,size
            new_id = self.search_for_id()
            db = DB(schema=schema, size=size, id=new_id)
            print db
            #self.db.insert(id=new_id, name=name, schema=simplejson.dumps(schema))
        return db
    def close(self):
        self.db.close()



