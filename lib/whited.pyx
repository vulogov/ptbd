__version__="0.1"
__author__="Vladimir Ulogov"

import wgdb
import types

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

class RECORD:
    def __init__(self, db, rec):
        if not db.ready:
            raise StopIteration()
        self.db = db
        self.rec = rec
    def __getattr__(self, key):
        if key not in self.db.attrs.keys():
            raise KeyError,key
        try:
            return wgdb.get_field(self.db, self.rec, self.db.attrs[key])
        except:
            raise DBError(self.db, "Can not read %s[%s]"%(self.db.ID(),key))
    def __setattr__(self, key, value):
        if key not in self.db.attrs.keys():
            raise KeyError,key
        try:
            return wgdb.set_field(self.db, self.rec, self.db.attrs[key], value)
        except:
            raise DBError(self.db, "Can not set %s[%s]" % (self.db.ID(), key))
    def delete(self):
        try:
            wgdb.delete_record(self.db, self.rec)
        except:
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
            rec = RECORD(self.db, self.rec)
        else:
            raise StopIteration()
        try:
            self.rec = wgdb.get_next_record(self.db, self.rec)
        except:
            self.rec = None
        return rec

class QUERY(CURSOR):
    def __init__(self, db, rec, q):
        CURSOR.__init__(self, db, rec)
        self.q = q
    def next(self):
        if self.rec != None:
            rec = RECORD(self.db, self.rec)
        else:
            raise StopIteration()
        try:
            self.rec = wgdb.fetch(self.db, self.rec)
        except:
            self.rec = None
        return rec




class DB:
    def __init__(self, *schema, **kw):
        self.schema = schema
        self.kw = kw
        self.ready = False
        self.db = None
        if not kw.has_key("id"):
            raise DBError(self, "Database constructor do not have database ID")
        self.__check_schema()
        self.__open()
        self.__createindexes()
    def __check_schema(self):
        self.attrs = {}
        c = 0
        for i in self.schema:
            if len(i) != 3:
                raise SchemaError(self, "Schema element is not valid", element=i)
            if types.StrType != type(i[0]):
                raise SchemaError(self, "Column name isn't string", element=i)
            if i[1] not in [True, False]:
                raise SchemaError(self, "Schema Indexing parameter is not valid", element=i)
            self.attrs[i[0]] = c
            c += 1
    def __open(self):
        if self.ready:
            return True
        try:
            self.db = wgdb.attach_existing_database(self.kw["id"])
        except:
            if not self.kw.has_key("size"):
                raise DBError(self, "Request for a new database %s but no size provided"%self.kw["id"])
            try:
                self.db = wgdb.attach_database(shmname=self.kw["id"], size=self.kw["size"])
                self.ready = True
            except:
                raise DBError(self, "Error creating memory segment with key: %s size %d"%(self.kw["id"], self.kw["size"]))
        return True
    def __createindexes(self):
        if not self.ready:
            return False
        c = 0
        for i in self.schema:
            if i[1] == True:
                try:
                    if not wgdb.createindex(self.db, c):
                        raise DBError(self)
                except:
                    raise DBError(self, "Error creating index for %s[%d]"%(self.kw["id"],c))
            c += 1
    def ID(self):
        return self.kw["id"]
    def close(self):
        if not self.ready:
            return False
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
            _rec = wgdb.create_record(self.db, len(self.schema))
        except:
            raise DBError(self, "Can not create record for %s"%self.ID())
        c = 0
        try:
            for i in self.schema:
                wgdb.set_field(self.db, _rec, c, i[2])
                c += 1
            for k in attrs.keys():
                wgdb.set_field(self.db, _rec, self.attrs[k], attrs[k])
        except:
            raise DBError(self, "Error setting fields in Record in %s"%self.ID())
    def first(self):
        if not self.ready:
            raise DBError(self, "Database %s not ready"%self.ID())
        try:
            rec = wgdb.get_first_record(self.db)
        except:
            raise DBError(self, "Can not position at the first record in %s"%self.ID())
        return CURSOR(self, rec)
    def query(self, *qargs):
        if not self.ready:
            raise DBError(self, "Database %s not ready" % self.ID())
        q = wgdb.make_query(self.db, arglist=list(qargs))
        try:
            rec = wgdb.fetch(self.db, q)
            return QUERY(self.db, rec, q)
        except:
            return None



