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

class DB:
    def __init__(self, *schema, **kw):
        self.schema = schema
        if not kw.has_key("id"):
            raise DBError(self, "Database constructor do not have database ID")
        self.__check_schema()
    def __check_schema(self):
        for i in self.schema:
            if len(i) != 3:
                raise SchemaError(self, "Schema element is not valid", element=i)
            if types.StrType != type(i[0]):
                raise SchemaError(self, "Column name isn't string", element=i)
            if i[1] not in [True, False]:
                raise SchemaError(self, "Schema Indexing parameter is not valid", element=i)

