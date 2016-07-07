import whited

c = whited.CATALOG("10051", "20M")
#print c.db.journals()
#c.db.sync()
print c.has_key("test")
print c.has_name("test")
#print c.add("test", '10M', ('key', True, 0), ('value', False, ""))
c.close()
