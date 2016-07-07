import whited

c = whited.CATALOG("10051", 1000000)
print c.db.journals()
c.db.sync()
c.close()
