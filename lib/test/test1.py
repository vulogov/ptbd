import whited

c = whited.CATALOG("10051", "20M")
print c.db.journals()
c.db.sync()
c.close()
