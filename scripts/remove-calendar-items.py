#!/usr/bin/env python

import zarafa

for user in zarafa.Server().users(parse=True):
    print "Removing %d calendar item(s) for user '%s'" % (user.store.calendar.count, user.name)
    user.store.calendar.delete(user.store.calendar.items())
