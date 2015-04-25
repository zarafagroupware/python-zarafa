#!/usr/bin/python

import zarafa

server = zarafa.Server()
for user in server.users():
    oof = user.outofoffice
    if oof.enabled:
        # FIXME: show subject/timespan?
        print 'User: %s' % (user.name)
