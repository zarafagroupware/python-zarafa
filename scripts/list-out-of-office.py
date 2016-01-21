#!/usr/bin/env python
import zarafa

server = zarafa.Server()
for user in server.users():
    oof = user.outofoffice
    if oof.enabled:
        print 'User: %s' % user.name
        print oof.subject
        if oof.start:
            print 'Start: %s' % oof.start
        if oof.end:
            print 'End: %s' % oof.end
