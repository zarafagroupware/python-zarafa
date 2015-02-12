#!/usr/bin/env python
import zarafa
from MAPI.Tags import *
import base64

z = zarafa.Server()

def main():
    for user in z.users(remote=True):
        for item in user.store.root.associated.items():
            if item.prop(PR_MESSAGE_CLASS_A).value == 'Webapp.Security.Private':
                pkcs12 = item.body.text
                fname = "%s.p12" % user.email
                print "Dumping %s" % fname
                f = open(fname, 'w')
                f.write(base64.standard_b64decode(pkcs12))
                f.close()

if __name__ == '__main__':
    main()
