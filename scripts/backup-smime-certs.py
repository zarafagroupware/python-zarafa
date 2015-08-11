#!/usr/bin/env python
import zarafa
from MAPI.Tags import *
import base64

z = zarafa.Server()


def main():
    for user in z.users(remote=True):
        for item in user.store.root.associated.items():

            fname = user.name
            if item.prop(PR_MESSAGE_CLASS_W):
                if item.prop(PR_MESSAGE_CLASS_W).value.lower() == 'webapp.security.private':
                    with open(fname + '.p12', 'w') as f:
                        f.write(base64.b64decode(item.body.text))

                if item.prop(PR_MESSAGE_CLASS_W).value.lower() == 'webapp.security.public':
                    with open(fname + '.pub.pem', 'w') as f:
                        f.write(base64.b64decode(item.body.text))


if __name__ == '__main__':
    main()
