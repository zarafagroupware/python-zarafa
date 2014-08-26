#!/usr/bin/env python
import zarafa

fname = 'Sent Items'

for user in zarafa.Server().users():
    try:
        if user.store.folder(fname):
            for item in user.store.folder(fname):
                attachments = [attach.filename for attach in item.attachments()]
                if not attachments:
                    continue

                try:
                    for to in item.recipients():
                        if to.addrtype == 'SMTP':
                            print "%s folder: '%s' subject: '%s' attachments: '%s' email: '%s'" % (user.name, fname, item.subject,
                                                                  ', '.join(attachments), to.email)
                        if to.addrtype == 'ZARAFA':
                            print "%s folder: '%s' subject: '%s' attachments: '%s' user: '%s'" % (user.name, fname, item.subject,
                                                                  ', '.join(attachments), to.name)
                except:
                    pass

    except zarafa.ZarafaException:
        pass
