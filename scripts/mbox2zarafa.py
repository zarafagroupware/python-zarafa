#!/usr/bin/env python
import zarafa
import email
import mailbox
from email.header import *

version = 'Mbox 2 Zarafa 1.0'

# Connect to Zarafa server
server = zarafa.Server()
user = server.user('zarafaUser')

# Connect to Mailbox file
mbox = mailbox.mbox('mailbox')

debug = False


def import_mail(mailroot, item):
    msg = email.message_from_string(item)
    msg.add_header('X-Imported', version)
    mailroot.create_item(eml=msg.as_string())


def main():
    imported = 0
    skipped = 0

    allheaders = []
    mailroot = user.store.inbox

    # Create list of Message-Id already in the Inbox
    for headers in mailroot.items():
        allheaders.append(headers.header('Message-Id'))
    print "Items in Zarafa Inbox: %s" % len(allheaders)

    # Iterate through the mailbox import messages which do not exist in the
    # Zarafa Inbox
    for key, msg in mbox.iteritems():
        # Messages without Message-Id are usually spam so skip them
        if msg['Message-Id'] is None:
            break

        if not msg['Message-Id'] in allheaders:

            if debug:
                print '\tImporting Message-Id', msg['Message-Id']

            import_mail(mailroot, msg.as_string())
            allheaders.append(msg['Message-Id'])
            imported += 1

        else:
            if debug:
                print '\tSkipping Message-Id', msg['Message-Id']
            skipped += 1

    print "Imported : %s" % imported
    print "Skipped: %s" % skipped

if __name__ == "__main__":
    main()
