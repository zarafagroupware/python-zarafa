#!/usr/bin/env python
import imaplib
import zarafa
import email
from email.header import *

version = 'Imap 2 Zarafa 1.0'

# Connect to Zarafa server
server = zarafa.Server()
user = server.user('zarafaUser')

# Connect to IMAP server
mail = imaplib.IMAP4('localhost')
mail.login('imapUser', 'password')

debug = False


def create_folder(folder, fname):
    # Create folder and return created folder object
    return folder.create_folder(fname)


def check_folder(folder, fname):
    # Check folder handle exception return result
    try:
        folder.folder(fname, recurse=False)
        return True
    except:
        return False


def create_folders(folder):
    # Create Zarafa folders in the correct hierarchy if they do not exist yet.
    folders = folder.split('/')
    displayname = ''
    mailroot = user.subtree
    for fname in folders:
        if displayname == '':
            displayname = fname
        else:
            displayname = displayname + '/' + fname
        if debug:
            print 'Check folder %s' % displayname
        if check_folder(mailroot, fname):
            if debug:
                print 'Folder %s exists' % displayname
            mailroot = mailroot.folder(fname, recurse=False)
        else:
            if debug:
                print 'Folder %s created' % displayname
            mailroot = create_folder(mailroot, fname)


def import_mail(mailroot, item):
    # Check if Message-Id does not exist in the folder, then Import eml
    msg = email.message_from_string(item)
    msg.add_header('X-Imported', version)

    allheaders = []
    for headers in mailroot.items():
        allheaders.append(headers.header('Message-Id'))

    if not msg['Message-Id'] in allheaders:
        print '\tImporting Message-Id', msg['Message-Id']
        mailroot.create_item(eml=msg.as_string())


def main():
    # Retrieve RFC mail from IMAP and import into Zarafa.
    for item in mail.list()[1]:
        fname = item.split('\"')[3].replace('INBOX', 'Inbox', 1)

        select = mail.select(fname)
        if select[0] == 'OK':
            print 'Syncing folder %s' % fname
            mailroot = user.subtree
            create_folders(fname)

            for syncfolder in fname.split('/'):
                mailroot = mailroot.folder(syncfolder, recurse=False)

            typ, data = mail.search(None, 'ALL')
            for num in data[0].split():
                typ, data = mail.fetch(num, '(RFC822)')
                import_mail(mailroot, data[0][1])

        else:
            print 'Skipping folder %s, (%s)' % (fname, select[1])

if __name__ == "__main__":
    main()
