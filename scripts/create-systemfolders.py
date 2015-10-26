
#!/usr/bin/env python

import zarafa
import sys
import binascii
import inspect

def opt_args():
    parser = zarafa.parser('skpcfm')
    parser.add_option("--user", dest="user", action="store", help="Run script for user")
    parser.add_option("--foldername", dest="foldername", action="store", help="name of the folder")
    parser.add_option("--type", dest="Type", action="store", help="""type the store
                                                                   Appointment, Journal, Task,
                                                                   Contacts, Note or Drafts""")
    return parser.parse_args()

def getprop(item, myprop):
    try:
        return item.prop(myprop).value
    except:
        return None

def main():
    options, args = opt_args()
    if not options.user:
        sys.exit('Please use:\n %s --user <username>  ' % (sys.argv[0]))

    user = zarafa.Server(options).user(options.user)

    try:
        print 'Create folder %s' % options.foldername
        user.store.subtree.create_folder(options.foldername)
        print 'Change %s  to %s ' % (options.foldername, options.Type)
        user.folder(options.foldername).container_class = 'IPF.%s'  % options.Type
        entryid = binascii.hexlify(getprop(user.store.folder(options.foldername),0x0FF90102))
        print entryid
        if 'Appointment' in options.Type:
            user.store.root.prop(0x36d00102L).set_value(binascii.unhexlify(entryid))
        if 'Journal' in options.Type:
            user.store.root.prop(0x36d20102L).set_value(binascii.unhexlify(entryid))
        if 'Tasks' in options.Type:
            user.store.root.prop(0x36d40102L).set_value(binascii.unhexlify(entryid))
        if 'Contact'  in options.Type:
            user.store.root.prop(0x36d10102L).set_value(binascii.unhexlify(entryid))
        if 'Note'  in options.Type:
            user.store.root.prop(0x36d30102L).set_value(binascii.unhexlify(entryid))
        if 'Drafts'  in options.Type:
           user.store.root.prop(0x36d70102L).set_value(binascii.unhexlify(entryid))

        print 'Restore entryid for %s' % options.foldername

    except:
        print "can't create folder"

if __name__ == "__main__":
    main()
