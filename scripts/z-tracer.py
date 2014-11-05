#!/usr/bin/env python
'''
Program which traces the ICS events of a user and displays the changed/new MAPI properties in a grep/sed/awk way
so users can extract and parse it.
'''
import time
import zarafa
from MAPI.Tags import *

ITEM_MAPPING = {}

def opt_args():
    parser = zarafa.parser()
    return parser.parse_args()

def prettyprinter(item, old_item=False, delete=False):
    # TODO: Intergrate this code in python-zarafa, aka make an Item printable?
    biggest = max((len(str((prop.name) if prop.named else prop.idname)) for prop in item.props()))

    # Delete or new Item
    operation = '-' if delete else '+'
    if old_item:
        old_biggest =  max((len(str((prop.name) if prop.named else prop.idname)) for prop in old_item.props()))
    biggest = max((len(str((prop.name) if prop.named else prop.idname)) for prop in item.props()))

    for prop in item.props():
        # FIXME: some namedprops name's are numbers, so call str()
        idname = str(prop.name if prop.named else prop.idname)
        offset = biggest - len(idname or '')

        # Updated item
        if old_item:
            for old_prop in old_item.props():
                # FIXME: some namedprops are still None, we can't compare these
                old_idname = str(old_prop.name if old_prop.named else old_prop.idname)
                old_offset = old_biggest - len(old_idname or '')

                if old_idname == idname and prop.strval() != old_prop.strval() and prop.proptag == old_prop.proptag:
                    print '- %s %s - %s' % (old_idname, ' ' * old_offset, old_prop.strval())
                    print '+ %s %s - %s' % (idname, ' ' * offset, prop.strval())
        else: # New or Delete item
            print '%s %s %s - %s' % (operation, idname, '' * offset, prop.strval())

class Importer:
    def update(self, item, flags):
        print '\033[1;41mUpdate: subject: %s folder: %s sender: %s \033[1;m' % (item.subject, item.folder, item.sender.email)
        if not flags & SYNC_NEW_MESSAGE:
            old_item = ITEM_MAPPING[item.entryid]
        else: 
            ITEM_MAPPING[item.entryid] = item 
            old_item = False

        prettyprinter(item, old_item)
        print '\033[1;41mEnd Update\033[1;m\n'

    def delete(self, item, flags): # only item.sourcekey is available here!
        entryid = [map_item for map_item in ITEM_MAPPING.values() if item.sourcekey == map_item.sourcekey]
        rm_item = entryid[0]
        if rm_item:
            print '\033[1;41mBegin Delete: subject: %s folder: %s sender: %s \033[1;m' % (rm_item.subject, rm_item.folder, rm_item.sender.email)
            prettyprinter(rm_item, False, True)
            print '\033[1;41mEnd Delete\033[1;m\n'
            del ITEM_MAPPING[rm_item.entryid]

def main():
    options, args = opt_args()
    server = zarafa.Server(options)
    # TODO: use optparse to figure this out?
    if not server.options.auth_user:
        print 'No user specified'
    if not server.options.folders:
        print 'No folder specified'
    else:
        user = zarafa.Server().user(server.options.auth_user)
        # TODO: support multiple folders with multiprocessing?
        folder = user.store.folders().next() # First Folder

        print 'Monitoring folder %s of %s for update and delete events' % (folder, user.fullname)
        # Create mapping
        for item in folder.items():
            ITEM_MAPPING[item.entryid] = item
        print 'Memory mapping of items complete'

        folder_state = folder.state
        new_state = folder.sync(Importer(), folder_state) # from last known state

        while True:
            new_state = folder.sync(Importer(), folder_state) # from last known state
            if new_state != folder_state:
                folder_state = new_state
            time.sleep(1)

if __name__ == '__main__':
    main()
