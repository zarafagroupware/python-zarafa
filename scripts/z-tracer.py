#!/usr/bin/env python
'''
Program which traces the ICS events of a user and displays the changed/new MAPI properties in a grep/sed/awk way
so users can extract and parse it.
'''
import time, sys, difflib

from MAPI.Tags import SYNC_NEW_MESSAGE
import zarafa

ITEM_MAPPING = {}

def proplist(item):
    biggest = max((len(str((prop.name) if prop.named else prop.idname)) for prop in item.props()))
    props = []
    for prop in item.props():
        idname = str(prop.name if prop.named else prop.idname)
        offset = biggest - len(idname or '')
        props.append('%s %s%s\n' % (idname, ' ' * offset,  prop.strval()))
    return props

def diffitems(item, old_item=[], delete=False):
    if delete:
        oldprops = proplist(item)
        newprops = []
        new_name = ''
        old_name = item.subject
    else:
        oldprops = proplist(old_item) if old_item else []
        newprops = proplist(item)
        new_name = item.subject
        old_name = item.subject if old_item else ''

    for line in difflib.unified_diff(oldprops, newprops, tofile=new_name, fromfile=old_name):
        sys.stdout.write(line)

class Importer:
    def update(self, item, flags):
        print '\033[1;41mUpdate: subject: %s folder: %s sender: %s \033[1;m' % (item.subject, item.folder, item.sender.email)
        if not flags & SYNC_NEW_MESSAGE:
            old_item = ITEM_MAPPING[item.sourcekey]
        else: 
            ITEM_MAPPING[item.sourcekey] = item 
            old_item = False

        diffitems(item, old_item)
        print '\033[1;41mEnd Update\033[1;m\n'

    def delete(self, item, flags): # only item.sourcekey is available here!
        rm_item = ITEM_MAPPING[item.sourcekey]
        if rm_item:
            print '\033[1;41mBegin Delete: subject: %s folder: %s sender: %s \033[1;m' % (rm_item.subject, rm_item.folder, rm_item.sender.email)
            diffitems(rm_item, delete=True)
            print '\033[1;41mEnd Delete\033[1;m\n'
            del ITEM_MAPPING[rm_item.sourcekey]

def main():
    options, _ = zarafa.parser().parse_args()
    server = zarafa.Server(options)
    # TODO: use optparse to figure this out?
    if not server.options.auth_user:
        print 'No user specified'
    if not server.options.folders:
        print 'No folder specified'
    else:
        user = zarafa.Server().user(server.options.auth_user)
        folder = user.store.folders().next() # First Folder
        print 'Monitoring folder %s of %s for update and delete events' % (folder, user.fullname)
        # Create mapping
        [ITEM_MAPPING[item.sourcekey] = item for item in folder.items()]
        print 'Mapping of items and sourcekey complete'

        folder_state = folder.state
        new_state = folder.sync(Importer(), folder_state) # from last known state
        while True:
            new_state = folder.sync(Importer(), folder_state) # from last known state
            if new_state != folder_state:
                folder_state = new_state
            time.sleep(1)

if __name__ == '__main__':
    main()
