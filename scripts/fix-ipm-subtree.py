#!/usr/bin/env python

from MAPI import *
from MAPI.Util import *
import zarafa


def opt_args():
    parser = zarafa.parser("skpcuf")
    parser.add_option("--move", dest="move", action="store_true", help="attempt to move broken ipm_subtree into zarafa restored folders")
    parser.add_option("--dry-run", dest="dryrun", action="store_true", help="run the script without executing any actions")
    return parser.parse_args()


def main():
    options, args = opt_args()

    restorefoldername = "Zarafa Restored Folders"

    ipmsubtree = {}  # Always correct values, as we look for the IPM_SUBTREE.
    entryidstore = {}  # IPM_SUBTREE_ENTRYID values on user.store, could be incorrect.

    for user in zarafa.Server(options).users(parse=True):
        restoredfolders = False

        print "Processing user: %s" % user.name
        try:
            entryidstore[user.name] = user.store.mapiobj.GetProps([PR_IPM_SUBTREE_ENTRYID], 0)[0].Value.encode("hex").upper()
        except:
            continue

        for folder in user.store.folders(system=True):
            if folder.name == "IPM_SUBTREE":
                if user.name not in ipmsubtree.keys():
                    ipmsubtree[user.name] = folder.entryid

        if entryidstore[user.name] != ipmsubtree[user.name]:
            if options.dryrun:
                print "!! Script running in dry-run mode, nothing will be modified."
            print "- Incorrect IPM_SUBTREE is set: '%s'" % entryidstore[user.name]
            print "* Updating IPM_SUBTREE_ENTRYID to: '%s'" % ipmsubtree[user.name]
            if ipmsubtree[user.name]:
                if not options.dryrun:
                    user.store.mapiobj.SetProps([SPropValue(PR_IPM_SUBTREE_ENTRYID, ipmsubtree[user.name].decode("hex"))])
            if ipmsubtree[user.name] and entryidstore[user.name] and options.move:
                srcfld = user.store.folder(entryidstore[user.name])
                dstfld = user.store.folder(ipmsubtree[user.name])
                print "* Copying source folder '%s' to '%s'" % (srcfld.name, dstfld.name)
                if not options.dryrun:
                    try:
                        if user.store.folder(restorefoldername):
                            resfolder = user.store.folder(restorefoldername)
                            restoredfolders = True
                    except zarafa.ZarafaNotFoundException:
                            restorefolder = dstfld.create_folder(restorefoldername)
                            resfolder = user.store.folder(restorefolder.entryid)
                            restoredfolders = True

                    if restoredfolders:
                        if resfolder:
                            srcfld.move(srcfld, resfolder)
                        else:
                            print "- Unable to move folder into IPM_SUBTREE."

            elif not ipmsubtree[user.name]:
                print "- No IPM_SUBTREE present, does the user have a store?"


def reminder():
    print "\nIf the /resetfolders parameter was used in Outlook, it will be necessary to run the following script as well:"
    print "https://github.com/zarafagroupware/zarafa-tools/blob/master/mailstore/resetfolders.py"


if __name__ == "__main__":
    main()
    reminder()
