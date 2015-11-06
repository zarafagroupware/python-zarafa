#!/usr/bin/env python

import zarafa
from datetime import datetime, timedelta
import time
import sys


def opt_args():
    parser = zarafa.parser('skpcufm')
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true', help='enable verbose mode')
    return parser.parse_args()

def main():
    options, args = opt_args()

    if not args:
        print 'You must specify the amount of days!'
        sys.exit(1)

    try:
        val = int(args[0])
    except ValueError:
        return sys.exit("Days not specified as a number!")

    for user in zarafa.Server(options=options).users(parse=True):
        print 'Running for user:', user.name
        for folder in user.store.folders(parse=True):
            print 'Folder:', folder.name
            for item in folder.items():
                if not item.received:
                    continue
                if item.received < datetime.today() - timedelta(days=int(args[0])):
                    if options.verbose:
                        print 'Item:', item.subject, 'Received:', datetime.today() - item.received
                    else:
                        print 'Item:', item.subject
                    if options.modify:
                        folder.delete([item])

if __name__ == '__main__':
    main()
