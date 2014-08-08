#!/usr/bin/env python

import zarafa
from MAPI.Util import *
from types import *
import datetime
import time
import sys

def opt_args():
    parser = zarafa.parser('skpcufm')
    parser.add_option('-v','--verbose', dest='verbose', action='store_true', help='enable verbose mode')
    return parser.parse_args()

def getMailAge(timestamp):
    messagetime = datetime.datetime.fromtimestamp(timestamp)
    curtime = datetime.datetime.now()
    tdiff = curtime - messagetime
    days = tdiff.days
    minutes, seconds = divmod(tdiff.seconds, 60)
    hours, minutes = divmod(minutes, 60)
    remdays = '{0} days '.format(days) if days>0 else ''
    remhours = '{0} hours '.format(hours) if hours>0 else ''
    mailage = '%s%s%s minutes' % (remdays, remhours, minutes)
    return mailage

def main():
    options, args = opt_args()

    assert args, 'You must specify the amount of days!'

    try:
        val = int(args[0])
    except ValueError:
        return sys.exit("Days not specify as a number!")

    for user in zarafa.Server(options=options).users(parse=True):
        print 'Running for user:', user.name
        for folder in user.store.folders(recurse=True):
            print 'Folder:', folder.name
            for item in folder.items():
                if item.received.date() < datetime.date.today()-datetime.timedelta(days=int(args[0])):
                    if options.verbose:
                        print 'Email:', item.subject, 'Received:', getMailAge(item.prop(PR_MESSAGE_DELIVERY_TIME).mapi_value.unixtime)
                    else:
                        print 'Email:', item.subject
                if options.modify:
                    folder.delete([item])

if __name__ == '__main__':
    main()
