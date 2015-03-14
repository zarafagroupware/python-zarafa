#!/usr/bin/env python
import zarafa
from MAPI.Util import *

'''
TODO:
* Add seperator between data and header
* -d outputs ; seperated and not ,
* --company throws an exception vs.
[jelle@P9][~/projects/python-zarafa]%sudo zarafa-stats --company
Unable to open requested statistics table
* --top is missing
* --session is implemented, but unusable even on 1920x1080, we somehow have to use less spacing

'''

def opt_args():
    parser = zarafa.parser('skpc')
    parser.add_option('--system', dest='system', action='store_true',  help='Gives information about threads, SQL and caches')
    parser.add_option('--users', dest='users', action='store_true', help='Gives information about users, store sizes and quotas')
    parser.add_option('--company', dest='company', action='store_true', help='Gives information about companies, company sizes and quotas')
    parser.add_option('--servers', dest='servers', action='store_true', help='Gives information about cluster nodes')
    parser.add_option('--top', dest='top', action='store_true', help='Shows top-like information about sessions')
    parser.add_option('-d','--dump', dest='dump', action='store_true', help='print output as csv')
    return parser.parse_args()

def main():
    options, args = opt_args()

    if options.system:
        table = PR_EC_STATSTABLE_SYSTEM
    elif options.users:
        table = PR_EC_STATSTABLE_USERS
    elif options.company:
        table = PR_EC_STATSTABLE_COMPANY
    elif options.servers:
        table = PR_EC_STATSTABLE_SERVERS
    else:
        return
    try:
        table = zarafa.Server(options).table(table)
        if options.dump:
            print table.csv(delimiter=';')
        else:
            print table.text()
    except MAPIErrorNotFound:
        print 'Unable to open requested statistics table'


if __name__ == '__main__':
    main()
