#!/usr/bin/env python
import zarafa
from MAPI.Util import *

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
    table = zarafa.Server(options).table(table)
    if options.dump:
        print table.csv()
    else:
        print table.text()

if __name__ == '__main__':
    main()
