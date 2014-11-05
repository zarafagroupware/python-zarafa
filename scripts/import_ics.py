#!/usr/bin/env python
import zarafa

def opt_args():
    parser = zarafa.parser()
    parser.add_option('--file', dest='ics', action='store',  help='File to import')
    return parser.parse_args()

def main():
    options, args = opt_args()
    zarafa.Server(options).users().next().store.calendar.create_item(ics=file(options.ics).read())

if __name__ == '__main__':
    main()
