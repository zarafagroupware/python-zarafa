#!/usr/bin/env python
import zarafa

def main():
    for user in zarafa.Server().users(parse=True):
        print 'user:', user.name
        if user.store:
            for folder in user.store.folders(recurse=True, system=True):
                print 'regular: count=%s size=%s %s%s' % (str(folder.count).ljust(8), str(folder.size).ljust(10), folder.depth*'    ', folder.name)

if __name__ == '__main__':
    main()
