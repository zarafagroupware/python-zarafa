import zarafa

options, args = zarafa.parser('skpmcuf').parse_args()
assert args, 'please specify search pattern'

for user in zarafa.Server().users(parse=True):
    print "Running for user:", user.name
    for folder in user.store.folders(parse=True):
        print "Folder:", folder.name
        for item in folder.items():
            if args[0].lower() in item.subject.lower():
                print "Deleting:", item
                if options.modify:
                    folder.delete([item])
