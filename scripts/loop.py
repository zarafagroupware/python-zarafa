import zarafa

server = zarafa.Server()

for prop in server.admin_store.props():
    print prop.name, prop.proptag, repr(prop.value)

for company in server.companies():
	print 'company:', company.name

for user in server.users(remote=False):
	print 'local user:', user.name

print server.guid, [user.store.guid for user in server.users()]

for user in server.users():
    if user.name == 'user1':
        print [folder.name for folder in user.store.folders()]

for user in server.users():
    if user.name == 'user1':
        print user.store.props()
        for folder in user.store.folders():
            if folder.name == 'Sent Items':
                for item in folder:
                    print 'item:', item.subject, item.props(), [(att.filename, att.mimetag, len(att.data)) for att in item.attachments()]

for user in server.users():
    for folder in user.store.folders():
        for item in folder:
            print user, folder, item