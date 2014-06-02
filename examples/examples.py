import zarafa
s = zarafa.Server()

for prop in s.admin_store.properties():
    print prop.name, prop.proptag, prop.value

for company in s.companies():
	print 'company:', company.name

for user in s.users(remote=False):
	print 'local user:', user.name

print s.guid, [user.store.guid for user in s.users()]

for user in s.users():
    if user.name == 'user1':
        print [folder.name for folder in user.store.folders()]

print 'server state:', s.state

for user in s.users():
    if user.name == 'user1':
        print user.store.properties()
        for folder in user.store.folders():
            if folder.name == 'Sent Items':
                sync_folder = folder
                for item in folder.items():
                    print 'item:', item.subject, item.properties(), [(att.filename, att.mimetag, len(att.data)) for att in item.attachments()]

print 'syncing'
class Importer:
    def update(self, item, flags):
        print 'update', item.subject

    def delete(self, item, flags):
        pass

sync_folder.sync(Importer())
