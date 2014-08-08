import zarafa

server = zarafa.Server()

item = server.user('user1').store.inbox.create_item(eml=file('spam.eml').read())

print item.received, item.subject
for prop in item.props():
    print prop.idname, repr(prop.value)
