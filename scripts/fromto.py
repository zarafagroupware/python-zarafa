import zarafa

server = zarafa.Server()

for item in server.user('user1').store.inbox:
    print item
    print 'from:', repr(item.sender.name), repr(item.sender.email),
    for rec in item.recipients():
        print 'to:', repr(rec.name), repr(rec.email),
    print
