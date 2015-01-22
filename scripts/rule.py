import zarafa
import time

class importer:
    def __init__(self, folder, target):
        self.folder = folder
        self.target = target

    def update(self, item, flags):
        if 'spam' in item.subject:
            print 'trashing..', item
            self.folder.move(item, self.target)

    def delete(self, item, flags):
        pass

server = zarafa.Server()
store = server.user(server.options.auth_user).store
inbox, junk = store.inbox, store.junk

state = inbox.state
while True:
    state = inbox.sync(importer(inbox, junk), state)
    time.sleep(1)
