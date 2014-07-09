from MAPI.Util import *

import zarafa
z = zarafa.Server()

for table in z.tables():
    print table
    print table.csv(delimiter=';')

for item in z.user('user1').store.inbox:
    print item
    for table in item.tables():
        print table
        for row in table:
            print row
    print item.table(PR_MESSAGE_ATTACHMENTS).text()
    print
