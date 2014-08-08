import zarafa
from MAPI.Tags import PR_SUBJECT

server = zarafa.Server()

for item in server.user('user1').store.inbox:
    if item.stubbed:
        print 'all properties:'
        for prop in item.props():
            print 'id=%s, idname=%s, type=%s, typename=%s, value=%r, named=%s, guid=%s, namespace=%s, name=%s, kind=%s, kindname=%s' % (hex(prop.id_), prop.idname, prop.type_, prop.typename, repr(prop.value)[:20], prop.named, prop.guid, prop.namespace, prop.name, prop.kind, prop.kindname)

        print 'archive properties:'
        for prop in item.props(namespace='archive'):
            print prop.name, repr(prop.value)

        print 'pr_subject:'
        print item.prop(PR_SUBJECT)
  
        print 'archive:stubbed:'
        print item.prop('archive:stubbed')
        break
