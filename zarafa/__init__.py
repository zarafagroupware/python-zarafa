""" 
High-level python bindings for Zarafa

Copyright 2014 Zarafa and contributors, license AGPLv3 (see LICENSE file for details)                                                                                                                                                                                                                                          

Some goals:

- To be fully object-oriented, pythonic, layer above MAPI
- To be usable for many common system administration tasks
- To provide full access to the underlying MAPI layer if needed
- To return all text as unicode strings
- To return/accept binary identifiers in readable (hex-encoded) form
- To raise well-described exceptions if something goes wrong

Main classes:

:class:`Server`

:class:`Store`

:class:`User`

:class:`Company`

:class:`Store`

:class:`Folder`

:class:`Item`

:class:`Body`

:class:`Attachment`

:class:`Address`

:class:`Quota`

:class:`Config`

:class:`Service`


"""

# Python 2.5 doesn't have with
from __future__ import with_statement


import contextlib
import csv
try:
    import daemon.pidlockfile
except ImportError:
    pass
import datetime
import grp
try:
    import libcommon # XXX distribute with python-mapi? or rewrite functionality here?
except ImportError:
    pass
import logging.handlers
from multiprocessing import Process, Queue
from Queue import Empty
import optparse
import os.path
import pwd
import socket
import sys
import StringIO
import threading
import traceback
import mailbox
from email.parser import Parser
import signal
import ssl
import time

from MAPI.Util import *
from MAPI.Util.Generators import *
import MAPI.Util.AddressBook
import MAPI.Tags
import _MAPICore
import icalmapi
import inetmapi

try:
    REV_TYPE
except NameError:
    REV_TYPE = {}
    for K, V in _MAPICore.__dict__.items():
        if K.startswith('PT_'):
            REV_TYPE[V] = K

try:
    REV_TAG
except NameError:
    REV_TAG = {}
    for K, V in MAPI.Tags.__dict__.items():
        if K.startswith('PR_'):
            REV_TAG[V] = K

# XXX clean up and improve for common guids/namepaces
PSETID_Archive = DEFINE_GUID(0x72e98ebc, 0x57d2, 0x4ab5, 0xb0, 0xaa, 0xd5, 0x0a, 0x7b, 0x53, 0x1c, 0xb9)
NAMED_PROPS_ARCHIVER = [MAPINAMEID(PSETID_Archive, MNID_STRING, u'store-entryids'), MAPINAMEID(PSETID_Archive, MNID_STRING, u'item-entryids'), MAPINAMEID(PSETID_Archive, MNID_STRING, u'stubbed'),]

GUID_NAMESPACE = {PSETID_Archive: 'archive'}
NAMESPACE_GUID = {'archive': PSETID_Archive}

# XXX copied from common/ECDefs.h - can we SWIG this stuff?
def OBJECTCLASS(__type, __class):
    return (__type << 16) | (__class & 0xFFFF)

OBJECTTYPE_MAILUSER = 1
ACTIVE_USER = OBJECTCLASS(OBJECTTYPE_MAILUSER, 1)

def _prop(self, mapiobj, proptag):
    if isinstance(proptag, (int, long)):
        mapiprop = HrGetOneProp(mapiobj, proptag)
        return Property(mapiobj, proptag, mapiprop.Value, PROP_TYPE(proptag))
    else:
        namespace, name = proptag.split(':')
        for prop in self.props(namespace=namespace): # XXX megh
            if prop.name == name:
                return prop

def _props(mapiobj, namespace=None):
    result = []
    proptags = mapiobj.GetPropList(MAPI_UNICODE)
    props = mapiobj.GetProps(proptags, MAPI_UNICODE)
    result = [(prop.ulPropTag, prop.Value, PROP_TYPE(prop.ulPropTag)) for prop in props]
    result.sort()
    props1 =[Property(mapiobj, b, c, d) for (b, c, d) in result]
    return [p for p in props1 if not namespace or p.namespace == namespace]

def _state(mapiobj):
    exporter = mapiobj.OpenProperty(PR_CONTENTS_SYNCHRONIZER, IID_IExchangeExportChanges, 0, 0)
    exporter.Config(None, SYNC_NORMAL | SYNC_CATCHUP, None, None, None, None, 0)
    steps, step = None, 0
    while steps != step:
        steps, step = exporter.Synchronize(step)
    stream = IStream()
    exporter.UpdateState(stream)
    stream.Seek(0, MAPI.STREAM_SEEK_SET)
    return bin2hex(stream.Read(0xFFFFF))

def _sync(server, syncobj, importer, state, log, max_changes):
    importer = TrackingContentsImporter(server, importer, log)
    exporter = syncobj.OpenProperty(PR_CONTENTS_SYNCHRONIZER, IID_IExchangeExportChanges, 0, 0)
    stream = IStream()
    stream.Write(state.decode('hex'))
    stream.Seek(0, MAPI.STREAM_SEEK_SET)
    exporter.Config(stream, SYNC_NORMAL | SYNC_UNICODE, importer, None, None, None, 0)
    step = retry = changes = 0
    while True:
        try:
            try:
                (steps, step) = exporter.Synchronize(step)
            finally:
                importer.skip = False
            changes += 1
            retry = 0
            if (steps == step) or (max_changes and changes >= max_changes):
                break
        except MAPIError, e:
            if log:
                log.warn("Received a MAPI error or timeout (error=0x%x, retry=%d/5)" % (e.hr, retry))
            if retry < 5:
                retry += 1
            else:
                if log:
                    log.error("Too many retries, skipping change")
                importer.skip = True # in case of a timeout or other issue, try to skip the change after trying several times
                retry = 0
    exporter.UpdateState(stream)
    stream.Seek(0, MAPI.STREAM_SEEK_SET)
    state = bin2hex(stream.Read(0xFFFFF))
    return state

def _openentry_raw(mapistore, entryid, flags): # avoid underwater action for archived items
    try:
        return mapistore.OpenEntry(entryid, IID_IECMessageRaw, flags)
    except MAPIErrorInterfaceNotSupported:
        return mapistore.OpenEntry(entryid, None, flags)

class ZarafaException(Exception):
    pass

class ZarafaConfigException(ZarafaException):
    pass

class ZarafaNotFoundException(ZarafaException):
    pass


class Property(object):
    """ 
Wrapper around MAPI properties 

"""

    def __init__(self, mapiobj, proptag, value, proptype): # XXX rethink attributes, names.. add guidname..?
        self.mapiobj = mapiobj
        self.proptag = proptag

        self.id_ = proptag >> 16
        self.idname = REV_TAG.get(proptag)
        self.type_ = proptype
        self.typename = REV_TYPE.get(proptype)

        self.named = (self.id_ >= 0x8000)
        self.kind = None
        self.kindname = None
        self.guid = None
        self.name = None
        self.namespace = None

        self.mapi_value = value # XXX mapiobj?
        if proptype == PT_SYSTIME: # XXX generalize
            self._value = datetime.datetime.utcfromtimestamp(value.unixtime)
        else:
            self._value = value

        if self.named:
            try:
                lpname = mapiobj.GetNamesFromIDs([proptag], None, 0)[0]
                self.guid = bin2hex(lpname.guid)
                self.namespace = GUID_NAMESPACE.get(lpname.guid)
                self.name = lpname.id
                self.kind = lpname.kind
                self.kindname = 'MNID_STRING' if lpname.kind == MNID_STRING else 'MNID_ID'
            except MAPIErrorNoSupport: # XXX user.props()?
                pass

    def get_value(self):
        return self._value

    def set_value(self, value):
        self._value = value
        self.mapiobj.SetProps([SPropValue(self.proptag, value)])
        self.mapiobj.SaveChanges(KEEP_OPEN_READWRITE)
    value = property(get_value, set_value)

    def strval(self, sep=','):
        def flatten(v):
            if isinstance(v, list):
                return sep.join(flatten(e) for e in v)
            elif isinstance(v, bool):
                return '01'[v]
            elif self.type_ == PT_BINARY:
                return v.encode('hex').upper()
            else:
                return unicode(v).encode('utf-8')
        return flatten(self._value)

    def __unicode__(self):
        return u'Property(%s, %s)' % (self.name if self.named else self.idname, repr(self._value))

    # TODO: check if data is binary and convert it to hex
    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Table(object):
    """
    Wrapper around MAPI tables

"""

    def __init__(self, server, mapitable, proptag, restriction=None, order=None, columns=None):
        self.server = server
        self.mapitable = mapitable
        self.proptag = proptag
        if columns:
            mapitable.SetColumns(columns, 0)
        else:
            mapitable.SetColumns(mapitable.QueryColumns(TBL_ALL_COLUMNS), 0) # some columns are hidden by default

    @property
    def header(self):
        return [REV_TAG.get(c, hex(c)) for c in self.mapitable.QueryColumns(0)]

    def rows(self): # XXX custom Row class, with dict-like access? namedtuple?
        try:
            for row in self.mapitable.QueryRows(-1, 0):
                yield [Property(self.server.mapistore, c.ulPropTag, c.Value, PROP_TYPE(c.ulPropTag)) for c in row]
        except MAPIErrorNotFound:
            pass

    # TODO: refactor function
    def dict_rows(self):
        if self.proptag == PR_EC_STATSTABLE_SYSTEM:
            try:
                return dict([(row[0].Value, row[2].Value) for row in self.mapitable.QueryRows(-1, 0)])
            except MAPIErrorNotFound:
                pass
        else:
            try:
                return (dict([(c.ulPropTag, c.Value) for c in row]) for row in self.mapitable.QueryRows(-1, 0))
            except MAPIErrorNotFound:
                pass

    def data(self, header=False):
        data = [[p.strval() for p in row] for row in self.rows()]
        if header:
            data = [self.header] + data
        return data

    def text(self, borders=False):
        result = []
        data = self.data(header=True)
        colsizes = [max(len(d[i]) for d in data) for i in range(len(data[0]))]
        for d in data:
            line = []
            for size, c in zip(colsizes, d):
                line.append(c.ljust(size))
            result.append(' '.join(line))
        return '\n'.join(result)

    def csv(self, *args, **kwargs):
        csvfile = StringIO.StringIO()
        writer = csv.writer(csvfile, *args, **kwargs)
        writer.writerows(self.data(header=True))
        return csvfile.getvalue()

    def __iter__(self):
        return self.rows()

    def __repr__(self):
        return u'Table(%s)' % REV_TAG.get(self.proptag)

class Server(object):
    """ 
Server class 

By default, tries to connect to a Zarafa server as configured in ``/etc/zarafa/admin.cfg`` or at UNIX socket ``/var/run/zarafa``

Looks at command-line to see if another server address or other related options were given (such as -c, -s, -k, -p)

:param server_socket: similar to 'server_socket' option in config file
:param sslkey_file: similar to 'sslkey_file' option in config file
:param sslkey_pass: similar to 'sslkey_pass' option in config file
:param config: path of configuration file containing common server options, for example ``/etc/zarafa/admin.cfg``
:param auth_user: username to user for user authentication
:param auth_pass: password to use for user authentication
:param log: logger object to receive useful (debug) information
:param options: OptionParser instance to get settings from (see :func:`parser`)

    
"""

    def __init__(self, options=None, config=None, sslkey_file=None, sslkey_pass=None, server_socket=None, auth_user=None, auth_pass=None, log=None, service=None):
        self.log = log
        self.server_socket = self.sslkey_file = self.sslkey_pass = None

        # get cmd-line options
        self.options = options
        if not self.options:
            self.options, args = parser().parse_args()

        # determine config file
        if config:
            pass
        elif getattr(self.options, 'config_file', None):
            config_file = os.path.abspath(self.options.config_file)
            config = globals()['Config'](None, filename=self.options.config_file) # XXX snarf
        else:
            config_file = '/etc/zarafa/admin.cfg'
            try:
                file(config_file) # check if accessible
                config = globals()['Config'](None, filename=config_file) # XXX snarf
            except IOError:
                pass

        # get defaults
        if os.getenv('ZARAFA_SOCKET'): # env variable used in testset
            self.server_socket = os.getenv('ZARAFA_SOCKET')
        elif config:
            if not (server_socket or getattr(self.options, 'server_socket')): # XXX generalize
                self.server_socket = config.get('server_socket')
                self.sslkey_file = config.get('sslkey_file') 
                self.sslkey_pass = config.get('sslkey_pass')
        else:
            self.server_socket = 'file:///var/run/zarafa'

        # override with explicit or command-line args
        self.server_socket = server_socket or getattr(self.options, 'server_socket', None) or self.server_socket
        self.sslkey_file = sslkey_file or getattr(self.options, 'sslkey_file', None) or self.sslkey_file
        self.sslkey_pass = sslkey_pass or getattr(self.options, 'sslkey_pass', None) or self.sslkey_pass

        # make actual connection. in case of service, wait until this succeeds.
        self.auth_user = auth_user or getattr(self.options, 'auth_user', None) or 'SYSTEM' # XXX override with args
        self.auth_pass = auth_pass or getattr(self.options, 'auth_pass', None) or ''
        while True:
            try:
                self.mapisession = OpenECSession(self.auth_user, self.auth_pass, self.server_socket, sslkey_file=self.sslkey_file, sslkey_pass=self.sslkey_pass)
                break
            except MAPIErrorNetworkError:
                if service:
                    service.log.warn("could not connect to server at '%s', retrying in 5 sec" % self.server_socket)
                    time.sleep(5)
                else:
                    raise ZarafaException("could not connect to server at '%s'" % self.server_socket)

        # start talking dirty
        self.mapistore = GetDefaultStore(self.mapisession)
        self.admin_store = Store(self, self.mapistore)
        self.sa = self.mapistore.QueryInterface(IID_IECServiceAdmin)
        self.ems = self.mapistore.QueryInterface(IID_IExchangeManageStore)
        entryid = HrGetOneProp(self.mapistore, PR_STORE_ENTRYID).Value
        self.pseudo_url = entryid[entryid.find('pseudo:'):-1] # XXX ECSERVER
        self.name = self.pseudo_url[9:]
        self._archive_sessions = {}

    def table(self, name, restriction=None, order=None, columns=None):
        return Table(self, self.mapistore.OpenProperty(name, IID_IMAPITable, 0, 0), name, restriction=restriction, order=order, columns=columns)

    def tables(self):
        for table in (PR_EC_STATSTABLE_SYSTEM, PR_EC_STATSTABLE_SESSIONS, PR_EC_STATSTABLE_USERS, PR_EC_STATSTABLE_COMPANY, PR_EC_STATSTABLE_SERVERS):
            try:
                yield self.table(table)
            except MAPIErrorNotFound:
                pass

    def gab_table(self): # XXX separate addressbook class? useful to add to self.tables?
        ab = self.mapisession.OpenAddressBook(0, None, 0)
        gab = ab.OpenEntry(ab.GetDefaultDir(), None, 0)
        ct = gab.GetContentsTable(MAPI_DEFERRED_ERRORS)
        return Table(self, ct, PR_CONTAINER_CONTENTS)

    def _archive_session(self, host):
        if host not in self._archive_sessions:
            try:
                self._archive_sessions[host] = OpenECSession('SYSTEM', '', 'https://%s:237/zarafa' % host, sslkey_file=self.sslkey_file, sslkey_pass=self.sslkey_pass)
            except: # MAPIErrorLogonFailed, MAPIErrorNetworkError:
                self._archive_sessions[host] = None # XXX avoid subsequent timeouts for now
                raise ZarafaException("could not connect to server at '%s'" % host)
        return self._archive_sessions[host]

    @property
    def guid(self):
        """ Server GUID """

        return bin2hex(HrGetOneProp(self.mapistore, PR_MAPPING_SIGNATURE).Value)

    def user(self, name):
        """ Return :class:`user <User>` with given name; raise exception if not found """

        return User(self, name)

    def get_user(self, name):
        """ Return :class:`user <User>` with given name or *None* if not found """

        try:
            return self.user(name)
        except ZarafaException:
            pass

    def users(self, remote=False, system=False, parse=True):
        """ Return all :class:`users <User>` on server 

            :param remote: include users on remote server nodes
            :param system: include system users
        """

        if parse and getattr(self.options, 'users', None):
            for username in self.options.users:
                yield User(self, username)
            return
        try:
            for name in self._companylist():
                for user in Company(self, name).users(): # XXX remote/system check
                    yield user
        except MAPIErrorNoSupport:
            for username in AddressBook.GetUserList(self.mapisession, None, MAPI_UNICODE):
                user = User(self, username)
                if system or username != u'SYSTEM':
                    if remote or user._ecuser.Servername in (self.name, ''):
                        yield user
                    # XXX following two lines not necessary with python-mapi from trunk
                    elif not remote and user.local: # XXX check if GetUserList can filter local/remote users
                        yield user

    def create_user(self, name, password=None, company=None, fullname=None, create_store=True):
        name = unicode(name)
        fullname = unicode(fullname or '')
        if password:
            password = unicode(password)
        if company:
            company = unicode(company)
        if company and company != u'Default':
            usereid = self.sa.CreateUser(ECUSER(u'%s@%s' % (name, company), password, u'email@domain.com', fullname), MAPI_UNICODE)
            user = self.company(company).user(u'%s@%s' % (name, company))
        else:
            usereid = self.sa.CreateUser(ECUSER(name, password, u'email@domain.com', fullname), MAPI_UNICODE)
            user = self.user(name)
        if create_store:
            self.sa.CreateStore(ECSTORE_TYPE_PRIVATE, user.userid.decode('hex'))
        return user

    def remove_user(self, name): # XXX delete(object)?
        user = self.user(name)
        self.sa.DeleteUser(user._ecuser.UserID)

    def company(self, name):
        """ Return :class:`company <Company>` with given name; raise exception if not found """

        return Company(self, name)

    def get_company(self, name):
        """ Return :class:`company <Company>` with given name or *None* if not found """

        try:
            return self.company(name)
        except ZarafaException:
            pass

    def remove_company(self, name): # XXX delete(object)?
        company = self.company(name)
        self.sa.DeleteCompany(company._eccompany.CompanyID)

    def _companylist(self): # XXX fix self.sa.GetCompanyList(MAPI_UNICODE)? looks like it's not swigged correctly?
        self.sa.GetCompanyList(MAPI_UNICODE) # XXX exception for single-tenant....
        return MAPI.Util.AddressBook.GetCompanyList(self.mapisession, MAPI_UNICODE)

    def companies(self, remote=False): # XXX remote?
        """ Return all :class:`companies <Company>` on server 

            :param remote: include companies without users on this server node
        """

        try:
            for name in self._companylist():
                yield Company(self, name)
        except MAPIErrorNoSupport:
            yield Company(self, u'Default')

    def create_company(self, name):
        name = unicode(name)
        companyeid = self.sa.CreateCompany(ECCOMPANY(name, None), MAPI_UNICODE)
        return self.company(name)

    def store(self, guid):
        """ Return :class:`store <Store>` with given GUID; raise exception if not found """

        if len(guid) != 32:
            raise ZarafaException("invalid store id: '%s'" % guid)
        try:
            storeid = guid.decode('hex')
        except:
            raise ZarafaException("invalid store id: '%s'" % guid)
        table = self.ems.GetMailboxTable(None, 0) # XXX merge with Store.__init__
        table.SetColumns([PR_ENTRYID, PR_EC_STORETYPE], 0)
        table.Restrict(SPropertyRestriction(RELOP_EQ, PR_STORE_RECORD_KEY, SPropValue(PR_STORE_RECORD_KEY, storeid)), TBL_BATCH)
        for row in table.QueryRows(-1, 0):
             return Store(self, self.mapisession.OpenMsgStore(0, row[0].Value, None, MDB_WRITE), row[1].Value == ECSTORE_TYPE_PUBLIC)
        raise ZarafaException("no such store: '%s'" % guid)

    def get_store(self, guid):
        """ Return :class:`store <Store>` with given GUID or *None* if not found """

        try:
            return self.store(guid)
        except ZarafaException:
            pass

    def stores(self, system=False, remote=False): # XXX implement remote
        """ Return all :class:`stores <Store>` on server node

        :param system: include system stores
        :param remote: include stores on other nodes

        """

        table = self.ems.GetMailboxTable(None, 0)
        table.SetColumns([PR_DISPLAY_NAME_W, PR_ENTRYID, PR_EC_STORETYPE], 0)
        for row in table.QueryRows(100, 0):
            store = Store(self, self.mapisession.OpenMsgStore(0, row[1].Value, None, MDB_WRITE), row[2].Value == ECSTORE_TYPE_PUBLIC)
            if system or (store.user and store.user.name != 'SYSTEM'):
                yield store

    @property
    def public_store(self):
        """ public :class:`store <Store>` in single-company mode """

        try:
            self.sa.GetCompanyList(MAPI_UNICODE)
            raise ZarafaException('request for server-wide public store in multi-company setup')
        except MAPIErrorNoSupport:
            return self.companies().next().public_store

    @property
    def state(self):
        """ Current server state """

        return _state(self.mapistore)

    def sync(self, importer, state, log=None, max_changes=None):
        """ Perform synchronization against server node 

        :param importer: importer instance with callbacks to process changes
        :param state: start from this state (has to be given)
        :log: logger instance to receive important warnings/errors 
        
        """

        importer.store = None
        return _sync(self, self.mapistore, importer, state, log or self.log, max_changes)

    def __unicode__(self):
        return u'Server(%s)' % self.server_socket

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Company(object):
    """ Company class """

    def __init__(self, server, name):
        self._name = name = unicode(name)
        self.server = server
        if name != u'Default': # XXX
            try:
                self._eccompany = self.server.sa.GetCompany(self.server.sa.ResolveCompanyName(self._name, MAPI_UNICODE), MAPI_UNICODE)
            except MAPIErrorNotFound:
                raise ZarafaException("no such company: '%s'" % name)

    @property
    def name(self):
        """ Company name """

        return self._name

    @property
    def public_store(self):
        """ Company public :class:`store <Store>` """

        if self._name == u'Default': # XXX 
            pubstore = GetPublicStore(self.server.mapisession)
            if pubstore is None:
                return None
            return Store(self.server, pubstore, True)
        publicstoreid = self.server.ems.CreateStoreEntryID(None, self._name, 0)
        publicstore = self.server.mapisession.OpenMsgStore(0, publicstoreid, None, MDB_WRITE)
        return Store(self.server, publicstore, True)

    def user(self, name):
        """ Return :class:`user <User>` with given name; raise exception if not found """

        name = unicode(name)
        for user in self.users():
            if user.name == name:
                return User(self.server, name)

    def get_user(self, name):
        """ Return :class:`user <User>` with given name or *None* if not found """

        try:
            return self.user(name)
        except ZarafaException:
            pass

    def users(self):
        """ Return all :class:`users <User>` within company """

        for username in AddressBook.GetUserList(self.server.mapisession, self._name if self._name != u'Default' else None, MAPI_UNICODE): # XXX serviceadmin?
            if username != 'SYSTEM':
                yield User(self.server, username)

    def create_user(self, name, password=None):
        self.server.create_user(name, password=password, company=self._name)
        return self.user('%s@%s' % (name, self._name))

    @property
    def quota(self):
        """ Company :class:`Quota` """

        if self._name == u'Default':
            return Quota(self.server, None)
        else:
            return Quota(self.server, self._eccompany.CompanyID)

    def __unicode__(self):
        return u'Company(%s)' % self._name

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Store(object):
    """ 
    Item store
    
    """

    def __init__(self, server, mapistore, public=False):
        self.server = server
        self.mapiobj = mapistore
        self.public = public
        self._root = self.mapiobj.OpenEntry(None, None, 0)

    @property
    def guid(self):
        """ Store GUID """

        return bin2hex(HrGetOneProp(self.mapiobj, PR_STORE_RECORD_KEY).Value)

    @property
    def root(self):
        """ :class:`Folder` designated as store root """

        return Folder(self, HrGetOneProp(self._root, PR_ENTRYID).Value, root=True)

    @property
    def inbox(self):
        """ :class:`Folder` designated as inbox """

        return Folder(self, self.mapiobj.GetReceiveFolder('IPM', 0)[0])

    @property
    def junk(self):
        """ :class:`Folder` designated as junk """

        # PR_ADDITIONAL_REN_ENTRYIDS is a multi-value property, 4th entry is the junk folder
        return Folder(self, HrGetOneProp(self._root, PR_ADDITIONAL_REN_ENTRYIDS).Value[4])

    @property
    def calendar(self):
        """ :class:`Folder` designated as calendar """

        return Folder(self, HrGetOneProp(self._root, PR_IPM_APPOINTMENT_ENTRYID).Value)

    @property
    def outbox(self):
        """ :class:`Folder` designated as outbox """

        return Folder(self, HrGetOneProp(self.mapiobj, PR_IPM_OUTBOX_ENTRYID).Value)

    @property
    def contacts(self):
        """ :class:`Folder` designated as contacts """

        return Folder(self, HrGetOneProp(self._root, PR_IPM_CONTACT_ENTRYID).Value)

    @property
    def drafts(self):
        """ :class:`Folder` designated as drafts """

        return Folder(self, HrGetOneProp(self._root, PR_IPM_DRAFTS_ENTRYID).Value)

    @property
    def wastebasket(self):
        """ :class:`Folder` designated as wastebasket """

        return Folder(self, HrGetOneProp(self.mapiobj, PR_IPM_WASTEBASKET_ENTRYID).Value)

    @property
    def sentmail(self):
        """ :class:`Folder` designated as sentmail """

        return Folder(self, HrGetOneProp(self.mapiobj, PR_IPM_SENTMAIL_ENTRYID).Value)

    @property
    def tasks(self):
        """ :class:`Folder` designated as tasks """

        return Folder(self, HrGetOneProp(self.mapiobj, PR_IPM_TASK_ENTRYID).Value)

    @property
    def subtree(self):
        """ :class:`Folder` designated as IPM.Subtree """
        # TODO: doesn't work, needs to be swigged
        return Folder(self, HrGetOneProp(self.mapiobj, PR_IPM_SUBTREE_ENTRYID).Value)

    @property
    def user(self):
        """ Store :class:`owner <User>` """

        try:
            userid = HrGetOneProp(self.mapiobj, PR_MAILBOX_OWNER_ENTRYID).Value
            return User(self.server, self.server.sa.GetUser(userid, 0).Username)
        except MAPIErrorNotFound:
            pass

    def folder(self, key): # XXX sloowowowww
        """ Return :class:`Folder` with given name or entryid; raise exception if not found

            :param key: name or entryid
        """

        matches = [f for f in self.folders() if f.entryid == key or f.name == key]
        if len(matches) == 0:
            raise ZarafaException("no such folder: '%s'" % key)
        elif len(matches) > 1:
            raise ZarafaException("multiple folders with name/entryid '%s'" % key)
        else:
            return matches[0]

    def folders(self, recurse=True, system=False, mail=False, parse=True): # XXX mail flag semantic difference?
        """ Return all :class:`folders <Folder>` in store

        :param recurse: include all sub-folders
        :param system: include system folders
        :param mail: only include mail folders

        """

        # filter function to determine if we return a folder or not
        filter_names = None
        if parse and getattr(self.server.options, 'folders', None):
            filter_names = self.server.options.folders

        def check_folder(folder):
            if filter_names and folder.name not in filter_names:
                return False
            if mail:
                try:
                    if folder.prop(PR_CONTAINER_CLASS) != 'IPF.Note':
                        return False
                except MAPIErrorNotFound:
                    pass
            return True

        # determine root folder
        if system:
            root = self.mapiobj.OpenEntry(None, None, 0)
        else:
            try:
                if self.public:
                    ipmsubtreeid = HrGetOneProp(self.mapiobj, PR_IPM_PUBLIC_FOLDERS_ENTRYID).Value
                else:
                    ipmsubtreeid = HrGetOneProp(self.mapiobj, PR_IPM_SUBTREE_ENTRYID).Value
            except MAPIErrorNotFound: # SYSTEM store
                return
            root = self.mapiobj.OpenEntry(ipmsubtreeid, IID_IMAPIFolder, MAPI_DEFERRED_ERRORS)

        # loop over and filter all subfolders 
        table = root.GetHierarchyTable(0)
        table.SetColumns([PR_ENTRYID], TBL_BATCH)
        table.Restrict(SPropertyRestriction(RELOP_EQ, PR_FOLDER_TYPE, SPropValue(PR_FOLDER_TYPE, FOLDER_GENERIC)), TBL_BATCH)
        for row in table.QueryRows(-1, 0):
            folder = Folder(self, row[0].Value)
            folder.depth = 0
            if check_folder(folder):
                yield folder
            if recurse:
                for subfolder in folder.folders(depth=1):
                    if check_folder(subfolder):
                        yield subfolder

    def item(self, entryid):
        """ Return :class:`Item` with given entryid; raise exception of not found """ # XXX better exception?

        item = Item() # XXX copy-pasting..
        item.store = self
        item.server = self.server
        item.mapiobj = _openentry_raw(self.mapiobj, entryid.decode('hex'), MAPI_MODIFY)
        return item

    @property
    def size(self):
        """ Store size """

        return HrGetOneProp(self.mapiobj, PR_MESSAGE_SIZE_EXTENDED).Value

    def config_item(self, name):
        item = Item()
        item.mapiobj = libcommon.GetConfigMessage(self.mapiobj, 'Zarafa.Quota')
        return item

    @property
    def last_logon(self):
        """ Return :datetime Last logon of a user on this store """
        return self.prop(PR_LAST_LOGON_TIME).value or None

    @property
    def last_logoff(self):
        """ Return :datetime of the last logoff of a user on this store """
        return self.prop(PR_LAST_LOGOFF_TIME).value or None

    def prop(self, proptag):
        return _prop(self, self.mapiobj, proptag)

    def props(self):
        return _props(self.mapiobj)

    def __unicode__(self):
        return u'Store(%s)' % self.guid

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Folder(object):
    """ 
    Item Folder 

    """

    def __init__(self, store, entryid, associated=False, root=False):
        self.store = store
        self.server = store.server
        self.root = root
        self._entryid = entryid
        self.mapiobj = store.mapiobj.OpenEntry(entryid, IID_IMAPIFolder, MAPI_MODIFY)
        self.content_flag = MAPI_ASSOCIATED if associated else 0

    @property
    def entryid(self):
        """ Folder entryid """

        return bin2hex(self._entryid)

    @property
    def folderid(self):
        return HrGetOneProp(self.mapiobj, PR_EC_HIERARCHYID).Value

    @property
    def name(self):
        """ Folder name """

        if self.root:
            return u'ROOT'
        else:
            return HrGetOneProp(self.mapiobj, PR_DISPLAY_NAME_W).Value

    def item(self, entryid):
        """ Return :class:`Item` with given entryid; raise exception of not found """ # XXX better exception?

        item = Item() # XXX copy-pasting..
        item.store = self.store
        item.server = self.server
        item.mapiobj = _openentry_raw(self.store.mapiobj, entryid.decode('hex'), MAPI_MODIFY)
        return item

    def items(self):
        """ Return all :class:`items <Item>` in folder, reverse sorted on received date """

        table = self.mapiobj.GetContentsTable(self.content_flag)
        table.SortTable(SSortOrderSet([SSort(PR_MESSAGE_DELIVERY_TIME, TABLE_SORT_DESCEND)], 0, 0), 0) # XXX configure
        while True:
            rows = table.QueryRows(50, 0)
            if len(rows) == 0:
                break
            for row in rows:
                item = Item()
                item.store = self.store
                item.server = self.server
                item.mapiobj = _openentry_raw(self.store.mapiobj, PpropFindProp(row, PR_ENTRYID).Value, MAPI_MODIFY)
                yield item

    def create_item(self, eml=None, ics=None, **kwargs): # XXX associated
        item = Item(self, eml=eml, ics=ics, create=True)
        item.server = self.server
        for key, val in kwargs.items():
            setattr(item, key, val)
        return item

    @property
    def size(self): # XXX bit slow perhaps? :P
        """ Folder size """

        size = 0
        table = self.mapiobj.GetContentsTable(self.content_flag)
        table.SetColumns([PR_MESSAGE_SIZE], 0)
        table.SeekRow(BOOKMARK_BEGINNING, 0)
        rows = table.QueryRows(-1, 0)
        for row in rows:
            size += row[0].Value
        return size

    @property
    def count(self, recurse=False): # XXX implement recurse?
        """ Number of items in folder

        :param recurse: include items in sub-folders

        """

        return self.mapiobj.GetContentsTable(self.content_flag).GetRowCount(0) # XXX PR_CONTENT_COUNT, PR_ASSOCIATED_CONTENT_COUNT

    def _get_entryids(self, items):
        if isinstance(items, (Item, Folder)):
            items = [items]
        else:
            items = list(items)
        item_entryids = [item.entryid.decode('hex') for item in items if isinstance(item, Item)]
        folder_entryids = [item.entryid.decode('hex') for item in items if isinstance(item, Folder)]
        return item_entryids, folder_entryids

    def delete(self, items): # XXX associated
        item_entryids, folder_entryids = self._get_entryids(items)
        if item_entryids:
            self.mapiobj.DeleteMessages(item_entryids, 0, None, DELETE_HARD_DELETE)
        for entryid in folder_entryids:
            self.mapiobj.DeleteFolder(entryid, 0, None, DEL_FOLDERS|DEL_MESSAGES)

    def copy(self, items, folder, _delete=False):
        item_entryids, folder_entryids = self._get_entryids(items)
        if item_entryids:
            self.mapiobj.CopyMessages(item_entryids, IID_IMAPIFolder, folder.mapiobj, 0, None, (MESSAGE_MOVE if _delete else 0))
        for entryid in folder_entryids:
            self.mapiobj.CopyFolder(entryid, IID_IMAPIFolder, folder.mapiobj, None, 0, None, (FOLDER_MOVE if _delete else 0))

    def move(self, items, folder):
        self.copy(items, folder, _delete=True)

    def folder(self, key): # XXX sloowowowww, see also Store.folder
        """ Return :class:`Folder` with given name or entryid; raise exception if not found

            :param key: name or entryid
        """

        matches = [f for f in self.folders() if f.entryid == key or f.name == key]
        if len(matches) == 0:
            raise ZarafaNotFoundException("no such folder: '%s'" % key)
        elif len(matches) > 1:
            raise ZarafaNotFoundException("multiple folders with name/entryid '%s'" % key)
        else:
            return matches[0]

    def folders(self, recurse=True, depth=0):
        """ Return all :class:`sub-folders <Folder>` in folder

        :param recurse: include all sub-folders
        """

#        if self.mapiobj.GetProps([PR_SUBFOLDERS], MAPI_UNICODE)[0].Value: # XXX no worky?
        if True:
            table = self.mapiobj.GetHierarchyTable(MAPI_UNICODE)
            table.SetColumns([PR_ENTRYID, PR_FOLDER_TYPE, PR_DISPLAY_NAME_W], 0)
            rows = table.QueryRows(-1, 0)
            for row in rows:
                subfolder = self.mapiobj.OpenEntry(row[0].Value, None, MAPI_MODIFY)
                entryid = subfolder.GetProps([PR_ENTRYID], MAPI_UNICODE)[0].Value
                folder = Folder(self.store, entryid)
                folder.depth = depth
                yield folder
                if recurse:
                    for subfolder in folder.folders(depth=depth+1):
                        yield subfolder

    def create_folder(self, name):
        mapifolder = self.mapiobj.CreateFolder(FOLDER_GENERIC, name, '', None, 0)
        return Folder(self.store, HrGetOneProp(mapifolder, PR_ENTRYID).Value)

    def prop(self, proptag):
        return _prop(self, self.mapiobj, proptag)

    def props(self):
        return _props(self.mapiobj)

    def table(self, name, restriction=None, order=None, columns=None): # XXX associated, PR_CONTAINER_CONTENTS?
        return Table(self.server, self.mapiobj.OpenProperty(name, IID_IMAPITable, 0, 0), name, restriction=restriction, order=order, columns=columns)

    def tables(self): # XXX associated
        yield self.table(PR_CONTAINER_CONTENTS)
        yield self.table(PR_FOLDER_ASSOCIATED_CONTENTS)
        yield self.table(PR_CONTAINER_HIERARCHY)

    @property
    def state(self):
        """ Current folder state """

        return _state(self.mapiobj)

    def sync(self, importer, state=None, log=None, max_changes=None):
        """ Perform synchronization against folder

        :param importer: importer instance with callbacks to process changes
        :param state: start from this state; if not given sync from scratch
        :log: logger instance to receive important warnings/errors 
        
        """

        if state is None:
            state = (8*'\0').encode('hex').upper()
        importer.store = self.store
        return _sync(self.store.server, self.mapiobj, importer, state, log, max_changes)

    def readmbox(self, location):
        for message in mailbox.mbox(location):
            newitem = Item(self, message.__str__())

    def mbox(self, location):
        mboxfile = mailbox.mbox(location)
        mboxfile.lock()
        for item in self.items():
            mboxfile.add(item.eml())
        mboxfile.unlock()

    def maildir(self):
        destination = mailbox.MH(self.name)
        destination.lock()
        for item in self.items():
            destination.add(item.eml())
        destination.unlock()

    def read_maildir(self, location):
        for message in mailbox.MH(location):
            newitem = Item(self, message.__str__())

    @property
    def associated(self):
        """ Associated folder containing hidden items """

        return Folder(self.store, self._entryid, associated=True)

    def __iter__(self):
        return self.items()

    def __unicode__(self): # XXX associated?
        return u'Folder(%s)' % self.name

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Item(object):
    """ Item """

    def __init__(self, folder=None, eml=None, ics=None, create=False):
        # TODO: self.folder fix this!
        self.emlfile = eml
        self._folder = folder
        self._architem = None

        if create:
            self.mapiobj = self.folder.mapiobj.CreateMessage(None, 0)
            server = self.folder.store.server # XXX

            if eml is not None:
                # options for CreateMessage: 0 / MAPI_ASSOCIATED
                dopt = inetmapi.delivery_options()
                inetmapi.IMToMAPI(server.mapisession, self.folder.store.mapiobj, None, self.mapiobj, self.emlfile, dopt)

            elif ics is not None:
                ab = server.mapisession.OpenAddressBook(0, None, 0)
                icm = icalmapi.CreateICalToMapi(self.mapiobj, ab, False)
                icm.ParseICal(ics, 'utf-8', '', None, 0)
                icm.GetItem(0, 0, self.mapiobj)

            else:
                container_class = HrGetOneProp(self.folder.mapiobj, PR_CONTAINER_CLASS).Value
                if container_class == 'IPF.Contact': # XXX just skip first 4 chars?
                    self.mapiobj.SetProps([SPropValue(PR_MESSAGE_CLASS, 'IPM.Contact')])
                elif container_class == 'IPF.Appointment':
                    self.mapiobj.SetProps([SPropValue(PR_MESSAGE_CLASS, 'IPM.Appointment')])
                else:
                    self.mapiobj.SetProps([SPropValue(PR_MESSAGE_CLASS, 'IPM.Note')])

            self.mapiobj.SaveChanges(KEEP_OPEN_READWRITE)

    @property
    def _arch_item(self): # make an explicit connection to archive server so we can handle otherwise silenced errors (MAPI errors in mail bodies for example)
        if self._architem is None:
            if self.stubbed:
                ids = self.mapiobj.GetIDsFromNames(NAMED_PROPS_ARCHIVER, 0)
                PROP_STORE_ENTRYIDS = CHANGE_PROP_TYPE(ids[0], PT_MV_BINARY)
                try:
                    # support for multiple archives was a mistake, and is not and _should not_ be used. so we just pick nr 0.
                    arch_storeid = HrGetOneProp(self.mapiobj, PROP_STORE_ENTRYIDS).Value[0]
                    arch_server = arch_storeid[arch_storeid.find('pseudo://')+9:-1]
                    arch_session = self.server._archive_session(arch_server)
                    if arch_session is None: # XXX first connection failed, no need to report about this multiple times
                        self._architem = self.mapiobj
                    else:
                        PROP_ITEM_ENTRYIDS = CHANGE_PROP_TYPE(ids[1], PT_MV_BINARY)
                        item_entryid = HrGetOneProp(self.mapiobj, PROP_ITEM_ENTRYIDS).Value[0]
                        arch_store = arch_session.OpenMsgStore(0, arch_storeid, None, 0)
                        self._architem = arch_store.OpenEntry(item_entryid, None, 0)
                except MAPIErrorNotFound: # XXX fix 'stubbed' definition!!
                    self._architem = self.mapiobj
            else:
                self._architem = self.mapiobj
        return self._architem

    @property
    def entryid(self):
        """ Item entryid """

        return bin2hex(HrGetOneProp(self.mapiobj, PR_ENTRYID).Value)

    @property
    def sourcekey(self):
        """ Item sourcekey """

        if not hasattr(self, '_sourcekey'): # XXX more general caching solution
            self._sourcekey = bin2hex(HrGetOneProp(self.mapiobj, PR_SOURCE_KEY).Value)
        return self._sourcekey

    @property
    def subject(self):
        """ Item subject or *None* if no subject """

        try:
            return HrGetOneProp(self.mapiobj, PR_SUBJECT_W).Value
        except MAPIErrorNotFound:
            pass

    @subject.setter
    def subject(self, x):
        self.mapiobj.SetProps([SPropValue(PR_SUBJECT_W, unicode(x))])
        self.mapiobj.SaveChanges(KEEP_OPEN_READWRITE)

    @property
    def body(self):
        """ Item :class:`body <Body>` """
        return Body(self) # XXX return None if no body..?

    @body.setter
    def body(self, x):
        self.mapiobj.SetProps([SPropValue(PR_BODY_W, unicode(x))])
        self.mapiobj.SaveChanges(KEEP_OPEN_READWRITE)

    @property
    def received(self):
        """ Datetime instance with item delivery time """

        try:
            return self.prop(PR_MESSAGE_DELIVERY_TIME).value
        except MAPIErrorNotFound:
            pass

    @property
    def stubbed(self):
        """ Is item stubbed by archiver? """

        ids = self.mapiobj.GetIDsFromNames(NAMED_PROPS_ARCHIVER, 0) # XXX cache folder.GetIDs..?
        PROP_STUBBED = CHANGE_PROP_TYPE(ids[2], PT_BOOLEAN)
        try:
            return HrGetOneProp(self.mapiobj, PROP_STUBBED).Value # False means destubbed
        except MAPIErrorNotFound:
            return False

    @property
    def folder(self):
        """ Parent :class:`Folder` of an item """
        if self._folder:
            return self._folder
        try:
            return Folder(self.store, HrGetOneProp(self.mapiobj, PR_PARENT_ENTRYID).Value)
        except MAPIErrorNotFound:
            pass

    def prop(self, proptag):
        return _prop(self, self.mapiobj, proptag)

    def props(self, namespace=None):
        return _props(self.mapiobj, namespace)

    def attachments(self, embedded=False):
        """ Return item :class:`attachments <Attachment>`

        :param embedded: include embedded attachments
        
        """

        mapiitem = self._arch_item
        table = mapiitem.GetAttachmentTable(MAPI_DEFERRED_ERRORS)
        table.SetColumns([PR_ATTACH_NUM, PR_ATTACH_METHOD], TBL_BATCH)
        attachments = []
        while True:
            rows = table.QueryRows(50, 0)
            if len(rows) == 0:
                break
            for row in rows:
                if row[1].Value == ATTACH_BY_VALUE or (embedded and row[1].Value == ATTACH_EMBEDDED_MSG):
                    att = mapiitem.OpenAttach(row[0].Value, IID_IAttachment, 0)
                    attachments.append(Attachment(att))
        return attachments

    def header(self, name):
        """ Return transport message header with given name """

        return self.headers().get(name)

    def headers(self):
        """ Return transport message headers """

        try:
            message_headers = self.prop(PR_TRANSPORT_MESSAGE_HEADERS)
            headers = Parser().parsestr(message_headers.value, headersonly=True)
            return headers
        except MAPIErrorNotFound:
            return {}

    def eml(self):
        """ Return .eml version of item """

        if not self.emlfile:
            sopt = inetmapi.sending_options()
            sopt.no_recipients_workaround = True
            self.emlfile = inetmapi.IMToINet(self.store.server.mapisession, None, self.mapiobj, sopt)
        return self.emlfile

    def send(self):
        props = []
        props.append(SPropValue(PR_SENTMAIL_ENTRYID, self.folder.store.sentmail.entryid.decode('hex')))
        props.append(SPropValue(PR_DELETE_AFTER_SUBMIT, True))
        self.mapiobj.SetProps(props)
        self.mapiobj.SubmitMessage(0)

    @property
    def sender(self):
        """ Sender :class:`Address` """

        return Address(self.server, *(self.prop(p).value for p in (PR_SENT_REPRESENTING_ADDRTYPE, PR_SENT_REPRESENTING_NAME_W, PR_SENT_REPRESENTING_EMAIL_ADDRESS, PR_SENT_REPRESENTING_ENTRYID)))

    def table(self, name, restriction=None, order=None, columns=None):
        return Table(self.server, self.mapiobj.OpenProperty(name, IID_IMAPITable, 0, 0), name, restriction=restriction, order=order, columns=columns)

    def tables(self):
        yield self.table(PR_MESSAGE_RECIPIENTS)
        yield self.table(PR_MESSAGE_ATTACHMENTS)

    def recipients(self):
        """ Return recipient :class:`addresses <Address>` """

        result = []
        for row in self.table(PR_MESSAGE_RECIPIENTS):
            row = dict([(x.proptag, x) for x in row])
            result.append(Address(self.server, *(row[p].value for p in (PR_ADDRTYPE, PR_DISPLAY_NAME_W, PR_EMAIL_ADDRESS, PR_ENTRYID))))
        return result

    @property
    def to(self):
        return self.recipients() # XXX filter

    @to.setter
    def to(self, addrs):
        if isinstance(addrs, (str, unicode)):
            addrs = [Address(email=s.strip()) for s in unicode(addrs).split(';')]
        ab = self.server.mapisession.OpenAddressBook(0, None, 0) # XXX
        names = []
        for addr in addrs:
            names.append([
                SPropValue(PR_RECIPIENT_TYPE, MAPI_TO), 
                SPropValue(PR_DISPLAY_NAME_W, addr.name or u'nobody'), 
                SPropValue(PR_ADDRTYPE, 'SMTP'), 
                SPropValue(PR_EMAIL_ADDRESS, unicode(addr.email)),
                SPropValue(PR_ENTRYID, ab.CreateOneOff(addr.name or u'nobody', u'SMTP', unicode(addr.email), MAPI_UNICODE)),
            ])
        self.mapiobj.ModifyRecipients(0, names)
        self.mapiobj.SaveChanges(KEEP_OPEN_READWRITE)

    def __unicode__(self):
        return u'Item(%s)' % self.subject

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Body:
    """ Body """

    def __init__(self, mapiitem):
        self.mapiitem = mapiitem

    @property
    def text(self):
        """ Plaintext representation (possibly from archive server) """

        try:
            mapiitem = self.mapiitem._arch_item # XXX server already goes 'underwater'.. check details
            stream = mapiitem.OpenProperty(PR_BODY_W, IID_IStream, 0, 0)
            data = []
            while True:
                blup = stream.Read(0xFFFFF) # 1 MB
                if len(blup) == 0:
                    break
                data.append(blup)
            return ''.join(data).decode('utf-32le') # XXX under windows this be utf-16le or something
        except MAPIErrorNotFound:
            pass

    @property
    def html(self): # XXX decode using PR_INTERNET_CPID
        """ HTML representation (possibly from archive server), in original encoding """

        try:
            mapiitem = self.mapiitem._arch_item
            stream = mapiitem.OpenProperty(PR_HTML, IID_IStream, 0, 0)
            data = []
            while True:
                blup = stream.Read(0xFFFFF) # 1 MB
                if len(blup) == 0:
                    break
                data.append(blup)
            return ''.join(data) # XXX do we need to do something about encodings? 
        except MAPIErrorNotFound:
            pass

    def __unicode__(self):
        return u'Body()'

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Address:
    """ Address """

    def __init__(self, server=None, addrtype=None, name=None, email=None, entryid=None):
        self.server = server
        self.addrtype = addrtype
        self._name = name
        self._email = email
        self.entryid = entryid

    @property
    def name(self):
        """ Full name """

        return self._name

    @property
    def email(self):
        """ Email address """

        if self.addrtype == 'ZARAFA':
            try:
                mapiuser = self.server.mapisession.OpenEntry(self.entryid, None, 0)
                return self.server.user(HrGetOneProp(mapiuser, PR_ACCOUNT).Value).email
            except ZarafaException:
                return None # XXX 'Support Delft'??
        else:
            return self._email

    def __unicode__(self):
        return u'Address(%s)' % self.email

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Attachment(object):
    """ Attachment """

    def __init__(self, att):
        self.att = att
        self._data = None

    @property
    def mimetype(self):
        """ Mime-type or *None* if not found """

        try:
            return HrGetOneProp(self.att, PR_ATTACH_MIME_TAG).Value
        except MAPIErrorNotFound:
            pass

    @property
    def filename(self):
        """ Filename or *None* if not found """

        try:
            return HrGetOneProp(self.att, PR_ATTACH_LONG_FILENAME_W).Value
        except MAPIErrorNotFound:
            pass

    def __len__(self):
        """ Size """

        try:
            return int(HrGetOneProp(self.att, PR_ATTACH_SIZE).Value) # XXX why is this not equal to len(data)??
        except MAPIErrorNotFound:
            pass

    @property
    def data(self):
        """ Binary data """

        if self._data is not None:
            return self._data
        try:
            method = HrGetOneProp(self.att, PR_ATTACH_METHOD).Value # XXX unused
            stream = self.att.OpenProperty(PR_ATTACH_DATA_BIN, IID_IStream, 0, 0)
        except MAPIErrorNotFound:
            self._data = ''
            return self._data
        data = []
        while True:
            blup = stream.Read(0xFFFFF) # 1 MB
            if len(blup) == 0:
                break
            data.append(blup)
        self._data = ''.join(data)
        return self._data

    # file-like behaviour
    def read(self):
        return self.data

    @property
    def name(self):
        return self.filename

    def prop(self, proptag):
        return _prop(self, self.att, proptag)

    def props(self):
        return _props(self.att)

class User(object):
    """ User class """

    def __init__(self, server, name):
        self._name = name = unicode(name)
        self.server = server
        try:
            self._ecuser = self.server.sa.GetUser(self.server.sa.ResolveUserName(self._name, MAPI_UNICODE), MAPI_UNICODE)
        except MAPIErrorNotFound:
            raise ZarafaException("no such user: '%s'" % name)
        self.mapiobj = self.server.mapisession.OpenEntry(self._ecuser.UserID, None, 0)

    @property
    def name(self):
        """ Account name """

        return self._name

    @property
    def fullname(self):
        """ Full name """

        return self._ecuser.FullName

    @property
    def email(self):
        """ Email address """

        return self._ecuser.Email

    @property
    def userid(self):
        """ Userid """

        return bin2hex(self._ecuser.UserID)

    @property
    def company(self):
        """ :class:`Company` the user belongs to """

        return Company(self.server, HrGetOneProp(self.mapiobj, PR_EC_COMPANY_NAME_W).Value or u'Default')

    @property # XXX
    def local(self):
        store = self.store
        return bool(store and (self.server.guid == bin2hex(HrGetOneProp(store.mapiobj, PR_MAPPING_SIGNATURE).Value)))

    @property
    def store(self):
        """ Default :class:`Store` for user or *None* if no store is attached """

        try:
            storeid = self.server.ems.CreateStoreEntryID(None, self._name, MAPI_UNICODE)
            mapistore = self.server.mapisession.OpenMsgStore(0, storeid, IID_IMsgStore, MDB_WRITE|MAPI_DEFERRED_ERRORS)
            return Store(self.server, mapistore)
        except MAPIErrorNotFound:
            pass

    @property
    def archive_store(self):
        """ Archive :class:`Store` for user or *None* if not found """

        mapistore = self.store.mapiobj
        ids = mapistore.GetIDsFromNames(NAMED_PROPS_ARCHIVER, 0) # XXX merge namedprops stuff
        PROP_STORE_ENTRYIDS = CHANGE_PROP_TYPE(ids[0], PT_MV_BINARY)
        try:
            # support for multiple archives was a mistake, and is not and _should not_ be used. so we just pick nr 0.
            arch_storeid = HrGetOneProp(mapistore, PROP_STORE_ENTRYIDS).Value[0]
        except MAPIErrorNotFound:
            return
        arch_server = arch_storeid[arch_storeid.find('pseudo://')+9:-1]
        arch_session = self.server._archive_session(arch_server)
        if arch_session is None:
            return
        arch_store = arch_session.OpenMsgStore(0, arch_storeid, None, MDB_WRITE)
        return Store(self.server, arch_store) # XXX server?

    @property
    def active(self):
        return self._ecuser.Class == ACTIVE_USER

    @property
    def home_server(self):
        return self._ecuser.Servername

    @property
    def archive_servers(self):
       return HrGetOneProp(self.mapiobj, PR_EC_ARCHIVE_SERVERS).Value

    def prop(self, proptag):
        return _prop(self, self.mapiobj, proptag)

    def props(self):
        return _props(self.mapiobj)

    @property
    def quota(self):
        """ User :class:`Quota` """

        return Quota(self.server, self._ecuser.UserID)

    def __unicode__(self):
        return u'User(%s)' % self._name

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Quota(object):
    """ Quota """

    def __init__(self, server, userid):
        self.server = server
        self.userid = userid
        self._warning_limit = self._soft_limit = self._hard_limit = 0 # XXX quota for 'default' company?
        if userid:
            quota = server.sa.GetQuota(userid, False)
            self._warning_limit = quota.llWarnSize
            self._soft_limit = quota.llSoftSize
            self._hard_limit = quota.llHardSize

    @property
    def warning_limit(self):
        """ Warning limit """

        return self._warning_limit

    @property
    def soft_limit(self):
        """ Soft limit """

        return self._soft_limit

    @property
    def hard_limit(self):
        """ Hard limit """

        return self._hard_limit

    @property
    def recipients(self):
        if self.userid:
            return [self.server.user(ecuser.Username) for ecuser in self.server.sa.GetQuotaRecipients(self.userid, 0)]
        else:
            return []

class TrackingContentsImporter(ECImportContentsChanges):
    def __init__(self, server, importer, log):
        ECImportContentsChanges.__init__(self, [IID_IExchangeImportContentsChanges, IID_IECImportContentsChanges])
        self.server = server
        self.importer = importer
        self.log = log
        self.skip = False

    def ImportMessageChangeAsAStream(self, props, flags):
        self.ImportMessageChange(props, flags)

    def ImportMessageChange(self, props, flags):
        if self.skip:
            raise MAPIError(SYNC_E_IGNORE)
        try:
            entryid = PpropFindProp(props, PR_ENTRYID)
            if self.importer.store:
                mapistore = self.importer.store.mapiobj
            else:
                store_entryid = PpropFindProp(props, PR_STORE_ENTRYID).Value
                store_entryid = WrapStoreEntryID(0, 'zarafa6client.dll', store_entryid[:-4])+self.server.pseudo_url+'\x00'
                mapistore = self.server.mapisession.OpenMsgStore(0, store_entryid, None, 0)
            item = Item()
            item.server = self.server
            item.store = Store(self.server, mapistore) # XXX public arg? improve item constructor to do more
            try:
                item.mapiobj = _openentry_raw(mapistore, entryid.Value, 0)
                item.folderid = PpropFindProp(props, PR_EC_PARENT_HIERARCHYID).Value
                props = item.mapiobj.GetProps([PR_EC_HIERARCHYID, PR_EC_PARENT_HIERARCHYID, PR_STORE_RECORD_KEY], 0) # XXX properties don't exist?
                item.docid = props[0].Value
                # item.folderid = props[1].Value # XXX 
                item.storeid = bin2hex(props[2].Value)
                self.importer.update(item, flags)
            except (MAPIErrorNotFound, MAPIErrorNoAccess): # XXX, mail already deleted, can we do this in a cleaner way?
                if self.log:
                    self.log.debug('received change for entryid %s, but it could not be opened' % bin2hex(entryid.Value))
        except Exception, e:
            if self.log:
                self.log.error('could not process change for entryid %s (%r):' % (bin2hex(entryid.Value), props))
                self.log.error(traceback.format_exc(e))
            else:
                traceback.print_exc(e)
        raise MAPIError(SYNC_E_IGNORE)

    def ImportMessageDeletion(self, flags, entries):
        if self.skip:
            return
        try:
            for entry in entries:
                item = Item()
                item.server = self.server
                item._sourcekey = bin2hex(entry)
                self.importer.delete(item, flags)
        except Exception, e:
            if self.log:
                self.log.error('could not process delete for entries: %s' % [bin2hex(entry) for entry in entries])
                self.log.error(traceback.format_exc(e))
            else:
                traceback.print_exc(e)

    def ImportPerUserReadStateChange(self, states):
        pass

    def UpdateState(self, stream):
        pass

def daemon_helper(func, service, log):
    try:
        if not service or isinstance(service, Service):
            if isinstance(service, Service): # XXX
                service.log_queue = Queue()
                service.ql = QueueListener(service.log_queue, *service.log.handlers)
                service.ql.start()
            func()
        else:
            func(service)
    finally:
        if isinstance(service, Service):
            service.ql.stop()
        if log and service:
            log.info('stopping %s', service.name)

def daemonize(func, options=None, foreground=False, args=[], log=None, config=None, service=None):
    if log and service:
        log.info('starting %s', service.name)
    if foreground or (options and options.foreground):
        try:
            if isinstance(service, Service): # XXX
                service.log_queue = Queue()
                service.ql = QueueListener(service.log_queue, *service.log.handlers)
                service.ql.start()
            func(*args)
        finally:
            if log and service:
                log.info('stopping %s', service.name)
    else:
        uid = gid = None
        working_directory = '/'
        pidfile = None
        if args:
            pidfile = '/var/run/zarafa-%s.pid' % args[0].name
        if config: 
            working_directory = config.get('running_path')
            pidfile = config.get('pid_file')
            if config.get('run_as_user'):
                uid = pwd.getpwnam(config.get('run_as_user')).pw_uid
            if config.get('run_as_group'):
                gid = grp.getgrnam(config.get('run_as_group')).gr_gid
        if pidfile: # following checks copied from zarafa-ws
            pidfile = daemon.pidlockfile.TimeoutPIDLockFile(pidfile, 10)
            oldpid = pidfile.read_pid()
            if oldpid is None:
                # there was no pidfile, remove the lock if it's there
                pidfile.break_lock()
            elif oldpid:
                try:
                    cmdline = open('/proc/%u/cmdline' % oldpid).read().split('\0')
                except IOError, error:
                    if error.errno != errno.ENOENT:
                        raise
                    # errno.ENOENT indicates that no process with pid=oldpid exists, which is ok
                    pidfile.break_lock()
#                else: # XXX can we do this in general? are there libraries to avoid having to deal with this? daemonrunner? 
#                    # A process exists with pid=oldpid, check if it's a zarafa-ws instance.
#                    # sys.argv[0] contains the script name, which matches cmdline[1]. But once compiled
#                    # sys.argv[0] is probably the executable name, which will match cmdline[0].
#                    if not sys.argv[0] in cmdline[:2]:
#                        # break the lock if it's another process
#                        pidfile.break_lock()
        with daemon.DaemonContext(
                pidfile=pidfile,
                uid=uid, 
                gid=gid,
                working_directory=working_directory,
                files_preserve=[h.stream for h in log.handlers if isinstance(h, logging.handlers.WatchedFileHandler)] if log else None, 
                prevent_core=False,
            ):
            daemon_helper(func, service, log)

def logger(service, options=None, stdout=False, config=None, name=''):
    logger = logging.getLogger(name=name or service)
    if logger.handlers:
        return logger
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_method = 'file'
    log_file = '/var/log/zarafa/%s.log' % service
    log_level = 6
    if config:
        log_method = config.get('log_method') or log_method
        log_file = config.get('log_file') or log_file
        log_level = config.get('log_level')
    if name:
        log_file = log_file.replace(service, name) # XXX
    if log_method == 'file':
        fh = logging.handlers.WatchedFileHandler(log_file)
    elif log_method == 'syslog':
        fh = logging.handlers.SysLogHandler(address='/dev/log')
    log_level = {
        0: logging.NOTSET,
        1: logging.FATAL,
        2: logging.ERROR,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.INFO,
        6: logging.DEBUG,
    }[log_level]
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler() # XXX via options?
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    if stdout or (options and options.foreground):
        logger.addHandler(ch)
    logger.setLevel(log_level)
    return logger

def parser(options='cskpUPufmv'):
    """ 
Return OptionParser instance from the standard ``optparse`` module, containing common zarafa command-line options

:param options: string containing a char for each desired option, default "cskpUPufmvV"

Available options:

-c, --config: Path to configuration file 

-s, --server-socket: Zarafa server socket address

-k, --sslkey-file: SSL key file

-p, --sslkey-password: SSL key password

-U, --auth-user: Login as user

-P, --auth-pass: Login with password

-u, --user: Run program for specific user(s)

-f, --folder: Run program for specific folder(s)

-F, --foreground: Run service in foreground

-m, --modify: Depending on program, enable database modification (python-zarafa does not check this!)

-v, --verbose: Depending on program, enable verbose output (python-zarafa does not check this!)

-V, --version: Show program version and exit
    
"""

    parser = optparse.OptionParser()

    if 'c' in options: parser.add_option('-c', '--config', dest='config_file', help='Load settings from FILE', metavar='FILE')

    if 's' in options: parser.add_option('-s', '--server-socket', dest='server_socket', help='Connect to server SOCKET', metavar='SOCKET')
    if 'k' in options: parser.add_option('-k', '--ssl-key', dest='sslkey_file', help='SSL key file', metavar='FILE')
    if 'p' in options: parser.add_option('-p', '--ssl-pass', dest='sslkey_pass', help='SSL key password', metavar='PASS')
    if 'U' in options: parser.add_option('-U', '--auth-user', dest='auth_user', help='Login as user', metavar='USER')
    if 'P' in options: parser.add_option('-P', '--auth-pass', dest='auth_pass', help='Login with password', metavar='PASS')

    if 'u' in options: parser.add_option('-u', '--user', dest='users', action='append', default=[], help='Run program for specific user(s)', metavar='USER')
    if 'f' in options: parser.add_option('-f', '--folder', dest='folders', action='append', default=[], help='Run program for specific folder(s)', metavar='FOLDER')

    if 'F' in options: parser.add_option('-F', '--foreground', dest='foreground', action='store_true', help='Run program in foreground')

    if 'm' in options: parser.add_option('-m', '--modify', dest='modify', action='store_true', help='Depending on program, enable database modification')
    if 'v' in options: parser.add_option('-v', '--verbose', dest='verbose', action='store_true', help='Depending on program, enable verbose output')
    if 'V' in options: parser.add_option('-V', '--version', dest='version', action='store_true', help='Show program version')

    return parser

@contextlib.contextmanager # it logs errors, that's all you need to know :-)
def log_exc(log):
    """
Context-manager to log any exception in sub-block to given logger instance

:param log: logger instance

Example usage::

    with log_exc(log):
        .. # any exception will be logged when exiting sub-block

"""
    try: yield
    except Exception, e: log.error(traceback.format_exc(e))

def _bytes_to_human(b):
    suffixes = ['b', 'kb', 'mb', 'gb', 'tb', 'pb']
    if b == 0: return '0 b'
    i = 0
    len_suffixes = len(suffixes)-1
    while b >= 1024 and i < len_suffixes:
        b /= 1024
        i += 1
    f = ('%.2f' % b).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])

def _human_to_bytes(s):
    '''
    Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
    License: MIT
    '''
    s = s.lower()
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for sset in [('b', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y'),
                 ('b', 'kb', 'mb', 'gb', 'tb', 'pb', 'eb', 'zb', 'yb'),
                 ('b', 'kib', 'mib', 'gib', 'tib', 'pib', 'eib', 'zib', 'yib')]:
        if letter in sset:
            break
    else:
        raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]:1}
    for i, s in enumerate(sset[1:]):
        prefix[s] = 1 << (i+1)*10
    return int(num * prefix[letter])

class ConfigOption:
    def __init__(self, type_, **kwargs):
        self.type_ = type_
        self.kwargs = kwargs

    def parse(self, key, value):
        return getattr(self, 'parse_'+self.type_)(key, value)

    def parse_string(self, key, value):
        if self.kwargs.get('multiple') == True:
            values = value.split()
        else:
            values = [value]
        for value in values:
            if self.kwargs.get('check_path') is True and not os.path.exists(value): # XXX moved to parse_path
                raise ZarafaConfigException("%s: path '%s' does not exist" % (key, value))
            if self.kwargs.get('options') is not None and value not in self.kwargs.get('options'):
                raise ZarafaConfigException("%s: '%s' is not a legal value" % (key, value))
        if self.kwargs.get('multiple') == True:
            return values
        else:
            return values[0]

    def parse_path(self, key, value):
        if self.kwargs.get('check', True) and not os.path.exists(value):
            raise ZarafaConfigException("%s: path '%s' does not exist" % (key, value))
        return value

    def parse_integer(self, key, value):
        if self.kwargs.get('options') is not None and int(value) not in self.kwargs.get('options'):
            raise ZarafaConfigException("%s: '%s' is not a legal value" % (key, value))
        if self.kwargs.get('multiple') == True:
            return [int(x, base=self.kwargs.get('base', 10)) for x in value.split()]
        return int(value, base=self.kwargs.get('base', 10))

    def parse_boolean(self, key, value):
        return {'no': False, 'yes': True, '0': False, '1': True, 'false': False, 'true': True}[value]

    def parse_size(self, key, value):
        return _human_to_bytes(value)

class Config:
    """
Configuration class

:param config: dictionary describing configuration options. TODO describe available options

Example::

    config = Config({ 
        'some_str': Config.String(default='blah'),
        'number': Config.Integer(),
        'filesize': Config.size(), # understands '5MB' etc
    })

"""
    def __init__(self, config, service=None, options=None, filename=None, log=None):
        self.config = config
        self.service = service
        self.warnings = []
        self.errors = []
        if filename:
            pass
        elif options and getattr(options, 'config_file', None):
            filename = options.config_file
        elif service:
            filename = '/etc/zarafa/%s.cfg' % service
        self.data = {}
        if self.config is not None:
            for key, val in self.config.items():
                if 'default' in val.kwargs:
                    self.data[key] = val.kwargs.get('default')
        for line in file(filename):
            line = line.strip().decode('utf-8')
            if not line.startswith('#'):
                pos = line.find('=')
                if pos != -1:
                    key = line[:pos].strip()
                    value = line[pos+1:].strip()
                    if self.config is None:
                        self.data[key] = value
                    elif key in self.config:
                        if self.config[key].type_ == 'ignore':
                            self.data[key] = None
                            self.warnings.append('%s: config option ignored' % key)
                        else:
                            try:
                                self.data[key] = self.config[key].parse(key, value)
                            except ZarafaConfigException, e:
                                if service:
                                    self.errors.append(e.message)
                                else:
                                    raise
                    else:
                        msg = "%s: unknown config option" % key
                        if service:
                            self.warnings.append(msg)
                        else:
                            raise ZarafaConfigException(msg)
        if self.config is not None:
            for key, val in self.config.items():
                if key not in self.data and val.type_ != 'ignore':
                    msg = "%s: missing in config file" % key
                    if service: # XXX merge
                        self.errors.append(msg)
                    else:
                        raise ZarafaConfigException(msg)

    @staticmethod
    def string(**kwargs):
        return ConfigOption(type_='string', **kwargs)

    @staticmethod
    def path(**kwargs):
        return ConfigOption(type_='path', **kwargs)

    @staticmethod
    def boolean(**kwargs):
        return ConfigOption(type_='boolean', **kwargs)

    @staticmethod
    def integer(**kwargs):
        return ConfigOption(type_='integer', **kwargs)

    @staticmethod
    def size(**kwargs):
        return ConfigOption(type_='size', **kwargs)

    @staticmethod
    def ignore(**kwargs):
        return ConfigOption(type_='ignore', **kwargs)

    def get(self, x, default=None):
        return self.data.get(x, default)

    def __getitem__(self, x):
        return self.data[x]

CONFIG = {
    'log_method': Config.string(options=['file', 'syslog'], default='file'),
    'log_level': Config.integer(options=range(7), default=2),
    'log_file': Config.string(default=None),
    'log_timestamp': Config.integer(options=[0,1], default=1),
    'pid_file': Config.string(default=None),
    'run_as_user': Config.string(default=None),
    'run_as_group': Config.string(default=None),
    'running_path': Config.string(check_path=True, default='/'),
    'server_socket': Config.string(default=None),
    'sslkey_file': Config.string(default=None),
    'sslkey_pass': Config.string(default=None),
}

# log-to-queue handler copied from Vinay Sajip
class QueueHandler(logging.Handler):
    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def enqueue(self, record):
        self.queue.put_nowait(record)

    def prepare(self, record):
        self.format(record)
        record.msg, record.args, record.exc_info = record.message, None, None
        return record

    def emit(self, record):
        try:
            self.enqueue(self.prepare(record))
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

# log-to-queue listener copied from Vinay Sajip
class QueueListener(object):
    _sentinel = None

    def __init__(self, queue, *handlers):
        self.queue = queue
        self.handlers = handlers
        self._stop = threading.Event()
        self._thread = None

    def dequeue(self, block):
        return self.queue.get(block)

    def start(self):
        self._thread = t = threading.Thread(target=self._monitor)
        t.setDaemon(True)
        t.start()

    def prepare(self, record):
        return record

    def handle(self, record):
        record = self.prepare(record)
        for handler in self.handlers:
            handler.handle(record)

    def _monitor(self):
        q = self.queue
        has_task_done = hasattr(q, 'task_done')
        while not self._stop.isSet():
            try:
                record = self.dequeue(True)
                if record is self._sentinel:
                    break
                self.handle(record)
                if has_task_done:
                    q.task_done()
            except Empty:
                pass
        # There might still be records in the queue.
        while True:
            try:
                record = self.dequeue(False)
                if record is self._sentinel:
                    break
                self.handle(record)
                if has_task_done:
                    q.task_done()
            except Empty:
                break

    def stop(self):
        self._stop.set()
        self.queue.put_nowait(self._sentinel)
        self._thread.join()
        self._thread = None

class Service:
    """ 
Encapsulates everything to create a simple Zarafa service, such as:

- Locating and parsing a configuration file
- Performing logging, as specifified in the configuration file
- Handling common command-line options (-c, -F)
- Daemonization (if no -F specified)

:param name: name of the service; if for example 'search', the configuration file should be called ``/etc/zarafa/search.cfg`` or passed with -c
:param config: :class:`Configuration <Config>` to use
:param options: OptionParser instance to get settings from (see :func:`parser`)

"""

    def __init__(self, name, config=None, options=None, **kwargs):
        self.name = name
        self.__dict__.update(kwargs)
        if not options:
            options, args = parser('cskpUPufmvVF').parse_args() # XXX store args?
        self.options = options
        self.name = name
        config2 = CONFIG.copy()
        if config:
            config2.update(config)
        if getattr(options, 'config_file', None):
            options.config_file = os.path.abspath(options.config_file) # XXX useful during testing. could be generalized with optparse callback?
        self.config = Config(config2, service=name, options=options)
        self.config.data['server_socket'] = os.getenv('ZARAFA_SOCKET') or self.config.data['server_socket']
        self.log = logger(self.name, options=self.options, config=self.config) # check that this works here or daemon may die silently XXX check run_as_user..?
        for msg in self.config.warnings:
            self.log.warn(msg)
        if self.config.errors:
            for msg in self.config.errors:
                self.log.error(msg)
            sys.exit(1)

    @property
    def server(self):
        return Server(options=self.options, config=self.config.data, log=self.log, service=self)

    def start(self):
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *args: sys.exit(-sig))
        with log_exc(self.log):
            daemonize(self.main, options=self.options, args=[], log=self.log, config=self.config, service=self)

class Worker(Process):
    def __init__(self, service, name, **kwargs):
        Process.__init__(self)
        self.daemon = True
        self.name = name
        self.service = service
        self.__dict__.update(kwargs)
        self.log = logging.getLogger(name=self.name)
        if not self.log.handlers:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            qh = QueueHandler(service.log_queue)
            qh.setFormatter(formatter)
            qh.setLevel(logging.DEBUG)
            self.log.addHandler(qh)
            self.log.setLevel(logging.DEBUG)

    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        with log_exc(self.log):
            self.main()

class _ZSocket: # XXX megh, double wrapper
    def __init__(self, addr, ssl_key, ssl_cert):
        self.ssl_key = ssl_key
        self.ssl_cert = ssl_cert
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind(addr)
        self.s.listen(5)

    def accept(self):
        newsocket, fromaddr = self.s.accept()
        connstream = ssl.wrap_socket(newsocket, server_side=True, keyfile=self.ssl_key, certfile=self.ssl_cert)
        return connstream, fromaddr


def server_socket(addr, ssl_key=None, ssl_cert=None, log=None): # XXX https, merge code with client_socket
    if addr.startswith('file://'):
        addr2 = addr.replace('file://', '')
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        os.system('rm -f %s' % addr2)
        s.bind(addr2)
        s.listen(5)
    elif addr.startswith('https://'):
        addr2 = addr.replace('https://', '').split(':')
        addr2 = (addr2[0], int(addr2[1]))
        s = _ZSocket(addr2, ssl_key=ssl_key, ssl_cert=ssl_cert)
    else:
        addr2 = addr.replace('http://', '').split(':')
        addr2 = (addr2[0], int(addr2[1]))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(addr2)
        s.listen(5)
    if log:
        log.info('listening on socket %s', addr)
    return s

def client_socket(addr, ssl_cert=None, log=None):
    if addr.startswith('file://'):
        addr2 = addr.replace('file://', '')
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    elif addr.startswith('https://'):
        addr2 = addr.replace('https://', '').split(':')
        addr2 = (addr2[0], int(addr2[1]))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s = ssl.wrap_socket(s, ca_certs=ssl_cert, cert_reqs=ssl.CERT_REQUIRED)
    else:
        addr2 = addr.replace('http://', '').split(':')
        addr2 = (addr2[0], int(addr2[1]))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(addr2)
    return s
