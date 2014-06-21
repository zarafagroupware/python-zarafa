#!/usr/bin/env python
# python-zarafa: high-level Python bindings to Zarafa
#
# Copyright 2014 Zarafa and contributors, license AGPLv3 (see LICENSE file for details)
#

import contextlib
try:
    import daemon.pidlockfile
except:
    pass
import datetime
import grp
try:
    import libcommon # XXX distribute with python-mapi?
except:
    pass
import logging.handlers
from multiprocessing import Process, Queue
from Queue import Empty
import optparse
import os.path
import pwd
import socket
import sys
import threading
import traceback
import mailbox
from email.parser import Parser
import signal
import time

from MAPI.Util import *
from MAPI.Util.Generators import *
import MAPI.Tags
import _MAPICore
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

PSETID_Archive = DEFINE_GUID(0x72e98ebc, 0x57d2, 0x4ab5, 0xb0, 0xaa, 0xd5, 0x0a, 0x7b, 0x53, 0x1c, 0xb9)
NAMED_PROPS_ARCHIVER = [MAPINAMEID(PSETID_Archive, MNID_STRING, u'store-entryids'), MAPINAMEID(PSETID_Archive, MNID_STRING, u'item-entryids'), MAPINAMEID(PSETID_Archive, MNID_STRING, u'stubbed'),]

GUID_NAMESPACE = {PSETID_Archive: 'archive'}
NAMESPACE_GUID = {'archive': PSETID_Archive}

def _properties(mapiobj, namespace=None):
    result = []
    proptags = mapiobj.GetPropList(MAPI_UNICODE)
    props = mapiobj.GetProps(proptags, MAPI_UNICODE)
    for prop in props:
        result.append((prop.ulPropTag, prop.Value, PROP_TYPE(prop.ulPropTag)))
    result.sort()
    props1 =[Property(mapiobj,b,c,d) for (b,c,d) in result]
    return [p for p in props1 if not namespace or p.namespace==namespace]

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
            (steps, step) = exporter.Synchronize(step)
            changes += 1
            retry = 0
            if (steps == step) or (max_changes and changes >= max_changes):
                break;
        except MAPIError, e:
            if retry < 5:
                if log:
                    log.warn("Received a MAPI error or timeout (error=0x%x, retry=%d/5)" % (e.hr, retry))
            else:
                if log:
                    log.error("Too many retries, abandon ship")
                raise
            retry += 1
    exporter.UpdateState(stream)
    stream.Seek(0, MAPI.STREAM_SEEK_SET)
    state = bin2hex(stream.Read(0xFFFFF))
    return state

def _openentry_raw(mapistore, entryid, flags):
    try:
        return mapistore.OpenEntry(entryid, IID_IECMessageRaw, flags)
    except MAPIErrorInterfaceNotSupported: # XXX can we do this simpler/faster?
        return mapistore.OpenEntry(entryid, None, flags)

class ZarafaException(Exception):
    pass

class ZarafaConfigException(Exception):
    pass

class Server(object):
    def __init__(self, options=None, config=None, sslkey_file=None, sslkey_pass=None, server_socket=None, log=None, service=None):
        self.log = log
        # default connection
        self.sslkey_file = sslkey_file
        self.sslkey_pass = sslkey_pass
        self.server_socket = server_socket or os.getenv('ZARAFA_SOCKET', 'file:///var/run/zarafa')
        # check optional config file
        if config:
            self.sslkey_file = config.get('sslkey_file') or self.sslkey_file
            self.sslkey_pass = config.get('sslkey_pass') or self.sslkey_pass
            self.server_socket = config.get('server_socket') or self.server_socket
        # override with command-line args
        self.options = options
        if not self.options:
            self.options, args = parser('skpcumfF').parse_args() # XXX store args?
        if getattr(self.options, 'config_file', None):
            self.options.config_file = os.path.abspath(self.options.config_file) # XXX useful during testing. could be generalized with optparse callback?
        if getattr(self.options, 'config_file', None):
            cfg = globals()['Config'](None, filename=self.options.config_file) # XXX ugh
            self.server_socket = cfg.get('server_socket') or self.server_socket
            self.sslkey_file = cfg.get('sslkey_file') or self.sslkey_file
            self.sslkey_pass = cfg.get('sslkey_pass') or self.sslkey_pass
        self.server_socket = getattr(self.options, 'server_socket', None) or self.server_socket
        self.sslkey_file = getattr(self.options, 'sslkey_file', None) or self.sslkey_file
        self.sslkey_pass = getattr(self.options, 'sslkey_pass', None) or self.sslkey_pass
        while True:
            try:
                self.mapisession = OpenECSession('SYSTEM','', self.server_socket, sslkey_file=self.sslkey_file, sslkey_pass=self.sslkey_pass)
                break
            except MAPIErrorNetworkError:
                if service:
                    service.log.warn("could not connect to server at '%s', retrying in 5 sec" % self.server_socket)
                    time.sleep(5)
                else:
                    raise ZarafaException("could not connect to server at '%s'" % self.server_socket)
        self.mapistore = GetDefaultStore(self.mapisession)
        self.admin_store = Store(self, self.mapistore)
        self.sa = self.mapistore.QueryInterface(IID_IECServiceAdmin)
        self.ems = self.mapistore.QueryInterface(IID_IExchangeManageStore)
        entryid = HrGetOneProp(self.mapistore, PR_STORE_ENTRYID).Value
        self.pseudo_url = entryid[entryid.find('pseudo:'):-1] # XXX ECSERVER
        self.name = self.pseudo_url[9:]
        self._archive_sessions = {}

    def statstable_servers(self):
        st = self.mapistore.OpenProperty(PR_EC_STATSTABLE_SERVERS, IID_IMAPITable, 0, 0)
        st.SortTable(SSortOrderSet([SSort(PR_EC_STATS_SERVER_NAME, TABLE_SORT_ASCEND)], 0, 0), 0)

        # Properties which are in the table
        # [SPropValue(0x67F0001E, 'Archiver'), SPropValue(0x67F1001E, '192.168.50.154'), SPropValue(0x67F20003, 236L), SPropValue(0x67F30003, 237L), SPropValue(0x67F5001E, ''), SPropValue(0x67F6001E, 'http://192.168.50.154:236/zarafa'), SPropValue(0x67F7001E, 'https://192.168.50.154:237/zarafa'), SPropValue(0x67F8001E,'file:/ //var/run/zarafa')],
        try:
            rows = st.QueryRows(-1, 0)
            for row in rows:
                # Https url
                print PpropFindProp(row, 0x67F7001E).Value
        except MAPIErrorNotFound:
            pass

    def _archive_session(self, host):
        if host not in self._archive_sessions:
            try:
                self._archive_sessions[host] = OpenECSession('SYSTEM','', 'https://%s:237/zarafa' % host, sslkey_file=self.sslkey_file, sslkey_pass=self.sslkey_pass)
            except: # MAPIErrorLogonFailed, MAPIErrorNetworkError:
                self._archive_sessions[host] = None # XXX avoid subsequent timeouts for now
                raise ZarafaException("could not connect to server at '%s'" % host)
        return self._archive_sessions[host]

    @property
    def guid(self):
        return bin2hex(HrGetOneProp(self.mapistore, PR_MAPPING_SIGNATURE).Value)

    def user(self, name):
        return User(self, name)

    def users(self, remote=True, system=False, parse=False):
        if parse:
            if self.options.users:
                for username in self.options.users:
                    yield User(self, username)
                return
        try:
            for comp in self.sa.GetCompanyList(0):
                for user in Company(self, comp.Companyname).users():
                    yield user
        except MAPIErrorNoSupport:
            for ecuser in self.sa.GetUserList(None, 0):
                if system or ecuser.Username != 'SYSTEM':
                    if remote or ecuser.Servername in (self.name, ''):
                        yield User(self, ecuser.Username)

    def create_user(self, name, password=None, company=None):
        usereid = self.sa.CreateUser(ECUSER('%s@%s' % (name, company), password, 'email@domain.com', 'Full Name'), 0)
        return self.company(company).user('%s@%s' % (name, company))

    def company(self, name):
        return Company(self, name)

    def companies(self, remote=True):
        try:
            for comp in self.sa.GetCompanyList(0):
                yield Company(self, comp.Companyname)
        except MAPIErrorNoSupport:
            yield Company(self, u'Default')

    def create_company(self, name):
        company = ECCOMPANY(name, None)
        companyeid = self.sa.CreateCompany(company, 0)
        return self.company(name)

    def stores(self, remote=True):
        table = self.ems.GetMailboxTable(None, 0)
        table.SetColumns([PR_DISPLAY_NAME_W, PR_ENTRYID, PR_EC_STORETYPE], 0)
        for row in table.QueryRows(100, 0):
             yield Store(self, self.mapisession.OpenMsgStore(0, row[1].Value, None, MDB_WRITE), row[2].Value == ECSTORE_TYPE_PUBLIC)

    @property
    def public_store(self):
        try:
            self.sa.GetCompanyList(0)
            raise ZarafaException('request for server-wide public store in multi-company setup')
        except MAPIErrorNoSupport:
            return self.companies().next().public_store

    @property
    def state(self):
        exporter = self.mapistore.OpenProperty(PR_CONTENTS_SYNCHRONIZER, IID_IExchangeExportChanges, 0, 0)
        exporter.Config(None, SYNC_NORMAL | SYNC_CATCHUP, None, None, None, None, 0)
        steps, step = None, 0
        while steps != step:
            steps, step = exporter.Synchronize(step)
        stream = IStream()
        exporter.UpdateState(stream)
        stream.Seek(0, MAPI.STREAM_SEEK_SET)
        return bin2hex(stream.Read(0xFFFFF))

    def gab(self):
        ab = self.mapisession.OpenAddressBook(0, None, 0)
        gab = ab.OpenEntry(ab.GetDefaultDir(), None, 0)
        ct = gab.GetContentsTable(MAPI_DEFERRED_ERRORS)
        rows = ct.QueryRows(-1, 0)
        if len(rows):
            return rows
        return []

    def sync(self, importer, state, log=None, max_changes=None):
        importer.store = None
        return _sync(self, self.mapistore, importer, state, log or self.log, max_changes)

    def __unicode__(self):
        return u'Server(%s)' % self.server_socket

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Company(object):
    def __init__(self, server, name):
        self.server = server
        self.name = name
        if name != u'Default': # XXX
            try:
                self._eccompany = self.server.sa.GetCompany(self.server.sa.ResolveCompanyName(str(self.name), 0), 0) # XXX unicode?
            except MAPIErrorNotFound:
                raise ZarafaException("no such company: '%s'" % name)

    @property
    def public_store(self):
        if self.name == u'Default': # XXX 
            pubstore = GetPublicStore(self.server.mapisession)
            if pubstore is None:
                return None
            return Store(self.server, pubstore, True)
        publicstoreid = self.server.ems.CreateStoreEntryID(None, self.name, 0)
        publicstore = self.server.mapisession.OpenMsgStore(0, publicstoreid, None, MDB_WRITE)
        return Store(self.server, publicstore, True)

    def user(self, name):
        for user in self.users():
            if user.name == name:
                return User(self.server, name)

    def users(self):
        for username in AddressBook.GetUserList(self.server.mapisession, self.name if self.name != u'Default' else None, MAPI_UNICODE): # XXX serviceadmin?
            if username != 'SYSTEM':
                yield User(self.server, username)

    def create_user(self, name, password=None):
        self.server.create_user(name, password=password, company=self.name)
        return self.user('%s@%s' % (name, self.name))

    @property
    def quota(self):
        if self.name == u'Default':
            return Quota(self.server, None)
        else:
            return Quota(self.server, self._eccompany.CompanyID)

    def __unicode__(self):
        return u'Company(%s)' % self.name

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Store(object):
    def __init__(self, server, mapistore, public=False):
        self.server = server
        self.mapistore = mapistore
        self.public = public

    @property
    def guid(self):
        return bin2hex(HrGetOneProp(self.mapistore, PR_STORE_RECORD_KEY).Value)

    @property
    def inbox(self):
        return Folder(self, self.mapistore.GetReceiveFolder('IPM', 0)[0])

    @property
    def calendar(self):
        root = self.mapistore.OpenEntry(None, None, 0)
        return Folder(self, HrGetOneProp(root, PR_IPM_APPOINTMENT_ENTRYID).Value)

    @property
    def contacts(self):
        root = self.mapistore.OpenEntry(None, None, 0)
        return Folder(self, HrGetOneProp(root, PR_IPM_CONTACT_ENTRYID).Value)

    def folder(self, name): # XXX slow
        matches = [f for f in self.folders() if f.name == name]
        if len(matches) == 0:
            raise ZarafaException("no such folder: '%s'" % name)
        elif len(matches) > 1:
            raise ZarafaException("multiple folders with name '%s'" % name)
        else:
            return matches[0]

    def folders(self, recurse=False, system=False, parse=False):
        filter_names = None
        if parse:
            filter_names = self.server.options.folders
        if system:
            root = self.mapistore.OpenEntry(None, None, 0)
        else:
            ipmsubtreeid = self.mapistore.GetProps([PR_IPM_SUBTREE_ENTRYID], 0)[0]
            root = self.mapistore.OpenEntry(ipmsubtreeid.Value, IID_IMAPIFolder, MAPI_DEFERRED_ERRORS)
        table = root.GetHierarchyTable(0)
        table.SetColumns([PR_ENTRYID], TBL_BATCH)
        table.Restrict(SPropertyRestriction(RELOP_EQ, PR_FOLDER_TYPE, SPropValue(PR_FOLDER_TYPE, FOLDER_GENERIC)), TBL_BATCH)
        while True:
            rows = table.QueryRows(50, 0)
            if len(rows) == 0:
                break
            for row in rows:
                folder = Folder(self, row[0].Value)
                folder.depth = 0
                if not filter_names or folder.name in filter_names:
                    yield folder
                if recurse:
                    for subfolder in folder.folders(recurse=True, depth=1):
                        if not filter_names or folder.name in filter_names:
                            yield subfolder

    @property
    def size(self):
        return HrGetOneProp(self.mapistore, PR_MESSAGE_SIZE_EXTENDED).Value

    def config_item(self, name):
        item = Item()
        item.mapiitem = libcommon.GetConfigMessage(self.mapistore, 'Zarafa.Quota')
        return item

    def properties(self):
        return _properties(self.mapistore)

    def __unicode__(self):
        return u'Store(%s)' % self.guid

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Folder(object):
    def __init__(self, store, entryid):
        self.store = store
        self.server = store.server
        self._entryid = entryid # XXX make readable!
        self.mapifolder = store.mapistore.OpenEntry(entryid, IID_IMAPIFolder, MAPI_MODIFY)

    @property
    def entryid(self):
        return bin2hex(self._entryid)

    @property
    def folderid(self):
        return HrGetOneProp(self.mapifolder, PR_EC_HIERARCHYID).Value

    @property
    def name(self):
        return HrGetOneProp(self.mapifolder, PR_DISPLAY_NAME_W).Value

    def items(self):
        table = self.mapifolder.GetContentsTable(0)
        while True:
            rows = table.QueryRows(50, 0)
            if len(rows) == 0:
                break
            for row in rows:
                item = Item()
                item.store = self.store
                item.server = self.server
                item.mapiitem = _openentry_raw(self.store.mapistore, PpropFindProp(row, PR_ENTRYID).Value, MAPI_MODIFY)
                yield item

    def create_item(self, eml=None):
        return Item(self, eml=eml)

    def __iter__(self):
        return self.items()

    @property
    def size(self):
        size = 0
        table = self.mapifolder.GetContentsTable(0)
        table.SetColumns([PR_MESSAGE_SIZE], 0)
        table.SeekRow(BOOKMARK_BEGINNING, 0)
        rows = table.QueryRows(-1, 0)
        for row in rows:
            size += row[0].Value
        return size

    @property
    def count(self):
        return self.mapifolder.GetContentsTable(0).GetRowCount(0)

    def delete(self, items):
        if all(isinstance(item,Item) for item in items):
            self.mapifolder.DeleteMessages([item.entryid for item in items], 0, None, DELETE_HARD_DELETE)
        elif all(isinstance(item,Folder) for item in items):
            for item in items:
                self.mapifolder.DeleteFolder(item.entryid, 0, None, DEL_FOLDERS|DEL_MESSAGES)

    def folders(self, recurse=False, depth=0):
        if self.mapifolder.GetProps([PR_SUBFOLDERS], MAPI_UNICODE)[0].Value:
            table = self.mapifolder.GetHierarchyTable(MAPI_UNICODE)
            table.SetColumns([PR_ENTRYID, PR_FOLDER_TYPE, PR_DISPLAY_NAME_W], 0)
            rows = table.QueryRows(-1, 0)
            for row in rows:
                subfolder = self.mapifolder.OpenEntry(row[0].Value, None, MAPI_MODIFY)
                entryid = subfolder.GetProps([PR_ENTRYID], MAPI_UNICODE)[0].Value
                folder = Folder(self.store, entryid)
                folder.depth = depth
                yield folder
                if recurse:
                    for subfolder in folder.folders(recurse=True, depth=depth+1):
                        yield subfolder

    def create_folder(self, name):
        return self.mapifolder.CreateFolder(FOLDER_GENERIC, name, '', None, 0)

    def properties(self):
        return _properties(self.mapifolder)

    def sync(self, importer, state=None, log=None, max_changes=None):
        if state is None:
            state = (8*'\0').encode('hex').upper()
        importer.store = self.store
        return _sync(self.store.server, self.mapifolder, importer, state, log or self.log, max_changes)

    def readmbox(self, location):
        for message in mailbox.mbox(location):
            newitem = Item(self,message.__str__())

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
            newitem = Item(self,message.__str__())

    def __unicode__(self):
        return u'Folder(%s)' % self.name

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Item(object):
    def __init__(self, folder=None, eml=None, mail=None):
        # TODO: self.folder fix this!
        self.emlfile = eml
        if folder is not None:
            self.folder = folder

        # if eml is given, set Item
        if eml is not None and self.folder is not None:
            # options for CreateMessage: 0 / MAPI_ASSOCIATED
            self.mapiitem = self.folder.mapifolder.CreateMessage(None, 0)
            dopt = inetmapi.delivery_options()
            inetmapi.IMToMAPI(self.folder.store.server.mapisession, self.folder.store.mapistore, None, self.mapiitem, self.emlfile, dopt)
            self.mapiitem.SaveChanges(0)

        self._architem = None

    @property
    def _arch_item(self): # make an explicit connection to archive server so we can handle otherwise silenced errors (MAPI errors in mail bodies for example)
        if self._architem is None:
            if self.stubbed:
                ids = self.mapiitem.GetIDsFromNames(NAMED_PROPS_ARCHIVER, 0)
                PROP_STORE_ENTRYIDS = CHANGE_PROP_TYPE(ids[0], PT_MV_BINARY)
                try:
                    arch_storeid = HrGetOneProp(self.mapiitem, PROP_STORE_ENTRYIDS).Value[0] # XXX XXX multiple archives?!?!
                    arch_server = arch_storeid[arch_storeid.find('pseudo://')+9:-1]
                    arch_session = self.server._archive_session(arch_server)
                    if arch_session is None: # XXX first connection failed, no need to report about this multiple times
                        self._architem = self.mapiitem
                    else:
                        PROP_ITEM_ENTRYIDS = CHANGE_PROP_TYPE(ids[1], PT_MV_BINARY)
                        item_entryid = HrGetOneProp(self.mapiitem, PROP_ITEM_ENTRYIDS).Value[0]
                        arch_store = arch_session.OpenMsgStore(0, arch_storeid, None, 0)
                        self._architem = arch_store.OpenEntry(item_entryid, None, 0)
                except MAPIErrorNotFound: # XXX fix 'stubbed' definition!!
                    self._architem = self.mapiitem
            else:
                self._architem = self.mapiitem
        return self._architem

    @property
    def entryid(self): # XXX make readable!
        return HrGetOneProp(self.mapiitem, PR_ENTRYID).Value

    @property
    def sourcekey(self):
        if not hasattr(self, '_sourcekey'): # XXX more general caching solution
            self._sourcekey = bin2hex(HrGetOneProp(self.mapiitem, PR_SOURCE_KEY).Value)
        return self._sourcekey

    @property
    def subject(self):
        try:
            return HrGetOneProp(self.mapiitem, PR_SUBJECT_W).Value
        except MAPIErrorNotFound:
            pass

    @property
    def received(self):
        try:
            filetime = HrGetOneProp(self.mapiitem, PR_MESSAGE_DELIVERY_TIME).Value
            return datetime.datetime.utcfromtimestamp(filetime.unixtime)
        except MAPIErrorNotFound:
            pass

    @property
    def body(self):
        mapiitem = self._arch_item # XXX server already goes 'underwater'.. check details
        try:
            stream = mapiitem.OpenProperty(PR_BODY_W, IID_IStream, 0, 0)
            data = []
            while True:
                blup = stream.Read(0xFFFFF) # 1 MB
                if len(blup) == 0:
                    break
                data.append(blup)
            return ''.join(data).decode('utf-32le')
        except MAPIErrorNotFound:
            pass

    def properties(self, namespace=None):
        return _properties(self.mapiitem, namespace)
 
    @property
    def stubbed(self): # XXX check does not always work correctly yet
        ids = self.mapiitem.GetIDsFromNames(NAMED_PROPS_ARCHIVER, 0) # XXX cache folder.GetIDs..?
        PROP_STUBBED = CHANGE_PROP_TYPE(ids[2], PT_BOOLEAN)
        try:
            return HrGetOneProp(self.mapiitem, PROP_STUBBED).Value # False means destubbed
        except MAPIErrorNotFound:
            return False

    def property_(self, proptag):
        if isinstance(proptag, (int, long)):
            mapiprop = HrGetOneProp(self.mapiitem, proptag)
            return Property(self.mapiitem, proptag, mapiprop.Value, PROP_TYPE(proptag))
        else:
            namespace, name = proptag.split(':')
            for prop in self.properties(namespace=namespace):
                if prop.name == name:
                    return prop

    def attachments(self, embedded=False):
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
       return self.headers().get(name)

    def headers(self):
        # Fetches the mail headers and returns a dict
        try:
            message_headers = self.property_(PR_TRANSPORT_MESSAGE_HEADERS)
            headers = Parser().parsestr(message_headers.value, headersonly=True)
            return headers
        except MAPIErrorNotFound:
            return {}

    def recipients(self):
        table = self.mapiitem.GetRecipientTable(0)
        recipients = []
        for recipient in table.QueryRows(50, 0):
            temp = []
            for prop in recipient:
                temp.append(Property(self.mapiitem, prop.ulPropTag, prop.Value, PROP_TYPE(prop.ulPropTag)))
            recipients.append(temp)
        return recipients

    def eml(self):
        if not self.emlfile:
            sopt = inetmapi.sending_options()
            sopt.no_recipients_workaround = True
            self.emlfile = inetmapi.IMToINet(self.store.server.mapisession, None, self.mapiitem, sopt)
        return self.emlfile

    def __unicode__(self):
        return u'Item(%s)' % self.subject

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Property(object):
    def __init__(self, mapiobj, proptag, value, proptype):
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
        self._value = value
        if self.named:
            lpname = mapiobj.GetNamesFromIDs([proptag], None, 0)[0]
            self.guid = bin2hex(lpname.guid)
            self.namespace = GUID_NAMESPACE.get(lpname.guid)
            self.name = lpname.id
            self.kind = lpname.kind
            self.kindname = 'MNID_STRING' if lpname.kind == MNID_STRING else 'MNID_ID'

    def get_value(self):
        return self._value
    def set_value(self, value):
        self._value = value
        self.mapiobj.SetProps([SPropValue(self.proptag, value)])
        self.mapiobj.SaveChanges(0)
    value = property(get_value, set_value)

    @property
    def pyval(self): # XXX merge with 'value'?
        if PROP_TYPE(self.proptag) == PT_SYSTIME: # XXX generalize
            return datetime.datetime.utcfromtimestamp(self._value.unixtime)
        return self._value

    def __unicode__(self):
        return u'Property(%s, %s)' % (self.name if self.named else self.idname, repr(self.value))

    # TODO: check if data is binary and convert it to hex
    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Attachment(object):
    def __init__(self, att):
        self.att = att

    def properties(self):
        return _properties(self.att)

    @property
    def mimetag(self):
        try:
            return HrGetOneProp(self.att, PR_ATTACH_MIME_TAG).Value
        except MAPIErrorNotFound:
            pass

    @property
    def filename(self):
        try:
            return HrGetOneProp(self.att, PR_ATTACH_LONG_FILENAME_W).Value
        except MAPIErrorNotFound:
            pass

    def __len__(self):
        try:
            return int(HrGetOneProp(self.att, PR_ATTACH_SIZE).Value) # XXX why is this not equal to len(data)??
        except MAPIErrorNotFound:
            pass

    @property
    def data(self):
        try:
            method = HrGetOneProp(self.att, PR_ATTACH_METHOD).Value
            stream = self.att.OpenProperty(PR_ATTACH_DATA_BIN, IID_IStream, 0, 0)
        except MAPIErrorNotFound:
            return ''
        data = []
        while True:
            blup = stream.Read(0xFFFFF) # 1 MB
            if len(blup) == 0:
                break
            data.append(blup)
        return ''.join(data)

class User(object):
    def __init__(self, server, name):
        self.server = server
        self.name = name
        try:
            self._ecuser = self.server.sa.GetUser(self.server.sa.ResolveUserName(str(self.name), 0), 0) # XXX unicode?
        except MAPIErrorNotFound:
            raise ZarafaException("no such user: '%s'" % name)
        self.mapiuser = self.server.mapisession.OpenEntry(self._ecuser.UserID, None, 0)

    @property
    def email(self):
        return self._ecuser.Email

    @property
    def userid(self):
        return bin2hex(self._ecuser.UserID)

    @property
    def fullname(self):
        return self._ecuser.FullName

    @property
    def company(self):
        return Company(self.server, HrGetOneProp(self.mapiuser, PR_EC_COMPANY_NAME).Value or u'Default')

    @property # XXX
    def local(self):
        store = self.store
        return store and (self.server.guid == bin2hex(HrGetOneProp(store.mapistore, PR_MAPPING_SIGNATURE).Value))

    @property
    def store(self):
        try:
            storeid = self.server.ems.CreateStoreEntryID(None, str(self.name), 0)
            mapistore = self.server.mapisession.OpenMsgStore(0, storeid, IID_IMsgStore, MDB_WRITE|MAPI_DEFERRED_ERRORS)
            return Store(self.server, mapistore)
        except MAPIErrorNotFound:
            pass

    @property
    def archive_store(self):
        mapistore = self.store.mapistore
        ids = mapistore.GetIDsFromNames(NAMED_PROPS_ARCHIVER, 0) # XXX merge namedprops stuff
        PROP_STORE_ENTRYIDS = CHANGE_PROP_TYPE(ids[0], PT_MV_BINARY)
        try:
            arch_storeid = HrGetOneProp(mapistore, PROP_STORE_ENTRYIDS).Value[0] # XXX XXX multiple archives?!?!
        except MAPIErrorNotFound:
            return
        arch_server = arch_storeid[arch_storeid.find('pseudo://')+9:-1]
        arch_session = self.server._archive_session(arch_server)
        if arch_session is None:
            return
        arch_store = arch_session.OpenMsgStore(0, arch_storeid, None, MDB_WRITE)
        return Store(self.server, arch_store) # XXX server?

    @property
    def home_server(self):
        return self._ecuser.Servername

    @property
    def archive_servers(self):
       return HrGetOneProp(self.mapiuser, PR_EC_ARCHIVE_SERVERS).Value

    def properties(self):
        return _properties(self.mapiuser)

    @property
    def quota(self):
        return Quota(self.server, self._ecuser.UserID)

    def __unicode__(self):
        return u'User(%s)' % self.name

    def __repr__(self):
        return unicode(self).encode(sys.stdout.encoding or 'utf8')

class Quota(object):
    def __init__(self, server, userid):
        self.server = server
        self.userid = userid
        self.warn_limit = self.soft_limit = self.hard_limit = 0 # XXX quota for 'default' company?
        if userid:
            quota = server.sa.GetQuota(userid, False)
            self.warn_limit = quota.llWarnSize
            self.soft_limit = quota.llSoftSize
            self.hard_limit = quota.llHardSize

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

    def ImportMessageChangeAsAStream(self, props, flags):
        self.ImportMessageChange(props, flags)

    def ImportMessageChange(self, props, flags):
        try:
            entryid = PpropFindProp(props, PR_ENTRYID)
            if self.importer.store:
                mapistore = self.importer.store.mapistore
            else:
                store_entryid = PpropFindProp(props, PR_STORE_ENTRYID).Value
                store_entryid = WrapStoreEntryID(0, 'zarafa6client.dll', store_entryid[:-4])+self.server.pseudo_url+'\x00'
                mapistore = self.server.mapisession.OpenMsgStore(0, store_entryid, None, 0)
            item = Item()
            item.server = self.server
            try:
                item.mapiitem = _openentry_raw(mapistore, entryid.Value, 0)
                item.folderid = PpropFindProp(props, PR_EC_PARENT_HIERARCHYID).Value
                props = item.mapiitem.GetProps([PR_EC_HIERARCHYID, PR_EC_PARENT_HIERARCHYID, PR_STORE_RECORD_KEY], 0) # XXX properties niet aanwezig?
                item.docid = props[0].Value
#            item.folderid = props[1].Value # XXX 
                item.storeid = bin2hex(props[2].Value)
                self.importer.update(item, flags)
            except MAPIErrorNotFound, MAPIErrorNoAccess: # XXX, mail already deleted, can we do this in a cleaner way?
                if self.log:
                    self.log.debug('received change for entryid %s, but it could not be opened' % bin2hex(entryid.Value))
        except Exception, e:
            if self.log:
                self.log.error('could not process change for entryid %s (%r):' % (bin2hex(entryid.Value), props))
                self.log.error(traceback.format_exc(e))
        raise MAPIError(SYNC_E_IGNORE)

    def ImportMessageDeletion(self, flags, entries):
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
            log.info('stopping %s' % service.name)

def daemonize(func, options=None, foreground=False, args=[], log=None, config=None, service=None):
    if log and service:
        log.info('starting %s' % service.name)
    if foreground or (options and options.foreground):
        try:
            if isinstance(service, Service): # XXX
                service.log_queue = Queue()
                service.ql = QueueListener(service.log_queue, *service.log.handlers)
                service.ql.start()
            func(*args)
        finally:
            if log and service:
                log.info('stopping %s' % service.name)
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
        if pidfile:
            pidfile = daemon.pidlockfile.TimeoutPIDLockFile(pidfile, 10)
#            pidfile.break_lock() # XXX add checks? see zarafa-ws
        with daemon.DaemonContext(
                pidfile=pidfile, 
                uid=uid, 
                gid=gid, 
                working_directory=working_directory, 
                files_preserve=[h.stream for h in log.handlers if isinstance(h, logging.FileHandler)] if log else None, 
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
        fh = logging.FileHandler(log_file)
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

def parser(opts='cmskpu'):
    parser = optparse.OptionParser()
    if 's' in opts: parser.add_option('-s', '--server', dest='server_socket', help='Connect to server HOST', metavar='HOST')
    if 'k' in opts: parser.add_option('-k', '--sslkey-file', dest='sslkey_file', help='SSL key file')
    if 'p' in opts: parser.add_option('-p', '--sslkey-pass', dest='sslkey_pass', help='SSL key password')
    if 'c' in opts: parser.add_option('-c', '--config', dest='config_file', help='Load settings from FILE', metavar='FILE')
    if 'u' in opts: parser.add_option('-u', '--user', dest='users', action='append', default=[], help='Run program for specific user(s)', metavar='USER')
    if 'F' in opts: parser.add_option('-F', '--foreground', dest='foreground', action='store_true', help='Run program in foreground')
    if 'f' in opts: parser.add_option('-f', '--folder', dest='folders', action='append', default=[], help='Run program for specific folder(s)', metavar='FOLDER')
    if 'm' in opts: parser.add_option('-m', '--modify', dest='modify', action='store_true', help='Actually modify database')
    return parser

@contextlib.contextmanager # XXX it logs errors, that's all you need to know :-)
def log_exc(log):
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

    def prepare(self , record):
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
    def __init__(self, name, config=None, options=None, **kwargs):
        self.name = name
        self.__dict__.update(kwargs)
        if not options:
            options, args = parser('skpcumfF').parse_args() # XXX store args?
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

def server_socket(addr, log=None): # XXX https, merge code with client_socket
    if addr.startswith('file://'):
        addr2 = addr.replace('file://', '')
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        os.system('rm -f %s' % addr2)
    else:
        addr2 = addr.replace('http://', '').split(':')
        addr2 = (addr2[0], int(addr2[1]))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(addr2)
    s.listen(5)
    if log:
        log.info('listening on socket %s' % addr)
    return s

def client_socket(addr, log=None):
    if addr.startswith('file://'):
        addr2 = addr.replace('file://', '')
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    else:
        addr2 = addr.replace('http://', '').split(':')
        addr2 = (addr2[0], int(addr2[1]))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(addr2)
    return s
