#!/usr/bin/env python
import ConfigParser
import sys
import re
import subprocess
import datetime
import zarafa
import shlex
delcounter = 0


def main():
    global delcounter
    learncounter = 0
    hamlearncounter = 0
    (users, allusers, remoteusers, autolearn, autodelete, deleteafter, spamcommand, hamfolder, hammarkertoremove, hamfoldercreate, hamcommand, hamlimit) = getconfig()
    z = zarafa.Server()
    if allusers and not users:
        users = []
        for user in z.users(remote=remoteusers):
            users.append(user.name)
    for username in users:
        try:
            user = z.user(username)
            inboxelements = 0
            try:
               nospamFolder = user.store.folder(hamfolder)
            except:
               if(hamfoldercreate):
                  print "%s : create ham folder [%s]" % (user.name, hamfolder)
                  nospamFolder = user.store.subtree.create_folder(hamfolder)
               else:
                  print "%s : has not ham folder [%s]" % (user.name, hamfolder)
                  continue
            p = re.compile(hammarkertoremove)
            for item in nospamFolder.items():
                if (hamlimit > 0 and inboxelements > hamlimit):
                    break
                if autolearn:
                    if (item.header('x-spam-flag') and item.header('x-spam-flag') == 'YES'):
                        inboxelements += 1
                        print "%s : tagged spam in inbox [Subject: %s]" % (user.name, item.subject)
                        try:
                            hp = subprocess.Popen(shlex.split(hamcommand), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
                            learn, ham_output_error = hp.communicate(item.eml())
                        except:
                            print "failed to run [HAM] [%s]" % ham_output_err
                        if learn:
                            item.subject=p.sub('',item.subject)
                            nospamFolder.move(item, user.store.inbox)
                            print "%s : learned [%s]" % (user.name, learn.rstrip('\n'))
                            hamlearncounter += 1
                        continue
            for item in user.store.junk.items():
                if autolearn:
                    if (not item.header('x-spam-flag')) or (item.header('x-spam-flag') == 'NO'):
                        print "%s : untagged spam [Subject: %s]" % (user.name, item.subject)
                        try:
                            p = subprocess.Popen(shlex.split(spamcommand), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
                            learn, output_err = p.communicate(item.eml())
                        except:
                            print 'failed to run [%s] [%s]' % (SPAM, output_err)
                        if learn:
                            print "%s : learned [%s]" % (user.name, learn.rstrip('\n'))
                            delmsg = 'delete after learn'
                            deletejunk(user, item, delmsg)
                            learncounter += 1
                        continue
                if autodelete:
                    if item.received.date() < (datetime.date.today()-datetime.timedelta(days=deleteafter)):
                        delmsg = 'autodelete'
                        deletejunk(user, item, delmsg)
        except Exception as error:
            print "%s : Unable to open store/item : [%s] [%s]" % (username, username, error)
            continue
    print "Summary learned %d SPAM items %d HAM items, deleted %d items" % (learncounter, hamlearncounter, delcounter)


def deletejunk(user, item, delmsg):
    global delcounter
    try:
        user.store.junk.delete([item])
        print "%s : %s [Subject: %s]" % (user.name, delmsg, item.subject)
        delcounter += 1
    except Exception as error:
        print "%s : Unable to %s item [Subject: %s] [%s]" % (user.name, delmsg, item.subject, error)
        pass
    return


def getconfig():
    Config = ConfigParser.ConfigParser()
    try:
        Config.read('zarafa-spamhandler.cfg')
        users = Config.get('users', 'users')
        remoteusers = Config.getboolean('users', 'remoteusers')
        autolearn = Config.getboolean('learning', 'autolearn')
        autodelete = Config.getboolean('deleting', 'autodelete')
        deleteafter = Config.getint('deleting', 'deleteafter')
        spamcommand = Config.get('spamcommand', 'command')
        hamfolder = Config.get('ham', 'folder')
        hamfoldercreate = Config.getboolean('ham', 'create')
        hammarkertoremove = Config.get('ham', 'marker')
        hamcommand = Config.get('ham', 'command')
        hamlimit = Config.get('ham', 'limit')

        if not users:
            allusers = True
        else:
            allusers = False
            users = users.replace(" ", "").split(",")
        return (users, allusers, remoteusers, autolearn, autodelete, deleteafter, spamcommand, hamfolder, hammarkertoremove, hamfoldercreate, hamcommand, hamlimit)
    except Exception as error:
        print error
        exit('Configuration error, please check zarafa-spamhandler.cfg')


if __name__ == '__main__':
    main()
