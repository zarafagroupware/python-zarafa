#!/usr/bin/env python
import ConfigParser
import datetime
import re
import shlex
import subprocess

import zarafa

delcounter = 0


def main():
    global delcounter
    learncounter = 0
    hamlearncounter = 0
    (users, allusers, remoteusers, autolearn, autodelete, deleteafter, spamcommand, hamfolder, hammarkertoremove,
     hamfoldercreate, hamcommand, hamlimit, autoham) = getconfig()
    z = zarafa.Server()
    if allusers and not users:
        users = []
        for user in z.users(remote=remoteusers):
            users.append(user.name)
    for username in users:
        try:
            user = z.user(username)
            inboxelements = 0
            print autoham
            if autoham:
                try:
                    nospamfolder = user.store.folder(hamfolder)
                except:
                    if hamfoldercreate:
                        print "%s : create ham folder [%s]" % (user.name, hamfolder)
                        nospamfolder = user.store.subtree.create_folder(hamfolder)
                    else:
                        print "%s : has no ham folder [%s]" % (user.name, hamfolder)

                p = re.compile(hammarkertoremove)
                for item in nospamfolder.items():
                    if 0 < hamlimit < inboxelements:
                        break
                    if autolearn:
                        if item.header('x-spam-flag') and item.header('x-spam-flag') == 'YES':
                            inboxelements += 1
                            print "%s : tagged spam in inbox [Subject: %s]" % (user.name, item.subject)
                            try:
                                hp = subprocess.Popen(shlex.split(hamcommand), stdin=subprocess.PIPE,
                                                      stdout=subprocess.PIPE)
                                learn, ham_output_error = hp.communicate(item.eml())
                            except:
                                print "failed to run [HAM] [%s]" % ham_output_err
                            if learn:
                                item.subject = p.sub('', item.subject)
                                nospamfolder.move(item, user.store.inbox)
                                print "%s : learned [%s]" % (user.name, learn.rstrip('\n'))
                                hamlearncounter += 1

            for item in user.store.junk.items():
                if autolearn:
                    if (not item.header('x-spam-flag')) or (item.header('x-spam-flag') == 'NO'):
                        print "%s : untagged spam [Subject: %s]" % (user.name, item.subject)
                        try:
                            p = subprocess.Popen(shlex.split(spamcommand), stdin=subprocess.PIPE,
                                                 stdout=subprocess.PIPE)
                            learn, output_err = p.communicate(item.eml())
                        except:
                            print 'failed to run [SPAM] [%s]' % output_err
                        if learn:
                            print "%s : learned [%s]" % (user.name, learn.rstrip('\n'))
                            delmsg = 'delete after learn'
                            deletejunk(user, item, delmsg)
                            learncounter += 1
                        continue
                if autodelete:
                    if item.received.date() < (datetime.date.today() - datetime.timedelta(days=deleteafter)):
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
    config = ConfigParser.ConfigParser()
    try:
        config.read('zarafa-spamhandler.cfg')
        users = config.get('users', 'users')
        remoteusers = config.getboolean('users', 'remoteusers')
        autolearn = config.getboolean('learning', 'autolearn')
        autodelete = config.getboolean('deleting', 'autodelete')
        deleteafter = config.getint('deleting', 'deleteafter')
        spamcommand = config.get('spamcommand', 'command')
        hamfolder = config.get('ham', 'folder')
        hamfoldercreate = config.getboolean('ham', 'create')
        hammarkertoremove = config.get('ham', 'marker')
        hamcommand = config.get('ham', 'command')
        hamlimit = config.get('ham', 'limit')
        autoham = config.getboolean('ham', 'autoham')

        if not users:
            allusers = True
        else:
            allusers = False
            users = users.replace(" ", "").split(",")
        return (
            users, allusers, remoteusers, autolearn, autodelete, deleteafter, spamcommand, hamfolder, hammarkertoremove,
            hamfoldercreate, hamcommand, hamlimit, autoham)
    except Exception as error:
        print error
        exit('Configuration error, please check zarafa-spamhandler.cfg')


if __name__ == '__main__':
    main()
