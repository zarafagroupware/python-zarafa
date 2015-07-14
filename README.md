# python-zarafa
High-level Python bindings to Zarafa.

# Dependencies
python-zarafa depends on the following modules:

* Python 2.6 or higher
* [python-mapi](http://download.zarafa.com/community/final/7.1/7.1.9-44333/) (Provided by ZCP)
* [python-daemon](https://pypi.python.org/pypi/python-daemon/)
* [python-vobject](http://vobject.skyhouseconsulting.com/) Optional dependency for exporting MAPI items to vcard format (for contact.vcf())

# Preface
Please keep in mind that this API is written to work on the latest version of Zarafa.
Certain features may not work on older versions.
The version on GitHub is taken directly from our 'trunk' branch.

> __Currently it is not recommended to install this release on a Zarafa server which runs a Zarafa release which provides a python-zarafa package such as Zarafa 7.2.__

In certain cases the system installation of python-zarafa can cause an issue in existing provided software.

# Installation
To install python-zarafa just simply run the following command.
```
$ python setup.py install
```
# Stand alone usage
Certain scripts require the more advanced features offered by the more recent version on GitHub.
If you require the more advanced features you can do the following.
Extract the download from GitHub.
````
$ mkdir python-zarafa
$ cp python-zarafa-master/zarafa/__init__.py python-zarafa/zarafa.py
$ cp myadvancedscript.py python-zarafa
$ cd python-zarafa
$ ./myadvancesscript.py
````
This way you can use the GitHub version with your scripts without interfering whith the python-zarafa as provided by  Zarafa.

# Documentation
To generate documentation just simply run the following command or visit the [following url](http://doc.zarafa.com/trunk/Python_Zarafa/).

```bash
cd doc
make singlehtml
firefox _build/singlehtml/index.html
```

# Usage

```python
import zarafa

# connect to server, as specified on command-line (possibly including SSL info) or via defaults
server = zarafa.Server()

# loop over users on this server, with 'parse' checking the command-line for specific user names
for user in server.users(parse=True):
# some basic user info
print user.name, user.email, user.store.guid

# print an indented overview of all folders in the user's default store
for folder in user.store.folders(recurse=True):
    print folder.depth*'    '+folder.name

# print an overview of attachments in inbox
for item in user.store.inbox:
    print item.subject
    for attachment in item.attachments():
        print attachment.filename, len(attachment.data)

# dive into MAPI if needed
print user.props(), user.store.props()
```

# Example script listing folders of users
The following script lists the all the folders of all users on a remote server using SSL.

    python scripts/list-folder-size.py -s https://remoteip:237/zarafa -k /etc/zarafa/ssl/server.pem -p password

Or using the /etc/zarafa/admin.cfg configuration file and specifying a user

    python scripts/list-folder-size.py -c /etc/zarafa/admin.cfg --user user1

Contents of list-folder-size.py

```python
#!/usr/bin/env python
import zarafa

def main():
    for user in zarafa.Server().users(parse=True):
        print 'user:', user.name
        if user.store:
            for folder in user.store.folders(recurse=True, system=True):
                print 'regular: count=%s size=%s %s%s' % (str(folder.count).ljust(8), str(folder.size).ljust(10), folder.depth*'    ', folder.name)

if __name__ == '__main__':
    main()
```
