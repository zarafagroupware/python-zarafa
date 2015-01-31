zarafa-spamhandler
====
A description of the zarafa-spamhandler can be found in this [blog post](http://www.zarafa.com/blog/post/2014/08/cool-scripts-and-tools-automated-spam-processing-python-zarafa-and-zarafa-spamhand)

delete\_olditems.py
=====
Allows system administrators to delete old emails.  
E.g. python delete\_olditems.py -m -u username 30  
This will delete all emails older than 30 days.

delete\_pattern.py
=====
Delete emails based on a pattern.  
E.g. python delete\_pattern.py -m -u username "I like Giraffes"  
This will delete all emails with the pattern "I like Giraffes".

examples.py
=====
Some basic examples on how to use python-zarafa

list-folder-size.py
=====
List folder sizes for a specific or all users on a server.

monitor.py
=====
WIP, re-implementation of zarafa-monitor in Python.

tables.py
=====
MAPI table examples.

zarafa-stats.py
=====

Re-implementation of zarafa-stats in Python but without --curses.

z-plot.py
====

Simple Python program that shows graphs of user statistics (for example items per user)

z-barplot.py
====

Simple Python program that shows a barplot per user (for example items per user)

z-tracer.py
===========

Z-tracer is a simple Python program that uses ICS to monitor a Folder in the users store for changes: Update/Delete/Create of an item and shows this in a diff format.
Example usage:

python z-tracer.py -U username -f Inbox (No locale support for Folders)

import\_ics.py
=============

Imports ics (calendar) files into the specified users calendar.
Example usage:

python import\_ics.py -u username --file item.ics

urwap.py
========

Simple MAPI console client
Example usage:

python urwap.py -U username -P password -s 'https://server.com:237/zarafa'

rule.py
=======

Example of a client side rule using ICS.


send.py
=======

Example of sending a simple email

set-out-of-office.py
====================

Example to show how to set Out of Office with python-zarafa
