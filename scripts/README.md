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
