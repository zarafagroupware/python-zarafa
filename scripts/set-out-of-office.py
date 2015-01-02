#!/usr/bin/env python
# Example to show how to set Out of Office with python-zarafa
import zarafa


def opt_args(help):
    parser = zarafa.parser('skpc')
    parser.add_option('--user', dest='user', action='store', help='User for which you want to do Out of Office actions')
    parser.add_option('--subject', dest='subject', action='store', help='Subject of the Out of Office e-mail')
    parser.add_option('--message', dest='message', action='store', help='Message of the Out of Office e-mail')
    parser.add_option('--mode', dest='mode', action='store', help='Enable or Disable Out of Office')

    if help:
        parser.print_help()
    else:
        return parser.parse_args()


def main():
    options, args = opt_args(help=False)

    if options.user:

        oof = zarafa.Server(options).user(options.user).outofoffice

        if options.user and options.mode is None and options.subject is None and options.message is None:

            if oof.enabled:
                print "User %s: Out of Office is enabled" % (options.user)
            else:
                print "User %s: Out of Office is disabled" % (options.user)

            print "User %s: Subject: (%s)" % (options.user, oof.subject)
            print "User %s: Message: (%s)" % (options.user, oof.message)

        else:

            if options.mode == 'enable':
                oof.enabled = True
                print "User %s: Enabled Out of Office" % (options.user)

            if options.mode == 'disable':
                oof.enabled = False
                print "User %s: Disabled Out of Office" % (options.user)

            if options.subject:
                oof.subject = options.subject
                print "User %s: Set subject to (%s)" % (options.user, options.subject)

            if options.message:
                oof.message = options.message
                print "User %s: Set message to (%s)" % (options.user, options.message)
    else:
        opt_args(help=True)

if __name__ == '__main__':
    main()
