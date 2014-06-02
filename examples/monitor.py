import time
import os
import zarafa

CONFIG = {
    'quota_check_interval': zarafa.Config.integer(default=15),
    'mailquota_resend_interval': zarafa.Config.integer(default=1),
    'userquota_warning_template': zarafa.Config.path(default='/etc/zarafa/quotamail/userwarning.mail'),
    'userquota_soft_template': zarafa.Config.path(default='/etc/zarafa/quotamail/usersoft.mail'),
    'userquota_hard_template': zarafa.Config.path(default='/etc/zarafa/quotamail/userhard.mail'),
    'companyquota_warning_template': zarafa.Config.path(default='/etc/zarafa/quotamail/companywarning.mail'),
}

class Monitor(zarafa.Service):
    def main(self):
        while True:
            self.log.info('one second..')
            self.log.info('path: %s' % os.getcwd())
            time.sleep(1)

if __name__ == '__main__':
    Monitor('monitor', config=CONFIG).start()
