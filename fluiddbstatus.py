#!/usr/bin/env python

import pickle
import os
import sys
import logging
import ConfigParser
from httplib import HTTPConnection, socket
from fom.session import Fluid
import twitter

def get_site_status(url):
    response = get_response(url)
    try:
        if response.status in (200, 302):
            return 'up'
    except AttributeError:
        pass
    loggin.error('DOWN: %s status: %s', url, response.status)
    return 'down'

def get_response(url):
    '''Return response object from URL'''
    try:
        conn = HTTPConnection(url)
        conn.request('HEAD', '/')
        return conn.getresponse()
    except socket.error:
        return None
    except:
        logging.error('Bad URL: %s', url)
        exit(1)

def load_old_results(file_path):
    '''Attempts to load most recent results'''
    pickledata = {}
    if os.path.isfile(file_path):
        picklefile = open(file_path, 'rb')
        pickledata = pickle.load(picklefile)
        picklefile.close()
    return pickledata

def store_results(file_path, data):
    '''Pickles results to compare on next run'''
    output = open(file_path, 'wb')
    pickle.dump(data, output)
    output.close()

def is_internet_reachable():
    '''Checks if Google is down'''
    return get_site_status('www.google.com') == 'up'

class  FluidDBConnection(object):
    def __init__(self, shortname, url):
        self.shortname = shortname
        self.url = url

    def test_user(self):
        "Check if fluiddb user exists"
        fdb = Fluid(self.url)
        try:
            ret = fdb.__call__('GET', '/users/fluiddb')
            if ret[0] == 200:
                return (True,
                        '%s instance is now reachable' % self.shortname.capitalize(),
                        ret[1]['id'])
        except socket.error:
            return (False, '%s instance is unreachable' % self.shortname.capitalize())
        except:
            pass
        return (False, 'Wrong thing happends on %s instance' % self.shortname)


maininstance = FluidDBConnection('main', 'http://fluiddb.fluidinfo.com')
sandbox = FluidDBConnection('sandbox', 'http://sandbox.fluidinfo.com')


tests = [
    ('Test FluidDB user', maininstance.test_user, 'FluidDB user on main has changed id'),
    ('Test FluidDB Sandbox user', sandbox.test_user, 'FluidDB user on sandbox has changed id'),
        ]

def main():
    config = ConfigParser.RawConfigParser()
    config.read(os.path.join(os.path.expanduser('~'), '.fluiddbstatus.rc'))

    twitterusername = config.get('twitter', 'username')
    twitterpassword = config.get('twitter', 'password')
    pickle_file = config.get('core', 'datafile')
    log_file = config.get('core', 'logfile')
    
    logging.basicConfig(level=logging.INFO, filename=log_file,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

    if not is_internet_reachable():
        logging.error('Not connected to the net')
        exit(1)

    pickledata = load_old_results(pickle_file)

    twit = twitter.Api(username=twitterusername, password=twitterpassword)

    for testname, testfunc, testchangedmsg in tests:
        ret = testfunc()
        pickleidx = 'laststatus-' + testname
        if ret[0] is False:
            testresult = 'fail'
            logging.error('%s: fail', testname)
        else:
            testresult = 'pass'
            logging.info('%s: passed!', testname)
            

        if pickleidx in pickledata and pickledata[pickleidx] != testresult:
            twit.PostUpdate(ret[1])
            logging.info(ret[1])
            pickledata[pickleidx] = testresult

        if testresult == 'pass':
            pickleidx = 'change-' + testname 
            if pickleidx in pickledata and pickledata[pickleidx] != ret[2]:
                message = '%s: %s' % (testchangedmsg, ret[2])
                twit.PostUpdate(message)
                logging.info(message)
            pickledata[pickleidx] = ret[2]

    store_results(pickle_file, pickledata)

if __name__ == '__main__':
    main()

