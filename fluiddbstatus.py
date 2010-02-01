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

def test_fluiddb_user():
    '''Checks if fluiddb user exists'''
    fdb = Fluid()
    try:
        ret = fdb.__call__('GET', '/users/fluiddb')
        if ret[0] == 200:
            return (True, 'Main instance is now reachable', ret[1]['id'])
    except socket.error:
        return (False, 'Main instance is unreachable')
    except:
        pass
    return (False, 'Wrong thing happends on main instance')

def test_fluiddb_sandbox_user():
    '''Checks if fluiddb user exists'''
    fdb = Fluid('http://sandbox.fluidinfo.com')
    try:
        ret = fdb.__call__('GET', '/users/fluiddb')
        if ret[0] == 200:
            return (True, 'Sandbox instance is now reachable', ret[1]['id'])
    except socket.error:
        return (False, 'Sandbox instance is unreachable')
    except:
        pass
    return (False, 'Wrong thing happends on sandbox instance')

tests = (
        ('Test FluidDB user', test_fluiddb_user, 'FluidDB user on main has changed id')
        ,('Test FluidDB Sandbox user', test_fluiddb_sandbox_user, 'FluidDB user on sandbox has changed id')
        )

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
        if ret[0] is False:
            logging.error('%s: fail', testname)

            pickleidx = 'laststatus-' + testname
            if pickleidx in pickledata and pickledata[pickleidx] != 'fail':
                twit.PostUpdate(ret[1])
                logging.info(ret[1])
            pickledata[pickleidx] = 'fail'
        else:
            logging.info('%s: passed!', testname)

            pickleidx = 'laststatus-' + testname
            if pickleidx in pickledata and pickledata[pickleidx] != 'pass':
                twit.PostUpdate(ret[1])
                logging.info(ret[1])
            pickledata[pickleidx] = 'pass'

            pickleidx = 'change-' + testname 
            if pickleidx in pickledata and pickledata[pickleidx] != ret[2]:
                message = '%s: %s' % (testchangedmsg, ret[2])
                twit.PostUpdate(message)
                logging.info(message)
            pickledata[pickleidx] = ret[2]

    store_results(pickle_file, pickledata)

if __name__ == '__main__':
    main()

