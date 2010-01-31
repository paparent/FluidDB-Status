
"""
fluiddb.db
==========

Raw connection and querying.
"""

import urllib
import httplib2
import types

try:
    import json
except ImportError:
    import simplejson as json

from api import FluidApi

BASE_URL = 'http://fluiddb.fluidinfo.com'
PRIMITIVE_CONTENT_TYPE = 'application/vnd.fluiddb.value+json'


def _generate_endpoint_url(base, path, urlargs):
    url = ''.join([base, path])
    if urlargs:
        url = '?'.join([url, urllib.urlencode(urlargs)])
    return url


def _get_body_and_type(payload, content_type):
    if content_type:
        return payload, content_type
    
    pt = type(payload)
    
    if pt in (types.NoneType, bool, int, float, str, unicode, list, tuple):
        if pt in (list, tuple):
            if not all([isinstance(x, basestring) for x in payload]):
                raise ValueError('Non-string in list payload %r.' % (payload,))
        return json.dumps(payload), PRIMITIVE_CONTENT_TYPE
    
    raise ValueError("Can't handle payload %r of type %s" % (payload, pt))


class RestClient(object):
    """HTTP client.

    Could/Should be swapped out for other implementations. Although is
    generally synchronous.
    """

    def __init__(self, db):
        self.base_url = db.base_url
        self.headers = {
            'User-agent': 'fom',
        }

    def __call__(self, method, path, payload=None, urlargs=None):
        """Make a request and return a response.
        """
        req, params = self.build_request(method, path, payload, urlargs, None)
        # print 'params: %r' % (params,)
        # print 'urlargs: %r' % (urlargs,)
        response, content = req(*params)
        if content:
            content = json.loads(content)
        else:
            content = None
        return response.status, content

    def build_request(self, method, path, payload, urlargs, content_type):
        # print 'build urlargs: %r' % (urlargs,)
        if content_type is None:
            if isinstance(payload, dict):
                content_type = 'application/json'
                payload = json.dumps(payload)
            elif payload is None:
                pass
            else:
                raise ValueError(
                    "Content type %r not compatible with payload %r" %
                    (content_type, payload))
        else:
            if payload is None:
                raise ValueError(
                    "Content type %r passed with None payload." % content_type)
            
        urlargs = urlargs or {}
        headers = self._get_headers(content_type)
        url = self._get_url(path, urlargs)
        http = httplib2.Http()
        return http.request, (url, method, payload, headers)

    def _get_headers(self, content_type):
        headers = self.headers.copy()
        if content_type:
            headers['content-type'] = content_type
        return headers

    def _get_url(self, path, urlargs=None):
        # print 'get urlargs: %r' % (urlargs,)
        return _generate_endpoint_url(self.base_url, path, urlargs)

    def put_value(self, path, value, value_type):
        body, content_type = _get_body_and_type(value, value_type)
        req, params = self.build_request('PUT', path, body, None, content_type)
        response, content = req(*params)
        return response.status
        
    def get_value(self, path):
        req, params = self.build_request('GET', path, None, None, None)
        response, content = req(*params)
        content_type = response['content-type']
        if response.status == 200:
            if response['content-type'] == PRIMITIVE_CONTENT_TYPE:
                value = json.loads(content)
                content_type = None
            else:
                value = content
            return 200, value, content_type
        else:
            return response.status, content, content_type
            

    def login(self, username, password):
        userpass = username + ':' + password
        auth = 'Basic ' + userpass.encode('base64').strip()
        self.headers['Authorization'] = auth

    def logout(self):
        del self.headers['Authorization']


class FluidDB(object):
    """A fluiddb connector.
    """

    def __init__(self, base_url=BASE_URL):
        self.base_url = base_url
        self.client = RestClient(self)

    def __call__(self, method, path, payload=None, urlargs=None, **kw):
        """Perform a call on the fluiddb.
        """
        return self.client.__call__(method, path, payload, urlargs, **kw)

    def put_value(self, path, value, value_type=None):
        """Set a tag value in fluiddb.
        """
        return self.client.put_value(path, value, value_type)

    def get_value(self, path):
        """Get a tag's value and type from fluiddb.
        """
        return self.client.get_value(path)

