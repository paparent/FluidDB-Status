# encoding=utf8

"""
fom.api
=======

Thin wrapper over the fluiddb restish api. This module attempts to provide an
api that is:

    * loyal to fluiddb
    * low-level

Other, more friendly objectish APIs can be built on this.

Example:

>>> from fom.session import Fluid
>>> fluid = Fluid('http://sandbox.fluidinfo.com')
>>> fluid.users['test'].get()
(200, {u'name': u'test', u'id': ...})
>>> fluid.namespaces['test'].get()
(200, {u'id': u'id': ...})


TODO:

* tags



"""


class ApiBase(object):
    """Base class for an api component.

    Stores the db instance as state and uses it to call the required call.
    """

    def __init__(self, db):
        self.db = db

    def _make_path(self, path):
        if path is None:
            path = self.root_path
        else:
            path = '/'.join([self.root_path, path])
        return path
    
    def __call__(self, method, path=None, payload=None, urlargs=None, **kw):
        path = self._make_path(path)
        return self.db(method, path, payload, urlargs, **kw)

    def put_value(self, path=None, value=None, value_type=None):
        path = self._make_path(path)
        return self.db.put_value(path, value, value_type)

    def get_value(self, path=None):
        path = self._make_path(path)
        return self.db.get_value(path)


class UserApi(ApiBase):
    """API component for a single user.

    http://api.fluidinfo.com/fluidDB/api/*/users/*
    """
    root_path = '/users'

    def __init__(self, username, db):
        self.username = username
        self.db = db

    def get(self):
        """Return information about the user.

        http://api.fluidinfo.com/fluidDB/api/*/users/GET
        """
        return self('GET', self.username)


class UsersApi(ApiBase):
    """API component for users.

    This is a container API that handles getting the path for individual
    users.
    """

    def __getitem__(self, key):
        return UserApi(key, self.db)


class ObjectTagApi(ApiBase):
    """API component for a tag on an object.

    .. seealso:: `<http://api.fluidinfo.com/fluidDB/api/*/objects/*>`_
    """

    root_path = '/objects'

    def __init__(self, uid, tagpath, db):
        self.uid = uid
        self.path = '/'.join([self.uid, tagpath])
        self.db = db

    def get(self):
        """Call GET on an individual object's tag.

        .. seealso:: `<http://api.fluidinfo.com/fluidDB/api/*/objects/GET>`_
        """
        return self.get_value(self.path)

    def head(self):
        """Call HEAD on an individial object's tag.

        .. seealso:: `<http://api.fluidinfo.com/fluidDB/api/*/objects/HEAD>`_
        """
        return self('HEAD', self.path)

    def delete(self):
        """Call DELETE on an individual object's tag.

        .. seealso:: `<http://api.fluidinfo.com/fluidDB/api/*/objects/DELETE>`_
        """
        return self('DELETE', self.path)

    def put(self, value, value_type=None):
        """Call PUT on an individual object's tag.

        http://api.fluidinfo.com/fluidDB/api/*/objects/PUT
        """
        return self.put_value(self.path, value, value_type)


class ObjectApi(ApiBase):
    """API component for a single object.

    http://api.fluidinfo.com/fluidDB/api/*/objects/*
    """

    root_path = '/objects'

    def __init__(self, uid, db):
        self.uid = uid
        self.db = db

    def get(self, showAbout=False):
        """Call GET on an individual object.

        http://api.fluidinfo.com/fluidDB/api/*/objects/GET
        """
        return self('GET', self.uid, urlargs={'showAbout':showAbout})

    def __getitem__(self, tagpath):
        return ObjectTagApi(self.uid, tagpath, self.db)


class ObjectsApi(ApiBase):
    """API Component for the /objects toplevel
    """

    root_path = '/objects'

    def get(self, query):
        """Call GET on the /objects toplevel

        http://api.fluidinfo.com/fluidDB/api/*/objects/GET
        """
        return self('GET', path=None, payload=None, urlargs={'query': query})

    def post(self, about=None):
        """Call POST on the /objects toplevel to create a new object.

        :param about: The value for the `about` tag. If this is omitted, or
        None is passed, no `about` tag will be set.

        .. seealso:: `<http://api.fluidinfo.com/fluidDB/api/*/objects/POST>`_
        """
        payload = {}
        if about is not None:
            payload[u'about'] = about
        return self('POST', path=None, payload=payload)

    def __getitem__(self, key):
        """Dict-like access for objects by ID.
        """
        return ObjectApi(key, self.db)


class NamespaceApi(ApiBase):
    """API Component for a single Namespace.

    This is usually returned by using the NamespacesApi[path], or can be
    created as required for a specific namespace:

    >>> from fom.db import FluidDB
    >>> db = FluidDB()
    >>> ns_api = NamespaceApi('fluiddb', db) # api for the fluiddb namespace
    >>> ns_api.get(returnDescription=True)

    `<http://api.fluidinfo.com/fluidDB/api/*/namespaces/*>`_
    """

    root_path = '/namespaces'


    def __init__(self, path, db):
        self.db = db
        self.path = path

    def get(self, returnDescription=False, returnNamespaces=False,
                  returnTags=False):
        """
        Call GET on the namespaces to fetch information about it.

        http://api.fluidinfo.com/fluidDB/api/*/namespaces/GET
        """
        urlargs = {
            'returnDescription': returnDescription,
            'returnNamespaces': returnNamespaces,
            'returnTags': returnTags,
        }
        return self('GET', self.path, urlargs=urlargs)

    def post(self, name, description):
        """
        Call POST on the namespace to create a new child namespace.

        http://api.fluidinfo.com/fluidDB/api/*/namespaces/POST
        """
        return self('POST', self.path, {'name':name, 'description':description})

    def delete(self):
        """
        Call DELETE on the namespace to delete it.

        http://api.fluidinfo.com/fluidDB/api/*/namespaces/DELETE
        """
        return self('DELETE', self.path)

    def put(self, description):
        """
        Call PUT on ther namespace to update its description.

        http://api.fluidinfo.com/fluidDB/api/*/namespaces/PUT
        """
        return self('PUT', self.path, {u'description': description})


class NamespacesApi(ApiBase):
    """API Component for the /namespaces target.

    Provides no methods, only access to named namespaces.
    """

    def __getitem__(self, key):
        """Dict-like access.

        Returns an API component for the namespace path in key.
        """
        return NamespaceApi(key, self.db)


#TODO
class TagApi(ApiBase):
    """API Componsne for a single tag.
    """

    root_path = '/tags'

    def __init__(self, path, db):
        self.path = path
        self.db = db

    def get(self, returnDescription=False):
        return self('GET', self.path,
            urlargs={u'returnDescription': returnDescription})

    def post(self, name, description, indexed):
        return self('POST', self.path, payload=
            {u'name': name, u'description': description, u'indexed': indexed})

    def delete(self):
        return self('DELETE', self.path)

    def put(self, description):
        return self('PUT', self.path, payload={u'description': description})


class TagsApi(ApiBase):
    """API Component for /tags toplevel.
    """

    root_path = '/tags'

    def __getitem__(self, key):
        """Get the API component for the tag with the given path.
        """
        return TagApi(key, self.db)


class ItemPermissionsApi(ApiBase):
    """API component for all individual permissions.
    """

    def __init__(self, root_path, path, db):
        self.root_path = root_path
        self.path = path
        self.db = db

    def put(self, action, policy, exceptions):
        return self('PUT', self.path,
            payload={u'policy': policy, u'exceptions': exceptions},
            urlargs={u'action': action})

    def get(self, action):
        return self('GET', self.path, urlargs={u'action': action})


class ItemsPermissionsApi(ApiBase):
    """API component for all groups of permissions for a type of toplevel.
    """

    def __init__(self, root_path, db):
        self.root_path = root_path
        self.db = db

    def __getitem__(self, key):
        return ItemPermissionsApi(self.root_path, key, self.db)


class PermissionsApi(ApiBase):
    """API Component for /permissions toplevel.
    """
    root_path = '/permissions'

    def __init__(self, db):
        self.db = db
        self.namespaces = ItemsPermissionsApi('/permissions/namespaces', self.db)
        self.tags = ItemsPermissionsApi('/permissions/tags', self.db)
        self.tag_values = ItemsPermissionsApi('/permissions/tag-values', self.db)


class PolicyApi(ApiBase):
    """API Component for a specific permission
    """

    root_path = '/policies'

    def __init__(self, username, category, action, db):
        self.db = db
        self.username = username
        self.category = category
        self.action = action

    @property
    def path(self):
        """Get the path of this component relative to the toplevel.
        """
        return '/'.join([self.username, self.category, self.action])

    def get(self):
        """Call get on the Policy.

        .. seealso::

            `<http://api.fluidinfo.com/fluidDB/api/*/policies/GET>`_
        """
        return self('GET', self.path)

    def put(self, policy, exceptions):
        """Call put on the policy.

        .. seealso::

            `<http://api.fluidinfo.com/fluidDB/api/*/policies/PUT>`_
        """
        return self('PUT', self.path, payload={u'policy': policy,
                                               u'exceptions':exceptions})


class PoliciesApi(ApiBase):
    """API Component for the /policies toplevel.
    """

    root_path = '/policies'

    def __getitem__(self, key):
        # key should be a tuple of username, category, action
        if len(key) == 3:
            username, category, action = key
            return PolicyApi(username, category, action, self.db)


class FluidApi(object):
    """Complete fluiddb api

    provides the API toplevels.

    The API toplevels are each bound to the instance of
    :class:`fom.db.FluidDB`, and correspond to FluidDB's toplevels.

    .. attribute:: tags

        An instance of :class:`fom.api.TagsApi`.

    .. attribute:: namespaces

        An instance of :class:`fom.api.NamespacesApi`.
    """

    def __init__(self, db):
        self.db = db
        self.tags = TagsApi(self.db)
        self.namespaces = NamespacesApi(self.db)
        self.users = UsersApi(self.db)
        self.objects = ObjectsApi(self.db)
        self.permissions = PermissionsApi(self.db)
        self.policies = PoliciesApi(self.db)

