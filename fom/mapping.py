

"""
fom.mapping
===========

Object orientated interface into FluidDB

"""


from fom.session import Fluid




def path_child(path, child):
    """Get the named child for a path.
    """
    if path == '':
        return child
    else:
        return '/'.join([path, child])

def path_split(path):
    """Split a path into parent, self
    """
    return path.rsplit('/', 1)


class SessionBound(object):
    """Something with a path that is bound to a database.

    .. attribute:: path

        The path of the item relative to the toplevel.

    .. attribute:: fluid

        The instance of fom.session.Fluid bound to this item.
    """

    def __init__(self, path, fluid=None):
        self.path = path
        if fluid is None:
            fluid = Fluid.bound
        self.fluid = fluid


class Namespace(SessionBound):
    """A Namespace
    """

    @property
    def api(self):
        return self.fluid.namespaces[self.path]

    def delete(self):
        """Delete this namespace
        """
        status, response = self.api.delete()

    def create(self, description):
        """Create this namespace.

        :param description: The description of the Namespace
        """
        parent, name = path_split(self.path)
        parent_api = self.fluid.namespaces[parent]
        parent_api.post(name, description)

    def create_namespace(self, name, description):
        """Create a child namespace, and return it.

        :param name: The name of the Namespace to be created
        :param description: The description of the Namespace to be created
        """
        status, response = self.api.post(name, description)
        if status == 201:
            return Namespace(path_child(self.path, name))
        else:
            # print response
            pass

    def create_tag(self, name, description, indexed):
        """Create a tag in this namespace.
        """
        status, response = self.fluid.tags[self.path].post(name, description, indexed)

    def _get_description(self):
        """Get the description for a tag.
        """
        status, response = self.api.get(returnDescription=True)
        return response[u'description']

    def _set_description(self, description):
        """Set the description for a tag.
        """
        status, response = self.api.put(description)

    description = property(_get_description, _set_description)

    @property
    def tag_names(self):
        """Return the tag names.
        """
        status, response = self.api.get(returnTags=True)
        return response[u'tagNames']


    @property
    def tag_paths(self):
        """Return the tag paths
        """
        return [
            path_child(self.path, child) for child in self.tag_names
        ]

    @property
    def tags(self):
        return [
            Tag(path) for path in self.tag_paths
        ]

    @property
    def namespace_names(self):
        """Return the namespace names.
        """
        status, response = self.api.get(returnNamespaces=True)
        return response[u'namespaceNames']

    @property
    def namespace_paths(self):
        """Return the namespace paths.
        """
        return [
            path_child(self.path, child) for child in self.namespace_names
        ]

    @property
    def namespaces(self):
        """Return the child namespaces.
        """
        return [
            Namespace(path) for path in self.namespace_paths
        ]

    def tag(self, name):
        """Get a child tag.
        """
        return Tag(path_child(self.path, name))

    def namespace(self, name):
        """Get a child namespace.
        """
        return Namespace(path_child(self.path, name))


class Tag(SessionBound):
    """A Tag
    """

    @property
    def api(self):
        """The api TagApi for this instance.
        """
        return self.fluid.tags[self.path]

    def _get_description(self):
        """Get the description for a tag.
        """
        status, response = self.api.get(returnDescription=True)
        return response[u'description']

    def _set_description(self, description):
        """Set the description for a tag.
        """
        status, response = self.api.put(description)

    description = property(_get_description, _set_description)


class Object(SessionBound):
    """An object
    """

    def __init__(self, uid=None, fluid=None):
        SessionBound.__init__(self, uid, fluid)

    def create(self, about=None):
        """Create a new object.
        """
        status, response = self.fluid.objects.post(about)
        self.path = response[u'id']

    @property
    def uid(self):
        return self.path

    @property
    def api(self):
        """The api ObjectApi for this instance.
        """
        return self.fluid.objects[self.path]

    def get(self, tag):
        """Get the value of a tag.
        """
        tagpath = tag
        status, value, value_type = self.api[tagpath].get()
        if status == 200:
            return value, value_type
        else:
            return None

    def set(self, tag, value, valueType=None):
        """Set the value of a tag.
        """
        tagpath = tag
        status = self.api[tagpath].put(value, valueType)
        assert status == 204

    def has(self, tag):
        """Check if an object has a tag.
        """
        tagpath = tag
        status, response = self.api[tagpath].head()
        return status == 200

    @property
    def tag_paths(self):
        status, response = self.api.get()
        return response[u'tagPaths']

    @property
    def tags(self):
        return [Tag(path) for path in self.tag_paths]



class tag_value(object):
    """Descriptor to provide a tag value lookup on an object to simulate a
    simple attribute.
    """

    def __init__(self, tag):
        self.tagpath = tag

    def __get__(self, instance, owner):
        return instance.get(self.tagpath)

    def __set__(self, instance, value, valueType=None):
        return instance.set(self.tagpath, value, valueType)


class tag_relation(tag_value):
    """Descriptor to provide a relation lookup.

    An id is actually stored in the database.
    """

    def __init__(self, tag, object_type=Object):
        tag_value.__init__(self, tag)
        self.object_type = object_type

    def __get__(self, instance, owner):
        uid, content_type = tag_value.__get__(self, instance, owner)
        # Must be a primitive type?  (I.e., uid is a string)
        assert content_type is None
        return self.object_type(uid)

    def __set__(self, instance, value):
        return tag_value.__set__(self, instance, value.uid)
