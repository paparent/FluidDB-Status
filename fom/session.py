
"""
fom.session
~~~~~~~~~~~

Combining a db instance with an API.

"""

from fom.api import FluidApi
from fom.db import FluidDB


class Fluid(FluidApi):
    """A fluiddb session.
    """

    def __init__(self, base_url=None):
        if base_url is not None:
            db = FluidDB(base_url)
        else:
            db = FluidDB()
        FluidApi.__init__(self, db)

    def __call__(self, method, path, payload=None, urlargs=None, **kw):
        """Perform a call on the fluiddb.
        """
        return self.db.__call__(method, path, payload, urlargs, **kw)

    def bind(self):
        # this is particularly nasty
        Fluid.bound = self



