
"""
fom.dev
=======

Sandbox support for fom

"""

from fom.session import Fluid

# SANDBOX_URL = 'http://localhost:6060/proxy'
SANDBOX_URL = 'http://sandbox.fluidinfo.com'

class SandboxFluid(Fluid):

    def __init__(self):
        Fluid.__init__(self, SANDBOX_URL)

def sandbox_fluid():
    fluid = SandboxFluid()
    fluid.db.client.login('test', 'test')
    fluid.bind()
    return fluid

