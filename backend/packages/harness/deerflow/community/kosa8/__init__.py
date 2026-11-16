"""kosa8 Cloud sandbox provider for DeerFlow.

Runs each DeerFlow sandbox as a `kosa8 <https://kosa8.com>`_ microVM through
kosa8's REST API, behind DeerFlow's :class:`Sandbox` / :class:`SandboxProvider`
contract. It needs no extra package: the client uses ``httpx``, a core harness
dependency.

Configuration example (``config.yaml``)::

    sandbox:
      use: deerflow.community.kosa8:Kosa8SandboxProvider
      kosa8_api_key: $KOSA8_API_KEY   # or set KOSA8_API_KEY in the Gateway environment

See ``README.md`` in this package for every option.
"""

from .provider import Kosa8SandboxProvider
from .sandbox import Kosa8Sandbox

__all__ = [
    "Kosa8Sandbox",
    "Kosa8SandboxProvider",
]
