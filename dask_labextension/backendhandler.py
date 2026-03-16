"""
Backend handler for the Dask JupyterLab extension.

Exposes GET /dask/backends which returns the list of configured cluster
backends so the frontend can populate its backend-selector dropdown.

Response shape
--------------
[
  {"id": "local",      "display_name": "Local Cluster"},
  {"id": "htcondor",   "display_name": "HTCondor"},
  {"id": "kubernetes", "display_name": "Kubernetes"}
]
"""

import json

from jupyter_server.base.handlers import JupyterHandler
from tornado import web

from .manager import DaskClusterManager


class BackendHandler(JupyterHandler):
    """GET /dask/backends — list available cluster backends."""

    def initialize(self, manager: DaskClusterManager) -> None:
        self.cluster_manager = manager

    @web.authenticated
    def get(self) -> None:
        backends = self.cluster_manager.list_backends()
        self.finish(json.dumps(backends))

# Alias for any code that references the upstream-style name
DaskBackendHandler = BackendHandler