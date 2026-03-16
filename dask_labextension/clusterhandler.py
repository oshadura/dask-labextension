"""
Cluster handler for the Dask JupyterLab extension.

Modifications vs upstream
--------------------------
* ``post()`` now reads an optional ``"backend"`` key from the JSON body and
  passes it to ``manager.start_cluster()`` so the correct backend config is
  used.
* ``get()`` on a cluster list now includes the ``"backend"`` field in each
  model.

REST surface (unchanged from upstream)
---------------------------------------
GET    /dask/clusters          → list all clusters
POST   /dask/clusters          → create a cluster   ← NEW: accepts {backend: id}
GET    /dask/clusters/<id>     → get one cluster
PATCH  /dask/clusters/<id>     → scale / adapt
DELETE /dask/clusters/<id>     → delete cluster
"""

import json

from jupyter_server.base.handlers import JupyterHandler
from tornado import web

from .manager import DaskClusterManager


class ClusterHandler(JupyterHandler):
    """Handler for /dask/clusters[/<cluster_id>]."""

    def initialize(self, manager: DaskClusterManager) -> None:
        self.cluster_manager = manager

    @web.authenticated
    def get(self, cluster_id: str = "") -> None:
        if cluster_id:
            model = self.cluster_manager.get_cluster(cluster_id)
            if model is None:
                raise web.HTTPError(404, f"Cluster {cluster_id!r} not found")
            self.finish(json.dumps(model))
        else:
            self.finish(json.dumps(self.cluster_manager.list_clusters()))

    @web.authenticated
    async def post(self, cluster_id: str = "") -> None:
        """Create a new cluster.

        Optional JSON body::

            {
              "backend": "htcondor",   // id from GET /dask/backends
              "name":    "My Cluster"  // optional display name
            }

        If ``"backend"`` is omitted the first configured backend is used
        (preserves upstream behaviour for clients that send no body).
        """
        body: dict = {}
        if self.request.body:
            try:
                body = json.loads(self.request.body.decode("utf-8"))
            except json.JSONDecodeError:
                raise web.HTTPError(400, "Invalid JSON body")

        backend_id = body.get("backend", None)
        cluster_name = body.get("name", None)

        try:
            model = self.cluster_manager.start_cluster(
                cluster_name=cluster_name,
                backend_id=backend_id,
            )
        except ValueError as exc:
            raise web.HTTPError(400, str(exc))
        except Exception as exc:
            raise web.HTTPError(500, f"Failed to start cluster: {exc}")

        self.set_status(200)
        self.finish(json.dumps(model))

    @web.authenticated
    async def patch(self, cluster_id: str = "") -> None:
        """Scale an existing cluster.

        JSON body::

            {"workers": 10}            // fixed scale
            {"adapt": {"minimum": 2, "maximum": 20}}  // adaptive
        """
        if not cluster_id:
            raise web.HTTPError(400, "cluster_id required for PATCH")

        body = json.loads(self.request.body.decode("utf-8"))
        workers = body.get("workers")
        adapt = body.get("adapt")

        model = self.cluster_manager.scale_cluster(
            cluster_id, workers=workers, adapt=adapt
        )
        if model is None:
            raise web.HTTPError(404, f"Cluster {cluster_id!r} not found")

        self.finish(json.dumps(model))

    @web.authenticated
    async def delete(self, cluster_id: str = "") -> None:
        if not cluster_id:
            raise web.HTTPError(400, "cluster_id required for DELETE")

        model = self.cluster_manager.close_cluster(cluster_id)
        if model is None:
            raise web.HTTPError(404, f"Cluster {cluster_id!r} not found")

        self.set_status(204)
        self.finish()

# Alias expected by upstream __init__.py
DaskClusterHandler = ClusterHandler