"""
Jupyter server extension entry-point.

Registers the Dask REST handlers with the Jupyter server.
Added route: GET /dask/backends
"""

from jupyter_server.utils import url_path_join

from .backendhandler import BackendHandler
from .clusterhandler import ClusterHandler
from .dashboardhandler import DaskDashboardHandler, DaskWebsocketHandler
from .manager import DaskClusterManager


def _jupyter_server_extension_points():
    return [{"module": "dask_labextension"}]


def _load_jupyter_server_extension(server_app):
    """Called by JupyterLab to register our extension."""
    web_app = server_app.web_app
    base_url: str = web_app.settings.get("base_url", "/")

    manager = DaskClusterManager()
    # Store manager on settings so handlers can find it
    web_app.settings["dask_cluster_manager"] = manager

    init = dict(manager=manager)

    handlers = [
        # Backend list  (NEW)
        (
            url_path_join(base_url, "dask", "backends"),
            BackendHandler,
            init,
        ),
        # Cluster CRUD
        (
            url_path_join(base_url, "dask", "clusters") + "(?:/([^/]+))?",
            ClusterHandler,
            init,
        ),
        # Dashboard proxy (HTTP)
        (
            url_path_join(base_url, "dask", "dashboard") + r"(?:/([^/]+))(.*)",
            DaskDashboardHandler,
            init,
        ),
        # Dashboard proxy (WebSocket)
        (
            url_path_join(base_url, "dask", "dashboard") + r"(?:/([^/]+))(.*ws.*)",
            DaskWebsocketHandler,
            init,
        ),
    ]

    web_app.add_handlers(".*$", handlers)
    server_app.log.info("Dask labextension loaded (multi-backend support enabled)")