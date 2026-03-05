"""
Dask cluster manager — Dask Gateway–only.

All backends route through a Dask Gateway server.  Two client-side patterns:

  gateway_style "direct"  →  GatewayCluster(address=..., cluster_options=...)
  gateway_style "factory" →  GatewaySubclass(**kwargs).new_cluster(**method_kwargs)

The server-side backend (Kubernetes, HTCondor, SLURM…) is an admin concern
and is completely transparent to this module.
"""

from __future__ import annotations

import importlib
import logging
import uuid
from typing import Any, Dict, List, Optional

import dask

log = logging.getLogger(__name__)

ClusterModel = Dict[str, Any]
BackendModel  = Dict[str, str]


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _labextension_config() -> Dict[str, Any]:
    return dask.config.get("labextension", default={})


def get_backends() -> List[BackendModel]:
    """Return [{id, display_name}, ...] for all configured gateway backends."""
    cfg = _labextension_config()
    backends = cfg.get("backends", [])
    return [
        {"id": b["id"], "display_name": b.get("display_name", b["id"])}
        for b in backends
    ]


def _get_backend_config(backend_id: Optional[str] = None) -> Dict[str, Any]:
    cfg = _labextension_config()
    backends: List[Dict] = cfg.get("backends", [])
    if not backends:
        raise RuntimeError(
            "No backends configured. Add a 'backends' list to "
            "~/.config/dask/labextension.yaml"
        )
    if backend_id is None:
        return backends[0]
    for b in backends:
        if b["id"] == backend_id:
            return b
    raise ValueError(
        f"Unknown backend '{backend_id}'. "
        f"Available: {[b['id'] for b in backends]}"
    )


# ---------------------------------------------------------------------------
# Gateway helpers
# ---------------------------------------------------------------------------

def _build_cluster_options(opts_dict: Dict[str, Any]) -> Any:
    """Convert a plain dict to a dask_gateway.ClusterOptions object."""
    try:
        from dask_gateway import ClusterOptions  # type: ignore
        opts = ClusterOptions()
        for k, v in opts_dict.items():
            opts[k] = v
        return opts
    except ImportError:
        return opts_dict          # older versions accept plain dicts


def _resolve_auth(auth_cfg: Any) -> Any:
    """Convert an auth sub-dict to the appropriate dask_gateway auth object."""
    if not isinstance(auth_cfg, dict):
        return auth_cfg           # already an object or string like "jupyterhub"
    auth_type   = auth_cfg.pop("type", "jupyterhub").lower()
    auth_kwargs = auth_cfg.pop("kwargs", {})
    try:
        import dask_gateway.auth as gw_auth  # type: ignore
        classes = {
            "jupyterhub": "JupyterHubAuth",
            "kerberos":   "KerberosAuth",
            "basic":      "BasicAuth",
        }
        AuthClass = getattr(gw_auth, classes.get(auth_type, "JupyterHubAuth"))
        return AuthClass(**auth_kwargs)
    except (ImportError, AttributeError) as exc:
        log.warning("Could not resolve auth type '%s': %s", auth_type, exc)
        return None


# ---------------------------------------------------------------------------
# Cluster creation
# ---------------------------------------------------------------------------

def _make_gateway_direct(factory: Dict[str, Any]) -> Any:
    """GatewayCluster(address=..., cluster_options=ClusterOptions(...))"""
    module_name: str  = factory.get("module", "dask_gateway")
    class_name:  str  = factory.get("class", "GatewayCluster")
    args:        List = list(factory.get("args", []))
    kwargs:      Dict = dict(factory.get("kwargs", {}))

    # cluster_options dict → ClusterOptions object
    opts_dict = kwargs.pop("cluster_options", None)
    if opts_dict:
        kwargs["cluster_options"] = _build_cluster_options(opts_dict)

    # auth dict → auth object
    auth_raw = kwargs.get("auth")
    if auth_raw is not None:
        resolved = _resolve_auth(auth_raw)
        if resolved is None:
            kwargs.pop("auth", None)
        else:
            kwargs["auth"] = resolved

    log.info("Gateway-direct: %s.%s", module_name, class_name)
    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)(*args, **kwargs)


def _make_gateway_factory(factory: Dict[str, Any]) -> Any:
    """GatewaySubclass(**kwargs).factory_method(**method_kwargs)

    Used by HTCGateway: it is a Gateway connection manager, not a cluster,
    so cluster = HTCGateway(**kw).new_cluster(**method_kw).
    """
    module_name:   str  = factory.get("module", "htcdaskgateway")
    class_name:    str  = factory.get("class", "HTCGateway")
    method_name:   str  = factory.get("factory_method", "new_cluster")
    args:          List = list(factory.get("args", []))
    kwargs:        Dict = dict(factory.get("kwargs", {}))
    method_kwargs: Dict = dict(factory.get("method_kwargs", {}))

    auth_raw = kwargs.get("auth")
    if auth_raw is not None:
        resolved = _resolve_auth(auth_raw)
        if resolved is None:
            kwargs.pop("auth", None)
        else:
            kwargs["auth"] = resolved

    log.info("Gateway-factory: %s.%s(**kw).%s(**mkw)", module_name, class_name, method_name)
    mod          = importlib.import_module(module_name)
    gw_instance  = getattr(mod, class_name)(*args, **kwargs)
    return getattr(gw_instance, method_name)(**method_kwargs)


def make_cluster(
    configuration: Optional[Dict[str, Any]] = None,
    backend_id: Optional[str] = None,
) -> Any:
    """Create and return a GatewayCluster using the configured backend."""
    if configuration is None:
        configuration = _get_backend_config(backend_id)

    gateway_style: str  = configuration.get("gateway_style", "direct")
    factory:       Dict = configuration.get("factory", {})

    if gateway_style == "factory":
        return _make_gateway_factory(factory)
    return _make_gateway_direct(factory)


# ---------------------------------------------------------------------------
# Cluster model
# ---------------------------------------------------------------------------

def make_cluster_model(
    cluster_id:  str,
    cluster_name: str,
    cluster:     Any,
    adaptive:    Any,
    backend_id:  Optional[str] = None,
) -> ClusterModel:
    """Build a JSON-serialisable cluster representation for GatewayCluster."""

    # Worker info — GatewayCluster.scheduler_info may be unavailable until
    # workers connect; guard everything.
    try:
        info = cluster.scheduler_info
        if callable(info):
            info = {}
    except AttributeError:
        info = {}

    workers_info: Dict = info.get("workers", {}) if isinstance(info, dict) else {}
    cores  = sum(d.get("nthreads",      0) for d in workers_info.values())
    memory = sum(d.get("memory_limit",  0) for d in workers_info.values())

    # Scheduler address
    scheduler_address = ""
    for attr in ("scheduler_address", "scheduler_comm"):
        val = getattr(cluster, attr, None)
        if isinstance(val, str) and val:
            scheduler_address = val
            break
        if hasattr(val, "address"):
            scheduler_address = str(val.address)
            break

    # Dashboard link — GatewayCluster returns a JupyterHub-relative path,
    # e.g. /services/dask-gateway/clusters/<name>/status
    dashboard_link = ""
    try:
        dashboard_link = str(cluster.dashboard_link or "")
    except AttributeError:
        pass

    adapt_info = None
    if adaptive is not None:
        adapt_info = {
            "minimum": getattr(adaptive, "minimum", None),
            "maximum": getattr(adaptive, "maximum", None),
        }

    return {
        "id":        cluster_id,
        "name":      cluster_name,
        "status":    "running",
        "cores":     cores,
        "memory":    memory,
        "workers":   len(workers_info),
        "scheduler_address": scheduler_address,  # flat — matches IClusterModel
        "dashboard_link":    dashboard_link,       # flat — matches IClusterModel
        "adapt":     adapt_info,
        "backend":   backend_id,
    }


# ---------------------------------------------------------------------------
# Cluster registry
# ---------------------------------------------------------------------------

class DaskClusterManager:
    """Manages multiple GatewayCluster instances across configured backends."""

    def __init__(self) -> None:
        self._clusters:      Dict[str, Any]           = {}
        self._cluster_names: Dict[str, str]           = {}
        self._adaptives:     Dict[str, Any]           = {}
        self._backend_ids:   Dict[str, Optional[str]] = {}

    def list_backends(self) -> List[BackendModel]:
        return get_backends()

    def start_cluster(
        self,
        cluster_name:  Optional[str]            = None,
        configuration: Optional[Dict[str, Any]] = None,
        backend_id:    Optional[str]            = None,
    ) -> ClusterModel:
        cluster      = make_cluster(configuration=configuration, backend_id=backend_id)
        cluster_id   = str(uuid.uuid4())
        cluster_name = cluster_name or f"Cluster {len(self._clusters) + 1}"

        self._clusters[cluster_id]      = cluster
        self._cluster_names[cluster_id] = cluster_name
        self._adaptives[cluster_id]     = None
        self._backend_ids[cluster_id]   = backend_id

        cfg       = configuration or _get_backend_config(backend_id)
        default   = cfg.get("default", {})
        adapt_cfg = default.get("adapt") or {}
        workers   = default.get("workers")

        if adapt_cfg:
            try:
                self._adaptives[cluster_id] = cluster.adapt(
                    minimum=adapt_cfg.get("minimum", 0),
                    maximum=adapt_cfg.get("maximum", 10),
                )
            except Exception as exc:
                log.warning("adapt() unavailable: %s", exc)
        elif workers:
            try:
                cluster.scale(workers)
            except Exception as exc:
                log.warning("scale() unavailable: %s", exc)

        return make_cluster_model(
            cluster_id, cluster_name, cluster,
            self._adaptives[cluster_id], backend_id=backend_id,
        )

    def close_cluster(self, cluster_id: str) -> Optional[ClusterModel]:
        cluster = self._clusters.pop(cluster_id, None)
        if cluster is None:
            return None
        model = make_cluster_model(
            cluster_id,
            self._cluster_names.pop(cluster_id, ""),
            cluster,
            self._adaptives.pop(cluster_id, None),
            backend_id=self._backend_ids.pop(cluster_id, None),
        )
        try:
            cluster.close()
        except Exception as exc:
            log.warning("close() error: %s", exc)
        return model

    def get_cluster(self, cluster_id: str) -> Optional[ClusterModel]:
        cluster = self._clusters.get(cluster_id)
        if cluster is None:
            return None
        return make_cluster_model(
            cluster_id,
            self._cluster_names[cluster_id],
            cluster,
            self._adaptives.get(cluster_id),
            backend_id=self._backend_ids.get(cluster_id),
        )

    def list_clusters(self) -> List[ClusterModel]:
        return [self.get_cluster(cid) for cid in self._clusters]  # type: ignore[misc]

    def scale_cluster(
        self,
        cluster_id: str,
        workers:    Optional[int]            = None,
        adapt:      Optional[Dict[str, int]] = None,
    ) -> Optional[ClusterModel]:
        cluster = self._clusters.get(cluster_id)
        if cluster is None:
            return None
        prev = self._adaptives.get(cluster_id)
        if prev:
            try:
                prev.stop()
            except Exception:
                pass
            self._adaptives[cluster_id] = None
        if adapt is not None:
            try:
                self._adaptives[cluster_id] = cluster.adapt(
                    minimum=adapt.get("minimum", 0),
                    maximum=adapt.get("maximum", 10),
                )
            except Exception as exc:
                log.warning("adapt() failed: %s", exc)
        elif workers is not None:
            cluster.scale(workers)
        return self.get_cluster(cluster_id)

    def close_all(self) -> None:
        for cid in list(self._clusters):
            self.close_cluster(cid)