// src/clusters.tsx
//
// Drop-in replacement for the upstream dask-labextension clusters.tsx.
// Adds multi-backend Dask Gateway support:
//   1. On panel mount, fetches GET /dask/backends.
//   2. "New Cluster" opens a dialog to pick a gateway backend.
//   3. POST /dask/clusters includes { backend: selectedBackendId }.
//   4. Each cluster row shows a backend badge.
//
// Public API kept identical to upstream:
//   export interface IClusterModel   (same field names as upstream)
//   export class DaskClusterManager  (renamed from ClusterPanel)

import {
  Dialog,
  InputDialog,
  ReactWidget,
  showDialog,
  showErrorMessage
} from '@jupyterlab/apputils';

import { URLExt } from '@jupyterlab/coreutils';
import { ServerConnection } from '@jupyterlab/services';

import * as React from 'react';

// ─── Types ────────────────────────────────────────────────────────────────────

export interface IClusterModel {
  id: string;
  name: string;
  /** Flat scheduler address string — matches upstream field name. */
  scheduler_address: string;
  /** Flat dashboard URL string — matches upstream field name. */
  dashboard_link: string;
  workers: number;
  cores: number;
  memory: number;
  adapt: { minimum: number; maximum: number } | null;
  /** Backend id — extension-specific field, absent in plain upstream. */
  backend?: string | null;
}

interface IBackend {
  id: string;
  display_name: string;
}

// ─── Server helpers ───────────────────────────────────────────────────────────

const SERVER_CONNECTION = ServerConnection.makeSettings();

function apiURL(path: string): string {
  return URLExt.join(SERVER_CONNECTION.baseUrl, 'dask', path);
}

async function fetchBackends(): Promise<IBackend[]> {
  const response = await ServerConnection.makeRequest(
    apiURL('backends'),
    {},
    SERVER_CONNECTION
  );
  if (!response.ok) {
    throw new Error(`Failed to fetch backends: ${response.statusText}`);
  }
  return response.json();
}

async function createCluster(
  backendId: string | null,
  name: string | null
): Promise<IClusterModel> {
  const body: Record<string, string> = {};
  if (backendId) body.backend = backendId;
  if (name) body.name = name;

  const response = await ServerConnection.makeRequest(
    apiURL('clusters'),
    {
      method: 'POST',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' }
    },
    SERVER_CONNECTION
  );
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to create cluster: ${text}`);
  }
  return response.json();
}

async function fetchClusters(): Promise<IClusterModel[]> {
  const response = await ServerConnection.makeRequest(
    apiURL('clusters'),
    {},
    SERVER_CONNECTION
  );
  if (!response.ok) {
    throw new Error('Failed to list clusters');
  }
  return response.json();
}

async function deleteCluster(clusterId: string): Promise<void> {
  await ServerConnection.makeRequest(
    apiURL(`clusters/${clusterId}`),
    { method: 'DELETE' },
    SERVER_CONNECTION
  );
}

async function scaleCluster(
  clusterId: string,
  workers?: number,
  adapt?: { minimum: number; maximum: number }
): Promise<IClusterModel> {
  const body: Record<string, unknown> = {};
  if (workers !== undefined) body.workers = workers;
  if (adapt !== undefined) body.adapt = adapt;

  const response = await ServerConnection.makeRequest(
    apiURL(`clusters/${clusterId}`),
    {
      method: 'PATCH',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' }
    },
    SERVER_CONNECTION
  );
  return response.json();
}

// ─── Backend selector dialog ──────────────────────────────────────────────────

interface IBackendDialogBodyProps {
  backends: IBackend[];
  selectedId: string;
  onChange: (id: string) => void;
}

class BackendDialogBody extends React.Component<IBackendDialogBodyProps> {
  render() {
    const { backends, selectedId, onChange } = this.props;
    return (
      <div className="dask-BackendDialog">
        <p className="dask-BackendDialog-label">Select Dask Gateway backend:</p>
        <div className="dask-BackendDialog-options">
          {backends.map(b => (
            <label
              key={b.id}
              className={`dask-BackendDialog-option${
                b.id === selectedId ? ' dask-BackendDialog-option--selected' : ''
              }`}
            >
              <input
                type="radio"
                name="dask-backend"
                value={b.id}
                checked={b.id === selectedId}
                onChange={() => onChange(b.id)}
              />
              <span className="dask-BackendDialog-optionName">
                {b.display_name}
              </span>
            </label>
          ))}
        </div>
      </div>
    );
  }
}

async function showBackendDialog(
  backends: IBackend[]
): Promise<string | null> {
  if (backends.length === 0) return null;
  if (backends.length === 1) return backends[0].id;

  let selected = backends[0].id;

  class Body extends ReactWidget {
    getValue() {
      return selected;
    }
    render() {
      return (
        <BackendDialogBody
          backends={backends}
          selectedId={selected}
          onChange={id => {
            selected = id;
            this.update();
          }}
        />
      );
    }
  }

  const result = await showDialog({
    title: 'New Dask Cluster',
    body: new Body(),
    buttons: [Dialog.cancelButton(), Dialog.okButton({ label: 'Create' })]
  });

  return result.button.accept ? selected : null;
}

// ─── Cluster row component ────────────────────────────────────────────────────

interface IClusterRowProps {
  model: IClusterModel;
  backends: IBackend[];
  onDelete: (id: string) => void;
  onScale: (id: string) => void;
  onInjectClient: (model: IClusterModel) => void;
}

function backendLabel(model: IClusterModel, backends: IBackend[]): string {
  if (!model.backend) return '';
  const b = backends.find(x => x.id === model.backend);
  return b ? b.display_name : model.backend;
}

function ClusterRow(props: IClusterRowProps) {
  const { model, backends, onDelete, onScale, onInjectClient } = props;
  const label = backendLabel(model, backends);

  return (
    <div className="dask-ClusterRow">
      <div className="dask-ClusterRow-title">
        <span className="dask-ClusterRow-name">{model.name}</span>
        {label && (
          <span
            className={`dask-ClusterRow-backend dask-ClusterRow-backend--${model.backend}`}
            title={`Backend: ${label}`}
          >
            {label}
          </span>
        )}
      </div>
      <div className="dask-ClusterRow-stats">
        <span>{model.workers} workers</span>
        <span>{model.cores} cores</span>
      </div>
      <div className="dask-ClusterRow-buttons">
        <button
          className="dask-ClusterRow-btn"
          title="Inject client code"
          onClick={() => onInjectClient(model)}
        >
          {'</>'}
        </button>
        <button
          className="dask-ClusterRow-btn"
          title="Scale cluster"
          onClick={() => onScale(model.id)}
        >
          ⚙
        </button>
        <button
          className="dask-ClusterRow-btn dask-ClusterRow-btn--delete"
          title="Shut down cluster"
          onClick={() => onDelete(model.id)}
        >
          ✕
        </button>
      </div>
    </div>
  );
}

// ─── Main cluster panel ───────────────────────────────────────────────────────

interface IDaskClusterManagerState {
  clusters: IClusterModel[];
  backends: IBackend[];
  loading: boolean;
  error: string | null;
}

/**
 * Main cluster management panel.
 * Exported as DaskClusterManager to match the name expected by index.ts,
 * sidebar.ts, and any other upstream consumers.
 */
export class DaskClusterManager extends ReactWidget {
  private _state: IDaskClusterManagerState = {
    clusters: [],
    backends: [],
    loading: false,
    error: null
  };
  private _pollInterval: ReturnType<typeof setInterval> | null = null;

  constructor() {
    super();
    this.addClass('dask-DaskClusterManager');
    this.title.label = 'Dask Clusters';
    this.title.iconClass = 'dask-DaskLogo';
  }

  protected onAfterAttach(): void {
    this._initialize();
    this._pollInterval = setInterval(() => this._refreshClusters(), 5000);
  }

  protected onBeforeDetach(): void {
    if (this._pollInterval !== null) {
      clearInterval(this._pollInterval);
    }
  }

  private async _initialize(): Promise<void> {
    this._setState({ loading: true, error: null });
    try {
      const [backends, clusters] = await Promise.all([
        fetchBackends(),
        fetchClusters()
      ]);
      this._setState({ backends, clusters, loading: false });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this._setState({ loading: false, error: msg });
    }
  }

  private async _refreshClusters(): Promise<void> {
    try {
      const clusters = await fetchClusters();
      this._setState({ clusters });
    } catch {
      // silently ignore poll errors
    }
  }

  private _setState(patch: Partial<IDaskClusterManagerState>): void {
    this._state = { ...this._state, ...patch };
    this.update();
  }

  // ── Public actions (callable from index.ts toolbar commands) ─────────────

  async newCluster(): Promise<void> {
    const { backends } = this._state;
    if (backends.length === 0) {
      void showErrorMessage('No backends', 'No gateway backends are configured.');
      return;
    }

    const backendId = await showBackendDialog(backends);
    if (backendId === null) return;

    this._setState({ loading: true, error: null });
    try {
      const model = await createCluster(backendId, null);
      this._setState({
        clusters: [...this._state.clusters, model],
        loading: false
      });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this._setState({ loading: false, error: msg });
    }
  }

  private async _deleteCluster(clusterId: string): Promise<void> {
    try {
      await deleteCluster(clusterId);
      this._setState({
        clusters: this._state.clusters.filter(c => c.id !== clusterId)
      });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      void showErrorMessage('Delete failed', msg);
    }
  }

  private async _scaleCluster(clusterId: string): Promise<void> {
    const result = await InputDialog.getNumber({
      title: 'Scale cluster',
      label: 'Number of workers',
      value: 1
    });
    if (!result.button.accept || result.value === null) return;
    try {
      const updated = await scaleCluster(clusterId, result.value);
      this._setState({
        clusters: this._state.clusters.map(c =>
          c.id === clusterId ? updated : c
        )
      });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      void showErrorMessage('Scale failed', msg);
    }
  }

  private _injectClientCode(model: IClusterModel): void {
    this._injectRequested.emit(model);
  }

  // Signal exposed to index.ts for kernel client injection
  private _injectRequested = new Private.Signal<IClusterModel>(this);
  get injectRequested() {
    return this._injectRequested;
  }

  // ── Render ───────────────────────────────────────────────────────────────

  render() {
    const { clusters, backends, loading, error } = this._state;

    return (
      <div className="dask-DaskClusterManager-content">
        <div className="dask-ClusterPanel-toolbar">
          <button
            className="dask-ClusterPanel-newBtn"
            title="New Cluster"
            disabled={loading}
            onClick={() => void this.newCluster()}
          >
            + New Cluster
          </button>
          <button
            className="dask-ClusterPanel-refreshBtn"
            title="Refresh"
            onClick={() => void this._refreshClusters()}
          >
            ↻
          </button>
        </div>

        {error && (
          <div className="dask-ClusterPanel-error">⚠ {error}</div>
        )}

        {loading && (
          <div className="dask-ClusterPanel-loading">Loading…</div>
        )}

        {!loading && clusters.length === 0 && !error && (
          <div className="dask-ClusterPanel-empty">
            No clusters. Click "+ New Cluster" to start one.
          </div>
        )}

        {clusters.map(model => (
          <ClusterRow
            key={model.id}
            model={model}
            backends={backends}
            onDelete={id => void this._deleteCluster(id)}
            onScale={id => void this._scaleCluster(id)}
            onInjectClient={m => this._injectClientCode(m)}
          />
        ))}
      </div>
    );
  }
}

// ─── Private Signal shim ──────────────────────────────────────────────────────

namespace Private {
  type Listener<T> = (sender: unknown, args: T) => void;

  export class Signal<T> {
    private _listeners: Listener<T>[] = [];

    constructor(private _sender: unknown) {}

    connect(listener: Listener<T>): void {
      this._listeners.push(listener);
    }

    disconnect(listener: Listener<T>): void {
      this._listeners = this._listeners.filter(l => l !== listener);
    }

    emit(args: T): void {
      for (const l of this._listeners) l(this._sender, args);
    }
  }
}
