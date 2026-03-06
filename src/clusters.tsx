// src/clusters.tsx
//
// Upstream-compatible DaskClusterManager with multi-backend Dask Gateway support.
//
// Changes vs upstream:
//   1. On start(), fetches GET /dask/backends and shows a backend-picker dialog.
//   2. POST /dask/clusters sends { backend: selectedBackendId }.
//   3. Each cluster row shows a coloured backend badge.
//
// The public class API (constructor signature, all getters/methods) is
// identical to upstream so that index.ts, sidebar.ts and scaling.tsx
// compile without changes.

import { IChangedArgs } from '@jupyterlab/coreutils';
import { URLExt } from '@jupyterlab/coreutils';
import { ServerConnection } from '@jupyterlab/services';
import {
  Dialog,
  InputDialog,
  ReactWidget,
  showDialog,
  showErrorMessage
} from '@jupyterlab/apputils';

import { ISignal, Signal } from '@lumino/signaling';
import { Widget } from '@lumino/widgets';

import * as React from 'react';
import * as ReactDOM from 'react-dom';

// ─── Public model type — identical field names to upstream ────────────────────

export interface IClusterModel {
  /** Unique string ID. */
  id: string;
  /** Display name. */
  name: string;
  /** Scheduler address URI. */
  scheduler_address: string;
  /** Dashboard URL (may be a JupyterHub-proxied path for Gateway clusters). */
  dashboard_link: string;
  /** Total worker count. */
  workers: number;
  /** Total core count. */
  cores: number;
  /** Total memory (bytes). */
  memory: number;
  /** Adaptive scaling config, or null. */
  adapt: { minimum: number; maximum: number } | null;
  /** Backend id — extension-specific, not present in plain upstream. */
  backend?: string | null;
}

// ─── Backend type (extension-specific) ───────────────────────────────────────

interface IBackend {
  id: string;
  display_name: string;
}

// ─── Server helpers ───────────────────────────────────────────────────────────

function makeSettings(): ServerConnection.ISettings {
  return ServerConnection.makeSettings();
}

function apiURL(settings: ServerConnection.ISettings, path: string): string {
  return URLExt.join(settings.baseUrl, 'dask', path);
}

async function requestBackends(
  settings: ServerConnection.ISettings
): Promise<IBackend[]> {
  const resp = await ServerConnection.makeRequest(
    apiURL(settings, 'backends'),
    {},
    settings
  );
  if (!resp.ok) return [];
  return resp.json();
}

async function requestCreateCluster(
  settings: ServerConnection.ISettings,
  backendId: string | null
): Promise<IClusterModel> {
  const body: Record<string, string> = {};
  if (backendId) body.backend = backendId;
  const resp = await ServerConnection.makeRequest(
    apiURL(settings, 'clusters'),
    {
      method: 'POST',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' }
    },
    settings
  );
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Failed to start Dask cluster: ${txt}`);
  }
  return resp.json();
}

async function requestListClusters(
  settings: ServerConnection.ISettings
): Promise<IClusterModel[]> {
  const resp = await ServerConnection.makeRequest(
    apiURL(settings, 'clusters'),
    {},
    settings
  );
  if (!resp.ok) {
    throw new Error(
      'Failed to list clusters: might the server extension not be installed/enabled?'
    );
  }
  return resp.json();
}

async function requestDeleteCluster(
  settings: ServerConnection.ISettings,
  id: string
): Promise<void> {
  await ServerConnection.makeRequest(
    apiURL(settings, `clusters/${id}`),
    { method: 'DELETE' },
    settings
  );
}

async function requestScaleCluster(
  settings: ServerConnection.ISettings,
  id: string,
  workers?: number,
  adapt?: { minimum: number; maximum: number }
): Promise<IClusterModel> {
  const body: Record<string, unknown> = {};
  if (workers !== undefined) body.workers = workers;
  if (adapt !== undefined) body.adapt = adapt;
  const resp = await ServerConnection.makeRequest(
    apiURL(settings, `clusters/${id}`),
    {
      method: 'PATCH',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' }
    },
    settings
  );
  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Failed to scale cluster: ${txt}`);
  }
  return resp.json();
}

// ─── Backend-picker dialog ────────────────────────────────────────────────────

interface IBackendBodyProps {
  backends: IBackend[];
  selectedId: string;
  onChange: (id: string) => void;
}

class BackendDialogBody extends React.Component<IBackendBodyProps> {
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

// ─── React UI components ──────────────────────────────────────────────────────

interface IClusterRowProps {
  model: IClusterModel;
  backends: IBackend[];
  isActive: boolean;
  onSelect: (id: string) => void;
  onStop: (id: string) => void;
  onScale: (id: string) => void;
  onInjectClient: (model: IClusterModel) => void;
}

function backendLabel(model: IClusterModel, backends: IBackend[]): string {
  if (!model.backend) return '';
  const found = backends.find(b => b.id === model.backend);
  return found ? found.display_name : model.backend;
}

function ClusterRow(props: IClusterRowProps) {
  const { model, backends, isActive, onSelect, onStop, onScale, onInjectClient } = props;
  const label = backendLabel(model, backends);

  return (
    <div
      className={`dask-ClusterRow${isActive ? ' dask-ClusterRow--active' : ''}`}
      onClick={() => onSelect(model.id)}
    >
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
        <span>{model.workers} workers · {model.cores} cores</span>
      </div>
      <div className="dask-ClusterRow-buttons">
        <button
          className="dask-ClusterRow-btn"
          title="Inject Dask Client Connection Code"
          onClick={e => { e.stopPropagation(); onInjectClient(model); }}
        >
          {'</>'}
        </button>
        <button
          className="dask-ClusterRow-btn"
          title="Scale Cluster"
          onClick={e => { e.stopPropagation(); onScale(model.id); }}
        >
          ⚙
        </button>
        <button
          className="dask-ClusterRow-btn dask-ClusterRow-btn--stop"
          title="Shut Down Cluster"
          onClick={e => { e.stopPropagation(); onStop(model.id); }}
        >
          ✕
        </button>
      </div>
    </div>
  );
}

interface IClusterPanelProps {
  clusters: IClusterModel[];
  backends: IBackend[];
  activeClusterId: string | undefined;
  isReady: boolean;
  onNewCluster: () => void;
  onStopCluster: (id: string) => void;
  onScaleCluster: (id: string) => void;
  onSelectCluster: (id: string) => void;
  onInjectClient: (model: IClusterModel) => void;
}

function ClusterPanel(props: IClusterPanelProps) {
  const {
    clusters, backends, activeClusterId, isReady,
    onNewCluster, onStopCluster, onScaleCluster, onSelectCluster, onInjectClient
  } = props;

  return (
    <div className="dask-DaskClusterManager-content">
      <div className="dask-ClusterPanel-toolbar">
        <button
          className="dask-ClusterPanel-newBtn"
          disabled={!isReady}
          onClick={onNewCluster}
        >
          + New Cluster
        </button>
      </div>
      {clusters.length === 0 && isReady && (
        <div className="dask-ClusterPanel-empty">
          No clusters. Click "+ New Cluster" to start one.
        </div>
      )}
      {!isReady && (
        <div className="dask-ClusterPanel-loading">Connecting…</div>
      )}
      {clusters.map(model => (
        <ClusterRow
          key={model.id}
          model={model}
          backends={backends}
          isActive={model.id === activeClusterId}
          onSelect={onSelectCluster}
          onStop={onStopCluster}
          onScale={onScaleCluster}
          onInjectClient={onInjectClient}
        />
      ))}
    </div>
  );
}

// ─── DaskClusterManager ───────────────────────────────────────────────────────
//
// Extends Widget (not ReactWidget) to match the upstream class contract.
// React rendering is done manually via ReactDOM.render in onUpdateRequest.

export class DaskClusterManager extends Widget {
  constructor(options: DaskClusterManager.IOptions) {
    super();
    this.addClass('dask-DaskClusterManager');
    this._settings = makeSettings();
    this._injectClientCodeForCluster = options.injectClientCodeForCluster;
    this._getClientCodeForCluster    = options.getClientCodeForCluster;
    // Store any extra options the upstream passes (launchClusterId etc.)
    this._launchClusterId = options.launchClusterId;
  }

  // ── Public API — identical to upstream ─────────────────────────────────────

  get activeCluster(): IClusterModel | undefined {
    return this._clusters.find(c => c.id === this._activeClusterId);
  }

  get activeClusterChanged(): ISignal<
    this,
    IChangedArgs<IClusterModel | undefined>
  > {
    return this._activeClusterChanged;
  }

  get isReady(): boolean {
    return this._isReady;
  }

  get clusters(): IClusterModel[] {
    return this._clusters;
  }

  setActiveCluster(id: string): void {
    this._setActiveById(id);
  }

  async refresh(): Promise<void> {
    await this._updateClusterList();
  }

  /** Start a new cluster. Shows backend dialog if >1 backend configured. */
  async start(): Promise<IClusterModel> {
    return this._launchCluster();
  }

  async stop(id: string): Promise<void> {
    if (!this._clusters.find(c => c.id === id)) {
      throw new Error(`Cannot find cluster ${id}`);
    }
    await this._stopById(id);
  }

  async scale(id: string): Promise<IClusterModel> {
    const cluster = this._clusters.find(c => c.id === id);
    if (!cluster) {
      throw new Error(`Cannot find cluster ${id}`);
    }
    return this._scaleById(id);
  }

  // ── Lifecycle ───────────────────────────────────────────────────────────────

  protected onAfterAttach(): void {
    this._updateClusterList().then(() => {
      this._isReady = true;
      this._render();
    });
    this._pollHandle = setInterval(() => this._updateClusterList(), 5000);
  }

  protected onBeforeDetach(): void {
    if (this._pollHandle !== null) {
      clearInterval(this._pollHandle);
    }
    ReactDOM.unmountComponentAtNode(this.node);
  }

  protected onUpdateRequest(): void {
    this._render();
  }

  // ── Private ─────────────────────────────────────────────────────────────────

  private _render(): void {
    ReactDOM.render(
      <ClusterPanel
        clusters={this._clusters}
        backends={this._backends}
        activeClusterId={this._activeClusterId}
        isReady={this._isReady}
        onNewCluster={() => void this._launchCluster()}
        onStopCluster={id => void this._stopById(id)}
        onScaleCluster={id => void this._scaleById(id)}
        onSelectCluster={id => this._setActiveById(id)}
        onInjectClient={model => this._injectClientCodeForCluster(model)}
      />,
      this.node
    );
  }

  private async _updateClusterList(): Promise<void> {
    // Fetch backends on first call
    if (this._backends.length === 0) {
      try {
        this._backends = await requestBackends(this._settings);
      } catch {
        // non-fatal: backends endpoint may not exist in older setups
      }
    }
    try {
      this._clusters = await requestListClusters(this._settings);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      void showErrorMessage('Dask Server Error', msg);
    }
    this.update();
  }

  private async _launchCluster(): Promise<IClusterModel> {
    // Ensure backends are loaded
    if (this._backends.length === 0) {
      try {
        this._backends = await requestBackends(this._settings);
      } catch {
        // ignore — will create with no backend specified
      }
    }

    let backendId: string | null = null;
    if (this._backends.length > 0) {
      backendId = await showBackendDialog(this._backends);
      if (backendId === null) {
        // User cancelled dialog
        throw new Error('Cluster creation cancelled');
      }
    }

    const cluster = await requestCreateCluster(this._settings, backendId);
    this._clusters = [...this._clusters, cluster];
    this._setActiveById(cluster.id);
    this.update();
    return cluster;
  }

  private async _stopById(id: string): Promise<void> {
    await requestDeleteCluster(this._settings, id);
    this._clusters = this._clusters.filter(c => c.id !== id);
    if (this._activeClusterId === id) {
      const next = this._clusters[0];
      this._setActiveById(next ? next.id : '');
    }
    this.update();
  }

  private async _scaleById(id: string): Promise<IClusterModel> {
    const result = await InputDialog.getNumber({
      title: 'Scale Dask Cluster',
      label: 'Number of workers',
      value: 1
    });
    if (!result.button.accept || result.value === null) {
      return this._clusters.find(c => c.id === id)!;
    }
    const updated = await requestScaleCluster(this._settings, id, result.value);
    this._clusters = this._clusters.map(c => c.id === id ? updated : c);
    this.update();
    return updated;
  }

  private _setActiveById(id: string): void {
    const oldCluster = this.activeCluster;
    this._activeClusterId = id;
    const newCluster = this.activeCluster;
    this._activeClusterChanged.emit({
      name: 'activeCluster',
      oldValue: oldCluster,
      newValue: newCluster
    });
    // Notify index.ts via the injected callback when selection changes
    if (newCluster && this._getClientCodeForCluster) {
      // index.ts watches activeClusterChanged — no direct call needed here
    }
    this.update();
  }

  // ── Private state ────────────────────────────────────────────────────────────

  private _settings:                   ServerConnection.ISettings;
  private _clusters:                   IClusterModel[] = [];
  private _backends:                   IBackend[]       = [];
  private _activeClusterId:            string | undefined;
  private _isReady:                    boolean          = false;
  private _pollHandle:                 ReturnType<typeof setInterval> | null = null;
  private _launchClusterId:            string | undefined;

  private readonly _injectClientCodeForCluster: (model: IClusterModel) => Promise<void>;
  private readonly _getClientCodeForCluster:    (model: IClusterModel) => string;

  private readonly _activeClusterChanged = new Signal<
    this,
    IChangedArgs<IClusterModel | undefined>
  >(this);
}

// ─── Options namespace — mirrors upstream ─────────────────────────────────────

export namespace DaskClusterManager {
  export interface IOptions {
    /** Callback to inject client connection code into the active notebook. */
    injectClientCodeForCluster: (model: IClusterModel) => Promise<void>;
    /** Returns the Python client code string for a cluster model. */
    getClientCodeForCluster: (model: IClusterModel) => string;
    /** JupyterLab command registry (passed through but not used directly). */
    registry?: unknown;
    /** Optional ID of a cluster to auto-activate on startup. */
    launchClusterId?: string;
  }
}
