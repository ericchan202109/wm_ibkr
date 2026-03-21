import React, { startTransition, useEffect, useState } from 'react'
import * as wmill from 'windmill-client'

type Summary = {
  date?: string | null
  posts_today?: number | null
  signals?: string[]
  consensus?: string | null
  system_health?: string | null
  total_rules?: number | null
  model_count?: number | null
  captured_at?: string | null
}

type ModelRow = {
  model_key: string
  name?: string | null
  win_rate?: number | null
  avg_return?: number | null
  total_trades?: number | null
}

type PostRow = {
  event_at?: string | null
  event_date?: string | null
  event_time?: string | null
  text: string
  url?: string | null
  source?: string | null
}

type MarketRow = {
  slug: string
  question: string
  yes_price?: number | null
  no_price?: number | null
  liquidity?: number | null
  volume?: number | null
  url?: string | null
}

type PredictionRow = {
  model_id?: string
  model_name?: string
  date_signal?: string
  direction?: string
  hold_days?: number
  status?: string
  actual_return?: number | null
  correct?: boolean | null
}

type SyncRun = {
  id: number
  sync_mode: string
  status: string
  started_at?: string | null
  completed_at?: string | null
  error_text?: string | null
}

type Bundle = {
  summary?: Summary | null
  models?: ModelRow[]
  recent_posts?: PostRow[]
  markets?: MarketRow[]
  predictions_preview?: PredictionRow[]
  sync_runs?: SyncRun[]
  reports?: {
    daily_report?: Record<string, unknown>
    learning_report?: Record<string, unknown>
    opus_analysis?: Record<string, unknown>
    signal_confidence?: Record<string, unknown>
  }
}

const DASHBOARD_SCRIPT = 'u/admin2/trump_code_dashboard_bundle'
const DAILY_FLOW = 'u/admin2/trump_code_daily'
const REALTIME_FLOW = 'u/admin2/trump_code_realtime'
const DB_RESOURCE_PATH = 'u/admin2/supabase_postgresql'

function formatNumber(value: number | null | undefined, digits = 2): string {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return 'N/A'
  }
  return value.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  })
}

function formatPercent(value: number | null | undefined): string {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return 'N/A'
  }
  return `${value.toFixed(1)}%`
}

function formatDateTime(value?: string | null): string {
  if (!value) {
    return 'N/A'
  }
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return value
  }
  return parsed.toLocaleString()
}

function summaryLine(bundle: Bundle | null): string {
  const report = bundle?.reports?.daily_report
  const reportSummary = report && typeof report === 'object' ? (report as Record<string, unknown>).summary : null
  if (reportSummary && typeof reportSummary === 'object') {
    const zh = (reportSummary as Record<string, unknown>).zh
    if (typeof zh === 'string' && zh.trim()) {
      return zh.trim()
    }
  }
  return 'Windmill-native sync of the public trump-code dashboard and curated data files.'
}

export default function App() {
  const [bundle, setBundle] = useState<Bundle | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [activeSync, setActiveSync] = useState<'daily' | 'realtime' | null>(null)

  async function loadBundle() {
    setLoading(true)
    setError(null)
    try {
      const result = (await wmill.runScriptByPath(DASHBOARD_SCRIPT, {
        db_resource_path: DB_RESOURCE_PATH,
        recent_post_limit: 16,
        market_limit: 18,
        prediction_limit: 16,
      })) as Bundle
      startTransition(() => {
        setBundle(result)
      })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load dashboard data.')
    } finally {
      setLoading(false)
    }
  }

  async function triggerSync(mode: 'daily' | 'realtime') {
    setActiveSync(mode)
    setNotice(null)
    try {
      const jobId = await wmill.runFlowAsync(mode === 'daily' ? DAILY_FLOW : REALTIME_FLOW, {
        db_resource_path: DB_RESOURCE_PATH,
      })
      setNotice(`${mode === 'daily' ? 'Daily' : 'Realtime'} sync queued as job ${jobId}. Refresh after completion.`)
    } catch (err) {
      setError(err instanceof Error ? err.message : `Failed to queue ${mode} sync.`)
    } finally {
      setActiveSync(null)
    }
  }

  useEffect(() => {
    void loadBundle()
  }, [])

  const summary = bundle?.summary
  const models = bundle?.models ?? []
  const posts = bundle?.recent_posts ?? []
  const markets = bundle?.markets ?? []
  const predictions = bundle?.predictions_preview ?? []
  const syncRuns = bundle?.sync_runs ?? []
  const signalConfidence =
    bundle?.reports?.signal_confidence && typeof bundle.reports.signal_confidence === 'object'
      ? Object.entries(bundle.reports.signal_confidence as Record<string, number>).slice(0, 6)
      : []

  return (
    <main className="page-shell">
      <section className="hero-panel">
        <div className="hero-copy">
          <p className="eyebrow">WMILL-9 / u/admin2</p>
          <h1>Trump Code</h1>
          <p className="hero-summary">{summaryLine(bundle)}</p>
        </div>
        <div className="hero-metrics">
          <MetricCard label="Signal Date" value={summary?.date ?? 'N/A'} />
          <MetricCard label="Consensus" value={summary?.consensus ?? 'N/A'} />
          <MetricCard label="System Health" value={summary?.system_health ?? 'N/A'} />
          <MetricCard label="Rules" value={summary?.total_rules?.toString() ?? 'N/A'} />
        </div>
      </section>

      <section className="control-bar">
        <div className="button-row">
          <button onClick={() => void loadBundle()} disabled={loading}>
            {loading ? 'Refreshing…' : 'Refresh Bundle'}
          </button>
          <button onClick={() => void triggerSync('realtime')} disabled={activeSync !== null}>
            {activeSync === 'realtime' ? 'Queueing…' : 'Run Realtime Sync'}
          </button>
          <button className="accent" onClick={() => void triggerSync('daily')} disabled={activeSync !== null}>
            {activeSync === 'daily' ? 'Queueing…' : 'Run Daily Sync'}
          </button>
        </div>
        <div className="status-strip">
          <span>Posts Today: {summary?.posts_today ?? 'N/A'}</span>
          <span>Signals: {(summary?.signals ?? []).join(', ') || 'None'}</span>
          <span>Models: {summary?.model_count ?? models.length}</span>
          <span>Snapshot: {formatDateTime(summary?.captured_at)}</span>
        </div>
        {notice ? <p className="notice success">{notice}</p> : null}
        {error ? <p className="notice error">{error}</p> : null}
      </section>

      <section className="grid">
        <Panel title="Model Leaderboard" subtitle="Latest synced ranking from the upstream dashboard">
          <div className="model-list">
            {models.map((model) => (
              <article className="model-card" key={model.model_key}>
                <div>
                  <p className="model-key">{model.model_key}</p>
                  <h3>{model.name ?? model.model_key}</h3>
                </div>
                <div className="model-stats">
                  <span>Win {formatPercent(model.win_rate)}</span>
                  <span>Return {formatNumber(model.avg_return)}%</span>
                  <span>Trades {formatNumber(model.total_trades)}</span>
                </div>
              </article>
            ))}
          </div>
        </Panel>

        <Panel title="Recent Posts" subtitle="Latest persisted post feed from the realtime and daily syncs">
          <div className="stack">
            {posts.map((post, index) => (
              <article className="post-card" key={`${post.event_at ?? post.event_date ?? 'post'}-${index}`}>
                <div className="post-meta">
                  <span>{formatDateTime(post.event_at ?? post.event_date)}</span>
                  <span>{post.source ?? 'unknown source'}</span>
                </div>
                <p>{post.text}</p>
                {post.url ? (
                  <a href={post.url} rel="noreferrer" target="_blank">
                    View source
                  </a>
                ) : null}
              </article>
            ))}
          </div>
        </Panel>

        <Panel title="Prediction Markets" subtitle="Top Polymarket rows persisted into Postgres">
          <div className="stack">
            {markets.map((market) => (
              <article className="market-card" key={market.slug}>
                <h3>{market.question}</h3>
                <div className="market-stats">
                  <span>YES {formatPercent((market.yes_price ?? 0) * 100)}</span>
                  <span>NO {formatPercent((market.no_price ?? 0) * 100)}</span>
                  <span>Liquidity {formatNumber(market.liquidity, 0)}</span>
                </div>
                {market.url ? (
                  <a href={market.url} rel="noreferrer" target="_blank">
                    Open market
                  </a>
                ) : null}
              </article>
            ))}
          </div>
        </Panel>

        <Panel title="Prediction Log Preview" subtitle="Curated slice from predictions_log.json">
          <div className="stack compact">
            {predictions.map((prediction, index) => (
              <article className="prediction-row" key={`${prediction.model_id ?? 'model'}-${prediction.date_signal ?? index}`}>
                <div>
                  <strong>{prediction.model_name ?? prediction.model_id ?? 'Unknown model'}</strong>
                  <p>
                    {prediction.date_signal ?? 'N/A'} · {prediction.direction ?? 'N/A'} · hold {prediction.hold_days ?? 'N/A'}d
                  </p>
                </div>
                <div className="prediction-status">
                  <span>{prediction.status ?? 'N/A'}</span>
                  <span>{typeof prediction.actual_return === 'number' ? `${prediction.actual_return}%` : 'N/A'}</span>
                </div>
              </article>
            ))}
          </div>
        </Panel>

        <Panel title="Signal Confidence" subtitle="Top keys from the synced signal confidence object">
          <div className="confidence-grid">
            {signalConfidence.length === 0 ? <p className="empty">No signal confidence payload synced yet.</p> : null}
            {signalConfidence.map(([key, value]) => (
              <article className="confidence-card" key={key}>
                <span>{key}</span>
                <strong>{typeof value === 'number' ? value.toFixed(3) : String(value)}</strong>
              </article>
            ))}
          </div>
        </Panel>

        <Panel title="Sync Runs" subtitle="Recent flow/script executions persisted in trump_code.sync_runs">
          <div className="stack compact">
            {syncRuns.map((run) => (
              <article className="sync-row" key={run.id}>
                <div>
                  <strong>{run.sync_mode}</strong>
                  <p>{formatDateTime(run.started_at)}</p>
                </div>
                <div className={`sync-pill status-${run.status}`}>{run.status}</div>
              </article>
            ))}
          </div>
        </Panel>
      </section>
    </main>
  )
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <article className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  )
}

function Panel({
  title,
  subtitle,
  children,
}: {
  title: string
  subtitle: string
  children: React.ReactNode
}) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2>{title}</h2>
          <p>{subtitle}</p>
        </div>
      </div>
      {children}
    </section>
  )
}
