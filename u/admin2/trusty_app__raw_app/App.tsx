import React, { useEffect, useMemo, useState } from 'react'
import { backend } from './wmill'

type ViewKey =
  | 'run-center'
  | 'run-monitor'
  | 'symbol-configs'
  | 'approvals'
  | 'portfolio-overview'
  | 'portfolio-ledger'

type Option = { value: string; label: string }

type RunRow = {
  signal_run_id: number
  symbol: string
  final_signal: string
  canonical_strategy_key: string
  execution_mode: string
  approval_status: string
  order_status: string
  contract_status: string
  conflict_detected: boolean
  source_flow_job_id?: string | null
  source_job_id?: string | null
  created_at?: string | null
}

type SignalRunDetail = {
  signal_run_id: number
  symbol: string
  contract_status?: string | null
  engine_status?: string | null
  final_signal?: string | null
  canonical_strategy_key?: string | null
  execution_mode?: string | null
  approval_required?: boolean
  approval_status?: string | null
  order_status?: string | null
  conflict_detected?: boolean
  decision_json?: Record<string, unknown>
  strategy_results_json?: unknown[]
  order_candidate_json?: Record<string, unknown> | null
  chart_refs_json?: Record<string, unknown>
  llm_prompt_text?: string | null
  llm_response_text?: string | null
  llm_response_json?: Record<string, unknown> | null
  integrated_input_json?: Record<string, unknown>
  integrated_output_json?: Record<string, unknown>
  report_markdown?: string | null
  warnings_json?: unknown[]
  errors_json?: unknown[]
  source_flow_job_id?: string | null
  source_job_id?: string | null
  created_at?: string | null
  updated_at?: string | null
  approvals?: ApprovalHistoryRow[]
  order_events?: OrderEventRow[]
}

type ApprovalRow = {
  approval_request_id: number
  signal_run_id: number
  symbol: string
  final_signal: string
  status: string
  created_at?: string | null
  canonical_strategy_key?: string | null
  report_markdown?: string | null
  chart_refs?: Record<string, unknown>
  decision?: Record<string, unknown>
  llm_response_text?: string | null
}

type ApprovalHistoryRow = {
  approval_request_id: number
  status: string
  reviewer?: string | null
  review_comment?: string | null
  execution_result_json?: Record<string, unknown> | null
  created_at?: string | null
  decided_at?: string | null
}

type OrderEventRow = {
  order_event_id: number
  event_type: string
  status: string
  ibkr_order_id?: string | null
  execution_payload_json?: Record<string, unknown> | null
  created_at?: string | null
}

type StockConfigRow = {
  symbol: string
  conid: number
  exchange: string
  currency: string
  sec_type: string
  status: string
  scheduler_mode?: string
  timezone: string
  daily_run_time: string
  last_run_at?: string | null
  execution_mode: string
  order_exchange: string
  primary_exchange: string
  use_rth: boolean
  notes: string
  strategy_key: string
  strategy_active: boolean
  strategy_priority: number
}

type ContractLookupResult = {
  symbol: string
  conid: number
  exchange: string
  currency: string
  secType: string
}

type PortfolioResponse = {
  snapshot: {
    snapshot_id: number
    account_id?: string | null
    status: string
    summary_json?: Record<string, { value?: string; currency?: string; account?: string }>
    error_message?: string | null
    created_at?: string | null
  } | null
  positions: Array<Record<string, unknown>>
  orders: Array<Record<string, unknown>>
}

type Notice = {
  tone: 'neutral' | 'success' | 'error'
  message: string
}

const api = backend as any

const NAV_ITEMS: Array<{ key: ViewKey; label: string; eyebrow: string }> = [
  { key: 'run-center', label: 'Run Center', eyebrow: 'Trigger' },
  { key: 'run-monitor', label: 'Run Monitor', eyebrow: 'Observe' },
  { key: 'symbol-configs', label: 'Symbol Configs', eyebrow: 'Control' },
  { key: 'approvals', label: 'Approvals', eyebrow: 'Gate' },
  { key: 'portfolio-overview', label: 'Portfolio Overview', eyebrow: 'Exposure' },
  { key: 'portfolio-ledger', label: 'Portfolio Ledger', eyebrow: 'Positions' },
]

const EMPTY_CONFIG: StockConfigRow = {
  symbol: '',
  conid: 0,
  exchange: 'NASDAQ',
  currency: 'USD',
  sec_type: 'STK',
  status: 'ACTIVE',
  scheduler_mode: 'daily',
  timezone: 'America/New_York',
  daily_run_time: '09:35',
  last_run_at: null,
  execution_mode: 'approval_gate',
  order_exchange: 'SMART',
  primary_exchange: 'NASDAQ',
  use_rth: true,
  notes: '',
  strategy_key: 'daily_weekly_llm',
  strategy_active: true,
  strategy_priority: 10,
}

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function prettyJson(value: unknown): string {
  return JSON.stringify(value ?? {}, null, 2)
}

function formatDate(value?: string | null): string {
  if (!value) return 'N/A'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

function formatNumber(value: unknown, digits = 2): string {
  const parsed = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(parsed)) return 'N/A'
  return parsed.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  })
}

function normalizeSymbol(value: string): string {
  return value.trim().toUpperCase()
}

function chartItemsFromValue(
  value: unknown,
  trail: string[] = [],
  seen = new Set<string>(),
): Array<{ url: string; label: string }> {
  if (!value) return []
  if (typeof value === 'string') {
    if (!/^https?:\/\//i.test(value) || seen.has(value)) return []
    seen.add(value)
    return [{ url: value, label: trail.join(' / ') || 'chart' }]
  }
  if (Array.isArray(value)) {
    return value.flatMap((item, index) => chartItemsFromValue(item, [...trail, `${index + 1}`], seen))
  }
  if (isObject(value)) {
    const directUrl =
      typeof value.url === 'string'
        ? value.url
        : typeof value.public_url === 'string'
          ? value.public_url
          : typeof value.presigned_url === 'string'
            ? value.presigned_url
            : null
    const directLabel =
      typeof value.label === 'string'
        ? value.label
        : typeof value.name === 'string'
          ? value.name
          : trail.join(' / ') || 'chart'
    const directItems =
      directUrl && !seen.has(directUrl) && /^https?:\/\//i.test(directUrl)
        ? (() => {
            seen.add(directUrl)
            return [{ url: directUrl, label: directLabel }]
          })()
        : []
    const nestedItems = Object.entries(value).flatMap(([key, nested]) =>
      key === 'url' || key === 'public_url' || key === 'presigned_url' || key === 'label' || key === 'name'
        ? []
        : chartItemsFromValue(nested, [...trail, key], seen),
    )
    return [...directItems, ...nestedItems]
  }
  return []
}

function chartItemsFromValues(...values: unknown[]): Array<{ url: string; label: string }> {
  const seen = new Set<string>()
  return values.flatMap((value) => chartItemsFromValue(value, [], seen))
}

function summaryMetric(
  summary: Record<string, { value?: string; currency?: string; account?: string }> | undefined,
  key: string,
) {
  const entry = summary?.[key]
  if (!entry) return null
  return {
    label: key,
    value: entry.currency ? `${entry.value ?? 'N/A'} ${entry.currency}` : entry.value ?? 'N/A',
  }
}

function Pill({ label, value, tone = 'neutral' }: { label: string; value: string; tone?: 'neutral' | 'accent' | 'danger' | 'success' }) {
  return (
    <div className={`pill pill-${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

function Section({
  title,
  subtitle,
  actions,
  children,
}: {
  title: string
  subtitle?: string
  actions?: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="panel-eyebrow">{title}</p>
          {subtitle ? <h2>{subtitle}</h2> : null}
        </div>
        {actions ? <div className="panel-actions">{actions}</div> : null}
      </div>
      {children}
    </section>
  )
}

function TextBlock({ value, empty = 'No content available.' }: { value?: string | null; empty?: string }) {
  return <pre className="text-block">{value?.trim() ? value : empty}</pre>
}

function JsonBlock({ value, empty = 'No JSON payload available.' }: { value?: unknown; empty?: string }) {
  const hasValue = value !== undefined && value !== null && (!(Array.isArray(value)) || value.length > 0)
  return <pre className="json-block">{hasValue ? prettyJson(value) : empty}</pre>
}

function App() {
  const [activeView, setActiveView] = useState<ViewKey>('run-center')
  const [notice, setNotice] = useState<Notice | null>(null)

  const [symbolOptions, setSymbolOptions] = useState<Option[]>([])
  const [symbolSearch, setSymbolSearch] = useState('')

  const [runSymbol, setRunSymbol] = useState('TSLA')
  const [runDataMode, setRunDataMode] = useState('run_new')
  const [runDryMode, setRunDryMode] = useState(false)
  const [runPersistReports, setRunPersistReports] = useState(false)
  const [runBusy, setRunBusy] = useState(false)
  const [runLaunchResult, setRunLaunchResult] = useState<Record<string, unknown> | null>(null)
  const [runMatched, setRunMatched] = useState<RunRow | null>(null)
  const [runPollingState, setRunPollingState] = useState('idle')

  const [runFilters, setRunFilters] = useState({
    symbol: '',
    final_signal: '',
    approval_status: '',
    order_status: '',
    canonical_strategy_key: '',
  })
  const [runsBusy, setRunsBusy] = useState(false)
  const [runs, setRuns] = useState<RunRow[]>([])
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null)
  const [runDetailBusy, setRunDetailBusy] = useState(false)
  const [runDetail, setRunDetail] = useState<SignalRunDetail | null>(null)

  const [configsBusy, setConfigsBusy] = useState(false)
  const [configSaving, setConfigSaving] = useState(false)
  const [configs, setConfigs] = useState<StockConfigRow[]>([])
  const [configForm, setConfigForm] = useState<StockConfigRow>(EMPTY_CONFIG)
  const [configLookupBusy, setConfigLookupBusy] = useState(false)
  const [configLookupMessage, setConfigLookupMessage] = useState('')
  const [configLookupError, setConfigLookupError] = useState('')

  const [approvalsBusy, setApprovalsBusy] = useState(false)
  const [approvalActing, setApprovalActing] = useState(false)
  const [approvals, setApprovals] = useState<ApprovalRow[]>([])
  const [selectedApprovalId, setSelectedApprovalId] = useState<number | null>(null)
  const [approvalComment, setApprovalComment] = useState('')
  const [approvalRunDetail, setApprovalRunDetail] = useState<SignalRunDetail | null>(null)

  const [portfolioBusy, setPortfolioBusy] = useState(false)
  const [portfolio, setPortfolio] = useState<PortfolioResponse | null>(null)

  const filteredSymbolOptions = useMemo(() => {
    const query = normalizeSymbol(symbolSearch)
    if (!query) return symbolOptions.slice(0, 200)
    return symbolOptions.filter((option) => option.value.includes(query)).slice(0, 200)
  }, [symbolOptions, symbolSearch])

  const selectedApproval = useMemo(
    () => approvals.find((item) => item.approval_request_id === selectedApprovalId) ?? null,
    [approvals, selectedApprovalId],
  )
  const normalizedConfigSymbol = useMemo(() => normalizeSymbol(configForm.symbol), [configForm.symbol])
  const configHasConid = Number.isFinite(Number(configForm.conid)) && Number(configForm.conid) > 0
  const configCanSave = Boolean(normalizedConfigSymbol) && configHasConid && !configSaving && !configLookupBusy

  const runCharts = useMemo(() => {
    if (!runDetail) return []
    return chartItemsFromValues(
      runDetail.chart_refs_json,
      runDetail.integrated_output_json?.chart_refs,
      runDetail.integrated_input_json?.daily_chart,
      runDetail.integrated_input_json?.weekly_chart,
      runDetail.strategy_results_json,
    )
  }, [runDetail])

  const approvalCharts = useMemo(() => chartItemsFromValues(selectedApproval?.chart_refs), [selectedApproval])

  const portfolioHighlights = useMemo(() => {
    const summary = portfolio?.snapshot?.summary_json
    return [
      summaryMetric(summary, 'NetLiquidation'),
      summaryMetric(summary, 'BuyingPower'),
      summaryMetric(summary, 'AvailableFunds'),
      summaryMetric(summary, 'ExcessLiquidity'),
      summaryMetric(summary, 'MaintMarginReq'),
      summaryMetric(summary, 'TotalCashValue'),
    ].filter(Boolean) as Array<{ label: string; value: string }>
  }, [portfolio])

  async function invoke<T>(name: string, args: Record<string, unknown> = {}): Promise<T> {
    const fn = api[name]
    if (typeof fn !== 'function') {
      throw new Error(`Backend runnable "${name}" is not available`)
    }
    return await fn(args)
  }

  function showNotice(message: string, tone: Notice['tone']) {
    setNotice({ message, tone })
  }

  async function loadSymbolOptions() {
    const result = await invoke<{ options?: Option[] }>('symbol_options')
    setSymbolOptions(result.options ?? [])
    if (!runSymbol && result.options?.[0]?.value) {
      setRunSymbol(result.options[0].value)
    }
  }

  async function loadRuns(nextSelectedRunId?: number | null) {
    setRunsBusy(true)
    try {
      const result = await invoke<{ rows?: RunRow[] }>('list_signal_runs', {
        ...runFilters,
        symbol: normalizeSymbol(runFilters.symbol),
        limit: 100,
      })
      const nextRows = result.rows ?? []
      setRuns(nextRows)
      const preferredId = nextSelectedRunId ?? selectedRunId ?? nextRows[0]?.signal_run_id ?? null
      setSelectedRunId(preferredId)
    } finally {
      setRunsBusy(false)
    }
  }

  async function loadRunDetail(signalRunId: number): Promise<SignalRunDetail> {
    setRunDetailBusy(true)
    try {
      const result = await invoke<SignalRunDetail>('get_signal_run_detail', {
        signal_run_id: signalRunId,
      })
      setRunDetail(result)
      return result
    } finally {
      setRunDetailBusy(false)
    }
  }

  async function loadConfigs(selectedSymbol?: string) {
    setConfigsBusy(true)
    try {
      const result = await invoke<{ rows?: StockConfigRow[] }>('stock_configs', {
        strategy_key: configForm.strategy_key || 'daily_weekly_llm',
        limit: 500,
      })
      const rows = result.rows ?? []
      setConfigs(rows)
      const match = rows.find((item) => item.symbol === normalizeSymbol(selectedSymbol ?? configForm.symbol))
      if (match) {
        setConfigForm(match)
      }
    } finally {
      setConfigsBusy(false)
    }
  }

  async function loadApprovals(nextSelectedId?: number | null) {
    setApprovalsBusy(true)
    try {
      const result = await invoke<{ rows?: ApprovalRow[] }>('list_pending_approvals', { limit: 100 })
      const rows = result.rows ?? []
      setApprovals(rows)
      const preferredId = nextSelectedId ?? selectedApprovalId ?? rows[0]?.approval_request_id ?? null
      setSelectedApprovalId(preferredId)
    } finally {
      setApprovalsBusy(false)
    }
  }

  async function loadPortfolio() {
    setPortfolioBusy(true)
    try {
      const result = await invoke<PortfolioResponse>('get_portfolio_dashboard')
      setPortfolio(result)
    } finally {
      setPortfolioBusy(false)
    }
  }

  useEffect(() => {
    Promise.all([loadSymbolOptions(), loadRuns(), loadConfigs(), loadApprovals(), loadPortfolio()]).catch((error) => {
      console.error(error)
      showNotice(error instanceof Error ? error.message : 'Failed to load app data.', 'error')
    })
  }, [])

  useEffect(() => {
    if (!selectedRunId) {
      setRunDetail(null)
      return
    }
    loadRunDetail(selectedRunId).catch((error) => {
      console.error(error)
      showNotice(error instanceof Error ? error.message : 'Failed to load signal run detail.', 'error')
    })
  }, [selectedRunId])

  useEffect(() => {
    if (!selectedApproval) {
      setApprovalRunDetail(null)
      return
    }
    loadRunDetail(selectedApproval.signal_run_id)
      .then((detail) => setApprovalRunDetail(detail))
      .catch((error) => {
        console.error(error)
        showNotice(error instanceof Error ? error.message : 'Failed to load approval run detail.', 'error')
      })
  }, [selectedApproval?.approval_request_id])

  useEffect(() => {
    if (!runLaunchResult?.job_id) return
    let cancelled = false
    let attempts = 0
    setRunMatched(null)
    setRunPollingState('waiting for persisted signal run')

    const timer = window.setInterval(async () => {
      attempts += 1
      try {
        const result = await invoke<{ rows?: RunRow[] }>('list_signal_runs', {
          source_flow_job_id: runLaunchResult.job_id,
          limit: 1,
        })
        const matched = result.rows?.[0] ?? null
        if (matched && !cancelled) {
          setRunMatched(matched)
          setSelectedRunId(matched.signal_run_id)
          setRunPollingState('persisted run found')
          window.clearInterval(timer)
        } else if (attempts >= 24 && !cancelled) {
          setRunPollingState('no persisted run found yet')
          window.clearInterval(timer)
        }
      } catch (error) {
        console.error(error)
        if (!cancelled) {
          setRunPollingState('poll failed')
        }
        window.clearInterval(timer)
      }
    }, 5000)

    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [runLaunchResult?.job_id])

  useEffect(() => {
    if (!normalizedConfigSymbol) {
      setConfigLookupBusy(false)
      setConfigLookupMessage('')
      setConfigLookupError('')
      return
    }

    let cancelled = false
    const timer = window.setTimeout(async () => {
      setConfigLookupBusy(true)
      setConfigLookupError('')
      setConfigLookupMessage(`Looking up ${normalizedConfigSymbol} in IB Gateway...`)
      try {
        const result = await invoke<ContractLookupResult>('symbol_contract_lookup', {
          symbol: normalizedConfigSymbol,
          exchange: configForm.exchange || 'NASDAQ',
        })
        if (cancelled) return
        setConfigForm((current) => {
          if (normalizeSymbol(current.symbol) !== normalizedConfigSymbol) {
            return current
          }
          return {
            ...current,
            symbol: result.symbol || normalizedConfigSymbol,
            conid: Number(result.conid) || current.conid,
            exchange: result.exchange || current.exchange,
            currency: result.currency || current.currency,
            sec_type: result.secType || current.sec_type,
          }
        })
        setConfigLookupMessage(`Loaded contract metadata for ${normalizedConfigSymbol}.`)
      } catch (error) {
        if (cancelled) return
        setConfigLookupError(error instanceof Error ? error.message : 'Failed to look up contract metadata.')
        setConfigLookupMessage('')
      } finally {
        if (!cancelled) {
          setConfigLookupBusy(false)
        }
      }
    }, 450)

    return () => {
      cancelled = true
      window.clearTimeout(timer)
    }
  }, [normalizedConfigSymbol])

  async function handleRunNow() {
    setRunBusy(true)
    try {
      const result = await invoke<Record<string, unknown>>('run_symbol_signal_now', {
        symbol: normalizeSymbol(runSymbol),
        data_mode: runDataMode,
        dry_run: runDryMode,
        persist_reports: runPersistReports,
      })
      setRunLaunchResult(result)
      setRunMatched(null)
      showNotice(`Submitted flow job ${String(result.job_id ?? '')} for ${normalizeSymbol(runSymbol)}.`, 'success')
    } catch (error) {
      console.error(error)
      showNotice(error instanceof Error ? error.message : 'Failed to launch signal run.', 'error')
    } finally {
      setRunBusy(false)
    }
  }

  async function handleSaveConfig() {
    setConfigSaving(true)
    try {
      const payload = {
        ...configForm,
        symbol: normalizedConfigSymbol,
        conid: Number(configForm.conid),
        strategy_priority: Number(configForm.strategy_priority),
      }
      await invoke('save_stock_config', payload)
      await Promise.all([loadConfigs(payload.symbol), loadSymbolOptions()])
      showNotice(`Saved stock config for ${payload.symbol}.`, 'success')
    } catch (error) {
      console.error(error)
      showNotice(error instanceof Error ? error.message : 'Failed to save stock config.', 'error')
    } finally {
      setConfigSaving(false)
    }
  }

  async function handleArchiveConfig(symbol: string) {
    const normalized = normalizeSymbol(symbol)
    if (!normalized) return
    setConfigSaving(true)
    try {
      await invoke('archive_stock_config', { symbol: normalized })
      await Promise.all([loadConfigs(), loadSymbolOptions()])
      setConfigForm({ ...EMPTY_CONFIG })
      showNotice(`Archived ${normalized}.`, 'success')
    } catch (error) {
      console.error(error)
      showNotice(error instanceof Error ? error.message : 'Failed to archive stock config.', 'error')
    } finally {
      setConfigSaving(false)
    }
  }

  async function handleApprovalDecision(decision: 'approve' | 'reject') {
    if (!selectedApprovalId) return
    setApprovalActing(true)
    try {
      await invoke('decide_signal_approval', {
        approval_request_id: selectedApprovalId,
        decision,
        comment: approvalComment,
      })
      await Promise.all([loadApprovals(), loadRuns(approvalRunDetail?.signal_run_id ?? null)])
      if (approvalRunDetail?.signal_run_id) {
        setSelectedRunId(approvalRunDetail.signal_run_id)
      }
      setApprovalComment('')
      showNotice(`Approval ${decision}d successfully.`, 'success')
    } catch (error) {
      console.error(error)
      showNotice(error instanceof Error ? error.message : 'Failed to update approval.', 'error')
    } finally {
      setApprovalActing(false)
    }
  }

  async function handleSyncPortfolio() {
    setPortfolioBusy(true)
    try {
      await invoke('sync_ibkr_portfolio')
      await loadPortfolio()
      showNotice('Portfolio snapshot refreshed from IBKR.', 'success')
    } catch (error) {
      console.error(error)
      showNotice(error instanceof Error ? error.message : 'Failed to sync portfolio.', 'error')
    } finally {
      setPortfolioBusy(false)
    }
  }

  const shellTitle = NAV_ITEMS.find((item) => item.key === activeView)?.label ?? 'Signal Ops'

  return (
    <div className="app-shell">
      <header className="hero">
        <div>
          <p className="hero-kicker">TRUSTY OPERATOR CONSOLE</p>
          <h1>{shellTitle}</h1>
          <p className="hero-copy">
            Launch integrated signal workflows, inspect the full LLM and chart payload, review approval gates, and track the live IBKR portfolio state from one app.
          </p>
        </div>
        <div className="hero-metrics">
          <Pill label="Tracked Symbols" value={String(configs.length)} tone="accent" />
          <Pill label="Pending Approvals" value={String(approvals.length)} tone={approvals.length ? 'danger' : 'success'} />
          <Pill label="Recent Runs" value={String(runs.length)} tone="neutral" />
        </div>
      </header>

      <nav className="nav-strip">
        {NAV_ITEMS.map((item) => (
          <button
            key={item.key}
            className={`nav-chip ${activeView === item.key ? 'nav-chip-active' : ''}`}
            onClick={() => setActiveView(item.key)}
          >
            <span>{item.eyebrow}</span>
            <strong>{item.label}</strong>
          </button>
        ))}
      </nav>

      {notice ? <div className={`notice notice-${notice.tone}`}>{notice.message}</div> : null}

      {activeView === 'run-center' ? (
        <div className="grid two-up">
          <Section
            title="Manual Launch"
            subtitle="Start a signal engine run"
            actions={
              <button className="button button-primary" disabled={runBusy || !normalizeSymbol(runSymbol)} onClick={handleRunNow}>
                {runBusy ? 'Submitting...' : 'Run Signal'}
              </button>
            }
          >
            <div className="form-grid">
              <label>
                <span>Symbol</span>
                <input value={runSymbol} onChange={(event) => setRunSymbol(event.target.value)} placeholder="AAPL" />
              </label>
              <label>
                <span>Known Symbols</span>
                <select value="" onChange={(event) => setRunSymbol(event.target.value || runSymbol)}>
                  <option value="">Select a configured symbol</option>
                  {filteredSymbolOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                <span>Search Symbol List</span>
                <input value={symbolSearch} onChange={(event) => setSymbolSearch(event.target.value)} placeholder="TSLA" />
              </label>
              <label>
                <span>Data Mode</span>
                <select value={runDataMode} onChange={(event) => setRunDataMode(event.target.value)}>
                  <option value="run_new">run_new</option>
                  <option value="load_latest">load_latest</option>
                </select>
              </label>
            </div>

            <div className="toggle-row">
              <label className="toggle">
                <input type="checkbox" checked={runDryMode} onChange={(event) => setRunDryMode(event.target.checked)} />
                <span>Dry run only</span>
              </label>
              <label className="toggle">
                <input
                  type="checkbox"
                  checked={runPersistReports}
                  onChange={(event) => setRunPersistReports(event.target.checked)}
                />
                <span>Persist historical reports</span>
              </label>
            </div>

            <div className="inline-stats">
              <Pill label="Launch Status" value={runPollingState} tone="neutral" />
              <Pill label="Chosen Symbol" value={normalizeSymbol(runSymbol) || 'N/A'} tone="accent" />
            </div>
          </Section>

          <Section title="Launch Output" subtitle="Follow the new run until it persists">
            <div className="stack">
              <div>
                <h3>Submission Result</h3>
                <JsonBlock value={runLaunchResult} empty="No run submitted yet." />
              </div>
              <div>
                <h3>Matched Persisted Run</h3>
                {runMatched ? (
                  <div className="run-card">
                    <div className="run-card-header">
                      <strong>{runMatched.symbol}</strong>
                      <span>{runMatched.final_signal || 'N/A'}</span>
                    </div>
                    <div className="run-card-grid">
                      <Pill label="Run ID" value={String(runMatched.signal_run_id)} tone="accent" />
                      <Pill label="Approval" value={runMatched.approval_status || 'N/A'} />
                      <Pill label="Order" value={runMatched.order_status || 'N/A'} />
                    </div>
                    <div className="toolbar">
                      <button
                        className="button button-secondary"
                        onClick={() => {
                          setActiveView('run-monitor')
                          setSelectedRunId(runMatched.signal_run_id)
                        }}
                      >
                        Open In Run Monitor
                      </button>
                    </div>
                  </div>
                ) : (
                  <p className="muted">No persisted signal run linked to the submitted flow yet.</p>
                )}
              </div>
            </div>
          </Section>
        </div>
      ) : null}

      {activeView === 'run-monitor' ? (
        <div className="grid monitor-grid">
          <Section
            title="Signal Runs"
            subtitle="Filter and inspect integrated workflow output"
            actions={
              <button className="button button-secondary" disabled={runsBusy} onClick={() => loadRuns()}>
                {runsBusy ? 'Refreshing...' : 'Refresh'}
              </button>
            }
          >
            <div className="form-grid compact">
              <label>
                <span>Symbol</span>
                <input
                  value={runFilters.symbol}
                  onChange={(event) => setRunFilters((current) => ({ ...current, symbol: event.target.value }))}
                  placeholder="TSLA"
                />
              </label>
              <label>
                <span>Final Signal</span>
                <select
                  value={runFilters.final_signal}
                  onChange={(event) => setRunFilters((current) => ({ ...current, final_signal: event.target.value }))}
                >
                  <option value="">All</option>
                  <option value="BUY">BUY</option>
                  <option value="SELL">SELL</option>
                  <option value="HOLD">HOLD</option>
                </select>
              </label>
              <label>
                <span>Approval</span>
                <select
                  value={runFilters.approval_status}
                  onChange={(event) => setRunFilters((current) => ({ ...current, approval_status: event.target.value }))}
                >
                  <option value="">All</option>
                  <option value="pending">pending</option>
                  <option value="approved">approved</option>
                  <option value="rejected">rejected</option>
                  <option value="not_required">not_required</option>
                </select>
              </label>
              <label>
                <span>Order</span>
                <select
                  value={runFilters.order_status}
                  onChange={(event) => setRunFilters((current) => ({ ...current, order_status: event.target.value }))}
                >
                  <option value="">All</option>
                  <option value="not_submitted">not_submitted</option>
                  <option value="submitted">submitted</option>
                  <option value="dry_run">dry_run</option>
                  <option value="error">error</option>
                </select>
              </label>
              <label>
                <span>Strategy Key</span>
                <input
                  value={runFilters.canonical_strategy_key}
                  onChange={(event) =>
                    setRunFilters((current) => ({ ...current, canonical_strategy_key: event.target.value }))
                  }
                  placeholder="daily_weekly_llm"
                />
              </label>
            </div>

            <div className="toolbar">
              <button className="button button-primary" disabled={runsBusy} onClick={() => loadRuns()}>
                Apply Filters
              </button>
            </div>

            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Signal</th>
                    <th>Approval</th>
                    <th>Order</th>
                    <th>Strategy</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((row) => (
                    <tr
                      key={row.signal_run_id}
                      className={selectedRunId === row.signal_run_id ? 'row-selected' : ''}
                      onClick={() => setSelectedRunId(row.signal_run_id)}
                    >
                      <td>{row.symbol}</td>
                      <td>{row.final_signal || 'N/A'}</td>
                      <td>{row.approval_status || 'N/A'}</td>
                      <td>{row.order_status || 'N/A'}</td>
                      <td>{row.canonical_strategy_key || 'N/A'}</td>
                      <td>{formatDate(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {!runs.length ? <p className="muted">No signal runs match the current filters.</p> : null}
            </div>
          </Section>

          <Section title="Run Detail" subtitle={runDetail ? `${runDetail.symbol} • run ${runDetail.signal_run_id}` : 'Select a run'}>
            {runDetailBusy ? <p className="muted">Loading run detail...</p> : null}
            {!runDetailBusy && !runDetail ? <p className="muted">Select a run on the left to inspect it.</p> : null}
            {runDetail ? (
              <div className="stack">
                <div className="inline-stats">
                  <Pill label="Signal" value={runDetail.final_signal || 'N/A'} tone="accent" />
                  <Pill label="Approval" value={runDetail.approval_status || 'N/A'} />
                  <Pill label="Order" value={runDetail.order_status || 'N/A'} />
                  <Pill label="Execution" value={runDetail.execution_mode || 'N/A'} />
                  <Pill label="Updated" value={formatDate(runDetail.updated_at)} />
                </div>

                <div>
                  <h3>Integrated Report</h3>
                  <TextBlock value={runDetail.report_markdown} />
                </div>

                <div>
                  <h3>Charts</h3>
                  {runCharts.length ? (
                    <div className="chart-grid">
                      {runCharts.map((item) => (
                        <figure key={`${item.label}-${item.url}`} className="chart-card">
                          <img src={item.url} alt={item.label} />
                          <figcaption>{item.label}</figcaption>
                        </figure>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">No chart images were attached to this run.</p>
                  )}
                </div>

                <div className="double-stack">
                  <div>
                    <h3>Warnings</h3>
                    <JsonBlock value={runDetail.warnings_json} empty="No warnings." />
                  </div>
                  <div>
                    <h3>Errors</h3>
                    <JsonBlock value={runDetail.errors_json} empty="No errors." />
                  </div>
                </div>

                <div className="double-stack">
                  <div>
                    <h3>LLM Response</h3>
                    <TextBlock value={runDetail.llm_response_text} empty="No LLM response stored." />
                  </div>
                  <div>
                    <h3>LLM JSON</h3>
                    <JsonBlock value={runDetail.llm_response_json} empty="No parsed LLM JSON stored." />
                  </div>
                </div>

                <div className="double-stack">
                  <div>
                    <h3>Integrated Input Snapshot</h3>
                    <JsonBlock value={runDetail.integrated_input_json} />
                  </div>
                  <div>
                    <h3>Integrated Output Snapshot</h3>
                    <JsonBlock value={runDetail.integrated_output_json} />
                  </div>
                </div>

                <div className="double-stack">
                  <div>
                    <h3>Approval History</h3>
                    <JsonBlock value={runDetail.approvals} empty="No approval records." />
                  </div>
                  <div>
                    <h3>Order Events</h3>
                    <JsonBlock value={runDetail.order_events} empty="No order events." />
                  </div>
                </div>
              </div>
            ) : null}
          </Section>
        </div>
      ) : null}

      {activeView === 'symbol-configs' ? (
        <div className="grid monitor-grid">
          <Section
            title="Configured Symbols"
            subtitle="Execution metadata and scheduling controls"
            actions={
              <button
                className="button button-secondary"
                onClick={() => {
                  setConfigForm({ ...EMPTY_CONFIG })
                }}
              >
                New Config
              </button>
            }
          >
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Status</th>
                    <th>Mode</th>
                    <th>Daily Run</th>
                    <th>Strategy</th>
                    <th>Last Dispatch</th>
                  </tr>
                </thead>
                <tbody>
                  {configs.map((row) => (
                    <tr
                      key={`${row.symbol}-${row.strategy_key}`}
                      className={configForm.symbol === row.symbol ? 'row-selected' : ''}
                      onClick={() => setConfigForm(row)}
                    >
                      <td>{row.symbol}</td>
                      <td>{row.status}</td>
                      <td>{row.execution_mode}</td>
                      <td>{`${row.daily_run_time} ${row.timezone}`}</td>
                      <td>{row.strategy_key}</td>
                      <td>{formatDate(row.last_run_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {configsBusy ? <p className="muted">Loading configs...</p> : null}
            </div>
          </Section>

          <Section
            title="Config Editor"
            subtitle={configForm.symbol ? `Edit ${configForm.symbol}` : 'Create a new symbol config'}
            actions={
              <div className="toolbar">
                <button className="button button-primary" disabled={!configCanSave} onClick={handleSaveConfig}>
                  {configSaving ? 'Saving...' : 'Save'}
                </button>
                <button
                  className="button button-danger"
                  disabled={configSaving || !configForm.symbol}
                  onClick={() => handleArchiveConfig(configForm.symbol)}
                >
                  Archive
                </button>
              </div>
            }
          >
            <div className="form-grid compact">
              <label>
                <span>Symbol</span>
                <input
                  value={configForm.symbol}
                  onChange={(event) =>
                    setConfigForm((current) => {
                      const nextSymbol = event.target.value
                      const symbolChanged = normalizeSymbol(nextSymbol) !== normalizeSymbol(current.symbol)
                      return {
                        ...current,
                        symbol: nextSymbol,
                        conid: symbolChanged ? 0 : current.conid,
                      }
                    })
                  }
                  placeholder="TSLA"
                />
                {configLookupBusy ? <p className="muted">Looking up contract metadata...</p> : null}
                {!configLookupBusy && configLookupMessage ? <p className="muted">{configLookupMessage}</p> : null}
              </label>
              <label>
                <span>Conid</span>
                <input
                  type="number"
                  value={String(configForm.conid)}
                  onChange={(event) => setConfigForm((current) => ({ ...current, conid: Number(event.target.value) }))}
                />
                {configLookupError ? <p className="muted">{configLookupError}</p> : null}
              </label>
              <label>
                <span>Exchange</span>
                <input
                  value={configForm.exchange}
                  onChange={(event) => setConfigForm((current) => ({ ...current, exchange: event.target.value }))}
                />
              </label>
              <label>
                <span>Primary Exchange</span>
                <input
                  value={configForm.primary_exchange}
                  onChange={(event) =>
                    setConfigForm((current) => ({ ...current, primary_exchange: event.target.value }))
                  }
                />
              </label>
              <label>
                <span>Order Exchange</span>
                <input
                  value={configForm.order_exchange}
                  onChange={(event) => setConfigForm((current) => ({ ...current, order_exchange: event.target.value }))}
                />
              </label>
              <label>
                <span>Currency</span>
                <input
                  value={configForm.currency}
                  onChange={(event) => setConfigForm((current) => ({ ...current, currency: event.target.value }))}
                />
              </label>
              <label>
                <span>Security Type</span>
                <input
                  value={configForm.sec_type}
                  onChange={(event) => setConfigForm((current) => ({ ...current, sec_type: event.target.value }))}
                />
              </label>
              <label>
                <span>Status</span>
                <select
                  value={configForm.status}
                  onChange={(event) => setConfigForm((current) => ({ ...current, status: event.target.value }))}
                >
                  <option value="ACTIVE">ACTIVE</option>
                  <option value="SUSPENDED">SUSPENDED</option>
                  <option value="ARCHIVED">ARCHIVED</option>
                </select>
              </label>
              <label>
                <span>Daily Run Time</span>
                <input
                  value={configForm.daily_run_time}
                  onChange={(event) =>
                    setConfigForm((current) => ({ ...current, daily_run_time: event.target.value }))
                  }
                  placeholder="09:35"
                />
              </label>
              <label>
                <span>Timezone</span>
                <input
                  value={configForm.timezone}
                  onChange={(event) => setConfigForm((current) => ({ ...current, timezone: event.target.value }))}
                />
              </label>
              <label>
                <span>Execution Mode</span>
                <select
                  value={configForm.execution_mode}
                  onChange={(event) =>
                    setConfigForm((current) => ({ ...current, execution_mode: event.target.value }))
                  }
                >
                  <option value="signal_only">signal_only</option>
                  <option value="approval_gate">approval_gate</option>
                  <option value="auto_place">auto_place</option>
                </select>
              </label>
              <label>
                <span>Strategy Key</span>
                <input
                  value={configForm.strategy_key}
                  onChange={(event) => setConfigForm((current) => ({ ...current, strategy_key: event.target.value }))}
                />
              </label>
              <label>
                <span>Strategy Priority</span>
                <input
                  type="number"
                  value={String(configForm.strategy_priority)}
                  onChange={(event) =>
                    setConfigForm((current) => ({ ...current, strategy_priority: Number(event.target.value) }))
                  }
                />
              </label>
            </div>

            <div className="toggle-row">
              <label className="toggle">
                <input
                  type="checkbox"
                  checked={configForm.use_rth}
                  onChange={(event) => setConfigForm((current) => ({ ...current, use_rth: event.target.checked }))}
                />
                <span>Use regular trading hours</span>
              </label>
              <label className="toggle">
                <input
                  type="checkbox"
                  checked={configForm.strategy_active}
                  onChange={(event) =>
                    setConfigForm((current) => ({ ...current, strategy_active: event.target.checked }))
                  }
                />
                <span>Strategy active</span>
              </label>
            </div>

            <label className="full-width">
              <span>Notes</span>
              <textarea
                rows={6}
                value={configForm.notes}
                onChange={(event) => setConfigForm((current) => ({ ...current, notes: event.target.value }))}
                placeholder="Optional operator notes, suspensions, event risk, or manual overrides."
              />
            </label>
          </Section>
        </div>
      ) : null}

      {activeView === 'approvals' ? (
        <div className="grid monitor-grid">
          <Section
            title="Pending Approvals"
            subtitle="Manual gate before order placement"
            actions={
              <button className="button button-secondary" disabled={approvalsBusy} onClick={() => loadApprovals()}>
                {approvalsBusy ? 'Refreshing...' : 'Refresh'}
              </button>
            }
          >
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Signal</th>
                    <th>Strategy</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {approvals.map((row) => (
                    <tr
                      key={row.approval_request_id}
                      className={selectedApprovalId === row.approval_request_id ? 'row-selected' : ''}
                      onClick={() => setSelectedApprovalId(row.approval_request_id)}
                    >
                      <td>{row.symbol}</td>
                      <td>{row.final_signal}</td>
                      <td>{row.canonical_strategy_key || 'N/A'}</td>
                      <td>{formatDate(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {!approvals.length ? <p className="muted">No pending approvals.</p> : null}
            </div>
          </Section>

          <Section
            title="Approval Detail"
            subtitle={selectedApproval ? `${selectedApproval.symbol} • request ${selectedApproval.approval_request_id}` : 'Select an approval'}
            actions={
              <div className="toolbar">
                <button className="button button-primary" disabled={approvalActing || !selectedApprovalId} onClick={() => handleApprovalDecision('approve')}>
                  {approvalActing ? 'Working...' : 'Approve'}
                </button>
                <button className="button button-danger" disabled={approvalActing || !selectedApprovalId} onClick={() => handleApprovalDecision('reject')}>
                  Reject
                </button>
              </div>
            }
          >
            {!selectedApproval ? <p className="muted">Select a pending approval on the left to review it.</p> : null}
            {selectedApproval ? (
              <div className="stack">
                <div className="inline-stats">
                  <Pill label="Signal" value={selectedApproval.final_signal} tone="accent" />
                  <Pill label="Created" value={formatDate(selectedApproval.created_at)} />
                  <Pill label="Run ID" value={String(selectedApproval.signal_run_id)} />
                </div>

                <div>
                  <h3>Report</h3>
                  <TextBlock value={selectedApproval.report_markdown} empty="No report markdown stored." />
                </div>

                <div>
                  <h3>Charts</h3>
                  {approvalCharts.length ? (
                    <div className="chart-grid">
                      {approvalCharts.map((item) => (
                        <figure key={`${item.label}-${item.url}`} className="chart-card">
                          <img src={item.url} alt={item.label} />
                          <figcaption>{item.label}</figcaption>
                        </figure>
                      ))}
                    </div>
                  ) : (
                    <p className="muted">No chart images attached to this approval.</p>
                  )}
                </div>

                <div className="double-stack">
                  <div>
                    <h3>Decision Payload</h3>
                    <JsonBlock value={selectedApproval.decision} empty="No decision JSON stored." />
                  </div>
                  <div>
                    <h3>LLM Response</h3>
                    <TextBlock value={selectedApproval.llm_response_text} empty="No LLM response stored." />
                  </div>
                </div>

                <label className="full-width">
                  <span>Reviewer Comment</span>
                  <textarea
                    rows={5}
                    value={approvalComment}
                    onChange={(event) => setApprovalComment(event.target.value)}
                    placeholder="Document why you approve or reject this order candidate."
                  />
                </label>

                <div>
                  <h3>Linked Run Detail</h3>
                  <JsonBlock value={approvalRunDetail} empty="Linked run detail unavailable." />
                </div>
              </div>
            ) : null}
          </Section>
        </div>
      ) : null}

      {activeView === 'portfolio-overview' ? (
        <div className="grid two-up">
          <Section
            title="Latest Snapshot"
            subtitle={portfolio?.snapshot ? `Snapshot ${portfolio.snapshot.snapshot_id}` : 'No portfolio snapshot'}
            actions={
              <button className="button button-primary" disabled={portfolioBusy} onClick={handleSyncPortfolio}>
                {portfolioBusy ? 'Syncing...' : 'Sync IBKR'}
              </button>
            }
          >
            {portfolio?.snapshot ? (
              <div className="stack">
                <div className="inline-stats">
                  <Pill label="Account" value={portfolio.snapshot.account_id || 'N/A'} tone="accent" />
                  <Pill label="Status" value={portfolio.snapshot.status || 'N/A'} />
                  <Pill label="Captured" value={formatDate(portfolio.snapshot.created_at)} />
                </div>
                <div className="metric-grid">
                  {portfolioHighlights.map((item) => (
                    <div key={item.label} className="metric-card">
                      <span>{item.label}</span>
                      <strong>{item.value}</strong>
                    </div>
                  ))}
                </div>
                <div>
                  <h3>Summary JSON</h3>
                  <JsonBlock value={portfolio.snapshot.summary_json} />
                </div>
              </div>
            ) : (
              <p className="muted">No portfolio snapshot is stored yet.</p>
            )}
          </Section>

          <Section title="Exposure Snapshot" subtitle="Quick counts from the latest capture">
            <div className="metric-grid">
              <div className="metric-card">
                <span>Open Positions</span>
                <strong>{portfolio?.positions?.length ?? 0}</strong>
              </div>
              <div className="metric-card">
                <span>Open Orders</span>
                <strong>{portfolio?.orders?.length ?? 0}</strong>
              </div>
              <div className="metric-card">
                <span>Snapshot Status</span>
                <strong>{portfolio?.snapshot?.status ?? 'missing'}</strong>
              </div>
              <div className="metric-card">
                <span>Last Error</span>
                <strong>{portfolio?.snapshot?.error_message || 'none'}</strong>
              </div>
            </div>
          </Section>
        </div>
      ) : null}

      {activeView === 'portfolio-ledger' ? (
        <div className="grid two-up">
          <Section title="Positions" subtitle="Latest stored IBKR positions">
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Qty</th>
                    <th>Avg Cost</th>
                    <th>Market Value</th>
                    <th>Unrealized PnL</th>
                    <th>Currency</th>
                  </tr>
                </thead>
                <tbody>
                  {(portfolio?.positions ?? []).map((row, index) => (
                    <tr key={`${String(row.symbol ?? 'row')}-${index}`}>
                      <td>{String(row.symbol ?? 'N/A')}</td>
                      <td>{formatNumber(row.quantity, 2)}</td>
                      <td>{formatNumber(row.average_cost, 2)}</td>
                      <td>{formatNumber(row.market_value, 2)}</td>
                      <td>{formatNumber(row.unrealized_pnl, 2)}</td>
                      <td>{String(row.currency ?? 'N/A')}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Section>

          <Section title="Orders" subtitle="Latest stored IBKR open orders">
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Side</th>
                    <th>Qty</th>
                    <th>Filled</th>
                    <th>Type</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {(portfolio?.orders ?? []).map((row, index) => (
                    <tr key={`${String(row.ibkr_order_id ?? 'order')}-${index}`}>
                      <td>{String(row.symbol ?? 'N/A')}</td>
                      <td>{String(row.side ?? 'N/A')}</td>
                      <td>{formatNumber(row.quantity, 2)}</td>
                      <td>{formatNumber(row.filled_quantity, 2)}</td>
                      <td>{String(row.order_type ?? 'N/A')}</td>
                      <td>{String(row.status ?? 'N/A')}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Section>
        </div>
      ) : null}
    </div>
  )
}

export default App
