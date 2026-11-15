// Shapes of the Gateway's JSON, as das/gateway/{app,listing,admin,usage}.py produce them. Timestamps are ISO 8601 UTC.

export const REQUEST_STATUSES = ['Pending', 'Running', 'Completed', 'Failed', 'Cancelled'] as const
export type RequestStatus = (typeof REQUEST_STATUSES)[number]
export type JobState = 'Waiting' | 'Ready' | 'Dispatched' | 'Succeeded' | 'Failed' | 'TimedOut' | 'Skipped' | 'Cancelled'
export type Priority = 'normal' | 'high'

export interface Page<T> {
  items: T[]
  next_cursor?: string
}

export interface TokenCounts {
  input: number
  output: number
  total: number
}

export interface RequestListItem {
  request_id: string
  status: RequestStatus
  created_at: string
  finished_at: string | null
  job_count: number
  token_usage: TokenCounts
}

export interface JobTransition {
  scope?: undefined
  job_id: string
  from_state: JobState | null
  to_state: JobState
  attempt: number
  worker_id?: string | null
  at: string
  dead_letter_id?: string | null
}

export interface RequestTransition {
  scope: 'request'
  from_status: RequestStatus
  to_status: RequestStatus
  dead_letter_id?: string | null
  at: string
}

export type Transition = JobTransition | RequestTransition

export interface Money {
  currency: string
  amount: string
  complete: boolean
}

export interface Budget {
  limits: { max_total_tokens?: number | null; max_cost?: string | null }
  used: { total_tokens: number; cost: string | null }
  exceeded: boolean
}

export interface JobReport {
  job_id: string
  state: JobState
  worker_id: string | null
  input_tokens: number
  output_tokens: number
}

export interface RequestView {
  request_id: string
  status: RequestStatus
  priority: Priority
  replans: number
  jobs: { job_id: string; state: JobState }[]
  transitions: Transition[]
  final_result?: { summary: string | null; jobs: JobReport[]; failures: unknown[]; error?: string | null }
  error?: string
  token_usage: Record<string, unknown>
  estimated_cost: Money | null
  replay_history: { dead_letter_id: string; at: string; error: string | null }[]
  budget: Budget | null
  webhook: { state: string; attempts: number; last_status: number | null } | null
}

export interface SubmitBody {
  prompt: string
  priority?: Priority
  budget?: { max_total_tokens?: number; max_cost?: string }
}

export type AgentStatus = 'Available' | 'Unavailable' | 'Draining'

export interface Agent {
  agent_id?: string // absent for a tenant other than the default one (Phase 5 Req 2.6)
  role: string
  host?: string
  capabilities: string[]
  status: AgentStatus
  last_heartbeat: string
  in_flight: number
  drained: boolean
}

export const DLQ_STATUSES = ['Pending', 'Replayed', 'Discarded'] as const
export type DlqStatus = (typeof DLQ_STATUSES)[number]
export const DLQ_REASONS = ['retries_exhausted', 'delivery_limit', 'malformed'] as const

export interface DeadLetter {
  dead_letter_id: string
  reason: (typeof DLQ_REASONS)[number]
  status: DlqStatus
  request_id: string | null
  job_id: string | null
  attempt: number | null
  capabilities: string[] | null
  last_error: string | null
  dead_lettered_at: string
  status_changed_at: string | null
  discarded_at: string | null
}

export interface DeadLetterDetail extends DeadLetter {
  message?: unknown
  raw_b64?: string
  truncated?: boolean
  request_status: string | null
  job_state: string | null
  job_attempt: number | null
  request_exists: boolean | null
}

export interface UsageModel {
  provider: string
  model: string
  input_tokens: number
  output_tokens: number
  total_tokens: number
  estimated_cost: { amount?: string } | string | number | null
}

export interface UsageDay {
  date: string
  requests: { submitted: number; finished: Record<string, number> }
  models: UsageModel[]
}

export interface UsageReport {
  tenant: string
  from: string
  to: string
  days: UsageDay[]
  totals: {
    requests: { submitted: number; finished: Record<string, number> }
    input_tokens: number
    output_tokens: number
    total_tokens: number
    estimated_cost: Money | null
  }
}

export interface Health {
  overall: string
  redis?: string
  http: number
}

/** What the signed-in key may do, probed once after sign-in. */
export interface Capabilities {
  operator: boolean
  admin: boolean
  tenants: string[]
}
