// Server state for every page: React Query owns caching and polling, so no view runs its own timer.
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useMemo, useRef, useState } from 'react'
import { useAuth } from '../auth/AuthContext'
import { useToast } from '../ui/Providers'
import { api, errorMessage, fetchHealth, type Query } from './client'
import { followEvents, type StreamState } from './events'
import type {
  Agent, DeadLetter, DeadLetterDetail, DlqStatus, Page, RequestListItem, RequestStatus, RequestView, SubmitBody, UsageReport,
} from './types'

const POLL_MS = 5_000
const HEALTH_POLL_MS = 10_000
const USAGE_POLL_MS = 30_000
const DLQ_PAGE = 50
const FEED_MAX = 200
const REFRESH_DEBOUNCE_MS = 400

const toIso = (local: string): string | undefined => (local ? new Date(local).toISOString() : undefined)

/** `tenant=…` for an Operator who picked a tenant; empty for everyone else (their key already scopes them). */
export function useTenantQuery(): Query {
  const { caps, tenant } = useAuth()
  return useMemo(() => (caps.operator && tenant ? { tenant } : {}), [caps.operator, tenant])
}

export const useHealth = () =>
  useQuery({ queryKey: ['health'], queryFn: ({ signal }) => fetchHealth(signal), refetchInterval: HEALTH_POLL_MS })

export const useAgents = () =>
  useQuery({ queryKey: ['agents'], queryFn: ({ signal }) => api<Agent[]>('/v1/agents', { signal }), refetchInterval: POLL_MS })

// ---- requests

export interface RequestFilters {
  status: RequestStatus[]
  after: string
  before: string
  limit: number
}

export function useRequestList(filters: RequestFilters, autoRefresh: boolean) {
  const tenant = useTenantQuery()
  return useInfiniteQuery({
    queryKey: ['requests', 'list', filters, tenant],
    initialPageParam: undefined as string | undefined,
    queryFn: ({ pageParam, signal }) =>
      api<Page<RequestListItem>>('/v1/requests', {
        signal,
        query: {
          status: filters.status.join(',') || undefined,
          created_after: toIso(filters.after),
          created_before: toIso(filters.before),
          limit: filters.limit,
          cursor: pageParam,
          ...tenant,
        },
      }),
    getNextPageParam: (last) => last.next_cursor,
    refetchInterval: autoRefresh ? POLL_MS : false,
  })
}

/** One page of requests, for the overview's counts and recent list. */
export function useRequestPage(status: RequestStatus | undefined, limit: number) {
  const tenant = useTenantQuery()
  return useQuery({
    queryKey: ['requests', 'page', status, limit, tenant],
    queryFn: ({ signal }) => api<Page<RequestListItem>>('/v1/requests', { signal, query: { status, limit, ...tenant } }),
    refetchInterval: POLL_MS,
  })
}

export const useRequest = (id: string) =>
  useQuery({
    queryKey: ['request', id],
    queryFn: ({ signal }) => api<RequestView>(`/v1/requests/${encodeURIComponent(id)}`, { signal }),
    refetchInterval: POLL_MS,
  })

export function useSubmitRequest() {
  const queryClient = useQueryClient()
  const toast = useToast()
  return useMutation({
    mutationFn: (v: { body: SubmitBody; idempotencyKey: string }) =>
      api<{ request_id: string }>('/v1/requests', { method: 'POST', body: v.body, headers: { 'Idempotency-Key': v.idempotencyKey } }),
    onSuccess: () => {
      toast.success('Request submitted')
      void queryClient.invalidateQueries({ queryKey: ['requests'] })
    },
  })
}

export function useCancelRequest(id: string) {
  const queryClient = useQueryClient()
  const toast = useToast()
  return useMutation({
    mutationFn: () => api(`/v1/requests/${encodeURIComponent(id)}/cancel`, { method: 'POST' }),
    onSuccess: () => {
      toast.success('Request cancelled')
      void queryClient.invalidateQueries({ queryKey: ['request', id] })
      void queryClient.invalidateQueries({ queryKey: ['requests'] })
    },
    onError: (e) => toast.error(errorMessage(e)),
  })
}

export interface FeedEvent {
  key: string
  event: string
  text: string
}

const FINISHED = new Set<RequestStatus>(['Completed', 'Failed', 'Cancelled'])

/**
 * Live Event_Stream of one Request; each event also refreshes the Request after a short debounce. `status` is the
 * Request's latest known status: the stream ends on its own once a Request is finished, and when a replay reopens it
 * the feed follows it again.
 */
export function useEventFeed(requestId: string, status?: RequestStatus) {
  const queryClient = useQueryClient()
  const [events, setEvents] = useState<FeedEvent[]>([])
  const [state, setState] = useState<StreamState>('connecting')
  const [reopened, setReopened] = useState(0)
  const wasFinished = useRef(false)
  useEffect(() => {
    if (status === undefined) return
    const finished = FINISHED.has(status)
    if (wasFinished.current && !finished) setReopened((n) => n + 1)
    wasFinished.current = finished
  }, [status])
  useEffect(() => {
    const controller = new AbortController()
    let timer: number | undefined
    setEvents([])
    void followEvents(requestId, {
      signal: controller.signal,
      onState: setState,
      onEvent: (frame) => {
        const text = typeof frame.data === 'string' ? frame.data : JSON.stringify(frame.data)
        setEvents((prev) => [...prev, { key: `${frame.id}#${prev.length}`, event: frame.event, text }].slice(-FEED_MAX))
        window.clearTimeout(timer)
        timer = window.setTimeout(() => void queryClient.invalidateQueries({ queryKey: ['request', requestId] }), REFRESH_DEBOUNCE_MS)
      },
    })
    return () => {
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [requestId, queryClient, reopened])
  return { events, state }
}

// ---- dead letters (admin scope)

export interface DlqFilters {
  status: DlqStatus
  reason: string
  request_id: string
  capability: string
}

export function useDlqList(filters: DlqFilters) {
  const tenant = useTenantQuery()
  return useInfiniteQuery({
    queryKey: ['dlq', 'list', filters, tenant],
    initialPageParam: undefined as string | undefined,
    queryFn: ({ pageParam, signal }) =>
      api<Page<DeadLetter>>('/v1/admin/dlq', { signal, query: { ...filters, limit: DLQ_PAGE, cursor: pageParam, ...tenant } }),
    getNextPageParam: (last) => last.next_cursor,
    refetchInterval: POLL_MS,
  })
}

/** Pending records for the overview tile; quiet when the key cannot read the DLQ. */
export function useDlqPending() {
  const { caps } = useAuth()
  const tenant = useTenantQuery()
  return useQuery({
    queryKey: ['dlq', 'pending', tenant],
    queryFn: ({ signal }) => api<Page<DeadLetter>>('/v1/admin/dlq', { signal, query: { status: 'Pending', limit: 100, ...tenant } }),
    enabled: caps.admin,
    retry: false,
    refetchInterval: POLL_MS,
  })
}

export const useDlqItem = (id: string) =>
  useQuery({
    queryKey: ['dlq', 'item', id],
    queryFn: ({ signal }) => api<DeadLetterDetail>(`/v1/admin/dlq/${encodeURIComponent(id)}`, { signal }),
    refetchInterval: POLL_MS * 2,
  })

export function useDlqAction(kind: 'replay' | 'discard') {
  const queryClient = useQueryClient()
  const toast = useToast()
  return useMutation({
    mutationFn: (deadLetterId: string) =>
      api<{ job_id?: string; attempt?: number }>(`/v1/admin/dlq/${encodeURIComponent(deadLetterId)}/${kind}`, { method: 'POST' }),
    onSuccess: (r) => {
      toast.success(kind === 'replay' ? `Replayed: job ${r.job_id}, attempt ${r.attempt}` : 'Discarded')
      void queryClient.invalidateQueries({ queryKey: ['dlq'] })
      void queryClient.invalidateQueries({ queryKey: ['requests'] })
    },
    onError: (e) => toast.error(errorMessage(e)),
  })
}

// ---- agents

export function useAgentControl() {
  const queryClient = useQueryClient()
  const toast = useToast()
  return useMutation({
    mutationFn: (v: { agentId: string; command: 'drain' | 'undrain'; releaseLeases: boolean }) =>
      api<{ status: string }>(`/v1/admin/agents/${encodeURIComponent(v.agentId)}/${v.command}`, {
        method: 'POST',
        body: v.command === 'drain' ? { release_leases: v.releaseLeases } : undefined,
      }),
    onSuccess: (r, v) => {
      toast.success(`${v.command} accepted: now ${r.status}`)
      void queryClient.invalidateQueries({ queryKey: ['agents'] })
    },
    onError: (e) => toast.error(errorMessage(e)),
  })
}

// ---- usage

export function useUsage(range: { from: string; to: string }) {
  const { caps } = useAuth()
  const tenant = useTenantQuery()
  return useQuery({
    queryKey: ['usage', range, caps.operator, tenant],
    queryFn: async ({ signal }): Promise<UsageReport[]> => {
      if (!caps.operator) return [await api<UsageReport>('/v1/usage', { signal, query: range })]
      const r = await api<UsageReport | { tenants: UsageReport[] }>('/v1/admin/usage', { signal, query: { ...range, ...tenant } })
      return 'tenants' in r ? r.tenants : [r]
    },
    refetchInterval: USAGE_POLL_MS,
  })
}
