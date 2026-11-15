import { useQueryClient } from '@tanstack/react-query'
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react'
import { ApiError, UNAUTHORIZED_EVENT, api, getKey, isRejectedKey, setKey, signInFailureMessage } from '../api/client'
import type { Capabilities } from '../api/types'

const TENANT_KEY = 'das.tenant'
const NO_ACCESS = new Set([401, 403, 404])
const ANONYMOUS: Capabilities = { operator: false, admin: false, tenants: [] }

type Status = 'loading' | 'anonymous' | 'authenticated'

interface AuthApi {
  status: Status
  caps: Capabilities
  /** The tenant an Operator scoped the console to; '' for all of them. Meaningless for other keys. */
  tenant: string
  error: string | undefined
  signIn: (key: string) => Promise<void>
  signOut: (message?: string) => void
  setTenant: (tenant: string) => void
}

const AuthContext = createContext<AuthApi | null>(null)

const isNoAccess = (e: unknown): boolean => e instanceof ApiError && NO_ACCESS.has(e.status)

/** What may this key do? Any valid key reads; the admin routes then say which pages to show. */
async function probe(): Promise<Capabilities> {
  await api('/v1/agents')
  try {
    const all = await api<{ tenants?: { tenant: string }[] }>('/v1/admin/usage')
    return { operator: true, admin: true, tenants: (all.tenants ?? []).map((t) => t.tenant) }
  } catch (e) {
    if (!isNoAccess(e)) throw e
  }
  try {
    await api('/v1/admin/dlq', { query: { limit: 1 } })
    return { operator: false, admin: true, tenants: [] }
  } catch (e) {
    if (!isNoAccess(e)) throw e
  }
  return ANONYMOUS
}

function readTenant(): string {
  try {
    return sessionStorage.getItem(TENANT_KEY) ?? ''
  } catch {
    return ''
  }
}

/** The Operator's tenant scope, remembered in sessionStorage for the tab. */
function useTenant() {
  const [tenant, setState] = useState('')
  const setTenant = useCallback((next: string) => {
    setState(next)
    try {
      sessionStorage.setItem(TENANT_KEY, next)
    } catch {
      /* not persisted */
    }
  }, [])
  /** Re-read the saved tenant, keeping it only if it still exists. */
  const restoreTenant = useCallback((known: string[]) => {
    const saved = readTenant()
    setState(known.includes(saved) ? saved : '')
  }, [])
  return { tenant, setTenant, restoreTenant }
}

/** A 401 after sign-in means the key was revoked or changed: sign out. */
function useRejectedKeyListener(active: boolean, signOut: (message?: string) => void) {
  useEffect(() => {
    if (!active) return
    const onRejected = () => signOut('Your session ended: the key was rejected.')
    window.addEventListener(UNAUTHORIZED_EVENT, onRejected)
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onRejected)
  }, [active, signOut])
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient()
  const [status, setStatus] = useState<Status>(() => (getKey() ? 'loading' : 'anonymous'))
  const [caps, setCaps] = useState<Capabilities>(ANONYMOUS)
  const [error, setError] = useState<string | undefined>()
  const { tenant, setTenant, restoreTenant } = useTenant()

  const signOut = useCallback(
    (message?: string) => {
      setKey('')
      queryClient.clear()
      setCaps(ANONYMOUS)
      setError(message)
      setStatus('anonymous')
    },
    [queryClient],
  )

  const signIn = useCallback(
    async (key: string) => {
      setKey(key)
      setStatus('loading')
      try {
        const found = await probe()
        setCaps(found)
        restoreTenant(found.tenants)
        setError(undefined)
        setStatus('authenticated')
      } catch (e) {
        // Forget the key only when the Gateway rejected it. A restart or a network blip during a reload must not sign you out.
        if (isRejectedKey(e)) setKey('')
        setError(signInFailureMessage(e))
        setStatus('anonymous')
      }
    },
    [restoreTenant],
  )

  useEffect(() => {
    const saved = getKey()
    // eslint-disable-next-line react-hooks/set-state-in-effect -- one-time restore of the stored key; the status starts at `loading` when there is one
    if (saved) void signIn(saved)
  }, [signIn])
  useRejectedKeyListener(status === 'authenticated', signOut)

  const value = useMemo(
    () => ({ status, caps, tenant, error, signIn, signOut, setTenant }),
    [status, caps, tenant, error, signIn, signOut, setTenant],
  )
  return <AuthContext value={value}>{children}</AuthContext>
}

export function useAuth(): AuthApi {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth outside AuthProvider')
  return ctx
}
