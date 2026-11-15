import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { HashRouter, Navigate, Route, Routes } from 'react-router-dom'
import { ApiError } from './api/client'
import { AuthProvider, useAuth } from './auth/AuthContext'
import LoginPage from './auth/LoginPage'
import Layout from './layout/Layout'
import Agents from './pages/Agents'
import Dlq from './pages/Dlq'
import DlqDetail from './pages/DlqDetail'
import Overview from './pages/Overview'
import RequestDetail from './pages/RequestDetail'
import Requests from './pages/Requests'
import Usage from './pages/Usage'
import { ConfirmProvider, FullPageSpinner, ThemeProvider, ToastProvider } from './ui/Providers'

const MAX_RETRIES = 2

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 2_000,
      // A 4xx is an answer (forbidden, not found), not a blip: do not retry it.
      retry: (count, error) => !(error instanceof ApiError && error.status < 500) && count < MAX_RETRIES,
    },
  },
})

/** Layout route: sign-in screen until the key is accepted, then the console shell with the page in its Outlet. */
function Gate() {
  const { status } = useAuth()
  if (status === 'loading') return <FullPageSpinner />
  if (status === 'anonymous') return <LoginPage />
  return <Layout />
}

function RequireAdmin({ children }: { children: ReactNode }) {
  const { caps } = useAuth()
  return caps.admin ? children : <Navigate to="/" replace />
}

/** `client` lets a test start from an empty query cache; the app itself uses the shared one. */
export default function App({ client = queryClient }: { client?: QueryClient }) {
  return (
    <QueryClientProvider client={client}>
      <ThemeProvider>
        <ToastProvider>
          <ConfirmProvider>
            <AuthProvider>
              <HashRouter>
                <Routes>
                  <Route element={<Gate />}>
                    <Route index element={<Overview />} />
                    <Route path="requests" element={<Requests />} />
                    <Route path="requests/:id" element={<RequestDetail />} />
                    <Route path="agents" element={<Agents />} />
                    <Route path="dlq" element={<RequireAdmin><Dlq /></RequireAdmin>} />
                    <Route path="dlq/:id" element={<RequireAdmin><DlqDetail /></RequireAdmin>} />
                    <Route path="usage" element={<Usage />} />
                    <Route path="*" element={<Navigate to="/" replace />} />
                  </Route>
                </Routes>
              </HashRouter>
            </AuthProvider>
          </ConfirmProvider>
        </ToastProvider>
      </ThemeProvider>
    </QueryClientProvider>
  )
}
