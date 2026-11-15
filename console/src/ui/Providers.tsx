// App-wide UI services: colour mode, toasts, and a promise-based confirm dialog, all on CoreUI components.
import {
  CButton, CModal, CModalBody, CModalFooter, CModalHeader, CModalTitle, CSpinner, CToast, CToastBody, CToastClose, CToaster,
  useColorModes,
} from '@coreui/react'
import { createContext, useCallback, useContext, useMemo, useRef, useState, type ReactNode } from 'react'

// ---- colour mode (CoreUI sets data-coreui-theme on <html> and remembers the choice)

export type ColorMode = 'light' | 'dark' | 'auto'

interface ThemeApi {
  colorMode: ColorMode
  setColorMode: (mode: ColorMode) => void
}

const ThemeContext = createContext<ThemeApi | null>(null)

export function ThemeProvider({ children }: { children: ReactNode }) {
  const { colorMode, setColorMode } = useColorModes('das.theme')
  const value = useMemo<ThemeApi>(
    () => ({ colorMode: (colorMode as ColorMode | undefined) ?? 'auto', setColorMode }),
    [colorMode, setColorMode],
  )
  return <ThemeContext value={value}>{children}</ThemeContext>
}

export function useTheme(): ThemeApi {
  const ctx = useContext(ThemeContext)
  if (!ctx) throw new Error('useTheme outside ThemeProvider')
  return ctx
}

// ---- toasts

type ToastKind = 'success' | 'danger' | 'info'

interface ToastItem {
  id: number
  kind: ToastKind
  text: string
}

interface ToastApi {
  success: (text: string) => void
  error: (text: string) => void
  info: (text: string) => void
}

const ToastContext = createContext<ToastApi | null>(null)
const TOAST_MS = 3500
const ERROR_TOAST_MS = 7000

export function ToastProvider({ children }: { children: ReactNode }) {
  const [items, setItems] = useState<ToastItem[]>([])
  const seq = useRef(0)
  const push = useCallback((kind: ToastKind, text: string) => {
    seq.current += 1
    const id = seq.current
    setItems((prev) => [...prev, { id, kind, text }])
  }, [])
  const remove = useCallback((id: number) => setItems((prev) => prev.filter((t) => t.id !== id)), [])
  const api = useMemo<ToastApi>(
    () => ({ success: (t) => push('success', t), error: (t) => push('danger', t), info: (t) => push('info', t) }),
    [push],
  )
  return (
    <ToastContext value={api}>
      {children}
      <CToaster placement="bottom-end" className="p-3">
        {items.map((t) => (
          <CToast
            key={t.id}
            visible
            autohide
            delay={t.kind === 'danger' ? ERROR_TOAST_MS : TOAST_MS}
            color={t.kind}
            className="text-white align-items-center"
            onClose={() => remove(t.id)}
          >
            <div className="d-flex">
              <CToastBody>{t.text}</CToastBody>
              <CToastClose className="me-2 m-auto" white />
            </div>
          </CToast>
        ))}
      </CToaster>
    </ToastContext>
  )
}

export function useToast(): ToastApi {
  const ctx = useContext(ToastContext)
  if (!ctx) throw new Error('useToast outside ToastProvider')
  return ctx
}

// ---- confirm dialog

export interface ConfirmOptions {
  title: string
  body: ReactNode
  confirmLabel?: string
  danger?: boolean
}

interface Pending {
  options: ConfirmOptions
  resolve: (ok: boolean) => void
}

type Confirm = (options: ConfirmOptions) => Promise<boolean>

const ConfirmContext = createContext<Confirm | null>(null)

export function ConfirmProvider({ children }: { children: ReactNode }) {
  const [pending, setPending] = useState<Pending | null>(null)
  const last = useRef<Pending | null>(null) // keeps the text on screen while the modal fades out
  if (pending) last.current = pending
  const shown = pending ?? last.current

  const confirm = useCallback<Confirm>((options) => new Promise<boolean>((resolve) => setPending({ options, resolve })), [])
  const settle = (ok: boolean) => {
    pending?.resolve(ok)
    setPending(null)
  }

  return (
    <ConfirmContext value={confirm}>
      {children}
      <CModal visible={pending !== null} alignment="center" onClose={() => settle(false)}>
        <CModalHeader>
          <CModalTitle>{shown?.options.title}</CModalTitle>
        </CModalHeader>
        <CModalBody>{shown?.options.body}</CModalBody>
        <CModalFooter>
          <CButton color="secondary" variant="outline" onClick={() => settle(false)}>
            Cancel
          </CButton>
          <CButton color={shown?.options.danger ? 'danger' : 'primary'} onClick={() => settle(true)}>
            {shown?.options.confirmLabel ?? 'Confirm'}
          </CButton>
        </CModalFooter>
      </CModal>
    </ConfirmContext>
  )
}

export function useConfirm(): Confirm {
  const ctx = useContext(ConfirmContext)
  if (!ctx) throw new Error('useConfirm outside ConfirmProvider')
  return ctx
}

export function FullPageSpinner() {
  return (
    <div className="min-vh-100 d-flex align-items-center justify-content-center">
      <CSpinner color="primary" />
    </div>
  )
}
