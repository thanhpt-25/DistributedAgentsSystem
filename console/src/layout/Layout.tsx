import { cilAccountLogout, cilChartPie, cilContrast, cilExternalLink, cilList, cilMemory, cilMenu, cilMoon, cilSpeedometer, cilSun, cilWarning } from '@coreui/icons'
import CIcon from '@coreui/icons-react'
import {
  CCloseButton, CContainer, CDropdown, CDropdownItem, CDropdownMenu, CDropdownToggle, CFormSelect, CHeader, CHeaderNav,
  CHeaderToggler, CNavItem, CNavLink, CSidebar, CSidebarBrand, CSidebarFooter, CSidebarHeader, CSidebarNav,
} from '@coreui/react'
import { useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'
import { useHealth } from '../api/hooks'
import { useAuth } from '../auth/AuthContext'
import { StatusBadge } from '../ui/parts'
import { useTheme, type ColorMode } from '../ui/Providers'

interface NavEntry {
  to: string
  label: string
  icon: string[]
  adminOnly?: boolean
}

const NAV: NavEntry[] = [
  { to: '/', label: 'Overview', icon: cilSpeedometer },
  { to: '/requests', label: 'Requests', icon: cilList },
  { to: '/agents', label: 'Agents', icon: cilMemory },
  { to: '/dlq', label: 'Dead letters', icon: cilWarning, adminOnly: true },
  { to: '/usage', label: 'Usage & cost', icon: cilChartPie },
]

const MODES: { mode: ColorMode; label: string; icon: string[] }[] = [
  { mode: 'light', label: 'Light', icon: cilSun },
  { mode: 'dark', label: 'Dark', icon: cilMoon },
  { mode: 'auto', label: 'Auto', icon: cilContrast },
]

const OBSERVABILITY = [
  { label: 'Grafana', port: 3000 },
  { label: 'Prometheus', port: 9090 },
  { label: 'Jaeger', port: 16686 },
]

function roleLabel(operator: boolean, admin: boolean): string {
  if (operator) return 'Operator'
  return admin ? 'Tenant admin' : 'Read-only'
}

/** Who you are, the Operator's tenant picker, and links to the observability stack. */
function SidebarFooter() {
  const { caps, tenant, setTenant } = useAuth()
  return (
    <CSidebarFooter className="border-top d-grid gap-2 small">
      <span className="text-body-secondary">{roleLabel(caps.operator, caps.admin)}</span>
      {caps.operator && (
        <CFormSelect size="sm" aria-label="Tenant scope" value={tenant} onChange={(e) => setTenant(e.target.value)}>
          <option value="">All tenants</option>
          {caps.tenants.map((t) => <option key={t} value={t}>{t}</option>)}
        </CFormSelect>
      )}
      <span className="text-body-secondary">Observability</span>
      {OBSERVABILITY.map((o) => (
        <a key={o.label} className="text-reset" href={`${location.protocol}//${location.hostname}:${o.port}`} target="_blank" rel="noopener noreferrer">
          {o.label} <CIcon icon={cilExternalLink} size="sm" />
        </a>
      ))}
    </CSidebarFooter>
  )
}

function AppSidebar({ visible, onVisible }: { visible: boolean; onVisible: (v: boolean) => void }) {
  const { caps } = useAuth()
  return (
    <CSidebar className="border-end" colorScheme="dark" position="fixed" visible={visible} onVisibleChange={onVisible}>
      <CSidebarHeader className="border-bottom">
        <CSidebarBrand as={NavLink} to="/">
          <span className="brand-mark">das<span>.</span></span>
        </CSidebarBrand>
        <CCloseButton className="d-lg-none" dark onClick={() => onVisible(false)} />
      </CSidebarHeader>
      <CSidebarNav>
        {NAV.filter((n) => !n.adminOnly || caps.admin).map((n) => (
          <CNavItem key={n.to}>
            <CNavLink as={NavLink} to={n.to} end={n.to === '/'}>
              <CIcon customClassName="nav-icon" icon={n.icon} />
              {n.label}
            </CNavLink>
          </CNavItem>
        ))}
      </CSidebarNav>
      <SidebarFooter />
    </CSidebar>
  )
}

function ColorModeMenu() {
  const { colorMode, setColorMode } = useTheme()
  const current = MODES.find((m) => m.mode === colorMode)?.icon ?? cilContrast
  return (
    <CDropdown variant="nav-item" placement="bottom-end">
      <CDropdownToggle caret={false} aria-label="Colour mode">
        <CIcon icon={current} size="lg" />
      </CDropdownToggle>
      <CDropdownMenu>
        {MODES.map((m) => (
          <CDropdownItem key={m.mode} as="button" type="button" active={colorMode === m.mode} className="d-flex align-items-center" onClick={() => setColorMode(m.mode)}>
            <CIcon className="me-2" icon={m.icon} size="lg" /> {m.label}
          </CDropdownItem>
        ))}
      </CDropdownMenu>
    </CDropdown>
  )
}

function AppHeader({ onToggle }: { onToggle: () => void }) {
  const { signOut } = useAuth()
  const health = useHealth().data
  return (
    <CHeader position="sticky" className="mb-4 p-0">
      <CContainer fluid className="border-bottom px-4">
        <CHeaderToggler onClick={onToggle} style={{ marginInlineStart: '-14px' }} aria-label="Toggle navigation">
          <CIcon icon={cilMenu} size="lg" />
        </CHeaderToggler>
        <CHeaderNav className="ms-auto align-items-center gap-2">
          <StatusBadge status={`Gateway ${health?.overall ?? '…'}`} tone={health?.overall === 'healthy' ? 'success' : health ? 'danger' : 'secondary'} />
          {health?.redis && <StatusBadge status={`Redis ${health.redis}`} tone={health.redis === 'connected' ? 'success' : 'danger'} />}
          <ColorModeMenu />
          <CHeaderToggler onClick={() => signOut()} aria-label="Sign out" title="Sign out">
            <CIcon icon={cilAccountLogout} size="lg" />
          </CHeaderToggler>
        </CHeaderNav>
      </CContainer>
    </CHeader>
  )
}

export default function Layout() {
  const [sidebarShow, setSidebarShow] = useState(true)
  return (
    <>
      <AppSidebar visible={sidebarShow} onVisible={setSidebarShow} />
      <div className="wrapper d-flex flex-column min-vh-100">
        <AppHeader onToggle={() => setSidebarShow(!sidebarShow)} />
        <div className="body flex-grow-1">
          <CContainer lg className="px-4 pb-5">
            <Outlet />
          </CContainer>
        </div>
      </div>
    </>
  )
}
