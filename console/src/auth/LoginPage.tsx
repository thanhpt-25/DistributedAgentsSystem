import { cilLockLocked } from '@coreui/icons'
import CIcon from '@coreui/icons-react'
import {
  CAlert, CButton, CCard, CCardBody, CCol, CContainer, CForm, CFormInput, CInputGroup, CInputGroupText, CRow, CSpinner,
} from '@coreui/react'
import { useState, type FormEvent } from 'react'
import { useAuth } from './AuthContext'

function KeyField({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <CInputGroup className="mb-4">
      <CInputGroupText><CIcon icon={cilLockLocked} /></CInputGroupText>
      <CFormInput
        type="password"
        name="key"
        placeholder="API key"
        aria-label="API key"
        autoComplete="off"
        autoFocus
        required
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    </CInputGroup>
  )
}

export default function LoginPage() {
  const { signIn, error } = useAuth()
  const [key, setKey] = useState('')
  const [busy, setBusy] = useState(false)

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true)
    await signIn(key.trim())
    setBusy(false)
  }

  return (
    <div className="min-vh-100 d-flex align-items-center login-page">
      <CContainer>
        <CRow className="justify-content-center">
          <CCol md={7} lg={5} xl={4}>
            <CCard className="p-2">
              <CCardBody>
                <CForm onSubmit={submit}>
                  <h1 className="brand-mark mb-1">das<span>.</span></h1>
                  <p className="text-body-secondary">
                    Distributed Agents System console. Operator keys unlock every page; tenant keys see their own tenant.
                  </p>
                  {error && <CAlert color="danger" role="alert">{error}</CAlert>}
                  <KeyField value={key} onChange={setKey} />
                  <CButton type="submit" color="primary" className="w-100" disabled={busy || !key.trim()}>
                    {busy ? <CSpinner size="sm" /> : 'Sign in'}
                  </CButton>
                </CForm>
              </CCardBody>
            </CCard>
          </CCol>
        </CRow>
      </CContainer>
    </div>
  )
}
