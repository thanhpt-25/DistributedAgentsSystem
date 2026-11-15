import {
  CAlert, CButton, CCol, CForm, CFormInput, CFormLabel, CFormSelect, CFormTextarea, CModal, CModalBody, CModalFooter, CModalHeader,
  CModalTitle, CRow, CSpinner,
} from '@coreui/react'
import { useState, type FormEvent } from 'react'
import { errorMessage } from '../api/client'
import { useSubmitRequest } from '../api/hooks'
import { buildSubmitBody, type SubmitForm } from '../lib/submit'

const EMPTY: SubmitForm = { prompt: '', priority: '', tokens: '', cost: '' }

interface FieldsProps {
  form: SubmitForm
  set: (patch: Partial<SubmitForm>) => void
}

function LimitFields({ form, set }: FieldsProps) {
  return (
    <CRow className="g-3">
      <CCol sm={4}>
        <CFormLabel htmlFor="priority">Priority</CFormLabel>
        <CFormSelect id="priority" value={form.priority} onChange={(e) => set({ priority: e.target.value })}>
          <option value="">tenant default</option>
          <option value="normal">normal</option>
          <option value="high">high</option>
        </CFormSelect>
      </CCol>
      <CCol sm={4}>
        <CFormLabel htmlFor="tokens">Max tokens</CFormLabel>
        <CFormInput id="tokens" type="number" min={1} step={1} placeholder="no limit" value={form.tokens} onChange={(e) => set({ tokens: e.target.value })} />
      </CCol>
      <CCol sm={4}>
        <CFormLabel htmlFor="cost">Max cost</CFormLabel>
        <CFormInput id="cost" type="number" min={0} step="any" placeholder="no limit" value={form.cost} onChange={(e) => set({ cost: e.target.value })} />
      </CCol>
    </CRow>
  )
}

interface Props {
  onClose: () => void
  onDone: (requestId: string) => void
}

/** Rendered only while open, so every opening gets a fresh Idempotency-Key: a double click replays instead of duplicating. */
export default function SubmitModal({ onClose, onDone }: Props) {
  const [idempotencyKey] = useState(() => crypto.randomUUID())
  const submit = useSubmitRequest()
  const [form, setForm] = useState<SubmitForm>(EMPTY)
  const set = (patch: Partial<SubmitForm>) => setForm((f) => ({ ...f, ...patch }))

  const send = (e: FormEvent) => {
    e.preventDefault()
    submit.mutate(
      { body: buildSubmitBody(form), idempotencyKey },
      { onSuccess: (r) => { onClose(); onDone(r.request_id) } },
    )
  }

  return (
    <CModal visible alignment="center" size="lg" onClose={onClose}>
      <CForm onSubmit={send}>
        <CModalHeader><CModalTitle>New request</CModalTitle></CModalHeader>
        <CModalBody className="d-grid gap-3">
          <div>
            <CFormLabel htmlFor="prompt">Prompt</CFormLabel>
            <CFormTextarea id="prompt" rows={6} required autoFocus placeholder="What should the agents do?" value={form.prompt} onChange={(e) => set({ prompt: e.target.value })} />
          </div>
          <LimitFields form={form} set={set} />
          {submit.isError && <CAlert color="danger" className="mb-0">{errorMessage(submit.error)}</CAlert>}
        </CModalBody>
        <CModalFooter>
          <CButton type="button" color="secondary" variant="outline" onClick={onClose}>Cancel</CButton>
          <CButton type="submit" color="primary" disabled={submit.isPending || !form.prompt.trim()}>
            {submit.isPending ? <CSpinner size="sm" /> : 'Submit'}
          </CButton>
        </CModalFooter>
      </CForm>
    </CModal>
  )
}
