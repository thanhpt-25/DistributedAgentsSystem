import { CTable, CTableBody, CTableDataCell, CTableHead, CTableHeaderCell, CTableRow } from '@coreui/react'
import type { ReactNode } from 'react'
import { EmptyState } from './parts'

export interface Column<T> {
  header: string
  cell: (row: T) => ReactNode
  numeric?: boolean
}

interface Props<T> {
  columns: Column<T>[]
  rows: T[]
  rowKey: (row: T) => string
  onRowClick?: (row: T) => void
  empty?: string
}

/** CoreUI table with right-aligned numeric columns and keyboard-reachable clickable rows. */
export default function DataTable<T>({ columns, rows, rowKey, onRowClick, empty = 'Nothing here.' }: Props<T>) {
  if (!rows.length) return <EmptyState text={empty} />
  return (
    <CTable hover={Boolean(onRowClick)} responsive align="middle" className="mb-0">
      <CTableHead>
        <CTableRow>
          {columns.map((c, i) => (
            <CTableHeaderCell key={`${c.header}-${i}`} className={c.numeric ? 'text-end' : undefined}>
              {c.header}
            </CTableHeaderCell>
          ))}
        </CTableRow>
      </CTableHead>
      <CTableBody>
        {rows.map((row) => (
          <CTableRow
            key={rowKey(row)}
            className={onRowClick ? 'click-row' : undefined}
            tabIndex={onRowClick ? 0 : undefined}
            onClick={onRowClick ? () => onRowClick(row) : undefined}
            onKeyDown={onRowClick ? (e) => e.key === 'Enter' && onRowClick(row) : undefined}
          >
            {columns.map((c, i) => (
              <CTableDataCell key={`${c.header}-${i}`} className={c.numeric ? 'text-end' : undefined}>
                {c.cell(row)}
              </CTableDataCell>
            ))}
          </CTableRow>
        ))}
      </CTableBody>
    </CTable>
  )
}
