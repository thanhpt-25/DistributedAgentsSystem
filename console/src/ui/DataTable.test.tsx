import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import DataTable from './DataTable'

interface Row {
  id: string
  name: string
  count: number
}

const rows: Row[] = [
  { id: 'a', name: 'alpha', count: 3 },
  { id: 'b', name: 'beta', count: 12 },
]

const columns = [
  { header: 'Name', cell: (r: Row) => r.name },
  { header: 'Count', numeric: true, cell: (r: Row) => r.count },
]

describe('DataTable', () => {
  it('renders a header and one row per item, with numbers right-aligned', () => {
    render(<DataTable columns={columns} rows={rows} rowKey={(r) => r.id} />)
    expect(screen.getByRole('columnheader', { name: 'Count' })).toHaveClass('text-end')
    expect(screen.getByRole('cell', { name: 'beta' })).toBeInTheDocument()
    expect(screen.getAllByRole('row')).toHaveLength(3)
  })

  it('says why it is empty instead of drawing an empty table', () => {
    render(<DataTable columns={columns} rows={[]} rowKey={(r: Row) => r.id} empty="No agents match." />)
    expect(screen.getByText('No agents match.')).toBeInTheDocument()
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  it('opens a row on click and on Enter, so the keyboard works too', async () => {
    const open = vi.fn()
    render(<DataTable columns={columns} rows={rows} rowKey={(r) => r.id} onRowClick={open} />)
    await userEvent.click(screen.getByText('alpha'))
    expect(open).toHaveBeenLastCalledWith(rows[0])
    screen.getByText('beta').closest('tr')?.focus()
    await userEvent.keyboard('{Enter}')
    expect(open).toHaveBeenLastCalledWith(rows[1])
  })

  it('does not make rows focusable when nothing happens on click', () => {
    render(<DataTable columns={columns} rows={rows} rowKey={(r) => r.id} />)
    expect(screen.getByText('alpha').closest('tr')).not.toHaveAttribute('tabindex')
  })
})
