// What each Worker was asked and answered, one panel per Job (the Gateway's `?detail=true` view).
import type { JobView } from '../api/types'
import { num } from '../lib/format'
import { Section, StatusBadge } from '../ui/parts'

const took = (ms: number): string => (ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1)}s`)

function JobPanel({ job }: { job: JobView }) {
  const res = job.result
  const failure = res?.error ?? job.reason
  return (
    <details className="job-panel p-3 border-bottom" open={res !== null}>
      <summary className="d-flex flex-wrap gap-2 align-items-center">
        <span className="mono">{job.job_id}</span>
        <StatusBadge status={job.state} />
        <span className="small text-body-secondary">
          {job.worker_id ?? 'no worker'}
          {job.attempt > 0 ? ` · attempt ${job.attempt}` : ''}
          {res ? ` · ${num(res.usage.input_tokens)} in / ${num(res.usage.output_tokens)} out · ${took(res.duration_ms)}` : ''}
        </span>
      </summary>
      <div className="d-grid gap-2 mt-3">
        <div>
          <div className="eyebrow mb-1">Instruction</div>
          <pre className="block">{job.instruction}</pre>
        </div>
        {job.depends_on.length > 0 && <div className="small text-body-secondary">Depends on {job.depends_on.join(', ')}</div>}
        {res && (
          <div>
            <div className="eyebrow mb-1">Result ({res.status})</div>
            <pre className="block">{res.content || '(empty)'}</pre>
          </div>
        )}
        {!res && <div className="text-body-secondary small">No result yet.</div>}
        {failure && <div className="text-danger small">{failure}</div>}
      </div>
    </details>
  )
}

export default function JobResultsCard({ jobs }: { jobs: JobView[] }) {
  if (jobs.length === 0) return null
  return (
    <Section title="Worker results" flush>
      {jobs.map((j) => <JobPanel key={j.job_id} job={j} />)}
    </Section>
  )
}
