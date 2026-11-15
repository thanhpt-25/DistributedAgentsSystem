import { CChartBar } from '@coreui/react-chartjs'
import { useEffect, useState } from 'react'
import { Section } from './parts'

interface Colors {
  bar: string
  text: string
  grid: string
}

const CHART_HEIGHT = 224

function readColors(): Colors {
  const style = getComputedStyle(document.documentElement)
  const css = (name: string) => style.getPropertyValue(name).trim()
  return { bar: css('--cui-primary'), text: css('--cui-secondary-color'), grid: css('--cui-border-color') }
}

/** Chart colours follow the CoreUI theme variables, and refresh when the light/dark switch flips the theme. */
function useThemeColors(): Colors {
  const [colors, setColors] = useState(readColors)
  useEffect(() => {
    const observer = new MutationObserver(() => setColors(readColors()))
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-coreui-theme'] })
    return () => observer.disconnect()
  }, [])
  return colors
}

interface Props {
  title: string
  datasetLabel: string
  labels: string[]
  values: number[]
}

export default function BarChartCard({ title, datasetLabel, labels, values }: Props) {
  const c = useThemeColors()
  return (
    <Section title={title}>
      <div style={{ position: 'relative', height: CHART_HEIGHT }} role="img" aria-label={`${title}: ${datasetLabel} per day`}>
        <CChartBar
          height={CHART_HEIGHT}
          data={{ labels, datasets: [{ label: datasetLabel, data: values, backgroundColor: c.bar, borderRadius: 2 }] }}
          options={{
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
              x: { grid: { display: false }, ticks: { color: c.text } },
              y: { beginAtZero: true, grid: { color: c.grid }, ticks: { color: c.text } },
            },
          }}
        />
      </div>
    </Section>
  )
}
