// The critical flows, in a real browser, against the production build and the mock Gateway (e2e/mock-gateway.mjs).
import { expect, test, type Page } from '@playwright/test'

const OPERATOR = 'e2e-operator-key'
const READER = 'e2e-reader-key'

test.beforeEach(async ({ request }) => {
  await request.post('/__reset') // the mock keeps state that drain, replay, cancel and submit change
})

async function signIn(page: Page, key: string) {
  await page.goto('/')
  await page.getByLabel('API key').fill(key)
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
}

test('refuses a wrong key', async ({ page }) => {
  await page.goto('/')
  await page.getByLabel('API key').fill('nope')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('alert')).toHaveText('That key was not accepted.')
})

test('shows an Operator the fleet, the queue and every page', async ({ page }) => {
  await signIn(page, OPERATOR)
  await expect(page.getByText('3/4')).toBeVisible() // agents available
  await expect(page.getByRole('link', { name: 'Dead letters' })).toBeVisible()
  await expect(page.getByLabel('Tenant scope')).toBeVisible()
})

test('shows a request with its result, and follows it live until it finishes', async ({ page }) => {
  await signIn(page, OPERATOR)
  await page.goto('/#/requests/r-done')
  await expect(page.getByText('Answer from the stub.')).toBeVisible()
  await expect(page.getByText('Worker answer j1', { exact: true })).toBeVisible() // each worker's own result, not only the summary
  await expect(page.getByRole('log', { name: 'Live events' })).toContainText('request.completed')
  await expect(page.getByText('ended')).toBeVisible() // the stream stops by itself once the request is finished
})

test('submits a request and opens it', async ({ page }) => {
  await signIn(page, OPERATOR)
  await page.getByRole('link', { name: 'Requests', exact: true }).click() // not "All requests" on the Overview
  await page.getByRole('button', { name: 'New request' }).click()
  await page.getByRole('dialog').getByLabel('Prompt').fill('Compare queues and HTTP for job dispatch')
  await page.getByRole('dialog').getByRole('button', { name: 'Submit' }).click()
  await expect(page).toHaveURL(/#\/requests\/r-new$/)
})

test('cancels a running request after confirming', async ({ page }) => {
  await signIn(page, OPERATOR)
  await page.goto('/#/requests/r-run')
  await page.getByRole('button', { name: 'Cancel request' }).click()
  await page.getByRole('dialog').getByRole('button', { name: 'Cancel request' }).click()
  await expect(page.getByText('Request cancelled')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Cancel request' })).toBeDisabled()
})

test('drains a worker, and the registry shows it draining', async ({ page }) => {
  await signIn(page, OPERATOR)
  await page.getByRole('link', { name: 'Agents' }).click()
  const row = page.getByRole('row', { name: /worker-research/ })
  await row.getByRole('button', { name: 'Drain' }).click()
  await page.getByRole('dialog').getByRole('button', { name: 'Drain' }).click()
  await expect(page.getByText('drain accepted: now Draining')).toBeVisible()
  await expect(row.getByText('Draining')).toBeVisible()
})

test('replays a dead letter, which then leaves the pending queue', async ({ page }) => {
  await signIn(page, OPERATOR)
  await page.getByRole('link', { name: 'Dead letters' }).click()
  await page.getByRole('row', { name: /retries_exhausted/ }).getByRole('button', { name: 'Replay' }).click()
  await page.getByRole('dialog').getByRole('button', { name: 'Replay' }).click()
  await expect(page.getByText('Replayed: job j1, attempt 4')).toBeVisible()
  await expect(page.getByText('No dead letters match. That is good news.')).toBeVisible()
})

test('keeps a read-only key out of the admin pages', async ({ page }) => {
  await signIn(page, READER)
  await expect(page.getByRole('link', { name: 'Dead letters' })).toHaveCount(0)
  await page.goto('/#/dlq')
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible() // redirected, not shown an error
})

test('a Gateway outage during a reload does not sign you out', async ({ page }) => {
  await signIn(page, OPERATOR)
  await page.route('**/v1/agents', (route) => route.abort())
  await page.reload()
  await expect(page.getByRole('alert')).toContainText('Your key is kept')
  await page.unroute('**/v1/agents')
  await page.reload()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible() // signed in again, without typing the key
})

test('remembers the colour mode across a reload', async ({ page }) => {
  await signIn(page, OPERATOR)
  await page.getByLabel('Colour mode').click()
  await page.getByRole('button', { name: 'Dark' }).click()
  await expect(page.locator('html')).toHaveAttribute('data-coreui-theme', 'dark')
  await page.reload()
  await expect(page.locator('html')).toHaveAttribute('data-coreui-theme', 'dark')
})
