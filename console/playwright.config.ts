import { defineConfig, devices } from '@playwright/test'

const PORT = 4173

// The browser tests run against the production build (npm run build) served by e2e/mock-gateway.mjs, which also stands
// in for the Gateway. One worker, because the mock keeps state that the mutating tests change.
export default defineConfig({
  testDir: 'e2e',
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? 'github' : 'list',
  use: { baseURL: `http://127.0.0.1:${PORT}`, trace: 'retain-on-failure' },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command: 'node e2e/mock-gateway.mjs',
    url: `http://127.0.0.1:${PORT}/healthz`,
    env: { PORT: String(PORT) },
    reuseExistingServer: !process.env.CI,
  },
})
