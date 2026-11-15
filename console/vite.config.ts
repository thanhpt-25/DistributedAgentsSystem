import react from '@vitejs/plugin-react'
import { loadEnv } from 'vite'
import { defineConfig } from 'vitest/config'

// `npm run dev` proxies the API to a running Gateway; DAS_GATEWAY_URL overrides the default.
export default defineConfig(({ mode }) => {
  const gateway = loadEnv(mode, '.', '').DAS_GATEWAY_URL || 'http://localhost:8080'
  return {
    plugins: [react()],
    css: {
      preprocessorOptions: {
        // CoreUI's Sass still uses @import and global colour functions; they work, and the warnings are not ours to fix.
        scss: {
          quietDeps: true,
          silenceDeprecations: ['import', 'global-builtin', 'color-functions', 'if-function'] as never[],
        },
      },
    },
    server: { port: 5173, proxy: { '/v1': { target: gateway, changeOrigin: true }, '/healthz': gateway } },
    build: { chunkSizeWarningLimit: 900 },
    test: {
      environment: 'jsdom',
      setupFiles: ['src/test/setup.ts'],
      include: ['src/**/*.test.{ts,tsx}'], // the browser tests in e2e/ belong to Playwright, not to Vitest
      css: false,
      coverage: {
        provider: 'v8',
        include: ['src/**/*.{ts,tsx}'],
        exclude: ['src/**/*.test.*', 'src/test/**', 'src/main.tsx', 'src/vite-env.d.ts'],
        reporter: ['text-summary', 'text'],
        // A floor just under what the suite reaches today (90 / 92 / 86 / 77): coverage may grow, not quietly shrink.
        thresholds: { statements: 85, lines: 85, functions: 80, branches: 70 },
      },
    },
  }
})
