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
    test: { environment: 'node', include: ['src/**/*.test.ts'] },
  }
})
