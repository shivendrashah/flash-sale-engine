// =====================================================================================================
// The frontend's build tool.
//   npm run dev     a dev server on http://localhost:5173 that reloads on every save. It forwards /api/... to
//                   http://localhost:8080: the app started with ./mvnw spring-boot:run, or nginx when the cluster
//                   runs (docker compose --profile cluster up). The browser sees one origin, so no CORS is needed.
//   npm run build   type-checks, then writes the optimized files to dist/. nginx serves those in the cluster
//                   (nginx/Dockerfile).
// =====================================================================================================
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
});
