import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: process.env.VITE_API_URL || "http://localhost:8000",
        changeOrigin: true
      }
    }
  },
  resolve: {
    alias: {
      "@components": "/src/components",
      "@pages": "/src/pages",
      "@services": "/src/services",
      "@context": "/src/context",
      "@hooks": "/src/hooks",
      "@types": "/src/types",
  "@cc-types": "/src/types",
      "@routes": "/src/routes",
      "@theme": "/src/theme",
      "@utils": "/src/utils"
    }
  }
});
