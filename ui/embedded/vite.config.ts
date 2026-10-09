import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig, loadEnv } from 'vite';

export default defineConfig(({ mode }) => {
	const env = loadEnv(mode, process.cwd(), 'PGKIT_');
	const target = env.PGKIT_ADMIN_API ?? 'http://127.0.0.1:18083';
	const token = env.PGKIT_DASHBOARD_TOKEN ?? 'change-me';
	const authorization = `Basic ${Buffer.from(`dev:${token}`).toString('base64')}`;

	return {
		plugins: [react(), tailwindcss()],
		build: {
			outDir: 'build',
			assetsDir: '_app',
			emptyOutDir: true,
			sourcemap: false,
			target: 'es2022',
			chunkSizeWarningLimit: 1024,
		},
		server: {
			port: 4173,
			strictPort: true,
			proxy: {
				'/api': {
					target,
					changeOrigin: true,
					headers: { authorization },
				},
			},
		},
	};
});
