import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { createBrowserRouter, RouterProvider } from 'react-router';
import { ApiError } from './api/client';
import { TooltipProvider } from './components/Tooltip';
import { DevDataToggle } from './dev/DevDataToggle';
import { AppShell } from './layout/AppShell';
import { ThemedToaster } from './layout/ThemedToaster';
import { LocksPage } from './routes/LocksPage';
import { NotFoundPage } from './routes/NotFoundPage';
import { OverviewPage } from './routes/OverviewPage';
import { QueuesPage } from './routes/QueuesPage';
import { RunPage } from './routes/RunPage';
import { WorkflowsPage } from './routes/WorkflowsPage';
import './app.css';

const queryClient = new QueryClient({
	defaultOptions: {
		queries: {
			staleTime: 1_000,
			retry: (failureCount, error) => !(error instanceof ApiError && error.status >= 400 && error.status < 500) && failureCount < 2,
			refetchOnWindowFocus: true,
		},
		mutations: { retry: false },
	},
});

const router = createBrowserRouter([
	{
		element: <AppShell />,
		children: [
			{ index: true, element: <OverviewPage /> },
			{ path: 'queues', element: <QueuesPage /> },
			{ path: 'workflows', element: <WorkflowsPage /> },
			{ path: 'workflows/:runId', element: <RunPage /> },
			{ path: 'locks', element: <LocksPage /> },
			{ path: '*', element: <NotFoundPage /> },
		],
	},
]);

createRoot(document.getElementById('root')!).render(
	<StrictMode>
		<QueryClientProvider client={queryClient}>
			<TooltipProvider>
				<RouterProvider router={router} />
				<ThemedToaster />
				{import.meta.env.DEV ? <DevDataToggle /> : null}
			</TooltipProvider>
		</QueryClientProvider>
	</StrictMode>,
);
