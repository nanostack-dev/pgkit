<script lang="ts">
	import '../app.css';
	import { ActivityIcon, Layers3Icon, WorkflowIcon, DatabaseIcon, RocketIcon, ChevronRightIcon } from 'lucide-svelte';
	import { page } from '$app/stores';

	let { children } = $props<{ children: () => unknown }>();

	const nav = [
		{ href: '/', label: 'Overview', icon: ActivityIcon },
		{ href: '/queues', label: 'Queues', icon: Layers3Icon },
		{ href: '/workflows', label: 'Workflows', icon: WorkflowIcon }
	];
</script>

<div class="flex h-screen bg-surface-50 text-surface-950 font-sans selection:bg-primary-200 selection:text-primary-900 overflow-hidden">
	<!-- Sidebar -->
	<aside class="w-64 flex-shrink-0 border-r border-surface-200 bg-white flex flex-col z-20">
		<div class="h-16 flex items-center px-6 border-b border-surface-200">
			<a href="/" class="flex items-center gap-3 text-primary-600 hover:opacity-80 transition-opacity">
				<div class="bg-primary-50 text-primary-600 p-1.5 rounded-lg border border-primary-100">
					<DatabaseIcon class="size-5" />
				</div>
				<span class="text-[1.05rem] font-bold tracking-tight text-surface-900">PGKit Control</span>
			</a>
		</div>
		
		<nav class="flex-1 overflow-y-auto p-4 space-y-1.5 mt-2">
			<div class="text-[0.65rem] font-bold text-surface-400 uppercase tracking-wider mb-3 px-3">Navigation</div>
			{#each nav as item}
				{@const active = $page.url.pathname === item.href || ($page.url.pathname.startsWith(item.href) && item.href !== '/')}
				<a 
					href={item.href} 
					class="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-200
					{active 
						? 'bg-primary-50 text-primary-700 shadow-sm ring-1 ring-primary-100/50' 
						: 'text-surface-600 hover:bg-surface-100 hover:text-surface-900'}"
				>
					<item.icon class="size-4 {active ? 'text-primary-600' : 'text-surface-400'}" />
					{item.label}
				</a>
			{/each}
		</nav>
		
		<div class="p-4 border-t border-surface-200 bg-surface-50/50">
			 <div class="flex items-center gap-3 bg-white p-3 rounded-xl border border-surface-200 shadow-sm">
				 <div class="relative flex h-2.5 w-2.5 items-center justify-center">
					<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-success-400 opacity-40"></span>
					<span class="relative inline-flex rounded-full h-2 w-2 bg-success-500"></span>
				 </div>
				 <div class="flex flex-col">
					 <span class="text-xs font-semibold text-surface-800">System Online</span>
					 <span class="text-[0.65rem] text-surface-500">PostgreSQL</span>
				 </div>
			 </div>
		</div>
	</aside>

	<!-- Main Content -->
	<main class="flex-1 flex flex-col min-w-0 overflow-hidden relative bg-surface-50/30">
		<!-- Header -->
		<header class="h-16 border-b border-surface-200 bg-white/80 backdrop-blur-xl flex items-center justify-between px-8 z-10 sticky top-0">
			<div class="flex items-center gap-2 text-sm text-surface-500">
				{#if $page.url.pathname === '/'}
					<span class="font-medium text-surface-900">Overview</span>
				{:else if $page.url.pathname.startsWith('/queues')}
					<Layers3Icon class="size-4" />
					<span class="font-medium text-surface-900">Queues</span>
				{:else if $page.url.pathname.startsWith('/workflows')}
					<a href="/workflows" class="hover:text-surface-900 transition-colors flex items-center gap-1.5"><WorkflowIcon class="size-4" /> Workflows</a>
					{#if $page.url.pathname.split('/').length > 2}
						<ChevronRightIcon class="size-3.5" />
						<span class="font-medium text-surface-900 mono text-xs">{$page.url.pathname.split('/')[2]}</span>
					{/if}
				{/if}
			</div>
			
			<div class="flex items-center gap-4">
				<div class="hidden md:flex items-center gap-2 px-3 py-1.5 rounded-full bg-surface-100 text-xs font-medium text-surface-600 border border-surface-200">
					<RocketIcon class="size-3.5 text-primary-500" />
					v1.0.0
				</div>
			</div>
		</header>

		<!-- Scrollable Content -->
		<div class="flex-1 overflow-y-auto relative z-0">
			<div class="absolute inset-0 pointer-events-none -z-10 bg-[radial-gradient(circle_at_top_right,var(--color-primary-500)_0%,transparent_20%),radial-gradient(circle_at_bottom_left,var(--color-secondary-500)_0%,transparent_20%)] opacity-[0.03]"></div>
			
			<div class="p-8 max-w-7xl mx-auto min-h-full">
				{@render children()}
			</div>
		</div>
	</main>
</div>
