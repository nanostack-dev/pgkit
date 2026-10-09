import { Layers, LayoutDashboard, Lock, Search, ShieldOff, Workflow, type LucideIcon } from 'lucide-react';
import { useCallback, useState } from 'react';
import { NavLink, Outlet, ScrollRestoration } from 'react-router';
import { useConfig } from '../api/queries';
import { Popover } from '@base-ui/react/popover';
import { cn } from '../lib/cn';
import { useGlobalShortcuts } from '../lib/shortcuts';
import { CommandMenu } from './CommandMenu';
import { ShortcutsDialog } from './ShortcutsDialog';
import { ThemeMenu } from './ThemeMenu';

type NavItem = { to: string; label: string; icon: LucideIcon; end?: boolean };

const navItems: NavItem[] = [
	{ to: '/', label: 'Overview', icon: LayoutDashboard, end: true },
	{ to: '/queues', label: 'Queues', icon: Layers },
	{ to: '/workflows', label: 'Workflows', icon: Workflow },
	{ to: '/locks', label: 'Locks', icon: Lock },
];

function Brand() {
	return (
		<NavLink to="/" className="flex min-h-11 items-center gap-2.5 rounded-lg outline-offset-4">
			<span aria-hidden className="flex size-7 flex-col justify-center gap-[3px] rounded-lg bg-fg px-[7px]">
				<span className="h-[3px] w-full rounded-full bg-page" />
				<span className="h-[3px] w-2/3 rounded-full bg-page/70" />
				<span className="h-[3px] w-1/3 rounded-full bg-page/45" />
			</span>
			<span className="text-[0.9375rem] font-semibold tracking-[-0.01em]">
				pgkit <span className="font-normal text-muted">admin</span>
			</span>
		</NavLink>
	);
}

function ReadOnlyChip({ side = 'bottom' }: { side?: 'top' | 'bottom' }) {
	const config = useConfig();
	if (!config.data || config.data.mutations_enabled) return null;
	return (
		<Popover.Root>
			<Popover.Trigger
				openOnHover
				delay={200}
				className="press inline-flex h-8 items-center gap-1.5 rounded-full border border-line bg-surface-2 px-2.5 text-xs font-medium text-muted transition-[color,transform] hover:text-fg data-[popup-open]:text-fg"
			>
				<ShieldOff aria-hidden className="size-3.5" />
				Read-only
			</Popover.Trigger>
			<Popover.Portal>
				<Popover.Positioner side={side} align="end" sideOffset={8} className="z-50">
					<Popover.Popup className="popup w-72 max-w-[calc(100vw-2rem)] rounded-xl border border-line bg-surface p-3.5 text-[0.8125rem] text-fg shadow-popover outline-none">
						<Popover.Title className="font-medium">Read-only server</Popover.Title>
						<Popover.Description className="mt-1 text-muted">
							This server was started with mutations disabled, so replay, delete, enqueue, retry and cancel are unavailable. Everything
							else stays live.
						</Popover.Description>
					</Popover.Popup>
				</Popover.Positioner>
			</Popover.Portal>
		</Popover.Root>
	);
}

function SearchButton({ onClick, compact }: { onClick: () => void; compact?: boolean }) {
	if (compact) {
		return (
			<button
				type="button"
				onClick={onClick}
				aria-label="Open command menu"
				className="press inline-flex size-11 items-center justify-center rounded-lg text-muted hover:bg-surface-2 hover:text-fg"
			>
				<Search aria-hidden className="size-[1.125rem]" />
			</button>
		);
	}
	return (
		<button
			type="button"
			onClick={onClick}
			className="press flex h-9 w-full items-center gap-2 rounded-lg border border-line bg-surface px-2.5 text-[0.8125rem] text-subtle transition-[background-color,color,transform] hover:bg-surface-2 hover:text-muted"
		>
			<Search aria-hidden className="size-4" />
			<span className="flex-1 text-left">Search…</span>
			<kbd className="rounded border border-line px-1 font-mono text-[0.6875rem]">⌘K</kbd>
		</button>
	);
}

export function AppShell() {
	const [paletteOpen, setPaletteOpen] = useState(false);
	const [helpOpen, setHelpOpen] = useState(false);
	const openPalette = useCallback(() => setPaletteOpen((open) => !open), []);
	const openHelp = useCallback(() => setHelpOpen(true), []);
	useGlobalShortcuts({ onPalette: openPalette, onHelp: openHelp });

	return (
		<div className="min-h-dvh md:grid md:grid-cols-[15rem_minmax(0,1fr)]">
			<a
				href="#main"
				className="sr-only z-50 rounded-lg bg-fg px-3 py-2 text-page focus:not-sr-only focus:fixed focus:top-3 focus:left-3"
			>
				Skip to content
			</a>
			<aside className="sticky top-0 hidden h-dvh flex-col border-r border-line bg-page px-3 py-4 md:flex">
				<div className="px-2">
					<Brand />
				</div>
				<div className="mt-4 px-1">
					<SearchButton onClick={() => setPaletteOpen(true)} />
				</div>
				<nav aria-label="Main" className="mt-4 flex flex-col gap-0.5">
					{navItems.map((item) => (
						<NavLink
							key={item.to}
							to={item.to}
							end={item.end}
							className={({ isActive }) =>
								cn(
									'flex h-9 items-center gap-2.5 rounded-lg px-2.5 text-sm transition-colors',
									isActive ? 'bg-surface-3 font-medium text-fg' : 'text-muted hover:bg-surface-2 hover:text-fg',
								)
							}
						>
							<item.icon aria-hidden className="size-4" />
							{item.label}
						</NavLink>
					))}
				</nav>
				<div className="mt-auto flex items-center justify-between gap-2 px-1">
					<ReadOnlyChip side="top" />
					<div className="ml-auto flex items-center">
						<button
							type="button"
							onClick={() => setHelpOpen(true)}
							className="press inline-flex h-9 items-center rounded-lg px-2 font-mono text-xs text-subtle hover:bg-surface-2 hover:text-fg"
							aria-label="Keyboard shortcuts"
							title="Keyboard shortcuts"
						>
							?
						</button>
						<ThemeMenu side="top" />
					</div>
				</div>
			</aside>

			<div className="min-w-0">
				<header className="sticky top-0 z-30 border-b border-line bg-page/85 pt-[var(--safe-top)] backdrop-blur-md md:hidden">
					<div className="flex h-14 items-center gap-1 pr-1 pl-4">
						<Brand />
						<div className="ml-auto flex items-center">
							<ReadOnlyChip />
							<SearchButton compact onClick={() => setPaletteOpen(true)} />
							<ThemeMenu />
						</div>
					</div>
				</header>
				<main
					id="main"
					className="mx-auto w-full max-w-[84rem] px-4 pt-5 pb-[calc(var(--tabbar-height)+var(--safe-bottom)+2rem)] md:px-8 md:pt-8 md:pb-16"
				>
					<Outlet />
				</main>
			</div>

			<nav
				aria-label="Main"
				className="fixed inset-x-0 bottom-0 z-30 border-t border-line bg-surface/92 pb-[var(--safe-bottom)] backdrop-blur-md md:hidden"
			>
				<ul className="grid h-[var(--tabbar-height)] grid-cols-4">
					{navItems.map((item) => (
						<li key={item.to} className="flex">
							<NavLink
								to={item.to}
								end={item.end}
								className={({ isActive }) =>
									cn(
										'flex flex-1 flex-col items-center justify-center gap-0.5 text-[0.6875rem] font-medium transition-colors',
										isActive ? 'text-fg' : 'text-subtle',
									)
								}
							>
								<item.icon aria-hidden className="size-5" strokeWidth={1.9} />
								{item.label}
							</NavLink>
						</li>
					))}
				</ul>
			</nav>

			<CommandMenu open={paletteOpen} onOpenChange={setPaletteOpen} onShowShortcuts={() => setHelpOpen(true)} />
			<ShortcutsDialog open={helpOpen} onOpenChange={setHelpOpen} />
			<ScrollRestoration />
		</div>
	);
}
