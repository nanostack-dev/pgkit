import { Dialog } from '@base-ui/react/dialog';
import { Command } from 'cmdk';
import {
	ArrowUpRight,
	Hash,
	Keyboard,
	Layers,
	LayoutDashboard,
	Lock,
	Monitor,
	Moon,
	Search,
	Sun,
	Workflow,
	type LucideIcon,
} from 'lucide-react';
import { useState, type ReactNode } from 'react';
import { useNavigate } from 'react-router';
import { setThemePreference } from '../lib/theme';

const itemClass =
	'flex min-h-11 cursor-default items-center gap-3 rounded-lg px-3 text-sm text-fg outline-none select-none data-[selected=true]:bg-surface-2 md:min-h-9';

function Item({
	icon: Icon,
	children,
	hint,
	onSelect,
	value,
	forceMount,
}: {
	icon: LucideIcon;
	children: ReactNode;
	hint?: string;
	onSelect: () => void;
	value: string;
	forceMount?: boolean;
}) {
	return (
		<Command.Item value={value} onSelect={onSelect} forceMount={forceMount} className={itemClass}>
			<Icon aria-hidden className="size-4 shrink-0 text-muted" />
			<span className="min-w-0 flex-1 truncate">{children}</span>
			{hint ? <kbd className="hidden font-mono text-[0.6875rem] text-subtle md:inline">{hint}</kbd> : null}
		</Command.Item>
	);
}

export function CommandMenu({
	open,
	onOpenChange,
	onShowShortcuts,
}: {
	open: boolean;
	onOpenChange: (open: boolean) => void;
	onShowShortcuts: () => void;
}) {
	const navigate = useNavigate();
	const [query, setQuery] = useState('');
	const trimmed = query.trim();
	const jobId = /^#?\d+$/.test(trimmed) ? trimmed.replace('#', '') : null;

	function go(path: string) {
		onOpenChange(false);
		setQuery('');
		navigate(path);
	}

	function run(action: () => void) {
		onOpenChange(false);
		setQuery('');
		action();
	}

	return (
		<Dialog.Root
			open={open}
			onOpenChange={(next) => {
				onOpenChange(next);
				if (!next) setQuery('');
			}}
		>
			<Dialog.Portal>
				<Dialog.Backdrop className="fixed inset-0 z-50 bg-[var(--overlay)]" />
				<Dialog.Viewport className="fixed inset-0 z-50 flex items-start justify-center px-3 pt-[calc(var(--safe-top)+0.75rem)] md:pt-[12vh]">
					<Dialog.Popup className="w-full max-w-xl overflow-hidden rounded-2xl border border-line bg-surface text-fg shadow-popover outline-none">
						<Dialog.Title className="sr-only">Command menu</Dialog.Title>
						<Command label="Command menu" loop>
							<div className="flex items-center gap-2.5 border-b border-line px-4">
								<Search aria-hidden className="size-4 shrink-0 text-subtle" />
								<Command.Input
									value={query}
									onValueChange={setQuery}
									placeholder="Jump to a page, run ID, job number or key…"
									className="h-13 min-w-0 flex-1 bg-transparent text-base outline-none placeholder:text-subtle md:h-12 md:text-sm"
								/>
								<kbd className="hidden rounded border border-line px-1.5 font-mono text-[0.6875rem] text-subtle md:inline">esc</kbd>
							</div>
							<Command.List className="max-h-[min(26rem,60dvh)] overflow-y-auto overscroll-contain p-1.5">
								<Command.Empty className="px-3 py-8 text-center text-sm text-muted">No matching commands.</Command.Empty>
								{trimmed ? (
									<Command.Group heading="Find" forceMount className="[&_[cmdk-group-heading]]:px-3 [&_[cmdk-group-heading]]:pt-2 [&_[cmdk-group-heading]]:pb-1 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:text-subtle">
										{jobId ? (
											<Item icon={Hash} value={`open job ${jobId}`} forceMount onSelect={() => go(`/queues?job=${jobId}`)}>
												Open job #{jobId}
											</Item>
										) : null}
										{!jobId && trimmed.length >= 8 ? (
											<Item
												icon={ArrowUpRight}
												value={`open run ${trimmed}`}
												forceMount
												onSelect={() => go(`/workflows/${encodeURIComponent(trimmed)}`)}
											>
												Open run <code className="font-mono">{trimmed}</code>
											</Item>
										) : null}
										<Item
											icon={Workflow}
											value={`search runs ${trimmed}`}
											forceMount
											onSelect={() => go(`/workflows?search=${encodeURIComponent(trimmed)}&children=1`)}
										>
											Search runs for “{trimmed}”
										</Item>
										<Item
											icon={Layers}
											value={`search jobs ${trimmed}`}
											forceMount
											onSelect={() => go(`/queues?search=${encodeURIComponent(trimmed)}`)}
										>
											Search jobs for “{trimmed}”
										</Item>
									</Command.Group>
								) : null}
								<Command.Group heading="Pages" className="[&_[cmdk-group-heading]]:px-3 [&_[cmdk-group-heading]]:pt-2 [&_[cmdk-group-heading]]:pb-1 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:text-subtle">
									<Item icon={LayoutDashboard} value="overview health dashboard" hint="G O" onSelect={() => go('/')}>
										Overview
									</Item>
									<Item icon={Layers} value="queues jobs" hint="G Q" onSelect={() => go('/queues')}>
										Queues
									</Item>
									<Item icon={Workflow} value="workflows runs" hint="G W" onSelect={() => go('/workflows')}>
										Workflows
									</Item>
									<Item icon={Lock} value="advisory locks" hint="G L" onSelect={() => go('/locks')}>
										Locks
									</Item>
									<Item icon={Workflow} value="failed runs" onSelect={() => go('/workflows?status=failed')}>
										Failed runs
									</Item>
									<Item icon={Workflow} value="waiting runs" onSelect={() => go('/workflows?status=waiting')}>
										Waiting runs
									</Item>
									<Item icon={Layers} value="failed jobs" onSelect={() => go('/queues?status=failed')}>
										Failed jobs
									</Item>
								</Command.Group>
								<Command.Group heading="Preferences" className="[&_[cmdk-group-heading]]:px-3 [&_[cmdk-group-heading]]:pt-2 [&_[cmdk-group-heading]]:pb-1 [&_[cmdk-group-heading]]:text-xs [&_[cmdk-group-heading]]:text-subtle">
									<Item icon={Monitor} value="theme system" onSelect={() => run(() => setThemePreference('system'))}>
										Use system theme
									</Item>
									<Item icon={Sun} value="theme light" onSelect={() => run(() => setThemePreference('light'))}>
										Use light theme
									</Item>
									<Item icon={Moon} value="theme dark" onSelect={() => run(() => setThemePreference('dark'))}>
										Use dark theme
									</Item>
									<Item icon={Keyboard} value="keyboard shortcuts help" hint="?" onSelect={() => run(onShowShortcuts)}>
										Keyboard shortcuts
									</Item>
								</Command.Group>
							</Command.List>
						</Command>
					</Dialog.Popup>
				</Dialog.Viewport>
			</Dialog.Portal>
		</Dialog.Root>
	);
}
