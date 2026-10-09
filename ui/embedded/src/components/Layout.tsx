import { Link } from 'react-router';
import type { ReactNode } from 'react';
import { ChevronRight } from 'lucide-react';
import { useClientNow } from '../lib/clock';
import { cn } from '../lib/cn';
import { formatRelative } from '../lib/format';

export function PageHeader({
	title,
	description,
	actions,
	children,
}: {
	title: ReactNode;
	description?: ReactNode;
	actions?: ReactNode;
	children?: ReactNode;
}) {
	return (
		<header className="mb-5 md:mb-6">
			<div className="flex flex-wrap items-end justify-between gap-x-4 gap-y-3">
				<div className="min-w-0">
					<h1 className="text-xl font-semibold tracking-[-0.01em] text-balance md:text-2xl">{title}</h1>
					{description ? <p className="mt-1 text-[0.8125rem] text-muted text-pretty md:text-sm">{description}</p> : null}
				</div>
				{actions ? <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div> : null}
			</div>
			{children}
		</header>
	);
}

export function Section({
	title,
	description,
	action,
	children,
	className,
	flush = false,
	id,
}: {
	title: ReactNode;
	description?: ReactNode;
	action?: ReactNode;
	children: ReactNode;
	className?: string;
	flush?: boolean;
	id?: string;
}) {
	const headingId = id ? `${id}-title` : undefined;
	return (
		<section aria-labelledby={headingId} className={cn('card min-w-0 overflow-hidden', className)}>
			<div className="flex min-h-12 items-center justify-between gap-3 border-b border-line px-4 py-2.5">
				<div className="min-w-0">
					<h2 id={headingId} className="truncate text-sm font-semibold">
						{title}
					</h2>
					{description ? <p className="truncate text-xs text-muted">{description}</p> : null}
				</div>
				{action ? <div className="shrink-0">{action}</div> : null}
			</div>
			<div className={flush ? undefined : 'p-4'}>{children}</div>
		</section>
	);
}

export function SectionLink({ to, children }: { to: string; children: ReactNode }) {
	return (
		<Link
			to={to}
			className="press -mr-2 inline-flex min-h-10 items-center gap-0.5 rounded-md px-2 text-[0.8125rem] font-medium text-muted transition-colors hover:text-fg md:min-h-8"
		>
			{children}
			<ChevronRight aria-hidden className="size-3.5" />
		</Link>
	);
}

export function LiveIndicator({
	updatedAt,
	error,
	paused = false,
}: {
	updatedAt: number;
	error: boolean;
	paused?: boolean;
}) {
	const now = useClientNow();
	const stale = updatedAt > 0 && now - updatedAt > 30_000;
	const label = error ? 'Reconnecting' : paused ? 'Paused' : stale ? 'Stale' : 'Live';
	const tone = error ? 'bg-bad' : paused || stale ? 'bg-idle' : 'bg-ok';
	return (
		<span
			className="inline-flex items-center gap-1.5 rounded-full border border-line bg-surface px-2.5 py-1 text-xs text-muted"
			title={updatedAt ? `Updated ${formatRelative(new Date(updatedAt).toISOString(), now)}` : undefined}
			aria-live="polite"
		>
			<span aria-hidden className={cn('size-1.5 rounded-full', tone, label === 'Live' && 'live-dot')} />
			<span>{label}</span>
		</span>
	);
}

export function Fact({ label, children, className }: { label: string; children: ReactNode; className?: string }) {
	return (
		<div className={cn('min-w-0', className)}>
			<dt className="text-xs text-subtle">{label}</dt>
			<dd className="mt-0.5 min-w-0 text-[0.8125rem] text-fg">{children}</dd>
		</div>
	);
}
