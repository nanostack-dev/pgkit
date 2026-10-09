import { CircleAlert, RefreshCw, type LucideIcon } from 'lucide-react';
import type { ReactNode } from 'react';
import { errorMessage } from '../api/client';
import { cn } from '../lib/cn';
import { Button } from './Button';

export function EmptyState({
	icon: Icon,
	title,
	children,
	action,
	className,
}: {
	icon: LucideIcon;
	title: string;
	children?: ReactNode;
	action?: ReactNode;
	className?: string;
}) {
	return (
		<div className={cn('flex flex-col items-center px-6 py-12 text-center', className)}>
			<div className="mb-3 flex size-10 items-center justify-center rounded-xl border border-line bg-surface-2 text-subtle">
				<Icon aria-hidden className="size-5" strokeWidth={1.75} />
			</div>
			<p className="text-sm font-medium text-fg">{title}</p>
			{children ? <div className="mt-1 max-w-sm text-[0.8125rem] text-muted text-pretty">{children}</div> : null}
			{action ? <div className="mt-4">{action}</div> : null}
		</div>
	);
}

export function ErrorState({
	error,
	title = 'Could not load this data',
	onRetry,
	className,
}: {
	error: unknown;
	title?: string;
	onRetry?: () => void;
	className?: string;
}) {
	return (
		<div role="alert" className={cn('flex flex-col items-center px-6 py-12 text-center', className)}>
			<div className="mb-3 flex size-10 items-center justify-center rounded-xl bg-bad-soft text-bad">
				<CircleAlert aria-hidden className="size-5" strokeWidth={1.75} />
			</div>
			<p className="text-sm font-medium text-fg">{title}</p>
			<p className="mt-1 max-w-sm text-[0.8125rem] text-muted text-pretty">{errorMessage(error)}</p>
			{onRetry ? (
				<Button className="mt-4" size="sm" icon={RefreshCw} onClick={onRetry}>
					Try again
				</Button>
			) : null}
		</div>
	);
}

export function Skeleton({ className }: { className?: string }) {
	return <div aria-hidden className={cn('skeleton', className)} />;
}

export function ListSkeleton({ rows = 6, label = 'Loading' }: { rows?: number; label?: string }) {
	return (
		<div role="status" aria-label={label} className="divide-y divide-line">
			{Array.from({ length: rows }, (_, index) => (
				<div key={index} className="flex items-center gap-3 px-4 py-3.5">
					<Skeleton className="size-2 rounded-full" />
					<div className="flex min-w-0 flex-1 flex-col gap-2">
						<Skeleton className="h-3.5 w-2/5" />
						<Skeleton className="h-3 w-3/5" />
					</div>
					<Skeleton className="hidden h-3 w-16 sm:block" />
				</div>
			))}
		</div>
	);
}
