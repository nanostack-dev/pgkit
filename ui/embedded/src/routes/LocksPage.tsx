import { Lock } from 'lucide-react';
import { useLocks } from '../api/queries';
import type { AdvisoryLock } from '../api/types';
import { CopyButton } from '../components/Copy';
import { LiveIndicator, PageHeader, Section } from '../components/Layout';
import { EmptyState, ErrorState, ListSkeleton } from '../components/States';
import { Elapsed } from '../components/Time';
import { cn } from '../lib/cn';
import { formatDateTime, plural } from '../lib/format';

function lockKey(lock: AdvisoryLock): string {
	return lock.key ?? `${lock.classid},${lock.objid}`;
}

function LockRow({ lock }: { lock: AdvisoryLock }) {
	const key = lockKey(lock);
	const mode = lock.mode.replace(/Lock$/, '').replace(/([a-z])([A-Z])/g, '$1 $2').toLowerCase();
	return (
		<li className="@container">
			<div className="flex flex-col gap-1.5 px-4 py-3 text-[0.8125rem] @3xl:grid @3xl:grid-cols-[minmax(0,1.4fr)_6.5rem_minmax(0,1fr)_minmax(0,1fr)_7rem] @3xl:items-center @3xl:gap-4 @3xl:py-2.5">
				<span className="flex min-w-0 items-center gap-1">
					<code className="truncate font-mono font-medium" title={key}>
						{key}
					</code>
					<CopyButton value={key} label="Copy lock key" />
				</span>
				<span
					className={cn(
						'inline-flex w-fit items-center gap-1.5 rounded-full px-2 py-0.5 text-[0.6875rem] font-medium whitespace-nowrap',
						lock.granted ? 'bg-ok-soft text-ok' : 'bg-warn-soft text-warn',
					)}
				>
					<span aria-hidden className={cn('size-1.5 rounded-full', lock.granted ? 'bg-ok' : 'bg-warn live-dot')} />
					{lock.granted ? 'Held' : 'Waiting'}
				</span>
				<span className="flex min-w-0 flex-wrap items-baseline gap-x-2 text-muted">
					<span className="tabular font-mono text-xs text-fg">pid {lock.pid}</span>
					<span className="truncate" title={lock.application_name || undefined}>
						{lock.application_name || 'unnamed client'}
					</span>
				</span>
				<span className="flex min-w-0 flex-wrap gap-x-2 text-xs text-muted">
					<span>{mode}</span>
					{lock.state ? <span>· {lock.state}</span> : null}
					{lock.wait_event ? (
						<span className="truncate">
							· {lock.wait_event_type}: {lock.wait_event}
						</span>
					) : null}
				</span>
				<span className="text-xs text-muted @3xl:text-right" title={lock.xact_start ? `Transaction started ${formatDateTime(lock.xact_start)}` : undefined}>
					{lock.xact_start ? (
						<>
							{lock.granted ? 'held ' : 'waiting '}
							<Elapsed from={lock.xact_start} to={null} className="text-fg" />
						</>
					) : (
						'session lock'
					)}
				</span>
			</div>
		</li>
	);
}

export function LocksPage() {
	const locks = useLocks();
	const items = locks.data?.items ?? [];
	const waiting = items.filter((lock) => !lock.granted).length;

	return (
		<>
			<PageHeader
				title="Advisory locks"
				description="PostgreSQL advisory locks in this database, including pglock keys and waiting sessions."
				actions={<LiveIndicator updatedAt={locks.dataUpdatedAt} error={locks.isError && Boolean(locks.data)} />}
			/>
			<Section
				title={locks.data ? plural(items.length, 'lock', 'locks') : 'Locks'}
				description={waiting > 0 ? `${plural(waiting, 'session', 'sessions')} waiting` : 'Held and waiting sessions'}
				flush
			>
				{locks.isPending ? (
					<ListSkeleton rows={3} label="Loading locks" />
				) : locks.isError && !locks.data ? (
					<ErrorState error={locks.error} onRetry={() => locks.refetch()} />
				) : items.length === 0 ? (
					<EmptyState icon={Lock} title="No advisory locks right now">
						Locks taken with pglock or pg_advisory_lock appear here while a session holds or waits for them.
					</EmptyState>
				) : (
					<ul className="divide-y divide-line">
						{items.map((lock, index) => (
							<LockRow key={`${lock.pid}-${lockKey(lock)}-${lock.mode}-${index}`} lock={lock} />
						))}
					</ul>
				)}
			</Section>
			<p className="mt-3 px-1 text-xs text-muted text-pretty">
				Keys are 64-bit integers; pglock derives them by hashing the lock name, so the original name cannot be recovered here.
				Two-part keys show as <code className="font-mono">classid,objid</code>.
			</p>
		</>
	);
}
