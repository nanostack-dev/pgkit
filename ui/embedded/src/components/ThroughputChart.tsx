import { useState } from 'react';
import { cn } from '../lib/cn';
import { formatClock, formatNumber } from '../lib/format';

export type SeriesDef<B> = {
	key: keyof B & string;
	label: string;
	barClass: string;
	dotClass: string;
};

export function ThroughputChart<B extends { start: string }>({
	title,
	buckets,
	series,
	rangeLabel,
}: {
	title: string;
	buckets: B[];
	series: SeriesDef<B>[];
	rangeLabel: string;
}) {
	const [active, setActive] = useState<number | null>(null);
	const value = (bucket: B, key: keyof B) => Number(bucket[key] ?? 0);
	const totals = series.map((item) => buckets.reduce((sum, bucket) => sum + value(bucket, item.key), 0));
	const max = Math.max(1, ...buckets.map((bucket) => series.reduce((sum, item) => sum + value(bucket, item.key), 0)));
	const summary = series.map((item, index) => `${formatNumber(totals[index] ?? 0)} ${item.label.toLowerCase()}`).join(', ');
	const activeBucket = active === null ? undefined : buckets[active];

	return (
		<figure className="min-w-0">
			<figcaption className="flex min-h-10 flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
				<span className="text-[0.8125rem] font-medium">{title}</span>
				<span className="tabular flex flex-wrap items-center gap-x-3 text-xs text-muted" aria-live="off">
					{activeBucket ? <span className="text-fg">{formatClock(activeBucket.start)}</span> : <span>{rangeLabel}</span>}
					{series.map((item, index) => (
						<span key={item.key} className="inline-flex items-center gap-1.5">
							<span aria-hidden className={cn('size-2 rounded-[2px]', item.dotClass)} />
							{formatNumber(activeBucket ? value(activeBucket, item.key) : (totals[index] ?? 0))} {item.label.toLowerCase()}
						</span>
					))}
				</span>
			</figcaption>
			<div
				role="img"
				aria-label={`${title}, ${rangeLabel.toLowerCase()}: ${summary}`}
				className="mt-2 flex h-24 items-end gap-px md:h-28"
				onPointerLeave={() => setActive(null)}
			>
				{buckets.map((bucket, index) => {
					const total = series.reduce((sum, item) => sum + value(bucket, item.key), 0);
					return (
						<div
							key={bucket.start}
							className="group flex h-full min-w-0 flex-1 flex-col justify-end"
							onPointerEnter={() => setActive(index)}
							onPointerDown={() => setActive(index)}
						>
							<div
								className={cn(
									'flex w-full flex-col-reverse overflow-hidden rounded-[2px]',
									active === index ? 'opacity-100' : active === null ? 'opacity-100' : 'opacity-60',
								)}
								style={{ height: total === 0 ? '2px' : `${Math.max(4, (total / max) * 100)}%` }}
							>
								{total === 0 ? (
									<div className="h-full w-full bg-line" />
								) : (
									series.map((item) => {
										const part = value(bucket, item.key);
										return part > 0 ? (
											<div key={item.key} className={item.barClass} style={{ height: `${(part / total) * 100}%` }} />
										) : null;
									})
								)}
							</div>
						</div>
					);
				})}
			</div>
			<div aria-hidden className="tabular mt-1.5 flex justify-between text-[0.6875rem] text-subtle">
				<span>{buckets[0] ? formatClock(buckets[0].start) : ''}</span>
				<span>now</span>
			</div>
		</figure>
	);
}
