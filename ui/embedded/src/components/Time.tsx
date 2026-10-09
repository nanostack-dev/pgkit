import { useNow } from '../lib/clock';
import { cn } from '../lib/cn';
import { formatCountdown, formatDateTime, formatDuration, formatRelative, parseTime } from '../lib/format';

export function RelativeTime({ value, className }: { value: string | null | undefined; className?: string }) {
	const now = useNow();
	if (!value) return <span className={cn('text-subtle', className)}>—</span>;
	return (
		<time dateTime={value} title={formatDateTime(value)} className={cn('tabular whitespace-nowrap', className)}>
			{formatRelative(value, now)}
		</time>
	);
}

export function Countdown({
	value,
	className,
	prefix,
	overdue = 'now',
}: {
	value: string | null | undefined;
	className?: string;
	prefix?: string;
	overdue?: string;
}) {
	const now = useNow();
	const time = parseTime(value);
	if (!value || time === null) return null;
	if (time <= now) return <span className={cn('tabular whitespace-nowrap', className)}>{overdue}</span>;
	return (
		<span className={cn('tabular whitespace-nowrap', className)}>
			{prefix ? `${prefix} ` : null}
			<time dateTime={value} title={formatDateTime(value)}>
				{formatCountdown(value, now)}
			</time>
		</span>
	);
}

export function Elapsed({
	from,
	to,
	className,
}: {
	from: string | null | undefined;
	to: string | null | undefined;
	className?: string;
}) {
	const now = useNow();
	const start = parseTime(from);
	if (start === null) return <span className={cn('text-subtle', className)}>—</span>;
	const end = parseTime(to) ?? now;
	return <span className={cn('tabular whitespace-nowrap', className)}>{formatDuration(Math.max(0, end - start))}</span>;
}
