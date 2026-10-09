import { cn } from '../lib/cn';
import { isActive, statusLabel, statusTone, toneDot, toneSoft, type AnyStatus } from '../lib/status';

export function StatusDot({ status, className }: { status: AnyStatus; className?: string }) {
	const tone = statusTone(status);
	return (
		<span
			aria-hidden
			className={cn('inline-block size-2 shrink-0 rounded-full', toneDot[tone], isActive(status) && 'live-dot', className)}
		/>
	);
}

export function StatusBadge({ status, size = 'md', className }: { status: AnyStatus; size?: 'sm' | 'md'; className?: string }) {
	const tone = statusTone(status);
	return (
		<span
			className={cn(
				'status-pill inline-flex shrink-0 items-center gap-1.5 rounded-full font-medium whitespace-nowrap',
				size === 'sm' ? 'h-5 px-2 text-[0.6875rem]' : 'h-6 px-2.5 text-xs',
				toneSoft[tone],
				className,
			)}
		>
			<StatusDot status={status} className={size === 'sm' ? 'size-1.5' : undefined} />
			{statusLabel(status)}
		</span>
	);
}
