import { cn } from '../lib/cn';

export function MiddleTruncate({ text, tail = 10, className }: { text: string; tail?: number; className?: string }) {
	const characters = Array.from(text);
	if (characters.length <= tail * 2) {
		return (
			<span className={cn('truncate', className)} title={text}>
				{text}
			</span>
		);
	}
	const head = characters.slice(0, -tail).join('');
	const end = characters.slice(-tail).join('');
	return (
		<span className={cn('flex min-w-0', className)} title={text}>
			<span className="truncate">{head}</span>
			<span className="shrink-0 whitespace-pre">{end}</span>
		</span>
	);
}
