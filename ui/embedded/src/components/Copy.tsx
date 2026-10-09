import { Check, Copy } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { cn } from '../lib/cn';
import { shortId } from '../lib/format';

async function writeClipboard(text: string): Promise<boolean> {
	try {
		await navigator.clipboard.writeText(text);
		return true;
	} catch {
		const area = document.createElement('textarea');
		area.value = text;
		area.setAttribute('readonly', '');
		area.style.position = 'fixed';
		area.style.opacity = '0';
		document.body.appendChild(area);
		area.select();
		const copied = document.execCommand('copy');
		area.remove();
		return copied;
	}
}

export function CopyButton({ value, label, className }: { value: string; label: string; className?: string }) {
	const [copied, setCopied] = useState(false);
	const timer = useRef<number | undefined>(undefined);

	useEffect(() => () => window.clearTimeout(timer.current), []);

	async function copy(event: React.MouseEvent) {
		event.preventDefault();
		event.stopPropagation();
		if (await writeClipboard(value)) {
			setCopied(true);
			window.clearTimeout(timer.current);
			timer.current = window.setTimeout(() => setCopied(false), 1500);
		} else {
			toast.error('Could not copy to the clipboard.');
		}
	}

	const Icon = copied ? Check : Copy;
	return (
		<button
			type="button"
			onClick={copy}
			aria-label={copied ? 'Copied' : label}
			title={label}
			className={cn(
				'press relative inline-flex size-8 shrink-0 items-center justify-center rounded-md text-subtle transition-[color,background-color,transform] hover:bg-surface-2 hover:text-fg md:size-6',
				'before:absolute before:-inset-1.5 before:content-[""] md:before:-inset-1',
				copied && 'text-ok hover:text-ok',
				className,
			)}
		>
			<Icon aria-hidden className="size-3.5" strokeWidth={2.25} />
			<span className="sr-only" aria-live="polite">
				{copied ? 'Copied' : ''}
			</span>
		</button>
	);
}

export function IdText({ id, className, full = false }: { id: string; className?: string; full?: boolean }) {
	return (
		<span className={cn('inline-flex min-w-0 items-center gap-0.5', className)}>
			<code title={id} className={cn('min-w-0 font-mono text-[0.8125rem] text-muted', full ? 'break-all' : 'truncate')}>
				{full ? id : shortId(id)}
			</code>
			<CopyButton value={id} label="Copy ID" />
		</span>
	);
}
