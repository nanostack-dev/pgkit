import { WrapText } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';
import type { PayloadEncoding } from '../api/types';
import { cn } from '../lib/cn';
import { formatBytes, formatNumber } from '../lib/format';
import { CopyButton } from './Copy';

const collapsedLines = 14;
const highlightLimit = 150_000;
const tokenPattern = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;

function highlight(source: string): ReactNode[] {
	const nodes: ReactNode[] = [];
	let cursor = 0;
	for (const match of source.matchAll(tokenPattern)) {
		const index = match.index ?? 0;
		if (index > cursor) nodes.push(source.slice(cursor, index));
		const [whole, string, colon, literal, number] = match;
		if (string !== undefined) {
			nodes.push(
				<span key={index} className={colon ? 'json-key' : 'json-string'}>
					{string}
				</span>,
			);
			if (colon) nodes.push(colon);
		} else if (literal !== undefined) {
			nodes.push(
				<span key={index} className="json-literal">
					{literal}
				</span>,
			);
		} else if (number !== undefined) {
			nodes.push(
				<span key={index} className="json-number">
					{number}
				</span>,
			);
		} else {
			nodes.push(whole);
		}
		cursor = index + whole.length;
	}
	if (cursor < source.length) nodes.push(source.slice(cursor));
	return nodes;
}

function prettify(value: string, encoding: PayloadEncoding): { text: string; isJson: boolean } {
	if (encoding !== 'json') return { text: value, isJson: false };
	try {
		return { text: JSON.stringify(JSON.parse(value), null, 2), isJson: true };
	} catch {
		return { text: value, isJson: false };
	}
}

export function JsonView({
	value,
	label,
	encoding = 'json',
	bytes,
	truncated = false,
	emptyLabel = 'No value',
	className,
}: {
	value: string | null | undefined;
	label: string;
	encoding?: PayloadEncoding;
	bytes?: number;
	truncated?: boolean;
	emptyLabel?: string;
	className?: string;
}) {
	const [expanded, setExpanded] = useState(false);
	const [wrap, setWrap] = useState(true);
	const pretty = useMemo(() => prettify(value ?? '', encoding), [value, encoding]);
	const lines = useMemo(() => pretty.text.split('\n'), [pretty.text]);
	const collapsible = lines.length > collapsedLines + 2;
	const visible = collapsible && !expanded ? lines.slice(0, collapsedLines).join('\n') : pretty.text;
	const content = useMemo(
		() => (pretty.isJson && visible.length <= highlightLimit ? highlight(visible) : visible),
		[pretty.isJson, visible],
	);
	const size = bytes ?? new TextEncoder().encode(value ?? '').length;

	if (value === null || value === undefined || value === '') {
		return (
			<div className={cn('rounded-lg border border-dashed border-line px-3 py-4 text-center text-[0.8125rem] text-subtle', className)}>
				{emptyLabel}
			</div>
		);
	}

	return (
		<div className={cn('min-w-0 overflow-hidden rounded-lg border border-line bg-surface-2', className)}>
			<div className="flex items-center gap-2 border-b border-line py-1 pr-1.5 pl-3">
				<span className="min-w-0 flex-1 truncate text-xs text-muted">
					<span className="font-medium text-fg">{label}</span>
					<span className="tabular">
						{' · '}
						{formatBytes(size)}
						{lines.length > 1 ? ` · ${formatNumber(lines.length)} lines` : ''}
					</span>
					{encoding === 'base64' ? ' · base64' : ''}
					{truncated ? ' · truncated' : ''}
				</span>
				<button
					type="button"
					aria-pressed={wrap}
					onClick={() => setWrap((current) => !current)}
					title={wrap ? 'Disable line wrapping' : 'Wrap long lines'}
					aria-label="Wrap long lines"
					className={cn(
						'press inline-flex size-8 items-center justify-center rounded-md transition-[color,background-color,transform] hover:bg-surface-3 md:size-6',
						wrap ? 'text-fg' : 'text-subtle',
					)}
				>
					<WrapText aria-hidden className="size-3.5" strokeWidth={2.25} />
				</button>
				<CopyButton value={pretty.text} label={`Copy ${label.toLowerCase()}`} />
			</div>
			<div className="relative">
				<pre
					tabIndex={0}
					aria-label={label}
					className={cn(
						'overflow-auto overscroll-contain px-3 py-2.5 font-mono text-[0.75rem] leading-[1.6] text-fg',
						expanded ? 'max-h-[70dvh]' : 'max-h-[24rem]',
						wrap ? 'break-words whitespace-pre-wrap [overflow-wrap:anywhere]' : 'whitespace-pre',
					)}
				>
					{content}
				</pre>
				{collapsible && !expanded ? (
					<div className="pointer-events-none absolute inset-x-0 bottom-0 h-12 bg-gradient-to-t from-surface-2 to-transparent" />
				) : null}
			</div>
			{truncated ? (
				<p className="border-t border-line px-3 py-2 text-xs text-muted">
					Showing the first 1 MB. The stored value is {formatBytes(size)}.
				</p>
			) : null}
			{collapsible ? (
				<button
					type="button"
					onClick={() => setExpanded((current) => !current)}
					aria-expanded={expanded}
					className="flex min-h-10 w-full items-center justify-center border-t border-line text-xs font-medium text-muted transition-colors hover:bg-surface-3 hover:text-fg md:min-h-8"
				>
					{expanded ? 'Show less' : `Show all ${formatNumber(lines.length)} lines`}
				</button>
			) : null}
		</div>
	);
}
