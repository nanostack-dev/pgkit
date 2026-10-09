const numberFormat = new Intl.NumberFormat();
const compactFormat = new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 });
const pluralRules = new Intl.PluralRules();
const dateTimeFormat = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
const timeFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
const timeWithSecondsFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });

export function formatNumber(value: number): string {
	return numberFormat.format(value);
}

export function formatCompact(value: number): string {
	return Math.abs(value) < 10_000 ? numberFormat.format(value) : compactFormat.format(value);
}

export function plural(count: number, one: string, other: string): string {
	return `${formatNumber(count)} ${pluralRules.select(count) === 'one' ? one : other}`;
}

export function isParked(value: string | null | undefined): boolean {
	return Boolean(value && (value === 'infinity' || value.startsWith('9999-')));
}

export function parseTime(value: string | null | undefined): number | null {
	if (!value) return null;
	const parsed = Date.parse(value);
	return Number.isNaN(parsed) ? null : parsed;
}

export function formatDateTime(value: string | null | undefined): string {
	const time = parseTime(value);
	return time === null ? '—' : dateTimeFormat.format(time);
}

export function formatClock(value: string | number, withSeconds = false): string {
	const time = typeof value === 'number' ? value : parseTime(value);
	if (time === null) return '—';
	return (withSeconds ? timeWithSecondsFormat : timeFormat).format(time);
}

export function formatDuration(ms: number): string {
	const abs = Math.abs(ms);
	if (abs < 1) return '<1ms';
	if (abs < 1_000) return `${Math.round(abs)}ms`;
	if (abs < 10_000) return `${(abs / 1_000).toFixed(1).replace(/\.0$/, '')}s`;
	const seconds = Math.round(abs / 1_000);
	if (seconds < 60) return `${seconds}s`;
	const minutes = Math.floor(seconds / 60);
	if (minutes < 60) return seconds % 60 ? `${minutes}m ${seconds % 60}s` : `${minutes}m`;
	const hours = Math.floor(minutes / 60);
	if (hours < 48) return minutes % 60 ? `${hours}h ${minutes % 60}m` : `${hours}h`;
	const days = Math.floor(hours / 24);
	return hours % 24 ? `${days}d ${hours % 24}h` : `${days}d`;
}

function compactSpan(ms: number): string {
	const seconds = Math.round(Math.abs(ms) / 1_000);
	if (seconds < 60) return `${seconds}s`;
	const minutes = Math.round(seconds / 60);
	if (minutes < 60) return `${minutes}m`;
	const hours = Math.round(minutes / 60);
	if (hours < 48) return `${hours}h`;
	const days = Math.round(hours / 24);
	if (days < 60) return `${days}d`;
	if (days < 730) return `${Math.round(days / 30)}mo`;
	return `${Math.round(days / 365)}y`;
}

export function formatRelative(value: string | null | undefined, now: number): string {
	const time = parseTime(value);
	if (time === null) return '—';
	const diff = time - now;
	if (Math.abs(diff) < 5_000) return diff >= 0 ? 'in a moment' : 'just now';
	return diff > 0 ? `in ${compactSpan(diff)}` : `${compactSpan(diff)} ago`;
}

export function formatCountdown(value: string | null | undefined, now: number): string {
	const time = parseTime(value);
	if (time === null) return '—';
	const diff = time - now;
	if (diff <= 0) return 'due now';
	return formatDuration(Math.ceil(diff / 1_000) * 1_000);
}

export function jobLabel(id: number): string {
	return `#${id}`;
}

export function untilPhrase(value: string | null | undefined, now: number, prefix: string, overdue: string): string | null {
	const time = parseTime(value);
	if (time === null) return null;
	return time - now > 0 ? `${prefix} ${formatCountdown(value, now)}` : overdue;
}

export function formatBytes(bytes: number): string {
	if (bytes < 1024) return plural(bytes, 'byte', 'bytes');
	const units = ['KB', 'MB', 'GB'];
	let value = bytes / 1024;
	let unit = 0;
	while (value >= 1024 && unit < units.length - 1) {
		value /= 1024;
		unit += 1;
	}
	return `${value.toFixed(value < 10 ? 1 : 0)} ${units[unit]}`;
}

export function shortId(id: string): string {
	return id.length <= 12 ? id : `…${id.slice(-8)}`;
}

export function splitStepName(name: string): { base: string; repeat: number | null } {
	const marker = name.lastIndexOf('#');
	if (marker <= 0) return { base: name, repeat: null };
	const repeat = Number(name.slice(marker + 1));
	return Number.isInteger(repeat) && repeat > 1 ? { base: name.slice(0, marker), repeat } : { base: name, repeat: null };
}

export function firstLine(text: string, max = 160): string {
	const line = text.split('\n', 1)[0] ?? '';
	return line.length > max ? `${line.slice(0, max - 1)}…` : line;
}
