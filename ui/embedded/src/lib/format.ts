export function formatDateTime(value: string | null | undefined): string {
	if (!value) {
		return '-';
	}
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) {
		return value;
	}
	return new Intl.DateTimeFormat(undefined, {
		dateStyle: 'medium',
		timeStyle: 'short'
	}).format(date);
}

export function formatRelative(value: string | null | undefined): string {
	if (!value) {
		return '-';
	}
	const date = new Date(value);
	if (Number.isNaN(date.getTime())) {
		return value;
	}
	const diffMs = date.getTime() - Date.now();
	const diffMin = Math.round(diffMs / 60000);
	if (Math.abs(diffMin) < 60) {
		return `${Math.abs(diffMin)}m ${diffMin >= 0 ? 'from now' : 'ago'}`;
	}
	const diffHours = Math.round(diffMin / 60);
	if (Math.abs(diffHours) < 24) {
		return `${Math.abs(diffHours)}h ${diffHours >= 0 ? 'from now' : 'ago'}`;
	}
	const diffDays = Math.round(diffHours / 24);
	return `${Math.abs(diffDays)}d ${diffDays >= 0 ? 'from now' : 'ago'}`;
}

export function prettyJSON(value: string | null | undefined): string {
	if (!value) {
		return '{}';
	}
	try {
		return JSON.stringify(JSON.parse(value), null, 2);
	} catch {
		return value;
	}
}

export function formatCount(value: number): string {
	return new Intl.NumberFormat().format(value);
}
