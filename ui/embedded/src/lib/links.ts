export function jobHref(id: number, search: URLSearchParams | string = ''): string {
	const params = new URLSearchParams(search);
	params.set('job', String(id));
	return `/queues?${params.toString()}`;
}

export function runHref(id: string): string {
	return `/workflows/${encodeURIComponent(id)}`;
}
