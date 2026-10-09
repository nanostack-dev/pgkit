import { syncServerClock } from '../lib/clock';

export class ApiError extends Error {
	readonly status: number;

	constructor(status: number, message: string) {
		super(message);
		this.name = 'ApiError';
		this.status = status;
	}
}

export function errorMessage(error: unknown): string {
	if (error instanceof Error) return error.message;
	return 'Something went wrong.';
}

type QueryValue = string | number | boolean | undefined | null;

export function withQuery(path: string, values: Record<string, QueryValue>): string {
	const params = new URLSearchParams();
	for (const [key, value] of Object.entries(values)) {
		if (value === undefined || value === null || value === '' || value === false) continue;
		params.set(key, String(value));
	}
	const encoded = params.toString();
	return encoded ? `${path}?${encoded}` : path;
}

const messagesByStatus: Record<number, string> = {
	401: 'Your session is not authorized. Reload the page and sign in again.',
	403: 'The server refused this request.',
	404: 'Not found.',
	405: 'This action is disabled on this server.',
	409: 'The item changed state; refresh and try again.',
	500: 'The server could not complete the request.',
	502: 'The admin server is unreachable.',
	503: 'The admin server is unavailable.',
};

async function readError(response: Response): Promise<ApiError> {
	let message = messagesByStatus[response.status] ?? `Request failed (${response.status}).`;
	if (response.status < 500) {
		try {
			const body = (await response.json()) as { error?: unknown };
			if (typeof body.error === 'string' && body.error.trim()) message = body.error;
		} catch {
			message = messagesByStatus[response.status] ?? message;
		}
	}
	return new ApiError(response.status, message);
}

async function send<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
	if (import.meta.env.DEV) {
		const { mockResponse } = await import('../dev/mock');
		const mocked = await mockResponse(method, path, body);
		if (mocked !== undefined) return mocked as T;
	}
	const headers: Record<string, string> = { accept: 'application/json' };
	if (method !== 'GET') headers['x-requested-with'] = 'pgkit-admin-ui';
	if (body !== undefined) headers['content-type'] = 'application/json';
	let response: Response;
	try {
		response = await fetch(path, {
			method,
			headers,
			credentials: 'same-origin',
			body: body === undefined ? undefined : JSON.stringify(body),
			signal,
		});
	} catch (error) {
		if (error instanceof DOMException && error.name === 'AbortError') throw error;
		throw new ApiError(0, 'Network error: the admin server did not respond.');
	}
	if (!response.ok) throw await readError(response);
	if (response.status === 204) return undefined as T;
	const data = (await response.json()) as T;
	if (data && typeof data === 'object' && 'now' in data && typeof data.now === 'string') syncServerClock(data.now);
	return data;
}

export function get<T>(path: string, signal?: AbortSignal): Promise<T> {
	return send<T>('GET', path, undefined, signal);
}

export function post<T>(path: string, body?: unknown): Promise<T> {
	return send<T>('POST', path, body);
}

export function del<T>(path: string): Promise<T> {
	return send<T>('DELETE', path);
}
