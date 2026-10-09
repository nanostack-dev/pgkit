export type DataMode = 'live' | 'demo' | 'worst' | 'empty';

export const dataModes: DataMode[] = ['live', 'demo', 'worst', 'empty'];

const storageKey = 'pgkit-dev-data';

function readInitialMode(): DataMode {
	const fromUrl = new URLSearchParams(window.location.search).get('data');
	if (dataModes.includes(fromUrl as DataMode)) {
		try {
			sessionStorage.setItem(storageKey, fromUrl as DataMode);
		} catch {
			return fromUrl as DataMode;
		}
		return fromUrl as DataMode;
	}
	try {
		const stored = sessionStorage.getItem(storageKey);
		return dataModes.includes(stored as DataMode) ? (stored as DataMode) : 'live';
	} catch {
		return 'live';
	}
}

let mode: DataMode | null = null;
const listeners = new Set<() => void>();

export function getDataMode(): DataMode {
	mode ??= readInitialMode();
	return mode;
}

export function setDataMode(next: DataMode) {
	mode = next;
	try {
		sessionStorage.setItem(storageKey, next);
	} catch {
		mode = next;
	}
	const url = new URL(window.location.href);
	if (next === 'live') url.searchParams.delete('data');
	else url.searchParams.set('data', next);
	window.history.replaceState(window.history.state, '', url);
	for (const notify of listeners) notify();
}

export function subscribeDataMode(listener: () => void) {
	listeners.add(listener);
	return () => listeners.delete(listener);
}
