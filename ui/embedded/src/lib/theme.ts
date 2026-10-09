import { useSyncExternalStore } from 'react';

export type ThemePreference = 'system' | 'light' | 'dark';

const storageKey = 'pgkit-theme';
const darkQuery = window.matchMedia('(prefers-color-scheme: dark)');
const listeners = new Set<() => void>();

function readPreference(): ThemePreference {
	try {
		const stored = localStorage.getItem(storageKey);
		return stored === 'light' || stored === 'dark' ? stored : 'system';
	} catch {
		return 'system';
	}
}

let preference = readPreference();

function resolvedTheme(): 'light' | 'dark' {
	if (preference === 'system') return darkQuery.matches ? 'dark' : 'light';
	return preference;
}

function applyTheme() {
	const root = document.documentElement;
	root.setAttribute('data-disable-transitions', '');
	root.dataset.theme = resolvedTheme();
	requestAnimationFrame(() => requestAnimationFrame(() => root.removeAttribute('data-disable-transitions')));
}

darkQuery.addEventListener('change', () => {
	if (preference !== 'system') return;
	applyTheme();
	for (const notify of listeners) notify();
});

export function setThemePreference(next: ThemePreference) {
	preference = next;
	try {
		if (next === 'system') localStorage.removeItem(storageKey);
		else localStorage.setItem(storageKey, next);
	} catch {
		preference = next;
	}
	applyTheme();
	for (const notify of listeners) notify();
}

function subscribe(listener: () => void) {
	listeners.add(listener);
	return () => listeners.delete(listener);
}

export function useThemePreference(): ThemePreference {
	return useSyncExternalStore(subscribe, () => preference);
}
