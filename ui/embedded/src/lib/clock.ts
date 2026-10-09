import { useSyncExternalStore } from 'react';

let serverOffsetMs = 0;

export function syncServerClock(serverNow: string) {
	const parsed = Date.parse(serverNow);
	if (Number.isNaN(parsed)) return;
	serverOffsetMs = parsed - Date.now();
}

export function serverNow(): number {
	return Date.now() + serverOffsetMs;
}

const listeners = new Set<() => void>();
let clientTick = Date.now();
let tick = serverNow();
let timer: number | undefined;

function subscribe(listener: () => void) {
	listeners.add(listener);
	if (timer === undefined) {
		clientTick = Date.now();
		tick = serverNow();
		timer = window.setInterval(() => {
			clientTick = Date.now();
			tick = serverNow();
			for (const notify of listeners) notify();
		}, 1000);
	}
	return () => {
		listeners.delete(listener);
		if (listeners.size === 0 && timer !== undefined) {
			window.clearInterval(timer);
			timer = undefined;
		}
	};
}

function snapshot() {
	return tick;
}

export function useNow(): number {
	return useSyncExternalStore(subscribe, snapshot, snapshot);
}

function clientSnapshot() {
	return clientTick;
}

export function useClientNow(): number {
	return useSyncExternalStore(subscribe, clientSnapshot, clientSnapshot);
}
