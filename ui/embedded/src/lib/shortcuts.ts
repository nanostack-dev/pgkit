import { useEffect, useRef } from 'react';
import { useNavigate } from 'react-router';

export const navigationShortcuts: Record<string, string> = {
	o: '/',
	q: '/queues',
	w: '/workflows',
	l: '/locks',
};

function isTyping(target: EventTarget | null): boolean {
	if (!(target instanceof HTMLElement)) return false;
	if (target.isContentEditable) return true;
	const tag = target.tagName;
	return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';
}

function moveRowFocus(direction: 1 | -1) {
	const rows = Array.from(document.querySelectorAll<HTMLElement>('[data-row]')).filter(
		(row) => row.offsetParent !== null,
	);
	if (rows.length === 0) return;
	const current = rows.indexOf(document.activeElement as HTMLElement);
	const next = current === -1 ? (direction === 1 ? 0 : rows.length - 1) : Math.min(rows.length - 1, Math.max(0, current + direction));
	const row = rows[next];
	row?.focus();
	row?.scrollIntoView({ block: 'nearest' });
}

type Handlers = {
	onPalette: () => void;
	onHelp: () => void;
};

export function useGlobalShortcuts({ onPalette, onHelp }: Handlers) {
	const navigate = useNavigate();
	const chordAt = useRef(0);
	const handlers = useRef({ onPalette, onHelp });

	useEffect(() => {
		handlers.current = { onPalette, onHelp };
	}, [onPalette, onHelp]);

	useEffect(() => {
		function onKeyDown(event: KeyboardEvent) {
			if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
				event.preventDefault();
				handlers.current.onPalette();
				return;
			}
			if (event.metaKey || event.ctrlKey || event.altKey || event.defaultPrevented || isTyping(event.target)) return;
			if (document.querySelector('[role="dialog"][data-open], [role="alertdialog"][data-open]')) return;

			const key = event.key;
			if (Date.now() - chordAt.current < 1200) {
				chordAt.current = 0;
				const path = navigationShortcuts[key.toLowerCase()];
				if (path) {
					event.preventDefault();
					navigate(path);
				}
				return;
			}
			if (key === 'g') {
				chordAt.current = Date.now();
				return;
			}
			if (key === '/') {
				const search = document.querySelector<HTMLInputElement>('[data-search-input]');
				if (search) {
					event.preventDefault();
					search.focus();
					search.select();
				}
				return;
			}
			if (key === '?') {
				event.preventDefault();
				handlers.current.onHelp();
				return;
			}
			if (key === 'j' || key === 'ArrowDown') {
				if (key === 'ArrowDown' && !(document.activeElement as HTMLElement | null)?.hasAttribute('data-row')) return;
				event.preventDefault();
				moveRowFocus(1);
				return;
			}
			if (key === 'k' || key === 'ArrowUp') {
				if (key === 'ArrowUp' && !(document.activeElement as HTMLElement | null)?.hasAttribute('data-row')) return;
				event.preventDefault();
				moveRowFocus(-1);
			}
		}
		window.addEventListener('keydown', onKeyDown);
		return () => window.removeEventListener('keydown', onKeyDown);
	}, [navigate]);
}
