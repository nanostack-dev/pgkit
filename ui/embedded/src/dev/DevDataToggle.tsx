import { useQueryClient } from '@tanstack/react-query';
import { useSyncExternalStore } from 'react';
import { dataModes, getDataMode, setDataMode, subscribeDataMode, type DataMode } from './mode';

const labels: Record<DataMode, string> = {
	live: 'Live API',
	demo: 'Demo data',
	worst: 'Worst case',
	empty: 'Empty',
};

export function DevDataToggle() {
	const client = useQueryClient();
	const mode = useSyncExternalStore(subscribeDataMode, getDataMode);
	return (
		<div
			role="radiogroup"
			aria-label="Development data source"
			style={{
				position: 'fixed',
				left: '50%',
				bottom: 'calc(var(--tabbar-height) + var(--safe-bottom) + 10px)',
				transform: 'translateX(-50%)',
				zIndex: 60,
				display: 'flex',
				gap: 2,
				padding: 2,
				borderRadius: 999,
				background: '#e5e5e5',
				font: '500 11px/1 system-ui, sans-serif',
				boxShadow: '0 1px 3px rgb(0 0 0 / 0.2)',
			}}
		>
			{dataModes.map((value) => (
				<button
					key={value}
					type="button"
					role="radio"
					aria-checked={mode === value}
					onClick={() => {
						setDataMode(value);
						client.resetQueries();
					}}
					style={{
						border: 0,
						borderRadius: 999,
						padding: '6px 10px',
						cursor: 'pointer',
						background: mode === value ? '#fff' : 'transparent',
						color: '#171717',
					}}
				>
					{labels[value]}
				</button>
			))}
		</div>
	);
}
