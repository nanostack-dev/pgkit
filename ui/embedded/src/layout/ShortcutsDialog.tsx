import { Modal } from '../components/Dialogs';

const groups: { title: string; items: [string[], string][] }[] = [
	{
		title: 'Navigation',
		items: [
			[['⌘', 'K'], 'Open the command menu'],
			[['G', 'O'], 'Go to overview'],
			[['G', 'Q'], 'Go to queues'],
			[['G', 'W'], 'Go to workflows'],
			[['G', 'L'], 'Go to locks'],
		],
	},
	{
		title: 'Lists',
		items: [
			[['/'], 'Focus search'],
			[['J'], 'Next row'],
			[['K'], 'Previous row'],
			[['↵'], 'Open the focused row'],
			[['Esc'], 'Close a panel or clear search'],
		],
	},
];

export function ShortcutsDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
	return (
		<Modal open={open} onOpenChange={onOpenChange} title="Keyboard shortcuts" animated={false}>
			<div className="mt-4 space-y-5">
				{groups.map((group) => (
					<section key={group.title}>
						<h3 className="mb-1.5 text-xs font-medium text-subtle">{group.title}</h3>
						<dl className="divide-y divide-line">
							{group.items.map(([keys, label]) => (
								<div key={label} className="flex items-center justify-between gap-4 py-2">
									<dt className="text-sm text-fg">{label}</dt>
									<dd className="flex shrink-0 gap-1">
										{keys.map((key) => (
											<kbd
												key={key}
												className="inline-flex h-6 min-w-6 items-center justify-center rounded-md border border-line bg-surface-2 px-1.5 font-mono text-xs text-muted"
											>
												{key}
											</kbd>
										))}
									</dd>
								</div>
							))}
						</dl>
					</section>
				))}
			</div>
		</Modal>
	);
}
