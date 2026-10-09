import { Drawer } from '@base-ui/react/drawer';
import { X } from 'lucide-react';
import type { ReactNode } from 'react';
import { cn } from '../lib/cn';
import { useIsDesktop } from '../lib/media';

export function Sheet({
	open,
	onOpenChange,
	title,
	subtitle,
	children,
	footer,
}: {
	open: boolean;
	onOpenChange: (open: boolean) => void;
	title: ReactNode;
	subtitle?: ReactNode;
	children: ReactNode;
	footer?: ReactNode;
}) {
	const desktop = useIsDesktop();
	return (
		<Drawer.Root open={open} onOpenChange={(next) => onOpenChange(next)} swipeDirection={desktop ? 'right' : 'down'}>
			<Drawer.Portal>
				<Drawer.Backdrop className="sheet-backdrop fixed inset-0 z-40 bg-[var(--overlay)]" />
				<Drawer.Viewport
					className={cn('fixed inset-0 z-40 flex', desktop ? 'items-stretch justify-end p-2' : 'items-end justify-center')}
				>
					<Drawer.Popup
						className={cn(
							'sheet flex flex-col overflow-hidden border-line bg-surface text-fg shadow-popover outline-none',
							desktop
								? 'h-full w-[min(34rem,calc(100vw-1rem))] rounded-2xl border'
								: 'max-h-[calc(100dvh-var(--safe-top)-1.5rem)] w-full rounded-t-2xl border-t',
						)}
					>
						{desktop ? null : (
							<div aria-hidden className="flex shrink-0 justify-center pt-2 pb-1">
								<div className="h-1 w-10 rounded-full bg-line-strong" />
							</div>
						)}
						<header className="flex shrink-0 items-start gap-3 border-b border-line px-4 pt-2 pb-3 md:px-5 md:pt-4">
							<div className="min-w-0 flex-1">
								<Drawer.Title className="text-base font-semibold break-words">{title}</Drawer.Title>
								{subtitle ? <Drawer.Description className="mt-0.5 text-[0.8125rem] text-muted">{subtitle}</Drawer.Description> : null}
							</div>
							<Drawer.Close
								aria-label="Close"
								className="press -mr-1.5 inline-flex size-10 shrink-0 items-center justify-center rounded-lg text-muted hover:bg-surface-2 hover:text-fg md:size-8"
							>
								<X aria-hidden className="size-4" />
							</Drawer.Close>
						</header>
						<Drawer.Content className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-4 py-4 md:px-5">{children}</Drawer.Content>
						{footer ? (
							<footer className="shrink-0 border-t border-line bg-surface px-4 pt-3 pb-[calc(0.75rem+var(--safe-bottom))] md:px-5 md:pb-3">
								{footer}
							</footer>
						) : (
							<div className="shrink-0 pb-[var(--safe-bottom)]" />
						)}
					</Drawer.Popup>
				</Drawer.Viewport>
			</Drawer.Portal>
		</Drawer.Root>
	);
}
