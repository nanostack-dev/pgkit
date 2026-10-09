import { AlertDialog } from '@base-ui/react/alert-dialog';
import { Dialog } from '@base-ui/react/dialog';
import { X } from 'lucide-react';
import type { ReactNode } from 'react';
import { cn } from '../lib/cn';
import { Button } from './Button';

const viewportClass = 'fixed inset-0 z-50 flex items-end justify-center p-3 pb-[calc(0.75rem+var(--safe-bottom))] sm:items-center sm:p-6';
const popupClass =
	'w-full max-w-md rounded-2xl border border-line bg-surface p-5 text-fg shadow-popover outline-none sm:p-6';

export function ConfirmDialog({
	open,
	onOpenChange,
	title,
	description,
	confirmLabel,
	tone = 'default',
	pending = false,
	onConfirm,
	children,
}: {
	open: boolean;
	onOpenChange: (open: boolean) => void;
	title: string;
	description: ReactNode;
	confirmLabel: string;
	tone?: 'default' | 'danger';
	pending?: boolean;
	onConfirm: () => void;
	children?: ReactNode;
}) {
	return (
		<AlertDialog.Root open={open} onOpenChange={(next) => onOpenChange(next)}>
			<AlertDialog.Portal>
				<AlertDialog.Backdrop className="backdrop fixed inset-0 z-50 bg-[var(--overlay)]" />
				<AlertDialog.Viewport className={viewportClass}>
					<AlertDialog.Popup className={cn('modal', popupClass)}>
						<AlertDialog.Title className="text-base font-semibold text-balance">{title}</AlertDialog.Title>
						<AlertDialog.Description className="mt-1.5 text-sm text-muted text-pretty">{description}</AlertDialog.Description>
						{children}
						<div className="mt-5 flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
							<AlertDialog.Close render={<Button variant="secondary">Keep it</Button>} />
							<Button variant={tone === 'danger' ? 'danger' : 'primary'} disabled={pending} onClick={onConfirm}>
								{pending ? 'Working…' : confirmLabel}
							</Button>
						</div>
					</AlertDialog.Popup>
				</AlertDialog.Viewport>
			</AlertDialog.Portal>
		</AlertDialog.Root>
	);
}

export function Modal({
	open,
	onOpenChange,
	title,
	description,
	children,
	className,
	animated = true,
}: {
	open: boolean;
	onOpenChange: (open: boolean) => void;
	title: string;
	description?: ReactNode;
	children: ReactNode;
	className?: string;
	animated?: boolean;
}) {
	return (
		<Dialog.Root open={open} onOpenChange={(next) => onOpenChange(next)}>
			<Dialog.Portal>
				<Dialog.Backdrop className={cn('fixed inset-0 z-50 bg-[var(--overlay)]', animated && 'backdrop')} />
				<Dialog.Viewport className={viewportClass}>
					<Dialog.Popup className={cn(animated && 'modal', popupClass, className)}>
						<div className="flex items-start gap-3">
							<div className="min-w-0 flex-1">
								<Dialog.Title className="text-base font-semibold text-balance">{title}</Dialog.Title>
								{description ? (
									<Dialog.Description className="mt-1 text-sm text-muted text-pretty">{description}</Dialog.Description>
								) : null}
							</div>
							<Dialog.Close
								aria-label="Close"
								className="press -mt-1.5 -mr-1.5 inline-flex size-10 shrink-0 items-center justify-center rounded-lg text-muted hover:bg-surface-2 hover:text-fg md:size-8"
							>
								<X aria-hidden className="size-4" />
							</Dialog.Close>
						</div>
						{children}
					</Dialog.Popup>
				</Dialog.Viewport>
			</Dialog.Portal>
		</Dialog.Root>
	);
}
