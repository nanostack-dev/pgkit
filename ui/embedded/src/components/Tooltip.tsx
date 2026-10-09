import { Tooltip as BaseTooltip } from '@base-ui/react/tooltip';
import type { ReactElement, ReactNode } from 'react';

export function TooltipProvider({ children }: { children: ReactNode }) {
	return <BaseTooltip.Provider delay={500}>{children}</BaseTooltip.Provider>;
}

export function Tooltip({ content, children }: { content: ReactNode; children: ReactElement }) {
	return (
		<BaseTooltip.Root>
			<BaseTooltip.Trigger render={children} />
			<BaseTooltip.Portal>
				<BaseTooltip.Positioner sideOffset={6} className="z-50">
					<BaseTooltip.Popup className="tooltip-popup max-w-64 rounded-md bg-fg px-2 py-1 text-xs text-page shadow-popover">
						{content}
					</BaseTooltip.Popup>
				</BaseTooltip.Positioner>
			</BaseTooltip.Portal>
		</BaseTooltip.Root>
	);
}
