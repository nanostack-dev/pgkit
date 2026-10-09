import { Menu } from '@base-ui/react/menu';
import { Check, Monitor, Moon, Sun } from 'lucide-react';
import { cn } from '../lib/cn';
import { setThemePreference, useThemePreference, type ThemePreference } from '../lib/theme';

const options: { value: ThemePreference; label: string; icon: typeof Sun }[] = [
	{ value: 'system', label: 'System', icon: Monitor },
	{ value: 'light', label: 'Light', icon: Sun },
	{ value: 'dark', label: 'Dark', icon: Moon },
];

export function ThemeMenu({ className, side = 'bottom' }: { className?: string; side?: 'top' | 'bottom' }) {
	const preference = useThemePreference();
	const current = options.find((option) => option.value === preference) ?? options[0]!;
	const Icon = current.icon;
	return (
		<Menu.Root>
			<Menu.Trigger
				aria-label={`Theme: ${current.label}`}
				title="Theme"
				className={cn(
					'press inline-flex size-11 items-center justify-center rounded-lg text-muted transition-[background-color,color,transform] hover:bg-surface-2 hover:text-fg data-[popup-open]:bg-surface-2 data-[popup-open]:text-fg md:size-9',
					className,
				)}
			>
				<Icon aria-hidden className="size-[1.125rem]" />
			</Menu.Trigger>
			<Menu.Portal>
				<Menu.Positioner side={side} align="end" sideOffset={6} className="z-50 outline-none">
					<Menu.Popup className="popup min-w-40 rounded-xl border border-line bg-surface p-1 text-fg shadow-popover outline-none">
						<Menu.RadioGroup value={preference} onValueChange={(value) => setThemePreference(value as ThemePreference)}>
							{options.map((option) => (
								<Menu.RadioItem
									key={option.value}
									value={option.value}
									closeOnClick
									className="grid min-h-11 cursor-default grid-cols-[1rem_1fr_1rem] items-center gap-2.5 rounded-lg px-2.5 text-sm outline-none select-none data-[highlighted]:bg-surface-2 md:min-h-8"
								>
									<option.icon aria-hidden className="size-4 text-muted" />
									{option.label}
									<Menu.RadioItemIndicator>
										<Check aria-hidden className="size-3.5" strokeWidth={2.5} />
									</Menu.RadioItemIndicator>
								</Menu.RadioItem>
							))}
						</Menu.RadioGroup>
					</Menu.Popup>
				</Menu.Positioner>
			</Menu.Portal>
		</Menu.Root>
	);
}
