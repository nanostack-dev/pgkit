import { Select } from '@base-ui/react/select';
import { Switch } from '@base-ui/react/switch';
import { Toggle } from '@base-ui/react/toggle';
import { ToggleGroup } from '@base-ui/react/toggle-group';
import { Check, ChevronDown, ChevronLeft, ChevronRight, Search, X } from 'lucide-react';
import { useEffect, useId, useRef, useState } from 'react';
import { cn } from '../lib/cn';
import { formatCompact, formatNumber } from '../lib/format';
import { IconButton } from './Button';

export function SearchInput({
	value,
	onChange,
	placeholder,
	label,
	className,
}: {
	value: string;
	onChange: (value: string) => void;
	placeholder: string;
	label: string;
	className?: string;
}) {
	const [draft, setDraft] = useState(value);
	const [synced, setSynced] = useState(value);
	const timer = useRef<number | undefined>(undefined);

	if (value !== synced) {
		setSynced(value);
		setDraft(value);
	}

	useEffect(() => () => window.clearTimeout(timer.current), []);

	function update(next: string) {
		setDraft(next);
		window.clearTimeout(timer.current);
		timer.current = window.setTimeout(() => onChange(next.trim()), 250);
	}

	function clear() {
		window.clearTimeout(timer.current);
		setDraft('');
		onChange('');
	}

	return (
		<div className={cn('relative flex min-w-0 items-center', className)}>
			<Search aria-hidden className="pointer-events-none absolute left-3 size-4 text-subtle" />
			<input
				type="search"
				data-search-input
				value={draft}
				aria-label={label}
				placeholder={placeholder}
				onChange={(event) => update(event.target.value)}
				onKeyDown={(event) => {
					if (event.key === 'Escape' && draft) {
						event.stopPropagation();
						clear();
					}
					if (event.key === 'Enter') {
						window.clearTimeout(timer.current);
						onChange(draft.trim());
					}
				}}
				autoComplete="off"
				spellCheck={false}
				className="h-11 w-full min-w-0 rounded-lg border border-line-strong bg-surface pr-10 pl-9 text-base text-fg placeholder:text-subtle [&::-webkit-search-cancel-button]:hidden md:h-9 md:text-sm"
			/>
			{draft ? (
				<button
					type="button"
					aria-label="Clear search"
					onClick={clear}
					className="press absolute right-1 inline-flex size-9 items-center justify-center rounded-md text-subtle hover:text-fg md:size-7"
				>
					<X aria-hidden className="size-4" />
				</button>
			) : (
				<kbd className="pointer-events-none absolute right-2.5 hidden rounded border border-line px-1.5 font-mono text-[0.6875rem] text-subtle md:block">
					/
				</kbd>
			)}
		</div>
	);
}

export type FilterOption<T extends string> = {
	value: T;
	label: string;
	count?: number;
};

export function SegmentedFilter<T extends string>({
	label,
	value,
	options,
	onChange,
	className,
}: {
	label: string;
	value: T | undefined;
	options: FilterOption<T>[];
	onChange: (value: T | undefined) => void;
	className?: string;
}) {
	const pressed: string[] = value ? [value] : [''];
	return (
		<ToggleGroup
			aria-label={label}
			value={pressed}
			onValueChange={(next) => {
				const selected = next.find((item) => item !== value) ?? next[0];
				onChange(selected ? (selected as T) : undefined);
			}}
			className={cn('no-scrollbar -mx-4 flex gap-1.5 overflow-x-auto px-4 md:mx-0 md:px-0', className)}
		>
			<FilterChip value="" label="All" />
			{options.map((option) => (
				<FilterChip key={option.value} value={option.value} label={option.label} count={option.count} />
			))}
		</ToggleGroup>
	);
}

function FilterChip({ value, label, count }: { value: string; label: string; count?: number }) {
	return (
		<Toggle
			value={value}
			className="press inline-flex h-10 shrink-0 items-center gap-1.5 rounded-full border border-line bg-surface px-3.5 text-[0.8125rem] font-medium text-muted transition-[background-color,color,border-color,transform] select-none hover:text-fg data-[pressed]:border-fg data-[pressed]:bg-fg data-[pressed]:text-page md:h-8 md:px-3"
		>
			{label}
			{count !== undefined ? <span className="tabular text-xs opacity-70">{formatCompact(count)}</span> : null}
		</Toggle>
	);
}

export type SelectOption = { value: string; label: string; hint?: string };

export function SelectField({
	label,
	value,
	options,
	onChange,
	placeholder,
	className,
}: {
	label: string;
	value: string | undefined;
	options: SelectOption[];
	onChange: (value: string | undefined) => void;
	placeholder: string;
	className?: string;
}) {
	const items = [{ value: '', label: placeholder }, ...options];
	return (
		<Select.Root
			items={items}
			value={value ?? ''}
			onValueChange={(next) => onChange(next ? String(next) : undefined)}
		>
			<Select.Trigger
				aria-label={label}
				className={cn(
					'press inline-flex h-11 min-w-0 items-center justify-between gap-2 rounded-lg border border-line-strong bg-surface pr-2.5 pl-3 text-left text-sm text-fg transition-[background-color,transform] hover:bg-surface-2 md:h-9',
					className,
				)}
			>
				<Select.Value className="min-w-0 truncate" />
				<Select.Icon className="shrink-0 text-subtle">
					<ChevronDown aria-hidden className="size-4" />
				</Select.Icon>
			</Select.Trigger>
			<Select.Portal>
				<Select.Positioner sideOffset={6} alignItemWithTrigger={false} className="z-50 outline-none">
					<Select.Popup className="popup max-h-[min(24rem,var(--available-height))] min-w-[max(var(--anchor-width),12rem)] max-w-[min(28rem,calc(100vw-2rem))] overflow-y-auto overscroll-contain rounded-xl border border-line bg-surface p-1 text-fg shadow-popover outline-none">
						<Select.List>
							{items.map((item) => (
								<Select.Item
									key={item.value}
									value={item.value}
									className="grid min-h-11 cursor-default grid-cols-[1rem_minmax(0,1fr)_auto] items-center gap-2 rounded-lg px-2.5 text-sm outline-none select-none data-[highlighted]:bg-surface-2 md:min-h-8"
								>
									<Select.ItemIndicator className="col-start-1 text-fg">
										<Check aria-hidden className="size-3.5" strokeWidth={2.5} />
									</Select.ItemIndicator>
									<Select.ItemText className="col-start-2 truncate">{item.label}</Select.ItemText>
									{'hint' in item && item.hint ? (
										<span className="tabular col-start-3 text-xs text-subtle">{item.hint}</span>
									) : null}
								</Select.Item>
							))}
						</Select.List>
					</Select.Popup>
				</Select.Positioner>
			</Select.Portal>
		</Select.Root>
	);
}

export function SwitchField({
	label,
	checked,
	onChange,
	className,
}: {
	label: string;
	checked: boolean;
	onChange: (checked: boolean) => void;
	className?: string;
}) {
	const id = useId();
	return (
		<label htmlFor={id} className={cn('inline-flex min-h-11 cursor-pointer items-center gap-2.5 text-[0.8125rem] text-muted select-none md:min-h-9', className)}>
			<Switch.Root
				id={id}
				checked={checked}
				onCheckedChange={(next) => onChange(next)}
				className="relative inline-flex h-6 w-10 shrink-0 rounded-full bg-line-strong p-0.5 transition-colors duration-150 data-[checked]:bg-fg md:h-5 md:w-8"
			>
				<Switch.Thumb className="block size-5 rounded-full bg-surface shadow-sm transition-transform duration-150 ease-out data-[checked]:translate-x-4 md:size-4 md:data-[checked]:translate-x-3" />
			</Switch.Root>
			{label}
		</label>
	);
}

export function Pagination({
	total,
	limit,
	offset,
	onOffsetChange,
	noun,
}: {
	total: number;
	limit: number;
	offset: number;
	onOffsetChange: (offset: number) => void;
	noun: string;
}) {
	if (total === 0) return null;
	const first = Math.min(total, offset + 1);
	const last = Math.min(total, offset + limit);
	return (
		<nav aria-label={`${noun} pages`} className="flex items-center justify-between gap-3 px-1 pt-3">
			<p className="tabular text-[0.8125rem] text-muted" aria-live="polite">
				{formatNumber(first)}–{formatNumber(last)} of {formatNumber(total)}
			</p>
			<div className="flex items-center gap-1">
				<IconButton
					icon={ChevronLeft}
					label="Previous page"
					disabled={offset === 0}
					onClick={() => onOffsetChange(Math.max(0, offset - limit))}
				/>
				<IconButton
					icon={ChevronRight}
					label="Next page"
					disabled={offset + limit >= total}
					onClick={() => onOffsetChange(offset + limit)}
				/>
			</div>
		</nav>
	);
}
