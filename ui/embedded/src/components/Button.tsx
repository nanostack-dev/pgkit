import { forwardRef, type ButtonHTMLAttributes, type ReactNode } from 'react';
import type { LucideIcon } from 'lucide-react';
import { cn } from '../lib/cn';

type Variant = 'primary' | 'secondary' | 'ghost' | 'danger' | 'danger-outline';
type Size = 'sm' | 'md';

const variants: Record<Variant, string> = {
	primary: 'bg-fg text-page hover:bg-fg/90 border border-transparent',
	secondary: 'bg-surface text-fg border border-line-strong hover:bg-surface-2',
	ghost: 'bg-transparent text-muted border border-transparent hover:bg-surface-2 hover:text-fg',
	danger: 'bg-bad text-white border border-transparent hover:bg-bad/90 dark:text-page',
	'danger-outline': 'bg-surface text-bad border border-line-strong hover:bg-bad-soft',
};

const sizes: Record<Size, string> = {
	sm: 'min-h-10 md:min-h-8 px-3 text-[0.8125rem] gap-1.5 rounded-lg',
	md: 'min-h-11 md:min-h-9 px-3.5 text-sm gap-2 rounded-lg',
};

export type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
	variant?: Variant;
	size?: Size;
	icon?: LucideIcon;
	children?: ReactNode;
};

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
	{ variant = 'secondary', size = 'md', icon: Icon, className, children, type = 'button', ...props },
	ref,
) {
	return (
		<button
			ref={ref}
			type={type}
			className={cn(
				'press inline-flex shrink-0 items-center justify-center font-medium whitespace-nowrap select-none transition-[background-color,color,border-color,transform] disabled:cursor-not-allowed disabled:opacity-50 data-[disabled]:cursor-not-allowed data-[disabled]:opacity-50',
				variants[variant],
				sizes[size],
				className,
			)}
			{...props}
		>
			{Icon ? <Icon aria-hidden className="size-4 shrink-0" strokeWidth={2} /> : null}
			{children}
		</button>
	);
});

export type IconButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
	icon: LucideIcon;
	label: string;
	size?: Size;
};

export const IconButton = forwardRef<HTMLButtonElement, IconButtonProps>(function IconButton(
	{ icon: Icon, label, size = 'md', className, type = 'button', ...props },
	ref,
) {
	return (
		<button
			ref={ref}
			type={type}
			aria-label={label}
			title={label}
			className={cn(
				'press inline-flex shrink-0 items-center justify-center rounded-lg text-muted transition-[background-color,color,transform] hover:bg-surface-2 hover:text-fg disabled:cursor-not-allowed disabled:opacity-50',
				size === 'md' ? 'size-11 md:size-9' : 'size-10 md:size-7',
				className,
			)}
			{...props}
		>
			<Icon aria-hidden className={size === 'md' ? 'size-[1.125rem]' : 'size-4'} strokeWidth={2} />
		</button>
	);
});
