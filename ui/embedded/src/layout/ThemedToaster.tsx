import { Toaster } from 'sonner';
import { useThemePreference } from '../lib/theme';

export function ThemedToaster() {
	const theme = useThemePreference();
	return (
		<Toaster
			theme={theme}
			position="bottom-right"
			mobileOffset={{ bottom: 'calc(var(--tabbar-height) + var(--safe-bottom) + 12px)' }}
			toastOptions={{ className: 'font-sans' }}
		/>
	);
}
