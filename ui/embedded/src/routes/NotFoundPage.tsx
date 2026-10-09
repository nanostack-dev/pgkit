import { Compass } from 'lucide-react';
import { Link } from 'react-router';
import { EmptyState } from '../components/States';

export function NotFoundPage() {
	return (
		<div className="card">
			<EmptyState
				icon={Compass}
				title="This page does not exist"
				action={
					<Link to="/" className="text-[0.8125rem] font-medium text-fg underline underline-offset-4">
						Go to the overview
					</Link>
				}
			>
				Check the address, or use ⌘K to jump to a page, run or job.
			</EmptyState>
		</div>
	);
}
