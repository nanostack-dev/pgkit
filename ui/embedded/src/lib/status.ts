import type { QueueJobStatus, WorkflowRunStatus, WorkflowStepStatus } from './types';

export function queueStatusTone(status: QueueJobStatus): string {
	switch (status) {
		case 'done':
			return 'preset-tonal-success';
		case 'failed':
			return 'preset-tonal-error';
		case 'processing':
			return 'preset-tonal-warning';
		default:
			return 'preset-tonal-primary';
	}
}

type BadgeStatus = WorkflowRunStatus | WorkflowStepStatus;

const badgeClasses: Record<BadgeStatus, string> = {
	succeeded: 'bg-success-50 text-success-700 ring-1 ring-success-500/20',
	failed: 'bg-error-50 text-error-700 ring-1 ring-error-500/20',
	running: 'bg-primary-50 text-primary-700 ring-1 ring-primary-500/20',
	waiting: 'bg-secondary-50 text-secondary-700 ring-1 ring-secondary-500/20',
	retrying: 'bg-warning-50 text-warning-700 ring-1 ring-warning-500/20',
	timed_out: 'bg-warning-50 text-warning-700 ring-1 ring-warning-500/20',
	pending: 'bg-surface-100 text-surface-700 ring-1 ring-surface-500/20',
	cancelled: 'bg-surface-100 text-surface-500 ring-1 ring-surface-500/20'
};

export function statusBadgeClass(status: BadgeStatus): string {
	return badgeClasses[status] ?? badgeClasses.pending;
}

export function isFinished(status: WorkflowRunStatus): boolean {
	return status === 'succeeded' || status === 'failed' || status === 'cancelled';
}

export function isRetryable(status: WorkflowRunStatus): boolean {
	return status === 'failed' || status === 'cancelled';
}

export function statusLabel(status: BadgeStatus): string {
	return status.replace('_', ' ');
}
