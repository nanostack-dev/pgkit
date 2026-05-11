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

export function workflowRunTone(status: WorkflowRunStatus): string {
	switch (status) {
		case 'succeeded':
			return 'preset-tonal-success';
		case 'failed':
			return 'preset-tonal-error';
		case 'cancelled':
			return 'preset-tonal-surface';
		case 'cancelling':
			return 'preset-tonal-warning';
		default:
			return 'preset-tonal-primary';
	}
}

export function workflowStepTone(status: WorkflowStepStatus): string {
	switch (status) {
		case 'succeeded':
			return 'preset-tonal-success';
		case 'failed':
			return 'preset-tonal-error';
		case 'running':
			return 'preset-tonal-warning';
		case 'waiting_retry':
			return 'preset-tonal-secondary';
		case 'cancelled':
			return 'preset-tonal-surface';
		case 'skipped':
			return 'preset-tonal-tertiary';
		default:
			return 'preset-tonal-primary';
	}
}

export function statusDotColor(status: QueueJobStatus | WorkflowRunStatus | WorkflowStepStatus): string {
	switch (status) {
		case 'done':
		case 'succeeded':
			return 'background: var(--color-success-500);';
		case 'failed':
			return 'background: var(--color-error-500);';
		case 'processing':
		case 'running':
		case 'cancelling':
			return 'background: var(--color-warning-500);';
		case 'waiting_retry':
			return 'background: var(--color-secondary-500);';
		case 'cancelled':
		case 'skipped':
			return 'background: var(--color-surface-500);';
		default:
			return 'background: var(--color-primary-500);';
	}
}
