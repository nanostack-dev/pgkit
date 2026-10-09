import { Box, Hourglass, RadioTower, Workflow, type LucideIcon } from 'lucide-react';
import type { JobStatus, RunStatus, StepKind, StepStatus } from '../api/types';

export type Tone = 'ok' | 'bad' | 'warn' | 'run' | 'wait' | 'idle';

export type AnyStatus = JobStatus | RunStatus | StepStatus;

const tones: Record<AnyStatus, Tone> = {
	pending: 'idle',
	processing: 'run',
	done: 'ok',
	failed: 'bad',
	running: 'run',
	waiting: 'wait',
	succeeded: 'ok',
	cancelled: 'idle',
	retrying: 'warn',
	timed_out: 'bad',
};

const labels: Record<AnyStatus, string> = {
	pending: 'Pending',
	processing: 'Processing',
	done: 'Done',
	failed: 'Failed',
	running: 'Running',
	waiting: 'Waiting',
	succeeded: 'Succeeded',
	cancelled: 'Cancelled',
	retrying: 'Retrying',
	timed_out: 'Timed out',
};

export function statusTone(status: AnyStatus): Tone {
	return tones[status] ?? 'idle';
}

export function statusLabel(status: AnyStatus): string {
	return labels[status] ?? status;
}

export function isActive(status: AnyStatus): boolean {
	return status === 'running' || status === 'processing' || status === 'retrying';
}

export const toneText: Record<Tone, string> = {
	ok: 'text-ok',
	bad: 'text-bad',
	warn: 'text-warn',
	run: 'text-run',
	wait: 'text-wait',
	idle: 'text-muted',
};

export const toneSoft: Record<Tone, string> = {
	ok: 'bg-ok-soft text-ok',
	bad: 'bg-bad-soft text-bad',
	warn: 'bg-warn-soft text-warn',
	run: 'bg-run-soft text-run',
	wait: 'bg-wait-soft text-wait',
	idle: 'bg-idle-soft text-muted',
};

export const toneDot: Record<Tone, string> = {
	ok: 'bg-ok',
	bad: 'bg-bad',
	warn: 'bg-warn',
	run: 'bg-run',
	wait: 'bg-wait',
	idle: 'bg-idle',
};

export function runFinished(status: RunStatus): boolean {
	return status === 'succeeded' || status === 'failed' || status === 'cancelled';
}

export function runRetryable(status: RunStatus): boolean {
	return status === 'failed' || status === 'cancelled';
}

export function jobReplayable(status: JobStatus): boolean {
	return status === 'done' || status === 'failed';
}

export const kindMeta: Record<StepKind, { label: string; icon: LucideIcon }> = {
	step: { label: 'Step', icon: Box },
	sleep: { label: 'Sleep', icon: Hourglass },
	signal: { label: 'Signal', icon: RadioTower },
	child: { label: 'Child run', icon: Workflow },
};
