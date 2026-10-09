import type { StepRef, WorkflowStep } from '../api/types';
import { plural, splitStepName, untilPhrase } from './format';
import type { Tone } from './status';

export type StepDescription = {
	text: string;
	detail: string | null;
	tone: Tone;
};

type StepLike = Pick<StepRef, 'name' | 'kind' | 'status' | 'attempts' | 'signal' | 'wake_at' | 'child_run_id'> &
	Partial<Pick<WorkflowStep, 'error'>>;

export function displayStepName(name: string): string {
	const { base, repeat } = splitStepName(name);
	return repeat ? `${base} #${repeat}` : base;
}

export function describeStep(step: StepLike, now: number): StepDescription {
	const name = displayStepName(step.name);
	switch (step.status) {
		case 'waiting':
			if (step.kind === 'signal') {
				return {
					text: `Waiting for signal ${step.signal ?? name}`,
					detail: untilPhrase(step.wake_at, now, 'times out in', 'timeout reached') ?? 'no timeout',
					tone: 'wait',
				};
			}
			if (step.kind === 'sleep')
				return { text: `Sleeping in ${name}`, detail: untilPhrase(step.wake_at, now, 'wakes in', 'waking up now'), tone: 'wait' };
			if (step.kind === 'child') return { text: `Waiting for child run ${name}`, detail: null, tone: 'wait' };
			return { text: `Waiting in ${name}`, detail: untilPhrase(step.wake_at, now, 'resumes in', 'resuming now'), tone: 'wait' };
		case 'retrying':
			return {
				text: `Retrying ${name}`,
				detail: [`${plural(step.attempts, 'attempt', 'attempts')} so far`, untilPhrase(step.wake_at, now, 'next in', 'retrying now')]
					.filter(Boolean)
					.join(' · '),
				tone: 'warn',
			};
		case 'running':
			return {
				text: step.kind === 'child' ? `Starting child run ${name}` : `Running ${name}`,
				detail: step.attempts > 1 ? `attempt ${step.attempts}` : null,
				tone: 'run',
			};
		case 'failed':
			return {
				text: `Failed at ${name}`,
				detail: step.attempts > 0 ? `after ${plural(step.attempts, 'attempt', 'attempts')}` : null,
				tone: 'bad',
			};
		case 'timed_out':
			return {
				text: step.kind === 'signal' ? `Timed out waiting for ${step.signal ?? name}` : `Timed out at ${name}`,
				detail: null,
				tone: 'bad',
			};
		default:
			return { text: `Completed ${name}`, detail: null, tone: 'ok' };
	}
}
