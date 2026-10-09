import { errorMessage } from '../../api/client';
import { useId, useState, type FormEvent } from 'react';
import { toast } from 'sonner';
import { useEnqueueJob } from '../../api/queries';
import { Button } from '../../components/Button';
import { Modal } from '../../components/Dialogs';
import { jobLabel } from '../../lib/format';

const fieldClass =
	'w-full min-w-0 rounded-lg border border-line-strong bg-surface px-3 text-base text-fg placeholder:text-subtle md:text-sm aria-[invalid=true]:border-bad';

function parsePayload(text: string): { value: unknown; error: string | null } {
	if (!text.trim()) return { value: {}, error: null };
	try {
		return { value: JSON.parse(text), error: null };
	} catch (error) {
		return { value: undefined, error: error instanceof Error ? error.message : 'Invalid JSON' };
	}
}

export function EnqueueDialog({
	open,
	onOpenChange,
	queues,
	defaultQueue,
	onEnqueued,
}: {
	open: boolean;
	onOpenChange: (open: boolean) => void;
	queues: string[];
	defaultQueue?: string;
	onEnqueued: (id: number) => void;
}) {
	const id = useId();
	const enqueue = useEnqueueJob();
	const [queue, setQueue] = useState(defaultQueue ?? '');
	const [payload, setPayload] = useState('{\n  \n}');
	const [maxAttempts, setMaxAttempts] = useState('5');
	const [delay, setDelay] = useState('0');
	const [touched, setTouched] = useState(false);
	const [wasOpen, setWasOpen] = useState(open);

	if (open !== wasOpen) {
		setWasOpen(open);
		if (open) {
			setQueue(defaultQueue ?? '');
			setTouched(false);
		}
	}

	const parsed = parsePayload(payload);
	const attempts = Number(maxAttempts);
	const delaySeconds = Number(delay);
	const queueError = touched && !queue.trim() ? 'Enter a queue name.' : null;
	const attemptsError = !Number.isInteger(attempts) || attempts < 1 || attempts > 1000 ? 'Use a whole number from 1 to 1,000.' : null;
	const delayError = !Number.isInteger(delaySeconds) || delaySeconds < 0 ? 'Use zero or a positive whole number.' : null;
	const invalid = !queue.trim() || parsed.error !== null || attemptsError !== null || delayError !== null;

	function submit(event: FormEvent) {
		event.preventDefault();
		setTouched(true);
		if (invalid) return;
		enqueue.mutate(
			{ queue_name: queue.trim(), payload: parsed.value, max_attempts: attempts, delay_seconds: delaySeconds },
			{
				onSuccess: (result) => {
					onOpenChange(false);
					toast.success(`Enqueued job ${jobLabel(result.id)}`, {
						action: { label: 'Open', onClick: () => onEnqueued(result.id) },
					});
				},
				onError: (error) => toast.error(errorMessage(error)),
			},
		);
	}

	return (
		<Modal open={open} onOpenChange={onOpenChange} title="Enqueue a job" description="The job is stored durably and picked up by any worker on that queue.">
			<form onSubmit={submit} noValidate className="mt-4 space-y-4">
				<div>
					<label htmlFor={`${id}-queue`} className="mb-1.5 block text-[0.8125rem] font-medium">
						Queue
					</label>
					<input
						id={`${id}-queue`}
						list={`${id}-queues`}
						value={queue}
						onChange={(event) => setQueue(event.target.value)}
						onBlur={() => setTouched(true)}
						placeholder="emails.send"
						autoComplete="off"
						spellCheck={false}
						aria-invalid={Boolean(queueError)}
						aria-describedby={queueError ? `${id}-queue-error` : undefined}
						className={`${fieldClass} h-11 font-mono md:h-9`}
					/>
					<datalist id={`${id}-queues`}>
						{queues.map((name) => (
							<option key={name} value={name} />
						))}
					</datalist>
					{queueError ? (
						<p id={`${id}-queue-error`} className="mt-1 text-xs text-bad">
							{queueError}
						</p>
					) : null}
				</div>
				<div>
					<label htmlFor={`${id}-payload`} className="mb-1.5 block text-[0.8125rem] font-medium">
						Payload <span className="font-normal text-muted">JSON</span>
					</label>
					<textarea
						id={`${id}-payload`}
						value={payload}
						onChange={(event) => setPayload(event.target.value)}
						rows={6}
						spellCheck={false}
						aria-invalid={Boolean(parsed.error)}
						aria-describedby={parsed.error ? `${id}-payload-error` : undefined}
						className={`${fieldClass} resize-y py-2 font-mono text-[0.8125rem] leading-relaxed md:text-[0.8125rem]`}
					/>
					{parsed.error ? (
						<p id={`${id}-payload-error`} className="mt-1 text-xs text-bad">
							{parsed.error}
						</p>
					) : null}
				</div>
				<div className="grid grid-cols-2 gap-3">
					<div>
						<label htmlFor={`${id}-attempts`} className="mb-1.5 block text-[0.8125rem] font-medium">
							Max attempts
						</label>
						<input
							id={`${id}-attempts`}
							type="number"
							inputMode="numeric"
							min={1}
							max={1000}
							value={maxAttempts}
							onChange={(event) => setMaxAttempts(event.target.value)}
							aria-invalid={Boolean(attemptsError)}
							className={`${fieldClass} tabular h-11 md:h-9`}
						/>
						{attemptsError ? <p className="mt-1 text-xs text-bad">{attemptsError}</p> : null}
					</div>
					<div>
						<label htmlFor={`${id}-delay`} className="mb-1.5 block text-[0.8125rem] font-medium">
							Delay <span className="font-normal text-muted">seconds</span>
						</label>
						<input
							id={`${id}-delay`}
							type="number"
							inputMode="numeric"
							min={0}
							value={delay}
							onChange={(event) => setDelay(event.target.value)}
							aria-invalid={Boolean(delayError)}
							className={`${fieldClass} tabular h-11 md:h-9`}
						/>
						{delayError ? <p className="mt-1 text-xs text-bad">{delayError}</p> : null}
					</div>
				</div>
				<div className="flex flex-col-reverse gap-2 pt-1 sm:flex-row sm:justify-end">
					<Button variant="secondary" onClick={() => onOpenChange(false)}>
						Cancel
					</Button>
					<Button type="submit" variant="primary" disabled={enqueue.isPending || (touched && invalid)}>
						{enqueue.isPending ? 'Enqueuing…' : 'Enqueue job'}
					</Button>
				</div>
			</form>
		</Modal>
	);
}
