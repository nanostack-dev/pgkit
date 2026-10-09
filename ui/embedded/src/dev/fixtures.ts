import type {
	AdminConfig,
	AdvisoryLock,
	JobStatus,
	Overview,
	QueueJob,
	QueueJobDetail,
	QueueStats,
	RunDetail,
	RunStatus,
	RunSummary,
	RunTreeNode,
	StepKind,
	StepStatus,
	WorkflowStats,
	WorkflowStep,
} from '../api/types';

export type Dataset = {
	config: AdminConfig;
	queues: QueueStats[];
	jobs: QueueJobDetail[];
	workflows: WorkflowStats[];
	runs: RunSummary[];
	details: Map<string, RunDetail>;
	trees: Map<string, { nodes: RunTreeNode[]; truncated: boolean }>;
	locks: AdvisoryLock[];
	overview: (range: string) => Overview;
};

const minute = 60_000;

function at(offsetMs: number): string {
	return new Date(Date.now() + offsetMs).toISOString();
}

function uuid(seed: number): string {
	const hex = (seed * 2654435761 >>> 0).toString(16).padStart(8, '0');
	return `01a11e35-${hex.slice(0, 4)}-7${hex.slice(4, 7)}-b${hex.slice(0, 3)}-${hex}${hex.slice(0, 4)}`;
}

function job(id: number, queue: string, status: JobStatus, extra: Partial<QueueJobDetail> = {}): QueueJobDetail {
	const payload = extra.payload ?? JSON.stringify({ id, queue });
	return {
		id,
		queue_name: queue,
		status,
		attempts: status === 'pending' ? 0 : status === 'failed' ? 5 : 1,
		max_attempts: 5,
		claims: status === 'pending' ? 0 : status === 'failed' ? 5 : 1,
		available_at: at(-id * minute),
		claimed_by: status === 'processing' ? 'worker-1' : null,
		claimed_at: status === 'processing' ? at(-20_000) : null,
		done_at: status === 'done' ? at(-id * 30_000) : null,
		last_error: status === 'failed' ? 'smtp: 421 too many connections' : null,
		payload_preview: payload.length > 120 ? `${payload.slice(0, 120)}...` : payload,
		created_at: at(-id * minute - 5_000),
		updated_at: at(-id * 20_000),
		payload,
		payload_encoding: 'json',
		payload_bytes: payload.length,
		payload_truncated: false,
		run_id: null,
		...extra,
	};
}

function stepRow(name: string, kind: StepKind, status: StepStatus, extra: Partial<WorkflowStep> = {}): WorkflowStep {
	return {
		name,
		kind,
		status,
		attempts: kind === 'step' ? 1 : 0,
		output: status === 'succeeded' ? JSON.stringify({ ok: true }) : null,
		error: null,
		wake_at: null,
		child_run_id: null,
		signal: kind === 'signal' ? name : null,
		created_at: at(-10 * minute),
		completed_at: status === 'succeeded' ? at(-9 * minute) : null,
		updated_at: at(-9 * minute),
		...extra,
	};
}

function run(id: string, workflow: string, status: RunStatus, extra: Partial<RunSummary> = {}): RunSummary {
	const finished = status === 'succeeded' || status === 'failed' || status === 'cancelled';
	return {
		id,
		workflow,
		version: 0,
		key: null,
		status,
		error: status === 'failed' ? 'workflow: step "charge-card" failed after 3 attempt(s): card declined' : null,
		timed_out: false,
		parent_run_id: null,
		wake_at: null,
		deadline_at: null,
		created_at: at(-12 * minute),
		started_at: at(-12 * minute + 800),
		completed_at: finished ? at(-2 * minute) : null,
		updated_at: at(-2 * minute),
		child_count: 0,
		step_count: 3,
		current_step: null,
		...extra,
	};
}

function stats(queue: string, jobs: QueueJobDetail[]): QueueStats {
	const of = (status: JobStatus) => jobs.filter((item) => item.queue_name === queue && item.status === status).length;
	return {
		queue_name: queue,
		total: jobs.filter((item) => item.queue_name === queue).length,
		pending: of('pending'),
		due: of('pending'),
		processing: of('processing'),
		done: of('done'),
		failed: of('failed'),
		oldest_due_at: of('pending') ? at(-3 * minute) : null,
		last_activity_at: at(-minute),
	};
}

function workflowStats(name: string, runs: RunSummary[], versions = [0]): WorkflowStats {
	const of = (status: RunStatus) => runs.filter((item) => item.workflow === name && item.status === status).length;
	return {
		workflow: name,
		versions,
		total: runs.filter((item) => item.workflow === name).length,
		pending: of('pending'),
		running: of('running'),
		waiting: of('waiting'),
		succeeded: of('succeeded'),
		failed: of('failed'),
		cancelled: of('cancelled'),
		last_run_at: at(-minute),
	};
}

function buckets(range: string, shape: (index: number, count: number) => number[]) {
	const count = range === '24h' ? 48 : 60;
	const size = range === '24h' ? 1_800 : 60;
	const end = Math.floor(Date.now() / 1000 / size) * size;
	return Array.from({ length: count }, (_, index) => ({
		start: new Date((end - (count - 1 - index) * size) * 1000).toISOString(),
		values: shape(index, count),
	}));
}

function overviewFrom(
	queues: QueueStats[],
	jobs: QueueJobDetail[],
	runs: RunSummary[],
	shape: (index: number, count: number) => number[],
): (range: string) => Overview {
	return (range) => {
		const series = buckets(range, shape);
		const sum = (key: keyof QueueStats) => queues.reduce((total, item) => total + Number(item[key]), 0);
		const count = (status: RunStatus) => runs.filter((item) => item.status === status).length;
		return {
			now: new Date().toISOString(),
			queue: {
				summary: {
					total_jobs: sum('total'),
					pending_jobs: sum('pending'),
					processing_jobs: sum('processing'),
					done_jobs: sum('done'),
					failed_jobs: sum('failed'),
					advisory_locks: 2,
					queues: queues.length,
				},
				due_jobs: sum('due'),
				oldest_due_at: queues.find((item) => item.oldest_due_at)?.oldest_due_at ?? null,
				throughput: {
					bucket_seconds: range === '24h' ? 1_800 : 60,
					buckets: series.map((bucket) => ({ start: bucket.start, done: bucket.values[0] ?? 0, failed: bucket.values[1] ?? 0 })),
				},
				failed_jobs: jobs.filter((item) => item.status === 'failed').slice(0, 5),
				recent_jobs: jobs.slice(0, 8),
			},
			workflow: {
				counts: {
					pending: count('pending'),
					running: count('running'),
					waiting: count('waiting'),
					succeeded: count('succeeded'),
					failed: count('failed'),
					cancelled: count('cancelled'),
					total: runs.length,
				},
				throughput: {
					bucket_seconds: range === '24h' ? 1_800 : 60,
					buckets: series.map((bucket) => ({
						start: bucket.start,
						succeeded: bucket.values[2] ?? 0,
						failed: bucket.values[3] ?? 0,
						cancelled: bucket.values[4] ?? 0,
					})),
				},
				waiting: runs.filter((item) => item.status === 'waiting').slice(0, 8),
				failed: runs.filter((item) => item.status === 'failed').slice(0, 5),
				recent: runs.slice(0, 8),
			},
		};
	};
}

function detailFor(summary: RunSummary, steps: WorkflowStep[], extra: Partial<RunDetail> = {}): RunDetail {
	return {
		now: new Date().toISOString(),
		run: { ...summary, input: JSON.stringify({ order: summary.key ?? summary.id }), output: summary.status === 'succeeded' ? '{"ok":true}' : null },
		job_id: 42,
		steps,
		children: [],
		child_total: 0,
		ancestors: [],
		signals: [],
		...extra,
	};
}

export function demoDataset(): Dataset {
	const queues = ['emails.send', 'pgworkflow:fulfil-order', 'pgworkflow:ship-item', 'reports.nightly'];
	const jobs: QueueJobDetail[] = Array.from({ length: 36 }, (_, index) => {
		const statuses: JobStatus[] = ['done', 'done', 'done', 'pending', 'processing', 'failed'];
		return job(36 - index, queues[index % queues.length]!, statuses[index % statuses.length]!);
	});
	const runs: RunSummary[] = [
		run(uuid(1), 'fulfil-order', 'waiting', {
			key: 'ORD-1005',
			current_step: { name: 'manager-approval', kind: 'signal', status: 'waiting', attempts: 0, signal: 'manager-approval', wake_at: at(9 * minute), child_run_id: null, error: null },
		}),
		run(uuid(2), 'fulfil-order', 'running', {
			key: 'ORD-1006',
			current_step: { name: 'book-label', kind: 'step', status: 'retrying', attempts: 2, signal: null, wake_at: at(12_000), child_run_id: null, error: 'carrier API timed out' },
		}),
		run(uuid(3), 'fulfil-order', 'failed', {
			key: 'ORD-1004',
			error: 'order ORD-1004 rejected by sam',
			current_step: null,
		}),
		run(uuid(4), 'fulfil-order', 'succeeded', { key: 'ORD-1003', child_count: 2 }),
		run(uuid(5), 'fulfil-order', 'cancelled', { key: 'ORD-1002', error: 'workflow: run cancelled' }),
		run(uuid(6), 'ship-item', 'succeeded', { parent_run_id: uuid(4) }),
	];
	const details = new Map<string, RunDetail>();
	for (const item of runs) {
		details.set(
			item.id,
			detailFor(item, [
				stepRow('record-order', 'step', 'succeeded'),
				stepRow('fraud-check', 'step', 'succeeded'),
				item.status === 'waiting'
					? stepRow('manager-approval', 'signal', 'waiting', { wake_at: at(9 * minute) })
					: stepRow('cooling-off', 'sleep', 'succeeded'),
			]),
		);
	}
	const queueStats = queues.map((name) => stats(name, jobs));
	return {
		config: { mutations_enabled: true, workflows_enabled: true, page_size: 20 },
		queues: queueStats,
		jobs,
		workflows: [workflowStats('fulfil-order', runs), workflowStats('ship-item', runs)],
		runs,
		details,
		trees: new Map(),
		locks: [
			{ pid: 4121, mode: 'ExclusiveLock', granted: true, key: '7340912773645128011', classid: 1709302, objid: 991223, objsubid: 1, application_name: 'pgcron', state: 'idle in transaction', xact_start: at(-40_000), state_change: at(-30_000), wait_event_type: 'Client', wait_event: 'ClientRead' },
		],
		overview: overviewFrom(queueStats, jobs, runs, (index) => [index % 7, index % 13 === 0 ? 1 : 0, index % 5, index % 17 === 0 ? 1 : 0, 0]),
	};
}

const longWorkflow = 'customer-onboarding-identity-verification-and-kyc-review';
const longKey = 'tenant:northwind-industries-holdings:account:bartholomew.fitzgerald@northwind-industries-holdings.example.com:onboarding:2026-10-08';
const longQueue = 'pgworkflow:customer-onboarding-identity-verification-and-kyc-review@12';
const panic = [
	'panic: runtime error: invalid memory address or nil pointer dereference',
	'[signal SIGSEGV: segmentation violation code=0x1 addr=0x18 pc=0x104c2b1e8]',
	'',
	'goroutine 1284 [running]:',
	...Array.from({ length: 60 }, (_, index) => [
		`github.com/northwind-industries/platform/internal/onboarding/kyc.(*ReviewService).verifyDocument${index}(0x14000a2c000, {0x105a1b2c8, 0x14000b4e0f0})`,
		`\t/home/runner/work/platform/platform/internal/onboarding/kyc/review_service_with_a_very_long_file_name.go:${200 + index} +0x1c4`,
	]).flat(),
].join('\n');

function hugeJSON(entries: number): string {
	return JSON.stringify({
		tenant: 'northwind-industries-holdings',
		owner: { name: 'Aleksandra Wiśniewska-Kowalczyk', email: 'bartholomew.fitzgerald@northwind-industries-holdings.example.com', locale: 'pl-PL' },
		notes: 'Ünïcödé ✓ 日本語のテキスト 🚚📦 עברית العربية',
		unbrokenToken: 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9'.repeat(12),
		lines: Array.from({ length: entries }, (_, index) => ({
			sku: `SKU-${String(index).padStart(6, '0')}`,
			quantity: index % 7,
			cents: index * 1_299,
			tags: ['fragile', 'oversized', index % 3 === 0 ? 'hazmat-class-9-lithium-batteries' : 'standard'],
			address: { line1: `${index} Long Industrial Estate Road, Unit ${index % 40}`, city: 'Llanfairpwllgwyngyllgogerychwyrndrobwllllantysiliogogogoch', country: 'GB' },
		})),
	});
}

export function worstDataset(): Dataset {
	const queueNames = [longQueue, 'billing.invoices.generate-monthly-statement-pdf-and-email-to-account-owner', 'x', '通知.送信', 'emoji-🚀-queue', 'emails.send', 'webhooks.deliver', 'search.reindex'];
	const bigPayload = hugeJSON(9_000);
	const jobs: QueueJobDetail[] = [
		job(9_223_372_036, longQueue, 'failed', {
			attempts: 25,
			max_attempts: 25,
			claims: 31,
			last_error: panic,
			claimed_by: 'worker-7f9c2b1e-4d3a-4e8f-9a6b-2c1d0e9f8a7b@ip-10-0-143-27.eu-west-1.compute.internal',
			payload: bigPayload.slice(0, 1_048_576),
			payload_encoding: 'text',
			payload_bytes: 1_874_221,
			payload_truncated: true,
			run_id: uuid(101),
		}),
		job(1_284_392, queueNames[1]!, 'processing', { claimed_by: 'worker-7f9c2b1e-4d3a-4e8f-9a6b-2c1d0e9f8a7b@ip-10-0-143-27.eu-west-1.compute.internal', attempts: 1, max_attempts: 1 }),
		job(3, 'x', 'pending', { payload: '', payload_preview: '', payload_bytes: 0, available_at: at(86_400_000 * 3) }),
		job(2, '通知.送信', 'done', { payload: JSON.stringify({ 件名: 'ご注文ありがとうございます', 本文: 'お届け予定日は10月12日です。' }) }),
		job(1, 'emoji-🚀-queue', 'failed', {
			attempts: 1,
			max_attempts: 1,
			last_error: 'context deadline exceeded (Client.Timeout exceeded while awaiting headers) after retrying https://api.payments.northwind-industries-holdings.example.com/v3/accounts/acct_1Nv0FGQ9RKHgCVdK/charges?expand[]=balance_transaction&expand[]=customer.invoice_settings.default_payment_method',
			payload: 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
			payload_encoding: 'base64',
			payload_preview: 'base64:iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
			payload_bytes: 70,
		}),
		...Array.from({ length: 60 }, (_, index) => job(1_000 + index, queueNames[index % queueNames.length]!, (['done', 'pending', 'failed', 'done'] as const)[index % 4]!)),
	];
	const queues: QueueStats[] = queueNames.map((name, index) => ({
		queue_name: name,
		total: index === 0 ? 1_284_392 : index === 2 ? 1 : 12 * index,
		pending: index === 0 ? 48_211 : index === 2 ? 1 : index,
		due: index === 0 ? 48_211 : 0,
		processing: index === 1 ? 1 : 0,
		done: index === 0 ? 1_236_180 : 10 * index,
		failed: index === 0 ? 1 : index % 3,
		oldest_due_at: index === 0 ? at(-26 * 60 * minute) : null,
		last_activity_at: at(-index * minute),
	}));

	const runs: RunSummary[] = [
		run(uuid(101), longWorkflow, 'failed', {
			key: longKey,
			version: 12,
			error: panic,
			timed_out: false,
			child_count: 1_284,
			step_count: 260,
			current_step: { name: 'verify-document#40', kind: 'step', status: 'failed', attempts: 25, signal: null, wake_at: null, child_run_id: null, error: panic },
		}),
		run(uuid(102), 'x', 'waiting', {
			key: 'a',
			current_step: { name: 'approval-from-the-compliance-officer-on-duty-for-the-emea-region', kind: 'signal', status: 'waiting', attempts: 0, signal: 'approval-from-the-compliance-officer-on-duty-for-the-emea-region', wake_at: null, child_run_id: null, error: null },
		}),
		run(uuid(103), '订单履行', 'running', {
			key: '订单-🚚-ORD-00000000001',
			current_step: { name: 'charge-card#3', kind: 'step', status: 'retrying', attempts: 24, signal: null, wake_at: at(3_600_000 * 5), child_run_id: null, error: 'card declined' },
		}),
		run(uuid(104), longWorkflow, 'waiting', {
			version: 12,
			current_step: { name: 'cooling-off-period-before-account-activation', kind: 'sleep', status: 'waiting', attempts: 0, signal: null, wake_at: at(-4_000), child_run_id: null, error: null },
		}),
		run(uuid(105), longWorkflow, 'pending', { key: null, step_count: 0, current_step: null }),
		run(uuid(106), 'ship-item', 'cancelled', { error: 'workflow: run cancelled', parent_run_id: uuid(101) }),
		...Array.from({ length: 40 }, (_, index) =>
			run(uuid(200 + index), index % 2 ? 'ship-item' : longWorkflow, (['succeeded', 'failed', 'running', 'waiting'] as const)[index % 4]!, {
				key: index % 3 ? `ORD-${2000 + index}` : null,
				parent_run_id: index % 5 === 0 ? uuid(101) : null,
				error: index % 4 === 1 ? 'carrier API timed out' : null,
			}),
		),
	];

	const stepStatuses: StepStatus[] = ['succeeded', 'succeeded', 'succeeded', 'succeeded', 'failed'];
	const manySteps: WorkflowStep[] = Array.from({ length: 260 }, (_, index) => {
		const name = index < 40 ? (index === 0 ? 'verify-document' : `verify-document#${index + 1}`) : `step-${index}-${'with-a-rather-long-descriptive-name'.slice(0, (index % 4) * 9)}`;
		const status = index === 39 ? 'failed' : index > 255 ? 'retrying' : stepStatuses[index % 4]!;
		return stepRow(name, 'step', status, {
			attempts: index === 39 ? 25 : 1 + (index % 3),
			error: status === 'failed' ? panic : status === 'retrying' ? 'connection reset by peer' : null,
			wake_at: status === 'retrying' ? at(90_000 + index * 1000) : null,
			output: status === 'succeeded' ? (index === 3 ? hugeJSON(400) : JSON.stringify({ index })) : null,
			created_at: at(-(300 - index) * 1000 * 60),
			completed_at: status === 'succeeded' ? at(-(300 - index) * 1000 * 60 + 1_400) : null,
		});
	});
	manySteps.splice(
		1,
		0,
		stepRow('approval-from-the-compliance-officer-on-duty-for-the-emea-region', 'signal', 'timed_out', { signal: 'approval-from-the-compliance-officer-on-duty-for-the-emea-region' }),
		stepRow('ship', 'child', 'succeeded', { child_run_id: uuid(106) }),
		stepRow('ship#2', 'child', 'waiting', { child_run_id: uuid(999) }),
		stepRow('cooling-off', 'sleep', 'waiting', { wake_at: at(3 * 86_400_000) }),
	);

	const ancestors = Array.from({ length: 9 }, (_, index) => ({
		id: uuid(500 + index),
		workflow: index % 2 ? longWorkflow : 'tenant-provisioning',
		status: (['running', 'waiting'] as const)[index % 2]!,
		key: index === 0 ? longKey : null,
	}));

	const details = new Map<string, RunDetail>();
	details.set(
		uuid(101),
		detailFor(runs[0]!, manySteps, {
			run: { ...runs[0]!, input: hugeJSON(3_000), output: null },
			children: runs.filter((item) => item.parent_run_id === uuid(101)),
			child_total: 1_284,
			ancestors,
			signals: Array.from({ length: 50 }, (_, index) => ({
				id: index + 1,
				name: index % 2 ? 'approval-from-the-compliance-officer-on-duty-for-the-emea-region' : 'x',
				received: index % 3 !== 0,
				payload_preview: JSON.stringify({ by: 'Aleksandra Wiśniewska-Kowalczyk', approved: index % 2 === 0, reason: 'Looks good, but please double-check the beneficial ownership declaration' }).slice(0, 200),
				created_at: at(-index * minute),
			})),
		}),
	);
	for (const item of runs.slice(1)) {
		details.set(item.id, detailFor(item, item.step_count === 0 ? [] : manySteps.slice(0, 5)));
	}

	const treeNodes: RunTreeNode[] = [];
	let treeSeed = 10_000;
	function grow(id: string, parent: string | null, depth: number) {
		if (treeNodes.length >= 300) return;
		treeNodes.push({
			id,
			parent_run_id: parent,
			workflow: depth % 2 ? 'ship-item' : longWorkflow,
			version: depth,
			key: depth === 3 ? longKey : null,
			status: (['running', 'succeeded', 'failed', 'waiting', 'cancelled'] as const)[depth % 5]!,
			depth,
			created_at: at(-depth * minute),
			completed_at: null,
			child_count: depth < 12 ? 2 : 0,
		});
		if (depth < 12) {
			grow(uuid((treeSeed += 1)), id, depth + 1);
			grow(uuid((treeSeed += 1)), id, depth + 1);
		}
	}
	grow(uuid(101), null, 0);

	const locks: AdvisoryLock[] = Array.from({ length: 40 }, (_, index) => ({
		pid: 10_000 + index,
		mode: index % 3 ? 'ExclusiveLock' : 'ShareLock',
		granted: index % 4 !== 3,
		key: index === 0 ? '-9223372036854775808' : index === 1 ? null : String(9_223_372_036_854_775_807n - BigInt(index)),
		classid: 1_709_302 + index,
		objid: 991_223 + index,
		objsubid: index === 1 ? 2 : 1,
		application_name: index % 5 === 0 ? '' : 'northwind-platform-onboarding-worker-eu-west-1-production-canary',
		state: index % 2 ? 'idle in transaction (aborted)' : 'active',
		xact_start: index % 6 === 0 ? null : at(-index * 3 * minute),
		state_change: at(-index * minute),
		wait_event_type: index % 4 === 3 ? 'Lock' : null,
		wait_event: index % 4 === 3 ? 'advisory' : null,
	}));

	const workflows: WorkflowStats[] = [
		{ ...workflowStats(longWorkflow, runs, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]), total: 1_284_392, succeeded: 1_280_000, failed: 4_391, running: 1 },
		{ ...workflowStats('x', runs), total: 1, waiting: 1 },
		workflowStats('订单履行', runs),
		workflowStats('ship-item', runs),
		...Array.from({ length: 20 }, (_, index) => ({ ...workflowStats(`workflow-${index}`, runs), total: index })),
	];

	return {
		config: { mutations_enabled: false, workflows_enabled: true, page_size: 20 },
		queues,
		jobs,
		workflows,
		runs,
		details,
		trees: new Map([[uuid(101), { nodes: treeNodes, truncated: true }]]),
		locks,
		overview: (range) => {
			const base = overviewFrom(queues, jobs, runs, (index, count) =>
				index === count - 1 ? [4_200, 1, 3_900, 1_284, 7] : index % 9 === 0 ? [0, 0, 0, 0, 0] : [index * 3, index % 11 === 0 ? 400 : 0, index * 2, 0, 0],
			)(range);
			return {
				...base,
				queue: { ...base.queue, summary: { ...base.queue.summary, failed_jobs: 1, advisory_locks: 1 } },
				workflow: base.workflow ? { ...base.workflow, counts: { ...base.workflow.counts, failed: 4_391, total: 1_284_392 } } : null,
			};
		},
	};
}

export function emptyDataset(): Dataset {
	return {
		config: { mutations_enabled: true, workflows_enabled: true, page_size: 20 },
		queues: [],
		jobs: [],
		workflows: [],
		runs: [],
		details: new Map(),
		trees: new Map(),
		locks: [],
		overview: overviewFrom([], [], [], () => [0, 0, 0, 0, 0]),
	};
}

export function asJob(detail: QueueJobDetail): QueueJob {
	return {
		id: detail.id,
		queue_name: detail.queue_name,
		status: detail.status,
		attempts: detail.attempts,
		max_attempts: detail.max_attempts,
		claims: detail.claims,
		available_at: detail.available_at,
		claimed_by: detail.claimed_by,
		claimed_at: detail.claimed_at,
		done_at: detail.done_at,
		last_error: detail.last_error,
		payload_preview: detail.payload_preview,
		created_at: detail.created_at,
		updated_at: detail.updated_at,
	};
}
