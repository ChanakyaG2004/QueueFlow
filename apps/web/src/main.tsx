import { FormEvent, StrictMode, useCallback, useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity, ArrowLeft, ArrowUpRight, Boxes, Check, CheckCircle2, ChevronRight,
  Clock3, Cpu, Database, KeyRound, Plus, RefreshCw, Server, Settings2, X, Zap,
} from "lucide-react";
import "./styles.css";

type JobStatus = "QUEUED" | "RUNNING" | "RETRYING" | "COMPLETED" | "FAILED" | "SUBMISSION_FAILED";
type Job = {
  id: string; type: string; status: JobStatus; progress: number; priority: number;
  created_at: string; cpu_required: number; memory_required_mb: number; gpu_required: number;
  attempt_count: number; max_attempts: number; worker_id?: string; result?: unknown;
  result_location?: string; error?: string; input_text?: string;
};
type Worker = {
  id: string; name: string; status: "IDLE" | "BUSY" | "OFFLINE"; healthy: boolean;
  cpu_capacity: number; memory_capacity_mb: number; gpu_capacity: number; current_job_id?: string;
  last_heartbeat: string;
};
type JobEvent = { id: string; event_type: string; message?: string; metadata?: Record<string, unknown>; created_at: string };
type View = "overview" | "jobs" | "workers";

const analyzeText = (text: string) => {
  const words = text.toLowerCase().match(/\b\w+\b/g) || [];
  const sentences = text.split(/[.!?]+/).filter(sentence => sentence.trim());
  const counts = new Map<string, number>();
  words.forEach(word => counts.set(word, (counts.get(word) || 0) + 1));

  return {
    word_count: words.length,
    character_count: text.length,
    sentence_count: sentences.length,
    top_words: [...counts.entries()].sort((left, right) => right[1] - left[1]).slice(0, 5),
  };
};

const now = Date.now();
const demoJobs: Job[] = [
  { id: "8f31a9c2-450f-4ab1-90cf-ae0dbe2a12f7", type: "text_analysis", status: "RUNNING", progress: 72, priority: 8, created_at: new Date(now - 14_000).toISOString(), cpu_required: 1, memory_required_mb: 256, gpu_required: 0, attempt_count: 1, max_attempts: 3, worker_id: "worker-east-02", input_text: "QueueFlow coordinates distributed jobs across a healthy pool of workers. Workers process jobs and report progress back to QueueFlow." },
  { id: "21dc44e7-9df1-44c3-8fb5-b54b87ef74df", type: "simulated_compute", status: "QUEUED", progress: 0, priority: 5, created_at: new Date(now - 31_000).toISOString(), cpu_required: 2, memory_required_mb: 512, gpu_required: 0, attempt_count: 0, max_attempts: 3 },
  { id: "c7a80b16-6d72-4939-bd07-cfbf48813af0", type: "text_analysis", status: "COMPLETED", progress: 100, priority: 3, created_at: new Date(now - 120_000).toISOString(), cpu_required: 1, memory_required_mb: 256, gpu_required: 0, attempt_count: 1, max_attempts: 3, worker_id: "worker-east-01", result: { word_count: 246, sentence_count: 18, character_count: 1528, top_words: [["queue", 12], ["worker", 9], ["job", 8]] }, result_location: "s3://queueflow-results/jobs/c7a80b16/result.json" },
  { id: "4ed239aa-002f-4d22-9c5d-82d28df61a70", type: "simulated_compute", status: "COMPLETED", progress: 100, priority: 2, created_at: new Date(now - 360_000).toISOString(), cpu_required: 2, memory_required_mb: 1024, gpu_required: 0, attempt_count: 1, max_attempts: 3, worker_id: "worker-east-02", result: { message: "Simulated compute job completed" } },
];

const demoWorkers: Worker[] = [
  { id: "worker-east-01", name: "worker-east-01", status: "IDLE", healthy: true, cpu_capacity: 4, memory_capacity_mb: 8192, gpu_capacity: 0, last_heartbeat: new Date(now - 3_000).toISOString() },
  { id: "worker-east-02", name: "worker-east-02", status: "BUSY", healthy: true, cpu_capacity: 4, memory_capacity_mb: 8192, gpu_capacity: 0, current_job_id: demoJobs[0].id, last_heartbeat: new Date(now - 2_000).toISOString() },
];

const eventLabel: Record<string, string> = { SUBMITTED: "Job submitted", STARTED: "Worker started execution", COMPLETED: "Result stored successfully", RETRYING: "Retry scheduled", FAILED: "Execution failed" };
const relativeTime = (date: string) => {
  const seconds = Math.max(1, Math.floor((Date.now() - new Date(date).getTime()) / 1000));
  if (seconds < 60) return `${seconds} sec ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  return `${Math.floor(seconds / 3600)} hr ago`;
};
const shortId = (id: string) => `j_${id.slice(0, 8)}`;

function App() {
  const [view, setView] = useState<View>("overview");
  const [jobs, setJobs] = useState<Job[]>(demoJobs);
  const [workers, setWorkers] = useState<Worker[]>(demoWorkers);
  const [selected, setSelected] = useState<Job | null>(null);
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [showNew, setShowNew] = useState(false);
  const [showConnect, setShowConnect] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [notice, setNotice] = useState("");
  const [mode, setMode] = useState<"demo" | "live">(() => localStorage.getItem("qf-mode") === "live" ? "live" : "demo");
  const [apiUrl, setApiUrl] = useState(() => localStorage.getItem("qf-api") || "/api");
  const [apiKey, setApiKey] = useState(() => localStorage.getItem("qf-key") || "queueflow-demo-key");

  const request = useCallback(async <T,>(path: string, init?: RequestInit): Promise<T> => {
    const response = await fetch(`${apiUrl.replace(/\/$/, "")}${path}`, { ...init, headers: { "Content-Type": "application/json", "x-api-key": apiKey, ...init?.headers } });
    if (!response.ok) { const body = await response.json().catch(() => ({})); throw new Error(body.error || `Request failed (${response.status})`); }
    return response.json();
  }, [apiKey, apiUrl]);

  const refresh = useCallback(async (quiet = false) => {
    if (mode === "demo") return;
    if (!quiet) setRefreshing(true);
    try {
      const [nextJobs, nextWorkers] = await Promise.all([request<Job[]>("/jobs"), request<Worker[]>("/workers")]);
      setJobs(nextJobs); setWorkers(nextWorkers);
    } catch (error) {
      if (!quiet) setNotice(error instanceof Error ? error.message : "Could not reach QueueFlow");
    } finally { setRefreshing(false); }
  }, [mode, request]);

  useEffect(() => { refresh(true); const timer = window.setInterval(() => refresh(true), 4000); return () => clearInterval(timer); }, [refresh]);
  useEffect(() => { if (!notice) return; const timer = window.setTimeout(() => setNotice(""), 3500); return () => clearTimeout(timer); }, [notice]);
  useEffect(() => {
    if (mode !== "demo") return;
    const timer = window.setInterval(() => setJobs(current => current.map(job => {
      if (job.status !== "RUNNING") return job;
      const progress = Math.min(100, job.progress + 2);
      const result = progress === 100
        ? job.type === "text_analysis" ? analyzeText(job.input_text || "") : { message: "Demo job completed successfully" }
        : job.result;
      return { ...job, progress, status: progress === 100 ? "COMPLETED" : "RUNNING", result };
    })), 1800);
    return () => clearInterval(timer);
  }, [mode]);
  useEffect(() => {
    if (mode !== "demo") return;
    setWorkers(current => current.map(worker => {
      const activeJob = jobs.find(job => job.status === "RUNNING" && job.worker_id === worker.id);
      return {
        ...worker,
        status: activeJob ? "BUSY" : "IDLE",
        current_job_id: activeJob?.id,
        last_heartbeat: new Date().toISOString(),
      };
    }));
  }, [jobs, mode]);
  useEffect(() => {
    setSelected(current => current ? jobs.find(job => job.id === current.id) || current : null);
  }, [jobs]);

  const openJob = async (job: Job) => {
    setSelected(job);
    if (mode === "live") {
      try { setEvents(await request<JobEvent[]>(`/jobs/${job.id}/events`)); } catch { setEvents([]); }
    } else {
      const base = new Date(job.created_at).getTime();
      const items: JobEvent[] = [{ id: "e1", event_type: "SUBMITTED", message: "Job submitted to QueueFlow", created_at: new Date(base).toISOString(), metadata: { priority: job.priority, cpu: job.cpu_required, memoryMb: job.memory_required_mb } }];
      if (job.status !== "QUEUED") items.push({ id: "e2", event_type: "STARTED", created_at: new Date(base + 2500).toISOString(), metadata: { attempt: 1, worker_id: job.worker_id } });
      if (job.status === "COMPLETED") items.push({ id: "e3", event_type: "COMPLETED", created_at: new Date(base + 8200).toISOString(), metadata: { result_location: job.result_location } });
      setEvents(items);
    }
  };

  const submitJob = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault(); setSubmitting(true);
    const data = new FormData(event.currentTarget);
    const payload = { type: data.get("type"), text: data.get("text"), cpu: Number(data.get("cpu")), memoryMb: Number(data.get("memory")), gpu: 0, priority: Number(data.get("priority")) };
    try {
      if (mode === "live") {
        const job = await request<Job>("/jobs", { method: "POST", body: JSON.stringify(payload) });
        setJobs(current => [job, ...current]);
      } else {
        const availableWorker = workers.find(worker => worker.healthy && worker.status === "IDLE");
        const job: Job = { id: crypto.randomUUID(), type: String(payload.type), status: availableWorker ? "RUNNING" : "QUEUED", progress: availableWorker ? 8 : 0, priority: payload.priority, created_at: new Date().toISOString(), cpu_required: payload.cpu, memory_required_mb: payload.memoryMb, gpu_required: 0, attempt_count: availableWorker ? 1 : 0, max_attempts: 3, worker_id: availableWorker?.id, input_text: String(payload.text || "") };
        setJobs(current => [job, ...current]);
      }
      setShowNew(false); setNotice("Job submitted successfully");
    } catch (error) { setNotice(error instanceof Error ? error.message : "Could not submit job"); }
    finally { setSubmitting(false); }
  };

  const saveConnection = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    localStorage.setItem("qf-api", apiUrl); localStorage.setItem("qf-key", apiKey); localStorage.setItem("qf-mode", mode);
    if (mode === "demo") { setJobs(demoJobs); setWorkers(demoWorkers); }
    setShowConnect(false); setNotice(mode === "live" ? "Live API settings saved" : "Sample data restored");
    window.setTimeout(() => refresh(), 0);
  };

  const stats = useMemo(() => ({
    active: jobs.filter(job => ["QUEUED", "RUNNING", "RETRYING"].includes(job.status)).length,
    completed: jobs.filter(job => job.status === "COMPLETED").length,
    success: jobs.length ? (jobs.filter(job => job.status === "COMPLETED").length / Math.max(1, jobs.filter(job => ["COMPLETED", "FAILED"].includes(job.status)).length) * 100) : 100,
  }), [jobs]);
  const visibleJobs = view === "overview" ? jobs.slice(0, 4) : jobs;

  return <div className="app-shell">
    <aside className="sidebar">
      <button className="brand" onClick={() => setView("overview")}><span className="brand-mark"><span /></span>QueueFlow</button>
      <nav>
        <button className={view === "overview" ? "active" : ""} onClick={() => setView("overview")}><Boxes size={18} />Overview</button>
        <button className={view === "jobs" ? "active" : ""} onClick={() => setView("jobs")}><Activity size={18} />Jobs <span className="nav-count">{jobs.length}</span></button>
        <button className={view === "workers" ? "active" : ""} onClick={() => setView("workers")}><Server size={18} />Workers</button>
      </nav>
      <div className="sidebar-bottom">
        <div className="cluster-card"><div><span className="pulse" />System operational</div><p>{workers.filter(worker => worker.healthy).length} workers connected</p></div>
        <button className="user-chip" onClick={() => setShowConnect(true)}><span>CD</span><div><strong>Demo workspace</strong><small>{mode === "demo" ? "Sample data" : "Live API"}</small></div><Settings2 size={14} /></button>
      </div>
    </aside>

    <main>
      <header>
        <div><p className="eyebrow">THURSDAY, AUG 20</p><h1>{view === "overview" ? <>Good morning, <em>Chanakya.</em></> : view === "jobs" ? <>Your <em>jobs.</em></> : <>Compute <em>workers.</em></>}</h1></div>
        <div className="header-actions"><button className="mode-pill" onClick={() => setShowConnect(true)}><span className={mode} />{mode === "demo" ? "Demo mode" : "Live API"}</button>{view !== "workers" && <button className="primary" onClick={() => setShowNew(true)}><Plus size={18} />New job</button>}</div>
      </header>

      {view === "overview" && <section className="hero-grid">
        <article className="lead-card"><div><span className="live-pill"><span />LIVE</span><h2>Your queue is<br /><em>flowing smoothly.</em></h2><p>{stats.active === 1 ? "One job is active" : `${stats.active} jobs are active`} and {workers.filter(w => w.status === "IDLE").length} workers are ready for more.</p></div><div className="orbital" aria-hidden="true"><div className="orbit orbit-one"><i /></div><div className="orbit orbit-two"><i /></div><div className="core"><Activity size={31} /></div></div></article>
        <div className="metrics">
          <article><div className="metric-icon lime"><Activity size={19} /></div><p>Active jobs</p><strong>{String(stats.active).padStart(2, "0")}</strong><small><b>{jobs.filter(j => j.status === "RUNNING").length} running</b> right now</small></article>
          <article><div className="metric-icon"><CheckCircle2 size={19} /></div><p>Success rate</p><strong>{stats.success.toFixed(1)}<span>%</span></strong><small>Across {stats.completed} completed jobs</small></article>
          <article><div className="metric-icon"><Clock3 size={19} /></div><p>Avg. runtime</p><strong>8.2<span>s</span></strong><small><b>↓ 12%</b> this week</small></article>
        </div>
      </section>}

      {(view === "overview" || view === "jobs") && <section className={`section ${view === "jobs" ? "section-first" : ""}`}>
        <div className="section-title"><div><p className="eyebrow">{view === "overview" ? "LATEST ACTIVITY" : "EXECUTION HISTORY"}</p><h3>{view === "overview" ? "Recent jobs" : `${jobs.length} total jobs`}</h3></div><div className="section-tools"><button className="icon-button" aria-label="Refresh jobs" onClick={() => refresh()}><RefreshCw size={15} className={refreshing ? "spin" : ""} /></button>{view === "overview" && <button className="text-button" onClick={() => setView("jobs")}>View all <ArrowUpRight size={16} /></button>}</div></div>
        <JobTable jobs={visibleJobs} openJob={openJob} />
      </section>}

      {(view === "overview" || view === "workers") && <section className={`section workers ${view === "workers" ? "section-first" : ""}`}>
        <div className="section-title"><div><p className="eyebrow">COMPUTE POOL</p><h3>{view === "workers" ? `${workers.length} registered workers` : "Workers"}</h3></div><span className="updated"><span className="pulse" />Updated just now</span></div>
        <div className="worker-grid">{workers.map(worker => <article key={worker.id}><div className="worker-top"><span className={`worker-symbol ${worker.status === "BUSY" ? "active" : ""}`}><Server size={19} /></span><span className={`status ${worker.status.toLowerCase()}`}><i />{worker.healthy ? worker.status : "OFFLINE"}</span></div><h4>{worker.name}</h4><p>{worker.status === "BUSY" ? `Processing ${shortId(worker.current_job_id || "")}` : "Ready to accept a job"}</p><div className="capacity"><span>CPU <b>{worker.cpu_capacity} cores</b></span><span>MEMORY <b>{worker.memory_capacity_mb / 1024} GB</b></span><span>GPU <b>{worker.gpu_capacity}</b></span></div></article>)}</div>
      </section>}
    </main>

    {showNew && <div className="modal-backdrop" onMouseDown={() => setShowNew(false)}><div className="modal" onMouseDown={event => event.stopPropagation()}><button className="close-button" onClick={() => setShowNew(false)} aria-label="Close"><X size={18} /></button><span className="modal-icon"><Zap size={19} /></span><p className="eyebrow">CREATE WORKLOAD</p><h2>Submit a new job</h2><p className="modal-copy">Define the work and QueueFlow will match it to an available worker.</p><form onSubmit={submitJob}><label>Job type<select name="type" defaultValue="text_analysis"><option value="text_analysis">Text analysis</option><option value="simulated_compute">Simulated compute</option><option value="always_fail">Intentional failure</option></select></label><label>Input text<textarea name="text" rows={4} defaultValue="QueueFlow makes distributed work easier to see and manage." /></label><div className="form-grid"><label>CPU cores<input name="cpu" type="number" min="1" max="8" defaultValue="1" /></label><label>Memory (MB)<input name="memory" type="number" min="128" step="128" defaultValue="256" /></label><label>Priority<input name="priority" type="number" min="0" max="10" defaultValue="5" /></label></div><button className="primary submit" disabled={submitting}>{submitting ? "Submitting…" : <><Zap size={16} />Submit job</>}</button></form></div></div>}

    {showConnect && <div className="modal-backdrop" onMouseDown={() => setShowConnect(false)}><div className="modal compact" onMouseDown={event => event.stopPropagation()}><button className="close-button" onClick={() => setShowConnect(false)} aria-label="Close"><X size={18} /></button><span className="modal-icon"><Database size={19} /></span><p className="eyebrow">DATA SOURCE</p><h2>Connect QueueFlow</h2><p className="modal-copy">Use sample data for a quick demo, or point the dashboard at your running API.</p><form onSubmit={saveConnection}><div className="mode-options"><button type="button" className={mode === "demo" ? "selected" : ""} onClick={() => setMode("demo")}><Check size={15} />Sample data<small>No services required</small></button><button type="button" className={mode === "live" ? "selected" : ""} onClick={() => setMode("live")}><Activity size={15} />Live API<small>Refreshes every 4 seconds</small></button></div>{mode === "live" && <><label>API URL<input value={apiUrl} onChange={e => setApiUrl(e.target.value)} placeholder="http://localhost:3001" /></label><label>API key<div className="input-icon"><KeyRound size={15} /><input type="password" value={apiKey} onChange={e => setApiKey(e.target.value)} /></div></label></>}<button className="primary submit">Save connection</button></form></div></div>}

    {selected && <div className="drawer-backdrop" onMouseDown={() => setSelected(null)}><aside className="drawer" onMouseDown={event => event.stopPropagation()}><div className="drawer-head"><button className="back-button" onClick={() => setSelected(null)}><ArrowLeft size={17} />Back to jobs</button><button className="close-button static" onClick={() => setSelected(null)}><X size={18} /></button></div><div className="drawer-title"><span className="worker-symbol active"><Cpu size={19} /></span><div><p className="eyebrow">JOB DETAIL</p><h2>{selected.type.replace("_", " ")}</h2><code>{shortId(selected.id)}</code></div></div><div className="detail-summary"><span>Status<b className={`status ${selected.status.toLowerCase()}`}><i />{selected.status}</b></span><span>Progress<b>{selected.progress}%</b></span><span>Attempt<b>{selected.attempt_count} / {selected.max_attempts}</b></span></div><h3>Execution timeline</h3><div className="timeline">{events.map((item, index) => <div className="event" key={item.id}><span className={`event-dot ${item.event_type.toLowerCase()}`}>{item.event_type === "COMPLETED" ? <Check size={13} /> : index + 1}</span><div><strong>{eventLabel[item.event_type] || item.event_type.replace("_", " ")}</strong><p>{item.message || relativeTime(item.created_at)}</p>{item.metadata && <code>{JSON.stringify(item.metadata, null, 2)}</code>}</div></div>)}</div>{selected.result != null && <div className="result-card"><div><CheckCircle2 size={17} /><h3>Result</h3></div><pre>{JSON.stringify(selected.result, null, 2)}</pre>{selected.result_location && <small>{selected.result_location}</small>}</div>}{selected.error && <div className="error-card"><strong>Execution error</strong><p>{selected.error}</p></div>}</aside></div>}
    {notice && <div className="toast"><CheckCircle2 size={16} />{notice}</div>}
  </div>;
}

function JobTable({ jobs, openJob }: { jobs: Job[]; openJob: (job: Job) => void }) {
  return <div className="job-table"><div className="table-head"><span>Job</span><span>Status</span><span>Progress</span><span>Priority</span><span>Created</span><span /></div>{jobs.length === 0 ? <div className="empty"><Boxes size={24} /><strong>No jobs yet</strong><p>Submit a job to start the queue.</p></div> : jobs.map(job => <button className="job-row" key={job.id} onClick={() => openJob(job)}><span className="job-name"><i className={job.status === "RUNNING" ? "running-icon" : ""}><Cpu size={16} /></i><span><b>{job.type.replace("_", " ")}</b><small>{shortId(job.id)}</small></span></span><span><b className={`status ${job.status.toLowerCase()}`}><i />{job.status}</b></span><span className="progress-wrap"><span className="progress"><i style={{ width: `${job.progress}%` }} /></span><small>{job.progress}%</small></span><span><b className="priority">P{job.priority}</b></span><span className="time">{relativeTime(job.created_at)}</span><ChevronRight size={15} className="row-arrow" /></button>)}</div>;
}

createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
