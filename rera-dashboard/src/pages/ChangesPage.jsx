import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { getChanges, getChangesSummary } from "../api";
import toast from "react-hot-toast";

function TrendIcon() { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><polyline points="1,11 5,7 8,9 11,4 15,6"/></svg>; }
function ChevLeft() { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><polyline points="10,3 6,8 10,13"/></svg>; }
function ChevRight() { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><polyline points="6,3 10,8 6,13"/></svg>; }

const FIELD_COLORS = {
  apartments_booked:  "bg-[#E6F1FB] text-[#185FA5]",
  project_status:     "bg-[#EEEDFE] text-[#534AB7]",
  booking_rate_pct:   "bg-[#EAF3DE] text-[#3B6D11]",
  unsold_units:       "bg-[#FCEBEB] text-[#A32D2D]",
  construction_status_summary: "bg-[#FAEEDA] text-[#854F0B]",
};

function FieldTag({ field }) {
  const cls = FIELD_COLORS[field] || "bg-[#f0efeb] text-[#666]";
  return <span className={`badge ${cls}`}>{field.replaceAll("_", " ")}</span>;
}

function ValueDiff({ oldVal, newVal }) {
  const fmt = (v) => (v == null ? "—" : String(v));
  return (
    <span className="flex items-center gap-1.5 text-[12px]">
      <span className="text-[#A32D2D] line-through">{fmt(oldVal)}</span>
      <span className="text-muted">→</span>
      <span className="text-[#3B6D11] font-medium">{fmt(newVal)}</span>
    </span>
  );
}

export default function ChangesPage() {
  const navigate = useNavigate();
  const [changes, setChanges] = useState({ results: [], total: 0, pages: 1 });
  const [summary, setSummary] = useState(null);
  const [page, setPage]       = useState(1);
  const [pincode, setPincode] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => { getChangesSummary().then(({ data }) => setSummary(data)).catch(() => {}); }, []);

  useEffect(() => {
    setLoading(true);
    const params = { page, limit: 50 };
    if (pincode) params.pincode = pincode;
    getChanges(params)
      .then(({ data }) => setChanges(data))
      .catch(() => toast.error("Failed to load changes"))
      .finally(() => setLoading(false));
  }, [page, pincode]);

  return (
    <div className="p-6 md:p-8 space-y-5 max-w-5xl">
      <div>
        <h1 className="text-[18px] font-semibold text-ink tracking-tight">Changes</h1>
        <p className="text-[12px] text-muted mt-0.5">Field-level diff log from every scrape run</p>
      </div>

      {/* 7-day summary */}
      {summary && (
        <div className="card p-5">
          <div className="flex items-center gap-2 mb-4">
            <TrendIcon />
            <p className="text-[11px] font-semibold text-muted uppercase tracking-wide">Last 7 days</p>
          </div>
          <div className="grid grid-cols-3 gap-4 mb-4">
            <div>
              <p className="text-[20px] font-semibold text-ink tracking-tight">{summary.total_changes.toLocaleString("en-IN")}</p>
              <p className="text-[11px] text-muted mt-0.5">Total changes</p>
            </div>
            <div>
              <p className="text-[20px] font-semibold text-[#185FA5] tracking-tight">{summary.total_projects_affected}</p>
              <p className="text-[11px] text-muted mt-0.5">Projects affected</p>
            </div>
            <div>
              <p className="text-[20px] font-semibold text-[#3B6D11] tracking-tight">{summary.by_field?.length ?? 0}</p>
              <p className="text-[11px] text-muted mt-0.5">Fields changed</p>
            </div>
          </div>
          {summary.by_field?.length > 0 && (
            <div className="flex flex-wrap gap-2 pt-3 border-t border-[#f0efeb]">
              {summary.by_field.slice(0, 8).map((f) => (
                <div key={f.field} className="flex items-center gap-1.5">
                  <FieldTag field={f.field} />
                  <span className="text-[10px] text-muted">×{f.change_count}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Filter + count */}
      <div className="flex items-center gap-3">
        <input
          type="text" className="input max-w-[180px] mono text-[12px]"
          placeholder="Filter by pincode..."
          maxLength={6} value={pincode}
          onChange={(e) => { setPincode(e.target.value.replace(/\D/, "")); setPage(1); }}
        />
        {pincode && (
          <button onClick={() => { setPincode(""); setPage(1); }} className="btn text-[11px]">Clear</button>
        )}
        <p className="text-[12px] text-muted ml-auto">{changes.total.toLocaleString("en-IN")} changes</p>
      </div>

      {/* Feed */}
      <div className="card divide-y divide-[#f8f7f4]">
        {loading ? (
          Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="px-5 py-3.5 flex gap-3 animate-pulse">
              <div className="h-3 bg-surface rounded flex-1" />
            </div>
          ))
        ) : changes.results.length === 0 ? (
          <div className="py-16 text-center">
            <p className="text-[13px] text-muted">No changes recorded yet</p>
            <p className="text-[11px] text-muted/60 mt-1">Changes appear after the second scrape run</p>
          </div>
        ) : (
          changes.results.map((c) => (
            <div
              key={c._id}
              onClick={() => navigate(`/projects/${c.project_id}`)}
              className="px-5 py-3.5 flex flex-wrap items-start gap-x-4 gap-y-1.5 hover:bg-surface cursor-pointer transition-colors"
            >
              <div className="min-w-0 flex-1">
                <p className="text-[12px] font-medium text-ink truncate">
                  {c.project_name || c.project_id}
                </p>
                <div className="flex flex-wrap items-center gap-2 mt-1">
                  {c.pincode && <span className="text-[10px] text-muted mono">{c.pincode}</span>}
                  <FieldTag field={c.field} />
                  <ValueDiff oldVal={c.old_value} newVal={c.new_value} />
                </div>
              </div>
              <p className="text-[10px] text-muted shrink-0 mt-0.5">
                {new Date(c.changed_at).toLocaleString("en-IN", {
                  day: "2-digit", month: "short",
                  hour: "2-digit", minute: "2-digit",
                })}
              </p>
            </div>
          ))
        )}
      </div>

      {/* Pagination */}
      {changes.pages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-[11px] text-muted">Page {page} of {changes.pages}</p>
          <div className="flex gap-1.5">
            <button onClick={() => setPage(page - 1)} disabled={page <= 1} className="btn px-2 py-1 disabled:opacity-30"><ChevLeft /></button>
            <button onClick={() => setPage(page + 1)} disabled={page >= changes.pages} className="btn px-2 py-1 disabled:opacity-30"><ChevRight /></button>
          </div>
        </div>
      )}
    </div>
  );
}