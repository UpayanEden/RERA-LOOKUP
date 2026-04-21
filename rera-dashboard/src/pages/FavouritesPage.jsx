import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  getFavourites, removeFavourite, addFavourite,
  getProjects, getProjectFavourites, removeProjectFavourite,
} from "../api";
import toast from "react-hot-toast";
import StatusBadge from "../components/StatusBadge";
import BookingBar from "../components/BookingBar";

// ── Icons ─────────────────────────────────────────────────────────────────────
function PlusIcon()    { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2"><line x1="8" y1="3" x2="8" y2="13"/><line x1="3" y1="8" x2="13" y2="8"/></svg>; }
function TrashIcon()   { return <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><polyline points="3,5 13,5"/><path d="M5 5V3h6v2"/><path d="M4 5l1 9h6l1-9"/></svg>; }
function ChevRight()   { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><polyline points="6,3 10,8 6,13"/></svg>; }
function StarFilled()  { return <svg width="16" height="16" viewBox="0 0 16 16" fill="#F0997B" stroke="#F0997B" strokeWidth="1"><polygon points="8,2 10,6 14,6.5 11,9.5 11.8,14 8,12 4.2,14 5,9.5 2,6.5 6,6"/></svg>; }
function BuildingIcon(){ return <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><rect x="2" y="2" width="12" height="13" rx="1"/><line x1="6" y1="15" x2="6" y2="9"/><line x1="10" y1="15" x2="10" y2="9"/><rect x="6" y="9" width="4" height="6"/><line x1="5" y1="5" x2="5" y2="5.5"/><line x1="8" y1="5" x2="8" y2="5.5"/><line x1="11" y1="5" x2="11" y2="5.5"/></svg>; }

function DownloadIcon({ spinning }) {
  if (spinning) return (
    <svg className="animate-spin" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
      <path d="M21 12a9 9 0 11-6.219-8.56"/>
    </svg>
  );
  return (
    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
      <line x1="8" y1="2" x2="8" y2="11"/>
      <polyline points="5,8 8,11 11,8"/>
      <path d="M3 13h10"/>
    </svg>
  );
}

// ── Excel download ─────────────────────────────────────────────────────────────
async function downloadExcel(pincode) {
  const token   = localStorage.getItem("token");
  const baseUrl = import.meta.env.VITE_API_URL || "http://127.0.0.1:8000";
  const res     = await fetch(`${baseUrl}/favourites/${pincode}/export`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error("Export failed");
  const blob = await res.blob();
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href     = url;
  a.download = `RERA_${pincode}.xlsx`;
  a.click();
  URL.revokeObjectURL(url);
}

// ── Tab toggle ─────────────────────────────────────────────────────────────────
function TabButton({ active, onClick, children }) {
  return (
    <button
      onClick={onClick}
      className={`px-4 py-2 text-[13px] font-medium rounded-lg transition-colors ${
        active ? "bg-ink text-white" : "text-muted hover:text-ink hover:bg-surface"
      }`}
    >
      {children}
    </button>
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// PINCODE FAVOURITES
// ══════════════════════════════════════════════════════════════════════════════
function PincodeFavourites() {
  const navigate = useNavigate();
  const [favourites,  setFavourites]  = useState([]);
  const [loading,     setLoading]     = useState(true);
  const [addInput,    setAddInput]    = useState("");
  const [adding,      setAdding]      = useState(false);
  const [expanded,    setExpanded]    = useState(null);
  const [pinProjects, setPinProjects] = useState({});
  const [downloading, setDownloading] = useState({});

  const load = async () => {
    try {
      const { data } = await getFavourites();
      setFavourites(data.favourites);
    } catch { toast.error("Failed to load favourites"); }
    finally  { setLoading(false); }
  };
  useEffect(() => { load(); }, []);

  const handleAdd = async () => {
    const pin = addInput.trim();
    if (!/^\d{6}$/.test(pin)) { toast.error("Enter a valid 6-digit pincode"); return; }
    setAdding(true);
    try {
      await addFavourite(pin);
      toast.success(`Added ${pin}`);
      setAddInput("");
      load();
    } catch (err) { toast.error(err.response?.data?.detail || "Failed"); }
    finally { setAdding(false); }
  };

  const handleRemove = async (pin) => {
    try {
      await removeFavourite(pin);
      setFavourites(f => f.filter(x => x.pincode !== pin));
      toast.success(`Removed ${pin}`);
    } catch { toast.error("Failed"); }
  };

  const toggleExpand = async (pin) => {
    if (expanded === pin) { setExpanded(null); return; }
    setExpanded(pin);
    if (!pinProjects[pin]) {
      try {
        const { data } = await getProjects({ pincode: pin, limit: 100 });
        setPinProjects(p => ({ ...p, [pin]: data.results }));
      } catch { toast.error("Failed to load projects"); }
    }
  };

  const handleDownload = async (pin) => {
    setDownloading(d => ({ ...d, [pin]: true }));
    try {
      await downloadExcel(pin);
      toast.success(`Downloaded RERA_${pin}.xlsx`);
    } catch { toast.error("Download failed"); }
    finally { setDownloading(d => ({ ...d, [pin]: false })); }
  };

  return (
    <div className="space-y-3">
      {/* Add pincode */}
      <div className="card p-4 flex items-center gap-3">
        <input
          type="text" className="input flex-1 mono text-[13px]"
          placeholder="Enter 6-digit pincode..."
          maxLength={6} value={addInput}
          onChange={e => setAddInput(e.target.value.replace(/\D/, ""))}
          onKeyDown={e => e.key === "Enter" && handleAdd()}
        />
        <button onClick={handleAdd} disabled={adding} className="btn-primary shrink-0 text-[12px]">
          <PlusIcon /> Add pincode
        </button>
      </div>

      {loading ? (
        <div className="space-y-2">
          {[1,2,3].map(i => <div key={i} className="card p-5 h-20 animate-pulse bg-surface" />)}
        </div>
      ) : favourites.length === 0 ? (
        <div className="card p-12 text-center">
          <div className="w-10 h-10 rounded-full bg-surface flex items-center justify-center mx-auto mb-3">
            <StarFilled />
          </div>
          <p className="text-[13px] font-medium text-ink">No favourite pincodes yet</p>
          <p className="text-[12px] text-muted mt-1">Add a pincode above to start tracking</p>
        </div>
      ) : (
        favourites.map(fav => (
          <div key={fav.pincode} className="card overflow-hidden">
            {/* Summary row */}
            <div className="flex items-center gap-3 p-4">
              <div className="w-9 h-9 rounded-lg bg-[#FAEEDA] flex items-center justify-center shrink-0">
                <StarFilled />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <p className="text-[14px] font-semibold text-ink mono">{fav.pincode}</p>
                  <span className="badge bg-surface-hover text-muted">{fav.total_projects} projects</span>
                </div>
                <div className="flex flex-wrap gap-1 mt-1.5">
                  {fav.statuses?.slice(0, 3).map(s => <StatusBadge key={s} status={s} />)}
                  {fav.statuses?.length > 3 && (
                    <span className="badge bg-surface text-muted">+{fav.statuses.length - 3}</span>
                  )}
                </div>
              </div>

              {/* Action buttons */}
              <div className="flex items-center gap-1.5 shrink-0 flex-wrap">
                <button onClick={() => toggleExpand(fav.pincode)} className="btn text-[11px]">
                  <BuildingIcon />
                  {expanded === fav.pincode ? "Hide" : "View projects"}
                </button>
                <button onClick={() => navigate(`/projects?pincode=${fav.pincode}`)} className="btn text-[11px]">
                  <ChevRight />
                </button>
                {/* Excel download button */}
                <button
                  onClick={() => handleDownload(fav.pincode)}
                  disabled={downloading[fav.pincode]}
                  className="btn text-[11px] border-[#EAF3DE] bg-[#EAF3DE] text-[#3B6D11] hover:bg-[#C0DD97]"
                  title={`Download all projects in ${fav.pincode} as Excel`}
                >
                  <DownloadIcon spinning={downloading[fav.pincode]} />
                  {downloading[fav.pincode] ? "Downloading..." : "Export"}
                </button>
                <button onClick={() => handleRemove(fav.pincode)} className="btn-danger text-[11px]">
                  <TrashIcon />
                </button>
              </div>
            </div>

            {/* Expanded project list */}
            {expanded === fav.pincode && (
              <div className="border-t border-[#f0efeb]">
                {!pinProjects[fav.pincode] ? (
                  <div className="px-4 py-3 text-[12px] text-muted animate-pulse">Loading...</div>
                ) : pinProjects[fav.pincode].length === 0 ? (
                  <div className="px-4 py-3 text-[12px] text-muted">No projects found</div>
                ) : (
                  pinProjects[fav.pincode].map(p => (
                    <div
                      key={p.project_id}
                      onClick={() => navigate(`/projects/${p.project_id}`)}
                      className="flex items-center justify-between px-4 py-3 border-b border-[#f8f7f4] last:border-0 hover:bg-surface cursor-pointer transition-colors"
                    >
                      <div className="min-w-0 flex-1">
                        <p className="text-[12px] font-medium text-ink truncate">{p.project_name || "—"}</p>
                        <p className="text-[10px] text-muted truncate">{p.developer || "—"}</p>
                      </div>
                      <div className="flex items-center gap-3 shrink-0 ml-4">
                        <StatusBadge status={p.project_status} />
                        <BookingBar pct={p.booking_rate_pct} />
                        <ChevRight />
                      </div>
                    </div>
                  ))
                )}
              </div>
            )}
          </div>
        ))
      )}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// PROJECT FAVOURITES
// ══════════════════════════════════════════════════════════════════════════════
function ProjectFavourites() {
  const navigate = useNavigate();
  const [projects, setProjects] = useState([]);
  const [loading,  setLoading]  = useState(true);

  const load = async () => {
    try {
      const { data } = await getProjectFavourites();
      setProjects(data.favourites || []);
    } catch { toast.error("Failed to load project favourites"); }
    finally  { setLoading(false); }
  };
  useEffect(() => { load(); }, []);

  const handleRemove = async (projectId) => {
    try {
      await removeProjectFavourite(projectId);
      setProjects(p => p.filter(x => x.project_id !== projectId));
      toast.success("Removed from favourites");
    } catch { toast.error("Failed"); }
  };

  if (loading) return (
    <div className="space-y-2">
      {[1,2,3].map(i => <div key={i} className="card p-5 h-20 animate-pulse bg-surface" />)}
    </div>
  );

  if (projects.length === 0) return (
    <div className="card p-12 text-center">
      <div className="w-10 h-10 rounded-full bg-surface flex items-center justify-center mx-auto mb-3">
        <StarFilled />
      </div>
      <p className="text-[13px] font-medium text-ink">No favourite projects yet</p>
      <p className="text-[12px] text-muted mt-1">
        Star a project from the projects table or detail page
      </p>
    </div>
  );

  return (
    <div className="space-y-2">
      {projects.map(p => (
        <div
          key={p.project_id}
          className="card p-4 flex items-center gap-3 hover:bg-surface transition-colors cursor-pointer"
          onClick={() => navigate(`/projects/${p.project_id}`)}
        >
          <div className="w-9 h-9 rounded-lg bg-[#FAEEDA] flex items-center justify-center shrink-0">
            <StarFilled />
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-[13px] font-medium text-ink truncate">
              {p.project_name || "Unnamed project"}
            </p>
            <div className="flex items-center gap-2 mt-0.5 flex-wrap">
              <span className="text-[11px] text-muted">{p.developer || "—"}</span>
              <span className="badge bg-surface text-muted mono">{p.pincode}</span>
              <span className="text-[10px] text-muted">{p.district}</span>
            </div>
          </div>
          <div className="flex items-center gap-3 shrink-0">
            <StatusBadge status={p.project_status} />
            <BookingBar pct={p.booking_rate_pct} />
            <button
              onClick={e => { e.stopPropagation(); handleRemove(p.project_id); }}
              className="btn-danger text-[11px]"
            >
              <TrashIcon />
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════════════════════
// MAIN PAGE
// ══════════════════════════════════════════════════════════════════════════════
export default function FavouritesPage() {
  const [tab, setTab] = useState("pincodes");

  return (
    <div className="p-6 md:p-8 space-y-5 max-w-3xl">
      {/* Header */}
      <div>
        <h1 className="text-[18px] font-semibold text-ink tracking-tight">Favourites</h1>
        <p className="text-[12px] text-muted mt-0.5">
          Track projects by pincode or save individual projects
        </p>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-1 bg-surface rounded-xl p-1 w-fit">
        <TabButton active={tab === "pincodes"} onClick={() => setTab("pincodes")}>
          Pincodes
        </TabButton>
        <TabButton active={tab === "projects"} onClick={() => setTab("projects")}>
          Projects
        </TabButton>
      </div>

      {/* Content */}
      {tab === "pincodes" ? <PincodeFavourites /> : <ProjectFavourites />}
    </div>
  );
}