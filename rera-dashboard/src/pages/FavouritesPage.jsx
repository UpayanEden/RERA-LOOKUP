import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { getFavourites, removeFavourite, addFavourite, getProjects } from "../api";
import toast from "react-hot-toast";
import StatusBadge from "../components/StatusBadge";
import BookingBar from "../components/BookingBar";

function PlusIcon() { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2"><line x1="8" y1="3" x2="8" y2="13"/><line x1="3" y1="8" x2="13" y2="8"/></svg>; }
function TrashIcon() { return <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><polyline points="3,5 13,5"/><path d="M5 5V3h6v2"/><path d="M4 5l1 9h6l1-9"/></svg>; }
function ChevronRight() { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><polyline points="6,3 10,8 6,13"/></svg>; }
function StarFilledIcon() { return <svg width="16" height="16" viewBox="0 0 16 16" fill="#F0997B" stroke="#F0997B" strokeWidth="1"><polygon points="8,2 10,6 14,6.5 11,9.5 11.8,14 8,12 4.2,14 5,9.5 2,6.5 6,6"/></svg>; }

export default function FavouritesPage() {
  const navigate = useNavigate();
  const [favourites, setFavourites] = useState([]);
  const [loading, setLoading]       = useState(true);
  const [addInput, setAddInput]     = useState("");
  const [adding, setAdding]         = useState(false);
  const [expanded, setExpanded]     = useState(null);
  const [pinProjects, setPinProjects] = useState({});

  const load = async () => {
    try { const { data } = await getFavourites(); setFavourites(data.favourites); }
    catch { toast.error("Failed to load favourites"); }
    finally { setLoading(false); }
  };
  useEffect(() => { load(); }, []);

  const handleAdd = async () => {
    const pin = addInput.trim();
    if (!/^\d{6}$/.test(pin)) { toast.error("Enter a valid 6-digit pincode"); return; }
    setAdding(true);
    try { await addFavourite(pin); toast.success(`Added ${pin}`); setAddInput(""); load(); }
    catch (err) { toast.error(err.response?.data?.detail || "Failed"); }
    finally { setAdding(false); }
  };

  const handleRemove = async (pin) => {
    try { await removeFavourite(pin); setFavourites((f) => f.filter((x) => x.pincode !== pin)); toast.success(`Removed ${pin}`); }
    catch { toast.error("Failed"); }
  };

  const toggleExpand = async (pin) => {
    if (expanded === pin) { setExpanded(null); return; }
    setExpanded(pin);
    if (!pinProjects[pin]) {
      try { const { data } = await getProjects({ pincode: pin, limit: 50 }); setPinProjects((p) => ({ ...p, [pin]: data.results })); }
      catch { toast.error("Failed to load projects"); }
    }
  };

  return (
    <div className="p-6 md:p-8 space-y-5 max-w-3xl">
      <div>
        <h1 className="text-[18px] font-semibold text-ink tracking-tight">Favourites</h1>
        <p className="text-[12px] text-muted mt-0.5">Track projects by pincode</p>
      </div>

      {/* Add */}
      <div className="card p-4 flex items-center gap-3">
        <input
          type="text" className="input flex-1 mono text-[13px]"
          placeholder="Enter 6-digit pincode..."
          maxLength={6} value={addInput}
          onChange={(e) => setAddInput(e.target.value.replace(/\D/, ""))}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
        />
        <button onClick={handleAdd} disabled={adding} className="btn-primary shrink-0 text-[12px]">
          <PlusIcon /> Add pincode
        </button>
      </div>

      {/* List */}
      {loading ? (
        <div className="space-y-2">
          {[1,2,3].map((i) => <div key={i} className="card p-5 h-20 animate-pulse bg-surface" />)}
        </div>
      ) : favourites.length === 0 ? (
        <div className="card p-12 text-center">
          <div className="w-10 h-10 rounded-full bg-surface flex items-center justify-center mx-auto mb-3">
            <StarFilledIcon />
          </div>
          <p className="text-[13px] font-medium text-ink">No favourite pincodes yet</p>
          <p className="text-[12px] text-muted mt-1">Add a pincode above to start tracking</p>
        </div>
      ) : (
        <div className="space-y-2">
          {favourites.map((fav) => (
            <div key={fav.pincode} className="card overflow-hidden">
              <div className="flex items-center gap-3 p-4">
                <div className="w-9 h-9 rounded-lg bg-[#FAEEDA] flex items-center justify-center shrink-0">
                  <StarFilledIcon />
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <p className="text-[14px] font-semibold text-ink mono">{fav.pincode}</p>
                    <span className="badge bg-surface-hover text-muted">{fav.total_projects} projects</span>
                  </div>
                  <div className="flex flex-wrap gap-1 mt-1.5">
                    {fav.statuses?.slice(0, 3).map((s) => <StatusBadge key={s} status={s} />)}
                    {fav.statuses?.length > 3 && (
                      <span className="badge bg-surface text-muted">+{fav.statuses.length - 3}</span>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-1.5 shrink-0">
                  <button onClick={() => toggleExpand(fav.pincode)} className="btn text-[11px]">
                    {expanded === fav.pincode ? "Hide" : "View projects"}
                  </button>
                  <button onClick={() => navigate(`/projects?pincode=${fav.pincode}`)} className="btn text-[11px]">
                    <ChevronRight />
                  </button>
                  <button onClick={() => handleRemove(fav.pincode)} className="btn-danger text-[11px]">
                    <TrashIcon />
                  </button>
                </div>
              </div>

              {expanded === fav.pincode && (
                <div className="border-t border-[#f0efeb]">
                  {!pinProjects[fav.pincode] ? (
                    <div className="px-4 py-3 text-[12px] text-muted animate-pulse">Loading...</div>
                  ) : pinProjects[fav.pincode].length === 0 ? (
                    <div className="px-4 py-3 text-[12px] text-muted">No projects found</div>
                  ) : (
                    pinProjects[fav.pincode].map((p) => (
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
                          <ChevronRight />
                        </div>
                      </div>
                    ))
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}