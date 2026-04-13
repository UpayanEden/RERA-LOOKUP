import { useEffect, useState, useCallback } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { getProjects, getFilters, addFavourite, removeFavourite, getFavourites } from "../api";
import toast from "react-hot-toast";
import StatusBadge from "../components/StatusBadge";
import BookingBar from "../components/BookingBar";

const LIMIT = 20;

const COLS = [
  { key: "project_name",     label: "Project",       sortable: true  },
  { key: "developer",        label: "Developer",     sortable: true  },
  { key: "pincode",          label: "Pincode",       sortable: true  },
  { key: "district",         label: "District",      sortable: true  },
  { key: "project_type",     label: "Type",          sortable: false },
  { key: "project_status",   label: "Status",        sortable: true  },
  { key: "total_apartments", label: "Units",         sortable: true  },
  { key: "apartments_booked",label: "Booked",        sortable: true  },
  { key: "booking_rate_pct", label: "Booking rate",  sortable: true  },
];

function ChevronUp() { return <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor"><path d="M5 2l4 5H1z"/></svg>; }
function ChevronDown() { return <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor"><path d="M5 8L1 3h8z"/></svg>; }
function SearchIcon() { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><circle cx="7" cy="7" r="4.5"/><line x1="10.5" y1="10.5" x2="14" y2="14"/></svg>; }
function FilterIcon() { return <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5"><line x1="2" y1="5" x2="14" y2="5"/><line x1="4" y1="9" x2="12" y2="9"/><line x1="6" y1="13" x2="10" y2="13"/></svg>; }
function StarIcon({ filled }) {
  return <svg width="14" height="14" viewBox="0 0 16 16" fill={filled ? "#F0997B" : "none"} stroke={filled ? "#F0997B" : "#ccc"} strokeWidth="1.5">
    <polygon points="8,2 10,6 14,6.5 11,9.5 11.8,14 8,12 4.2,14 5,9.5 2,6.5 6,6"/>
  </svg>;
}

function StatCard({ label, value, sub, subColor }) {
  return (
    <div className="stat-card">
      <p className="text-[11px] font-medium text-muted uppercase tracking-wide mb-1.5">{label}</p>
      <p className="text-[22px] font-semibold text-ink tracking-tight leading-none">{value}</p>
      {sub && <p className={`text-[11px] mt-1.5 ${subColor || "text-muted"}`}>{sub}</p>}
    </div>
  );
}

export default function ProjectsPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [data, setData]       = useState({ results: [], total: 0, pages: 1 });
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({ pincodes: [], districts: [], statuses: [] });
  const [favPins, setFavPins] = useState(new Set());
  const [showFilters, setShowFilters] = useState(false);

  const page     = parseInt(searchParams.get("page") || "1");
  const search   = searchParams.get("search") || "";
  const pincode  = searchParams.get("pincode") || "";
  const district = searchParams.get("district") || "";
  const status   = searchParams.get("status") || "";
  const sortKey  = searchParams.get("sort") || "";
  const sortDir  = searchParams.get("dir") || "asc";

  const setParam = (key, val) => {
    const p = new URLSearchParams(searchParams);
    if (val) p.set(key, val); else p.delete(key);
    p.set("page", "1");
    setSearchParams(p);
  };
  const setPage = (n) => {
    const p = new URLSearchParams(searchParams);
    p.set("page", String(n));
    setSearchParams(p);
  };
  const toggleSort = (key) => {
    const p = new URLSearchParams(searchParams);
    if (sortKey === key) p.set("dir", sortDir === "asc" ? "desc" : "asc");
    else { p.set("sort", key); p.set("dir", "asc"); }
    setSearchParams(p);
  };

  useEffect(() => {
    getFilters().then(({ data }) => setFilters(data)).catch(() => {});
    getFavourites().then(({ data }) => setFavPins(new Set(data.favourites.map((f) => f.pincode)))).catch(() => {});
  }, []);

  const loadProjects = useCallback(async () => {
    setLoading(true);
    try {
      const params = { page, limit: LIMIT };
      if (search)   params.search   = search;
      if (pincode)  params.pincode  = pincode;
      if (district) params.district = district;
      if (status)   params.status   = status;
      const { data: res } = await getProjects(params);
      let results = res.results;
      if (sortKey) {
        results = [...results].sort((a, b) => {
          const av = a[sortKey] ?? "", bv = b[sortKey] ?? "";
          if (typeof av === "number" && typeof bv === "number")
            return sortDir === "asc" ? av - bv : bv - av;
          return sortDir === "asc"
            ? String(av).localeCompare(String(bv))
            : String(bv).localeCompare(String(av));
        });
      }
      setData({ ...res, results });
    } catch { toast.error("Failed to load projects"); }
    finally { setLoading(false); }
  }, [page, search, pincode, district, status, sortKey, sortDir]);

  useEffect(() => { loadProjects(); }, [loadProjects]);

  const toggleFav = async (pin, e) => {
    e.stopPropagation();
    try {
      if (favPins.has(pin)) {
        await removeFavourite(pin);
        setFavPins((s) => { const n = new Set(s); n.delete(pin); return n; });
        toast.success(`Removed ${pin}`);
      } else {
        await addFavourite(pin);
        setFavPins((s) => new Set(s).add(pin));
        toast.success(`Added ${pin}`);
      }
    } catch (err) { toast.error(err.response?.data?.detail || "Failed"); }
  };

  // Derived stats from current page (rough; real stats would need API aggregation)
  const totalStr = data.total.toLocaleString("en-IN");
  const activeFilters = [pincode, district, status, search].filter(Boolean).length;

  return (
    <div className="p-6 md:p-8 space-y-5">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-[18px] font-semibold text-ink tracking-tight">Projects</h1>
          <p className="text-[12px] text-muted mt-0.5">{totalStr} projects across West Bengal</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`btn ${activeFilters > 0 ? "bg-ink text-white border-ink" : ""}`}
          >
            <FilterIcon />
            Filters
            {activeFilters > 0 && (
              <span className="bg-white text-ink rounded-full w-4 h-4 flex items-center justify-center text-[10px] font-bold">
                {activeFilters}
              </span>
            )}
          </button>
        </div>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Total projects" value={totalStr} sub="Across West Bengal" />
        <StatCard label="This page" value={data.results.length} sub={`Page ${page} of ${data.pages}`} />
        <StatCard
          label="Avg booking"
          value={
            data.results.length
              ? (data.results.reduce((s, p) => s + (p.booking_rate_pct || 0), 0) / data.results.filter(p => p.booking_rate_pct != null).length || 0).toFixed(1) + "%"
              : "—"
          }
          sub="On this page"
        />
        <StatCard
          label="Fully booked"
          value={data.results.filter((p) => p.booking_rate_pct === 100).length}
          sub="100% booking rate"
          subColor="text-[#3B6D11]"
        />
      </div>

      {/* Search */}
      <div className="relative">
        <div className="absolute left-3 top-1/2 -translate-y-1/2 text-muted"><SearchIcon /></div>
        <input
          type="text" className="input pl-9"
          placeholder="Search project name or developer..."
          value={search}
          onChange={(e) => setParam("search", e.target.value)}
        />
      </div>

      {/* Filter panel */}
      {showFilters && (
        <div className="card p-4 grid grid-cols-1 sm:grid-cols-3 gap-4">
          <div>
            <label className="block text-[11px] font-medium text-muted mb-1.5 uppercase tracking-wide">Pincode</label>
            <select className="input text-[13px]" value={pincode} onChange={(e) => setParam("pincode", e.target.value)}>
              <option value="">All pincodes</option>
              {filters.pincodes.map((p) => <option key={p} value={p}>{p}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-[11px] font-medium text-muted mb-1.5 uppercase tracking-wide">District</label>
            <select className="input text-[13px]" value={district} onChange={(e) => setParam("district", e.target.value)}>
              <option value="">All districts</option>
              {filters.districts.map((d) => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-[11px] font-medium text-muted mb-1.5 uppercase tracking-wide">Status</label>
            <select className="input text-[13px]" value={status} onChange={(e) => setParam("status", e.target.value)}>
              <option value="">All statuses</option>
              {filters.statuses.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
          {activeFilters > 0 && (
            <div className="sm:col-span-3">
              <button className="btn text-[#A32D2D] border-[#F7C1C1] bg-[#FCEBEB] hover:bg-[#F7C1C1] text-[11px]"
                onClick={() => setSearchParams(new URLSearchParams())}>
                Clear all filters
              </button>
            </div>
          )}
        </div>
      )}

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr className="border-b border-[#f0efeb]">
                <th className="w-9 px-3 py-3" />
                {COLS.map((col) => (
                  <th
                    key={col.key}
                    onClick={() => col.sortable && toggleSort(col.key)}
                    className={`px-4 py-3 text-left text-[10px] font-semibold text-muted uppercase tracking-[0.6px] whitespace-nowrap ${col.sortable ? "cursor-pointer hover:text-ink select-none" : ""}`}
                  >
                    <span className="flex items-center gap-1">
                      {col.label}
                      {col.sortable && sortKey === col.key && (
                        <span className="text-ink">{sortDir === "asc" ? <ChevronUp /> : <ChevronDown />}</span>
                      )}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i} className="border-b border-[#f8f7f4]">
                    <td colSpan={COLS.length + 1} className="px-4 py-3">
                      <div className="h-3.5 bg-surface rounded animate-pulse" style={{ width: `${60 + Math.random() * 30}%` }} />
                    </td>
                  </tr>
                ))
              ) : data.results.length === 0 ? (
                <tr>
                  <td colSpan={COLS.length + 1} className="px-4 py-16 text-center text-muted text-[13px]">
                    No projects found
                  </td>
                </tr>
              ) : (
                data.results.map((p) => (
                  <tr
                    key={p.project_id}
                    onClick={() => navigate(`/projects/${p.project_id}`)}
                    className="border-b border-[#f8f7f4] hover:bg-surface cursor-pointer transition-colors"
                  >
                    <td className="px-3 py-3" onClick={(e) => e.stopPropagation()}>
                      <button onClick={(e) => toggleFav(p.pincode, e)} className="hover:scale-110 transition-transform">
                        <StarIcon filled={favPins.has(p.pincode)} />
                      </button>
                    </td>
                    <td className="px-4 py-3 max-w-[200px]">
                      <p className="text-[12px] font-medium text-ink truncate">{p.project_name || "—"}</p>
                      <p className="text-[10px] text-muted mono truncate mt-0.5">{p.rera_reg_no || p.project_id}</p>
                    </td>
                    <td className="px-4 py-3 text-[12px] text-[#555] max-w-[140px] truncate">{p.developer || "—"}</td>
                    <td className="px-4 py-3"><span className="mono text-[11px] text-[#555]">{p.pincode || "—"}</span></td>
                    <td className="px-4 py-3 text-[12px] text-[#555]">{p.district || "—"}</td>
                    <td className="px-4 py-3 text-[12px] text-[#555]">{p.project_type || "—"}</td>
                    <td className="px-4 py-3"><StatusBadge status={p.project_status} /></td>
                    <td className="px-4 py-3 text-[12px] text-[#555] text-right mono">{p.total_apartments ?? "—"}</td>
                    <td className="px-4 py-3 text-[12px] text-[#555] text-right mono">{p.apartments_booked ?? "—"}</td>
                    <td className="px-4 py-3"><BookingBar pct={p.booking_rate_pct} /></td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {data.pages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-[#f0efeb]">
            <p className="text-[11px] text-muted">
              Page {page} of {data.pages} · {data.total.toLocaleString("en-IN")} results
            </p>
            <div className="flex items-center gap-1">
              <button onClick={() => setPage(page - 1)} disabled={page <= 1}
                className="btn px-2 py-1 disabled:opacity-30">‹</button>
              {Array.from({ length: Math.min(5, data.pages) }, (_, i) => {
                const n = Math.max(1, Math.min(page - 2, data.pages - 4)) + i;
                return (
                  <button key={n} onClick={() => setPage(n)}
                    className={`btn px-2.5 py-1 text-[11px] ${n === page ? "bg-ink text-white border-ink" : ""}`}>
                    {n}
                  </button>
                );
              })}
              <button onClick={() => setPage(page + 1)} disabled={page >= data.pages}
                className="btn px-2 py-1 disabled:opacity-30">›</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}