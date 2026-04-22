import { useEffect, useRef, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";

const api = axios.create({ baseURL: import.meta.env.VITE_API_URL || "http://127.0.0.1:8000" });
api.interceptors.request.use((c) => {
  const t = localStorage.getItem("token");
  if (t) c.headers.Authorization = `Bearer ${t}`;
  return c;
});

function pinColor(pct) {
  if (pct == null) return "#888780";
  if (pct >= 75)   return "#3B6D11";
  if (pct >= 40)   return "#BA7517";
  return "#A32D2D";
}

function pinSvg(color, highlight = false) {
  const s = highlight ? 32 : 24;
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(
    `<svg xmlns="http://www.w3.org/2000/svg" width="${s}" height="${Math.round(s * 4 / 3)}" viewBox="0 0 24 32">
      <path d="M12 0C5.373 0 0 5.373 0 12c0 9 12 20 12 20s12-11 12-20C24 5.373 18.627 0 12 0z"
        fill="${color}" stroke="white" stroke-width="${highlight ? 2.5 : 1.5}"/>
      <circle cx="12" cy="12" r="${highlight ? 5 : 4}" fill="white" opacity="0.9"/>
    </svg>`
  )}`;
}

const KOLKATA          = [22.5726, 88.3639];
const DEF_KM           = 3;
const DEBOUNCE_MS      = 400;

// ── Load Leaflet once globally ────────────────────────────────────────────────
let _leafletLoading = false;

function loadLeaflet() {
  return new Promise((resolve) => {
    if (window.L) { resolve(); return; }
    if (_leafletLoading) {
      const t = setInterval(() => { if (window.L) { clearInterval(t); resolve(); } }, 50);
      return;
    }
    _leafletLoading = true;
    if (!document.getElementById("leaflet-css")) {
      const lnk = document.createElement("link");
      lnk.id = "leaflet-css"; lnk.rel = "stylesheet";
      lnk.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
      document.head.appendChild(lnk);
    }
    const scr = document.createElement("script");
    scr.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
    scr.onload = resolve;
    document.body.appendChild(scr);
  });
}

function popupHtml(p, color) {
  const pct = p.booking_rate_pct != null
    ? `${parseFloat(p.booking_rate_pct).toFixed(1)}%`
    : "—";
  return `
    <div style="font-family:sans-serif;min-width:200px">
      <p style="font-size:13px;font-weight:600;margin:0 0 3px;color:#1a1a1a;line-height:1.3">
        ${p.project_name || "Unknown"}
      </p>
      <p style="font-size:11px;color:#888;margin:0 0 8px">${p.developer || "—"}</p>
      <div style="display:flex;gap:5px;margin-bottom:8px;flex-wrap:wrap">
        <span style="background:#f0efeb;color:#555;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600">
          ${p.pincode || ""}
        </span>
        <span style="background:${color}22;color:${color};padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600">
          ${p.project_status || ""}
        </span>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:11px;color:#555;margin-bottom:8px">
        <span>Units <b>${p.total_apartments || "—"}</b></span>
        <span>Booked <b style="color:${color}">${pct}</b></span>
      </div>
      <button onclick="window._reraNav('${p.project_id}')"
        style="width:100%;padding:6px 0;background:#1a1a1a;color:#fff;border:none;border-radius:6px;font-size:11px;cursor:pointer">
        View details →
      </button>
    </div>`;
}

export default function MapPage() {
  const navigate     = useNavigate();
  const containerRef = useRef(null);
  const debounceRef  = useRef(null);

  // All mutable map state in a single ref — avoids stale closures
  const S = useRef({
    map:             null,
    markers:         null,   // L.layerGroup
    circle:          null,
    handle:          null,
    highlightMarker: null,
    drawMode:        false,
    resizing:        false,
    moving:          false,
    dragStart:       null,
    origCenter:      null,
    statusFilter:    "",
    radiusParams:    null,   // {lat,lon,radius_km} when circle active
    searchQuery:     "",
  });

  const [loading,     setLoading]     = useState(false);
  const [viewCount,   setViewCount]   = useState(0);
  const [totalCount,  setTotalCount]  = useState(0);
  const [search,      setSearch]      = useState("");
  const [searchType,  setSearchType]  = useState("location");
  const [statusF,     setStatusF]     = useState("");
  const [showF,       setShowF]       = useState(false);
  const [drawMode,    setDrawMode]    = useState(false);
  const [circleInfo,  setCircleInfo]  = useState(null);
  const [suggestions, setSuggestions] = useState([]);
  const [showSuggest, setShowSuggest] = useState(false);
  const [searching,   setSearching]   = useState(false);

  // ── Render markers into layer group ────────────────────────────────────────
  const renderMarkers = useCallback((features) => {
    const s = S.current;
    const L = window.L;
    if (!L || !s.map) return;

    if (s.markers) { s.map.removeLayer(s.markers); }
    s.markers = L.layerGroup().addTo(s.map);

    features.forEach(({ geometry, properties: p }) => {
      const [lon, lat] = geometry.coordinates;
      const color = pinColor(p.booking_rate_pct);
      const icon  = L.icon({
        iconUrl:     pinSvg(color),
        iconSize:    [24, 32],
        iconAnchor:  [12, 32],
        popupAnchor: [0, -32],
      });
      L.marker([lat, lon], { icon })
        .bindPopup(popupHtml(p, color), { maxWidth: 260 })
        .addTo(s.markers);
    });
  }, []);

  // ── Load projects in viewport ───────────────────────────────────────────────
  const loadViewport = useCallback(async (extra = {}) => {
    const s   = S.current;
    const map = s.map;
    if (!map) return;

    setLoading(true);
    try {
      let data;

      if (s.radiusParams) {
        // Circle mode — use radius endpoint
        const res = await api.get("/map/projects", {
          params: { ...s.radiusParams, status: s.statusFilter || undefined, ...extra },
        });
        data = res.data;
      } else {
        // Normal viewport mode
        const b = map.getBounds();
        const res = await api.get("/map/projects/bounds", {
          params: {
            north:  b.getNorth(),
            south:  b.getSouth(),
            east:   b.getEast(),
            west:   b.getWest(),
            zoom:   map.getZoom(),
            status: s.statusFilter || undefined,
            search: s.searchQuery  || undefined,
            ...extra,
          },
        });
        data = res.data;
      }

      setViewCount(data.count);
      renderMarkers(data.features);
    } catch (e) {
      console.error("loadViewport", e);
    } finally {
      setLoading(false);
    }
  }, [renderMarkers]);

  const loadDebounced = useCallback((extra = {}) => {
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => loadViewport(extra), DEBOUNCE_MS);
  }, [loadViewport]);

  // ── Circle helpers ──────────────────────────────────────────────────────────
  const placeCircle = useCallback((lat, lon, km) => {
    const s = S.current;
    const L = window.L;
    if (!L || !s.map) return;

    if (s.circle) { s.map.removeLayer(s.circle); s.circle = null; }
    if (s.handle) { s.map.removeLayer(s.handle); s.handle = null; }

    s.circle = L.circle([lat, lon], {
      radius: km * 1000, color: "#1d4ed8",
      fillColor: "#3b82f6", fillOpacity: 0.15, weight: 2.5,
    }).addTo(s.map);

    const eLon = lon + km / (111.32 * Math.cos(lat * Math.PI / 180));
    s.handle = L.circleMarker([lat, eLon], {
      radius: 9, color: "#fff", fillColor: "#1d4ed8", fillOpacity: 1, weight: 3,
    }).addTo(s.map);

    s.radiusParams = { lat, lon, radius_km: km };
    setCircleInfo({ lat, lon, km: parseFloat(km.toFixed(2)) });
    loadViewport();
  }, [loadViewport]);

  const clearCircle = useCallback(() => {
    const s = S.current;
    if (s.circle) { s.map?.removeLayer(s.circle); s.circle = null; }
    if (s.handle) { s.map?.removeLayer(s.handle); s.handle = null; }
    s.radiusParams = null;
    s.drawMode     = false;
    setDrawMode(false);
    setCircleInfo(null);
    loadViewport();
  }, [loadViewport]);

  // ── Highlight a single project ──────────────────────────────────────────────
  const highlightProject = useCallback((p) => {
    const s = S.current;
    const L = window.L;
    if (!L || !s.map || !p.lat || !p.lon) return;

    if (s.highlightMarker) { s.map.removeLayer(s.highlightMarker); s.highlightMarker = null; }

    const color = pinColor(p.booking_rate_pct);
    s.highlightMarker = L.marker([p.lat, p.lon], {
      icon: L.icon({
        iconUrl: pinSvg(color, true), iconSize: [32, 43],
        iconAnchor: [16, 43], popupAnchor: [0, -43],
      }),
      zIndexOffset: 2000,
    }).addTo(s.map).bindPopup(popupHtml(p, color), { maxWidth: 260 });

    s.map.setView([p.lat, p.lon], 16);
    setTimeout(() => s.highlightMarker?.openPopup(), 300);
  }, []);

  // ── Search handler ──────────────────────────────────────────────────────────
  const handleSearch = async (e) => {
    e?.preventDefault();
    setShowSuggest(false);
    const q = search.trim();
    if (!q) { clearCircle(); S.current.searchQuery = ""; loadViewport(); return; }

    if (searchType === "project") {
      setSearching(true);
      try {
        const { data } = await api.get("/map/projects/bounds", {
          params: { search: q, north: 90, south: -90, east: 180, west: -180 },
        });
        if (data.features?.length > 0) {
          const first     = data.features[0];
          const [lon, lat] = first.geometry.coordinates;
          S.current.searchQuery = q;
          highlightProject({ ...first.properties, lat, lon });
          loadDebounced({ search: q });
          setViewCount(data.count);
        } else {
          alert("No projects found");
        }
      } catch { alert("Search failed"); }
      finally  { setSearching(false); }
      return;
    }

    // Pincode
    if (/^\d{6}$/.test(q)) {
      clearCircle();
      S.current.searchQuery = "";
      try {
        const r = await fetch(
          `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q + ", West Bengal, India")}&format=json&limit=1&countrycodes=in`,
          { headers: { "User-Agent": "WB-RERA/1.0" } }
        );
        const d = await r.json();
        if (d[0]) S.current.map?.setView([parseFloat(d[0].lat), parseFloat(d[0].lon)], 14);
      } catch {}
      loadDebounced({ pincode: q });
      return;
    }

    // Location name
    try {
      const r = await fetch(
        `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q + ", West Bengal, India")}&format=json&limit=1&countrycodes=in`,
        { headers: { "User-Agent": "WB-RERA/1.0" } }
      );
      const d = await r.json();
      if (!d[0]) { alert("Location not found"); return; }
      S.current.map?.setView([parseFloat(d[0].lat), parseFloat(d[0].lon)], 13);
      placeCircle(parseFloat(d[0].lat), parseFloat(d[0].lon), DEF_KM);
    } catch { alert("Geocode failed"); }
  };

  // ── Autocomplete ────────────────────────────────────────────────────────────
  useEffect(() => {
    if (searchType !== "project" || search.length < 2) { setSuggestions([]); return; }
    const t = setTimeout(async () => {
      try {
        const { data } = await api.get("/map/projects/bounds", {
          params: { search, north: 90, south: -90, east: 180, west: -180 },
        });
        setSuggestions((data.features || []).slice(0, 8));
      } catch { setSuggestions([]); }
    }, 300);
    return () => clearTimeout(t);
  }, [search, searchType]);

  // ── Sync statusFilter ───────────────────────────────────────────────────────
  useEffect(() => {
    S.current.statusFilter = statusF;
    if (S.current.map) loadDebounced();
  }, [statusF]);

  // ── Init map ────────────────────────────────────────────────────────────────
  useEffect(() => {
    const s = S.current;
    let destroyed = false;

    loadLeaflet().then(() => {
      if (destroyed || !containerRef.current) return;

      // Clean up any previous instance
      if (containerRef.current._leaflet_id != null) {
        try { window.L.map(containerRef.current).remove(); } catch {}
        delete containerRef.current._leaflet_id;
      }

      const L   = window.L;
      const map = L.map(containerRef.current, { center: KOLKATA, zoom: 12 });

      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        maxZoom: 19,
      }).addTo(map);

      s.map = map;
      window._reraNav = (pid) => navigate(`/projects/${pid}`);

      // Reload on pan / zoom
      map.on("moveend", () => {
        if (s.radiusParams) return;
        loadDebounced(s.searchQuery ? { search: s.searchQuery } : {});
      });

      // Draw circle on click
      map.on("click", (e) => {
        if (!s.drawMode) return;
        s.drawMode = false;
        setDrawMode(false);
        placeCircle(e.latlng.lat, e.latlng.lng, DEF_KM);
      });

      // Circle resize / move
      map.on("mousemove", (e) => {
        if (s.resizing && s.circle) {
          const km = Math.max(0.2, Math.min(50, s.circle.getLatLng().distanceTo(e.latlng) / 1000));
          s.circle.setRadius(km * 1000);
          const c = s.circle.getLatLng();
          s.handle?.setLatLng([c.lat, c.lng + km / (111.32 * Math.cos(c.lat * Math.PI / 180))]);
          setCircleInfo({ lat: c.lat, lon: c.lng, km: parseFloat(km.toFixed(2)) });
        }
        if (s.moving && s.circle && s.dragStart) {
          const nLat = s.origCenter.lat + (e.latlng.lat - s.dragStart.lat);
          const nLng = s.origCenter.lng + (e.latlng.lng - s.dragStart.lng);
          s.circle.setLatLng([nLat, nLng]);
          const km = s.circle.getRadius() / 1000;
          s.handle?.setLatLng([nLat, nLng + km / (111.32 * Math.cos(nLat * Math.PI / 180))]);
          setCircleInfo({ lat: nLat, lon: nLng, km: parseFloat(km.toFixed(2)) });
        }
      });

      map.on("mouseup", () => {
        if (!s.resizing && !s.moving) return;
        s.resizing = false;
        s.moving   = false;
        map.dragging.enable();
        if (s.circle) {
          const c  = s.circle.getLatLng();
          const km = s.circle.getRadius() / 1000;
          s.radiusParams = { lat: c.lat, lon: c.lng, radius_km: km };
          setCircleInfo({ lat: c.lat, lon: c.lng, km: parseFloat(km.toFixed(2)) });
          loadViewport();
        }
      });

      // Attach drag events to circle / handle after render
      window._attachHandleEvents = () => {
        if (!s.handle || !s.circle) return;
        s.handle.on("mousedown", (e) => {
          s.resizing = true;
          map.dragging.disable();
          L.DomEvent.stopPropagation(e);
        });
        s.circle.on("mousedown", (e) => {
          if (s.drawMode) return;
          s.moving     = true;
          s.dragStart  = e.latlng;
          s.origCenter = s.circle.getLatLng();
          map.dragging.disable();
          L.DomEvent.stopPropagation(e);
        });
        if (s.circle._path) s.circle._path.style.cursor = "move";
      };

      // Fetch total geocoded count
      api.get("/map/geocode-status")
        .then(({ data }) => setTotalCount(data.geocoded))
        .catch(() => {});

      loadViewport();
    });

    return () => {
      destroyed = true;
      if (s.map) {
        s.map.remove();
        s.map = null; s.markers = null; s.circle = null; s.handle = null;
      }
    };
  }, []);

  useEffect(() => {
    if (circleInfo) setTimeout(() => window._attachHandleEvents?.(), 100);
  }, [circleInfo]);

  useEffect(() => { S.current.drawMode = drawMode; }, [drawMode]);

  // ── Render ──────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col" style={{ height: "calc(100vh - 48px)" }}>

      {/* Toolbar */}
      <div className="bg-white border-b border-border px-4 py-3 flex items-center gap-3 flex-wrap shrink-0">
        <div className="shrink-0">
          <h1 className="text-[15px] font-semibold text-ink tracking-tight">Map</h1>
          <p className="text-[11px] text-muted">
            {loading
              ? "Loading..."
              : `${viewCount.toLocaleString("en-IN")} in view · ${totalCount.toLocaleString("en-IN")} total`}
          </p>
        </div>

        {/* Search type toggle */}
        <div className="flex items-center bg-surface rounded-lg p-0.5 shrink-0">
          {["location", "project"].map((t) => (
            <button key={t}
              onClick={() => {
                setSearchType(t);
                setSearch("");
                setSuggestions([]);
                S.current.searchQuery = "";
              }}
              className={`px-3 py-1.5 rounded-md text-[11px] font-medium transition-colors ${
                searchType === t ? "bg-white text-ink shadow-sm" : "text-muted hover:text-ink"
              }`}
            >
              {t === "location" ? "Location" : "Project name"}
            </button>
          ))}
        </div>

        {/* Search box */}
        <div className="relative flex-1 max-w-sm">
          <form onSubmit={handleSearch} className="flex items-center gap-2">
            <div className="relative flex-1">
              <div className="absolute left-3 top-1/2 -translate-y-1/2 text-muted">
                <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
                  <circle cx="7" cy="7" r="4.5"/><line x1="10.5" y1="10.5" x2="14" y2="14"/>
                </svg>
              </div>
              <input
                type="text"
                className="input pl-9 text-[12px]"
                placeholder={searchType === "project" ? "Type project name..." : "Locality or 6-digit pincode..."}
                value={search}
                onChange={(e) => { setSearch(e.target.value); setShowSuggest(true); }}
                onFocus={() => setShowSuggest(true)}
                onBlur={() => setTimeout(() => setShowSuggest(false), 200)}
              />
            </div>
            <button type="submit" disabled={searching} className="btn-primary text-[11px] shrink-0">
              {searching ? "..." : "Search"}
            </button>
          </form>

          {/* Autocomplete dropdown */}
          {searchType === "project" && showSuggest && suggestions.length > 0 && (
            <div className="absolute top-full left-0 right-12 mt-1 bg-white border border-border rounded-xl shadow-lg z-[2000] overflow-hidden">
              {suggestions.map((f, i) => {
                const p        = f.properties;
                const [lon, lat] = f.geometry.coordinates;
                return (
                  <div key={i}
                    className="px-3 py-2.5 hover:bg-surface cursor-pointer border-b border-[#f8f7f4] last:border-0"
                    onMouseDown={() => {
                      setSearch(p.project_name || "");
                      setShowSuggest(false);
                      S.current.searchQuery = p.project_name || "";
                      highlightProject({ ...p, lat, lon });
                      loadDebounced({ search: p.project_name });
                    }}
                  >
                    <p className="text-[12px] font-medium text-ink truncate">{p.project_name || "—"}</p>
                    <p className="text-[10px] text-muted">{p.developer || "—"} · {p.pincode} · {p.district}</p>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Draw circle button */}
        <button
          onClick={() => {
            if (circleInfo) { clearCircle(); return; }
            const nm = !drawMode;
            S.current.drawMode = nm;
            setDrawMode(nm);
          }}
          className={`btn text-[11px] shrink-0 gap-1.5 ${
            drawMode   ? "bg-[#1d4ed8] text-white border-[#1d4ed8]" :
            circleInfo ? "bg-[#FCEBEB] text-[#A32D2D] border-[#F7C1C1]" : ""
          }`}
        >
          <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
            <circle cx="8" cy="8" r="6"/>
            <line x1="8" y1="2" x2="8" y2="5"/><line x1="8" y1="11" x2="8" y2="14"/>
            <line x1="2" y1="8" x2="5" y2="8"/><line x1="11" y1="8" x2="14" y2="8"/>
          </svg>
          {circleInfo ? "Clear circle" : drawMode ? "Click map to place" : "Draw circle"}
        </button>

        {circleInfo && (
          <span className="text-[11px] text-muted shrink-0">
            <span className="font-medium mono text-ink">{circleInfo.km} km</span>
            {" "}· drag dot=resize · drag fill=move
          </span>
        )}

        {/* Filters button */}
        <button
          onClick={() => setShowF((f) => !f)}
          className={`btn text-[11px] shrink-0 ${showF ? "bg-ink text-white border-ink" : ""}`}
        >
          <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
            <line x1="2" y1="5" x2="14" y2="5"/>
            <line x1="4" y1="9" x2="12" y2="9"/>
            <line x1="6" y1="13" x2="10" y2="13"/>
          </svg>
          Filters
        </button>
      </div>

      {/* Filter bar */}
      {showF && (
        <div className="bg-white border-b border-border px-4 py-2.5 flex items-center gap-3 shrink-0">
          <span className="text-[11px] text-muted uppercase tracking-wide font-medium">Status</span>
          <select
            className="input text-[12px] w-52"
            value={statusF}
            onChange={(e) => setStatusF(e.target.value)}
          >
            <option value="">All statuses</option>
            <option value="Under Construction">Under Construction</option>
            <option value="Completed">Completed</option>
            <option value="Not Started">Not Started</option>
          </select>
        </div>
      )}

      {/* Draw mode hint */}
      {drawMode && (
        <div className="bg-[#EFF6FF] border-b border-[#BFDBFE] px-4 py-2 text-[12px] text-[#1d4ed8] font-medium shrink-0">
          Click anywhere on the map to place your search circle — drag the blue dot to resize, drag the fill to move.
        </div>
      )}

      {/* Map container */}
      <div className="flex-1 relative overflow-hidden">
        {loading && (
          <div className="absolute top-3 left-1/2 -translate-x-1/2 z-[1000] bg-white border border-border rounded-lg px-3 py-1.5 text-[12px] text-muted shadow-sm pointer-events-none">
            Loading...
          </div>
        )}

        {/* Legend */}
        <div className="absolute bottom-6 right-4 z-[1000] bg-white border border-border rounded-xl px-3 py-3 shadow-sm">
          <p className="text-[10px] font-semibold text-muted uppercase tracking-wide mb-2">Booking rate</p>
          {[
            ["#3B6D11", "≥ 75% — High"],
            ["#BA7517", "40–74% — Mid"],
            ["#A32D2D", "< 40% — Low"],
            ["#888780", "No data"],
          ].map(([c, l]) => (
            <div key={l} className="flex items-center gap-2 mb-1 last:mb-0">
              <div className="w-3 h-3 rounded-full shrink-0" style={{ background: c }} />
              <span className="text-[11px] text-[#555]">{l}</span>
            </div>
          ))}
        </div>

        <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
      </div>
    </div>
  );
}