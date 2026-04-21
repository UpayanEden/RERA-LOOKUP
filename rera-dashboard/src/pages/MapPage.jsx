import { useEffect, useRef, useState } from "react";
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

function pinSvg(color) {
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(
    `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="32" viewBox="0 0 24 32">
      <path d="M12 0C5.373 0 0 5.373 0 12c0 9 12 20 12 20s12-11 12-20C24 5.373 18.627 0 12 0z"
        fill="${color}" stroke="white" stroke-width="1.5"/>
      <circle cx="12" cy="12" r="4" fill="white" opacity="0.9"/>
    </svg>`
  )}`;
}

const KOLKATA  = [22.5726, 88.3639];
const DEF_KM   = 3;

// Load Leaflet script once globally
let leafletLoaded = false;
function loadLeaflet() {
  return new Promise((resolve) => {
    if (window.L) { resolve(); return; }
    if (leafletLoaded) {
      const wait = setInterval(() => { if (window.L) { clearInterval(wait); resolve(); } }, 50);
      return;
    }
    leafletLoaded = true;
    if (!document.getElementById("leaflet-css")) {
      const lnk = document.createElement("link");
      lnk.id = "leaflet-css"; lnk.rel = "stylesheet";
      lnk.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
      document.head.appendChild(lnk);
    }
    const s = document.createElement("script");
    s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
    s.onload = resolve;
    document.body.appendChild(s);
  });
}

export default function MapPage() {
  const navigate    = useNavigate();
  const containerRef = useRef(null);  // the div
  const stateRef    = useRef({        // all mutable map state lives here
    map:     null,
    markers: null,
    circle:  null,
    handle:  null,
    drawMode: false,
    resizing: false,
    moving:   false,
    dragStart: null,
    origCenter: null,
  });

  const [loading,    setLoading]    = useState(false);
  const [count,      setCount]      = useState(0);
  const [geoStat,    setGeoStat]    = useState(null);
  const [search,     setSearch]     = useState("");
  const [statusF,    setStatusF]    = useState("");
  const [showF,      setShowF]      = useState(false);
  const [drawMode,   setDrawMode]   = useState(false);
  const [circleInfo, setCircleInfo] = useState(null);

  // ── load projects ────────────────────────────────────────────────────────
  const loadProjects = async (params = {}) => {
    const s = stateRef.current;
    if (!s.map) return;
    setLoading(true);
    try {
      const { data } = await api.get("/map/projects", { params });
      setCount(data.count);
      const L = window.L;
      if (!s.markers) {
        s.markers = L.layerGroup().addTo(s.map);
      } else {
        s.markers.clearLayers();
      }
      data.features.forEach(({ geometry, properties: p }) => {
        const [lon, lat] = geometry.coordinates;
        const color = pinColor(p.booking_rate_pct);
        const icon  = L.icon({ iconUrl: pinSvg(color), iconSize: [24,32], iconAnchor: [12,32], popupAnchor: [0,-32] });
        const pct   = p.booking_rate_pct != null ? `${p.booking_rate_pct.toFixed(1)}%` : "—";
        L.marker([lat, lon], { icon }).bindPopup(`
          <div style="font-family:sans-serif;min-width:200px">
            <p style="font-size:13px;font-weight:600;margin:0 0 3px;color:#1a1a1a">${p.project_name || "Unknown"}</p>
            <p style="font-size:11px;color:#888;margin:0 0 8px">${p.developer || "—"}</p>
            <div style="display:flex;gap:5px;margin-bottom:8px;flex-wrap:wrap">
              <span style="background:#f0efeb;color:#555;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600">${p.pincode||""}</span>
              <span style="background:${color}22;color:${color};padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600">${p.project_status||""}</span>
            </div>
            <div style="display:flex;justify-content:space-between;font-size:11px;color:#555;margin-bottom:8px">
              <span>Units <b>${p.total_apartments||"—"}</b></span>
              <span>Booked <b style="color:${color}">${pct}</b></span>
            </div>
            <button onclick="window._reraNav('${p.project_id}')"
              style="width:100%;padding:6px 0;background:#1a1a1a;color:#fff;border:none;border-radius:6px;font-size:11px;cursor:pointer">
              View details →
            </button>
          </div>
        `, { maxWidth: 260 }).addTo(s.markers);
      });
    } catch(e) { console.error(e); }
    finally    { setLoading(false); }
  };

  // ── place circle ──────────────────────────────────────────────────────────
  const placeCircle = (lat, lon, km) => {
    const s = stateRef.current;
    const L = window.L;
    if (!L || !s.map) return;

    if (s.circle) { s.map.removeLayer(s.circle); s.circle = null; }
    if (s.handle) { s.map.removeLayer(s.handle); s.handle = null; }

    // Circle
    s.circle = L.circle([lat, lon], {
      radius:      km * 1000,
      color:       "#1d4ed8",
      fillColor:   "#3b82f6",
      fillOpacity: 0.15,
      weight:      2.5,
    }).addTo(s.map);

    // Handle — blue circle marker on east edge
    const eLon = lon + km / (111.32 * Math.cos(lat * Math.PI / 180));
    s.handle = L.circleMarker([lat, eLon], {
      radius:      9,
      color:       "#ffffff",
      fillColor:   "#1d4ed8",
      fillOpacity: 1,
      weight:      3,
      interactive: true,
    }).addTo(s.map);

    setCircleInfo({ lat, lon, km: parseFloat(km.toFixed(2)) });
    loadProjects({ lat, lon, radius_km: km, status: statusF || undefined });
  };

  // ── clear circle ──────────────────────────────────────────────────────────
  const clearCircle = () => {
    const s = stateRef.current;
    if (s.circle) { s.map?.removeLayer(s.circle); s.circle = null; }
    if (s.handle) { s.map?.removeLayer(s.handle); s.handle = null; }
    s.drawMode = false;
    setDrawMode(false);
    setCircleInfo(null);
    loadProjects({ status: statusF || undefined });
  };

  // ── search ────────────────────────────────────────────────────────────────
  const doSearch = async (e) => {
    e?.preventDefault();
    if (!search.trim()) { clearCircle(); return; }
    const q = search.trim();
    const nom = async (query) => {
      const r = await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query + ", West Bengal, India")}&format=json&limit=1&countrycodes=in`, { headers: { "User-Agent": "WB-RERA/1.0" } });
      const d = await r.json();
      return d[0] ? { lat: parseFloat(d[0].lat), lon: parseFloat(d[0].lon) } : null;
    };
    if (/^\d{6}$/.test(q)) {
      clearCircle();
      loadProjects({ pincode: q, status: statusF || undefined });
      const geo = await nom(q);
      if (geo) stateRef.current.map?.setView([geo.lat, geo.lon], 14);
      return;
    }
    const geo = await nom(q);
    if (!geo) { alert("Location not found"); return; }
    stateRef.current.map?.setView([geo.lat, geo.lon], 13);
    placeCircle(geo.lat, geo.lon, DEF_KM);
  };

  // ── init map ──────────────────────────────────────────────────────────────
  useEffect(() => {
    const s = stateRef.current;
    let destroyed = false;

    loadLeaflet().then(() => {
      if (destroyed || !containerRef.current) return;

      // Destroy any previous Leaflet instance on this DOM node
      if (containerRef.current._leaflet_id != null) {
        try { window.L.map(containerRef.current).remove(); } catch {}
        delete containerRef.current._leaflet_id;
      }

      const L   = window.L;
      const map = L.map(containerRef.current, { center: KOLKATA, zoom: 12, preferCanvas: false });
      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        maxZoom: 19,
      }).addTo(map);

      s.map = map;
      window._reraNav = (pid) => navigate(`/projects/${pid}`);

      // ── map click: place circle in draw mode ──
      map.on("click", (e) => {
        if (!s.drawMode) return;
        s.drawMode = false;
        setDrawMode(false);
        placeCircle(e.latlng.lat, e.latlng.lng, DEF_KM);
      });

      // ── handle drag: resize ──
      map.on("mousemove", (e) => {
        if (s.resizing && s.circle) {
          const km = Math.max(0.2, Math.min(50, s.circle.getLatLng().distanceTo(e.latlng) / 1000));
          s.circle.setRadius(km * 1000);
          const c    = s.circle.getLatLng();
          const eLon = c.lng + km / (111.32 * Math.cos(c.lat * Math.PI / 180));
          s.handle?.setLatLng([c.lat, eLon]);
          setCircleInfo({ lat: c.lat, lon: c.lng, km: parseFloat(km.toFixed(2)) });
        }
        if (s.moving && s.circle && s.dragStart) {
          const dlat = e.latlng.lat - s.dragStart.lat;
          const dlng = e.latlng.lng - s.dragStart.lng;
          const nLat = s.origCenter.lat + dlat;
          const nLng = s.origCenter.lng + dlng;
          s.circle.setLatLng([nLat, nLng]);
          const km   = s.circle.getRadius() / 1000;
          const eLon = nLng + km / (111.32 * Math.cos(nLat * Math.PI / 180));
          s.handle?.setLatLng([nLat, eLon]);
          setCircleInfo({ lat: nLat, lon: nLng, km: parseFloat(km.toFixed(2)) });
        }
      });

      map.on("mouseup", () => {
        if (s.resizing || s.moving) {
          const wasResizing = s.resizing;
          const wasMoving   = s.moving;
          s.resizing = false;
          s.moving   = false;
          map.dragging.enable();
          if ((wasResizing || wasMoving) && s.circle) {
            const c  = s.circle.getLatLng();
            const km = s.circle.getRadius() / 1000;
            loadProjects({ lat: c.lat, lon: c.lng, radius_km: km, status: statusF || undefined });
          }
        }
      });

      // Attach handle events after a tick so the SVG is in DOM
      const attachHandleEvents = () => {
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
      };

      // override placeCircle to attach events after placing
      const originalPlace = placeCircle;
      window._attachHandleEvents = attachHandleEvents;

      loadProjects();
      api.get("/map/geocode-status").then(({ data }) => setGeoStat(data)).catch(() => {});
    });

    return () => {
      destroyed = true;
      if (s.map) { s.map.remove(); s.map = null; s.markers = null; s.circle = null; s.handle = null; }
    };
  }, []);

  // attach handle events whenever circle changes
  useEffect(() => {
    if (!circleInfo) return;
    setTimeout(() => window._attachHandleEvents?.(), 100);
  }, [circleInfo]);

  return (
    <div className="flex flex-col" style={{ height: "calc(100vh - 48px)" }}>

      {/* toolbar */}
      <div className="bg-white border-b border-border px-4 py-3 flex items-center gap-3 flex-wrap shrink-0">
        <div className="shrink-0">
          <h1 className="text-[15px] font-semibold text-ink tracking-tight">Map</h1>
          <p className="text-[11px] text-muted">
            {loading ? "Loading..." : `${count.toLocaleString("en-IN")} projects`}
            {geoStat && ` · ${geoStat.geocoded.toLocaleString("en-IN")} geocoded`}
          </p>
        </div>

        <form onSubmit={doSearch} className="flex items-center gap-2 flex-1 max-w-sm">
          <div className="relative flex-1">
            <div className="absolute left-3 top-1/2 -translate-y-1/2 text-muted">
              <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
                <circle cx="7" cy="7" r="4.5"/><line x1="10.5" y1="10.5" x2="14" y2="14"/>
              </svg>
            </div>
            <input type="text" className="input pl-9 text-[12px]"
              placeholder="Locality or 6-digit pincode..."
              value={search} onChange={(e) => setSearch(e.target.value)} />
          </div>
          <button type="submit" className="btn-primary text-[11px]">Search</button>
        </form>

        <button
          onClick={() => {
            if (circleInfo) { clearCircle(); return; }
            const s = stateRef.current;
            const newMode = !drawMode;
            s.drawMode = newMode;
            setDrawMode(newMode);
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
          <span className="text-[11px] text-muted">
            <span className="font-medium mono text-ink">{circleInfo.km} km</span>
            {" "}· drag blue dot = resize · drag fill = move
          </span>
        )}

        <button onClick={() => setShowF(f => !f)}
          className={`btn text-[11px] shrink-0 ${showF ? "bg-ink text-white border-ink" : ""}`}>
          <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
            <line x1="2" y1="5" x2="14" y2="5"/><line x1="4" y1="9" x2="12" y2="9"/><line x1="6" y1="13" x2="10" y2="13"/>
          </svg>
          Filters
        </button>
      </div>

      {showF && (
        <div className="bg-white border-b border-border px-4 py-2.5 flex items-center gap-3 shrink-0">
          <span className="text-[11px] text-muted uppercase tracking-wide font-medium">Status</span>
          <select className="input text-[12px] w-52" value={statusF} onChange={(e) => { setStatusF(e.target.value); }}>
            <option value="">All statuses</option>
            <option value="Under Construction">Under Construction</option>
            <option value="Completed">Completed</option>
            <option value="Not Started">Not Started</option>
          </select>
        </div>
      )}

      {drawMode && (
        <div className="bg-[#EFF6FF] border-b border-[#BFDBFE] px-4 py-2 text-[12px] text-[#1d4ed8] font-medium shrink-0">
          Click anywhere on the map to place your search circle — then drag the blue dot to resize, or drag the filled area to move it.
        </div>
      )}

      <div className="flex-1 relative overflow-hidden">
        {loading && (
          <div className="absolute top-3 left-1/2 -translate-x-1/2 z-[1000] bg-white border border-border rounded-lg px-3 py-1.5 text-[12px] text-muted shadow-sm pointer-events-none">
            Loading...
          </div>
        )}
        <div className="absolute bottom-6 right-4 z-[1000] bg-white border border-border rounded-xl px-3 py-3 shadow-sm">
          <p className="text-[10px] font-semibold text-muted uppercase tracking-wide mb-2">Booking rate</p>
          {[["#3B6D11","≥ 75% — High"],["#BA7517","40–74% — Mid"],["#A32D2D","< 40% — Low"],["#888780","No data"]].map(([c,l]) => (
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