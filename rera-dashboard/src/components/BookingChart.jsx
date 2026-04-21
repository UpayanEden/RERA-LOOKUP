import { useEffect, useState } from "react";
import axios from "axios";

const api = axios.create({ baseURL: import.meta.env.VITE_API_URL || "http://127.0.0.1:8000" });
api.interceptors.request.use((c) => {
  const t = localStorage.getItem("token");
  if (t) c.headers.Authorization = `Bearer ${t}`;
  return c;
});

function formatDate(iso) {
  return new Date(iso).toLocaleDateString("en-IN", {
    day: "2-digit", month: "short", year: "2-digit"
  });
}

export default function BookingChart({ projectId, totalApartments }) {
  const [timeline, setTimeline] = useState([]);
  const [loading,  setLoading]  = useState(true);

  useEffect(() => {
    api.get(`/projects/${projectId}/booking-history`)
      .then(({ data }) => setTimeline(data.timeline || []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [projectId]);

  if (loading) return (
    <div className="card p-5 animate-pulse">
      <div className="h-3 bg-surface rounded w-32 mb-4" />
      <div className="h-24 bg-surface rounded" />
    </div>
  );

  if (timeline.length === 0) return (
    <div className="card p-5">
      <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px] mb-2">
        Booking trend
      </p>
      <p className="text-[12px] text-muted">
        No booking changes recorded yet — trend will appear after the next scrape run detects a change.
      </p>
    </div>
  );

  // Build chart data points — include starting point
  const points = [];
  // Add initial point from first change's old_value
  if (timeline[0]?.old_value != null) {
    points.push({ date: null, value: timeline[0].old_value, label: "Initial" });
  }
  timeline.forEach((t) => {
    if (t.new_value != null) {
      points.push({ date: t.date, value: t.new_value, label: formatDate(t.date) });
    }
  });

  if (points.length < 2) {
    // Only one data point — show as a simple stat
    return (
      <div className="card p-5">
        <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px] mb-3">
          Booking trend
        </p>
        <div className="flex items-center gap-4">
          <div>
            <p className="text-[20px] font-semibold text-ink">
              {points[0]?.value ?? "—"}
            </p>
            <p className="text-[11px] text-muted">units booked</p>
          </div>
          {totalApartments && (
            <div>
              <p className="text-[20px] font-semibold text-[#185FA5]">
                {((points[0]?.value / totalApartments) * 100).toFixed(1)}%
              </p>
              <p className="text-[11px] text-muted">of {totalApartments} units</p>
            </div>
          )}
          <p className="text-[11px] text-muted ml-auto">
            More data points will appear as daily scrapes detect changes
          </p>
        </div>
      </div>
    );
  }

  // SVG chart dimensions
  const W       = 560;
  const H       = 140;
  const PAD_L   = 36;
  const PAD_R   = 16;
  const PAD_T   = 16;
  const PAD_B   = 32;
  const chartW  = W - PAD_L - PAD_R;
  const chartH  = H - PAD_T - PAD_B;

  const values  = points.map(p => p.value);
  const minV    = Math.max(0, Math.min(...values) - 2);
  const maxV    = Math.max(...values) + 2;
  const range   = maxV - minV || 1;

  const toX = (i) => PAD_L + (i / (points.length - 1)) * chartW;
  const toY = (v) => PAD_T + chartH - ((v - minV) / range) * chartH;

  // Build SVG path
  const pathD = points.map((p, i) => `${i === 0 ? "M" : "L"} ${toX(i).toFixed(1)} ${toY(p.value).toFixed(1)}`).join(" ");

  // Fill area under line
  const fillD = `${pathD} L ${toX(points.length - 1).toFixed(1)} ${(PAD_T + chartH).toFixed(1)} L ${PAD_L} ${(PAD_T + chartH).toFixed(1)} Z`;

  // Y axis labels
  const yTicks = [minV, Math.round((minV + maxV) / 2), maxV];

  // Latest change
  const latest     = timeline[timeline.length - 1];
  const latestDiff = latest?.new_value != null && latest?.old_value != null
    ? latest.new_value - latest.old_value
    : null;

  return (
    <div className="card p-5">
      <div className="flex items-center justify-between mb-4">
        <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px]">
          Booking trend
        </p>
        <div className="flex items-center gap-3">
          {latestDiff != null && latestDiff !== 0 && (
            <span className={`badge ${latestDiff > 0 ? "bg-[#EAF3DE] text-[#3B6D11]" : "bg-[#FCEBEB] text-[#A32D2D]"}`}>
              {latestDiff > 0 ? "+" : ""}{latestDiff} in last update
            </span>
          )}
          <span className="text-[10px] text-muted">{timeline.length} data point{timeline.length !== 1 ? "s" : ""}</span>
        </div>
      </div>

      {/* SVG chart */}
      <div className="overflow-x-auto">
        <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ minWidth: 300 }}>
          {/* Grid lines */}
          {yTicks.map((tick, i) => (
            <g key={i}>
              <line
                x1={PAD_L} y1={toY(tick).toFixed(1)}
                x2={W - PAD_R} y2={toY(tick).toFixed(1)}
                stroke="#f0efeb" strokeWidth="1"
              />
              <text
                x={PAD_L - 4} y={toY(tick)}
                textAnchor="end" dominantBaseline="middle"
                fontSize="9" fill="#aaa"
              >
                {Math.round(tick)}
              </text>
            </g>
          ))}

          {/* Fill */}
          <path d={fillD} fill="#185FA5" fillOpacity="0.08" />

          {/* Line */}
          <path d={pathD} fill="none" stroke="#185FA5" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />

          {/* Data points */}
          {points.map((p, i) => (
            <g key={i}>
              <circle
                cx={toX(i).toFixed(1)} cy={toY(p.value).toFixed(1)}
                r="4" fill="#185FA5" stroke="white" strokeWidth="2"
              />
              {/* Value label above point */}
              <text
                x={toX(i).toFixed(1)} y={(toY(p.value) - 8).toFixed(1)}
                textAnchor="middle" fontSize="9" fill="#185FA5" fontWeight="600"
              >
                {p.value}
              </text>
              {/* Date label below */}
              {p.label && p.label !== "Initial" && (
                <text
                  x={toX(i).toFixed(1)} y={(PAD_T + chartH + 14).toFixed(1)}
                  textAnchor="middle" fontSize="8" fill="#bbb"
                  transform={points.length > 4 ? `rotate(-30, ${toX(i)}, ${PAD_T + chartH + 14})` : ""}
                >
                  {p.label}
                </text>
              )}
            </g>
          ))}

          {/* Total apartments reference line */}
          {totalApartments && totalApartments <= maxV + 5 && (
            <g>
              <line
                x1={PAD_L} y1={toY(totalApartments).toFixed(1)}
                x2={W - PAD_R} y2={toY(totalApartments).toFixed(1)}
                stroke="#A32D2D" strokeWidth="1" strokeDasharray="4 3"
              />
              <text
                x={W - PAD_R + 2} y={toY(totalApartments)}
                dominantBaseline="middle" fontSize="9" fill="#A32D2D"
              >
                Total
              </text>
            </g>
          )}
        </svg>
      </div>

      {/* Change log */}
      <div className="mt-3 pt-3 border-t border-[#f0efeb] space-y-1">
        {timeline.slice(-5).reverse().map((t, i) => {
          const diff = t.new_value != null && t.old_value != null
            ? t.new_value - t.old_value : null;
          return (
            <div key={i} className="flex items-center justify-between text-[11px]">
              <span className="text-muted">{formatDate(t.date)}</span>
              <span className="mono text-ink">
                {t.old_value ?? "—"} → {t.new_value ?? "—"}
              </span>
              {diff != null && (
                <span className={`font-medium ${diff > 0 ? "text-[#3B6D11]" : diff < 0 ? "text-[#A32D2D]" : "text-muted"}`}>
                  {diff > 0 ? "+" : ""}{diff} units
                </span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}