import { useEffect, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { getProject } from "../api";
import StatusBadge from "../components/StatusBadge";
import BookingBar from "../components/BookingBar";
import BookingChart from "../components/BookingChart";
import PricePanel from "../components/PricePanel";
import toast from "react-hot-toast";

function BackIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
      <polyline points="10,3 4,8 10,13"/>
    </svg>
  );
}

function ExternalIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
      <path d="M7 3H3a1 1 0 00-1 1v9a1 1 0 001 1h9a1 1 0 001-1V9"/>
      <path d="M10 2h4v4"/>
      <line x1="14" y1="2" x2="7" y2="9"/>
    </svg>
  );
}

function CalendarIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
      <rect x="1" y="3" width="14" height="12" rx="2"/>
      <line x1="1" y1="7" x2="15" y2="7"/>
      <line x1="5" y1="1" x2="5" y2="5"/>
      <line x1="11" y1="1" x2="11" y2="5"/>
    </svg>
  );
}

function Section({ title, children }) {
  const valid = Array.isArray(children)
    ? children.filter(Boolean)
    : children;
  if (!valid || (Array.isArray(valid) && valid.length === 0)) return null;
  return (
    <div className="card p-5">
      <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px] mb-4">{title}</p>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-x-6 gap-y-4">
        {children}
      </div>
    </div>
  );
}

function Field({ label, value, mono }) {
  if (value == null || value === "") return null;
  return (
    <div>
      <p className="text-[10px] font-medium text-muted uppercase tracking-wide mb-1">{label}</p>
      <p className={`text-[13px] text-ink font-medium ${mono ? "font-mono" : ""}`}>
        {String(value)}
      </p>
    </div>
  );
}

function StatBox({ label, value, color }) {
  return (
    <div className="stat-card text-center">
      <p className={`text-[20px] font-semibold tracking-tight ${color || "text-ink"}`}>
        {value ?? "—"}
      </p>
      <p className="text-[10px] text-muted mt-1 uppercase tracking-wide">{label}</p>
    </div>
  );
}

function DateTag({ label, value, color }) {
  if (!value) return null;
  return (
    <div className={`flex items-center gap-2 px-3 py-2 rounded-lg border ${color || "border-[#e8e6e1] bg-surface"}`}>
      <CalendarIcon />
      <div>
        <p className="text-[10px] text-muted">{label}</p>
        <p className="text-[12px] font-medium text-ink mono">{value}</p>
      </div>
    </div>
  );
}

export default function ProjectDetailPage() {
  const { id }    = useParams();
  const navigate  = useNavigate();
  const [project, setProject] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getProject(id)
      .then(({ data }) => setProject(data))
      .catch(() => toast.error("Project not found"))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return (
      <div className="p-6 md:p-8 space-y-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="card p-5 animate-pulse bg-surface"
            style={{ height: i === 0 ? 96 : 128 }} />
        ))}
      </div>
    );
  }

  if (!project) {
    return (
      <div className="p-8 text-center text-muted text-[13px]">
        Project not found.
      </div>
    );
  }

  return (
    <div className="p-6 md:p-8 space-y-4 max-w-5xl">

      {/* Back */}
      <button onClick={() => navigate(-1)} className="btn text-[12px]">
        <BackIcon /> Back
      </button>

      {/* ── Hero card ── */}
      <div className="card p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="min-w-0">
            <h1 className="text-[18px] font-semibold text-ink tracking-tight leading-snug">
              {project.project_name || "Unnamed Project"}
            </h1>
            <p className="text-[13px] text-muted mt-1">{project.developer || "Unknown developer"}</p>
            <p className="text-[11px] text-muted mono mt-1">
              {project.rera_reg_no || project.project_id}
            </p>
          </div>
          <div className="flex items-center gap-2 shrink-0 flex-wrap">
            <StatusBadge status={project.project_status} />
            {project.details_url && (
              <a href={project.details_url} target="_blank" rel="noreferrer" className="btn text-[11px]">
                <ExternalIcon /> RERA site
              </a>
            )}
          </div>
        </div>

        {/* Booking bar */}
        {project.booking_rate_pct != null && (
          <div className="mt-4 pt-4 border-t border-[#f0efeb] flex items-center gap-4">
            <p className="text-[11px] text-muted uppercase tracking-wide shrink-0">Booking rate</p>
            <BookingBar pct={project.booking_rate_pct} />
          </div>
        )}
      </div>

      {/* ── Key dates row ── */}
      {(project.completion_date || project.extension_completion_date || project.update_date || project.quarter_ending) && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <DateTag
            label="Completion date"
            value={project.completion_date}
          />
          <DateTag
            label="Extension date"
            value={project.extension_completion_date}
            color="border-[#FAEEDA] bg-[#FAEEDA]/30"
          />
          <DateTag
            label="Quarter ending"
            value={project.quarter_ending}
          />
          <DateTag
            label="Last updated"
            value={project.update_date}
            color="border-[#E6F1FB] bg-[#E6F1FB]/30"
          />
        </div>
      )}

      {/* ── Stats row ── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <StatBox label="Total units"      value={project.total_apartments} />
        <StatBox label="Booked"           value={project.apartments_booked}   color="text-[#185FA5]" />
        <StatBox label="Unsold"           value={project.unsold_units}
          color={project.unsold_units > 0 ? "text-[#A32D2D]" : "text-ink"} />
        <StatBox label="Covered parking"  value={project.covered_parking} />
      </div>

      {/* ── Booking trend chart ── */}
      <BookingChart
        projectId={project.project_id}
        totalApartments={project.total_apartments}
      />

      {/* ── Project details ── */}
      <Section title="Project details">
        <Field label="Project type"   value={project.project_type} />
        <Field label="RERA reg no"    value={project.rera_reg_no} mono />
        <Field label="Project ID"     value={project.project_id_display} mono />
        <Field label="Flat sizes"     value={project.flat_size_details} />
        <Field label="Commercial units" value={project.commercial_units} />
      </Section>

      {/* ── Location ── */}
      <Section title="Location">
        <Field label="Address"        value={project.address} />
        <Field label="Pincode"        value={project.pincode}        mono />
        <Field label="District"       value={project.district} />
        <Field label="Police station" value={project.police_station} />
      </Section>

      {/* ── Area & parking ── */}
      <Section title="Area & parking">
        <Field label="Land area (sqm)"       value={project.land_area_sqm} />
        <Field label="Built-up area (sqm)"   value={project.builtup_area_sqm} />
        <Field label="Carpet area (sqm)"     value={project.carpet_area_sqm} />
        <Field label="Avg carpet / apt"      value={project.avg_carpet_area_per_apt_sqm} />
        <Field label="Covered parking"       value={project.covered_parking} />
        <Field label="Basement parking"      value={project.basement_parking} />
        <Field label="Mechanical parking"    value={project.mechanical_parking} />
        <Field label="Parking ratio"         value={project.parking_ratio} />
        <Field label="FSI ratio"             value={project.fsi_builtup_land_ratio} />
      </Section>

      {/* ── Booking details ── */}
      <Section title="Booking details">
        <Field label="Commercial booked"          value={project.commercial_units_booked} />
        <Field label="Covered parking booked"     value={project.covered_parking_booked} />
        <Field label="Basement parking booked"    value={project.basement_parking_booked} />
        <Field label="Mechanical parking booked"  value={project.mechanical_parking_booked} />
      </Section>

      {/* ── Construction status ── */}
      {project.construction_status_summary && (
        <div className="card p-5">
          <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px] mb-3">
            Construction status
          </p>
          <p className="text-[13px] text-ink">{project.construction_status_summary}</p>
          {project.construction_details && (
            <p className="text-[11px] text-muted mt-1">{project.construction_details}</p>
          )}
        </div>
      )}

      {/* ── Common area status ── */}
      {project.common_area_status && (
        <div className="card p-5">
          <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px] mb-3">
            Common area status
          </p>
          <div className="space-y-1.5">
            {project.common_area_status.split(" | ").map((item, i) => (
              <div key={i} className="flex items-start gap-2">
                <span className="w-1 h-1 rounded-full bg-muted mt-2 shrink-0" />
                <p className="text-[12px] text-[#555]">{item}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Market prices ── */}
      <PricePanel
        projectId={project.project_id}
        projectName={project.project_name}
        pincode={project.pincode}
        district={project.district}
      />

    </div>
  );
}