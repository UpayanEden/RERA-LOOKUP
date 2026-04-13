import { useEffect, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { getProject } from "../api";
import StatusBadge from "../components/StatusBadge";
import BookingBar from "../components/BookingBar";
import toast from "react-hot-toast";

function BackIcon() {
  return <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
    <polyline points="10,3 4,8 10,13"/>
  </svg>;
}
function ExternalIcon() {
  return <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
    <path d="M7 3H3a1 1 0 00-1 1v9a1 1 0 001 1h9a1 1 0 001-1V9"/>
    <path d="M10 2h4v4"/><line x1="14" y1="2" x2="7" y2="9"/>
  </svg>;
}

function Section({ title, children }) {
  return (
    <div className="card p-5">
      <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px] mb-4">{title}</p>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-x-6 gap-y-4">{children}</div>
    </div>
  );
}

function Field({ label, value, mono }) {
  if (value == null || value === "") return null;
  return (
    <div>
      <p className="text-[10px] font-medium text-muted uppercase tracking-wide mb-1">{label}</p>
      <p className={`text-[13px] text-ink ${mono ? "font-mono" : "font-medium"}`}>{String(value)}</p>
    </div>
  );
}

function StatBox({ label, value, color }) {
  return (
    <div className="stat-card text-center">
      <p className={`text-[20px] font-semibold tracking-tight ${color || "text-ink"}`}>{value ?? "—"}</p>
      <p className="text-[10px] text-muted mt-1 uppercase tracking-wide">{label}</p>
    </div>
  );
}

export default function ProjectDetailPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [project, setProject] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getProject(id)
      .then(({ data }) => setProject(data))
      .catch(() => toast.error("Project not found"))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) return (
    <div className="p-6 md:p-8 space-y-4">
      {Array.from({ length: 3 }).map((_, i) => (
        <div key={i} className="card p-5 h-32 animate-pulse bg-surface" />
      ))}
    </div>
  );

  if (!project) return (
    <div className="p-8 text-center text-muted text-[13px]">Project not found.</div>
  );

  return (
    <div className="p-6 md:p-8 space-y-4 max-w-5xl">
      <button onClick={() => navigate(-1)} className="btn text-[12px]">
        <BackIcon /> Back
      </button>

      {/* Hero card */}
      <div className="card p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="min-w-0">
            <h1 className="text-[18px] font-semibold text-ink tracking-tight">
              {project.project_name || "Unnamed Project"}
            </h1>
            <p className="text-[13px] text-muted mt-1">{project.developer || "Unknown developer"}</p>
            <p className="text-[11px] text-muted mono mt-1">{project.rera_reg_no || project.project_id}</p>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <StatusBadge status={project.project_status} />
            {project.details_url && (
              <a href={project.details_url} target="_blank" rel="noreferrer" className="btn text-[11px]">
                <ExternalIcon /> RERA site
              </a>
            )}
          </div>
        </div>

        {/* Inline booking bar */}
        {project.booking_rate_pct != null && (
          <div className="mt-4 pt-4 border-t border-[#f0efeb] flex items-center gap-4">
            <p className="text-[11px] text-muted uppercase tracking-wide">Booking rate</p>
            <BookingBar pct={project.booking_rate_pct} />
          </div>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <StatBox label="Total units"  value={project.total_apartments} />
        <StatBox label="Booked"       value={project.apartments_booked}   color="text-[#185FA5]" />
        <StatBox label="Unsold"       value={project.unsold_units}         color={project.unsold_units > 0 ? "text-[#A32D2D]" : "text-ink"} />
        <StatBox label="Parking"      value={project.covered_parking} />
      </div>

      {/* Detail sections */}
      <Section title="Project details">
        <Field label="Project type"    value={project.project_type} />
        <Field label="Completion date" value={project.completion_date} />
        <Field label="Extension date"  value={project.extension_completion_date} />
        <Field label="Quarter ending"  value={project.quarter_ending} />
        <Field label="Last updated"    value={project.update_date} />
        <Field label="Flat sizes"      value={project.flat_size_details} />
      </Section>

      <Section title="Location">
        <Field label="Address"         value={project.address} />
        <Field label="Pincode"         value={project.pincode} mono />
        <Field label="District"        value={project.district} />
        <Field label="Police station"  value={project.police_station} />
      </Section>

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

      <Section title="Booking details">
        <Field label="Commercial units"          value={project.commercial_units} />
        <Field label="Commercial booked"         value={project.commercial_units_booked} />
        <Field label="Covered parking booked"    value={project.covered_parking_booked} />
        <Field label="Basement parking booked"   value={project.basement_parking_booked} />
        <Field label="Mechanical parking booked" value={project.mechanical_parking_booked} />
      </Section>

      {project.construction_status_summary && (
        <div className="card p-5">
          <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px] mb-3">Construction status</p>
          <p className="text-[13px] text-ink">{project.construction_status_summary}</p>
          {project.construction_details && (
            <p className="text-[11px] text-muted mt-1">{project.construction_details}</p>
          )}
        </div>
      )}

      {project.common_area_status && (
        <div className="card p-5">
          <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px] mb-3">Common area status</p>
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
    </div>
  );
}