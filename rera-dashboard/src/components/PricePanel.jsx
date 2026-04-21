import { useState } from "react";

function ExternalIcon() {
  return (
    <svg width="11" height="11" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
      <path d="M7 3H3a1 1 0 00-1 1v9a1 1 0 001 1h9a1 1 0 001-1V9"/>
      <path d="M10 2h4v4"/><line x1="14" y1="2" x2="7" y2="9"/>
    </svg>
  );
}

function Portal99AcresIcon() {
  return (
    <div style={{ width: 20, height: 20, background: "#E8421A", borderRadius: 4, display: "flex", alignItems: "center", justifyContent: "center" }}>
      <span style={{ color: "white", fontSize: 9, fontWeight: 700 }}>99</span>
    </div>
  );
}

function HousingIcon() {
  return (
    <div style={{ width: 20, height: 20, background: "#0066FF", borderRadius: 4, display: "flex", alignItems: "center", justifyContent: "center" }}>
      <span style={{ color: "white", fontSize: 8, fontWeight: 700 }}>H</span>
    </div>
  );
}

function MagicBricksIcon() {
  return (
    <div style={{ width: 20, height: 20, background: "#E8421A", borderRadius: 4, display: "flex", alignItems: "center", justifyContent: "center" }}>
      <span style={{ color: "white", fontSize: 7, fontWeight: 700 }}>MB</span>
    </div>
  );
}

function buildUrls(projectName, pincode, district) {
  const encoded = encodeURIComponent(projectName);
  const city    = (district || "kolkata").toLowerCase().replace(/\s+/g, "-");

  return [
    {
      portal: "99acres",
      label:  "99acres",
      icon:   <Portal99AcresIcon />,
      color:  "#E8421A",
      bg:     "#FEF2EE",
      urls: [
        {
          label: "Search by project name",
          url:   `https://www.99acres.com/search/property/buy/residential?keyword=${encoded}&pincode=${pincode}&intent=BUY`,
        },
        {
          label: "Search by pincode",
          url:   `https://www.99acres.com/search/property/buy/residential?pincode=${pincode}&intent=BUY`,
        },
      ],
    },
    {
      portal: "housing",
      label:  "Housing.com",
      icon:   <HousingIcon />,
      color:  "#0066FF",
      bg:     "#EEF4FF",
      urls: [
        {
          label: "Search by project name",
          url:   `https://housing.com/in/buy/residential/${city}?q=${encoded}`,
        },
        {
          label: "Search by pincode",
          url:   `https://housing.com/in/buy/residential/${city}?pincode=${pincode}`,
        },
      ],
    },
    {
      portal: "magicbricks",
      label:  "MagicBricks",
      icon:   <MagicBricksIcon />,
      color:  "#E8421A",
      bg:     "#FEF2EE",
      urls: [
        {
          label: "Search by project name",
          url:   `https://www.magicbricks.com/property-for-sale/residential-real-estate?proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment&cityName=Kolkata&keyword=${encoded}`,
        },
        {
          label: "Search by pincode",
          url:   `https://www.magicbricks.com/property-for-sale/residential-real-estate?proptype=Multistorey-Apartment,Builder-Floor-Apartment&cityName=Kolkata&pincode=${pincode}`,
        },
      ],
    },
  ];
}

export default function PricePanel({ projectId, projectName, pincode, district }) {
  const [expanded, setExpanded] = useState(false);

  if (!projectName || !pincode) return null;

  const portals = buildUrls(projectName, pincode, district);

  return (
    <div className="card p-5">
      <div className="flex items-center justify-between mb-1">
        <p className="text-[10px] font-semibold text-muted uppercase tracking-[0.8px]">
          Market prices
        </p>
        <button
          onClick={() => setExpanded(e => !e)}
          className="btn text-[11px]"
        >
          {expanded ? "Hide links" : "View on portals"}
          <svg width="10" height="10" viewBox="0 0 10 10" fill="currentColor"
            style={{ transform: expanded ? "rotate(180deg)" : "none", transition: "transform 0.15s" }}>
            <path d="M1 3l4 4 4-4z"/>
          </svg>
        </button>
      </div>

      <p className="text-[11px] text-muted mb-3">
        Live prices from 99acres, Housing.com and MagicBricks
      </p>

      {expanded && (
        <div className="space-y-3 mt-3 pt-3 border-t border-[#f0efeb]">
          {portals.map((p) => (
            <div key={p.portal} className="rounded-lg border border-[#f0efeb] overflow-hidden">
              {/* Portal header */}
              <div className="flex items-center gap-2.5 px-3 py-2.5 border-b border-[#f0efeb]"
                style={{ background: p.bg }}>
                {p.icon}
                <p className="text-[12px] font-semibold text-ink">{p.label}</p>
              </div>

              {/* Links */}
              <div className="divide-y divide-[#f8f7f4]">
                {p.urls.map((link, i) => (
                  <a
                    key={i}
                    href={link.url}
                    target="_blank"
                    rel="noreferrer"
                    className="flex items-center justify-between px-3 py-2.5 hover:bg-surface transition-colors group"
                  >
                    <span className="text-[12px] text-[#555] group-hover:text-ink transition-colors">
                      {link.label}
                    </span>
                    <div className="flex items-center gap-1.5 shrink-0">
                      <span className="text-[10px] text-muted font-mono truncate max-w-[180px]">
                        {new URL(link.url).hostname}
                      </span>
                      <ExternalIcon />
                    </div>
                  </a>
                ))}
              </div>
            </div>
          ))}

          <p className="text-[10px] text-muted pt-1">
            Links open in a new tab — search results are live and not stored.
          </p>
        </div>
      )}
    </div>
  );
}