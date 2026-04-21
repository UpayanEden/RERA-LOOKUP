import { Outlet, NavLink, useNavigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import { useState } from "react";

const GridIcon = () => (
  <svg width="15" height="15" viewBox="0 0 16 16" fill="currentColor">
    <rect x="1" y="1" width="6" height="6" rx="1.5"/>
    <rect x="9" y="1" width="6" height="6" rx="1.5"/>
    <rect x="1" y="9" width="6" height="6" rx="1.5"/>
    <rect x="9" y="9" width="6" height="6" rx="1.5"/>
  </svg>
);
const StarIcon = () => (
  <svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
    <polygon points="8,2 10,6 14,6.5 11,9.5 11.8,14 8,12 4.2,14 5,9.5 2,6.5 6,6"/>
  </svg>
);
const MapIcon = () => (
  <svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
    <polygon points="1,3 5,1 11,3 15,1 15,13 11,15 5,13 1,15"/>
    <line x1="5" y1="1" x2="5" y2="13"/>
    <line x1="11" y1="3" x2="11" y2="15"/>
  </svg>
);
const ActivityIcon = () => (
  <svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
    <polyline points="1,11 5,7 8,9 11,4 15,6"/>
  </svg>
);
const LogoutIcon = () => (
  <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
    <path d="M6 2H3a1 1 0 00-1 1v10a1 1 0 001 1h3"/>
    <polyline points="11,11 14,8 11,5"/>
    <line x1="6" y1="8" x2="14" y2="8"/>
  </svg>
);
const MenuIcon = () => (
  <svg width="18" height="18" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
    <line x1="2" y1="4" x2="14" y2="4"/>
    <line x1="2" y1="8" x2="14" y2="8"/>
    <line x1="2" y1="12" x2="14" y2="12"/>
  </svg>
);

const navItems = [
  { to: "/projects",   label: "Projects",   icon: GridIcon,     section: "Explore" },
  { to: "/favourites", label: "Favourites", icon: StarIcon,     section: null },
  { to: "/map",        label: "Map",        icon: MapIcon,      section: null },
  { to: "/changes",    label: "Changes",    icon: ActivityIcon, section: "Monitor" },
];

function Sidebar({ onClose }) {
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate("/login");
  };

  return (
    <aside className="flex flex-col h-full bg-white border-r border-border w-[220px] shrink-0">
      {/* Logo */}
      <div className="px-[18px] pt-5 pb-4 border-b border-border">
        <div className="w-7 h-7 bg-ink rounded-lg flex items-center justify-center mb-2.5">
          <svg width="13" height="13" viewBox="0 0 16 16" fill="white">
            <rect x="2" y="2" width="5" height="5" rx="1"/>
            <rect x="9" y="2" width="5" height="5" rx="1"/>
            <rect x="2" y="9" width="5" height="5" rx="1"/>
            <rect x="9" y="9" width="5" height="5" rx="1"/>
          </svg>
        </div>
        <p className="text-[13px] font-semibold text-ink tracking-tight">WB-RERA</p>
        <p className="text-[11px] text-muted mt-0.5">Project Registry</p>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2.5 py-3">
        {navItems.map(({ to, label, icon: Icon, section }) => (
          <div key={to}>
            {section && (
              <p className="text-[10px] font-semibold text-[#bbb] tracking-[0.8px] uppercase px-2.5 pt-3 pb-1.5">
                {section}
              </p>
            )}
            <NavLink
              to={to}
              onClick={onClose}
              className={({ isActive }) =>
                `flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-[13px] font-medium transition-colors mb-0.5 ${
                  isActive
                    ? "bg-surface-hover text-ink"
                    : "text-[#666] hover:bg-surface hover:text-ink"
                }`
              }
            >
              <Icon />
              {label}
            </NavLink>
          </div>
        ))}
      </nav>

      {/* User */}
      <div className="px-3.5 py-3.5 border-t border-border">
        <div className="flex items-center gap-2.5 mb-2.5">
          <div className="w-7 h-7 rounded-full bg-ink flex items-center justify-center text-white text-[11px] font-semibold shrink-0">
            {user?.name?.[0]?.toUpperCase()}
          </div>
          <div className="min-w-0">
            <p className="text-[12px] font-medium text-ink truncate">{user?.name}</p>
            <p className="text-[11px] text-muted truncate">{user?.email}</p>
          </div>
        </div>
        <button
          onClick={handleLogout}
          className="flex items-center gap-2 text-[11px] text-muted hover:text-[#A32D2D] transition-colors w-full px-1"
        >
          <LogoutIcon /> Sign out
        </button>
      </div>
    </aside>
  );
}

export default function Layout() {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Desktop sidebar */}
      <div className="hidden md:flex">
        <Sidebar onClose={() => {}} />
      </div>

      {/* Mobile overlay */}
      {sidebarOpen && (
        <div className="fixed inset-0 z-40 md:hidden">
          <div className="absolute inset-0 bg-black/30" onClick={() => setSidebarOpen(false)} />
          <div className="absolute left-0 top-0 h-full z-50 shadow-xl">
            <Sidebar onClose={() => setSidebarOpen(false)} />
          </div>
        </div>
      )}

      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Mobile header */}
        <header className="md:hidden flex items-center gap-3 px-4 py-3 bg-white border-b border-border">
          <button onClick={() => setSidebarOpen(true)} className="text-muted hover:text-ink">
            <MenuIcon />
          </button>
          <p className="text-[13px] font-semibold">WB-RERA</p>
        </header>

        <main className="flex-1 overflow-y-auto bg-surface">
          <Outlet />
        </main>
      </div>
    </div>
  );
}