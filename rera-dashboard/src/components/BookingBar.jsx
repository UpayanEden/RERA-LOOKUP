export default function BookingBar({ pct }) {
  if (pct == null) return <span className="text-muted text-xs">—</span>;
  const color =
    pct >= 75 ? "#639922" :
    pct >= 40 ? "#BA7517" : "#E24B4A";
  const textColor =
    pct >= 75 ? "text-[#3B6D11]" :
    pct >= 40 ? "text-[#854F0B]" : "text-[#A32D2D]";
  return (
    <div className="flex items-center gap-2">
      <div className="w-14 h-1.5 bg-[#f0efeb] rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all"
          style={{ width: `${Math.min(pct, 100)}%`, background: color }}
        />
      </div>
      <span className={`text-xs font-medium mono ${textColor}`}>
        {pct.toFixed(1)}%
      </span>
    </div>
  );
}