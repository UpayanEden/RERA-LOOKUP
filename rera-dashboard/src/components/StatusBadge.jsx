export default function StatusBadge({ status }) {
  if (!status) return <span className="text-muted text-xs">—</span>;
  const s = status.toLowerCase();
  let cls = "badge ";
  if (s.includes("complet") && s.includes("cc"))
    cls += "bg-[#EAF3DE] text-[#3B6D11]";
  else if (s.includes("complet"))
    cls += "bg-[#E6F1FB] text-[#185FA5]";
  else if (s.includes("under") || s.includes("progress") || s.includes("ongoing"))
    cls += "bg-[#E6F1FB] text-[#185FA5]";
  else if (s.includes("not start"))
    cls += "bg-[#FAEEDA] text-[#854F0B]";
  else if (s.includes("lapse") || s.includes("expire") || s.includes("cancel"))
    cls += "bg-[#FCEBEB] text-[#A32D2D]";
  else if (s.includes("extend"))
    cls += "bg-[#FAEEDA] text-[#854F0B]";
  else
    cls += "bg-[#f0efeb] text-[#666]";
  return <span className={cls}>{status}</span>;
}