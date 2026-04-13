import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import toast from "react-hot-toast";

export default function RegisterPage() {
  const { register } = useAuth();
  const navigate = useNavigate();
  const [form, setForm] = useState({ name: "", email: "", password: "" });
  const [loading, setLoading] = useState(false);
  const set = (f) => (e) => setForm({ ...form, [f]: e.target.value });

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (form.password.length < 6) { toast.error("Password must be at least 6 characters"); return; }
    setLoading(true);
    try {
      await register(form.name, form.email, form.password);
      toast.success("Account created");
      navigate("/projects");
    } catch (err) {
      toast.error(err.response?.data?.detail || "Registration failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-surface flex items-center justify-center px-4">
      <div className="w-full max-w-sm">
        <div className="flex items-center gap-2.5 mb-8">
          <div className="w-8 h-8 bg-ink rounded-lg flex items-center justify-center">
            <svg width="14" height="14" viewBox="0 0 16 16" fill="white">
              <rect x="2" y="2" width="5" height="5" rx="1"/>
              <rect x="9" y="2" width="5" height="5" rx="1"/>
              <rect x="2" y="9" width="5" height="5" rx="1"/>
              <rect x="9" y="9" width="5" height="5" rx="1"/>
            </svg>
          </div>
          <div>
            <p className="text-[14px] font-semibold text-ink">WB-RERA Dashboard</p>
            <p className="text-[11px] text-muted">Create your account</p>
          </div>
        </div>
        <div className="card p-6">
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="block text-[11px] font-medium text-muted mb-1.5 uppercase tracking-wide">Full name</label>
              <input type="text" required className="input" placeholder="Upayan" value={form.name} onChange={set("name")} />
            </div>
            <div>
              <label className="block text-[11px] font-medium text-muted mb-1.5 uppercase tracking-wide">Email</label>
              <input type="email" required className="input" placeholder="you@example.com" value={form.email} onChange={set("email")} />
            </div>
            <div>
              <label className="block text-[11px] font-medium text-muted mb-1.5 uppercase tracking-wide">Password</label>
              <input type="password" required className="input" placeholder="Min. 6 characters" value={form.password} onChange={set("password")} />
            </div>
            <button type="submit" disabled={loading} className="btn-primary w-full justify-center py-2 text-[13px] mt-1">
              {loading ? "Creating account..." : "Create account"}
            </button>
          </form>
        </div>
        <p className="text-[12px] text-center text-muted mt-5">
          Already have an account?{" "}
          <Link to="/login" className="text-ink font-medium hover:underline">Sign in</Link>
        </p>
      </div>
    </div>
  );
}