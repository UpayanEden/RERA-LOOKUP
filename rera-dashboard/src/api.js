import axios from "axios";

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || "http://127.0.0.1:8000",
});

// Attach JWT token to every request automatically
api.interceptors.request.use((config) => {
  const token = localStorage.getItem("token");
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// Redirect to login on 401
api.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err.response?.status === 401) {
      localStorage.removeItem("token");
      localStorage.removeItem("user");
      window.location.href = "/login";
    }
    return Promise.reject(err);
  }
);

// ── Auth ──
export const register = (data) => api.post("/auth/register", data);
export const login = (email, password) =>
  api.post(
    "/auth/login",
    new URLSearchParams({ username: email, password }),
    { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
  );
export const getMe = () => api.get("/auth/me");

// ── Projects ──
export const getProjects = (params) => api.get("/projects", { params });
export const getProject  = (id)     => api.get(`/projects/${id}`);
export const getFilters  = ()       => api.get("/projects/meta/filters");

// ── Favourites ──
export const getFavourites    = ()        => api.get("/favourites");
export const addFavourite     = (pincode) => api.post(`/favourites/${pincode}`);
export const removeFavourite  = (pincode) => api.delete(`/favourites/${pincode}`);

// ── Changes ──
export const getChanges = (params) => api.get("/changes", { params });
export const getChangesSummary = () => api.get("/changes/summary");
