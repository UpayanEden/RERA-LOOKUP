import axios from "axios";

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || "http://127.0.0.1:8000",
});

api.interceptors.request.use((config) => {
  const token = localStorage.getItem("token");
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

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
export const register = (data)           => api.post("/auth/register", data);
export const login    = (email, password) =>
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

// ── Pincode Favourites ──
export const getFavourites   = ()        => api.get("/favourites");
export const addFavourite    = (pincode) => api.post(`/favourites/${pincode}`);
export const removeFavourite = (pincode) => api.delete(`/favourites/${pincode}`);

// ── Project Favourites ──
export const getProjectFavourites   = ()          => api.get("/favourites/projects");
export const addProjectFavourite    = (projectId) => api.post(`/favourites/projects/${projectId}`);
export const removeProjectFavourite = (projectId) => api.delete(`/favourites/projects/${projectId}`);

// ── Changes ──
export const getChanges        = (params) => api.get("/changes", { params });
export const getChangesSummary = ()       => api.get("/changes/summary");

// ── Prices ──
export const getPrices          = (projectId) => api.get(`/prices/${projectId}`);
export const getPricesByPincode = (pincode, bhk) =>
  api.get(`/prices/pincode/${pincode}`, { params: bhk ? { bhk } : {} });

// ── Map ──
export const getMapProjects   = (params) => api.get("/map/projects", { params });
export const getGeocodeStatus = ()       => api.get("/map/geocode-status");

export const exportPincodeExcel = async (pincode) => {
  const token = localStorage.getItem("token");
  const res   = await fetch(
    `${import.meta.env.VITE_API_URL || "http://127.0.0.1:8000"}/favourites/${pincode}/export`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const blob  = await res.blob();
  const url   = URL.createObjectURL(blob);
  const a     = document.createElement("a");
  a.href      = url;
  a.download  = `RERA_${pincode}.xlsx`;
  a.click();
  URL.revokeObjectURL(url);
};