import { createContext, useContext, useState, useEffect } from "react";
import { login as apiLogin, register as apiRegister } from "../api";

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem("user"));
    } catch {
      return null;
    }
  });

  const login = async (email, password) => {
    const { data } = await apiLogin(email, password);
    localStorage.setItem("token", data.access_token);
    localStorage.setItem("user", JSON.stringify({ name: data.name, email: data.email }));
    setUser({ name: data.name, email: data.email });
    return data;
  };

  const register = async (name, email, password) => {
    const { data } = await apiRegister({ name, email, password });
    localStorage.setItem("token", data.access_token);
    localStorage.setItem("user", JSON.stringify({ name: data.name, email: data.email }));
    setUser({ name: data.name, email: data.email });
    return data;
  };

  const logout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("user");
    setUser(null);
  };

  return (
    <AuthContext.Provider value={{ user, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => useContext(AuthContext);
