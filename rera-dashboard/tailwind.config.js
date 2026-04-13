/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["'DM Sans'", "sans-serif"],
        mono: ["'DM Mono'", "monospace"],
      },
      colors: {
        ink:    "#1a1a1a",
        muted:  "#888",
        border: "#e8e6e1",
        surface:"#f8f7f4",
        "surface-hover": "#f0efeb",
      },
    },
  },
  plugins: [],
};