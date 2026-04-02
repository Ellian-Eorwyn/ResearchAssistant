import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        surface: "#090e1a",
        "surface-container-lowest": "#000000",
        "surface-container-low": "#0c1324",
        "surface-container": "#0f192f",
        "surface-container-high": "#111f3b",
        "surface-container-highest": "#142547",
        "surface-bright": "#162b54",
        "surface-variant": "#142547",
        primary: "#a6c8ff",
        "primary-dim": "#8dbbff",
        tertiary: "#8895ff",
        "on-surface": "#dde5ff",
        "on-surface-variant": "#9baad5",
        outline: "#65759c",
        "outline-variant": "#38476c",
        error: "#ee7d77",
        warning: "#f6b85f",
        success: "#5ed4a8",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "monospace"],
      },
      borderRadius: {
        sm: "0.125rem",
        md: "0.25rem",
        lg: "0.5rem",
      },
      boxShadow: {
        ambient: "0 12px 24px rgba(221,229,255,0.06)",
      },
      backgroundImage: {
        "primary-gradient": "linear-gradient(135deg, #a6c8ff, #8dbbff)",
      },
      keyframes: {
        pulseSoft: {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.5" },
        },
      },
      animation: {
        "pulse-soft": "pulseSoft 1.3s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};

export default config;
