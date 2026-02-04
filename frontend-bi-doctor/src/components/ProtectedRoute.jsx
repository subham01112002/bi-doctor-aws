import { useEffect, useState } from "react";
import { Navigate } from "react-router-dom";

// const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:9000";

export default function ProtectedRoute({ children }) {
  const [isAuth, setIsAuth] = useState(null); // null = checking

  useEffect(() => {
    fetch(`/bi/auth/me`, {
      credentials: "include",
    })
      .then((res) => {
        console.log("Auth check response:", res.ok);
        setIsAuth(res.ok);
      })
      .catch(() => setIsAuth(false));
  }, []);

  // ⏳ While checking authentication
  if (isAuth === null) {
    return <p style={{ textAlign: "center" }}>Checking authentication...</p>;
  }

  //  Not authenticated → redirect to login
  if (!isAuth) {
    return <Navigate to="/" replace />;
  }

  //  Authenticated → allow access
  return children;
}
