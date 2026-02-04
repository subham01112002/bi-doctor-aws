import { useState,useEffect, useRef  } from "react";
import { NavLink, Outlet } from "react-router-dom";
import "../css/DashboardShell.css";
import metaIcon from "./images/ME_logo.png";
import deployIcon from "./images/DA_logo.png";
import { useNavigate } from "react-router-dom";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faArrowRightFromBracket } from "@fortawesome/free-solid-svg-icons";
// import { Database, UploadCloud } from "lucide-react";
 
// const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

// Helper function to get cookie by name with proper formatting by cleaning
function getCookie(name) {
  const value = `; ${document.cookie}`;
  const parts = value.split(`; ${name}=`);

  if (parts.length === 2) {
    const cookieValue = parts.pop().split(";").shift();
    return cookieValue?.replace(/^"(.*)"$/, "$1");
  }
  return null;
}
function getInitialsFromEmail(email) {
  if (!email) return "U";

  const namePart = email.split("@")[0]; // subham.patra
  const parts = namePart.split(/[._]/); // ["subham", "patra"]

  const first = parts[0]?.[0] || "";
  const second = parts[1]?.[0] || "";

  return (first + second).toUpperCase();
}

export default function DashboardShell() {
  const [expanded, setExpanded] = useState(true);
  const [logoutOpen, setLogoutOpen] = useState(false);
  const [username, setUsername] = useState("User");
  const settingsRef = useRef(null);
  const [loggingOut, setLoggingOut] = useState(false);
  const [theme, setTheme] = useState("light");

  const navigate = useNavigate();

  // Load theme from localStorage on mount
  useEffect(() => {
    const savedTheme = localStorage.getItem("theme") || "light";
    setTheme(savedTheme);
    document.documentElement.setAttribute("data-theme", savedTheme);
  }, []);
  const toggleTheme = () => {
    const nextTheme = theme === "light" ? "dark" : "light";
      setTheme(nextTheme);
      document.documentElement.setAttribute("data-theme", nextTheme);
      localStorage.setItem("theme", nextTheme);
  };
  // Get username from cookie on mount
  useEffect(() => {
    const storedUsername = getCookie("username");
    if (storedUsername) {
      setUsername(storedUsername);
    }
  }, []);

  // Close logout popover when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (
        settingsRef.current &&
        !settingsRef.current.contains(event.target)
      ) {
        setLogoutOpen(false);
      }
    };

    if (logoutOpen) {
      document.addEventListener("mousedown", handleClickOutside);
    }

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [logoutOpen]);

    // Handle sign out
    const handleSignOut = async () => {
      if (loggingOut) return;

      setLoggingOut(true);
      try {
      await fetch("/bi/auth/logout", {
        method: "POST",
        credentials: "include",
      });
    } catch (err) {
      console.error(err);
    } finally {
      navigate("/", { replace: true });
    }
  };

  return (
    <div className="dashboard-container">
      <aside className={`sidebar ${expanded ? "expanded" : "collapsed"}`}>
        <div className="sidebar-header">
          <button className="sidebar-toggle" onClick={() => setExpanded(!expanded)}>
            <div className="icon-wrapper">
              {expanded ? "✕" : "☰"}
            </div>
          </button>
          <span className="sidebar-title">BI Doctor</span>
        </div>

        <nav className="sidebar-nav">
          <NavLink
            to="SourceIQ"
            title={!expanded ? "SourceIQ" : undefined}
            className={({ isActive }) => isActive ? "active" : ""}
          >
            <div className="menu-item">
              <img src={metaIcon} alt="Meta Extractor" className="nav-icon" />
              {/* <Database size={20} /> */}
            </div>
            <span className="nav-text">SourceIQ</span>
          </NavLink>

          <NavLink
            to="PushOps"
            title={!expanded ? "PushOps" : undefined}
            className={({ isActive }) =>
              `nav-link ${isActive ? "active" : ""}`
            }
          >
            <div className="menu-item">
              <img src={deployIcon} alt="Deploy Assist" className="nav-icon" />
              {/* <UploadCloud size={20} /> */}
            </div>
            <span className="nav-text">PushOps</span>
          </NavLink>
        </nav>

        {/* <div className="sidebar-footer">
          <div className="icon-wrapper">⚙️</div>
          <span className="nav-text">Settings & Help</span>
        </div> */}

        {/* FOOTER WITH LOGOUT */}
        <div className="sidebar-footer" ref={settingsRef}>
        <button
          className="footer-trigger"
          onClick={() => setLogoutOpen((p) => !p)}
          type="button"
        >
          <div className="icon-wrapper">⚙️</div>
          <span className="nav-text">Settings</span>
        </button>
 
        {logoutOpen && (
          <div  className={`account-popover ${expanded ? "expanded" : "collapsed"}`}>
            {/* User header */}
            <div className="account-user">
              <div className="account-avatar">{getInitialsFromEmail(username)}</div>
              <div>
                <div className="account-name">{username}</div>
                {/* <div className="account-username">{username}</div> */}
              </div>
            </div>
            {/* <button className="account-item" onClick={toggleTheme}>
              {theme === "light" ? "Dark Mode" : "Light Mode"}
            </button> */}
            <div className="account-divider" />
 
            {/* Logout */}
            <button className="account-item logout"
            onClick={handleSignOut}
            disabled={loggingOut}>
              <FontAwesomeIcon icon={faArrowRightFromBracket} />
              {loggingOut ? "Logging out..." : "Logout"}
            </button>
          </div>
        )}
        </div>
      </aside>

      <main className={`main-content ${expanded ? "shifted" : ""}`}>
        <Outlet />
      </main>
    </div>
  );
}