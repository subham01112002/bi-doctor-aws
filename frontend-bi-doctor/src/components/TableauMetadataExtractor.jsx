import React, { useEffect, useState, useRef } from "react";
import logo from "./images/exavalu-logo.png";
import "../css/TableauMetadataExtractor.css";
import { useNavigate } from "react-router-dom";
// import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
// import { faArrowRightFromBracket } from "@fortawesome/free-solid-svg-icons";


// const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";
export default function TableauMetadataExtractor() {
  const [biTool, setBiTool] = useState("Tableau");
  const [reports, setReports] = useState([]);
  const [dsList, setDsList] = useState([]);
  const [projects, setProjects] = useState([]);
  const [selectedProject, setSelectedProject] = useState("");
  const [selectedProjectVizportalUrlId, setSelectedProjectVizportalUrlId] = useState("");
  const [selectedWorkbooks, setSelectedWorkbooks] = useState([]);
  const [selectedDatasources, setSelectedDatasources] = useState([]);
  const [projectOpen, setProjectOpen] = useState(false);
  const [projectSearch, setProjectSearch] = useState("");
  const allSelected = reports.length > 0 && selectedWorkbooks.length === reports.length;
  const allSelectedDs = dsList.length > 0 && selectedDatasources.length === dsList.length;
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();
  const projectRef = useRef(null);
  // const [logoutOpen, setLogoutOpen] = useState(false);
  const [metadataReady, setMetadataReady] = useState(false);
  // const SOURCE_IQ_DASHBOARD_URL = "https://us-west-2b.online.tableau.com/t/exavalu/views/WorkbookSummary_17683044081920/WorkbookSummary";

  const clearFilter = () => {
    setBiTool("Tableau");
    setSelectedProject("");
    setReports([]);
    setDsList([]);
    setSelectedWorkbooks([]);
    setSelectedDatasources([]);
    setMetadataReady(false);
  };

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (projectOpen && projectRef.current && !projectRef.current.contains(event.target)) {
        setProjectOpen(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [projectOpen]);


  useEffect(() => {
  setMetadataReady(false);
}, [selectedProject]);
  
  useEffect(() => {
  fetch(`/bi/tableau/projects`, {
    credentials: "include",
  })
    .then((res) => {
      if (!res.ok) throw new Error("Failed to load projects");
      return res.json();
    })
    .then((data) => {
      console.log("Projects from API:", data); 
      setProjects(data); //  array
    })
    .catch((err) => console.error(err));
  }, []);

  //for select workbooks based on project
  useEffect(() => {
  if (!selectedProject) {
    setReports([]);
    return;
  }

  fetch(`/bi/tableau/workbooks?project_luid=${selectedProject}`, {
    credentials: "include",
  })
    .then(res => res.json())
    .then(data => {
      console.log("Filtered workbooks:", data);
      setReports(data);
      setSelectedWorkbooks([]);
    })
    .catch(console.error);
}, [selectedProject]);
  
useEffect(() => {
  if(!selectedProjectVizportalUrlId){
    setDsList([]);
    return;
  }
  fetch(`/bi/tableau/datasources?project_vizportal_url_id=${selectedProjectVizportalUrlId}`, {
      credentials: "include",
    })
      .then(res => res.json())
      .then(data => {
        console.log("Filtered datasources:", data);
        setDsList(data);
        setSelectedDatasources([]);
      })
      .catch(console.error);
    }, [selectedProjectVizportalUrlId]);

const waitForExcel = async (sessionKey) => {
  const maxAttempts = 90;          // 90 × 10s = 15 minutes
  const intervalMs = 10000;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    await new Promise((r) => setTimeout(r, intervalMs));

    const res = await fetch(
      `/bi/tableau/excel/status?session_key=${sessionKey}`,
      { credentials: "include" }
    );

    if (!res.ok) {
      console.warn("Status check failed, retrying...");
      continue;
    }

    const data = await res.json();

    if (data.status === "completed") {
      return data.download_url;   // S3 / presigned URL
    }

    if (data.status === "failed") {
      throw new Error(data.message || "Excel generation failed");
    }

    console.log("Excel still processing...");
  }

  throw new Error("Excel generation timed out");
};    
const handleDownloadMetadata = async () => {
  // if (selectedWorkbooks.length === 0) {
  //   alert("Please select at least one workbook");
  //   return;
  // }
  // if(selectedDatasources.length === 0){
  //   alert("Please select at least one datasource");
  //   return;
  // }
  setLoading(true);
 
  try {
    // Generate unique session key
    const sessionKey = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    console.log("Session Key:", sessionKey);

    const workbook_luids = selectedWorkbooks.map(wb => wb.luid);
    const workbook_ids = selectedWorkbooks.map(wb => wb.id);
    console.log("Selected Workbook LUIDs:", workbook_luids);
    console.log("Selected Workbook IDs:", workbook_ids);
    const res = await fetch(`/bi/tableau/workbook_metadata`, {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        workbook_ids,
        workbook_luids,
        session_key: sessionKey  // Pass session key
      }),
    });
 
    if (!res.ok) {
      throw new Error("Failed to fetch workbook metadata");
    }
 
    const workbookData = await res.json();
    console.log("Workbook Metadata:", workbookData);

    // Step 2: Fetch datasource metadata
    const datasource_luids = selectedDatasources.map(ds => ds.luid);
    const datasource_ids = selectedDatasources.map(ds => ds.id);
    console.log("Selected Datasource LUIDs:", datasource_luids);
    console.log("Selected Datasource IDs:", datasource_ids);
    const dsres =  await fetch(`/bi/tableau/datasource_metadata`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ datasource_ids , datasource_luids, session_key: sessionKey  }), // Pass same session key
    });
    if (!dsres.ok) {
      throw new Error("Failed to fetch datasource metadata");
    }
    const datasourceData = await dsres.json();
    console.log("Datasource Metadata:", datasourceData);

    // Step 3: Generate combined Excel file
    const excelRes = await fetch(`/bi/tableau/generate_combined_excel`, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
          session_key: sessionKey
      }),
    });
    
    if (!excelRes.ok) {
      throw new Error("Failed to generate combined Excel");
    }
    
    // -----------------------------
    // Step 4: POLLING (NEW)
    // -----------------------------
    const downloadUrl = await waitForExcel(sessionKey);

    // Trigger download
    // window.location.href = downloadUrl;

    alert("Metadata downloaded successfully!");
    setMetadataReady(true);

    // Step 2 will start from here
  } catch (err) {
    console.error(err);
    alert("Error downloading metadata");
  } finally {
    setLoading(false);
  }
};
const [showSourceIQ, setShowSourceIQ] = useState(false);

// const handleLaunchSourceIQ = () => {
//   window.open(
//     SOURCE_IQ_DASHBOARD_URL,
//     "_blank",
//     "noopener,noreferrer"
//   );
// };
const TABLEAU_SERVER_URL = "https://us-west-2b.online.tableau.com"

  // const [showSourceIQ, setShowSourceIQ] = useState(false);
  const [jwtToken, setJwtToken] = useState("");
  const vizRef = useRef(null);
  // Load Tableau Embedding API srcript in haed tag to use tableau-viz custom element
  useEffect(() => {
    const script = document.createElement('script');
    script.src = 'https://us-west-2b.online.tableau.com/javascripts/api/tableau.embedding.3.latest.min.js';
    script.type = 'module';
    script.async = true;
    
    script.onload = () => {
      console.log('✅ Tableau Embedding API loaded');
    };
    
    document.head.appendChild(script);

    return () => {
      if (document.head.contains(script)) {
        document.head.removeChild(script);
      }
    };
  }, []);

  const handleLaunchSourceIQ = async () => {
    try {
      console.log("🚀 Authenticating with Tableau...");

      const response = await fetch(`/bi/auth/tableau-auth`, {
        method: "POST",
        credentials: "include",
      });

      if (!response.ok) {
        const err = await response.json();
        throw new Error(err.detail || "Auth failed");
      }

      const data = await response.json();
      console.log("✅ Got JWT token");

      // Set JWT token and show the viz
      setJwtToken(data.jwt_token);
      setShowSourceIQ(true);
      // ✅ Store JWT temporarily
      sessionStorage.setItem("tableau_jwt", data.jwt_token);

      // ✅ Open dashboard in new tab
      window.open(`${window.location.origin}/sourceiq`, "_blank");

    } catch (err) {
      console.error("❌ Tableau authentication error:", err);
      alert(err.message);
    }
  };
const handleSignOut = async () => {
  try {
    await fetch(`/bi/auth/logout`, {
      method: "POST",
      credentials: "include",
    })
    .then((res) => {
      if (res.ok) {
        navigate("/", { replace: true });
      }
    })
    .catch(() => {
      alert("Logout failed");
    });
  } catch (err) {
    console.error(err);
    alert("Logout failed");
  }
};


  return (
    <div className="metadata-root">
      <div className="metadata-card">
        {/* LOGO */}
        <div className="metadata-logo">
          <img src={logo} alt="Logo" />
        </div>

        {/* BI TOOL */}
        <div>
          <div className="field-row">
            <label className="field-label">BI Tool</label>

            <button
              className="clear-btn"
              onClick={clearFilter}
              type="button"
            >
              Clear Filter
            </button>
          </div>

          <div className="select-wrap bi-tool-select">
            <select
              className="select-full"
              value={biTool ?? ""}
              onChange={(e) => setBiTool(e.target.value)}
            >
              <option value="Tableau">Tableau</option>
            </select>
          </div>
        </div>

        {/* PROJECTS */}
        <div className="projects">
          <label className="field-label">Projects</label>

          <div ref={projectRef} className={`dropdown ${projectOpen ? "open" : ""}`}>
            {/* Trigger */}
            <div
              className="dropdown-control"
              onClick={() => setProjectOpen((o) => !o)}
            >
              <span>
                {projects.find(p => p.project_luid === selectedProject)?.project_name || "Select project"}
              </span>
              <span className="dropdown-arrow" />
            </div>

            {/* Dropdown panel */}
            {projectOpen && (
              <div className="dropdown-menu">
                <input
                  type="text"
                  className="dropdown-search"
                  placeholder="Search projects..."
                  value={projectSearch}
                  onChange={(e) => setProjectSearch(e.target.value)}
                  autoFocus
                />

                <div className="dropdown-list">
                  {projects
                    .filter(p =>
                      p.project_name.toLowerCase().includes(projectSearch.toLowerCase())
                    )
                    .map((p) => (
                      <div
                        key={p.project_luid}
                        className="dropdown-item"
                        onClick={() => {
                          setSelectedProject(p.project_luid);
                          setSelectedProjectVizportalUrlId(p.projectvizporturl_id);
                          setProjectOpen(false);
                          setProjectSearch("");
                        }}
                      >
                        {p.project_name}
                      </div>
                    ))}

                  {projects.length === 0 && (
                    <div className="dropdown-empty">No projects</div>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>

        {/* REPORTS */}
        <div className="reports">
          <div className="reports-split">
            {/* LEFT: WORKBOOKS */}
            <div className="reports-col">
              <label className="label">Workbooks</label>
              
              <div className="reports-list">
                {/* SELECT ALL */}
                {reports.length > 0 && (<label>
                    <input
                      type="checkbox"
                      checked={allSelected}
                      onChange={(e) => {
                        if (e.target.checked) {
                          // Select all reports
                          setSelectedWorkbooks(
                            reports.map((wb) => ({
                              id: wb.id,
                              luid: wb.luid,
                              name: wb.name,
                            }))
                          );
                        } else {
                          // Deselect all
                          setSelectedWorkbooks([]);
                        }
                      }}
                    />
                    Select All
                  </label>)}
                  {/* Reports list  */}
                {reports.map((wb) => (
                  <label key={wb.luid}>
                    <input
                      type="checkbox"
                      checked={selectedWorkbooks.some(
                        (w) => w.luid === wb.luid
                      )}
                      onChange={(e) => {
                        if (e.target.checked) {
                          // ✅ ADD workbook (id + luid + name)
                          setSelectedWorkbooks((prev) => [
                            ...prev,
                            {
                              id: wb.id,
                              luid: wb.luid,
                              name: wb.name,
                            },
                          ]);
                        } else {
                          // ❌ REMOVE workbook
                          setSelectedWorkbooks((prev) =>
                            prev.filter((w) => w.luid !== wb.luid)
                          );
                        }
                      }}
                    />
                    {wb.name}
                  </label>
                ))}
                {reports.length === 0 && (
                  <div className="placeholder">No reports available</div>
                )}
                {/* true && something   → something
                    false && something  → false */}
              </div>
            </div>

            {/* RIGHT: DATASOURCE */}
            <div className="reports-col">
              <label className="label">Datasource</label>

              <div className="reports-list">
                {/* SELECT ALL */}
                {dsList.length > 0 && (<label>
                    <input
                      type="checkbox"
                      checked={allSelectedDs}
                      onChange={(e) => {
                        if (e.target.checked) {
                          // Select all reports
                          setSelectedDatasources(
                            dsList.map((ds) => ({
                              id: ds.id,
                              luid: ds.luid,
                              name: ds.name,
                            }))
                          );
                        } else {
                          // Deselect all
                          setSelectedDatasources([]);
                        }
                      }}
                    />
                    Select All
                  </label>)}
                {/* Reports datasource  */}
                {dsList.map((ds) => (
                  <label key={ds.id}>
                    <input
                      type="checkbox"
                      checked={selectedDatasources.some(
                        (d) => d.id === ds.id
                      )}
                      onChange={(e) => {
                        if (e.target.checked) {
                          // ✅ ADD datasources (id + luid + name)
                          setSelectedDatasources((prev) => [
                            ...prev,
                            {
                              id: ds.id,
                              luid: ds.luid,
                              name: ds.name,
                            },
                          ]);
                        } else {
                          // ❌ REMOVE datasources
                          setSelectedDatasources((prev) =>
                            prev.filter((d) => d.id !== ds.id)
                          );
                        }
                      }}
                    />
                    {ds.name}
                  </label>
                ))}
                {dsList.length === 0 && (
                  <div className="placeholder">No datasources available</div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* DOWNLOAD BUTTON */}
        <div className="download-wrap">
          <button
            className="download-btn"
            type="button"
            disabled={loading || (selectedWorkbooks.length === 0 && selectedDatasources.length === 0)}
            onClick={handleDownloadMetadata}
          >
            {loading ? "Extracting..." : "Extract"}
          </button>

          <button
            className="launch-btn"
            type="button"
            disabled={!metadataReady}
            onClick={handleLaunchSourceIQ}
            title={
              metadataReady
                ? "Open SourceIQ Dashboard"
                : "Generate metadata first"
            }
          >
            Launch SourceIQ Dashboard
          </button>
        </div>
      </div>
      {/* {showSourceIQ && (
        <div className="sourceiq-embed-container">
          <iframe
            src="https://online.tableau.com/t/exavalu/views/Superstore?:embed=y" 
            width="100%"
            height="800"
            style={{ border: "none", marginTop: "20px" }}
            title="SourceIQ Dashboard"
          />
        </div>
      )} */}
      {/* ✅ FIXED: Only render iframe when we have the URL */}
      {/* {showSourceIQ && jwtToken && (
        <div className="sourceiq-embed-container" style={{ marginTop: "20px" }}>
          <tableau-viz
            id="tableau-viz"
            src="https://us-west-2b.online.tableau.com/t/exavalu/views/WorkbookSummary_17683044081920/WorkbookSummary"
            token={jwtToken}
            width="100%"
            height="800px"
            hide-tabs
            toolbar="hidden"
          />
        </div>
      )} */}

    </div>
  );
}
