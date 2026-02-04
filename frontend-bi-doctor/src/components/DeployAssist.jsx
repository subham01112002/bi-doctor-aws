import React, { useEffect, useState, useRef } from "react";
import logo from "./images/exavalu-logo.png";
import ProgressBar from "./ProgressBar"; 
import "../css/TableauMetadataExtractor.css";
import "../css/DeployAssist.css";

// const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:9000";

export default function DeployAssist() {
  const [step, setStep] = useState(1);
  const [deploying, setDeploying] = useState(false);
  const [testingConnection, setTestingConnection] = useState(false);
  // const [connectionStatus, setConnectionStatus] = useState(null); // 'success' | 'error' | null
  // Progress tracking states
  const [deploymentStage, setDeploymentStage] = useState(0);
  const [deploymentLog, setDeploymentLog] = useState("");
  //Deployment results(url)
  const [deploymentWebUrl, setDeploymentWebUrl] = useState(null);
  // const [currentTaskId, setCurrentTaskId] = useState(null);
  // datasource index for multiple datasources
  const [currentDsIndex, setCurrentDsIndex] = useState(0);
  const [datasourceConfigs, setDatasourceConfigs] = useState({});
  const [biTool, setBiTool] = useState("Tableau");
  const [reports, setReports] = useState([]);
  const [projects, setProjects] = useState([]);
  const [selectedProject, setSelectedProject] = useState("");
  const [selectedWorkbooks, setselectedWorkbookss] = useState(null);
  const [projectOpen, setProjectOpen] = useState(false);
  const [projectSearch, setProjectSearch] = useState("");
  const [targetProject, setTargetProject] = useState("");
  const [targetProjectOpen, setTargetProjectOpen] = useState(false);
  const [targetProjectSearch, setTargetProjectSearch] = useState("");
  const projectRef = useRef(null);
  const targetProjectRef = useRef(null);
  const dbTypeRef = useRef(null);
  const [loadingWorkbooks, setLoadingWorkbooks] = useState(false);
  const fetchControllerRef = useRef(null);
  const [iFlag, setIFlag] = useState(false);
  const [showDbInfo, setShowDbInfo] = useState(false);
  const [sourceDbInfo, setSourceDbInfo] = useState(null);
  const [loadingDbInfo, setLoadingDbInfo] = useState(false);

  // const [dbType, setDbType] = useState("");
  const [dbTypeOpen, setDbTypeOpen] = useState(false);
  // const [dbConfig, setDbConfig] = useState({
  //   siteUrl: "",
  //   dbPassword: "",
  //   dbUserName: "",
  //   dbPort: "",
  //   dbName: ""
  // });
  const dbInfoAbortRef = useRef(null);
  const dbInfoRequestIdRef = useRef(0);


  const [testingDb, setTestingDb] = useState(false);

  const datasources = selectedWorkbooks?.datasources || [];
  const totalDatasources = datasources.length;
  const currentDatasource = datasources[currentDsIndex];
  const currentConfig =
    datasourceConfigs[currentDatasource?.luid] || {
      dbType: "",
      dbConfig: {
        siteUrl: "",
        dbPassword: "",
        dbUserName: "",
        dbPort: "",
        dbName: ""
      },
      status: null
    };

  const { dbType, dbConfig } = currentConfig;
  const [selectedSourceConn, setSelectedSourceConn] = useState(null);
  // const isDbSelected = dbType && dbType.trim() !== "";


  // const currentConfig = datasourceConfigs[currentDatasource?.luid];
  const goNextDatasource = () => {
    if (currentDsIndex < totalDatasources - 1) {
      setCurrentDsIndex(i => i + 1);
    }
  };

  const goPrevDatasource = () => {
    if (currentDsIndex > 0) {
      setCurrentDsIndex(i => i - 1);
    }
  };
  
  const updateCurrentConfig = (updates) => {
    setDatasourceConfigs(prev => ({
      ...prev,
      [currentDatasource.luid]: {
        ...prev[currentDatasource.luid],
        ...updates
      }
    }));
  };

  const handleSourceCheckbox = (checked, conn, idx) => {
  if (checked) {
    setSelectedSourceConn(idx);

    updateCurrentConfig({
      dbConfig: {
        siteUrl: conn.host || "",
        dbPort: conn.port || "",
        dbUserName: conn.username || "",
        dbPassword: "",        // never auto-fill password
        dbName: ""
      },
      status: null
    });
  } else {
    setSelectedSourceConn(null);
    updateCurrentConfig({
      dbConfig: {
        siteUrl: "",
        dbPort: "",
        dbUserName: "",
        dbPassword: "",
        dbName: ""
      },
      status: null,
      autoFilled: false
    });
    
    clearCopiedCredentials();
  }
};
useEffect(() => {
  setSelectedSourceConn(null);
}, [currentDsIndex]);


  
  const canDeploy =
    dbConfig.siteUrl.trim() !== "" &&
    dbConfig.dbPassword.trim() !== "" &&
    dbConfig.dbUserName.trim() !== "" &&
    dbConfig.dbPort.trim() !== "";

  const canTestConnection =
    // dbType.trim() !== "" &&
    dbConfig.siteUrl.trim() !== "" &&
    dbConfig.dbPassword.trim() !== "" &&
    dbConfig.dbUserName.trim() !== "" &&
    dbConfig.dbPort.trim() !== "";

  // Check if all datasources are ready
  const allDatasourcesReady =
    selectedWorkbooks?.datasources?.every(ds => {
      const cfg = datasourceConfigs[ds.luid];
      return (
        cfg &&
        cfg.dbType &&
        cfg.dbConfig.siteUrl &&
        cfg.dbConfig.dbUserName &&
        cfg.dbConfig.dbPassword &&
        cfg.dbConfig.dbPort &&
        cfg.status === "success"
      );
    });

  // STEP 1 CLEAR
  const clearFilter = () => {

    if (fetchControllerRef.current) {
      fetchControllerRef.current.abort();
      fetchControllerRef.current = null;
    }
    setBiTool("Tableau");
    setSelectedProject("");
    setselectedWorkbookss(null);
    setStep(1);
    setShowDbInfo(false);
    setSourceDbInfo(null);
    setLoadingWorkbooks(false);
    setReports([]);
  };

  // STEP 2 CLEAR
  const clearTargetFilter = () => {
    setTargetProject("");
    setTargetProjectSearch("");
    setTargetProjectOpen(false);
    setDbTypeOpen(false);
    setShowDbInfo(false);
    setSourceDbInfo(null);
    setIFlag(false);
    setDeploymentWebUrl(null);
    // reset all datasource DB configs
    const reset = {};
    selectedWorkbooks?.datasources?.forEach(ds => {
      reset[ds.luid] = {
        dbType: "",
        dbConfig: {
          siteUrl: "",
          dbPassword: "",
          dbUserName: "",
          dbPort: "",
          dbName: ""
        },
        status: null
      };
    });

    setDatasourceConfigs(reset);
    setCurrentDsIndex(0);
  };

  // Test Connection Handler
  const handleTestConnection = async () => {
    if (!dbType) {
      alert('Please select a database type');
      return;
    }
    if (!canTestConnection) {
      alert("Please fill in all database credentials");
      return;
    }

    setTestingConnection(true);
    try {
      const response = await fetch("/bi/db/test-connection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          db_type: dbType.trim(),
          host: dbConfig.siteUrl.trim(),
          port: dbConfig.dbPort.trim(),
          dbname: dbConfig.dbName.trim(),
          username: dbConfig.dbUserName.trim(),
          password: dbConfig.dbPassword.trim()
        })
      });

      const data = await response.json();

      if (data.status === "success") {
        updateCurrentConfig({ status: "success" });
        alert(data.message);
      } else {
        updateCurrentConfig({ status: "error" });
        alert(data.message);
      }
    } catch (error) {
      updateCurrentConfig({ status: "error" });
      alert(`Connection test failed: ${error.message}`);
    } finally {
      setTestingConnection(false);
    }
  };

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (projectOpen && projectRef.current && !projectRef.current.contains(event.target)) {
        setProjectOpen(false);
      }

      if (targetProjectOpen && targetProjectRef.current && !targetProjectRef.current.contains(event.target)) {
        setTargetProjectOpen(false);
      }

      if (dbTypeOpen && dbTypeRef.current && !dbTypeRef.current.contains(event.target)) {
        setDbTypeOpen(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [projectOpen, targetProjectOpen, dbTypeOpen]);

  // Fetch projects on component mount
  useEffect(() => {
    fetch(`/bi/tableau/projects_list`, {
      credentials: "include",
    })
      .then((res) => {
        if (!res.ok) throw new Error("Failed to load projects");
        return res.json();
      })
      .then((data) => {
        console.log("Projects from API:", data); 
        setProjects(data);
      })
      .catch((err) => console.error(err));
  }, []);
  
  // Fetch workbooks based on selected project
  useEffect(() => {
    if (!selectedProject) {
      setReports([]);
      setLoadingWorkbooks(false);
      return;
    }
    const controller = new AbortController();
    fetchControllerRef.current = controller;
    setLoadingWorkbooks(true);

    fetch(`/bi/tableau/sqlproxy-workbooks?project_luid=${selectedProject}`, {
      credentials: "include",
      signal: controller.signal //` Pass abort signal
    })
      .then(res => res.json())
      .then(data => {
        console.log("Filtered workbooks:", data);
        setReports(data);
        setselectedWorkbookss(null);
      })
      .catch((err) => {
        if (err.name !== 'AbortError') {
          console.error(err);
        }
      })
      .finally(() => {
        setLoadingWorkbooks(false);
        fetchControllerRef.current = null;
      });

    // Cleanup function
    return () => {
      controller.abort();
    };
  }, [selectedProject]);

  // Clear target filters when source project changes
  useEffect(() => {
    clearTargetFilter();
  }, [selectedProject]);

  // Initialize datasource configs when moving to step 2
  useEffect(() => {
    if (!selectedWorkbooks?.datasources?.length) return;

    const initial = {};
    selectedWorkbooks.datasources.forEach(ds => {
      initial[ds.luid] = {
        dbType: "",
        dbConfig: {
          siteUrl: "",
          dbPassword: "",
          dbUserName: "",
          dbPort: "",
          dbName: ""
        },
        status: null
      };
    });

    setDatasourceConfigs(initial);
    setCurrentDsIndex(0);
  }, [selectedWorkbooks]);
  
  // Cleanup SSE connection on unmount
  useEffect(() => {
    return () => {
      if (window.deploymentEventSource) {
        window.deploymentEventSource.close();
        window.deploymentEventSource = null;
      }
    };
  }, []);

  // Fetch source DB info
  const fetchSourceDbInfo = async () => {
    // If panel is already open, close it
    // if (showDbInfo) {
    //   setShowDbInfo(false);
    //   return;
    // }
    setIFlag(true);
    const datasources = selectedWorkbooks?.datasources || [];
    const currentDatasource = datasources[currentDsIndex];

    if (!currentDatasource?.luid) {
      alert("No datasource selected");
      return;
    }
    setShowDbInfo(prev => !prev);
    setLoadingDbInfo(true);
    try {
      const res = await fetch(
        `/bi/tableau/datasource_connection?datasource_luid=${currentDatasource.luid}`,
        { credentials: "include" }
      );

      if (!res.ok) throw new Error("Failed to fetch DB info");

      const data = await res.json();
      console.log("Source DB info:", data);
      setSourceDbInfo({
      ...data,
      datasource_name: currentDatasource.name
    });
      //setSourceDbInfo(data);
      setShowDbInfo(true);
    } catch (err) {
      console.error(err);
      alert("Failed to load database info");
    } finally {
      setLoadingDbInfo(false);
    }
};
// useEffect(() => {
//   if (showDbInfo) {
//     fetchSourceDbInfo();
//   }
// }, [currentDsIndex]);

// Refetch DB info when switching datasources (if panel is open)
  useEffect(() => {
    if (!showDbInfo) return;

    const datasources = selectedWorkbooks?.datasources || [];
    const currentDatasource = datasources[currentDsIndex];
    if (!currentDatasource?.luid) return;

      // Abort previous request
    if (dbInfoAbortRef.current) {
      dbInfoAbortRef.current.abort();
    }

    const controller = new AbortController();
    dbInfoAbortRef.current = controller;

    //  Unique request id
    const requestId = ++dbInfoRequestIdRef.current;

    const load = async () => {
      setLoadingDbInfo(true);
      try {
        const res = await fetch(
          `/bi/tableau/datasource_connection?datasource_luid=${currentDatasource.luid}`,
          { credentials: "include",
            signal: controller.signal
           }
        );
        const data = await res.json();
        console.log("Source DB info:", data);
        //  Ignore stale responses
        if (requestId !== dbInfoRequestIdRef.current) return;

        setSourceDbInfo({
          ...data,
          datasource_name: currentDatasource.name
        });
      } catch (err) {
          if (err.name !== "AbortError") {
          console.error("DB info load failed:", err);
          }
        //console.error(err);
      } finally {
          if (requestId === dbInfoRequestIdRef.current) {
            setLoadingDbInfo(false);
          }
      }
    };

    load();
    return () => controller.abort();
  }, [currentDsIndex]);


  const handleNext = () => {
    setStep(2);
    if(iFlag === true){
      setShowDbInfo(true);
    }else{
      setShowDbInfo(false);
    }
  };

  const handleBackClick = () => {
  setStep(1);
  setShowDbInfo(false);
};
  
// Handle deployment with REAL-TIME progress tracking via SSE
const handleDeploy = async () => {
  // if (!selectedWorkbooks || !targetProject) {
  //   alert("Missing required selections");
  //   return;
  // }
  if (!targetProject) {
    alert("Please select a target project!");
    return;
  }

  const payload = {
    source_workbook_luid: selectedWorkbooks.luid,
    datasource_luids: selectedWorkbooks.datasources.map(ds => ds.luid),
    target_project_luid: targetProject,
    datasources: selectedWorkbooks.datasources.map(ds => {
      const cfg = datasourceConfigs[ds.luid];

      return {
        datasource_luid: ds.luid,
        db_config: {
          db_type: cfg.dbType,
          host: cfg.dbConfig.siteUrl,
          dbname: cfg.dbConfig.dbName,
          port: cfg.dbConfig.dbPort,
          username: cfg.dbConfig.dbUserName,
          password: cfg.dbConfig.dbPassword
        }
      };
    })
  };

  setDeploying(true);
  setDeploymentStage(0);
  setDeploymentLog("Initializing deployment...");
  setDeploymentWebUrl(null);

  let eventSource = null;

  try {
    // ✅ CHANGE 1: Start deployment
    console.log(" Starting deployment...");
    const res = await fetch(`/bi/deploy/full-migration`, {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: Failed to start deployment`);
    }

    const data = await res.json();
    console.log(" Deployment response:", data);
    
    if (!data.task_id) {
      throw new Error("No task_id returned from backend");
    }

    const taskId = data.task_id;

    // ✅ CHANGE 2: Wait for backend to initialize
    console.log(" Waiting 500ms for backend initialization...");
    await new Promise(resolve => setTimeout(resolve, 500));

    // ✅ CHANGE 3: Verify task exists before connecting SSE
    console.log(" Verifying task exists...");
    try {
      const statusRes = await fetch(`/bi/deploy/status/${taskId}`, {
        credentials: "include"
      });
      const statusData = await statusRes.json();
      
      console.log(" Task status:", statusData);
      
      if (!statusData.exists) {
        throw new Error("Task not initialized on backend");
      }
    } catch (verifyError) {
      console.error(" Task verification failed:", verifyError);
      throw new Error("Backend task initialization failed");
    }

    // ✅ CHANGE 4: Connect to SSE
    console.log(` Connecting to SSE: ${API_BASE}/deploy/progress/${taskId}`);
    
    eventSource = new EventSource(
      `${API_BASE}/deploy/progress/${taskId}`,
      { withCredentials: true }
    );

    let hasReceivedData = false;
    let hasShownAlert = false;
    
    // ✅ CHANGE 5: Connection timeout safety
    let connectionTimeout = setTimeout(() => {
      if (!hasReceivedData) {
        console.error(" SSE timeout - no data received in 10s");
        eventSource?.close();
        setDeploying(false);
        alert(" Connection timeout. Please check backend logs.");
      }
    }, 10000);

    // ✅ CHANGE 6: Handle connection open
    eventSource.onopen = () => {
      console.log(" SSE connection opened");
    };

    // ✅ CHANGE 7: Handle ping events (keepalive)
    eventSource.addEventListener("ping", () => {
      // Just keepalive - no UI update needed
      console.log(" Keepalive ping received");
    });

    // ✅ CHANGE 8: Handle keepalive events
    eventSource.addEventListener("keepalive", () => {
      // Keepalive - no action needed
      console.log(" Keepalive received");
    });

    // ✅ CHANGE 9: Handle progress events
    eventSource.addEventListener("progress", (event) => {
      clearTimeout(connectionTimeout);
      hasReceivedData = true;
      
      try {
        const progress = JSON.parse(event.data);
        console.log(" Progress update:", progress);
        
        setDeploymentStage(progress.stage);
        setDeploymentLog(progress.message);
      } catch (err) {
        console.error("Error parsing progress:", err);
      }
    });

    // ✅ CHANGE 10: Handle completion events
    eventSource.addEventListener("complete", (event) => {
      clearTimeout(connectionTimeout);
      
      if (hasShownAlert) return; // Prevent duplicate alerts
      hasShownAlert = true;
      
      try {
        const progress = JSON.parse(event.data);
        console.log("🏁 Deployment complete:", progress);
        
        setDeploymentStage(progress.stage);
        setDeploymentLog(progress.message);
        
        if (progress.status === 'completed') {
          if (progress.workbook_url) {
            setDeploymentWebUrl(progress.workbook_url);
            alert(`Migration completed successfully!`);
          } else {
            alert("Migration completed!");
          }
        } else if (progress.status === 'failed') {
          alert(`Migration failed: ${progress.message}`);
        }
        
        setDeploying(false);
        
      } catch (err) {
        console.error("Error parsing completion:", err);
        setDeploying(false);
      } finally {
        eventSource?.close();
      }
    });

    // ✅ CHANGE 11: Handle error events
    eventSource.addEventListener("error", (event) => {
      clearTimeout(connectionTimeout);
      
      if (hasShownAlert) return;
      hasShownAlert = true;
      
      try {
        const errorData = JSON.parse(event.data);
        console.error("❌ SSE error event:", errorData);
        alert(`Error: ${errorData.message}`);
      } catch {
        console.error("❌ SSE error (no data)");
      }
      
      setDeploying(false);
      eventSource?.close();
    });

    // ✅ CHANGE 12: Handle connection errors
    eventSource.onerror = (error) => {
      clearTimeout(connectionTimeout);
      console.error("❌ SSE connection error:", error);
      
      // Only show error if we haven't received any data yet
      if (!hasReceivedData && !hasShownAlert) {
        hasShownAlert = true;
        alert("Lost connection to deployment progress. Check backend logs.");
        setDeploying(false);
      }
      
      eventSource?.close();
    };

    // ✅ CHANGE 13: Store reference for cleanup
    window.deploymentEventSource = eventSource;

  } catch (err) {
    console.error("❌ Deployment error:", err);
    alert(`Failed to start deployment: ${err.message}`);
    setDeploying(false);
    setDeploymentStage(0);
    setDeploymentLog("");
    
    eventSource?.close();
  }
};

// Cleanup on unmount (keep existing)
useEffect(() => {
  return () => {
    if (window.deploymentEventSource) {
      console.log("Cleaning up SSE connection on unmount");
      window.deploymentEventSource.close();
      window.deploymentEventSource = null;
    }
  };
}, []);

  const handleLaunch = () => {
    if (deploymentWebUrl) {
      window.open(deploymentWebUrl, '_blank', 'noopener,noreferrer');
    } else {
      alert("No deployment URL available. Please deploy first.");
    }
  };

  return (
    <div className="metadata-root">
      {/* Show progress bar overlay when deploying */}
      {deploying && (
        <ProgressBar 
          currentStage={deploymentStage} 
          logMessage={deploymentLog}
          totalDatasources={selectedWorkbooks?.datasources?.length || 1}
        />
      )}

      <div className="metadata-card">
        {/* LOGO */}
        <div className="metadata-logo">
          <img src={logo} alt="Logo" />
        </div>

        {/* ================= STEP 1 ================= */}
        {step === 1 && (
          <>
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
              <label className="field-label">Source Project</label>

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
                    {loadingWorkbooks ? (
                      <div className="reports-loading-wrapper">
                        <div className="loading-container">
                          <div className="spinner"></div>
                          <p className="loading-text">Loading workbooks...</p>
                        </div>
                      </div>
                    ) : reports.length > 0 ? (
                      reports.map((wb) => (
                        <label key={wb.luid}>
                          <input
                            type="radio"
                            name="workbook"
                            checked={selectedWorkbooks?.luid === wb.luid}
                            onChange={() => setselectedWorkbookss(wb)}
                          />
                          {wb.name}
                        </label>
                      ))
                    ) : selectedProject ? (
                      <div className="placeholder">
                        No workbooks found
                      </div>
                    ) : (
                      <div className="placeholder">
                        Select a project to load workbooks
                      </div>
                    )}
                  </div>
                </div>

                {/* RIGHT: DATASOURCE */}
                <div className="reports-col">
                  <label className="label">Datasource</label>

                  <div className="reports-list">
                    {selectedWorkbooks && !loadingWorkbooks ? (
                      selectedWorkbooks.datasources?.length > 0 ? (
                        // <label>
                        //   <input type="radio" checked disabled />
                        //   {selectedWorkbooks.datasources[0].name}
                        // </label>
                        selectedWorkbooks.datasources.map((ds) => (
                          <label key={ds.id}>
                            <input
                              type="radio"
                              checked
                              disabled
                            />
                            {ds.name}
                          </label>
                        ))
                      ) : (
                        <div className="placeholder">
                          No datasources found for this workbook
                        </div>
                      )
                    ) : (
                      <div className="placeholder">
                        Select a workbook to load datasources
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* NEXT */}
            <div className="download-wrap">
              <button
                className="download-btn"
                type="button"
                onClick={handleNext}
                disabled={!selectedWorkbooks || !selectedWorkbooks.datasources?.length}
              >
                Next
              </button>
            </div>
          </>
        )}

        {/* ================= STEP 2 ================= */}
        {step === 2  && (
          <>
            {/* TARGET PROJECT */}
            <div className="projects">
              <div className="field-row">
                {/* <label className="field-label">Target Project</label> */}
                <div className="field-label target-project-label">
                  <span>Target Project</span>

                  {/* {selectedWorkbooks && selectedWorkbooks.datasources?.[0] && (
                    <span
                      className="source-info-inline"
                      title={`${selectedWorkbooks.name} | ${selectedWorkbooks.datasources[0].name}`}
                    >
                      ({selectedWorkbooks.name} | {selectedWorkbooks.datasources[0].name})
                    </span>
                  )} */}
                </div>
                
                <button
                  className="clear-btn"
                  type="button"
                  onClick={clearTargetFilter}
                >
                  Clear Filter
                </button>
              </div>

              <div ref={targetProjectRef} className={`dropdown ${targetProjectOpen ? "open" : ""}`}>
                {/* Trigger */}
                <div
                  className="dropdown-control"
                  onClick={() => setTargetProjectOpen((o) => !o)}
                >
                  <span>
                    {projects.find(p => p.project_luid === targetProject)?.project_name
                      || "Select project"}
                  </span>
                  <span className="dropdown-arrow" />
                </div>

                {/* Dropdown panel */}
                {targetProjectOpen && (
                  <div className="dropdown-menu">
                    <input
                      type="text"
                      className="dropdown-search"
                      placeholder="Search projects..."
                      value={targetProjectSearch}
                      onChange={(e) => setTargetProjectSearch(e.target.value)}
                      autoFocus
                    />

                    <div className="dropdown-list">
                      {projects
                        .filter(p =>
                          p.project_luid !== selectedProject &&
                          p.project_name
                            .toLowerCase()
                            .includes(targetProjectSearch.toLowerCase())
                        )
                        .map((p) => (
                          <div
                            key={p.project_luid}
                            className="dropdown-item"
                            onClick={() => {
                              setTargetProject(p.project_luid);
                              setTargetProjectOpen(false);
                              setTargetProjectSearch("");
                              setDeploymentWebUrl(null);
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

            {/* DATASOURCE CARD
            <div className="datasource-header-bar">
              <div className="datasource-name">
                Datasource:{" "}
                <strong>
                  {selectedWorkbooks?.datasources?.[0]?.name}
                </strong>
              </div>

              <div className="datasource-counter">
                1 / {selectedWorkbooks?.datasources?.length || 1}
              </div>
            </div> */}

            {/* DATABASE TYPE */}
            <div className="projects">
              <div className="database-label-row">
                <label className="field-label">Database</label>

                <div className="datasource-nav">
                  <button
                    className="nav-btn"
                    onClick={goPrevDatasource}
                    disabled={totalDatasources <= 1 || currentDsIndex === 0}
                  >
                    &lt;
                  </button>

                  <span className="nav-counter">
                    {currentDsIndex + 1}/{totalDatasources || 1}
                  </span>

                  <button
                    className="nav-btn"
                    onClick={goNextDatasource}
                    disabled={
                      totalDatasources <= 1 ||
                      currentDsIndex === totalDatasources - 1
                    }
                  >
                    &gt;
                  </button>
                </div>
              </div>


              <div  ref={dbTypeRef} className={`dropdown ${dbTypeOpen ? "open" : ""}`}>
                <div
                  className="dropdown-control"
                  onClick={() => setDbTypeOpen(o => !o)}
                >
                  <span>{dbType || "Select database"}</span>
                  <span className="dropdown-arrow" />
                </div>

                {dbTypeOpen && (
                  <div className="dropdown-menu">
                    <div className="dropdown-list">
                      {["MySQL", "PostgreSQL", "Redshift"].map(db => (
                        <div
                          key={db}
                          className="dropdown-item"
                          onClick={() => {
                            updateCurrentConfig({
                              dbType: db,
                              // dbConfig: {
                              //   siteUrl: "",
                              //   dbPassword: "",
                              //   dbName: "",
                              //   dbUserName: "",
                              //   dbPort: ""
                              // }
                            });
                            setDbTypeOpen(false);
                          }}
                        >
                          {db}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* DB CREDENTIALS */}
            <div className="db-section">
              <div className="db-header">
                <span>DB Credentials</span>
                <span className="db-info" title="Source Database connection info"
                  onClick={fetchSourceDbInfo}
                    style={{ cursor: 'pointer' }}
                  >
                    {loadingDbInfo ? "..." : "i"}
                </span>
                {/* TEST CONNECTION BUTTON */}
                <button
                  className="clear-btn"
                  type="button"
                  onClick={handleTestConnection}
                  disabled={!canTestConnection || testingConnection}
                  style={{ marginLeft: "auto", cursor: canTestConnection ? "pointer" : "not-allowed" }}
                >
                  {testingConnection ? "Testing..." : "Test Connection"}
                </button>
              </div>

              <div className="db-form">
                {/* DB HOST */}
                <input
                  className="input-field"
                  placeholder="DB Host Url"
                  value={dbConfig.siteUrl}
                  // disabled={!isDbSelected}
                  onChange={(e) =>
                    updateCurrentConfig({
                      dbConfig: {
                        ...dbConfig,
                        siteUrl: e.target.value
                      },
                      status: null
                    })
                  }
                />

                {/* DB NAME */}
                <div className="db-row">
                  <input
                    className="input-field"
                    placeholder="DB Name"
                    value={dbConfig.dbName}
                    // disabled={!isDbSelected}
                    onChange={(e) =>
                    updateCurrentConfig({
                      dbConfig: {
                        ...dbConfig,
                        dbName: e.target.value
                      },
                      status: null
                    })
                  }
                  />
                  {/* DB PORT */}
                  <input
                    className="input-field"
                    placeholder="DB Port"
                    value={dbConfig.dbPort}
                    // disabled={!isDbSelected}
                    onChange={(e) =>
                      updateCurrentConfig({
                        dbConfig: {
                          ...dbConfig,
                          dbPort: e.target.value
                        },
                        status: null
                      })
                    }
                  />
                </div>

                {/* USERNAME + PASSWORD */}
                <div className="db-row">
                  <input
                    className="input-field"
                    placeholder="DB User Name"
                    value={dbConfig.dbUserName}
                    // disabled={!isDbSelected}
                    onChange={(e) =>
                    updateCurrentConfig({
                      dbConfig: {
                        ...dbConfig,
                        dbUserName: e.target.value
                      },
                      status: null
                    })
                  }
                  />
                  <input
                    className="input-field"
                    type="password"
                    placeholder="DB Password"
                    value={dbConfig.dbPassword}
                    // disabled={!isDbSelected}
                    onChange={(e) =>
                    updateCurrentConfig({
                      dbConfig: {
                        ...dbConfig,
                        dbPassword: e.target.value
                      },
                      status: null
                    })
                  }
                  />
                </div>
              </div>
            </div>
            {/* ACTIONS */}
            <div className="action-row">
              <button
                className="back-btn"
                type="button"
                // onClick={() => setStep(1)}
                onClick={handleBackClick}
              >
                Back
              </button>

              <button
                className="download-btn"
                type="button"
                disabled={deploying || !allDatasourcesReady}
                onClick={handleDeploy}
              >
                {deploying ? "Deploying..." : "Deploy"}
              </button>

              <button
                className="launch-btn"
                type="button"
                disabled={!deploymentWebUrl || deploying || !allDatasourcesReady}
                onClick={handleLaunch}
                title={deploymentWebUrl ? "Open deployed workbook" : "Deploy first to enable launch"}
              >
                Launch Target Project
              </button>
            </div>
          </>
        )}
      </div>
      {/* DB Info Modal */}
      {/* {showDbInfo && sourceDbInfo && (
        <div className="modal-overlay" onClick={() => setShowDbInfo(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Source Database Connection Info</h3>
              <button onClick={() => setShowDbInfo(false)}>×</button>
            </div>
            
            <div className="modal-body">
              <div className="db-info-item">
                <span className="db-info-label">Datasource:</span>
                <span className="db-info-value">{sourceDbInfo.datasource_name}</span>
              </div>
              
              {sourceDbInfo.connections.map((conn, idx) => (
                <div key={idx} className="db-connection-block">
                  <div className="db-info-item">
                    <span className="db-info-label">Type:</span>
                    <span className="db-info-value">{conn.type}</span>
                  </div>
                  <div className="db-info-item">
                    <span className="db-info-label">Host:</span>
                    <span className="db-info-value">{conn.host}</span>
                  </div>
                  <div className="db-info-item">
                    <span className="db-info-label">Port:</span>
                    <span className="db-info-value">{conn.port}</span>
                  </div>
                  <div className="db-info-item">
                    <span className="db-info-label">Username:</span>
                    <span className="db-info-value">{conn.username}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )} */}
      {/* DB Info Panel - Top Right */}
      {showDbInfo && (
        <div className="db-info-panel">
          {(loadingDbInfo || !sourceDbInfo) && (
            <div className="db-info-loading-overlay">
              <div className="spinner"></div>
              <p className="loading-text">Fetching details...</p>
            </div>
          )}
          {sourceDbInfo && (
            <>
              <div className="db-info-header">
                <h3>Source Info</h3>
                <button 
                  className="db-info-close" 
                  onClick={() => setShowDbInfo(false)}
                  title="Close"
                >
                  ×
                </button>
              </div>
        
              <div className="db-info-body">
                <div className="db-datasource-name">
                  WB: {selectedWorkbooks.name}
                </div>
                <div className="db-datasource-name">
                  DS: {sourceDbInfo.datasource_name}
                </div>
                {sourceDbInfo.connections.map((conn, idx) => (
                  <div key={idx} className="db-connection-block">
                    <label className="set-credentials-checkbox">
                      <input
                        type="checkbox"
                        checked={selectedSourceConn === idx}
                        // disabled={!isDbSelected}
                        onChange={(e) => handleSourceCheckbox(e.target.checked, conn, idx)}
                      />
                      <span>Use this connection</span>
                    </label>
                    <div className="db-info-item">
                      <span className="db-info-label">Type:</span>
                      <span className="db-info-value"><b>{conn.type}</b></span>
                    </div>
                    <div className="db-info-item">
                      <span className="db-info-label">Host:</span>
                      <span className="db-info-value">{conn.host}</span>
                    </div>
                    <div className="db-info-item">
                      <span className="db-info-label">Port:</span>
                      <span className="db-info-value">{conn.port}</span>
                    </div>
                    <div className="db-info-item">
                      <span className="db-info-label">Username:</span>
                      <span className="db-info-value">{conn.username}</span>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}