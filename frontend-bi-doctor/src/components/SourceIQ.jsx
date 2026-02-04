import { useEffect, useState, useRef } from "react";
import "../css/SourceIQ.css";
export default function SourceIQ() {
  const jwtToken = sessionStorage.getItem("tableau_jwt");

  useEffect(() => {
    if (!jwtToken) {
      alert("Session expired. Please relaunch dashboard.");
    }
  }, [jwtToken]);

  if (!jwtToken) return null;

  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const viz = document.getElementById("sourceiq-viz");
    if (!viz) return;

    const onFirstInteractive = () => {
      setLoading(false);
    };

    viz.addEventListener("firstinteractive", onFirstInteractive);

    return () => {
      viz.removeEventListener("firstinteractive", onFirstInteractive);
    };
  }, []);

  return (
    <div className="sourceiq-page">
        {loading && (
            <div className="skeleton">
            Loading SourceIQ dashboard…
            </div>
        )}

      <tableau-viz
        src="https://us-west-2b.online.tableau.com/t/exavalu/views/WorkbookSummary_17683044081920/WorkbookSummary"
        token={jwtToken}
        // width="100%"
        // height="95vh"
        //hide-tabs
        //toolbar='bottom'
        device='default'
         toolbar="top"
        tabs = "yes"
      />
    </div>
  );
}
