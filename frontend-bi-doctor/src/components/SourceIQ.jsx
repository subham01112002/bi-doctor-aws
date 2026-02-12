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
        src="https://us-west-2b.online.tableau.com/t/exavalu/views/SOURCEIQ/SourceIQWorkbookSummaryHist/4b84ee2f-d57e-4d7a-a339-c37c58f7040e/8020ba55-3165-4fdd-a1ba-e88deb20d541%27"
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
