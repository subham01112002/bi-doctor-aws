import { Routes, Route, Navigate } from "react-router-dom";
import LoginPage from "./components/LoginPage";
import TableauMetadataExtractor from "./components/TableauMetadataExtractor";
import ProtectedRoute from "./components/ProtectedRoute";
import DashboardShell from "./components/DashboardShell";
import DeployAssist from "./components/DeployAssist";
import SourceIQ from "./components/SourceIQ";




export default function App() {
  return (
    <Routes>
      {/* Public route */}
      <Route path="/" element={<LoginPage />} />
      <Route path="/sourceiq" element={<SourceIQ />} />
      {/* Protected dashboard */}
      <Route
        path="/dashboard"
        element={
          <ProtectedRoute>
            <DashboardShell />
          </ProtectedRoute>
        }
      >
        {/* Default redirect */}
        <Route index element={<Navigate to="SourceIQ" replace />} />

        {/* Sidebar pages */}
        <Route
          path="SourceIQ"
          element={<TableauMetadataExtractor />}
        />

        <Route
          path="PushOps"
          // element={<div>Deploy Assist Coming Soon</div>}
          element={<DeployAssist />}
        />
      </Route>
    </Routes>
  );
}
