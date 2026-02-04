import React,{useCallback, useState} from 'react';
import exavaluLogo from './images/exavalu-logo.png';
import { required } from './utils/validators';
import  '../css/LoginPage.css';
import { useNavigate } from "react-router-dom";

// const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";
 
export default function LoginPage({ onSubmit: parentOnSubmit } = {}) {
  const [patToken, setPatToken] = useState(''); // react state variables
  const [patTokenSecret, setPatSecret] = useState('');
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();
  const [showPassword, setShowPassword] = useState(false);


  // Default submit - posts token_name and token_value to backend
  const defaultOnSubmit = useCallback(async ({ token_name, token_value }) => {
  const url = `/bi/auth/login`;
     // send as JSON
    const res = await fetch(url, {
     method: "POST",
     credentials: "include", // ✅ REQUIRED
      headers: {
         "Content-Type": "application/json",
      },
      body: JSON.stringify({ token_name, token_value }),
     });

     if (!res.ok) {
       // try to surface backend message
       const txt = await res.text();
       throw new Error(txt || `HTTP ${res.status}`);
     }

     const data = await res.json();
     return data;
   }, []);

  const onSubmit = parentOnSubmit || defaultOnSubmit;

  const handleSubmit = useCallback(
    async (e) => {
      e.preventDefault();
      setError(null);

      if (!required(patToken) || !required(patTokenSecret)) {
        setError("Both fields are required.");
        return;
      }

      setLoading(true);
      try {
        const resp = await onSubmit({
          token_name: patToken.trim(),
          token_value: patTokenSecret,
        });

        // ✅ login success → redirect
        navigate("/dashboard"); //We don’t check resp.token .If backend sets cookie → login is successful
        // optionally notify parent
        console.log("login success", resp);
      } catch (err) {
        setError("Wrong Credentials!");  //setError(err?.message || "Submission failed"); for  debugging
        console.error("login error", err);
      } finally {
        setLoading(false);
      }
    },
    [patToken, patTokenSecret, onSubmit, navigate]
  );


  return (
    <div className="login-page">
        <form className="login-box" onSubmit={handleSubmit}>
          <header className="logo-wrap">
            <img src={exavaluLogo} alt="Exavalu logo" />
          </header>
          <div className="space-y-8">
            <div className="field">
              <input
                id="patToken"
                type="text"
                value={patToken}
                onChange={(e) => setPatToken(e.target.value)}
                placeholder=" "
                className="input-field"
              />
              <label htmlFor="patToken">PAT Token</label>
            </div>

            <div className="field">
              <input
                id="patSecret"
                type={showPassword ? "text" : "password"}
                value={patTokenSecret}
                onChange={(e) => setPatSecret(e.target.value)}
                placeholder=" "
                className="input-field"
              />
              <label htmlFor="patSecret">PAT Secret</label>
              <button
                type="button"
                className="eye-btn"
                onClick={() => setShowPassword(prev => !prev)}
                aria-label={showPassword ? "Hide password" : "Show password"}
              >
                <i className={`fa-solid ${showPassword ? "fa-eye" : "fa-eye-slash"}`}></i>
              </button>
            </div>

            {error && <p className="text-sm text-red-600">{error}</p>}

            <button
              type="submit"
              disabled={loading}
              className="login-btn"
            >
              {loading ? 'Authenticating...' : 'Login'}
            </button>
          </div>
          <div className="login-footer">
            © Login to Access BI Doctor
          </div>
        </form>
        

      
    </div>
  );
}
