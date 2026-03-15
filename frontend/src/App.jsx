import { useState } from 'react'
import './App.css'

const EXAMPLE_SAS = `%let schema = PROD_DW.ANALYTICS;

data &schema..active_customers;
    set &schema..customers;
    if status = 'ACTIVE' and account_balance > 0;
    keep customer_id name email status account_balance;
run;`

function App() {
  const [sasCode, setSasCode] = useState(EXAMPLE_SAS)
  const [sqlOutput, setSqlOutput] = useState('')
  const [warnings, setWarnings] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  async function handleConvert() {
    setLoading(true)
    setError(null)
    setWarnings([])
    try {
      const res = await fetch('/api/convert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sas_code: sasCode })
      })
      const data = await res.json()
      if (res.ok) {
        setSqlOutput(data.sql)
        setWarnings(data.warnings || [])
      } else {
        setError(data.detail || 'Conversion failed')
      }
    } catch (e) {
      setError('Could not reach the API server. Is it running?')
    }
    setLoading(false)
  }

  function handleCopy() {
    navigator.clipboard.writeText(sqlOutput)
  }

  return (
    <div className="app">
      <header>
        <h1>SAS to Snowflake Converter</h1>
        <button className="convert-btn" onClick={handleConvert} disabled={loading || !sasCode.trim()}>
          {loading ? 'Converting...' : 'Convert'}
        </button>
      </header>

      <main>
        <div className="panel">
          <div className="panel-header">
            <span>SAS Code</span>
          </div>
          <textarea
            value={sasCode}
            onChange={e => setSasCode(e.target.value)}
            placeholder="Paste your SAS DATA step code here..."
            spellCheck={false}
          />
        </div>

        <div className="panel">
          <div className="panel-header">
            <span>Snowflake SQL</span>
            {sqlOutput && (
              <button className="copy-btn" onClick={handleCopy}>Copy</button>
            )}
          </div>
          <textarea
            value={sqlOutput}
            readOnly
            placeholder="Converted SQL will appear here..."
            spellCheck={false}
          />
        </div>
      </main>

      {error && <div className="error">{error}</div>}
      {warnings.length > 0 && (
        <div className="warnings">
          {warnings.map((w, i) => <div key={i}>{w}</div>)}
        </div>
      )}
    </div>
  )
}

export default App
