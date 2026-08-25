import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import EmbedApp from './EmbedApp.jsx'
import './index.css'

// Hash-based routing — never stripped by proxies or XMPro
// Full app:  https://your-domain/
// Embed:     https://your-domain/#embed
const isEmbed = window.location.hash === '#embed' || window.location.hostname === 'dcmagstracked.172.188.51.215.nip.io'

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    {/* {isEmbed ? <EmbedApp /> : <App />} */}
    <EmbedApp />
  </React.StrictMode>,
)
