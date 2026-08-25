import React, { useState, useRef, useEffect } from 'react';
import './EmbedApp.css';

const BACKEND_URL = 'http://localhost:8000';

const AGENTS = [
  { id: 'demand',      name: 'Demand & Conditions Agent',        shortName: 'Demand &\nConditions',      role: 'Forecasting',         icon: '📊', color: '#4A9EFF', position: 0,
    description: 'Forecasts cooling demand based on IT load trends, wet-bulb temperature, and occupancy schedules.',
    responsibilities: ['Real-time load forecasting','Weather & humidity analysis','Peak demand prediction','Thermal trend monitoring'] },
  { id: 'chiller',     name: 'Chiller Optimization Agent',       shortName: 'Chiller\nOptimization',     role: 'Staging & Efficiency', icon: '❄️', color: '#9C6ADE', position: 1,
    description: 'Determines optimal chiller staging to maximise COP while meeting cooling load.',
    responsibilities: ['Chiller staging decisions','COP optimisation','Part-load efficiency','Sequencing & rotation'] },
  { id: 'building',    name: 'Building System Agent',            shortName: 'Building\nSystem',          role: 'Pumps & Towers',       icon: '🏢', color: '#00BFA5', position: 2,
    description: 'Manages pumps, cooling towers, and auxiliary systems to support chiller plant operations.',
    responsibilities: ['Pump VFD control','Cooling tower fan staging','Flow balancing','Delta-T optimisation'] },
  { id: 'energy',      name: 'Energy & Cost Optimization Agent', shortName: 'Energy &\nCost',            role: 'Cost Optimization',    icon: '💰', color: '#FFA726', position: 3,
    description: 'Evaluates energy tariffs, PUE impact, and cost trade-offs for every proposed operational change.',
    responsibilities: ['Energy cost modelling','PUE tracking','Tariff period awareness','Demand charge avoidance'] },
  { id: 'maintenance', name: 'Maintenance & Compliance Agent',   shortName: 'Maintenance\n& Compliance', role: 'Equipment Health',     icon: '🔧', color: '#EC407A', position: 4,
    description: 'Monitors equipment health, maintenance schedules, and regulatory compliance constraints.',
    responsibilities: ['Equipment health scoring','Maintenance window tracking','Warranty compliance','Run-hour balancing'] },
  { id: 'safety',      name: 'Operations & Safety Agent',        shortName: 'Operations\n& Safety',      role: 'Safety & SOPs',        icon: '🛡️', color: '#EF5350', position: 5,
    description: 'Enforces safety protocols, N+1 redundancy requirements, and operational SOPs. Holds veto authority.',
    responsibilities: ['N+1 redundancy enforcement','SOP compliance checks','Veto authority on unsafe actions','Alarm & interlock monitoring'] },
];

const ORCHESTRATOR = {
  id: 'orchestrator', name: 'Orchestrator', shortName: 'Orchestrator', role: 'System Coordinator', icon: '🎯', color: '#00BFA5',
  description: 'Coordinates all agents through a structured 2-round debate, builds consensus, and presents decisions for human approval.',
  responsibilities: ['Multi-agent debate facilitation','Consensus building','Human approval interface','Decision logging & audit trail']
};

const ALL_AGENTS = [...AGENTS, ORCHESTRATOR];

const SUGGESTED_QUESTIONS = [
  'Should we reduce chiller staging tonight?',
  'What cooling strategy optimises energy cost during peak tariff hours?',
  'How can we reduce PUE below 1.2 this quarter?',
  'Is it safe to take Chiller-2 offline for scheduled maintenance?',
  'How to reduce energy expenditure by 25% this year?',
  'What is the optimal wet-bulb setpoint given current conditions?',
];

function nameToId(name) {
  if (!name) return 'orchestrator';
  const n = name.toLowerCase();
  if (n.includes('orchestrator'))                      return 'orchestrator';
  if (n.includes('demand'))                            return 'demand';
  if (n.includes('chiller'))                           return 'chiller';
  if (n.includes('building'))                          return 'building';
  if (n.includes('energy'))                            return 'energy';
  if (n.includes('maintenance'))                       return 'maintenance';
  if (n.includes('safety') || n.includes('operations')) return 'safety';
  return 'orchestrator';
}

function agentById(id) { return ALL_AGENTS.find(a => a.id === id) || ORCHESTRATOR; }

// Highlight important numbers/units (green) and action keywords (amber) in agent body text
function highlightText(text) {
  if (!text) return null;

  // Strip markdown bold/italic so **~210 kW** becomes ~210 kW before matching
  const clean = text
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .replace(/\*([^*]+)\*/g, '$1');

  // Matches: optional ~ prefix, comma-formatted or decimal numbers,
  // optional en/em-dash range (e.g. 1–2), then unit
  const NUM_RE = /(~?\d{1,3}(?:,\d{3})*(?:\.\d+)?(?:\s*[–\-]\s*~?\d{1,3}(?:,\d{3})*(?:\.\d+)?)?\s*(?:%|kWh|kW|MWh|MW|°C|SGD|USD|\$|tons?|hours?|hrs?|mins?|days?|weeks?|months?|years?|PUE|COP|kVA|MVA|rpm|cfm|psi|bar|Hz))/gi;
  const ACTION_RE = /\b(reduce|increase|optimise|optimize|implement|deploy|upgrade|replace|shut down|activate|enable|disable|schedule|monitor|inspect|maintain|prioritize|recommend|approve|reject|veto|escalate|investigate|resolve|alert|critical|urgent|warning|required|must|immediately)\b/gi;

  const parts = [];
  let i = 0;

  const matches = [];
  let m;
  const numRe = new RegExp(NUM_RE.source, 'gi');
  const actRe = new RegExp(ACTION_RE.source, 'gi');
  while ((m = numRe.exec(clean)) !== null) matches.push({ start: m.index, end: m.index + m[0].length, cls: 'hl-num', val: m[0] });
  while ((m = actRe.exec(clean)) !== null) matches.push({ start: m.index, end: m.index + m[0].length, cls: 'hl-action', val: m[0] });
  matches.sort((a, b) => a.start - b.start);

  // Deduplicate overlapping spans (keep first)
  const deduped = [];
  let lastEnd = 0;
  for (const span of matches) {
    if (span.start >= lastEnd) { deduped.push(span); lastEnd = span.end; }
  }

  for (const span of deduped) {
    if (span.start > i) parts.push(clean.slice(i, span.start));
    parts.push(<span key={span.start} className={span.cls}>{span.val}</span>);
    i = span.end;
  }
  if (i < clean.length) parts.push(clean.slice(i));

  return parts.length > 0 ? parts : clean;
}

// Extract up to 4 concise, reader-friendly summary points from an agent response
function getAgentSummary(text) {
  if (!text || text.length < 100) return null;
  // Strip markdown bold/italic markers before splitting so **298.0 kW** doesn't break sentence detection
  const clean = text
    .replace(/\*\*([^*]+)\*\*/g, '$1')  // **bold** → bold
    .replace(/\*([^*]+)\*/g, '$1')       // *italic* → italic
    .replace(/\n+/g, ' ');
  // Protect decimal numbers (e.g. 17.057) before splitting into sentences
  const protected_ = clean.replace(/(\d)\.(\d)/g, '$1\x00$2');
  const rawSentences = protected_.match(/[^.!?]+[.!?]+/g) || [];
  const sentences = rawSentences.map(s => s.replace(/\x00/g, '.'));
  if (sentences.length < 2) return null;

  const scored = sentences.map(s => {
    const t = s.trim();
    let score = 0;
    if (/\b(recommend|I (?:recommend|support|oppose|suggest|approve|reject)|therefore|bottom line|my (?:position|vote)|verdict|action:)\b/i.test(t)) score += 5;
    if (/\b(approve|reject|veto|must|critical|immediately|warning|risk|unsafe|safe)\b/i.test(t)) score += 3;
    if (/\d+(?:\.\d+)?\s*(?:%|kW|MW|kWh|MWh|°C|°F|COP|PUE|SGD|\$|tons?|kPa)/.test(t)) score += 2;
    if (/\b(save|cost|energy|reduce|increase|stage|avoid|impact|concern|benefit)\b/i.test(t)) score += 1;
    return { text: t, score };
  });

  const MAX = 90;
  return scored
    .filter(s => s.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 4)
    .map(s => s.text.length > MAX ? s.text.slice(0, MAX - 1) + '…' : s.text);
}

// Parse a structured **Section**\ncontent response into [{title, lines}]
function parseDecisionSections(text) {
  if (!text) return null;
  const sectionRe = /\*\*([^*]+)\*\*/g;
  const indices = [];
  let m;
  while ((m = sectionRe.exec(text)) !== null)
    indices.push({ title: m[1].trim(), start: m.index, contentStart: m.index + m[0].length });
  if (indices.length === 0) return null;

  return indices.map((sec, i) => {
    const raw = text.slice(sec.contentStart, indices[i + 1]?.start ?? text.length).trim();
    const lines = raw.split('\n').map(l => l.replace(/^[•\-]\s*/, '').trim()).filter(Boolean);
    return { title: sec.title, lines };
  });
}

function voteClass(v) {
  const vl = (v || '').toLowerCase();
  if (vl === 'approve') return 'approve';
  if (vl.includes('approve_with') || vl.includes('with_conditions')) return 'conditional';
  if (vl.includes('veto')) return 'veto';
  return 'reject';
}

function loadLS(key, fallback) {
  try { const v = localStorage.getItem(key); return v ? JSON.parse(v) : fallback; } catch { return fallback; }
}

// URL of the MAGS embed app (used by pop-out button)
const MAGS_URL = 'https://dcmagstracked.172.188.51.215.nip.io/';

// Detect if this tab was opened as a pop-out (↗ button adds ?popout=1)
const IS_POPOUT = new URLSearchParams(window.location.search).get('popout') === '1';

// Read initial state snapshot written synchronously by the iframe before opening this tab
const _lsState = (() => {
  try {
    const v = localStorage.getItem('mags_popout');
    return v ? JSON.parse(v) : null;
  } catch { return null; }
})();

export default function EmbedApp() {
  const [input, setInput]               = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingMode, setProcessingMode] = useState(null); // 'debate' | 'knowledge' | null
  const [activeAgent, setActiveAgent]   = useState(null);
  const [currentRound, setCurrentRound] = useState(null);
  const [votes, setVotes]               = useState(() => _lsState?.votes            ?? loadLS('mags_votes', []));
  const [selectedAgent, setSelectedAgent] = useState(null);
  const [showFullChat, setShowFullChat] = useState(false);
  const [priorSummary, setPriorSummary] = useState(() => _lsState?.priorSummary    ?? loadLS('mags_prior_summary', null));
  const [tooltip, setTooltip]           = useState(null);
  // Single source of truth for all messages
  const [messages, setMessages]         = useState(() => _lsState?.messages         ?? loadLS('mags_messages', []));
  const [knowledgeFindings, setKnowledgeFindings] = useState(() => _lsState?.knowledgeFindings ?? loadLS('mags_findings', null));
  const [showFindings, setShowFindings] = useState(() => _lsState?.showFindings     ?? loadLS('mags_show_findings', false));
  const [summaryOpen, setSummaryOpen]   = useState(true);
  const [openSummaries, setOpenSummaries] = useState(() => _lsState?.openSummaries  ?? loadLS('mags_open_summaries', {}));

  const miniLogEndRef           = useRef(null);
  const fullLogEndRef           = useRef(null);
  const roundTracker            = useRef(0);
  const readerRef               = useRef(null);
  const findingsMsgId           = useRef(null);
  const findingsAnchorRef       = useRef(null);
  const fullChatFindingsRef     = useRef(null);
  const pendingScrollToFindings = useRef(false);
  const popoutRef               = useRef(null); // reference to the open pop-out window

  // On mount: clean URL param, consume one-shot popout key
  useEffect(() => {
    if (_lsState) localStorage.removeItem('mags_popout');
    if (IS_POPOUT) window.history.replaceState(null, '', window.location.pathname);
  }, []);

  // IFRAME → POP-OUT: listen for pop-out saying it's ready, then send full state snapshot
  useEffect(() => {
    if (IS_POPOUT) return;
    const onMsg = (e) => {
      if (e.data?.type === 'MAGS_READY') {
        try {
          e.source.postMessage({ type: 'MAGS_STATE', state:
            { messages, votes, priorSummary, knowledgeFindings, showFindings, openSummaries }
          }, '*');
        } catch {}
      }
    };
    window.addEventListener('message', onMsg);
    return () => window.removeEventListener('message', onMsg);
  }, [messages, votes, priorSummary, knowledgeFindings, showFindings, openSummaries]);

  // IFRAME → POP-OUT: push live state updates as debate streams in
  useEffect(() => {
    if (IS_POPOUT) return;
    try {
      if (popoutRef.current && !popoutRef.current.closed) {
        popoutRef.current.postMessage({ type: 'MAGS_UPDATE', state:
          { messages, votes, priorSummary, knowledgeFindings, showFindings, openSummaries }
        }, '*');
      }
    } catch {}
  }, [messages, votes, knowledgeFindings, showFindings]);

  // POP-OUT: signal ready to iframe on mount, then apply all incoming state
  useEffect(() => {
    if (!IS_POPOUT) return;
    try { window.opener?.postMessage({ type: 'MAGS_READY' }, '*'); } catch {}
    const onMsg = (e) => {
      const s = e.data?.state;
      if (!s) return;
      if (e.data.type === 'MAGS_STATE' || e.data.type === 'MAGS_UPDATE') {
        if (s.messages           !== undefined) setMessages(s.messages);
        if (s.votes              !== undefined) setVotes(s.votes);
        if (s.priorSummary       !== undefined) setPriorSummary(s.priorSummary);
        if (s.knowledgeFindings  !== undefined) setKnowledgeFindings(s.knowledgeFindings);
        if (s.showFindings       !== undefined) setShowFindings(s.showFindings);
        if (s.openSummaries      !== undefined) setOpenSummaries(s.openSummaries);
      }
    };
    window.addEventListener('message', onMsg);
    return () => window.removeEventListener('message', onMsg);
  }, []);

  // Persist to localStorage on every change so refresh restores state (works in pop-out; silently fails in sandboxed iframe)
  useEffect(() => { try { localStorage.setItem('mags_messages',       JSON.stringify(messages));          } catch {} }, [messages]);
  useEffect(() => { try { localStorage.setItem('mags_votes',          JSON.stringify(votes));             } catch {} }, [votes]);
  useEffect(() => { try { localStorage.setItem('mags_prior_summary',  JSON.stringify(priorSummary));      } catch {} }, [priorSummary]);
  useEffect(() => { try { localStorage.setItem('mags_findings',       JSON.stringify(knowledgeFindings)); } catch {} }, [knowledgeFindings]);
  useEffect(() => { try { localStorage.setItem('mags_show_findings',  JSON.stringify(showFindings));      } catch {} }, [showFindings]);
  useEffect(() => { try { localStorage.setItem('mags_open_summaries', JSON.stringify(openSummaries));     } catch {} }, [openSummaries]);

  useEffect(() => {
    if (pendingScrollToFindings.current && findingsAnchorRef.current) {
      pendingScrollToFindings.current = false;
      findingsAnchorRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
      return;
    }
    miniLogEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    if (!showFullChat) return;
    // Scroll to summary/docs message when opening, otherwise go to bottom
    if (fullChatFindingsRef.current) {
      setTimeout(() => fullChatFindingsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }), 50);
    } else {
      fullLogEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [showFullChat]);

  const addMsg = (type, agentId, speaker, content, round, meta = {}) => {
    setMessages(prev => [...prev, {
      id: Date.now() + Math.random(),
      type, agentId, speaker, content, round, ...meta
    }]);
  };

  const getPos = (index) => {
    const angle = (index / 6) * 2 * Math.PI - Math.PI / 2;
    return {
      svgX: 250 + Math.cos(angle) * 175,
      svgY: 250 + Math.sin(angle) * 175,
      left: `${50 + Math.cos(angle) * 35}%`,
      top:  `${50 + Math.sin(angle) * 35}%`,
    };
  };

  const handleStop = () => {
    if (readerRef.current) {
      readerRef.current.cancel();
      readerRef.current = null;
    }
    setIsProcessing(false);
    setActiveAgent(null);
    addMsg('system', null, 'System', 'Debate stopped by user.', null);
  };

  const handleNewSession = (e) => {
    e.stopPropagation();
    ['mags_messages','mags_votes','mags_prior_summary','mags_findings','mags_show_findings','mags_open_summaries']
      .forEach(k => localStorage.removeItem(k));
    setMessages([]);
    setVotes([]);
    setPriorSummary(null);
    setKnowledgeFindings(null);
    setShowFindings(false);
    setOpenSummaries({});
    setInput('');
    setActiveAgent(null);
    setCurrentRound(null);
    setProcessingMode(null);
    setSelectedAgent(null);
    setShowFullChat(false);
    roundTracker.current = 0;
  };

  const TRIVIAL_PATTERN = /^\s*(hi+|hello+|hey+|good\s*(morning|afternoon|evening|day|night)|morning|afternoon|evening|howdy|greetings|sup|what'?s\s*up|how\s*are\s*you|how'?s\s*it\s*going|thanks?|thank\s*you|ok+|okay|cool|great|nice|bye|goodbye|see\s*you|cheers)[!?.]*\s*$/i;

  const TRIVIAL_REPLY = `Hello! I'm the MAGS Orchestrator for datacenter cooling management.\n\nYou can ask me things like:\n• "Can we power down a chiller to save energy?"\n• "What are the chiller operating limits?"\n• "Is it safe to reduce cooling load by 15%?"\n• "Optimise cooling for peak afternoon demand"\n\nI'll either search the knowledge base or start a multi-agent debate to give you the best answer.`;

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!input.trim() || isProcessing) return;

    const query = input.trim();
    setInput('');

    if (messages.length > 0) {
      addMsg('divider', null, null, '── New Question ──', null);
    }
    addMsg('human', null, 'Human', query, null);

    // Handle trivial / greeting queries without calling the backend
    if (TRIVIAL_PATTERN.test(query)) {
      addMsg('greeting', 'orchestrator', 'Orchestrator', TRIVIAL_REPLY, null);
      return;
    }

    setVotes([]);
    roundTracker.current = 0;
    setCurrentRound(null);
    // Keep previous findings visible until new ones arrive
    setIsProcessing(true);
    setProcessingMode(null);
    setActiveAgent('orchestrator');

    try {
      const res = await fetch(`${BACKEND_URL}/analyze/stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          context: {
            cooling_load_kw: 2800, it_load_kw: 9500,
            wet_bulb_temp: 25.5, dry_bulb_temp: 31.0,
            humidity_percent: 78, current_pue: 1.24,
            chillers_online: ['Chiller-1', 'Chiller-2'],
            timestamp: new Date().toISOString(),
          },
          human_input: query,
          prior_summary: priorSummary || null,
        }),
      });

      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const reader  = res.body.getReader();
      readerRef.current = reader;
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop();

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const raw = line.slice(6).trim();
          if (!raw) continue;
          let event;
          try { event = JSON.parse(raw); } catch { continue; }

          if (event.__error__) {
            addMsg('error', null, 'System', `Error: ${event.__error__}`, null);
            break;
          }

          if (event.__docs__) {
            const d = event.data;
            const results = d?.results ?? [];
            setProcessingMode('knowledge');
            // Store for the front-page summary card
            setKnowledgeFindings({ count: results.length, results, summary: d?.summary || null });
            setShowFindings(true);
            setSummaryOpen(true);
            // Add anchor message to mini-log and scroll to it
            const anchorId = Date.now() + Math.random();
            findingsMsgId.current = anchorId;
            pendingScrollToFindings.current = true;
            setMessages(prev => [...prev, {
              id: anchorId,
              type: 'docs', agentId: 'orchestrator', speaker: 'Orchestrator', content: d, round: null
            }]);
            break;
          }

          if (event.__done__) {
            const decision = event.decision;
            if (decision?.executive_summary) {
              const s = decision.executive_summary;
              const isAdvisory = s.question_type === 'ADVISORY';
              const mainText = s.answer || s.description || 'No action required';
              addMsg('decision', 'orchestrator', 'Orchestrator', mainText, null, { isAdvisory });
              setPriorSummary(s.answer || s.description || '');
            }
            if (decision?.agent_consensus?.votes) {
              const v = Object.entries(decision.agent_consensus.votes).map(([agent, vote]) => ({
                agent,
                vote:      typeof vote === 'string' ? vote : (vote.vote || 'UNKNOWN'),
                reasoning: typeof vote === 'object' ? (vote.reasoning || '') : '',
              }));
              setVotes(v);
            }
            setActiveAgent(null);
            break;
          }

          if (event.__round__) {
            const rn = event.__round__;
            roundTracker.current = rn;
            setCurrentRound(rn);
            setProcessingMode('debate');
            const labels = { 1: 'Initial Proposals', 2: 'Debate & Vote' };
            addMsg('round', null, 'System', `Round ${rn}/2 — ${labels[rn] || ''}`, rn);
            continue;
          }

          const { speaker, message } = event;
          if (!speaker || !message) continue;
          if (speaker === 'HUMAN' || speaker === 'Human') continue;

          const agentId = nameToId(speaker);
          setActiveAgent(agentId);
          addMsg('agent', agentId, speaker, message, roundTracker.current || 1);
        }
      }
    } catch (err) {
      addMsg('error', null, 'System', `Error: ${err.message}`, null);
    } finally {
      setIsProcessing(false);
      setActiveAgent(null);
    }
  };

  // Messages for selected agent detail panel
  const selectedAgentData = selectedAgent ? ALL_AGENTS.find(a => a.id === selectedAgent) : null;
  const agentContributions = selectedAgent
    ? messages.filter(m => m.type === 'agent' && m.agentId === selectedAgent)
    : [];

  // Mini log: show condensed (160 chars) — used below the ring
  const renderMiniEntry = (msg) => {
    if (msg.type === 'divider') return (
      <div key={msg.id} className="emb-log-divider">{msg.content}</div>
    );
    if (msg.type === 'round') return (
      <div key={msg.id} className="emb-log-round">{msg.content}</div>
    );
    if (msg.type === 'human') return (
      <div key={msg.id} className="emb-log-entry emb-t-human">
        <span className="emb-log-icon emb-human-icon">👤</span>
        <span className="emb-log-text">{msg.content}</span>
      </div>
    );
    if (msg.type === 'docs') {
      const d = msg.content;
      const isFindingsAnchor = msg.id === findingsMsgId.current;
      const count = d.total_found ?? d.results?.length ?? 0;
      const isOpen = openSummaries[msg.id] !== false; // default open
      const toggleSummary = () => setOpenSummaries(prev => ({ ...prev, [msg.id]: !isOpen }));
      return (
        <div key={msg.id} ref={isFindingsAnchor ? findingsAnchorRef : null}
          className="emb-t-docs-block">
          <div className="emb-docs-header">
            <span className="emb-docs-icon">📚</span>
            <div className="emb-docs-meta">
              <span className="emb-docs-name">Knowledge Base — {count} result{count !== 1 ? 's' : ''}</span>
              <span className="emb-docs-source">{d.results?.[0]?.title || 'Documents retrieved'}</span>
            </div>
            <button className="emb-docs-toggle" onClick={toggleSummary}>
              {isOpen ? '▾' : '▸'} Summary
            </button>
          </div>
          {isOpen && d.summary && (
            <div className="emb-docs-summary">
              {d.summary}
            </div>
          )}
          {isOpen && !d.summary && (
            <div className="emb-docs-summary emb-docs-summary--empty">No summary available.</div>
          )}
        </div>
      );
    }
    if (msg.type === 'decision') return (
      <div key={msg.id} className="emb-log-entry emb-t-decision">
        <span className="emb-log-icon" style={{ background: '#00BFA5' }}>🎯</span>
        <div className="emb-log-body">
          <span className="emb-log-name">Final Decision</span>
          <span className="emb-log-text">{msg.content.split('\n')[0]}</span>
        </div>
      </div>
    );
    if (msg.type === 'greeting') return (
      <div key={msg.id} className="emb-greeting-block">
        <div className="emb-greeting-header">
          <span className="emb-greeting-icon">🎯</span>
          <span className="emb-greeting-name">Orchestrator</span>
        </div>
        <div className="emb-greeting-text">{msg.content}</div>
      </div>
    );
    if (msg.type === 'error') return (
      <div key={msg.id} className="emb-log-entry emb-t-error">
        <span className="emb-log-text">{msg.content}</span>
      </div>
    );
    // agent
    const ag = agentById(msg.agentId);
    const clean = msg.content.replace(/\[VOTE:[^\]]+\]/g, '').trim();
    const excerpt = clean.slice(0, 160) + (clean.length > 160 ? '…' : '');
    return (
      <div key={msg.id} className={`emb-log-entry emb-t-agent ${activeAgent === msg.agentId ? 'emb-t-speaking' : ''}`}
        style={{ '--col': ag.color }}>
        <span className="emb-log-icon" style={{ background: ag.color }}>{ag.icon}</span>
        <div className="emb-log-body">
          <span className="emb-log-name">{ag.name.replace(' Agent', '')}</span>
          <span className="emb-log-text">{excerpt}</span>
        </div>
      </div>
    );
  };

  // Relevance score → label + colour
  const scoreLabel = (score) => {
    if (score >= 0.75) return { label: 'High', color: '#4ade80' };
    if (score >= 0.50) return { label: 'Medium', color: '#FFA726' };
    return { label: 'Low', color: '#94a3b8' };
  };

  const collectionIcon = { 'SOPs': '📋', 'Equipment Manuals': '🔧', 'Regulations': '⚖️', 'KPI Definitions': '📊' };

  // Full chat: renders complete messages
  const renderFullEntry = (msg) => {
    if (msg.type === 'divider') return (
      <div key={msg.id} className="emb-fc-divider">{msg.content}</div>
    );
    if (msg.type === 'round') return (
      <div key={msg.id} className="emb-fc-round">{msg.content}</div>
    );
    if (msg.type === 'human') return (
      <div key={msg.id} className="emb-fc-msg emb-t-human">
        <div className="emb-fc-speaker"><span className="emb-fc-icon emb-human-icon">👤</span><span>Human</span></div>
        <div className="emb-fc-text">{msg.content}</div>
      </div>
    );
    if (msg.type === 'greeting') return (
      <div key={msg.id} className="emb-fc-msg emb-t-greeting">
        <div className="emb-fc-speaker"><span className="emb-fc-icon" style={{ background: '#00BFA5' }}>🎯</span><span>Orchestrator</span></div>
        <div className="emb-fc-text emb-fc-greeting-text">{msg.content}</div>
      </div>
    );
    if (msg.type === 'docs') {
      const d = msg.content;
      const isFindingsAnchor = msg.id === findingsMsgId.current;
      return (
        <div key={msg.id} ref={isFindingsAnchor ? fullChatFindingsRef : null} className="emb-fc-docs">
          <div className="emb-fc-docs-header">
            <span className="emb-fc-docs-icon">📚</span>
            <div className="emb-fc-docs-meta">
              <span className="emb-fc-docs-title">Knowledge Base Results</span>
              <span className="emb-fc-docs-query">"{d.query}"</span>
            </div>
            <span className="emb-fc-docs-count">{d.total_found} result{d.total_found !== 1 ? 's' : ''}</span>
          </div>
          {d.total_found === 0 ? (
            <div className="emb-fc-docs-empty">No matching documents found in the knowledge base.</div>
          ) : (
            <>
              {d.summary && (
                <div className="emb-doc-summary">
                  <div className="emb-doc-summary-label">
                    <span>📝</span> Summary <span className="emb-doc-summary-note">based solely on retrieved documents — no external knowledge</span>
                  </div>
                  <div className="emb-doc-summary-text">{d.summary}</div>
                </div>
              )}
              <div className="emb-doc-sources-label">Source Documents ({d.total_found})</div>
            <div className="emb-fc-docs-list">
              {d.results.map((r, i) => {
                const rel = scoreLabel(r.score);
                return (
                  <div key={i} className="emb-doc-card">
                    <div className="emb-doc-card-top">
                      <span className="emb-doc-collection-icon">{collectionIcon[r.collection] || '📄'}</span>
                      <div className="emb-doc-card-info">
                        <span className="emb-doc-collection">{r.collection}</span>
                        <span className="emb-doc-title">{r.title}</span>
                      </div>
                      <span className="emb-doc-relevance" style={{ color: rel.color }}>
                        {rel.label} relevance
                      </span>
                    </div>
                    <div className="emb-doc-text">{r.text}</div>
                    <div className="emb-doc-source">
                      <span className="emb-doc-source-label">Source:</span>
                      <span className="emb-doc-source-val">{r.source}</span>
                      {r.page && <span className="emb-doc-page">p.{r.page}</span>}
                      {r.section && <span className="emb-doc-section">§ {r.section}</span>}
                    </div>
                  </div>
                );
              })}
            </div>
            </>
          )}
        </div>
      );
    }
    if (msg.type === 'decision') {
      const sections = parseDecisionSections(msg.content);
      const isAdvisory = msg.isAdvisory;
      const decisionColor = isAdvisory ? '#4A9EFF' : '#00BFA5';
      const decisionLabel = isAdvisory ? 'Orchestrator — Path Forward' : 'Orchestrator — Final Decision';
      return (
        <div key={msg.id} className={`emb-fc-msg emb-t-decision${isAdvisory ? ' emb-t-advisory' : ''}`}>
          <div className="emb-fc-speaker">
            <span className="emb-fc-icon" style={{ background: decisionColor }}>🎯</span>
            <span>{decisionLabel}</span>
          </div>
          <div className="emb-fc-text emb-decision-body">
            {sections ? sections.map((sec, i) => (
              <div key={i} className="emb-decision-section">
                <span className="emb-decision-section-title">{sec.title}</span>
                {sec.lines.length === 1
                  ? <p className="emb-decision-line">{sec.lines[0]}</p>
                  : <ul className="emb-decision-list">
                      {sec.lines.map((l, j) => <li key={j}>{l}</li>)}
                    </ul>
                }
              </div>
            )) : <p className="emb-decision-line">{msg.content}</p>}
          </div>
        </div>
      );
    }
    if (msg.type === 'error') return (
      <div key={msg.id} className="emb-fc-msg emb-t-error">
        <div className="emb-fc-text">{msg.content}</div>
      </div>
    );
    // agent
    const ag = agentById(msg.agentId);
    const voteMatch = msg.content?.match(/\[VOTE:\s*([^\]]+)\]/);
    const voteVal   = voteMatch?.[1]?.trim();
    const bodyText  = msg.content?.replace(/\[VOTE:[^\]]+\]/, '').trim();
    return (
      <div key={msg.id} className="emb-fc-msg emb-t-agent" style={{ '--col': ag.color }}>
        <div className="emb-fc-speaker">
          <span className="emb-fc-icon" style={{ background: ag.color }}>{ag.icon}</span>
          <span>{msg.speaker}</span>
          {msg.round && <span className="emb-fc-round-tag">R{msg.round}</span>}
          {voteVal && <span className={`emb-vote-badge ${voteClass(voteVal)}`}>{voteVal}</span>}
        </div>
        <div className="emb-fc-text" style={{ '--col': ag.color }}>
          <div className="emb-fc-body emb-fc-highlighted">{highlightText(bodyText)}</div>
          {(() => {
            const pts = getAgentSummary(bodyText);
            if (!pts) return null;
            return (
              <div className="emb-fc-gist">
                <span className="emb-fc-gist-label">Key Points</span>
                <ul className="emb-fc-gist-list">
                  {pts.map((p, i) => <li key={i}>{p}</li>)}
                </ul>
              </div>
            );
          })()}
        </div>
      </div>
    );
  };

  return (
    <div className="emb-shell" onClick={() => setSelectedAgent(null)}>

      {/* ── Header ──────────────────────────────────── */}
      <header className="emb-header">
        <div className="emb-brand">
          {currentRound && isProcessing && <span className="emb-round-pill">R{currentRound}/2</span>}
        </div>
        <div className="emb-btn-group">
          <button className="emb-btn-group-item emb-btn-danger" title="Clear all logs and start fresh"
            onClick={handleNewSession}>
            ↺ New Session
          </button>
          <button className="emb-btn-group-item" title="View full debate log"
            onClick={e => { e.stopPropagation(); setShowFullChat(true); }}>
            ☰ Chat Logs
          </button>
          <button className="emb-btn-group-item" title="Open in new window"
            onClick={e => {
              e.stopPropagation();
              // Open without noopener so the new tab can postMessage back via window.opener
              const w = window.open(MAGS_URL + '?popout=1', '_blank');
              if (w) popoutRef.current = w;
            }}>
            ↗
          </button>
        </div>
      </header>

      {/* ── Agent Ring — centred ─────────────────────── */}
      <div className="emb-ring-section" onClick={e => e.stopPropagation()}>
        <div className="emb-ring-container">
          <svg className="emb-connections" viewBox="0 0 500 500">
            <defs>
              <filter id="emb-glow">
                <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
                <feMerge><feMergeNode in="coloredBlur"/><feMergeNode in="SourceGraphic"/></feMerge>
              </filter>
            </defs>
            <circle cx="250" cy="250" r="175" fill="none" stroke="#00BFA5"
              strokeWidth="1.5" strokeDasharray="8 8" opacity="0.7" className="emb-orbit"/>
            {AGENTS.map(agent => {
              const pos = getPos(agent.position);
              const isActive = activeAgent === agent.id;
              return (
                <line key={agent.id} x1="250" y1="250" x2={pos.svgX} y2={pos.svgY}
                  stroke={isActive ? agent.color : '#5a7a9a'}
                  strokeWidth={isActive ? 2 : 1}
                  strokeDasharray={isActive ? '0' : '5,5'}
                  opacity={isActive ? 0.9 : 0.5}
                  filter={isActive ? 'url(#emb-glow)' : ''}/>
              );
            })}
          </svg>

          {/* Orchestrator centre */}
          <div
            className={`emb-node emb-orch ${activeAgent === 'orchestrator' ? 'active' : ''} ${selectedAgent === 'orchestrator' ? 'selected' : ''}`}
            style={{ left: '50%', top: '50%', '--col': ORCHESTRATOR.color }}
            onClick={e => { e.stopPropagation(); setSelectedAgent(s => s === 'orchestrator' ? null : 'orchestrator'); }}
            onMouseEnter={e => setTooltip({ agent: ORCHESTRATOR, x: e.clientX, y: e.clientY })}
            onMouseMove={e  => setTooltip(t => t ? { ...t, x: e.clientX, y: e.clientY } : t)}
            onMouseLeave={() => setTooltip(null)}>
            <div className="emb-icon" style={{ background: ORCHESTRATOR.color }}>{ORCHESTRATOR.icon}</div>
            <div className="emb-name">{ORCHESTRATOR.shortName}</div>
          </div>

          {/* 6 Agents */}
          {AGENTS.map(agent => {
            const pos = getPos(agent.position);
            return (
              <div key={agent.id}
                className={`emb-node ${activeAgent === agent.id ? 'active' : ''} ${selectedAgent === agent.id ? 'selected' : ''}`}
                style={{ left: pos.left, top: pos.top, '--col': agent.color }}
                onClick={e => { e.stopPropagation(); setSelectedAgent(s => s === agent.id ? null : agent.id); }}
                onMouseEnter={e => setTooltip({ agent, x: e.clientX, y: e.clientY })}
                onMouseMove={e  => setTooltip(t => t ? { ...t, x: e.clientX, y: e.clientY } : t)}
                onMouseLeave={() => setTooltip(null)}>
                <div className="emb-icon" style={{ background: agent.color }}>{agent.icon}</div>
                <div className="emb-name">{agent.shortName}</div>
              </div>
            );
          })}
        </div>
      </div>

      {/* ── Processing indicator — mode-aware ── */}
      {isProcessing && (
        <div className="emb-searching-bar">
          <span className="emb-searching-dot" /><span className="emb-searching-dot" /><span className="emb-searching-dot" />
          <span>
            {processingMode === 'debate'
              ? 'Agents are discussing…'
              : 'Orchestrator sweeping knowledge base…'}
          </span>
        </div>
      )}

      {/* ── Mini Chat Log — below ring ───────────────── */}
      <div className="emb-mini-log">
        {messages.length === 0 && !isProcessing && (
          <p className="emb-log-empty">Ask a question to start the debate.</p>
        )}
        {messages.map(renderMiniEntry)}
        <div ref={miniLogEndRef}/>
      </div>

      {/* ── Agent Votes ──────────────────────────────── */}
      {votes.length > 0 && (
        <div className="emb-votes-section">
          <div className="emb-votes-title">Agent Votes</div>
          <div className="emb-votes-list">
            {votes.map((v, i) => {
              const agent = AGENTS.find(a =>
                v.agent.toLowerCase().includes(a.id) ||
                v.agent.toLowerCase().includes(a.name.toLowerCase().split(' ')[0])
              );
              return (
                <div key={i} className="emb-vote-row">
                  <div className="emb-vote-left">
                    <span className="emb-vote-icon" style={{ background: agent?.color || '#64748B' }}>{agent?.icon || '•'}</span>
                    <span className="emb-vote-agent">{agent?.name?.split(' ')[0] || v.agent.split(' ')[0]}</span>
                    <span className={`emb-vote-badge ${voteClass(v.vote)}`}>{v.vote}</span>
                  </div>
                  {v.reasoning && (
                    <div className="emb-vote-reason">
                      {v.reasoning.slice(0, 120)}{v.reasoning.length > 120 ? '…' : ''}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Suggested Questions ──────────────────────── */}
      {messages.length === 0 && !isProcessing && (
        <div className="emb-suggest-row" onClick={e => e.stopPropagation()}>
          <span className="emb-suggest-label">Suggested</span>
          <div className="emb-suggest-chips">
            {SUGGESTED_QUESTIONS.map((q, i) => (
              <button key={i} className="emb-suggest-chip" onClick={() => setInput(q)}>
                {q}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* ── Input ────────────────────────────────────── */}
      <form className="emb-input-bar" onSubmit={handleSubmit} onClick={e => e.stopPropagation()}>
        <button type="button" className="emb-info-btn" tabIndex={-1}>
          i
          <div className="emb-info-tooltip">
            <span className="emb-info-title">Orchestrator Modes</span>
            <div className="emb-info-item">
              <span className="emb-info-mode">Debate</span>
              <span>Agents discuss, challenge each other, and vote to reach a consensus decision.</span>
            </div>
            <div className="emb-info-item">
              <span className="emb-info-mode">Knowledge Search</span>
              <span>Retrieves and summarises relevant documents from the knowledge base.</span>
            </div>
          </div>
        </button>
        <input type="text" value={input} onChange={e => setInput(e.target.value)}
          placeholder="Ask orchestrator anything…" />
        {isProcessing ? (
          <button type="button" className="emb-stop-btn" onClick={handleStop} title="Stop debate">■</button>
        ) : (
          <button type="submit" disabled={!input.trim()}>➤</button>
        )}
      </form>

      {/* ── Agent Detail Slide Panel ─────────────────── */}
      {selectedAgentData && (
        <div className="emb-detail-panel" onClick={e => e.stopPropagation()}>
          <button className="emb-detail-close" onClick={() => setSelectedAgent(null)}>✕</button>
          <div className="emb-detail-head">
            <div className="emb-detail-icon" style={{ background: selectedAgentData.color }}>
              {selectedAgentData.icon}
            </div>
            <div>
              <div className="emb-detail-name">{selectedAgentData.name.replace(' Agent', '')}</div>
              <div className="emb-detail-role">{selectedAgentData.role}</div>
            </div>
          </div>
          <p className="emb-detail-desc">{selectedAgentData.description}</p>
          <div className="emb-detail-section-title">Responsibilities</div>
          <ul className="emb-detail-list">
            {selectedAgentData.responsibilities.map(r => <li key={r}>{r}</li>)}
          </ul>

          {agentContributions.length > 0 && (
            <>
              <div className="emb-detail-section-title" style={{ marginTop: 14 }}>
                Responses ({agentContributions.length})
              </div>
              <div className="emb-contributions">
                {agentContributions.map((m, i) => {
                  const voteMatch = m.content?.match(/\[VOTE:\s*([^\]]+)\]/);
                  const voteVal   = voteMatch?.[1]?.trim();
                  const body      = m.content?.replace(/\[VOTE:[^\]]+\]/, '').trim();
                  return (
                    <div key={i} className="emb-contribution">
                      <div className="emb-contribution-meta">
                        {m.round && <span className="emb-round-tag">Round {m.round}</span>}
                        {voteVal && <span className={`emb-vote-badge ${voteClass(voteVal)}`}>{voteVal}</span>}
                      </div>
                      <p>{body}</p>
                    </div>
                  );
                })}
              </div>
            </>
          )}
          {agentContributions.length === 0 && (
            <p className="emb-no-contributions">No responses yet for this agent.</p>
          )}
        </div>
      )}

      {/* ── Hover Tooltip ───────────────────────────── */}
      {tooltip && (
        <div className="emb-tooltip" style={{ left: tooltip.x + 14, top: tooltip.y - 10 }}>
          <div className="emb-tooltip-header" style={{ color: tooltip.agent.color }}>
            {tooltip.agent.icon} {tooltip.agent.name}
          </div>
          <div className="emb-tooltip-role">{tooltip.agent.role}</div>
          <div className="emb-tooltip-desc">{tooltip.agent.description}</div>
          <ul className="emb-tooltip-list">
            {tooltip.agent.responsibilities.map(r => <li key={r}>{r}</li>)}
          </ul>
        </div>
      )}

      {/* ── Full Chat Overlay ────────────────────────── */}
      {showFullChat && (
        <div className="emb-fullchat-overlay">
          <div className="emb-fullchat-header">
            <span>Full Debate Log</span>
            <button className="emb-fc-close" onClick={() => setShowFullChat(false)}>✕</button>
          </div>
          <div className="emb-fullchat-body">
            {messages.length === 0 ? (
              <p className="emb-log-empty">No debate yet — ask a question first.</p>
            ) : (
              messages.map(renderFullEntry)
            )}
            <div ref={fullLogEndRef}/>
          </div>
        </div>
      )}
    </div>
  );
}
