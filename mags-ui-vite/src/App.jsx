import React, { useState, useEffect, useRef } from 'react';
import './App.css';

const BACKEND_URL = '/api';

function App() {
  const [debateMessages, setDebateMessages] = useState([]);
  const [selectedAgent, setSelectedAgent] = useState(null);
  const [input, setInput] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [activeAgent, setActiveAgent] = useState(null);
  const [currentRound, setCurrentRound] = useState(null);
  const [sessionId, setSessionId] = useState(null);
  const [systemStatus, setSystemStatus] = useState(null);
  const [agentVotes, setAgentVotes] = useState([]);
  const [priorSummary, setPriorSummary] = useState(null);
  const [isDark, setIsDark] = useState(true);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [tooltip, setTooltip] = useState(null); // { agent, x, y }
  const [knowledgeFindings, setKnowledgeFindings] = useState(null); // { count, results }
  const [showFindings, setShowFindings] = useState(false);
  const [summaryOpen, setSummaryOpen] = useState(true);
  const messagesEndRef = useRef(null);
  const messagesContainerRef = useRef(null);
  const roundTracker = useRef(0); // tracks current round outside state updaters
  const readerRef = useRef(null); // holds active SSE reader for cancellation
  const findingsMsgId = useRef(null);   // ID of the knowledge-header message in chat
  const findingsStartRef = useRef(null); // DOM ref for that message
  const pendingScrollToFindings = useRef(false);

  const agents = [
    {
      id: 'demand', name: 'Demand & Conditions Agent', shortName: 'Demand &\nConditions', role: 'Forecasting', icon: '📊', color: '#4A9EFF', position: 0,
      description: 'Forecasts cooling demand based on IT load trends, wet-bulb temperature, and occupancy schedules.',
      responsibilities: ['Real-time load forecasting', 'Weather & humidity analysis', 'Peak demand prediction', 'Thermal trend monitoring']
    },
    {
      id: 'chiller', name: 'Chiller Optimization Agent', shortName: 'Chiller\nOptimization', role: 'Staging & Efficiency', icon: '❄️', color: '#9C6ADE', position: 1,
      description: 'Determines optimal chiller staging configuration to maximise COP while meeting cooling load.',
      responsibilities: ['Chiller staging decisions', 'COP optimisation', 'Part-load efficiency', 'Sequencing & rotation logic']
    },
    {
      id: 'building', name: 'Building System Agent', shortName: 'Building\nSystem', role: 'Pumps & Towers', icon: '🏢', color: '#00BFA5', position: 2,
      description: 'Manages pumps, cooling towers, and auxiliary systems to support chiller plant operations.',
      responsibilities: ['Pump VFD control', 'Cooling tower fan staging', 'Flow balancing', 'Delta-T optimisation']
    },
    {
      id: 'energy', name: 'Energy & Cost Optimization Agent', shortName: 'Energy &\nCost', role: 'Cost Optimization', icon: '💰', color: '#FFA726', position: 3,
      description: 'Evaluates energy tariffs, PUE impact, and cost trade-offs for every proposed operational change.',
      responsibilities: ['Energy cost modelling', 'PUE tracking', 'Tariff period awareness', 'Demand charge avoidance']
    },
    {
      id: 'maintenance', name: 'Maintenance & Compliance Agent', shortName: 'Maintenance\n& Compliance', role: 'Equipment Health', icon: '🔧', color: '#EC407A', position: 4,
      description: 'Monitors equipment health, maintenance schedules, and regulatory compliance constraints.',
      responsibilities: ['Equipment health scoring', 'Maintenance window tracking', 'Warranty compliance', 'Run-hour balancing']
    },
    {
      id: 'safety', name: 'Operations & Safety Agent', shortName: 'Operations\n& Safety', role: 'Safety & SOPs', icon: '🛡️', color: '#EF5350', position: 5,
      description: 'Enforces safety protocols, N+1 redundancy requirements, and operational SOPs. Holds veto authority.',
      responsibilities: ['N+1 redundancy enforcement', 'SOP compliance checks', 'Veto authority on unsafe actions', 'Alarm & interlock monitoring']
    }
  ];

  const orchestrator = {
    id: 'orchestrator',
    name: 'Orchestrator',
    role: 'System Coordinator',
    icon: '🎯',
    color: '#00BFA5',
    description: 'Coordinates all agents through a structured 4-round debate, builds consensus, and presents decisions for human approval.',
    responsibilities: ['Multi-agent debate facilitation', 'Consensus building', 'Human approval interface', 'Decision logging & audit trail']
  };

  useEffect(() => {
    fetchSystemStatus();
  }, []);


  useEffect(() => {
    const handler = () => setIsFullscreen(!!document.fullscreenElement);
    document.addEventListener('fullscreenchange', handler);
    return () => document.removeEventListener('fullscreenchange', handler);
  }, []);

  useEffect(() => {
    const el = messagesContainerRef.current;
    if (!el) return;
    // Scroll to findings anchor when knowledge results arrive
    if (pendingScrollToFindings.current && findingsStartRef.current) {
      pendingScrollToFindings.current = false;
      findingsStartRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
      return;
    }
    // Otherwise auto-scroll only if near the bottom
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 120;
    if (nearBottom) messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [debateMessages]);

  const fetchSystemStatus = async () => {
    try {
      const response = await fetch(`${BACKEND_URL}/status`);
      const data = await response.json();
      setSystemStatus(data);
    } catch (error) {
      console.error('Status fetch failed:', error);
    }
  };

  // Returns both SVG viewBox coords (for <line>) and CSS % (for <div>)
  const getAgentPosition = (index) => {
    const angle = (index / 6) * 2 * Math.PI - Math.PI / 2;
    const cos = Math.cos(angle);
    const sin = Math.sin(angle);
    return {
      svgX: 250 + cos * 175,   // viewBox units — used in <line x2 y2>
      svgY: 250 + sin * 175,
      left: `${50 + cos * 35}%`, // % of container — used in div style
      top:  `${50 + sin * 35}%`
    };
  };

  const addMessage = (speaker, content, type = 'agent', round = null) => {
    const message = {
      id: Date.now() + Math.random(),
      speaker,
      content,
      type,
      round,
      timestamp: new Date()
    };
    setDebateMessages(prev => [...prev, message]);
  };

  const handleStop = () => {
    if (readerRef.current) {
      readerRef.current.cancel();
      readerRef.current = null;
    }
    setIsProcessing(false);
    setActiveAgent(null);
    addMessage('System', 'Debate stopped by user.', 'system');
  };

  const TRIVIAL_PATTERN = /^\s*(hi+|hello+|hey+|good\s*(morning|afternoon|evening|day|night)|morning|afternoon|evening|howdy|greetings|sup|what'?s\s*up|how\s*are\s*you|how'?s\s*it\s*going|thanks?|thank\s*you|ok+|okay|cool|great|nice|bye|goodbye|see\s*you|cheers)[!?.]*\s*$/i;

  const TRIVIAL_REPLY = `Hello! I'm the MAGS Orchestrator for datacenter cooling management.\n\nYou can ask me things like:\n• "Can we power down a chiller to save energy?"\n• "What are the chiller operating limits?"\n• "Is it safe to reduce cooling load by 15%?"\n• "Optimise cooling for peak afternoon demand"\n\nI'll either search the knowledge base or run a multi-agent debate to give you the best answer.`;

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!input.trim() || isProcessing) return;

    const query = input.trim();
    setInput('');
    setAgentVotes([]);
    roundTracker.current = 0;

    // If previous messages exist, insert a question divider (not a full clear)
    setDebateMessages(prev => {
      const divider = prev.length > 0 ? [{
        id: Date.now() + Math.random(),
        speaker: 'System',
        content: `── New Question ──`,
        type: 'question-divider',
        round: null,
        timestamp: new Date()
      }] : [];
      return [
        ...prev,
        ...divider,
        {
          id: Date.now() + Math.random() + 0.1,
          speaker: 'Human',
          content: query,
          type: 'human',
          round: null,
          timestamp: new Date()
        }
      ];
    });

    // Handle trivial / greeting queries without calling the backend
    if (TRIVIAL_PATTERN.test(query)) {
      addMessage('Orchestrator', TRIVIAL_REPLY, 'greeting');
      return;
    }

    setKnowledgeFindings(null);
    setShowFindings(false);
    setIsProcessing(true);
    setActiveAgent('orchestrator');
    addMessage('Orchestrator', 'Analyzing query...', 'system');

    try {
      const response = await fetch(`${BACKEND_URL}/analyze/stream`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          context: {
            cooling_load_kw: 2800,
            it_load_kw: 9500,
            wet_bulb_temp: 25.5,
            dry_bulb_temp: 31.0,
            humidity_percent: 78,
            current_pue: 1.24,
            chillers_online: ['Chiller-1', 'Chiller-2'],
            timestamp: new Date().toISOString()
          },
          human_input: query,
          prior_summary: priorSummary || null
        })
      });

      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      const reader = response.body.getReader();
      readerRef.current = reader;
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop(); // keep incomplete last line

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const raw = line.slice(6).trim();
          if (!raw) continue;

          let event;
          try { event = JSON.parse(raw); } catch { continue; }

          if (event.__error__) {
            addMessage('System', `Error: ${event.__error__}`, 'error');
            break;
          }

          if (event.__docs__) {
            const docs = event.data;
            const results = docs?.results ?? [];
            // Store for the findings panel
            setKnowledgeFindings({ count: results.length, results, summary: docs?.summary || null });
            setShowFindings(true);
            setSummaryOpen(true);
            // Add a single anchor message to the chat log
            const anchorId = Date.now() + Math.random();
            findingsMsgId.current = anchorId;
            pendingScrollToFindings.current = true;
            setDebateMessages(prev => [...prev, {
              id: anchorId,
              speaker: 'Knowledge Base',
              content: results.length === 0
                ? 'No relevant documents found for that query.'
                : `Found ${results.length} relevant section(s) — see Findings panel →`,
              type: 'knowledge-header',
              round: null,
              timestamp: new Date()
            }]);
            break;
          }

          if (event.__done__) {
            const decision = event.decision;
            if (decision) {
              extractVotes(decision);
              displayFinalSummary(decision);
              setSessionId(decision.session_id);
              // Store summary for follow-up context
              if (decision.executive_summary) {
                const s = decision.executive_summary;
                setPriorSummary(`Recommendation: ${s.recommendation}\n${s.description || ''}\nConfidence: ${s.confidence ? (s.confidence * 100).toFixed(0) + '%' : 'N/A'}`);
              }
            }
            break;
          }

          // ── Explicit round marker from backend ───────────────────────────
          if (event.__round__) {
            const roundNum = event.__round__;
            const roundLabels = {
              1: '── Round 1: Initial Proposals ──',
              2: '── Round 2: Debate & Vote ──'
            };
            roundTracker.current = roundNum;
            setCurrentRound(roundNum);
            setDebateMessages(prev => [...prev, {
              id: Date.now() + Math.random(),
              speaker: 'System',
              content: roundLabels[roundNum] || `── Round ${roundNum} ──`,
              type: 'round-divider',
              round: roundNum,
              timestamp: new Date()
            }]);
            continue;
          }

          // ── Regular agent message ─────────────────────────────────────
          const { speaker, message } = event;
          if (!speaker || !message) continue;
          if (speaker === 'HUMAN' || speaker === 'Human') continue;

          setActiveAgent(getAgentIdFromName(speaker));

          const isVote = message.includes('[VOTE:');

          setDebateMessages(prev => [...prev, {
            id: Date.now() + Math.random(),
            speaker,
            content: message,
            type: getMessageType(speaker, message),
            round: roundTracker.current || 1,
            timestamp: new Date()
          }]);

          // ── Vote panel ───────────────────────────────────────────────────
          if (isVote) {
            const vm = message.match(/\[VOTE:\s*([^\]]+)\]/);
            if (vm) {
              const voteDecision = vm[1].trim();
              const reasoning = message
                .replace(vm[0], '')
                .replace(/^VOTE:\s*\S+\s*/i, '')
                .replace(/^REASONING:\s*/i, '')
                .trim();
              setAgentVotes(prev =>
                prev.find(v => v.agent === speaker)
                  ? prev
                  : [...prev, { agent: speaker, vote: voteDecision, reasoning }]
              );
            }
          }

          setTimeout(() => setActiveAgent(null), 600);
        }
      }

    } catch (error) {
      console.error('Stream error:', error);
      addMessage('System', `Error: ${error.message}`, 'error');
    } finally {
      setIsProcessing(false);
      setActiveAgent(null);
    }
  };

  const processBackendResponse = (decision) => {
    console.log('Processing decision...');
    
    // Look in ALL possible places for conversation data
    const possibleLogs = [
      decision.full_conversation_log,
      decision.debate_transcript_summary?.full_debate_log,
      decision.full_debate_log,
      decision.conversation_log,
      decision.debate_log,
      decision.transcript
    ];
    
    let conversationLog = null;
    for (let i = 0; i < possibleLogs.length; i++) {
      const log = possibleLogs[i];
      if (log && Array.isArray(log) && log.length > 0) {
        conversationLog = log;
        console.log(`Found conversation log at index ${i}, length: ${log.length}`);
        break;
      }
    }
    
    if (conversationLog) {
      console.log(`Processing ${conversationLog.length} messages from conversation log`);
      displayConversationLog(conversationLog, decision);
      return;
    }
    
    // If no conversation log, try to reconstruct from debate_result
    if (decision.debate_result) {
      console.log('No conversation log found, trying debate_result...');
      reconstructFromDebateResult(decision.debate_result, decision);
      return;
    }
    
    // Last resort
    console.error('No usable conversation data found!');
    console.log('Available keys:', Object.keys(decision));
    
    addMessage('System', 'Backend completed debate but conversation log is not in expected format', 'error');
    
    if (decision.executive_summary) {
      displayFinalSummary(decision);
    }
    
    extractVotes(decision);
  };

  const displayConversationLog = (log, decision) => {
    console.log(`Starting to display ${log.length} messages...`);
    
    let currentIndex = 0;
    
    const interval = setInterval(() => {
      if (currentIndex >= log.length) {
        clearInterval(interval);
        console.log('All messages displayed');
        
        // Small delay before showing final results
        setTimeout(() => {
          setActiveAgent(null);
          extractVotes(decision);
          displayFinalSummary(decision);
        }, 300);
        return;
      }
      
      const msg = log[currentIndex];
      const speaker = msg.speaker || msg.agent || 'Unknown';
      const content = msg.message || msg.content || msg.text || '';
      
      if (!content) {
        currentIndex++;
        return; // Skip empty, continue interval
      }
      
      console.log(`[${currentIndex + 1}/${log.length}] ${speaker}: ${content.substring(0, 50)}...`);
      
      // Detect round
      let round = msg.round || null;
      if (!round && typeof content === 'string') {
        if (content.includes('━━━ ROUND 1') || content.includes('ROUND 1')) round = 1;
        else if (content.includes('━━━ ROUND 2') || content.includes('ROUND 2')) round = 2;
        else if (content.includes('━━━ ROUND 3') || content.includes('ROUND 3')) round = 3;
        else if (content.includes('━━━ ROUND 4') || content.includes('ROUND 4')) round = 4;
      }
      
      if (round) {
        console.log(`Setting round to ${round}`);
        setCurrentRound(round);
      }
      
      // Light up agent
      const agentId = getAgentIdFromName(speaker);
      setActiveAgent(agentId);
      
      // Add message
      setDebateMessages(prev => [...prev, {
        id: Date.now() + Math.random(),
        speaker,
        content,
        type: getMessageType(speaker, content),
        round,
        timestamp: new Date()
      }]);
      
      // Clear agent highlight after short time
      setTimeout(() => setActiveAgent(null), 400);
      
      currentIndex++;
      
    }, 800); // Fire every 800ms
  };

  const reconstructFromDebateResult = (debateResult, decision) => {
    console.log('Reconstructing from debate_result...');
    
    const rounds = debateResult.rounds || [];
    const messages = [];
    
    // Build all messages first
    rounds.forEach((round, roundIdx) => {
      const roundNum = round.round || roundIdx + 1;
      
      messages.push({
        speaker: 'System',
        message: `━━━ ROUND ${roundNum}: ${round.phase || 'Debate'} ━━━`,
        type: 'system',
        round: roundNum
      });
      
      if (round.proposals && Array.isArray(round.proposals)) {
        round.proposals.forEach(prop => {
          const agent = prop.agent || 'Unknown Agent';
          const content = `[PROPOSAL] ${prop.proposal_type || prop.action_type || 'ACTION'}: ${prop.description || prop.justification || 'No details'}`;
          messages.push({
            speaker: agent,
            message: content,
            type: 'proposal',
            round: roundNum
          });
        });
      }
      
      if (round.responses && Array.isArray(round.responses)) {
        round.responses.forEach(resp => {
          const agent = resp.agent || 'Unknown Agent';
          messages.push({
            speaker: agent,
            message: resp.response_text || resp.content || 'Response given',
            type: 'response',
            round: roundNum
          });
        });
      }
      
      if (round.votes && Array.isArray(round.votes)) {
        round.votes.forEach(vote => {
          const agent = vote.agent || 'Unknown Agent';
          const voteText = `[VOTE: ${vote.vote}]\n\n${vote.reasoning || vote.reasoning_text || 'No reasoning provided'}`;
          messages.push({
            speaker: agent,
            message: voteText,
            type: 'vote',
            round: roundNum
          });
        });
      }
    });
    
    console.log(`Built ${messages.length} messages from debate_result`);
    
    // Display them sequentially
    displayConversationLog(messages, decision);
  };

  const extractVotes = (decision) => {
    console.log('Extracting votes...');
    
    let votes = [];
    
    if (decision.agent_consensus?.votes) {
      const votesObj = decision.agent_consensus.votes;
      votes = Object.entries(votesObj).map(([agent, vote]) => ({
        agent,
        vote: typeof vote === 'string' ? vote : (vote.vote || vote.decision || 'UNKNOWN'),
        reasoning: typeof vote === 'object' ? (vote.reasoning || vote.reasoning_text || '') : ''
      }));
    }
    
    if (votes.length === 0 && decision.debate_result?.rounds) {
      const lastRound = decision.debate_result.rounds[decision.debate_result.rounds.length - 1];
      if (lastRound?.votes) {
        votes = lastRound.votes.map(v => ({
          agent: v.agent,
          vote: v.vote || v.decision,
          reasoning: v.reasoning || v.reasoning_text || ''
        }));
      }
    }
    
    console.log(`Extracted ${votes.length} votes:`, votes);
    
    if (votes.length > 0) {
      setAgentVotes(votes);
    }
  };

  const displayFinalSummary = (decision) => {
    const summary = decision.executive_summary;
    if (!summary) return;

    const confidence = summary.confidence != null ? `${(summary.confidence * 100).toFixed(0)}%` : 'N/A';
    const consensus = summary.consensus_strength || decision.agent_consensus?.consensus_strength || 'N/A';

    // Primary answer — use LLM-synthesised answer when available, fall back to description
    const mainText = summary.answer || summary.description || 'No action required';

    let summaryText = `${mainText}\n\nRecommended action: ${summary.recommendation}\nConfidence: ${confidence} | Consensus: ${consensus}`;

    // Secondary options
    if (summary.secondary_options && summary.secondary_options.length > 0) {
      summaryText += `\n\nAlternatives raised during debate:\n${summary.secondary_options.join('\n')}`;
    }

    addMessage('Orchestrator', summaryText, 'decision');
  };

  const getMessageType = (speaker, message) => {
    if (!message) return 'agent';
    const msgStr = typeof message === 'string' ? message : '';
    
    if (speaker === 'Human' || speaker === 'HUMAN') return 'human';
    if (speaker === 'System' || speaker === 'SYSTEM') return 'system';
    if (speaker === 'Orchestrator' && msgStr.includes('✅')) return 'decision';
    if (msgStr.includes('ROUND') || msgStr.includes('━━━')) return 'system';
    if (msgStr.includes('[VOTE:')) return 'vote';
    return 'agent';
  };

  const getAgentIdFromName = (name) => {
    if (!name) return null;
    if (name === 'Orchestrator') return 'orchestrator';
    
    const nameLower = name.toLowerCase();
    const agent = agents.find(a => 
      nameLower.includes(a.name.toLowerCase()) ||
      nameLower.includes(a.name.split(' ')[0].toLowerCase())
    );
    return agent ? agent.id : null;
  };

  const getAgentColor = (speaker) => {
    if (speaker === 'Orchestrator') return orchestrator.color;
    if (speaker === 'Human' || speaker === 'HUMAN') return '#4A9EFF';
    if (speaker === 'System' || speaker === 'SYSTEM') return '#9C6ADE';
    
    if (!speaker) return '#64748B';
    
    const agent = agents.find(a => 
      speaker.toLowerCase().includes(a.name.toLowerCase()) ||
      speaker.toLowerCase().includes(a.name.split(' ')[0].toLowerCase())
    );
    return agent ? agent.color : '#64748B';
  };

  const handleAgentClick = (agentId) => {
    setSelectedAgent(agentId);
  };

  const selectedAgentData = agents.find(a => a.id === selectedAgent);
  const selectedAgentMessages = debateMessages.filter(m => {
    if (!selectedAgentData || !m.speaker) return false;
    return m.speaker.toLowerCase().includes(selectedAgentData.name.toLowerCase()) ||
           m.speaker.toLowerCase().includes(selectedAgentData.name.split(' ')[0].toLowerCase());
  });

  return (
    <div className={`app${isDark ? '' : ' light'}`}>
      <header className="app-header">
        <div className="header-left">
          {/* Yokogawa logo — inline SVG, no external file needed */}
          <div className="yokogawa-logo">
            <span className="yoko-text">YOKOGAWA</span>
            <svg width="14" height="14" viewBox="0 0 14 14" className="yoko-diamond">
              <polygon points="7,0 14,7 7,14 0,7" fill="#FFD700"/>
            </svg>
          </div>
          <div className="header-divider" />
          <div className="header-title-group">
            <h1>Multi-Agent Chiller Optimization System</h1>
            <span className="header-subtitle">AI-Powered Decision Intelligence</span>
          </div>
        </div>
        <div className="header-status">
          <button className="theme-toggle" onClick={() => setIsDark(d => !d)}>
            {isDark ? '☀ Light' : '☾ Dark'}
          </button>
          <button className="fullscreen-toggle" onClick={() => {
            if (!document.fullscreenElement) {
              document.documentElement.requestFullscreen();
            } else {
              document.exitFullscreen();
            }
          }} title="Toggle fullscreen">
            {isFullscreen ? '⛶ Exit' : '⛶ Full Screen'}
          </button>
          <div className="status-badge">
            <span className="status-dot"></span>
            {systemStatus ? `${systemStatus.agents_online || 6} Agents Online` : '6 Agents Online'}
          </div>
        </div>
      </header>

      <div className="main-container" onClick={() => { setSelectedAgent(null); setShowFindings(false); }}>
        
        {/* Left: Agent Ring + Votes */}
        <div className="agent-ring-section">
          <div className="ring-area">
          <div className="ring-container">
            <svg className="connections" width="100%" height="100%" viewBox="0 0 500 500">
              <defs>
                <filter id="glow">
                  <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
                  <feMerge>
                    <feMergeNode in="coloredBlur"/>
                    <feMergeNode in="SourceGraphic"/>
                  </feMerge>
                </filter>
              </defs>
              
              {/* Animated rotating dotted circle */}
              <circle
                cx="250"
                cy="250"
                r="175"
                fill="none"
                stroke="#00BFA5"
                strokeWidth="2"
                strokeDasharray="8 8"
                opacity="0.4"
                className="rotating-circle"
              />
              
              {/* Connection lines */}
              {agents.map(agent => {
                const pos = getAgentPosition(agent.position);
                const isActive = activeAgent === agent.id;
                return (
                  <line
                    key={agent.id}
                    x1="250" y1="250"
                    x2={pos.svgX} y2={pos.svgY}
                    stroke={isActive ? agent.color : '#2D3748'}
                    strokeWidth={isActive ? 2 : 1}
                    strokeDasharray={isActive ? '0' : '5,5'}
                    opacity={isActive ? 0.8 : 0.3}
                    filter={isActive ? 'url(#glow)' : ''}
                  />
                );
              })}
            </svg>

            <div className="agents">
              {/* Orchestrator */}
              <div
                className={`agent orchestrator ${activeAgent === 'orchestrator' ? 'active' : ''}`}
                style={{ left: '50%', top: '50%' }}
                onMouseEnter={e => setTooltip({ agent: orchestrator, x: e.clientX, y: e.clientY })}
                onMouseMove={e => setTooltip(t => t ? { ...t, x: e.clientX, y: e.clientY } : t)}
                onMouseLeave={() => setTooltip(null)}
              >
                <div className="agent-icon" style={{ background: orchestrator.color }}>
                  {orchestrator.icon}
                </div>
                <div className="agent-name">{orchestrator.name}</div>
                <div className="agent-role">{orchestrator.role}</div>
              </div>

              {/* 6 Agents */}
              {agents.map(agent => {
                const pos = getAgentPosition(agent.position);
                const isActive = activeAgent === agent.id;
                return (
                  <div
                    key={agent.id}
                    className={`agent ${isActive ? 'active' : ''} ${selectedAgent === agent.id ? 'selected' : ''}`}
                    style={{
                      left: pos.left,
                      top: pos.top,
                      borderColor: isActive ? agent.color : '#2D3748'
                    }}
                    onClick={e => { e.stopPropagation(); handleAgentClick(agent.id); }}
                    onMouseEnter={e => setTooltip({ agent, x: e.clientX, y: e.clientY })}
                    onMouseMove={e => setTooltip(t => t ? { ...t, x: e.clientX, y: e.clientY } : t)}
                    onMouseLeave={() => setTooltip(null)}
                  >
                    <div className="agent-icon" style={{ background: agent.color }}>
                      {agent.icon}
                    </div>
                    <div className="agent-name">{agent.shortName}</div>
                    <div className="agent-role">{agent.role}</div>
                    <div className="status-dot"></div>
                  </div>
                );
              })}
            </div>
          </div>
          </div>

          {/* Agent Votes */}
          <div className="agent-votes">
            <h4>Agent Votes</h4>
            {agentVotes.length > 0 ? (
              agentVotes.map((vote, idx) => {
                const agent = agents.find(a => 
                  vote.agent.toLowerCase().includes(a.name.toLowerCase().split(' ')[0])
                );
                
                const agentShortName = agent ? agent.name.split(' ')[0] : vote.agent.split(' ')[0];
                
                return (
                  <div key={idx} className="vote-item">
                    <div className="vote-header">
                      <div 
                        className="vote-icon"
                        style={{ background: agent?.color || '#64748B' }}
                      >
                        {agent?.icon || '•'}
                      </div>
                      <div className="vote-info">
                        <div className="vote-agent-name">{agentShortName}</div>
                        <span className={`vote-badge ${
                          vote.vote && vote.vote.toLowerCase() === 'approve' ? 'approve' :
                          vote.vote && (vote.vote.toLowerCase().includes('approve_with') || vote.vote.toLowerCase().includes('with_conditions') || vote.vote.toLowerCase().includes('with conditions')) ? 'conditional' :
                          vote.vote && vote.vote.toLowerCase().includes('veto') ? 'veto' : 'reject'
                        }`}>
                          {vote.vote}
                        </span>
                      </div>
                    </div>
                    {vote.reasoning && (
                      <div className="vote-reasoning">{vote.reasoning}</div>
                    )}
                  </div>
                );
              })
            ) : (
              <p className="no-votes">Waiting for debate results...</p>
            )}
          </div>
        </div>

        {/* Center: Debate */}
        <div className="debate-section">
          <div className="debate-header">
            <h3>Live Debate</h3>
            {currentRound && <span className="round-badge">Round {currentRound}/4</span>}
          </div>

          <div className="debate-messages" ref={messagesContainerRef}>
            {debateMessages.length === 0 && !isProcessing && (
              <div className="empty-debate">
                <p>💬 No debate active. Ask a question to start.</p>
              </div>
            )}
            {debateMessages.map(msg => {
              // Parse [VOTE: DECISION] prefix out of vote messages
              const voteMatch = msg.content?.match(/^\[VOTE:\s*([^\]]+)\]\s*/);
              const voteDecision = voteMatch ? voteMatch[1].trim() : null;
              const bodyText = voteDecision ? msg.content.replace(voteMatch[0], '').trim() : msg.content;

              // Skip purely redundant "VOTE: APPROVE..." repeat after the badge
              const cleanBody = bodyText?.replace(/^VOTE:\s*\S+\s*/, '').trim();

              // Detect round-header messages (system dividers)
              const isRoundHeader = msg.type === 'round-divider' || msg.type === 'question-divider' ||
                (msg.type === 'system' && (msg.content?.includes('ROUND') || msg.content?.includes('━━━')));

              // Avoid CSS collision: '.agent' is the ring-node class — use 'msg-agent' instead
              const typeClass = msg.type === 'agent' ? 'msg-agent' : msg.type;

              // Attach scroll anchor ref to findings summary message
              const isFindingsAnchor = msg.id === findingsMsgId.current;

              return (
                <div
                  key={msg.id}
                  ref={isFindingsAnchor ? findingsStartRef : null}
                  className={`debate-message ${typeClass}${isRoundHeader ? ' round-divider' : ''}`}
                >
                  {isRoundHeader ? (
                    <div className="round-header-text">{msg.content}</div>
                  ) : (
                    <>
                      <div className="message-header">
                        <div
                          className="speaker-icon"
                          style={{ background: getAgentColor(msg.speaker) }}
                        >
                          {msg.speaker?.charAt(0) || '?'}
                        </div>
                        <div className="speaker-info">
                          <span className="speaker-name">{msg.speaker}</span>
                          {voteDecision && (
                            <span className={`vote-pill ${voteDecision.toLowerCase() === 'approve' ? 'approve' : voteDecision.toLowerCase().includes('approve_with') || voteDecision.toLowerCase().includes('with_conditions') || voteDecision.toLowerCase().includes('with conditions') ? 'conditional' : voteDecision.toLowerCase().includes('veto') ? 'veto' : 'reject'}`}>
                              {voteDecision}
                            </span>
                          )}
                          <span className="message-time">
                            {msg.timestamp.toLocaleTimeString('en-US', { hour12: false })}
                          </span>
                        </div>
                      </div>
                      <div className="message-content">{cleanBody}</div>
                    </>
                  )}
                </div>
              );
            })}
            <div ref={messagesEndRef} />
          </div>

          {!isProcessing && debateMessages.length === 0 && (
            <div className="prompt-suggestions">
              {[
                'Can we power down 1 chiller to save energy?',
                'Should we stage a 3rd chiller online?',
                'Is it safe to reduce chiller load by 15%?',
                'Optimize cooling for peak afternoon demand',
              ].map(suggestion => (
                <button
                  key={suggestion}
                  className="prompt-chip"
                  onClick={() => setInput(suggestion)}
                >
                  {suggestion}
                </button>
              ))}
            </div>
          )}

          <form className="input-form" onSubmit={handleSubmit}>
            <button type="button" className="info-icon-btn" tabIndex={-1}>
              ℹ
              <div className="info-tooltip">
                <span className="info-tooltip-title">Orchestrator Modes</span>
                <div className="info-tooltip-item"><span>💬</span><span><strong>Debate</strong> — agents discuss and vote on your query</span></div>
                <div className="info-tooltip-item"><span>📋</span><span><strong>Knowledge Search</strong> — retrieves relevant summaries from the knowledge base</span></div>
              </div>
            </button>
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Ask orchestrator anything..."
            />
            {isProcessing ? (
              <button type="button" className="stop-btn" onClick={handleStop} title="Stop debate">
                ■
              </button>
            ) : (
              <button type="submit" disabled={!input.trim()}>
                ➤
              </button>
            )}
          </form>
        </div>

        {/* Right: Findings Panel or Agent Details — slides in from right */}
        <div className={`agent-details-section${(selectedAgentData || showFindings) ? ' open' : ''}`} onClick={e => e.stopPropagation()}>
          <button className="details-close" onClick={() => { setSelectedAgent(null); setShowFindings(false); }}>✕</button>

          {/* Findings panel — shown on knowledge retrieval when no agent selected */}
          {showFindings && !selectedAgentData && knowledgeFindings && (
            <div className="findings-panel">
              <div className="findings-panel-header">
                <span className="findings-icon">📋</span>
                <h3>Knowledge Summary</h3>
                <span className="findings-count">{knowledgeFindings.count} source{knowledgeFindings.count !== 1 ? 's' : ''}</span>
              </div>
              {knowledgeFindings.results.length === 0 ? (
                <p className="no-findings">No relevant documents found.</p>
              ) : (
                <>
                  <button
                    className={`summary-toggle-btn${summaryOpen ? ' open' : ''}`}
                    onClick={() => setSummaryOpen(o => !o)}
                  >
                    <span className="summary-toggle-chevron">{summaryOpen ? '▾' : '▸'}</span>
                    Summary
                  </button>
                  {summaryOpen && (
                    <div className="findings-summary-text">
                      {knowledgeFindings.summary || 'No summary available.'}
                    </div>
                  )}
                  <details className="findings-sources-toggle">
                    <summary>View {knowledgeFindings.count} source{knowledgeFindings.count !== 1 ? 's' : ''}</summary>
                    <div className="findings-list">
                      {knowledgeFindings.results.map((r, i) => (
                        <div key={i} className="finding-card">
                          <div className="finding-meta">
                            <span className="finding-collection">{r.collection}</span>
                            <span className="finding-score">{typeof r.score === 'number' ? r.score.toFixed(3) : r.score}</span>
                          </div>
                          <div className="finding-title">{r.title || r.source}{r.page ? ` — p.${r.page}` : ''}</div>
                          <div className="finding-text">{r.text?.slice(0, 300)}{r.text?.length > 300 ? '…' : ''}</div>
                        </div>
                      ))}
                    </div>
                  </details>
                </>
              )}
            </div>
          )}

          {/* Agent details — shown when agent clicked */}
          {selectedAgentData && (
            <>
              <div className="agent-detail-header">
                <div className="agent-detail-icon" style={{ background: selectedAgentData.color }}>
                  {selectedAgentData.icon}
                </div>
                <div>
                  <h3>{selectedAgentData.name.replace(' Agent', '')}</h3>
                  <p>{selectedAgentData.role}</p>
                </div>
              </div>

              <div className="agent-messages-list">
                <h4>Agent Contributions ({selectedAgentMessages.length})</h4>
                {selectedAgentMessages.map(msg => (
                  <div key={msg.id} className="agent-contribution">
                    <div className="contribution-time">
                      {msg.timestamp.toLocaleTimeString('en-US', { hour12: false })}
                      {msg.round && <span className="round-tag">R{msg.round}</span>}
                    </div>
                    <div className="contribution-text">{msg.content}</div>
                  </div>
                ))}
                {selectedAgentMessages.length === 0 && (
                  <p className="no-messages">No contributions yet</p>
                )}
              </div>
            </>
          )}
        </div>

      </div>

      {/* Fixed tooltip — renders outside any clipping context */}
      {tooltip && (
        <div
          className="agent-tooltip-fixed"
          style={{
            left: tooltip.x + 14,
            top: tooltip.y - 10,
          }}
        >
          <div className="tooltip-header" style={{ color: tooltip.agent.color }}>
            {tooltip.agent.icon} {tooltip.agent.name}
          </div>
          <div className="tooltip-desc">{tooltip.agent.description}</div>
          <ul className="tooltip-responsibilities">
            {tooltip.agent.responsibilities.map(r => <li key={r}>{r}</li>)}
          </ul>
        </div>
      )}
    </div>
  );
}

export default App;