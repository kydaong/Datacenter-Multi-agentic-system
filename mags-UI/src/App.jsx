import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import AgentRing from './components/AgentRing';
import ConversationLog from './components/ConversationLog';
import AgentResponseLog from './components/AgentResponseLog';
import HumanInterface from './components/HumanInterface';
import SystemStats from './components/SystemStats';
import DebateViewer from './components/DebateViewer';
import './App.css';

function App() {
  const [activeAgent, setActiveAgent] = useState(null);
  const [conversationLog, setConversationLog] = useState([]);
  const [selectedAgentLog, setSelectedAgentLog] = useState(null);
  const [systemStats, setSystemStats] = useState({
    decisionsToday: 0,
    avgConfidence: 0,
    currentPUE: 1.24,
    energySavedToday: 0,
    agentsOnline: 6
  });
  const [debateSession, setDebateSession] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [currentView, setCurrentView] = useState('conversation'); // 'conversation' | 'debate'

  // Agent definitions
  const agents = [
    {
      id: 'orchestrator',
      name: 'Orchestrator',
      role: 'System Coordinator',
      icon: '🎯',
      color: '#10b981',
      position: 'center'
    },
    {
      id: 'demand',
      name: 'Demand & Conditions',
      role: 'Forecasting',
      icon: '📊',
      color: '#3b82f6',
      position: 0
    },
    {
      id: 'chiller',
      name: 'Chiller Optimization',
      role: 'Staging & Efficiency',
      icon: '❄️',
      color: '#8b5cf6',
      position: 1
    },
    {
      id: 'building',
      name: 'Building Systems',
      role: 'Pumps & Towers',
      icon: '🏢',
      color: '#06b6d4',
      position: 2
    },
    {
      id: 'energy',
      name: 'Energy & Cost',
      role: 'Cost Optimization',
      icon: '💰',
      color: '#f59e0b',
      position: 3
    },
    {
      id: 'maintenance',
      name: 'Maintenance & Compliance',
      role: 'Equipment Health',
      icon: '🔧',
      color: '#ec4899',
      position: 4
    },
    {
      id: 'safety',
      name: 'Operations & Safety',
      role: 'Safety & SOPs',
      icon: '🛡️',
      color: '#ef4444',
      position: 5
    }
  ];

  // Simulate agent activity
  useEffect(() => {
    // Initial welcome message
    addMessage('system', 'MAGS System initialized. All 6 agents online and ready.', 'Orchestrator');
  }, []);

  const addMessage = (type, content, speaker, metadata = {}) => {
    const message = {
      id: Date.now() + Math.random(),
      type,
      content,
      speaker,
      timestamp: new Date(),
      ...metadata
    };
    setConversationLog(prev => [...prev, message]);
  };

  const handleHumanMessage = async (message, isNudge = false) => {
    // Add human message
    addMessage('human', message, 'Human');
    
    setIsProcessing(true);
    
    // Simulate orchestrator processing
    setActiveAgent('orchestrator');
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    addMessage('agent', 
      isNudge 
        ? `Understood. Let me re-analyze with your feedback: "${message}". Initiating debate with updated constraints...`
        : `Received your query. Analyzing system state and coordinating with all agents. Starting 4-round debate...`,
      'Orchestrator'
    );
    
    // Simulate debate rounds
    await simulateDebate();
    
    setActiveAgent(null);
    setIsProcessing(false);
  };

  const simulateDebate = async () => {
    const debateData = {
      sessionId: `SESSION_${Date.now()}`,
      rounds: []
    };

    // Round 1: Initial Proposals
    setActiveAgent('orchestrator');
    addMessage('system', '🗣️ ROUND 1: Initial Proposals', 'System');
    await new Promise(resolve => setTimeout(resolve, 500));

    const agentProposals = [
      { agentId: 'demand', proposal: 'IT load forecast shows stable conditions. No economizer opportunities detected.' },
      { agentId: 'chiller', proposal: 'Current staging optimal at 2 chillers. Load distribution balanced at 68% each.' },
      { agentId: 'building', proposal: 'Pump VFD speeds can be reduced by 5% based on current differential pressure.' },
      { agentId: 'energy', proposal: 'Current PUE at 1.24. Pump optimization could save 12 kW.' },
      { agentId: 'maintenance', proposal: 'All equipment health nominal. No maintenance conflicts.' },
      { agentId: 'safety', proposal: 'N+1 redundancy maintained. No safety concerns.' }
    ];

    for (const { agentId, proposal } of agentProposals) {
      setActiveAgent(agentId);
      await new Promise(resolve => setTimeout(resolve, 800));
      const agent = agents.find(a => a.id === agentId);
      addMessage('agent', proposal, agent.name);
    }

    debateData.rounds.push({ round: 1, proposals: agentProposals });

    // Round 2: Responses
    await new Promise(resolve => setTimeout(resolve, 1000));
    addMessage('system', '💬 ROUND 2: Agent Responses', 'System');
    await new Promise(resolve => setTimeout(resolve, 500));

    setActiveAgent('energy');
    await new Promise(resolve => setTimeout(resolve, 1000));
    addMessage('agent', 
      "I've reviewed Building Systems Agent's proposal for pump VFD reduction. From a cost perspective, a 12 kW savings translates to SGD 2,400/month. I strongly support this optimization. The payback is immediate with no capital investment required.",
      'Energy & Cost'
    );

    setActiveAgent('safety');
    await new Promise(resolve => setTimeout(resolve, 1000));
    addMessage('agent',
      "Agreed with the pump optimization. I've verified that reducing pump speed by 5% maintains differential pressure above our 1.5 bar minimum. No safety issues. This is a good operational efficiency gain.",
      'Operations & Safety'
    );

    debateData.rounds.push({ round: 2, responses: 2 });

    // Round 3: Consensus
    await new Promise(resolve => setTimeout(resolve, 1000));
    addMessage('system', '🤝 ROUND 3: Building Consensus', 'System');
    await new Promise(resolve => setTimeout(resolve, 500));

    setActiveAgent('building');
    await new Promise(resolve => setTimeout(resolve, 1000));
    addMessage('agent',
      "Based on the positive feedback, I'm refining my recommendation: reduce PCHWP speeds to 70% and SCHWP to 65%. This maintains our target delta-T while capturing the 12 kW savings. I'll coordinate with the BMS for gradual implementation.",
      'Building Systems'
    );

    debateData.rounds.push({ round: 3, refined: 1 });

    // Round 4: Final Vote
    await new Promise(resolve => setTimeout(resolve, 1000));
    addMessage('system', '🗳️ ROUND 4: Final Vote', 'System');
    await new Promise(resolve => setTimeout(resolve, 500));

    const votes = [
      { agentId: 'demand', vote: 'APPROVE' },
      { agentId: 'chiller', vote: 'APPROVE' },
      { agentId: 'building', vote: 'APPROVE' },
      { agentId: 'energy', vote: 'APPROVE' },
      { agentId: 'maintenance', vote: 'APPROVE' },
      { agentId: 'safety', vote: 'APPROVE' }
    ];

    for (const { agentId, vote } of votes) {
      setActiveAgent(agentId);
      await new Promise(resolve => setTimeout(resolve, 600));
      const agent = agents.find(a => a.id === agentId);
      addMessage('vote', `${vote}`, agent.name);
    }

    debateData.rounds.push({ round: 4, votes });

    // Final decision
    await new Promise(resolve => setTimeout(resolve, 1000));
    setActiveAgent('orchestrator');
    addMessage('decision',
      '✅ CONSENSUS REACHED: Pump VFD Optimization approved (6/6 agents). Predicted savings: 12 kW, SGD 2,400/month. Confidence: 0.92. Ready for human approval.',
      'Orchestrator'
    );

    setDebateSession(debateData);

    // Update stats
    setSystemStats(prev => ({
      ...prev,
      decisionsToday: prev.decisionsToday + 1,
      avgConfidence: 0.92,
      energySavedToday: prev.energySavedToday + 12
    }));
  };

  const handleAgentClick = (agentId) => {
    setSelectedAgentLog(agentId);
  };

  return (
    <div className="app">
      {/* Header */}
      <header className="app-header">
        <div className="header-content">
          <div className="logo">
            <div className="logo-icon">⚡</div>
            <div>
              <h1>MAGS</h1>
              <p>Multi-Agent Chiller Optimization System</p>
            </div>
          </div>
          <div className="header-stats">
            <div className="stat">
              <span className="stat-value">{systemStats.agentsOnline}</span>
              <span className="stat-label">Agents Online</span>
            </div>
            <div className="stat">
              <span className="stat-value">{systemStats.currentPUE}</span>
              <span className="stat-label">Current PUE</span>
            </div>
            <div className="stat">
              <span className="stat-value">{systemStats.decisionsToday}</span>
              <span className="stat-label">Decisions Today</span>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="main-content">
        {/* Left Panel - Agent Ring */}
        <div className="left-panel">
          <AgentRing 
            agents={agents}
            activeAgent={activeAgent}
            onAgentClick={handleAgentClick}
            isProcessing={isProcessing}
          />
          
          <SystemStats stats={systemStats} />
        </div>

        {/* Center Panel - Conversation */}
        <div className="center-panel">
          <div className="view-tabs">
            <button 
              className={`tab ${currentView === 'conversation' ? 'active' : ''}`}
              onClick={() => setCurrentView('conversation')}
            >
              💬 Conversation
            </button>
            <button 
              className={`tab ${currentView === 'debate' ? 'active' : ''}`}
              onClick={() => setCurrentView('debate')}
              disabled={!debateSession}
            >
              🗣️ Debate Viewer
            </button>
          </div>

          {currentView === 'conversation' ? (
            <ConversationLog 
              messages={conversationLog}
              agents={agents}
            />
          ) : (
            <DebateViewer 
              session={debateSession}
              agents={agents}
            />
          )}

          <HumanInterface 
            onSendMessage={handleHumanMessage}
            disabled={isProcessing}
          />
        </div>

        {/* Right Panel - Agent Details */}
        <div className="right-panel">
          <AgentResponseLog 
            agentId={selectedAgentLog}
            messages={conversationLog}
            agents={agents}
          />
        </div>
      </div>
    </div>
  );
}

export default App;