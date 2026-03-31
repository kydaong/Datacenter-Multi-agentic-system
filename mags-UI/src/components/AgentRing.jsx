import React from 'react';
import { motion } from 'framer-motion';

const AgentRing = ({ agents, activeAgent, onAgentClick, isProcessing }) => {
  const orchestrator = agents.find(a => a.position === 'center');
  const surroundingAgents = agents.filter(a => a.position !== 'center');

  const getAgentPosition = (index, total) => {
    const angle = (index / total) * 2 * Math.PI - Math.PI / 2;
    const radius = 180;
    return {
      x: Math.cos(angle) * radius,
      y: Math.sin(angle) * radius
    };
  };

  return (
    <div className="agent-ring-container">
      <svg className="connection-lines" width="500" height="500">
        <defs>
          <filter id="glow">
            <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
            <feMerge>
              <feMergeNode in="coloredBlur"/>
              <feMergeNode in="SourceGraphic"/>
            </feMerge>
          </filter>
        </defs>
        
        {/* Connection lines */}
        {surroundingAgents.map((agent, idx) => {
          const pos = getAgentPosition(idx, surroundingAgents.length);
          const isActive = activeAgent === agent.id;
          
          return (
            <motion.line
              key={agent.id}
              x1="250"
              y1="250"
              x2={250 + pos.x}
              y2={250 + pos.y}
              stroke={isActive ? agent.color : '#2d3748'}
              strokeWidth={isActive ? 2 : 1}
              strokeDasharray={isActive ? "0" : "5,5"}
              opacity={isActive ? 0.8 : 0.3}
              filter={isActive ? "url(#glow)" : ""}
              animate={{
                opacity: isActive ? [0.3, 0.8, 0.3] : 0.3,
              }}
              transition={{
                duration: 2,
                repeat: isActive ? Infinity : 0
              }}
            />
          );
        })}

        {/* Circular ring */}
        <circle
          cx="250"
          cy="250"
          r="180"
          fill="none"
          stroke="#2d3748"
          strokeWidth="1"
          strokeDasharray="5,5"
          opacity="0.3"
        />
      </svg>

      <div className="agents-container">
        {/* Orchestrator - Center */}
        <motion.div
          className={`agent-node orchestrator ${activeAgent === 'orchestrator' ? 'active' : ''} ${isProcessing ? 'processing' : ''}`}
          style={{
            position: 'absolute',
            left: '50%',
            top: '50%',
            transform: 'translate(-50%, -50%)'
          }}
          whileHover={{ scale: 1.1 }}
          onClick={() => onAgentClick('orchestrator')}
        >
          <div className="agent-icon" style={{ background: orchestrator.color }}>
            {orchestrator.icon}
          </div>
          <div className="agent-info">
            <div className="agent-name">{orchestrator.name}</div>
            <div className="agent-role">{orchestrator.role}</div>
          </div>
          {activeAgent === 'orchestrator' && (
            <motion.div
              className="pulse-ring"
              initial={{ scale: 1, opacity: 0.5 }}
              animate={{ scale: 2, opacity: 0 }}
              transition={{ duration: 1.5, repeat: Infinity }}
            />
          )}
        </motion.div>

        {/* Surrounding Agents */}
        {surroundingAgents.map((agent, idx) => {
          const pos = getAgentPosition(idx, surroundingAgents.length);
          const isActive = activeAgent === agent.id;

          return (
            <motion.div
              key={agent.id}
              className={`agent-node ${isActive ? 'active' : ''}`}
              style={{
                position: 'absolute',
                left: `calc(50% + ${pos.x}px)`,
                top: `calc(50% + ${pos.y}px)`,
                transform: 'translate(-50%, -50%)'
              }}
              whileHover={{ scale: 1.1 }}
              onClick={() => onAgentClick(agent.id)}
              animate={{
                borderColor: isActive ? agent.color : '#2d3748'
              }}
            >
              <div className="agent-icon" style={{ background: agent.color }}>
                {agent.icon}
              </div>
              <div className="agent-info">
                <div className="agent-name">{agent.name}</div>
                <div className="agent-role">{agent.role}</div>
              </div>
              {isActive && (
                <motion.div
                  className="pulse-ring"
                  style={{ borderColor: agent.color }}
                  initial={{ scale: 1, opacity: 0.5 }}
                  animate={{ scale: 2, opacity: 0 }}
                  transition={{ duration: 1.5, repeat: Infinity }}
                />
              )}
              <div 
                className="status-indicator"
                style={{ background: isActive ? agent.color : '#4ade80' }}
              />
            </motion.div>
          );
        })}
      </div>
    </div>
  );
};

export default AgentRing;