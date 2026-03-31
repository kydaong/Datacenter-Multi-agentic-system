import React from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { format } from 'date-fns';
import { Activity, MessageSquare } from 'lucide-react';

const AgentResponseLog = ({ agentId, messages, agents }) => {
  const selectedAgent = agents.find(a => a.id === agentId);
  
  const agentMessages = messages.filter(
    msg => selectedAgent && msg.speaker === selectedAgent.name
  );

  if (!selectedAgent) {
    return (
      <div className="agent-response-log">
        <div className="empty-state">
          <Activity size={48} opacity={0.3} />
          <p>Select an agent to view their responses</p>
        </div>
      </div>
    );
  }

  return (
    <div className="agent-response-log">
      <div className="agent-log-header">
        <div 
          className="agent-log-icon"
          style={{ background: selectedAgent.color }}
        >
          {selectedAgent.icon}
        </div>
        <div>
          <h3>{selectedAgent.name}</h3>
          <p>{selectedAgent.role}</p>
        </div>
      </div>

      <div className="agent-stats">
        <div className="stat-item">
          <span className="stat-label">Total Responses</span>
          <span className="stat-value">{agentMessages.length}</span>
        </div>
        <div className="stat-item">
          <span className="stat-label">Status</span>
          <span className="stat-value status-online">Online</span>
        </div>
      </div>

      <div className="agent-messages">
        <AnimatePresence>
          {agentMessages.length === 0 ? (
            <div className="empty-state-small">
              <MessageSquare size={24} opacity={0.3} />
              <p>No responses yet from this agent</p>
            </div>
          ) : (
            agentMessages.map((message) => (
              <motion.div
                key={message.id}
                className="agent-message-item"
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: 20 }}
              >
                <div className="agent-message-time">
                  {format(new Date(message.timestamp), 'HH:mm:ss')}
                </div>
                <div className="agent-message-content">
                  {message.type === 'vote' && (
                    <span 
                      className="vote-badge-small"
                      style={{ background: selectedAgent.color }}
                    >
                      {message.content}
                    </span>
                  )}
                  {message.type !== 'vote' && message.content}
                </div>
              </motion.div>
            ))
          )}
        </AnimatePresence>
      </div>
    </div>
  );
};

export default AgentResponseLog;