import React, { useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { format } from 'date-fns';
import { MessageCircle, User, Cpu, CheckCircle, AlertTriangle } from 'lucide-react';

const ConversationLog = ({ messages, agents }) => {
  const logEndRef = useRef(null);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const getAgentColor = (speakerName) => {
    const agent = agents.find(a => a.name === speakerName);
    return agent ? agent.color : '#6b7280';
  };

  const getMessageIcon = (type, speaker) => {
    switch (type) {
      case 'human':
        return <User size={16} />;
      case 'agent':
        return <MessageCircle size={16} />;
      case 'system':
        return <Cpu size={16} />;
      case 'vote':
        return <CheckCircle size={16} />;
      case 'decision':
        return <AlertTriangle size={16} />;
      default:
        return <MessageCircle size={16} />;
    }
  };

  return (
    <div className="conversation-log">
      <div className="conversation-header">
        <MessageCircle size={20} />
        <h3>System Conversation</h3>
        <span className="message-count">{messages.length} messages</span>
      </div>

      <div className="messages-container">
        <AnimatePresence>
          {messages.map((message, index) => (
            <motion.div
              key={message.id}
              className={`message message-${message.type}`}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.3, delay: index * 0.05 }}
            >
              <div className="message-header">
                <div 
                  className="message-icon"
                  style={{ 
                    background: message.type === 'human' ? '#3b82f6' : getAgentColor(message.speaker)
                  }}
                >
                  {getMessageIcon(message.type, message.speaker)}
                </div>
                <div className="message-meta">
                  <span className="message-speaker">{message.speaker}</span>
                  <span className="message-time">
                    {format(new Date(message.timestamp), 'HH:mm:ss')}
                  </span>
                </div>
              </div>
              <div className="message-content">
                {message.type === 'vote' && (
                  <span className="vote-badge">{message.content}</span>
                )}
                {message.type !== 'vote' && message.content}
              </div>
            </motion.div>
          ))}
        </AnimatePresence>
        <div ref={logEndRef} />
      </div>
    </div>
  );
};

export default ConversationLog;