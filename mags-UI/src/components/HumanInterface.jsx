import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Send, Lightbulb, Loader2 } from 'lucide-react';

const HumanInterface = ({ onSendMessage, disabled }) => {
  const [input, setInput] = useState('');
  const [isNudge, setIsNudge] = useState(false);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (input.trim() && !disabled) {
      onSendMessage(input.trim(), isNudge);
      setInput('');
      setIsNudge(false);
    }
  };

  const quickActions = [
    { label: 'Analyze current state', value: 'What optimization opportunities exist right now?' },
    { label: 'Energy savings', value: 'How can we reduce energy consumption?' },
    { label: 'System health', value: 'Check equipment health and maintenance status' },
    { label: 'PUE improvement', value: 'What actions can improve our PUE?' }
  ];

  const nudgeSuggestions = [
    'Give me a more conservative option',
    'Show me alternatives that prioritize cost',
    'What if we wait longer before acting?',
    'Make this more aggressive'
  ];

  return (
    <div className="human-interface">
      <div className="interface-header">
        <div className="interface-title">
          <Send size={18} />
          <span>Human Interface</span>
        </div>
        <button
          className={`nudge-toggle ${isNudge ? 'active' : ''}`}
          onClick={() => setIsNudge(!isNudge)}
          disabled={disabled}
        >
          <Lightbulb size={16} />
          {isNudge ? 'Nudge Mode' : 'Normal Mode'}
        </button>
      </div>

      {isNudge && (
        <motion.div
          className="nudge-suggestions"
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          exit={{ opacity: 0, height: 0 }}
        >
          <p className="suggestions-label">💡 Nudge Suggestions:</p>
          <div className="suggestions-grid">
            {nudgeSuggestions.map((suggestion, idx) => (
              <button
                key={idx}
                className="suggestion-chip"
                onClick={() => setInput(suggestion)}
                disabled={disabled}
              >
                {suggestion}
              </button>
            ))}
          </div>
        </motion.div>
      )}

      {!isNudge && (
        <div className="quick-actions">
          <p className="actions-label">Quick Actions:</p>
          <div className="actions-grid">
            {quickActions.map((action, idx) => (
              <button
                key={idx}
                className="action-chip"
                onClick={() => setInput(action.value)}
                disabled={disabled}
              >
                {action.label}
              </button>
            ))}
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit} className="input-form">
        <div className="input-container">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={
              isNudge 
                ? "Provide feedback or request alternative solutions..."
                : "Ask a question or give instructions to the orchestrator..."
            }
            disabled={disabled}
            rows={3}
          />
          <motion.button
            type="submit"
            className="send-button"
            disabled={disabled || !input.trim()}
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            {disabled ? (
              <Loader2 size={20} className="spinner" />
            ) : (
              <Send size={20} />
            )}
          </motion.button>
        </div>
      </form>
    </div>
  );
};

export default HumanInterface;