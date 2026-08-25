import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronRight, MessageCircle, Users, CheckCircle } from 'lucide-react';

const DebateViewer = ({ session, agents }) => {
  const [selectedRound, setSelectedRound] = useState(1);

  if (!session) {
    return (
      <div className="debate-viewer">
        <div className="empty-state">
          <MessageCircle size={48} opacity={0.3} />
          <p>No debate session available yet</p>
          <p className="empty-state-hint">Start a conversation to see agent debates</p>
        </div>
      </div>
    );
  }

  const rounds = [
    { 
      number: 1, 
      title: 'Initial Proposals', 
      icon: MessageCircle,
      description: 'Each agent presents their analysis and recommendations'
    },
    { 
      number: 2, 
      title: 'Agent Responses', 
      icon: Users,
      description: 'Agents discuss and respond to each other\'s proposals'
    },
    { 
      number: 3, 
      title: 'Consensus Building', 
      icon: ChevronRight,
      description: 'Agents refine positions based on peer feedback'
    },
    { 
      number: 4, 
      title: 'Final Vote', 
      icon: CheckCircle,
      description: 'Each agent casts their final vote on the primary proposal'
    }
  ];

  const getRoundData = (roundNum) => {
    const round = session.rounds.find(r => r.round === roundNum);
    return round || null;
  };

  return (
    <div className="debate-viewer">
      <div className="debate-header">
        <h3>Debate Session: {session.sessionId}</h3>
        <span className="session-badge">4 Rounds Complete</span>
      </div>

      <div className="rounds-navigation">
        {rounds.map((round) => {
          const Icon = round.icon;
          return (
            <motion.button
              key={round.number}
              className={`round-button ${selectedRound === round.number ? 'active' : ''}`}
              onClick={() => setSelectedRound(round.number)}
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
            >
              <div className="round-number">
                <Icon size={16} />
                <span>Round {round.number}</span>
              </div>
              <div className="round-title">{round.title}</div>
            </motion.button>
          );
        })}
      </div>

      <AnimatePresence mode="wait">
        <motion.div
          key={selectedRound}
          className="round-content"
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, x: -20 }}
          transition={{ duration: 0.3 }}
        >
          <div className="round-header">
            <h4>{rounds[selectedRound - 1].title}</h4>
            <p className="round-description">{rounds[selectedRound - 1].description}</p>
          </div>

          <div className="round-details">
            {selectedRound === 1 && (
              <div className="proposals-list">
                {getRoundData(1)?.proposals.map((proposal, idx) => {
                  const agent = agents.find(a => a.id === proposal.agentId);
                  return (
                    <motion.div
                      key={idx}
                      className="proposal-card"
                      initial={{ opacity: 0, y: 20 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: idx * 0.1 }}
                    >
                      <div className="proposal-header">
                        <div 
                          className="proposal-icon"
                          style={{ background: agent?.color }}
                        >
                          {agent?.icon}
                        </div>
                        <span className="proposal-agent">{agent?.name}</span>
                      </div>
                      <p className="proposal-text">{proposal.proposal}</p>
                    </motion.div>
                  );
                })}
              </div>
            )}

            {selectedRound === 2 && (
              <div className="responses-list">
                <div className="info-card">
                  <MessageCircle size={20} />
                  <span>{getRoundData(2)?.responses || 0} conversational exchanges</span>
                </div>
                <p className="round-note">
                  Agents engaged in natural dialogue, discussing proposals and raising concerns
                  from their domain perspectives.
                </p>
              </div>
            )}

            {selectedRound === 3 && (
              <div className="consensus-view">
                <div className="info-card">
                  <Users size={20} />
                  <span>{getRoundData(3)?.refined || 0} position refinements</span>
                </div>
                <p className="round-note">
                  Agents refined their positions based on peer feedback, working toward consensus
                  on the optimal approach.
                </p>
              </div>
            )}

            {selectedRound === 4 && (
              <div className="votes-list">
                {getRoundData(4)?.votes.map((vote, idx) => {
                  const agent = agents.find(a => a.id === vote.agentId);
                  return (
                    <motion.div
                      key={idx}
                      className="vote-card"
                      initial={{ opacity: 0, scale: 0.9 }}
                      animate={{ opacity: 1, scale: 1 }}
                      transition={{ delay: idx * 0.1 }}
                    >
                      <div 
                        className="vote-icon"
                        style={{ background: agent?.color }}
                      >
                        <CheckCircle size={16} />
                      </div>
                      <div className="vote-details">
                        <span className="vote-agent">{agent?.name}</span>
                        <span 
                          className={`vote-value ${vote.vote === 'APPROVE' ? 'approve' : ''}`}
                        >
                          {vote.vote}
                        </span>
                      </div>
                    </motion.div>
                  );
                })}
              </div>
            )}
          </div>
        </motion.div>
      </AnimatePresence>
    </div>
  );
};

export default DebateViewer;