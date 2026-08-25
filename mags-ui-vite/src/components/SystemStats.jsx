import React from 'react';
import { motion } from 'framer-motion';
import { TrendingUp, Zap, DollarSign, Activity } from 'lucide-react';

const SystemStats = ({ stats }) => {
  const statCards = [
    {
      label: 'Decisions Today',
      value: stats.decisionsToday,
      icon: Activity,
      color: '#3b82f6',
      trend: '+2 from yesterday'
    },
    {
      label: 'Avg Confidence',
      value: (stats.avgConfidence * 100).toFixed(0) + '%',
      icon: TrendingUp,
      color: '#10b981',
      trend: 'High reliability'
    },
    {
      label: 'Energy Saved',
      value: stats.energySavedToday + ' kW',
      icon: Zap,
      color: '#f59e0b',
      trend: 'SGD ' + (stats.energySavedToday * 0.20 * 24).toFixed(0) + '/day'
    },
    {
      label: 'Current PUE',
      value: stats.currentPUE,
      icon: DollarSign,
      color: stats.currentPUE <= 1.25 ? '#10b981' : '#f59e0b',
      trend: stats.currentPUE <= 1.25 ? 'Excellent' : 'Good'
    }
  ];

  return (
    <div className="system-stats">
      <h3 className="stats-title">System Statistics</h3>
      <div className="stats-grid">
        {statCards.map((stat, index) => {
          const Icon = stat.icon;
          return (
            <motion.div
              key={stat.label}
              className="stat-card"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.1 }}
              whileHover={{ y: -5 }}
            >
              <div className="stat-card-header">
                <div 
                  className="stat-icon"
                  style={{ background: stat.color }}
                >
                  <Icon size={20} />
                </div>
                <span className="stat-label">{stat.label}</span>
              </div>
              <div className="stat-value">{stat.value}</div>
              <div className="stat-trend">{stat.trend}</div>
            </motion.div>
          );
        })}
      </div>
    </div>
  );
};

export default SystemStats;