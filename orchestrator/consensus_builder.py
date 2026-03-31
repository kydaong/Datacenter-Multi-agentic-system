"""
Consensus Builder
Aggregates votes, handles vetoes, and determines final decision
"""

from typing import Dict, List, Optional
from datetime import datetime
import numpy as np


class ConsensusBuilder:
    """
    Builds consensus from agent debate results
    
    Responsibilities:
    - Aggregate votes from all agents
    - Handle veto power (Operations & Safety, Maintenance, Energy & Cost)
    - Calculate weighted confidence
    - Determine consensus type (STRONG, MODERATE, WEAK, CONFLICT)
    - Select final decision
    """
    
    def __init__(self, agents: List):
        """
        Initialize consensus builder
        
        Args:
            agents: List of all agent instances
        """
        self.agents = agents
        
        # Agent weights for decision making
        self.agent_weights = {
            'Demand & Conditions Agent': 0.15,
            'Chiller Optimization Agent': 0.20,
            'Building System Agent': 0.15,
            'Energy & Cost Optimization Agent': 0.15,
            'Maintenance & Compliance Agent': 0.15,
            'Operations & Safety Agent': 0.20  # Highest weight + veto power
        }
        
        # Agents with veto power
        self.veto_agents = [
            'Operations & Safety Agent',
            'Maintenance & Compliance Agent',
            'Energy & Cost Optimization Agent'
        ]
    
    def build_consensus(self, debate_result: Dict) -> Dict:
        """
        Build consensus from debate results
        
        Args:
            debate_result: Complete debate session
        
        Returns:
            Consensus decision with metadata
        """
        
        print("\n  Analyzing votes...")
        
        # Get final vote (Round 4)
        round_4 = debate_result['rounds'][3]
        votes = round_4['votes']
        primary_proposal = round_4['primary_proposal']
        
        # Check for vetoes
        vetoes = self._check_vetoes(votes)
        
        if vetoes:
            print(f"  ⚠️  VETO detected from: {', '.join([v['agent'] for v in vetoes])}")
            return self._handle_veto(vetoes, primary_proposal, debate_result)
        
        # Calculate vote distribution
        vote_distribution = self._calculate_vote_distribution(votes)
        
        print(f"  Vote distribution: {vote_distribution}")
        
        # Calculate weighted confidence
        weighted_confidence = self._calculate_weighted_confidence(votes)
        
        print(f"  Weighted confidence: {weighted_confidence:.2f}")
        
        # Determine consensus type
        consensus_type = self._determine_consensus_type(vote_distribution, weighted_confidence)
        
        print(f"  Consensus type: {consensus_type}")
        
        # Select final decision
        final_decision = self._select_final_decision(
            primary_proposal,
            votes,
            consensus_type,
            debate_result
        )
        
        # Calculate support percentage
        approve_votes = vote_distribution.get('APPROVE', 0) + vote_distribution.get('APPROVE_WITH_CONDITIONS', 0)
        total_votes = len(votes)
        support_percentage = (approve_votes / total_votes * 100) if total_votes > 0 else 0
        
        # Collect concerns and conditions
        concerns = self._collect_concerns(votes)
        conditions = self._collect_conditions(votes)
        
        consensus_result = {
            'decision': final_decision,
            'consensus_type': consensus_type,
            'confidence': weighted_confidence,
            'votes': votes,
            'vote_distribution': vote_distribution,
            'support_percentage': support_percentage,
            'vetoes': vetoes,
            'concerns': concerns,
            'conditions': conditions,
            'timestamp': datetime.now().isoformat()
        }
        
        return consensus_result
    
    def _check_vetoes(self, votes: List[Dict]) -> List[Dict]:
        """Check if any agent with veto power has vetoed"""
        
        vetoes = []
        
        for vote in votes:
            agent_name = vote['agent']
            vote_value = vote['vote']
            
            # Check if agent has veto power and voted VETO
            if agent_name in self.veto_agents and vote_value == 'VETO':
                vetoes.append({
                    'agent': agent_name,
                    'reasoning': vote.get('reasoning', 'No reason provided'),
                    'veto_type': self._get_veto_type(agent_name)
                })
        
        return vetoes
    
    def _get_veto_type(self, agent_name: str) -> str:
        """Get type of veto based on agent"""
        
        if agent_name == 'Operations & Safety Agent':
            return 'SAFETY_VETO'
        elif agent_name == 'Maintenance & Compliance Agent':
            return 'COMPLIANCE_VETO'
        elif agent_name == 'Energy & Cost Optimization Agent':
            return 'COST_VETO'
        else:
            return 'UNKNOWN_VETO'
    
    def _handle_veto(
        self,
        vetoes: List[Dict],
        primary_proposal: Dict,
        debate_result: Dict
    ) -> Dict:
        """Handle veto situation"""
        
        print("\n  ⚠️  VETO SITUATION - Proposal blocked")
        
        # Veto overrides everything
        # Return NO-ACTION decision with veto explanation
        
        veto_decision = {
            'action_type': 'VETO_BLOCKED',
            'description': 'Proposal vetoed by agent(s) with veto authority',
            'original_proposal': primary_proposal,
            'veto_details': vetoes,
            'status': 'BLOCKED',
            'requires_human_review': True
        }
        
        return {
            'decision': veto_decision,
            'consensus_type': 'VETO',
            'confidence': 0.99,  # High confidence in veto
            'votes': [],
            'vote_distribution': {'VETO': len(vetoes)},
            'support_percentage': 0,
            'vetoes': vetoes,
            'concerns': [v['reasoning'] for v in vetoes],
            'conditions': [],
            'timestamp': datetime.now().isoformat()
        }
    
    def _calculate_vote_distribution(self, votes: List[Dict]) -> Dict[str, int]:
        """Calculate distribution of votes"""
        
        distribution = {}
        
        for vote in votes:
            vote_value = vote['vote']
            distribution[vote_value] = distribution.get(vote_value, 0) + 1
        
        return distribution
    
    def _calculate_weighted_confidence(self, votes: List[Dict]) -> float:
        """Calculate weighted confidence based on agent weights and individual confidences"""
        
        total_weight = 0
        weighted_sum = 0
        
        for vote in votes:
            agent_name = vote['agent']
            confidence = vote.get('confidence', 0.80)
            weight = self.agent_weights.get(agent_name, 0.10)
            
            # Only include APPROVE and APPROVE_WITH_CONDITIONS in confidence
            if vote['vote'] in ['APPROVE', 'APPROVE_WITH_CONDITIONS']:
                weighted_sum += confidence * weight
                total_weight += weight
        
        if total_weight == 0:
            return 0.0
        
        return weighted_sum / total_weight
    
    def _determine_consensus_type(
        self,
        vote_distribution: Dict[str, int],
        weighted_confidence: float
    ) -> str:
        """
        Determine consensus type
        
        Returns:
            STRONG: >80% approval, high confidence
            MODERATE: 60-80% approval
            WEAK: 50-60% approval
            CONFLICT: <50% approval or low confidence
        """
        
        total_votes = sum(vote_distribution.values())
        approve_votes = vote_distribution.get('APPROVE', 0) + vote_distribution.get('APPROVE_WITH_CONDITIONS', 0)
        
        approval_rate = (approve_votes / total_votes) if total_votes > 0 else 0
        
        if approval_rate > 0.80 and weighted_confidence > 0.75:
            return 'STRONG'
        elif approval_rate > 0.60:
            return 'MODERATE'
        elif approval_rate > 0.50:
            return 'WEAK'
        else:
            return 'CONFLICT'
    
    def _select_final_decision(
        self,
        primary_proposal: Dict,
        votes: List[Dict],
        consensus_type: str,
        debate_result: Dict
    ) -> Dict:
        """Select final decision based on consensus"""
        
        # If strong/moderate consensus, use primary proposal
        if consensus_type in ['STRONG', 'MODERATE']:
            decision = primary_proposal.copy()
            decision['consensus_type'] = consensus_type
            decision['approved_by_consensus'] = True
            return decision
        
        # If weak consensus or conflict, default to monitoring
        else:
            return {
                'action_type': 'MONITORING_UPDATE',
                'description': 'No consensus reached. Continue monitoring.',
                'status': 'NO_CONSENSUS',
                'original_proposal': primary_proposal,
                'consensus_type': consensus_type,
                'requires_human_review': True
            }
    
    def _collect_concerns(self, votes: List[Dict]) -> List[str]:
        """Collect concerns raised by agents"""
        
        concerns = []
        
        for vote in votes:
            reasoning = vote.get('reasoning', '')
            
            # Look for concern indicators
            if any(word in reasoning.lower() for word in ['concern', 'risk', 'issue', 'warning']):
                concerns.append(f"{vote['agent']}: {reasoning}")
        
        return concerns
    
    def _collect_conditions(self, votes: List[Dict]) -> List[str]:
        """Collect conditions from conditional approvals"""
        
        conditions = []
        
        for vote in votes:
            if vote['vote'] == 'APPROVE_WITH_CONDITIONS':
                condition = vote.get('reasoning', '')
                conditions.append(f"{vote['agent']}: {condition}")
        
        return conditions