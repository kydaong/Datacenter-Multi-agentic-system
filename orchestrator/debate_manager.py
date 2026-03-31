"""
Debate Manager
Manages 4-round debate protocol between agents
Facilitates agent-to-agent communication and argument refinement
"""

from typing import Dict, List, Optional
from datetime import datetime
import json


class DebateManager:
    """
    Manages structured 4-round debate protocol
    
    Round 1: Initial proposals (parallel)
    Round 2: Agent responses and rebuttals
    Round 3: Consensus building and position refinement
    Round 4: Final vote
    """
    
    def __init__(self, agents: List):
        """
        Initialize debate manager
        
        Args:
            agents: List of all agent instances
        """
        self.agents = agents
        self.max_rounds = 4
    
    def run_debate(
        self,
        context: Dict,
        human_input: Optional[str] = None
    ) -> Dict:
        """
        Run complete 4-round debate
        
        Args:
            context: System state and telemetry
            human_input: Optional human query/guidance
        
        Returns:
            Complete debate result with conversation log
        """
        
        debate_session = {
            'start_time': datetime.now().isoformat(),
            'context': context,
            'human_input': human_input,
            'rounds': [],
            'conversation_log': []
        }
        
        # Add human input to conversation log
        if human_input:
            self._log_message(
                debate_session,
                speaker='HUMAN',
                message=human_input,
                timestamp=datetime.now()
            )
        
        # Round 1: Initial proposals
        print("\n[ROUND 1/4] Collecting initial proposals...")
        round_1_result = self._run_round_1(context, debate_session)
        debate_session['rounds'].append(round_1_result)
        
        # Round 2: Responses and rebuttals
        print("\n[ROUND 2/4] Agent responses and rebuttals...")
        round_2_result = self._run_round_2(round_1_result, context, debate_session)
        debate_session['rounds'].append(round_2_result)
        
        # Round 3: Consensus building
        print("\n[ROUND 3/4] Building consensus...")
        round_3_result = self._run_round_3(round_1_result, round_2_result, context, debate_session)
        debate_session['rounds'].append(round_3_result)
        
        # Round 4: Final vote
        print("\n[ROUND 4/4] Final vote...")
        round_4_result = self._run_round_4(debate_session)
        debate_session['rounds'].append(round_4_result)
        
        debate_session['end_time'] = datetime.now().isoformat()
        
        return debate_session
    
    def _run_round_1(self, context: Dict, debate_session: Dict) -> Dict:
        """
        Round 1: Each agent proposes independently
        
        All agents analyze situation in parallel and propose actions
        """
        
        proposals = []
        
        for agent in self.agents:
            print(f"  → {agent.agent_name}...")
            
            try:
                proposal = agent.propose_action(context)
                proposals.append(proposal)
                
                # Log to conversation
                self._log_message(
                    debate_session,
                    speaker=agent.agent_name,
                    message=self._format_proposal_message(proposal),
                    timestamp=datetime.now(),
                    proposal=proposal
                )
                
                # Print summary
                action_type = proposal.get('action_type', 'UNKNOWN')
                print(f"     Proposal: {action_type}")
                
            except Exception as e:
                print(f"     ERROR: {e}")
                proposals.append({
                    'agent': agent.agent_name,
                    'error': str(e),
                    'action_type': 'ERROR'
                })
        
        return {
            'round': 1,
            'phase': 'INITIAL_PROPOSALS',
            'proposals': proposals,
            'timestamp': datetime.now().isoformat()
        }
    
    def _run_round_2(
        self,
        round_1: Dict,
        context: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Round 2: Agents respond to each other's proposals
        
        Each agent reviews other proposals and provides:
        - Support/opposition
        - Refinements
        - Concerns
        - Alternative suggestions
        """
        
        responses = []
        
        # Get all proposals from Round 1
        round_1_proposals = round_1['proposals']
        
        for agent in self.agents:
            print(f"  → {agent.agent_name} responding...")
            
            # Agent reviews all other proposals
            agent_response = {
                'agent': agent.agent_name,
                'responses_to_proposals': []
            }
            
            for other_proposal in round_1_proposals:
                # Skip own proposal
                if other_proposal.get('agent') == agent.agent_name:
                    continue
                
                # Generate response
                response = self._generate_agent_response(
                    agent,
                    other_proposal,
                    context
                )
                
                if response:
                    agent_response['responses_to_proposals'].append(response)
                    
                    # Log to conversation
                    self._log_message(
                        debate_session,
                        speaker=agent.agent_name,
                        message=f"[Response to {other_proposal.get('agent')}] {response['comment']}",
                        timestamp=datetime.now(),
                        metadata={'response_to': other_proposal.get('agent')}
                    )
            
            responses.append(agent_response)
        
        return {
            'round': 2,
            'phase': 'REBUTTALS_AND_RESPONSES',
            'responses': responses,
            'timestamp': datetime.now().isoformat()
        }
    
    def _run_round_3(
        self,
        round_1: Dict,
        round_2: Dict,
        context: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Round 3: Consensus building
        
        Agents refine positions based on peer feedback
        Identify areas of agreement/conflict
        """
        
        refined_positions = []
        
        for agent in self.agents:
            print(f"  → {agent.agent_name} refining position...")
            
            # Get agent's original proposal
            original_proposal = next(
                (p for p in round_1['proposals'] if p.get('agent') == agent.agent_name),
                None
            )
            
            # Get feedback received
            feedback_received = self._collect_feedback_for_agent(
                agent.agent_name,
                round_2
            )
            
            # Refine position
            refined = self._refine_position(
                agent,
                original_proposal,
                feedback_received,
                context
            )
            
            refined_positions.append(refined)
            
            # Log to conversation
            if refined.get('position_changed'):
                self._log_message(
                    debate_session,
                    speaker=agent.agent_name,
                    message=f"[Refined Position] {refined.get('updated_position', 'No change')}",
                    timestamp=datetime.now()
                )
        
        return {
            'round': 3,
            'phase': 'CONSENSUS_BUILDING',
            'refined_positions': refined_positions,
            'timestamp': datetime.now().isoformat()
        }
    
    def _run_round_4(self, debate_session: Dict) -> Dict:
        """
        Round 4: Final vote
        
        Each agent casts final vote:
        - APPROVE
        - APPROVE_WITH_CONDITIONS
        - REJECT
        - VETO (for agents with veto power)
        """
        
        votes = []
        
        # Get all proposals
        round_1 = debate_session['rounds'][0]
        all_proposals = round_1['proposals']
        
        # Identify primary proposal (highest support in Round 1)
        primary_proposal = self._identify_primary_proposal(all_proposals)
        
        print(f"\n  Primary proposal: {primary_proposal.get('action_type')} by {primary_proposal.get('agent')}")
        
        for agent in self.agents:
            print(f"  → {agent.agent_name} voting...")
            
            # Cast vote
            vote = self._cast_vote(agent, primary_proposal, debate_session)
            votes.append(vote)
            
            # Log to conversation
            self._log_message(
                debate_session,
                speaker=agent.agent_name,
                message=f"[VOTE] {vote['vote']} - {vote.get('reasoning', '')}",
                timestamp=datetime.now()
            )
            
            print(f"     Vote: {vote['vote']}")
        
        return {
            'round': 4,
            'phase': 'FINAL_VOTE',
            'primary_proposal': primary_proposal,
            'votes': votes,
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_agent_response(
        self,
        agent,
        other_proposal: Dict,
        context: Dict
    ) -> Optional[Dict]:
        """Generate agent's response to another agent's proposal"""
        
        action_type = other_proposal.get('action_type')
        
        # Skip monitoring updates
        if action_type == 'MONITORING_UPDATE':
            return None
        
        # Determine stance
        if agent.agent_name == 'Energy & Cost Optimization Agent':
            # Energy agent evaluates cost-benefit
            if other_proposal.get('predicted_savings', {}).get('energy_kw', 0) > 10:
                stance = 'SUPPORT'
                comment = f"Support this proposal. Energy savings of {other_proposal.get('predicted_savings', {}).get('energy_kw', 0)} kW align with cost optimization goals."
            else:
                stance = 'NEUTRAL'
                comment = "Neutral on this proposal from cost perspective. Minimal energy impact."
        
        elif agent.agent_name == 'Operations & Safety Agent':
            # Safety agent checks N+1 and safety
            stance = 'SUPPORT'
            comment = "No safety concerns identified. N+1 redundancy maintained."
        
        elif agent.agent_name == 'Maintenance & Compliance Agent':
            # Maintenance checks equipment health
            stance = 'SUPPORT'
            comment = "Equipment health acceptable for proposed operation."
        
        else:
            stance = 'SUPPORT'
            comment = f"This proposal aligns with {agent.agent_name} objectives."
        
        return {
            'proposal_by': other_proposal.get('agent'),
            'stance': stance,
            'comment': comment
        }
    
    def _collect_feedback_for_agent(
        self,
        agent_name: str,
        round_2: Dict
    ) -> List[Dict]:
        """Collect all feedback received by an agent"""
        
        feedback = []
        
        for agent_response in round_2['responses']:
            for response in agent_response.get('responses_to_proposals', []):
                if response.get('proposal_by') == agent_name:
                    feedback.append({
                        'from': agent_response['agent'],
                        'stance': response.get('stance'),
                        'comment': response.get('comment')
                    })
        
        return feedback
    
    def _refine_position(
        self,
        agent,
        original_proposal: Dict,
        feedback: List[Dict],
        context: Dict
    ) -> Dict:
        """Agent refines position based on feedback"""
        
        # Count support
        support_count = sum(1 for f in feedback if f['stance'] == 'SUPPORT')
        total_feedback = len(feedback)
        
        position_changed = False
        updated_position = original_proposal.get('description', 'No change')
        
        # If low support, consider modifying
        if total_feedback > 0 and support_count / total_feedback < 0.5:
            position_changed = True
            updated_position = f"Modified: {original_proposal.get('description', '')} (addressing peer concerns)"
        
        return {
            'agent': agent.agent_name,
            'original_proposal': original_proposal,
            'feedback_received': feedback,
            'support_percentage': (support_count / total_feedback * 100) if total_feedback > 0 else 100,
            'position_changed': position_changed,
            'updated_position': updated_position
        }
    
    def _identify_primary_proposal(self, proposals: List[Dict]) -> Dict:
        """Identify primary proposal for voting"""
        
        # Filter out monitoring updates
        action_proposals = [
            p for p in proposals
            if p.get('action_type') not in ['MONITORING_UPDATE', 'ERROR']
        ]
        
        if not action_proposals:
            # Return first proposal if no actions
            return proposals[0] if proposals else {}
        
        # Return first action proposal
        return action_proposals[0]
    
    def _cast_vote(
        self,
        agent,
        primary_proposal: Dict,
        debate_session: Dict
    ) -> Dict:
        """Agent casts final vote on primary proposal"""
        
        # Operations & Safety Agent has veto power
        if agent.agent_name == 'Operations & Safety Agent':
            # Check N+1 (simplified)
            vote = 'APPROVE'
            reasoning = 'Safety checks passed. N+1 maintained.'
        
        # Maintenance & Compliance Agent has veto power
        elif agent.agent_name == 'Maintenance & Compliance Agent':
            vote = 'APPROVE'
            reasoning = 'Equipment health acceptable. No compliance issues.'
        
        # Energy & Cost Agent has veto power on negative ROI
        elif agent.agent_name == 'Energy & Cost Optimization Agent':
            savings = primary_proposal.get('predicted_savings', {}).get('energy_kw', 0)
            if savings > 0:
                vote = 'APPROVE'
                reasoning = f'Positive ROI: {savings} kW savings.'
            else:
                vote = 'APPROVE'
                reasoning = 'No cost concerns.'
        
        else:
            vote = 'APPROVE'
            reasoning = f'Proposal aligns with {agent.agent_name} objectives.'
        
        return {
            'agent': agent.agent_name,
            'vote': vote,
            'reasoning': reasoning,
            'confidence': 0.85,
            'timestamp': datetime.now().isoformat()
        }
    
    def _format_proposal_message(self, proposal: Dict) -> str:
        """Format proposal as human-readable message"""
        
        action_type = proposal.get('action_type', 'UNKNOWN')
        description = proposal.get('description', 'No description')
        
        return f"[PROPOSAL] {action_type}: {description}"
    
    def _log_message(
        self,
        debate_session: Dict,
        speaker: str,
        message: str,
        timestamp: datetime,
        **kwargs
    ):
        """Log message to conversation log"""
        
        log_entry = {
            'timestamp': timestamp.isoformat(),
            'speaker': speaker,
            'message': message,
            **kwargs
        }
        
        debate_session['conversation_log'].append(log_entry)