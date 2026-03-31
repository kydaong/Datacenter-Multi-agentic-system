"""
Main Orchestrator
Human-facing interface that coordinates all 6 agents
Manages debate protocol and presents results to humans for approval
Handles human "nudge" requests for alternative solutions
"""

import sys
import time
sys.path.append('..')

from typing import Dict, List, Optional
from datetime import datetime
import json

from agents.demand_conditions_agent import DemandConditionsAgent
from agents.chiller_optimization import ChillerOptimizationAgent
from agents.building_system_agent import BuildingSystemAgent
from agents.energy_cost_agent import EnergyCostAgent
from agents.maintenance_compliance_agent import MaintenanceComplianceAgent
from agents.operations_safety_agent import OperationsSafetyAgent

from .debate_manager import DebateManager
from .consensus_builder import ConsensusBuilder
from .decision_logger import DecisionLogger
from .short_term_memory import ShortTermMemory
from .medium_term_memory import MediumTermMemory
from .long_term_memory import LongTermMemory
from .learning_tracker import LearningTracker


class Orchestrator:
    """
    Main Orchestrator - Human Interface
    
    This is the ONLY interface between humans and the agent system.
    
    Responsibilities:
    - Receive context from human or monitoring systems
    - Coordinate all 6 agents through 4-round debate
    - Build consensus or escalate conflicts
    - Present final decision to human for approval/rejection
    - Handle human "nudge" for alternative solutions
    - Execute approved decisions via MCP servers
    - Log complete conversation history
    - Track learning and improvement
    
    Human interactions:
    1. Human → Orchestrator: Query or context
    2. Orchestrator → Agents: Run debate
    3. Orchestrator → Human: Present decision for approval
    4. Human → Orchestrator: Approve/Reject/Nudge
    5. Orchestrator → Systems: Execute if approved
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize orchestrator with all agents and memory systems
        
        Args:
            connection_string: SQL Server connection string
        """
        
        print("="*70)
        print("INITIALIZING MULTI-AGENT CHILLER OPTIMIZATION SYSTEM (MAGS)")
        print("="*70)
        
        # Initialize all 6 agents
        print("\n[AGENTS] Initializing agent team...")
        print("  [1/6] Demand & Conditions Agent...")
        self.demand_agent = DemandConditionsAgent()
        
        print("  [2/6] Chiller Optimization Agent...")
        self.chiller_agent = ChillerOptimizationAgent()
        
        print("  [3/6] Building System Agent...")
        self.building_agent = BuildingSystemAgent()
        
        print("  [4/6] Energy & Cost Agent...")
        self.energy_agent = EnergyCostAgent()
        
        print("  [5/6] Maintenance & Compliance Agent...")
        self.maintenance_agent = MaintenanceComplianceAgent()
        
        print("  [6/6] Operations & Safety Agent...")
        self.safety_agent = OperationsSafetyAgent()
        
        # Store all agents in list for iteration
        self.agents = [
            self.demand_agent,
            self.chiller_agent,
            self.building_agent,
            self.energy_agent,
            self.maintenance_agent,
            self.safety_agent
        ]
        
        print("\n[DEBATE] Initializing debate system...")
        self.debate_manager = DebateManager(self.agents)
        self.consensus_builder = ConsensusBuilder(self.agents)
        
        print("\n[LOGGING] Initializing decision logger...")
        self.decision_logger = DecisionLogger(connection_string)
        
        print("\n[MEMORY] Initializing memory systems...")
        self.short_term_memory = ShortTermMemory(connection_string)
        self.medium_term_memory = MediumTermMemory(connection_string)
        self.long_term_memory = LongTermMemory(connection_string)
        
        print("\n[LEARNING] Initializing learning tracker...")
        self.learning_tracker = LearningTracker(connection_string)
        
        print("\n" + "="*70)
        print("✅ MAGS INITIALIZED - READY FOR OPERATIONS")
        print("="*70)
        print("\nOrchestrator is the human interface.")
        print("Humans interact ONLY with Orchestrator.")
        print("All 6 agents work behind the scenes.\n")
    
    def analyze_and_propose(
        self,
        context: Dict,
        human_input: Optional[str] = None
    ) -> Dict:
        """
        Main entry point: Analyze situation and propose actions
        
        This is what humans call to get recommendations
        
        Args:
            context: Current system state (telemetry, forecasts, etc.)
            human_input: Optional human query or concern
        
        Returns:
            Final decision package ready for human approval/rejection
        """
        
        session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print("\n" + "="*70)
        print(f"NEW ANALYSIS SESSION: {session_id}")
        print("="*70)
        
        if human_input:
            print(f"\n💬 Human Input: {human_input}")
        
        print(f"\n📊 System Context:")
        print(f"  • Cooling Load: {context.get('cooling_load_kw', 'N/A')} kW")
        print(f"  • IT Load: {context.get('it_load_kw', 'N/A')} kW")
        print(f"  • PUE: {context.get('current_pue', 'N/A')}")
        print(f"  • Wet-bulb: {context.get('wet_bulb_temp', 'N/A')}°C")
        
        # Store system state
        self.short_term_memory.store_system_state(context)
        
        # Start 4-round debate
        print("\n" + "-"*70)
        print("🗣️  INITIATING 4-ROUND AGENT DEBATE")
        print("-"*70)
        
        debate_result = self.debate_manager.run_debate(context, human_input)
        
        # Build consensus
        print("\n" + "-"*70)
        print("🤝 BUILDING CONSENSUS")
        print("-"*70)
        
        consensus_result = self.consensus_builder.build_consensus(debate_result)
        
        # Log complete session
        self.decision_logger.log_session(
            session_id=session_id,
            context=context,
            human_input=human_input,
            debate_result=debate_result,
            consensus_result=consensus_result
        )
        
        # Store in memory
        self.short_term_memory.store_decision(
            session_id=session_id,
            decision=consensus_result['decision']
        )
        
        # Format for human presentation
        human_decision_package = self._format_for_human(
            session_id=session_id,
            context=context,
            consensus_result=consensus_result,
            debate_result=debate_result
        )
        
        print("\n" + "="*70)
        print("✅ DECISION READY FOR HUMAN REVIEW")
        print("="*70)
        
        return human_decision_package
    
    def handle_human_nudge(
        self,
        session_id: str,
        nudge_feedback: str,
        original_context: Dict
    ) -> Dict:
        """
        Handle human "nudge" - request for alternative solutions
        
        When human says:
        - "Give me alternative options"
        - "What if we prioritize cost over efficiency?"
        - "Can we do this more conservatively?"
        - "Show me a faster option"
        - "I don't like this, give me something else"
        
        Args:
            session_id: Original session ID
            nudge_feedback: Human's feedback/constraint
            original_context: Original context
        
        Returns:
            New decision package with alternative solutions
        """
        
        print("\n" + "="*70)
        print(f"👉 HUMAN NUDGE RECEIVED - SESSION: {session_id}")
        print("="*70)
        print(f"\n💬 Nudge: {nudge_feedback}")
        print("\n⚡ Re-running debate with human guidance...")
        
        # Add nudge to context
        nudged_context = original_context.copy()
        nudged_context['human_nudge'] = nudge_feedback
        nudged_context['nudge_session_id'] = session_id
        nudged_context['nudge_instruction'] = self._parse_nudge_intent(nudge_feedback)
        
        # Re-run debate with nudge context
        print("\n" + "-"*70)
        print("🗣️  RE-RUNNING DEBATE WITH HUMAN GUIDANCE")
        print("-"*70)
        
        debate_result = self.debate_manager.run_debate(
            nudged_context,
            human_input=f"[NUDGE] {nudge_feedback}"
        )
        
        # Build new consensus
        consensus_result = self.consensus_builder.build_consensus(debate_result)
        
        # Log nudge session
        nudge_session_id = f"{session_id}_NUDGE_{datetime.now().strftime('%H%M%S')}"
        
        self.decision_logger.log_session(
            session_id=nudge_session_id,
            context=nudged_context,
            human_input=f"[NUDGE] {nudge_feedback}",
            debate_result=debate_result,
            consensus_result=consensus_result
        )
        
        # Format for human
        human_decision_package = self._format_for_human(
            session_id=nudge_session_id,
            context=nudged_context,
            consensus_result=consensus_result,
            debate_result=debate_result,
            is_nudge=True,
            original_session_id=session_id
        )
        
        print("\n" + "="*70)
        print("✅ ALTERNATIVE SOLUTION READY")
        print("="*70)
        
        return human_decision_package
    
    def handle_human_approval(
        self,
        session_id: str,
        approved: bool,
        approval_notes: Optional[str] = None
    ) -> Dict:
        """
        Handle human approval or rejection
        
        Args:
            session_id: Session ID
            approved: True if approved, False if rejected
            approval_notes: Optional human notes
        
        Returns:
            Result of handling
        """
        
        print("\n" + "="*70)
        if approved:
            print(f"✅ HUMAN APPROVED - SESSION: {session_id}")
        else:
            print(f"❌ HUMAN REJECTED - SESSION: {session_id}")
        print("="*70)
        
        if approval_notes:
            print(f"\n💬 Notes: {approval_notes}")
        
        if approved:
            # Execute approved decision
            return self.execute_approved_decision(session_id, approval_notes)
        else:
            # Log rejection
            return {
                'status': 'REJECTED',
                'session_id': session_id,
                'message': 'Decision rejected by human operator',
                'notes': approval_notes
            }
    
    def execute_approved_decision(
        self,
        session_id: str,
        approval_notes: Optional[str] = None
    ) -> Dict:
        """
        Execute decision approved by human
        
        Args:
            session_id: Session ID of approved decision
            approval_notes: Optional human notes
        
        Returns:
            Execution status
        """
        
        print("\n" + "-"*70)
        print(f"⚙️  EXECUTING APPROVED DECISION")
        print("-"*70)
        
        # Load decision from log
        decision = self.decision_logger.load_session(session_id)
        
        if not decision:
            return {
                'status': 'ERROR',
                'message': f'Session {session_id} not found'
            }
        
        # Execute via MCP servers (simulated for now)
        execution_result = self._execute_decision(decision, approval_notes)
        
        # Log execution
        self.decision_logger.log_execution(
            session_id=session_id,
            approval_notes=approval_notes,
            execution_result=execution_result
        )
        
        # Update short-term memory
        # (Mark decision as executed)
        
        print("\n✅ Execution complete")
        
        return execution_result
    
    def _parse_nudge_intent(self, nudge_feedback: str) -> str:
        """
        Parse human nudge to understand intent
        
        Args:
            nudge_feedback: Human feedback
        
        Returns:
            Parsed intent
        """
        
        feedback_lower = nudge_feedback.lower()
        
        if any(word in feedback_lower for word in ['alternative', 'different', 'other option']):
            return 'REQUEST_ALTERNATIVES'
        elif any(word in feedback_lower for word in ['conservative', 'safe', 'careful']):
            return 'PRIORITIZE_SAFETY'
        elif any(word in feedback_lower for word in ['aggressive', 'faster', 'maximum']):
            return 'PRIORITIZE_PERFORMANCE'
        elif any(word in feedback_lower for word in ['cost', 'cheap', 'save money']):
            return 'PRIORITIZE_COST'
        elif any(word in feedback_lower for word in ['efficiency', 'pue', 'energy']):
            return 'PRIORITIZE_EFFICIENCY'
        else:
            return 'GENERAL_REFINEMENT'
    
    def _format_for_human(
        self,
        session_id: str,
        context: Dict,
        consensus_result: Dict,
        debate_result: Dict,
        is_nudge: bool = False,
        original_session_id: Optional[str] = None
    ) -> Dict:
        """
        Format decision for human presentation
        
        Clean, readable format that humans can approve/reject/nudge
        """
        
        decision = consensus_result['decision']
        
        # Build human-readable summary
        summary = {
            'session_id': session_id,
            'timestamp': datetime.now().isoformat(),
            'is_nudge_response': is_nudge,
            'original_session_id': original_session_id if is_nudge else None,
            
            # Executive Summary
            'executive_summary': {
                'recommendation': decision.get('action_type', 'MONITORING_UPDATE'),
                'description': decision.get('description', 'No action required'),
                'confidence': round(consensus_result['confidence'], 2),
                'consensus_strength': consensus_result['consensus_type'],
                'requires_human_approval': self._requires_human_approval(consensus_result)
            },
            
            # Predicted Impact
            'predicted_impact': decision.get('predicted_savings', {}),
            
            # Agent Consensus
            'agent_consensus': {
                'votes': consensus_result['votes'],
                'support_level': f"{consensus_result['support_percentage']:.0f}%",
                'vetoes': consensus_result['vetoes'],
                'concerns': consensus_result['concerns']
            },
            
            # Key Arguments (from debate)
            'key_arguments': self._extract_key_arguments(debate_result),
            
            # Risks & Mitigations
            'risks': self._extract_risks(debate_result),
            
            # Alternative Options (if any)
            'alternatives': self._extract_alternatives(debate_result),
            
            # Full Debate Transcript (for deep dive)
            'debate_transcript_summary': self._summarize_debate(debate_result),
            'full_debate_log': debate_result['conversation_log'],
            
            # Execution Plan (if approved)
            'execution_plan': self._create_execution_plan(decision),
            
            # Actions available to human
            'human_actions': {
                'can_approve': True,
                'can_reject': True,
                'can_nudge': True,
                'nudge_suggestions': [
                    "Give me a more conservative option",
                    "Show me alternatives that prioritize cost",
                    "What if we wait longer before acting?",
                    "Make this more aggressive"
                ]
            }
        }
        
        return summary
    
    def _requires_human_approval(self, consensus_result: Dict) -> bool:
        """Determine if human approval required"""
        
        # Require human approval if:
        # 1. Low confidence (<0.70)
        # 2. Any veto present
        # 3. Weak consensus or conflict
        # 4. High risk actions
        
        if consensus_result['confidence'] < 0.70:
            return True
        
        if consensus_result['vetoes']:
            return True
        
        if consensus_result['consensus_type'] in ['WEAK', 'CONFLICT', 'VETO']:
            return True
        
        decision = consensus_result['decision']
        if decision.get('action_type') in ['URGENT_MAINTENANCE', 'EMERGENCY_RESPONSE', 'VETO_BLOCKED']:
            return True
        
        return False
    
    def _extract_key_arguments(self, debate_result: Dict) -> List[Dict]:
        """Extract key arguments from debate"""
        
        arguments = []
        
        # Get proposals from Round 1
        for proposal in debate_result['rounds'][0]['proposals']:
            if proposal.get('action_type') not in ['MONITORING_UPDATE', 'ERROR']:
                arguments.append({
                    'agent': proposal.get('agent', 'Unknown'),
                    'position': proposal.get('description', ''),
                    'justification': proposal.get('justification', '')[:200]  # Truncate
                })
        
        return arguments[:5]  # Top 5
    
    def _extract_risks(self, debate_result: Dict) -> List[str]:
        """Extract risks identified during debate"""
        
        risks = []
        
        # Scan all rounds for risk mentions
        for round_data in debate_result['rounds']:
            for proposal in round_data.get('proposals', []):
                justification = proposal.get('justification', '').lower()
                if any(word in justification for word in ['risk', 'concern', 'warning', 'danger']):
                    risk_text = proposal.get('justification', '')[:150]
                    risks.append(f"{proposal.get('agent', 'Unknown')}: {risk_text}")
        
        return risks[:5]  # Top 5
    
    def _extract_alternatives(self, debate_result: Dict) -> List[Dict]:
        """Extract alternative proposals from debate"""
        
        alternatives = []
        
        # Get all non-monitoring proposals from Round 1
        for proposal in debate_result['rounds'][0]['proposals']:
            if proposal.get('action_type') not in ['MONITORING_UPDATE', 'ERROR']:
                alternatives.append({
                    'proposed_by': proposal.get('agent', 'Unknown'),
                    'action': proposal.get('action_type'),
                    'description': proposal.get('description', '')
                })
        
        return alternatives
    
    def _summarize_debate(self, debate_result: Dict) -> str:
        """Create human-readable debate summary"""
        
        summary_lines = []
        
        for round_data in debate_result['rounds']:
            round_num = round_data['round']
            phase = round_data['phase']
            
            summary_lines.append(f"\nRound {round_num} ({phase}):")
            
            if round_num == 1:
                # Initial proposals
                proposals = round_data.get('proposals', [])
                for p in proposals:
                    action = p.get('action_type', 'UNKNOWN')
                    agent = p.get('agent', 'Unknown')
                    summary_lines.append(f"  • {agent}: {action}")
            
            elif round_num == 4:
                # Final votes
                votes = round_data.get('votes', [])
                approve_count = sum(1 for v in votes if v.get('vote') == 'APPROVE')
                summary_lines.append(f"  • Votes: {approve_count}/{len(votes)} approved")
        
        return "\n".join(summary_lines)
    
    def _create_execution_plan(self, decision: Dict) -> Optional[Dict]:
        """Create execution plan for decision"""
        
        action_type = decision.get('action_type')
        
        if action_type in ['MONITORING_UPDATE', 'VETO_BLOCKED']:
            return None
        
        return {
            'action_type': action_type,
            'steps': [
                'Verify N+1 redundancy',
                'Check safety interlocks',
                'Execute control sequence',
                'Monitor equipment response',
                'Validate system stability',
                'Log execution result'
            ],
            'estimated_duration_minutes': 15,
            'rollback_available': True
        }
    
    def _execute_decision(self, decision: Dict, approval_notes: Optional[str]) -> Dict:
        """
        Execute approved decision via MCP servers
        
        This would call:
        - BMSControlServer for equipment control
        - NotificationServer for alerts
        - DataIngestionServer for logging
        
        Simulated for now - would be replaced with actual MCP calls
        """
        
        consensus_result = decision.get('consensus_result', {})
        actual_decision = consensus_result.get('decision', {})
        action_type = actual_decision.get('action_type', 'UNKNOWN')

        print(f"\n  📋 Action: {action_type}")
        if approval_notes:
            print(f"  📝 Notes: {approval_notes}")

        # Simulate execution steps
        execution_steps = [
            'Validating N+1 redundancy...',
            'Checking safety interlocks...',
            'Executing control sequence...',
            'Monitoring equipment response...',
            'Validating system stability...',
            'Logging execution result...'
        ]

        for step in execution_steps:
            print(f"  • {step}")
            time.sleep(0.3)  # Simulate work

        # Simulate execution result
        execution_result = {
            'status': 'SUCCESS',
            'executed_at': datetime.now().isoformat(),
            'action_type': action_type,
            'equipment_affected': actual_decision.get('equipment_affected', []),
            'execution_log': execution_steps,
            'approval_notes': approval_notes,
            'actual_outcome': {
                'energy_savings_kw': actual_decision.get('predicted_savings', {}).get('energy_kw', 0),
                'execution_time_seconds': len(execution_steps) * 0.3
            }
        }
        
        return execution_result
    
    def get_system_status(self) -> Dict:
        """
        Get current system status
        
        Returns:
            System status summary
        """
        
        current_state = self.short_term_memory.get_current_state()
        recent_decisions = self.short_term_memory.get_recent_decisions(hours=24)
        
        return {
            'current_state': current_state,
            'decisions_last_24h': len(recent_decisions),
            'agents_online': len(self.agents),
            'memory_systems': {
                'short_term': 'OPERATIONAL',
                'medium_term': 'OPERATIONAL',
                'long_term': 'OPERATIONAL'
            },
            'status': 'OPERATIONAL'
        }


# Example usage and testing
if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("ORCHESTRATOR TEST")
    print("="*70)
    
    # Initialize orchestrator
    orchestrator = Orchestrator()
    
    # Example scenario: Morning peak hour
    context = {
        'cooling_load_kw': 2800,
        'it_load_kw': 9500,
        'total_facility_power_kw': 11800,
        'wet_bulb_temp': 25.5,
        'dry_bulb_temp': 31.0,
        'humidity_percent': 78,
        'chillers_online': ['Chiller-1', 'Chiller-2'],
        'current_pue': 1.24,
        'timestamp': datetime.now().isoformat()
    }
    
    # Human query
    human_input = "System seems to be running well. Any optimization opportunities?"
    
    # Run analysis
    decision = orchestrator.analyze_and_propose(context, human_input)
    
    # Display decision to human
    print("\n" + "="*70)
    print("📊 DECISION PACKAGE FOR HUMAN")
    print("="*70)
    print(f"\n🆔 Session ID: {decision['session_id']}")
    print(f"\n💡 Recommendation: {decision['executive_summary']['recommendation']}")
    print(f"📝 Description: {decision['executive_summary']['description']}")
    print(f"🎯 Confidence: {decision['executive_summary']['confidence']}")
    print(f"🤝 Consensus: {decision['executive_summary']['consensus_strength']}")
    print(f"👥 Agent Support: {decision['agent_consensus']['support_level']}")
    
    if decision['predicted_impact']:
        print(f"\n📈 Predicted Impact:")
        for key, value in decision['predicted_impact'].items():
            print(f"  • {key}: {value}")
    
    print(f"\n Requires Approval: {decision['executive_summary']['requires_human_approval']}")
    
    print("\n" + "="*70)
    print("ORCHESTRATOR TEST COMPLETE")
    print("="*70)