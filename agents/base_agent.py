"""
Base Agent Class
All agents inherit from this class
Provides common functionality: memory access, Qdrant search, SQL queries
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import anthropic
import os
from dotenv import load_dotenv
from datetime import datetime
import json

# Import memory systems
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'orchestrator'))

try:
    from orchestrator.qdrant_interface import qdrant
    from orchestrator.medium_term_memory import medium_term_memory
    from orchestrator.short_term_memory import short_term_memory
    from orchestrator.long_term_memory import long_term_memory
except ImportError:
    from qdrant_interface import qdrant
    from medium_term_memory import medium_term_memory
    from short_term_memory import short_term_memory
    from long_term_memory import long_term_memory

load_dotenv()

class BaseAgent(ABC):
    """
    Base class for all agents in the MAGS system
    
    Provides:
    - Access to all memory systems (short/medium/long-term, Qdrant)
    - Claude API integration
    - Common utility methods
    - Logging and tracking
    """
    
    def __init__(self, agent_name: str, agent_role: str):
        """
        Initialize base agent
        
        Args:
            agent_name: Agent identifier (e.g., "Chiller Optimization Agent")
            agent_role: Agent role description
        """
        self.agent_name = agent_name
        self.agent_role = agent_role
        
        # Initialize Claude client
        self.client = anthropic.Anthropic(
            api_key=os.getenv("ANTHROPIC_API_KEY")
        )
        
        # Memory access
        self.qdrant = qdrant
        self.medium_term_memory = medium_term_memory
        self.short_term_memory = short_term_memory
        self.long_term_memory = long_term_memory
        
        # Agent state
        self.confidence_level = 0.0
        self.last_proposal = None
        self.accuracy_history = []
        
        print(f"✅ {self.agent_name} initialized")
    
    @abstractmethod
    def analyze_situation(self, context: Dict) -> Dict:
        """
        Analyze current situation and generate insights
        Must be implemented by each agent
        
        Args:
            context: Current system state
        
        Returns:
            Analysis results
        """
        pass
    
    @abstractmethod
    def propose_action(self, context: Dict) -> Dict:
        """
        Propose an action based on current context
        Must be implemented by each agent
        
        Args:
            context: Current system state
        
        Returns:
            Proposed action with justification
        """
        pass
    
    def call_claude(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.7,
        max_tokens: int = 2000
    ) -> str:
        """
        Call Claude API with agent-specific prompt
        
        Args:
            system_prompt: System context
            user_message: User query
            temperature: Sampling temperature
            max_tokens: Max response tokens
        
        Returns:
            Claude's response
        """
        
        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=max_tokens,
                temperature=temperature,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_message}
                ]
            )
            
            return message.content[0].text
            
        except Exception as e:
            print(f"❌ Claude API error: {e}")
            return f"Error: {e}"
    
    def search_knowledge(
        self,
        query: str,
        knowledge_type: str = "sops",
        filters: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Search Qdrant knowledge base
        
        Args:
            query: Natural language query
            knowledge_type: Type of knowledge (sops, manuals, regulations, kpis)
            filters: Optional filters (equipment_type, etc.)
        
        Returns:
            Relevant knowledge chunks
        """
        
        if knowledge_type == "sops":
            equipment_type = filters.get('equipment_type') if filters else None
            return self.qdrant.search_sops(query, equipment_type=equipment_type)
            
        elif knowledge_type == "manuals":
            equipment_id = filters.get('equipment_id') if filters else None
            return self.qdrant.search_equipment_manuals(query, equipment_id=equipment_id)
            
        elif knowledge_type == "regulations":
            return self.qdrant.search_regulations(query)
            
        elif knowledge_type == "kpis":
            return self.qdrant.search_kpi_definitions(query)
            
        else:
            return []
    
    def get_historical_precedents(
        self,
        cooling_load_kw: float,
        wet_bulb_temp: float,
        days: int = 30
    ) -> List[Dict]:
        """
        Find similar historical situations
        
        Args:
            cooling_load_kw: Current cooling load
            wet_bulb_temp: Current wet-bulb temp
            days: Days to search back
        
        Returns:
            Similar historical situations with outcomes
        """
        
        df = self.medium_term_memory.get_similar_operating_conditions(
            cooling_load_kw=cooling_load_kw,
            wet_bulb_temp=wet_bulb_temp,
            days=days
        )
        
        if df.empty:
            return []
        
        return df.to_dict('records')
    
    def get_current_metrics(self) -> Dict:
        """
        Get current system metrics from short-term memory
        
        Returns:
            Current metrics dictionary
        """
        
        state = self.short_term_memory.get_current_state()
        return state.get('state', {}) if state else {}
    
    def log_proposal(self, proposal: Dict):
        """
        Log agent proposal for tracking
        
        Args:
            proposal: Proposal dictionary
        """
        
        self.last_proposal = {
            'timestamp': datetime.now(),
            'agent': self.agent_name,
            'proposal': proposal,
            'confidence': self.confidence_level
        }
        
        # Store in short-term memory
        self.short_term_memory.store_proposal(
            agent_name=self.agent_name,
            proposal=proposal,
            context={}
        )
    
    def calculate_confidence(
        self,
        historical_matches: int,
        data_quality: float,
        risk_level: str
    ) -> float:
        """
        Calculate confidence level for a proposal
        
        Args:
            historical_matches: Number of similar past cases
            data_quality: Data quality score (0-1)
            risk_level: Risk level (LOW, MEDIUM, HIGH)
        
        Returns:
            Confidence score (0-1)
        """
        
        # Base confidence from historical precedents
        if historical_matches >= 10:
            base_confidence = 0.9
        elif historical_matches >= 5:
            base_confidence = 0.7
        elif historical_matches >= 2:
            base_confidence = 0.5
        else:
            base_confidence = 0.3
        
        # Adjust for data quality
        confidence = base_confidence * data_quality
        
        # Adjust for risk
        risk_penalties = {
            'LOW': 0.0,
            'MEDIUM': 0.1,
            'HIGH': 0.2
        }
        confidence -= risk_penalties.get(risk_level, 0.1)
        
        # Clamp to [0, 1]
        self.confidence_level = max(0.0, min(1.0, confidence))
        
        return self.confidence_level
    
    def format_proposal(
        self,
        action_type: str,
        description: str,
        justification: str,
        predicted_savings: Dict,
        evidence: List[Dict],
        confidence: float
    ) -> Dict:
        """
        Format proposal in standard structure
        
        Args:
            action_type: Type of action (STAGING, SETPOINT, etc.)
            description: Action description
            justification: Why this action
            predicted_savings: Energy/cost/PUE predictions
            evidence: Supporting evidence
            confidence: Confidence level
        
        Returns:
            Formatted proposal dictionary
        """
        
        return {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action_type': action_type,
            'description': description,
            'justification': justification,
            'predicted_savings': predicted_savings,
            'evidence': evidence,
            'confidence': confidence,
            'requires_approval': confidence < 0.7 or predicted_savings.get('risk') == 'HIGH'
        }
    
    def cite_evidence(
        self,
        evidence_type: str,
        source: str,
        data: Any
    ) -> Dict:
        """
        Create evidence citation
        
        Args:
            evidence_type: Type (HISTORICAL, SOP, MANUFACTURER, REGULATION)
            source: Data source
            data: Evidence data
        
        Returns:
            Evidence citation
        """
        
        return {
            'type': evidence_type,
            'source': source,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }


# Example subclass implementation
class ExampleAgent(BaseAgent):
    """
    Example agent showing how to inherit from BaseAgent
    """
    
    def __init__(self):
        super().__init__(
            agent_name="Example Agent",
            agent_role="Demonstrates base agent usage"
        )
    
    def analyze_situation(self, context: Dict) -> Dict:
        """Example analysis implementation"""
        
        # Get current metrics
        current = self.get_current_metrics()
        
        # Search knowledge base
        sop_results = self.search_knowledge(
            query="chiller staging procedure",
            knowledge_type="sops",
            filters={'equipment_type': 'chiller'}
        )
        
        # Find historical precedents
        precedents = self.get_historical_precedents(
            cooling_load_kw=context.get('cooling_load_kw', 2800),
            wet_bulb_temp=context.get('wet_bulb_temp', 25.0)
        )
        
        return {
            'current_state': current,
            'sop_guidance': sop_results,
            'historical_precedents': precedents
        }
    
    def propose_action(self, context: Dict) -> Dict:
        """Example proposal implementation"""
        
        # Analyze situation
        analysis = self.analyze_situation(context)
        
        # Calculate confidence
        confidence = self.calculate_confidence(
            historical_matches=len(analysis['historical_precedents']),
            data_quality=0.9,
            risk_level='MEDIUM'
        )
        
        # Format proposal
        proposal = self.format_proposal(
            action_type="EXAMPLE_ACTION",
            description="Example action description",
            justification="Based on historical data and SOPs",
            predicted_savings={
                'energy_kw': 50,
                'cost_sgd': 100,
                'pue_improvement': 0.02
            },
            evidence=[
                self.cite_evidence(
                    'HISTORICAL',
                    'Medium-term memory',
                    analysis['historical_precedents'][:3]
                ),
                self.cite_evidence(
                    'SOP',
                    'SOP Database',
                    analysis['sop_guidance'][:1]
                )
            ],
            confidence=confidence
        )
        
        # Log proposal
        self.log_proposal(proposal)
        
        return proposal


# Test base agent
if __name__ == "__main__":
    
    print("="*70)
    print("TESTING BASE AGENT")
    print("="*70)
    
    # Create example agent
    agent = ExampleAgent()
    
    # Test context
    context = {
        'cooling_load_kw': 2800,
        'wet_bulb_temp': 25.5,
        'time': datetime.now()
    }
    
    # Test analysis
    print("\n[TEST 1] Analyze Situation...")
    analysis = agent.analyze_situation(context)
    print(f"  Current state: {len(analysis['current_state'])} metrics")
    print(f"  SOP results: {len(analysis['sop_guidance'])} documents")
    print(f"  Historical precedents: {len(analysis['historical_precedents'])} cases")
    
    # Test proposal
    print("\n[TEST 2] Propose Action...")
    proposal = agent.propose_action(context)
    print(f"  Agent: {proposal['agent']}")
    print(f"  Action: {proposal['action_type']}")
    print(f"  Confidence: {proposal['confidence']:.2f}")
    print(f"  Predicted savings: {proposal['predicted_savings']}")
    
    print("\n Base agent tests complete!")