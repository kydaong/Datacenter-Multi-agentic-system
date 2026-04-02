"""
Debate Manager
Manages 4-round debate protocol between agents
Facilitates natural, conversational agent-to-agent communication
"""

from typing import Dict, List, Optional
from datetime import datetime
import json
import random
import os
import anthropic
from dotenv import load_dotenv

load_dotenv()


class DebateManager:
    """
    Manages structured 4-round debate protocol with natural conversations
    
    Round 1: Initial proposals (parallel)
    Round 2: Agent responses and rebuttals (conversational)
    Round 3: Consensus building and position refinement (dialogue)
    Round 4: Final vote
    
    Uses Claude API for natural language generation
    """
    
    def __init__(self, agents: List, stream_callback=None):
        """
        Initialize debate manager

        Args:
            agents: List of all agent instances
            stream_callback: Optional callable(event_dict) for real-time streaming
        """
        self.agents = agents
        self.max_rounds = 4
        self.stream_callback = stream_callback

        # Initialize Claude client for conversational responses
        self.claude_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.claude_model = "claude-sonnet-4-6"
    
    def run_debate(
        self,
        context: Dict,
        human_input: Optional[str] = None
    ) -> Dict:
        """
        Run complete 4-round debate with natural conversations
        
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
        self._emit_round_marker(1, "Initial Proposals")
        round_1_result = self._run_round_1(context, debate_session)
        debate_session['rounds'].append(round_1_result)

        # Round 2: Conversational responses and rebuttals
        print("\n[ROUND 2/4] Agent conversations and rebuttals...")
        self._emit_round_marker(2, "Agent Responses")
        round_2_result = self._run_round_2_conversational(round_1_result, context, debate_session)
        debate_session['rounds'].append(round_2_result)

        # Round 3: Consensus building with dialogue
        print("\n[ROUND 3/4] Building consensus through dialogue...")
        self._emit_round_marker(3, "Consensus Building")
        round_3_result = self._run_round_3_conversational(round_1_result, round_2_result, context, debate_session)
        debate_session['rounds'].append(round_3_result)

        # Round 4: Final vote
        print("\n[ROUND 4/4] Final vote...")
        self._emit_round_marker(4, "Final Vote")
        round_4_result = self._run_round_4(debate_session)
        debate_session['rounds'].append(round_4_result)
        
        debate_session['end_time'] = datetime.now().isoformat()
        
        return debate_session
    
    def _run_round_1(self, context: Dict, debate_session: Dict) -> Dict:
        """
        Round 1: Each agent proposes independently

        All agents analyze situation in parallel and propose actions
        """

        # Inject human question into context so agents can factor it in
        round_context = dict(context)
        human_input = debate_session.get('human_input')
        if human_input:
            round_context['human_question'] = human_input

        proposals = []

        for agent in self.agents:
            print(f"  → {agent.agent_name}...")
            
            try:
                proposal = agent.propose_action(round_context)
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
    
    def _run_round_2_conversational(
        self,
        round_1: Dict,
        context: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Round 2: Agents have natural conversations about each other's proposals
        
        Each agent reviews other proposals and generates conversational responses
        using Claude API for natural language
        """
        
        responses = []
        
        # Get all proposals from Round 1
        round_1_proposals = round_1['proposals']
        
        # Filter out monitoring updates and errors for debate
        debate_proposals = [
            p for p in round_1_proposals 
            if p.get('action_type') not in ['MONITORING_UPDATE', 'ERROR']
        ]
        
        if not debate_proposals:
            # If no actionable proposals, have brief acknowledgments
            for agent in self.agents:
                print(f"  → {agent.agent_name} acknowledging...")
                
                response_text = self._generate_acknowledgment(agent, round_1_proposals)
                
                self._log_message(
                    debate_session,
                    speaker=agent.agent_name,
                    message=response_text,
                    timestamp=datetime.now()
                )
                
                responses.append({
                    'agent': agent.agent_name,
                    'response_text': response_text
                })
            
            return {
                'round': 2,
                'phase': 'ACKNOWLEDGMENTS',
                'responses': responses,
                'timestamp': datetime.now().isoformat()
            }
        
        # Have real conversations about proposals
        for agent in self.agents:
            print(f"  → {agent.agent_name} responding...")
            
            # Generate conversational response
            agent_response = self._generate_conversational_response(
                agent,
                debate_proposals,
                round_1_proposals,
                context,
                debate_session
            )
            
            responses.append(agent_response)
            
            # Log to conversation
            self._log_message(
                debate_session,
                speaker=agent.agent_name,
                message=agent_response['response_text'],
                timestamp=datetime.now(),
                metadata={'responding_to': agent_response.get('responding_to', [])}
            )
        
        return {
            'round': 2,
            'phase': 'CONVERSATIONAL_RESPONSES',
            'responses': responses,
            'timestamp': datetime.now().isoformat()
        }
    
    def _run_round_3_conversational(
        self,
        round_1: Dict,
        round_2: Dict,
        context: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Round 3: Consensus building through dialogue
        
        Agents refine positions based on peer feedback with natural dialogue
        """
        
        refined_positions = []
        
        for agent in self.agents:
            print(f"  → {agent.agent_name} refining position...")
            
            # Get agent's original proposal
            original_proposal = next(
                (p for p in round_1['proposals'] if p.get('agent') == agent.agent_name),
                None
            )
            
            # Get feedback received from Round 2
            feedback_received = self._collect_feedback_for_agent(
                agent.agent_name,
                round_2,
                debate_session
            )
            
            # Generate refined position with natural language
            refined = self._generate_refined_position(
                agent,
                original_proposal,
                feedback_received,
                context,
                debate_session
            )
            
            refined_positions.append(refined)
            
            # Log to conversation
            if refined.get('response_text'):
                self._log_message(
                    debate_session,
                    speaker=agent.agent_name,
                    message=refined['response_text'],
                    timestamp=datetime.now()
                )
        
        return {
            'round': 3,
            'phase': 'CONSENSUS_BUILDING_DIALOGUE',
            'refined_positions': refined_positions,
            'timestamp': datetime.now().isoformat()
        }
    
    def _run_round_4(self, debate_session: Dict) -> Dict:
        """
        Round 4: Final vote with brief reasoning
        
        Each agent casts final vote with conversational reasoning
        """
        
        votes = []
        
        # Get all proposals
        round_1 = debate_session['rounds'][0]
        all_proposals = round_1['proposals']
        
        # Identify primary proposal
        primary_proposal = self._identify_primary_proposal(all_proposals)
        
        print(f"\n  Primary proposal: {primary_proposal.get('action_type')} by {primary_proposal.get('agent')}")
        
        for agent in self.agents:
            print(f"  → {agent.agent_name} voting...")
            
            # Cast vote with conversational reasoning
            vote = self._cast_vote_conversational(
                agent,
                primary_proposal,
                debate_session
            )
            
            votes.append(vote)
            
            # Log to conversation
            self._log_message(
                debate_session,
                speaker=agent.agent_name,
                message=f"[VOTE: {vote['vote']}] {vote.get('reasoning_text', '')}",
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
    
    def _generate_conversational_response(
        self,
        agent,
        debate_proposals: List[Dict],
        all_proposals: List[Dict],
        context: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Generate natural conversational response using Claude API
        
        Agent reviews proposals and responds naturally as if in a meeting
        """
        
        # Build conversation context
        conversation_history = self._build_conversation_context(debate_session)
        
        # Create prompt for conversational response
        response_prompt = self._create_response_prompt(
            agent,
            debate_proposals,
            all_proposals,
            context,
            conversation_history
        )
        
        try:
            # Call Claude API for natural response
            message = self.claude_client.messages.create(
                model=self.claude_model,
                max_tokens=1000,
                system=agent.system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": response_prompt
                    }
                ]
            )
            
            response_text = message.content[0].text
            
            return {
                'agent': agent.agent_name,
                'response_text': response_text,
                'responding_to': [p.get('agent') for p in debate_proposals],
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"     Warning: Claude API error: {e}")
            # Fallback to template response
            return self._generate_template_response(agent, debate_proposals)
    
    def _generate_refined_position(
        self,
        agent,
        original_proposal: Dict,
        feedback_received: List[str],
        context: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Generate refined position through natural dialogue
        """
        
        if not original_proposal or original_proposal.get('action_type') == 'MONITORING_UPDATE':
            return {
                'agent': agent.agent_name,
                'position_changed': False,
                'response_text': f"I maintain my position. The system is operating within normal parameters."
            }
        
        # Build conversation context
        conversation_history = self._build_conversation_context(debate_session)
        
        # Create refinement prompt
        refinement_prompt = self._create_refinement_prompt(
            agent,
            original_proposal,
            feedback_received,
            conversation_history
        )
        
        try:
            # Call Claude API
            message = self.claude_client.messages.create(
                model=self.claude_model,
                max_tokens=800,
                system=agent.system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": refinement_prompt
                    }
                ]
            )
            
            response_text = message.content[0].text
            
            # Determine if position changed
            position_changed = any(
                word in response_text.lower() 
                for word in ['adjust', 'modify', 'refine', 'change', 'update', 'revise']
            )
            
            return {
                'agent': agent.agent_name,
                'position_changed': position_changed,
                'response_text': response_text,
                'original_proposal': original_proposal,
                'feedback_received': feedback_received
            }
            
        except Exception as e:
            print(f"     Warning: Claude API error: {e}")
            return {
                'agent': agent.agent_name,
                'position_changed': False,
                'response_text': f"Based on the discussion, I stand by my original position."
            }
    
    def _cast_vote_conversational(
        self,
        agent,
        primary_proposal: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Cast vote with conversational reasoning
        """
        
        # Build conversation context
        conversation_history = self._build_conversation_context(debate_session)
        
        # Create voting prompt
        human_question = debate_session.get('human_input', '')
        vote_prompt = self._create_vote_prompt(
            agent,
            primary_proposal,
            conversation_history,
            human_question=human_question
        )
        
        try:
            # Call Claude API
            message = self.claude_client.messages.create(
                model=self.claude_model,
                max_tokens=500,
                system=agent.system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": vote_prompt
                    }
                ]
            )
            
            response_text = message.content[0].text
            
            # Parse vote from response
            vote_value = self._parse_vote_from_text(response_text, agent, primary_proposal)
            
            return {
                'agent': agent.agent_name,
                'vote': vote_value,
                'reasoning_text': response_text,
                'confidence': 0.85,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"     Warning: Claude API error: {e}")
            # Fallback vote
            return {
                'agent': agent.agent_name,
                'vote': 'APPROVE',
                'reasoning_text': f"I approve this proposal based on my analysis.",
                'confidence': 0.75,
                'timestamp': datetime.now().isoformat()
            }
    
    def _create_response_prompt(
        self,
        agent,
        debate_proposals: List[Dict],
        all_proposals: List[Dict],
        context: Dict,
        conversation_history: str
    ) -> str:
        """Create prompt for Round 2 conversational response"""
        
        # Format proposals
        proposals_text = "\n\n".join([
            f"**{p.get('agent')}** proposes: {p.get('action_type')}\n"
            f"Description: {p.get('description', 'No description')}\n"
            f"Justification: {p.get('justification', 'No justification')}"
            for p in debate_proposals
        ])
        
        # Surface the human's question for context
        human_question = context.get('human_question', '')
        human_context_line = f"\nHUMAN QUESTION: {human_question}\n" if human_question else ""

        prompt = f"""You are participating in a collaborative optimization meeting for a chiller plant. This is Round 2 of the debate.
{human_context_line}
CONVERSATION SO FAR:
{conversation_history}

PROPOSALS ON THE TABLE:
{proposals_text}

YOUR TASK:
Respond naturally to these proposals as if you're in a professional meeting. Your response should:

1. Acknowledge proposals from other agents
2. Share your perspective on their ideas
3. Point out synergies or conflicts with your area of expertise
4. Raise any concerns from your domain perspective
5. Suggest refinements or alternatives if appropriate

Be conversational but professional. Speak naturally like a domain expert in a meeting.
- Use phrases like "I agree with..." or "I have concerns about..." or "Building on what [Agent] said..."
- Reference specific agents by name when responding to their ideas
- Be specific about your reasoning
- Keep it focused (2-4 paragraphs)

Respond now as {agent.agent_name}:"""
        
        return prompt
    
    def _create_refinement_prompt(
        self,
        agent,
        original_proposal: Dict,
        feedback_received: List[str],
        conversation_history: str
    ) -> str:
        """Create prompt for Round 3 position refinement"""
        
        feedback_text = "\n".join([f"- {fb}" for fb in feedback_received]) if feedback_received else "No specific feedback received"
        
        prompt = f"""You are participating in Round 3 of a collaborative optimization meeting. Based on the discussion so far, you need to refine your position.

CONVERSATION SO FAR:
{conversation_history}

YOUR ORIGINAL PROPOSAL:
{original_proposal.get('action_type')}: {original_proposal.get('description')}

FEEDBACK RECEIVED:
{feedback_text}

YOUR TASK:
Based on the discussion and feedback, respond naturally:

1. If the feedback raises valid concerns, acknowledge them and adjust your position
2. If your original position is still sound, reaffirm it with additional context
3. If you see opportunities to incorporate others' ideas, suggest how to combine approaches
4. Be open to collaboration while maintaining your domain expertise

Be conversational. Use phrases like:
- "After hearing everyone's input..."
- "I see [Agent]'s point about..."
- "I'd like to refine my recommendation to..."
- "I still believe... because..."

Keep it focused (2-3 paragraphs).

Respond now as {agent.agent_name}:"""
        
        return prompt
    
    def _create_vote_prompt(
        self,
        agent,
        primary_proposal: Dict,
        conversation_history: str,
        human_question: str = ''
    ) -> str:
        """Create prompt for Round 4 final vote"""

        human_context_line = f"\nHUMAN QUESTION: {human_question}\n" if human_question else ""

        prompt = f"""You are in the final round of a collaborative optimization meeting. Time to vote.
{human_context_line}
CONVERSATION SO FAR:
{conversation_history}

PRIMARY PROPOSAL:
{primary_proposal.get('action_type')} by {primary_proposal.get('agent')}
Description: {primary_proposal.get('description')}

YOUR TASK:
Cast your final vote and explain your reasoning briefly. Your vote must be ONE of:
- APPROVE
- APPROVE_WITH_CONDITIONS
- REJECT
- VETO (only if you have veto authority and there's a critical safety/compliance issue)

Format your response as:
VOTE: [your vote]
REASONING: [1-2 sentences explaining your decision]

Be direct and clear. Reference key points from the debate if relevant.

Cast your vote now as {agent.agent_name}:"""
        
        return prompt
    
    def _parse_vote_from_text(self, response_text: str, agent, primary_proposal: Dict) -> str:
        """Parse vote value from natural language response"""
        
        text_upper = response_text.upper()
        
        # Check for explicit vote declaration
        if 'VOTE:' in text_upper:
            vote_line = [line for line in response_text.split('\n') if 'VOTE:' in line.upper()][0]
            if 'APPROVE WITH CONDITIONS' in vote_line.upper() or 'APPROVE_WITH_CONDITIONS' in vote_line.upper():
                return 'APPROVE_WITH_CONDITIONS'
            elif 'VETO' in vote_line.upper():
                return 'VETO'
            elif 'REJECT' in vote_line.upper():
                return 'REJECT'
            elif 'APPROVE' in vote_line.upper():
                return 'APPROVE'
        
        # Infer from text sentiment
        if 'veto' in text_upper or 'cannot approve' in text_upper:
            return 'VETO' if agent.agent_name in ['Operations & Safety Agent', 'Maintenance & Compliance Agent', 'Energy & Cost Optimization Agent'] else 'REJECT'
        elif 'reject' in text_upper or 'oppose' in text_upper:
            return 'REJECT'
        elif 'condition' in text_upper or 'provided that' in text_upper:
            return 'APPROVE_WITH_CONDITIONS'
        elif 'approve' in text_upper or 'support' in text_upper or 'agree' in text_upper:
            return 'APPROVE'
        
        # Default
        return 'APPROVE'
    
    def _generate_acknowledgment(self, agent, proposals: List[Dict]) -> str:
        """Generate brief acknowledgment when no actionable proposals"""
        
        templates = [
            f"From my perspective as {agent.agent_name}, the system looks stable right now. All metrics are within acceptable ranges. I don't see any immediate optimization opportunities.",
            f"I concur with the monitoring updates. The facility is operating normally from a {agent.agent_role.split()[0].lower()} standpoint.",
            f"Everything checks out from my end. The system is running as expected with no flags or concerns."
        ]
        
        return random.choice(templates)
    
    def _generate_template_response(self, agent, debate_proposals: List[Dict]) -> Dict:
        """Fallback template response if Claude API fails"""
        
        if not debate_proposals:
            response_text = f"I've reviewed the current state. From my perspective, the system is operating normally."
        else:
            first_proposal = debate_proposals[0]
            response_text = f"I've reviewed {first_proposal.get('agent')}'s proposal for {first_proposal.get('action_type')}. From my {agent.agent_role.split()[0].lower()} perspective, I need to evaluate the impacts carefully before committing."
        
        return {
            'agent': agent.agent_name,
            'response_text': response_text,
            'responding_to': [p.get('agent') for p in debate_proposals]
        }
    
    def _build_conversation_context(self, debate_session: Dict) -> str:
        """Build conversation history for context"""
        
        conversation_log = debate_session.get('conversation_log', [])
        
        # Get last 10 messages for context
        recent_messages = conversation_log[-10:] if len(conversation_log) > 10 else conversation_log
        
        context_lines = []
        for msg in recent_messages:
            speaker = msg.get('speaker', 'Unknown')
            message = msg.get('message', '')
            context_lines.append(f"{speaker}: {message}")
        
        return "\n\n".join(context_lines)
    
    def _collect_feedback_for_agent(
        self,
        agent_name: str,
        round_2: Dict,
        debate_session: Dict
    ) -> List[str]:
        """Collect feedback mentions for an agent from Round 2"""
        
        feedback = []
        
        # Extract from conversation log
        conversation_log = debate_session.get('conversation_log', [])
        
        for msg in conversation_log:
            message_text = msg.get('message', '')
            speaker = msg.get('speaker', '')
            
            # Check if this message mentions the agent
            if agent_name in message_text and speaker != agent_name:
                feedback.append(f"{speaker}: {message_text}")
        
        return feedback[-5:]  # Last 5 relevant mentions
    
    def _identify_primary_proposal(self, proposals: List[Dict]) -> Dict:
        """Identify primary proposal for voting"""
        
        # Filter out monitoring updates
        action_proposals = [
            p for p in proposals
            if p.get('action_type') not in ['MONITORING_UPDATE', 'ERROR']
        ]
        
        if not action_proposals:
            return proposals[0] if proposals else {}
        
        return action_proposals[0]
    
    def _format_proposal_message(self, proposal: Dict) -> str:
        """Format proposal as human-readable message"""
        
        action_type = proposal.get('action_type', 'UNKNOWN')
        description = proposal.get('description', 'No description')
        
        return f"[PROPOSAL] {action_type}: {description}"
    
    def _emit_round_marker(self, round_num: int, label: str):
        """Emit an explicit round-start marker to the UI stream."""
        if self.stream_callback:
            self.stream_callback({
                '__round__': round_num,
                'label': label
            })

    def _log_message(
        self,
        debate_session: Dict,
        speaker: str,
        message: str,
        timestamp: datetime,
        **kwargs
    ):
        """Log message to conversation log and print to console"""

        log_entry = {
            'timestamp': timestamp.isoformat(),
            'speaker': speaker,
            'message': message,
            **kwargs
        }

        debate_session['conversation_log'].append(log_entry)

        # Print to console so debate is visible in real time
        print(f"\n  ┌─ {speaker}")
        # Wrap long messages at 90 chars
        for line in message.split('\n'):
            line = line.strip()
            if not line:
                continue
            while len(line) > 90:
                print(f"  │  {line[:90]}")
                line = line[90:]
            print(f"  │  {line}")
        print(f"  └─────────────────────────────")

        # Stream to UI if callback registered
        if self.stream_callback:
            self.stream_callback({
                'speaker': speaker,
                'message': message,
                'timestamp': timestamp.isoformat()
            })

