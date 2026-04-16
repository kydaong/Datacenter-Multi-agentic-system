"""
Debate Manager
Manages 2-round debate protocol between agents
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
    Manages structured 2-round debate protocol with natural conversations

    Round 1: Initial proposals — each agent states their position and reasoning
    Round 2: Debate + vote — agents critique/rebut peers, then cast their final vote

    Uses Claude API for natural language generation
    """
    
    # Plant config injected into every Claude prompt so all agents reason correctly
    PLANT_CONFIG = """
CHILLER PLANT CONFIGURATION (authoritative — use these values, do not guess):
  Chiller-1: 1000 tons rated capacity
  Chiller-2: 1000 tons rated capacity
  Chiller-3:  500 tons rated capacity
  Total plant: 2500 tons

N+1 REDUNDANCY RULE (correct formula):
  If the largest running chiller trips, remaining online units must still cover full load.
  Check: (online_capacity_tons - largest_online_unit_tons) >= cooling_load_tons
  Examples:
    2 chillers [C1+C2=2000T], load 800T  → remaining=1000T >= 800T  ✓ N+1 MAINTAINED
    3 chillers [all=2500T],   load 1200T → remaining=1500T >= 1200T ✓ N+1 MAINTAINED
    1 chiller  [C1=1000T],    load 800T  → remaining=0T    < 800T   ✗ N+1 VIOLATED
    2 chillers [C1+C2=2000T], load 1100T → remaining=1000T < 1100T  ✗ N+1 VIOLATED

UNITS RULE (mandatory — no exceptions):
  All temperatures must be expressed in degrees Celsius (°C) only.
  Never use Fahrenheit (°F) in any response or recommendation.
"""

    def __init__(self, agents: List, stream_callback=None):
        """
        Initialize debate manager

        Args:
            agents: List of all agent instances
            stream_callback: Optional callable(event_dict) for real-time streaming
        """
        self.agents = agents
        self.max_rounds = 2
        self.stream_callback = stream_callback

        # Initialize Claude client for conversational responses
        self.claude_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.claude_model = "claude-sonnet-4-6"
    
    def run_debate(
        self,
        context: Dict,
        human_input: Optional[str] = None,
        prior_context: Optional[str] = None
    ) -> Dict:
        """
        Run complete 4-round debate with natural conversations

        Args:
            context: System state and telemetry
            human_input: Optional human query/guidance
            prior_context: Summary of the previous debate (for follow-up questions)

        Returns:
            Complete debate result with conversation log
        """

        # Pull recent memory context to ground the debate
        memory_context = self._fetch_memory_context()

        question_type = 'ADVISORY' if self._is_advisory_question(human_input) else 'OPERATIONAL'
        print(f"\n  Question type detected: {question_type}")

        debate_session = {
            'start_time': datetime.now().isoformat(),
            'context': context,
            'human_input': human_input,
            'memory_context': memory_context,
            'prior_context': prior_context,
            'question_type': question_type,
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
        print("\n[ROUND 1/2] Collecting initial proposals...")
        self._emit_round_marker(1, "Initial Proposals")
        round_1_result = self._run_round_1(context, debate_session)
        debate_session['rounds'].append(round_1_result)

        # Round 2: Debate, rebuttals, and final vote combined
        print("\n[ROUND 2/2] Agent debate and final vote...")
        self._emit_round_marker(2, "Debate & Vote")
        round_2_result = self._run_round_2_debate_and_vote(round_1_result, context, debate_session)
        debate_session['rounds'].append(round_2_result)
        
        debate_session['end_time'] = datetime.now().isoformat()
        
        return debate_session
    
    def _run_round_1(self, context: Dict, debate_session: Dict) -> Dict:
        """
        Round 1: Each agent proposes independently, grounded in the human question.

        Analytical proposal provides structured data; Claude generates the
        conversational message that directly addresses the human question.
        """

        round_context = dict(context)
        human_input = debate_session.get('human_input', '')
        if human_input:
            round_context['human_question'] = human_input

        proposals = []

        for agent in self.agents:
            print(f"  → {agent.agent_name}...")

            try:
                proposal = agent.propose_action(round_context)
                proposals.append(proposal)

                        # Generate a Claude-powered message that addresses the human question
                spoken_message = self._generate_round1_llm_message(
                    agent, proposal, human_input, round_context,
                    memory_context=debate_session.get('memory_context', ''),
                    prior_context=debate_session.get('prior_context', '')
                )

                self._log_message(
                    debate_session,
                    speaker=agent.agent_name,
                    message=spoken_message,
                    timestamp=datetime.now(),
                    proposal=proposal
                )

                print(f"     Proposal: {proposal.get('action_type', 'UNKNOWN')}")

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

    def _generate_round1_llm_message(
        self,
        agent,
        proposal: Dict,
        human_question: str,
        context: Dict,
        memory_context: str = '',
        prior_context: str = ''
    ) -> str:
        """
        Use Claude to generate a Round 1 proposal message that directly addresses
        the human question, grounded in the analytical proposal from the agent.
        """

        action_type = proposal.get('action_type', 'MONITORING_UPDATE')
        description = proposal.get('description', '')
        justification = proposal.get('justification', '')
        savings = proposal.get('predicted_savings', {})

        # Build a compact context summary for Claude
        ctx_summary = (
            f"Cooling load: {context.get('cooling_load_kw', 'N/A')} kW | "
            f"IT load: {context.get('it_load_kw', 'N/A')} kW | "
            f"Chillers online: {context.get('chillers_online', 'N/A')} | "
            f"PUE: {context.get('current_pue', 'N/A')}"
        )

        savings_summary = ""
        if savings:
            savings_summary = (
                f"Predicted savings — Energy: {savings.get('energy_kw', 0):.1f} kW, "
                f"Cost: SGD {savings.get('cost_sgd', 0):.2f}, "
                f"PUE improvement: {savings.get('pue_improvement', 0):.4f}"
            )

        # Surface Qdrant evidence (SOPs, historical, regulations) from the proposal
        evidence_summary = ""
        evidence_list = proposal.get('evidence', [])
        if evidence_list:
            evidence_lines = []
            for ev in evidence_list:
                ev_type = ev.get('type', '')
                ev_source = ev.get('source', '')
                ev_data = ev.get('data', '')
                if ev_type in ('SOP', 'HISTORICAL', 'REGULATION', 'MANUFACTURER', 'ANALYTICAL'):
                    if isinstance(ev_data, list) and ev_data:
                        snippet = str(ev_data[0])[:200]
                    else:
                        snippet = str(ev_data)[:200]
                    evidence_lines.append(f"[{ev_type} | {ev_source}] {snippet}")
            if evidence_lines:
                evidence_summary = "SUPPORTING EVIDENCE FROM KNOWLEDGE BASE & HISTORY:\n" + "\n".join(evidence_lines)

        human_line = f"\nHUMAN QUESTION: {human_question}\n" if human_question else ""
        memory_line = f"\n{memory_context}\n" if memory_context else ""
        prior_line = (
            f"\nPREVIOUS DEBATE OUTCOME (this is a FOLLOW-UP question — revise/build on this):\n{prior_context}\n"
            if prior_context else ""
        )

        prompt = f"""You are {agent.agent_name} opening a 4-round expert debate about a chiller plant operation question.
{human_line}
{prior_line}
{self.PLANT_CONFIG}
LIVE SYSTEM STATE:
{ctx_summary}
{memory_line}
YOUR ANALYTICAL ASSESSMENT:
- Action type: {action_type}
- Description: {description}
- Justification: {justification}
{savings_summary}

{evidence_summary}

YOUR TASK:
Give your opening position as a domain expert DIRECTLY answering the human question above.
- Your response MUST address what the human asked — do NOT pivot to unrelated optimizations
- If no human question: comment on the most significant issue visible in the live data
- Ground your answer in the live system numbers, memory context, and evidence — do NOT invent values
- If recent decisions show a similar action was taken recently, factor in cooldown constraints
- Be specific to YOUR domain expertise (not generic)
- Reference actual values (kW, temps, COP, run hours, PUE, etc.) from the live state
- You may mention a relevant secondary observation from your domain, but keep the primary answer focused on the question
- 2-4 sentences, conversational but authoritative

Respond now as {agent.agent_name}:"""

        try:
            message = self.claude_client.messages.create(
                model=self.claude_model,
                max_tokens=400,
                system=agent.system_prompt,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            print(f"     Warning: LLM error in Round 1 message: {e}")
            return self._format_proposal_message(proposal)
    
    def _run_round_2_debate_and_vote(
        self,
        round_1: Dict,
        context: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Round 2: Combined debate + final vote.

        Each agent:
          1. Critiques/supports other proposals with reasoning
          2. Casts their final APPROVE / APPROVE_WITH_CONDITIONS / REJECT / VETO vote

        Single LLM call per agent — keeps the debate tight and fast.
        """

        # Advisory/planning questions skip voting — use collaborative strategy round instead
        if debate_session.get('question_type') == 'ADVISORY':
            return self._run_round_2_advisory(round_1, context, debate_session)

        round_1_proposals = round_1['proposals']
        debate_proposals = [
            p for p in round_1_proposals
            if p.get('action_type') not in ['MONITORING_UPDATE', 'ERROR']
        ]

        human_question = debate_session.get('human_input', '')
        primary_proposal = self._identify_primary_proposal(round_1_proposals, human_question)
        print(f"\n  Primary proposal: {primary_proposal.get('action_type')} by {primary_proposal.get('agent')}")

        responses = []
        votes = []

        if not debate_proposals:
            for agent in self.agents:
                print(f"  → {agent.agent_name} acknowledging + voting...")
                ack_text = self._generate_acknowledgment(agent, round_1_proposals)
                vote_entry = {
                    'agent': agent.agent_name,
                    'vote': 'APPROVE',
                    'reasoning': ack_text,
                    'reasoning_text': ack_text,
                    'confidence': 0.80,
                    'timestamp': datetime.now().isoformat()
                }
                responses.append({'agent': agent.agent_name, 'response_text': ack_text})
                votes.append(vote_entry)
                self._log_message(
                    debate_session,
                    speaker=agent.agent_name,
                    message=f"[VOTE: APPROVE] {ack_text}",
                    timestamp=datetime.now()
                )
        else:
            for agent in self.agents:
                print(f"  → {agent.agent_name} debating + voting...")
                result = self._generate_debate_and_vote(
                    agent, debate_proposals, round_1_proposals, primary_proposal, context, debate_session
                )
                responses.append({'agent': agent.agent_name, 'response_text': result['debate_text']})
                votes.append(result['vote'])
                self._log_message(
                    debate_session,
                    speaker=agent.agent_name,
                    message=f"[VOTE: {result['vote']['vote']}] {result['debate_text']}",
                    timestamp=datetime.now()
                )
                print(f"     Vote: {result['vote']['vote']}")

        return {
            'round': 2,
            'phase': 'DEBATE_AND_VOTE',
            'primary_proposal': primary_proposal,
            'responses': responses,
            'votes': votes,
            'timestamp': datetime.now().isoformat()
        }

    def _generate_debate_and_vote(
        self,
        agent,
        debate_proposals: List[Dict],
        all_proposals: List[Dict],
        primary_proposal: Dict,
        context: Dict,
        debate_session: Dict
    ) -> Dict:
        """
        Single LLM call: agent critiques peers and casts their final vote.
        """
        conversation_history = self._build_conversation_context(debate_session)
        human_question = debate_session.get('human_input', '')
        human_line = f"\nHUMAN QUESTION: {human_question}\n" if human_question else ""

        proposals_text = "\n\n".join([
            f"**{p.get('agent')}** proposes: {p.get('action_type')}\n"
            f"Description: {p.get('description', '')}\n"
            f"Justification: {p.get('justification', '')}"
            for p in debate_proposals
        ])

        question_anchor = (
            f"THE HUMAN'S QUESTION IS: \"{human_question}\"\n"
            f"Your debate and vote MUST directly answer this question. "
            f"Do not drift to unrelated optimizations.\n"
            if human_question else ""
        )

        prompt = f"""You are in Round 2 (final round) of a rapid expert debate about a chiller plant decision.
{self.PLANT_CONFIG}
{question_anchor}
CONVERSATION SO FAR (Round 1 proposals):
{conversation_history}

ALL PROPOSALS ON THE TABLE:
{proposals_text}

PRIMARY PROPOSAL SELECTED AS MOST RELEVANT TO THE HUMAN'S QUESTION:
Agent: {primary_proposal.get('agent')}
Action: {primary_proposal.get('action_type')}
Description: {primary_proposal.get('description')}

YOUR TASK — complete BOTH parts:

PART 1 — DEBATE (2-3 sentences):
Respond directly to the human's question from your domain expertise. \
Comment on the primary proposal and any other proposals where relevant. \
Reference specific agents by name. Be direct — do NOT introduce unrelated optimizations.

PART 2 — VOTE (required):
Your vote answers: "Should this proposal be the team's recommended response to the human's question?"
VOTE: [APPROVE | APPROVE_WITH_CONDITIONS | REJECT | VETO]
REASONING: [1-2 sentences tied to the human's question and live system data]

Vote meanings:
  APPROVE — this proposal directly and safely answers the human's question
  APPROVE_WITH_CONDITIONS — answers the question but requires specific preconditions first
  REJECT — does NOT answer the question or is unsafe/inadvisable
  VETO — critical safety / N+1 / compliance violation (domain authority only)

CONSISTENCY RULE: if your debate says the action is unsafe → vote REJECT or VETO, not APPROVE.

Respond now as {agent.agent_name}:"""

        try:
            message = self.claude_client.messages.create(
                model=self.claude_model,
                max_tokens=600,
                system=agent.system_prompt,
                messages=[{"role": "user", "content": prompt}]
            )
            full_text = message.content[0].text

            # Split debate text from vote block
            vote_split = full_text.upper().find('VOTE:')
            if vote_split != -1:
                debate_text = full_text[:vote_split].strip()
                vote_block = full_text[vote_split:]
            else:
                debate_text = full_text.strip()
                vote_block = full_text

            vote_value = self._parse_vote_from_text(vote_block, agent, primary_proposal)

            vote_entry = {
                'agent': agent.agent_name,
                'vote': vote_value,
                'reasoning': full_text,
                'reasoning_text': full_text,
                'confidence': 0.85,
                'timestamp': datetime.now().isoformat()
            }
            return {'debate_text': full_text, 'vote': vote_entry}

        except Exception as e:
            print(f"     Warning: LLM error in Round 2: {e}")
            return {
                'debate_text': "I support the primary proposal based on current system conditions.",
                'vote': {
                    'agent': agent.agent_name,
                    'vote': 'APPROVE',
                    'reasoning': 'Fallback approval.',
                    'reasoning_text': 'Fallback approval.',
                    'confidence': 0.70,
                    'timestamp': datetime.now().isoformat()
                }
            }
    
    # ── Advisory / planning question mode ─────────────────────────────────

    def _is_advisory_question(self, human_input: str) -> bool:
        """Detect strategic/planning questions that don't need a vote."""
        import re
        if not human_input:
            return False
        text = human_input.lower()
        patterns = [
            r'\bhow\s+can\s+(i|we|the\s+\w+)\b',
            r'\bhow\s+(do|should|would|could)\s+(i|we|the\s+\w+)\b',
            r'\bwhat\s+(steps?|strateg\w*|actions?|measures?|approach\w*|ways?|options?)\b',
            r'\bwhat\s+can\s+(i|we)\b',
            r'\bhow\s+to\s+(achieve|improve|reduce|increase|optimis?e|reach|get\s+to)\b',
            r'\b(this\s+year|this\s+quarter|long[- ]term|over\s+the\s+next|roadmap|plan\s+for)\b',
            r'\b(suggestions?\s+for|recommendations?\s+for)\b',
            r'\bpath\s+forward\b',
            r'\bstrateg(y|ies)\b',
        ]
        return any(re.search(p, text) for p in patterns)

    def _run_round_2_advisory(self, round_1: Dict, context: Dict, debate_session: Dict) -> Dict:
        """
        Round 2 for advisory questions — agents contribute domain strategies, no voting.
        Ends with a synthesized path forward.
        """
        self._emit_round_marker(2, "Strategic Contributions")
        print("  Advisory mode — collecting strategic contributions...")

        responses = []
        for agent in self.agents:
            print(f"  → {agent.agent_name} contributing strategy...")
            contribution = self._generate_advisory_contribution(agent, round_1, context, debate_session)
            responses.append({'agent': agent.agent_name, 'response_text': contribution})
            self._log_message(debate_session, speaker=agent.agent_name,
                              message=contribution, timestamp=datetime.now())

        print("  → Synthesizing path forward...")
        synthesized = self._synthesize_advisory_path(
            debate_session.get('human_input', ''), responses, context, debate_session
        )

        return {
            'round': 2,
            'phase': 'ADVISORY_PATH_FORWARD',
            'primary_proposal': {},
            'responses': responses,
            'votes': [],
            'synthesized_path': synthesized,
            'timestamp': datetime.now().isoformat()
        }

    def _generate_advisory_contribution(self, agent, round_1: Dict, context: Dict, debate_session: Dict) -> str:
        """Each agent contributes domain-specific strategies — no vote prompt."""
        human_question = debate_session.get('human_input', '')
        memory_context = debate_session.get('memory_context', '')
        conversation_history = self._build_conversation_context(debate_session)
        ctx_summary = (
            f"Cooling load: {context.get('cooling_load_kw','N/A')} kW | "
            f"IT load: {context.get('it_load_kw','N/A')} kW | "
            f"PUE: {context.get('current_pue','N/A')} | "
            f"Chillers online: {context.get('chillers_online','N/A')}"
        )
        memory_line = f"\nRECENT CONTEXT:\n{memory_context}\n" if memory_context else ""

        prompt = f"""You are {agent.agent_name} in a strategic planning session.

HUMAN QUESTION: {human_question}

LIVE SYSTEM STATE:
{ctx_summary}
{memory_line}
{self.PLANT_CONFIG}
ROUND 1 — COLLEAGUES' PROPOSALS:
{conversation_history}

YOUR TASK:
Contribute 2-3 specific strategies from your domain ({agent.agent_role}) that help answer the \
human's question. Build on colleagues' ideas — don't repeat what they covered. Use actual numbers \
where possible. Do NOT vote or approve/reject — this is a planning discussion.

2-4 sentences. Respond as {agent.agent_name}:"""

        try:
            message = self.claude_client.messages.create(
                model=self.claude_model, max_tokens=400,
                system=agent.system_prompt,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            print(f"     Warning: advisory contribution error: {e}")
            return f"From a {agent.agent_role} perspective, targeted optimisation of current operating parameters can contribute meaningfully toward the goal."

    def _synthesize_advisory_path(self, human_question: str, responses: List[Dict],
                                   context: Dict, debate_session: Dict) -> str:
        """Synthesize all agent contributions into a structured path forward."""
        contributions = "\n\n".join([f"{r['agent']}:\n{r['response_text']}" for r in responses])
        memory_line = f"\nRECENT CONTEXT:\n{debate_session.get('memory_context','')}\n" \
            if debate_session.get('memory_context') else ""

        prompt = f"""You are the Orchestrator synthesising a multi-agent planning session.

HUMAN QUESTION: {human_question}

AGENT CONTRIBUTIONS:
{contributions}
{memory_line}
Write a concise answer (4-6 bullet points) that:
1. Opens with the single highest-impact action and its quantified benefit
2. Lists the next 2-4 most valuable steps in priority order, each with expected outcome
3. Closes with the realistic overall result if the steps are followed

Rules: only use numbers from the agent contributions — never invent figures. \
No boilerplate headers. Every bullet must add value. Plain English."""

        try:
            message = self.claude_client.messages.create(
                model=self.claude_model, max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        except Exception as e:
            print(f"     Warning: advisory synthesis error: {e}")
            return "\n".join([r['response_text'] for r in responses])

    # ── End advisory mode ──────────────────────────────────────────────────

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

        human_question = debate_session.get('human_input', '')

        if not original_proposal or original_proposal.get('action_type') == 'MONITORING_UPDATE':
            if human_question:
                return {
                    'agent': agent.agent_name,
                    'position_changed': False,
                    'response_text': (
                        f"Regarding the question about '{human_question}': from my {agent.agent_role} "
                        f"perspective, current parameters are nominal and no intervention is warranted at this time."
                    )
                }
            return {
                'agent': agent.agent_name,
                'position_changed': False,
                'response_text': "I maintain my position. The system is operating within normal parameters."
            }

        # Build conversation context
        conversation_history = self._build_conversation_context(debate_session)

        # Create refinement prompt
        refinement_prompt = self._create_refinement_prompt(
            agent,
            original_proposal,
            feedback_received,
            conversation_history,
            human_question=human_question
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
{self.PLANT_CONFIG}
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
        conversation_history: str,
        human_question: str = ''
    ) -> str:
        """Create prompt for Round 3 position refinement"""

        feedback_text = "\n".join([f"- {fb}" for fb in feedback_received]) if feedback_received else "No specific feedback received"
        human_line = f"\nHUMAN QUESTION WE ARE ANSWERING: {human_question}\n" if human_question else ""

        prompt = f"""You are participating in Round 3 of a collaborative optimization meeting. Based on the discussion so far, refine your position.
{self.PLANT_CONFIG}
{human_line}
CONVERSATION SO FAR:
{conversation_history}

YOUR ORIGINAL PROPOSAL:
{original_proposal.get('action_type')}: {original_proposal.get('description')}

FEEDBACK RECEIVED:
{feedback_text}

YOUR TASK:
Keep the human question in mind — your refined position should move toward a concrete answer to it.

1. If feedback raises valid concerns, acknowledge and adjust
2. If your position is sound, reaffirm it with additional reasoning tied to the human question
3. Suggest how to combine approaches where applicable

Use phrases like "After hearing everyone's input...", "I see [Agent]'s point...", "I'd refine my recommendation to..."

Keep it focused (2-3 paragraphs). Respond now as {agent.agent_name}:"""

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
{self.PLANT_CONFIG}
{human_context_line}
CONVERSATION SO FAR:
{conversation_history}

PRIMARY PROPOSAL:
{primary_proposal.get('action_type')} by {primary_proposal.get('agent')}
Description: {primary_proposal.get('description')}

YOUR TASK:
Cast your final vote. Choose EXACTLY ONE of the following — your vote MUST be logically consistent with your reasoning:

  APPROVE              — The proposal is safe and beneficial to execute RIGHT NOW.
  APPROVE_WITH_CONDITIONS — The proposal is safe in principle BUT requires specific,
                         measurable preconditions to be satisfied BEFORE execution.
                         Use this ONLY when those conditions are realistic and clearly
                         stated. Do NOT use this if the action itself violates safety
                         or N+1 under current or foreseeable conditions.
  REJECT               — The proposal should NOT be executed. Use this when the action
                         is unsafe, not beneficial, or the risks outweigh the gains.
                         If your reasoning says "not safe" → vote REJECT, not APPROVE_WITH_CONDITIONS.
  VETO                 — Critical safety, N+1, or compliance violation. Overrides all
                         other votes. Use only for your domain authority.

CONSISTENCY RULE: If your reasoning concludes the action is unsafe or inadvisable, your
vote MUST be REJECT or VETO — never APPROVE or APPROVE_WITH_CONDITIONS.

Format your response as:
VOTE: [APPROVE | APPROVE_WITH_CONDITIONS | REJECT | VETO]
REASONING: [2-3 sentences. State the key facts from live data that drove your decision.]

Cast your vote now as {agent.agent_name}:"""
        
        return prompt
    
    def _parse_vote_from_text(self, response_text: str, agent, primary_proposal: Dict) -> str:
        """
        Parse vote from response text.
        Applies a consistency check: if reasoning contains strong unsafe signals,
        override a permissive explicit vote (APPROVE / APPROVE_WITH_CONDITIONS) to REJECT.
        """
        text_upper = response_text.upper()

        # Detect strong "unsafe" signals in the reasoning text
        unsafe_signals = [
            'NOT SAFE', 'IS NOT SAFE', 'UNSAFE', 'N+1 VIOLATION', 'N+1 VIOLATED',
            'CANNOT BE EXECUTED', 'SHOULD NOT BE EXECUTED', 'DO NOT EXECUTE',
            'INADVISABLE', 'TOO RISKY', 'CRITICAL RISK'
        ]
        reasoning_says_unsafe = any(sig in text_upper for sig in unsafe_signals)

        # Parse the explicit VOTE: line
        parsed_vote = None
        if 'VOTE:' in text_upper:
            try:
                vote_line = next(l for l in response_text.split('\n') if 'VOTE:' in l.upper())
                if 'APPROVE_WITH_CONDITIONS' in vote_line.upper() or 'APPROVE WITH CONDITIONS' in vote_line.upper():
                    parsed_vote = 'APPROVE_WITH_CONDITIONS'
                elif 'VETO' in vote_line.upper():
                    parsed_vote = 'VETO'
                elif 'REJECT' in vote_line.upper():
                    parsed_vote = 'REJECT'
                elif 'APPROVE' in vote_line.upper():
                    parsed_vote = 'APPROVE'
            except StopIteration:
                pass

        # Consistency check: if reasoning says unsafe but vote is permissive → REJECT
        if reasoning_says_unsafe and parsed_vote in ('APPROVE', 'APPROVE_WITH_CONDITIONS'):
            print(f"     ⚠ Vote consistency override: reasoning says unsafe but vote was {parsed_vote} → REJECT")
            return 'REJECT'

        if parsed_vote:
            return parsed_vote

        # Fallback: infer from full text sentiment
        if 'veto' in text_upper or 'cannot approve' in text_upper:
            return 'VETO' if agent.agent_name in [
                'Operations & Safety Agent', 'Maintenance & Compliance Agent'
            ] else 'REJECT'
        elif 'reject' in text_upper or 'oppose' in text_upper or reasoning_says_unsafe:
            return 'REJECT'
        elif 'condition' in text_upper or 'provided that' in text_upper:
            return 'APPROVE_WITH_CONDITIONS'
        elif 'approve' in text_upper or 'support' in text_upper or 'agree' in text_upper:
            return 'APPROVE'

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
        """Build conversation history for context, prepending prior debate if this is a follow-up."""

        prior_context = debate_session.get('prior_context', '')
        prior_block = (
            f"=== PRIOR DEBATE OUTCOME (follow-up question — revise/build on this) ===\n{prior_context}\n"
            f"=== NOW ADDRESSING NEW QUESTION ===\n\n"
            if prior_context else ""
        )

        conversation_log = debate_session.get('conversation_log', [])

        # Get last 10 messages for context
        recent_messages = conversation_log[-10:] if len(conversation_log) > 10 else conversation_log

        context_lines = []
        for msg in recent_messages:
            speaker = msg.get('speaker', 'Unknown')
            message = msg.get('message', '')
            context_lines.append(f"{speaker}: {message}")

        return prior_block + "\n\n".join(context_lines)
    
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
    
    def _identify_primary_proposal(self, proposals: List[Dict], human_question: str = '') -> Dict:
        """
        Identify the primary proposal most relevant to the human question.

        If a human question is provided, uses LLM to score proposals and pick
        the one that best answers what was asked.
        Falls back to the first actionable proposal if no question or LLM fails.
        """
        action_proposals = [
            p for p in proposals
            if p.get('action_type') not in ['MONITORING_UPDATE', 'ERROR']
        ]

        if not action_proposals:
            return proposals[0] if proposals else {}

        if len(action_proposals) == 1 or not human_question:
            return action_proposals[0]

        # Use LLM to pick the proposal most relevant to the human question
        try:
            summaries = "\n".join([
                f"{i+1}. [{p.get('agent')}] {p.get('action_type')}: {p.get('description', '')}"
                for i, p in enumerate(action_proposals)
            ])
            prompt = (
                f"A human asked: \"{human_question}\"\n\n"
                f"The following proposals were made by agents:\n{summaries}\n\n"
                f"Which proposal number (1-{len(action_proposals)}) best addresses "
                f"the human's question? Reply with only the number."
            )
            response = self.claude_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=5,
                messages=[{"role": "user", "content": prompt}]
            )
            idx = int(response.content[0].text.strip()) - 1
            if 0 <= idx < len(action_proposals):
                chosen = action_proposals[idx]
                print(f"  [PRIMARY] '{chosen.get('action_type')}' by {chosen.get('agent')} (query-matched)")
                return chosen
        except Exception as e:
            print(f"  [PRIMARY] LLM selection failed ({e}), using first proposal")

        return action_proposals[0]
    
    def _format_proposal_message(self, proposal: Dict) -> str:
        """Format proposal as human-readable message"""
        
        action_type = proposal.get('action_type', 'UNKNOWN')
        description = proposal.get('description', 'No description')
        
        return f"[PROPOSAL] {action_type}: {description}"
    
    def _fetch_memory_context(self) -> str:
        """
        Pull recent decisions and proposals from short-term memory and
        recent patterns from long-term memory to ground the debate.
        Returns a formatted string injected into agent prompts.
        """
        lines = []

        # Short-term: recent decisions (last 24h)
        try:
            from orchestrator.short_term_memory import short_term_memory
        except ImportError:
            try:
                from short_term_memory import short_term_memory
            except ImportError:
                short_term_memory = None

        if short_term_memory:
            try:
                recent_decisions = short_term_memory.get_recent_decisions(hours=24)
                if recent_decisions:
                    lines.append("RECENT DECISIONS (last 24h):")
                    for d in recent_decisions[:5]:  # cap at 5
                        dt = d.get('decision_time', '')
                        dtype = d.get('decision_type', 'UNKNOWN')
                        executed = 'executed' if d.get('executed') else 'pending'
                        lines.append(f"  • [{dt}] {dtype} — {executed}")

                recent_proposals = short_term_memory.get_recent_proposals(hours=6)
                if recent_proposals:
                    lines.append("RECENT AGENT PROPOSALS (last 6h):")
                    for p in recent_proposals[:5]:
                        agent = p.get('agent_name', '')
                        atype = p.get('action_type', '')
                        pt = p.get('proposal_time', '')
                        lines.append(f"  • [{pt}] {agent}: {atype}")
            except Exception as e:
                print(f"  Warning: STM read error in memory context: {e}")

        # Long-term: load learned patterns
        try:
            from orchestrator.long_term_memory import long_term_memory
        except ImportError:
            try:
                from long_term_memory import long_term_memory
            except ImportError:
                long_term_memory = None

        if long_term_memory:
            try:
                strategies = long_term_memory.get_proven_strategies(
                    min_success_rate=70.0, min_attempts=3
                )
                if strategies:
                    lines.append("PROVEN STRATEGIES (long-term memory):")
                    for s in strategies[:3]:
                        name = s.get('strategy_name', '')
                        rate = s.get('success_rate', 0)
                        savings = s.get('avg_savings_kw', 0)
                        lines.append(f"  • {name} — {rate:.0f}% success, avg {savings:.0f} kW saved")
            except Exception:
                pass  # long-term memory read is best-effort

        if lines:
            return "\n".join(lines)
        return ""

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

