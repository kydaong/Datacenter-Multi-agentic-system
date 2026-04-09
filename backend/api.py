"""
FastAPI Backend for MAGS UI
Real-time integration with Python Orchestrator
"""

from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, Dict, List
import sys
import os
import json
import queue
import threading
import asyncio
from datetime import datetime, date
from decimal import Decimal

# Add parent directory to path to import orchestrator
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env before anything else so ANTHROPIC_API_KEY and DB creds are available
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

from orchestrator.orchestrator import Orchestrator

app = FastAPI(title="MAGS API")

# CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize orchestrator
orchestrator = Orchestrator()

# Active WebSocket connections
active_connections: List[WebSocket] = []


class AnalyzeRequest(BaseModel):
    context: Dict
    human_input: Optional[str] = None
    prior_summary: Optional[str] = None  # previous debate summary for follow-up questions


class FetchDocsRequest(BaseModel):
    query: str


class NudgeRequest(BaseModel):
    session_id: str
    nudge_feedback: str
    original_context: Dict


class ApprovalRequest(BaseModel):
    session_id: str
    approved: bool
    approval_notes: Optional[str] = None


@app.get("/")
async def root():
    return {"message": "MAGS Backend API", "status": "online"}


@app.get("/status")
async def get_status():
    """Get system status"""
    try:
        return orchestrator.get_system_status()
    except Exception as e:
        return {"status": "OPERATIONAL", "agents_online": 6, "error": str(e)}


@app.post("/analyze")
async def analyze(request: AnalyzeRequest):
    """
    Main endpoint: Analyze situation and run debate
    
    Returns complete debate with all rounds and agent conversations
    """
    try:
        # Run orchestrator analysis
        decision = orchestrator.analyze_and_propose(
            context=request.context,
            human_input=request.human_input
        )
        
        return {
            "status": "success",
            "decision": decision
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/nudge")
async def nudge(request: NudgeRequest):
    """Handle human nudge for alternative solutions"""
    try:
        decision = orchestrator.handle_human_nudge(
            session_id=request.session_id,
            nudge_feedback=request.nudge_feedback,
            original_context=request.original_context
        )
        
        return {
            "status": "success",
            "decision": decision
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/approve")
async def approve(request: ApprovalRequest):
    """Handle human approval/rejection"""
    try:
        result = orchestrator.handle_human_approval(
            session_id=request.session_id,
            approved=request.approved,
            approval_notes=request.approval_notes
        )
        
        return {
            "status": "success",
            "result": result
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debate/{session_id}")
async def get_debate(session_id: str):
    """Get complete debate details"""
    try:
        debate = orchestrator.get_debate_details(session_id)
        
        return {
            "status": "success",
            "debate": debate
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket for real-time agent updates
    
    Streams agent activity, debate progress, and decisions in real-time
    """
    await websocket.accept()
    active_connections.append(websocket)
    
    try:
        while True:
            # Keep connection alive and listen for messages
            data = await websocket.receive_text()
            
            # Echo back for now (can be enhanced for real-time streaming)
            await websocket.send_json({
                "type": "ping",
                "message": "Connection active"
            })
    
    except Exception as e:
        if websocket in active_connections:
            active_connections.remove(websocket)


async def broadcast_agent_activity(agent_name: str, message: str):
    """Broadcast agent activity to all connected WebSocket clients"""
    for connection in active_connections:
        try:
            await connection.send_json({
                "type": "agent_activity",
                "agent": agent_name,
                "message": message,
                "timestamp": str(datetime.now())
            })
        except:
            active_connections.remove(connection)


@app.post("/fetch-docs")
async def fetch_docs(request: FetchDocsRequest):
    """
    Direct knowledge-base lookup — bypasses the agent debate entirely.
    Queries all Qdrant collections and returns cited document excerpts.
    """
    try:
        result = orchestrator.fetch_documents(request.query)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze/stream")
async def analyze_stream(request: AnalyzeRequest):
    """
    Streaming SSE endpoint: streams debate messages as they are generated.
    If the orchestrator classifies the query as a document lookup it emits a
    single __docs__ event instead of starting the 4-round debate.
    """
    loop = asyncio.get_event_loop()
    async_queue: asyncio.Queue = asyncio.Queue()

    # ── Intent classification: doc fetch or debate? ──────────────────────
    intent = orchestrator.classify_query_intent(request.human_input)

    if intent == 'FETCH_DOCS':
        def run_fetch():
            try:
                docs = orchestrator.fetch_documents(request.human_input)
                loop.call_soon_threadsafe(
                    async_queue.put_nowait,
                    {'__docs__': True, 'data': docs}
                )
            except Exception as e:
                loop.call_soon_threadsafe(async_queue.put_nowait, {'__error__': str(e)})

        threading.Thread(target=run_fetch, daemon=True).start()
    else:
        def stream_callback(event):
            loop.call_soon_threadsafe(async_queue.put_nowait, event)

        orchestrator.debate_manager.stream_callback = stream_callback

        def run_debate():
            try:
                result = orchestrator.analyze_and_propose(
                    context=request.context,
                    human_input=request.human_input,
                    prior_summary=request.prior_summary
                )
                loop.call_soon_threadsafe(async_queue.put_nowait, {'__done__': True, 'decision': result})
            except Exception as e:
                loop.call_soon_threadsafe(async_queue.put_nowait, {'__error__': str(e)})
            finally:
                orchestrator.debate_manager.stream_callback = None

        threading.Thread(target=run_debate, daemon=True).start()

    def _serialise(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")

    async def event_generator():
        while True:
            event = await async_queue.get()
            yield f"data: {json.dumps(event, default=_serialise)}\n\n"
            if '__done__' in event or '__error__' in event or '__docs__' in event:
                break

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)