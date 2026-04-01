# MAGS UI - Multi-Agent Chiller Optimization System

Professional React interface for the Multi-Agent Chiller Optimization System.

## Features

- **Agent Ring Visualization** - Central orchestrator with 6 surrounding agents
- **Active Agent Highlighting** - Visual feedback when agents are responding
- **Conversation Log** - Complete system conversation history
- **Per-Agent Response Log** - Individual agent activity tracking
- **Human Interface** - Query input with normal/nudge modes
- **Debate Viewer** - Round-by-round debate breakdown
- **System Statistics** - Real-time MAGS performance metrics
- **Professional Dark Theme** - Modern, polished design

## Installation
```bash
cd mags-ui
npm install
```

## Development
```bash
npm start
```

Runs on http://localhost:3000

## Build for Production
```bash
npm run build
```

## Technology Stack

- **React 18** - UI framework
- **Framer Motion** - Smooth animations
- **Lucide React** - Icon library
- **Recharts** - Data visualization
- **Date-fns** - Time formatting

## Architecture
```
src/
├── components/
│   ├── AgentRing.jsx           # Central agent ring with connections
│   ├── ConversationLog.jsx     # System conversation display
│   ├── AgentResponseLog.jsx    # Per-agent response viewer
│   ├── HumanInterface.jsx      # Human input interface
│   ├── SystemStats.jsx         # Performance statistics
│   └── DebateViewer.jsx        # Round-by-round debate viewer
├── App.jsx                     # Main application
├── App.css                     # Styling
└── index.js                    # Entry point
```

## Key Components

### AgentRing
- Orchestrator in center
- 6 agents positioned in circle
- SVG connection lines
- Active agent border lighting
- Hover effects and animations

### ConversationLog
- Scrollable message feed
- Message type indicators (human, agent, system, vote, decision)
- Auto-scroll to latest
- Timestamp display

### AgentResponseLog
- Filters messages by selected agent
- Shows agent-specific statistics
- Individual response timeline

### HumanInterface
- Normal/Nudge mode toggle
- Quick action chips
- Nudge suggestion chips
- Text input with send button

### DebateViewer
- 4 round navigation
- Round-specific content display
- Proposals, responses, votes visualization

### SystemStats
- Decisions today
- Average confidence
- Energy saved (kW & cost)
- Current PUE

## Next Steps

### Backend Integration
Create FastAPI server to connect Python orchestrator to React UI:
```python
# backend/api.py
from fastapi import FastAPI, WebSocket
from orchestrator import Orchestrator

app = FastAPI()
orchestrator = Orchestrator()

@app.post("/analyze")
async def analyze(context: dict, human_input: str):
    return orchestrator.analyze_and_propose(context, human_input)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Real-time agent updates
    pass
```

### WebSocket Support
- Real-time agent activity streaming
- Live debate progress updates
- System metrics push notifications

## License

Proprietary - STT GDC Singapore