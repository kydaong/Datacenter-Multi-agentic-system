"""
MLflow Tracker — Orchestrator Decision Logging & API Cost Tracking

Uses MLflow's Tracing API (start_span) so sessions appear under the
GenAI / Traces section with Token Usage and Cost Breakdown panels.
"""

import os
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# USD per 1 million tokens (update as pricing changes)
_MODEL_PRICING: Dict[str, Dict[str, float]] = {
    "claude-haiku-4-5-20251001": {"input": 0.80,  "output": 4.00},
    "claude-sonnet-4-6":         {"input": 3.00,  "output": 15.00},
    "claude-opus-4-7":           {"input": 15.00, "output": 75.00},
}


class MLflowTracker:
    def __init__(
        self,
        tracking_uri: str = "http://localhost:5000",
        experiment_name: str = "orchestrator-decisions",
    ):
        self._mlflow = None
        self._span = None
        self._span_ctx = None
        self._token_totals: Dict[str, Dict[str, int]] = {}
        self._call_counts: Dict[str, int] = {}

        try:
            import mlflow  # type: ignore
            mlflow.set_tracking_uri(tracking_uri)
            mlflow.set_experiment(experiment_name)
            self._mlflow = mlflow
            print(f"[MLflow] Enabled at {tracking_uri}", flush=True)
        except Exception as exc:
            print(f"[MLflow] INIT FAILED: {exc}", flush=True)

    # ── Public API ──────────────────────────────────────────────────────────

    def is_active(self) -> bool:
        return self._mlflow is not None and self._span is not None

    def start_session(self, session_id: str, tags: Optional[Dict[str, str]] = None) -> None:
        if self._mlflow is None:
            return
        try:
            self._token_totals = {}
            self._call_counts = {}
            self._span_ctx = self._mlflow.start_span(
                name=session_id,
                span_type="AGENT",
                attributes=tags or {},
            )
            self._span = self._span_ctx.__enter__()
            print(f"[MLflow] Trace started: {session_id}", flush=True)
        except Exception as exc:
            print(f"[MLflow] START_SPAN FAILED: {exc}", flush=True)
            self._span = None
            self._span_ctx = None

    def record_api_call(self, model: str, input_tokens: int, output_tokens: int) -> None:
        if not self.is_active():
            return
        if model not in self._token_totals:
            self._token_totals[model] = {"input": 0, "output": 0}
            self._call_counts[model] = 0
        self._token_totals[model]["input"] += input_tokens
        self._token_totals[model]["output"] += output_tokens
        self._call_counts[model] += 1

    def end_session(
        self,
        params: Optional[Dict] = None,
        metrics: Optional[Dict[str, float]] = None,
    ) -> None:
        if not self.is_active():
            return
        try:
            if params:
                self._span.set_inputs({k: str(v)[:500] for k, v in params.items()})

            total_input, total_output, total_cost = 0, 0, 0.0
            for model, tokens in self._token_totals.items():
                short = model.replace("claude-", "").replace("-", "_")
                pricing = _MODEL_PRICING.get(model, {"input": 0.0, "output": 0.0})
                cost = (
                    tokens["input"] * pricing["input"]
                    + tokens["output"] * pricing["output"]
                ) / 1_000_000
                total_input  += tokens["input"]
                total_output += tokens["output"]
                total_cost   += cost
                self._span.set_attribute(f"llm.token_count.prompt.{short}",     tokens["input"])
                self._span.set_attribute(f"llm.token_count.completion.{short}",  tokens["output"])
                self._span.set_attribute(f"api_calls.{short}",                   float(self._call_counts[model]))
                self._span.set_attribute(f"cost_usd.{short}",                    round(cost, 6))

            self._span.set_attribute("llm.token_count.prompt",      total_input)
            self._span.set_attribute("llm.token_count.completion",   total_output)
            self._span.set_attribute("llm.token_count.total",        total_input + total_output)
            self._span.set_attribute("total_cost_usd",               round(total_cost, 6))

            outputs = {k: str(v) for k, v in (metrics or {}).items()}
            outputs["total_cost_usd"] = str(round(total_cost, 6))
            self._span.set_outputs(outputs)

            self._span_ctx.__exit__(None, None, None)
            print(f"[MLflow] Trace ended — tokens: {total_input}in/{total_output}out  cost: ${total_cost:.4f}", flush=True)
        except Exception as exc:
            print(f"[MLflow] END_SPAN FAILED: {exc}", flush=True)
            try:
                self._span_ctx.__exit__(None, None, None)
            except Exception:
                pass
        finally:
            self._span = None
            self._span_ctx = None


def record_tokens(model: str, response) -> None:
    """Convenience wrapper: record usage from an Anthropic response object."""
    try:
        if hasattr(response, "usage") and tracker.is_active():
            tracker.record_api_call(
                model,
                response.usage.input_tokens,
                response.usage.output_tokens,
            )
    except Exception:
        pass


# Module-level singleton — imported by orchestrator.py and debate_manager.py
tracker = MLflowTracker(
    tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"),
    experiment_name=os.getenv("MLFLOW_EXPERIMENT_NAME", "orchestrator-decisions"),
)
