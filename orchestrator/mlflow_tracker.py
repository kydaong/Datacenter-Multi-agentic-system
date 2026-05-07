"""
MLflow Tracker — Orchestrator Decision Logging & API Cost Tracking

Tracks two things per session:
  1. Decision metrics (confidence, consensus, debate rounds, vote breakdown)
  2. Anthropic API usage (input/output tokens per model → USD cost)

Usage pattern:
  tracker.start_session(session_id)          # at start of analyze_and_propose
  tracker.record_api_call(model, in, out)    # after every messages.create()
  tracker.end_session(params={}, metrics={}) # before returning to caller

Falls back to a no-op if MLflow is not installed or the server is unreachable.
"""

import os
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# USD per 1 million tokens — https://www.anthropic.com/pricing
_MODEL_PRICING: Dict[str, Dict[str, float]] = {
    "claude-haiku-4-5-20251001": {"input": 0.80, "output": 4.00},
    "claude-sonnet-4-6":         {"input": 3.00, "output": 15.00},
}


class MLflowTracker:
    def __init__(
        self,
        tracking_uri: str = "http://localhost:5000",
        experiment_name: str = "orchestrator-decisions",
    ):
        self._mlflow = None
        self._run = None
        self._token_totals: Dict[str, Dict[str, int]] = {}
        self._call_counts: Dict[str, int] = {}

        try:
            import mlflow  # type: ignore
            mlflow.set_tracking_uri(tracking_uri)
            mlflow.set_experiment(experiment_name)
            self._mlflow = mlflow
            logger.info("MLflow tracking enabled at %s", tracking_uri)
        except Exception as exc:
            logger.warning("MLflow unavailable, tracking disabled: %s", exc)

    # ── Public API ──────────────────────────────────────────────────────────

    def is_active(self) -> bool:
        return self._mlflow is not None and self._run is not None

    def start_session(self, session_id: str, tags: Optional[Dict[str, str]] = None) -> None:
        if self._mlflow is None:
            return
        try:
            self._token_totals = {}
            self._call_counts = {}
            self._run = self._mlflow.start_run(run_name=session_id, tags=tags or {})
        except Exception as exc:
            logger.warning("MLflow start_run failed: %s", exc)
            self._run = None

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
                self._mlflow.log_params({k: str(v)[:500] for k, v in params.items()})
            if metrics:
                self._mlflow.log_metrics(metrics)
            self._log_token_costs()
            self._mlflow.end_run()
        except Exception as exc:
            logger.warning("MLflow end_session failed: %s", exc)
        finally:
            self._run = None

    # ── Private helpers ─────────────────────────────────────────────────────

    def _log_token_costs(self) -> None:
        total_cost = 0.0
        for model, tokens in self._token_totals.items():
            # Shorten model name to a valid MLflow metric key
            short = model.replace("claude-", "").replace("-", "_")
            pricing = _MODEL_PRICING.get(model, {"input": 0.0, "output": 0.0})
            cost = (
                tokens["input"] * pricing["input"]
                + tokens["output"] * pricing["output"]
            ) / 1_000_000
            self._mlflow.log_metrics({
                f"tokens_input_{short}":  tokens["input"],
                f"tokens_output_{short}": tokens["output"],
                f"api_calls_{short}":     float(self._call_counts[model]),
                f"cost_usd_{short}":      round(cost, 6),
            })
            total_cost += cost
        self._mlflow.log_metric("total_cost_usd", round(total_cost, 6))


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
