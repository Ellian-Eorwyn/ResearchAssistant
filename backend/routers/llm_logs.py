"""LLM call log router."""

from __future__ import annotations

from fastapi import APIRouter, Query

from backend.llm.call_log import LLMCallLogSummary, get_llm_call_logger

router = APIRouter()


@router.get("/llm/call-log", response_model=LLMCallLogSummary)
async def get_llm_call_log(limit: int = Query(20, ge=1, le=100)) -> LLMCallLogSummary:
    return get_llm_call_logger().summary(limit=limit)
