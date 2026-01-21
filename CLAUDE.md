# CLAUDE.md - Project Instructions for AI Agents

> **STOP**: Read `.ai-context.md` before responding to any request.

## Required Reading
1. `.ai-context.md` - Complete project context, rules, session history

## Critical Rules (DO NOT VIOLATE)
- 4-stage sequential pipeline: `1.GET_NBS_BASE.sh` → `2.FETCH_DAILY_DATA.sh` → `3.PROCESS_DAILY_AND_BUILD_VIEW.sh` → `4.BUILD_TRANSACTION_COUNTERS.sh`
- 6 transaction types: ACT, RENO, DCT, CNR, RFND, PPD
- Refund counting: `sum(rfnd_cnt)` NOT `count(rows)`
- Exclude upgrades: `channel_act != 'UPGRADE'`, `channel_dct != 'UPGRADE'`
- Python path: `/opt/anaconda3/bin/python`
- No PII in logs (tmuserid, msisdn)

## Commands
- `update docs` → Update `.ai-context.md` session history + sync dates

## Project
Telecom subscription ETL pipeline. See `.ai-context.md` for full details.
