# Antigravity Orchestrator Loop (Best Practices Edition)

The Main Antigravity Agent operates strictly as a "dumb" router and state manager. It does not perform complex reasoning about project scope or architecture.

## 1. Central Blackboard (State Management)
Maintain a temporary JSON state object for the duration of the task. Do not pass the entire conversation history to subagents.

```json
{
  "trace_id": "req-12345",
  "task_goal": "Implement /api/v1/events",
  "phase": 5,
  "modified_files": [],
  "known_context": "Feature engine is already producing to features.vwap.v1",
  "current_status": "in_progress"
}
```

## 2. Tools as Agents (Function Calling)
Expose subagents as explicitly defined tools to the Main Agent. The Main Agent routes work by calling these tools.

- `call_product_shepherd(request)` -> Returns a validated execution plan or rejects the request based on `NON_GOALS.md`.
- `call_backend_engineer(dispatch_brief, blackboard_state)` -> Returns JSON result.
- `call_streaming_data_engineer(dispatch_brief, blackboard_state)` -> Returns JSON result.

## 3. Execution Flow
1. **Receive Request:** User provides a goal.
2. **Triage (Delegate):** Call `call_product_shepherd(goal)`. Wait for JSON plan.
3. **Initialize Blackboard:** Create the central state JSON based on the Shepherd's plan.
4. **Execute (Parallel or DAG):**
   - For each step in the Shepherd's plan, call the appropriate subagent tool (e.g., `call_backend_engineer`).
   - Pass only the specific `dispatch_brief` for that step and the current `blackboard_state`.
5. **Parse JSON Contracts:** Subagents return strict JSON (no markdown).
6. **Update Blackboard:** Merge the subagent's JSON output (modified files, discovered context) into the central Blackboard.
7. **Handoff:** If the subagent JSON specifies `handoff_target`, update the loop and call the next subagent tool.
8. **Finalize:** When no handoffs remain and the plan is complete, summarize the final Blackboard state to the human user.
