#!/bin/bash
# Ralph Loop Runner — PPL-to-SqlNode Converter Migration
# Usage: ./ralph.sh [--agent name] [--max N]
# Stop:  touch STOP_RALPH

set -e

AGENT="general"
MAX_ITERATIONS=0  # 0 = unlimited (use STOP_RALPH to stop)

while [[ $# -gt 0 ]]; do
  case $1 in
    --agent)
      AGENT="$2"
      shift 2
      ;;
    --agent=*)
      AGENT="${1#*=}"
      shift
      ;;
    --max)
      MAX_ITERATIONS="$2"
      shift 2
      ;;
    --max=*)
      MAX_ITERATIONS="${1#*=}"
      shift
      ;;
    *)
      if [[ "$1" =~ ^[0-9]+$ ]]; then
        MAX_ITERATIONS="$1"
      fi
      shift
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRD_FILE="$SCRIPT_DIR/prd.json"
PROGRESS_FILE="$SCRIPT_DIR/progress.txt"
PROMPT_FILE="$SCRIPT_DIR/ralph-prompt.md"

# Initialize progress file if missing
if [ ! -f "$PROGRESS_FILE" ]; then
  echo "# Codebase Patterns" > "$PROGRESS_FILE"
  echo "(Patterns discovered during implementation will be consolidated here)" >> "$PROGRESS_FILE"
  echo "" >> "$PROGRESS_FILE"
  echo "# Iteration Log" >> "$PROGRESS_FILE"
fi

echo "Starting Ralph Loop — Agent: $AGENT — Max: ${MAX_ITERATIONS:-unlimited}"
[ -f "$PRD_FILE" ] && echo "Stories: $(jq '[.userStories[] | select(.passes == false)] | length' "$PRD_FILE") remaining"

trap 'echo "Interrupted."; exit 0' INT TERM
ITERATION=0

while [ ! -f "$SCRIPT_DIR/STOP_RALPH" ]; do
  ITERATION=$((ITERATION + 1))

  if [ "$MAX_ITERATIONS" -gt 0 ] && [ "$ITERATION" -gt "$MAX_ITERATIONS" ]; then
    echo "Max iterations ($MAX_ITERATIONS) reached."
    exit 1
  fi

  REMAINING=$(jq '[.userStories[] | select(.passes == false)] | length' "$PRD_FILE" 2>/dev/null || echo "?")
  echo ""
  echo "==============================================================="
  echo "  Ralph Iteration $ITERATION — $REMAINING stories remaining"
  echo "==============================================================="

  OUTPUT=$(kiro-cli chat --agent "$AGENT" --no-interactive --trust-all-tools \
    "You are in the repo root directory. Read ralph-prompt.md, prd.json, and progress.txt. Pick the next incomplete story, implement it, verify, commit, push, and update state. One task only." \
    2>&1 | tee /dev/stderr) || true

  if echo "$OUTPUT" | grep -q "RALPH_COMPLETE"; then
    echo ""
    echo "All stories complete! Finished at iteration $ITERATION."
    exit 0
  fi

  echo "Iteration $ITERATION complete. Continuing in 2s..."
  sleep 2
done

echo "STOP_RALPH file detected. Stopping."
rm -f "$SCRIPT_DIR/STOP_RALPH"
exit 0
