#!/bin/bash
# ralph-loop.sh — run from repo root
# Usage: ./ralph-loop.sh [--max-iter N]
MAX_ITERATIONS=10
while [[ $# -gt 0 ]]; do
  case $1 in
    --max-iter) MAX_ITERATIONS="$2"; shift 2 ;;
    *) echo "Usage: $0 [--max-iter N]"; exit 1 ;;
  esac
done
for i in $(seq 1 $MAX_ITERATIONS); do
  echo "=== Ralph iteration $i ==="
  
  # Check if all stories pass
  if jq -e '[.userStories[] | select(.passes == false)] | length == 0' prd.json > /dev/null 2>&1; then
    echo "COMPLETE — all stories pass"
    exit 0
  fi
  
  # Get next story
  NEXT=$(jq -r '[.userStories[] | select(.passes == false)] | sort_by(.priority) | .[0].id' prd.json)
  echo "Working on: $NEXT"
  
  kiro-cli chat -m "Read prd.json and progress.txt. Pick the highest priority story where passes=false (should be $NEXT). Implement ONLY that one story. Follow its acceptance criteria exactly. When done: commit, git push origin poc/ppl-to-sql-doctest, update prd.json to set passes=true, append learnings to progress.txt, commit and push again."
  
  sleep 2
done
echo "Max iterations reached"
