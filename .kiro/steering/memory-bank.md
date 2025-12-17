---
inclusion: always
---

# Memory Bank Usage

The `memory-bank/` folder contains detailed technical reference for on-demand lookup. Search and read only when needed to avoid token bloat.

## Memory Bank Structure

### Core Files (Required)
Files build upon each other: `projectbrief` → `productContext/systemPatterns/techContext` → `progress`

- `projectbrief.md` - Foundation: project scope and requirements (source of truth)
- `productContext.md` - Why project exists, problems solved, user experience
- `systemPatterns.md` - Architecture, design patterns, component relationships
- `techContext.md` - Tech stack, dependencies, development setup
- `progress.md` - What works, what's left, current status, known issues

### OpenSearch-Specific
- `opensearch-specific/calcite-integration.md` - Calcite patterns and visitors
- `opensearch-specific/module-architecture.md` - Module interactions and dependencies
- `opensearch-specific/ppl-parser-patterns.md` - ANTLR grammar and AST patterns

### Additional Context
Create additional files/folders for: complex features, integrations, API docs, testing strategies, deployment procedures

## When to Use

Search memory-bank/ when you need:
- Detailed architecture patterns or implementation examples
- Technical deep-dives (Calcite, parser, modules)
- Project context or development setup details
- Current project status from `progress.md`

## Usage Rules

**DO:**
- Search memory-bank/ for specific technical areas
- Read selectively - only relevant sections
- Check `progress.md` for project status

**DON'T:**
- Read entire memory-bank/ upfront
- Duplicate content into steering files
- Load files unless needed for current task

## Update Memory Bank When

- Discovering new project patterns or insights
- After implementing significant changes
- Project state changes (features completed, new issues)
- Context needs clarification

## Quick Examples

- **New PPL command**: Read `opensearch-specific/ppl-parser-patterns.md`
- **Calcite work**: Read `opensearch-specific/calcite-integration.md`
- **Module dependencies**: Read `opensearch-specific/module-architecture.md`
- **Project status**: Read `progress.md`
