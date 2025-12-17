# Kiro Configuration for OpenSearch SQL

This directory contains Kiro AI assistant configuration for the OpenSearch SQL project, including steering files, hooks, and spec templates that guide development workflows.

## Overview

Kiro is configured to understand the OpenSearch SQL codebase structure, coding standards, and development workflows. This configuration helps maintain consistency and quality across contributions.

## Directory Structure

```
.kiro/
├── README.md                    # This file
├── hooks/                       # Automated agent triggers
│   └── update-project-steerings.kiro.hook
├── specs/                       # Feature specifications (created as needed)
│   └── [feature-name]/
│       ├── requirements.md      # RFC-based requirements
│       ├── design.md           # Technical design (if needed)
│       └── tasks.md            # Implementation breakdown
└── steering/                    # Context and guidelines for Kiro
    ├── coding-standards.md     # Design principles (references CodeRabbit)
    ├── memory-bank.md          # Memory Bank usage guide
    ├── product.md              # Product overview
    ├── structure.md            # Project structure and modules
    ├── tech.md                 # Technology stack and commands
    ├── team-memory.md          # Memory MCP usage instructions
    └── ppl-command-workflow.md # PPL command RFC workflow
```

## Documentation Layers

The project uses multiple documentation systems, each serving a distinct purpose:

### 1. Steering Files (.kiro/steering/) - Always Loaded
Lightweight guidance automatically included in every Kiro interaction:
- Core principles and project structure
- Essential context for all development tasks
- Kept concise to avoid token bloat

### 2. Memory Bank (memory-bank/) - On-Demand Search
Detailed technical reference searched only when needed:
- Deep architecture patterns and implementation examples
- Calcite integration, parser patterns, module details
- Project status and progress tracking
- See `steering/memory-bank.md` for usage guide

### 3. Memory MCP - Opinion Storage
Graph-based storage for your personal preferences:
- Design taste from PR reviews
- Team tenets and decision patterns
- Extracted via `steering/team-memory.md` guidance

### 4. CodeRabbit (.rules/REVIEW_GUIDELINES.md)
Automated code review standards:
- Language-specific conventions
- Testing requirements
- Review criteria

## Steering Files

### Always Loaded

**`coding-standards.md`** - High-level design principles
- Composition over inheritance, DRY, single responsibility
- Fail fast, design by contract
- Boy Scout Rule, refactor in small steps
- References CodeRabbit for detailed standards

**`memory-bank.md`** - Memory Bank usage guide
- When and how to use memory-bank/ folder
- File structure and hierarchy
- Search strategy and examples

**`product.md`** - Product overview
- OpenSearch SQL plugin capabilities
- SQL and PPL query language support
- Multi-engine architecture (V2, V3)

**`structure.md`** - Project organization
- Multi-module Gradle structure
- Module responsibilities and dependencies
- Package organization patterns

**`tech.md`** - Technology stack
- Build system (Gradle 8.x, Java 21)
- Key dependencies (ANTLR4, Calcite, Lombok)
- Common build/test/development commands

**`team-memory.md`** - Memory MCP integration
- How to extract principles from PR reviews
- Entity types: core_principle, design_pattern, code_maintenance, etc.
- Memory retrieval and update workflow

### Manual Inclusion

**`ppl-command-workflow.md`** - PPL command RFC workflow
- RFC template structure (Problem, Proposal, Approach, etc.)
- Baseline study requirements (Splunk documentation)
- Concrete PPL query examples with input/output
- Use with: `#ppl-command-workflow` in chat

## Memory Bank Structure

Located in `memory-bank/` folder (search on-demand):

### Core Files
- `projectbrief.md` - Project scope and requirements
- `productContext.md` - Product goals and user experience
- `systemPatterns.md` - Architecture and design patterns
- `techContext.md` - Tech stack, dependencies, setup
- `progress.md` - Current status and roadmap

### OpenSearch-Specific
- `opensearch-specific/calcite-integration.md` - Calcite patterns
- `opensearch-specific/module-architecture.md` - Module interactions
- `opensearch-specific/ppl-parser-patterns.md` - Parser patterns

### Features
- `features/` - Detailed feature specifications

**Usage:** Search memory-bank/ when you need detailed technical patterns. See `steering/memory-bank.md` for guidance.

## Hooks

Hooks automatically trigger Kiro actions when specific events occur.

### `update-project-steerings.kiro.hook`

**Trigger:** When key project files are edited
- `README.md`, `settings.gradle`, `build.gradle`
- `DEVELOPER_GUIDE.rst`, `gradle.properties`

**Action:** Prompts Kiro to review and suggest updates to steering files

**Purpose:** Keeps steering documentation in sync with project evolution

## Specs Workflow

Kiro supports spec-driven development for complex features:

### PPL Command Specs (RFC-Based)

1. **requirements.md** - RFC template structure
   - Problem Statement, Current State, Long-Term Goals
   - Proposal with concrete PPL query examples
   - Approach (design decisions)
   - Implementation Discussion (behavioral notes)
   - Appendix: Baseline Study (Splunk documentation)

2. **design.md** - Usually not needed
   - Only for complex features requiring diagrams, algorithms, performance analysis

3. **tasks.md** - Implementation breakdown
   - Evidence from recent PRs
   - Planned change list (grouped by category)
   - Task breakdown with acceptance criteria

### Creating a Spec

Start a conversation with Kiro describing your feature idea. For PPL commands, include `#ppl-command-workflow`.

### Executing Tasks

Open `.kiro/specs/[feature-name]/tasks.md` and click "Start task" next to any task item to have Kiro implement it.

## Using Kiro Effectively

### Include Context

Use `#` in chat to reference specific context:
- `#File` or `#Folder` - Include specific files/folders
- `#ppl-command-workflow` - Include PPL workflow steering
- `#Codebase` - Search across the entire codebase

### Common Workflows

**Adding a new PPL command:**
```
I want to add support for the [command] PPL command
#ppl-command-workflow
```

**Understanding architecture patterns:**
```
How does Calcite integration work?
[Kiro will search memory-bank/opensearch-specific/calcite-integration.md]
```

**Checking project status:**
```
What's the current state of the project?
[Kiro will read memory-bank/progress.md]
```

**Understanding a module:**
```
Explain how the async-query module works
#Folder async-query
```

**Fixing a test:**
```
This test is failing: [test name]
#Problems
```

**Code review:**
```
Review my changes for best practices
#Git Diff
```

---

**Last Updated:** December 2024
**Kiro Version:** Compatible with Kiro IDE
