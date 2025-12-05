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
│       ├── requirements.md      # User stories and acceptance criteria
│       ├── design.md           # Technical design and architecture
│       └── tasks.md            # Implementation task list
└── steering/                    # Context and guidelines for Kiro
    ├── coding-standards.md     # General coding principles
    ├── product.md              # Product overview and capabilities
    ├── structure.md            # Project structure and modules
    ├── tech.md                 # Technology stack and commands
    └── ppl-command-workflow.md # PPL command development workflow
```

## Steering Files

Steering files provide context and guidelines that Kiro uses to understand the project and make informed suggestions.

### Global Standards

**`coding-standards.md`** - Core engineering principles
- Naming conventions and code clarity
- Design principles (composition, DRY, single responsibility)
- Error handling and testing practices
- Performance considerations

### Project Context

**`product.md`** - What we're building
- OpenSearch SQL plugin capabilities
- SQL and PPL query language support
- Multi-engine architecture (V2, V3)
- Related ecosystem projects

**`structure.md`** - How the code is organized
- Multi-module Gradle structure
- Module responsibilities (core, sql, ppl, opensearch, etc.)
- Package organization patterns
- Published artifacts

**`tech.md`** - Tools and technologies
- Build system (Gradle 8.x, Java 21)
- Key dependencies (ANTLR4, Calcite, Lombok)
- Common build/test/development commands
- Code formatting with Spotless

### Workflow-Specific

**`ppl-command-workflow.md`** (manual inclusion)
- Specialized workflow for PPL command development
- Requires baseline study of Splunk documentation
- Mandates concrete query examples with input/output
- Enforces specific design.md structure
- Use with: `#ppl-command-workflow` in chat

## Hooks

Hooks automatically trigger Kiro actions when specific events occur.

### `update-project-steerings.kiro.hook`

**Trigger:** When key project files are edited
- `README.md`
- `settings.gradle`
- `build.gradle`
- `DEVELOPER_GUIDE.rst`
- `gradle.properties`

**Action:** Prompts Kiro to review and suggest updates to steering files
- Checks if product.md, tech.md, or structure.md need updates
- Only updates relevant sections
- Conservative approach - avoids unnecessary changes

**Purpose:** Keeps steering documentation in sync with project evolution

## Specs Workflow

Kiro supports spec-driven development for complex features:

1. **Requirements** - User stories with EARS-compliant acceptance criteria
2. **Design** - Architecture, components, correctness properties
3. **Tasks** - Actionable implementation checklist

### Creating a Spec

Start a conversation with Kiro describing your feature idea. Kiro will guide you through:
- Clarifying requirements
- Creating a design document
- Generating an implementation task list

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

**Last Updated:** December 2025
**Kiro Version:** Compatible with Kiro IDE
