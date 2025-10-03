# Features Documentation

This directory contains detailed implementation guides and checklists for specific features and development tasks.

## Contents

### ppl-command-checklist.md
Comprehensive step-by-step checklist for implementing new PPL commands, including:
- Prerequisites and RFC requirements
- Grammar and parser implementation steps
- AST development guidelines
- Complete testing strategy (unit, integration, cross-cluster)
- Documentation requirements
- Gradle test commands reference

## Usage

These documents serve as reference materials and detailed procedures for specific development tasks. They complement the high-level principles defined in `.clinerules/` by providing concrete implementation steps.

## Note on Duplication

As of 2025-03-10, duplicate content has been removed from:
- `memory-bank/opensearch-specific/ppl-parser-patterns.md` - Removed duplicate PPL command checklist (now only in features/ppl-command-checklist.md)

The memory bank now focuses on project-specific implementation details, while general principles and best practices are maintained in `.clinerules/`.
