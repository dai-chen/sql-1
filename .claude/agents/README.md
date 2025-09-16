# PPL Command Development Agents

This directory contains specialized agents for developing new PPL (Piped Processing Language) commands in OpenSearch SQL using a **two-phase approach**.

## Agent Workflow

```
User Request → PPL RFC Analyst → RFC Review → PPL Command Developer → Implementation
```

## Phase 1: Requirements & Design (ppl-rfc-analyst.md)

**Role**: Product Manager
**Input**: User requirements and command specifications
**Output**: Comprehensive RFC document

### Responsibilities:
- Analyze user requirements and use cases
- Research existing patterns and SPL compatibility
- Define syntax, parameters, and behavior specifications
- Create usage examples and acceptance criteria
- Document implementation approach and constraints

### Usage:
```
Use the PPL RFC Analyst agent when you need to:
- Define requirements for a new PPL command
- Create specifications before implementation
- Research and document command behavior
- Plan integration with existing PPL infrastructure
```

## Phase 2: Implementation (ppl-command-developer.md)

**Role**: Software Engineer
**Input**: Approved RFC document
**Output**: Complete implementation with tests and documentation

### Responsibilities:
- Implement grammar and parser updates (ANTLR)
- Create AST nodes and visitor pattern implementations
- Develop comprehensive test suite (unit, integration, explain)
- Write user documentation and examples
- Ensure code quality and performance standards

### Usage:
```
Use the PPL Command Developer agent when you have:
- An approved RFC specification
- Clear implementation requirements
- Confirmed technical approach
```

## Quality Gates

### RFC Approval Gates
- [ ] User scenarios documented with examples
- [ ] Syntax follows PPL command patterns
- [ ] Error conditions and edge cases specified
- [ ] Performance implications considered
- [ ] Integration requirements clear
- [ ] Testing strategy comprehensive

### Implementation Delivery Gates
- [ ] All gradle test commands pass
- [ ] Code follows established patterns
- [ ] Comprehensive test coverage
- [ ] User documentation complete
- [ ] Integration verified

## Example Workflow

### Step 1: Create RFC
```bash
# Use PPL RFC Analyst agent
"I need to implement a makeresults command similar to SPL that generates synthetic data for testing"
→ Receives comprehensive RFC document
```

### Step 2: Review & Approve
```bash
# Review RFC output
- Check syntax definitions
- Validate examples and use cases
- Confirm implementation approach
- Provide approval: "RFC approved, proceed with implementation"
```

### Step 3: Implement
```bash
# Use PPL Command Developer agent with approved RFC
"Here's the approved RFC for makeresults command. Please implement it."
→ Receives complete implementation with tests and docs
```

## Benefits of Two-Phase Approach

1. **Clear Separation of Concerns**: PM thinking vs Engineering execution
2. **Quality Gates**: Proper specification before implementation
3. **Faster Iteration**: Refine requirements without touching code
4. **Better Documentation**: Forces complete specs upfront
5. **Reusable Patterns**: RFC process works for any PPL command
6. **Risk Reduction**: Catch design issues before implementation

## Agent Files

- `ppl-rfc-analyst.md` - Product Management agent for requirements analysis
- `ppl-command-developer.md` - Engineering agent for implementation
- `README.md` - This workflow documentation

---

**Note**: Always start with the RFC Analyst agent for new PPL commands. The Command Developer agent requires an approved RFC specification to proceed.