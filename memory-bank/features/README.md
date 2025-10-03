# Feature Investigation & Design Documentation

This directory contains investigation results and design decisions for features in the OpenSearch SQL project. Each feature gets its own dedicated file to preserve the knowledge gained during analysis and the rationale behind design choices.

## Purpose

The goal is to capture **investigation results** and **design decisions** so that:
- You can easily restart feature development from scratch with full context
- Design rationale is preserved for future reference
- Alternative approaches and trade-offs are documented
- Technical discoveries are not lost

## Structure

Each feature file follows this naming convention:
- `feature-[name].md` - For new features being investigated/designed
- `enhancement-[name].md` - For enhancements to existing features
- `investigation-[topic].md` - For technical investigations without specific features

## Template Structure

Each feature file focuses on:

### 1. Investigation Summary
- **Problem Analysis**: What problem does this solve?
- **Requirements Discovery**: Core requirements and constraints
- **User Impact**: Who benefits and how

### 2. Technical Investigation
- **Architecture Analysis**: How it fits into existing system
- **Implementation Approaches**: Multiple approaches considered with pros/cons
- **Technical Constraints**: Limitations and dependencies
- **Integration Points**: What components are affected

### 3. Design Decisions
- **Chosen Approach**: Final decision with rationale
- **Key Design Choices**: Important decisions and trade-offs
- **Architecture Decisions**: High-level structural choices
- **API Design**: Interface decisions (if applicable)

### 4. Implementation Strategy
- **High-Level Plan**: Overall implementation approach
- **Module Impact**: How each module is affected
- **Risk Assessment**: Main risks and mitigation strategies
- **Performance Considerations**: Expected impact and optimizations

### 5. Knowledge Gained
- **Codebase Insights**: What was learned about existing code
- **Technical Discoveries**: New patterns or knowledge
- **Lessons from Similar Features**: Learning from existing implementations

## Usage

### Starting Feature Investigation
1. Copy the template: `cp feature-template.md feature-[name].md`
2. Fill in the investigation summary and problem analysis
3. Document all approaches considered during investigation
4. Record design decisions and rationale
5. Update `activeContext.md` to reference the new feature file

### During Implementation
1. Update design decisions if they change during development
2. Add new insights or discoveries to the knowledge gained section
3. Document any architectural changes or lessons learned
4. Keep the investigation results current for future reference

### Restarting Development
1. Read the feature file to understand the complete investigation
2. Review design decisions and implementation strategy
3. Use the documented approach or revisit alternatives if needed
4. Build on the knowledge gained rather than starting from scratch

## Benefits

- **Knowledge Preservation**: Investigation results and design rationale preserved
- **Easy Restart**: Can restart feature development with full context
- **Design Continuity**: Consistent approach across development cycles
- **Team Knowledge**: Shared understanding of technical decisions
- **Historical Reference**: Complete record of why decisions were made

## Integration with Memory Bank

Feature files are automatically included in Memory Bank context when:
- Feature name is mentioned in conversation
- Working on related code files
- Explicitly referenced in `activeContext.md`
- Starting development on a previously investigated feature

This ensures Cline always has access to:
- Previous investigation results
- Design decisions and rationale
- Technical constraints and considerations
- Implementation strategy and approach

The Memory Bank system will automatically read relevant feature files to provide complete context for continuing or restarting feature development.
