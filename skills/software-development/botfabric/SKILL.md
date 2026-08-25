# BotFabric Skill

**Core capability:** Build, document, and maintain a fully plugin-based QQ robot middleware layer with protocol adapters, event bus, and plugin interop.

**When to use:** When you need a protocol-adapter + pluginized intermediate layer between QQ bot protocol and application logic. Not for simple API gateways or pure plugin interop.

**Prerequisites:**  
- Go 1.21+ installed  
- Node.js (for optional TS SDK)  
- Python 3.11+ (for Python SDK)  
- hermes-agent skill authoring standards (see hermes-agent-skill-authoring)

## How to Run

```bash
# Create new skill in-repo (this is how you add BotFabric)
hermes skills create botfabric --category software-development

# Or manually via write_file to skills/software-development/botfabric/SKILL.md
```

## Procedure

1. **Decide tier**  
   - Bundled if broadly useful (most users need QQ bot middleware)  
   - Optional if very vertical-specific

2. **Create directory structure**  
   ```
   skills/software-development/botfabric/
   ├── SKILL.md
   ├── references/
   │   ├── qq-protocol.md
   │   ├── event-bus.md
   │   └── sdk-templates.md
   ├── templates/
   │   └── qq-plugin-example.go
   ├── scripts/
   │   └── validate.go
   ```

3. **Write SKILL.md** (follow hermes-agent-skill-authoring standards)

4. **Add support files** (references, templates, scripts)

5. **Commit** and PR

## Pitfalls

- Do not use `skill_manage(action='create')` for in-repo skills — use `write_file` directly
- Always credit human first in `author:` field
- Description must be ≤ 60 characters
- Never reference user-local skills in `related_skills`

## Verification

- Run `make validate` (or `python scripts/validate.go`)
- Ensure docs regenerate cleanly
- PR must pass skill authoring review

## Next Steps

This skill can now be used to scaffold the full BotFabric project.