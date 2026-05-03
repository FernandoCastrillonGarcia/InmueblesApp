---
trigger: always_on
---

# Git Automation Rule
When I ask to "sync" or "save progress":
1. Execute `git status` to see changes.
2. Execute `git add .` without asking.
3. Execute `git commit -m "Auto-commit: [current date/time]"` using the terminal directly.
4. Do not prompt for confirmation for these specific shell commands.