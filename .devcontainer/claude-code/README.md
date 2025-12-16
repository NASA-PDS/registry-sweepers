# Claude Code Devcontainer

This directory contains a devcontainer configuration for running Claude Code in an isolated Docker environment.

## What are devcontainers?

[Devcontainers](https://containers.dev/) are Docker-based development environments which are self-contained, and run a headless IDE. They provide:
- Secure, isolated workspace (i.e. Claude Code can only access project files and other files explicitly mounted to the container)
- One-click, consistent development environment setups across team members or for fast onboarding

## Usage with JetBrains IDEs

1. Open this project in PyCharm
2. Open devcontainer.json in the IDE
3. Click the gutter icon to build/run a devcontainer, and follow the wizard.

## Usage with VSCode (AI-generated, untested)

1. Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) in VS Code
2. Open this project in VS Code
3. Click "Reopen in Container" when prompted (or use Command Palette → "Dev Containers: Reopen in Container")

## Usage

Once the container is running, open a terminal in VS Code and run:

```bash
claude
```

Claude Code will have access to your project files but remain isolated from your host system.

## Learn More

Full documentation: https://docs.claude.com/en/docs/claude-code/devcontainer
