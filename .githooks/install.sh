#!/bin/sh
set -e
 
HOOKS_DIR=".githooks"
GIT_HOOKS_DIR=".git/hooks"
 
cp "$HOOKS_DIR/pre-commit" "$GIT_HOOKS_DIR/pre-commit"
cp "$HOOKS_DIR/pre-push" "$GIT_HOOKS_DIR/pre-push"
 
chmod +x "$GIT_HOOKS_DIR/pre-commit"
chmod +x "$GIT_HOOKS_DIR/pre-push"
 
echo "Git hooks installed."