#!/bin/bash

# === Git Commit Config
BRANCH="main"
STAMP=$(date +"%Y-%m-%d_%H-%M-%S")

echo "📦 Staging core code only..."
git add core/ streamlit_app/ utils/ requirements.txt

echo "✅ Committing..."
git commit -m "💾 Code snapshot @ $STAMP – Phases 1–3 stable"

echo "🚀 Pushing to GitHub..."
git push origin $BRANCH

echo "🏷️ Tagging commit..."
git tag -a "code-freeze-$STAMP" -m "Code freeze checkpoint @ $STAMP"
git push origin "code-freeze-$STAMP"

echo "✅ Code-only commit + tag complete."
