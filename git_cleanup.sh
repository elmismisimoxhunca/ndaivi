#!/bin/bash

echo "=== Git Repository Cleanup Script ==="
echo "This script will remove large files from your Git history."
echo "WARNING: This is a destructive operation that rewrites Git history."
echo ""

# Make sure we're in the repository root
cd "$(git rev-parse --show-toplevel)"

# Backup current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: $CURRENT_BRANCH"

# Create a temporary branch for cleanup
echo "Creating temporary branch for cleanup..."
git checkout --orphan temp_branch

# Add all files to the new branch (except ignored ones)
echo "Adding all files (except ignored ones) to the temporary branch..."
git add .

# Create a commit
echo "Creating initial commit..."
git commit -m "Initial commit after repository cleanup"

# Remove the old branch
echo "Removing old branch..."
git branch -D $CURRENT_BRANCH

# Rename the temporary branch to the original branch name
echo "Renaming temporary branch to $CURRENT_BRANCH..."
git branch -m $CURRENT_BRANCH

# Make sure .gitignore is properly set up
echo "Verifying .gitignore..."
if ! grep -q "manuals.db" .gitignore; then
  echo "Updating .gitignore to include database files..."
  cat > .gitignore << EOF
/logs
/output
/venv
manuals.db
manuals.db.backup 
manuals.db-shm
manuals.db-wal
scraper.logs
*.log
__pycache__/
*.py[cod]
*$py.class
EOF
  git add .gitignore
  git commit -m "Update .gitignore"
fi

echo ""
echo "=== Cleanup Complete ==="
echo "Your Git repository has been cleaned up."
echo "Now you can force push these changes with: git push --force origin $CURRENT_BRANCH"
echo "WARNING: This will overwrite the remote branch history."
