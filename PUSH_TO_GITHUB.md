# Push to GitHub & Databricks

## Repo name to create on GitHub

Create a new repository with this name:

**`kmati-ingestion`**

(Or: `kmati-ingestion-framework` if you prefer.)

This repo will contain:
- The Python package (`kmati-ingestion` from `pyproject.toml`)
- The **Databricks asset bundle** (`databricks.yml`, `resources/`, `src/`, `extractor/`, configs)
- One push includes both; no separate repo needed for the bundle.

---

## 1. Create the repo on GitHub

1. Go to https://github.com/new
2. **Repository name:** `kmati-ingestion`
3. Choose **Private** or **Public**
4. Do **not** add a README, .gitignore, or license (you already have content)
5. Click **Create repository**

---

## 2. Push this folder to GitHub

From **PowerShell**, in the folder that contains `kmati_ingest` (your workspace root):

```powershell
cd "c:\Users\admin\Downloads\kmati_ingest"

# Initialize git (if not already)
git init

# Add all files (venv and .databricks are ignored by .gitignore)
git add .
git status

# First commit
git commit -m "Initial commit: kmati-ingestion + Databricks asset bundle"

# Add your new GitHub repo as remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/kmati-ingestion.git

# Push (main branch)
git branch -M main
git push -u origin main
```

If the repo already had `git init` and a remote, use:

```powershell
git remote set-url origin https://github.com/YOUR_USERNAME/kmati-ingestion.git
git add .
git commit -m "Add Databricks bundle and project code"
git push -u origin main
```

---

## 3. Deploy the asset bundle to Databricks (after push)

The bundle code is already in this repo. To deploy it to Databricks:

```powershell
cd "c:\Users\admin\Downloads\kmati_ingest\kmati_ingest\kmati_ing\kmati_ingestion"

# Dev (default)
databricks bundle deploy --target dev

# Production
databricks bundle deploy --target prod
```

Make sure you’re logged in: `databricks configure` (or use a token/profile).

---

## Summary

| Step | What |
|------|------|
| Repo name | **kmati-ingestion** |
| Push | From `c:\Users\admin\Downloads\kmati_ingest` (root). One repo = app + Databricks bundle. |
| Deploy bundle | From `...\kmati_ingestion` with `databricks bundle deploy --target dev` or `prod`. |
