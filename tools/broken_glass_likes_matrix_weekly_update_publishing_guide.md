# Broken Glass Likes Matrix — Weekly Update & Publishing Guide

This is a beginner‑friendly, step‑by‑step playbook for keeping Sean’s matrix current and getting a shareable link online.

---

## Project files this page refers to

- **HTML (visual matrix):** `broken_glass_likes_matrix_v3.html`
- **Likes (editable, source‑of‑truth):** `data/survivor_bg_likes_YN.csv`
- **Schedule (used by the HTML to show vs/@ & H/A):** `data/2025_nfl_schedule_cleaned.csv`
- **(Optional) Opponent matrix for Excel/Dashboard:** `data/survivor_bg_likes_OPP_matrix.csv`
- **Scripts**
  - Recompute stats: `scripts/survivor_bg_likes_y_n.py`
  - Export opponent matrix: `scripts/export_bg_likes_opponent_matrix_v2.py`

> Tip: the HTML only needs the **Y/N CSV** and the **Schedule**. The OPP matrix CSV is for Excel/reporting.

---

## A. Weekly maintenance (what you do every week)

1. **Edit likes**

   - Open: `data/survivor_bg_likes_YN.csv`
   - Columns: `W1..W16`, `TG`, `CH`
   - Put `Y` where Sean likes a team that week, `N` otherwise.

2. **(Optional but recommended) Recompute helper columns** This refreshes `Cross_Weeks` and `Likes_N` for quick QA.

   ```powershell
   (venv) PS C:\Users\Spencer\OneDrive\Desktop\nfl25-agent> python -m scripts.survivor_bg_likes_y_n --yn data\survivor_bg_likes_YN.csv
   ```

   Output: `data/survivor_bg_likes_YN_enriched.csv`

3. **(Optional) Export the OPP matrix for Excel / dashboard**

   ```powershell
   (venv) PS C:\Users\Spencer\OneDrive\Desktop\nfl25-agent> python -m scripts.export_bg_likes_opponent_matrix_v2 --with-like-flag
   ```

   Output: `data/survivor_bg_likes_OPP_matrix.csv` (every cell → `OPP (H)` or `OPP (A)` plus `Like_*` columns)

4. **Preview the HTML locally**

   ```powershell
   (venv) PS C:\Users\Spencer\OneDrive\Desktop\nfl25-agent> python -m http.server 8000
   ```

   Open in your browser:

   - `http://localhost:8000/broken_glass_likes_matrix_v3.html`

If the table looks good locally, publish it (Section C or D).

---

## B. What the HTML shows (so you can QA quickly)

- **Green chip** = team is a like; label is `OPP (H/A)`.
- **Gray text** = not a like; shows `vs/@ OPP`.
- **Gold rim on liked cells BEFORE a team’s TG/CH game** (never on the TG/CH cell itself). Helps you avoid burning a team early if you want to save them for the holiday slate.
- **TG/CH column highlight only appears for teams that actually have a TG/CH game.**

---

## C. Publish a live link with GitHub Pages (simple & free)

> Goal: a URL you can share with Sean that updates each time you push the CSV.

**One‑time setup**

1. In your repo, create a `docs/` folder.
2. Copy these into `docs/` (preserve the `data/` subfolder structure):
   - `broken_glass_likes_matrix_v3.html` → `docs/index.html` *(rename to **`index.html`** so it opens by default)*
   - `data/survivor_bg_likes_YN.csv` → `docs/data/...`
   - `data/2025_nfl_schedule_cleaned.csv` → `docs/data/...`
3. Commit & push.
4. On GitHub: **Settings → Pages → Build and deployment**
   - **Source:** `Deploy from a branch`
   - **Branch:** `main` / `/docs`
5. Wait \~1–2 minutes. Your site will be at:
   - `https://<your‑github‑user>.github.io/<repo>/`

**Weekly update using Pages**

- Each time you edit `data/survivor_bg_likes_YN.csv`, copy it into `docs/data/` and push.
- Refresh the URL; Sean sees the new likes.

> Tip: keep `docs/data/` as the live copy, and continue editing your working copy in `data/`. After edits, copy the CSV across: `data/… → docs/data/…`.

---

## D. Publish a live link with Netlify (drag‑and‑drop, also free)

**One‑time setup**

1. Create a Netlify account and a new site from Git.
2. **Publish directory:** repo root (or `/public` if you prefer a `public/` folder).
3. Make sure the HTML + `data/` folder exist in that publish directory.

**Weekly update**

- Commit and push new `data/survivor_bg_likes_YN.csv` → Netlify auto‑deploys → share the site URL.

---

## E. Troubleshooting

- **Red error bar in the HTML** → Path mismatch. The page expects:
  - `data/survivor_bg_likes_YN.csv`
  - `data/2025_nfl_schedule_cleaned.csv` Make sure those files exist *relative to the HTML file*.
- **Holiday highlight looks wrong** → Verify the schedule rows around Thanksgiving (Thu/Fri) and Christmas (Dec 25) have correct dates/teams.
- **Excel shows weird characters** → When exporting the OPP matrix, `export_bg_likes_opponent_matrix_v2.py` writes UTF‑8 with BOM and uses ASCII `-`. Re‑export using that script.

---

## F. Quick checklist (each week)

-

---

## G. Advanced (optional helpers)

If you want a single command to refresh the live copy for GitHub Pages, add a simple PowerShell helper:

```powershell
# tools\publish_pages.ps1
Copy-Item data\survivor_bg_likes_YN.csv docs\data\survivor_bg_likes_YN.csv -Force
Copy-Item data\2025_nfl_schedule_cleaned.csv docs\data\2025_nfl_schedule_cleaned.csv -Force
Copy-Item broken_glass_likes_matrix_v3.html docs\index.html -Force
```

Run it after edits:

```powershell
(venv) PS> .\tools\publish_pages.ps1
(venv) PS> git add -A; git commit -m "Update BG likes"; git push
```

That’s it — you now have a clean weekly flow and a repeatable way to publish a live link for Sean.

