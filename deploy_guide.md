# 100% Free Portfolio Deployment Guide
## For Yash Jain (Snowflake Data Engineer Portfolio)

This portfolio is a self-contained, lightning-fast static web app designed with modern CSS, Tailwind, responsive SVG diagrams, and real resume PDFs. It requires **$0.00 / month forever** to host.

---

### Option 1: GitHub Pages (Recommended for Data Engineers)
Hiring managers love seeing a live portfolio hosted on your real GitHub account (`https://<your-username>.github.io` or `https://<your-username>.github.io/portfolio`).

#### 3-Step Setup:
1. **Create a GitHub Repository**:
   - Go to [GitHub.com](https://github.com/new) -> Create repository named `portfolio` (or `<your-username>.github.io`).
   - Choose **Public**.

2. **Upload Portfolio Files**:
   - Run in terminal inside `C:\Users\yjain\.gemini\antigravity\scratch\portfolio`:
     ```bash
     git init
     git add .
     git commit -m "feat: launch Yash Jain enterprise data engineering portfolio"
     git branch -M main
     git remote add origin https://github.com/<your-username>/portfolio.git
     git push -u origin main
     ```
   *(Or alternatively, simply drag-and-drop the entire `portfolio` folder contents into GitHub's web interface).*

3. **Enable GitHub Pages**:
   - Go to your repository **Settings** -> **Pages** (in left sidebar).
   - Under **Build and deployment** -> Source: **Deploy from a branch**.
   - Select Branch: `main` / Folder: `/(root)`. Click **Save**.
   - Your site is live in ~30 seconds at:  
     `https://<your-username>.github.io/portfolio/`

---

### Option 2: Vercel (Alternative 1-Click Free Hosting)
1. Go to [Vercel.com](https://vercel.com) (free signup with GitHub or Email).
2. Click **Add New Project** -> Select your `portfolio` GitHub repo (or drag & drop the `portfolio` folder via Vercel CLI).
3. Click **Deploy**.
4. You get an instant custom URL like:  
   `https://yash-jain-data-engineer.vercel.app`

---

### Verifying Resumes on Live URL:
- All 4 PDF resumes are located in `assets/resumes/` and will automatically preview and download from your live site without any backend server.
- The interactive Medallion Lakehouse tabs, code copy buttons, and mobile navigation work entirely client-side.
