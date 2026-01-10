# 🚀 Creator Harvester: Automated Growth Engine for AI Startups

**A high-performance, autonomous pipeline engineered to discover, analyze, and engage AI creators at scale.**

> **POC Result:** 29 valid business emails from 111 targeted channels (**26% Conversion Rate**) in 40 minutes on a single thread.

---

### 🛠 Tech Stack

* **Orchestration:** Apache Airflow
* **Data Warehouse:** ClickHouse (Optimized for 1M+ records)
* **Scraping Engine:** DrissionPage (Advanced bypass for Cloudflare, TLS Fingerprinting, and reCAPTCHA)
* **Intelligence:** OpenAI API (GPT-4o for Semantic Keyword Expansion & Hyper-Personalized Outreach)
* **Analysis:** Pandas, Matplotlib, Seaborn (Predictive ROI & Lead Scoring)
* **Infrastructure:** Docker & Docker-compose

---

### 🎯 Strategic Target: The "Faceless YouTube" Niche

The engine specifically targets the **Faceless YouTube** creator ecosystem. This niche represents the perfect Product-Market Fit for AI video tools:

* **High Tool Dependency:** These creators require AI tools for every stage of production.
* **Massive Reach:** Built for virality and high-volume output.
* **Business Ready:** Higher probability of professional collaboration vs. hobbyist channels.

---

### 📊 Data-Driven Insights (`analysis.ipynb`)

The project transforms raw data into a strategic marketing roadmap. Key metrics calculated:

* **Lead Score:** Automated quality assessment of each creator.
* **Predictive ROI:** Estimated return on collaboration based on engagement metrics.
* **LTV & Effectiveness:** Analysis of historical performance and audience retention.
* **Visual Analytics:** Distribution of ROI, top-tier acquisition targets, and cost-vs-profit modeling.

<img width="1058" height="550" alt="image" src="https://github.com/user-attachments/assets/7ff966ef-b826-44a1-9885-9cd3ff1a7638" />

<img width="1058" height="550" alt="image" src="https://github.com/user-attachments/assets/5a680e4d-fbda-4ca3-a3af-c65708dbba30" />

<img width="1184" height="658" alt="image" src="https://github.com/user-attachments/assets/9f7a757a-b119-4d85-bea8-ba13ef8a2ac7" />

<img width="1075" height="550" alt="image" src="https://github.com/user-attachments/assets/ad27ae9c-bd0a-40d5-bbbc-555ae02e9065" />

<img width="871" height="550" alt="image" src="https://github.com/user-attachments/assets/4a74794c-a321-460d-b38f-2252c1fabae4" />

---

### ⚙️ Core Features & Scalability

* **Full Autonomy:** Zero-human intervention from search query to lead storage.
* **Scalable Architecture:** Designed to transition from 2500 to **1,000,000+ leads per week** via:
* Distributed Proxy-rotation (Residential/Mobile).
* Multi-node asynchronous scraping.
* Horizontal scaling of Dockerized workers.


* **Smart Data Filtering:** Automatically distinguishes between "Hobbyists" and "Professional Leads" based on data availability (e.g., presence of business contact info).

---

### 📩 AI-Powered Outreach

The system includes a framework for **Hyper-Personalized Cold Outreach**:

* **Contextual Awareness:** GPT-4o analyzes the creator's latest video content to generate unique, non-spammy messages.
* **Efficiency:** Aiming for a **20-30% Reply Rate** by providing immediate value (Early Access/Creator Fund offers).
* **Anti-Spam:** Human-like interaction patterns and variable content generation to bypass Gmail/Outlook filters.

---

### 🗺 Future Roadmap

* [ ] **Semantic Expansion:** Automated keyword generation using NLP to find hidden niches.
* [ ] **Sentiment Analysis:** Evaluating creator audience quality through comment section analysis.
* [ ] **Distributed Scraping:** Moving the pipeline to a VPS cluster with TLS fingerprint randomization.

---

### 🚀 Quick Start
Set .env file

```bash
docker-compose build
docker-compose up -d
# This command deploys Airflow and ClickHouse, initializing the first automated cycle.

```

---

**Developed in 24 hours** 

**24 hours challenge finished**
