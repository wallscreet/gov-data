# 📊 FRED Aggregator API

## About

This project provides a **unified, consistent, and developer-friendly API layer** over the Federal Reserve Economic Data (FRED).
While FRED is one of the richest public data repositories available, it can be frustrating to work with directly:

* Datasets vary in frequency (monthly, quarterly, annual).
* Column naming is inconsistent.
* Transformations are left to the end-user.
* Bulk analysis across multiple series is cumbersome.

This API normalizes FRED data into a **clean, predictable structure**, making it easier to use in:

* Public data applications
* Research & analysis pipelines
* AI/agentic systems that require structured economic signals

By handling frequency normalization, naming consistency, and flexible output formats, this service turns FRED into a **plug-and-play economic data backbone**.

---

## Features

* 🔄 **Frequency Normalization**
  All datasets are output in **monthly frequency by default**, even when the source is annual or quarterly (values forward-filled).
  This guarantees alignment across series and removes the guesswork.

* 🧩 **Dataset-Specific Endpoints**
  Each dataset is exposed through its own endpoint, giving fine-grained access and minimizing the risk of systemic bugs when experimenting with transformations.

* 📖 **Rich Documentation**
  Every dataset includes clear metadata and descriptions in the Swagger/OpenAPI docs, making discovery straightforward.

* 📊 **Flexible Transformations**
  Endpoints support query parameters to resample (monthly, quarterly, annual), choose aggregation logic (`mean`, `max`, etc.), and adjust output to suit different analytical needs.

* 🤖 **Agent-Ready**
  Designed with **machine consumption** in mind. JSON outputs are clean, normalized, and consistent — ideal for LLMs, agentic systems, or even integration into the Model Context Protocol (MCP).

* ⚡ **Extensible Design**
  Each dataset fetch function is modular and isolated, making it easy to add, modify, or transform data without affecting the rest of the system.

---

## Why It Matters

Economic data underpins critical decision-making for policy, research, and everyday life. But access shouldn’t require a PhD in time series wrangling.

This project **lowers the barrier to entry** for working with U.S. economic indicators by:

* Standardizing structure and frequency
* Providing intuitive endpoints
* Making economic data more usable for both humans and machines

The result is a **public data service** that amplifies the value of FRED and expands its reach into new domains like **AI agents, autonomous analysis, and real-time applications**.


#ENDTHEFED