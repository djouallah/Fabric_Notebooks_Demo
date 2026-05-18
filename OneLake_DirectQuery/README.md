# onelake_catalog_direct_Query

DirectQuery from Power BI Desktop into **Microsoft Fabric OneLake** (Iceberg tables)
through a local **DuckDB** process exposed by the `quack` extension over its own
remote protocol (`quack://`).

```
Power BI Desktop  --(quack.mez / ADBC)-->  quack://127.0.0.1:9494  --(Iceberg REST + ABFSS)-->  OneLake
                                           duckdb.exe + quack
```

The launcher (`Iceberg.vbs`) grabs a short-lived Entra token from your current
`az login` session, attaches a Fabric Lakehouse as an Iceberg catalog, and starts
DuckDB serving the `quack` protocol on `127.0.0.1:9494`. Open `DQ.pbix` and refresh.

## Files

- `Iceberg.vbs` — launcher. Downloads `duckdb.exe` on first run, installs the
  `iceberg` / `azure` / `quack` extensions, fetches an Entra token, writes an
  init script to `C:\ProgramData\duckdb_init.sql`, and starts the server.
- `DQ.pbix` — Power BI report wired to `127.0.0.1:9494` via the `quack.mez` custom connector.

## Prerequisites

1. **Windows** (the launcher is VBScript + PowerShell).
2. **Azure CLI** — https://aka.ms/installazurecliwindows
3. **Power BI Desktop** — https://aka.ms/pbidesktopstore
4. **`quack.mez`** — the Power BI custom connector for DuckDB/quack
   (ADBC-based). Built from https://github.com/CurtHagenlocher/quack-net.

### Installing the quack.mez connector

1. Download **`quack.mez`** from the latest release:
   https://github.com/CurtHagenlocher/quack-net/releases/latest
2. Copy it into
   `%USERPROFILE%\Documents\Power BI Desktop\Custom Connectors\`
   (create the `Custom Connectors` folder if it doesn't exist).
3. In Power BI Desktop, go to
   **File → Options and settings → Options → Security → Data Extensions**
   and select **"(Not Recommended) Allow any extension to load without
   validation or warning"**.
4. Restart Power BI Desktop. The connector appears in **Get Data** as **Quack**.

`duckdb.exe` does **not** need a manual install — `Iceberg.vbs` downloads the
latest CLI build on first run.

## Run

1. `az login` once (the refresh token persists for ~90 days; you only need to
   re-login when it expires or is revoked). The launcher fetches a fresh
   ~1-hour access token from your existing session each time it runs — so if
   OneLake queries start failing, just close the DuckDB console and relaunch
   `Iceberg.vbs` to mint a new token. No re-login needed.
2. Edit the first line of `Iceberg.vbs` to point at your warehouse:
   ```vbs
   warehouse = "<workspace>/<lakehouse>.Lakehouse"
   ```
3. Double-click `Iceberg.vbs`. A DuckDB console opens and prints
   `Serving warehouse: ...` once it's listening on `127.0.0.1:9494`.
4. Open `DQ.pbix` and Refresh. When prompted, use the **Quack** connector;
   the token is `test` (matches `quack_serve(... token => 'test')` in the launcher).

## Troubleshooting

- **`'az' not found on PATH`** — install Azure CLI (link above) and reopen the shell.
- **`Empty token. Run 'az login' first.`** — your CLI session expired; `az login` again.
- **Power BI doesn't show the Quack connector** — confirm `quack.mez` is in
  `Documents\Power BI Desktop\Custom Connectors\` and that **Allow any extension**
  is enabled under *Options → Security → Data Extensions*, then restart.



## Limitations

- **No token auto-refresh.** As far as I can tell, DuckDB does not refresh the
  Entra access token on its own. That's fine for an interactive CLI session,
  but it's a real problem here because DuckDB is acting as a long-running
  server: once the ~1-hour token expires, OneLake reads start failing and the
  only fix is to close the DuckDB console and relaunch `Iceberg.vbs` (which
  mints a new token and writes a fresh init script).
- **Remote-file cache is not persistent.** DuckDB caches remote parquet pages
  while the process is running, but the cache lives in memory only — restart
  the server and it's gone, so the next session pays the full cold-read cost
  against OneLake again.
- **Cold runs are slow.** The first query of a session is network-bound on
  parquet reads from OneLake (not CPU-bound). Subsequent queries hitting the
  in-memory cache are much faster — until you restart, see above.
