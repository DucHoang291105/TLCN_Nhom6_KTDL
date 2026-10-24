"""Screenshot every provisioned Superset dashboard (real browser rendering).

Logs in with the admin account from .env, opens each dashboard listed in
docs/validation/superset_dashboard_check.json, waits until no chart is still
loading and saves a full-page PNG to docs/evidence/<folder>/.

Run on the host: python -m src.dashboard.screenshot_dashboards [folder]
Requires: pip install playwright && python -m playwright install chromium
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from src.dashboard.provision_superset import BASE, PROJECT_ROOT, env

LOADING = ".loading, .ant-skeleton, [data-test='loading-indicator']"


def main(folder: str) -> None:
    from playwright.sync_api import sync_playwright

    check = json.loads((PROJECT_ROOT / "docs/validation/superset_dashboard_check.json").read_text(encoding="utf-8"))
    out = PROJECT_ROOT / "docs/evidence" / folder
    out.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1600, "height": 1000}, device_scale_factor=1)
        for attempt in range(3):
            page.goto(f"{BASE}/login/")
            # Superset 6 renders a React form next to a hidden input; type into
            # the visible fields only, then press the "Sign in" button.
            fields = page.locator("input:visible")
            fields.first.wait_for(timeout=60_000)
            fields.nth(0).fill(env("SUPERSET_ADMIN_USERNAME", "admin"))
            page.locator("input[type='password']:visible").fill(env("SUPERSET_ADMIN_PASSWORD"))
            page.get_by_role("button", name="Sign in").click()
            try:
                page.wait_for_url(lambda url: "/login" not in url, timeout=30_000)
                break
            except Exception:
                if attempt == 2:
                    raise SystemExit("Superset login failed; no screenshot written")
        for index, dash in enumerate(check["dashboards"], start=1):
            page.goto(dash["url"] + "?standalone=2")
            page.wait_for_load_state("networkidle")
            # Never save a login or error page as evidence.
            if "/login" in page.url:
                raise SystemExit(f"Not logged in while opening {dash['url']}")
            page.wait_for_selector(".dashboard-component-chart-holder", timeout=60_000)
            if "/login" in page.url:
                raise SystemExit(f"Redirected to login while opening {dash['url']}")
            page.wait_for_timeout(3000)
            for _ in range(60):
                if page.locator(LOADING).count() == 0:
                    break
                page.wait_for_timeout(1000)
            page.wait_for_timeout(2000)
            slug = dash["url"].rstrip("/").split("/")[-1]
            path = out / f"{index:02d}_{slug}.png"
            page.screenshot(path=str(path), full_page=True)
            print(path)
        browser.close()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "personC_20261024")
