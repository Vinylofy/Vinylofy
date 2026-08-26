import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import test from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

function walk(dir, files = []) {
  if (!existsSync(dir)) return files;

  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    const rel = relative(root, path);

    if (
      rel === ".git" ||
      rel === "node_modules" ||
      rel === ".next" ||
      rel.startsWith(".git/") ||
      rel.startsWith("node_modules/") ||
      rel.startsWith(".next/")
    ) {
      continue;
    }

    if (statSync(path).isDirectory()) {
      walk(path, files);
    } else {
      files.push(rel);
    }
  }

  return files;
}

function appRouteExists(pathname) {
  if (pathname === "/") {
    return existsSync(join(root, "app/page.tsx"));
  }

  const segmentPath = pathname.replace(/^\//, "");
  return (
    existsSync(join(root, "app", segmentPath, "page.tsx")) ||
    existsSync(join(root, "app", segmentPath, "route.ts"))
  );
}

test("gebruiksvoorwaarden route source contains the public page contract", () => {
  const source = read("app/gebruiksvoorwaarden/page.tsx");

  assert.match(source, /export default function GebruiksvoorwaardenPage/);
  assert.match(source, /Gebruiksvoorwaarden & databescherming/);
  assert.match(source, /Dit mag wel/);
  assert.match(source, /Hiervoor heb je toestemming nodig/);
  assert.match(source, /Laatst bijgewerkt: \{LAST_UPDATED_LABEL\}/);
  assert.match(source, /canonical: "https:\/\/vinylofy\.com\/gebruiksvoorwaarden"/);
  assert.doesNotMatch(source, /robots:\s*\{[^}]*index:\s*false/s);
});

test("footer links to the gebruiksvoorwaarden route", () => {
  const source = read("components/site-footer.tsx");

  assert.match(source, /href="\/gebruiksvoorwaarden"/);
  assert.match(source, /Gebruiksvoorwaarden/);
});

test("visible internal links on the gebruiksvoorwaarden page target existing app routes", () => {
  const source = read("app/gebruiksvoorwaarden/page.tsx");
  const hrefs = Array.from(source.matchAll(/href="(\/[^"#?]*)"/g), (match) => match[1]);

  assert.ok(hrefs.length > 0, "expected at least one internal link");

  for (const href of hrefs) {
    assert.ok(appRouteExists(href), `missing app route for ${href}`);
  }
});

test("tdmrep route returns the required numeric reservation contract", () => {
  const source = read("app/.well-known/tdmrep.json/route.ts");

  assert.match(source, /"tdm-reservation": 1/);
  assert.doesNotMatch(source, /"tdm-reservation": "1"/);
  assert.match(source, /"content-type": "application\/json; charset=utf-8"/);
});

test("sitewide TDM meta is present without blocking search indexing", () => {
  const layoutSource = read("app/layout.tsx");
  const pageSource = read("app/gebruiksvoorwaarden/page.tsx");

  assert.match(layoutSource, /"tdm-reservation": "1"/);
  assert.doesNotMatch(pageSource, /noindex|nofollow/);
});

test("robots and sitemap source files do not contain a broad crawl block", () => {
  const files = walk(root).filter((file) =>
    /(^|\/)(robots\.(txt|ts)|sitemap\.(xml|ts))$/.test(file),
  );

  for (const file of files) {
    const source = read(file);
    assert.doesNotMatch(source, /Disallow:\s*\/\s*$/m, `${file} blocks all crawling`);
  }
});
