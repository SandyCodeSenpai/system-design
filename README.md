# System Design Notebook

System design questions worked end to end: requirements, back-of-envelope estimates,
high-level design, deep dives, trade-offs and pitfalls, with the diagrams written as
Mermaid so they live next to the prose and cannot drift from it.

## How this works

The markdown files under `content/` are the source of truth.

```
content/
  designs/    one file per design question   → /designs/<file-name>/
  concepts/   one file per building block    → /concepts/<file-name>/
```

The site is an [Astro](https://astro.build) app that reads them at build time. Front
matter is validated against the schema in `src/content.config.ts`; a design that fails
it fails the build rather than silently disappearing. `concepts:` in a design's front
matter must name real files under `content/concepts/`, so a dangling link cannot ship.

Every ```` ```mermaid ```` fence is rendered as a diagram in the browser, with its source
available under the figure. Code fences are highlighted at build time with Shiki, in
five switchable themes.

## Adding a design

```bash
cp content/designs/_template.md content/designs/my-design.md
# edit the front matter and write the design
npm run check    # schema and types
npm run dev      # http://localhost:4321/system-design/
```

Files starting with `_` are templates and are not published.

## Running locally

```bash
npm install
npm run dev      # dev server
npm run build    # static site in dist/
npm run preview  # serve dist/
```

## Deploying

`.github/workflows/deploy.yml` builds on every push to `main` and deploys to GitHub
Pages. Pull requests build only. Enable Pages with source "GitHub Actions" once in the
repository settings.

The repo name drives the base path. If the repo is renamed, change `REPO` in
`src/lib/site.ts` and nothing else.

## Stack

| Piece | Why |
| --- | --- |
| Astro content collections | Markdown in, validated static HTML out, zero JS by default |
| Mermaid | Diagrams as text, rendered client-side only on pages that have one |
| Shiki (via Astro) | Build-time highlighting, multi-theme through CSS variables |
| Tailwind v4 | Design tokens in CSS, no config file |

No database, no CMS, no server. Four dependencies.
