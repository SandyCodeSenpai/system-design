/**
 * Small build-time helpers shared by pages and components.
 *
 * Everything internal must be routed through `href()`: the site is served from
 * a GitHub Pages sub-path, so a hard-coded root-absolute URL 404s in production.
 */

/** The one line to change when this repo moves. Drives base path, links and CI. */
export const REPO = 'SandyCodeSenpai/system-design';
export const REPO_URL = `https://github.com/${REPO}`;
export const BRANCH = 'main';
export const OWNER = 'Sai Sandeep Mandava';
export const SITE_NAME = 'System Design Notebook';

/** Prefix an internal path with the configured base. `href('/concepts/')`. */
export function href(path: string): string {
  const raw = import.meta.env.BASE_URL as string;
  const base = raw.endsWith('/') ? raw.slice(0, -1) : raw;
  const rest = path.startsWith('/') ? path : `/${path}`;
  return `${base}${rest}` || '/';
}

function encodePath(repoPath: string): string {
  return repoPath.split('/').map(encodeURIComponent).join('/');
}

/** Permalink to a file in the repo on github.com. */
export function githubBlobUrl(repoPath: string): string {
  return `${REPO_URL}/blob/${BRANCH}/${encodePath(repoPath)}`;
}

/** Open the file straight into the github.dev web editor. */
export function githubDevUrl(repoPath: string): string {
  return `https://github.dev/${REPO}/blob/${BRANCH}/${encodePath(repoPath)}`;
}

/** `#03` — position in the list, zero-padded so the column lines up. */
export function ref(index: number): string {
  return `#${String(index + 1).padStart(2, '0')}`;
}

/** Count of ```mermaid fences in a markdown body. */
export function diagramCount(body: string | undefined): number {
  return (body?.match(/^```mermaid/gm) ?? []).length;
}
