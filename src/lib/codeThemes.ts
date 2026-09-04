/**
 * The code themes the site ships, and the single source of truth for their keys.
 *
 * Astro's Shiki runs in multi-theme mode (see astro.config.mjs), so every token
 * span carries `--shiki-vs`, `--shiki-vt`, … instead of an inline colour and all
 * five palettes ship in one build. Switching is `data-code-theme` on <html>.
 *
 * Keys are two characters on purpose: each one is repeated in the `style`
 * attribute of every token span on the page.
 *
 * `surface` is the theme's own editor background, mirrored into `global.css`
 * as `--code-bg` under `[data-code-theme='<key>']`.
 */
export const CODE_THEMES = [
  { key: 'vs', id: 'dark-plus', label: 'VS Code Dark Modern', surface: '#1f1f1f' },
  { key: 'vt', id: 'vitesse-dark', label: 'Vitesse Dark', surface: '#121212' },
  { key: 'gh', id: 'github-dark-default', label: 'GitHub Dark', surface: '#0d1117' },
  { key: 'tn', id: 'tokyo-night', label: 'Tokyo Night', surface: '#1a1b26' },
  { key: 'mk', id: 'monokai', label: 'Monokai', surface: '#272822' },
] as const;

export type CodeThemeKey = (typeof CODE_THEMES)[number]['key'];

/** The theme rendered when nothing is stored — and the one plain CSS applies. */
export const DEFAULT_CODE_THEME: CodeThemeKey = 'vs';

/** localStorage key holding the reader's choice. */
export const CODE_THEME_STORAGE_KEY = 'sysdesign:code-theme';
