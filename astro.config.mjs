// @ts-check
import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';
import { unified } from '@astrojs/markdown-remark';
import { REPO } from './src/lib/site.ts';
import { CODE_THEMES } from './src/lib/codeThemes.ts';
import { remarkMermaid } from './src/lib/remark-mermaid.mjs';
import { rehypeTableWrap } from './src/lib/rehype-table-wrap.mjs';

// Published to GitHub Pages. A project site lives under `/<repo>/`, a user site
// (`<owner>.github.io`) at the root. Both derive from REPO in src/lib/site.ts.
const [owner, name] = REPO.split('/');
const base = name === `${owner}.github.io` ? '/' : `/${name}`;

export default defineConfig({
  site: `https://${owner}.github.io`,
  base,
  trailingSlash: 'always',
  vite: {
    plugins: [tailwindcss()],
    // mermaid is one large chunk, loaded only on pages with a diagram
    build: { chunkSizeWarningLimit: 1200 },
  },
  markdown: {
    processor: unified({
      remarkPlugins: [remarkMermaid],
      rehypePlugins: [rehypeTableWrap],
    }),
    shikiConfig: {
      // Multi-theme mode: every token carries one `--shiki-<key>` variable per
      // theme, so switching is pure CSS. See src/lib/codeThemes.ts.
      themes: Object.fromEntries(CODE_THEMES.map((t) => [t.key, t.id])),
      defaultColor: false,
    },
  },
});
