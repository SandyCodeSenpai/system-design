/**
 * Turns every ```mermaid fence into a `<figure class="diagram"><pre class="mermaid">`
 * so Shiki leaves it alone and the client script in Base.astro renders it.
 *
 * Without JavaScript the reader sees the diagram source, which is a fair
 * fallback: it is the same text the author wrote.
 */
const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;' };
const escape = (s) => s.replace(/[&<>]/g, (c) => ESC[c]);

function walk(node) {
  if (node.type === 'code' && node.lang === 'mermaid') {
    node.type = 'html';
    node.value = `<figure class="diagram"><pre class="mermaid">${escape(node.value)}</pre></figure>`;
    delete node.lang;
    delete node.meta;
    return;
  }
  for (const child of node.children ?? []) walk(child);
}

export function remarkMermaid() {
  return (tree) => walk(tree);
}
