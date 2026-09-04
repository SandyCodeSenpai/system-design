/** Wrap every markdown table in a scroll box so a wide table never widens the page. */
function walk(node) {
  const kids = node.children;
  if (!kids) return;
  for (let i = 0; i < kids.length; i++) {
    const child = kids[i];
    if (child.type === 'element' && child.tagName === 'table') {
      kids[i] = {
        type: 'element',
        tagName: 'div',
        properties: { className: ['table-wrap'] },
        children: [child],
      };
    } else {
      walk(child);
    }
  }
}

export function rehypeTableWrap() {
  return (tree) => walk(tree);
}
