import { chromium } from 'playwright';

const slide = process.argv[2] || '13';
const browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1920, height: 1080 } });
const page = await ctx.newPage();
await page.goto(`http://localhost:3030/${slide}?clicks=5`, { waitUntil: 'networkidle' });
await page.waitForTimeout(800);

const cdp = await ctx.newCDPSession(page);
await cdp.send('DOM.enable');
await cdp.send('CSS.enable');
const { root } = await cdp.send('DOM.getDocument', { depth: -1 });

function find(node, name) {
  if (node.nodeName === name) return node;
  if (!node.children) return null;
  for (const c of node.children) {
    const r = find(c, name);
    if (r) return r;
  }
  return null;
}
const textNode = find(root, 'text');
if (!textNode) { console.log('no text node'); await browser.close(); process.exit(1); }

const matched = await cdp.send('CSS.getMatchedStylesForNode', { nodeId: textNode.nodeId });
const fontRules = [];
for (const m of matched.matchedCSSRules || []) {
  for (const p of m.rule.style.cssProperties) {
    if (p.name === 'font-size') fontRules.push({ selector: m.rule.selectorList.text, value: p.value, source: m.rule.origin });
  }
}
for (const i of matched.inherited || []) {
  for (const m of i.matchedCSSRules || []) {
    for (const p of m.rule.style.cssProperties) {
      if (p.name === 'font-size') fontRules.push({ selector: m.rule.selectorList.text + ' (inherited)', value: p.value, source: m.rule.origin });
    }
  }
}
console.log(JSON.stringify(fontRules, null, 2));
await browser.close();
