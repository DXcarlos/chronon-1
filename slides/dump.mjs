import { chromium } from 'playwright';

const browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1920, height: 1080 } });
const page = await ctx.newPage();
await page.goto(`http://localhost:3030/13?clicks=6`, { waitUntil: 'networkidle' });
await page.waitForTimeout(800);

const svgHTML = await page.evaluate(() => {
  const svgs = document.querySelectorAll('svg');
  for (const s of svgs) {
    if (s.outerHTML.includes('status quo')) return s.outerHTML;
  }
  return 'no svg with status quo found';
});
console.log(svgHTML.slice(0, 5000));
await browser.close();
