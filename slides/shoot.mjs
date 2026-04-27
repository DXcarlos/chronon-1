import { chromium } from 'playwright';
import { mkdir } from 'node:fs/promises';

const slide = process.argv[2];
const clicks = parseInt(process.argv[3] || '0', 10);
const name = process.argv[4] || `slide-${slide}-c${clicks}`;
if (!slide) { console.error('usage: node shoot.mjs <slide> [clicks] [name]'); process.exit(1); }
await mkdir('shots', { recursive: true });

const browser = await chromium.launch();
const ctx = await browser.newContext({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 2 });
const page = await ctx.newPage();
await page.goto(`http://localhost:3030/${slide}?clicks=${clicks}`, { waitUntil: 'networkidle' });
await page.waitForTimeout(800);
await page.reload({ waitUntil: 'networkidle' });
await page.waitForTimeout(800);
await page.screenshot({ path: `shots/${name}.png`, fullPage: false });
await browser.close();
console.log(`shots/${name}.png`);
