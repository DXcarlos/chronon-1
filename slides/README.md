# Chronon Summit Slides

Designing for the next 10x in Chronon performance.

## Run

```
cd slides
npm install
npm run dev
```

Opens `http://localhost:3030`. Hot reloads on edits to `slides.md`.

## Deploy

```
npm run deploy      # build + rsync to gs://chronon-slides-static
```

Live at `https://storage.googleapis.com/chronon-slides-static/index.html`.

## Export

```
npm run build       # static site → dist/
npm run export      # PDF / PPTX
```
