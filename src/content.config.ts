import { defineCollection, reference, z } from 'astro:content';
import { glob } from 'astro/loaders';

/**
 * The markdown files under content/ are the source of truth. The schema below
 * is the whole contract: a design that does not satisfy it fails the build
 * loudly instead of being dropped from the site silently.
 *
 * Files beginning with `_` (the templates) are not content.
 */

export const DIFFICULTIES = ['Easy', 'Medium', 'Hard'] as const;

const designs = defineCollection({
  loader: glob({ pattern: '**/[^_]*.md', base: './content/designs' }),
  schema: z.object({
    title: z.string(),
    difficulty: z.enum(DIFFICULTIES),
    /** Free text; the list page groups by it. Reuse existing ones. */
    category: z.string(),
    /** One or two sentences, shown on the list page and above the design. */
    summary: z.string(),
    /** Slugs of concept pages this design leans on. Validated at build. */
    concepts: z.array(reference('concepts')).default([]),
    /** Where this question tends to come up. Display only. */
    askedAt: z.array(z.string()).default([]),
    references: z.array(z.object({ label: z.string(), url: z.string().url() })).default([]),
    /** Date written or last reworked. Drives the "Recent" sort. */
    date: z.coerce.date(),
  }),
});

const concepts = defineCollection({
  loader: glob({ pattern: '**/[^_]*.md', base: './content/concepts' }),
  schema: z.object({
    title: z.string(),
    summary: z.string(),
    date: z.coerce.date(),
  }),
});

export const collections = { designs, concepts };
